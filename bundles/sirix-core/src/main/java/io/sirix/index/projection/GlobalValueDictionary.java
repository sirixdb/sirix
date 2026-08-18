/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.ValueDictionaryDirectoryNode;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.NamePage;
import io.sirix.settings.Constants;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Read access to the global projection value dictionary — the {@code id <-> value} mapping that
 * backs {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL} columns.
 *
 * <h2>Why the dictionary is global</h2>
 *
 * A {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_DICT} column carries one dictionary per
 * row group, which is the right shape when a column has a few dozen distinct values: every leaf
 * stores each value once and the ids pack into a handful of bits. It is the wrong shape when a
 * column has millions, because then a recurring value is stored once <em>per leaf</em> — hundreds
 * of copies of the same string — and the column's bytes come out roughly the size of the raw
 * strings. Worse, nothing about a per-leaf id is comparable across leaves, so group identity has to
 * be recovered by hashing the bytes back out of every leaf's dictionary.
 *
 * <p>A global dictionary fixes both at once: a value is stored exactly once for the whole resource,
 * and the id it is stored under <em>is</em> its identity. Grouping becomes an integer group-by,
 * distinct-counting becomes a fold over integers, and equality against a literal becomes an integer
 * compare after a single probe.
 *
 * <h2>Node-key layout</h2>
 *
 * One sub-trie holds every column's dictionary (see
 * {@link NamePage#JSON_PROJECTION_VALUE_DICTIONARY_REFERENCE_OFFSET} for why it cannot be one
 * sub-trie per column). Each column's dictionary occupies one contiguous run of node keys reserved
 * from the offset's own counter, anchored by a {@link ValueDictionaryHeaderNode} whose key the
 * projection's metadata records:
 *
 * <pre>
 *   headerKey                                  the {@link ValueDictionaryHeaderNode}
 *   header.entryBase + {@value #ENTRY_STRIDE} * (id - 1)      the value for id, id &gt;= 1
 *   header.directoryBase + {@value #DIRECTORY_STRIDE} * block one {@link ValueDictionaryDirectoryNode}
 * </pre>
 *
 * <h2>Why a run, and not a namespace computed from the column</h2>
 *
 * Partitioning the key space by {@code (projectionDefId, columnOrdinal)} with a fixed stride is the
 * obvious layout and it does not work. The indirect-page trie underneath a sub-trie adds a level
 * only when the page key being prepared is exactly the power-of-two boundary of its current height
 * ({@code KeyedTrieWriter#prepareLeafOfTree}), so keys have to be allocated densely and
 * monotonically for the trie to ever grow deep enough to address them. A strided base leaps past
 * every boundary without triggering growth, and the traversal then resolves every page key to the
 * root reference — records at keys billions apart land on the same page and overwrite each other,
 * silently. Reserving a dense run is the shape the trie actually supports, and it costs nothing:
 * the run's start is one long in the projection metadata, which already travels with the column.
 *
 * <h2>Why ids do not sit at consecutive record keys</h2>
 *
 * A record page holds {@link io.sirix.settings.Constants#INP_REFERENCE_COUNT} records and its
 * slotted buffer has a hard capacity ceiling; a page whose records do not fit is a loud failure at
 * commit, not a slow path. Packing 1024 values of a few hundred bytes each into one page would
 * reach it. The {@value #ENTRY_STRIDE} stride puts {@value #ENTRIES_PER_PAGE} values on a page
 * instead, which keeps ids DENSE (so they stay a good dense group-by key) while bounding a page's
 * bytes with four times the headroom. Reserved-but-unused keys cost nothing; they are simply empty
 * slots, and the page keys they occupy still advance densely, which is what the trie needs.
 *
 * <h2>Cost model</h2>
 *
 * Every method here is a per-LITERAL or per-WINNER cost, never per-row. Rows carry ids and are
 * compared as integers; the only things that cross into this class are the literals a predicate
 * mentions and the values of the groups a query actually returns.
 */
public final class GlobalValueDictionary {

  /**
   * Node keys reserved per value. See the class javadoc: it is what bounds a record page's bytes
   * while leaving ids dense.
   */
  public static final int ENTRY_STRIDE = 4;

  /** Values that therefore share one record page. */
  public static final int ENTRIES_PER_PAGE = Constants.INP_REFERENCE_COUNT / ENTRY_STRIDE;

  /** Node keys reserved per directory block; a block is far bigger than a value. */
  public static final int DIRECTORY_STRIDE = 16;

  /** Directory blocks that therefore share one record page — about 100 KiB of them. */
  public static final int DIRECTORY_BLOCKS_PER_PAGE = Constants.INP_REFERENCE_COUNT / DIRECTORY_STRIDE;

  /** Answer of {@link #probe} when the dictionary provably does not hold the value. */
  public static final int ID_ABSENT = 0;

  /**
   * Answer of {@link #probe} when the dictionary cannot say. The caller must fall back to a route
   * that does not depend on the mapping rather than treat it as absent.
   */
  public static final int ID_UNKNOWN = -1;

  private GlobalValueDictionary() {
    throw new AssertionError("no instances");
  }

  /** How many node keys a dictionary of {@code entryCount} values and {@code blockCount} blocks needs. */
  public static long keysToReserve(final int entryCount, final int blockCount) {
    return 1L + (long) ENTRY_STRIDE * entryCount + (long) DIRECTORY_STRIDE * blockCount;
  }

  /** The node key holding the value for {@code id}. */
  public static long entryKey(final ValueDictionaryHeaderNode header, final int id) {
    return header.getEntryBase() + (long) ENTRY_STRIDE * (id - 1);
  }

  /** The node key holding directory block {@code block}. */
  public static long directoryKey(final ValueDictionaryHeaderNode header, final int block) {
    return header.getDirectoryBase() + (long) DIRECTORY_STRIDE * block;
  }

  /**
   * The hash a value is indexed under in the forward directory.
   *
   * @param utf8 the value's UTF-8 bytes
   * @param off offset into {@code utf8}
   * @param len length in {@code utf8}
   * @return the value hash
   */
  public static long valueHash(final byte[] utf8, final int off, final int len) {
    return ProjectionIndexByteScan.fnv1a64(utf8, off, len);
  }

  /**
   * The database type of a reader's resource. Mirrors the single derivation point on the reader
   * ({@code NodeStorageEngineReader#databaseType}), which is package-private to the page-access
   * layer; the two must agree, or records get written under one offset and looked up under another.
   *
   * @param reader the reader whose resource is wanted
   * @return the database type
   */
  public static DatabaseType databaseTypeOf(final StorageEngineReader reader) {
    return reader.getResourceSession() instanceof JsonResourceSession
        ? DatabaseType.JSON
        : DatabaseType.XML;
  }

  /**
   * Read a dictionary's header.
   *
   * @param headerNodeKey the header's node key, as recorded in the projection's metadata
   * @param reader the reader positioned at the revision wanted
   * @return the header, or {@code null} when this revision holds no readable dictionary there
   * @throws IllegalStateException if the record at that key is not a header
   */
  public static @Nullable ValueDictionaryHeaderNode header(final long headerNodeKey,
      final StorageEngineReader reader) {
    if (headerNodeKey <= 0) {
      return null;
    }
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    if (!namePage.hasProjectionValueDictionary(databaseType)) {
      return null;
    }
    final DataRecord record =
        namePage.getProjectionValueDictionaryRecord(headerNodeKey, databaseType, reader);
    if (record == null) {
      return null;
    }
    if (!(record instanceof ValueDictionaryHeaderNode header)) {
      throw new IllegalStateException("record at value dictionary header key " + headerNodeKey + " is a "
          + record.getKind() + ", not a header");
    }
    // An unknown layout is "no dictionary I can read", not a failure: a resource written by a newer
    // build must make an older one decline, never misparse.
    return header.getVersion() == ValueDictionaryHeaderNode.VERSION
        ? header
        : null;
  }

  /**
   * Materialise the value behind an id — the reverse direction, one record read.
   *
   * @param headerNodeKey the dictionary's header key
   * @param id the value id
   * @param reader the reader positioned at the revision wanted
   * @return the value's UTF-8 bytes, or {@code null} when the id is not stored in this revision
   */
  public static byte @Nullable [] valueBytes(final long headerNodeKey, final int id,
      final StorageEngineReader reader) {
    final ValueDictionaryHeaderNode header = header(headerNodeKey, reader);
    if (header == null) {
      return null;
    }
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    return valueBytes(header, id, namePage, databaseType, reader);
  }

  /** As above, with the header and page lookups already done. */
  private static byte @Nullable [] valueBytes(final ValueDictionaryHeaderNode header, final int id,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineReader reader) {
    if (id < 1 || id > header.getEntryCount()) {
      return null;
    }
    final DataRecord record =
        namePage.getProjectionValueDictionaryRecord(entryKey(header, id), databaseType, reader);
    if (record == null) {
      return null;
    }
    if (!(record instanceof ValueDictionaryEntryNode entry)) {
      throw new IllegalStateException(
          "record for value dictionary id " + id + " is a " + record.getKind() + ", not a value entry");
    }
    return entry.getValue();
  }

  /**
   * Materialise the value behind an id as a string.
   *
   * @param headerNodeKey the dictionary's header key
   * @param id the value id
   * @param reader the reader positioned at the revision wanted
   * @return the value, or {@code null} when the id is not stored in this revision
   */
  public static @Nullable String value(final long headerNodeKey, final int id,
      final StorageEngineReader reader) {
    final byte[] bytes = valueBytes(headerNodeKey, id, reader);
    return bytes == null
        ? null
        : new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Materialise several ids at once, resolving the header and page lookups a single time.
   *
   * <p>The winner-materialisation path: a top-k group-by hands over the k ids it is about to return
   * and gets their strings back. Ids are visited in ascending order so that ids sharing a record
   * page are resolved consecutively, which is what turns k random reads into far fewer page
   * touches; the caller's order is restored through the index carried alongside.
   *
   * @param headerNodeKey the dictionary's header key
   * @param ids the ids to resolve; not modified
   * @param reader the reader positioned at the revision wanted
   * @return the values, index-aligned to {@code ids}; an entry is {@code null} when its id is not
   *         stored in this revision
   */
  public static @Nullable String[] values(final long headerNodeKey, final int[] ids,
      final StorageEngineReader reader) {
    final String[] out = new String[ids.length];
    if (ids.length == 0) {
      return out;
    }
    final ValueDictionaryHeaderNode header = header(headerNodeKey, reader);
    if (header == null) {
      return out;
    }
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    final int[] order = new int[ids.length];
    for (int i = 0; i < ids.length; i++) {
      order[i] = i;
    }
    sortIndicesByValue(order, ids);
    for (final int slot : order) {
      final byte[] bytes = valueBytes(header, ids[slot], namePage, databaseType, reader);
      if (bytes != null) {
        out[slot] = new String(bytes, StandardCharsets.UTF_8);
      }
    }
    return out;
  }

  /**
   * Insertion-sort {@code order} (a permutation of {@code 0..n-1}) by {@code values[order[i]]}. The
   * arrays are k elements long, k being a query's LIMIT — insertion sort beats anything with an
   * allocation at that size.
   */
  private static void sortIndicesByValue(final int[] order, final int[] values) {
    for (int i = 1; i < order.length; i++) {
      final int slot = order[i];
      final int key = values[slot];
      int j = i - 1;
      while (j >= 0 && values[order[j]] > key) {
        order[j + 1] = order[j];
        j--;
      }
      order[j + 1] = slot;
    }
  }

  /**
   * Resolve a value to its id — the forward direction, a binary search over the directory.
   *
   * <p>Answers {@link #ID_ABSENT} only when the directory is complete and provably does not hold
   * the value; a directory that was never written, one that does not cover every id, and an
   * unreadable header all answer {@link #ID_UNKNOWN}, because "I cannot see it" and "it is not
   * there" lead to opposite query results and must never be confused. A hash match is confirmed by
   * reading the candidate's value entry and comparing bytes, so a hash collision costs an extra
   * read rather than a wrong id.
   *
   * @param headerNodeKey the dictionary's header key
   * @param utf8 the value's UTF-8 bytes
   * @param reader the reader positioned at the revision wanted
   * @return the id, {@link #ID_ABSENT}, or {@link #ID_UNKNOWN}
   */
  public static int probe(final long headerNodeKey, final byte[] utf8, final StorageEngineReader reader) {
    final ValueDictionaryHeaderNode header = header(headerNodeKey, reader);
    if (header == null || !header.isDirectoryComplete()) {
      return ID_UNKNOWN;
    }
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    final long wanted = valueHash(utf8, 0, utf8.length);
    final int blockCount = header.getDirectoryBlockCount();

    // Binary search for the LAST block whose first hash is <= wanted: blocks are sorted and
    // contiguous, so the run holding the value can only start there.
    int lo = 0;
    int hi = blockCount - 1;
    int candidate = 0;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      final ValueDictionaryDirectoryNode block = directoryBlock(header, mid, namePage, databaseType, reader);
      if (block == null) {
        return ID_UNKNOWN;
      }
      if (Long.compareUnsigned(block.getHashes()[0], wanted) <= 0) {
        candidate = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }

    // A run of equal hashes can straddle a block boundary, so keep walking while a following
    // block still opens on the wanted hash.
    for (int b = candidate; b < blockCount; b++) {
      final ValueDictionaryDirectoryNode block = directoryBlock(header, b, namePage, databaseType, reader);
      if (block == null) {
        return ID_UNKNOWN;
      }
      final long[] hashes = block.getHashes();
      if (b > candidate && Long.compareUnsigned(hashes[0], wanted) != 0) {
        break;
      }
      final int[] ids = block.getIds();
      for (int i = lowerBound(hashes, wanted); i < hashes.length; i++) {
        if (Long.compareUnsigned(hashes[i], wanted) != 0) {
          return ID_ABSENT;
        }
        final byte[] stored = valueBytes(header, ids[i], namePage, databaseType, reader);
        if (stored != null && Arrays.equals(stored, utf8)) {
          return ids[i];
        }
      }
    }
    return ID_ABSENT;
  }

  /** Index of the first entry in {@code hashes} not unsigned-less-than {@code wanted}. */
  private static int lowerBound(final long[] hashes, final long wanted) {
    int lo = 0;
    int hi = hashes.length;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (Long.compareUnsigned(hashes[mid], wanted) < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  private static @Nullable ValueDictionaryDirectoryNode directoryBlock(final ValueDictionaryHeaderNode header,
      final int block, final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineReader reader) {
    final DataRecord record =
        namePage.getProjectionValueDictionaryRecord(directoryKey(header, block), databaseType, reader);
    if (record == null) {
      return null;
    }
    if (!(record instanceof ValueDictionaryDirectoryNode directory)) {
      throw new IllegalStateException("record for value dictionary directory block " + block + " is a "
          + record.getKind() + ", not a directory block");
    }
    return directory;
  }
}
