/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.NamePage;
import io.sirix.settings.Constants;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

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
 * sub-trie per column). Each column starts with one contiguous base run reserved from the offset's
 * own counter, anchored by a stable {@link ValueDictionaryHeaderNode} whose key the projection's
 * metadata records. Maintenance copy-on-writes only affected paths in immutable forward-hash and
 * reverse-id radix directories while retaining the original header anchor:
 *
 * <pre>
 *   headerKey                                  the {@link ValueDictionaryHeaderNode}
 *   header.forwardRootKey                    hash-prefix radix root
 *   header.reverseRootKey                    id-prefix radix root
 *   radix leaf                               immutable hash or value bucket
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
 * <h2>Persistent record packing</h2>
 *
 * Every dictionary append occupies the smallest possible persistent units: its record keys are a
 * dense, stride-one interval.  Large individual values already use the key-value page's overflow
 * mechanism; leaving 63 empty slots between ordinary dictionary records would instead multiply
 * indirect-page and leaf-page churn without providing an ownership or versioning guarantee.
 *
 * <h2>Cost model</h2>
 *
 * Every method here is a per-LITERAL or per-WINNER cost, never per-row. Rows carry ids and are
 * compared as integers; the only things that cross into this class are the literals a predicate
 * mentions and the values of the groups a query actually returns.
 */
public final class GlobalValueDictionary {

  private static final boolean HFT_TELEMETRY_ENABLED = Boolean.getBoolean("sirix.hft.telemetry");
  private static final AtomicInteger HFT_MAX_PROBE_UNITS = new AtomicInteger();
  private static final LongHashFunction SECONDARY_HASH = LongHashFunction.xx3();

  public static final int PERSISTENT_RECORD_STRIDE = 1;

  public static final int PERSISTENT_RECORDS_PER_PAGE =
      Constants.INP_REFERENCE_COUNT / PERSISTENT_RECORD_STRIDE;

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

  public static long maximumKeysToReserve(final int entryCount) {
    if (entryCount < 0) throw new IllegalArgumentException("entryCount must not be negative");
    final long reverseBuckets = (entryCount + 255L) >>> 8;
    final long maximumRecords = 13L * entryCount + 4L * reverseBuckets;
    return 1L + Math.multiplyExact(maximumRecords, PERSISTENT_RECORD_STRIDE);
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

  static long secondaryValueHash(final byte[] utf8, final int off, final int len) {
    return SECONDARY_HASH.hashBytes(utf8, off, len);
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
    return GlobalValueDictionaryRadix.value(header.getReverseRootKey(), id, namePage,
        databaseType, reader);
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
    return probe(headerNodeKey, utf8, 0, utf8.length, reader);
  }

  static int probe(final long headerNodeKey, final byte[] utf8, final int offset, final int length,
      final StorageEngineReader reader) {
    Objects.checkFromIndexSize(offset, length, utf8.length);
    final ValueDictionaryHeaderNode header = header(headerNodeKey, reader);
    if (header == null || !header.isDirectoryComplete()) {
      return ID_UNKNOWN;
    }
    final DatabaseType databaseType = databaseTypeOf(reader);
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    final long wanted = valueHash(utf8, offset, length);
    final long secondary = secondaryValueHash(utf8, offset, length);
    final GlobalValueDictionaryRadix.ProbeResult result = GlobalValueDictionaryRadix.probe(
        header.getForwardRootKey(), header.getReverseRootKey(), header.getEntryCount(), wanted,
        secondary, utf8, offset, length, namePage, databaseType, reader);
    return recordProbeResult(result.id(), result.units());
  }

  private static int recordProbeResult(final int result, final int probeUnits) {
    if (HFT_TELEMETRY_ENABLED) {
      int maximum = HFT_MAX_PROBE_UNITS.get();
      while (probeUnits > maximum && !HFT_MAX_PROBE_UNITS.compareAndSet(maximum, probeUnits)) {
        maximum = HFT_MAX_PROBE_UNITS.get();
      }
    }
    return result;
  }

  public static void resetProbeTelemetry() {
    HFT_MAX_PROBE_UNITS.set(0);
  }

  public static int maxProbeUnits() {
    return HFT_MAX_PROBE_UNITS.get();
  }

}
