/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.node.ValueDictionaryDirectoryNode;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.page.NamePage;

import java.util.Arrays;

/**
 * Build-side half of the global projection value dictionary: interns values during a build and
 * materialises the namespace at the end of it.
 *
 * <h2>Why interning is done in memory and not against the trie</h2>
 *
 * The build asks "what is this value's id" once per ROW — ten million times on a ten-million-row
 * corpus. Answering that from the persistent forward directory would cost a binary search per row,
 * which is not a slower build, it is an impossible one. So the mapping is held in memory for the
 * duration of the build and the persistent structures are produced from it in one pass at the end,
 * which is also the only moment the entry count is known exactly — and therefore the only moment a
 * sorted directory can be laid out without guessing a capacity.
 *
 * <p>The memory that costs is one copy of each DISTINCT value plus twelve bytes of index per value:
 * about 45 MB per million distinct 32-byte values. It is transient — {@link #release()} drops it —
 * and it is bounded by the distinct count rather than the row count, which is what makes it
 * affordable at a scale where holding a per-row structure would not be. Nothing here allocates per
 * ROW: {@link #intern} takes a byte range and compares it against the arena in place.
 *
 * <h2>Structures written</h2>
 *
 * <ul>
 * <li>one {@link ValueDictionaryEntryNode} per value, at the key its id names — the reverse
 * direction, which needs no index because the id <em>is</em> the address;</li>
 * <li>{@link ValueDictionaryDirectoryNode} blocks holding every {@code (valueHash, id)} pair sorted
 * by hash — the forward direction;</li>
 * <li>one {@link ValueDictionaryHeaderNode} saying how much of each exists.</li>
 * </ul>
 */
public final class GlobalValueDictionaryWriter {

  /** Initial arena size; grown by doubling. */
  private static final int INITIAL_ARENA_BYTES = 1 << 16;

  /** Initial open-addressing capacity; a power of two, grown by doubling at half load. */
  private static final int INITIAL_TABLE_CAPACITY = 1 << 12;

  /** Concatenated value bytes; {@code offsets[id]} and {@code lengths[id]} slice into it. */
  private byte[] arena = new byte[INITIAL_ARENA_BYTES];

  private int arenaLength;

  /** Start of each value in {@link #arena}, indexed by id. Slot 0 is unused; ids start at 1. */
  private int[] offsets = new int[64];

  /** Length of each value in {@link #arena}, indexed by id. */
  private int[] lengths = new int[64];

  /** Value hash per id, kept so the directory can be built without rehashing the arena. */
  private long[] hashes = new long[64];

  /** Open-addressed slots: the value hash, meaningful only where {@link #tableIds} is non-zero. */
  private long[] tableHashes = new long[INITIAL_TABLE_CAPACITY];

  /** Open-addressed slots: the id, {@code 0} marking a free slot. */
  private int[] tableIds = new int[INITIAL_TABLE_CAPACITY];

  /** How many distinct values have been interned; the highest id in use. */
  private int entryCount;

  private boolean released;

  /** How many distinct values have been interned. */
  public int entryCount() {
    return entryCount;
  }

  /** Total bytes of distinct value data held, for the build's memory accounting. */
  public int valueBytes() {
    return arenaLength;
  }

  /**
   * Intern a value and return its id, minting one if the value is new.
   *
   * <p>Ids count from 1 so that {@code 0} can mean "no id" everywhere downstream without a separate
   * presence check.
   *
   * @param src the buffer holding the value's UTF-8 bytes
   * @param off offset into {@code src}
   * @param len length in {@code src}
   * @return the value's id, {@code >= 1}
   */
  public int intern(final byte[] src, final int off, final int len) {
    if (released) {
      throw new IllegalStateException("value dictionary writer was released");
    }
    final long hash = GlobalValueDictionary.valueHash(src, off, len);
    final int mask = tableIds.length - 1;
    int slot = ((int) (hash ^ (hash >>> 32))) & mask;
    while (true) {
      final int id = tableIds[slot];
      if (id == 0) {
        return insertAt(slot, hash, src, off, len);
      }
      if (tableHashes[slot] == hash && regionMatches(id, src, off, len)) {
        return id;
      }
      slot = (slot + 1) & mask;
    }
  }

  private boolean regionMatches(final int id, final byte[] src, final int off, final int len) {
    if (lengths[id] != len) {
      return false;
    }
    return Arrays.equals(arena, offsets[id], offsets[id] + len, src, off, off + len);
  }

  private int insertAt(final int slot, final long hash, final byte[] src, final int off, final int len) {
    final int id = entryCount + 1;
    if (id + 1 >= offsets.length) {
      final int grown = offsets.length << 1;
      offsets = Arrays.copyOf(offsets, grown);
      lengths = Arrays.copyOf(lengths, grown);
      hashes = Arrays.copyOf(hashes, grown);
    }
    while (arenaLength + len > arena.length) {
      arena = Arrays.copyOf(arena, Math.max(arena.length << 1, arenaLength + len));
    }
    System.arraycopy(src, off, arena, arenaLength, len);
    offsets[id] = arenaLength;
    lengths[id] = len;
    hashes[id] = hash;
    arenaLength += len;
    entryCount = id;
    tableHashes[slot] = hash;
    tableIds[slot] = id;
    if ((entryCount << 1) >= tableIds.length) {
      rehash();
    }
    return id;
  }

  private void rehash() {
    final int capacity = tableIds.length << 1;
    final long[] newHashes = new long[capacity];
    final int[] newIds = new int[capacity];
    final int mask = capacity - 1;
    for (int i = 0; i < tableIds.length; i++) {
      final int id = tableIds[i];
      if (id == 0) {
        continue;
      }
      final long hash = tableHashes[i];
      int slot = ((int) (hash ^ (hash >>> 32))) & mask;
      while (newIds[slot] != 0) {
        slot = (slot + 1) & mask;
      }
      newHashes[slot] = hash;
      newIds[slot] = id;
    }
    tableHashes = newHashes;
    tableIds = newIds;
  }

  /**
   * The bytes behind an id, copied out. Build-time only; the query path reads the persisted record
   * instead.
   *
   * @param id the value id
   * @return a fresh copy of the value's UTF-8 bytes
   */
  public byte[] valueBytes(final int id) {
    if (id < 1 || id > entryCount) {
      throw new IllegalArgumentException("no such value dictionary id: " + id);
    }
    return Arrays.copyOfRange(arena, offsets[id], offsets[id] + lengths[id]);
  }

  /**
   * Write the dictionary: every value entry, the forward directory over all of them, and the header
   * that ties the two together.
   *
   * <p>Every key comes out of one contiguous run reserved up front, because that is the only key
   * shape the sub-trie's indirect-page traversal can address — see
   * {@link NamePage#reserveProjectionValueDictionaryKeys}. Records are written in ascending key
   * order so each record page is prepared in turn, which is what lets the trie grow its levels at
   * the boundaries it expects.
   *
   * <p>The header is written last so a dictionary is never advertised as complete before the
   * structures it describes exist; a torn write leaves an older header, or none, which readers
   * treat as "cannot say" rather than as an empty dictionary.
   *
   * @param namePage the name page of the revision being built
   * @param databaseType the database type, which fixes the dictionary offset
   * @param storageEngineWriter the writer for the revision being built
   * @param log the transaction intent log of the revision being built
   * @return the header's node key, which is what a reader needs to find this dictionary again
   */
  public long flush(final NamePage namePage, final DatabaseType databaseType,
      final StorageEngineWriter storageEngineWriter, final TransactionIntentLog log) {
    if (released) {
      throw new IllegalStateException("value dictionary writer was released");
    }
    namePage.createProjectionValueDictionaryTree(databaseType, storageEngineWriter, log);

    final int perBlock = ValueDictionaryDirectoryNode.ENTRIES_PER_BLOCK;
    final int blockCount = (entryCount + perBlock - 1) / perBlock;
    final long runStart = namePage.reserveProjectionValueDictionaryKeys(databaseType,
        GlobalValueDictionary.keysToReserve(entryCount, blockCount));
    final long headerKey = runStart;
    final long entryBase = runStart + 1;
    final long directoryBase = entryBase + (long) GlobalValueDictionary.ENTRY_STRIDE * entryCount;

    final ValueDictionaryHeaderNode header = new ValueDictionaryHeaderNode(headerKey,
        ValueDictionaryHeaderNode.VERSION, entryCount, entryBase, directoryBase, blockCount, entryCount);

    for (int id = 1; id <= entryCount; id++) {
      namePage.putProjectionValueDictionaryRecord(
          new ValueDictionaryEntryNode(GlobalValueDictionary.entryKey(header, id),
              Arrays.copyOfRange(arena, offsets[id], offsets[id] + lengths[id])),
          databaseType, storageEngineWriter, log);
    }

    writeDirectory(header, blockCount, namePage, databaseType, storageEngineWriter, log);

    namePage.putProjectionValueDictionaryRecord(header, databaseType, storageEngineWriter, log);
    return headerKey;
  }

  /**
   * Sort every {@code (valueHash, id)} pair by hash and write it out in blocks.
   */
  private void writeDirectory(final ValueDictionaryHeaderNode header, final int blockCount,
      final NamePage namePage, final DatabaseType databaseType, final StorageEngineWriter storageEngineWriter,
      final TransactionIntentLog log) {
    if (entryCount == 0) {
      return;
    }
    final long[] sortedHashes = new long[entryCount];
    final int[] sortedIds = new int[entryCount];
    for (int id = 1; id <= entryCount; id++) {
      sortedHashes[id - 1] = hashes[id];
      sortedIds[id - 1] = id;
    }
    radixSortByHash(sortedHashes, sortedIds);

    final int perBlock = ValueDictionaryDirectoryNode.ENTRIES_PER_BLOCK;
    for (int block = 0; block < blockCount; block++) {
      final int from = block * perBlock;
      final int to = Math.min(from + perBlock, entryCount);
      namePage.putProjectionValueDictionaryRecord(
          new ValueDictionaryDirectoryNode(GlobalValueDictionary.directoryKey(header, block),
              Arrays.copyOfRange(sortedHashes, from, to), Arrays.copyOfRange(sortedIds, from, to)),
          databaseType, storageEngineWriter, log);
    }
  }

  /**
   * Least-significant-digit radix sort of {@code keys} into ascending UNSIGNED order, permuting
   * {@code values} alongside.
   *
   * <p>Radix rather than a comparison sort for two reasons beyond the linear time: it sorts
   * primitives without boxing a five-million-element index array, and it orders raw bytes, which is
   * unsigned order by construction — the same order {@code Long.compareUnsigned} gives the probe's
   * binary search, with no sign-bit trick to get wrong.
   */
  private static void radixSortByHash(final long[] keys, final int[] values) {
    final int n = keys.length;
    long[] srcKeys = keys;
    int[] srcValues = values;
    long[] dstKeys = new long[n];
    int[] dstValues = new int[n];
    final int[] histogram = new int[256];
    for (int shift = 0; shift < 64; shift += 8) {
      Arrays.fill(histogram, 0);
      for (int i = 0; i < n; i++) {
        histogram[(int) ((srcKeys[i] >>> shift) & 0xFF)]++;
      }
      // A digit the whole array shares changes nothing; skipping it saves a full pass, which on
      // 64-bit hashes of short strings is most of the high passes.
      if (histogram[(int) ((srcKeys[0] >>> shift) & 0xFF)] == n) {
        continue;
      }
      int sum = 0;
      for (int d = 0; d < 256; d++) {
        final int count = histogram[d];
        histogram[d] = sum;
        sum += count;
      }
      for (int i = 0; i < n; i++) {
        final int slot = histogram[(int) ((srcKeys[i] >>> shift) & 0xFF)]++;
        dstKeys[slot] = srcKeys[i];
        dstValues[slot] = srcValues[i];
      }
      final long[] swapKeys = srcKeys;
      srcKeys = dstKeys;
      dstKeys = swapKeys;
      final int[] swapValues = srcValues;
      srcValues = dstValues;
      dstValues = swapValues;
    }
    // An odd number of executed passes leaves the result in the scratch buffers.
    if (srcKeys != keys) {
      System.arraycopy(srcKeys, 0, keys, 0, n);
      System.arraycopy(srcValues, 0, values, 0, n);
    }
  }

  /** Drop the in-memory arena and index. The writer cannot be used afterwards. */
  public void release() {
    released = true;
    arena = null;
    offsets = null;
    lengths = null;
    hashes = null;
    tableHashes = null;
    tableIds = null;
  }
}
