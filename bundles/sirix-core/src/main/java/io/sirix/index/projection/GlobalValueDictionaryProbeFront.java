/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Arrays;
import java.util.Objects;

/**
 * Resident value-to-id map fronting one column's streaming global dictionary for the lifetime of a
 * bulk load.
 *
 * <p>
 * The durable dictionary is generation-segmented in the keyed trie. This front prevents a repeated
 * value from probing that trie once per row: it keeps the exact UTF-8 value, both hashes and the id
 * for every value minted by this load. A miss is therefore proof that the value is new while the
 * front covers every durable generation, and the streaming builder can append the new value without
 * a persistent read.
 *
 * <h2>Why every backing store is chunked</h2>
 *
 * <p>
 * The former implementation doubled one open-addressing table, three id arrays and one byte arena.
 * At full ClickBench scale those arrays survived enough young collections to enter old gen; the
 * next resize made the old copies garbage and forced a concurrent old-generation cycle solely to
 * reclaim them. A byte budget limited the final live set but could not make a whole-array copy
 * HFT-safe.
 *
 * <p>
 * This implementation derives one fixed logical hash-table capacity from the same per-column budget
 * and allocates its storage in at-most-128 KiB chunks. Id metadata and value bytes are chunked too.
 * No insertion rehashes an existing entry, copies a previous arena or allocates an array with more
 * than a 256 KiB payload. Chunks are installed lazily, and the budget check reserves every chunk an
 * insertion needs before mutating the map.
 *
 * <p>
 * Single-threaded by contract, like the projection builder that owns it. Ids are positive and dense
 * per column, so {@code 0} remains the empty-slot marker.
 */
final class GlobalValueDictionaryProbeFront {

  private static final int MAX_SAFE_ARRAY_PAYLOAD_BYTES = 256 << 10;

  /** 16,384 slots: 128 KiB of hashes plus 64 KiB of ids per allocated table chunk. */
  private static final int TABLE_CHUNK_SHIFT = 14;
  private static final int TABLE_CHUNK_SLOTS = 1 << TABLE_CHUNK_SHIFT;
  private static final int TABLE_CHUNK_MASK = TABLE_CHUNK_SLOTS - 1;

  /** 8,192 ids: two 64 KiB long arrays and one 32 KiB int array. */
  private static final int ID_CHUNK_SHIFT = 13;
  private static final int ID_CHUNK_ENTRIES = 1 << ID_CHUNK_SHIFT;
  private static final int ID_CHUNK_MASK = ID_CHUNK_ENTRIES - 1;

  /** Exact value bytes use the same non-humongous chunk size as the generation writer. */
  private static final int ARENA_CHUNK_SHIFT = 16;
  private static final int ARENA_CHUNK_BYTES = 1 << ARENA_CHUNK_SHIFT;
  private static final int ARENA_CHUNK_MASK = ARENA_CHUNK_BYTES - 1;

  private static final int INITIAL_TABLE_CAPACITY = 1 << 14;

  /** Keeps every outer chunk directory at or below a 256 KiB reference payload. */
  private static final int MAX_TABLE_CAPACITY = 1 << 26;
  private static final int MAX_ARENA_CHUNKS = MAX_SAFE_ARRAY_PAYLOAD_BYTES / Long.BYTES;

  /**
   * At 50% load one logical table slot costs 12 bytes, and its half-share of dense id metadata costs
   * another 10 bytes: {@code 8 + 4 + (8 + 8 + 4) / 2 == 22}. Value bytes spend the remaining budget.
   */
  private static final int BYTES_PER_LOGICAL_SLOT = 22;

  /**
   * Test seam: a lower fixed-table ceiling than the budget-derived one, so a streaming load can be
   * driven into the mid-feed refusal deterministically without a corpus of a million distinct values.
   * {@link Integer#MAX_VALUE} (the default) leaves the real ceiling in force. Only
   * {@code CoordinatorFeedBudgetAbandonTest} sets it.
   */
  static volatile int TEST_MAX_ENTRIES = Integer.MAX_VALUE;

  private final int column;
  private final long budgetBytes;
  private final int tableCapacity;
  private final int maxEntries;

  /** Primary hashes and ids by logical table slot; paired chunks are allocated together. */
  private long[][] tableHashes;
  private int[][] tableIds;

  /** Exact-confirm metadata by dictionary id. */
  private long[][] secondaryHashes;
  private long[][] valueOffsets;
  private int[][] valueLengths;

  /** Concatenated UTF-8 value bytes, addressed by the logical offsets above. */
  private byte[][] arenaChunks;

  private long arenaUsed;
  private int entryCount;
  private long retainedBytes;
  private boolean released;

  GlobalValueDictionaryProbeFront(final int column, final long budgetBytes) {
    if (column < 0) {
      throw new IllegalArgumentException("column must not be negative: " + column);
    }
    if (budgetBytes <= 0) {
      throw new IllegalArgumentException("budgetBytes must be positive: " + budgetBytes);
    }
    this.column = column;
    this.budgetBytes = budgetBytes;
    tableCapacity = tableCapacityForBudget(budgetBytes);
    maxEntries = tableCapacity >>> 1;

    final int tableChunks = tableCapacity >>> TABLE_CHUNK_SHIFT;
    final int idChunks = chunksFor((long) maxEntries + 1L, ID_CHUNK_ENTRIES);
    final int arenaDirectoryLength = arenaDirectoryLength(budgetBytes);
    tableHashes = new long[tableChunks][];
    tableIds = new int[tableChunks][];
    secondaryHashes = new long[idChunks][];
    valueOffsets = new long[idChunks][];
    valueLengths = new int[idChunks][];
    arenaChunks = new byte[arenaDirectoryLength][];
    retainedBytes = directoryPayloadBytes();
    if (budgetBytes != Long.MAX_VALUE && retainedBytes > budgetBytes) {
      throw new IllegalArgumentException(
          "budgetBytes " + budgetBytes + " cannot hold the probe-front chunk directories (" + retainedBytes + " B)");
    }
  }

  /**
   * The id this front has recorded for the exact value, or {@code 0} when the value has not been seen
   * by this load. Never touches storage and never allocates.
   */
  int findId(final long hash, final long secondaryHash, final byte[] source, final int offset, final int length) {
    assertLive();
    checkValueRange(source, offset, length);
    final int mask = tableCapacity - 1;
    int slot = slotOf(hash, mask);
    while (true) {
      final int id = tableId(slot);
      if (id == 0) {
        return 0;
      }
      if (tableHash(slot) == hash && secondaryHash(id) == secondaryHash && valueLength(id) == length
          && regionMatches(valueOffset(id), source, offset, length)) {
        return id;
      }
      slot = (slot + 1) & mask;
    }
  }

  /**
   * Record the id minted for a value {@link #findId} just missed. All required chunks are reserved
   * against the byte budget before any field or byte is mutated.
   */
  void put(final long hash, final long secondaryHash, final byte[] source, final int offset, final int length,
      final int id) {
    assertLive();
    checkValueRange(source, offset, length);
    if (id <= 0) {
      throw new IllegalArgumentException("dictionary ids are positive: " + id);
    }
    if (entryCount >= Math.min(maxEntries, TEST_MAX_ENTRIES) || id > maxEntries) {
      refuseTableCapacity();
    }

    final int mask = tableCapacity - 1;
    int slot = slotOf(hash, mask);
    while (true) {
      final int residentId = tableId(slot);
      if (residentId == 0) {
        break;
      }
      if (tableHash(slot) == hash && secondaryHash(residentId) == secondaryHash && valueLength(residentId) == length
          && regionMatches(valueOffset(residentId), source, offset, length)) {
        throw new IllegalStateException("value dictionary probe front already holds value as id " + residentId
            + " for column " + column + "; duplicate put attempted with id " + id);
      }
      slot = (slot + 1) & mask;
    }

    final long valueOffset = arenaUsed;
    final long requiredArenaBytes = Math.addExact(valueOffset, length);
    final long projectedRetainedBytes = prepareInsertionStorage(slot, id, requiredArenaBytes);
    copyIntoArena(valueOffset, source, offset, length);
    publishInsertion(slot, hash, secondaryHash, valueOffset, length, id, requiredArenaBytes, projectedRetainedBytes);
  }

  /**
   * Copy one value directly from a generation writer's chunked arena into this front's owned arena.
   * The caller guarantees the source writer's ids are distinct and invokes this exactly once per id;
   * the front still owns an independent byte copy after the call. No contiguous per-value buffer is
   * materialised.
   */
  void putDistinctFromWriter(final long hash, final long secondaryHash, final GlobalValueDictionaryWriter source,
      final int sourceId, final int id) {
    assertLive();
    Objects.requireNonNull(source, "source must not be null");
    if (id <= 0) {
      throw new IllegalArgumentException("dictionary ids are positive: " + id);
    }
    if (id != entryCount + 1) {
      throw new IllegalArgumentException(
          "direct writer seeding requires dense ordered ids: expected " + (entryCount + 1) + ", got " + id);
    }
    if (entryCount >= Math.min(maxEntries, TEST_MAX_ENTRIES) || id > maxEntries) {
      refuseTableCapacity();
    }
    final int length = source.valueLengthAt(sourceId);
    final int mask = tableCapacity - 1;
    int slot = slotOf(hash, mask);
    while (tableId(slot) != 0) {
      slot = (slot + 1) & mask;
    }

    final long valueOffset = arenaUsed;
    final long requiredArenaBytes = Math.addExact(valueOffset, length);
    final long projectedRetainedBytes = prepareInsertionStorage(slot, id, requiredArenaBytes);
    copyIntoArena(valueOffset, source, sourceId, length);
    publishInsertion(slot, hash, secondaryHash, valueOffset, length, id, requiredArenaBytes, projectedRetainedBytes);
  }

  private long prepareInsertionStorage(final int slot, final int id, final long requiredArenaBytes) {
    final int requiredArenaChunks = chunksFor(requiredArenaBytes, ARENA_CHUNK_BYTES);
    if (requiredArenaChunks > arenaChunks.length) {
      refuseArenaCapacity(requiredArenaBytes);
    }

    final int tableChunk = slot >>> TABLE_CHUNK_SHIFT;
    final int idChunk = id >>> ID_CHUNK_SHIFT;
    long allocation = 0L;
    if (tableIds[tableChunk] == null) {
      allocation += (long) TABLE_CHUNK_SLOTS * (Long.BYTES + Integer.BYTES);
    }
    if (secondaryHashes[idChunk] == null) {
      allocation += (long) ID_CHUNK_ENTRIES * (2L * Long.BYTES + Integer.BYTES);
    }
    final int allocatedArenaChunks = chunksFor(arenaUsed, ARENA_CHUNK_BYTES);
    allocation += (long) (requiredArenaChunks - allocatedArenaChunks) * ARENA_CHUNK_BYTES;
    final long projected = saturatedAdd(retainedBytes, allocation);
    if (budgetBytes != Long.MAX_VALUE && projected > budgetBytes) {
      refuseBudget(projected, "projected-resident");
    }

    if (tableIds[tableChunk] == null) {
      tableHashes[tableChunk] = new long[TABLE_CHUNK_SLOTS];
      tableIds[tableChunk] = new int[TABLE_CHUNK_SLOTS];
    }
    if (secondaryHashes[idChunk] == null) {
      secondaryHashes[idChunk] = new long[ID_CHUNK_ENTRIES];
      valueOffsets[idChunk] = new long[ID_CHUNK_ENTRIES];
      valueLengths[idChunk] = new int[ID_CHUNK_ENTRIES];
    }
    for (int chunk = allocatedArenaChunks; chunk < requiredArenaChunks; chunk++) {
      arenaChunks[chunk] = new byte[ARENA_CHUNK_BYTES];
    }
    return projected;
  }

  private void publishInsertion(final int slot, final long hash, final long secondaryHash, final long valueOffset,
      final int length, final int id, final long requiredArenaBytes, final long projectedRetainedBytes) {
    setIdMetadata(id, secondaryHash, valueOffset, length);
    final int tableChunk = slot >>> TABLE_CHUNK_SHIFT;
    final int tableOffset = slot & TABLE_CHUNK_MASK;
    tableHashes[tableChunk][tableOffset] = hash;
    tableIds[tableChunk][tableOffset] = id;
    arenaUsed = requiredArenaBytes;
    entryCount++;
    retainedBytes = projectedRetainedBytes;
  }

  int entryCount() {
    return entryCount;
  }

  long retainedBytes() {
    return retainedBytes;
  }

  long budgetBytesForTest() {
    return budgetBytes;
  }

  /** Drop every backing array; the front is unusable afterwards. Idempotent. */
  void release() {
    released = true;
    tableHashes = null;
    tableIds = null;
    secondaryHashes = null;
    valueOffsets = null;
    valueLengths = null;
    arenaChunks = null;
    arenaUsed = 0L;
    entryCount = 0;
    retainedBytes = 0L;
  }

  int tableCapacityForTest() {
    return tableCapacity;
  }

  int largestBackingArrayPayloadBytesForTest() {
    int largest = ARENA_CHUNK_BYTES;
    largest = Math.max(largest, TABLE_CHUNK_SLOTS * Long.BYTES);
    largest = Math.max(largest, TABLE_CHUNK_SLOTS * Integer.BYTES);
    largest = Math.max(largest, ID_CHUNK_ENTRIES * Long.BYTES);
    largest = Math.max(largest, ID_CHUNK_ENTRIES * Integer.BYTES);
    largest = Math.max(largest, tableHashes.length * Long.BYTES);
    largest = Math.max(largest, tableIds.length * Long.BYTES);
    largest = Math.max(largest, secondaryHashes.length * Long.BYTES);
    largest = Math.max(largest, valueOffsets.length * Long.BYTES);
    largest = Math.max(largest, valueLengths.length * Long.BYTES);
    return Math.max(largest, arenaChunks.length * Long.BYTES);
  }

  private void assertLive() {
    if (released) {
      throw new IllegalStateException("value dictionary probe front for column " + column + " is released");
    }
  }

  private static void checkValueRange(final byte[] source, final int offset, final int length) {
    if (source == null) {
      throw new NullPointerException("source must not be null");
    }
    Objects.checkFromIndexSize(offset, length, source.length);
  }

  private int tableId(final int slot) {
    final int[] chunk = tableIds[slot >>> TABLE_CHUNK_SHIFT];
    return chunk == null
        ? 0
        : chunk[slot & TABLE_CHUNK_MASK];
  }

  private long tableHash(final int slot) {
    return tableHashes[slot >>> TABLE_CHUNK_SHIFT][slot & TABLE_CHUNK_MASK];
  }

  private long secondaryHash(final int id) {
    return secondaryHashes[id >>> ID_CHUNK_SHIFT][id & ID_CHUNK_MASK];
  }

  private long valueOffset(final int id) {
    return valueOffsets[id >>> ID_CHUNK_SHIFT][id & ID_CHUNK_MASK];
  }

  private int valueLength(final int id) {
    return valueLengths[id >>> ID_CHUNK_SHIFT][id & ID_CHUNK_MASK];
  }

  private void setIdMetadata(final int id, final long secondaryHash, final long valueOffset, final int valueLength) {
    final int chunk = id >>> ID_CHUNK_SHIFT;
    final int offset = id & ID_CHUNK_MASK;
    secondaryHashes[chunk][offset] = secondaryHash;
    valueOffsets[chunk][offset] = valueOffset;
    valueLengths[chunk][offset] = valueLength;
  }

  private boolean regionMatches(long arenaOffset, final byte[] source, int sourceOffset, int remaining) {
    while (remaining > 0) {
      final int chunkIndex = Math.toIntExact(arenaOffset >>> ARENA_CHUNK_SHIFT);
      final int chunkOffset = (int) arenaOffset & ARENA_CHUNK_MASK;
      final int compared = Math.min(remaining, ARENA_CHUNK_BYTES - chunkOffset);
      if (!Arrays.equals(arenaChunks[chunkIndex], chunkOffset, chunkOffset + compared, source, sourceOffset,
          sourceOffset + compared)) {
        return false;
      }
      arenaOffset += compared;
      sourceOffset += compared;
      remaining -= compared;
    }
    return true;
  }

  private void copyIntoArena(long arenaOffset, final byte[] source, int sourceOffset, int remaining) {
    while (remaining > 0) {
      final int chunkIndex = Math.toIntExact(arenaOffset >>> ARENA_CHUNK_SHIFT);
      final int chunkOffset = (int) arenaOffset & ARENA_CHUNK_MASK;
      final int copied = Math.min(remaining, ARENA_CHUNK_BYTES - chunkOffset);
      System.arraycopy(source, sourceOffset, arenaChunks[chunkIndex], chunkOffset, copied);
      arenaOffset += copied;
      sourceOffset += copied;
      remaining -= copied;
    }
  }

  /** Direct chunk-to-chunk copy used only while wrapping the initial generation. */
  private void copyIntoArena(long arenaOffset, final GlobalValueDictionaryWriter source, final int sourceId,
      int remaining) {
    int sourceValueOffset = 0;
    while (remaining > 0) {
      final int chunkIndex = Math.toIntExact(arenaOffset >>> ARENA_CHUNK_SHIFT);
      final int chunkOffset = (int) arenaOffset & ARENA_CHUNK_MASK;
      final int copied = Math.min(remaining, ARENA_CHUNK_BYTES - chunkOffset);
      source.copyValueRegion(sourceId, sourceValueOffset, arenaChunks[chunkIndex], chunkOffset, copied);
      arenaOffset += copied;
      sourceValueOffset += copied;
      remaining -= copied;
    }
  }

  private long directoryPayloadBytes() {
    return (long) (tableHashes.length + tableIds.length + secondaryHashes.length + valueOffsets.length
        + valueLengths.length + arenaChunks.length) * Long.BYTES;
  }

  private void refuseTableCapacity() {
    if (budgetBytes == Long.MAX_VALUE) {
      throw GlobalDictionaryBudgetExceededException.structuralDecline(column, retainedBytes, budgetBytes, entryCount,
          "resident probe front reached its safe fixed-table capacity of " + maxEntries + " entries");
    }
    final long doubledTableReservation = (long) tableCapacity * (Long.BYTES + Integer.BYTES);
    refuseBudget(Math.max(saturatedAdd(retainedBytes, doubledTableReservation), budgetBytes + 1L),
        "fixed-table-capacity");
  }

  private void refuseArenaCapacity(final long requiredArenaBytes) {
    throw GlobalDictionaryBudgetExceededException.structuralDecline(column, retainedBytes, budgetBytes, entryCount,
        "resident probe front needs " + requiredArenaBytes + " value bytes, above its safe chunk-directory capacity of "
            + ((long) arenaChunks.length * ARENA_CHUNK_BYTES) + " bytes");
  }

  private void refuseBudget(final long projected, final String term) {
    throw GlobalDictionaryBudgetExceededException.budgetBreach(column, retainedBytes, projected, term, budgetBytes,
        entryCount,
        "resident probe front for the streaming load cannot grow further; the column's whole-load distinct set "
            + "no longer fits the per-column budget");
  }

  private static int tableCapacityForBudget(final long budgetBytes) {
    final long affordable = budgetBytes == Long.MAX_VALUE
        ? MAX_TABLE_CAPACITY
        : Math.max(INITIAL_TABLE_CAPACITY, budgetBytes / BYTES_PER_LOGICAL_SLOT);
    final int bounded = (int) Math.min(MAX_TABLE_CAPACITY, affordable);
    return Integer.highestOneBit(bounded);
  }

  private static int arenaDirectoryLength(final long budgetBytes) {
    if (budgetBytes == Long.MAX_VALUE) {
      return MAX_ARENA_CHUNKS;
    }
    // Ceiling division expressed without adding chunkSize-1: a caller may deliberately configure a
    // near-Long.MAX_VALUE budget, and the usual addition would overflow before the directory cap is
    // applied.
    final long chunks = 1L + (budgetBytes - 1L) / ARENA_CHUNK_BYTES;
    return (int) Math.min(MAX_ARENA_CHUNKS, chunks);
  }

  private static int chunksFor(final long units, final int chunkUnits) {
    if (units < 0) {
      throw new IllegalArgumentException("units must not be negative: " + units);
    }
    if (units == 0) {
      return 0;
    }
    return Math.toIntExact(1L + (units - 1L) / chunkUnits);
  }

  private static long saturatedAdd(final long left, final long right) {
    return left > Long.MAX_VALUE - right
        ? Long.MAX_VALUE
        : left + right;
  }

  private static int slotOf(final long hash, final int mask) {
    return (int) (hash ^ (hash >>> 32)) & mask;
  }
}
