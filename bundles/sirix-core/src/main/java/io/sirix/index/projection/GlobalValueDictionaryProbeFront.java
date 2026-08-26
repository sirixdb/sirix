/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Arrays;

/**
 * Resident value→id map fronting one column's streaming global dictionary for the lifetime of a
 * bulk load.
 *
 * <p>
 * The streaming build rotates bounded {@link GlobalValueDictionaryWriter} generations (each capped
 * at {@link GlobalValueDictionaryWriter#MAX_DISTINCT_ENTRIES_PER_APPEND} entries by the radix
 * append's safe-array arithmetic) and releases each one after its flush. Correct for durability —
 * and catastrophic for interning without this class: once a generation was released, every value
 * seen before the current epoch had to be re-resolved through the PERSISTENT radix, ~5 record reads
 * per probe, each a full page decode+checksum through the writer's uncached durable read path.
 * Measured at ~85% of total load CPU on 150k and 1M ClickBench ingests. This map keeps every
 * (value, id) pair interned since the load began resident, so a probe is one open-addressing hit
 * and the persistent radix is never consulted for a value this load has already seen.
 *
 * <p>
 * Deliberately NOT a {@code GlobalValueDictionaryWriter}: the writer enforces the per-append entry
 * ceiling at intern time (it must stay flushable in one radix append), while this map must hold the
 * whole load's distinct set — many generations' worth. It holds ids, hashes and value bytes only;
 * nothing here is ever flushed, so none of the append-planning arithmetic applies. Heap cost is
 * metered against the same per-column byte budget the election admitted the column under, and
 * crossing it raises the same typed {@link GlobalDictionaryBudgetExceededException} the abandon
 * path already understands.
 *
 * <p>
 * Single-threaded by contract, like the builder that owns it. Ids are positive and dense per
 * column, so id-indexed arrays address the confirm/locate side and {@code 0} doubles as the empty
 * slot marker in the table.
 */
final class GlobalValueDictionaryProbeFront {

  /** Slots, power of two; grown at 50% load so probes stay short. */
  private static final int INITIAL_TABLE_CAPACITY = 1 << 14;

  private static final int INITIAL_ID_CAPACITY = 1 << 14;

  private static final int INITIAL_ARENA_BYTES = 1 << 20;

  private final int column;
  private final long budgetBytes;

  /** Primary hash per slot; meaningful only where {@link #tableIds} is non-zero. */
  private long[] tableHashes = new long[INITIAL_TABLE_CAPACITY];

  /** Id per slot; {@code 0} marks an empty slot (ids start at 1). */
  private int[] tableIds = new int[INITIAL_TABLE_CAPACITY];

  /** Secondary hash by id, the cheap confirm before the byte compare. */
  private long[] secondaryHashes = new long[INITIAL_ID_CAPACITY];

  /** Arena offset by id. */
  private int[] valueOffsets = new int[INITIAL_ID_CAPACITY];

  /** Value length by id. */
  private int[] valueLengths = new int[INITIAL_ID_CAPACITY];

  /** Value bytes, appended in put order; doubled on demand under the budget. */
  private byte[] arena = new byte[INITIAL_ARENA_BYTES];

  private int arenaUsed;
  private int entryCount;
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
  }

  /**
   * The id this front has recorded for the value, or {@code 0} when the value has not been seen by
   * this load. Never touches storage.
   */
  int findId(final long hash, final long secondaryHash, final byte[] source, final int offset, final int length) {
    assertLive();
    final long[] hashes = tableHashes;
    final int[] ids = tableIds;
    final int mask = hashes.length - 1;
    int slot = slotOf(hash, mask);
    while (true) {
      final int id = ids[slot];
      if (id == 0) {
        return 0;
      }
      if (hashes[slot] == hash && secondaryHashes[id] == secondaryHash && valueLengths[id] == length
          && Arrays.equals(arena, valueOffsets[id], valueOffsets[id] + length, source, offset, offset + length)) {
        return id;
      }
      slot = (slot + 1) & mask;
    }
  }

  /**
   * Record the id minted (or resolved from a durable generation) for a value {@link #findId} just
   * missed. The caller owns the miss-then-put discipline; a duplicate put would shadow the first
   * entry harmlessly but waste arena bytes, so it is refused loudly instead.
   */
  void put(final long hash, final long secondaryHash, final byte[] source, final int offset, final int length,
      final int id) {
    assertLive();
    if (id <= 0) {
      throw new IllegalArgumentException("dictionary ids are positive: " + id);
    }
    ensureBudgetFor(length);
    if (entryCount * 2 >= tableIds.length) {
      growTable();
    }
    if (id >= secondaryHashes.length) {
      final int idCapacity = Integer.highestOneBit(id) << 1;
      secondaryHashes = Arrays.copyOf(secondaryHashes, idCapacity);
      valueOffsets = Arrays.copyOf(valueOffsets, idCapacity);
      valueLengths = Arrays.copyOf(valueLengths, idCapacity);
    }
    if (arenaUsed + length > arena.length) {
      int grown = Math.max(arena.length << 1, arenaUsed + length);
      arena = Arrays.copyOf(arena, grown);
    }
    System.arraycopy(source, offset, arena, arenaUsed, length);
    secondaryHashes[id] = secondaryHash;
    valueOffsets[id] = arenaUsed;
    valueLengths[id] = length;
    arenaUsed += length;

    final long[] hashes = tableHashes;
    final int[] ids = tableIds;
    final int mask = hashes.length - 1;
    int slot = slotOf(hash, mask);
    while (ids[slot] != 0) {
      if (hashes[slot] == hash && ids[slot] == id) {
        throw new IllegalStateException(
            "value dictionary probe front already holds id " + id + " for column " + column);
      }
      slot = (slot + 1) & mask;
    }
    hashes[slot] = hash;
    ids[slot] = id;
    entryCount++;
  }

  int entryCount() {
    return entryCount;
  }

  long retainedBytes() {
    return (long) arena.length + (long) tableHashes.length * Long.BYTES + (long) tableIds.length * Integer.BYTES
        + (long) secondaryHashes.length * Long.BYTES + (long) valueOffsets.length * Integer.BYTES
        + (long) valueLengths.length * Integer.BYTES;
  }

  /** Drop every backing array; the front is unusable afterwards. Idempotent. */
  void release() {
    released = true;
    tableHashes = null;
    tableIds = null;
    secondaryHashes = null;
    valueOffsets = null;
    valueLengths = null;
    arena = null;
  }

  private void assertLive() {
    if (released) {
      throw new IllegalStateException("value dictionary probe front for column " + column + " is released");
    }
  }

  /**
   * The growth this put may trigger, checked against the budget BEFORE any allocation so a refusal
   * leaves the front exactly as it was. The projected figure assumes every growable structure
   * doubles, which over-reserves slightly at the boundary — a refusal one entry early is a far better
   * failure than an allocation the budget was supposed to prevent.
   */
  private void ensureBudgetFor(final int length) {
    long projected = retainedBytes();
    if (arenaUsed + length > arena.length) {
      projected += Math.max(arena.length, length);
    }
    if (entryCount * 2 >= tableIds.length) {
      projected += (long) tableHashes.length * Long.BYTES + (long) tableIds.length * Integer.BYTES;
    }
    if (projected > budgetBytes) {
      throw new GlobalDictionaryBudgetExceededException(column, projected, budgetBytes, entryCount,
          "resident probe front for the streaming load cannot grow further; the column's whole-load distinct set "
              + "no longer fits the per-column budget");
    }
  }

  private void growTable() {
    final long[] oldHashes = tableHashes;
    final int[] oldIds = tableIds;
    final int newCapacity = oldHashes.length << 1;
    final long[] newHashes = new long[newCapacity];
    final int[] newIds = new int[newCapacity];
    final int mask = newCapacity - 1;
    for (int i = 0; i < oldIds.length; i++) {
      final int id = oldIds[i];
      if (id == 0) {
        continue;
      }
      int slot = slotOf(oldHashes[i], mask);
      while (newIds[slot] != 0) {
        slot = (slot + 1) & mask;
      }
      newHashes[slot] = oldHashes[i];
      newIds[slot] = id;
    }
    tableHashes = newHashes;
    tableIds = newIds;
  }

  private static int slotOf(final long hash, final int mask) {
    // Spread the low bits with the high ones; valueHash is already a 64-bit mix, this just folds it.
    return (int) (hash ^ (hash >>> 32)) & mask;
  }
}
