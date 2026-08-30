/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.index.IndexType;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Remembers the leaves whose background pre-serialization mints overflow carriers that only the
 * recursive final commit can key, so the flush lane can decline the encode instead of paying it and
 * throwing the bytes away.
 *
 * <p>
 * Keyed by leaf IDENTITY — {@code (recordPageKey, indexType)} — and deliberately not by page object.
 * The insert thread supersedes a hot leaf while the flush runs, so the next epoch's snapshot holds a
 * DIFFERENT {@link io.sirix.page.KeyValueLeafPage} instance for the same leaf; a flag on the page is
 * therefore lost on every single retry (measured on a 1M load: 10,811 refusals, not one of them ever
 * seen again on the same object).
 * </p>
 *
 * <p>
 * <b>Structure.</b> A direct-mapped table of packed identities in one {@link AtomicLongArray}: no
 * allocation and no lock on the flush worker's path, one relaxed-ordering array read per snapshot
 * entry. It never resizes and never chains. A collision simply overwrites the older identity, and
 * losing a mark costs exactly one further encode before the next refusal re-marks the leaf — so the
 * slot count is a throughput knob, never a correctness one. Because a stored identity is compared in
 * full, the table has no false positives; a false negative only forfeits the optimisation.
 * </p>
 *
 * <p>
 * <b>Why a lost mark is safe in the other direction too.</b> Declining to pre-serialize a leaf
 * reaches the identical outcome the refusal reaches — the leaf is promoted into the live intent log
 * and written by the recursive final commit. Skipping therefore cannot change which pages the
 * committed revision contains; it removes shadow writes of versions that a later epoch supersedes.
 * </p>
 */
final class RefusedOverflowLeafTable {

  /**
   * Default slots. 64 Ki identities = 512 KiB per write transaction, which comfortably covers the
   * hot name-dictionary leaves that dominate this population at every scale measured.
   */
  static final int DEFAULT_SLOTS = 1 << 16;

  /** Keeps a packed identity non-zero so {@code 0} stays available as the empty sentinel. */
  private static final long PRESENT_TAG = 1L << 62;

  /** Fibonacci-hashing multiplier; spreads consecutive record-page keys across the table. */
  private static final long MIX = 0x9E3779B97F4A7C15L;

  /** Index-type ids are a byte and never exceed 15 today; the pack asserts that invariant. */
  private static final int MAX_INDEX_TYPE_ID = 0x0F;

  private final AtomicLongArray slots;

  private final int mask;

  /**
   * @param slotCount number of slots; must be a positive power of two
   * @throws IllegalArgumentException if {@code slotCount} is not a positive power of two
   */
  RefusedOverflowLeafTable(final int slotCount) {
    if (slotCount <= 0 || Integer.bitCount(slotCount) != 1) {
      throw new IllegalArgumentException("slotCount must be a positive power of two: " + slotCount);
    }
    this.slots = new AtomicLongArray(slotCount);
    this.mask = slotCount - 1;
  }

  /**
   * Pack a leaf identity into one non-zero long.
   *
   * @param recordPageKey the leaf's record-page key; non-negative and far below {@code 2^58}
   * @param indexTypeId {@link IndexType#getID()} of the leaf
   * @return the packed identity
   * @throws IllegalArgumentException if either component is outside its representable range
   */
  static long pack(final long recordPageKey, final int indexTypeId) {
    if (recordPageKey < 0L || recordPageKey > (1L << 57)) {
      throw new IllegalArgumentException("recordPageKey out of range: " + recordPageKey);
    }
    if (indexTypeId < 0 || indexTypeId > MAX_INDEX_TYPE_ID) {
      throw new IllegalArgumentException("indexTypeId out of range: " + indexTypeId);
    }
    return ((recordPageKey << 4) | indexTypeId) | PRESENT_TAG;
  }

  /** The direct-mapped slot a packed identity occupies. */
  private int slotOf(final long packed) {
    long mixed = packed * MIX;
    mixed ^= mixed >>> 32;
    return (int) (mixed & mask);
  }

  /**
   * Remember that this leaf's content pre-serializes into carriers the background flush cannot key.
   *
   * @param recordPageKey the leaf's record-page key
   * @param indexTypeId {@link IndexType#getID()} of the leaf
   */
  void note(final long recordPageKey, final int indexTypeId) {
    final long packed = pack(recordPageKey, indexTypeId);
    slots.setRelease(slotOf(packed), packed);
  }

  /**
   * Whether an earlier epoch already proved this leaf's encode unpublishable.
   *
   * @param recordPageKey the leaf's record-page key
   * @param indexTypeId {@link IndexType#getID()} of the leaf
   * @return {@code true} only when this exact identity is still resident
   */
  boolean contains(final long recordPageKey, final int indexTypeId) {
    final long packed = pack(recordPageKey, indexTypeId);
    return slots.getAcquire(slotOf(packed)) == packed;
  }

  /** The slot a given identity maps to; for the collision test, which needs two colliding keys. */
  int slotForTesting(final long recordPageKey, final int indexTypeId) {
    return slotOf(pack(recordPageKey, indexTypeId));
  }
}
