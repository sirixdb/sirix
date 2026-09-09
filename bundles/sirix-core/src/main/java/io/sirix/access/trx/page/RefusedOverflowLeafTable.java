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
 * Keyed by leaf IDENTITY — {@code (recordPageKey, indexType)} — and deliberately not by page
 * object. The insert thread supersedes a hot leaf while the flush runs, so the next epoch's
 * snapshot holds a DIFFERENT {@link io.sirix.page.KeyValueLeafPage} instance for the same leaf; a
 * flag on the page is therefore lost on every single retry (measured on a 1M load: 10,811 refusals,
 * not one of them ever seen again on the same object).
 * </p>
 *
 * <p>
 * <b>Every mark EXPIRES.</b> A mark also carries the flush epoch that set it, and
 * {@link #shouldSkip} stops honouring it after a caller-supplied number of epochs. The refusal is a
 * property of the leaf's records, and records are only added inside a flush window, never removed —
 * so a leaf that refused once is expected to refuse again. "Expected" is not "guaranteed", and the
 * cost of being wrong is that a leaf which HAS become flushable waits for the final commit instead,
 * holding intent-log residency until then. Expiry turns that from unbounded into "at most N
 * epochs", at a cost of one encode per leaf per N epochs. It is the difference between an
 * optimisation that is safe because of an argument and one that is safe because it cannot run away.
 * </p>
 *
 * <p>
 * <b>Structure.</b> A direct-mapped table of (packed identity, epoch) pairs interleaved in one
 * {@link AtomicLongArray}: no allocation and no lock on the flush worker's path, two adjacent array
 * reads per snapshot entry. It never resizes and never chains. A collision simply overwrites the
 * older identity, and losing a mark costs exactly one further encode before the next refusal
 * re-marks the leaf — so the slot count is a throughput knob, never a correctness one. Because a
 * stored identity is compared in full, the table has no false positives; a false negative only
 * forfeits the optimisation.
 * </p>
 *
 * <p>
 * <b>Both directions of error are safe, and one is impossible.</b> Answering {@code false} when the
 * leaf would have been refused costs one encode — exactly today's behaviour. Answering {@code true}
 * when the leaf could have been written defers it to the recursive final commit, which is the same
 * outcome the refusal it is standing in for produces, and expiry bounds how long that can last. The
 * unsafe answer — a leaf skipped and then never written — is not reachable from here: the caller
 * sets the promote-to-intent-log sentinel in the same statement that skips, so declining an encode
 * and keeping the leaf alive for the final commit are one action, not two.
 * </p>
 */
final class RefusedOverflowLeafTable {

  /**
   * Default identities. 64 Ki entries = 1 MiB per write transaction (identity + epoch), which
   * comfortably covers the hot name-dictionary leaves that dominate this population at every scale
   * measured.
   */
  static final int DEFAULT_SLOTS = 1 << 16;

  /** Keeps a packed identity non-zero so {@code 0} stays available as the empty sentinel. */
  private static final long PRESENT_TAG = 1L << 62;

  /** Fibonacci-hashing multiplier; spreads consecutive record-page keys across the table. */
  private static final long MIX = 0x9E3779B97F4A7C15L;

  /** Index-type ids are a byte and never exceed 15 today; the pack asserts that invariant. */
  private static final int MAX_INDEX_TYPE_ID = 0x0F;

  /** Identity at {@code 2i}, the epoch that stored it at {@code 2i + 1}. */
  private final AtomicLongArray slots;

  private final int mask;

  /**
   * @param slotCount number of identities the table can hold; must be a positive power of two
   * @throws IllegalArgumentException if {@code slotCount} is not a positive power of two
   */
  RefusedOverflowLeafTable(final int slotCount) {
    if (slotCount <= 0 || Integer.bitCount(slotCount) != 1) {
      throw new IllegalArgumentException("slotCount must be a positive power of two: " + slotCount);
    }
    this.slots = new AtomicLongArray(slotCount * 2);
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

  /** The direct-mapped identity index a packed identity occupies. */
  private int indexOf(final long packed) {
    long mixed = packed * MIX;
    mixed ^= mixed >>> 32;
    return ((int) (mixed & mask)) << 1;
  }

  /**
   * Remember that this leaf's content pre-serializes into carriers the background flush cannot key.
   *
   * @param recordPageKey the leaf's record-page key
   * @param indexTypeId {@link IndexType#getID()} of the leaf
   * @param epoch the flush epoch observing the refusal; a later {@link #shouldSkip} expires against
   *        it
   */
  void note(final long recordPageKey, final int indexTypeId, final long epoch) {
    final long packed = pack(recordPageKey, indexTypeId);
    final int index = indexOf(packed);
    // Epoch first: a reader that then sees the identity sees an epoch at least as new as the one
    // this mark carries. Seeing an older epoch would only expire the mark early — one extra encode.
    slots.setRelease(index + 1, epoch);
    slots.setRelease(index, packed);
  }

  /**
   * Whether an earlier epoch proved this leaf's encode unpublishable AND that proof is still young
   * enough to act on.
   *
   * @param recordPageKey the leaf's record-page key
   * @param indexTypeId {@link IndexType#getID()} of the leaf
   * @param epoch the current flush epoch
   * @param maxSkippedEpochs how many epochs a mark may be honoured for; {@code <= 0} honours none, so
   *        the caller's kill switch can be expressed as a bound of zero
   * @return {@code true} only when this exact identity is resident and its mark has not expired
   */
  boolean shouldSkip(final long recordPageKey, final int indexTypeId, final long epoch, final int maxSkippedEpochs) {
    if (maxSkippedEpochs <= 0) {
      return false;
    }
    final long packed = pack(recordPageKey, indexTypeId);
    final int index = indexOf(packed);
    if (slots.getAcquire(index) != packed) {
      return false;
    }
    final long age = epoch - slots.getAcquire(index + 1);
    // Signed, and a negative age is NOT honoured. It means the mark carries a later epoch than the
    // one asking — a worker reading a stale flushEpoch while another writes a newer one. Both
    // answers are safe, so take the conservative one: when the age is not plainly inside the bound,
    // encode. That also keeps the arithmetic free of any unsigned reading, under which a
    // just-written mark would look astronomically old.
    return age >= 0L && age < maxSkippedEpochs;
  }

  /**
   * The identity index a given leaf maps to; for the collision test, which needs two colliding keys.
   */
  int indexForTesting(final long recordPageKey, final int indexTypeId) {
    return indexOf(pack(recordPageKey, indexTypeId));
  }
}
