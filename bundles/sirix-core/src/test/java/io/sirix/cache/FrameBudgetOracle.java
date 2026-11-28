/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The accounting oracle of MEM-101, shared by the step-1 tests: the two per-class identities, the
 * process-wide sum and the limits, read from the allocator's own state.
 *
 * <p>
 * For every class <em>c</em>:
 * {@code minted(c) = owned(c) + recycled(c) + inTransfer(c) + returned(c)} and
 * {@code chargedFrames(c) = slotSize(c) * (owned + recycled + inTransfer)}. Process-wide,
 * {@code committedBytes = sum(chargedFrames) + non-frame charges}, and frame charges never exceed
 * the frame commit limit <em>L_f</em>.
 */
final class FrameBudgetOracle {

  private FrameBudgetOracle() {}

  /** <em>L_f</em>: {@code B - B/8 - oversizedHeadroom} (today's {@code frameCommitLimit}). */
  static long frameCommitLimit(final FrameSlotAllocator allocator) {
    return scratchCommitLimit(allocator) - allocator.oversizedHeadroomBytes();
  }

  /** The limit non-frame charges are admitted against: {@code B - B/8}. */
  static long scratchCommitLimit(final FrameSlotAllocator allocator) {
    return allocator.budgetBytes() - allocator.budgetBytes() / AggregationBudget.DIVISOR;
  }

  /** Bytes charged for page frames: owned, recycled and in-transfer slots of every class. */
  static long chargedFrameBytes(final FrameSlotAllocator allocator) {
    long charged = 0L;
    for (int classIdx = 0; classIdx < FrameSlotAllocator.SIZE_CLASSES.length; classIdx++) {
      final long chargedSlots = (long) allocator.liveSlotCount(classIdx) + allocator.recycledSlotCount(classIdx)
          + MemPoolSeam.inTransfer(allocator, classIdx);
      charged += FrameSlotAllocator.SIZE_CLASSES[classIdx] * chargedSlots;
    }
    return charged;
  }

  /** Bytes the allocator holds recycled (resident, charged, owned by nobody) in {@code classIdx}. */
  static long recycledBytes(final FrameSlotAllocator allocator, final int classIdx) {
    return FrameSlotAllocator.SIZE_CLASSES[classIdx] * (long) allocator.recycledSlotCount(classIdx);
  }

  /**
   * Assert both identities of every class, the process-wide sum and the limit.
   *
   * @param nonFrameChargedBytes what the test itself holds charged outside page frames (scratch,
   *        slabs, oversized, aggregation); {@code 0} when it holds none
   * @param when what the allocator just did, for the failure message
   */
  static void assertHolds(final FrameSlotAllocator allocator, final long nonFrameChargedBytes, final String when) {
    long chargedFrames = 0L;
    for (int classIdx = 0; classIdx < FrameSlotAllocator.SIZE_CLASSES.length; classIdx++) {
      final long slotSize = FrameSlotAllocator.SIZE_CLASSES[classIdx];
      final long owned = allocator.liveSlotCount(classIdx);
      final long recycled = allocator.recycledSlotCount(classIdx);
      final long inTransfer = MemPoolSeam.inTransfer(allocator, classIdx);
      final long returned = MemPoolSeam.returned(allocator, classIdx);
      final long minted = MemPoolSeam.minted(allocator, classIdx);
      assertEquals(owned + recycled + inTransfer + returned, minted,
          "MEM-101 minted = owned + recycled + inTransfer + returned, class " + classIdx + " after " + when);
      chargedFrames += slotSize * (owned + recycled + inTransfer);
    }
    assertEquals(chargedFrames + nonFrameChargedBytes, allocator.getPhysicalMemoryBytes(),
        "MEM-101 committedBytes = sum of chargedFrames + non-frame charges after " + when);
    assertTrue(chargedFrames <= frameCommitLimit(allocator),
        "MEM-101 frame charges " + chargedFrames + " exceed L_f " + frameCommitLimit(allocator) + " after " + when);
    assertTrue(allocator.nonExecutionCommittedBytes() <= scratchCommitLimit(allocator),
        "MEM-101 the non-execution charge exceeds the limit that admitted it after " + when);
  }
}
