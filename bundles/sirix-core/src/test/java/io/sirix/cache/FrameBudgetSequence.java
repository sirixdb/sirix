/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * One seeded sequence of allocate / release / transfer / re-charge / shutdown operations over all
 * seven size classes, with the MEM-101 oracle asserted after every operation. Shared by
 * {@code FrameBudgetAccountingTest} (fixed seeds) and {@code FrameBudgetAccountingProperty} (jqwik
 * generated sequences), so both run the same oracle.
 *
 * <p>
 * The budget is 8 MiB, so a handful of 256 KiB frames reach <em>L_f</em> and the refusal path (and
 * with it the transfer) runs often. An operation code selects the operation; the class and the
 * sizes come from the seeded generator.
 */
final class FrameBudgetSequence {

  /** Operation code: allocate a frame. */
  static final int ALLOCATE = 0;
  /** Operation code: release a random held frame. */
  static final int RELEASE = 1;
  /** Operation code: a non-frame reservation that may be refused and so trigger a transfer. */
  static final int RESERVE_NON_FRAME = 2;
  /** Operation code: refund a random held non-frame reservation. */
  static final int REFUND_NON_FRAME = 3;
  /**
   * Operation code: release every held frame of one class, then allocate a frame of another class.
   */
  static final int FLIP_CLASS = 4;
  /** The number of distinct operation codes. */
  static final int OPERATIONS = 5;

  static final long BUDGET = 8L * MemPoolFixture.MIB;

  private FrameBudgetSequence() {}

  /** Run {@code operations} drawn from {@code seed}, asserting the oracle after each. */
  static void runSeeded(final long seed, final int operations) {
    final SplittableRandom random = new SplittableRandom(seed);
    final int[] codes = new int[operations];
    for (int i = 0; i < operations; i++) {
      codes[i] = random.nextInt(OPERATIONS);
    }
    run(seed, codes, 4096L);
  }

  /**
   * Run the operation {@code codes}, class and size choices drawn from {@code seed}, on a fake
   * reporting {@code osPageBytes}.
   */
  static void run(final long seed, final int[] codes, final long osPageBytes) {
    final SplittableRandom random = new SplittableRandom(seed);
    final int classes = FrameSlotAllocator.SIZE_CLASSES.length;
    final FrameSlotAllocator allocator;
    try (MemPoolFixture fixture = MemPoolFixture.create(BUDGET, osPageBytes)) {
      allocator = fixture.allocator;
      final List<FrameSlotAllocator.FrameSlot> frames = new ArrayList<>();
      final List<Long> reservations = new ArrayList<>();
      fixture.assertAccounting("start");
      for (int step = 0; step < codes.length; step++) {
        final String what = "step " + step + " (op " + codes[step] + ", seed " + seed + ")";
        switch (codes[step]) {
          case ALLOCATE -> {
            final FrameSlotAllocator.FrameSlot frame = fixture.allocate(random.nextInt(classes));
            if (frame != null) {
              frames.add(frame);
            }
          }
          case RELEASE -> {
            if (!frames.isEmpty()) {
              fixture.release(frames.remove(random.nextInt(frames.size())));
            }
          }
          case RESERVE_NON_FRAME -> {
            final long bytes = (1 + random.nextInt(64)) * 16L * MemPoolFixture.KIB;
            if (fixture.reserveNonFrame(bytes)) {
              reservations.add(bytes);
            }
          }
          case REFUND_NON_FRAME -> {
            if (!reservations.isEmpty()) {
              fixture.refundNonFrame(reservations.remove(random.nextInt(reservations.size())));
            }
          }
          case FLIP_CLASS -> {
            final int from = random.nextInt(classes);
            final List<FrameSlotAllocator.FrameSlot> doomed = new ArrayList<>();
            for (final FrameSlotAllocator.FrameSlot frame : frames) {
              if (frame.classIndex() == from) {
                doomed.add(frame);
              }
            }
            frames.removeAll(doomed);
            fixture.releaseAll(doomed);
            final FrameSlotAllocator.FrameSlot other =
                fixture.allocate((from + 1 + random.nextInt(classes - 1)) % classes);
            if (other != null) {
              frames.add(other);
            }
          }
          default -> throw new IllegalArgumentException("unknown operation code " + codes[step]);
        }
        fixture.assertAccounting(what);
      }
      fixture.releaseAll(frames);
      for (final long bytes : reservations) {
        fixture.refundNonFrame(bytes);
      }
      fixture.assertAccounting("draining the sequence");
    }
    assertEquals(0L, allocator.getPhysicalMemoryBytes(), "MEM-101 committedBytes after shutdown() must be 0");
  }
}
