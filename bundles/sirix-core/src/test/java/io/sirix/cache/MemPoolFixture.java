/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.util.ArrayList;
import java.util.List;

/**
 * An allocator under test with its recording {@link VirtualMemory}, plus the bookkeeping the step-1
 * tests share: the frames the test holds and the non-frame bytes it has charged.
 *
 * <p>
 * Budgets below 64 MiB reserve no oversized headroom ({@code FrameSlotAllocator.initInternal}), so
 * <em>L_f</em> equals the non-frame limit: {@code B - B/8}. {@link #DEFAULT_BUDGET} is 32 MiB,
 * whose <em>L_f</em> of 28 MiB the 64 KiB class fills exactly with 448 slots.
 */
final class MemPoolFixture implements AutoCloseable {

  static final long KIB = 1024L;
  static final long MIB = 1024L * KIB;

  /** 32 MiB: <em>L_f</em> is 28 MiB and the headroom is zero. */
  static final long DEFAULT_BUDGET = 32L * MIB;

  final RecordingVirtualMemory vm;
  final FrameSlotAllocator allocator;
  private final List<FrameSlotAllocator.FrameSlot> held = new ArrayList<>();
  private long nonFrameCharged;

  private MemPoolFixture(final RecordingVirtualMemory vm, final FrameSlotAllocator allocator) {
    this.vm = vm;
    this.allocator = allocator;
  }

  /**
   * A fixture over {@code budgetBytes} whose fake reports {@code osPageBytes} as the OS page size.
   */
  static MemPoolFixture create(final long budgetBytes, final long osPageBytes) {
    final RecordingVirtualMemory vm = RecordingVirtualMemory.withOsPageBytes(osPageBytes);
    return new MemPoolFixture(vm, MemPoolSeam.newAllocator(budgetBytes, vm));
  }

  /** A fixture over a fake the test configured first (a bounded-capacity class, for example). */
  static MemPoolFixture create(final long budgetBytes, final RecordingVirtualMemory vm) {
    return new MemPoolFixture(vm, MemPoolSeam.newAllocator(budgetBytes, vm));
  }

  static MemPoolFixture create(final long osPageBytes) {
    return create(DEFAULT_BUDGET, osPageBytes);
  }

  static long slotSize(final int classIdx) {
    return FrameSlotAllocator.SIZE_CLASSES[classIdx];
  }

  /** Allocate one frame of {@code classIdx}; {@code null} when the allocator refuses. */
  FrameSlotAllocator.FrameSlot allocate(final int classIdx) {
    final FrameSlotAllocator.FrameSlot slot = allocator.allocateSlot(slotSize(classIdx));
    if (slot != null) {
      held.add(slot);
    }
    return slot;
  }

  /** Allocate frames of {@code classIdx} until the allocator refuses; the frames, in slot order. */
  List<FrameSlotAllocator.FrameSlot> fill(final int classIdx) {
    final List<FrameSlotAllocator.FrameSlot> filled = new ArrayList<>();
    FrameSlotAllocator.FrameSlot slot;
    while ((slot = allocate(classIdx)) != null) {
      filled.add(slot);
    }
    return filled;
  }

  /** Allocate exactly {@code count} frames of {@code classIdx}. */
  List<FrameSlotAllocator.FrameSlot> allocate(final int classIdx, final int count) {
    final List<FrameSlotAllocator.FrameSlot> frames = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final FrameSlotAllocator.FrameSlot slot = allocate(classIdx);
      if (slot == null) {
        throw new AssertionError("setup: the allocator refused frame " + i + " of " + count + " in class " + classIdx);
      }
      frames.add(slot);
    }
    return frames;
  }

  /** Release a frame this fixture handed out. */
  void release(final FrameSlotAllocator.FrameSlot slot) {
    held.remove(slot);
    slot.close();
  }

  void releaseAll(final List<FrameSlotAllocator.FrameSlot> frames) {
    for (final FrameSlotAllocator.FrameSlot slot : frames) {
      release(slot);
    }
  }

  /** Charge {@code bytes} outside page frames (a non-mandatory scratch reservation). */
  boolean reserveNonFrame(final long bytes) {
    final boolean reserved = allocator.reserveScratchCommittedBytes(bytes);
    if (reserved) {
      nonFrameCharged += bytes;
    }
    return reserved;
  }

  /**
   * Charge {@code bytes} outside page frames as a mandatory scratch reservation (pressure, wait,
   * OOM).
   */
  boolean reserveNonFrameMandatory(final long bytes) {
    final boolean reserved = allocator.reserveScratchCommittedBytes(bytes, true);
    if (reserved) {
      nonFrameCharged += bytes;
    }
    return reserved;
  }

  /**
   * As {@link #reserveNonFrameMandatory}, but an {@link OutOfMemoryError} becomes an
   * {@link AssertionError}: JUnit treats an escaping {@code OutOfMemoryError} as unrecoverable and
   * aborts the whole run, which would hide every other test of a failing step.
   */
  boolean reserveNonFrameMandatoryOrFail(final long bytes) {
    try {
      return reserveNonFrameMandatory(bytes);
    } catch (final OutOfMemoryError exhausted) {
      throw new AssertionError(
          "a mandatory reservation of " + bytes + " bytes ended in OutOfMemoryError: " + exhausted.getMessage(),
          exhausted);
    }
  }

  /** Give back non-frame bytes this fixture charged. */
  void refundNonFrame(final long bytes) {
    allocator.refundCommittedBytes(bytes);
    nonFrameCharged -= bytes;
  }

  /** The non-frame bytes this fixture holds charged. */
  long nonFrameCharged() {
    return nonFrameCharged;
  }

  /** Assert the MEM-101 identities with this fixture's own non-frame charges. */
  void assertAccounting(final String when) {
    FrameBudgetOracle.assertHolds(allocator, nonFrameCharged, when);
  }

  long frameLimit() {
    return FrameBudgetOracle.frameCommitLimit(allocator);
  }

  @Override
  public void close() {
    for (final FrameSlotAllocator.FrameSlot slot : new ArrayList<>(held)) {
      slot.close();
    }
    held.clear();
    if (nonFrameCharged != 0L) {
      allocator.refundCommittedBytes(nonFrameCharged);
      nonFrameCharged = 0L;
    }
    allocator.shutdown();
  }
}
