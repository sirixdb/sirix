/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * The side of an allocator that lends scratch buffers to a {@link ScratchLease}: it owns every
 * buffer's backing, decides what stays resident between borrows, and is the only place a buffer is
 * returned to.
 */
abstract class ScratchSource {

  /**
   * Lend one buffer of at least {@code minBytes}, exclusively, until it is returned with
   * {@link #release}.
   *
   * @param minBytes the bytes the borrower needs; positive
   * @param use what the buffer is borrowed for
   * @param mandatory whether the borrower cannot proceed without the buffer: a mandatory borrow asks
   *        the caches for relief and waits like a page frame does before it fails; an optional borrow
   *        is refused at once
   * @return the buffer, or {@code null} for an optional borrow the budget has no room for
   * @throws OutOfMemoryError when a mandatory borrow cannot be served
   */
  abstract @Nullable ScratchCarrier acquire(long minBytes, ScratchUse use, boolean mandatory);

  /**
   * Return a buffer lent by {@link #acquire}.
   *
   * @param carrier the buffer
   * @param era the era {@link #acquire} lent it in
   * @return whether this call returned the buffer; {@code false} for a stale or repeated return,
   *         which leaves the buffer with whoever holds it now
   */
  abstract boolean release(ScratchCarrier carrier, long era);

  /** The scratch source of {@code allocator}. */
  static ScratchSource of(final MemorySegmentAllocator allocator) {
    Objects.requireNonNull(allocator, "allocator");
    return allocator instanceof FrameSlotAllocator frames
        ? frames.scratch()
        : FallbackScratch.of(allocator);
  }
}
