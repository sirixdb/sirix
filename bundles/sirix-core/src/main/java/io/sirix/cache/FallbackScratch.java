/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Scratch buffers for an allocator other than {@link FrameSlotAllocator}: the legacy pool
 * allocators kept for rollback, and test doubles. Correct and bounded, not tuned.
 *
 * <p>
 * A borrow that fits the allocator's largest segment size is that allocator's own
 * {@link MemorySegmentAllocator#allocate(long)}, charged and waited for exactly like one of its
 * page frames. The pool allocators have no larger allocation, so a larger borrow maps a region of
 * its own and is charged to a separate bound, {@link #largeLimitBytes}: an eighth of the
 * allocator's budget, at least {@value #MIN_LARGE_LIMIT_BYTES} bytes. Nothing stays resident
 * between borrows; the buffer's backing is returned with it.
 */
final class FallbackScratch extends ScratchSource {

  /** Floor of the bound on mapped borrows: two of the largest coalesced reads. */
  static final long MIN_LARGE_LIMIT_BYTES = 32L << 20;

  private static final long PAGE_BYTES = 4096L;

  private static final int FRAME_CLASS = 0;
  private static final int MAPPED_CLASS = 1;

  private static final long LARGEST_SEGMENT_BYTES =
      MemorySegmentAllocator.SEGMENT_SIZES[MemorySegmentAllocator.SEGMENT_SIZES.length - 1];

  /** One source per allocator instance; weak so a test double does not outlive its test. */
  private static final Map<MemorySegmentAllocator, FallbackScratch> SOURCES =
      Collections.synchronizedMap(new WeakHashMap<>());

  private final MemorySegmentAllocator allocator;
  private final AtomicLong mappedBytes = new AtomicLong();

  private FallbackScratch(final MemorySegmentAllocator allocator) {
    this.allocator = allocator;
  }

  static FallbackScratch of(final MemorySegmentAllocator allocator) {
    return SOURCES.computeIfAbsent(allocator, FallbackScratch::new);
  }

  /** Bound on the bytes of mapped borrows that may be lent at once. */
  long largeLimitBytes() {
    return Math.max(MIN_LARGE_LIMIT_BYTES, allocator.getMaxBufferSize() / 8);
  }

  /** Bytes of mapped borrows lent now. */
  long mappedBytes() {
    return mappedBytes.get();
  }

  @Override
  @Nullable
  ScratchCarrier acquire(final long minBytes, final ScratchUse use, final boolean mandatory) {
    final ScratchCarrier carrier;
    if (minBytes <= LARGEST_SEGMENT_BYTES) {
      // The allocator's own budgeted path; it waits for its page caches itself.
      carrier = new ScratchCarrier(this, FRAME_CLASS, allocator.allocate(minBytes), -1, 0L);
    } else {
      final long mapped = minBytes > Long.MAX_VALUE - PAGE_BYTES
          ? Long.MAX_VALUE
          : (minBytes + PAGE_BYTES - 1) & -PAGE_BYTES;
      if (!reserveMapped(mapped)) {
        if (!mandatory) {
          return null;
        }
        throw new OutOfMemoryError("scratch borrow of " + minBytes + " bytes exceeds the " + largeLimitBytes()
            + "-byte bound on mapped scratch buffers (" + mappedBytes.get() + " bytes lent)");
      }
      final MemorySegment region;
      try {
        final VirtualMemory memory = VirtualMemory.forCurrentPlatform();
        region = memory.reserve(mapped);
        try {
          memory.commitFresh(region);
        } catch (final RuntimeException | Error failure) {
          memory.release(region);
          throw failure;
        }
      } catch (final RuntimeException | Error failure) {
        mappedBytes.addAndGet(-mapped);
        throw failure;
      }
      carrier = new ScratchCarrier(this, MAPPED_CLASS, region, -1, 0L);
    }
    carrier.lend(minBytes, use);
    return carrier;
  }

  private boolean reserveMapped(final long bytes) {
    final long limit = largeLimitBytes();
    for (;;) {
      final long current = mappedBytes.get();
      if (bytes > limit - current) {
        return false;
      }
      if (mappedBytes.compareAndSet(current, current + bytes)) {
        return true;
      }
    }
  }

  @Override
  boolean release(final ScratchCarrier carrier, final long era) {
    if (!carrier.takeBack(era)) {
      return false;
    }
    if (carrier.capacityClass == FRAME_CLASS) {
      allocator.release(carrier.segment);
    } else {
      VirtualMemory.forCurrentPlatform().release(carrier.segment);
      mappedBytes.addAndGet(-carrier.capacity);
    }
    return true;
  }
}
