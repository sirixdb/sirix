/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.ref.WeakReference;
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
 * allocator's budget. A separate eighth is reserved for exclusive aggregation grants. Page
 * commitments cannot consume either eighth. Nothing stays resident between borrows; the buffer's
 * backing is returned with it.
 */
final class FallbackScratch extends ScratchSource {

  private static final long PAGE_BYTES = 4096L;

  private static final int FRAME_CLASS = 0;
  private static final int MAPPED_CLASS = 1;

  private static final long LARGEST_SEGMENT_BYTES =
      MemorySegmentAllocator.SEGMENT_SIZES[MemorySegmentAllocator.SEGMENT_SIZES.length - 1];

  /** One source per allocator instance; weak so a test double does not outlive its test. */
  private static final Map<MemorySegmentAllocator, WeakReference<FallbackScratch>> SOURCES =
      Collections.synchronizedMap(new WeakHashMap<>());

  private final MemorySegmentAllocator allocator;
  private final AtomicLong mappedBytes = new AtomicLong();
  private final AtomicLong aggregationBytes = new AtomicLong();
  private final AggregationBudget aggregationBudget;

  private FallbackScratch(final MemorySegmentAllocator allocator) {
    this.allocator = allocator;
    this.aggregationBudget = new AggregationBudget(allocator, this);
  }

  AggregationBudget aggregationBudget() {
    return aggregationBudget;
  }

  /** The legacy backends leave separate eighths for mapped scratch and execution. */
  static long pageAllowance(final long budget) {
    return budget - 2 * (budget / AggregationBudget.DIVISOR);
  }

  static FallbackScratch of(final MemorySegmentAllocator allocator) {
    synchronized (SOURCES) {
      final WeakReference<FallbackScratch> reference = SOURCES.get(allocator);
      FallbackScratch source = reference == null
          ? null
          : reference.get();
      if (source == null) {
        source = new FallbackScratch(allocator);
        SOURCES.put(allocator, new WeakReference<>(source));
      }
      return source;
    }
  }

  /** Bound on the bytes of mapped borrows that may be lent at once. */
  long largeLimitBytes() {
    return allocator.getMaxBufferSize() / AggregationBudget.DIVISOR;
  }

  /** Bytes of mapped borrows lent now. */
  long mappedBytes() {
    return mappedBytes.get() + aggregationBytes.get();
  }

  @Override
  ScratchCarrier acquireAggregation(final long minBytes, final AggregationGrant grant) {
    if (minBytes <= 0 || minBytes > Long.MAX_VALUE - PAGE_BYTES) {
      throw new IllegalArgumentException("invalid aggregation region size");
    }
    final long bytes = (minBytes + PAGE_BYTES - 1) & -PAGE_BYTES;
    grant.claim(bytes);
    final VirtualMemory memory = VirtualMemory.forCurrentPlatform();
    MemorySegment region = null;
    try {
      region = memory.reserve(bytes);
      memory.commitFresh(region);
      final ScratchCarrier carrier = new ScratchCarrier(this, MAPPED_CLASS, region, -1, 0L);
      carrier.aggregationGrant = grant;
      aggregationBytes.addAndGet(bytes);
      carrier.lend(minBytes, ScratchUse.AGGREGATION);
      return carrier;
    } catch (final RuntimeException | Error failure) {
      if (region != null) {
        memory.release(region);
      }
      grant.refund(bytes);
      throw failure;
    }
  }

  @Override
  @Nullable
  ScratchCarrier acquire(final long minBytes, final ScratchUse use, final boolean mandatory) {
    final ScratchCarrier carrier;
    if (minBytes <= LARGEST_SEGMENT_BYTES) {
      // The allocator's own budgeted path; it waits for its page caches itself.
      final MemorySegment segment = allocator.allocate(minBytes);
      try {
        carrier = new ScratchCarrier(this, FRAME_CLASS, segment, -1, 0L);
      } catch (final RuntimeException | Error failure) {
        allocator.release(segment);
        throw failure;
      }
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
          carrier = new ScratchCarrier(this, MAPPED_CLASS, region, -1, 0L);
        } catch (final RuntimeException | Error failure) {
          memory.release(region);
          throw failure;
        }
      } catch (final RuntimeException | Error failure) {
        mappedBytes.addAndGet(-mapped);
        throw failure;
      }
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
      final AggregationGrant grant = carrier.aggregationGrant;
      if (grant == null) {
        mappedBytes.addAndGet(-carrier.capacity);
      } else {
        carrier.aggregationGrant = null;
        aggregationBytes.addAndGet(-carrier.capacity);
        grant.refund(carrier.capacity);
      }
    }
    return true;
  }
}
