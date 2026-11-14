/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.util.concurrent.CancellationException;

/** Allocator-owned exclusive capacity for aggregation; no row-loop accounting or allocations. */
public final class AggregationBudget {
  public static final int DIVISOR = 8;
  private final MemorySegmentAllocator allocator;
  private long granted;
  private long peakGranted;
  private long waits;

  AggregationBudget(final MemorySegmentAllocator allocator) {
    this.allocator = allocator;
  }

  public long capacity() {
    return allocator.getMaxBufferSize() / DIVISOR;
  }

  /**
   * Obtain an exclusive grant before planning. Other scans queue when the minimum safe working set is
   * unavailable; interruption cancels admission without retaining a grant. A small tools-only
   * workspace may instead acquire an initially empty, growable grant with {@link #incremental()}.
   */
  public synchronized AggregationGrant acquire(final long preferred, final long minimum) {
    requireOpen();
    final long capacity = capacity();
    if (minimum <= 0 || preferred < minimum) {
      throw new IllegalArgumentException("invalid aggregation grant bounds");
    }
    if (minimum > capacity) {
      throw new OutOfMemoryError("aggregation minimum " + minimum + " exceeds execution capacity " + capacity);
    }
    while (capacity - granted < minimum) {
      waits++;
      try {
        wait();
        requireOpen();
      } catch (final InterruptedException _) {
        Thread.currentThread().interrupt();
        throw new CancellationException("interrupted while waiting for aggregation capacity");
      }
    }
    final long bytes = Math.min(preferred, capacity - granted);
    final AggregationGrant grant = new AggregationGrant(this, allocator, bytes, false);
    granted += bytes;
    peakGranted = Math.max(peakGranted, granted);
    return grant;
  }

  /** No up-front entitlement: small standalone tables claim capacity when borrowing regions. */
  public synchronized AggregationGrant incremental() {
    requireOpen();
    return new AggregationGrant(this, allocator, 0L, true);
  }

  synchronized boolean expand(final long bytes) {
    requireOpen();
    if (bytes > capacity() - granted) {
      return false;
    }
    granted += bytes;
    peakGranted = Math.max(peakGranted, granted);
    return true;
  }

  synchronized long preparePlan(final long current, final long minimum) {
    requireOpen();
    if (minimum <= 0 || minimum > capacity()) {
      throw new OutOfMemoryError("aggregation minimum working set exceeds execution capacity");
    }
    while (capacity() - granted + current < minimum) {
      if (current != 0L) {
        // Two partially built scans must never wait for each other's already held capacity.
        throw new OutOfMemoryError("insufficient capacity to plan an aggregation with live regions");
      }
      waits++;
      try {
        wait();
        requireOpen();
      } catch (final InterruptedException _) {
        Thread.currentThread().interrupt();
        throw new CancellationException("interrupted while waiting for aggregation capacity");
      }
    }
    final long extra = capacity() - granted;
    granted += extra;
    peakGranted = Math.max(peakGranted, granted);
    return current + extra;
  }

  synchronized void release(final long bytes) {
    granted -= bytes;
    notifyAll();
  }

  private void requireOpen() {
    if (!allocator.isInitialized() || (allocator instanceof FrameSlotAllocator frames && frames.isTerminated())) {
      throw new IllegalStateException("allocator is not open for aggregation admission");
    }
  }

  public synchronized long grantedBytes() {
    return granted;
  }

  public synchronized long peakGrantedBytes() {
    return peakGranted;
  }

  public synchronized long waits() {
    return waits;
  }
}
