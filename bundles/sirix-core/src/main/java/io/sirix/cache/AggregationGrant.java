/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

/**
 * One scan's exclusive byte-capacity grant. Regions charge their actual rounded capacity before
 * allocation. All workers share this grant; close returns it only after their leases have closed.
 */
public final class AggregationGrant implements AutoCloseable {
  private final AggregationBudget budget;
  private final MemorySegmentAllocator allocator;
  private boolean incremental;
  private long capacity;
  private long used;
  private long peakUsed;
  private boolean closed;

  AggregationGrant(final AggregationBudget budget, final MemorySegmentAllocator allocator, final long capacity,
      final boolean incremental) {
    this.budget = budget;
    this.allocator = allocator;
    this.capacity = capacity;
    this.incremental = incremental;
  }

  public long allocationCapacity(final long bytes) {
    return ScratchSource.of(allocator).aggregationCapacity(bytes);
  }

  public ScratchLease borrow(final long bytes) {
    return new ScratchLease(allocator).acquireAggregation(bytes, this);
  }

  /** Seal a scan's capacity before planning. No later worker can enlarge the grant. */
  public synchronized long preparePlan(final long minimum) {
    if (closed) {
      throw new IllegalStateException("aggregation grant is closed");
    }
    if (incremental) {
      capacity = budget.preparePlan(capacity, minimum);
      incremental = false;
    }
    if (capacity < minimum) {
      throw new OutOfMemoryError("aggregation minimum exceeds its existing grant");
    }
    return capacity;
  }

  synchronized void claim(final long bytes) {
    if (closed) {
      throw new IllegalStateException("aggregation grant is closed");
    }
    if (bytes <= 0) {
      throw new IllegalArgumentException("region capacity must be positive");
    }
    if (bytes > capacity - used) {
      final long extra = bytes - (capacity - used);
      if (!incremental || !budget.expand(extra)) {
        throw new OutOfMemoryError(
            "aggregation region of " + bytes + " exceeds grant (capacity=" + capacity + ", used=" + used + ")");
      }
      capacity += extra;
    }
    used += bytes;
    peakUsed = Math.max(peakUsed, used);
  }

  synchronized void refund(final long bytes) {
    if (bytes < 0 || bytes > used) {
      throw new IllegalStateException("invalid aggregation capacity refund");
    }
    used -= bytes;
  }

  public synchronized long capacity() {
    return capacity;
  }

  public synchronized long usedBytes() {
    return used;
  }

  public synchronized long peakUsedBytes() {
    return peakUsed;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    if (used != 0L) {
      throw new IllegalStateException("aggregation regions must be returned before closing their grant");
    }
    closed = true;
    budget.release(capacity);
  }
}
