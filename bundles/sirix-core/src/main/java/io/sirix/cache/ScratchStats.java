/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.util.Objects;

/**
 * A snapshot of the allocator's scratch buffers: what was asked for, what it was rounded to, what
 * is lent right now, what stays resident for the next borrow, and what was refused.
 *
 * <p>
 * The per-capacity arrays have one entry per reusable capacity in ascending order, followed by one
 * entry for exactly sized buffers beyond the largest capacity (whose {@link #capacityBytes} entry
 * is zero). The per-use arrays are indexed by {@link ScratchUse#ordinal()}. Counters never
 * decrease; the gauges ({@code lent*}, {@code idle*}) are exact while no borrow or return is in
 * flight.
 *
 * @param capacityBytes the bytes of each reusable capacity; zero for the trailing exact-size entry
 * @param acquisitions borrows served per capacity
 * @param created buffers given fresh backing per capacity; {@code acquisitions - created} borrows
 *        reused a resident buffer
 * @param reclaimed buffers whose backing was returned per capacity: under pressure, because the
 *        idle bound was full, at shutdown, or (exact sizes) on every return
 * @param lentBuffers buffers a lease holds now, per capacity
 * @param idleBuffers buffers kept resident for the next borrow, per capacity
 * @param requestedBytes sum of the bytes the borrowers asked for
 * @param roundedBytes sum of the capacities those borrows were served with
 * @param lentBytes capacity bytes leases hold now
 * @param idleBytes capacity bytes kept resident for the next borrow
 * @param refusals borrows refused because the budget had no room
 * @param refusedBytes sum of the bytes those refused borrows asked for
 * @param staleReleases returns rejected because the buffer had already been returned
 * @param acquisitionsByUse borrows per {@link ScratchUse}
 * @param requestedBytesByUse requested bytes per {@link ScratchUse}
 */
public record ScratchStats(long[] capacityBytes, long[] acquisitions, long[] created, long[] reclaimed,
    long[] lentBuffers, long[] idleBuffers, long requestedBytes, long roundedBytes, long lentBytes, long idleBytes,
    long refusals, long refusedBytes, long staleReleases, long[] acquisitionsByUse, long[] requestedBytesByUse) {

  /** No scratch buffers and no history: an allocator that has lent nothing. */
  public static final ScratchStats EMPTY = new ScratchStats(new long[0], new long[0], new long[0], new long[0],
      new long[0], new long[0], 0L, 0L, 0L, 0L, 0L, 0L, 0L, new long[ScratchUse.COUNT], new long[ScratchUse.COUNT]);

  public ScratchStats {
    Objects.requireNonNull(capacityBytes, "capacityBytes");
    Objects.requireNonNull(acquisitions, "acquisitions");
    Objects.requireNonNull(created, "created");
    Objects.requireNonNull(reclaimed, "reclaimed");
    Objects.requireNonNull(lentBuffers, "lentBuffers");
    Objects.requireNonNull(idleBuffers, "idleBuffers");
    Objects.requireNonNull(acquisitionsByUse, "acquisitionsByUse");
    Objects.requireNonNull(requestedBytesByUse, "requestedBytesByUse");
    final int capacities = capacityBytes.length;
    if (acquisitions.length != capacities || created.length != capacities || reclaimed.length != capacities
        || lentBuffers.length != capacities || idleBuffers.length != capacities) {
      throw new IllegalArgumentException("per-capacity arrays must have one entry per capacity");
    }
    if (acquisitionsByUse.length != ScratchUse.COUNT || requestedBytesByUse.length != ScratchUse.COUNT) {
      throw new IllegalArgumentException("per-use arrays must have one entry per ScratchUse");
    }
  }

  /** Borrows served over all capacities. */
  public long totalAcquisitions() {
    return sum(acquisitions);
  }

  /** Buffers given fresh backing over all capacities. */
  public long totalCreated() {
    return sum(created);
  }

  /** Buffers whose backing was returned over all capacities. */
  public long totalReclaimed() {
    return sum(reclaimed);
  }

  /** Borrows that reused a resident buffer instead of taking fresh backing. */
  public long totalReused() {
    return totalAcquisitions() - totalCreated();
  }

  /** Buffers a lease holds now over all capacities. */
  public long totalLentBuffers() {
    return sum(lentBuffers);
  }

  /** Buffers kept resident for the next borrow over all capacities. */
  public long totalIdleBuffers() {
    return sum(idleBuffers);
  }

  /** Borrows served for {@code use}. */
  public long acquisitions(final ScratchUse use) {
    return acquisitionsByUse[use.ordinal()];
  }

  /** Requested bytes of the borrows served for {@code use}. */
  public long requestedBytes(final ScratchUse use) {
    return requestedBytesByUse[use.ordinal()];
  }

  /**
   * The entry of the per-capacity arrays that serves a borrow of {@code bytes}: the smallest capacity
   * that fits, or the trailing exact-size entry.
   */
  public int capacityIndex(final long bytes) {
    for (int i = 0; i < capacityBytes.length; i++) {
      if (capacityBytes[i] != 0L && bytes <= capacityBytes[i]) {
        return i;
      }
    }
    return capacityBytes.length - 1;
  }

  private static long sum(final long[] values) {
    long total = 0L;
    for (final long value : values) {
      total += value;
    }
    return total;
  }
}
