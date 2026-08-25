/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.ChunkedBodyConfig;

import java.util.AbstractList;
import java.util.RandomAccess;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A {@code List<byte[]>} of a projection's row-group payloads that materializes WINDOWS of leaves on
 * demand instead of the whole column family at once.
 *
 * <p>
 * The eager whole-leaf list is the right shape while it fits: every byte-scan kernel takes a
 * {@code List<byte[]>}, materialization runs once, and the bytes serve every later query. At 100M
 * rows it is the wrong shape by an order of magnitude — a fat-string projection's leaves total
 * ~8-10 GB, and the first whole-leaf consumer (a string LIKE, a distinct over strings) OOMed the
 * catalog's materializer before serving a single row. This view keeps the {@code List} contract the
 * kernels already program against — random access, {@code subList} sharding across workers — while
 * holding only a bounded set of windows resident.
 *
 * <h2>Concurrency</h2> Windows publish by CAS into an {@link AtomicReferenceArray}; a concurrent
 * first-touch of the same window races benignly (first publish wins, content is identical — the
 * same doctrine as {@link ProjectionColumnStore}'s column fills). Eviction is CLOCK second-chance
 * over touched bits: a window in active iteration is re-marked on every {@link #get} and survives;
 * readers holding {@code byte[]} references across an eviction are safe because payload arrays are
 * immutable and reachability keeps them alive. The resident bound is therefore approximate — capped
 * windows plus whatever consumers transiently pin — which is exactly the bound the budget asked
 * for, not a guarantee the GC could not give anyway.
 *
 * <h2>Failure contract</h2> A missing leaf inside a window throws {@link IllegalStateException}
 * from {@link #get}, the same truncated-store verdict the eager materializer throws — callers
 * decline serving and the generic pipeline answers.
 */
public final class ProjectionWindowedRowGroupPayloads extends AbstractList<byte[]> implements RandomAccess {

  /** Materializes the payloads of the LOGICAL row-group range {@code [from, toExclusive)}. */
  @FunctionalInterface
  public interface WindowFetcher {
    byte[][] fetch(int fromLogical, int toLogicalExclusive);
  }

  private final int rowGroupCount;
  private final int windowLeaves;
  private final int residentCap;
  private final WindowFetcher fetcher;

  private final AtomicReferenceArray<byte[][]> windows;
  /** CLOCK touched bits, one int per window ({@code 1} = referenced since the hand last passed). */
  private final AtomicIntegerArray touched;
  private final AtomicInteger resident = new AtomicInteger();
  private final AtomicInteger clockHand = new AtomicInteger();

  public ProjectionWindowedRowGroupPayloads(final int rowGroupCount, final int windowLeaves, final int residentCap,
      final WindowFetcher fetcher) {
    if (rowGroupCount < 0) {
      throw new IllegalArgumentException("rowGroupCount must be non-negative: " + rowGroupCount);
    }
    if (windowLeaves <= 0) {
      throw new IllegalArgumentException("windowLeaves must be positive: " + windowLeaves);
    }
    this.rowGroupCount = rowGroupCount;
    this.windowLeaves = windowLeaves;
    final int windowCount = rowGroupCount == 0
        ? 0
        : 1 + (rowGroupCount - 1) / windowLeaves;
    // At least two windows resident, or a kernel touching a window boundary would thrash on the
    // spot; never more than exist.
    this.residentCap = Math.max(2, Math.min(residentCap, Math.max(1, windowCount)));
    this.fetcher = fetcher;
    this.windows = new AtomicReferenceArray<>(windowCount);
    this.touched = new AtomicIntegerArray(windowCount);
  }

  @Override
  public int size() {
    return rowGroupCount;
  }

  @Override
  public byte[] get(final int index) {
    if (index < 0 || index >= rowGroupCount) {
      throw new IndexOutOfBoundsException("row group " + index + " of " + rowGroupCount);
    }
    final int window = index / windowLeaves;
    byte[][] payloads = windows.get(window);
    if (payloads == null) {
      payloads = materialize(window);
    }
    touched.set(window, 1);
    final byte[] payload = payloads[index - window * windowLeaves];
    if (payload == null) {
      throw new IllegalStateException("projection row group " + index + " missing from its materialized window — "
          + "the store is truncated");
    }
    return payload;
  }

  private byte[][] materialize(final int window) {
    final int from = window * windowLeaves;
    final int toExclusive = Math.min(from + windowLeaves, rowGroupCount);
    final byte[][] fetched = fetcher.fetch(from, toExclusive);
    if (fetched == null || fetched.length != toExclusive - from) {
      throw new IllegalStateException("window fetcher returned " + (fetched == null
          ? "null"
          : fetched.length + " payloads") + " for logical range [" + from + ", " + toExclusive + ")");
    }
    if (windows.compareAndSet(window, null, fetched)) {
      ChunkedBodyConfig.recordColumnWindowMaterialization();
      if (resident.incrementAndGet() > residentCap) {
        evictOne(window);
      }
      return fetched;
    }
    // A concurrent first-touch published first; use the winner's identical content.
    final byte[][] winner = windows.get(window);
    return winner != null
        ? winner
        : fetched;
  }

  /** Advance the clock hand until a cold (untouched) window other than {@code keep} gives way. */
  private void evictOne(final int keep) {
    final int windowCount = windows.length();
    // Bounded sweep: one full pass clearing touched bits plus one pass finding a victim. If every
    // window stays hot (residentCap ~= windowCount under a scatter access pattern), give up rather
    // than spin — the cap is a target, not an invariant, and the weigher already accounts worst
    // case.
    for (int step = 0; step < windowCount * 2; step++) {
      final int candidate = Math.floorMod(clockHand.getAndIncrement(), windowCount);
      if (candidate == keep || windows.get(candidate) == null) {
        continue;
      }
      if (touched.getAndSet(candidate, 0) == 1) {
        continue; // second chance
      }
      if (windows.getAndSet(candidate, null) != null) {
        resident.decrementAndGet();
        return;
      }
    }
  }

  /** Windows currently resident — observability for the engagement witness. */
  public int residentWindows() {
    return resident.get();
  }

  /** Total windows this view partitions the row groups into. */
  public int windowCount() {
    return windows.length();
  }
}
