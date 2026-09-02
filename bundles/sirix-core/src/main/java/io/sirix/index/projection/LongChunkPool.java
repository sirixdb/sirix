package io.sirix.index.projection;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Recycles the fixed-size {@code long[]} storage chunks of {@link NumericGroupAggTable} across the
 * tables of one grouped scan: worker tables that flush, partition tables that grow, and the merged
 * tables a finished pass has emitted all hand their chunks back here, and the next table takes them
 * instead of allocating.
 *
 * <p>
 * Why: a hash-range pass allocates its tables ONCE per flush and per rehash and drops them shortly
 * after — at 100M (q32) ≈ 16 GB per 2.3 s pass, of which G1 copied ≈ 9 GB through survivor and old
 * regions (≈ 19% of the query's wall time in young pauses, 20% of its CPU in evacuation memcpy).
 * A chunk that lives in the pool for the whole query is promoted once and then never copied again,
 * while every array stays below the humongous threshold the storage design commits to.
 *
 * <p>
 * Chunks are zeroed when GIVEN, not when taken: a pooled chunk is therefore always an empty bucket
 * range, a take is one dequeue, and a stale reference into a released chunk reads EMPTY buckets
 * instead of another table's groups. Thread-safe; the counters are for diagnostics and tests.
 */
public final class LongChunkPool {

  private static final LongAdder HITS = new LongAdder();
  private static final LongAdder GIVES = new LongAdder();

  /** Takes served from the pool instead of the allocator, across every pool (test observability). */
  public static long totalHits() {
    return HITS.sum();
  }

  /** Chunks accepted by any pool (test observability). */
  public static long totalGives() {
    return GIVES.sum();
  }

  private final int chunkLanes;
  private final int maxChunks;
  private final ConcurrentLinkedQueue<long[]> free = new ConcurrentLinkedQueue<>();
  private final AtomicInteger pooled = new AtomicInteger();
  private final LongAdder hits = new LongAdder();
  private final LongAdder misses = new LongAdder();
  private final LongAdder dropped = new LongAdder();

  /**
   * @param chunkLanes the one array length this pool recycles; other lengths are refused by
   *        {@link #give}
   * @param maxChunks chunks the pool keeps at most; a give past it is dropped to the collector
   */
  public LongChunkPool(final int chunkLanes, final int maxChunks) {
    if (chunkLanes <= 0) {
      throw new IllegalArgumentException("chunkLanes must be positive: " + chunkLanes);
    }
    if (maxChunks <= 0) {
      throw new IllegalArgumentException("maxChunks must be positive: " + maxChunks);
    }
    this.chunkLanes = chunkLanes;
    this.maxChunks = maxChunks;
  }

  /** Array length this pool recycles. */
  public int chunkLanes() {
    return chunkLanes;
  }

  /** Upper bound on the chunks kept. */
  public int maxChunks() {
    return maxChunks;
  }

  /** An all-zero chunk of {@link #chunkLanes} lanes: a pooled one when there is one, else fresh. */
  public long[] take() {
    final long[] chunk = free.poll();
    if (chunk != null) {
      pooled.decrementAndGet();
      hits.increment();
      HITS.increment();
      return chunk;
    }
    misses.increment();
    return new long[chunkLanes];
  }

  /**
   * Hand a chunk back. It is zeroed here and must not be touched by the caller afterwards.
   *
   * @return whether the pool kept it ({@code false}: wrong length, or the pool is full)
   */
  public boolean give(final long[] chunk) {
    if (chunk == null || chunk.length != chunkLanes) {
      return false;
    }
    if (pooled.incrementAndGet() > maxChunks) {
      pooled.decrementAndGet();
      dropped.increment();
      return false;
    }
    Arrays.fill(chunk, 0L);
    free.offer(chunk);
    GIVES.increment();
    return true;
  }

  /**
   * Drop every pooled chunk to the collector. For the moment a caller is about to MEASURE live heap
   * (the pass restart's budget refresh): what the pool holds is retained by intent only, and a
   * measurement must not read it as needed.
   */
  public void drain() {
    while (free.poll() != null) {
      pooled.decrementAndGet();
    }
  }

  /** Chunks currently held. */
  public int pooled() {
    return pooled.get();
  }

  /** Takes served from the pool. */
  public long hits() {
    return hits.sum();
  }

  /** Takes that had to allocate. */
  public long misses() {
    return misses.sum();
  }

  /** Gives refused because the pool was full. */
  public long dropped() {
    return dropped.sum();
  }

  @Override
  public String toString() {
    return "LongChunkPool[lanes=" + chunkLanes + " pooled=" + pooled.get() + "/" + maxChunks + " hits=" + hits.sum()
        + " misses=" + misses.sum() + " dropped=" + dropped.sum() + "]";
  }
}
