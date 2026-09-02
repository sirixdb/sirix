package io.sirix.index.projection;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
 * A pool per SCAN still allocates every chunk once per hash-range pass — the pass's spill is new,
 * so its pool starts empty: at 100M (q16) each of the two passes of every try allocated 21,504
 * chunks (2.75 GB) that the previous pass had just handed back to a pool nobody would read again,
 * 5.5 GB of the 12.4 GB a hot try allocated, and the only part of it G1 promoted and copied. The
 * {@link #shared(int, int) shared} pools outlive the scan: one per chunk length for the JVM, holding
 * at most {@link #RETAIN_BYTES_PROPERTY} bytes IN TOTAL (default a quarter of the maximum heap — one
 * pass's tables at the clean-heap group budget). What they hold is retained BY INTENT and readable as
 * live heap by every collector record, so the group budget adds {@link #retainedBytes()} back to the
 * headroom it plans against ({@link GroupTableSpill#groupBudget()}): the next pass takes those chunks
 * instead of allocating, which is exactly the memory the budget would have planned to allocate.
 * {@code -Dsirix.projection.groupTable.chunkPool.retain=false} restores a pool per scan.
 *
 * <p>
 * The ceiling bounds the RESOURCE, not each pool: a scan's stride fixes its chunk length, a suite of
 * group-bys visits several strides, and a ceiling applied per pool let every geometry keep a pass's
 * tables at once — at 100M the leg after q16 held one such pool per stride it had run, until q28 ran
 * 199 collections per try and q30 stalled at a full heap. So {@link #shared(int, int)} drains every
 * OTHER geometry's pool (during this scan their chunks are dead weight, and the next scan of that
 * length allocates once, as it did before pooling), and {@link #give} refuses a chunk that would carry
 * the total retained past the ceiling regardless of the pool it lands in.
 *
 * <p>
 * Chunks are zeroed when GIVEN, not when taken: a pooled chunk is therefore always an empty bucket
 * range, a take is one dequeue, and a stale reference into a released chunk reads EMPTY buckets
 * instead of another table's groups. Thread-safe; the counters are for diagnostics and tests.
 */
public final class LongChunkPool {

  /** Keep chunks across scans in JVM-lifetime pools (default on); {@code false} = a pool per scan. */
  public static final String RETAIN_PROPERTY = "sirix.projection.groupTable.chunkPool.retain";
  /** Byte ceiling over every shared pool's retained chunks; default {@code maxMemory / 4}. */
  public static final String RETAIN_BYTES_PROPERTY = "sirix.projection.groupTable.chunkPool.retainBytes";
  private static final boolean RETAIN = !"false".equalsIgnoreCase(System.getProperty(RETAIN_PROPERTY, "true"));
  private static final long RETAIN_BYTES = retainBytesConfigured();
  private static final ConcurrentHashMap<Integer, LongChunkPool> SHARED = new ConcurrentHashMap<>();
  private static volatile int retainForTesting = -1;
  private static volatile long retainBytesForTesting = -1L;
  /** Bytes every shared pool holds right now: the one figure the ceiling is checked against. */
  private static final AtomicLong RETAINED_BYTES = new AtomicLong();

  private static final LongAdder HITS = new LongAdder();
  private static final LongAdder GIVES = new LongAdder();

  private static long retainBytesConfigured() {
    final long configured = Long.getLong(RETAIN_BYTES_PROPERTY, -1L);
    return configured >= 0L
        ? configured
        : Runtime.getRuntime().maxMemory() / 4L;
  }

  /** Whether scans draw from the shared pools (the property, or the test override). */
  public static boolean retainAcrossScans() {
    final int testing = retainForTesting;
    return testing >= 0
        ? testing != 0
        : RETAIN;
  }

  /**
   * Test seam: force the shared pools on or off.
   *
   * @param value {@code 1} on, {@code 0} off, negative restores the property
   * @return the previous override, for restoring in a finally block
   */
  public static int setRetainForTesting(final int value) {
    final int previous = retainForTesting;
    retainForTesting = value;
    return previous;
  }

  /**
   * The JVM-lifetime pool for {@code chunkLanes}-lane chunks, its capacity raised to {@code
   * maxChunks} when the caller's scan needs more than an earlier one did — never above the retain
   * ceiling. Every other chunk length's pool is drained first: one retained geometry at a time is
   * what keeps the ceiling a bound on the heap rather than on each pool.
   */
  public static LongChunkPool shared(final int chunkLanes, final int maxChunks) {
    final LongChunkPool pool = SHARED.computeIfAbsent(chunkLanes, lanes -> new LongChunkPool(lanes, 1, true));
    for (final LongChunkPool other : SHARED.values()) {
      if (other != pool) {
        other.drain();
      }
    }
    pool.raiseCapacity(Math.min(maxChunks, retainCeilingChunks(chunkLanes)));
    return pool;
  }

  /** The byte ceiling over every shared pool: the property, or the test override. */
  static long retainBytes() {
    final long testing = retainBytesForTesting;
    return testing >= 0L
        ? testing
        : RETAIN_BYTES;
  }

  /**
   * Test seam: pin the retain ceiling so the bound can be exercised with a handful of chunks.
   *
   * @param value the ceiling in bytes, or a negative value to restore the property
   * @return the previous override, for restoring in a finally block
   */
  public static long setRetainBytesForTesting(final long value) {
    final long previous = retainBytesForTesting;
    retainBytesForTesting = value;
    return previous;
  }

  /** Chunks of {@code chunkLanes} lanes the retain ceiling admits, at least one. */
  static int retainCeilingChunks(final int chunkLanes) {
    final long chunkBytes = (long) chunkLanes * Long.BYTES;
    return (int) Math.max(1L, Math.min(Integer.MAX_VALUE, retainBytes() / chunkBytes));
  }

  /** Bytes the shared pools hold right now — retained by intent, live to every collector record. */
  public static long retainedBytes() {
    return RETAINED_BYTES.get();
  }

  /** The shared pool of this chunk length if one exists (test observability). */
  static LongChunkPool sharedOrNull(final int chunkLanes) {
    return SHARED.get(chunkLanes);
  }

  /** Drop every chunk of every shared pool to the collector (tests; a caller shedding heap). */
  public static void releaseShared() {
    for (final LongChunkPool pool : SHARED.values()) {
      pool.drain();
    }
  }

  /** Takes served from the pool instead of the allocator, across every pool (test observability). */
  public static long totalHits() {
    return HITS.sum();
  }

  /** Chunks accepted by any pool (test observability). */
  public static long totalGives() {
    return GIVES.sum();
  }

  private final int chunkLanes;
  private final boolean shared;
  private volatile int maxChunks;
  private final ConcurrentLinkedQueue<long[]> free = new ConcurrentLinkedQueue<>();
  private final AtomicInteger pooled = new AtomicInteger();
  private final LongAdder hits = new LongAdder();
  private final LongAdder misses = new LongAdder();
  private final LongAdder gives = new LongAdder();
  private final LongAdder dropped = new LongAdder();

  /**
   * @param chunkLanes the one array length this pool recycles; other lengths are refused by
   *        {@link #give}
   * @param maxChunks chunks the pool keeps at most; a give past it is dropped to the collector
   */
  public LongChunkPool(final int chunkLanes, final int maxChunks) {
    this(chunkLanes, maxChunks, false);
  }

  private LongChunkPool(final int chunkLanes, final int maxChunks, final boolean shared) {
    if (chunkLanes <= 0) {
      throw new IllegalArgumentException("chunkLanes must be positive: " + chunkLanes);
    }
    if (maxChunks <= 0) {
      throw new IllegalArgumentException("maxChunks must be positive: " + maxChunks);
    }
    this.chunkLanes = chunkLanes;
    this.maxChunks = maxChunks;
    this.shared = shared;
  }

  /** Whether this pool outlives the scan that took it ({@link #shared(int, int)}). */
  public boolean isShared() {
    return shared;
  }

  /** Array length this pool recycles. */
  public int chunkLanes() {
    return chunkLanes;
  }

  /** Upper bound on the chunks kept. */
  public int maxChunks() {
    return maxChunks;
  }

  /** Raise the ceiling to {@code chunks} when it is above the current one; never lowers it. */
  private void raiseCapacity(final int chunks) {
    if (chunks > maxChunks) {
      synchronized (this) {
        if (chunks > maxChunks) {
          maxChunks = chunks;
        }
      }
    }
  }

  /** An all-zero chunk of {@link #chunkLanes} lanes: a pooled one when there is one, else fresh. */
  public long[] take() {
    final long[] chunk = free.poll();
    if (chunk != null) {
      pooled.decrementAndGet();
      if (shared) {
        RETAINED_BYTES.addAndGet(-chunkBytes());
      }
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
    if (shared) {
      // The ceiling is a bound on the heap: checked against what EVERY shared pool holds.
      final long bytes = chunkBytes();
      if (RETAINED_BYTES.addAndGet(bytes) > retainBytes()) {
        RETAINED_BYTES.addAndGet(-bytes);
        pooled.decrementAndGet();
        dropped.increment();
        return false;
      }
    }
    Arrays.fill(chunk, 0L);
    free.offer(chunk);
    gives.increment();
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
      if (shared) {
        RETAINED_BYTES.addAndGet(-chunkBytes());
      }
    }
  }

  private long chunkBytes() {
    return (long) chunkLanes * Long.BYTES;
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

  /** Chunks this pool accepted. */
  public long gives() {
    return gives.sum();
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
