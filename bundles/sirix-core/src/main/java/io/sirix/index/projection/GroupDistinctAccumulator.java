package io.sirix.index.projection;

import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

import static java.util.Objects.requireNonNull;

/**
 * Exact, memory-bounded grouped {@code COUNT(DISTINCT)} state shared by every scan worker.
 *
 * <p>
 * The per-worker design it replaces kept one {@code group → set} map per worker, so a value seen by
 * all workers was stored once per worker and unioned again at the end: memory was
 * {@code workers × distinct pairs} at its peak, the ceiling was split per worker and only FLAGGED
 * an overrun while inserts carried on to the end of the scan, and a state above the ceiling was
 * declined only after it had been fully built. At 100M rows that was a heap exhaustion before it
 * was a decline.
 *
 * <p>
 * Here a {@code (group, value)} pair is routed to ONE of 64 stripes by the group's hash (16 ways)
 * and the value's hash (4 ways). Every worker sends the same pair to the same stripe, so the
 * stripe's per-group set holds it once regardless of how many workers saw it: memory is the number
 * of distinct pairs, and a group's exact count is the sum of its (value-disjoint) sets over the
 * four value stripes — no union ever runs. Rows whose group key is missing go to a per-stripe
 * missing set by the value's hash alone, under the same disjointness. Workers batch their inserts
 * per stripe (256 pairs) and flush under the stripe's monitor, so the lock traffic is one
 * acquisition per 256 rows. The entry count is checked at every flush against a heap-derived
 * ceiling: past it, every sink drops its input and {@link #exceeded()} makes the caller DECLINE
 * (never sketch) — the state cannot grow past the ceiling by more than the workers' unflushed
 * batches.
 *
 * <p>
 * Thread contract: {@link #worker(int)} hands each parallel slot its own single-threaded
 * {@link Worker}; {@link #finish()} runs on the coordinating thread after the parallel section has
 * joined and publishes the sizes.
 */
public final class GroupDistinctAccumulator {

  /** Configured ceiling on the number of distinct {@code (group, value)} pairs kept exactly. */
  public static final String MAX_VALUES_PROPERTY = "sirix.projection.groupDistinct.maxValues";

  private static final int GROUP_STRIPE_BITS = 4;
  private static final int VALUE_STRIPE_BITS = 2;
  private static final int STRIPES = 1 << (GROUP_STRIPE_BITS + VALUE_STRIPE_BITS);
  private static final int STRIPE_MASK = STRIPES - 1;
  /** Pairs a worker stages per stripe before it takes the stripe's monitor. */
  public static final int BATCH = 256;
  private static final long MIX = 0x9E3779B97F4A7C15L;
  /** Approximate heap bytes per exact entry: an 8-byte key at fastutil's 0.75 load, plus slack. */
  private static final long BYTES_PER_ENTRY = 24L;
  private static final long FLOOR_VALUES = 1L << 24;
  private static final long CEILING_VALUES = 1L << 28;

  private static volatile long maxValuesForTesting = -1L;

  /**
   * The ceiling in effect: the system property when set, else an eighth of the heap at
   * {@value #BYTES_PER_ENTRY} bytes per entry, floored at 2^24 and capped at 2^28 entries.
   */
  public static long defaultMaxValues() {
    final long testing = maxValuesForTesting;
    if (testing >= 0L) {
      return testing;
    }
    final String configured = System.getProperty(MAX_VALUES_PROPERTY);
    if (configured != null && !configured.isEmpty()) {
      try {
        return Long.parseLong(configured.trim());
      } catch (final NumberFormatException ignored) {
        // fall through to the derived default
      }
    }
    return defaultMaxValuesFor(Runtime.getRuntime().maxMemory(), HeapHeadroom.headroomBytes());
  }

  /**
   * The derived ceiling for {@code maxMemory} and {@code headroom} bytes (pure, for tests): the
   * shared {@link HeapHeadroom#plannedShareBytes(long, long)} at {@value #BYTES_PER_ENTRY} bytes per
   * entry, within the floor and the cap. The share itself lives in ONE place, so this ceiling, the
   * per-pass group budget and the store's residency budget cannot drift apart.
   */
  public static long defaultMaxValuesFor(final long maxMemory, final long headroom) {
    final long planned = HeapHeadroom.plannedShareBytes(maxMemory, headroom) / BYTES_PER_ENTRY;
    return Math.max(FLOOR_VALUES, Math.min(CEILING_VALUES, planned));
  }

  /**
   * Test seam: pin the ceiling so a small corpus exercises the decline.
   *
   * @param value the ceiling in entries, or a negative value to restore the derived default
   * @return the previous override, for restoring in a finally block
   */
  public static long setMaxValuesForTesting(final long value) {
    final long previous = maxValuesForTesting;
    maxValuesForTesting = value;
    return previous;
  }

  /** The number of stripes, for sizing the growth slack in tests. */
  public static int stripes() {
    return STRIPES;
  }

  private static final class Stripe {
    final Long2ObjectOpenHashMap<LongOpenHashSet> groups = new Long2ObjectOpenHashMap<>();
    LongOpenHashSet missing;
  }

  /**
   * A worker's handle on one group (or on the missing-key rows): {@link #add(long)} is all it does.
   */
  public static final class Sink {
    private final Worker worker;
    private final long group;
    private final boolean missing;

    private Sink(final Worker worker, final long group, final boolean missing) {
      this.worker = worker;
      this.group = group;
      this.missing = missing;
    }

    /** Record {@code value} under this sink's group. Dropped once the ceiling has been exceeded. */
    public void add(final long value) {
      if (worker == null) {
        return; // the discard sink of a pass this group does not belong to
      }
      if (missing) {
        worker.addMissing(value);
      } else {
        worker.add(group, value);
      }
    }
  }

  /** One parallel slot's single-threaded front: per-stripe batches and a sink per group. */
  public final class Worker {
    private final long[][] batchGroups = new long[STRIPES][];
    private final long[][] batchValues = new long[STRIPES][];
    private final int[] batchFill = new int[STRIPES];
    private final long[][] missingBatch = new long[STRIPES][];
    private final int[] missingFill = new int[STRIPES];
    private final Long2ObjectOpenHashMap<Sink> sinks = new Long2ObjectOpenHashMap<>();
    private final Sink missingSink = new Sink(this, 0L, true);

    private Worker() {}

    /** The sink for {@code group}; one object per group per worker, cached. */
    public Sink sinkFor(final long group) {
      if (passShift < 64) {
        final int p = (int) (HashCommon.mix(group) >>> passShift);
        if (p < passLo || p >= passHi) {
          return discardSink;
        }
      }
      Sink sink = sinks.get(group);
      if (sink == null) {
        sink = new Sink(this, group, false);
        sinks.put(group, sink);
      }
      return sink;
    }

    /** The sink for rows whose group key is missing — owned by the pass holding partition 0. */
    public Sink missing() {
      return passShift < 64 && passLo > 0
          ? discardSink
          : missingSink;
    }

    private void add(final long group, final long value) {
      if (exceeded) {
        return;
      }
      final int stripe = stripeOf(group, value);
      long[] groups = batchGroups[stripe];
      if (groups == null) {
        groups = new long[BATCH];
        batchGroups[stripe] = groups;
        batchValues[stripe] = new long[BATCH];
      }
      final int fill = batchFill[stripe];
      groups[fill] = group;
      batchValues[stripe][fill] = value;
      if (fill + 1 == BATCH) {
        flushGroups(stripe, BATCH);
      } else {
        batchFill[stripe] = fill + 1;
      }
    }

    private void addMissing(final long value) {
      if (exceeded) {
        return;
      }
      final int stripe = missingStripeOf(value);
      long[] batch = missingBatch[stripe];
      if (batch == null) {
        batch = new long[BATCH];
        missingBatch[stripe] = batch;
      }
      final int fill = missingFill[stripe];
      batch[fill] = value;
      if (fill + 1 == BATCH) {
        flushMissing(stripe, BATCH);
      } else {
        missingFill[stripe] = fill + 1;
      }
    }

    private void flushGroups(final int stripe, final int fill) {
      final long[] groups = batchGroups[stripe];
      final long[] values = batchValues[stripe];
      final Stripe target = stripes[stripe];
      int added = 0;
      synchronized (target) {
        long lastGroup = 0L;
        LongOpenHashSet lastSet = null;
        for (int i = 0; i < fill; i++) {
          final long group = groups[i];
          LongOpenHashSet set = lastSet;
          if (set == null || group != lastGroup) {
            set = target.groups.get(group);
            if (set == null) {
              set = new LongOpenHashSet(16);
              target.groups.put(group, set);
            }
            lastGroup = group;
            lastSet = set;
          }
          if (set.add(values[i])) {
            added++;
          }
        }
      }
      batchFill[stripe] = 0;
      account(added);
    }

    private void flushMissing(final int stripe, final int fill) {
      final long[] values = missingBatch[stripe];
      final Stripe target = stripes[stripe];
      int added = 0;
      synchronized (target) {
        LongOpenHashSet set = target.missing;
        if (set == null) {
          set = new LongOpenHashSet(16);
          target.missing = set;
        }
        for (int i = 0; i < fill; i++) {
          if (set.add(values[i])) {
            added++;
          }
        }
      }
      missingFill[stripe] = 0;
      account(added);
    }

    private void flushAll() {
      for (int stripe = 0; stripe < STRIPES; stripe++) {
        if (batchFill[stripe] != 0) {
          flushGroups(stripe, batchFill[stripe]);
        }
        if (missingFill[stripe] != 0) {
          flushMissing(stripe, missingFill[stripe]);
        }
      }
    }
  }

  private final Stripe[] stripes = new Stripe[STRIPES];
  private final Worker[] workers;
  /** {@code 64} = no pass filter; else the group-table partition shift of the pass split. */
  private volatile int passShift = 64;
  private volatile int passLo;
  private volatile int passHi;
  private final Sink discardSink = new Sink(null, 0L, false);
  private final long maxValues;
  private final LongAdder entries = new LongAdder();
  private volatile boolean exceeded;
  private Long2LongOpenHashMap sizes;
  private long missingSize;
  private boolean finished;

  /** An accumulator for {@code workerCount} parallel slots under the default ceiling. */
  public GroupDistinctAccumulator(final int workerCount) {
    this(workerCount, defaultMaxValues());
  }

  /**
   * An accumulator for {@code workerCount} parallel slots keeping at most {@code maxValues} pairs.
   */
  public GroupDistinctAccumulator(final int workerCount, final long maxValues) {
    if (workerCount <= 0) {
      throw new IllegalArgumentException("workerCount must be positive: " + workerCount);
    }
    if (maxValues < 0L) {
      throw new IllegalArgumentException("maxValues must not be negative: " + maxValues);
    }
    this.maxValues = maxValues;
    for (int i = 0; i < STRIPES; i++) {
      stripes[i] = new Stripe();
    }
    workers = new Worker[workerCount];
    for (int i = 0; i < workerCount; i++) {
      workers[i] = new Worker();
    }
  }

  private void account(final int added) {
    if (added == 0) {
      return;
    }
    entries.add(added);
    if (entries.sum() > maxValues) {
      exceeded = true;
    }
  }

  /**
   * Restrict the accumulator to the groups of the partitions {@code [lo, hi)} under {@code shift} —
   * the same split {@link NumericGroupAggTable#setPassRange} applies, so a hash-range pass keeps the
   * distinct state of exactly the groups it keeps. Before any worker adds.
   */
  public void setPassRange(final int shift, final int lo, final int hi) {
    if (shift < 0 || shift > 64 || lo < 0 || hi <= lo) {
      throw new IllegalArgumentException("bad pass range [" + lo + ", " + hi + ") shift " + shift);
    }
    this.passShift = shift;
    this.passLo = lo;
    this.passHi = hi;
  }

  /**
   * Drop every pair and every cached sink so the accumulator can serve the next hash-range pass — the
   * pass filter changes, so a sink cached for a group in the previous pass must not survive.
   * Coordinating thread only, after {@link #finish()} has been consumed.
   */
  public void reset() {
    for (final Stripe stripe : stripes) {
      synchronized (stripe) {
        stripe.groups.clear();
        stripe.missing = null;
      }
    }
    for (final Worker w : workers) {
      w.sinks.clear();
      Arrays.fill(w.batchFill, 0);
      Arrays.fill(w.missingFill, 0);
    }
    entries.reset();
    exceeded = false;
    sizes = null;
    missingSize = 0L;
    finished = false;
  }

  /** The single-threaded front for parallel slot {@code idx}. */
  public Worker worker(final int idx) {
    return requireNonNull(workers[idx], "worker");
  }

  /** Whether the ceiling was exceeded — the caller must decline; sizes are meaningless. */
  public boolean exceeded() {
    return exceeded;
  }

  /** Distinct pairs counted so far (flushed batches only). */
  public long entries() {
    return entries.sum();
  }

  /** The ceiling in effect for this accumulator. */
  public long maxValues() {
    return maxValues;
  }

  /**
   * Flush every worker's batches and publish the exact per-group sizes. Coordinating thread only,
   * after the parallel section has joined; idempotent.
   */
  public void finish() {
    if (finished) {
      return;
    }
    for (final Worker w : workers) {
      w.flushAll();
    }
    final Long2LongOpenHashMap out = new Long2LongOpenHashMap();
    out.defaultReturnValue(0L);
    long missing = 0L;
    for (final Stripe stripe : stripes) {
      synchronized (stripe) {
        for (final Long2ObjectMap.Entry<LongOpenHashSet> e : stripe.groups.long2ObjectEntrySet()) {
          out.addTo(e.getLongKey(), e.getValue().size());
        }
        if (stripe.missing != null) {
          missing += stripe.missing.size();
        }
      }
    }
    sizes = out;
    missingSize = missing;
    finished = true;
  }

  /** Exact distinct count per group (0 for an unseen group), after {@link #finish()}. */
  public Long2LongOpenHashMap groupSizes() {
    if (!finished) {
      throw new IllegalStateException("finish() has not run");
    }
    return sizes;
  }

  /** Exact distinct count of the missing-key rows, after {@link #finish()}. */
  public long missingSize() {
    if (!finished) {
      throw new IllegalStateException("finish() has not run");
    }
    return missingSize;
  }

  /** 16 group stripes × 4 value stripes; the same pair always lands on the same stripe. */
  static int stripeOf(final long group, final long value) {
    final int g = (int) ((group * MIX) >>> (64 - GROUP_STRIPE_BITS));
    final int v = (int) ((value * MIX) >>> (64 - VALUE_STRIPE_BITS));
    return (g << VALUE_STRIPE_BITS) | v;
  }

  /** Missing-key rows spread over every stripe by value alone. */
  static int missingStripeOf(final long value) {
    return (int) ((value * MIX) >>> (64 - GROUP_STRIPE_BITS - VALUE_STRIPE_BITS)) & STRIPE_MASK;
  }
}
