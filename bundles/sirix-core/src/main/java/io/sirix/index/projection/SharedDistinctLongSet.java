package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * One distinct set of 64-bit values that several workers fill together: the values are split by the
 * top bits of their {@link DistinctLongSet#mix(long) mix} into partition sets, each guarded by its
 * own monitor, and every worker appends through a {@link #worker() buffered handle} that flushes a
 * partition's buffer into the partition set in one locked run.
 *
 * <p>
 * The 64-bit sibling of {@link SharedDistinctHash128Set}, for columns whose values ARE the keys — a
 * numeric column, or a GLOBAL string column's resource-wide ids. The alternative — one set per
 * worker, merged at the end — holds every value once per worker that saw it: 100M rows with 17.6M
 * distinct user ids over 20 workers would carry up to 80M values in flight against 17.6M in the
 * answer, and the merge would be the serial pass the workers were meant to remove. Here the
 * footprint is the answer's, plus one buffer page per (worker, partition); a drain of hundreds of
 * values into ONE partition table keeps that table's lines hot for the run, which a value-by-value
 * insert into the whole table never does.
 * </p>
 *
 * <p>
 * Every array is charged to the shared byte {@code budget}: partition sets as they grow, buffers as
 * a worker handle is created. A refusal throws
 * {@link DistinctHash128Set.ByteBudgetExceededException} from the worker that hit it, and the
 * operation declines as a whole. The count is read after every worker has {@link Worker#flush()
 * flushed} and the workers were joined.
 * </p>
 */
public final class SharedDistinctLongSet {
  /** Values a worker buffers per partition before a locked flush. */
  public static final int DEFAULT_BUFFER_KEYS = 512;

  private final DistinctLongSet[] partitions;
  private final int partitionBits;
  private final int bufferKeys;
  private final @Nullable AtomicLong budget;

  /**
   * A shared set of {@code partitions} partition sets, each sized for
   * {@code expectedKeysPerPartition} values before growing.
   *
   * @param partitions the partition count, a positive power of two
   * @param expectedKeysPerPartition values each partition set holds before its first growth
   * @param bufferKeys values a worker buffers per partition before flushing, at least one
   * @param budget the shared bytes remaining, or {@code null} for an unbounded set
   * @throws DistinctHash128Set.ByteBudgetExceededException when the budget refuses the partition sets
   */
  public SharedDistinctLongSet(final int partitions, final int expectedKeysPerPartition, final int bufferKeys,
      final @Nullable AtomicLong budget) {
    if (partitions <= 0 || Integer.bitCount(partitions) != 1) {
      throw new IllegalArgumentException("partitions must be a positive power of two: " + partitions);
    }
    if (bufferKeys <= 0) {
      throw new IllegalArgumentException("bufferKeys must be positive: " + bufferKeys);
    }
    this.partitions = new DistinctLongSet[partitions];
    for (int p = 0; p < partitions; p++) {
      this.partitions[p] = new DistinctLongSet(expectedKeysPerPartition, budget);
    }
    this.partitionBits = Integer.numberOfTrailingZeros(partitions);
    this.bufferKeys = bufferKeys;
    this.budget = budget;
  }

  /**
   * A buffered handle for one worker thread; the worker calls {@link Worker#flush()} when it is done.
   *
   * @throws DistinctHash128Set.ByteBudgetExceededException when the budget refuses the buffers
   */
  public Worker worker() {
    return new Worker();
  }

  /** The distinct values, exact once every worker flushed and was joined. */
  public long size() {
    long total = 0L;
    for (final DistinctLongSet partition : partitions) {
      synchronized (partition) {
        total += partition.size();
      }
    }
    return total;
  }

  /** Partition sets. */
  public int partitions() {
    return partitions.length;
  }

  /** Bytes the partition sets currently hold against the budget (buffers excluded). */
  public long chargedBytes() {
    long total = 0L;
    for (final DistinctLongSet partition : partitions) {
      synchronized (partition) {
        total += partition.chargedBytes();
      }
    }
    return total;
  }

  /**
   * One worker's buffers: {@code bufferKeys} values per partition, flushed into the partition set
   * when full.
   */
  public final class Worker implements DistinctLongSink {
    private final long[][] buffers;
    private final int[] fill;

    private Worker() {
      final int count = partitions.length;
      final long bytes = (long) count * bufferKeys * Long.BYTES;
      if (budget != null && budget.addAndGet(-bytes) < 0L) {
        budget.addAndGet(bytes);
        throw new DistinctHash128Set.ByteBudgetExceededException("distinct set refused " + bytes
            + " bytes of worker buffers: " + Math.max(0L, budget.get()) + " bytes of the shared budget remain");
      }
      buffers = new long[count][bufferKeys];
      fill = new int[count];
    }

    @Override
    public void put(final long value) {
      final int p = DistinctLongSet.partitionOf(DistinctLongSet.mix(value), partitionBits);
      final long[] buffer = buffers[p];
      int at = fill[p];
      buffer[at++] = value;
      if (at == buffer.length) {
        drain(p, buffer, at);
        at = 0;
      }
      fill[p] = at;
    }

    /**
     * Push every buffered value into its partition set; the worker's contribution is complete after
     * this.
     */
    public void flush() {
      for (int p = 0; p < buffers.length; p++) {
        final int at = fill[p];
        if (at > 0) {
          drain(p, buffers[p], at);
          fill[p] = 0;
        }
      }
    }

    private void drain(final int p, final long[] buffer, final int end) {
      final DistinctLongSet partition = partitions[p];
      synchronized (partition) {
        for (int i = 0; i < end; i++) {
          partition.add(buffer[i]);
        }
      }
    }
  }
}
