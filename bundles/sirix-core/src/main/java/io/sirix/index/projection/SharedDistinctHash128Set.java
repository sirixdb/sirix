package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * One distinct set of 128-bit keys that several workers fill together: the keys are split by the top
 * bits of their high half into partition sets, each guarded by its own monitor, and every worker
 * appends through a {@link #worker() buffered handle} that flushes a partition's buffer into the
 * partition set in one locked run.
 *
 * <p>
 * The alternative — one set per worker, merged at the end — holds every key once per worker that saw
 * it: a 100M-row dictionary column with 6M distinct values spread over 32 workers would carry up to
 * 24M keys in flight against 6M in the answer. Here the footprint is the answer's, plus one buffer
 * page per (worker, partition); the buffers amortise a monitor acquisition over hundreds of keys, so
 * the partition monitors see a few thousand acquisitions per worker instead of one per key.
 * </p>
 *
 * <p>
 * Every array is charged to the shared byte {@code budget}: partition sets as they grow, buffers as a
 * worker handle is created. A refusal throws {@link DistinctHash128Set.ByteBudgetExceededException}
 * from the worker that hit it, and the operation declines as a whole. The count is read after every
 * worker has {@link Worker#flush() flushed} and the workers were joined.
 * </p>
 */
public final class SharedDistinctHash128Set {
  /** Keys a worker buffers per partition before a locked flush. */
  public static final int DEFAULT_BUFFER_KEYS = 512;

  private final DistinctHash128Set[] partitions;
  private final int partitionBits;
  private final int bufferKeys;
  private final @Nullable AtomicLong budget;

  /**
   * A shared set of {@code partitions} partition sets, each sized for {@code expectedKeysPerPartition}
   * keys before growing.
   *
   * @param partitions the partition count, a positive power of two
   * @param expectedKeysPerPartition keys each partition set holds before its first growth
   * @param bufferKeys keys a worker buffers per partition before flushing, at least one
   * @param budget the shared bytes remaining, or {@code null} for an unbounded set
   * @throws DistinctHash128Set.ByteBudgetExceededException when the budget refuses the partition sets
   */
  public SharedDistinctHash128Set(final int partitions, final int expectedKeysPerPartition, final int bufferKeys,
      final @Nullable AtomicLong budget) {
    if (partitions <= 0 || Integer.bitCount(partitions) != 1) {
      throw new IllegalArgumentException("partitions must be a positive power of two: " + partitions);
    }
    if (bufferKeys <= 0) {
      throw new IllegalArgumentException("bufferKeys must be positive: " + bufferKeys);
    }
    this.partitions = new DistinctHash128Set[partitions];
    for (int p = 0; p < partitions; p++) {
      this.partitions[p] = new DistinctHash128Set(expectedKeysPerPartition, budget);
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

  /** The distinct keys, exact once every worker flushed and was joined. */
  public long size() {
    long total = 0L;
    for (final DistinctHash128Set partition : partitions) {
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

  /** One worker's buffers: {@code bufferKeys} key pairs per partition, flushed into the partition set when full. */
  public final class Worker implements DistinctHash128Sink {
    private final long[][] buffers;
    private final int[] fill;

    private Worker() {
      final int count = partitions.length;
      final long bytes = (long) count * bufferKeys * DistinctHash128Set.BYTES_PER_SLOT;
      if (budget != null && budget.addAndGet(-bytes) < 0L) {
        budget.addAndGet(bytes);
        throw new DistinctHash128Set.ByteBudgetExceededException("distinct set refused " + bytes
            + " bytes of worker buffers: " + Math.max(0L, budget.get()) + " bytes of the shared budget remain");
      }
      buffers = new long[count][bufferKeys << 1];
      fill = new int[count];
    }

    @Override
    public void put(final long lo, final long hi) {
      final int p = DistinctHash128Set.partitionOf(hi, partitionBits);
      final long[] buffer = buffers[p];
      int at = fill[p];
      buffer[at] = lo;
      buffer[at + 1] = hi;
      at += 2;
      if (at == buffer.length) {
        drain(p, buffer, at);
        at = 0;
      }
      fill[p] = at;
    }

    /** Push every buffered key into its partition set; the worker's contribution is complete after this. */
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
      final DistinctHash128Set partition = partitions[p];
      synchronized (partition) {
        for (int i = 0; i < end; i += 2) {
          partition.add(buffer[i], buffer[i + 1]);
        }
      }
    }
  }
}
