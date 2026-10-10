package io.sirix.index.aggregate;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Exact integer summaries. Signed 128-bit sums cover every long lane over a long record count. */
public final class AggregateSummary {
  private long count;
  private final long[] present;
  private final long[] low;
  private final long[] high;
  private final long[] min;
  private final long[] max;

  public AggregateSummary(final int lanes) {
    if (lanes < 0 || lanes > 16) {
      throw new IllegalArgumentException("Invalid aggregate lane count: " + lanes);
    }
    present = new long[lanes];
    low = new long[lanes];
    high = new long[lanes];
    min = new long[lanes];
    max = new long[lanes];
    Arrays.fill(min, Long.MAX_VALUE);
    Arrays.fill(max, Long.MIN_VALUE);
  }

  static AggregateSummary fromPacked(final long[] sums, final long[] minimum, final long[] maximum) {
    if (sums.length != 1 + 3 * minimum.length || maximum.length != minimum.length) {
      throw new IllegalArgumentException("Invalid aggregate summary dimensions");
    }
    final AggregateSummary result = new AggregateSummary(minimum.length);
    result.count = sums[0];
    for (int lane = 0; lane < minimum.length; lane++) {
      final int offset = 1 + 3 * lane;
      result.present[lane] = sums[offset];
      result.low[lane] = sums[offset + 1];
      result.high[lane] = sums[offset + 2];
      result.min[lane] = minimum[lane];
      result.max[lane] = maximum[lane];
    }
    return result;
  }

  public int lanes() {
    return present.length;
  }

  public long count() {
    return count;
  }

  public long present(final int lane) {
    return present[lane];
  }

  public long min(final int lane) {
    return min[lane];
  }

  public long max(final int lane) {
    return max[lane];
  }

  public BigInteger sum(final int lane) {
    final byte[] bytes = new byte[16];
    ByteBuffer.wrap(bytes).putLong(high[lane]).putLong(low[lane]);
    return new BigInteger(bytes);
  }

  public boolean sumFitsLong(final int lane) {
    return high[lane] == (low[lane] < 0
        ? -1
        : 0);
  }

  public long sumLongExact(final int lane) {
    if (!sumFitsLong(lane)) {
      throw new ArithmeticException("Aggregate sum exceeds a long");
    }
    return low[lane];
  }

  /** Merge into this caller-owned accumulator without allocation. */
  public void add(final AggregateSummary other) {
    if (other.lanes() != lanes()) {
      throw new IllegalArgumentException("Incompatible aggregate lane counts");
    }
    count = Math.addExact(count, other.count);
    for (int lane = 0; lane < present.length; lane++) {
      present[lane] = Math.addExact(present[lane], other.present[lane]);
      final long previous = low[lane];
      final long next = previous + other.low[lane];
      final long carry = Long.compareUnsigned(next, previous) < 0
          ? 1
          : 0;
      high[lane] = high[lane] + other.high[lane] + carry;
      low[lane] = next;
      min[lane] = Math.min(min[lane], other.min[lane]);
      max[lane] = Math.max(max[lane], other.max[lane]);
    }
  }

}
