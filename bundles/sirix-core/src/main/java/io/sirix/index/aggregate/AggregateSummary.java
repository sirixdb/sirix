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

  public static AggregateSummary row(final long[] values, final long presentMask) {
    final AggregateSummary summary = new AggregateSummary(values.length);
    summary.count = 1;
    for (int lane = 0; lane < values.length; lane++) {
      if ((presentMask & (1L << lane)) != 0) {
        final long value = values[lane];
        summary.present[lane] = 1;
        summary.low[lane] = value;
        summary.high[lane] = value < 0 ? -1 : 0;
        summary.min[lane] = value;
        summary.max[lane] = value;
      }
    }
    return summary;
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
    return high[lane] == (low[lane] < 0 ? -1 : 0);
  }

  public long sumLongExact(final int lane) {
    if (!sumFitsLong(lane)) {
      throw new ArithmeticException("Aggregate sum exceeds a long");
    }
    return low[lane];
  }

  /** Fold a persisted node without allocating a temporary summary or its five lane arrays. */
  void addEncoded(final ByteBuffer source) {
    final long otherCount = source.getLong();
    if (otherCount < 0) {
      throw new IllegalStateException("Negative aggregate count");
    }
    count = Math.addExact(count, otherCount);
    for (int lane = 0; lane < present.length; lane++) {
      final long otherPresent = source.getLong();
      final long otherLow = source.getLong();
      final long otherHigh = source.getLong();
      final long otherMin = source.getLong();
      final long otherMax = source.getLong();
      if (otherPresent < 0 || otherPresent > otherCount || (otherPresent != 0 && otherMin > otherMax)) {
        throw new IllegalStateException("Invalid aggregate summary");
      }
      present[lane] = Math.addExact(present[lane], otherPresent);
      final long previous = low[lane];
      final long next = previous + otherLow;
      high[lane] += otherHigh + (Long.compareUnsigned(next, previous) < 0 ? 1 : 0);
      low[lane] = next;
      min[lane] = Math.min(min[lane], otherMin);
      max[lane] = Math.max(max[lane], otherMax);
    }
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
      final long carry = Long.compareUnsigned(next, previous) < 0 ? 1 : 0;
      high[lane] = high[lane] + other.high[lane] + carry;
      low[lane] = next;
      min[lane] = Math.min(min[lane], other.min[lane]);
      max[lane] = Math.max(max[lane], other.max[lane]);
    }
  }

  /** Apply a single row delta; extrema are supplied by the value-frequency tree afterwards. */
  void adjustRow(final long[] values, final long mask, final long sumMask, final int sign) {
    count = Math.addExact(count, sign);
    for (int lane = 0; lane < lanes(); lane++) {
      if ((mask & (1L << lane)) == 0) {
        continue;
      }
      present[lane] = Math.addExact(present[lane], sign);
      if ((sumMask & (1L << lane)) == 0) {
        continue;
      }
      final long value = values[lane];
      final long previous = low[lane];
      if (sign > 0) {
        low[lane] += value;
        high[lane] += (value < 0 ? -1 : 0) + (Long.compareUnsigned(low[lane], previous) < 0 ? 1 : 0);
      } else {
        low[lane] -= value;
        high[lane] -= (value < 0 ? -1 : 0) + (Long.compareUnsigned(previous, value) < 0 ? 1 : 0);
      }
    }
  }

  void extrema(final int lane, final long minimum, final long maximum) {
    min[lane] = present[lane] == 0 ? Long.MAX_VALUE : minimum;
    max[lane] = present[lane] == 0 ? Long.MIN_VALUE : maximum;
  }

  int encodedBytes() {
    return Long.BYTES * (1 + 5 * lanes());
  }

  void write(final ByteBuffer target) {
    target.putLong(count);
    for (int lane = 0; lane < present.length; lane++) {
      target.putLong(present[lane]).putLong(low[lane]).putLong(high[lane]).putLong(min[lane]).putLong(max[lane]);
    }
  }

  static AggregateSummary read(final ByteBuffer source, final int lanes) {
    final AggregateSummary summary = new AggregateSummary(lanes);
    summary.count = source.getLong();
    if (summary.count < 0) {
      throw new IllegalStateException("Negative aggregate count");
    }
    for (int lane = 0; lane < lanes; lane++) {
      summary.present[lane] = source.getLong();
      summary.low[lane] = source.getLong();
      summary.high[lane] = source.getLong();
      summary.min[lane] = source.getLong();
      summary.max[lane] = source.getLong();
      if (summary.present[lane] < 0 || summary.present[lane] > summary.count
          || (summary.present[lane] != 0 && summary.min[lane] > summary.max[lane])) {
        throw new IllegalStateException("Invalid aggregate summary");
      }
    }
    return summary;
  }
}
