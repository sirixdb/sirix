package io.sirix.index.aggregate;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

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

  void setSums(final long[] sums) {
    if (sums.length != 1 + 3 * lanes()) {
      throw new IllegalArgumentException("Invalid aggregate summary dimensions");
    }
    count = sums[0];
    for (int lane = 0; lane < lanes(); lane++) {
      final int offset = 1 + 3 * lane;
      present[lane] = sums[offset];
      low[lane] = sums[offset + 1];
      high[lane] = sums[offset + 2];
    }
  }

  void setExtrema(final int lane, final long minimum, final long maximum) {
    min[lane] = minimum;
    max[lane] = maximum;
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

  /** Scan-owned packed rows share a backing array instead of per-group summaries and lane arrays. */
  public static final class Table {
    private final int lanes;
    private final int stride;
    private long[] data;
    private int size;

    public Table(final int lanes, final int initialCapacity) {
      if (lanes < 0 || lanes > 16 || initialCapacity < 0) {
        throw new IllegalArgumentException("Invalid aggregate table dimensions");
      }
      this.lanes = lanes;
      stride = 1 + 5 * lanes;
      if (initialCapacity > Integer.MAX_VALUE / stride) {
        throw new IllegalArgumentException("Aggregate table capacity is too large");
      }
      data = new long[initialCapacity * stride];
    }

    public int size() {
      return size;
    }

    public int lanes() {
      return lanes;
    }

    public int append(final AggregateSummary summary) {
      checkLanes(summary);
      if (size == Integer.MAX_VALUE / stride) {
        throw new IllegalStateException("Too many aggregate groups");
      }
      final int required = (size + 1) * stride;
      if (required > data.length) {
        final int capacity = (int) Math.min(Integer.MAX_VALUE / stride,
            Math.max((long) size + 1, Math.max(1L, (long) data.length / stride * 2)));
        data = Arrays.copyOf(data, capacity * stride);
      }
      final int row = size++;
      int offset = row * stride;
      data[offset++] = summary.count;
      for (int lane = 0; lane < lanes; lane++) {
        data[offset++] = summary.present[lane];
        data[offset++] = summary.low[lane];
        data[offset++] = summary.high[lane];
        data[offset++] = summary.min[lane];
        data[offset++] = summary.max[lane];
      }
      return row;
    }

    /** Merge a transient summary before its caller reuses it for the next dictionary group. */
    public void add(final int row, final AggregateSummary summary) {
      checkLanes(summary);
      int offset = Objects.checkIndex(row, size) * stride;
      data[offset] = Math.addExact(data[offset], summary.count);
      offset++;
      for (int lane = 0; lane < lanes; lane++, offset += 5) {
        data[offset] = Math.addExact(data[offset], summary.present[lane]);
        final long previous = data[offset + 1];
        final long next = previous + summary.low[lane];
        final long carry = Long.compareUnsigned(next, previous) < 0
            ? 1
            : 0;
        data[offset + 1] = next;
        data[offset + 2] += summary.high[lane] + carry;
        data[offset + 3] = Math.min(data[offset + 3], summary.min[lane]);
        data[offset + 4] = Math.max(data[offset + 4], summary.max[lane]);
      }
    }

    /** Copy into caller-owned scratch; neither the row nor its backing array escapes. */
    public void read(final int row, final AggregateSummary destination) {
      checkLanes(destination);
      int offset = Objects.checkIndex(row, size) * stride;
      destination.count = data[offset++];
      for (int lane = 0; lane < lanes; lane++) {
        destination.present[lane] = data[offset++];
        destination.low[lane] = data[offset++];
        destination.high[lane] = data[offset++];
        destination.min[lane] = data[offset++];
        destination.max[lane] = data[offset++];
      }
    }

    private void checkLanes(final AggregateSummary summary) {
      if (summary.lanes() != lanes) {
        throw new IllegalArgumentException("Incompatible aggregate lane counts");
      }
    }
  }
}
