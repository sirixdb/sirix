package io.sirix.index.aggregate;

import java.math.BigInteger;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class AggregateSummaryTest {
  @Test
  void reusedSummaryPreservesWideSumsAndOwnsItsArrays() {
    final long[] sums = {5, 3, Long.MIN_VALUE, 0, 2, Long.MAX_VALUE, -1};
    final long[] minimum = {Long.MIN_VALUE, -7};
    final long[] maximum = {Long.MAX_VALUE, 11};
    final AggregateSummary summary = summary(sums, minimum, maximum);
    Arrays.fill(sums, 0);
    Arrays.fill(minimum, 0);
    Arrays.fill(maximum, 0);

    assertEquals(5, summary.count());
    assertEquals(3, summary.present(0));
    assertEquals(2, summary.present(1));
    assertEquals(BigInteger.ONE.shiftLeft(63), summary.sum(0));
    assertEquals(BigInteger.ONE.shiftLeft(63).negate().subtract(BigInteger.ONE), summary.sum(1));
    assertFalse(summary.sumFitsLong(0));
    assertThrows(ArithmeticException.class, () -> summary.sumLongExact(1));
    assertEquals(Long.MIN_VALUE, summary.min(0));
    assertEquals(Long.MAX_VALUE, summary.max(0));
    assertEquals(-7, summary.min(1));
    assertEquals(11, summary.max(1));
  }

  @Test
  void mergingReusedSummaryDoesNotModifyItsInputs() {
    final long[] sums = {1, 1, 7, 0};
    final long[] minimum = {7};
    final long[] maximum = {7};
    final AggregateSummary summary = summary(sums, minimum, maximum);
    summary.add(summary(new long[] {1, 1, -9, -1}, new long[] {-9}, new long[] {-9}));

    assertEquals(2, summary.count());
    assertEquals(2, summary.present(0));
    assertEquals(-2, summary.sumLongExact(0));
    assertEquals(-9, summary.min(0));
    assertEquals(7, summary.max(0));
    assertEquals(7, sums[2]);
    assertEquals(7, minimum[0]);
    assertEquals(7, maximum[0]);
  }

  @Test
  void tableCopiesRowsAndSupportsGrowthAndMergingWithoutEscapingState() {
    final AggregateSummary.Table table = new AggregateSummary.Table(1, 0);
    final AggregateSummary source = summary(new long[] {1, 1, 7, 0}, new long[] {7}, new long[] {7});
    for (int row = 0; row < 100; row++) {
      assertEquals(row, table.append(source));
    }
    source.add(summary(new long[] {1, 1, -9, -1}, new long[] {-9}, new long[] {-9}));
    table.add(50, source);
    final AggregateSummary destination = new AggregateSummary(1);
    table.read(50, destination);
    assertEquals(3, destination.count());
    assertEquals(3, destination.present(0));
    assertEquals(5, destination.sumLongExact(0));
    assertEquals(-9, destination.min(0));
    assertEquals(7, destination.max(0));
    destination.add(source);
    table.read(49, destination);
    assertEquals(1, destination.count());
    assertEquals(7, destination.sumLongExact(0));
    assertEquals(7, destination.min(0));
    table.read(50, destination);
    assertEquals(3, destination.count());
    assertEquals(5, destination.sumLongExact(0));
    assertEquals(100, table.size());
  }

  @Test
  void tableKeepsWideCarriesAndSignedExtremaExact() {
    final AggregateSummary.Table table = new AggregateSummary.Table(2, 1);
    table.append(summary(new long[] {1, 1, Long.MAX_VALUE, 0, 1, Long.MIN_VALUE, -1},
        new long[] {Long.MAX_VALUE, Long.MIN_VALUE}, new long[] {Long.MAX_VALUE, Long.MIN_VALUE}));
    table.add(0, summary(new long[] {1, 1, 1, 0, 1, -1, -1}, new long[] {1, -1}, new long[] {1, -1}));
    final AggregateSummary result = new AggregateSummary(2);
    table.read(0, result);
    assertEquals(BigInteger.ONE.shiftLeft(63), result.sum(0));
    assertEquals(BigInteger.ONE.shiftLeft(63).negate().subtract(BigInteger.ONE), result.sum(1));
    assertEquals(1, result.min(0));
    assertEquals(Long.MAX_VALUE, result.max(0));
    assertEquals(Long.MIN_VALUE, result.min(1));
    assertEquals(-1, result.max(1));
  }

  @Test
  void tableValidatesRowsLanesCapacityAndCountOverflow() {
    assertThrows(IllegalArgumentException.class, () -> new AggregateSummary.Table(17, 0));
    assertThrows(IllegalArgumentException.class, () -> new AggregateSummary.Table(1, -1));
    assertThrows(IllegalArgumentException.class, () -> new AggregateSummary.Table(16, Integer.MAX_VALUE));
    final AggregateSummary.Table table = new AggregateSummary.Table(0, 0);
    table.append(summary(new long[] {Long.MAX_VALUE}, new long[0], new long[0]));
    assertThrows(ArithmeticException.class, () -> table.add(0, summary(new long[] {1}, new long[0], new long[0])));
    assertThrows(IndexOutOfBoundsException.class, () -> table.read(1, new AggregateSummary(0)));
    assertThrows(IllegalArgumentException.class, () -> table.append(new AggregateSummary(1)));
    assertThrows(IllegalArgumentException.class, () -> table.read(0, new AggregateSummary(1)));
    assertThrows(IllegalArgumentException.class, () -> table.add(0, new AggregateSummary(1)));
    final AggregateSummary.Table wide = new AggregateSummary.Table(16, 0);
    wide.append(new AggregateSummary(16));
    final AggregateSummary destination = new AggregateSummary(16);
    wide.read(0, destination);
    assertEquals(Long.MAX_VALUE, destination.min(15));
    assertEquals(Long.MIN_VALUE, destination.max(15));
  }

  private static AggregateSummary summary(final long[] sums, final long[] minimum, final long[] maximum) {
    final AggregateSummary result = new AggregateSummary(minimum.length);
    result.setSums(sums);
    for (int lane = 0; lane < minimum.length; lane++) {
      result.setExtrema(lane, minimum[lane], maximum[lane]);
    }
    return result;
  }
}
