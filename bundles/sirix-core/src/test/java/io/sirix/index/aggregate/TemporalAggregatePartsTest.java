package io.sirix.index.aggregate;

import io.sirix.index.aggregate.TemporalAggregateStorage.GroupHead;
import io.sirix.index.changelog.TemporalAggregateCatchUpBudget;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

final class TemporalAggregatePartsTest {
  private static final TemporalAggregateSpec SPEC =
      new TemporalAggregateSpec(0, List.of(0), List.of(new TemporalAggregateSpec.Lane(1)));

  private static final class Store implements TemporalAggregateParts.Store {
    private final Map<Long, byte[]> parts = new HashMap<>();
    private long reads;

    @Override
    public byte[] read(final long part, final int chunk) {
      reads++;
      final byte[] bytes = Objects.requireNonNull(parts.get(part));
      final int from = chunk * TemporalAggregateParts.CHUNK_BYTES;
      return Arrays.copyOfRange(bytes, from, Math.min(bytes.length, from + TemporalAggregateParts.CHUNK_BYTES));
    }
  }

  private record Row(long from, long to, long value, boolean present) {
    private void add(final TemporalAggregateParts.Builder builder, final int sign) {
      builder.add(from, to, new long[] {value}, present
          ? 1
          : 0, sign);
    }
  }

  @Test
  void signedPartsAndCheckpointsMatchHistoricalScanOracle() {
    final TemporalAggregateParts index = new TemporalAggregateParts(SPEC, 8);
    final Store store = new Store();
    final List<Long> parts = new ArrayList<>();
    final Map<Long, Row> rows = new HashMap<>();
    final Random random = new Random(417221);
    for (long revision = 1; revision <= 60; revision++) {
      final long[] history = ids(parts);
      final Map<Long, Row> prior = new HashMap<>(rows);
      final TemporalAggregateParts.Builder builder = index.builder();
      for (long id = 0; id < 100; id++) {
        if (random.nextInt(5) != 0) {
          continue;
        }
        final Row old = rows.remove(id);
        if (old != null) {
          old.add(builder, -1);
        }
        if (random.nextInt(4) != 0) {
          final long from = random.nextInt(256);
          final Row row =
              new Row(from, from + 1 + random.nextInt((int) (256 - from)), random.nextLong(), random.nextBoolean());
          rows.put(id, row);
          row.add(builder, 1);
        }
      }
      final boolean checkpoint = parts.size() == TemporalAggregateParts.FAN_IN;
      if (checkpoint) {
        for (final long part : parts) {
          builder.merge(store, part);
        }
        parts.clear();
      }
      store.parts.put(revision, builder.encode(checkpoint || revision == 1));
      parts.add(revision);
      verify(index, store, ids(parts), rows);
      verify(index, store, history, prior);
    }
  }

  @Test
  void unencodedTailMatchesEveryStoredFrontierAndSnapshotIsImmutable() {
    final TemporalAggregateParts index = new TemporalAggregateParts(SPEC, 8);
    final Store store = new Store();
    final List<Map<Long, Row>> history = new ArrayList<>();
    final Map<Long, Row> rows = new HashMap<>();
    final Random random = new Random(712391);
    for (int revision = 0; revision <= 18; revision++) {
      for (long id = 0; id < 24; id++) {
        if (random.nextInt(3) == 0) {
          rows.remove(id);
        } else {
          final long from = random.nextInt(128);
          rows.put(id, new Row(from, from + 1 + random.nextInt(128), random.nextLong(), random.nextBoolean()));
        }
      }
      history.add(Map.copyOf(rows));
    }
    for (int maintained = 0; maintained < history.size(); maintained++) {
      final TemporalAggregateParts.Builder checkpoint = index.builder();
      for (final Row row : history.get(maintained).values()) {
        row.add(checkpoint, 1);
      }
      store.parts.put(1L, checkpoint.encode(true));
      final TemporalAggregateParts.Builder tail = index.builder();
      for (int revision = maintained; revision < history.size(); revision++) {
        if (revision > maintained) {
          for (final Row row : history.get(revision - 1).values()) {
            row.add(tail, -1);
          }
          for (final Row row : history.get(revision).values()) {
            row.add(tail, 1);
          }
        }
        final TemporalAggregateParts.Delta snapshot = tail.snapshot();
        verify(index, store, new long[] {1}, history.get(revision), snapshot);
      }
      final TemporalAggregateParts.Delta frozen = tail.snapshot();
      tail.add(0, 256, new long[] {Long.MAX_VALUE}, 1, 1);
      verify(index, store, new long[] {1}, history.getLast(), frozen);
    }
  }

  @Test
  void compactSummariesMatchEveryStoredFrontierAndKeepFrozenTailsImmutable() {
    final TemporalAggregateParts index = new TemporalAggregateParts(SPEC, 8);
    final Store store = new Store();
    final List<Map<Long, Row>> history = new ArrayList<>();
    final Map<Long, Row> rows = new HashMap<>();
    final Random random = new Random(712391);
    for (int revision = 0; revision <= 18; revision++) {
      for (long id = 0; id < 24; id++) {
        if (random.nextInt(3) == 0) {
          rows.remove(id);
        } else {
          final long from = random.nextInt(128);
          rows.put(id, new Row(from, from + 1 + random.nextInt(128), random.nextLong(), random.nextBoolean()));
        }
      }
      history.add(Map.copyOf(rows));
    }
    for (int maintained = 0; maintained < history.size(); maintained++) {
      final TemporalAggregateParts.Builder checkpoint = index.builder();
      for (final Row row : history.get(maintained).values()) {
        row.add(checkpoint, 1);
      }
      store.parts.put(1L, checkpoint.encode(true));
      final TemporalAggregateParts.SummaryBuilder tail = index.summaryBuilder();
      final TemporalAggregateCatchUpBudget.Budget budget = new TemporalAggregateCatchUpBudget.Budget();
      for (int revision = maintained; revision < history.size(); revision++) {
        if (revision > maintained) {
          final TemporalAggregateParts.SummaryBuilder publication = index.summaryBuilder();
          for (final Row row : history.get(revision - 1).values()) {
            publication.add(row.from(), row.to(), new long[] {row.value()}, row.present()
                ? 1
                : 0, -1);
          }
          for (final Row row : history.get(revision).values()) {
            publication.add(row.from(), row.to(), new long[] {row.value()}, row.present()
                ? 1
                : 0, 1);
          }
          final byte[] operand = publication.encode(4096);
          assertNotNull(operand);
          if (operand.length != 0) {
            tail.merge(operand, budget);
          }
        }
        final TemporalAggregateParts.Delta snapshot = tail.snapshot();
        verify(index, store, new long[] {1}, history.get(revision), snapshot);
      }
      final TemporalAggregateParts.Delta frozen = tail.snapshot();
      tail.add(0, 256, new long[] {Long.MAX_VALUE}, 1, 1);
      verify(index, store, new long[] {1}, history.getLast(), frozen);
    }
  }

  @Test
  void repeatedChangesCollapseAndExtremaRemainDeletionSafe() {
    final TemporalAggregateParts index = new TemporalAggregateParts(SPEC, 8);
    final TemporalAggregateParts.SummaryBuilder publication = index.summaryBuilder();
    final Store store = new Store();
    final TemporalAggregateParts.Builder base = index.builder();
    for (int row = 0; row < 1250; row++) {
      base.add(0, 256, new long[] {Long.MIN_VALUE}, 1, 1);
      publication.add(0, 256, new long[] {Long.MIN_VALUE}, 1, -1);
      publication.add(0, 256, new long[] {Long.MAX_VALUE}, 1, 1);
    }
    final byte[] operand = publication.encode(4096);
    assertNotNull(operand);
    assertTrue(operand.length < 128, "Repeated values store multiplicities, not row copies");
    final TemporalAggregateParts.SummaryBuilder tail = index.summaryBuilder();
    tail.merge(operand, new TemporalAggregateCatchUpBudget.Budget());
    store.parts.put(1L, base.encode(true));
    final AggregateSummary answer = index.point(store, new long[] {1}, 17, tail.snapshot());
    assertEquals(1250, answer.count());
    assertEquals(Long.MAX_VALUE, answer.min(0));
    assertEquals(Long.MAX_VALUE, answer.max(0));
    assertEquals(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(1250)), answer.sum(0));
    final TemporalAggregateParts.SummaryBuilder cancelled = index.summaryBuilder();
    cancelled.add(0, 256, new long[] {3}, 1, -1);
    cancelled.add(0, 256, new long[] {3}, 1, 1);
    assertEquals(0, Objects.requireNonNull(cancelled.encode(4096)).length);
  }

  @Test
  void summaryBoundsRejectOversizedOperandsAndMalformedEncodings() {
    final TemporalAggregateParts index = new TemporalAggregateParts(SPEC, 8);
    final TemporalAggregateParts.SummaryBuilder builder = index.summaryBuilder();
    for (int value = 0; value < 1000; value++) {
      builder.add(0, 256, new long[] {value}, 1, 1);
    }
    assertNull(builder.encode(4096));
    assertThrows(TemporalAggregateCatchUpBudget.LimitExceeded.class,
        () -> index.summaryBuilder().merge(new byte[4097], new TemporalAggregateCatchUpBudget.Budget()));
    assertThrows(IllegalStateException.class,
        () -> index.summaryBuilder().merge(new byte[] {(byte) 0x80, 0}, new TemporalAggregateCatchUpBudget.Budget()));
    assertThrows(IllegalStateException.class,
        () -> index.summaryBuilder().merge(new byte[] {0, 0, 0}, new TemporalAggregateCatchUpBudget.Budget()));
    final TemporalAggregateCatchUpBudget.Budget budget = new TemporalAggregateCatchUpBudget.Budget();
    budget.summary(16384, 0, 0);
    assertThrows(TemporalAggregateCatchUpBudget.LimitExceeded.class, () -> budget.summary(1, 0, 0));
    final TemporalAggregateCatchUpBudget.Budget cells = new TemporalAggregateCatchUpBudget.Budget();
    assertThrows(TemporalAggregateCatchUpBudget.LimitExceeded.class, () -> cells.summary(6000, 0, 16));
  }

  @Test
  void longExtremumDeletionRunsUseWeightedSelectionAndWideSumsStayExact() {
    final TemporalAggregateParts index = new TemporalAggregateParts(SPEC, 48);
    final Store store = new Store();
    final TemporalAggregateParts.Builder initial = index.builder();
    for (int row = 0; row < 4096; row++) {
      initial.add(1024, 2048, new long[] {row}, 1, 1);
    }
    initial.add(1024, 2048, new long[] {Long.MIN_VALUE}, 1, 1);
    initial.add(1024, 2048, new long[] {Long.MAX_VALUE}, 1, 1);
    store.parts.put(1L, initial.encode(true));
    final TemporalAggregateParts.Builder deletion = index.builder();
    for (int row = 0; row < 1024; row++) {
      deletion.add(1024, 2048, new long[] {row}, 1, -1);
      deletion.add(1024, 2048, new long[] {4095 - row}, 1, -1);
    }
    deletion.add(1024, 2048, new long[] {Long.MIN_VALUE}, 1, -1);
    deletion.add(1024, 2048, new long[] {Long.MAX_VALUE}, 1, -1);
    store.parts.put(2L, deletion.encode(false));
    final AggregateSummary summary = index.point(store, new long[] {1, 2}, 1024);
    assertEquals(2048, summary.count());
    assertEquals(1024, summary.min(0));
    assertEquals(3071, summary.max(0));
    assertEquals(BigInteger.valueOf((1024L + 3071) * 2048 / 2), summary.sum(0));
    assertEquals(0, index.point(store, new long[] {1, 2}, 2048).count());
    final TemporalAggregateParts.Builder folded = index.builder();
    folded.merge(store, 1);
    folded.merge(store, 2);
    store.parts.put(3L, folded.encode(true));
    assertEquals(summary.sum(0), index.point(store, new long[] {3}, 1500).sum(0));
    assertEquals(1024, index.point(store, new long[] {3}, 1500).min(0));
  }

  @Test
  void smallPublicationReadsOnlyThreePartChunksBeside4096UntouchedRows() {
    final TemporalAggregateParts index = new TemporalAggregateParts(SPEC, 49);
    final Store store = new Store();
    final TemporalAggregateParts.Builder initial = index.builder();
    for (int row = 0; row < 4096; row++) {
      initial.add(1, 2, new long[] {row}, 1, 1);
    }
    store.parts.put(1L, initial.encode(true));
    final TemporalAggregateParts.Builder changed = index.builder();
    for (int row = 0; row < 20; row++) {
      changed.add(1, 2, new long[] {row}, 1, -1);
      changed.add(1, 2, new long[] {row + 7}, 1, 1);
    }
    store.parts.put(2L, changed.encode(false));
    store.reads = 0;
    final AggregateSummary result = index.point(store, new long[] {1, 2}, 1);
    assertEquals(4096, result.count());
    assertEquals(7, result.min(0));
    assertEquals(4095, result.max(0));
    assertTrue(store.reads <= 3, "A small publication must retain the existing point-read budget: " + store.reads);
    assertTrue(store.parts.get(2L).length <= TemporalAggregateParts.CHUNK_BYTES);
  }

  @Test
  void declaredLanesDeduplicateCanonicalExtremaAndCancelSwaps() {
    final TemporalAggregateSpec spec = new TemporalAggregateSpec(0, List.of(0),
        List.of(new TemporalAggregateSpec.Lane(1, -1, TemporalAggregateSpec.SUM),
            new TemporalAggregateSpec.Lane(2, -1, TemporalAggregateSpec.SUM),
            new TemporalAggregateSpec.Lane(3, -1, TemporalAggregateSpec.MIN | TemporalAggregateSpec.MAX)));
    final TemporalAggregateParts index = new TemporalAggregateParts(spec, 49);
    final TemporalAggregateParts.Builder build = index.builder();
    for (int row = 0; row < 25_000; row++) {
      build.add(8193, 12031, new long[] {row, row * 9L, row % 16}, 7, 1);
    }
    final byte[] bytes = build.encode(true);
    assertTrue(bytes.length <= 8192, "Only distinct extrema and declared sums belong in a compact part");
    final Store store = new Store();
    store.parts.put(1L, bytes);
    final AggregateSummary summary = index.point(store, new long[] {1}, 8193);
    assertEquals(BigInteger.valueOf(25_000L * 24_999 / 2), summary.sum(0));
    assertEquals(0, summary.min(2));
    assertEquals(15, summary.max(2));
    assertEquals(0, index.point(store, new long[] {1}, 12031).count());
    final TemporalAggregateParts.Builder swap = index.builder();
    swap.add(8193, 12031, new long[] {1, 9, 1}, 7, -1);
    swap.add(8193, 12031, new long[] {2, 18, 2}, 7, 1);
    swap.add(8193, 12031, new long[] {2, 18, 2}, 7, -1);
    swap.add(8193, 12031, new long[] {1, 9, 1}, 7, 1);
    assertTrue(swap.isEmpty(), "Cancelling a batch must need no appended part");
  }

  @Test
  void extremaOnlyReplacementKeepsBoundedRawCandidatesEvenWhenEndpointCountsCancel() {
    final TemporalAggregateSpec spec = new TemporalAggregateSpec(0, List.of(0),
        List.of(new TemporalAggregateSpec.Lane(1, -1, TemporalAggregateSpec.MIN | TemporalAggregateSpec.MAX)));
    final TemporalAggregateParts index = new TemporalAggregateParts(spec, 49);
    final Store store = new Store();
    final TemporalAggregateParts.Builder initial = index.builder();
    initial.add(8193, 12031, new long[] {5}, 1, 1);
    store.parts.put(1L, initial.encode(true));
    final TemporalAggregateParts.Builder delta = index.builder();
    delta.add(8193, 12031, new long[] {5}, 1, -1);
    delta.add(8193, 12031, new long[] {9}, 1, 1);
    assertFalse(delta.isEmpty());
    final byte[] bytes = delta.encode(false);
    assertTrue(bytes.length <= 128, "Two interval candidates must not carry a time-tree directory");
    store.parts.put(2L, bytes);
    assertEquals(9, index.point(store, new long[] {1, 2}, 9000).min(0));
    assertEquals(0, index.point(store, new long[] {1, 2}, 12031).count());
    final TemporalAggregateParts.Builder folded = index.builder();
    folded.merge(store, 1);
    folded.merge(store, 2);
    store.parts.put(3L, folded.encode(true));
    assertEquals(9, index.point(store, new long[] {3}, 9000).max(0));
  }

  @Test
  void rawCandidatesKeepIndependentLaneOrderingPresenceAndIntervalBounds() {
    final TemporalAggregateSpec spec = new TemporalAggregateSpec(0, List.of(0),
        List.of(new TemporalAggregateSpec.Lane(1), new TemporalAggregateSpec.Lane(2)));
    final TemporalAggregateParts index = new TemporalAggregateParts(spec, 8);
    final Store store = new Store();
    final TemporalAggregateParts.Builder initial = index.builder();
    initial.add(2, 9, new long[] {Long.MIN_VALUE, 100}, 3, 1);
    initial.add(2, 9, new long[] {5, -9}, 3, 1);
    initial.add(2, 9, new long[] {7, 0}, 1, 1);
    store.parts.put(1L, initial.encode(true));
    final TemporalAggregateParts.Builder delta = index.builder();
    delta.add(2, 9, new long[] {Long.MIN_VALUE, 100}, 3, -1);
    delta.add(2, 9, new long[] {Long.MAX_VALUE, 0}, 1, 1);
    delta.add(2, 9, new long[] {5, -9}, 3, -1);
    delta.add(6, 9, new long[] {5, -9}, 3, 1);
    store.parts.put(2L, delta.encode(false));
    final AggregateSummary early = index.point(store, new long[] {1, 2}, 2);
    assertEquals(2, early.count());
    assertEquals(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(7)), early.sum(0));
    assertEquals(7, early.min(0));
    assertEquals(Long.MAX_VALUE, early.max(0));
    assertEquals(0, early.present(1));
    final AggregateSummary later = index.point(store, new long[] {1, 2}, 6);
    assertEquals(3, later.count());
    assertEquals(5, later.min(0));
    assertEquals(1, later.present(1));
    assertEquals(-9, later.min(1));
    assertEquals(-9, later.max(1));
    assertEquals(BigInteger.valueOf(-9), later.sum(1));
    assertEquals(0, index.point(store, new long[] {1, 2}, 9).count());
  }

  @Test
  void tieredGroupHeadsRoundTripAndRejectUnfoldedTiers() {
    final long[] ids = new long[TemporalAggregateParts.MAX_PARTS];
    final byte[] tiers = new byte[ids.length];
    ids[0] = 1;
    tiers[0] = (byte) TemporalAggregateParts.MAX_TIERS;
    for (int index = 1; index < ids.length; index++) {
      ids[index] = index + 1L;
      tiers[index] = (byte) ((index - 1) / (TemporalAggregateParts.FAN_IN - 1));
    }
    final GroupHead head = new GroupHead(10, TemporalAggregateParts.MAX_TIERS, ids, tiers);
    final byte[] bytes = head.encode();
    assertTrue(bytes.length <= TemporalAggregateParts.CHUNK_BYTES);
    final GroupHead decoded = GroupHead.decode(bytes);
    assertEquals(head.rows(), decoded.rows());
    assertEquals(head.checkpointTier(), decoded.checkpointTier());
    assertArrayEquals(ids, decoded.parts());
    assertArrayEquals(tiers, decoded.tiers());
    ids[0] = -1;
    tiers[0] = -1;
    assertEquals(1, head.parts()[0]);
    assertEquals(TemporalAggregateParts.MAX_TIERS, head.tiers()[0]);
    final byte[] invalid = decoded.tiers();
    invalid[TemporalAggregateParts.FAN_IN] = 0;
    assertThrows(IllegalArgumentException.class,
        () -> new GroupHead(10, TemporalAggregateParts.MAX_TIERS, decoded.parts(), invalid));
    assertThrows(IllegalArgumentException.class, () -> new GroupHead(1, 1, new long[] {1, 2}, new byte[] {1, 1}));
  }

  private static long[] ids(final List<Long> parts) {
    final long[] result = new long[parts.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = parts.get(i);
    }
    return result;
  }

  private static void verify(final TemporalAggregateParts index, final Store store, final long[] parts,
      final Map<Long, Row> rows) {
    verify(index, store, parts, rows, null);
  }

  private static void verify(final TemporalAggregateParts index, final Store store, final long[] parts,
      final Map<Long, Row> rows, final TemporalAggregateParts.Delta tail) {
    for (int point = 0; point < 256; point++) {
      long count = 0;
      long present = 0;
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      BigInteger sum = BigInteger.ZERO;
      for (final Row row : rows.values()) {
        if (row.from <= point && point < row.to) {
          count++;
          if (row.present) {
            present++;
            min = Math.min(min, row.value);
            max = Math.max(max, row.value);
            sum = sum.add(BigInteger.valueOf(row.value));
          }
        }
      }
      final AggregateSummary actual = index.point(store, parts, point, tail);
      assertEquals(count, actual.count());
      assertEquals(present, actual.present(0));
      assertEquals(sum, actual.sum(0));
      assertEquals(min, actual.min(0));
      assertEquals(max, actual.max(0));
    }
  }
}
