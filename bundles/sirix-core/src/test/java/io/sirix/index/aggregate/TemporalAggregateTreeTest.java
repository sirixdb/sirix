package io.sirix.index.aggregate;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TemporalAggregateTreeTest {
  private record Key(long segment, long prefix, int bit) {
  }

  private static final class MemoryStore implements TemporalAggregateTree.Store {
    private final Map<Key, byte[]> slots;
    private final Set<Key> written = new HashSet<>();
    private long reads;

    private MemoryStore() {
      slots = new HashMap<>();
    }

    private MemoryStore(final MemoryStore other) {
      slots = new HashMap<>(other.slots);
    }

    @Override
    public byte[] read(final long segment, final long prefix, final int bit) {
      reads++;
      return slots.get(new Key(segment, prefix, bit));
    }

    @Override
    public void write(final long segment, final long prefix, final int bit, final byte[] value) {
      final Key key = new Key(segment, prefix, bit);
      assertTrue(written.add(key), "A batch must write each persistent node only once: " + key);
      slots.put(key, value);
    }

    private void reset() {
      written.clear();
      reads = 0;
    }
  }

  private record Row(long from, long to, long value, boolean present) {
    private AggregateSummary summary() {
      return AggregateSummary.row(new long[] {value}, present ? 1 : 0);
    }
  }

  @Test
  void randomizedBatchesMatchScanAndPreserveSnapshots() {
    final MemoryStore store = new MemoryStore();
    final TemporalAggregateTree tree = new TemporalAggregateTree(store, 8, 1);
    final Map<Long, Row> rows = new HashMap<>();
    final Random random = new Random(991781L);
    for (int commit = 0; commit < 80; commit++) {
      final MemoryStore snapshot = new MemoryStore(store);
      final Map<Long, Row> oldRows = new HashMap<>(rows);
      store.reset();
      final TemporalAggregateTree.Batch batch = tree.batch();
      for (long id = 0; id < 150; id++) {
        if (random.nextInt(5) != 0) {
          continue;
        }
        final Row old = rows.remove(id);
        if (old != null) {
          batch.replace(id, old.from(), old.to(), null);
        }
        if (random.nextInt(4) != 0) {
          final int start = random.nextInt(256);
          final Row next = new Row(start, start + 1 + random.nextInt(256 - start), random.nextLong(),
              random.nextBoolean());
          rows.put(id, next);
          batch.replace(id, next.from(), next.to(), next.summary());
        }
      }
      batch.apply();
      verify(tree, rows, 256);
      verify(new TemporalAggregateTree(snapshot, 8, 1), oldRows, 256);
    }
  }

  @Test
  void removesExtremaAndHonorsBothBoundaries() {
    final MemoryStore store = new MemoryStore();
    final TemporalAggregateTree tree = new TemporalAggregateTree(store, 48, 1);
    final long start = 1_700_000_000_001L;
    final long end = start + 86_400_000;
    final TemporalAggregateTree.Batch initial = tree.batch();
    initial.replace(1, start, end, AggregateSummary.row(new long[] {Long.MIN_VALUE}, 1));
    initial.replace(2, start, end, AggregateSummary.row(new long[] {Long.MAX_VALUE}, 1));
    initial.replace(3, start, end, AggregateSummary.row(new long[] {7}, 1));
    initial.apply();
    assertEquals(0, tree.point(start - 1).count());
    assertEquals(3, tree.point(start).count());
    assertEquals(BigInteger.valueOf(6), tree.point(end - 1).sum(0));
    assertEquals(0, tree.point(end).count());
    store.reset();
    final TemporalAggregateTree.Batch remove = tree.batch();
    remove.replace(1, start, end, null);
    remove.replace(2, start, end, null);
    remove.apply();
    final AggregateSummary summary = tree.point(start);
    assertEquals(1, summary.count());
    assertEquals(7, summary.min(0));
    assertEquals(7, summary.max(0));
  }

  @Test
  void sparseDomainAndBatchedWritesHaveBoundedWork() {
    final MemoryStore store = new MemoryStore();
    final TemporalAggregateTree tree = new TemporalAggregateTree(store, 48, 1);
    final TemporalAggregateTree.Batch initial = tree.batch();
    for (long id = 0; id < 4096; id++) {
      initial.replace(id, 1024, 2048, AggregateSummary.row(new long[] {id}, 1));
    }
    initial.apply();
    store.reset();
    final TemporalAggregateTree.Batch update = tree.batch();
    for (long id = 100; id < 120; id++) {
      update.replace(id, 1024, 2048, null);
    }
    update.apply();
    assertTrue(store.written.size() <= 48 + 2 * 20 + 24, "Batched subtree rewrite grew with corpus size");
    store.reset();
    assertEquals(4076, tree.point(1500).count());
    assertTrue(store.reads <= 3, "An empty unary path must be skipped");
  }

  @Test
  void sumLanesDoNotStorePerCoverContributorsAndDuplicatesShareExtremaLeaves() {
    final MemoryStore store = new MemoryStore();
    final TemporalAggregateDomain domain = new TemporalAggregateDomain();
    final TemporalAggregateTree tree = new TemporalAggregateTree(store, domain.height(), List.of(
        new TemporalAggregateSpec.Lane(0, -1, TemporalAggregateSpec.SUM),
        new TemporalAggregateSpec.Lane(1, -1, TemporalAggregateSpec.SUM),
        new TemporalAggregateSpec.Lane(2, -1, TemporalAggregateSpec.MIN | TemporalAggregateSpec.MAX)));
    final long from = domain.point(Instant.parse("2020-01-01T00:00:00Z"));
    final long to = domain.point(Instant.parse("2021-01-01T00:00:00Z"));
    final TemporalAggregateTree.Batch build = tree.batch();
    for (int row = 0; row < 25_000; row++) {
      build.replace(row, from, to, AggregateSummary.row(new long[] {row, row * 9L, row % 16}, 7));
    }
    build.apply();
    long bytes = 0;
    int frequencyNodes = 0;
    for (final var slot : store.slots.entrySet()) {
      bytes += 29 + slot.getValue().length;
      if (slot.getKey().bit() >= 0) {
        frequencyNodes++;
      }
    }
    assertTrue(bytes < 2_100_000, "Rows must be stored once, independent of canonical cover size: " + bytes);
    assertTrue(frequencyNodes <= 16 * 31, "Extrema must share leaves for duplicate values: " + frequencyNodes);
    assertEquals(25_000, tree.point(from).count());
    assertEquals(BigInteger.valueOf(24_999L * 25_000 / 2), tree.point(from).sum(0));
    assertEquals(0, tree.point(from).min(2));
    assertEquals(15, tree.point(from).max(2));
    assertEquals(0, tree.point(to).count());
  }

  @Test
  void valueSwapWritesOnlyTheChangedRows() {
    final MemoryStore store = new MemoryStore();
    final TemporalAggregateTree tree = new TemporalAggregateTree(store, 48, 1);
    final TemporalAggregateTree.Batch initial = tree.batch();
    initial.replace(1, 1024, 2048, AggregateSummary.row(new long[] {5}, 1));
    initial.replace(2, 1024, 2048, AggregateSummary.row(new long[] {9}, 1));
    initial.apply();
    store.reset();
    final TemporalAggregateTree.Batch swap = tree.batch();
    swap.replace(1, 1024, 2048, AggregateSummary.row(new long[] {9}, 1));
    swap.replace(2, 1024, 2048, AggregateSummary.row(new long[] {5}, 1));
    swap.apply();
    assertEquals(2, store.written.size(), "Unchanged summaries and links must not copy pages");
    assertEquals(BigInteger.valueOf(14), tree.point(1500).sum(0));
    assertEquals(5, tree.point(1500).min(0));
    assertEquals(9, tree.point(1500).max(0));
    store.reset();
    final TemporalAggregateTree.Batch unchanged = tree.batch();
    unchanged.replace(1, 1024, 2048, AggregateSummary.row(new long[] {9}, 1));
    unchanged.apply();
    assertTrue(store.written.isEmpty(), "An identical row must produce no persisted changes");
  }

  private static void verify(final TemporalAggregateTree tree, final Map<Long, Row> rows, final int domain) {
    for (int point = 0; point < domain; point++) {
      long count = 0;
      long present = 0;
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      BigInteger sum = BigInteger.ZERO;
      for (final Row row : rows.values()) {
        if (row.from() <= point && point < row.to()) {
          count++;
          if (row.present()) {
            present++;
            sum = sum.add(BigInteger.valueOf(row.value()));
            min = Math.min(min, row.value());
            max = Math.max(max, row.value());
          }
        }
      }
      final AggregateSummary actual = tree.point(point);
      assertEquals(count, actual.count(), "point " + point);
      assertEquals(present, actual.present(0));
      assertEquals(sum, actual.sum(0));
      assertEquals(min, actual.min(0));
      assertEquals(max, actual.max(0));
    }
  }
}
