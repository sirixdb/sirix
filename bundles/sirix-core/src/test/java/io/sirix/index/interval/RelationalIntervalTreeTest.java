package io.sirix.index.interval;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.TreeMap;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RelationalIntervalTreeTest {
  @Test
  void halfOpenBoundaryAnswers() {
    final MemoryStore lower = new MemoryStore();
    final MemoryStore upper = new MemoryStore();
    final RelationalIntervalTree tree = new RelationalIntervalTree(4, lower, upper);
    tree.insert(1, 1, 8);
    tree.insert(2, 8, 15);
    tree.insert(3, 8, 8);
    tree.insert(4, 7, 9);
    final LongOpenHashSet actual = new LongOpenHashSet();
    tree.stabHalfOpen(8, actual::add);
    assertEquals(LongOpenHashSet.of(2, 4), actual);
    actual.clear();
    tree.stabHalfOpen(15, actual::add);
    assertTrue(actual.isEmpty());
    tree.delete(4, 7, 9);
    tree.stabHalfOpen(8, actual::add);
    assertEquals(LongOpenHashSet.of(2), actual);
  }

  @Test
  void everyHalfOpenPointMatchesTheIndependentPredicateWithinItsProbeBudget() {
    final int height = 5;
    final MemoryStore lower = new MemoryStore();
    final MemoryStore upper = new MemoryStore();
    final RelationalIntervalTree tree = new RelationalIntervalTree(height, lower, upper);
    final int max = (1 << height) - 1;
    for (int lo = 1; lo <= max; lo++) {
      for (int hi = lo; hi <= max; hi++) {
        tree.insert(lo * 64L + hi, lo, hi);
      }
    }
    for (int point = 0; point <= max + 1; point++) {
      final LongArrayList actual = new LongArrayList();
      final LongOpenHashSet expected = new LongOpenHashSet();
      for (int lo = 1; lo <= max; lo++) {
        for (int hi = lo; hi <= max; hi++) {
          if (lo <= point && point < hi) {
            expected.add(lo * 64L + hi);
          }
        }
      }
      lower.scans = upper.scans = 0;
      tree.stabHalfOpen(point, actual::add);
      assertEquals(expected, new LongOpenHashSet(actual));
      assertEquals(expected.size(), actual.size(), "each interval must be emitted only once");
      assertTrue(lower.scans + upper.scans <= height);
    }
    final LongArrayList enumerated = new LongArrayList();
    lower.scans = upper.scans = 0;
    tree.forEachRef(enumerated::add);
    assertEquals(max * (max + 1) / 2, enumerated.size(), "every registration must be enumerated exactly once");
    assertEquals(1, lower.scans + upper.scans, "enumeration must be one ordered-store scan");
  }

  private static final class MemoryStore implements OrderedStore {
    private final TreeMap<Long, TreeMap<Long, LongOpenHashSet>> forks = new TreeMap<>();
    private int scans;

    @Override
    public void insert(final long fork, final long endpoint, final long ref) {
      forks.computeIfAbsent(fork, ignored -> new TreeMap<>())
           .computeIfAbsent(endpoint, ignored -> new LongOpenHashSet())
           .add(ref);
    }

    @Override
    public void remove(final long fork, final long endpoint, final long ref) {
      Objects.requireNonNull(Objects.requireNonNull(forks.get(fork)).get(endpoint)).remove(ref);
    }

    @Override
    public void scan(final long fork, final long lo, final long hi, final LongConsumer out) {
      scans++;
      final var endpoints = forks.get(fork);
      if (endpoints != null && lo <= hi) {
        for (final var refs : endpoints.subMap(lo, true, hi, true).values()) {
          refs.forEach(out);
        }
      }
    }

    @Override
    public void forEachRef(final LongConsumer out) {
      scans++;
      for (final var endpoints : forks.values()) {
        for (final var refs : endpoints.values()) {
          refs.forEach(out);
        }
      }
    }
  }
}
