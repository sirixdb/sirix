/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The property that makes a top-K over a segment-scoped column correct: a value that lives in two
 * segments must reach the kernel as ONE group key.
 */
final class SegmentGroupCanonicaliserTest {

  /** A dictionary stand-in: cell -> value, so a test can put one value in several segments. */
  private static SegmentGroupCanonicaliser over(final Map<Long, String> corpus, final int segments) {
    return new SegmentGroupCanonicaliser(corpus::get, segments);
  }

  /** A slice whose every row is PRESENT — the ordinary case. */
  private static ColumnSlice sliceOf(final long... cells) {
    final long[] presence = new long[(cells.length + 63) >>> 6];
    for (int row = 0; row < cells.length; row++) {
      presence[row >>> 6] |= 1L << (row & 63);
    }
    return new ColumnSlice(cells.length, (byte) 0, Long.MIN_VALUE, Long.MAX_VALUE, presence, cells, null, null, null,
        null, null);
  }

  /** A slice where the rows named by {@code absent} carry no value. */
  private static ColumnSlice sliceWithAbsent(final long[] cells, final int... absent) {
    final ColumnSlice present = sliceOf(cells);
    final long[] presence = present.presenceWords().clone();
    for (final int row : absent) {
      presence[row >>> 6] &= ~(1L << (row & 63));
    }
    return new ColumnSlice(cells.length, (byte) 0, present.min(), present.max(), presence, cells, null, null, null,
        null, null);
  }

  @Test
  @DisplayName("one value in two segments canonicalises to ONE key — the whole reason this class exists")
  void aValueSplitAcrossSegmentsBecomesOneGroup() {
    final long inZero = ProjectionIndexRowGroupPage.packSegmentCell(0, 5);
    final long inOne = ProjectionIndexRowGroupPage.packSegmentCell(1, 91);
    final Map<Long, String> corpus = new HashMap<>();
    corpus.put(inZero, "google.com");
    corpus.put(inOne, "google.com");

    final ColumnSlice[] out = over(corpus, 2).canonicalise(new ColumnSlice[] {sliceOf(inZero, inOne)});

    assertEquals(out[0].numericValues()[0], out[0].numericValues()[1],
        "the two cells name the same value and must reach the kernel as the same key");
    assertNotEquals(inZero, out[0].numericValues()[0], "and it must be a canonical id, not a cell");
  }

  @Test
  @DisplayName("distinct values stay distinct, and the id inverts to the value a winner must print")
  void distinctValuesStayDistinct() {
    final long a = ProjectionIndexRowGroupPage.packSegmentCell(0, 1);
    final long b = ProjectionIndexRowGroupPage.packSegmentCell(1, 1); // same ID, other segment
    final Map<Long, String> corpus = new HashMap<>();
    corpus.put(a, "alpha");
    corpus.put(b, "beta");

    final SegmentGroupCanonicaliser canonicaliser = over(corpus, 2);
    final ColumnSlice[] out = canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(a, b)});

    final int idA = (int) out[0].numericValues()[0];
    final int idB = (int) out[0].numericValues()[1];
    assertNotEquals(idA, idB, "the same id in two segments is two values and must stay two groups");
    assertEquals("alpha", canonicaliser.valueOf(idA));
    assertEquals("beta", canonicaliser.valueOf(idB));
    assertEquals(2, canonicaliser.size());
  }

  @Test
  @DisplayName("the zone map describes the canonical ids, not the cells they replaced")
  void minAndMaxDescribeTheNewLane() {
    final long low = ProjectionIndexRowGroupPage.packSegmentCell(7, 3);
    final long high = ProjectionIndexRowGroupPage.packSegmentCell(7, 9);
    final Map<Long, String> corpus = new HashMap<>();
    corpus.put(low, "x");
    corpus.put(high, "y");

    final ColumnSlice out = over(corpus, 8).canonicalise(new ColumnSlice[] {sliceOf(low, high)})[0];

    assertTrue(out.min() <= out.max(), "a zone map must bracket its lane");
    assertTrue(out.max() < (1L << 32),
        "carrying the cells' bounds over would let a range prune drop a leaf whose ids are tiny");
    assertEquals(Math.min(out.numericValues()[0], out.numericValues()[1]), out.min());
    assertEquals(Math.max(out.numericValues()[0], out.numericValues()[1]), out.max());
  }

  @Test
  @DisplayName("an unresolvable cell declines rather than inventing a group of its own")
  void anUnresolvableCellDeclines() {
    final long known = ProjectionIndexRowGroupPage.packSegmentCell(0, 1);
    final long orphan = ProjectionIndexRowGroupPage.packSegmentCell(3, 77);
    final Map<Long, String> corpus = new HashMap<>();
    corpus.put(known, "present");

    assertNull(over(corpus, 4).canonicalise(new ColumnSlice[] {sliceOf(known, orphan)}),
        "a key with no value must stop the pass, not silently become its own group");
  }

  @Test
  @DisplayName("under the parallel pass the lock-free memo still issues exactly one id per value")
  void concurrentCanonicalisationAgrees() throws Exception {
    // The memo's fast path reads an int[] without a lock, so two workers can resolve the same cell
    // at once. That race is only benign if both end up with the SAME id — which is what this checks,
    // with enough distinct cells per segment to force the growth path as well.
    final int segments = 4;
    final int idsPerSegment = 3_000;
    final Map<Long, String> corpus = new ConcurrentHashMap<>();
    for (int segment = 0; segment < segments; segment++) {
      for (int id = 0; id < idsPerSegment; id++) {
        // Every segment holds the SAME value set, so the correct answer is idsPerSegment ids total.
        corpus.put(ProjectionIndexRowGroupPage.packSegmentCell(segment, id), "v" + id);
      }
    }
    final SegmentGroupCanonicaliser canonicaliser = over(corpus, segments);

    final int workers = 16;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(workers);
    final Map<String, Integer> seen = new ConcurrentHashMap<>();
    final AtomicInteger disagreements = new AtomicInteger();
    for (int w = 0; w < workers; w++) {
      final int worker = w;
      Thread.ofPlatform().start(() -> {
        try {
          start.await();
          for (int id = 0; id < idsPerSegment; id++) {
            final int segment = (worker + id) % segments;
            final long cell = ProjectionIndexRowGroupPage.packSegmentCell(segment, id);
            final ColumnSlice out = canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(cell)})[0];
            final int canonical = (int) out.numericValues()[0];
            final Integer previous = seen.putIfAbsent("v" + id, canonical);
            if (previous != null && previous != canonical) {
              disagreements.incrementAndGet();
            }
          }
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }
    start.countDown();
    assertTrue(done.await(60, TimeUnit.SECONDS), "workers must finish");

    assertEquals(0, disagreements.get(), "a value must have one canonical id no matter who resolved it");
    assertEquals(idsPerSegment, canonicaliser.size(),
        "four segments holding the same values must yield one id per value, not one per cell");
  }

  @Test
  @DisplayName("an ABSENT row's lane content is not a cell and must not be resolved")
  void absentRowsAreNotResolved() {
    // A missing field leaves whatever the lane was filled with. Resolving it would either invent a
    // group or — because an unresolvable cell declines the whole pass — refuse a servable query.
    final long real = ProjectionIndexRowGroupPage.packSegmentCell(0, 1);
    final long junkInAnAbsentRow = ProjectionIndexRowGroupPage.packSegmentCell(9, 999);
    final Map<Long, String> corpus = new HashMap<>();
    corpus.put(real, "present"); // the junk cell is deliberately NOT resolvable

    final SegmentGroupCanonicaliser canonicaliser = over(corpus, 10);
    final ColumnSlice[] out =
        canonicaliser.canonicalise(new ColumnSlice[] {sliceWithAbsent(new long[] {real, junkInAnAbsentRow}, 1)});

    assertNotNull(out, "an absent row must not make the pass decline");
    assertEquals(1, canonicaliser.size(), "and it must not become a group of its own");
    assertEquals(out[0].numericValues()[0], out[0].min(), "the zone map covers the PRESENT rows only");
    assertEquals(out[0].numericValues()[0], out[0].max());
  }

  @Test
  @DisplayName("after the seal a lane's integer order IS the values' collation order")
  void sealedIdsCollate() {
    // The primitive MIN() and ORDER BY need: a cell's id is a MINT, so raw ids order by first sight.
    // Values are deliberately observed in an order that is NOT their collation order.
    final Map<Long, String> corpus = new HashMap<>();
    final String[] arrivalOrder = {"pear", "apple", "cherry", "banana"};
    final long[] cells = new long[arrivalOrder.length];
    for (int i = 0; i < arrivalOrder.length; i++) {
      cells[i] = ProjectionIndexRowGroupPage.packSegmentCell(i % 2, i + 1);
      corpus.put(cells[i], arrivalOrder[i]);
    }
    final SegmentGroupCanonicaliser canonicaliser = over(corpus, 2);
    assertTrue(canonicaliser.observe(new ColumnSlice[] {sliceOf(cells)}), "every cell resolves");
    assertFalse(canonicaliser.isOrderPreserving());
    canonicaliser.sealOrderPreserving();
    assertTrue(canonicaliser.isOrderPreserving());

    final long[] lane = canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(cells)})[0].numericValues();
    // "apple" < "banana" < "cherry" < "pear": the ids must rank the same way.
    assertTrue(lane[1] < lane[3], "apple before banana");
    assertTrue(lane[3] < lane[2], "banana before cherry");
    assertTrue(lane[2] < lane[0], "cherry before pear");
    // And the smallest id must invert to the smallest value — what makes an integer MIN correct.
    long min = Long.MAX_VALUE;
    for (final long id : lane) {
      min = Math.min(min, id);
    }
    assertEquals("apple", canonicaliser.valueOf((int) min));
  }

  @Test
  @DisplayName("a value first seen after the seal declines rather than taking an invented rank")
  void anUnobservedValueAfterTheSealDeclines() {
    final long observed = ProjectionIndexRowGroupPage.packSegmentCell(0, 1);
    final long late = ProjectionIndexRowGroupPage.packSegmentCell(0, 2);
    final Map<Long, String> corpus = new HashMap<>();
    corpus.put(observed, "seen");
    corpus.put(late, "late");

    final SegmentGroupCanonicaliser canonicaliser = over(corpus, 1);
    assertTrue(canonicaliser.observe(new ColumnSlice[] {sliceOf(observed)}));
    canonicaliser.sealOrderPreserving();

    assertNull(canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(observed, late)}),
        "any rank for 'late' would misorder it against everything already ranked");
  }

  @Test
  @DisplayName("sealing twice keeps the ids the caller is already carrying")
  void sealingIsIdempotent() {
    final long a = ProjectionIndexRowGroupPage.packSegmentCell(0, 1);
    final long b = ProjectionIndexRowGroupPage.packSegmentCell(0, 2);
    final Map<Long, String> corpus = new HashMap<>();
    corpus.put(a, "zulu");
    corpus.put(b, "alpha");

    final SegmentGroupCanonicaliser canonicaliser = over(corpus, 1);
    canonicaliser.observe(new ColumnSlice[] {sliceOf(a, b)});
    canonicaliser.sealOrderPreserving();
    final long[] first = canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(a, b)})[0].numericValues();
    canonicaliser.sealOrderPreserving();
    final long[] second = canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(a, b)})[0].numericValues();
    assertEquals(first[0], second[0]);
    assertEquals(first[1], second[1]);
    assertTrue(second[1] < second[0], "alpha ranks before zulu");
  }
}
