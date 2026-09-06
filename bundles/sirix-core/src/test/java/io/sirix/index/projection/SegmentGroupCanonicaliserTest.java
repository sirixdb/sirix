/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

  /**
   * A dictionary stand-in that ALSO answers positions, the way a sealed segment dictionary does:
   * within one segment, position order is collation order. Mints are deliberately NOT in that order,
   * so anything that folds on the mint instead of the position is wrong at almost every id.
   */
  /**
   * A dictionary stand-in that also answers in POSITION space, both ways: {@code positions} is
   * cell -> position, {@code mints} is {@code pack(segment, position)} -> mint, and {@code entries}
   * is the per-segment entry count — enough for the storage-order walk to run over it. Every read
   * of a value is counted, so a test can say what a pass cost.
   */
  private record PositionedCorpus(Map<Long, String> values, Map<Long, Integer> positions, long[] cells,
      Map<Long, Integer> mints, Map<Integer, Integer> entries, AtomicInteger reads, AtomicInteger collisions)
      implements SegmentGroupCanonicaliser.CellResolver {
    @Override
    public String valueOfCell(final long cell) {
      reads.incrementAndGet();
      return values.get(cell);
    }

    /** Asked only on a hash-chain hit: on a corpus of distinct values, every call is a duplicate landing. */
    @Override
    public boolean sameValue(final long left, final long right) {
      collisions.incrementAndGet();
      return SegmentGroupCanonicaliser.CellResolver.super.sameValue(left, right);
    }

    @Override
    public int positionOfCell(final long cell) {
      final Integer position = positions.get(cell);
      return position == null
          ? SegmentGroupCanonicaliser.NO_POSITION
          : position;
    }

    @Override
    public int entryCountOfSegment(final long cell) {
      final Integer count = entries.get(ProjectionIndexRowGroupPage.segmentOfCell(cell));
      return count == null
          ? -1
          : count;
    }

    @Override
    public int mintAtPosition(final long cell, final int position) {
      final Integer mint =
          mints.get(ProjectionIndexRowGroupPage.packSegmentCell(ProjectionIndexRowGroupPage.segmentOfCell(cell),
              position));
      return mint == null
          ? -1
          : mint;
    }
  }

  /**
   * {@code segments} segments of {@code perSegment} values each. Mints run DOWNWARD while positions
   * run upward, so mint order is the exact reverse of collation order inside every segment.
   */
  private static PositionedCorpus positionedCorpus(final int segments, final int perSegment) {
    return positionedCorpus(segments, perSegment, false);
  }

  /**
   * The same corpus; with {@code shared} every segment holds the SAME {@code perSegment} values, so
   * the correct canonical space has {@code perSegment} ids however many segments there are.
   */
  private static PositionedCorpus positionedCorpus(final int segments, final int perSegment, final boolean shared) {
    final Map<Long, String> values = new HashMap<>();
    final Map<Long, Integer> positions = new HashMap<>();
    final Map<Long, Integer> mints = new HashMap<>();
    final Map<Integer, Integer> entries = new HashMap<>();
    final long[] cells = new long[segments * perSegment];
    int at = 0;
    for (int segment = 0; segment < segments; segment++) {
      // Interleave the segments' value ranges so a merge that simply concatenates runs is wrong:
      // segment 0 holds value-000, value-004, ...; segment 1 holds value-001, value-005, ...
      final String[] mine = new String[perSegment];
      for (int i = 0; i < perSegment; i++) {
        mine[i] = String.format("value-%05d", shared
            ? i
            : i * segments + segment);
      }
      Arrays.sort(mine);
      for (int i = 0; i < perSegment; i++) {
        final int mint = perSegment - i; // downward: mint order is the reverse of position order
        final long cell = ProjectionIndexRowGroupPage.packSegmentCell(segment, mint);
        values.put(cell, mine[i]);
        positions.put(cell, i + 1); // 1-based, ascending with collation inside this segment
        mints.put(ProjectionIndexRowGroupPage.packSegmentCell(segment, i + 1), mint);
        cells[at++] = cell;
      }
      entries.put(segment, perSegment);
    }
    return new PositionedCorpus(values, positions, cells, mints, entries, new AtomicInteger(), new AtomicInteger());
  }

  /**
   * One leaf per segment — a leaf never straddles a segment, and the row loop leans on that — with
   * the rows in MINT order, which inside every segment is the reverse of position order.
   */
  private static ColumnSlice[] leavesInMintOrder(final PositionedCorpus corpus, final int segments,
      final int perSegment) {
    final ColumnSlice[] leaves = new ColumnSlice[segments];
    for (int segment = 0; segment < segments; segment++) {
      final long[] rows = new long[perSegment];
      for (int i = 0; i < perSegment; i++) {
        rows[i] = corpus.cells()[segment * perSegment + (perSegment - 1 - i)];
      }
      leaves[segment] = sliceOf(rows);
    }
    return leaves;
  }

  @Test
  @DisplayName("the storage-order walk resolves every cell before the row loop, in position order, once")
  void theStorageOrderWalkResolvesInPositionOrder() {
    final int segments = 3;
    final int perSegment = 50;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final List<Integer> walked = new ArrayList<>();
    final SegmentGroupCanonicaliser.SegmentRunner recording = (count, body) -> {
      for (int i = 0; i < count; i++) {
        walked.add(i);
        body.accept(i);
      }
    };

    final ColumnSlice[] out = canonicaliser.canonicalise(leavesInMintOrder(corpus, segments, perSegment), null,
        recording);

    assertNotNull(out);
    assertEquals(segments, walked.size(), "the runner must be handed one walk per referenced segment");
    assertEquals(segments * perSegment, corpus.reads().get(),
        "every cell is read exactly once — by the walk; the row loop must find all of them memoised");
    // The witness that the WALK issued the ids and not the row loop: inside a segment the ids rise
    // with POSITION. The rows run in mint order, the other way, so a row loop would have issued
    // them rising along the rows; the walk's ids FALL along the rows.
    for (int segment = 0; segment < segments; segment++) {
      final long[] ids = out[segment].numericValues();
      for (int row = 1; row < perSegment; row++) {
        assertTrue(ids[row] < ids[row - 1], "segment " + segment + ", row " + row
            + " sits one position BELOW row " + (row - 1) + " and must carry the smaller id");
      }
    }
    // Sealed by the merge, the space is exactly the sort's — the walk changed which thread reads, not the order.
    assertTrue(canonicaliser.sealByPositionMerge(canonicaliser.size()));
    for (int rank = 2; rank <= segments * perSegment; rank++) {
      assertTrue(canonicaliser.valueOf(rank - 1).compareTo(canonicaliser.valueOf(rank)) < 0);
    }

    // A second pass over the same cells is free: the walk skips memoised mints and the row loop hits.
    final int readsAfterFirst = corpus.reads().get();
    assertNotNull(canonicaliser.canonicalise(leavesInMintOrder(corpus, segments, perSegment), null, recording));
    assertEquals(readsAfterFirst, corpus.reads().get(), "a settled cell must never be read again");
  }

  @Test
  @DisplayName("walks on worker threads issue ONE id per value across segments, dense, batch after batch")
  void walksOnWorkersConvergeToOneIdPerValue() throws Exception {
    // Every segment holds the same values, so each of the 6 walks meets the others' values in the
    // hash chain, and 9000 marked mints per segment is more than two batches — the batch flush and
    // the last partial batch both run. The runner is a real pool: the walks overlap in time.
    final int segments = 6;
    final int perSegment = 9_000;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment, true);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ExecutorService pool = Executors.newFixedThreadPool(segments);
    try {
      final SegmentGroupCanonicaliser.SegmentRunner onThreads = (count, body) -> {
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<?>> walks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
          final int walk = i;
          walks.add(pool.submit(() -> {
            start.await();
            body.accept(walk);
            return null;
          }));
        }
        start.countDown();
        for (final Future<?> walk : walks) {
          try {
            walk.get(30, TimeUnit.SECONDS);
          } catch (final Exception failure) {
            throw new IllegalStateException(failure);
          }
        }
      };

      final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);
      assertTrue(canonicaliser.observe(leaves, onThreads));

      assertEquals(perSegment, canonicaliser.size(),
          "six copies of one value set must canonicalise to ONE id per value, whichever walk got there first");
      final ColumnSlice[] out = canonicaliser.canonicalise(leaves, null, onThreads);
      assertNotNull(out);
      for (int segment = 0; segment < segments; segment++) {
        final long[] ids = out[segment].numericValues();
        final long[] cells = leaves[segment].numericValues();
        for (int row = 0; row < perSegment; row++) {
          assertTrue(ids[row] >= 1 && ids[row] <= perSegment, "ids must be dense in [1, distinct]; row " + row);
          assertEquals(corpus.valueOfCell(cells[row]), canonicaliser.valueOf((int) ids[row]),
              "segment " + segment + " row " + row + " must map to the id of ITS value");
          assertEquals(out[0].numericValues()[row], ids[row],
              "the same value in segment 0 and segment " + segment + " must share one id");
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  @DisplayName("two passes walking the same segments at once still issue one id per value")
  void concurrentWalksOverTheSameCellsAgree() throws Exception {
    // The walk checks the memo outside the lock and lands its batch inside it, so two walks over the
    // SAME (segment, mint) race: both hash the cell, and the second batch must find the first's id
    // and keep it. Dropping that check would issue a second id for a value that already has one.
    final int segments = 2;
    final int perSegment = 5_000;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] slices = leavesInMintOrder(corpus, segments, perSegment);
    final int passes = 8;
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(passes);
    try {
      final List<Future<Boolean>> results = new ArrayList<>(passes);
      for (int pass = 0; pass < passes; pass++) {
        results.add(pool.submit(() -> {
          start.await();
          return canonicaliser.observe(slices);
        }));
      }
      start.countDown();
      for (final Future<Boolean> result : results) {
        assertTrue(result.get(30, TimeUnit.SECONDS), "every pass resolves every cell");
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(segments * perSegment, canonicaliser.size(),
        "eight racing passes over one corpus must still issue exactly one id per distinct value");
    assertEquals(0, corpus.collisions().get(), "a cell another pass already landed is skipped under the lock, "
        + "not re-issued through the hash chain — on distinct values the chain must never be consulted");
    final ColumnSlice[] out = canonicaliser.canonicalise(slices);
    assertNotNull(out);
    for (int segment = 0; segment < segments; segment++) {
      for (int row = 0; row < perSegment; row++) {
        assertEquals(corpus.valueOfCell(slices[segment].numericValues()[row]),
            canonicaliser.valueOf((int) out[segment].numericValues()[row]),
            "segment " + segment + " row " + row + " must map to the id of ITS value");
      }
    }
  }

  @Test
  @DisplayName("the position merge orders the whole value space exactly as sorting it would")
  void positionMergeMatchesTheSort() {
    final int segments = 7;
    final int perSegment = 40;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final int distinct = segments * perSegment;

    final SegmentGroupCanonicaliser merged = new SegmentGroupCanonicaliser(corpus, segments);
    assertNotNull(merged.canonicalise(new ColumnSlice[] {sliceOf(corpus.cells())}), "every cell resolves");
    assertEquals(distinct, merged.size(), "the corpus must hold no duplicate values");
    assertTrue(merged.sealByPositionMerge(merged.size()), "the merge must seal a corpus it has positions for");

    // The property, stated directly: reading the sealed space by ascending rank yields ascending
    // values. A merge that concatenated runs, or folded on the mint, fails at almost every rank.
    for (int rank = 2; rank <= distinct; rank++) {
      final String previous = merged.valueOf(rank - 1);
      final String current = merged.valueOf(rank);
      assertNotNull(previous);
      assertNotNull(current);
      assertTrue(previous.compareTo(current) < 0,
          "rank " + (rank - 1) + " (" + previous + ") must collate before rank " + rank + " (" + current + ")");
    }

    // And it agrees with the path it replaces, id for id, on the same corpus.
    final SegmentGroupCanonicaliser sorted = new SegmentGroupCanonicaliser(corpus, segments);
    assertNotNull(sorted.canonicalise(new ColumnSlice[] {sliceOf(corpus.cells())}));
    assertTrue(sorted.sealOrderPreserving(), "the small corpus takes the materialising path");
    for (int rank = 1; rank <= distinct; rank++) {
      assertEquals(sorted.valueOf(rank), merged.valueOf(rank), "the two seals disagree at rank " + rank);
    }
  }

  @Test
  @DisplayName("a resolver with no positions refuses the merge rather than inventing an order")
  void aResolverWithoutPositionsRefusesTheMerge() {
    final Map<Long, String> corpus = new HashMap<>();
    final long a = ProjectionIndexRowGroupPage.packSegmentCell(0, 1);
    final long b = ProjectionIndexRowGroupPage.packSegmentCell(1, 2);
    corpus.put(a, "alpha");
    corpus.put(b, "beta");
    // `corpus::get` is a plain CellResolver: it answers values and takes the NO_POSITION default,
    // which is what a TRANSFORMING resolver must also do, since a transform reorders what storage
    // ordered.
    final SegmentGroupCanonicaliser canonicaliser = over(corpus, 2);
    assertNotNull(canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(a, b)}));
    assertFalse(canonicaliser.sealByPositionMerge(canonicaliser.size()),
        "without positions the merge must decline, not order by mint");
    assertFalse(canonicaliser.isOrderPreserving(), "a refused merge must leave the space unsealed");
  }

  @Test
  @DisplayName("a leaf the predicates pruned is left null and costs no dictionary read")
  void aPrunedLeafIsNeverCanonicalised() {
    final Map<Long, String> corpus = new HashMap<>();
    // The KEPT cell lives in segment 1, so its packed form is a large long and cannot be confused
    // with the small canonical id that replaces it — a segment-0 cell IS a small integer, which is
    // precisely the collision this rewrite has to be readable against.
    final long kept = ProjectionIndexRowGroupPage.packSegmentCell(1, 7);
    final long pruned = ProjectionIndexRowGroupPage.packSegmentCell(0, 2);
    corpus.put(kept, "kept");
    corpus.put(pruned, "pruned");
    final AtomicInteger resolves = new AtomicInteger();
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(cell -> {
      resolves.incrementAndGet();
      return corpus.get(cell);
    }, 2);

    // Leaf 0 survives the zone maps, leaf 1 does not.
    final ColumnSlice[] slices = {sliceOf(kept), sliceOf(pruned)};
    final long[] keep = {1L}; // bit 0 set, bit 1 clear
    final ColumnSlice[] out = canonicaliser.canonicalise(slices, keep);

    assertNotNull(out);
    assertNotNull(out[0], "a kept leaf must be canonicalised");
    assertNull(out[1], "a pruned leaf must be left null, never passed through with raw cells");
    assertEquals(1, resolves.get(), "the pruned leaf's cell must cost no dictionary read");
    assertEquals(1, canonicaliser.size(), "and must issue no canonical id of its own");
    // The kept leaf really was rewritten: its lane carries a canonical id, not the packed cell.
    assertEquals(1L, out[0].numericValues()[0], "the kept leaf's lane must hold the canonical id");
    assertNotEquals(kept, out[0].numericValues()[0], "and not the raw cell it replaced");
  }

  /** The store's pruned sentinel: no rows, no lanes — what a windowed fill hands out for a dropped leaf. */
  private static ColumnSlice prunedSentinel() {
    return new ColumnSlice(0, (byte) 0, Long.MAX_VALUE, Long.MIN_VALUE, new long[0], null, null, null, null, null,
        null);
  }

  @Test
  @DisplayName("the rowless pruned sentinel passes through untouched instead of refusing the pass")
  void thePrunedSentinelPassesThrough() {
    final Map<Long, String> corpus = new HashMap<>();
    final long a = ProjectionIndexRowGroupPage.packSegmentCell(1, 3);
    final long b = ProjectionIndexRowGroupPage.packSegmentCell(2, 5);
    corpus.put(a, "alpha");
    corpus.put(b, "beta");
    final AtomicInteger resolves = new AtomicInteger();
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(cell -> {
      resolves.incrementAndGet();
      return corpus.get(cell);
    }, 3);
    final ColumnSlice sentinel = prunedSentinel();

    // A windowed morsel: kept leaf, dropped leaf (the sentinel), kept leaf — and no keep mask, which
    // is exactly how the parallel group pass calls it.
    final ColumnSlice[] out = canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(a), sentinel, sliceOf(b)});

    assertNotNull(out, "a dropped leaf holds no cell and must not decline the morsel");
    assertTrue(out[1] == sentinel, "the sentinel must pass through as the very same rowless slice");
    assertEquals(0, out[1].rowCount());
    assertEquals(1L, out[0].numericValues()[0]);
    assertEquals(2L, out[2].numericValues()[0], "the kept leaves around it are still canonicalised");
    assertEquals(2, resolves.get(), "and the sentinel costs no dictionary read");
    assertTrue(canonicaliser.observe(new ColumnSlice[] {sentinel, sliceOf(a)}),
        "observe must skip the sentinel the same way");
  }

  @Test
  @DisplayName("a slice WITH rows but without a long lane still refuses — its raw cells could collide with ids")
  void aRowfulSliceWithoutALaneRefuses() {
    final ColumnSlice noLane = new ColumnSlice(2, (byte) 0, 0L, 1L, new long[] {3L}, null, null, null, null, null,
        null);
    final SegmentGroupCanonicaliser canonicaliser = over(new HashMap<>(), 1);
    assertNull(canonicaliser.canonicalise(new ColumnSlice[] {noLane}));
    assertFalse(canonicaliser.observe(new ColumnSlice[] {noLane}));
  }

  @Test
  @DisplayName("a null keep mask still canonicalises every leaf")
  void aNullKeepMaskKeepsEverything() {
    final Map<Long, String> corpus = new HashMap<>();
    final long a = ProjectionIndexRowGroupPage.packSegmentCell(0, 1);
    final long b = ProjectionIndexRowGroupPage.packSegmentCell(1, 2);
    corpus.put(a, "alpha");
    corpus.put(b, "beta");
    final ColumnSlice[] out = over(corpus, 2).canonicalise(new ColumnSlice[] {sliceOf(a), sliceOf(b)}, null);
    assertNotNull(out);
    assertNotNull(out[0]);
    assertNotNull(out[1], "without a mask nothing is pruned");
  }
}
