/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
      Map<Long, Integer> mints, Map<Integer, Integer> entries, AtomicInteger reads, AtomicInteger collisions,
      AtomicInteger positionLookups, AtomicInteger positionWalks)
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
      positionLookups.incrementAndGet();
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
      positionWalks.incrementAndGet();
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
    return new PositionedCorpus(values, positions, cells, mints, entries, new AtomicInteger(), new AtomicInteger(),
        new AtomicInteger(), new AtomicInteger());
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

  /** A row mask over {@code rows} rows with exactly the named rows set. */
  private static long[] rowsKept(final int rows, final int... kept) {
    final long[] mask = new long[(rows + 63) >>> 6];
    for (final int row : kept) {
      mask[row >>> 6] |= 1L << (row & 63);
    }
    return mask;
  }

  @Test
  @DisplayName("PREDICATE-FIRST: only the rows a mask keeps are resolved, issued, and carried")
  void rowMasksBoundTheResolvedCells() {
    // Two segments of eight distinct values each, one leaf per segment. The mask keeps three rows
    // of leaf 0 and NO row of leaf 1 (null): the walk must resolve exactly those three cells, the
    // value space must hold exactly three values, and leaf 1 must come back null — a leaf the
    // kernel never reads a key from is a leaf nothing needs to canonicalise.
    final int segments = 2;
    final int perSegment = 8;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);
    final long[][] rowKeep = {rowsKept(perSegment, 1, 3, 6), null};

    final ColumnSlice[] out = canonicaliser.canonicalise(leaves, null, rowKeep,
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS);

    assertNotNull(out);
    assertNull(out[1], "a leaf whose every row the predicates drop is left null");
    assertEquals(3, corpus.reads().get(), "exactly the kept cells are read — the cleared rows cost nothing");
    assertEquals(3, canonicaliser.size(), "and only the kept values are issued into the space");
    final ColumnSlice kept = out[0];
    assertNotNull(kept);
    assertEquals(perSegment, kept.rowCount(), "the leaf keeps its row count: the kernel addresses rows by index");
    final long[] presence = kept.presenceWords();
    final long[] ids = kept.numericValues();
    for (int row = 0; row < perSegment; row++) {
      final boolean isKept = row == 1 || row == 3 || row == 6;
      assertEquals(isKept, (presence[0] & 1L << row) != 0L, "row " + row + " presence follows the mask");
      if (isKept) {
        assertTrue(ids[row] >= 1 && ids[row] <= 3, "row " + row + " carries a canonical id");
      } else {
        assertEquals(0L, ids[row], "row " + row + " was cleared and carries no id");
      }
    }
    // The rows run in mint order, the reverse of position order, and the walk issues along position:
    // the kept ids FALL along the rows, as they do without a mask.
    assertTrue(ids[6] < ids[3] && ids[3] < ids[1], "ids follow the storage-order walk");
    assertEquals(1L, kept.min());
    assertEquals(3L, kept.max());
    // The merge seal sees a space of three, sorted.
    assertTrue(canonicaliser.sealByPositionMerge(canonicaliser.size()));
    assertTrue(canonicaliser.valueOf(1).compareTo(canonicaliser.valueOf(2)) < 0);
    assertTrue(canonicaliser.valueOf(2).compareTo(canonicaliser.valueOf(3)) < 0);
  }

  @Test
  @DisplayName("a value kept in two segments is still ONE group under row masks")
  void rowMasksStillMergeAcrossSegments() {
    // Every segment holds the same eight values; row 2 of each leaf is the same value.
    final int segments = 2;
    final int perSegment = 8;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment, true);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);
    final long[][] rowKeep = {rowsKept(perSegment, 2), rowsKept(perSegment, 2, 5)};

    final ColumnSlice[] out = canonicaliser.canonicalise(leaves, null, rowKeep,
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS);

    assertNotNull(out);
    assertEquals(2, canonicaliser.size(), "two distinct values among the three kept rows");
    assertEquals(out[0].numericValues()[2], out[1].numericValues()[2], "the shared value is one group key");
    assertNotEquals(out[0].numericValues()[2], out[1].numericValues()[5]);
    assertEquals(1, corpus.collisions().get(), "the duplicate landed on the hash chain exactly once");
  }

  @Test
  @DisplayName("a mask that keeps few rows of a big segment walks the marks by position, not every position")
  void aSparseMaskWalksByMarksNotPositions() {
    final int segments = 2;
    final int perSegment = 200; // 3 marks in 200 entries: below one in SPARSE_WALK_RATIO, so the walk is sparse
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);
    final long[][] rowKeep = {rowsKept(perSegment, 1, 3, 6), rowsKept(perSegment, 0, 100, 199)};

    final ColumnSlice[] out = canonicaliser.canonicalise(leaves, null, rowKeep,
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS);

    assertNotNull(out);
    assertEquals(0, corpus.positionWalks().get(), "a sparse walk must not visit the positions between its marks");
    assertEquals(6, corpus.positionLookups().get(), "a sparse walk asks the position of each mark exactly once");
    assertEquals(6, corpus.reads().get(), "every marked cell is read exactly once, by the walk");
    assertEquals(6, canonicaliser.size());
    // Still storage order: inside a segment the rows run in mint order, the reverse of position
    // order, so ids issued by position FALL along the kept rows — a walk in mark (mint) order would
    // have issued them rising.
    final long[] first = out[0].numericValues();
    assertTrue(first[6] < first[3] && first[3] < first[1], "segment 0 ids must rise with position: "
        + first[1] + ", " + first[3] + ", " + first[6]);
    final long[] second = out[1].numericValues();
    assertTrue(second[199] < second[100] && second[100] < second[0], "segment 1 ids must rise with position: "
        + second[0] + ", " + second[100] + ", " + second[199]);
    assertTrue(canonicaliser.sealByPositionMerge(canonicaliser.size()));
    for (int rank = 2; rank <= 6; rank++) {
      assertTrue(canonicaliser.valueOf(rank - 1).compareTo(canonicaliser.valueOf(rank)) < 0);
    }
  }

  @Test
  @DisplayName("a mask that keeps most rows of a segment still walks every position")
  void aDenseMaskWalksEveryPosition() {
    final int segments = 1;
    final int perSegment = 64; // 8 marks in 64 entries: one in 8, above the sparse ratio
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);
    final long[][] rowKeep = {rowsKept(perSegment, 0, 9, 18, 27, 36, 45, 54, 63)};

    final ColumnSlice[] out = canonicaliser.canonicalise(leaves, null, rowKeep,
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS);

    assertNotNull(out);
    assertEquals(perSegment, corpus.positionWalks().get(), "a dense walk visits every position of the segment once");
    assertEquals(0, corpus.positionLookups().get(), "a dense walk never asks for a mark's position");
    assertEquals(8, corpus.reads().get());
    assertEquals(8, canonicaliser.size());
    final long[] ids = out[0].numericValues();
    for (int row = 9; row < perSegment; row += 9) {
      assertTrue(ids[row] < ids[row - 9], "ids must rise with position, row " + row);
    }
  }

  @Test
  @DisplayName("observe under row masks counts the kept rows' values only")
  void observeUnderRowMasks() {
    final int segments = 3;
    final int perSegment = 8;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);
    final long[][] rowKeep = {rowsKept(perSegment, 0, 7), null, rowsKept(perSegment, 4)};

    assertTrue(canonicaliser.observe(leaves, rowKeep, SegmentGroupCanonicaliser.SERIAL_SEGMENTS));

    assertEquals(3, canonicaliser.size(), "three kept rows, three distinct values");
    assertEquals(3, corpus.reads().get(), "nothing outside the masks was read");
    // A null mask array keeps everything, as before.
    assertTrue(canonicaliser.observe(leaves, null, SegmentGroupCanonicaliser.SERIAL_SEGMENTS));
    assertEquals(segments * perSegment, canonicaliser.size());
  }

  /** The same slice with one bit set in its presence word BEYOND the row count — a padding bit. */
  private static ColumnSlice withPaddingBit(final ColumnSlice slice, final int bit) {
    final long[] presence = slice.presenceWords().clone();
    presence[bit >>> 6] |= 1L << (bit & 63);
    return new ColumnSlice(slice.rowCount(), slice.flags(), slice.min(), slice.max(), presence,
        slice.numericValues(), null, null, null, null, null);
  }

  @Test
  @DisplayName("a presence word's padding bits beyond the row count are not rows, in every loop")
  void paddingBitsBeyondTheRowCountAreNotRows() {
    // Three rows in a word of 64: the loops now walk WORDS and pick rows off them with a bit scan,
    // so a set bit past the row count would name a row the lane does not have. Every loop that
    // reads a cell by row — the storage-order marking, the row loop, observe — must stop at the
    // row count, with and without a mask (the mask carries the same padding bit).
    final int segments = 1;
    final int perSegment = 3;
    final int padding = 40;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = {withPaddingBit(leavesInMintOrder(corpus, segments, perSegment)[0], padding)};
    final long[][] rowKeep = {rowsKept(perSegment, 0, 2, padding)};

    final ColumnSlice[] masked = canonicaliser.canonicalise(leaves, null, rowKeep,
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS);

    assertNotNull(masked);
    assertEquals(2, corpus.reads().get(), "the two kept rows are read; the padding bit reads nothing");
    assertEquals(2, canonicaliser.size());
    assertTrue(masked[0].numericValues()[0] >= 1 && masked[0].numericValues()[2] >= 1);
    assertEquals(0L, masked[0].numericValues()[1], "row 1 was cleared by the mask");

    assertTrue(canonicaliser.observe(leaves, rowKeep, SegmentGroupCanonicaliser.SERIAL_SEGMENTS));
    assertEquals(2, canonicaliser.size(), "observe under the padded mask sees the same two values");

    final ColumnSlice[] unmasked = canonicaliser.canonicalise(leaves, null, SegmentGroupCanonicaliser.SERIAL_SEGMENTS);
    assertNotNull(unmasked);
    assertEquals(3, corpus.reads().get(), "without a mask the third row is read, and nothing past it");
    assertEquals(3, canonicaliser.size());
    assertTrue(canonicaliser.observe(leaves, null, SegmentGroupCanonicaliser.SERIAL_SEGMENTS));
    assertEquals(3, canonicaliser.size());
  }

  @Test
  @DisplayName("the pruned sentinel passes through a row-masked pass, and a mismatched mask array refuses")
  void rowMasksAndTheSentinel() {
    final Map<Long, String> corpus = new HashMap<>();
    final long a = ProjectionIndexRowGroupPage.packSegmentCell(1, 3);
    corpus.put(a, "alpha");
    final SegmentGroupCanonicaliser canonicaliser = over(corpus, 2);
    final ColumnSlice sentinel = prunedSentinel();
    final ColumnSlice[] leaves = {sentinel, sliceOf(a)};

    final ColumnSlice[] out = canonicaliser.canonicalise(leaves, null, new long[][] {null, rowsKept(1, 0)},
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS);

    assertNotNull(out, "a dropped leaf holds no cell and must not decline the pass");
    assertTrue(out[0] == sentinel, "the sentinel passes through as the very same rowless slice");
    assertEquals(1L, out[1].numericValues()[0]);
    assertTrue(canonicaliser.observe(leaves, new long[][] {null, rowsKept(1, 0)},
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS));

    final long[][] tooShort = {rowsKept(1, 0)};
    assertThrows(IllegalArgumentException.class,
        () -> canonicaliser.canonicalise(leaves, null, tooShort, SegmentGroupCanonicaliser.SERIAL_SEGMENTS),
        "one mask per leaf, or the pass refuses");
    assertThrows(IllegalArgumentException.class,
        () -> canonicaliser.observe(leaves, tooShort, SegmentGroupCanonicaliser.SERIAL_SEGMENTS));
  }

  // ---------------------------------------------------------------------------------------------
  // The segment value MERGE: a whole column's value space from its segments' sorted runs.
  // ---------------------------------------------------------------------------------------------

  /** A runner that records every {@code count} it was handed and runs the bodies inline. */
  private static SegmentGroupCanonicaliser.SegmentRunner recording(final List<Integer> counts) {
    return (count, body) -> {
      counts.add(count);
      for (int i = 0; i < count; i++) {
        body.accept(i);
      }
    };
  }

  /**
   * The collation rank of the cell at row {@code row} of the leaf of {@code segment} in a
   * NON-shared {@link #positionedCorpus}: the value at position {@code p} of segment {@code s} is
   * {@code value-((p - 1) * segments + s)}, and {@link #leavesInMintOrder} puts position
   * {@code perSegment - row} at {@code row}. Dense from 1 over the whole column.
   */
  private static long rankOf(final int segments, final int perSegment, final int segment, final int row) {
    return (long) (perSegment - row - 1) * segments + segment + 1;
  }

  @Test
  @DisplayName("MERGE, NOT HASH: a whole column's ids are collation ranks, issued without a sort or a hash probe")
  void theMergeIssuesCollationRanks() {
    final int segments = 3;
    final int perSegment = 50;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final List<Integer> counts = new ArrayList<>();

    final ColumnSlice[] out = canonicaliser.canonicaliseColumn(leavesInMintOrder(corpus, segments, perSegment), null,
        null, recording(counts));

    assertNotNull(out);
    assertNull(canonicaliser.lastRefusal(), "the merge ran: nothing refused, nothing fell back");
    assertEquals(segments * perSegment, canonicaliser.size());
    assertEquals(0, corpus.collisions().get(), "no cell went through the hash index");
    // THE WITNESS that the merge issued the ids and not the walk: the walk numbers each segment's
    // run in arrival order (segment 0 would take 1..50), the merge numbers the whole column by
    // collation, so segment 0's smallest value is 1, segment 1's is 2, segment 2's is 3, and so on.
    for (int segment = 0; segment < segments; segment++) {
      final long[] ids = out[segment].numericValues();
      for (int row = 0; row < perSegment; row++) {
        assertEquals(rankOf(segments, perSegment, segment, row), ids[row],
            "segment " + segment + " row " + row + " carries its collation rank over the WHOLE column");
      }
    }
    // 150 marks under the default range target is ONE range: the mark phase ran per run and the
    // merge as one body; with a single range there is nothing to bound and nothing to offset.
    assertEquals(List.of(segments, 1), counts, "one mark per run, then one merge");
    // Ranks are the seal: nothing to sort, nothing to renumber, and the lane already collates.
    assertFalse(canonicaliser.isOrderPreserving(), "not sealed until asked");
    assertTrue(canonicaliser.sealOrderPreserving());
    assertTrue(canonicaliser.isOrderPreserving());
    for (int rank = 2; rank <= segments * perSegment; rank++) {
      assertTrue(canonicaliser.valueOf(rank - 1).compareTo(canonicaliser.valueOf(rank)) < 0,
          "rank " + rank + " collates after rank " + (rank - 1));
    }
    assertEquals("value-00000", canonicaliser.valueOf(1));
    assertEquals(String.format("value-%05d", segments * perSegment - 1), canonicaliser.valueOf(segments * perSegment));
    // A second pass finds everything settled: no read, and the same ids.
    final int readsAfterFirst = corpus.reads().get();
    final ColumnSlice[] again = canonicaliser.canonicaliseColumn(leavesInMintOrder(corpus, segments, perSegment),
        null, null, recording(counts));
    assertNotNull(again);
    assertEquals(readsAfterFirst, corpus.reads().get(), "a settled cell is never read again");
    for (int segment = 0; segment < segments; segment++) {
      assertTrue(Arrays.equals(out[segment].numericValues(), again[segment].numericValues()));
    }
    // A column the merge already settled is not a REFUSED merge: the second pass has nothing to mark,
    // so it neither records a refusal (the witness a real fallback leaves) nor dispatches a walk.
    assertNull(canonicaliser.lastRefusal(), "a settled column is not a refusal");
    assertEquals(List.of(segments, 1), counts, "a settled column dispatches no walk");
  }

  @Test
  @DisplayName("a value held by every segment is ONE rank in the merge — duplicates are adjacent in the merge order")
  void theMergeCoalescesAcrossSegments() {
    final int segments = 4;
    final int perSegment = 12;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment, true);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);

    final ColumnSlice[] out = canonicaliser.canonicaliseColumn(leavesInMintOrder(corpus, segments, perSegment), null,
        null, SegmentGroupCanonicaliser.SERIAL_SEGMENTS);

    assertNotNull(out);
    assertNull(canonicaliser.lastRefusal());
    assertEquals(perSegment, canonicaliser.size(), "four copies of twelve values are twelve ranks");
    assertEquals(0, corpus.collisions().get(), "identity came from the merge order, not from a hash chain");
    for (int segment = 0; segment < segments; segment++) {
      final long[] ids = out[segment].numericValues();
      for (int row = 0; row < perSegment; row++) {
        // Row `row` sits at position perSegment - row, and with one copy per value that IS its rank.
        assertEquals(perSegment - row, ids[row], "segment " + segment + " row " + row);
      }
    }
    assertTrue(canonicaliser.observeColumn(leavesInMintOrder(corpus, segments, perSegment), null,
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS));
    assertEquals(perSegment, canonicaliser.size(), "observing the same cells issues nothing new");
  }

  @Test
  @DisplayName("range-partitioned, the merge issues the same ranks as one range — and the ranges really ran")
  void rangesPartitionTheMerge() {
    final int segments = 4;
    final int perSegment = 40;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments).mergeRangeTarget(7);
    final List<Integer> counts = new ArrayList<>();

    final ColumnSlice[] out = canonicaliser.canonicaliseColumn(leavesInMintOrder(corpus, segments, perSegment), null,
        null, recording(counts));

    assertNotNull(out);
    assertNull(canonicaliser.lastRefusal());
    // 160 marked cells at 7 per range = 22 ranges, bounded by the longest run's 40 marks; the offset
    // pass covers every range but the first.
    final int ranges = Math.min(Math.max(1, segments * perSegment / 7), perSegment);
    assertTrue(ranges > 1, "the corpus must be large enough to partition");
    assertEquals(List.of(segments, segments, ranges, ranges - 1), counts, "mark, bound, merge, offset");
    for (int segment = 0; segment < segments; segment++) {
      final long[] ids = out[segment].numericValues();
      for (int row = 0; row < perSegment; row++) {
        assertEquals(rankOf(segments, perSegment, segment, row), ids[row],
            "segment " + segment + " row " + row + ": a rank is a rank whatever range issued it");
      }
    }
    assertEquals(segments * perSegment, canonicaliser.size());
  }

  @Test
  @DisplayName("a value shared by every run lands in ONE range: the pivots bound by lower bounds, so twins never split")
  void rangesKeepTwinsTogether() {
    final int segments = 3;
    final int perSegment = 40;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment, true);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments).mergeRangeTarget(7);
    final List<Integer> counts = new ArrayList<>();

    final ColumnSlice[] out = canonicaliser.canonicaliseColumn(leavesInMintOrder(corpus, segments, perSegment), null,
        null, recording(counts));

    assertNotNull(out);
    assertNull(canonicaliser.lastRefusal());
    assertTrue(counts.get(2) > 1, "more than one range ran: " + counts);
    assertEquals(perSegment, canonicaliser.size(), "three copies of forty values are forty ranks, across the ranges");
    for (int segment = 1; segment < segments; segment++) {
      assertTrue(Arrays.equals(out[0].numericValues(), out[segment].numericValues()),
          "segment " + segment + " carries the same ranks as segment 0 row for row");
    }
    for (int row = 1; row < perSegment; row++) {
      assertEquals(out[0].numericValues()[row - 1] - 1, out[0].numericValues()[row], "ranks fall with the rows");
    }
  }

  @Test
  @DisplayName("PREDICATE-FIRST through the merge: sparse marks are located by position, and only they are ranked")
  void sparseMarksMergeOnlyTheKeptRows() {
    final int segments = 2;
    final int perSegment = 200;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);
    // Three marks in a 200-entry segment is below the sparse break-even: the marks are located one
    // by one through positionOfCell, and no position is walked.
    final long[][] rowKeep = {rowsKept(perSegment, 1, 3, 6), rowsKept(perSegment, 0, 100, 199)};

    final ColumnSlice[] out = canonicaliser.canonicaliseColumn(leaves, null, rowKeep,
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS);

    assertNotNull(out);
    assertNull(canonicaliser.lastRefusal());
    assertEquals(6, canonicaliser.size(), "exactly the kept cells are in the space");
    assertEquals(6, corpus.positionLookups().get(), "one position lookup per mark");
    assertEquals(6, corpus.positionWalks().get(),
        "and one mint read per MARKED position, by the merge — none walked to locate the marks");
    // The kept values, in collation order over both segments: segment 1's positions 1 and 100 sort
    // first (value-00001, value-00199), then segment 0's positions 194, 197, 199 (value-00386,
    // -00392, -00396), then segment 1's position 200 (value-00399).
    final long[] ids0 = out[0].numericValues();
    final long[] ids1 = out[1].numericValues();
    assertEquals(1L, ids1[199]);
    assertEquals(2L, ids1[100]);
    assertEquals(3L, ids0[6]);
    assertEquals(4L, ids0[3]);
    assertEquals(5L, ids0[1]);
    assertEquals(6L, ids1[0]);
    for (int row = 0; row < perSegment; row++) {
      final boolean kept0 = row == 1 || row == 3 || row == 6;
      final boolean kept1 = row == 0 || row == 100 || row == 199;
      assertEquals(kept0, (out[0].presenceWords()[row >>> 6] & 1L << (row & 63)) != 0L, "leaf 0 row " + row);
      assertEquals(kept1, (out[1].presenceWords()[row >>> 6] & 1L << (row & 63)) != 0L, "leaf 1 row " + row);
      if (!kept0) {
        assertEquals(0L, ids0[row], "a cleared row carries no id");
      }
      if (!kept1) {
        assertEquals(0L, ids1[row], "a cleared row carries no id");
      }
    }
    assertEquals("value-00001", canonicaliser.valueOf(1));
    assertEquals("value-00399", canonicaliser.valueOf(6));
    assertTrue(canonicaliser.sealOrderPreserving(), "ranks are the seal");
  }

  @Test
  @DisplayName("after the merge, a cell it never saw finds its rank by value — and a NEW value takes the next id")
  void lateArrivalsAfterTheMerge() {
    final int segments = 3;
    final int perSegment = 10;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment, true);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);
    // Segments 0 and 1 go through the merge; segment 2 holds the same ten values and arrives later.
    final ColumnSlice[] merged = canonicaliser.canonicaliseColumn(new ColumnSlice[] {leaves[0], leaves[1]}, null,
        null, SegmentGroupCanonicaliser.SERIAL_SEGMENTS);
    assertNotNull(merged);
    assertEquals(perSegment, canonicaliser.size());

    final ColumnSlice[] late = canonicaliser.canonicalise(new ColumnSlice[] {leaves[2]});

    assertNotNull(late);
    assertEquals(perSegment, canonicaliser.size(), "a value the merge ranked is FOUND, not issued again");
    assertTrue(Arrays.equals(merged[0].numericValues(), late[0].numericValues()),
        "the late cells carry the ranks their twins in segment 0 carry");
    assertEquals(0, corpus.collisions().get(), "found by binary search over the ranked representatives, not by hash");

    // A value nothing ranked: it takes the id past the ranks, in arrival order, and the space is no
    // longer collated — until it is sealed, which sorts.
    final long fresh = ProjectionIndexRowGroupPage.packSegmentCell(2, perSegment + 1);
    corpus.values().put(fresh, "value-00003.5");
    final ColumnSlice[] out = canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(fresh)});
    assertNotNull(out);
    assertEquals(perSegment + 1, out[0].numericValues()[0], "the first id past the ranks");
    assertEquals(perSegment + 1, canonicaliser.size());
    assertFalse(canonicaliser.isOrderPreserving());
    assertTrue(canonicaliser.sealOrderPreserving(), "eleven values still sort");
    for (int rank = 2; rank <= perSegment + 1; rank++) {
      assertTrue(canonicaliser.valueOf(rank - 1).compareTo(canonicaliser.valueOf(rank)) < 0);
    }
    assertEquals("value-00003.5", canonicaliser.valueOf(5), "value-00003 < value-00003.5 < value-00004");
    final ColumnSlice[] sealed = canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(fresh)});
    assertNotNull(sealed);
    assertEquals(5L, sealed[0].numericValues()[0], "the lane carries the rank once sealed");
  }

  @Test
  @DisplayName("once the merge's ranks ARE the seal, a value first seen afterwards has no rank: the pass declines")
  void aLateValueAfterTheMergeSealDeclines() {
    final int segments = 2;
    final int perSegment = 10;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);
    assertTrue(canonicaliser.observeColumn(leaves, null, SegmentGroupCanonicaliser.SERIAL_SEGMENTS));
    assertTrue(canonicaliser.sealOrderPreserving());
    assertTrue(canonicaliser.isOrderPreserving());

    // Ranked cells still resolve, ranked values arriving through a new cell still resolve.
    final ColumnSlice[] out = canonicaliser.canonicalise(leaves);
    assertNotNull(out);
    assertEquals(rankOf(segments, perSegment, 1, 0), out[1].numericValues()[0]);

    final long fresh = ProjectionIndexRowGroupPage.packSegmentCell(1, perSegment + 1);
    corpus.values().put(fresh, "zzz-never-ranked");
    assertNull(canonicaliser.canonicalise(new ColumnSlice[] {sliceOf(fresh)}),
        "a value with no rank cannot be placed in a collated lane: decline rather than misorder");
    assertThrows(IllegalStateException.class, () -> canonicaliser.canonicalOfValue("a literal after the seal"));
  }

  @Test
  @DisplayName("a literal issued before the merge leaves the space non-empty: the merge refuses, the walk answers")
  void aLiteralBeforeTheMergeRefusesIt() {
    final int segments = 2;
    final int perSegment = 8;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment, true);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    canonicaliser.canonicalOfValue("else");
    final List<Integer> counts = new ArrayList<>();

    final ColumnSlice[] out = canonicaliser.canonicaliseColumn(leavesInMintOrder(corpus, segments, perSegment), null,
        null, recording(counts));

    assertNotNull(out, "the walk still canonicalises");
    assertNotNull(canonicaliser.lastRefusal(), "a fallback must say so");
    assertTrue(canonicaliser.lastRefusal().contains("not empty"), canonicaliser.lastRefusal());
    assertEquals(List.of(segments), counts, "one walk per segment, and no merge phase");
    assertTrue(Arrays.equals(out[0].numericValues(), out[1].numericValues()),
        "the walk merges twins across segments by hash, as before");
  }

  @Test
  @DisplayName("a rank table that disagrees with itself REFUSES the merge, and the walk answers the same question")
  void aDisagreeingRankTableRefusesTheMerge() {
    final int segments = 2;
    final int perSegment = 8;
    // Position 3 of segment 1 forgets which mint it stores.
    final PositionedCorpus forgetful = positionedCorpus(segments, perSegment, true);
    forgetful.mints().remove(ProjectionIndexRowGroupPage.packSegmentCell(1, 3));
    assertRefusedThenWalked(forgetful, segments, perSegment, "answers mint");
    // Position 3 of segment 1 claims the mint position 4 stores: one mint at two positions.
    final PositionedCorpus doubled = positionedCorpus(segments, perSegment, true);
    doubled.mints().put(ProjectionIndexRowGroupPage.packSegmentCell(1, 3),
        doubled.mints().get(ProjectionIndexRowGroupPage.packSegmentCell(1, 4)));
    assertRefusedThenWalked(doubled, segments, perSegment, "twice");
    // A sparse pass locates marks by position; a mint whose position is unknown refuses it too.
    final int large = 200;
    final PositionedCorpus positionless = positionedCorpus(segments, large, true);
    positionless.positions().remove(ProjectionIndexRowGroupPage.packSegmentCell(0, 2)); // row 1 of leaf 0
    final SegmentGroupCanonicaliser sparse = new SegmentGroupCanonicaliser(positionless, segments);
    final ColumnSlice[] out = sparse.canonicaliseColumn(leavesInMintOrder(positionless, segments, large), null,
        new long[][] {rowsKept(large, 1, 2), null}, SegmentGroupCanonicaliser.SERIAL_SEGMENTS);
    assertNotNull(out);
    assertNotNull(sparse.lastRefusal());
    assertTrue(sparse.lastRefusal().contains("answers position"), sparse.lastRefusal());
    assertEquals(2, sparse.size());
  }

  /** The merge refuses {@code corpus} with a message naming {@code why}, and the walk still answers. */
  private static void assertRefusedThenWalked(final PositionedCorpus corpus, final int segments,
      final int perSegment, final String why) {
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final List<Integer> counts = new ArrayList<>();

    final ColumnSlice[] out = canonicaliser.canonicaliseColumn(leavesInMintOrder(corpus, segments, perSegment), null,
        null, recording(counts));

    assertNotNull(out, "refused is not declined: the walk and the row loop still answer");
    assertNotNull(canonicaliser.lastRefusal());
    assertTrue(canonicaliser.lastRefusal().contains(why), canonicaliser.lastRefusal());
    assertEquals(2, counts.size(), "the merge's mark phase, then the walk — and no merge phase after the refusal");
    assertEquals(perSegment, canonicaliser.size(), "two copies of eight values are eight ids");
    assertTrue(Arrays.equals(out[0].numericValues(), out[1].numericValues()), "twins still coalesce");
  }

  @Test
  @DisplayName("observeColumn answers from the merge alone when every present kept cell was marked")
  void observeColumnSkipsTheRowLoopWhenTheMergeSettledEverything() {
    final int segments = 3;
    final int perSegment = 20;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);

    assertTrue(canonicaliser.observeColumn(leaves, null, SegmentGroupCanonicaliser.SERIAL_SEGMENTS));

    assertEquals(segments * perSegment, canonicaliser.size(), "COUNT(DISTINCT) is the merge's length");
    assertNull(canonicaliser.lastRefusal());

    // A cell no dictionary holds is unmarkable: the shortcut must not hide it, and the row loop
    // still reports it — the pass declines exactly as it does without the merge.
    final SegmentGroupCanonicaliser strict = new SegmentGroupCanonicaliser(corpus, segments);
    final long orphan = ProjectionIndexRowGroupPage.packSegmentCell(0, perSegment + 5);
    assertFalse(strict.observeColumn(new ColumnSlice[] {leaves[0], sliceOf(orphan)}, null,
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS), "an unmarkable cell still declines");
  }

  @Test
  @DisplayName("the parallel length table over merged positions equals the serial one, in both units")
  void lengthTableOverMergedPositions() {
    final int segments = 3;
    final int perSegment = 15;
    final PositionedCorpus corpus = positionedCorpus(segments, perSegment);
    // Give a few values multi-byte content so the two units differ: mint 12 sits at position 4 and
    // row 11, and a tail keeps the value between its neighbours.
    for (int segment = 0; segment < segments; segment++) {
      final long cell = ProjectionIndexRowGroupPage.packSegmentCell(segment, 12);
      corpus.values().put(cell, corpus.values().get(cell) + "é中");
    }
    final SegmentGroupCanonicaliser canonicaliser = new SegmentGroupCanonicaliser(corpus, segments);
    final ColumnSlice[] leaves = leavesInMintOrder(corpus, segments, perSegment);
    final long[][] rowKeep = {rowsKept(perSegment, 0, 4, 11), rowsKept(perSegment, 2, 11), null};
    assertNotNull(canonicaliser.canonicaliseColumn(leaves, null, rowKeep, SegmentGroupCanonicaliser.SERIAL_SEGMENTS));
    assertNull(canonicaliser.lastRefusal());
    assertEquals(5, canonicaliser.size());

    for (final byte mode : new byte[] {ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES,
        ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS}) {
      final int[] serial = canonicaliser.lengthTable(mode);
      final int[] parallel = canonicaliser.lengthTable(mode, SegmentGroupCanonicaliser.SERIAL_SEGMENTS);
      assertTrue(Arrays.equals(serial, parallel), "mode " + mode + ": " + Arrays.toString(serial) + " vs "
          + Arrays.toString(parallel));
      assertEquals(canonicaliser.size() + 1, parallel.length, "one slot per id, plus the unused zero");
      for (int id = 1; id <= canonicaliser.size(); id++) {
        final String value = canonicaliser.valueOf(id);
        final int expected = mode == ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS
            ? value.codePointCount(0, value.length())
            : value.getBytes(StandardCharsets.UTF_8).length;
        assertEquals(expected, parallel[id], "id " + id + " = '" + value + "' in mode " + mode);
      }
    }
    // Row 11 of leaf 0 is mint 12 = position 4, the value with the multi-byte tail.
    final int tailId = (int) canonicaliser.canonicalise(new ColumnSlice[] {leaves[0]}, null,
        new long[][] {rowKeep[0]}, SegmentGroupCanonicaliser.SERIAL_SEGMENTS)[0].numericValues()[11];
    assertEquals(canonicaliser.lengthTable(ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS)[tailId] + 3,
        canonicaliser.lengthTable(ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES)[tailId],
        "é is two bytes and 中 is three: five bytes for two code points");
  }
}
