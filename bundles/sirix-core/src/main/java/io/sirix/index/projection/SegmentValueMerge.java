/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.SegmentGroupCanonicaliser.CellResolver;
import io.sirix.index.projection.SegmentGroupCanonicaliser.SegmentRunner;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import static java.util.Objects.requireNonNull;

/**
 * Builds the whole value space of a segment-scoped column by MERGING its segments' sorted runs, in
 * parallel, instead of resolving one cell at a time into a hash table.
 *
 * <h2>Why a merge and not a hash</h2>
 *
 * Canonicalising a column means giving every distinct VALUE one id, however many segments carry it.
 * The per-cell path does that with a hash table under a lock: hash the cell's bytes, probe, confirm a
 * hit by re-reading both cells' bytes. Over a whole column that is one probe per referenced cell, one
 * random dictionary read per cross-segment duplicate, all serialised on one monitor — measured at
 * 100M as ~25 s of a 26 s {@code GROUP BY URL} (util 2 of 20) and ~700 MB of table.
 *
 * <p>
 * A sealed segment dictionary stores its values in COLLATION order, so the referenced cells of one
 * segment, taken in position order, are already a sorted run — and a column's value space is S such
 * runs. A total order over S sorted runs is a merge: every cross-segment duplicate is ADJACENT in the
 * merged sequence, so identity needs no hash and no lock, only a comparison with the previous winner.
 * The canonical id of a value is simply its rank in that sequence, which also makes the id space
 * collate — the seal an extremum or {@code ORDER BY} needs comes for free.
 * </p>
 *
 * <h2>Parallelism: range-partition the value space</h2>
 *
 * One merge is serial. Splitting the VALUE space into P ranges by pivots from the longest run, and
 * locating each pivot in every other run by binary search over its positions, gives P independent
 * merges over disjoint value ranges; equal values fall in the same range whatever their segment, so
 * each range deduplicates completely on its own. Ranks are local to a range and offset by a prefix
 * sum of the ranges' distinct counts afterwards.
 *
 * <p>
 * Phases, each parallel over runs or ranges through the caller's {@link SegmentRunner}:
 * <ol>
 * <li>MARK: turn each run's referenced MINTS into a bitmap over POSITIONS, sparse (one position
 * lookup per mark) or dense (one mint lookup per entry), and allocate its memo table.</li>
 * <li>BOUND: choose P-1 pivots at equal spacing among the longest run's marks; binary-search each
 * pivot's lower bound in every run.</li>
 * <li>MERGE: per range, a loser tree over the runs' marked positions in that range; the winner's
 * rank is issued on a value change and written into the run's table at the winner's mint.</li>
 * <li>OFFSET: add the range's rank base to every table entry the range wrote.</li>
 * </ol>
 * Reads stay SEQUENTIAL throughout: every run is walked in ascending position, the order its values
 * are stored in, so a decoded block serves every marked value in it before being dropped.
 * </p>
 *
 * <p>
 * Everything is built in local arrays and handed back as one {@link Result}; nothing is published to
 * the canonicaliser until the merge has completed, so a refusal or a failure in any phase leaves the
 * value space exactly as it was and the walk path takes over. Refusals are {@link Refused}: a
 * resolver that cannot answer in position space (a TRANSFORMING one never can), a rank table whose
 * two directions disagree, a value that cannot be read.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class SegmentValueMerge {

  /**
   * Marked cells per range the merge aims for. Ranges beyond the worker count balance the tail of
   * the phase; each range costs one binary search per run to bound.
   */
  static final int DEFAULT_RANGE_TARGET = 1 << 19;

  /** Ranges the merge never exceeds, whatever the target. */
  static final int MAX_RANGES = 4096;

  /** Head state of a run whose range is exhausted. */
  private static final int EXHAUSTED = -1;

  /** "No previous winner" — no packed cell is negative. */
  private static final long NO_CELL = Long.MIN_VALUE;

  /**
   * What a completed merge hands the canonicaliser.
   *
   * @param positions per run, bit {@code position} set for every referenced entry of that segment
   * @param tables per run, {@code table[mint]} = the canonical id (1-based rank), 0 where unreferenced
   * @param representatives {@code representatives[rank - 1]} = a cell carrying that rank
   * @param ranges how many value ranges the merge ran as
   * @param marked referenced cells over all runs
   */
  record Result(long[][] positions, int[][] tables, long[] representatives, int ranges, long marked) {
  }

  /** A designed refusal: the merge cannot run over this resolver or this dictionary; walk instead. */
  static final class Refused extends RuntimeException {
    private static final long serialVersionUID = 1L;

    Refused(final String why) {
      super(why, null, false, false); // no stack trace: a signal, not a fault
    }
  }

  private final CellResolver resolver;
  private final int[] segments;
  private final long[][] marks;
  private final int[] entryCounts;
  private final int runs;
  private final int rangeTarget;

  /** {@code pack(segment, 1)}: the probe {@link CellResolver#mintAtPosition} addresses a segment by. */
  private final long[] probes;

  // Phase 1.
  private final long[][] positions;
  private final int[] markedCounts;
  private final int[][] tables;

  // Phase 2. bounds[r][p] = first position of run r in range p; bounds[r][ranges] = entries + 1.
  private int ranges;
  private int[][] bounds;

  // Phase 3.
  private final int[] distinctPerRange;
  private final LongArrayList[] representativesPerRange;

  // Phase 4.
  private int[] rankBase;

  private SegmentValueMerge(final CellResolver resolver, final int[] segments, final long[][] marks,
      final int[] entryCounts, final int rangeTarget) {
    this.resolver = resolver;
    this.segments = segments;
    this.marks = marks;
    this.entryCounts = entryCounts;
    this.runs = segments.length;
    this.rangeTarget = rangeTarget;
    this.probes = new long[runs];
    for (int r = 0; r < runs; r++) {
      probes[r] = ProjectionIndexRowGroupPage.packSegmentCell(segments[r], 1);
    }
    this.positions = new long[runs][];
    this.markedCounts = new int[runs];
    this.tables = new int[runs][];
    this.distinctPerRange = new int[MAX_RANGES];
    this.representativesPerRange = new LongArrayList[MAX_RANGES];
  }

  /**
   * Merge the referenced cells of {@code segments} into one collation-ranked value space.
   *
   * @param resolver answers positions, mints and comparisons; a TRANSFORMING one refuses
   * @param segments the referenced segments, one run each
   * @param marks per run, bit {@code mint} set for every referenced mint {@code <= entryCounts[r]}
   * @param entryCounts per run, the segment dictionary's entry count
   * @param runner runs the phases' independent bodies — on the scan workers, from the planning thread
   * @param rangeTarget marked cells per value range to aim for ({@link #DEFAULT_RANGE_TARGET})
   * @param phaseNanos when non-null, receives the wall time of each of the four phases
   * @return the merged space
   * @throws Refused when the resolver or the dictionaries cannot support the merge
   */
  static Result merge(final CellResolver resolver, final int[] segments, final long[][] marks,
      final int[] entryCounts, final SegmentRunner runner, final int rangeTarget, final long[] phaseNanos) {
    requireNonNull(resolver, "resolver must not be null");
    requireNonNull(runner, "runner must not be null");
    if (segments.length == 0 || segments.length != marks.length || segments.length != entryCounts.length) {
      throw new IllegalArgumentException("runs, marks and entry counts must describe the same segments");
    }
    if (rangeTarget < 1) {
      throw new IllegalArgumentException("rangeTarget must be positive: " + rangeTarget);
    }
    if (phaseNanos != null && phaseNanos.length < 4) {
      throw new IllegalArgumentException("four phases to time");
    }
    final SegmentValueMerge merge = new SegmentValueMerge(resolver, segments, marks, entryCounts, rangeTarget);
    long t = System.nanoTime();
    runner.forEach(merge.runs, merge::markPositions);
    long marked = 0;
    for (final int count : merge.markedCounts) {
      marked += count;
    }
    if (marked > Integer.MAX_VALUE - 8) {
      throw new Refused(marked + " referenced cells: canonical ids are ints");
    }
    t = merge.phase(phaseNanos, 0, t);
    merge.bound(runner, marked);
    t = merge.phase(phaseNanos, 1, t);
    runner.forEach(merge.ranges, merge::mergeRange);
    t = merge.phase(phaseNanos, 2, t);
    final long[] representatives = merge.offset(runner);
    merge.phase(phaseNanos, 3, t);
    return new Result(merge.positions, merge.tables, representatives, merge.ranges, marked);
  }

  private long phase(final long[] phaseNanos, final int index, final long since) {
    final long now = System.nanoTime();
    if (phaseNanos != null) {
      phaseNanos[index] = now - since;
    }
    return now;
  }

  // ---------------------------------------------------------------------------------------------
  // Phase 1: referenced mints -> referenced positions, per run.
  // ---------------------------------------------------------------------------------------------

  /**
   * Turn run {@code r}'s referenced mints into a bitmap over its positions and size its memo table.
   *
   * <p>
   * Sparse or dense by the same break-even the storage-order walk uses: below one mark in
   * {@link SegmentGroupCanonicaliser#SPARSE_WALK_RATIO} entries the marks are located one by one,
   * otherwise every position is visited once. Both directions of the rank table are involved, and a
   * mint that lands nowhere or twice means the two disagree — refused, never guessed.
   * </p>
   */
  private void markPositions(final int r) {
    final long[] marked = marks[r];
    final int entries = entryCounts[r];
    final int segment = segments[r];
    if (entries < 1) {
      throw new Refused("segment " + segment + " has no walkable entries");
    }
    int markedCount = 0;
    for (final long word : marked) {
      markedCount += Long.bitCount(word);
    }
    final long[] bits = new long[(entries >>> 6) + 1];
    int landed = 0;
    if ((long) markedCount * SegmentGroupCanonicaliser.SPARSE_WALK_RATIO < entries) {
      for (int w = 0; w < marked.length; w++) {
        long word = marked[w];
        while (word != 0L) {
          final int mint = (w << 6) + Long.numberOfTrailingZeros(word);
          word &= word - 1;
          final int position = resolver.positionOfCell(ProjectionIndexRowGroupPage.packSegmentCell(segment, mint));
          if (position < 1 || position > entries) {
            throw new Refused("segment " + segment + " answers position " + position + " for mint " + mint);
          }
          final long bit = 1L << (position & 63);
          if ((bits[position >>> 6] & bit) != 0L) {
            throw new Refused("segment " + segment + " stores two mints at position " + position);
          }
          bits[position >>> 6] |= bit;
          landed++;
        }
      }
    } else {
      final int mintBits = marked.length << 6;
      final long probe = probes[r];
      // A marked mint that lands at two positions would rank one value twice and another never; the
      // count below cannot see that when both are marked, so each landing is checked against the
      // mints that landed before it.
      final long[] landedMints = new long[marked.length];
      for (int position = 1; position <= entries; position++) {
        final int mint = resolver.mintAtPosition(probe, position);
        if (mint < 1) {
          throw new Refused("segment " + segment + " answers mint " + mint + " at position " + position);
        }
        if (mint < mintBits && (marked[mint >>> 6] & (1L << (mint & 63))) != 0L) {
          final long mintBit = 1L << (mint & 63);
          if ((landedMints[mint >>> 6] & mintBit) != 0L) {
            throw new Refused("segment " + segment + " stores mint " + mint + " twice, at position " + position);
          }
          landedMints[mint >>> 6] |= mintBit;
          bits[position >>> 6] |= 1L << (position & 63);
          landed++;
        }
      }
    }
    if (landed != markedCount) {
      throw new Refused("segment " + segment + ": " + markedCount + " marked mints landed on " + landed + " positions");
    }
    positions[r] = bits;
    markedCounts[r] = markedCount;
    tables[r] = new int[entries + 1];
  }

  // ---------------------------------------------------------------------------------------------
  // Phase 2: pivots from the longest run, lower bounds in every run.
  // ---------------------------------------------------------------------------------------------

  /**
   * Split the value space into ranges: pivots at equal spacing among the longest run's marks, each
   * located in every run by a binary search over the run's positions (the whole segment is sorted,
   * not only its marked entries, so the search runs over all of them and the marks are filtered
   * afterwards by the bitmap).
   */
  private void bound(final SegmentRunner runner, final long marked) {
    int longest = 0;
    for (int r = 1; r < runs; r++) {
      if (markedCounts[r] > markedCounts[longest]) {
        longest = r;
      }
    }
    final int longestMarks = markedCounts[longest];
    int count = (int) Math.min(MAX_RANGES, Math.max(1L, marked / rangeTarget));
    count = Math.min(count, Math.max(1, longestMarks)); // a pivot per distinct mark at most
    ranges = count;
    bounds = new int[runs][count + 1];
    for (int r = 0; r < runs; r++) {
      bounds[r][0] = 1;
      bounds[r][count] = entryCounts[r] + 1;
    }
    if (count == 1) {
      return;
    }
    // Select the marks at indices j * longestMarks / count, j = 1..count-1, in one pass: the
    // targets are increasing, so a running count over the bitmap meets each in turn.
    final int[] pivotPositions = new int[count];
    final long[] pivots = new long[count];
    final long[] bits = positions[longest];
    int j = 1;
    long target = (long) j * longestMarks / count;
    long seen = 0;
    outer: for (int w = 0; w < bits.length; w++) {
      long word = bits[w];
      final int inWord = Long.bitCount(word);
      if (seen + inWord <= target) {
        seen += inWord;
        continue;
      }
      while (word != 0L) {
        final int position = (w << 6) + Long.numberOfTrailingZeros(word);
        word &= word - 1;
        if (seen == target) {
          pivotPositions[j] = position;
          pivots[j] = cellAt(longest, position);
          if (++j == count) {
            break outer;
          }
          target = (long) j * longestMarks / count;
        }
        seen++;
      }
    }
    if (j != count) {
      throw new IllegalStateException("selected " + (j - 1) + " of " + (count - 1) + " pivots");
    }
    final int pivotRun = longest;
    runner.forEach(runs, r -> boundRun(r, pivotRun, pivots, pivotPositions));
  }

  /** Lower bound of every pivot in run {@code r}: the first position whose value is not below it. */
  private void boundRun(final int r, final int pivotRun, final long[] pivots, final int[] pivotPositions) {
    final int[] bound = bounds[r];
    if (r == pivotRun) {
      // The pivots ARE this run's values, so each one's lower bound is its own position.
      System.arraycopy(pivotPositions, 1, bound, 1, ranges - 1);
      return;
    }
    final int entries = entryCounts[r];
    for (int j = 1; j < ranges; j++) {
      final long pivot = pivots[j];
      int lo = bound[j - 1]; // pivots ascend, so no bound lies below the previous one
      int hi = entries + 1;
      while (lo < hi) {
        final int mid = (lo + hi) >>> 1;
        if (resolver.compareValues(cellAt(r, mid), pivot) < 0) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      bound[j] = lo;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Phase 3: one loser-tree merge per range.
  // ---------------------------------------------------------------------------------------------

  /**
   * Merge range {@code p}: the runs' marked positions in {@code [bounds[r][p], bounds[r][p+1])}, by
   * value, issuing a range-local rank whenever the winner's value differs from the previous one.
   *
   * <p>
   * A loser tree over the runs costs exactly {@code ceil(log2 runs)} comparisons per winner, each a
   * byte comparison of two heads through the calling thread's own view. A same-segment tie is
   * impossible (a dictionary holds each value once), so the equality test against the previous
   * winner is the only cross-run identity work there is — the hash table, the lock and the confirming
   * re-read of the per-cell path all disappear.
   * </p>
   */
  private void mergeRange(final int p) {
    final int[] headPos = new int[runs];
    final long[] headCell = new long[runs];
    final int[] limit = new int[runs];
    for (int r = 0; r < runs; r++) {
      limit[r] = bounds[r][p + 1];
      final int first = nextMarked(positions[r], bounds[r][p], limit[r]);
      headPos[r] = first;
      if (first != EXHAUSTED) {
        headCell[r] = cellAt(r, first);
      }
    }
    // tree[0] is the winner; tree[1..runs-1] hold the loser of each internal match. Leaf r is node
    // runs + r, so its ancestors are (runs + r) >>> 1, then >>> 1 down to node 1.
    final int[] tree = new int[runs];
    {
      final int[] winners = new int[2 * runs];
      for (int r = 0; r < runs; r++) {
        winners[runs + r] = r;
      }
      for (int node = runs - 1; node >= 1; node--) {
        final int a = winners[node << 1];
        final int b = winners[(node << 1) + 1];
        if (before(a, b, headPos, headCell)) {
          winners[node] = a;
          tree[node] = b;
        } else {
          winners[node] = b;
          tree[node] = a;
        }
      }
      tree[0] = winners[1]; // the root's winner; with one run, its only leaf
    }
    final LongArrayList representatives = new LongArrayList();
    int rank = 0;
    long lastCell = NO_CELL;
    while (true) {
      final int w = tree[0];
      if (headPos[w] == EXHAUSTED) {
        break; // the tree's winner is exhausted: so is every run
      }
      final long cell = headCell[w];
      if (lastCell == NO_CELL || resolver.compareValues(cell, lastCell) != 0) {
        rank++;
        representatives.add(cell);
        lastCell = cell;
      }
      tables[w][ProjectionIndexRowGroupPage.idOfCell(cell)] = rank;
      final int next = nextMarked(positions[w], headPos[w] + 1, limit[w]);
      headPos[w] = next;
      if (next != EXHAUSTED) {
        headCell[w] = cellAt(w, next);
      }
      int winner = w;
      for (int node = (w + runs) >>> 1; node >= 1; node >>>= 1) {
        final int loser = tree[node];
        if (before(loser, winner, headPos, headCell)) {
          tree[node] = winner;
          winner = loser;
        }
      }
      tree[0] = winner;
    }
    distinctPerRange[p] = rank;
    representativesPerRange[p] = representatives;
  }

  /** Whether run {@code a}'s head orders before run {@code b}'s: exhausted heads sink, ties by run. */
  private boolean before(final int a, final int b, final int[] headPos, final long[] headCell) {
    if (headPos[a] == EXHAUSTED) {
      return false;
    }
    if (headPos[b] == EXHAUSTED) {
      return true;
    }
    final int order = resolver.compareValues(headCell[a], headCell[b]);
    return order != 0
        ? order < 0
        : a < b;
  }

  // ---------------------------------------------------------------------------------------------
  // Phase 4: range-local ranks -> global ranks.
  // ---------------------------------------------------------------------------------------------

  /** Offset every range's ranks by the distinct counts before it; the representatives, in rank order. */
  private long[] offset(final SegmentRunner runner) {
    rankBase = new int[ranges + 1];
    for (int p = 0; p < ranges; p++) {
      rankBase[p + 1] = rankBase[p] + distinctPerRange[p];
    }
    final int distinct = rankBase[ranges];
    final long[] representatives = new long[distinct];
    for (int p = 0; p < ranges; p++) {
      final LongArrayList mine = representativesPerRange[p];
      System.arraycopy(mine.elements(), 0, representatives, rankBase[p], mine.size());
      representativesPerRange[p] = null; // let the copy be the only one
    }
    if (ranges > 1) {
      runner.forEach(ranges - 1, i -> offsetRange(i + 1)); // range 0 has base 0
    }
    return representatives;
  }

  /** Re-walk range {@code p}'s marked positions, adding its base to the rank stored at each mint. */
  private void offsetRange(final int p) {
    final int base = rankBase[p];
    if (base == 0) {
      return;
    }
    for (int r = 0; r < runs; r++) {
      final long[] bits = positions[r];
      final int[] table = tables[r];
      final long probe = probes[r];
      final int limit = bounds[r][p + 1];
      for (int position = nextMarked(bits, bounds[r][p], limit); position != EXHAUSTED;
          position = nextMarked(bits, position + 1, limit)) {
        table[mintAt(r, probe, position)] += base;
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------------------------

  /** The first marked position in {@code [from, limit)}, or {@link #EXHAUSTED}. */
  static int nextMarked(final long[] bits, final int from, final int limit) {
    if (from >= limit) {
      return EXHAUSTED;
    }
    int w = from >>> 6;
    if (w >= bits.length) {
      return EXHAUSTED;
    }
    long word = bits[w] & (-1L << (from & 63));
    while (true) {
      if (word != 0L) {
        final int position = (w << 6) + Long.numberOfTrailingZeros(word);
        return position < limit
            ? position
            : EXHAUSTED;
      }
      if (++w >= bits.length) {
        return EXHAUSTED;
      }
      word = bits[w];
    }
  }

  /** The cell stored at {@code position} of run {@code r}. */
  private long cellAt(final int r, final int position) {
    return ProjectionIndexRowGroupPage.packSegmentCell(segments[r], mintAt(r, probes[r], position));
  }

  /** The mint stored at {@code position} of run {@code r}, refused when it is not one of the run's. */
  private int mintAt(final int r, final long probe, final int position) {
    final int mint = resolver.mintAtPosition(probe, position);
    if (mint < 1 || mint > entryCounts[r]) {
      throw new Refused("segment " + segments[r] + " answers mint " + mint + " at position " + position);
    }
    return mint;
  }
}
