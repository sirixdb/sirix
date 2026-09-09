/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.SegmentGroupCanonicaliser.CellResolver;
import io.sirix.index.projection.SegmentGroupCanonicaliser.SegmentRunner;
import io.sirix.node.ValueDictionaryEntryNode;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Builds the whole value space of a segment-scoped column by MERGING its segments' sorted runs, in
 * parallel, instead of resolving one cell at a time into a hash table.
 *
 * <h2>Why a merge and not a hash</h2>
 *
 * Canonicalising a column means giving every distinct VALUE one id, however many segments carry it.
 * The per-cell path does that with a hash table under a lock: hash the cell's bytes, probe, confirm
 * a hit by re-reading both cells' bytes. Over a whole column that is one probe per referenced cell,
 * one random dictionary read per cross-segment duplicate, all serialised on one monitor — measured
 * at 100M as ~25 s of a 26 s {@code GROUP BY URL} (util 2 of 20) and ~700 MB of table.
 *
 * <p>
 * A sealed segment dictionary stores its values in COLLATION order, so the referenced cells of one
 * segment, taken in position order, are already a sorted run — and a column's value space is S such
 * runs. A total order over S sorted runs is a merge: every cross-segment duplicate is ADJACENT in
 * the merged sequence, so identity needs no hash and no lock, only a comparison with the previous
 * winner. The canonical id of a value is simply its rank in that sequence, which also makes the id
 * space collate — the seal an extremum or {@code ORDER BY} needs comes for free.
 * </p>
 *
 * <h2>Parallelism: range-partition the value space</h2>
 *
 * One merge is serial. Splitting the VALUE space into P ranges by pivots, and locating each pivot
 * in every run by a search over its positions, gives P independent merges over disjoint value
 * ranges; equal values fall in the same range whatever their segment, so each range deduplicates
 * completely on its own. Ranks are local to a range and offset by a prefix sum of the ranges'
 * distinct counts afterwards.
 *
 * <p>
 * The pivots come from a REGULAR SAMPLE of every run — every {@code stride}-th marked value, so a
 * run's share of the sample is its share of the cells — gathered, sorted, and cut at equal
 * intervals (parallel sorting by regular sampling). Pivots taken from one run alone were measured
 * to fail at 100M: the rows arrive grouped by site, so one segment's URLs are one domain's, and
 * pivots spaced evenly through the longest run's domain left every other domain in a single range
 * that ran for 1.9 s of a 2.0 s phase while 181 ranges finished in 70 ms each. A regular sample
 * bounds every range at about twice the mean, whatever the runs hold.
 * </p>
 *
 * <p>
 * Phases, each parallel over runs or ranges through the caller's {@link SegmentRunner}:
 * <ol>
 * <li>MARK: turn each run's referenced MINTS into a bitmap over POSITIONS, sparse (one position
 * lookup per mark) or dense (one mint lookup per entry), and allocate its memo table.</li>
 * <li>BOUND: sample every run's marks at a regular stride, sort the samples, cut P-1 pivots at
 * equal intervals; locate each pivot's lower bound in every run by a galloping search from the
 * previous pivot's bound.</li>
 * <li>MERGE: per range, a loser tree over the runs' marked positions in that range; the winner's
 * rank is issued on a value change and written into the run's table at the winner's mint.</li>
 * <li>OFFSET: per run, add each range's rank base to the table entries the range wrote.</li>
 * </ol>
 * </p>
 *
 * <h2>Reads: a cursor per run, never the per-cell path</h2>
 *
 * Every run is walked in ascending position, the order its values are stored in, through a
 * {@link SegmentRunCursor} that holds the block and the inverse rank-table record it is in and
 * reads the next value off them. The heads are compared as the byte slices the cursors hold.
 * Nothing goes through the read view's per-mint route — that route translates a mint to its
 * position through the forward table (a random read per call), keeps its slices in a cache keyed by
 * mint (which random mints thrash into a block re-fetch per compare) and re-checks the reader's
 * revision every time; measured at 100M on {@code GROUP BY URL} it was three quarters of the
 * merge's CPU while the byte comparison was a fifth. With cursors a block is decoded once per range
 * it straddles and a compare is a compare.
 *
 * <p>
 * Everything is built in local arrays and handed back as one {@link Result}; nothing is published
 * to the canonicaliser until the merge has completed, so a refusal or a failure in any phase leaves
 * the value space exactly as it was and the walk path takes over. Refusals are {@link Refused}: a
 * resolver that cannot answer in position space or hand out a cursor (a TRANSFORMING one never
 * can), a rank table whose two directions disagree, a value that cannot be read.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class SegmentValueMerge {

  /**
   * Marked cells per range the merge aims for. Ranges beyond the worker count balance the tail of the
   * phase — the ranges are equal only on the run the pivots came from, so a phase of as many ranges
   * as workers ends when its slowest one does; each range costs one binary search per run to bound
   * and one block re-decode per run at its boundary.
   */
  static final int DEFAULT_RANGE_TARGET = 1 << 17;

  /** Ranges the merge never exceeds, whatever the target. */
  static final int MAX_RANGES = 4096;

  /**
   * Samples the pivot selection draws per range it will cut: a range's size is off its mean by about
   * the sample's spacing, so this many samples per range bound the error to a sixteenth of a range.
   */
  static final int SAMPLES_PER_RANGE = 16;

  /** Head state of a run whose range is exhausted. */
  private static final int EXHAUSTED = -1;

  /**
   * What a completed merge hands the canonicaliser.
   *
   * @param positions per run, bit {@code position} set for every referenced entry of that segment
   * @param tables per run, {@code table[mint]} = the canonical id (1-based rank), 0 where
   *        unreferenced
   * @param representatives {@code representatives[rank - 1]} = a cell carrying that rank
   * @param ranges how many value ranges the merge ran as
   * @param marked referenced cells over all runs
   * @param rangeNanos per range, the wall time of its merge — the phase's balance, for the diagnostic
   * @param rangeCells per range, the marked cells it merged — whether the pivots balanced the work
   * @param loads records the merge phase's cursors fetched, summed — a block per range it straddles
   */
  record Result(long[][] positions, int[][] tables, long[] representatives, int ranges, long marked, long[] rangeNanos,
      int[] rangeCells, long loads) {
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

  /**
   * {@code pack(segment, 1)}: the cell {@link CellResolver#cursorOfSegment} addresses a segment by.
   */
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
  private final long[] rangeNanos;
  private final int[] rangeCells;
  private final long[] rangeLoads;
  private long marked;

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
    this.rangeNanos = new long[MAX_RANGES];
    this.rangeCells = new int[MAX_RANGES];
    this.rangeLoads = new long[MAX_RANGES];
  }

  /**
   * Merge the referenced cells of {@code segments} into one collation-ranked value space.
   *
   * @param resolver answers positions, mints, and cursors over the runs; a TRANSFORMING one refuses
   * @param segments the referenced segments, one run each
   * @param marks per run, bit {@code mint} set for every referenced mint {@code <= entryCounts[r]}
   * @param entryCounts per run, the segment dictionary's entry count
   * @param runner runs the phases' independent bodies — on the scan workers, from the planning thread
   * @param rangeTarget marked cells per value range to aim for ({@link #DEFAULT_RANGE_TARGET})
   * @param phaseNanos when non-null, receives the wall time of each of the four phases
   * @return the merged space
   * @throws Refused when the resolver or the dictionaries cannot support the merge
   */
  static Result merge(final CellResolver resolver, final int[] segments, final long[][] marks, final int[] entryCounts,
      final SegmentRunner runner, final int rangeTarget, final long[] phaseNanos) {
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
    merge.marked = marked;
    t = merge.phase(phaseNanos, 0, t);
    merge.bound(runner, marked);
    t = merge.phase(phaseNanos, 1, t);
    runner.forEach(merge.ranges, merge::mergeRange);
    t = merge.phase(phaseNanos, 2, t);
    final long[] representatives = merge.offset(runner);
    merge.phase(phaseNanos, 3, t);
    long loads = 0;
    for (int p = 0; p < merge.ranges; p++) {
      loads += merge.rangeLoads[p];
    }
    return new Result(merge.positions, merge.tables, representatives, merge.ranges, marked,
        Arrays.copyOf(merge.rangeNanos, merge.ranges), Arrays.copyOf(merge.rangeCells, merge.ranges), loads);
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
   * {@link SegmentGroupCanonicaliser#SPARSE_WALK_RATIO} entries the marks are located one by one
   * through the forward table, otherwise every position's mint is read off the inverse table in
   * order. Both directions of the rank table are involved, and a mint that lands nowhere or twice
   * means the two disagree — refused, never guessed.
   * </p>
   */
  private void markPositions(final int r) {
    final long[] marked = marks[r];
    final int entries = entryCounts[r];
    final int segment = segments[r];
    if (entries < 1) {
      throw new Refused("segment " + segment + " has no walkable entries");
    }
    final SegmentRunCursor cursor = cursorOf(r);
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
      // A marked mint that lands at two positions would rank one value twice and another never; the
      // count below cannot see that when both are marked, so each landing is checked against the
      // mints that landed before it.
      final long[] landedMints = new long[marked.length];
      for (int position = 1; position <= entries; position++) {
        final int mint = mintAt(r, cursor, position);
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
  // Phase 2: pivots from a regular sample of every run, lower bounds in every run.
  // ---------------------------------------------------------------------------------------------

  /**
   * Split the value space into ranges. Every run contributes every {@code stride}-th of its marked
   * values (its cursor walking upward, a block per sample), the samples are sorted, and the pivots
   * are the samples at equal intervals — so each range holds about the same number of CELLS over all
   * runs together, not the same number of one run's. Equal pivots (a value many runs sampled)
   * collapse into one, so the ranges may come out fewer than aimed for. Each pivot is then located in
   * every run as a lower bound (the whole segment is sorted, not only its marked entries, so the
   * search runs over all of them and the marks are filtered afterwards by the bitmap).
   */
  private void bound(final SegmentRunner runner, final long marked) {
    int longestMarks = 0;
    for (int r = 0; r < runs; r++) {
      longestMarks = Math.max(longestMarks, markedCounts[r]);
    }
    int count = (int) Math.min(MAX_RANGES, Math.max(1L, marked / rangeTarget));
    count = Math.min(count, Math.max(1, longestMarks)); // a pivot per distinct mark at most
    if (count > 1) {
      final int stride = (int) Math.max(1L, marked / ((long) count * SAMPLES_PER_RANGE));
      final byte[][][] samples = new byte[runs][][];
      runner.forEach(runs, r -> samples[r] = sampleRun(r, stride));
      int total = 0;
      for (final byte[][] mine : samples) {
        total += mine.length;
      }
      final byte[][] sorted = new byte[total][];
      int at = 0;
      for (final byte[][] mine : samples) {
        System.arraycopy(mine, 0, sorted, at, mine.length);
        at += mine.length;
      }
      Arrays.sort(sorted, SegmentValueMerge::compareValues);
      final byte[][] pivots = new byte[count][];
      int kept = 0;
      for (int j = 1; j < count && total > 0; j++) {
        final byte[] pivot = sorted[(int) ((long) j * total / count)];
        if (kept == 0 || compareValues(pivots[kept], pivot) != 0) {
          pivots[++kept] = pivot;
        }
      }
      count = kept + 1;
      ranges = count;
      allocateBounds();
      if (count > 1) {
        runner.forEach(runs, r -> boundRun(r, pivots));
      }
      return;
    }
    ranges = 1;
    allocateBounds();
  }

  private void allocateBounds() {
    bounds = new int[runs][ranges + 1];
    for (int r = 0; r < runs; r++) {
      bounds[r][0] = 1;
      bounds[r][ranges] = entryCounts[r] + 1;
    }
  }

  /**
   * Run {@code r}'s regular sample: the values of its marks at indices {@code stride/2 + i*stride},
   * read in position order — the cursor walks upward and a block is decoded once however many samples
   * it holds.
   */
  private byte[][] sampleRun(final int r, final int stride) {
    final int markedCount = markedCounts[r];
    final int first = stride >>> 1;
    if (markedCount <= first) {
      return new byte[0][];
    }
    final byte[][] samples = new byte[(markedCount - first - 1) / stride + 1][];
    final long[] bits = positions[r];
    final int limit = entryCounts[r] + 1;
    final SegmentRunCursor cursor = cursorOf(r);
    int index = 0; // of the mark about to be visited, among the run's marks
    int next = first; // index of the next mark to sample
    int taken = 0;
    for (int position = nextMarked(bits, 1, limit); position != EXHAUSTED && taken < samples.length; position =
        nextMarked(bits, position + 1, limit), index++) {
      if (index == next) {
        cursor.seek(position);
        samples[taken++] = cursor.copyValue();
        next += stride;
      }
    }
    if (taken != samples.length) {
      throw new IllegalStateException("run " + r + " sampled " + taken + " of " + samples.length);
    }
    return samples;
  }

  /**
   * Lower bound of every pivot in run {@code r}: the first position whose value is not below it.
   * Pivots ascend and consecutive ones are near in every run, so each search gallops from the
   * previous bound (probes at doubling distances, mostly inside the block the cursor already holds)
   * and then bisects the interval the gallop closed.
   */
  private void boundRun(final int r, final byte[][] pivots) {
    final int[] bound = bounds[r];
    final int entries = entryCounts[r];
    final SegmentRunCursor cursor = cursorOf(r);
    for (int j = 1; j < ranges; j++) {
      final byte[] pivot = pivots[j];
      int lo = bound[j - 1];
      int hi = entries + 1;
      int probe = lo;
      int step = 1;
      while (probe < hi) {
        cursor.seek(probe);
        if (SegmentRunCursor.compareToRange(cursor, pivot, 0, pivot.length) < 0) {
          lo = probe + 1;
          probe = lo + step;
          step <<= 1;
        } else {
          hi = probe;
          break;
        }
      }
      while (lo < hi) {
        final int mid = (lo + hi) >>> 1;
        cursor.seek(mid);
        if (SegmentRunCursor.compareToRange(cursor, pivot, 0, pivot.length) < 0) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      bound[j] = lo;
    }
  }

  /** Collation order of two whole values. */
  private static int compareValues(final byte[] left, final byte[] right) {
    return ValueDictionaryEntryNode.compareUtf16Range(left, 0, left.length, right, 0, right.length);
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
   * byte comparison of the two slices the runs' cursors hold. A same-segment tie is impossible (a
   * dictionary holds each value once), so the equality test against the previous winner — whose slice
   * is kept in locals, since its cursor has moved on — is the only cross-run identity work there is:
   * the hash table, the lock and the confirming re-read of the per-cell path all disappear.
   * </p>
   */
  private void mergeRange(final int p) {
    final long started = System.nanoTime();
    final SegmentRunCursor[] cursors = new SegmentRunCursor[runs];
    final int[] headPos = new int[runs];
    final int[] limit = new int[runs];
    for (int r = 0; r < runs; r++) {
      limit[r] = bounds[r][p + 1];
      final int first = nextMarked(positions[r], bounds[r][p], limit[r]);
      headPos[r] = first;
      if (first != EXHAUSTED) {
        final SegmentRunCursor cursor = cursorOf(r);
        cursor.seek(first);
        cursors[r] = cursor;
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
        if (before(a, b, headPos, cursors)) {
          winners[node] = a;
          tree[node] = b;
        } else {
          winners[node] = b;
          tree[node] = a;
        }
      }
      tree[0] = winners[1]; // the root's winner; with one run, its only leaf
    }
    final LongArrayList representatives = new LongArrayList((int) Math.max(16L, marked / ranges));
    int rank = 0;
    int cells = 0;
    boolean haveLast = false;
    byte[] lastBacking = null;
    int lastOffset = 0;
    int lastLength = 0;
    ValueDictionaryEntryNode lastSpill = null;
    while (true) {
      final int w = tree[0];
      if (headPos[w] == EXHAUSTED) {
        break; // the tree's winner is exhausted: so is every run
      }
      final SegmentRunCursor head = cursors[w];
      final int mint = mintAt(w, head, headPos[w]);
      if (!haveLast || SegmentRunCursor.compare(head.backing, head.offset, head.length, head.spill, lastBacking,
          lastOffset, lastLength, lastSpill) != 0) {
        rank++;
        representatives.add(ProjectionIndexRowGroupPage.packSegmentCell(segments[w], mint));
        lastBacking = head.backing;
        lastOffset = head.offset;
        lastLength = head.length;
        lastSpill = head.spill;
        haveLast = true;
      }
      tables[w][mint] = rank;
      cells++;
      final int next = nextMarked(positions[w], headPos[w] + 1, limit[w]);
      headPos[w] = next;
      if (next != EXHAUSTED) {
        head.seek(next);
      }
      int winner = w;
      for (int node = (w + runs) >>> 1; node >= 1; node >>>= 1) {
        final int loser = tree[node];
        if (before(loser, winner, headPos, cursors)) {
          tree[node] = winner;
          winner = loser;
        }
      }
      tree[0] = winner;
    }
    distinctPerRange[p] = rank;
    representativesPerRange[p] = representatives;
    rangeCells[p] = cells;
    long loads = 0;
    for (final SegmentRunCursor cursor : cursors) {
      if (cursor != null) {
        loads += cursor.loads();
      }
    }
    rangeLoads[p] = loads;
    rangeNanos[p] = System.nanoTime() - started;
  }

  /**
   * Whether run {@code a}'s head orders before run {@code b}'s: exhausted heads sink, ties by run.
   */
  private static boolean before(final int a, final int b, final int[] headPos, final SegmentRunCursor[] cursors) {
    if (headPos[a] == EXHAUSTED) {
      return false;
    }
    if (headPos[b] == EXHAUSTED) {
      return true;
    }
    final int order = SegmentRunCursor.compare(cursors[a], cursors[b]);
    return order != 0
        ? order < 0
        : a < b;
  }

  // ---------------------------------------------------------------------------------------------
  // Phase 4: range-local ranks -> global ranks.
  // ---------------------------------------------------------------------------------------------

  /**
   * Offset every range's ranks by the distinct counts before it; the representatives, in rank order.
   */
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
      runner.forEach(runs, this::offsetRun); // range 0 has base 0, so its entries are already final
    }
    return representatives;
  }

  /**
   * Re-walk run {@code r}'s marked positions above range 0, adding each range's base to the rank
   * stored at the position's mint. One run per body rather than one range: the walk is then one
   * ascending pass over the run's inverse table, and the table it updates is the run's own, which
   * stays cache-resident where a range's body would touch every run's.
   */
  private void offsetRun(final int r) {
    final long[] bits = positions[r];
    final int[] table = tables[r];
    final int[] bound = bounds[r];
    final SegmentRunCursor cursor = cursorOf(r);
    for (int p = 1; p < ranges; p++) {
      final int base = rankBase[p];
      if (base == 0) {
        continue;
      }
      final int limit = bound[p + 1];
      for (int position = nextMarked(bits, bound[p], limit); position != EXHAUSTED; position =
          nextMarked(bits, position + 1, limit)) {
        table[mintAt(r, cursor, position)] += base;
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

  /**
   * A fresh cursor over run {@code r}'s positions, refused when the resolver cannot walk the segment.
   */
  private SegmentRunCursor cursorOf(final int r) {
    final SegmentRunCursor cursor = resolver.cursorOfSegment(probes[r]);
    if (cursor == null) {
      throw new Refused("segment " + segments[r] + " offers no cursor over its positions");
    }
    return cursor;
  }

  /**
   * The mint stored at {@code position} of run {@code r}, refused when it is not one of the run's.
   */
  private int mintAt(final int r, final SegmentRunCursor cursor, final int position) {
    final int mint = cursor.mintAt(position);
    if (mint < 1 || mint > entryCounts[r]) {
      throw new Refused("segment " + segments[r] + " answers mint " + mint + " at position " + position);
    }
    return mint;
  }
}
