/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/**
 * Turns a segment-scoped column's cells into CANONICAL VALUE IDS, so a kernel that groups integers
 * groups by value rather than by {@code (segment, value)}.
 *
 * <h2>Why a kernel cannot just group on the cell</h2>
 *
 * Grouping by the packed cell is a pre-aggregation: one value occurring in two segments carries a
 * different cell in each and comes back as two groups. Where nothing is dropped that is harmless —
 * merge the keys afterwards and the counts add up. Where a TOP-K prunes first it is not: the two
 * halves of a value compete separately and can both lose to a value that happens to sit in one
 * segment, so the answer is a plausible, wrong top-K. Canonicalising before the kernel puts the
 * merge on the right side of the pruning.
 *
 * <h2>What it costs</h2>
 *
 * One dictionary resolve and one hash per DISTINCT CELL, cached for the whole query — never per
 * row. A leaf holds at most {@link ProjectionIndexRowGroupPage#MAX_ROWS} rows and therefore at most
 * that many distinct cells, and repeats across leaves cost a single {@code long} lookup. The
 * per-row work is one array read and one map lookup into a table that stays in cache.
 *
 * <h2>Threading: a lock-free steady state</h2>
 *
 * The aggregation pass is parallel over leaves, so a shared map guarded by one monitor would put a
 * global lock in front of every distinct cell — roughly a million acquisitions across a 1M scan,
 * all contended by fifteen workers. Instead the memo is a per-segment {@code int[]} indexed by the
 * cell's ID, which is dense because ids are arrival-order mints. A hit is two array reads and no
 * lock at all; only the FIRST touch of a given {@code (segment, id)} takes the monitor, to resolve
 * the value and issue its canonical id.
 *
 * <p>
 * A stale read of a table entry is benign: {@code int} writes never tear, so a reader sees either 0
 * (and takes the slow path, which is idempotent under the lock and returns the same id) or a
 * complete, correct id. The volatile publication of the table array gives the happens-before edge
 * that makes a filled entry visible without a fence on the read path.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentGroupCanonicaliser {

  /** Answer for a cell whose value this revision cannot resolve. */
  private static final int UNRESOLVABLE = -1;

  private static final boolean PROJ_DIAG = Boolean.getBoolean("sirix.projDiag");

  /** {@link CellResolver#positionOfCell} could not answer: the merge seal declines. */
  public static final int NO_POSITION = -1;

  /**
   * How a packed cell becomes a value. Narrowed to this one operation so the merge semantics can be
   * tested without standing up a dictionary, and because the slow path is the only caller — the
   * indirection is never on the row path.
   */
  @FunctionalInterface
  public interface CellResolver {
    /** The value {@code cell} names, or {@code null} when this revision cannot resolve it. */
    @Nullable
    String valueOfCell(long cell);

    /**
     * A content hash of that value, {@code 0} when the cell resolves to nothing.
     *
     * <p>
     * The default builds the String, which is what a TRANSFORMING resolver has to do anyway. A plain
     * one overrides it to hash the dictionary's stored bytes and allocate nothing — the difference
     * between 150 bytes per distinct value and none, which at eighteen million values is the difference
     * between running and an OutOfMemoryError.
     * </p>
     */
    default long hashOfCell(final long cell) {
      return valueHash(valueOfCell(cell));
    }

    /** Whether two cells name the SAME value — the check that keeps a hash collision harmless. */
    default boolean sameValue(final long left, final long right) {
      final String a = valueOfCell(left);
      return a != null && a.equals(valueOfCell(right));
    }

    /**
     * The storage POSITION of {@code cell} within its own segment, or {@link #NO_POSITION} when this
     * resolver cannot answer in position space.
     *
     * <p>
     * The default refuses, and a TRANSFORMING resolver must keep that refusal: positions order the
     * dictionary's stored values, and a transform (a regex replacement, say) reorders them arbitrarily,
     * so a position says nothing about where the transformed value collates. Only the untransformed
     * byte resolver overrides it.
     * </p>
     */
    default int positionOfCell(final long cell) {
      return NO_POSITION;
    }

    /** Entries in {@code cell}'s segment dictionary, or {@code -1} when it cannot be walked. */
    default int entryCountOfSegment(final long cell) {
      return -1;
    }

    /** The mint stored at {@code position} of {@code cell}'s segment; only valid when walkable. */
    default int mintAtPosition(final long cell, final int position) {
      return -1;
    }

    /**
     * A sequential cursor over the storage positions of {@code cell}'s segment, or {@code null} when
     * this resolver cannot walk it — the merge's reader ({@link SegmentValueMerge}).
     *
     * <p>
     * The default refuses, for the reason {@link #positionOfCell} does: a TRANSFORMING resolver's
     * values are not the stored ones and do not collate as the positions do. Only the untransformed
     * byte resolver overrides it.
     * </p>
     */
    default @Nullable SegmentRunCursor cursorOfSegment(final long cell) {
      return null;
    }

    /**
     * The length of the value {@code cell} names in {@code lengthMode}'s unit, or {@code -1} when the
     * cell resolves to nothing. The default measures the String; a plain resolver counts the
     * dictionary's stored bytes and allocates nothing.
     */
    default int valueLengthOfCell(final long cell, final byte lengthMode) {
      final String value = valueOfCell(cell);
      if (value == null) {
        return -1;
      }
      return lengthMode == ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS
          ? value.codePointCount(0, value.length())
          : utf8Length(value);
    }

    /**
     * Order two cells by the values they name, under the dictionary's collation.
     *
     * <p>
     * The default compares Strings, which is UTF-16 code-unit order — the same order
     * {@code compareUtf16Range} imposes on the dictionary's storage, so the two agree. A plain resolver
     * overrides it with {@code compareCells} and touches no String at all.
     * </p>
     */
    default int compareValues(final long left, final long right) {
      final String a = valueOfCell(left);
      final String b = valueOfCell(right);
      if (a == null || b == null) {
        return a == null
            ? (b == null
                ? 0
                : -1)
            : 1;
      }
      return a.compareTo(b);
    }
  }

  private final CellResolver resolver;

  /** Physical positions are useful for a transform's reads, but never order its output values. */
  private final CellResolver storageResolver;
  private final @Nullable SegmentValueTransform transformed;

  private static long valueHash(final @Nullable String value) {
    if (value == null) {
      return 0L;
    }
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    final long hash = ProjectionIndexByteScan.fnv1a64(utf8, 0, utf8.length);
    return hash == 0L
        ? 1L
        : hash;
  }

  /**
   * {@code memo[segment][id]} is that cell's canonical id, {@code 0} when it has never been resolved.
   * Written only under this instance's monitor; read without one.
   */
  @SuppressWarnings("VolatileArrayField") // the volatile is on the reference, which is what publishes
  private volatile int[] @Nullable [] memo;

  /**
   * Content hash of a value to the canonical ids carrying it — what merges two segments' cells into
   * one group.
   *
   * <p>
   * Keyed by a HASH of the stored bytes rather than by a materialised {@link String}: one String per
   * distinct value costs about 150 bytes and a GC-visible object, which is nothing at a million
   * distinct values and 2.7 GB at eighteen million — measured, as an OutOfMemoryError at 100M. The
   * chain behind each hash keeps the identity EXACT: a hit is confirmed by comparing the two cells'
   * bytes, so a collision costs one extra compare and never merges two values.
   * </p>
   */
  private final Long2ObjectOpenHashMap<int[]> idsByHash = new Long2ObjectOpenHashMap<>();

  /**
   * A cell that carries canonical id {@code i}, at {@code i - 1}. Eight bytes standing in for the
   * value itself: the bytes stay in the dictionary, and only a WINNER is ever turned into a String.
   */
  private final LongArrayList representativeCell = new LongArrayList();

  /** Ids issued for LITERALS, which no cell carries; a conditional key's else branch needs one. */
  private final Object2IntOpenHashMap<String> canonicalByLiteral = new Object2IntOpenHashMap<>();

  private final Int2ObjectOpenHashMap<String> literalById = new Int2ObjectOpenHashMap<>();

  /**
   * After {@link #sealOrderPreserving}: {@code rankByArrival[arrivalId - 1]} is the id that lane
   * consumers see, ordered by VALUE. {@code null} while ids are arrival-ordered.
   */
  private int @Nullable [] rankByArrival;

  /** After sealing: {@code sortedArrival[rank - 1]} is the arrival id ranked there. */
  private int @Nullable [] sortedArrival;

  /**
   * After {@link SegmentValueMerge} built the value space: how many ids it issued, in COLLATION
   * order, at {@code 1..rankedCount}. Ids beyond it are later arrivals, in arrival order. {@code -1}
   * while no merge has run.
   */
  private volatile int rankedCount = -1;

  /**
   * Set by {@link #sealOrderPreserving} when the merge's ranks ARE the seal: nothing arrived after
   * the merge, so the ids the lanes carry already collate and no rank table is needed. From then on
   * an arrival above {@link #rankedCount} has no rank and is reported unresolvable, exactly as one
   * above {@link #rankByArrival}'s length is.
   */
  private volatile boolean sealedByMerge;

  /**
   * The segments the merge ranked, and per segment the POSITIONS it referenced; for the length table.
   */
  private int @Nullable [] mergedSegments;

  private long @Nullable [] @Nullable [] mergedPositions;

  /**
   * Marked cells per value range the merge aims for; package-private so a test can force many ranges.
   */
  private int mergeRangeTarget = SegmentValueMerge.DEFAULT_RANGE_TARGET;

  /**
   * @param views supplier of a view PRIVATE TO THE CALLING THREAD — a
   *        {@link GlobalValueDictionary.ReadView} holds plain mutable caches, and the resolver runs
   *        on every scan worker, so a supplier that hands the same view to two of them tears its
   *        state
   * @param segments how many segments the resource sealed, so the memo is sized once rather than
   *        grown under contention; a cell above it still resolves, through a grow on the slow path
   */
  public SegmentGroupCanonicaliser(final Supplier<GlobalValueDictionary.ReadView> views, final int segments) {
    this(byteResolver(requireNonNull(views, "views must not be null")), segments);
  }

  /**
   * Group by a deterministic, thread-safe string transform, evaluating selected dictionary entries on
   * the segment workers before mapping rows through their cell ids. Retained output strings are
   * bounded independently of input cardinality; ordinary untransformed grouping allocates no cache.
   */
  public static SegmentGroupCanonicaliser transforming(final Supplier<GlobalValueDictionary.ReadView> views,
      final int segments, final UnaryOperator<String> transform) {
    return new SegmentGroupCanonicaliser(byteResolver(requireNonNull(views, "views must not be null")), segments,
        transform, SegmentValueTransform.CACHE_BYTES);
  }

  SegmentGroupCanonicaliser(final CellResolver source, final int segments, final UnaryOperator<String> transform,
      final long cacheBytes) {
    this(new SegmentValueTransform(source, transform, cacheBytes), source, segments);
  }

  /**
   * The untransformed resolver: hashes and compares the dictionary's own bytes, allocating nothing.
   */
  private static CellResolver byteResolver(final Supplier<GlobalValueDictionary.ReadView> views) {
    return new CellResolver() {
      @Override
      public @Nullable String valueOfCell(final long cell) {
        return views.get().valueOfCell(cell); // winners only
      }

      @Override
      public long hashOfCell(final long cell) {
        return views.get().cellHash(cell);
      }

      @Override
      public boolean sameValue(final long left, final long right) {
        return views.get().compareCells(left, right) == 0;
      }

      @Override
      public int compareValues(final long left, final long right) {
        return views.get().compareCells(left, right);
      }

      @Override
      public int positionOfCell(final long cell) {
        return views.get().positionOfCell(cell);
      }

      @Override
      public int entryCountOfSegment(final long cell) {
        return views.get().segmentEntryCount(cell);
      }

      @Override
      public int mintAtPosition(final long cell, final int position) {
        return views.get().mintAtPositionOfCell(cell, position);
      }

      @Override
      public @Nullable SegmentRunCursor cursorOfSegment(final long cell) {
        return views.get().positionCursorOfCell(cell);
      }

      @Override
      public int valueLengthOfCell(final long cell, final byte lengthMode) {
        return views.get().valueLengthOfCell(cell, lengthMode);
      }
    };
  }

  /**
   * @param resolver how a packed cell becomes a value
   * @param segments how many segments the resource sealed
   */
  public SegmentGroupCanonicaliser(final CellResolver resolver, final int segments) {
    this(resolver, resolver, segments);
  }

  private SegmentGroupCanonicaliser(final CellResolver resolver, final CellResolver storageResolver,
      final int segments) {
    this.resolver = requireNonNull(resolver, "resolver must not be null");
    this.storageResolver = requireNonNull(storageResolver, "storageResolver must not be null");
    transformed = resolver instanceof SegmentValueTransform valueTransform
        ? valueTransform
        : null;
    if (segments < 0) {
      throw new IllegalArgumentException("segments must not be negative: " + segments);
    }
    this.memo = new int[Math.max(segments, 1)][];
  }

  /** Test hook: how many marked cells one value range of the merge aims for. */
  SegmentGroupCanonicaliser mergeRangeTarget(final int target) {
    if (target < 1) {
      throw new IllegalArgumentException("target must be positive: " + target);
    }
    this.mergeRangeTarget = target;
    return this;
  }

  /**
   * Test hook: why the last resolution refused, or {@code null} — the witness that a fallback fired.
   */
  @Nullable
  String lastRefusal() {
    return lastRefusal;
  }

  /**
   * The same slices with their cells replaced by canonical ids, or the slices unchanged when there is
   * nothing to canonicalise.
   *
   * @return {@code null} when any cell cannot be resolved — the caller must then decline, because a
   *         group whose key has no value would silently become a group of its own
   */
  public ColumnSlice @Nullable [] canonicalise(final ColumnSlice @Nullable [] slices) {
    return canonicalise(slices, null);
  }


  /**
   * Resolve every cell these slices reference ONCE, in each segment's STORAGE order, before the row
   * loop asks for any of them.
   *
   * <h2>Why order matters more than count here</h2>
   *
   * The row loop asks for cells in ROW order, which is arbitrary with respect to where their values
   * are stored, so each miss is a random dictionary read: it locates a block, decodes it, takes one
   * value out and moves on, and the next row's value is usually in a block that was just dropped.
   * That is the shape that made a group-by over a whole string column cost tens of seconds at 100M.
   *
   * <p>
   * A segment's values are STORED in collation order, so walking that segment's positions upward
   * touches each block once and takes every referenced value out of it before moving on. The cells a
   * query references are already in hand — they are these slices — so this marks them per segment and
   * then walks positions, resolving only the marked ones. No value is read that the row loop would
   * not have read anyway, and no canonical id is issued for one it would not have issued: the id
   * space stays exactly what it was, which {@link #size} and the seal both depend on.
   * </p>
   *
   * <p>
   * Best effort by design. A resolver that cannot answer in position space — a TRANSFORMING one
   * always refuses, since a transform reorders what storage ordered — simply skips this and the row
   * loop resolves as before, correctly and more slowly.
   * </p>
   */
  /**
   * Diagnostics only: the last resolver failure, so a refusal names its cause and not just its cell.
   */
  private volatile @Nullable String lastRefusal;

  /**
   * A refused canonicalisation names the cell it refused. Without this the caller sees only
   * {@code null}, and a query that declines for one cell out of a hundred million looks exactly like
   * a query that declines for a plan-shaped reason.
   */
  private void reportUnresolvable(final int leaf, final int row, final long cell) {
    if (!PROJ_DIAG) {
      return;
    }
    final int segment = ProjectionIndexRowGroupPage.segmentOfCell(cell);
    final int id = ProjectionIndexRowGroupPage.idOfCell(cell);
    int entries;
    try {
      entries = resolver.entryCountOfSegment(cell);
    } catch (final RuntimeException unavailable) {
      entries = -1;
    }
    final int[] ranks = rankByArrival;
    System.err.println("[proj] canonicalise REFUSED leaf " + leaf + " row " + row + ": segment=" + segment + " id=" + id
        + " segmentEntries=" + entries + " sealedRanks=" + (ranks == null
            ? -1
            : ranks.length)
        + " lastResolverFailure=" + lastRefusal);
  }

  private static void report(final String why) {
    if (PROJ_DIAG) {
      // A prefetch that silently does nothing is indistinguishable from one that does not help; say
      // which of the two happened, every time.
      System.err.println("[proj] storage-order resolve SKIPPED: " + why);
    }
  }

  /**
   * Runs {@code body} once per index in {@code [0, count)}: each index is one independent unit of a
   * pass over the value space — a segment's storage-order walk, or one run or one value range of the
   * merge. The units are independent — each reads through the calling thread's own view and lands its
   * result in memory it owns — so a caller with scan workers hands in its pool and the units run at
   * once; {@link #SERIAL_SEGMENTS} runs them one after another. A runner must run EVERY index to
   * completion before returning, and rethrow (possibly wrapped) whatever a body throws.
   */
  @FunctionalInterface
  public interface SegmentRunner {
    void forEach(int count, IntConsumer body);
  }

  /** The default runner: every walk on the calling thread. */
  public static final SegmentRunner SERIAL_SEGMENTS = (count, body) -> {
    for (int i = 0; i < count; i++) {
      body.accept(i);
    }
  };

  private void resolveInStorageOrder(final ColumnSlice[] slices, final long @Nullable [] keep) {
    resolveInStorageOrder(slices, keep, null, SERIAL_SEGMENTS);
  }

  /**
   * Resolve every cell the kept leaves reference BEFORE the row loop, walking each segment's
   * dictionary in STORAGE order.
   *
   * <p>
   * The row loop asks for cells in row order, and a sealed segment dictionary stores its values in
   * collation order, so those reads are random over the dictionary: every one is a block decode. The
   * same cells asked for in position order decode each block once. Marking the referenced mints per
   * segment costs one bit per entry; the walk then reads only the marked positions' values.
   * </p>
   *
   * <p>
   * Segments are independent, which is where the parallelism of this pass lives: the operand seal of
   * a 100M extremum query spent 44 s here on one thread, and the same walk over 148 segments on the
   * scan workers is bounded by the memo's insert lock, not by the reads.
   * </p>
   */
  private void resolveInStorageOrder(final ColumnSlice[] slices, final long @Nullable [] keep,
      final long @Nullable [] @Nullable [] rowKeep, final SegmentRunner runner) {
    final Marked marked = markReferenced(slices, keep, rowKeep);
    if (marked != null) {
      walkMarked(marked, runner);
    }
  }

  /**
   * What {@link #markReferenced} found.
   *
   * @param segments every segment a kept, present, not-yet-settled cell names
   * @param marks per segment, bit {@code mint} set for each such cell with {@code mint <= entries}
   * @param entryCounts per segment, its dictionary's entry count
   * @param unmarkable present kept cells that no walk can settle — a negative segment, a mint below 1
   *        or above the dictionary, or one already settled as unresolvable; the row loop must see
   *        them, so a pass that would skip it needs this to be zero
   */
  private record Marked(int[] segments, long[][] marks, int[] entryCounts, int unmarkable) {
  }

  /**
   * Mark the cells the kept leaves and rows reference, per segment, as one bit per mint.
   *
   * <p>
   * A bitmap over the dictionary is ~one bit per entry, so marking is cheaper than collecting the
   * cells: an 18M-entry column costs ~2 MB here against ~150 MB to hold its distinct cells. Cells the
   * memo has settled are left out — they need no read, and a walk that skips them is a walk over what
   * is actually missing.
   * </p>
   *
   * @return {@code null} when nothing can be walked — a slice without a long lane, a segment whose
   *         dictionary is not walkable, no present cell at all, or no cell left unsettled — after
   *         saying so; the row loop then resolves per cell
   */
  private @Nullable Marked markReferenced(final ColumnSlice[] slices, final long @Nullable [] keep,
      final long @Nullable [] @Nullable [] rowKeep) {
    Long2ObjectOpenHashMap<long[]> markedBySegment = null;
    Long2IntOpenHashMap entriesBySegment = null;
    final int[][] tables = memo;
    int unmarkable = 0;
    long placed = 0L;
    for (int i = 0; i < slices.length; i++) {
      final ColumnSlice slice = slices[i];
      if (slice == null || keep != null && (keep[i >>> 6] & 1L << (i & 63)) == 0L) {
        continue;
      }
      if (slice.rowCount() == 0) {
        continue; // the pruned sentinel: no rows, so nothing to mark
      }
      final long[] rowsKept = rowKeep == null
          ? null
          : rowKeep[i];
      if (rowKeep != null && rowsKept == null) {
        continue; // no row of this leaf passes the predicates: none of its cells is ever read
      }
      final long[] cells = slice.numericValues();
      if (cells == null) {
        report("a slice carries no long lane");
        return null; // canonicalise refuses it anyway
      }
      final long[] presence = slice.presenceWords();
      final int rows = slice.rowCount();
      // The segment's state is looked up on a CHANGE of segment, not per cell: a leaf never
      // straddles a segment, so this is once per leaf, and the per-cell work is a few compares.
      int lastSegment = -1;
      long[] marked = null;
      int[] settled = null;
      int entries = 0;
      // BY WORD, not by row: under a selective predicate almost every leaf the kernel still reads
      // keeps NO row (its mask is all-zero), and a per-row test over 100M rows to find the ~1000
      // kept ones was the serial cost of this pass. A word that keeps nothing costs one load.
      final int words = (rows + 63) >>> 6;
      for (int w = 0; w < words; w++) {
        long live = rowsKept == null
            ? presence[w]
            : presence[w] & rowsKept[w];
        while (live != 0L) {
          final int row = (w << 6) + Long.numberOfTrailingZeros(live);
          live &= live - 1;
          if (row >= rows) {
            break;
          }
          final long cell = cells[row];
          final int segment = ProjectionIndexRowGroupPage.segmentOfCell(cell);
          final int mint = ProjectionIndexRowGroupPage.idOfCell(cell);
          if (segment < 0 || mint < 1) {
            unmarkable++;
            continue;
          }
          if (segment != lastSegment) {
            if (markedBySegment == null) {
              markedBySegment = new Long2ObjectOpenHashMap<>();
              entriesBySegment = new Long2IntOpenHashMap();
              entriesBySegment.defaultReturnValue(-1);
            }
            marked = markedBySegment.get(segment);
            if (marked == null) {
              entries = storageResolver.entryCountOfSegment(cell);
              if (entries <= 0) {
                report("segment " + segment + " reports entryCount=" + entries + " — not walkable");
                return null; // leave the row loop to resolve
              }
              marked = new long[(entries >> 6) + 2];
              markedBySegment.put(segment, marked);
              entriesBySegment.put(segment, entries);
            } else {
              entries = entriesBySegment.get(segment);
            }
            settled = segment < tables.length
                ? tables[segment]
                : null;
            lastSegment = segment;
          }
          if (settled != null && mint < settled.length && settled[mint] != 0) {
            if (settled[mint] == UNRESOLVABLE) {
              unmarkable++; // settled, as "names nothing": the row loop must still report it
            }
            continue;
          }
          if (mint <= entries) {
            marked[mint >>> 6] |= 1L << (mint & 63);
            placed++;
          } else {
            unmarkable++;
          }
        }
      }
    }
    if (markedBySegment == null) {
      report("no present cell in any kept leaf");
      return null;
    }
    if (placed == 0L) {
      // Every present kept cell is settled (or unmarkable): a second pass over a column the merge
      // already built. Nothing to merge and nothing to walk, and saying "the value space is not
      // empty" here would make a settled column indistinguishable from a REFUSED merge.
      report("every present kept cell is settled");
      return null;
    }
    final int count = markedBySegment.size();
    final int[] segments = new int[count];
    final long[][] marks = new long[count][];
    final int[] entryCounts = new int[count];
    int n = 0;
    for (final Long2ObjectMap.Entry<long[]> entry : markedBySegment.long2ObjectEntrySet()) {
      segments[n] = (int) entry.getLongKey();
      marks[n] = entry.getValue();
      entryCounts[n] = entriesBySegment.get(segments[n]);
      n++;
    }
    return new Marked(segments, marks, entryCounts, unmarkable);
  }

  /** The storage-order walk over every marked segment, one walk per runner index. */
  private void walkMarked(final Marked marked, final SegmentRunner runner) {
    final int[] segments = marked.segments();
    final long[][] marks = marked.marks();
    final int[] entryCounts = marked.entryCounts();
    final int count = segments.length;
    final int[] resolvedPerSegment = new int[count]; // each walk writes its own slot: no sharing
    runner.forEach(count, i -> resolvedPerSegment[i] = walkSegment(segments[i], marks[i], entryCounts[i]));
    if (PROJ_DIAG) {
      // A prefetch that silently does nothing is indistinguishable from one that does not help.
      long resolved = 0;
      for (final int perSegment : resolvedPerSegment) {
        resolved += perSegment;
      }
      System.err.println("[proj] storage-order resolve: " + count + " segment(s), " + resolved
          + " cell(s) resolved before the row loop");
    }
  }

  /**
   * Build the value space of a WHOLE column at once, by merging its segments' sorted runs, and
   * canonicalise the slices over it — the whole-column twin of
   * {@link #canonicalise(ColumnSlice[], long[], long[][], SegmentRunner)}.
   *
   * <h2>Why a column is not a sequence of cells</h2>
   *
   * The per-cell path issues ids by hashing: one probe per referenced cell into one table under one
   * lock, plus a random re-read of both cells' bytes for every cross-segment duplicate. Over a whole
   * 100M string column that is ~25 s of a 26 s {@code GROUP BY URL}, on two of twenty cores. But a
   * sealed segment dictionary stores its values in collation order, so the referenced cells of a
   * segment, in position order, are an already-sorted run, and the column's value space is S such
   * runs: a MERGE, in which every duplicate is adjacent to its twin and identity is a comparison with
   * the previous winner. {@link SegmentValueMerge} runs that merge range-partitioned on the workers.
   *
   * <p>
   * The ids it issues are RANKS, so the lanes collate from the start and {@link #sealOrderPreserving}
   * has nothing left to do. The merge needs an EMPTY value space — its ranks are dense from 1 — and a
   * resolver that answers in position space; when either is missing it says so and the storage-order
   * walk runs instead, so the answer is the same either way and only the clock differs.
   * </p>
   *
   * <p>
   * PLANNING THREAD ONLY, like every canonicalising pass: the runner blocks on the workers.
   * </p>
   */
  public ColumnSlice @Nullable [] canonicaliseColumn(final ColumnSlice @Nullable [] slices,
      final long @Nullable [] keep, final long @Nullable [] @Nullable [] rowKeep, final SegmentRunner runner) {
    if (slices == null) {
      return null;
    }
    checkRowKeep(slices, rowKeep);
    requireNonNull(runner, "runner must not be null");
    final Marked marked = markReferenced(slices, keep, rowKeep);
    if (marked != null && !mergeColumn(marked, runner)) {
      walkMarked(marked, runner);
    }
    return canonicaliseMemoised(slices, keep, rowKeep);
  }

  /**
   * {@link #observe(ColumnSlice[], long[][], SegmentRunner)} over a whole column: the value space is
   * built by the merge where it can be, and the row loop is skipped when the merge settled every
   * present kept cell — which it did whenever none was unmarkable.
   */
  public boolean observeColumn(final ColumnSlice @Nullable [] slices, final long @Nullable [] @Nullable [] rowKeep,
      final SegmentRunner runner) {
    if (slices == null) {
      return false;
    }
    checkRowKeep(slices, rowKeep);
    requireNonNull(runner, "runner must not be null");
    final Marked marked = markReferenced(slices, null, rowKeep);
    if (marked != null) {
      if (mergeColumn(marked, runner)) {
        if (marked.unmarkable() == 0) {
          return true; // every present kept cell was marked, and every marked cell now has an id
        }
      } else {
        walkMarked(marked, runner);
      }
    }
    return observeMemoised(slices, rowKeep);
  }

  private static void checkRowKeep(final ColumnSlice[] slices, final long @Nullable [] @Nullable [] rowKeep) {
    if (rowKeep != null && rowKeep.length != slices.length) {
      throw new IllegalArgumentException(rowKeep.length + " row masks for " + slices.length + " leaves");
    }
  }

  /**
   * Run the merge over the marked cells and publish its value space, or explain why not.
   *
   * <p>
   * Held under the monitor from the emptiness check to the publication: the merge's ranks are only
   * valid over an EMPTY space, and a cell resolved through the per-cell path meanwhile would take an
   * id the merge is about to hand to a different value. The workers the runner uses never take this
   * monitor — they read through their own views and write arrays the merge owns — so holding it
   * across the runner cannot deadlock them.
   * </p>
   *
   * @return whether the space is now the merge's; {@code false} says the walk must run instead
   */
  private synchronized boolean mergeColumn(final Marked marked, final SegmentRunner runner) {
    if (transformed != null) {
      return false; // storage positions order the source, never an arbitrary transform of it
    }
    if (!fresh()) {
      final String why = "segment value merge: the value space is not empty";
      lastRefusal = why;
      report(why);
      return false;
    }
    final int[] segments = marked.segments();
    final long[] phaseNanos = new long[4];
    final SegmentValueMerge.Result result;
    try {
      result = SegmentValueMerge.merge(resolver, segments, marked.marks(), marked.entryCounts(), runner,
          mergeRangeTarget, phaseNanos);
    } catch (final RuntimeException refused) {
      // A designed refusal (a transforming resolver, a rank table that disagrees with itself) and an
      // unexpected failure both fall back to the walk, which answers the same question more slowly;
      // both are NAMED, because a fallback that cannot be told from a slow query is how a lever that
      // never fires gets credited with a win.
      final String why = refusalOf(refused);
      lastRefusal = why;
      if (PROJ_DIAG) {
        System.err.println("[proj] segment value merge " + (isRefused(refused)
            ? "REFUSED: "
            : "FAILED: ") + why + " — falling back to the storage-order walk");
      }
      return false;
    }
    int[][] tables = memo;
    int highest = -1;
    for (final int segment : segments) {
      if (segment > highest) {
        highest = segment;
      }
    }
    if (highest >= tables.length) {
      tables = Arrays.copyOf(tables, highest + 1);
    }
    final int[][] merged = result.tables();
    for (int i = 0; i < segments.length; i++) {
      tables[segments[i]] = merged[i];
    }
    final long[] representatives = result.representatives();
    representativeCell.addElements(0, representatives);
    rankedCount = representatives.length;
    mergedSegments = segments;
    mergedPositions = result.positions();
    memo = tables; // volatile write, LAST: a reader that sees a table sees the space behind it
    if (PROJ_DIAG) {
      // The merge phase's balance: a phase of P ranges over W workers ends when its slowest range
      // does, so the longest range's time against the phase's wall says whether the tail or the
      // bodies cost the time; the summed range time against the wall is the lanes actually busy.
      long longestRange = 0;
      long rangeSum = 0;
      int longestCells = 0;
      for (int p = 0; p < result.ranges(); p++) {
        final long nanos = result.rangeNanos()[p];
        rangeSum += nanos;
        if (nanos > longestRange) {
          longestRange = nanos;
          longestCells = result.rangeCells()[p];
        }
      }
      final long mergeMillis = phaseNanos[2] / 1_000_000;
      System.err.println("[proj] segment value merge: " + segments.length + " segment(s), " + result.marked()
          + " marked cell(s) -> " + representatives.length + " distinct in " + result.ranges() + " range(s); mark "
          + phaseNanos[0] / 1_000_000 + " ms, bound " + phaseNanos[1] / 1_000_000 + " ms, merge " + mergeMillis
          + " ms (longest range " + longestRange / 1_000_000 + " ms over " + longestCells + " cells, mean "
          + result.marked() / result.ranges() + "; sum " + rangeSum / 1_000_000 + " ms = "
          + String.format("%.1f", mergeMillis == 0
              ? 0.0
              : rangeSum / 1e6 / mergeMillis)
          + " lanes, " + result.loads() + " record loads), offset " + phaseNanos[3] / 1_000_000 + " ms");
    }
    return true;
  }

  /** Whether no id of any kind has been issued: the only state the merge's dense ranks fit into. */
  private boolean fresh() {
    if (!representativeCell.isEmpty() || !literalById.isEmpty() || rankedCount >= 0 || rankByArrival != null) {
      return false;
    }
    for (final int[] table : memo) {
      if (table != null) {
        return false;
      }
    }
    return true;
  }

  private static boolean isRefused(final Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t instanceof SegmentValueMerge.Refused) {
        return true;
      }
    }
    return false;
  }

  /**
   * The refusal's own words when there is one in the cause chain (a runner may wrap it), else all of
   * it.
   */
  private static String refusalOf(final RuntimeException failure) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t instanceof SegmentValueMerge.Refused) {
        return String.valueOf(t.getMessage());
      }
    }
    return failure.toString();
  }

  /**
   * Cells hashed per monitor acquisition by a walk: the lock is taken once per batch, not per cell.
   */
  private static final int WALK_BATCH = 4096;

  /** Expanding transforms flush at this charge, plus at most the single output crossing the limit. */
  private static final long TRANSFORM_BATCH_BYTES = 1L << 20;

  /**
   * One segment's storage-order walk: hash every marked, not-yet-memoised mint through the calling
   * thread's view and land the batch in the memo under one lock.
   *
   * <p>
   * The hashing is the read — a block decode per block, sequential here — and runs outside the
   * monitor, so walks on different threads overlap their I/O. What the monitor serialises is the
   * value-space insert, ~100 ns a cell, which is the floor a shared dense id space has.
   * </p>
   *
   * @return how many marked cells the walk resolved
   */
  private int walkSegment(final int segment, final long[] marked, final int entries) {
    int markedCount = 0;
    for (final long word : marked) {
      markedCount += Long.bitCount(word);
    }
    if (markedCount == 0) {
      return 0;
    }
    final WalkBatch batch = new WalkBatch(segment);
    if ((long) markedCount * SPARSE_WALK_RATIO < entries) {
      // SPARSE: a selective predicate marked a few hundred cells of a many-million-entry segment.
      // Visiting every position to find them costs one rank-table lookup per ENTRY; ordering the
      // marks by position costs one per MARK, and the resolves then run in storage order all the
      // same. ClickBench q21 at 100M: 760 marks over 97 segments of 18.3M entries.
      final long[] byPosition = new long[markedCount]; // (position << 32) | mint; sorted = storage order
      int n = 0;
      for (int w = 0; w < marked.length; w++) {
        long word = marked[w];
        while (word != 0L) {
          final int mint = (w << 6) + Long.numberOfTrailingZeros(word);
          word &= word - 1;
          final int position =
              storageResolver.positionOfCell(ProjectionIndexRowGroupPage.packSegmentCell(segment, mint));
          byPosition[n++] = (long) Math.max(position, 0) << 32 | mint;
        }
      }
      Arrays.sort(byPosition, 0, n);
      for (int i = 0; i < n; i++) {
        batch.add((int) byPosition[i]);
      }
    } else {
      final long probe = ProjectionIndexRowGroupPage.packSegmentCell(segment, 1);
      for (int position = 1; position <= entries; position++) {
        final int mint = storageResolver.mintAtPosition(probe, position);
        if (mint < 1 || mint >= marked.length << 6 || (marked[mint >>> 6] & 1L << (mint & 63)) == 0L) {
          continue;
        }
        batch.add(mint);
      }
    }
    return batch.finish();
  }

  /**
   * Below one mark in this many entries a segment is walked by its marks, not by its positions: the
   * break-even of one position lookup plus a sort slot per mark against one lookup per entry.
   */
  static final int SPARSE_WALK_RATIO = 16;

  /** A walk's batch of hashed mints on its way into the memo: the monitor is taken once per batch. */
  private final class WalkBatch {
    private final int segment;
    private final int[] ids = new int[WALK_BATCH];
    private final long[] hashes = new long[WALK_BATCH];
    private final String @Nullable [] values = transformed == null
        ? null
        : new String[WALK_BATCH];
    private final boolean @Nullable [] identities = transformed == null
        ? null
        : new boolean[WALK_BATCH];
    private int filled;
    private int resolved;
    private long valueBytes;

    WalkBatch(final int segment) {
      this.segment = segment;
    }

    /** Hash {@code mint} unless it is settled already, and land the batch when it is full. */
    void add(final int mint) {
      final int[][] tables = memo;
      final int[] table = segment < tables.length
          ? tables[segment]
          : null;
      if (table != null && mint < table.length && table[mint] != 0) {
        return; // settled by an earlier pass, or by a racing row loop
      }
      final long cell = ProjectionIndexRowGroupPage.packSegmentCell(segment, mint);
      long hash;
      try {
        if (values == null) {
          hash = resolver.hashOfCell(cell);
        } else {
          final String raw = storageResolver.valueOfCell(cell);
          values[filled] = raw == null
              ? null
              : transformed.apply(raw);
          identities[filled] = raw != null && raw.equals(values[filled]);
          hash = valueHash(values[filled]);
        }
      } catch (final RuntimeException unresolvable) {
        hash = 0L; // memoised as unresolvable, exactly as the row loop would
        if (PROJ_DIAG) {
          lastRefusal = unresolvable.toString();
        }
      }
      ids[filled] = mint;
      hashes[filled] = hash;
      if (values != null && values[filled] != null) {
        valueBytes += 64L + 2L * values[filled].length();
      }
      if (++filled == WALK_BATCH || valueBytes >= TRANSFORM_BATCH_BYTES) {
        memoiseBatch(segment, ids, hashes, values, identities, filled);
        filled = 0;
        valueBytes = 0;
      }
      resolved++;
    }

    /** Land the last partial batch; how many cells the walk resolved. */
    int finish() {
      if (filled > 0) {
        memoiseBatch(segment, ids, hashes, values, identities, filled);
        filled = 0;
      }
      return resolved;
    }
  }

  /** {@link #memoise} for a walk's batch: the monitor is taken once for {@code n} cells. */
  private synchronized void memoiseBatch(final int segment, final int[] ids, final long[] hashes,
      final String @Nullable [] values, final boolean @Nullable [] identities, final int n) {
    int[][] tables = memo;
    if (segment >= tables.length) {
      tables = Arrays.copyOf(tables, segment + 1);
    }
    int maxId = 0;
    for (int i = 0; i < n; i++) {
      if (ids[i] > maxId) {
        maxId = ids[i];
      }
    }
    int[] table = tables[segment];
    if (table == null) {
      table = new int[Math.max(maxId + 1, 1024)];
      tables[segment] = table;
    } else if (maxId >= table.length) {
      table = Arrays.copyOf(table, Math.max(maxId + 1, table.length << 1));
      tables[segment] = table;
    }
    for (int i = 0; i < n; i++) {
      final int id = ids[i];
      if (table[id] != 0) {
        continue; // another walk or the row loop got there first
      }
      final long cell = ProjectionIndexRowGroupPage.packSegmentCell(segment, id);
      table[id] = values == null
          ? issueCanonical(cell, hashes[i])
          : issueTransformedCanonical(cell, hashes[i], values[i], identities[i]);
    }
    if (values != null) {
      Arrays.fill(values, 0, n, null);
    }
    memo = tables; // volatile write: publishes the entries above and any grown array
  }

  /**
   * The same, canonicalising ONLY the leaves {@code keep} keeps.
   *
   * <h2>Why a keep mask belongs here</h2>
   *
   * Canonicalising is the price of a prepass-free load: an id means something only inside its
   * segment, so a cell must be mapped to a query-wide id before it can be a group key. That price is
   * one dictionary read per DISTINCT cell, and over a whole column at 100M rows it is eighteen
   * million random reads — which is fine when the query groups the whole table and absurd when its
   * predicate keeps a handful of rows. Three ClickBench queries that answer in milliseconds over a
   * resource-wide dictionary took ~39 s here for exactly that reason: every row was canonicalised
   * before a single predicate ran.
   *
   * <p>
   * A leaf the predicates' zone maps drop can produce no row at all, so its cells are never read as
   * group keys and need no canonical id. Such a leaf is left {@code null} rather than passed through:
   * a raw cell IS a small integer in segment 0, exactly the space canonical ids occupy, so passing
   * one through would let it group silently BESIDE a canonical id. A {@code null} cannot do that, and
   * the kernel never dereferences it — it reads {@code groupCol[leaf]} only after its own mask says
   * the leaf has surviving rows, from the same authority that built this mask. Should the two ever
   * disagree, this fails loudly instead of answering wrongly.
   * </p>
   *
   * @param keep bit {@code leaf} set = canonicalise it; {@code null} = canonicalise every leaf
   */
  public ColumnSlice @Nullable [] canonicalise(final ColumnSlice @Nullable [] slices, final long @Nullable [] keep) {
    return canonicalise(slices, keep, null, SERIAL_SEGMENTS);
  }

  /** The same, with the storage-order prefetch's segment walks run by {@code runner}. */
  public ColumnSlice @Nullable [] canonicalise(final ColumnSlice @Nullable [] slices, final long @Nullable [] keep,
      final SegmentRunner runner) {
    return canonicalise(slices, keep, null, runner);
  }

  /**
   * The same, canonicalising only the ROWS {@code rowKeep} keeps.
   *
   * <p>
   * PREDICATE-FIRST at row grain. {@code rowKeep[leaf]} is the predicates' verdict on the leaf's rows
   * as the kernel will compute it ({@code ProjectionColumnScan.rowKeepMasks}); {@code null} means no
   * row of the leaf passes, and such a leaf is left {@code null} here exactly like one the leaf mask
   * drops. A row the mask clears keeps no cell: its canonical entry stays zero and its presence bit
   * is cleared in the lane handed out, so nothing downstream can mistake it for a value. The kernel
   * never reads it anyway — its own mask, the same verdict, clears the row first — but the lane must
   * not depend on that. {@code rowKeep == null} keeps every row.
   * </p>
   */
  public ColumnSlice @Nullable [] canonicalise(final ColumnSlice @Nullable [] slices, final long @Nullable [] keep,
      final long @Nullable [] @Nullable [] rowKeep, final SegmentRunner runner) {
    if (slices == null) {
      return null;
    }
    checkRowKeep(slices, rowKeep);
    resolveInStorageOrder(slices, keep, rowKeep, requireNonNull(runner, "runner must not be null"));
    return canonicaliseMemoised(slices, keep, rowKeep);
  }

  /** The row loop: every kept present cell to the id the memo holds for it, or the slow path once. */
  private ColumnSlice @Nullable [] canonicaliseMemoised(final ColumnSlice[] slices, final long @Nullable [] keep,
      final long @Nullable [] @Nullable [] rowKeep) {
    final ColumnSlice[] out = new ColumnSlice[slices.length];
    for (int i = 0; i < slices.length; i++) {
      final ColumnSlice slice = slices[i];
      if (slice == null || keep != null && (keep[i >>> 6] & 1L << (i & 63)) == 0L) {
        continue;
      }
      final long[] rowsKept = rowKeep == null
          ? null
          : rowKeep[i];
      if (rowKeep != null && rowsKept == null && slice.rowCount() != 0) {
        continue; // no row passes: the kernel skips the leaf before it reads a key from it
      }
      if (slice.rowCount() == 0) {
        // THE PRUNED SENTINEL. A windowed fill hands out a shared rowless slice for every leaf the
        // zone maps dropped inside a morsel that also has kept leaves, and the kernels short-circuit
        // on its row count. It has no cells, so there is nothing to canonicalise and nothing that
        // could group beside a canonical id — it passes through as it is. Refusing it declined
        // ClickBench q39 at 100M (the one scale whose fill budget refuses residency) into a generic
        // pipeline that cannot run the query at all.
        out[i] = slice;
        continue;
      }
      final long[] cells = slice.numericValues();
      if (cells == null) {
        // A segment-scoped column IS a long lane, so a slice WITH rows but without one cannot be
        // canonicalised. Passing it through would be far worse than declining: its raw cells would
        // then group BESIDE canonical ids from the leaves that were canonicalised, and a segment-0
        // cell is a small integer — exactly the space canonical ids occupy. Unrelated values would
        // silently land in the same group.
        return null;
      }
      final int rows = slice.rowCount();
      final long[] presence = rowsKept == null
          ? slice.presenceWords()
          : presentAndKept(slice.presenceWords(), rowsKept, rows);
      final long[] canonical = new long[cells.length];
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      int present = 0;
      // HOISTED ONCE PER LEAF, not read per row: `memo` is volatile and `laneIdOf` is a call, and a
      // leaf never straddles a segment, so both answer the same thing for every row of it. What the
      // row loop keeps is an array read; a cell whose entry is not settled yet — at most one per
      // distinct cell over the whole query — is the only one that calls back.
      final int[][] tables = memo;
      final int[] ranks = rankByArrival;
      final int rankedLimit = ranks == null && sealedByMerge
          ? rankedCount
          : Integer.MAX_VALUE;
      int[] settled = null;
      // ABSENT ROWS ARE NOT CELLS. A row whose field is missing carries whatever the lane was
      // filled with — resolving that would either invent a group or, far worse, declare the whole
      // pass unresolvable and decline a query that is perfectly servable. The kernel reads presence
      // itself, so an absent row's canonical entry is never looked at. Walked BY WORD: under a
      // selective predicate most leaves keep nothing, and a word that keeps nothing costs one load.
      final int words = (rows + 63) >>> 6;
      for (int w = 0; w < words; w++) {
        long live = presence[w];
        while (live != 0L) {
          final int row = (w << 6) + Long.numberOfTrailingZeros(live);
          live &= live - 1;
          if (row >= rows) {
            break;
          }
          final long cell = cells[row];
          if (settled == null) {
            final int segment = ProjectionIndexRowGroupPage.segmentOfCell(cell);
            settled = segment >= 0 && segment < tables.length
                ? tables[segment]
                : null;
            if (settled == null) {
              settled = NO_SETTLED; // the segment has no table yet; every row takes the slow path
            }
          }
          final int cellId = ProjectionIndexRowGroupPage.idOfCell(cell);
          final int arrival = cellId >= 0 && cellId < settled.length
              ? settled[cellId]
              : 0;
          final int id = arrival != 0
              ? rankOf(arrival, ranks, rankedLimit)
              : laneIdOf(cell);
          if (id == UNRESOLVABLE) {
            reportUnresolvable(i, row, cell);
            return null;
          }
          canonical[row] = id;
          present++;
          if (id < min) {
            min = id;
          }
          if (id > max) {
            max = id;
          }
        }
      }
      if (present == 0) {
        min = 0;
        max = 0;
      }
      // The zone map must describe what the lane now HOLDS. Carrying the cells' min/max over would
      // let a range prune drop a leaf whose canonical ids are nowhere near them.
      out[i] = new ColumnSlice(rows, slice.flags(), min, max, presence, canonical, slice.boolWords(),
          slice.stringDictIds(), slice.dictBytes(), slice.dictOffsets(), slice.setCounts(), slice.dictHashes());
    }
    return out;
  }

  /** Presence with every row the mask clears cleared too, over the words {@code rows} span. */
  private static long[] presentAndKept(final long[] presence, final long[] kept, final int rows) {
    final int words = (rows + 63) >>> 6;
    if (kept.length < words) {
      throw new IllegalArgumentException("a row mask of " + kept.length + " words cannot cover " + rows + " rows");
    }
    final long[] out = new long[words];
    for (int w = 0; w < words; w++) {
      out[w] = presence[w] & kept[w];
    }
    return out;
  }

  /**
   * Resolve every present cell of {@code slices} into the value space, WITHOUT building canonical
   * lanes — {@link #size()} is then the number of distinct values those slices hold.
   *
   * <p>
   * The count-distinct shape: a caller that wants a cardinality has no use for the per-row ids, and
   * allocating a {@code long[]} per leaf to throw away would be the dominant cost of the answer. The
   * work that matters — one dictionary read per DISTINCT cell — is identical either way, because both
   * go through the same memo.
   * </p>
   *
   * @return {@code false} when a present cell has no value in this revision; the caller must decline
   */
  public boolean observe(final ColumnSlice @Nullable [] slices) {
    return observe(slices, SERIAL_SEGMENTS);
  }

  /**
   * The same, with the storage-order prefetch's segment walks run by {@code runner}.
   *
   * <p>
   * Observing used to be the row loop alone — one random dictionary read per distinct cell, in row
   * order, on the calling thread: 44 s of a 68 s extremum query at 100M. The prefetch walks each
   * segment's dictionary once in storage order instead, so the row loop below finds every cell
   * memoised and reads nothing.
   * </p>
   */
  public boolean observe(final ColumnSlice @Nullable [] slices, final SegmentRunner runner) {
    return observe(slices, null, runner);
  }

  /**
   * The same, observing only the ROWS {@code rowKeep} keeps — see
   * {@link #canonicalise(ColumnSlice[], long[], long[][], SegmentRunner)}. The value space then holds
   * exactly the values of rows that pass, which is what a seal over a filtered operand must order: a
   * value only failing rows carry is not a candidate for their MIN.
   */
  public boolean observe(final ColumnSlice @Nullable [] slices, final long @Nullable [] @Nullable [] rowKeep,
      final SegmentRunner runner) {
    if (slices == null) {
      return false;
    }
    checkRowKeep(slices, rowKeep);
    resolveInStorageOrder(slices, null, rowKeep, requireNonNull(runner, "runner must not be null"));
    return observeMemoised(slices, rowKeep);
  }

  /**
   * The row loop of {@link #observe}: every kept present cell must resolve, through the memo or once.
   */
  private boolean observeMemoised(final ColumnSlice[] slices, final long @Nullable [] @Nullable [] rowKeep) {
    for (int i = 0; i < slices.length; i++) {
      final ColumnSlice slice = slices[i];
      if (slice == null || slice.rowCount() == 0) {
        continue; // absent, or the pruned sentinel: no rows, no values
      }
      final long[] rowsKept = rowKeep == null
          ? null
          : rowKeep[i];
      if (rowKeep != null && rowsKept == null) {
        continue; // no row of the leaf passes: its values are not the query's
      }
      final long[] cells = slice.numericValues();
      if (cells == null) {
        return false; // a segment-scoped column IS a long lane; see canonicalise
      }
      final long[] presence = slice.presenceWords();
      final int rows = slice.rowCount();
      // An absent row holds no value and is not a distinct one; a cleared one is not read. By word,
      // as in canonicalise: a word that keeps nothing costs one load.
      final int words = (rows + 63) >>> 6;
      for (int w = 0; w < words; w++) {
        long live = rowsKept == null
            ? presence[w]
            : presence[w] & rowsKept[w];
        while (live != 0L) {
          final int row = (w << 6) + Long.numberOfTrailingZeros(live);
          live &= live - 1;
          if (row >= rows) {
            break;
          }
          if (canonicalOf(cells[row]) == UNRESOLVABLE) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Freeze the value space and renumber: from here the canonical ids a lane carries are in COLLATION
   * order, so an integer {@code min}, {@code max} or sort over that lane answers the question about
   * the values themselves.
   *
   * <p>
   * The primitive the arrival-order ids cannot supply. A cell's id is a MINT — the dictionary hands
   * them out in the order values were first seen, and the seal stores by rank behind a rank table —
   * so comparing two raw cells says nothing about how their values collate. Sorting once, over the
   * values the column actually REFERENCES rather than the whole dictionary, buys an order for every
   * later comparison at no per-row cost.
   * </p>
   *
   * <p>
   * The order is {@link String#compareTo}, which is UTF-16 code-unit order — the same order
   * {@code ValueDictionaryEntryNode.compareUtf16Range} imposes on the dictionary's own storage, so a
   * lane ranked here and a dictionary sealed there agree.
   * </p>
   *
   * <p>
   * Call it only after {@link #observe} has seen every cell the query will group on: once sealed, a
   * value that was never observed has no rank, and rather than invent one this class reports the cell
   * as unresolvable so the caller declines.
   * </p>
   */
  public synchronized boolean sealOrderPreserving() {
    if (rankByArrival != null || sealedByMerge) {
      return true; // idempotent: a second seal would renumber ids the caller is already carrying
    }
    final int count = representativeCell.size();
    if (rankedCount >= 0 && count == rankedCount && literalById.isEmpty()) {
      // The merge issued every id there is, as a rank: the lanes already collate. Nothing to sort,
      // nothing to renumber — only to refuse a later arrival, which has no rank.
      sealedByMerge = true;
      return true;
    }
    // MERGE FIRST, at every size. The values of a segment-scoped column are not an unsorted heap:
    // they are one already-sorted run per segment, so the order is a merge, and the merge holds S
    // strings rather than one per value. Trying it first rather than only above a threshold means
    // every gate exercises the path that 100M depends on — a route that only runs at the scale
    // nothing verifies at is a route nobody has tested.
    if (sealByPositionMerge(count)) {
      return true;
    }
    if (count > MAX_ORDERED_VALUES) {
      // No positions to merge on AND too many to materialise: one String per distinct value is the
      // heap at eighteen million URLs. Refuse, and let the caller decline.
      return false;
    }
    // Materialise once, sort on that. Comparing through the dictionary instead would pay two reads
    // per comparison, which measured 2-3x slower on the queries that seal.
    final String[] byArrival = new String[count];
    for (int arrival = 1; arrival <= count; arrival++) {
      byArrival[arrival - 1] = resolver.valueOfCell(representativeCell.getLong(arrival - 1));
      if (byArrival[arrival - 1] == null) {
        return false;
      }
    }
    final int[] arrivalByRank = new int[count];
    for (int i = 0; i < count; i++) {
      arrivalByRank[i] = i + 1;
    }
    IntArrays.quickSort(arrivalByRank, (left, right) -> byArrival[left - 1].compareTo(byArrival[right - 1]));
    final int[] ranks = new int[count];
    final int[] sorted = new int[count];
    for (int rank = 0; rank < count; rank++) {
      final int arrival = arrivalByRank[rank];
      ranks[arrival - 1] = rank + 1;
      sorted[rank] = arrival;
    }
    sortedArrival = sorted;
    rankByArrival = ranks; // last: a reader that sees this sees both tables
    return true;
  }

  /**
   * Seal a value space too large to materialise, by MERGING the segments instead of sorting the
   * whole.
   *
   * <h2>Why this is not another sort</h2>
   *
   * A sealed segment dictionary stores its values in collation order, so within one segment POSITION
   * order already IS value order — the identity {@code ReadView.compareIds} exploits for
   * {@code storageOrdered}. The distinct values of a segment-scoped column are therefore not an
   * unsorted heap of eighteen million strings but S runs that are each already sorted, and a total
   * order over them is a merge, not a sort.
   *
   * <p>
   * That changes both costs that made the plain sort refuse. MEMORY: the sorted path materialises one
   * {@link String} per distinct value, which at eighteen million URLs is the heap; the merge holds
   * only ONE head value per run, so S strings live at a time whatever the count. READS: a sort
   * touches values in whatever order the comparator asks, which is random; the merge walks each run
   * in ascending position, which is the order the values are STORED in, so a decoded block serves
   * every value in it before being dropped.
   * </p>
   *
   * <p>
   * Ordering the runs costs no value reads at all — {@code positionOfCell} answers from the rank
   * table, whose records cover {@code ENTRIES_PER_RECORD} keys each and are cached on the view, so a
   * pass over ascending ids loads each record once. What remains is {@code count} value reads and
   * {@code count log S} comparisons.
   * </p>
   *
   * @return {@code false} when the resolver cannot answer in position space (a TRANSFORMING resolver
   *         never can, since a transform reorders what storage ordered), when a value is unreadable,
   *         or above {@link #MAX_MERGED_VALUES}
   */
  boolean sealByPositionMerge(final int count) { // package-private: the merge is tested on a small corpus
    if (count > MAX_MERGED_VALUES) {
      return false;
    }
    // key = (segment, position), which sorts the arrivals into per-segment runs that are each in
    // collation order. Both halves are non-negative, so a plain long compare orders the pair.
    final long[] key = new long[count];
    for (int arrival = 1; arrival <= count; arrival++) {
      final long cell = representativeCell.getLong(arrival - 1);
      final int position = resolver.positionOfCell(cell);
      if (position == NO_POSITION) {
        return false;
      }
      key[arrival - 1] = (long) ProjectionIndexRowGroupPage.segmentOfCell(cell) << 32 | position;
    }
    final int[] order = new int[count];
    for (int i = 0; i < count; i++) {
      order[i] = i;
    }
    IntArrays.parallelQuickSort(order, (left, right) -> Long.compare(key[left], key[right]));

    // Run boundaries: one run per segment that contributed a value.
    int runs = 0;
    for (int i = 0; i < count; i++) {
      if (i == 0 || key[order[i]] >>> 32 != key[order[i - 1]] >>> 32) {
        runs++;
      }
    }
    final int[] runStart = new int[runs + 1];
    int run = 0;
    for (int i = 0; i < count; i++) {
      if (i == 0 || key[order[i]] >>> 32 != key[order[i - 1]] >>> 32) {
        runStart[run++] = i;
      }
    }
    runStart[runs] = count;

    final int[] cursor = new int[runs];
    final String[] head = new String[runs];
    final int[] heap = new int[runs];
    int heapSize = 0;
    for (int r = 0; r < runs; r++) {
      cursor[r] = runStart[r];
      final String value = valueAt(order[cursor[r]]);
      if (value == null) {
        return false;
      }
      head[r] = value;
      heap[heapSize++] = r;
    }
    for (int i = heapSize / 2 - 1; i >= 0; i--) {
      siftDown(heap, heapSize, i, head);
    }

    final int[] ranks = new int[count];
    final int[] sorted = new int[count];
    for (int rank = 1; rank <= count; rank++) {
      final int winner = heap[0];
      final int arrival = order[cursor[winner]] + 1;
      ranks[arrival - 1] = rank;
      sorted[rank - 1] = arrival;
      if (++cursor[winner] < runStart[winner + 1]) {
        final String next = valueAt(order[cursor[winner]]);
        if (next == null) {
          return false;
        }
        head[winner] = next;
      } else {
        heap[0] = heap[--heapSize]; // the run is exhausted; drop it and re-heapify
        if (heapSize == 0) {
          break;
        }
      }
      siftDown(heap, heapSize, 0, head);
    }
    sortedArrival = sorted;
    rankByArrival = ranks; // last: a reader that sees this sees both tables
    return true;
  }

  /** The value of the arrival at zero-based index {@code arrivalIndex}. */
  private @Nullable String valueAt(final int arrivalIndex) {
    return resolver.valueOfCell(representativeCell.getLong(arrivalIndex));
  }

  private static void siftDown(final int[] heap, final int size, final int from, final String[] head) {
    int parent = from;
    while (true) {
      final int left = (parent << 1) + 1;
      if (left >= size) {
        return;
      }
      final int right = left + 1;
      int smallest = right < size && head[heap[right]].compareTo(head[heap[left]]) < 0
          ? right
          : left;
      if (head[heap[smallest]].compareTo(head[heap[parent]]) >= 0) {
        return;
      }
      final int swap = heap[parent];
      heap[parent] = heap[smallest];
      heap[smallest] = swap;
      parent = smallest;
    }
  }

  /**
   * Distinct values above which {@link #sealOrderPreserving} refuses when it cannot MERGE — the
   * fallback materialises one {@link String} per distinct value, and that is the heap long before it
   * is the clock.
   */
  private static final int MAX_ORDERED_VALUES = 2_000_000;

  /**
   * Distinct values above which even the merge refuses. The merge holds four arrays over the value
   * space — the sort key, the permutation, and the two rank tables — at twenty bytes per value, so
   * this is the point where the ORDER itself, not the values, would take the heap.
   */
  private static final int MAX_MERGED_VALUES = 32_000_000;

  /**
   * The canonical id a LITERAL takes in this value space, minting one if the column never held it.
   *
   * <p>
   * What a conditional key needs: {@code CASE WHEN c THEN col ELSE lit END} must put an else-row and
   * a then-row whose value IS {@code lit} in the SAME group, which only holds if both sides speak one
   * identity space. Hashing the literal's bytes instead would leave them in two domains and split a
   * group that the query defines as one.
   * </p>
   *
   * @throws IllegalStateException after {@link #sealOrderPreserving}, where a new value has no rank
   */
  public synchronized int canonicalOfValue(final String value) {
    requireNonNull(value, "value must not be null");
    int canonical = canonicalByLiteral.getInt(value);
    if (canonical != 0) {
      final int[] ranks = rankByArrival;
      return ranks == null
          ? canonical
          : ranks[canonical - 1];
    }
    if (rankByArrival != null || sealedByMerge) {
      throw new IllegalStateException("the value space is sealed; '" + value + "' has no rank");
    }
    // A literal has no cell of its own; it takes an id past every cell-backed one and is remembered
    // here rather than in the dictionary, because nothing in the column may hold it.
    literalById.put(representativeCell.size() + literalById.size() + 1, value);
    canonical = representativeCell.size() + literalById.size();
    canonicalByLiteral.put(value, canonical);
    return canonical;
  }

  /** Whether {@link #sealOrderPreserving} has run, so lane ids are in collation order. */
  public synchronized boolean isOrderPreserving() {
    return rankByArrival != null || sealedByMerge;
  }

  /** Empty stand-in so the row loop never re-tests for a missing table. */
  private static final int[] NO_SETTLED = new int[0];

  /**
   * An arrival id as the lane carries it: itself, or its rank once the value space is sealed.
   *
   * @param rankedLimit the highest id that has a rank while {@code ranks} is null — every id, unless
   *        the merge's ranks are the seal, when it is {@link #rankedCount}
   */
  private static int rankOf(final int arrival, final int @Nullable [] ranks, final int rankedLimit) {
    if (ranks == null) {
      return arrival > rankedLimit
          ? UNRESOLVABLE
          : arrival;
    }
    return arrival > ranks.length
        ? UNRESOLVABLE
        : ranks[arrival - 1];
  }

  /**
   * The id a lane carries for {@code cell}: the arrival-order canonical id, or its RANK once the
   * value space has been sealed in collation order.
   */
  private int laneIdOf(final long cell) {
    final int arrival = canonicalOf(cell);
    if (arrival == UNRESOLVABLE) {
      return UNRESOLVABLE;
    }
    final int[] ranks = rankByArrival;
    if (ranks == null) {
      return sealedByMerge && arrival > rankedCount
          ? UNRESOLVABLE // first seen after the merge became the seal: no rank, as below
          : arrival;
    }
    if (arrival > ranks.length) {
      // A value first seen AFTER the seal. Its rank would have to be invented, and any choice would
      // misorder it against everything already ranked, so the pass declines instead.
      return UNRESOLVABLE;
    }
    return ranks[arrival - 1];
  }

  /** The value a lane id names, or {@code null} when the id was never issued. */
  public synchronized @Nullable String valueOf(final int laneId) {
    final int[] ranked = sortedArrival;
    if (ranked != null) {
      return laneId >= 1 && laneId <= ranked.length
          ? resolver.valueOfCell(representativeCell.getLong(ranked[laneId - 1] - 1))
          : null;
    }
    return laneId >= 1 && laneId <= representativeCell.size()
        ? resolver.valueOfCell(representativeCell.getLong(laneId - 1))
        : null;
  }

  /**
   * A per-id string-length table over this canonicaliser's id space — the segment-scoped twin of the
   * global dictionary's length table, and indexed exactly the same way, so the kernels need no arm of
   * their own: they index {@code table[id]} with the id the canonicalised lane already carries.
   *
   * @param lengthMode {@link ProjectionIndexByteScan#STRING_LENGTH_UTF8_BYTES} or
   *        {@link ProjectionIndexByteScan#STRING_LENGTH_CODE_POINTS}
   * @return {@code table[id]} for {@code id} in {@code 1..size()}; slot 0 is unused, as ids are
   *         1-based
   */
  public synchronized int[] lengthTable(final byte lengthMode) {
    checkLengthMode(lengthMode);
    final int count = size();
    final int[] table = new int[count + 1];
    for (int id = 1; id <= count; id++) {
      table[id] = lengthOfValue(id, lengthMode);
    }
    return table;
  }

  /**
   * The same table, filled in STORAGE order on the runner where the merge built the space.
   *
   * <p>
   * The serial table asks for one value per id, in id order — which after the merge is collation
   * order over the whole column, so consecutive ids alternate between segments and every read is a
   * random one: 18M block decodes for one {@code AVG(length(URL))}. The segments the merge ranked are
   * walked here by POSITION instead, each on its own runner index, and the length is counted on the
   * stored bytes; an id no walk reaches (a later arrival, a literal) is measured as before.
   * </p>
   */
  public int[] lengthTable(final byte lengthMode, final SegmentRunner runner) {
    checkLengthMode(lengthMode);
    requireNonNull(runner, "runner must not be null");
    final int count;
    final int[] segments;
    final long[][] positions;
    final int[][] tables;
    final int[] ranks;
    synchronized (this) {
      count = representativeCell.size();
      segments = mergedSegments;
      positions = mergedPositions;
      tables = memo;
      ranks = rankByArrival;
    }
    final int[] table = new int[count + 1];
    if (segments == null || positions == null) {
      for (int id = 1; id <= count; id++) {
        table[id] = lengthOfValue(id, lengthMode);
      }
      return table;
    }
    Arrays.fill(table, 1, count + 1, -1);
    runner.forEach(segments.length, i -> {
      final int segment = segments[i];
      final int[] memoised = segment < tables.length
          ? tables[segment]
          : null;
      if (memoised != null) {
        fillLengthsByPosition(table, segment, positions[i], memoised, ranks, lengthMode);
      }
    });
    for (int id = 1; id <= count; id++) {
      if (table[id] < 0) {
        table[id] = lengthOfValue(id, lengthMode);
      }
    }
    return table;
  }

  /**
   * One merged segment's contribution: its referenced positions upward, each to the lane id its memo
   * entry maps to. Two segments carrying one value write the same length to the same slot, which is
   * benign; a value nothing reaches stays {@code -1} for the serial path to measure.
   */
  private void fillLengthsByPosition(final int[] table, final int segment, final long[] positions, final int[] memoised,
      final int @Nullable [] ranks, final byte lengthMode) {
    final long probe = ProjectionIndexRowGroupPage.packSegmentCell(segment, 1);
    final int limit = Integer.MAX_VALUE;
    for (int position = SegmentValueMerge.nextMarked(positions, 1, limit); position >= 0; position =
        SegmentValueMerge.nextMarked(positions, position + 1, limit)) {
      final int mint = resolver.mintAtPosition(probe, position);
      if (mint < 1 || mint >= memoised.length) {
        continue;
      }
      final int arrival = memoised[mint];
      if (arrival <= 0) {
        continue;
      }
      final int laneId;
      if (ranks == null) {
        laneId = arrival;
      } else if (arrival <= ranks.length) {
        laneId = ranks[arrival - 1];
      } else {
        continue;
      }
      if (laneId < 1 || laneId >= table.length) {
        continue;
      }
      final int length =
          resolver.valueLengthOfCell(ProjectionIndexRowGroupPage.packSegmentCell(segment, mint), lengthMode);
      if (length >= 0) {
        table[laneId] = length;
      }
    }
  }

  private static void checkLengthMode(final byte lengthMode) {
    if (lengthMode != ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES
        && lengthMode != ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS) {
      throw new IllegalArgumentException("not a string-length mode: " + lengthMode);
    }
  }

  /** The length of the value lane id {@code id} names, by materialising it. */
  private int lengthOfValue(final int id, final byte lengthMode) {
    final String value = valueOf(id);
    if (value == null) {
      throw new IllegalStateException("canonical id " + id + " names no value");
    }
    return lengthMode == ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS
        ? value.codePointCount(0, value.length())
        : utf8Length(value);
  }

  /** UTF-8 byte length without encoding the string — the same count the dictionary stores. */
  private static int utf8Length(final String value) {
    int bytes = 0;
    final int length = value.length();
    for (int i = 0; i < length; i++) {
      final char c = value.charAt(i);
      if (c < 0x80) {
        bytes++;
      } else if (c < 0x800) {
        bytes += 2;
      } else if (Character.isHighSurrogate(c) && i + 1 < length && Character.isLowSurrogate(value.charAt(i + 1))) {
        bytes += 4; // one supplementary code point, encoded from the surrogate PAIR
        i++;
      } else {
        bytes += 3;
      }
    }
    return bytes;
  }

  /** Distinct values seen so far (test and diagnostic observability). */
  public synchronized int size() {
    return representativeCell.size();
  }

  /**
   * The canonical id of {@code cell}. The fast path is lock-free; the monitor is taken only the first
   * time a given {@code (segment, id)} is asked for.
   */
  private int canonicalOf(final long cell) {
    final int segment = ProjectionIndexRowGroupPage.segmentOfCell(cell);
    final int id = ProjectionIndexRowGroupPage.idOfCell(cell);
    final int[][] tables = memo;
    if (segment >= 0 && segment < tables.length && id >= 0) {
      final int[] table = tables[segment];
      if (table != null && id < table.length) {
        final int memoised = table[id];
        if (memoised != 0) {
          return memoised;
        }
      }
    }
    return resolveAndMemoise(cell, segment, id);
  }

  /**
   * Resolve one cell to its canonical id and remember it.
   *
   * <p>
   * The HASH is taken outside the monitor and only the group bookkeeping runs inside it, which is the
   * difference between a parallel scan and a serial one: {@code hashOfCell} reads the dictionary — a
   * page fetch and a block decode — and holding an instance-wide lock across it queues every worker
   * behind one thread doing I/O.
   * </p>
   *
   * <p>
   * That is sound ONLY because the resolver reads a view private to the calling thread.
   * {@link GlobalValueDictionary.ReadView} carries plain mutable caches, so two threads hashing
   * through one view tear each other's state; the read then either fails to parse or, far worse,
   * returns a hash for a torn slice and puts two different values in one group. Hoisting the hash out
   * while the view was still shared was measured and it broke exactly that way — which is why the
   * constructor takes a supplier and not a view.
   * </p>
   *
   * <p>
   * Racing workers may hash one cell twice. That costs a repeated read and changes no answer; issuing
   * the canonical id, which mutates the shared value space, stays under the lock where it belongs.
   * </p>
   */
  private int resolveAndMemoise(final long cell, final int segment, final int id) {
    if (segment < 0 || id < 0) {
      return UNRESOLVABLE;
    }
    long hash;
    String value = null;
    boolean identity = false;
    try {
      if (transformed == null) {
        hash = resolver.hashOfCell(cell);
      } else {
        final String raw = storageResolver.valueOfCell(cell);
        value = raw == null
            ? null
            : transformed.apply(raw);
        identity = raw != null && raw.equals(value);
        hash = valueHash(value);
      }
    } catch (final RuntimeException unresolvable) {
      hash = 0L; // the cell names no entry; 0 is not a hash here, so memoise refuses it below
      if (PROJ_DIAG) {
        lastRefusal = unresolvable.toString();
      }
    }
    return memoise(cell, segment, id, hash, value, identity);
  }

  private synchronized int memoise(final long cell, final int segment, final int id, final long hash,
      final @Nullable String value, final boolean identity) {
    int[][] tables = memo;
    if (segment >= tables.length) {
      tables = Arrays.copyOf(tables, segment + 1);
    }
    int[] table = tables[segment];
    if (table == null) {
      table = new int[Math.max(id + 1, 1024)];
      tables[segment] = table;
    } else if (id >= table.length) {
      // Ids are dense mints, so growth is rare; double so a scan that walks a dictionary upward does
      // not copy per entry.
      table = Arrays.copyOf(table, Math.max(id + 1, table.length << 1));
      tables[segment] = table;
    } else if (table[id] != 0) {
      return table[id]; // another worker resolved it while this one was hashing the dictionary
    }
    final int canonical = transformed == null
        ? issueCanonical(cell, hash)
        : issueTransformedCanonical(cell, hash, value, identity);
    table[id] = canonical;
    memo = tables; // volatile write: publishes both the entry above and any grown array
    return canonical;
  }

  private int issueCanonical(final long cell, final long hash) {
    if (hash == 0L) {
      return UNRESOLVABLE; // the cell names no entry; 0 is not a hash here
    }
    final int ranked = rankedCount;
    if (ranked > 0) {
      // The merge issued its ids without hashing anything, so a value it ranked is not in the hash
      // index; it IS in the representatives, in collation order, and a binary search finds it. Only
      // an arrival the merge never marked comes through here at all.
      final int found = rankedIdOf(cell, ranked);
      if (found > 0) {
        return found;
      }
    }
    final int[] existing = idsByHash.get(hash);
    if (existing != null) {
      for (final int candidate : existing) {
        // EXACT, not hash-equal: two values sharing a hash must not share a group, so the bytes
        // decide. A collision costs one comparison and is otherwise invisible.
        if (resolver.sameValue(cell, representativeCell.getLong(candidate - 1))) {
          return candidate;
        }
      }
    }
    representativeCell.add(cell);
    final int canonical = representativeCell.size();
    idsByHash.put(hash, existing == null
        ? new int[] {canonical}
        : append(existing, canonical));
    return canonical;
  }

  /**
   * The input has already been transformed outside the monitor; equality never transforms it again.
   */
  private int issueTransformedCanonical(final long cell, final long hash, final @Nullable String value,
      final boolean identity) {
    if (hash == 0L || value == null) {
      return UNRESOLVABLE;
    }
    final int[] existing = idsByHash.get(hash);
    if (existing != null) {
      for (final int candidate : existing) {
        if (value.equals(transformed.representative(candidate, representativeCell.getLong(candidate - 1)))) {
          return candidate;
        }
      }
    }
    representativeCell.add(cell);
    final int canonical = representativeCell.size();
    idsByHash.put(hash, existing == null
        ? new int[] {canonical}
        : append(existing, canonical));
    transformed.remember(canonical, cell, value, identity);
    return canonical;
  }

  /** Conservative retained-transform byte charge, exposed to the bounded-memory witness. */
  long retainedTransformBytes() {
    return transformed == null
        ? 0L
        : transformed.retainedBytes();
  }

  /**
   * The rank carrying {@code cell}'s value among the merge's {@code ranked} representatives, or 0.
   */
  private int rankedIdOf(final long cell, final int ranked) {
    int lo = 0;
    int hi = ranked - 1;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      final int order = resolver.compareValues(representativeCell.getLong(mid), cell);
      if (order < 0) {
        lo = mid + 1;
      } else if (order > 0) {
        hi = mid - 1;
      } else {
        return mid + 1;
      }
    }
    return 0;
  }

  private static int[] append(final int[] chain, final int id) {
    final int[] grown = Arrays.copyOf(chain, chain.length + 1);
    grown[chain.length] = id;
    return grown;
  }

}
