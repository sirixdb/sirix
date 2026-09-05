/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Answers a per-VALUE string predicate — containment, ordering — over a segment-scoped column, by
 * settling one whole segment's dictionary per first touch.
 *
 * <h2>How a segment is settled: one sequential sweep</h2>
 *
 * A resource-wide dictionary answers these ops by evaluating the predicate once against every entry
 * and handing the kernels a verdict bitset over id space. A sealed segment dictionary looked unable
 * to reuse that, because its ids are arrival-order MINTS while its storage is collation order — a
 * sweep walking storage would set the right bits at the wrong ids. That reasoning was right about the
 * hazard and wrong about the conclusion: the rank table translates a position to its mint, and
 * {@code stringOpVerdictByMint} calls it only for entries that MATCH, so a selective predicate pays
 * the translation for a handful of entries and nothing for the rest.
 *
 * <p>
 * So the first row to touch a segment settles the WHOLE segment: one pass over its dictionary in
 * storage order, decoding each block once. The alternative this replaced — resolving lazily per
 * REFERENCED cell — addresses an id and so needs no rank reasoning at all, but it is a random read
 * per distinct value, and a random read re-decodes a block that a neighbouring value has usually just
 * decoded. At 100M rows that difference was measured at 46 s for a single LIKE. The sweep pays for
 * values the column never references (a segment's dictionary is shared with fields this column does
 * not hold), and wins anyway, because sequential decode amortises over every entry in the block.
 * </p>
 *
 * <h2>Cost</h2>
 *
 * One byte-level comparison per dictionary ENTRY, once per segment, cached for the whole query; the
 * per-row work is then two array reads against an already-settled table. No {@link String} is built
 * at any point — the comparison runs on the stored bytes through the same per-entry authority the
 * per-leaf dictionary kernels use. A segment whose sweep is refused falls back to the per-cell path,
 * which answers identically and is what every unit test of the memo exercises.
 *
 * <h2>Threading</h2>
 *
 * The scan is parallel over leaves. The memo is a per-segment {@code byte[]} indexed by the cell's
 * dense id, so a hit is lock-free; only the first touch of a given {@code (segment, id)} takes this
 * instance's monitor. A stale read of a table entry is benign — {@code byte} writes never tear, so a
 * reader sees either {@link #UNKNOWN} (and takes the idempotent slow path) or a settled verdict.
 *
 * <p>
 * The DICTIONARY READ, by contrast, runs outside every lock, and that is only sound because the
 * constructor takes a {@link Supplier} of views rather than a view: each worker must be handed its
 * own. A {@link GlobalValueDictionary.ReadView} holds plain mutable caches, so two workers sharing
 * one tear its state and the parse fails with {@code AssertionError: Type not known}. Sharing a view
 * and serialising the read behind this monitor is correct but makes the read a scan-wide bottleneck,
 * because it fetches a page and decodes a block while every other worker waits.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentCellVerdicts {

  private static final boolean PROJ_DIAG = Boolean.getBoolean("sirix.projDiag");

  /** Not yet evaluated — a row whose entry reads this must take {@link #matchesSlow}. */
  public static final byte UNSETTLED = 0;

  /** Not yet evaluated. */
  private static final byte UNKNOWN = UNSETTLED;
  private static final byte MATCHES = 1;
  private static final byte REJECTS = 2;
  /** The cell names no entry of its segment: it can satisfy no per-value predicate. */
  private static final byte UNRESOLVABLE = 3;

  /** No sweep attempted for this segment yet. */
  private static final byte SWEEP_PENDING = 0;
  /** The segment's table is fully settled; every in-range id reads a verdict. */
  private static final byte SWEEP_DONE = 1;
  /**
   * The segment cannot be swept, so its table is filled per cell and stays PARTIALLY settled. Distinct
   * from {@link #SWEEP_DONE} because the two tables read identically but mean opposite things: an
   * {@link #UNKNOWN} entry in a settled table would be a wrong answer, in a partial one it is the
   * signal to take the slow path.
   */
  private static final byte SWEEP_REFUSED = 2;

  /**
   * How one cell's value is tested. Narrowed to this single operation so the memo's semantics can be
   * tested without a dictionary, and because only the slow path calls it — the indirection is never
   * on the row path.
   */
  @FunctionalInterface
  public interface CellMatcher {
    /** Whether the value {@code cell} names satisfies the predicate; {@code null} if it names none. */
    @Nullable Boolean matches(long cell);
  }

  /**
   * How one segment's dictionary is settled in a single pass.
   *
   * <p>
   * Separate from {@link CellMatcher} so the memo's semantics stay testable without a dictionary, and
   * so a resource whose segments cannot be swept — one whose view is not a union, or a segment that
   * sealed nothing — degrades to the per-cell path rather than failing.
   * </p>
   */
  @FunctionalInterface
  public interface SegmentSweeper {
    /**
     * Verdict bits over the MINT ids of the segment {@code anyCellInSegment} names, or {@code null}
     * when that segment cannot be swept.
     */
    long @Nullable [] sweep(long anyCellInSegment);
  }

  private final CellMatcher matcher;
  private final @Nullable SegmentSweeper sweeper;
  /** One lock per segment, so two workers settle two different segments concurrently. */
  private final Object[] sweepLocks;
  /** {@code sweepState[s]} — guarded by {@code sweepLocks[s]}; see {@link #SWEEP_PENDING}. */
  private final byte[] sweepState;
  private final AtomicInteger sweeps = new AtomicInteger();
  private final AtomicInteger refusals = new AtomicInteger();
  private final ProjectionIndexScan.Op op;
  private final byte[] literalUtf8;
  private final boolean literalHasSupplementary;

  /** {@code memo[segment][id]} — written under this instance's monitor, read without one. */
  @SuppressWarnings("VolatileArrayField") // the volatile is on the reference, which is what publishes
  private volatile byte[] @Nullable [] memo;

  /**
   * @param views supplier of a view PRIVATE TO THE CALLING THREAD — see the threading note above; a
   *        supplier that hands the same view to two workers will tear its caches
   * @param op a per-value string op ({@code STR_*}, {@code EQ}, {@code NE})
   * @param literalUtf8 the literal's UTF-8 bytes
   * @param segments how many segments the resource sealed, so the memo is sized once
   */
  public SegmentCellVerdicts(final Supplier<GlobalValueDictionary.ReadView> views,
      final ProjectionIndexScan.Op op, final byte[] literalUtf8, final int segments) {
    this(matcherOver(requireNonNull(views, "views must not be null"), op,
            requireNonNull(literalUtf8, "literalUtf8 must not be null")), sweeperOver(views, op, literalUtf8), op,
        literalUtf8, segments);
  }

  /**
   * @param matcher how one cell's value is tested
   * @param op the op being answered
   * @param literalUtf8 the literal, kept for the predicate's own bookkeeping
   * @param segments how many segments the resource sealed
   */
  public SegmentCellVerdicts(final CellMatcher matcher, final ProjectionIndexScan.Op op, final byte[] literalUtf8,
      final int segments) {
    this(matcher, null, op, literalUtf8, segments);
  }

  /**
   * @param matcher how one cell's value is tested, and the fallback whenever a sweep is refused
   * @param sweeper how a whole segment is settled at once, or {@code null} for the per-cell path only
   * @param op the op being answered
   * @param literalUtf8 the literal, kept for the predicate's own bookkeeping
   * @param segments how many segments the resource sealed
   */
  public SegmentCellVerdicts(final CellMatcher matcher, final @Nullable SegmentSweeper sweeper,
      final ProjectionIndexScan.Op op, final byte[] literalUtf8, final int segments) {
    this.matcher = requireNonNull(matcher, "matcher must not be null");
    this.sweeper = sweeper;
    this.op = requireNonNull(op, "op must not be null");
    this.literalUtf8 = requireNonNull(literalUtf8, "literalUtf8 must not be null");
    if (segments < 0) {
      throw new IllegalArgumentException("segments must not be negative: " + segments);
    }
    this.literalHasSupplementary = ProjectionIndexScan.hasFourByteUtf8(literalUtf8, 0, literalUtf8.length);
    final int lanes = Math.max(segments, 1);
    this.memo = new byte[lanes][];
    this.sweepState = new byte[lanes];
    this.sweepLocks = new Object[lanes];
    for (int segment = 0; segment < lanes; segment++) {
      this.sweepLocks[segment] = new Object();
    }
  }

  private static SegmentSweeper sweeperOver(final Supplier<GlobalValueDictionary.ReadView> views,
      final ProjectionIndexScan.Op op, final byte[] literalUtf8) {
    return cell -> views.get().stringOpVerdictByMintOfCell(cell, op, literalUtf8);
  }

  private static CellMatcher matcherOver(final Supplier<GlobalValueDictionary.ReadView> views,
      final ProjectionIndexScan.Op op, final byte[] literalUtf8) {
    final boolean supplementary = ProjectionIndexScan.hasFourByteUtf8(literalUtf8, 0, literalUtf8.length);
    return cell -> views.get().cellMatchesStringOp(cell, op, literalUtf8, supplementary);
  }

  /** The op this verdict answers. */
  public ProjectionIndexScan.Op op() {
    return op;
  }

  /**
   * The settled-verdict table for the leaf {@code anyCellInLeaf} belongs to, or {@code null} when the
   * segment has none yet.
   *
   * <p>
   * The HOT-PATH entry point, and the reason it exists: a row loop that calls {@link #matches} pays a
   * volatile read of the memo and a virtual call FOR EVERY ROW. A leaf never straddles a segment, so
   * the table is the same for all of its rows — read it once here, then test rows against the array
   * directly. A table replaced by a concurrent grow leaves the captured reference stale, which is
   * benign: its entries read {@link #UNSETTLED} and those rows take {@link #matchesSlow}, whose
   * answer is identical.
   * </p>
   */
  public byte @Nullable [] tableForLeaf(final long anyCellInLeaf) {
    final int segment = ProjectionIndexRowGroupPage.segmentOfCell(anyCellInLeaf);
    final byte[][] tables = memo;
    return segment >= 0 && segment < tables.length
        ? tables[segment]
        : null;
  }

  /** Whether a settled entry from {@link #tableForLeaf} is a match. */
  public static boolean isMatch(final byte settled) {
    return settled == MATCHES;
  }

  /** The full lookup, for a row whose entry is not settled yet. */
  public boolean matchesSlow(final long cell) {
    return matches(cell);
  }

  /** Whether the value {@code cell} names satisfies the predicate. An unresolvable cell never does. */
  public boolean matches(final long cell) {
    final int segment = ProjectionIndexRowGroupPage.segmentOfCell(cell);
    final int id = ProjectionIndexRowGroupPage.idOfCell(cell);
    final byte[][] tables = memo;
    if (segment >= 0 && segment < tables.length && id >= 0) {
      final byte[] table = tables[segment];
      if (table != null && id < table.length) {
        final byte settled = table[id];
        if (settled != UNKNOWN) {
          return settled == MATCHES;
        }
      }
    }
    return evaluateAndMemoise(cell, segment, id) == MATCHES;
  }

  private byte evaluateAndMemoise(final long cell, final int segment, final int id) {
    if (segment < 0 || id < 0) {
      return UNRESOLVABLE;
    }
    final SegmentSweeper sweep = sweeper;
    if (sweep != null && segment < sweepLocks.length) {
      final byte[] settled = settleSegment(sweep, cell, segment);
      if (settled != null) {
        // A settled table answers every id its dictionary holds; anything above it names no entry.
        return id < settled.length
            ? settled[id]
            : UNRESOLVABLE;
      }
    }
    return evaluatePerCell(cell, segment, id);
  }

  /**
   * Settle {@code segment} in one pass, returning its fully-populated table, or {@code null} when the
   * sweep is refused and the caller must fall back to the per-cell path.
   *
   * <p>
   * Locked per segment rather than per instance: the scan is parallel over leaves and leaves of
   * DIFFERENT segments arrive together, so an instance-wide monitor would serialise every segment's
   * sweep onto one thread while the rest of the pool waited on work it could have been doing.
   * </p>
   */
  private byte @Nullable [] settleSegment(final SegmentSweeper sweep, final long cell, final int segment) {
    synchronized (sweepLocks[segment]) {
      final byte state = sweepState[segment];
      if (state == SWEEP_DONE) {
        return memo[segment];
      }
      if (state == SWEEP_REFUSED) {
        return null;
      }
      final long[] bits;
      try {
        bits = sweep.sweep(cell);
      } catch (final RuntimeException unsweepable) {
        // A segment that sealed no dictionary for this column, or whose directory cannot answer a
        // sweep, is not an error: its rows keep their bytes and the per-cell path resolves them. It
        // IS worth reporting, because a silent refusal looks exactly like a slow query.
        refuse(segment, unsweepable.getMessage(), unsweepable);
        return null;
      }
      if (bits == null) {
        refuse(segment, "the sweeper declined this segment");
        return null;
      }
      final byte[] table = expand(bits);
      install(segment, table);
      sweepState[segment] = SWEEP_DONE;
      sweeps.incrementAndGet();
      return table;
    }
  }

  /**
   * Record a refused sweep, reporting the first one under {@code -Dsirix.projDiag}.
   *
   * <p>
   * A refusal costs nothing but correctness-preserving slowness, so it must never throw; but it also
   * cannot be silent, because a whole run falling back to the per-cell path is indistinguishable from
   * a query that is simply slow. One line names the segment and the reason.
   * </p>
   */
  private void refuse(final int segment, final @Nullable String reason) {
    refuse(segment, reason, null);
  }

  private void refuse(final int segment, final @Nullable String reason, final @Nullable Throwable trace) {
    sweepState[segment] = SWEEP_REFUSED;
    if (PROJ_DIAG && refusals.getAndIncrement() == 0) {
      System.err.println("[proj] segment verdict sweep REFUSED for " + op + ", first at segment " + segment + ": "
          + reason + " — falling back to one dictionary read per referenced cell");
      if (trace != null) {
        trace.printStackTrace(System.err);
      }
    }
  }

  /** Segments settled by a sweep, and segments that refused one — the instrument for a slow run. */
  public String sweepAudit() {
    return "swept=" + sweeps.get() + " refused=" + refusals.get();
  }

  /**
   * A verdict bitset as a settled byte table — {@link #MATCHES} where a bit is set, {@link #REJECTS}
   * everywhere else.
   *
   * <p>
   * The fill is over the whole table and the loop then runs ONCE PER SET BIT rather than once per id:
   * {@code word & (word - 1)} clears the lowest set bit, so a selective predicate walks only its
   * matches. Reading a bit per row instead would save this table's bytes, but the row loop already
   * indexes a {@code byte[]} for the per-cell path, and one settled representation is worth more than
   * the eight-fold shrink of a per-query structure.
   * </p>
   */
  private static byte[] expand(final long[] bits) {
    final byte[] table = new byte[bits.length << 6];
    Arrays.fill(table, REJECTS);
    for (int word = 0; word < bits.length; word++) {
      final int base = word << 6;
      long remaining = bits[word];
      while (remaining != 0L) {
        table[base | Long.numberOfTrailingZeros(remaining)] = MATCHES;
        remaining &= remaining - 1;
      }
    }
    return table;
  }

  /**
   * Publish {@code table} as {@code segment}'s memo, growing the memo array if the segment lies beyond
   * the count the constructor was told.
   *
   * <p>
   * Under the INSTANCE monitor, not the segment's, because it may replace the memo array itself —
   * which {@link #evaluatePerCell} can also do, and two threads replacing it from different arrays
   * would drop one of them. The sweep itself stays outside this monitor, so segments still settle in
   * parallel and only the O(1) install serialises. Lock order is always segment-then-instance; nothing
   * takes them the other way round.
   * </p>
   */
  private synchronized void install(final int segment, final byte[] table) {
    byte[][] tables = memo;
    if (segment >= tables.length) {
      tables = Arrays.copyOf(tables, segment + 1);
    }
    tables[segment] = table;
    memo = tables; // volatile write: publishes the table's contents to every reader of the memo
  }

  /**
   * Resolve one cell and remember the answer.
   *
   * <p>
   * The dictionary read happens OUTSIDE the monitor; only the install is under it. That split is the
   * point, and it is only sound because the matcher reads a view PRIVATE TO THE CALLING THREAD.
   * {@link GlobalValueDictionary.ReadView} carries plain mutable caches — a per-id slice cache, a
   * retained bucket and block — so two threads reading ONE view tear each other's state and the parse
   * fails with {@code AssertionError: Type not known}. Hoisting the read out of the lock while the
   * view was still shared was measured, and it broke exactly that way; the fix is a view per worker,
   * not a smaller critical section, which is why the constructor takes a supplier and not a view.
   * </p>
   *
   * <p>
   * Racing threads may evaluate the same cell twice. That costs a repeated read and changes no
   * answer — {@link #evaluate} is idempotent and free of side effects.
   * </p>
   */
  private byte evaluatePerCell(final long cell, final int segment, final int id) {
    return memoise(segment, id, evaluate(cell));
  }

  private synchronized byte memoise(final int segment, final int id, final byte verdict) {
    byte[][] tables = memo;
    if (segment >= tables.length) {
      tables = Arrays.copyOf(tables, segment + 1);
    }
    byte[] table = tables[segment];
    if (table == null) {
      table = new byte[Math.max(id + 1, 1024)];
      tables[segment] = table;
    } else if (id >= table.length) {
      // Ids are dense mints, so growth is rare; double so a scan walking a dictionary upward does not
      // copy per entry.
      table = Arrays.copyOf(table, Math.max(id + 1, table.length << 1));
      tables[segment] = table;
    } else if (table[id] != UNKNOWN) {
      return table[id]; // another worker settled it while this one was reading the dictionary
    }
    table[id] = verdict;
    memo = tables; // volatile write: publishes both the entry above and any grown array
    return verdict;
  }

  private byte evaluate(final long cell) {
    final Boolean matched;
    try {
      matched = matcher.matches(cell);
    } catch (final RuntimeException unresolvable) {
      return UNRESOLVABLE;
    }
    if (matched == null) {
      return UNRESOLVABLE;
    }
    return matched
        ? MATCHES
        : REJECTS;
  }
}
