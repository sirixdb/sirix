/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Answers a per-VALUE string predicate — containment, ordering — over a segment-scoped column, one
 * distinct {@code (segment, id)} cell at a time.
 *
 * <h2>Why not a dictionary sweep</h2>
 *
 * A resource-wide dictionary answers these ops by evaluating the predicate once against every entry
 * and handing the kernels a verdict bitset over id space. A segment-scoped column cannot reuse that
 * as it stands, for two reasons, and the second is the interesting one:
 * <ul>
 * <li>the bitset would have to be one per segment, keyed by the cell's low 32 bits; and</li>
 * <li>the sweep walks STORAGE in position order, while a sealed segment dictionary carries a rank
 * table — its ids are arrival-order mints and its storage is collation order, so the sweep would set
 * the right bits at the wrong ids.</li>
 * </ul>
 *
 * <p>
 * Evaluating lazily per referenced cell sidesteps both. It addresses an ID, so a rank table is the
 * view's problem and not this class's; and it pays for the values the column actually REFERENCES
 * rather than every value the segment's document pages happened to intern — which is at most the
 * dictionary's size and usually far less, since a segment's dictionary is shared with fields this
 * column never holds.
 * </p>
 *
 * <h2>Cost</h2>
 *
 * One dictionary read and one byte-level comparison per distinct cell, cached for the whole query;
 * the per-row work is two array reads. No {@link String} is built at any point — the comparison runs
 * on the stored bytes through the same per-entry authority the per-leaf dictionary kernels use.
 *
 * <h2>Threading</h2>
 *
 * The scan is parallel over leaves. The memo is a per-segment {@code byte[]} indexed by the cell's
 * dense id, so a hit is lock-free; only the first touch of a given {@code (segment, id)} takes this
 * instance's monitor. A stale read of a table entry is benign — {@code byte} writes never tear, so a
 * reader sees either {@link #UNKNOWN} (and takes the idempotent slow path) or a settled verdict.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentCellVerdicts {

  /** Not yet evaluated — a row whose entry reads this must take {@link #matchesSlow}. */
  public static final byte UNSETTLED = 0;

  /** Not yet evaluated. */
  private static final byte UNKNOWN = UNSETTLED;
  private static final byte MATCHES = 1;
  private static final byte REJECTS = 2;
  /** The cell names no entry of its segment: it can satisfy no per-value predicate. */
  private static final byte UNRESOLVABLE = 3;

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

  private final CellMatcher matcher;
  private final ProjectionIndexScan.Op op;
  private final byte[] literalUtf8;
  private final boolean literalHasSupplementary;

  /** {@code memo[segment][id]} — written under this instance's monitor, read without one. */
  @SuppressWarnings("VolatileArrayField") // the volatile is on the reference, which is what publishes
  private volatile byte[] @Nullable [] memo;

  /**
   * @param view resolver for the packed cells, typically a segment union view
   * @param op a per-value string op ({@code STR_*}, {@code EQ}, {@code NE})
   * @param literalUtf8 the literal's UTF-8 bytes
   * @param segments how many segments the resource sealed, so the memo is sized once
   */
  public SegmentCellVerdicts(final GlobalValueDictionary.ReadView view, final ProjectionIndexScan.Op op,
      final byte[] literalUtf8, final int segments) {
    this(matcherOver(requireNonNull(view, "view must not be null"), op,
        requireNonNull(literalUtf8, "literalUtf8 must not be null")), op, literalUtf8, segments);
  }

  /**
   * @param matcher how one cell's value is tested
   * @param op the op being answered
   * @param literalUtf8 the literal, kept for the predicate's own bookkeeping
   * @param segments how many segments the resource sealed
   */
  public SegmentCellVerdicts(final CellMatcher matcher, final ProjectionIndexScan.Op op, final byte[] literalUtf8,
      final int segments) {
    this.matcher = requireNonNull(matcher, "matcher must not be null");
    this.op = requireNonNull(op, "op must not be null");
    this.literalUtf8 = requireNonNull(literalUtf8, "literalUtf8 must not be null");
    if (segments < 0) {
      throw new IllegalArgumentException("segments must not be negative: " + segments);
    }
    this.literalHasSupplementary = ProjectionIndexScan.hasFourByteUtf8(literalUtf8, 0, literalUtf8.length);
    this.memo = new byte[Math.max(segments, 1)][];
  }

  private static CellMatcher matcherOver(final GlobalValueDictionary.ReadView view, final ProjectionIndexScan.Op op,
      final byte[] literalUtf8) {
    final boolean supplementary = ProjectionIndexScan.hasFourByteUtf8(literalUtf8, 0, literalUtf8.length);
    return cell -> view.cellMatchesStringOp(cell, op, literalUtf8, supplementary);
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

  private synchronized byte evaluateAndMemoise(final long cell, final int segment, final int id) {
    if (segment < 0 || id < 0) {
      return UNRESOLVABLE;
    }
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
      return table[id]; // another worker settled it between the fast-path read and this lock
    }
    final byte verdict = evaluate(cell);
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
