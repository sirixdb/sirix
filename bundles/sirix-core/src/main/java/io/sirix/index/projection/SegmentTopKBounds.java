/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ZoneIndex;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.jspecify.annotations.Nullable;

/**
 * Query-local whole-segment bounds for one ordered string key, refined past a directly resolved
 * literal exclusion when the query carries one.
 * Zones prove segment membership only: their mint extrema are never interpreted as lexical extrema.
 * The bound comes from the dictionary's first/last COLLATION position, skipping the excluded value.
 * At most two positions are read per admitted segment; no rows or full dictionaries are scanned.
 * The planning view is used only on the caller's thread. Evaluation compares bounds through each
 * heap's own view, just as it compares row values.
 */
final class SegmentTopKBounds {
  static final long UNKNOWN = Long.MIN_VALUE;
  static final long EMPTY = Long.MIN_VALUE + 1;

  private final GlobalValueDictionary.ReadView view;
  private final @Nullable ColumnPredicate exclusion;
  private final boolean descending;
  private final long[] bounds;
  /** Dense collation ordinals of {@link #bounds}, by segment; built once by {@link #rankBounds()}. */
  private int @Nullable [] ranks;
  private int positionLookups;

  private SegmentTopKBounds(final GlobalValueDictionary.ReadView view, final @Nullable ColumnPredicate exclusion,
      final boolean descending) {
    this.view = view;
    this.exclusion = exclusion;
    this.descending = descending;
    bounds = new long[view.segmentCount()]; // zero is not a valid cell: it means not yet requested
  }

  /**
   * Bounds for an ordered LIMIT over a segment-scoped key, with or without a predicate on that key.
   * A directly resolved {@code <>} refines the endpoint past its literal; an EQ, a range, a second
   * predicate on the key or an exclusion that needs per-cell verdicts is DECLINED, because those
   * would have to prove which of the endpoint's neighbours still match, which the endpoints alone
   * cannot say. Missing keys are not ruled out here: a bound is usable only where the caller has
   * proven the column all-present on the leaf or a predicate names it (missing then never matches).
   */
  static @Nullable SegmentTopKBounds create(final GlobalValueDictionary.ReadView view,
      final ColumnPredicate[] predicates, final int column, final boolean descending) {
    if (!view.isSegmentUnion()) {
      return null;
    }
    ColumnPredicate exclusion = null;
    for (final ColumnPredicate predicate : predicates) {
      if (predicate.column != column) {
        continue; // residual predicates can only narrow the bounded set
      }
      if (exclusion != null || predicate.op != Op.NE || predicate.segmentLiteralCells == null
          || predicate.segmentCellVerdicts != null) {
        return null;
      }
      exclusion = predicate;
    }
    return new SegmentTopKBounds(view, exclusion, descending);
  }

  long forLeaf(final ZoneIndex zone, final int leaf) {
    if (!zone.known(leaf) || zone.allMissing(leaf)) {
      return UNKNOWN;
    }
    final long min = zone.min(leaf);
    final long max = zone.max(leaf);
    final int segment = ProjectionIndexRowGroupPage.segmentOfCell(min);
    if (min > max || segment < 0 || segment >= bounds.length
        || segment != ProjectionIndexRowGroupPage.segmentOfCell(max)) {
      return UNKNOWN;
    }
    final int count = view.entryCountOfSegment(segment);
    if (ProjectionIndexRowGroupPage.idOfCell(min) < 1
        || ProjectionIndexRowGroupPage.idOfCell(max) > count || count < 1) {
      return UNKNOWN;
    }
    long bound = bounds[segment];
    if (bound == 0L) {
      if (view.segmentEntryCount(min) < 1) {
        bound = UNKNOWN; // unordered storage has no collation endpoint to read
      } else {
        int position = descending ? count : 1;
        bound = cellAt(min, segment, position, count);
        if (exclusion != null && bound != UNKNOWN && bound == exclusion.literalForLeaf(min)) {
          position += descending ? -1 : 1;
          bound = position < 1 || position > count ? EMPTY : cellAt(min, segment, position, count);
        }
      }
      bounds[segment] = bound;
    }
    return bound;
  }

  private long cellAt(final long probe, final int segment, final int position, final int count) {
    positionLookups++;
    final int mint = view.mintAtPositionOfCell(probe, position);
    return mint < 1 || mint > count ? UNKNOWN : ProjectionIndexRowGroupPage.packSegmentCell(segment, mint);
  }

  /**
   * Rank the requested bounds — at most one per segment — through their dictionary VALUES once, so
   * ordering the leaves compares dense ordinals instead of resolving two dictionary slices per
   * comparison. Equal values share an ordinal, which keeps the caller's tie handling intact.
   */
  void rankBounds() {
    final int segmentCount = bounds.length;
    final int[] order = new int[segmentCount];
    int n = 0;
    for (int segment = 0; segment < segmentCount; segment++) {
      final long bound = bounds[segment];
      if (bound != 0L && bound != UNKNOWN && bound != EMPTY) {
        order[n++] = segment;
      }
    }
    IntArrays.mergeSort(order, 0, n, (left, right) -> {
      final int cmp = view.compareCells(bounds[left], bounds[right]);
      return cmp == 0 ? Integer.compare(left, right) : cmp;
    });
    final int[] ranked = new int[segmentCount];
    int rank = 0;
    for (int i = 0; i < n; i++) {
      if (i > 0 && view.compareCells(bounds[order[i - 1]], bounds[order[i]]) != 0) {
        rank++;
      }
      ranked[order[i]] = rank;
    }
    ranks = ranked;
  }

  /** The collation ordinal of a bound cell this instance produced; {@link #rankBounds()} runs first. */
  int rankOf(final long cell) {
    final int[] ranked = ranks;
    if (ranked == null) {
      throw new IllegalStateException("rankBounds() must run before a bound is ranked");
    }
    return ranked[ProjectionIndexRowGroupPage.segmentOfCell(cell)];
  }

  /** Position requests, not physical page reads; used by diagnostics and the bounded-setup witness. */
  int positionLookups() {
    return positionLookups;
  }
}
