/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ZoneIndex;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import org.jspecify.annotations.Nullable;

/**
 * Query-local whole-segment bounds for one string key with a directly resolved literal exclusion.
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
  private final ColumnPredicate exclusion;
  private final boolean descending;
  private final long[] bounds;
  private int positionLookups;

  private SegmentTopKBounds(final GlobalValueDictionary.ReadView view, final ColumnPredicate exclusion,
      final boolean descending) {
    this.view = view;
    this.exclusion = exclusion;
    this.descending = descending;
    bounds = new long[view.segmentCount()]; // zero is not a valid cell: it means not yet requested
  }

  /** A key predicate excludes missing keys; unsupported refinements retain unknown bounds. */
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
    return exclusion == null ? null : new SegmentTopKBounds(view, exclusion, descending);
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
        if (bound != UNKNOWN && bound == exclusion.literalForLeaf(min)) {
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

  int compare(final long left, final long right) {
    return left == right ? 0 : view.compareCells(left, right);
  }

  /** Position requests, not physical page reads; used by diagnostics and the bounded-setup witness. */
  int positionLookups() {
    return positionLookups;
  }
}
