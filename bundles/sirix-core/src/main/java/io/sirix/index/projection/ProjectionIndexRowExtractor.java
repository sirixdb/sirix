/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Extracts one projection row — the declared fields of a single record —
 * from the document via rtx navigation. This is the SINGLE source of truth
 * for extraction semantics, shared by the bulk {@link ProjectionIndexBuilder}
 * (initial build) and the incremental maintenance path in
 * {@link ProjectionIndexChangeListener} (per-record re-extraction at
 * pre-commit), so a row produced by maintenance is byte-for-byte what a full
 * rebuild would produce for the same record state.
 *
 * <h2>HFT-grade hot path</h2>
 * Per-record work allocates nothing — the extractor owns reusable per-row
 * arrays ({@code long[]} / {@code boolean[]} / {@code String[]}) sized to the
 * declared field count and a reusable DFS work-list, and populates them in
 * place via rtx navigation + primitive-typed getters. The field
 * (pathNodeKey → column) mapping is kept as two parallel flat arrays — at
 * typical projection width the linear scan fits a cache line and
 * JIT-inlines cleanly.
 *
 * <p>The PCR mapping is resolved from the path summary at CONSTRUCTION time;
 * maintenance constructs a fresh extractor per commit so field paths created
 * by the running transaction are picked up.
 */
public final class ProjectionIndexRowExtractor {

  /**
   * Flattened (pathNodeKey → column) pairs for the declared fields. A field
   * path resolving to MULTIPLE pathNodeKeys (same shape under different
   * roots) contributes one pair per PCR — {@link #findField} matches any of
   * them. A field whose path resolves to nothing contributes no pair: such
   * records carry only {@code present == false} for that column.
   */
  private final long[] fieldPcrKeys;
  private final int[] fieldPcrColumns;

  /** Per-field column kind, index-aligned with projection fields. */
  private final byte[] columnKinds;

  /** Reusable per-row extraction buffers — one entry per field. Zero alloc in the hot loop. */
  private final long[] rowLongs;
  private final boolean[] rowBools;
  private final String[] rowStrings;
  /** Per-row presence: the field EXISTS on the record (even when unrepresentable). */
  private final boolean[] rowPresent;
  /** Per-row poison: present field whose value the column kind cannot hold (null / object / array / mismatch). */
  private final boolean[] rowUnrepresentable;
  /** Per-row provenance: NUMERIC_LONG cell truncated from a non-integral number. */
  private final boolean[] rowNonIntegral;

  /**
   * Per-column flag: a NUMERIC_LONG cell was fed from a non-integral number
   * (double/decimal with a fraction) and was therefore TRUNCATED by
   * {@code Number#longValue()}. Consumers must not serve value-exact
   * answers (aggregates, comparisons) from such a column.
   */
  private final boolean[] numericColumnSawNonIntegral;

  /**
   * Reusable DFS work-list (pre-sized) — holds nodeKeys of unprocessed
   * subtree roots. Generic for any nested record shape; grown once when deep
   * records are seen, never per row.
   */
  private long[] workList = new long[64];
  private int workListSize;

  public ProjectionIndexRowExtractor(final IndexDef indexDef, final PathSummaryReader pathSummary) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionIndexRowExtractor requires an IndexType.PROJECTION IndexDef; got "
              + indexDef.getType());
    }
    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final List<Type> fieldTypes = indexDef.getProjectionFieldTypes();
    this.columnKinds = new byte[fieldPaths.size()];
    this.numericColumnSawNonIntegral = new boolean[fieldPaths.size()];
    final LongArrayList pcrKeys = new LongArrayList();
    final IntArrayList pcrCols = new IntArrayList();
    for (int i = 0; i < fieldPaths.size(); i++) {
      final Set<Path<QNm>> one = new HashSet<>();
      one.add(fieldPaths.get(i));
      for (final Long pcr : pathSummary.getPCRsForPaths(one)) {
        pcrKeys.add(pcr.longValue());
        pcrCols.add(i);
      }
      columnKinds[i] = ProjectionIndexBuilder.mapTypeToColumnKind(fieldTypes.get(i));
    }
    this.fieldPcrKeys = pcrKeys.toLongArray();
    this.fieldPcrColumns = pcrCols.toIntArray();
    this.rowLongs = new long[fieldPaths.size()];
    this.rowBools = new boolean[fieldPaths.size()];
    this.rowStrings = new String[fieldPaths.size()];
    this.rowPresent = new boolean[fieldPaths.size()];
    this.rowUnrepresentable = new boolean[fieldPaths.size()];
    this.rowNonIntegral = new boolean[fieldPaths.size()];
  }

  /** Per-column kinds, index-aligned with the projection's declared fields. */
  public byte[] columnKinds() {
    return columnKinds.clone();
  }

  /** No-clone view for same-package hot paths (leaf construction). */
  byte[] columnKindsRef() {
    return columnKinds;
  }

  /** Snapshot of the per-column non-integral flags, index-aligned with the fields. */
  public boolean[] numericColumnNonIntegralFlags() {
    return numericColumnSawNonIntegral.clone();
  }

  /**
   * Navigate to {@code recordKey} and fill the row buffers from its current
   * state. The cursor is left positioned at {@code recordKey}.
   *
   * @return {@code false} when the record no longer exists (deleted in the
   *         running transaction) — the caller drops the row
   */
  public boolean extractInto(final JsonNodeReadOnlyTrx rtx, final long recordKey) {
    if (!rtx.moveTo(recordKey)) {
      return false;
    }
    extractAt(rtx, recordKey);
    return true;
  }

  /**
   * Append the buffers filled by the last {@link #extractInto}/{@link #extractAt}
   * call as one row of {@code leaf}.
   *
   * @return {@code false} when {@code leaf} is at capacity (caller opens a
   *         fresh leaf and retries)
   */
  public boolean appendTo(final ProjectionIndexLeafPage leaf, final long recordKey) {
    return leaf.appendRow(recordKey, rowLongs, rowBools, rowStrings, rowPresent,
        rowUnrepresentable, rowNonIntegral);
  }

  /**
   * Fill the row buffers for the record at {@code recordKey}; the cursor is
   * assumed to be able to reach it (bulk-build path positions it during
   * traversal). Ends with the cursor back at {@code recordKey}.
   */
  void extractAt(final JsonNodeReadOnlyTrx rtx, final long recordKey) {
    // Reset per-row slots — fields we fail to resolve stay "missing"
    // (presence bit clear) and serialise as defaults on the leaf page.
    for (int i = 0; i < columnKinds.length; i++) {
      rowLongs[i] = 0L;
      rowBools[i] = false;
      rowStrings[i] = "";
      rowPresent[i] = false;
      rowUnrepresentable[i] = false;
      rowNonIntegral[i] = false;
    }
    // Generic DFS: walk every descendant of recordKey via an explicit
    // work-list of unvisited first-children. For each node we visit:
    //   - a fused OBJECT_NAMED_* record matching a declared field reads its
    //     inline value straight into the row
    //   - structured kinds (OBJECT / ARRAY / fused containers) descend so
    //     declared NESTED fields below them are found.
    workListSize = 0;
    pushFirstChild(rtx, recordKey);
    while (workListSize > 0) {
      final long top = workList[--workListSize];
      rtx.moveTo(top);
      // Walk right-sibling chain at this level inline.
      long cur = top;
      do {
        final NodeKind kind = rtx.getKind();
        if (kind == NodeKind.OBJECT_NAMED_OBJECT || kind == NodeKind.OBJECT_NAMED_ARRAY) {
          final long pk = rtx.getPathNodeKey();
          final int col = findField(pk);
          if (col >= 0) {
            // Object/array-valued field declared as a primitive column: present
            // but UNREPRESENTABLE.
            rowPresent[col] = true;
            rowUnrepresentable[col] = true;
          }
          // Descend regardless — declared NESTED fields live below this node.
          pushFirstChild(rtx, cur);
        } else if (kind == NodeKind.OBJECT_NAMED_BOOLEAN
            || kind == NodeKind.OBJECT_NAMED_NUMBER
            || kind == NodeKind.OBJECT_NAMED_STRING
            || kind == NodeKind.OBJECT_NAMED_NULL) {
          // Fused OBJECT_NAMED_* record — value lives inline on this node. Zero-alloc
          // direct extraction, no synthetic-child navigation.
          final long pk = rtx.getPathNodeKey();
          final int col = findField(pk);
          if (col >= 0) {
            readFusedValueIntoRow(rtx, kind, col);
          }
          // Fused nodes have no children. No descent.
        } else if (kind == NodeKind.OBJECT || kind == NodeKind.ARRAY) {
          // Structured — descend.
          pushFirstChild(rtx, cur);
        }
        // Primitives have no children; skip.
        if (!rtx.moveToRightSibling()) break;
        cur = rtx.getNodeKey();
      } while (true);
    }
    rtx.moveTo(recordKey);
  }

  private void pushFirstChild(final JsonNodeReadOnlyTrx rtx, final long parentKey) {
    final long saved = rtx.getNodeKey();
    rtx.moveTo(parentKey);
    if (rtx.moveToFirstChild()) {
      if (workListSize == workList.length) {
        workList = Arrays.copyOf(workList, workList.length * 2);
      }
      workList[workListSize++] = rtx.getNodeKey();
    }
    rtx.moveTo(saved);
  }

  private int findField(final long pathNodeKey) {
    // Linear scan over the flattened (pcr -> column) pairs is cheaper than
    // a HashMap lookup at typical projection width (~5 fields, one or a few
    // PCRs each) — fits in a cache line and JIT-inlines cleanly.
    for (int i = 0; i < fieldPcrKeys.length; i++) {
      if (fieldPcrKeys[i] == pathNodeKey) return fieldPcrColumns[i];
    }
    return -1;
  }

  private static boolean isNonIntegral(final Number n) {
    if (n instanceof Double || n instanceof Float) {
      final double d = n.doubleValue();
      return d != Math.rint(d) || Math.abs(d) > (double) Long.MAX_VALUE;
    }
    if (n instanceof BigDecimal bd) {
      return bd.stripTrailingZeros().scale() > 0;
    }
    return false;
  }

  /**
   * Read the primitive value off a fused {@code OBJECT_NAMED_*} record directly into
   * the current row. The rtx's value predicates already return true on a fused
   * record; dispatch is by record kind: fused-number → numeric column, fused-boolean
   * → boolean column, fused-string → string column, fused-null → present but
   * unrepresentable (no column kind can hold it).
   */
  private void readFusedValueIntoRow(final JsonNodeReadOnlyTrx rtx, final NodeKind fusedKind,
      final int col) {
    rowPresent[col] = true;
    final byte columnKind = columnKinds[col];
    switch (fusedKind) {
      case OBJECT_NAMED_NUMBER -> {
        final Number n = columnKind == ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG
            ? rtx.getNumberValue()
            : null;
        if (n != null) {
          if (isNonIntegral(n)) {
            numericColumnSawNonIntegral[col] = true;
            rowNonIntegral[col] = true;
          }
          rowLongs[col] = n.longValue();
        } else {
          // Kind mismatch (number where the column expects bool/string) or a
          // null Number — present but unrepresentable.
          rowUnrepresentable[col] = true;
        }
      }
      case OBJECT_NAMED_BOOLEAN -> {
        if (columnKind == ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN) {
          rowBools[col] = rtx.getBooleanValue();
        } else {
          rowUnrepresentable[col] = true;
        }
      }
      case OBJECT_NAMED_STRING -> {
        final String v = columnKind == ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT ? rtx.getValue() : null;
        if (v != null) {
          rowStrings[col] = v;
        } else {
          rowUnrepresentable[col] = true;
        }
      }
      // OBJECT_NAMED_NULL → present-but-null: no column kind can represent it.
      default -> rowUnrepresentable[col] = true;
    }
  }
}
