/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.json.FusedStringCursor;
import io.sirix.access.trx.node.json.PrimitiveNumberCursor;
import io.sirix.access.trx.node.json.objectvalue.PrimitiveNumberValue;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Extracts one projection row — the declared fields of a single record — from the document via rtx
 * navigation. This is the SINGLE source of truth for extraction semantics, shared by the bulk
 * {@link ProjectionIndexBuilder} (initial build) and the incremental maintenance path in
 * {@link ProjectionIndexChangeListener} (per-record re-extraction at pre-commit), so a row produced
 * by maintenance is byte-for-byte what a full rebuild would produce for the same record state.
 *
 * <h2>HFT-grade hot path</h2> Per-record work allocates nothing — the extractor owns reusable
 * per-row arrays ({@code long[]} / {@code boolean[]} / {@code String[]}) sized to the declared
 * field count and a reusable DFS work-list, and populates them in place via rtx navigation +
 * primitive-typed getters. The field (pathNodeKey → column) mapping is kept as two parallel flat
 * arrays — at typical projection width the linear scan fits a cache line and JIT-inlines cleanly.
 *
 * <p>
 * The PCR mapping is resolved from the path summary at CONSTRUCTION time; maintenance constructs a
 * fresh extractor per commit so field paths created by the running transaction are picked up.
 */
public final class ProjectionIndexRowExtractor {

  static final int MAX_STRING_SET_ELEMENTS_PER_ROW = 1 << 14;

  /**
   * Flattened (pathNodeKey → column) pairs for the declared fields. A field path resolving to
   * MULTIPLE pathNodeKeys (same shape under different roots) contributes one pair per PCR —
   * {@link #nextFieldMapping(long, long[], int)} matches all of them. A field whose path resolves to
   * nothing contributes no pair: such records carry only {@code present == false} for that column.
   *
   * <p>
   * Non-final because an INCREMENTAL (load-time) build resolves them against a path summary that is
   * still growing: on an empty resource no declared field has a path class yet, and a field whose
   * first occurrence is in record 5,000,000 gets its path class only then. {@link #refresh} re-reads
   * them; a stale set would record a present field as ABSENT, which is a wrong answer rather than a
   * slow one. The bulk builder holds ONE extractor for the whole build (the column-kinds array is
   * shared by reference with every leaf it produced), so refreshing in place is the only option.
   */
  private long[] fieldPcrKeys;
  private int[] fieldPcrColumns;

  /** PCRs that currently match the declared record-root path. */
  private long[] rootPcrKeys;

  /** The declared field paths, kept for {@link #refresh}. */
  private final List<Path<QNm>> fieldPaths;

  /** Declared source type per column; XML lexical conversion must retain this provenance. */
  private final Type[] fieldTypes;

  /** The declared record-root path, kept for membership refresh after XML renames/moves. */
  private final Path<QNm> rootPath;

  /** Per-field column kind, index-aligned with projection fields. */
  private final byte[] columnKinds;

  /** Reusable per-row extraction buffers — one entry per field. Zero alloc in the hot loop. */
  private final long[] rowLongs;
  private final boolean[] rowBools;
  private final byte[][] rowStringUtf8;
  private final int[] rowStringUtf8Lengths;

  /**
   * Grow-only caller-owned scalar-string buffers. Kept separate from {@link #rowStringUtf8} because a
   * custom cursor's public byte API may return node-owned storage that must never be overwritten.
   */
  private final byte[][] rowStringUtf8Scratch;

  /**
   * Per-column set elements for {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SET}. Reused
   * across rows and refilled in place — a projection build walks millions of records, so a fresh list
   * per row would be the dominant allocation of the build.
   */
  private final String[][] rowStringSets;

  /** Live element count per set column for the row being built. */
  private final int[] rowStringSetLen;
  /** Per-row presence: the field EXISTS on the record (even when unrepresentable). */
  private final boolean[] rowPresent;
  /**
   * Per-row poison: present field whose value the column kind cannot hold (null / object / array /
   * mismatch).
   */
  private final boolean[] rowUnrepresentable;
  /** Per-row provenance: NUMERIC_LONG cell truncated from a non-integral number. */
  private final boolean[] rowNonIntegral;
  /**
   * Per-row provenance: NUMERIC_DOUBLE cell converted from a source other than {@code Double} —
   * clears the leaf's {@link ProjectionIndexRowGroupPage#COLUMN_FLAG_PURE_DOUBLE_SOURCE} assertion
   * even when the conversion was exact (the interpreted fallback's result TYPE depends on source
   * typing: Integer/Big* rows aggregate decimal-exactly as {@code Dec}, Float rows in float
   * arithmetic as {@code Flt} — neither matches a served {@code Dbl}).
   */
  private final boolean[] rowNonDoubleSource;

  /**
   * Per-column flag: a NUMERIC_LONG cell was fed from a non-integral number (double/decimal with a
   * fraction) and was therefore TRUNCATED by {@code Number#longValue()}. Consumers must not serve
   * value-exact answers (aggregates, comparisons) from such a column.
   */
  private final boolean[] numericColumnSawNonIntegral;

  /**
   * Reusable DFS work-list (pre-sized) — holds nodeKeys of unprocessed subtree roots. Generic for any
   * nested record shape; grown once when deep records are seen, never per row.
   */
  private long[] workList = new long[64];
  private int workListSize;

  public ProjectionIndexRowExtractor(final IndexDef indexDef, final PathSummaryReader pathSummary) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionIndexRowExtractor requires an IndexType.PROJECTION IndexDef; got " + indexDef.getType());
    }
    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final List<Type> fieldTypes = indexDef.getProjectionFieldTypes();
    this.fieldPaths = fieldPaths;
    this.fieldTypes = fieldTypes.toArray(Type[]::new);
    this.rootPath = indexDef.getProjectionRootPath();
    this.columnKinds = new byte[fieldPaths.size()];
    this.numericColumnSawNonIntegral = new boolean[fieldPaths.size()];
    for (int i = 0; i < fieldPaths.size(); i++) {
      columnKinds[i] = ProjectionIndexBuilder.mapTypeToColumnKind(fieldTypes.get(i), fieldPaths.get(i));
    }
    resolveFieldPcrs(pathSummary);
    this.rowLongs = new long[fieldPaths.size()];
    this.rowBools = new boolean[fieldPaths.size()];
    this.rowStringUtf8 = new byte[fieldPaths.size()][];
    this.rowStringUtf8Lengths = new int[fieldPaths.size()];
    this.rowStringUtf8Scratch = new byte[fieldPaths.size()][];
    this.rowStringSets = new String[fieldPaths.size()][];
    this.rowStringSetLen = new int[fieldPaths.size()];
    this.rowPresent = new boolean[fieldPaths.size()];
    this.rowUnrepresentable = new boolean[fieldPaths.size()];
    this.rowNonIntegral = new boolean[fieldPaths.size()];
    this.rowNonDoubleSource = new boolean[fieldPaths.size()];
  }

  /**
   * Re-resolve the declared field paths against {@code pathSummary}.
   *
   * <p>
   * Only the INCREMENTAL build needs this: it extracts rows while the resource — and therefore the
   * path summary — is still growing, so an extractor built at one auto-commit epoch does not yet know
   * the path classes of fields whose first occurrence comes later. Without the refresh those fields
   * extract as ABSENT on every row built before their path node existed, which is indistinguishable
   * downstream from a record that genuinely lacks them.
   *
   * <p>
   * Cheap enough to run once per extraction batch (one path-summary lookup per declared field) and
   * deliberately in place: {@link #columnKindsRef()} hands the kinds array out BY REFERENCE to every
   * leaf the build has produced, so replacing the extractor mid-build would detach them from the
   * global-dictionary decision that flips entries in it.
   */
  public void refresh(final PathSummaryReader pathSummary) {
    resolveFieldPcrs(pathSummary);
  }

  private void resolveFieldPcrs(final PathSummaryReader pathSummary) {
    this.rootPcrKeys = pathSummary.getPCRsForPath(rootPath).toLongArray();
    final LongArrayList pcrKeys = new LongArrayList();
    final IntArrayList pcrCols = new IntArrayList();
    for (int i = 0; i < fieldPaths.size(); i++) {
      // Primitive iteration — getPCRsForPath returns a fastutil LongSet;
      // the LongIterator avoids boxing a Long per PCR.
      final LongSet fieldPcrs = pathSummary.getPCRsForPath(fieldPaths.get(i));
      for (final LongIterator it = fieldPcrs.iterator(); it.hasNext();) {
        pcrKeys.add(it.nextLong());
        pcrCols.add(i);
      }
    }
    this.fieldPcrKeys = pcrKeys.toLongArray();
    this.fieldPcrColumns = pcrCols.toIntArray();
  }

  /** Per-column kinds, index-aligned with the projection's declared fields. */
  public byte[] columnKinds() {
    return columnKinds.clone();
  }

  /**
   * The current (pathNodeKey → column) mapping arrays, for a chunk batch's dispatch snapshot.
   * {@link #resolveFieldPcrs} REPLACES these arrays rather than mutating them, so the returned
   * references stay valid snapshots across later refreshes.
   */
  long[] fieldPcrKeysRef() {
    return fieldPcrKeys;
  }

  int[] fieldPcrColumnsRef() {
    return fieldPcrColumns;
  }

  /**
   * Fill the row buffers from one worker-extracted batch row, in place of {@link #extractInto}'s
   * navigation. The batch classified every cell with this extractor's own helpers, so the buffers end
   * in the state {@link #extractAt} would have produced for the same record; {@link #appendTo} then
   * packs them through the identical leaf path.
   */
  void loadRowFromBatch(final ProjectionChunkRowBatch batch, final int row) {
    resetRow(null);
    for (int column = 0; column < columnKinds.length; column++) {
      if (!batch.flagPresent(column, row)) {
        continue;
      }
      rowPresent[column] = true;
      final boolean cellUnrepresentable = batch.flagUnrepresentable(column, row);
      if (cellUnrepresentable) {
        rowUnrepresentable[column] = true;
      }
      if (batch.flagNonIntegral(column, row)) {
        rowNonIntegral[column] = true;
        numericColumnSawNonIntegral[column] = true;
      }
      if (batch.flagNonDoubleSource(column, row)) {
        rowNonDoubleSource[column] = true;
      }
      switch (columnKinds[column]) {
        // The temporal kinds join the long lane here, not the string one: the batch already parsed
        // the document's canonical text into an epoch when the worker fed the cell, so nothing is
        // re-parsed per row and nothing string-shaped reaches the leaf.
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP,
            ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
          rowLongs[column] = batch.longValue(column, row);
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> rowBools[column] = batch.booleanValue(column, row);
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL -> {
          final int length = batch.stringLength(column, row);
          if (length >= 0 && !cellUnrepresentable) {
            byte[] scratch = rowStringUtf8Scratch[column];
            if (scratch == null || scratch.length < length) {
              scratch = new byte[grownStringCapacity(scratch == null
                  ? 0
                  : scratch.length, Math.max(1, length))];
              rowStringUtf8Scratch[column] = scratch;
            }
            batch.copyStringTo(column, row, scratch);
            rowStringUtf8[column] = scratch;
            rowStringUtf8Lengths[column] = length;
          }
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
          if (!cellUnrepresentable) {
            final String[] elements = batch.setElementsAt(column, row);
            rowStringSets[column] = elements;
            rowStringSetLen[column] = elements.length;
          }
        }
        default -> throw new IllegalStateException(
            "unknown projection column kind " + columnKinds[column] + " for column " + column);
      }
    }
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
   * Navigate to {@code recordKey} and fill the row buffers from its current state. The cursor is left
   * positioned at {@code recordKey}.
   *
   * @return {@code false} when the record no longer exists (deleted in the running transaction) — the
   *         caller drops the row
   */
  public boolean extractInto(final JsonNodeReadOnlyTrx rtx, final long recordKey) {
    return extractInto(rtx, recordKey, null);
  }

  boolean extractInto(final JsonNodeReadOnlyTrx rtx, final long recordKey, final long[] selectedColumns) {
    if (!rtx.moveTo(recordKey)) {
      return false;
    }
    extractAt(rtx, recordKey, selectedColumns);
    return true;
  }

  public boolean extractInto(final XmlNodeReadOnlyTrx rtx, final long recordKey) {
    return extractInto(rtx, recordKey, null);
  }

  boolean extractInto(final XmlNodeReadOnlyTrx rtx, final long recordKey, final long[] selectedColumns) {
    if (!rtx.moveTo(recordKey)) {
      return false;
    }
    if (rtx.getKind() != NodeKind.ELEMENT) {
      throw new IllegalStateException(
          "XML projection record roots must be elements; node " + recordKey + " has kind " + rtx.getKind());
    }
    if (!isXmlRecordRoot(rtx.getPathNodeKey())) {
      // Attribution said this key roots a record; the path class disagreeing is an invariant
      // break, not a vanished record. It must be loud: unlike the JSON load path, XML record
      // sets have no end-of-load row-count assertion, so a silent false here would persist a
      // short index with no witness.
      throw new IllegalStateException("XML projection record " + recordKey + " at path class " + rtx.getPathNodeKey()
          + " is not at a record-set root path — attribution and extraction disagree");
    }
    extractAt(rtx, recordKey, selectedColumns);
    return true;
  }

  /**
   * Read an array node's STRING elements into the row's set buffer for {@code col}.
   *
   * <p>
   * Only a set of STRINGS is representable. A non-string element makes the whole cell unrepresentable
   * rather than silently dropping it: a membership predicate that saw a partial set would answer "no"
   * for a record whose array does hold the value, which is a wrong answer, not a slower one.
   *
   * @return {@code false} if any element is not a string
   */
  private boolean collectStringSet(final JsonNodeReadOnlyTrx rtx, final long arrayKey, final int col) {
    int n = rowStringSetLen[col];
    boolean allStrings = true;
    if (rtx.hasFirstChild()) {
      rtx.moveToFirstChild();
      do {
        if (rtx.getKind() == NodeKind.STRING_VALUE) {
          if (n == MAX_STRING_SET_ELEMENTS_PER_ROW) {
            allStrings = false;
            continue;
          }
          String[] buf = rowStringSets[col];
          if (buf == null) {
            buf = new String[Math.min(8, MAX_STRING_SET_ELEMENTS_PER_ROW)];
            rowStringSets[col] = buf;
          } else if (n == buf.length) {
            final int newLength = Math.min(MAX_STRING_SET_ELEMENTS_PER_ROW, Math.multiplyExact(n, 2));
            final String[] grown = new String[newLength];
            System.arraycopy(buf, 0, grown, 0, n);
            rowStringSets[col] = grown;
            buf = grown;
          }
          buf[n++] = rtx.getValue();
        } else {
          allStrings = false;
        }
      } while (rtx.moveToRightSibling());
    }
    rowStringSetLen[col] = n;
    rtx.moveTo(arrayKey);
    return allStrings;
  }

  /**
   * The row's set elements per column, trimmed to their live length, or {@code null} when no set
   * column is declared. Allocates only when a set column exists, and only the outer array.
   */
  private String[][] stringSetsForAppend() {
    String[][] out = null;
    for (int c = 0; c < columnKinds.length; c++) {
      if (columnKinds[c] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
        continue;
      }
      if (out == null) {
        out = new String[columnKinds.length][];
      }
      if (rowUnrepresentable[c]) {
        out[c] = EMPTY_SET;
        continue;
      }
      final int n = rowStringSetLen[c];
      if (n == 0) {
        out[c] = EMPTY_SET;
      } else {
        final String[] trimmed = new String[n];
        System.arraycopy(rowStringSets[c], 0, trimmed, 0, n);
        out[c] = trimmed;
      }
    }
    return out;
  }

  private static final byte[] EMPTY_UTF8 = new byte[0];
  private static final String[] EMPTY_SET = new String[0];

  /**
   * Append the buffers filled by the last {@link #extractInto}/{@link #extractAt} call as one row of
   * {@code leaf}.
   *
   * @return {@code false} when {@code leaf} is at capacity (caller opens a fresh leaf and retries)
   */
  boolean appendTo(final ProjectionIndexRowGroupPage leaf, final long recordKey, final boolean orderException,
      final byte[] orderLabel) {
    return leaf.appendExtractedUtf8Row(recordKey, rowLongs, rowBools, rowStringUtf8, rowStringUtf8Lengths,
        stringSetsForAppend(), rowPresent, rowUnrepresentable, rowNonIntegral, rowNonDoubleSource, orderException,
        orderLabel);
  }

  /**
   * Append one selected source column to a one-column maintenance page without manufacturing
   * projection-width value arrays or a trimmed set array. The row buffers are borrowed only for the
   * synchronous append; the page copies/interns every value it retains.
   */
  boolean appendColumnTo(final ProjectionIndexRowGroupPage leaf, final long recordKey, final int sourceColumn) {
    if (sourceColumn < 0 || sourceColumn >= columnKinds.length) {
      throw new IndexOutOfBoundsException("projection source column out of range: " + sourceColumn);
    }
    final byte sourceKind = columnKinds[sourceColumn];
    final byte maintenanceKind = leaf.columnKind(0);
    // A resource-wide dictionary is a persisted encoding choice made after extraction has already
    // classified the logical string column as STRING_DICT. Ordinary maintenance must keep feeding
    // the extractor's UTF-8 cell into that elected STRING_GLOBAL page; requiring byte-identical
    // kinds here rejects every value-only update to such a column.
    final boolean compatibleGlobalString = sourceKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
        && maintenanceKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
    if (leaf.getColumnCount() != 1 || (maintenanceKind != sourceKind && !compatibleGlobalString)) {
      throw new IllegalArgumentException("maintenance page kind does not match source column " + sourceColumn);
    }
    return leaf.appendExtractedSingleColumnRow(recordKey, rowLongs[sourceColumn], rowBools[sourceColumn],
        rowStringUtf8[sourceColumn], rowStringUtf8Lengths[sourceColumn], rowStringSets[sourceColumn],
        rowStringSetLen[sourceColumn], rowPresent[sourceColumn], rowUnrepresentable[sourceColumn],
        rowNonIntegral[sourceColumn], rowNonDoubleSource[sourceColumn]);
  }

  /**
   * Fill the row buffers for the record at {@code recordKey}; the cursor is assumed to be able to
   * reach it (bulk-build path positions it during traversal). Ends with the cursor back at
   * {@code recordKey}.
   */
  /**
   * Clear every per-row slot. A field this row fails to resolve stays "missing" — presence bit clear
   * — and serialises as the column's default on the leaf page.
   */
  private void resetRow(final long[] selectedColumns) {
    for (int i = 0; i < columnKinds.length; i++) {
      if (!isSelected(i, selectedColumns)) {
        continue;
      }
      rowLongs[i] = 0L;
      rowBools[i] = false;
      rowStringUtf8[i] = null;
      rowStringUtf8Lengths[i] = 0;
      rowPresent[i] = false;
      rowUnrepresentable[i] = false;
      rowNonIntegral[i] = false;
      rowNonDoubleSource[i] = false;
      rowStringSetLen[i] = 0;
    }
  }

  void extractAt(final JsonNodeReadOnlyTrx rtx, final long recordKey) {
    extractAt(rtx, recordKey, null);
  }

  private void extractAt(final JsonNodeReadOnlyTrx rtx, final long recordKey, final long[] selectedColumns) {
    resetRow(selectedColumns);
    // Generic DFS: walk every descendant of recordKey via an explicit
    // work-list of unvisited first-children. For each node we visit:
    // - a fused OBJECT_NAMED_* record matching a declared field reads its
    // inline value straight into the row
    // - structured kinds (OBJECT / ARRAY / fused containers) descend so
    // declared NESTED fields below them are found.
    workListSize = 0;
    pushFirstChild(rtx, recordKey);
    while (workListSize > 0) {
      final long top = workList[--workListSize];
      rtx.moveTo(top);
      // Walk right-sibling chain at this level inline.
      long cur = top;
      do {
        final NodeKind kind = rtx.getKind();
        if (isFusedScalarKind(kind)) {
          // Fused OBJECT_NAMED_* record — value lives inline on this node. Zero-alloc
          // direct extraction, no synthetic-child navigation. Fused nodes have no children,
          // so there is nothing to descend into.
          final long pathNodeKey = rtx.getPathNodeKey();
          for (int mapping = nextFieldMapping(pathNodeKey, selectedColumns, 0); mapping >= 0; mapping =
              nextFieldMapping(pathNodeKey, selectedColumns, mapping + 1)) {
            final int col = fieldPcrColumns[mapping];
            readFusedValueIntoRow(rtx, kind, col);
          }
        } else if (kind == NodeKind.OBJECT_NAMED_OBJECT || kind == NodeKind.OBJECT_NAMED_ARRAY) {
          final long pk = rtx.getPathNodeKey();
          for (int mapping = nextFieldMapping(pk, selectedColumns, 0); mapping >= 0; mapping =
              nextFieldMapping(pk, selectedColumns, mapping + 1)) {
            final int col = fieldPcrColumns[mapping];
            if (kind == NodeKind.OBJECT_NAMED_ARRAY
                && columnKinds[col] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
              // An array-valued field declared as a SET column: its string elements ARE the value.
              // Every other declared column kind is scalar, which is why this used to be recorded
              // as present-but-unrepresentable and the index could say nothing about it.
              rowPresent[col] = true;
              if (!collectStringSet(rtx, cur, col)) {
                rowUnrepresentable[col] = true; // a non-string element: not a set of strings
              }
            } else {
              // Object/array-valued field declared as a primitive column: present
              // but UNREPRESENTABLE.
              rowPresent[col] = true;
              rowUnrepresentable[col] = true;
            }
          }
          // Descend regardless — declared NESTED fields live below this node.
          pushFirstChild(rtx, cur);
        } else if (kind == NodeKind.OBJECT || kind == NodeKind.ARRAY) {
          // Structured — descend.
          pushFirstChild(rtx, cur);
        }
        // Primitives have no children; skip.
        if (!rtx.moveToRightSibling())
          break;
        cur = rtx.getNodeKey();
      } while (true);
    }
    rtx.moveTo(recordKey);
  }

  void extractAt(final XmlNodeReadOnlyTrx rtx, final long recordKey) {
    extractAt(rtx, recordKey, null);
  }

  private void extractAt(final XmlNodeReadOnlyTrx rtx, final long recordKey, final long[] selectedColumns) {
    resetRow(selectedColumns);
    workListSize = 0;
    if (!rtx.moveTo(recordKey) || rtx.getKind() != NodeKind.ELEMENT) {
      throw new IllegalStateException("XML projection record root is not an element: " + recordKey);
    }
    extractXmlAttributes(rtx, recordKey, selectedColumns);
    pushFirstChild(rtx, recordKey);
    while (workListSize > 0) {
      final long top = workList[--workListSize];
      rtx.moveTo(top);
      long current = top;
      do {
        final NodeKind kind = rtx.getKind();
        if (kind == NodeKind.ELEMENT) {
          extractXmlAttributes(rtx, current, selectedColumns);
          final long pathNodeKey = rtx.getPathNodeKey();
          for (int mapping = nextFieldMapping(pathNodeKey, selectedColumns, 0); mapping >= 0; mapping =
              nextFieldMapping(pathNodeKey, selectedColumns, mapping + 1)) {
            final int column = fieldPcrColumns[mapping];
            readXmlElementIntoRow(rtx, current, column);
          }
          pushFirstChild(rtx, current);
        }
        if (!rtx.moveToRightSibling()) {
          break;
        }
        current = rtx.getNodeKey();
      } while (true);
    }
    rtx.moveTo(recordKey);
  }

  private void extractXmlAttributes(final XmlNodeReadOnlyTrx rtx, final long elementKey, final long[] selectedColumns) {
    final int attributeCount = rtx.getAttributeCount();
    for (int index = 0; index < attributeCount; index++) {
      if (!rtx.moveToAttribute(index)) {
        throw new IllegalStateException("XML attribute " + index + " disappeared during projection extraction");
      }
      final long pathNodeKey = rtx.getPathNodeKey();
      for (int mapping = nextFieldMapping(pathNodeKey, selectedColumns, 0); mapping >= 0; mapping =
          nextFieldMapping(pathNodeKey, selectedColumns, mapping + 1)) {
        final int column = fieldPcrColumns[mapping];
        readXmlScalarIntoRow(rtx.getValue(), column);
      }
      rtx.moveTo(elementKey);
    }
  }

  private void readXmlElementIntoRow(final XmlNodeReadOnlyTrx rtx, final long elementKey, final int column) {
    final boolean setColumn = columnKinds[column] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
    if (rowPresent[column] && !setColumn) {
      // XML permits repeated element names and wildcard paths can intentionally match more than
      // one element. A scalar projection has no sequence semantics, so retaining either the first
      // or last value would be a wrong answer. Poison the cell and force value consumers to the
      // generic XML pipeline.
      rowUnrepresentable[column] = true;
      return;
    }
    if (!rtx.moveToFirstChild()) {
      readXmlScalarIntoRow("", column);
      rtx.moveTo(elementKey);
      return;
    }
    if (rtx.getKind() != NodeKind.TEXT || rtx.hasRightSibling()) {
      rowPresent[column] = true;
      rowUnrepresentable[column] = true;
      rtx.moveTo(elementKey);
      return;
    }
    readXmlScalarIntoRow(rtx.getValue(), column);
    rtx.moveTo(elementKey);
  }

  private void readXmlScalarIntoRow(final String value, final int column) {
    final byte columnKind = columnKinds[column];
    if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
      rowPresent[column] = true;
      if (value == null) {
        rowUnrepresentable[column] = true;
      } else {
        appendXmlSetValue(column, value);
      }
      return;
    }
    if (rowPresent[column]) {
      rowUnrepresentable[column] = true;
      return;
    }
    rowPresent[column] = true;
    if (value == null) {
      rowUnrepresentable[column] = true;
      return;
    }
    try {
      if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
        if ("true".equals(value) || "1".equals(value)) {
          rowBools[column] = true;
        } else if ("false".equals(value) || "0".equals(value)) {
          rowBools[column] = false;
        } else {
          rowUnrepresentable[column] = true;
        }
      } else if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
        rowLongs[column] = new BigDecimal(value.trim()).longValueExact();
      } else if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE) {
        final String lexical = value.trim();
        final Type sourceType = fieldTypes[column];
        final double number;
        if (sourceType == Type.DBL) {
          number = Double.parseDouble(lexical);
        } else if (sourceType == Type.FLO) {
          number = Float.parseFloat(lexical);
          rowNonDoubleSource[column] = true;
        } else if (sourceType == Type.DEC) {
          final BigDecimal decimal = new BigDecimal(lexical);
          number = decimal.doubleValue();
          rowNonDoubleSource[column] = true;
          if (!Double.isFinite(number) || isLossyDoubleConversion(decimal, number)) {
            rowUnrepresentable[column] = true;
          }
        } else {
          rowUnrepresentable[column] = true;
          return;
        }
        if (!Double.isFinite(number)) {
          rowUnrepresentable[column] = true;
        } else {
          rowLongs[column] = ProjectionDoubleEncoding.encode(number);
        }
      } else if (ProjectionIndexRowGroupPage.isTemporalKind(columnKind)) {
        final long epoch = ProjectionTemporalCodec.parse(columnKind, value);
        if (epoch == ProjectionTemporalCodec.NOT_CANONICAL) {
          throw ProjectionTemporalCodec.notCanonical(columnKind, column, value);
        }
        rowLongs[column] = epoch;
      } else {
        final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        rowStringUtf8[column] = utf8;
        rowStringUtf8Lengths[column] = utf8.length;
      }
    } catch (final ArithmeticException | NumberFormatException malformed) {
      rowUnrepresentable[column] = true;
    }
  }

  private void appendXmlSetValue(final int column, final String value) {
    int length = rowStringSetLen[column];
    String[] values = rowStringSets[column];
    if (values == null) {
      values = new String[8];
      rowStringSets[column] = values;
    } else if (length == values.length) {
      values = Arrays.copyOf(values, length << 1);
      rowStringSets[column] = values;
    }
    values[length] = value;
    rowStringSetLen[column] = length + 1;
  }

  /** The fused kinds whose value sits inline on the record itself, rather than on a child node. */
  private static boolean isFusedScalarKind(final NodeKind kind) {
    return kind == NodeKind.OBJECT_NAMED_BOOLEAN || kind == NodeKind.OBJECT_NAMED_NUMBER
        || kind == NodeKind.OBJECT_NAMED_STRING || kind == NodeKind.OBJECT_NAMED_NULL;
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

  private void pushFirstChild(final XmlNodeReadOnlyTrx rtx, final long parentKey) {
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

  private boolean isXmlRecordRoot(final long pathNodeKey) {
    for (final long rootPcrKey : rootPcrKeys) {
      if (rootPcrKey == pathNodeKey) {
        return true;
      }
    }
    return false;
  }

  private static boolean isSelected(final int column, final long[] selectedColumns) {
    if (selectedColumns == null) {
      return true;
    }
    final int word = column >>> 6;
    return word < selectedColumns.length && (selectedColumns[word] & (1L << (column & 63))) != 0;
  }

  private int nextFieldMapping(final long pathNodeKey, final long[] selectedColumns, final int start) {
    for (int index = Math.max(0, start); index < fieldPcrKeys.length; index++) {
      final int column = fieldPcrColumns[index];
      if (fieldPcrKeys[index] == pathNodeKey && isSelected(column, selectedColumns)) {
        return index;
      }
    }
    return -1;
  }

  long rowLong(final int column) {
    return rowLongs[column];
  }

  boolean rowBoolean(final int column) {
    return rowBools[column];
  }

  byte[] rowStringUtf8(final int column) {
    return rowStringUtf8[column];
  }

  int rowStringUtf8Length(final int column) {
    return rowStringUtf8Lengths[column];
  }

  String[] rowStringSet(final int column) {
    return rowStringSets[column];
  }

  int rowStringSetLength(final int column) {
    return rowStringSetLen[column];
  }

  boolean rowPresent(final int column) {
    return rowPresent[column];
  }

  boolean rowUnrepresentable(final int column) {
    return rowUnrepresentable[column];
  }

  boolean rowNonIntegral(final int column) {
    return rowNonIntegral[column];
  }

  boolean rowNonDoubleSource(final int column) {
    return rowNonDoubleSource[column];
  }

  /**
   * {@code true} when converting {@code n} to {@code d = n.doubleValue()} lost information — the
   * NUMERIC_DOUBLE value-exactness probe. Double/Float/Integer convert exactly (float and int widen
   * losslessly); Long round-trips iff |value| ≤ 2^53-ish (checked by round-trip); Big* fall back to
   * an exact BigDecimal compare (allocates, but only on the rare Big* path).
   */
  static boolean isLossyDoubleConversion(final Number n, final double d) {
    return switch (n) {
      case Double ignored -> false;
      case Float ignored -> false;
      case Integer ignored -> false;
      // Long round-trip check with the saturation edge: Long.MAX_VALUE's doubleValue rounds UP
      // to 2^63 and the narrowing cast saturates BACK to MAX_VALUE, so the round trip alone
      // would falsely certify it exact (the stored double is off by one).
      case Long l -> l == Long.MAX_VALUE || (long) d != l;
      case BigInteger bi -> new BigDecimal(d).compareTo(new BigDecimal(bi)) != 0;
      case BigDecimal bd -> new BigDecimal(d).compareTo(bd) != 0;
      default -> true; // unknown Number subtype — assume lossy, fail closed
    };
  }

  static boolean isNonIntegral(final Number n) {
    if (n instanceof Double || n instanceof Float) {
      final double d = n.doubleValue();
      return d != Math.rint(d) || Math.abs(d) > (double) Long.MAX_VALUE;
    }
    if (n instanceof BigDecimal bd) {
      return bd.stripTrailingZeros().scale() > 0;
    }
    return false;
  }

  private static final BigDecimal LONG_MIN_DEC = BigDecimal.valueOf(Long.MIN_VALUE);
  private static final BigDecimal LONG_MAX_DEC = BigDecimal.valueOf(Long.MAX_VALUE);

  /**
   * {@code true} when storing {@code n} as a long does NOT reproduce the interpreter-visible
   * value/type exactly — the NUMERIC_LONG value-exactness probe (the twin of
   * {@link #isLossyDoubleConversion}):
   * <ul>
   * <li>out-of-long-range integers ({@code BigInteger}, big integral {@code BigDecimal}) WRAP through
   * {@code longValue()} — silently wrong values;</li>
   * <li>{@code Double}/{@code Float} sources type the interpreter's arithmetic in double/float space
   * even when the VALUE is integral ({@code Dbl} serialization switches to scientific notation at
   * 1e6+, and the fold's result type differs under composition).</li>
   * </ul>
   * Flagged cells raise the value-exactness bit so value-exact consumers decline to the typed re-walk
   * / generic pipeline; counts stay servable.
   */
  static boolean isLossyLongConversion(final Number n) {
    return switch (n) {
      case Long ignored -> false;
      case Integer ignored -> false;
      case Short ignored -> false;
      case Byte ignored -> false;
      case Double ignored -> true;
      case Float ignored -> true;
      case BigInteger bi -> bi.bitLength() > 63;
      case BigDecimal bd -> bd.compareTo(LONG_MIN_DEC) < 0 || bd.compareTo(LONG_MAX_DEC) > 0;
      default -> true; // unknown Number subtype — assume lossy, fail closed
    };
  }

  /**
   * Reads an integral fused-number payload through the internal primitive cursor capability. Returns
   * {@code false} for non-integral wire kinds and custom cursors, which preserves the ordinary
   * {@link Number} fallback below.
   */
  private boolean readPrimitiveNumberIntoRow(final JsonNodeReadOnlyTrx rtx, final byte columnKind, final int col) {
    if (!(rtx instanceof PrimitiveNumberCursor primitiveNumberCursor)) {
      return false;
    }

    final byte primitiveType = primitiveNumberCursor.readFusedPrimitiveNumber(rowLongs, col);
    if (primitiveType != PrimitiveNumberValue.INT && primitiveType != PrimitiveNumberValue.LONG) {
      return false;
    }
    if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
      // Integer and Long both map exactly to the long column. readFusedPrimitiveNumber already
      // sign-extended an int into the caller-owned long slot.
      return true;
    }

    final long integralValue = rowLongs[col];
    final double doubleValue = primitiveType == PrimitiveNumberValue.INT
        ? (double) (int) integralValue
        : (double) integralValue;
    if (primitiveType == PrimitiveNumberValue.LONG
        && (integralValue == Long.MAX_VALUE || (long) doubleValue != integralValue)) {
      // Mirrors isLossyDoubleConversion(Long, double), including the saturation edge where
      // Long.MAX_VALUE rounds up to 2^63 and narrows back to MAX_VALUE.
      numericColumnSawNonIntegral[col] = true;
      rowNonIntegral[col] = true;
    }
    // Neither integral source is a Double, even when its conversion is value-exact. The
    // interpreter fallback therefore has different result typing and the pure-double assertion
    // must be cleared exactly as on the boxed path.
    rowNonDoubleSource[col] = true;
    rowLongs[col] = ProjectionDoubleEncoding.encode(doubleValue);
    return true;
  }

  /**
   * Copy a fused string through the allocation-stable internal cursor lane.
   *
   * @return {@code true} when the cursor supplied the value; {@code false} requests the unchanged
   *         public-byte fallback
   */
  private boolean readFusedStringIntoRow(final JsonNodeReadOnlyTrx rtx, final int col) {
    if (!(rtx instanceof FusedStringCursor fusedStringCursor)) {
      return false;
    }

    byte[] scratch = rowStringUtf8Scratch[col];
    if (scratch == null) {
      scratch = EMPTY_UTF8;
    }
    int result = fusedStringCursor.readFusedStringUtf8(scratch);
    while (result < FusedStringCursor.UNAVAILABLE) {
      final int required = FusedStringCursor.requiredCapacity(result);
      if (required <= scratch.length) {
        throw new IllegalStateException("fused string cursor requested capacity " + required + " after receiving a "
            + scratch.length + "-byte destination");
      }
      scratch = Arrays.copyOf(scratch, grownStringCapacity(scratch.length, required));
      rowStringUtf8Scratch[col] = scratch;
      result = fusedStringCursor.readFusedStringUtf8(scratch);
    }
    if (result == FusedStringCursor.UNAVAILABLE) {
      return false;
    }
    if (result > scratch.length) {
      throw new IllegalStateException(
          "fused string cursor reported " + result + " bytes from a " + scratch.length + "-byte destination");
    }
    rowStringUtf8[col] = scratch;
    rowStringUtf8Lengths[col] = result;
    return true;
  }

  private static int grownStringCapacity(final int currentCapacity, final int requiredCapacity) {
    int grown = Math.max(64, currentCapacity);
    while (grown < requiredCapacity) {
      final int doubled = grown << 1;
      if (doubled <= grown) {
        return requiredCapacity;
      }
      grown = doubled;
    }
    return grown;
  }

  /**
   * Convert the UTF-8 already buffered for {@code col} into the column's epoch lane.
   *
   * <p>
   * A value that is not exactly the declared shape FAILS THE BUILD rather than degrading the cell.
   * That is the whole contract of a declared temporal column: the numeric lane is lossless only
   * because every stored value formats back to the bytes the document held, so a value that cannot
   * make the round trip must never be stored — and a silently unrepresentable cell would make the
   * column answer nothing while looking healthy.
   */
  private void readTemporalIntoRow(final byte columnKind, final int col) {
    final byte[] utf8 = rowStringUtf8[col];
    final int length = rowStringUtf8Lengths[col];
    final long epoch = ProjectionTemporalCodec.parse(columnKind, utf8, 0, length);
    if (epoch == ProjectionTemporalCodec.NOT_CANONICAL) {
      throw ProjectionTemporalCodec.notCanonical(columnKind, col, utf8, 0, length);
    }
    rowLongs[col] = epoch;
    // The cell lives in the long lane from here on; leaving the borrowed slice visible would let a
    // string-shaped consumer read a value this column does not store.
    rowStringUtf8[col] = null;
    rowStringUtf8Lengths[col] = 0;
  }

  /**
   * Read the primitive value off a fused {@code OBJECT_NAMED_*} record directly into the current row.
   * The rtx's value predicates already return true on a fused record; dispatch is by record kind:
   * fused-number → numeric column, fused-boolean → boolean column, fused-string → string column,
   * fused-null → present but unrepresentable (no column kind can hold it).
   */
  private void readFusedValueIntoRow(final JsonNodeReadOnlyTrx rtx, final NodeKind fusedKind, final int col) {
    if (rowPresent[col]) {
      // A descendant/wildcard field path can resolve more than one scalar below one record. Scalar
      // projection columns have no sequence/last-wins semantics, so retaining either match would let
      // an indexed predicate return a wrong answer. Mirror XML's repeated-scalar discipline and
      // poison the cell so value consumers fall back to the generic pipeline.
      rowUnrepresentable[col] = true;
      return;
    }
    rowPresent[col] = true;
    final byte columnKind = columnKinds[col];
    switch (fusedKind) {
      case OBJECT_NAMED_NUMBER -> {
        if (!ProjectionIndexRowGroupPage.isNumericKind(columnKind)) {
          // Kind mismatch (number where the column expects bool/string) or a
          // null Number — present but unrepresentable.
          rowUnrepresentable[col] = true;
        } else if (!readPrimitiveNumberIntoRow(rtx, columnKind, col)) {
          final Number n = rtx.getNumberValue();
          if (n == null) {
            rowUnrepresentable[col] = true;
          } else if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
            if (isNonIntegral(n) || isLossyLongConversion(n)) {
              numericColumnSawNonIntegral[col] = true;
              rowNonIntegral[col] = true;
            }
            rowLongs[col] = n.longValue();
          } else {
            // NUMERIC_DOUBLE: store the order-preserving transform of the exact double value.
            // Non-finite values cannot arise from JSON but are defensively unrepresentable (no
            // stored pattern may collide with the zone-map sentinels). Lossy Big*/long→double
            // conversions raise the value-exactness bit (COLUMN_FLAG_NON_INTEGRAL semantics for
            // this kind) so value-exact consumers decline — same fail-closed discipline as
            // integrality on long columns.
            final double d = n.doubleValue();
            if (!Double.isFinite(d)) {
              rowUnrepresentable[col] = true;
            } else {
              if (isLossyDoubleConversion(n, d)) {
                numericColumnSawNonIntegral[col] = true;
                rowNonIntegral[col] = true;
              }
              if (!(n instanceof Double)) {
                // Strict source typing, not exactness: an exact Integer→double cell clears
                // purity because the fallback would type the aggregate Dec, not Dbl — and
                // Float clears it too (the interpreter wraps Float as xs:float and
                // accumulates in FLOAT arithmetic, surfacing Flt; only Double sources make
                // the fallback provably compute-and-type in double space).
                rowNonDoubleSource[col] = true;
              }
              rowLongs[col] = ProjectionDoubleEncoding.encode(d);
            }
          }
        }
      }
      case OBJECT_NAMED_BOOLEAN -> {
        if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
          rowBools[col] = rtx.getBooleanValue();
        } else {
          rowUnrepresentable[col] = true;
        }
      }
      case OBJECT_NAMED_STRING -> {
        final boolean temporal = ProjectionIndexRowGroupPage.isTemporalKind(columnKind);
        if (temporal || columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
            || columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          // The production cursor copies/decodes into a grow-only per-column buffer. Custom cursors
          // retain the public-byte fallback; their returned array may be node-owned, so it is never
          // installed as reusable scratch.
          if (!readFusedStringIntoRow(rtx, col)) {
            final byte[] value = rtx.getValueBytes();
            rowStringUtf8[col] = value;
            rowStringUtf8Lengths[col] = value == null
                ? 0
                : value.length;
          }
          if (temporal) {
            // A declared temporal column takes the STRING off the record and stores its EPOCH. The
            // bytes are read into the same reusable buffer a string column uses and converted in
            // place, so the parse costs no allocation and the leaf never sees a string lane.
            readTemporalIntoRow(columnKind, col);
          }
        } else {
          rowUnrepresentable[col] = true;
        }
      }
      // OBJECT_NAMED_NULL → present-but-null: no column kind can represent it.
      default -> rowUnrepresentable[col] = true;
    }
  }
}
