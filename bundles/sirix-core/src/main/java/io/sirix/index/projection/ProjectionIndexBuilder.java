/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Walks a JSON resource's current revision and materialises one row per
 * record (= node whose pathNodeKey matches the projection's root path)
 * into {@link ProjectionIndexLeafPage}s. Serialised leaf byte[]s are
 * delivered to the caller-supplied {@code leafSink} in append order so
 * the caller can stream them into the HOT backing tree without holding
 * more than one leaf in memory.
 *
 * <h2>Traversal shape</h2>
 * The builder is driven directly rather than via the node-visitor
 * pattern CAS/PATH/NAME indexes use — projection extraction needs to
 * look at each matching record's descendants (to fetch field values),
 * which is easier expressed as "hit a root-matching node, then
 * navigate" than as a visitor that sees individual leaf values.
 *
 * <h2>HFT-grade hot path</h2>
 * Per-record work allocates nothing — the builder owns reusable per-row
 * arrays ({@code long[]} / {@code boolean[]} / {@code String[]}) sized
 * to the declared field count, and populates them in place via rtx
 * navigation + primitive-typed getters before handing them to
 * {@link ProjectionIndexLeafPage#appendRow}. The only per-record heap
 * activity is the varint-decoded nodeKey load the rtx already pays for,
 * plus the FSST-decoded UTF-8 byte[] for string fields (deduplicated
 * inside the leaf page's local dictionary, so it only occurs once per
 * distinct string per leaf).
 */
public final class ProjectionIndexBuilder {

  private final IndexDef indexDef;
  private final PathSummaryReader pathSummary;
  private final Consumer<byte[]> leafSink;

  /** Resolved pathNodeKey of the projection root (e.g. {@code $doc[]}). */
  private final long rootPathNodeKey;

  /**
   * Resolved pathNodeKey per declared field, index-aligned with
   * {@code indexDef.getProjectionFields()}. {@code -1L} when the path is
   * unresolvable in the current PathSummary — such records contribute
   * only {@code presentKind == 0} to that column.
   */
  private final long[] fieldPathNodeKeys;

  /** Per-field column kind, index-aligned with projection fields. */
  private final byte[] columnKinds;

  /** Reusable per-row extraction buffers — one entry per field. Zero alloc in the hot loop. */
  private final long[] rowLongs;
  private final boolean[] rowBools;
  private final String[] rowStrings;

  private ProjectionIndexLeafPage currentLeaf;
  private long rowsEmitted;
  private long leavesEmitted;

  public ProjectionIndexBuilder(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final Consumer<byte[]> leafSink) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionIndexBuilder requires an IndexType.PROJECTION IndexDef; got " + indexDef.getType());
    }
    this.indexDef = indexDef;
    this.pathSummary = pathSummary;
    this.leafSink = leafSink;
    final Path<QNm> rootPath = indexDef.getProjectionRootPath();
    final Set<Path<QNm>> rootSet = new HashSet<>();
    rootSet.add(rootPath);
    final Set<Long> rootPcrs = pathSummary.getPCRsForPaths(rootSet);
    if (rootPcrs.isEmpty()) {
      throw new IllegalStateException(
          "Projection root path '" + rootPath + "' did not resolve to any pathNodeKey — "
              + "declare the index after the resource has records matching the root");
    }
    // For the initial cut we require the root path to resolve to exactly
    // one pathNodeKey. Multi-path roots (e.g. identical paths under sibling
    // arrays) land in a follow-up once the multi-pathNodeKey scan is wired.
    if (rootPcrs.size() > 1) {
      throw new IllegalStateException(
          "Projection root path '" + rootPath + "' resolves to " + rootPcrs.size()
              + " pathNodeKeys; only single-path roots are supported in this version");
    }
    this.rootPathNodeKey = rootPcrs.iterator().next();

    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final List<Type> fieldTypes = indexDef.getProjectionFieldTypes();
    this.fieldPathNodeKeys = new long[fieldPaths.size()];
    this.columnKinds = new byte[fieldPaths.size()];
    for (int i = 0; i < fieldPaths.size(); i++) {
      final Set<Path<QNm>> one = new HashSet<>();
      one.add(fieldPaths.get(i));
      final Set<Long> pcrs = pathSummary.getPCRsForPaths(one);
      fieldPathNodeKeys[i] = pcrs.isEmpty() ? -1L : pcrs.iterator().next();
      columnKinds[i] = mapTypeToColumnKind(fieldTypes.get(i));
    }
    this.rowLongs = new long[fieldPaths.size()];
    this.rowBools = new boolean[fieldPaths.size()];
    this.rowStrings = new String[fieldPaths.size()];
    this.currentLeaf = new ProjectionIndexLeafPage(columnKinds);
  }

  private static byte mapTypeToColumnKind(final Type type) {
    if (type == Type.BOOL) return ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN;
    if (type == Type.INR || type == Type.LON || type == Type.INT
        || type == Type.DEC || type == Type.DBL || type == Type.FLO) {
      return ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG;
    }
    return ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT;
  }

  /**
   * Walk the resource from the document root, materialising one projection
   * row per node whose pathNodeKey equals {@code rootPathNodeKey}. Flushes
   * any partially-filled trailing leaf on completion.
   */
  public void build(final JsonNodeReadOnlyTrx rtx) {
    final long restoreNodeKey = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final DescendantAxis axis = new DescendantAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        if (!isRecordRoot(rtx)) continue;
        final long matchKey = rtx.getNodeKey();
        // In Sirix's JSON path model, a trailing {@code /[]} path matches
        // the ARRAY container, not each of its elements. The projection's
        // records live one level below: each array child is a row.
        // OBJECT_KEY roots (e.g. projection over a single keyed subtree)
        // are treated as the row themselves.
        if (rtx.getKind() == NodeKind.ARRAY) {
          if (rtx.moveToFirstChild()) {
            do {
              final long elementKey = rtx.getNodeKey();
              extractRow(rtx, elementKey);
              rtx.moveTo(elementKey);
            } while (rtx.moveToRightSibling());
          }
        } else {
          extractRow(rtx, matchKey);
        }
        rtx.moveTo(matchKey);
      }
      flushCurrentLeaf();
    } finally {
      rtx.moveTo(restoreNodeKey);
    }
  }

  /** @return total rows appended across all emitted leaves. */
  public long rowsEmitted() {
    return rowsEmitted;
  }

  /** @return number of serialised leaves handed to {@code leafSink}. */
  public long leavesEmitted() {
    return leavesEmitted;
  }

  /**
   * True when the current rtx position is a record root under this
   * projection. Matches by pathNodeKey so the check is O(1) — no path
   * walk — and correctly handles both OBJECT- and ARRAY-rooted records
   * (any kind whose pathNodeKey matches the declared root counts).
   */
  private boolean isRecordRoot(final JsonNodeReadOnlyTrx rtx) {
    if (rtx.isDocumentRoot()) return false;
    return getPathNodeKeyAtCursor(rtx) == rootPathNodeKey;
  }

  private static long getPathNodeKeyAtCursor(final JsonNodeReadOnlyTrx rtx) {
    // Only structured-kind nodes carry a pathNodeKey; primitives (value
    // nodes) live under an OBJECT_KEY so they return their parent's key
    // via the rtx node API. We consult the current node's kind and
    // dispatch accordingly.
    final NodeKind kind = rtx.getKind();
    if (kind == NodeKind.OBJECT || kind == NodeKind.ARRAY || kind == NodeKind.OBJECT_KEY) {
      return rtx.getPathNodeKey();
    }
    return -1L;
  }

  private void extractRow(final JsonNodeReadOnlyTrx rtx, final long recordKey) {
    // Reset per-row slots — fields we fail to resolve become "missing"
    // and serialise as defaults on the leaf page (no-op for numeric/bool
    // zone-map maintenance, null dict-id for string).
    for (int i = 0; i < fieldPathNodeKeys.length; i++) {
      rowLongs[i] = 0L;
      rowBools[i] = false;
      rowStrings[i] = "";
    }
    // Walk the record's descendants once, dispatching each declared field's
    // primitive value into the right column slot. DescendantAxis reads rtx's
    // position between steps, so we cannot dive to the value child and
    // return — instead we track a pending-column state machine: on a matching
    // OBJECT_KEY, remember the target column; on the axis's next step the
    // cursor naturally lands on the value child, where we read.
    final DescendantAxis fieldAxis = new DescendantAxis(rtx);
    int pendingCol = -1;
    while (fieldAxis.hasNext()) {
      fieldAxis.nextLong();
      if (pendingCol >= 0) {
        // Prior step matched an OBJECT_KEY for a declared field; this step is
        // its (first) value child in document order.
        readValueIntoRow(rtx, pendingCol);
        pendingCol = -1;
        // fall through to also evaluate this node as a potential OBJECT_KEY
        // match — rare in JSON (a value is a leaf), but keeps the pass robust.
      }
      final long pk = getPathNodeKeyAtCursor(rtx);
      if (pk < 0) continue;
      final int col = findField(pk);
      if (col < 0) continue;
      pendingCol = col;
    }
    if (!currentLeaf.appendRow(recordKey, rowLongs, rowBools, rowStrings)) {
      flushCurrentLeaf();
      currentLeaf = new ProjectionIndexLeafPage(columnKinds);
      currentLeaf.appendRow(recordKey, rowLongs, rowBools, rowStrings);
    }
    rowsEmitted++;
  }

  private int findField(final long pathNodeKey) {
    // Linear scan is cheaper than a HashMap lookup at typical projection
    // width (~5 fields) — fits in a single cache line and JIT-inlines
    // cleanly.
    for (int i = 0; i < fieldPathNodeKeys.length; i++) {
      if (fieldPathNodeKeys[i] == pathNodeKey) return i;
    }
    return -1;
  }

  private void readValueIntoRow(final JsonNodeReadOnlyTrx rtx, final int col) {
    switch (columnKinds[col]) {
      case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG -> {
        if (rtx.isNumberValue()) {
          final Number n = rtx.getNumberValue();
          if (n != null) rowLongs[col] = n.longValue();
        }
      }
      case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> {
        if (rtx.isBooleanValue()) rowBools[col] = rtx.getBooleanValue();
      }
      case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> {
        if (rtx.isStringValue()) {
          final String v = rtx.getValue();
          rowStrings[col] = v == null ? "" : v;
        }
      }
      default -> { /* unknown — leave defaults */ }
    }
  }

  private void flushCurrentLeaf() {
    if (currentLeaf.getRowCount() == 0) return;
    leafSink.accept(currentLeaf.serialize());
    leavesEmitted++;
  }
}
