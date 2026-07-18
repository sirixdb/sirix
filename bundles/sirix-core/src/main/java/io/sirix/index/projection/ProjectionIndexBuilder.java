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

import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.HashSet;
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
 * Per-record extraction is delegated to a {@link ProjectionIndexRowExtractor}
 * — the single source of truth shared with the incremental maintenance path
 * ({@link ProjectionIndexChangeListener}) — which owns reusable per-row
 * primitive buffers and allocates nothing per record. The only per-record
 * heap activity is the varint-decoded nodeKey load the rtx already pays for,
 * plus the FSST-decoded UTF-8 byte[] for string fields (deduplicated
 * inside the leaf page's local dictionary, so it only occurs once per
 * distinct string per leaf).
 */
public final class ProjectionIndexBuilder {

  private final Consumer<byte[]> leafSink;

  /** Shared per-record extraction engine (also used by incremental maintenance). */
  private final ProjectionIndexRowExtractor extractor;

  /**
   * Resolved pathNodeKeys of the projection root (e.g. {@code $doc[]}).
   * Multi-PCR roots — the same path shape under sibling subtrees — are
   * supported: every node whose pathNodeKey is in this set is a record
   * (set) root.
   */
  private final LongSet rootPathNodeKeys;

  /** Strict ancestor pathNodeKeys of every root PCR — guides pruned descent. */
  private final LongSet rootAncestorPathNodeKeys;

  private ProjectionIndexLeafPage currentLeaf;
  private long rowsEmitted;
  private long leavesEmitted;

  public ProjectionIndexBuilder(final IndexDef indexDef, final PathSummaryReader pathSummary,
      final Consumer<byte[]> leafSink) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionIndexBuilder requires an IndexType.PROJECTION IndexDef; got " + indexDef.getType());
    }
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
    // Multi-PCR roots (identical path shapes under sibling subtrees) are
    // supported: every PCR is a record(-set) root and the pruned descent
    // follows the union of their ancestor chains.
    this.rootPathNodeKeys = new LongOpenHashSet(rootPcrs.size());
    for (final Long pcr : rootPcrs) {
      rootPathNodeKeys.add(pcr.longValue());
    }
    // Fail fast on NESTED root PCRs (one record set living inside another
    // matched record's subtree, e.g. //records/[] over self-nested
    // "records" arrays): the pruned descent stops at the outer match, and
    // the per-record field DFS would let the inner record's fields
    // overwrite the outer row's columns — silently wrong results. Sibling
    // multi-PCR roots (the supported case) have no ancestor relation.
    assertNoNestedRootPcrs(pathSummary, rootPathNodeKeys, rootPath);
    // Pre-compute the set of pathNodeKeys along every path from docRoot
    // to each root PCR — used to PRUNE the walk to only descend into
    // subtrees that can structurally contain records. For deep nested
    // projections (e.g. /wrapper/records/[]) this turns O(total-nodes)
    // into O(ancestor-depth + records). Reference to a HashSet of longs
    // via fastutil to avoid boxing.
    this.rootAncestorPathNodeKeys = computeAncestorPathNodeKeys(pathSummary, rootPathNodeKeys);

    this.extractor = new ProjectionIndexRowExtractor(indexDef, pathSummary);
    this.currentLeaf = new ProjectionIndexLeafPage(extractor.columnKindsRef());
  }

  private static void assertNoNestedRootPcrs(final PathSummaryReader pathSummary,
      final LongSet rootPathNodeKeys, final Path<QNm> rootPath) {
    final long saved = pathSummary.getNodeKey();
    try {
      final LongIterator roots = rootPathNodeKeys.iterator();
      while (roots.hasNext()) {
        final long root = roots.nextLong();
        if (!pathSummary.moveTo(root)) continue;
        while (pathSummary.moveToParent()) {
          final long pk = pathSummary.getNodeKey();
          if (pk <= 0) break;
          if (rootPathNodeKeys.contains(pk)) {
            throw new IllegalStateException(
                "Projection root path '" + rootPath + "' resolves to NESTED record sets "
                    + "(pathNodeKey " + root + " lies inside record set " + pk + ") — "
                    + "self-nested roots are not supported; declare a more specific root path");
          }
        }
      }
    } finally {
      pathSummary.moveTo(saved);
    }
  }

  private static LongSet computeAncestorPathNodeKeys(
      final PathSummaryReader pathSummary, final LongSet rootPathNodeKeys) {
    final LongSet ancestors = new LongOpenHashSet();
    final long saved = pathSummary.getNodeKey();
    try {
      final LongIterator roots = rootPathNodeKeys.iterator();
      while (roots.hasNext()) {
        final long root = roots.nextLong();
        if (!pathSummary.moveTo(root)) continue;
        while (pathSummary.moveToParent()) {
          final long pk = pathSummary.getNodeKey();
          if (pk <= 0) break; // document root / no more
          ancestors.add(pk);
        }
      }
    } finally {
      pathSummary.moveTo(saved);
    }
    return ancestors;
  }

  /**
   * Canonical declared-type → column-kind mapping. The SINGLE source of
   * truth — the creation function, the persisted metadata, and the builder
   * must agree, or hydration's shape validation would reject healthy
   * stores.
   */
  public static byte mapTypeToColumnKind(final Type type) {
    if (type == Type.BOOL) return ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN;
    if (type == Type.INR || type == Type.LON || type == Type.INT
        || type == Type.DEC || type == Type.DBL || type == Type.FLO) {
      return ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG;
    }
    return ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT;
  }

  /**
   * Walk the resource from the document root, materialising one projection
   * row per node whose pathNodeKey is a projection-root PCR. Flushes
   * any partially-filled trailing leaf on completion.
   */
  public void build(final JsonNodeReadOnlyTrx rtx) {
    final long restoreNodeKey = rtx.getNodeKey();
    try {
      // Optional: -Dsirix.projection.builder=generic forces the original
      // DescendantAxis walk. Used for A/B verification against the pruned
      // descent.
      final boolean forceGeneric = "generic".equals(
          System.getProperty("sirix.projection.builder"));
      if (forceGeneric) {
        genericBuild(rtx);
      } else {
        rtx.moveToDocumentRoot();
        if (!tryFastArrayIteration(rtx)) {
          genericBuild(rtx);
        }
      }
      flushCurrentLeaf();
    } finally {
      rtx.moveTo(restoreNodeKey);
    }
  }

  /**
   * Generic pruned descent: walk from docRoot, descending only into
   * subtrees whose pathNodeKey is an ancestor of any root PCR
   * (pre-computed from PathSummary). When a descendant matches a root
   * pathNodeKey, process it as a record — either as an array whose
   * children are rows, or as a single-record itself.
   *
   * <p>Cost bound: O(ancestor-depth + records + record-field-walk) —
   * independent of the total document node count. Works for arbitrary
   * nesting depths, multiple matching roots across sibling subtrees,
   * and any structured record shape.
   */
  private boolean tryFastArrayIteration(final JsonNodeReadOnlyTrx rtx) {
    final long docRoot = rtx.getNodeKey();
    if (rootPathNodeKeys.isEmpty()) return false;
    boolean processedAny = descendToRoots(rtx, docRoot);
    rtx.moveTo(docRoot);
    return processedAny;
  }

  /**
   * Recursively descend from {@code parentKey} into children whose
   * pathNodeKey lies on the path to the projection root, processing each
   * root-matching node. Returns true if any record(s) were processed.
   */
  private boolean descendToRoots(final JsonNodeReadOnlyTrx rtx, final long parentKey) {
    rtx.moveTo(parentKey);
    if (!rtx.moveToFirstChild()) return false;
    boolean any = false;
    do {
      final long pk = getPathNodeKeyAtCursor(rtx);
      if (pk >= 0 && rootPathNodeKeys.contains(pk)) {
        // Match — process as record(s).
        final long matchKey = rtx.getNodeKey();
        final NodeKind matchKind = rtx.getKind();
        // iter#32 fusion: OBJECT_NAMED_ARRAY plays both the OBJECT_KEY and ARRAY role —
        // its children are the array elements directly.
        final boolean arrayLike = matchKind == NodeKind.ARRAY
            || matchKind == NodeKind.OBJECT_NAMED_ARRAY;
        if (arrayLike) {
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
        any = true;
      } else if (pk >= 0 && rootAncestorPathNodeKeys.contains(pk)) {
        // Structural ancestor of the root — descend further.
        final long curKey = rtx.getNodeKey();
        if (descendToRoots(rtx, curKey)) any = true;
        rtx.moveTo(curKey);
      }
      // else: pathNodeKey is unrelated — prune this subtree entirely.
    } while (rtx.moveToRightSibling());
    return any;
  }

  /** Fallback: original descendant-axis walk from the document root. */
  private void genericBuild(final JsonNodeReadOnlyTrx rtx) {
    rtx.moveToDocumentRoot();
    final DescendantAxis axis = new DescendantAxis(rtx);
    while (axis.hasNext()) {
      axis.nextLong();
      if (!isRecordRoot(rtx)) continue;
      final long matchKey = rtx.getNodeKey();
      final NodeKind matchKind = rtx.getKind();
      final boolean arrayLike = matchKind == NodeKind.ARRAY
          || matchKind == NodeKind.OBJECT_NAMED_ARRAY;
      if (arrayLike) {
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
  }

  /** @return total rows appended across all emitted leaves. */
  public long rowsEmitted() {
    return rowsEmitted;
  }

  /** Per-column kinds, index-aligned with the projection's declared fields. */
  public byte[] columnKinds() {
    return extractor.columnKinds();
  }

  /** @return number of serialised leaves handed to {@code leafSink}. */
  public long leavesEmitted() {
    return leavesEmitted;
  }

  /** Snapshot of the per-column non-integral flags, index-aligned with fieldNames. */
  public boolean[] numericColumnNonIntegralFlags() {
    return extractor.numericColumnNonIntegralFlags();
  }

  /**
   * True when the current rtx position is a record root under this
   * projection. Matches by pathNodeKey so the check is O(1) — no path
   * walk — and correctly handles both OBJECT- and ARRAY-rooted records
   * (any kind whose pathNodeKey matches the declared root counts).
   */
  private boolean isRecordRoot(final JsonNodeReadOnlyTrx rtx) {
    if (rtx.isDocumentRoot()) return false;
    final long pk = getPathNodeKeyAtCursor(rtx);
    return pk >= 0 && rootPathNodeKeys.contains(pk);
  }

  private static long getPathNodeKeyAtCursor(final JsonNodeReadOnlyTrx rtx) {
    // Only structured-kind nodes carry a pathNodeKey; primitives (value
    // nodes) live under an OBJECT_KEY so they return their parent's key
    // via the rtx node API. We consult the current node's kind and
    // dispatch accordingly. Fused OBJECT_NAMED_* records also carry a
    // pathNodeKey because they play the OBJECT_KEY role structurally.
    final NodeKind kind = rtx.getKind();
    if (kind == NodeKind.OBJECT || kind == NodeKind.ARRAY || kind.playsObjectKeyRole()) {
      return rtx.getPathNodeKey();
    }
    return -1L;
  }

  private void extractRow(final JsonNodeReadOnlyTrx rtx, final long recordKey) {
    extractor.extractAt(rtx, recordKey);
    if (!extractor.appendTo(currentLeaf, recordKey)) {
      flushCurrentLeaf();
      currentLeaf = new ProjectionIndexLeafPage(extractor.columnKindsRef());
      extractor.appendTo(currentLeaf, recordKey);
    }
    rowsEmitted++;
  }

  private void flushCurrentLeaf() {
    if (currentLeaf.getRowCount() == 0) return;
    leafSink.accept(currentLeaf.serialize());
    leavesEmitted++;
  }
}
