package io.sirix.axis.temporal;

import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.index.IndexType;
import io.sirix.node.RevisionReferencesNode;
import io.sirix.node.interfaces.DataRecord;
import org.jspecify.annotations.Nullable;

/**
 * Reads the {@link IndexType#RECORD_TO_REVISIONS} index to discover the set of
 * revisions in which a record was created or modified.
 *
 * <p>Used by {@link AllTimeAxis} (and similar temporal axes) to skip past the
 * prefix of revisions that pre-date the node's creation. Returns {@code null}
 * when the resource was created without {@code storeNodeHistory} (the index is
 * absent), so callers can fall back to the linear scan that pre-dates this
 * optimization.
 *
 * <h2>Why this matters</h2>
 *
 * The naive {@code AllTimeAxis} opens a fresh {@link NodeReadOnlyTrx} for every
 * revision in {@code [1, mostRecentRevision]} and tries {@code moveTo(nodeKey)}.
 * For a node created late in a long-lived resource (say revision 990 of 1000),
 * 989 of those transactions yield no useful work — every one pays full
 * {@code beginNodeReadOnlyTrx} startup cost (UberPage navigation, RevisionRootPage
 * load, indirect-page traversal) only to have {@code moveTo} return false.
 * Reading the {@code RECORD_TO_REVISIONS} index once tells us the create
 * revision so we can jump directly to it.
 */
public final class RecordRevisionsLookup {

  private RecordRevisionsLookup() {
  }

  /**
   * Fetch the ascending sorted list of revisions in which {@code nodeKey} was
   * created or modified. Returns {@code null} when the resource has no
   * {@code RECORD_TO_REVISIONS} index entry for the key (either because
   * {@code storeNodeHistory} is disabled, or the record never existed).
   *
   * <p>The returned array is the live array stored on the
   * {@link RevisionReferencesNode} — callers must treat it as read-only.
   * Mutating it would corrupt the index.
   */
  public static int @Nullable [] revisionsFor(final NodeReadOnlyTrx rtx, final long nodeKey) {
    // Resources without storeNodeHistory have no RECORD_TO_REVISIONS index. The
    // underlying trie infrastructure is shared across index types, so a lookup
    // there can still return a stale / unrelated record — guard up-front.
    if (!rtx.getResourceSession().getResourceConfig().storeNodeHistory()) {
      return null;
    }
    final DataRecord record = rtx.getStorageEngineReader()
        .getRecord(nodeKey, IndexType.RECORD_TO_REVISIONS, 0);
    if (!(record instanceof RevisionReferencesNode rrn)) {
      // Defensive — RECORD_TO_REVISIONS may return a placeholder of a different
      // kind on an empty slot. Treat as "no entry" rather than misinterpreting
      // unrelated bytes as a revisions array.
      return null;
    }
    final int[] revs = rrn.getRevisions();
    return (revs == null || revs.length == 0) ? null : revs;
  }
}
