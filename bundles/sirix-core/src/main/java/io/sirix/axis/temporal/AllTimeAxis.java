package io.sirix.axis.temporal;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.AbstractTemporalAxis;

import static java.util.Objects.requireNonNull;

/**
 * Retrieve a node by node key in all revisions. In each revision a {@link XmlNodeReadOnlyTrx} is
 * opened which is moved to the node with the given node key if it exists. Otherwise the iterator
 * has no more elements (the {@link XmlNodeReadOnlyTrx} moved to the node by it's node key).
 *
 * @author Johannes Lichtenberger
 *
 */
public final class AllTimeAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractTemporalAxis<R, W> {

  /** The revision number. */
  private int revision;

  /** Sirix {@link ResourceSession}. */
  private final ResourceSession<R, W> resourceSession;

  /** Node key to lookup and retrieve. */
  private final long nodeKey;

  /** Determines if node has been found before and now has been deleted. */
  private boolean hasMoved;

  /**
   * Upper bound for the walk. Equals the most-recent revision when the node is
   * still alive at the latest, or the last entry from the
   * {@link io.sirix.index.IndexType#RECORD_TO_REVISIONS} index when the index is
   * present and tells us the last revision the node was touched.
   */
  private final int maxRevision;

  /**
   * Constructor.
   *
   * @param resourceSession the resource session
   * @param rtx the read only transactional cursor
   */
  public AllTimeAxis(final ResourceSession<R, W> resourceSession, final R rtx) {
    this.resourceSession = requireNonNull(resourceSession);
    nodeKey = rtx.getNodeKey();
    maxRevision = resourceSession.getMostRecentRevisionNumber();
    // Fast path: when storeNodeHistory is on, the RECORD_TO_REVISIONS index tells us
    // the first revision in which this node existed. Skipping the prefix below it
    // avoids opening one NodeReadOnlyTrx (UberPage navigation, RevisionRootPage load,
    // indirect-page traversal) per useless revision.
    final int[] revs = RecordRevisionsLookup.revisionsFor(rtx, nodeKey);
    revision = revs == null ? 1 : revs[0];
  }

  @Override
  protected R computeNext() {
    while (revision <= maxRevision) {
      final R rtx = resourceSession.beginNodeReadOnlyTrx(revision);
      revision++;
      if (rtx.moveTo(nodeKey)) {
        hasMoved = true;
        return rtx;
      }
      // moveTo failed: either we're still in the prefix before the node was created, or
      // the node was deleted after a prior yield. Either way, this rtx is ours alone —
      // close it so it does not leak. (Past/FutureAxis already do this; AllTimeAxis used
      // to leak the prefix rtxs.)
      rtx.close();
      if (hasMoved) {
        return endOfData();
      }
      // Index might be missing (storeNodeHistory off) — keep scanning.
    }

    return endOfData();
  }

  @Override
  public ResourceSession<R, W> getResourceSession() {
    return resourceSession;
  }
}
