package org.sirix.axis.temporal;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.AbstractTemporalAxis;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

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
  private int mRevision;

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<R, W> mResourceManager;

  /** Node key to lookup and retrieve. */
  private long mNodeKey;

  /** Determines if node has been found before and now has been deleted. */
  private boolean mHasMoved;

  /**
   * Constructor.
   *
   * @param resourceManager the resource manager
   * @param rtx the read only transactional cursor
   */
  public AllTimeAxis(final ResourceManager<R, W> resourceManager, final R rtx) {
    mResourceManager = checkNotNull(resourceManager);
    mRevision = 1;
    mNodeKey = rtx.getNodeKey();
  }

  @Override
  protected R computeNext() {
    while (mRevision <= mResourceManager.getMostRecentRevisionNumber()) {
      final Optional<R> optionalRtx = mResourceManager.getNodeReadTrxByRevisionNumber(mRevision);

      final R rtx;
      if (optionalRtx.isPresent()) {
        rtx = optionalRtx.get();
      } else {
        rtx = mResourceManager.beginNodeReadOnlyTrx(mRevision);
      }

      mRevision++;

      if (rtx.moveTo(mNodeKey).hasMoved()) {
        mHasMoved = true;
        return rtx;
      } else if (mHasMoved) {
        rtx.close();
        return endOfData();
      }
    }

    return endOfData();
  }

  @Override
  public ResourceManager<R, W> getResourceManager() {
    return mResourceManager;
  }
}
