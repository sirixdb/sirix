package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.utils.Pair;

/**
 * Open the next revision and try to move to the node with the given node key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class NextAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractTemporalAxis<R, W> {

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<R, W> mResourceManager;

  /** Determines if it's the first call. */
  private boolean mFirst;

  /** The revision number. */
  private int mRevision;

  /** Node key to lookup and retrieve. */
  private long mNodeKey;

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link NodeReadOnlyTrx}
   */
  public NextAxis(final ResourceManager<R, W> resourceManager, final R rtx) {
    mResourceManager = checkNotNull(resourceManager);
    mRevision = 0;
    mNodeKey = rtx.getNodeKey();
    mRevision = rtx.getRevisionNumber() + 1;
    mFirst = true;
  }

  @Override
  protected Pair<Integer, Long> computeNext() {
    if (mRevision <= mResourceManager.getMostRecentRevisionNumber() && mFirst) {
      mFirst = false;
      try (final NodeReadOnlyTrx rtx = mResourceManager.beginNodeReadOnlyTrx(mRevision)) {
        if (rtx.moveTo(mNodeKey).hasMoved())
          return new Pair<>(rtx.getRevisionNumber(), rtx.getNodeKey());
        else
          return endOfData();
      }
    } else {
      return endOfData();
    }
  }

  @Override
  public ResourceManager<R, W> getResourceManager() {
    return mResourceManager;
  }
}
