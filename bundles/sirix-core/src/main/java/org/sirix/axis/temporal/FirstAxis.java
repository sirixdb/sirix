package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.utils.Pair;

/**
 * Open the first revision and try to move to the node with the given node key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FirstAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractTemporalAxis<R, W> {

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<R, W> mResourceManager;

  /** Node key to lookup and retrieve. */
  private final long mNodeKey;

  /** Determines if it's the first call. */
  private boolean mFirst;

  /**
   * Constructor.
   *
   * @param resourceManager the resource manager
   * @param rtx the transactional cursor
   */
  public FirstAxis(final ResourceManager<R, W> resourceManager, final R rtx) {
    mResourceManager = checkNotNull(resourceManager);
    mNodeKey = rtx.getNodeKey();
    mFirst = true;
  }

  @Override
  protected Pair<Integer, Long> computeNext() {
    if (mFirst) {
      mFirst = false;
      try (final NodeReadOnlyTrx rtx = mResourceManager.beginNodeReadOnlyTrx(1)) {
        if (rtx.moveTo(mNodeKey).hasMoved())
          return new Pair<>(1, rtx.getNodeKey());
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
