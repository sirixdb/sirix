package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.axis.IncludeSelf;

/**
 * Retrieve a node by node key in all future revisions. In each revision a {@link XmlNodeReadOnlyTrx} is
 * opened which is moved to the node with the given node key if it exists. Otherwise the iterator
 * has no more elements (the {@link XmlNodeReadOnlyTrx} moved to the node by it's node key).
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FutureAxis<R extends NodeReadOnlyTrx> extends AbstractTemporalAxis<R> {

  /** The revision number. */
  private int mRevision;

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx> mResourceManager;

  /** Node key to lookup and retrieve. */
  private long mNodeKey;

  /** Sirix {@link NodeReadOnlyTrx}. */
  private R mRtx;

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link XmlNodeReadOnlyTrx}
   */
  public FutureAxis(final R rtx) {
    // Using telescope pattern instead of builder (only one optional parameter).
    this(rtx, IncludeSelf.NO);
  }

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link XmlNodeReadOnlyTrx}
   * @param includeSelf determines if current revision must be included or not
   */
  public FutureAxis(final NodeReadOnlyTrx rtx, final IncludeSelf includeSelf) {
    mResourceManager = checkNotNull(rtx.getResourceManager());
    mNodeKey = rtx.getNodeKey();
    mRevision = checkNotNull(includeSelf) == IncludeSelf.YES
        ? rtx.getRevisionNumber()
        : rtx.getRevisionNumber() + 1;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected R computeNext() {
    // != a little bit faster?
    if (mRevision <= mResourceManager.getMostRecentRevisionNumber()) {
      mRtx = (R) mResourceManager.beginNodeReadOnlyTrx(mRevision++);
      return mRtx.moveTo(mNodeKey).hasMoved()
          ? mRtx
          : endOfData();
    } else {
      return endOfData();
    }
  }

  @Override
  public R getTrx() {
    return mRtx;
  }
}
