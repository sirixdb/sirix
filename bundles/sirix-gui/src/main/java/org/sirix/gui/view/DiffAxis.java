package org.sirix.gui.view;

import org.sirix.api.NodeReadTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.diff.DiffFactory.DiffType;

public class DiffAxis extends AbstractDiffAxis {

  /** {@link VisualItemAxis} reference. */
  private final VisualItemAxis mAxis;

  /** {@link NodeReadTrx} on new revision. */
  private NodeReadTrx mNewRtx;

  /** {@link NodeReadTrx} on old revision. */
  private NodeReadTrx mOldRtx;

  /** Determines if element has been peeked. */
  private boolean mHasPeeked;

  /** Peeked element. */
  private Long mPeekedElement;

  /** First call to {@code hasNext()}? */
  private boolean mFirst;

  /** Current {@link DiffType} value. */
  private DiffType mDiff;

  /** Temp. {@link DiffType} value. */
  private DiffType mTmpDiff;

  private boolean mLastHasNext;

  private int mDepth;

  /**
   * Constructor initializing internal state.
   * 
   * @param pIncludeSelf
   *          determines if self is included
   * @param pNewRtx
   *          {@link NodeReadTrx} on new revision
   * @param pOldRtx
   *          {@link NodeReadTrx} on old revision
   * @param pAxis
   *          {@link VisualItemAxis} reference
   */
  public DiffAxis(final IncludeSelf pIncludeSelf, final NodeReadTrx pNewRtx, final NodeReadTrx pOldRtx,
    final VisualItemAxis pAxis) {
    super(pNewRtx, pIncludeSelf);
    mAxis = pAxis;
    mNewRtx = pNewRtx;
    mOldRtx = pOldRtx;
    
//    mKey = mAxis.
  }

  @Override
  public void reset(long pNodeKey) {
    super.reset(pNodeKey);
    mFirst = true;
    mHasPeeked = false;
    mDiff = DiffType.SAME;

    if (mAxis != null) {
      mAxis.reset();
    }
  }

  @Override
  public boolean hasNext() {
    if (mFirst && isSelfIncluded() == IncludeSelf.NO) {
      mFirst = false;
      resetToLastKey();
      if (mAxis.hasNext()) {
        mAxis.next();
      }
      return true;
    }
    if (mHasPeeked) {
      getTransaction().moveTo(mKey);
      return true;
    }
    if (getNext() && mLastHasNext) {
      return true;
    }

    resetToLastKey();

    // Setup everything.
    if (mAxis.hasNext()) {
      final VisualItem item = mAxis.next();
      mDiff = item.getDiff();
      final boolean isOldTransaction =
        (mDiff == DiffType.DELETED || mDiff == DiffType.MOVEDFROM || mDiff == DiffType.REPLACEDOLD);
      final long nodeKey = item.getKey();
      mDepth = item.getOriginalDepth();
      
      setTransaction(isOldTransaction ? mOldRtx : mNewRtx);

      // Move to next key.
      mKey = nodeKey;

      if (mDiff == DiffType.UPDATED || mDiff == DiffType.REPLACEDNEW || mDiff == DiffType.REPLACEDOLD) {
        // For EDiff.UPDATE or EDiff.REPLACED the transaction needs to be on the right node.
        mOldRtx.moveTo(nodeKey);
      }

      if (mDiff == DiffType.REPLACEDOLD) {
        mNewRtx.moveTo(item.getKey());
      }
      mLastHasNext = true;
      return mLastHasNext;
    } else {
      mLastHasNext = false;
      return mLastHasNext;
    }
  }

  @Override
  public Long next() {
    mTmpDiff = mDiff;
    if (!mHasPeeked) {
      return super.next();
    }
    final Long result = mPeekedElement;
    mHasPeeked = false;
    mPeekedElement = null;
    mDiff = mTmpDiff;
    return result;
  }

  /**
   * Peek element.
   * 
   * @return peeked element
   */
  public Long peek() {
    if (!mHasPeeked) {
      mPeekedElement = next();
      mHasPeeked = true;
    }
    return mPeekedElement;
  }

  /**
   * Get diff type.
   * 
   * @return {@link DiffType} value
   */
  public DiffType getDiff() {
    return mDiff;
  }
  
  public int getDepth() {
    return mDepth;
  }
  
  public NodeReadTrx getOldRtx() {
    return mOldRtx;
  }
  
  public NodeReadTrx getNewRtx() {
    return mNewRtx;
  }

}
