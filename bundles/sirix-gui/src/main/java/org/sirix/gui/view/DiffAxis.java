package org.sirix.gui.view;

import org.sirix.api.INodeReadTrx;
import org.sirix.axis.EIncludeSelf;
import org.sirix.diff.DiffFactory.EDiff;

public class DiffAxis extends AbsDiffAxis {

  /** {@link VisualItemAxis} reference. */
  private final VisualItemAxis mAxis;

  /** {@link INodeReadTrx} on new revision. */
  private INodeReadTrx mNewRtx;

  /** {@link INodeReadTrx} on old revision. */
  private INodeReadTrx mOldRtx;

  /** Determines if element has been peeked. */
  private boolean mHasPeeked;

  /** Peeked element. */
  private Long mPeekedElement;

  /** First call to {@code hasNext()}? */
  private boolean mFirst;

  /** Current {@link EDiff} value. */
  private EDiff mDiff;

  /** Temp. {@link EDiff} value. */
  private EDiff mTmpDiff;

  private boolean mLastHasNext;

  private int mDepth;

  /**
   * Constructor initializing internal state.
   * 
   * @param pIncludeSelf
   *          determines if self is included
   * @param pNewRtx
   *          {@link INodeReadTrx} on new revision
   * @param pOldRtx
   *          {@link INodeReadTrx} on old revision
   * @param pAxis
   *          {@link VisualItemAxis} reference
   */
  public DiffAxis(final EIncludeSelf pIncludeSelf, final INodeReadTrx pNewRtx, final INodeReadTrx pOldRtx,
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
    mDiff = EDiff.SAME;

    if (mAxis != null) {
      mAxis.reset();
    }
  }

  @Override
  public boolean hasNext() {
    if (mFirst && isSelfIncluded() == EIncludeSelf.NO) {
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
      final IVisualItem item = mAxis.next();
      mDiff = item.getDiff();
      final boolean isOldTransaction =
        (mDiff == EDiff.DELETED || mDiff == EDiff.MOVEDFROM || mDiff == EDiff.REPLACEDOLD);
      final long nodeKey = item.getKey();
      mDepth = item.getOriginalDepth();
      
      setTransaction(isOldTransaction ? mOldRtx : mNewRtx);

      // Move to next key.
      mKey = nodeKey;

      if (mDiff == EDiff.UPDATED || mDiff == EDiff.REPLACEDNEW || mDiff == EDiff.REPLACEDOLD) {
        // For EDiff.UPDATE or EDiff.REPLACED the transaction needs to be on the right node.
        mOldRtx.moveTo(nodeKey);
      }

      if (mDiff == EDiff.REPLACEDOLD) {
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
   * @return {@link EDiff} value
   */
  public EDiff getDiff() {
    return mDiff;
  }
  
  public int getDepth() {
    return mDepth;
  }
  
  public INodeReadTrx getOldRtx() {
    return mOldRtx;
  }
  
  public INodeReadTrx getNewRtx() {
    return mNewRtx;
  }

}
