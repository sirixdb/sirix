package org.sirix.gui.view;

import org.sirix.api.NodeReadTrx;
import org.sirix.diff.DiffFactory.DiffType;

public class TransactionTuple {
  private long mKey;
  private NodeReadTrx mRtx;
  private DiffType mDiff;
  private int mDepth;
  
  public TransactionTuple(final long pKey, final NodeReadTrx pRtx, final DiffType pDiff, final int pDepth) {
    mKey = pKey;
    mRtx = pRtx;
    mDiff = pDiff;
    mDepth = pDepth;
  }
  
  public long getKey() {
    return mKey;
  }
  
  public NodeReadTrx getRtx() {
    return mRtx;
  }
  
  public DiffType getDiff() {
    return mDiff;
  }

  public int getDepth() {
    return mDepth;
  }

}
