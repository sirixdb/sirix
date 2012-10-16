package org.sirix.gui.view;

import org.sirix.api.NodeReadTrx;
import org.sirix.diff.DiffFactory.EDiff;

public class TransactionTuple {
  private long mKey;
  private NodeReadTrx mRtx;
  private EDiff mDiff;
  private int mDepth;
  
  public TransactionTuple(final long pKey, final NodeReadTrx pRtx, final EDiff pDiff, final int pDepth) {
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
  
  public EDiff getDiff() {
    return mDiff;
  }

  public int getDepth() {
    return mDepth;
  }

}
