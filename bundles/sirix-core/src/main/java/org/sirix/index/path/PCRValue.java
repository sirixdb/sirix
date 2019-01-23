package org.sirix.index.path;

import java.util.Collections;
import java.util.Set;

public class PCRValue {
  private final long mMaxPCR;

  private final Set<Long> mPCRs;

  private PCRValue(final long maxPCR, final Set<Long> pcrs) {
    mMaxPCR = maxPCR;
    mPCRs = pcrs;
  }

  public static final PCRValue getInstance(final long maxPCR, final Set<Long> pcrs) {
    return new PCRValue(maxPCR, pcrs);
  }

  public static final PCRValue getEmptyInstance() {
    return new PCRValue(0, Collections.emptySet());
  }

  public long getMaxPCR() {
    return mMaxPCR;
  }

  public Set<Long> getPCRs() {
    return mPCRs;
  }
}
