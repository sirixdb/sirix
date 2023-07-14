package org.sirix.index.path;

import it.unimi.dsi.fastutil.longs.LongSet;

public class PCRValue {
  private final long maxPCR;

  private final LongSet pcrs;

  private PCRValue(final long maxPCR, final LongSet pcrs) {
    this.maxPCR = maxPCR;
    this.pcrs = pcrs;
  }

  public static PCRValue getInstance(final long maxPCR, final LongSet pcrs) {
    return new PCRValue(maxPCR, pcrs);
  }

  public static PCRValue getEmptyInstance() {
    return new PCRValue(0, LongSet.of());
  }

  public long getMaxPCR() {
    return maxPCR;
  }

  public LongSet getPCRs() {
    return pcrs;
  }
}
