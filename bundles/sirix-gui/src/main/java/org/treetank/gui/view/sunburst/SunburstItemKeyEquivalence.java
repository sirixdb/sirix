package org.treetank.gui.view.sunburst;

import com.google.common.base.Equivalence;

/**
 * Equivalence based on item keys.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class SunburstItemKeyEquivalence extends Equivalence<SunburstItem> {
  /** {@inheritDoc} */
  @Override
  protected boolean doEquivalent(final SunburstItem paramFirst, final SunburstItem paramSecond) {
    if (!(paramFirst instanceof SunburstItem)) {
      return false;
    }
    if (!(paramSecond instanceof SunburstItem)) {
      return false;
    }
    return paramFirst.getKey() == paramSecond.getKey();
  }

  /** {@inheritDoc} */
  @Override
  protected int doHash(final SunburstItem paramItem) {
    int result = 17;
    /*
     * It takes a (64-bit) long l, exclusive-or's the top and bottom halves
     * (of 32 bits each) into the bottom 32 bits of a 64-bit results, then
     * takes only the bottom 32 bits with the (int) cast.
     */
    result = 31 * result + (int)(paramItem.getKey() ^ (paramItem.getKey() >>> 32));
    return result;
  }
}
