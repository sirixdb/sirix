package org.sirix.axis;

import org.sirix.api.XdmNodeReadTrx;
import com.google.common.collect.AbstractIterator;

/**
 * TemporalAxis abstract class.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public abstract class AbstractTemporalAxis extends AbstractIterator<XdmNodeReadTrx> {

  /**
   * Get the transaction.
   * 
   * @return Sirix {@link XdmNodeReadTrx}
   */
  public abstract XdmNodeReadTrx getTrx();
}
