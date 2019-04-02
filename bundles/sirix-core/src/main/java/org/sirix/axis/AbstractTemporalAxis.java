package org.sirix.axis;

import org.sirix.api.NodeReadOnlyTrx;
import com.google.common.collect.AbstractIterator;

/**
 * TemporalAxis abstract class.
 *
 * @author Johannes Lichtenberger
 *
 */
public abstract class AbstractTemporalAxis<R extends NodeReadOnlyTrx> extends AbstractIterator<R> {

  /**
   * Get the transaction.
   *
   * @return Sirix {@link NodeReadOnlyTrx}
   */
  public abstract R getTrx();
}
