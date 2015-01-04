package org.sirix.axis;

import org.sirix.api.NodeReadTrx;

import com.google.common.collect.AbstractIterator;

/**
 * TemporalAxis abstract class.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public abstract class AbstractTemporalAxis extends
		AbstractIterator<NodeReadTrx> {

	/**
	 * Get the transaction.
	 * 
	 * @return Sirix {@link NodeReadTrx}
	 */
	public abstract NodeReadTrx getTrx();
}
