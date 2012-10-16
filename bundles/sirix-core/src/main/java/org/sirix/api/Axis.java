package org.sirix.api;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.visitor.IVisitor;
import org.sirix.axis.IncludeSelf;

import com.google.common.collect.PeekingIterator;

/**
 * Interface for all axis, including temporal XPath axis.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface Axis extends PeekingIterator<Long>, Iterable<Long> {
	/**
	 * Get the transaction associated with the axis.
	 * 
	 * @return the transaction or {@code null}
	 */
	NodeReadTrx getTrx();

	/**
	 * Foreach-iterator calling a {@link IVistor} for each iteration.
	 * 
	 * @param pVisitor
	 *          {@link IVisitor} implementation
	 */
	void foreach(@Nonnull IVisitor pVisitor);

	/**
	 * Thread safe node iterator.
	 * 
	 * @return next node kind if one is available via the axis or
	 *         {@code EKing.UNKNOWN} if not
	 */
	long nextNode();

	/**
	 * Resetting the nodekey of this axis to a given nodekey.
	 * 
	 * @param pNodeKey
	 *          the nodekey where the reset should occur to
	 */
	void reset(@Nonnegative long pNodeKey);

	/**
	 * Is self included?
	 * 
	 * @return {@link IncludeSelf} value
	 */
	IncludeSelf isSelfIncluded();

	/**
	 * Get the start node key.
	 * 
	 * @return start node key
	 */
	long getStartKey();
}
