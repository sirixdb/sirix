package org.sirix.api;

import java.util.Iterator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.visitor.IVisitor;
import org.sirix.axis.EIncludeSelf;

/**
 * Interface for all axis, including temporal XPath axis.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface IAxis extends Iterator<Long>, Iterable<Long> {
  /**
   * Get the transaction associated with the axis. 
   * 
   * @return the transaction or {@code null}
   */
  INodeReadTrx getTransaction();

  /**
   * Foreach-iterator using a {@link IVistor}.
   * 
   * @param pVisitor
   *          {@link IVisitor} implementation
   */
  void foreach(@Nonnull IVisitor pVisitor);

  /**
   * Thread safe node iterator.
   * 
   * @return next node key if one is available via the axis or {@code -1} if not
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
   * @return {@link EIncludeSelf} value
   */
  EIncludeSelf isSelfIncluded();
}
