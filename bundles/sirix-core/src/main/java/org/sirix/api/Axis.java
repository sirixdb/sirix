package org.sirix.api;

import javax.annotation.Nonnegative;
import org.sirix.api.visitor.XdmNodeVisitor;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.index.path.summary.PathSummaryReader;
import com.google.common.collect.PeekingIterator;

/**
 * Interface for all axis, excluding temporal XPath axis.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface Axis extends PeekingIterator<Long>, Iterable<Long>, SirixAxis {
  /**
   * Get the cursor associated with the axis.
   *
   * @return the cursor
   */
  NodeCursor getCursor();

  /**
   * Get the transaction associated with the axis.
   *
   * @return the transaction
   * @throws ClassCastException if the node cursor is no {@link XdmNodeReadTrx}
   */
  XdmNodeReadTrx asXdmNodeReadTrx();

  /**
   * Foreach-iterator calling a {@link IVistor} for each iteration.
   *
   * @param visitor {@link XdmNodeVisitor} implementation
   */
  void foreach(XdmNodeVisitor visitor);

  /**
   * Thread safe node iterator.
   *
   * @return next node kind if one is available via the axis or {@code EKing.UNKNOWN} if not
   */
  long nextNode();

  /**
   * Resetting the nodekey of this axis to a given nodekey.
   *
   * @param nodeKey the nodekey where the reset should occur to
   */
  void reset(@Nonnegative long nodeKey);

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

  PathSummaryReader asPathSummary();
}
