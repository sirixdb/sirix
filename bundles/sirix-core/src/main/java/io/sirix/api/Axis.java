package io.sirix.api;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.visitor.NodeVisitor;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.IncludeSelf;
import it.unimi.dsi.fastutil.longs.LongIterable;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.checkerframework.checker.index.qual.NonNegative;
import io.sirix.index.path.summary.PathSummaryReader;

/**
 * Interface for all axis, excluding temporal XPath axis.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface Axis extends LongIterable, LongIterator, SirixAxis {
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
   * @throws ClassCastException if the node cursor is no {@link XmlNodeReadOnlyTrx}
   */
  XmlNodeReadOnlyTrx asXmlNodeReadTrx();

  /**
   * Foreach-iterator calling a {@link NodeVisitor} for each iteration.
   *
   * @param visitor {@link XmlNodeVisitor} implementation
   */
  void foreach(XmlNodeVisitor visitor);

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
  void reset(@NonNegative long nodeKey);

  /**
   * Is self included?
   *
   * @return {@link IncludeSelf} value
   */
  IncludeSelf includeSelf();

  long peek();

  /**
   * Get the start node key.
   *
   * @return start node key
   */
  long getStartKey();

  /**
   * Get the path summary.
   *
   * @return the path summary
   * @throws ClassCastException if the node cursor is no {@link PathSummaryReader}
   */
  PathSummaryReader asPathSummary();

  /**
   * Get the transaction.
   *
   * @return the transaction
   */
  <T extends NodeReadOnlyTrx & NodeCursor> T getTrx();

  /**
   * Get the transaction associated with the axis.
   *
   * @return the transaction
   * @throws ClassCastException if the node cursor is no {@link JsonNodeReadOnlyTrx}
   */
  JsonNodeReadOnlyTrx asJsonNodeReadTrx();
}
