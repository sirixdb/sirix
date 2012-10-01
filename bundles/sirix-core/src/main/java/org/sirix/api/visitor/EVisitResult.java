package org.sirix.api.visitor;

/**
 * The result type of an {@link IVisitor} implementation.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
public enum EVisitResult implements IVisitResult {
  /** Continue without visiting the siblings of this node. */
  SKIPSIBLINGS,

  /** Continue without visiting the descendants of this node. */
  SKIPSUBTREE,

  /** Continue traversal. */
  CONTINUE,

  /** Terminate traversal. */
  TERMINATE
}
