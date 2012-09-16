package org.sirix.api.visitor;

/**
 * The result type of an {@link IVisitor} implementation.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
public enum EVisitResult {
  /** Continue without visiting the siblings of this structural node. */
  SKIPSIBLINGS,

  /** Continue without visiting the descendants of this element. */
  SKIPSUBTREE,

  /** Continue traversal. */
  CONTINUE,

  /** Terminate traversal. */
  TERMINATE,

  /** Pop from the right sibling stack. */
  SKIPSUBTREEPOPSTACK
}
