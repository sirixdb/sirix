package org.sirix.axis.visitor;

import org.sirix.api.visitor.IVisitResult;
import org.sirix.api.visitor.IVisitor;

/**
 * The result type of an {@link IVisitor} implementation (for internal use only).
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
enum LocalVisitResult implements IVisitResult {
  /** Pop from the right sibling stack. */
  SKIPSUBTREEPOPSTACK
}
