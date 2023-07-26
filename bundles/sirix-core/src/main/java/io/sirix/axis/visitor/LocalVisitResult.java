package io.sirix.axis.visitor;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;

/**
 * The result type of an {@link XmlNodeVisitor} implementation (for internal use only).
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
enum LocalVisitResult implements VisitResult {
  /** Pop from the right sibling stack. */
  SKIPSUBTREEPOPSTACK
}
