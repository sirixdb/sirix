package org.sirix.axis.visitor;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XdmNodeVisitor;

/**
 * The result type of an {@link XdmNodeVisitor} implementation (for internal use only).
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
enum LocalVisitResult implements VisitResult {
  /** Pop from the right sibling stack. */
  SKIPSUBTREEPOPSTACK
}
