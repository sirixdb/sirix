package org.sirix.node.interfaces.immutable;

import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XdmNodeVisitor;

public interface ImmutableJsonNode extends ImmutableNode {
  /**
   * Accept a visitor and use double dispatching to invoke the visitor method.
   *
   * @param visitor implementation of the {@link XdmNodeVisitor} interface
   * @return the result of a visit
   */
  VisitResult acceptVisitor(JsonNodeVisitor visitor);
}
