package org.sirix.node.interfaces.immutable;

import java.util.Optional;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XdmNodeVisitor;
import org.sirix.node.SirixDeweyID;

public interface ImmutableXdmNode extends ImmutableNode {
  /**
   * Get the optional dewey ID
   *
   * @return dewey ID
   */
  Optional<SirixDeweyID> getDeweyID();

  /**
   * Gets value type of the item.
   *
   * @return value type
   */
  int getTypeKey();

  /**
   * Accept a visitor and use double dispatching to invoke the visitor method.
   *
   * @param visitor implementation of the {@link XdmNodeVisitor} interface
   * @return the result of a visit
   */
  VisitResult acceptVisitor(XdmNodeVisitor visitor);
}
