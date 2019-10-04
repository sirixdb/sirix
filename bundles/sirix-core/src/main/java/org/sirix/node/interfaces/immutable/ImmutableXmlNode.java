package org.sirix.node.interfaces.immutable;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.SirixDeweyID;

import java.util.Optional;

public interface ImmutableXmlNode extends ImmutableNode {
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
   * @param visitor implementation of the {@link XmlNodeVisitor} interface
   * @return the result of a visit
   */
  VisitResult acceptVisitor(XmlNodeVisitor visitor);
}
