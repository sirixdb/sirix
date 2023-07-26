package io.sirix.node.interfaces.immutable;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.SirixDeweyID;

public interface ImmutableXmlNode extends ImmutableNode {
  /**
   * Get the optional dewey ID
   *
   * @return dewey ID
   */
  SirixDeweyID getDeweyID();

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
