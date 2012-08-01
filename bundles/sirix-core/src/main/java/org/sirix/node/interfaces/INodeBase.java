package org.sirix.node.interfaces;

import org.sirix.node.EKind;

public interface INodeBase {
  /**
   * Get unique item key.
   * 
   * @return item key
   */
  long getNodeKey();
  
  /**
   * Gets the kind of the item (atomic value, element node, attribute
   * node....).
   * 
   * @return kind of item
   */
  EKind getKind();
}
