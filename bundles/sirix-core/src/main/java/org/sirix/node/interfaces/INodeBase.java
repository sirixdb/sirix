package org.sirix.node.interfaces;

import org.sirix.node.EKind;

/**
 * Base interface for all nodes (even binary nodes, etc.pp.).
 * 
 * @author Johannes Lichtenberger
 *
 */
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
  
  /**
   * Get the revision this node has been inserted.
   * 
   * @return revision this node has been inserted
   */
  long getRevision();
}
