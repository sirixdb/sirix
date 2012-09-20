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
   * Get unique node key.
   * 
   * @return node key
   */
  long getNodeKey();
  
  /**
   * Gets the kind of the node (element node, text node, attribute
   * node....).
   * 
   * @return kind of node
   */
  EKind getKind();
  
  /**
   * Get the revision this node has been inserted.
   * 
   * @return revision this node has been inserted
   */
  long getRevision();
}
