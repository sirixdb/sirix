package org.sirix.api;

import com.google.common.base.Optional;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;

public interface INodeCursor extends AutoCloseable {

  /**
   * This method returns the current {@link INode} as a {@link IStructNode}.
   * 
   * @return the current node as {@link IStructNode} if possible,
   *         otherwise a special {@link NullNode} which wraps the non-structural node
   */
  IStructNode getStructuralNode();
  
  /**
   * Getting the current node.
   * 
   * @return the node
   */
  INode getNode();
  
  /**
   * Move cursor to a node by its node key.
   * 
   * @param pKey
   *          key of node to select
   * @return {@code true} if the node with the key {@code pKey} is selected, {@code false} otherwise
   */
  boolean moveTo(long pKey);

  /**
   * Move cursor to document root node.
   * 
   * @return {@code true} if the document node is selected, {@code false} otherwise
   */
  boolean moveToDocumentRoot();

  /**
   * Move cursor to parent node of currently selected node.
   * 
   * @return {@code true} if the parent node is selected, {@code false} otherwise
   */
  boolean moveToParent();

  /**
   * Move cursor to first child node of currently selected node.
   * 
   * @return {@code true} if the first child node is selected, {@code false} otherwise
   */
  boolean moveToFirstChild();
  
  /**
   * Move cursor to last child node of currently selected node.
   * 
   * @return {@code true} if the last child node is selected, {@code false} otherwise
   */
  boolean moveToLastChild();

  /**
   * Move cursor to left sibling node of the currently selected node.
   * 
   * @return {@code true} if the left sibling node is selected, {@code false} otherwise
   */
  boolean moveToLeftSibling();

  /**
   * Move cursor to right sibling node of the currently selected node.
   * 
   * @return {@code true} if the right sibling node is selected, {@code false} otherwise
   */
  boolean moveToRightSibling();
  
  Optional<IStructNode> moveToAndGetRightSibling();
  
  Optional<IStructNode> moveToAndGetLeftSibling();
  
  Optional<IStructNode> moveToAndGetParent();
  
  Optional<IStructNode> moveToAndGetFirstChild();
  
  Optional<IStructNode> moveToAndGetLastChild();
  
  Optional<IStructNode> getRightSibling();
  
  Optional<IStructNode> getLeftSibling();
  
  Optional<IStructNode> getParent();
  
  Optional<IStructNode> getFirstChild();
  
  Optional<IStructNode> getLastChild();
  
}
