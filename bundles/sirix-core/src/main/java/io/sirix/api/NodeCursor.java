package io.sirix.api;

import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;

/**
 * Cursor interface which supports moving and other very basic functionality to query the currently
 * selected node.
 *
 * @author Johannes Lichtenberger
 *
 */
public interface NodeCursor extends AutoCloseable {
  /**
   * Move cursor to a node by its node key. Check the postcondition with {@code Moved#hasMoved()} or
   * get the current cursor instance with{Moved#get()}. In case the node does not exist
   * {@code Moved#hasMoved()} returns false and the cursor has not been moved.
   *
   * @param key key of node to select
   * @return {@code true} if the node is selected, {@code false} otherwise
   */
  boolean moveTo(long key);

  /**
   * Move to the next following node, that is the next node on the XPath {@code following::-axis},
   * that is the next node which is not a descendant of the current node.
   *
   * @return {@code true} if the node is selected, {@code false} otherwise
   */
  boolean moveToNextFollowing();

  /**
   * Move cursor to document root node. Check the postcondition with {@code Moved#hasMoved()} or get
   * the current cursor instance with{Moved#get()}. In case the node does not exist
   * {@code Moved#hasMoved()} returns false and the cursor has not been moved.
   *
   * @return {@code true} if the node is selected, {@code false} otherwise
   */
  boolean moveToDocumentRoot();

  /**
   * Move cursor to parent node of currently selected node. Check the postcondition with
   * {@code Moved#hasMoved()} or get the current cursor instance with{Moved#get()}. In case the node
   * does not exist {@code Moved#hasMoved()} returns false and the cursor has not been moved.
   *
   * @return {@code true} if the node is selected, {@code false} otherwise
   */
  boolean moveToParent();

  /**
   * Move cursor to first child node of currently selected node. Check the postcondition with
   * {@code Moved#hasMoved()} or get the current cursor instance with{Moved#get()}. In case the node
   * does not exist {@code Moved#hasMoved()} returns false and the cursor has not been moved.
   *
   * @return {@code true} if the node is selected, {@code false} otherwise
   */
  boolean moveToFirstChild();

  /**
   * Move cursor to last child node of currently selected node. Check the postcondition with
   * {@code Moved#hasMoved()} or get the current cursor instance with{Moved#get()}. In case the node
   * does not exist {@code Moved#hasMoved()} returns false and the cursor has not been moved.
   *
   * @return {@code true} if the node is selected, {@code false} otherwise
   */
  boolean moveToLastChild();

  /**
   * Move cursor to left sibling node of the currently selected node. Check the postcondition with
   * {@code Moved#hasMoved()} or get the current cursor instance with{Moved#get()}. In case the node
   * does not exist {@code Moved#hasMoved()} returns false and the cursor has not been moved.
   *
   * @return {@code true} if the node is selected, {@code false} otherwise
   */
  boolean moveToLeftSibling();

  /**
   * Move cursor to right sibling node of the currently selected node. Check the postcondition with
   * {@code Moved#hasMoved()} or get the current cursor instance with{Moved#get()}. In case the node
   * does not exist {@code Moved#hasMoved()} returns false and the cursor has not been moved.
   *
   * @return {@code true} if the node is selected, {@code false} otherwise
   */
  boolean moveToRightSibling();

  /**
   * Move cursor to the previous node of the currently selected node in document order. Check the
   * postcondition with {@code Moved#hasMoved()} or get the current cursor instance with{Moved#get()}.
   * In case the node does not exist {@code Moved#hasMoved()} returns false and the cursor has not
   * been moved.
   *
   * @return {@code true} if the node is selected, {@code false} otherwise
   */
  boolean moveToPrevious();

  /**
   * Move cursor to the next node of the currently selected node in document order. Check the
   * postcondition with {@code Moved#hasMoved()} or get the current cursor instance with{Moved#get()}.
   * In case the node does not exist {@code Moved#hasMoved()} returns false and the cursor has not
   * been moved.
   *
   * @return {@code true} if the node is selected, {@code false} otherwise
   */
  boolean moveToNext();

  /**
   * Determines if a node with the given key exists.
   *
   * @param key unique key of node
   * @return {@code true} if the node with the key {@code key} exists, {@code false} otherwise
   */
  boolean hasNode(long key);

  /**
   * Determines if the node located at the current cursor position has a parent.
   *
   * @return {@code true} if the node has a parent, {@code false} otherwise
   */
  boolean hasParent();

  /**
   * Determines if the node located at the current cursor position has a first child.
   *
   * @return {@code true} if the node has a parent, {@code false} otherwise
   */
  boolean hasFirstChild();

  /**
   * Determines if the node located at the current cursor position has a last child.
   *
   * @return {@code true} if the node has a parent, {@code false} otherwise
   */
  boolean hasLastChild();

  /**
   * Determines if the node located at the current cursor position has a left sibling.
   *
   * @return {@code true} if the node has a parent, {@code false} otherwise
   */
  boolean hasLeftSibling();

  /**
   * Determines if the node located at the current cursor position has a right sibling.
   *
   * @return {@code true} if the node has a right sibling, {@code false} otherwise
   */
  boolean hasRightSibling();

  /**
   * Get the left sibling node key of the currently selected node.
   *
   * @return left sibling node key of the currently selected node
   */
  long getLeftSiblingKey();

  /**
   * Get the right sibling node key of the currently selected node.
   *
   * @return right sibling node key of the currently selected node
   */
  long getRightSiblingKey();

  /**
   * Get the first child key of the currently selected node.
   *
   * @return first child key of the currently selected node
   */
  long getFirstChildKey();

  /**
   * Get the last child key of the currently selected node.
   *
   * @return last child key of the currently selected node
   */
  long getLastChildKey();

  /**
   * Get the parent key of the currently selected node.
   *
   * @return parent key of the currently selected node
   */
  long getParentKey();

  /**
   * Get the immutable node where the cursor currently is located.
   *
   * @return the immutable node instance
   */
  ImmutableNode getNode();

  /**
   * Get unique node key, that is the unique ID of the currently selected node.
   *
   * @return unique node key
   */
  long getNodeKey();

  /**
   * Get the kind/type of node of the right sibling of the currently selected node.
   *
   * @return kind of right sibling
   */
  NodeKind getRightSiblingKind();

  /**
   * Get the kind/type of node of the left sibling of the currently selected node.
   *
   * @return kind of left sibling
   */
  NodeKind getLeftSiblingKind();

  /**
   * Get the kind/type of node of the first child of the currently selected node.
   *
   * @return kind of right sibling
   */
  NodeKind getFirstChildKind();

  /**
   * Get the kind/type of node of the last child of the currently selected node.
   *
   * @return kind of last child
   */
  NodeKind getLastChildKind();

  /**
   * Get the kind/type of node of the parent of the currently selected node.
   *
   * @return kind of parent
   */
  NodeKind getParentKind();

  /**
   * Get the kind of node.
   *
   * @return kind of node
   */
  NodeKind getKind();
}
