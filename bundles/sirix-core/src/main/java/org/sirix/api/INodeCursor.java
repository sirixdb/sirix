package org.sirix.api;

import javax.annotation.Nonnull;

import org.sirix.access.Move;
import org.sirix.access.Moved;
import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.EKind;

/**
 * Cursor interface.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface INodeCursor extends AutoCloseable {
	/**
	 * Move cursor to a node by its node key. Check the postcondition with
	 * {@code Moved#hasMoved()} or get the current cursor instance
	 * with{Moved#get()}. In case the node does not exist {@code Moved#hasMoved()}
	 * returns false and the cursor has not been moved.
	 * 
	 * @param pKey
	 *          key of node to select
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends INodeCursor> moveTo(long pKey);

	/**
	 * Move cursor to document root node. Check the postcondition with
	 * {@code Moved#hasMoved()} or get the current cursor instance
	 * with{Moved#get()}. In case the node does not exist {@code Moved#hasMoved()}
	 * returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends INodeCursor> moveToDocumentRoot();

	/**
	 * Move cursor to parent node of currently selected node. Check the
	 * postcondition with {@code Moved#hasMoved()} or get the current cursor
	 * instance with{Moved#get()}. In case the node does not exist {@code Moved#hasMoved()}
	 * returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends INodeCursor> moveToParent();

	/**
	 * Move cursor to first child node of currently selected node. Check the
	 * postcondition with {@code Moved#hasMoved()} or get the current cursor
	 * instance with{Moved#get()}. In case the node does not exist {@code Moved#hasMoved()}
	 * returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends INodeCursor> moveToFirstChild();

	/**
	 * Move cursor to last child node of currently selected node. Check the
	 * postcondition with {@code Moved#hasMoved()} or get the current cursor
	 * instance with{Moved#get()}. In case the node does not exist {@code Moved#hasMoved()}
	 * returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends INodeCursor> moveToLastChild();

	/**
	 * Move cursor to left sibling node of the currently selected node. Check the
	 * postcondition with {@code Moved#hasMoved()} or get the current cursor
	 * instance with{Moved#get()}. In case the node does not exist {@code Moved#hasMoved()}
	 * returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends INodeCursor> moveToLeftSibling();

	/**
	 * Move cursor to right sibling node of the currently selected node. Check the
	 * postcondition with {@code Moved#hasMoved()} or get the current cursor
	 * instance with{Moved#get()}. In case the node does not exist {@code Moved#hasMoved()}
	 * returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends INodeCursor> moveToRightSibling();

	/**
	 * Determines if a node with the given key exists.
	 * 
	 * @param pKey
	 *          unique key of node
	 * @return {@code true} if the node with the key {@code pKey} exists,
	 *         {@code false} otherwise
	 */
	boolean hasNode(long pKey);

	/**
	 * Determines if the node located at the current cursor position has a parent.
	 * 
	 * @return {@code true} if the node has a parent, {@code false} otherwise
	 */
	boolean hasParent();

	/**
	 * Determines if the node located at the current cursor position has a first
	 * child.
	 * 
	 * @return {@code true} if the node has a parent, {@code false} otherwise
	 */
	boolean hasFirstChild();

	/**
	 * Determines if the node located at the current cursor position has a last
	 * child.
	 * 
	 * @return {@code true} if the node has a parent, {@code false} otherwise
	 */
	boolean hasLastChild();

	/**
	 * Determines if the node located at the current cursor position has a left
	 * sibling.
	 * 
	 * @return {@code true} if the node has a parent, {@code false} otherwise
	 */
	boolean hasLeftSibling();

	/**
	 * Determines if the node located at the current cursor position has a right
	 * sibling.
	 * 
	 * @return {@code true} if the node has a right sibling, {@code false}
	 *         otherwise
	 */
	boolean hasRightSibling();

	/**
	 * Accept a visitor.
	 * 
	 * @param pVisitor
	 *          {@link IVisitor} implementation
	 * @return {@link EVisitResult} value
	 */
	EVisitResult acceptVisitor(@Nonnull IVisitor pVisitor);

	/**
	 * Get unique node key, that is the unique ID of the currently selected node.
	 * 
	 * @return unique node key
	 */
	long getNodeKey();

	/**
	 * Get the kind/type of node of the right sibling of the currently selected
	 * node.
	 * 
	 * @return kind of right sibling
	 */
	EKind getRightSiblingKind();

	/**
	 * Get the kind/type of node of the left sibling of the currently selected
	 * node.
	 * 
	 * @return kind of left sibling
	 */
	EKind getLeftSiblingKind();

	/**
	 * Get the kind/type of node of the first child of the currently selected
	 * node.
	 * 
	 * @return kind of right sibling
	 */
	EKind getFirstChildKind();

	/**
	 * Get the kind/type of node of the last child of the currently selected node.
	 * 
	 * @return kind of last child
	 */
	EKind getLastChildKind();

	/**
	 * Get the kind/type of node of the parent of the currently selected node.
	 * 
	 * @return kind of parent
	 */
	EKind getParentKind();
}
