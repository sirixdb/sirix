package org.sirix.api;

import javax.annotation.Nonnull;

import org.sirix.access.Move;
import org.sirix.access.Moved;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Node;

/**
 * Cursor interface which supports moving and other very basic functionality to
 * query the currently selected node.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface NodeCursor extends AutoCloseable {
	/**
	 * Move cursor to a node by its node key. Check the postcondition with
	 * {@code Moved#hasMoved()} or get the current cursor instance
	 * with{Moved#get()}. In case the node does not exist {@code Moved#hasMoved()}
	 * returns false and the cursor has not been moved.
	 * 
	 * @param key
	 *          key of node to select
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends NodeCursor> moveTo(long key);

	/**
	 * Move cursor to document root node. Check the postcondition with
	 * {@code Moved#hasMoved()} or get the current cursor instance
	 * with{Moved#get()}. In case the node does not exist {@code Moved#hasMoved()}
	 * returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends NodeCursor> moveToDocumentRoot();

	/**
	 * Move cursor to parent node of currently selected node. Check the
	 * postcondition with {@code Moved#hasMoved()} or get the current cursor
	 * instance with{Moved#get()}. In case the node does not exist
	 * {@code Moved#hasMoved()} returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends NodeCursor> moveToParent();

	/**
	 * Move cursor to first child node of currently selected node. Check the
	 * postcondition with {@code Moved#hasMoved()} or get the current cursor
	 * instance with{Moved#get()}. In case the node does not exist
	 * {@code Moved#hasMoved()} returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends NodeCursor> moveToFirstChild();

	/**
	 * Move cursor to last child node of currently selected node. Check the
	 * postcondition with {@code Moved#hasMoved()} or get the current cursor
	 * instance with{Moved#get()}. In case the node does not exist
	 * {@code Moved#hasMoved()} returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends NodeCursor> moveToLastChild();

	/**
	 * Move cursor to left sibling node of the currently selected node. Check the
	 * postcondition with {@code Moved#hasMoved()} or get the current cursor
	 * instance with{Moved#get()}. In case the node does not exist
	 * {@code Moved#hasMoved()} returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends NodeCursor> moveToLeftSibling();

	/**
	 * Move cursor to right sibling node of the currently selected node. Check the
	 * postcondition with {@code Moved#hasMoved()} or get the current cursor
	 * instance with{Moved#get()}. In case the node does not exist
	 * {@code Moved#hasMoved()} returns false and the cursor has not been moved.
	 * 
	 * @return {@link Moved} instance if the attribute node is selected,
	 *         {@code NotMoved} instance otherwise
	 */
	Move<? extends NodeCursor> moveToRightSibling();

	/**
	 * Determines if a node with the given key exists.
	 * 
	 * @param key
	 *          unique key of node
	 * @return {@code true} if the node with the key {@code pKey} exists,
	 *         {@code false} otherwise
	 */
	boolean hasNode(long key);

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
	 * @param visitor
	 *          {@link IVisitor} implementation
	 * @return {@link VisitResultType} value
	 */
	VisitResult acceptVisitor(@Nonnull IVisitor visitor);
	
	/**
	 * Get the node where the cursor currently is located.
	 * 
	 * @return the immutable node instance
	 */
	Node getNode();

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
	Kind getRightSiblingKind();

	/**
	 * Get the kind/type of node of the left sibling of the currently selected
	 * node.
	 * 
	 * @return kind of left sibling
	 */
	Kind getLeftSiblingKind();

	/**
	 * Get the kind/type of node of the first child of the currently selected
	 * node.
	 * 
	 * @return kind of right sibling
	 */
	Kind getFirstChildKind();

	/**
	 * Get the kind/type of node of the last child of the currently selected node.
	 * 
	 * @return kind of last child
	 */
	Kind getLastChildKind();

	/**
	 * Get the kind/type of node of the parent of the currently selected node.
	 * 
	 * @return kind of parent
	 */
	Kind getParentKind();

	/**
	 * Get the kind of node.
	 * 
	 * @return kind of node
	 */
	Kind getKind();
}
