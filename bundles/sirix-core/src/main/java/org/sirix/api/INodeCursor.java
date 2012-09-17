package org.sirix.api;

import com.google.common.base.Optional;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;

/** 
 * Cursor interface.
 * 
 * @author Johannes Lichtenberger
 *
 */
public interface INodeCursor extends AutoCloseable {

	/**
	 * This method returns the current {@link INode} as a {@link IStructNode}.
	 * 
	 * @return the current node as {@link IStructNode} if possible, otherwise a
	 *         special {@link NullNode} which wraps the non-structural node
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
	 * @return {@code true} if the node with the key {@code pKey} is selected,
	 *         {@code false} otherwise
	 */
	boolean moveTo(long pKey);

	/**
	 * Move cursor to document root node.
	 * 
	 * @return {@code true} if the document node is selected, {@code false}
	 *         otherwise
	 */
	boolean moveToDocumentRoot();

	/**
	 * Move cursor to parent node of currently selected node.
	 * 
	 * @return {@code true} if the parent node is selected, {@code false}
	 *         otherwise
	 */
	boolean moveToParent();

	/**
	 * Move cursor to first child node of currently selected node.
	 * 
	 * @return {@code true} if the first child node is selected, {@code false}
	 *         otherwise
	 */
	boolean moveToFirstChild();

	/**
	 * Move cursor to last child node of currently selected node.
	 * 
	 * @return {@code true} if the last child node is selected, {@code false}
	 *         otherwise
	 */
	boolean moveToLastChild();

	/**
	 * Move cursor to left sibling node of the currently selected node.
	 * 
	 * @return {@code true} if the left sibling node is selected, {@code false}
	 *         otherwise
	 */
	boolean moveToLeftSibling();

	/**
	 * Move cursor to right sibling node of the currently selected node.
	 * 
	 * @return {@code true} if the right sibling node is selected, {@code false}
	 *         otherwise
	 */
	boolean moveToRightSibling();

	/**
	 * Move to the right sibling and return the right sibling.
	 * 
	 * @return an {@link Optional} instance which encapsulates the right sibling
	 *         if it exists. Otherwise the right sibling is absent.
	 */
	Optional<IStructNode> moveToAndGetRightSibling();

	/**
	 * Move to the left sibling and return the left sibling.
	 * 
	 * @return an {@link Optional} instance which encapsulates the left sibling
	 *         if it exists. Otherwise the left sibling is absent.
	 */
	Optional<IStructNode> moveToAndGetLeftSibling();

	/**
	 * Move to the parent and return it.
	 * 
	 * @return an {@link Optional} instance which encapsulates the parent
	 *         if it exists. Otherwise the parent is absent.
	 */
	Optional<IStructNode> moveToAndGetParent();

	/**
	 * Move to the first child and return the first child.
	 * 
	 * @return an {@link Optional} instance which encapsulates the first child
	 *         if it exists. Otherwise the first child is absent.
	 */
	Optional<IStructNode> moveToAndGetFirstChild();

	/**
	 * Move to the last child and return the last child.
	 * 
	 * @return an {@link Optional} instance which encapsulates the last child
	 *         if it exists. Otherwise the last child is absent.
	 */
	Optional<IStructNode> moveToAndGetLastChild();

	/**
	 * Get the right sibling. Postcondition: The transaction-cursor isn't moved.
	 * 
	 * @return an {@link Optional} instance which encapsulates the right sibling
	 *         if it exists. Otherwise the right sibling is absent.
	 */
	Optional<IStructNode> getRightSibling();

	/**
	 * Get the left sibling. Postcondition: The transaction-cursor isn't moved.
	 * 
	 * @return an {@link Optional} instance which encapsulates the left sibling
	 *         if it exists. Otherwise the left sibling is absent.
	 */
	Optional<IStructNode> getLeftSibling();

	/**
	 * Get the parent. Postcondition: The transaction-cursor isn't moved.
	 * 
	 * @return an {@link Optional} instance which encapsulates the right sibling
	 *         if it exists. Otherwise the parent is absent.
	 */
	Optional<IStructNode> getParent();

	/**
	 * Get the first child. Postcondition: The transaction-cursor isn't moved.
	 * 
	 * @return an {@link Optional} instance which encapsulates the first child
	 *         if it exists. Otherwise the first child is absent.
	 */
	Optional<IStructNode> getFirstChild();

	/**
	 * Get the last child. Postcondition: The transaction-cursor isn't moved.
	 * 
	 * @return an {@link Optional} instance which encapsulates the last child
	 *         if it exists. Otherwise the last child is absent.
	 */
	Optional<IStructNode> getLastChild();
}
