package org.sirix.node.interfaces.immutable;

/**
 * Immutable structural node (for instance element-, text-, PI-, document-node...).
 * 
 * @author Johannes Lichtenberger
 *
 */
public interface ImmutableStructNode extends ImmutableNode {
	/**
	 * Declares, whether the item has a first child.
	 * 
	 * @return true, if item has a first child, otherwise false
	 */
	boolean hasFirstChild();

	/**
	 * Declares, whether the item has a left sibling.
	 * 
	 * @return true, if item has a left sibling, otherwise false
	 */
	boolean hasLeftSibling();

	/**
	 * Declares, whether the item has a right sibling.
	 * 
	 * @return true, if item has a right sibling, otherwise false
	 */
	boolean hasRightSibling();

	/**
	 * Get the number of children of the node.
	 * 
	 * @return node's number of children
	 */
	long getChildCount();

	/**
	 * Get the number of descendants of the node.
	 * 
	 * @return node's number of descendants
	 */
	long getDescendantCount();

	/**
	 * Gets key of the context item's first child.
	 * 
	 * @return first child's key
	 */
	long getFirstChildKey();

	/**
	 * Gets key of the context item's left sibling.
	 * 
	 * @return left sibling key
	 */
	long getLeftSiblingKey();

	/**
	 * Gets key of the context item's right sibling.
	 * 
	 * @return right sibling key
	 */
	long getRightSiblingKey();
}
