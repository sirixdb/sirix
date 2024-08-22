package io.sirix.index.redblacktree.interfaces;

/**
 * Mutable RBNode.
 *
 * @author Johannes Lichtenberger
 *
 */
public interface MutableRBNodeKey<K extends Comparable<? super K>> extends ImmutableRBNodeKey<K> {
	/**
	 * Set the key.
	 *
	 * @param key
	 *            key to set
	 */
	void setKey(K key);

	/**
	 * Set left child.
	 *
	 * @param left
	 *            child pointer
	 */
	void setLeftChildKey(long left);

	/**
	 * Set right child.
	 *
	 * @param right
	 *            child pointer
	 */
	void setRightChildKey(long right);

	/**
	 * Flag which determines if node is changed.
	 *
	 * @param changed
	 *            flag which indicates if node is changed or not
	 */
	void setChanged(boolean changed);
}
