package org.sirix.index.value.interfaces;

import java.util.Set;

import javax.annotation.Nonnegative;

public interface References {

	/**
	 * Get an unmodifiable set view.
	 * 
	 * @return set of all keys
	 */
	Set<Long> getNodeKeys();

	/**
	 * Remove a nodeKey.
	 * 
	 * @param nodeKey
	 *          the node key to remove
	 * @return {@code true}, if the node key is removed, {@code false} if it isn't
	 *         present
	 * @throws IllegalArgumentException
	 *           if {@code nodeKey} < {@code 0}
	 */
	boolean removeNodeKey(@Nonnegative long nodeKey);

	/**
	 * Set a new nodeKey.
	 * 
	 * @param nodeKey
	 *          node key to set
	 * @throws IllegalArgumentException
	 *           if {@code nodeKey} < {@code 0}
	 */
	void setNodeKey(@Nonnegative long nodeKey);

	/**
	 * Retrieve if a node-ID is present with the given key.
	 * 
	 * @param nodeKey
	 *          node key to lookup
	 * @return {@code true} if it is indexed, {@code false} otherwise
	 * @throws IllegalArgumentException
	 *           if {@code nodeKey} < {@code 0}
	 */
	boolean isPresent(@Nonnegative long nodeKey);
	
	/**
	 * Determines if nodeKeys are stored or not.
	 * 
	 * @return {@code true}, if node keys are stored, {@code false} otherwise
	 */
	boolean hasNodeKeys();
}
