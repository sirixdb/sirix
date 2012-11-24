package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

/**
 * Text node-ID references.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class TextReferences {
	/** A {@link Set} of node-keys. */
	private final Set<Long> mNodeKeys;

	/**
	 * Constructor.
	 * 
	 * @param nodeKeys
	 *          {@link Set} of unique node-keys
	 * @param nodeKey
	 *          node key of this node
	 */
	public TextReferences(final @Nonnull Set<Long> nodeKeys) {
		mNodeKeys = Collections.synchronizedSet(checkNotNull(nodeKeys));
	}

	/**
	 * Retrieve if a node-ID is present with the given key.
	 * 
	 * @param nodeKey
	 *          node key to lookup
	 * @return {@code true} if it is indexed, {@code false} otherwise
	 */
	public boolean isPresent(final @Nonnegative long nodeKey) {
		return mNodeKeys.contains(nodeKey);
	}

	/**
	 * Get an unmodifiable set view.
	 * 
	 * @return set of all keys
	 */
	public Set<Long> getNodeKeys() {
		return Collections.unmodifiableSet(mNodeKeys);
	}

	/**
	 * Set a new nodeKey.
	 * 
	 * @param nodeKey
	 *          node key to set
	 */
	public void setNodeKey(final @Nonnegative long nodeKey) {
		mNodeKeys.add(nodeKey);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mNodeKeys);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof TextReferences) {
			final TextReferences refs = (TextReferences) obj;
			return mNodeKeys.equals(refs.mNodeKeys);
		}
		return false;
	}
	
	@Override
	public String toString() {
		final ToStringHelper helper = Objects.toStringHelper(this);
		for (final long nodeKey : mNodeKeys) {
			helper.add("referenced node key", nodeKey);
		}
		return helper.toString();
	}
}
