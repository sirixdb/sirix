package org.sirix.node;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.node.interfaces.INodeBase;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

/**
 * Text node-ID references.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class TextReferences implements INodeBase {
	/** A {@link Set} of node-keys. */
	private final Set<Long> mNodeKeys;

	/** Node-ID. */
	private final long mNodeKey;

	/**
	 * Constructor.
	 * 
	 * @param pNodeKeys
	 *          {@link Set} of unique node-keys
	 * @param pNodeKey
	 *          node key of this node
	 */
	public TextReferences(final @Nonnull Set<Long> pNodeKeys, final long pNodeKey) {
		mNodeKeys = Collections.synchronizedSet(checkNotNull(pNodeKeys));
		checkArgument(pNodeKey >= 0, "pNodeKey must be >= 0!");
		mNodeKey = pNodeKey;
	}

	/**
	 * Retrieve if a node-ID is present with the given key.
	 * 
	 * @param pNodeKey
	 *          node key to lookup
	 * @return {@code true} if it is indexed, {@code false} otherwise
	 */
	public boolean getNodeKey(final @Nonnegative long pNodeKey) {
		return mNodeKeys.contains(pNodeKey);
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
	 * @param pNodeKey
	 *          node key to set
	 */
	public void setNodeKey(final @Nonnegative long pNodeKey) {
		mNodeKeys.add(pNodeKey);
	}

	@Override
	public long getNodeKey() {
		return mNodeKey;
	}

	@Override
	public EKind getKind() {
		return EKind.TEXT_REFERENCES;
	}

	@Override
	public String toString() {
		final ToStringHelper helper = Objects.toStringHelper(this).add(
				"this nodeKey", mNodeKey);
		for (final long nodeKey : mNodeKeys) {
			helper.add("referenced node key", nodeKey);
		}
		return helper.toString();
	}
}
