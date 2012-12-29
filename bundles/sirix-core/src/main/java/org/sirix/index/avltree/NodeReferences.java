package org.sirix.index.avltree;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.index.avltree.interfaces.References;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

/**
 * Text node-ID references.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class NodeReferences implements References {
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
	public NodeReferences(final @Nonnull Set<Long> nodeKeys) {
		mNodeKeys = Collections.synchronizedSet(checkNotNull(nodeKeys));
	}

	@Override
	public boolean isPresent(final @Nonnegative long nodeKey) {
		return mNodeKeys.contains(nodeKey);
	}

	@Override
	public Set<Long> getNodeKeys() {
		return Collections.unmodifiableSet(mNodeKeys);
	}

	@Override
	public void setNodeKey(final @Nonnegative long nodeKey) {
		mNodeKeys.add(nodeKey);
	}
	
	@Override
	public boolean removeNodeKey(@Nonnegative long nodeKey) {
		return mNodeKeys.remove(nodeKey);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mNodeKeys);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof NodeReferences) {
			final NodeReferences refs = (NodeReferences) obj;
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

	@Override
	public boolean hasNodeKeys() {
		return !mNodeKeys.isEmpty();
	}
}
