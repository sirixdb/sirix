package org.sirix.node;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.Node;

import com.google.common.collect.ForwardingObject;

/**
 * Skeletal implementation of {@link Node} interface.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractForwardingNode extends ForwardingObject implements
		Node {

	/** Constructor for use by subclasses. */
	protected AbstractForwardingNode() {
	}

	@Override
	protected abstract NodeDelegate delegate();

	/**
	 * Get a snapshot of the node delegate.
	 * 
	 * @return new {@link NodeDelegate} instance (snapshot of the current one)
	 */
	@Nonnull
	public NodeDelegate getNodeDelegate() {
		return delegate();
	}

	@Override
	public int getTypeKey() {
		return delegate().getTypeKey();
	}

	@Override
	public void setTypeKey(final int typeKey) {
		delegate().setTypeKey(typeKey);
	}

	@Override
	public boolean hasParent() {
		return delegate().hasParent();
	}

	@Override
	public long getNodeKey() {
		return delegate().getNodeKey();
	}

	@Override
	public long getParentKey() {
		return delegate().getParentKey();
	}

	@Override
	public void setParentKey(final long parentKey) {
		delegate().setParentKey(parentKey);
	}

	@Override
	public long getHash() {
		return delegate().getHash();
	}

	@Override
	public void setHash(final long hash) {
		delegate().setHash(hash);
	}

	@Override
	public long getRevision() {
		return delegate().getRevision();
	}

	@Override
	public String toString() {
		return delegate().toString();
	}

	@Override
	public boolean isSameItem(final @Nullable Node other) {
		return delegate().isSameItem(other);
	}

	@Override
	public void setDeweyID(Optional<SirixDeweyID> id) {
		delegate().setDeweyID(id);
	}

	@Override
	public Optional<SirixDeweyID> getDeweyID() {
		return delegate().getDeweyID();
	}
}
