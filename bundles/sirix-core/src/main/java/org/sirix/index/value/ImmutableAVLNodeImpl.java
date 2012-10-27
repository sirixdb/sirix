package org.sirix.index.value;

import javax.annotation.Nonnull;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.index.value.interfaces.ImmutableAVLNode;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.Kind;
import org.sirix.node.delegates.NodeDelegate;

/**
 * Immutable AVLNode.
 * 
 * @author Johannes Lichtenberger
 * 
 * @param <K>
 *          key which has to be comparable (implement Comparable interface)
 * @param <V>
 *          value
 */
public final class ImmutableAVLNodeImpl<K extends Comparable<? super K>, V>
		extends AbstractForwardingNode implements ImmutableAVLNode<K, V> {

	/** {@link AVLNode} to wrap. */
	private final AVLNode<K, V> mNode;

	/**
	 * Constructor.
	 * 
	 * @param node
	 *          {@link AVLNode} to wrap.
	 */
	public ImmutableAVLNodeImpl(final @Nonnull AVLNode<K, V> node) {
		assert node != null;
		mNode = node;
	}

	@Override
	public Kind getKind() {
		return mNode.getKind();
	}

	@Override
	public VisitResult acceptVisitor(@Nonnull Visitor visitor) {
		return mNode.acceptVisitor(visitor);
	}

	@Override
	public K getKey() {
		return mNode.getKey();
	}

	@Override
	public V getValue() {
		return mNode.getValue();
	}

	@Override
	public boolean isChanged() {
		return mNode.isChanged();
	}

	@Override
	public boolean hasLeftChild() {
		return mNode.hasLeftChild();
	}

	@Override
	public boolean hasRightChild() {
		return mNode.hasRightChild();
	}

	@Override
	public long getLeftChildKey() {
		return mNode.getLeftChildKey();
	}

	@Override
	public long getRightChildKey() {
		return mNode.getRightChildKey();
	}

	@Override
	protected NodeDelegate delegate() {
		return mNode.delegate();
	}
}
