package org.sirix.index.value;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;

import javax.annotation.Nonnull;

import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.Visitor;
import org.sirix.index.value.interfaces.MutableAVLNode;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.Kind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.settings.Fixed;

/**
 * AVLNode.
 */
public class AVLNode<K extends Comparable<? super K>, V> extends
		AbstractForwardingNode implements MutableAVLNode<K, V> {
	/** Key token. */
	private K mKey;

	/** Value. */
	private V mValue;

	/** Reference to the left node. */
	private long mLeft;

	/** Reference to the right node. */
	private long mRight;

	/** 'changed' status of tree node. */
	private boolean mChanged;

	/** {@link NodeDelegate} reference. */
	private NodeDelegate mNodeDelegate;

	/**
	 * Constructor.
	 * 
	 * @param pToken
	 *          token
	 * @param pParent
	 *          id of the parent node
	 */
	public AVLNode(final @Nonnull K key, final @Nonnull V value,
			final @Nonnull NodeDelegate delegate) {
		mKey = checkNotNull(key);
		mValue = checkNotNull(value);
		mNodeDelegate = checkNotNull(delegate);
	}

	@Override
	public Kind getKind() {
		return Kind.AVL;
	}

	@Override
	protected NodeDelegate delegate() {
		return mNodeDelegate;
	}

	@Override
	public K getKey() {
		return mKey;
	}

	@Override
	public V getValue() {
		return mValue;
	}

	/**
	 * Flag which determines if node is changed.
	 * 
	 * @return {@code true} if it has been changed in memory, {@code false}
	 *         otherwise
	 */
	public boolean isChanged() {
		return mChanged;
	}

	@Override
	public void setChanged(final boolean changed) {
		mChanged = changed;
	}

	@Override
	public boolean hasLeftChild() {
		return mLeft != Fixed.NULL_NODE_KEY.getStandardProperty();
	}

	@Override
	public boolean hasRightChild() {
		return mRight != Fixed.NULL_NODE_KEY.getStandardProperty();
	}

	@Override
	public long getLeftChildKey() {
		return mLeft;
	}

	@Override
	public long getRightChildKey() {
		return mRight;
	}

	@Override
	public void setLeftChildKey(final long left) {
		mLeft = left;
	}

	@Override
	public void setRightChildKey(final long right) {
		mLeft = right;
	}

	@Override
	public VisitResultType acceptVisitor(final @Nonnull Visitor visitor) {
		return VisitResultType.CONTINUE;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("node delegate", mNodeDelegate)
				.add("left child", mLeft).add("right child", mRight)
				.add("changed", mChanged).add("key", mKey).add("value", mValue)
				.toString();
	}
}
