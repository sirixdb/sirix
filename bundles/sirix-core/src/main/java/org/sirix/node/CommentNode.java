package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.immutable.ImmutableComment;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.node.interfaces.IValNode;
import org.sirix.settings.EFixed;

import com.google.common.base.Objects;

/**
 * Comment node implementation.
 * 
 * @author Johannes Lichtenberger
 *
 */
public class CommentNode extends AbsStructForwardingNode implements IValNode {

	/** {@link StructNodeDelegate} reference. */
	private final StructNodeDelegate mStructNodeDel;
	
	/** {@link ValNodeDelegate} reference. */
	private final ValNodeDelegate mValDel;

	/** Value of the node. */
	private byte[] mValue;

	/**
	 * Constructor for TextNode.
	 * 
	 * @param pDel
	 *          delegate for {@link INode} implementation
	 * @param pValDel
	 *          delegate for {@link IValNode} implementation
	 * @param pStructDel
	 *          delegate for {@link IStructNode} implementation
	 */
	public CommentNode(final @Nonnull ValNodeDelegate pValDel,
			final @Nonnull StructNodeDelegate pStructDel) {
		mStructNodeDel = checkNotNull(pStructDel);
		mValDel = checkNotNull(pValDel);
	}

	@Override
	public EKind getKind() {
		return EKind.COMMENT;
	}

	@Override
	public byte[] getRawValue() {
		if (mValue == null) {
			mValue = mValDel.getRawValue();
		}
		return mValue;
	}

	@Override
	public void setValue(final @Nonnull byte[] pVal) {
		mValue = null;
		mValDel.setValue(pVal);
	}

	@Override
	public long getFirstChildKey() {
		return EFixed.NULL_NODE_KEY.getStandardProperty();
	}

	@Override
	public EVisitResult acceptVisitor(final @Nonnull IVisitor pVisitor) {
		return pVisitor.visit(ImmutableComment.of(this));
	}

	@Override
	public void decrementChildCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void incrementChildCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getDescendantCount() {
		return 0;
	}

	@Override
	public void decrementDescendantCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void incrementDescendantCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDescendantCount(final long pDescendantCount) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mStructNodeDel.getNodeDelegate(), mValDel);
	}

	@Override
	public boolean equals(final @Nullable Object pObj) {
		if (pObj instanceof CommentNode) {
			final CommentNode other = (CommentNode) pObj;
			return Objects.equal(mStructNodeDel.getNodeDelegate(),
					other.getNodeDelegate())
					&& mValDel.equals(other.mValDel);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("node delegate", mStructNodeDel.getNodeDelegate())
				.add("value delegate", mValDel).toString();
	}
	
	public ValNodeDelegate getValNodeDelegate() {
		return mValDel;
	}

	@Override
	protected NodeDelegate delegate() {
		return mStructNodeDel.getNodeDelegate();
	}

	@Override
	protected StructNodeDelegate structDelegate() {
		return mStructNodeDel;
	}

}
