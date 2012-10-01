package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.IVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.CommentNode;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.node.interfaces.IValNode;

/**
 * Immutable comment node wrapper.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class ImmutableComment implements IValNode, IStructNode {

	/** Mutable {@link CommentNode}. */
	private final CommentNode mNode;

	/**
	 * Constructor.
	 * 
	 * @param pNode
	 *          mutable {@link CommentNode}
	 */
	private ImmutableComment(final @Nonnull CommentNode pNode) {
		mNode = checkNotNull(pNode);
	}

	/**
	 * Get an immutable comment node instance.
	 * 
	 * @param pNode
	 *          the mutable {@link CommentNode} to wrap
	 * @return immutable comment node instance
	 */
	public static ImmutableComment of(final @Nonnull CommentNode pNode) {
		return new ImmutableComment(pNode);
	}

	@Override
	public int getTypeKey() {
		return mNode.getTypeKey();
	}

	@Override
	public void setTypeKey(final int pTypeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isSameItem(final @Nullable INode pOther) {
		return mNode.isSameItem(pOther);
	}

	@Override
	public IVisitResult acceptVisitor(final @Nonnull IVisitor pVisitor) {
		return pVisitor.visit(this);
	}

	@Override
	public void setHash(final long pHash) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getHash() {
		return mNode.getHash();
	}

	@Override
	public void setParentKey(final long pNodeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getParentKey() {
		return mNode.getParentKey();
	}

	@Override
	public boolean hasParent() {
		return mNode.hasParent();
	}

	@Override
	public long getNodeKey() {
		return mNode.getNodeKey();
	}

	@Override
	public EKind getKind() {
		return mNode.getKind();
	}

	@Override
	public long getRevision() {
		return mNode.getRevision();
	}

	@Override
	public byte[] getRawValue() {
		return mNode.getRawValue();
	}

	@Override
	public void setValue(final @Nonnull byte[] pValue) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasFirstChild() {
		return mNode.hasFirstChild();
	}

	@Override
	public boolean hasLeftSibling() {
		return mNode.hasLeftSibling();
	}

	@Override
	public boolean hasRightSibling() {
		return mNode.hasRightSibling();
	}

	@Override
	public long getChildCount() {
		return mNode.getChildCount();
	}

	@Override
	public long getDescendantCount() {
		return mNode.getDescendantCount();
	}

	@Override
	public long getFirstChildKey() {
		return mNode.getFirstChildKey();
	}

	@Override
	public long getLeftSiblingKey() {
		return mNode.getLeftSiblingKey();
	}

	@Override
	public long getRightSiblingKey() {
		return mNode.getRightSiblingKey();
	}

	@Override
	public void setRightSiblingKey(final long pNodeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setLeftSiblingKey(final long pNodeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setFirstChildKey(final long pNodeKey) {
		throw new UnsupportedOperationException();
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
	public void decrementDescendantCount() {
		throw new UnsupportedOperationException();

	}

	@Override
	public void incrementDescendantCount() {
		throw new UnsupportedOperationException();

	}

	@Override
	public void setDescendantCount(final @Nonnegative long pDescendantCount) {
		throw new UnsupportedOperationException();
	}
}
