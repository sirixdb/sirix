package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.TextNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.node.interfaces.IValNode;

/**
 * Immutable text wrapper.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class ImmutableText implements IValNode, IStructNode {
	/** Mutable {@link TextNode}. */
	private final TextNode mNode;

	/**
	 * Private constructor.
	 * 
	 * @param pNode
	 *          {@link TextNode} to wrap
	 */
	private ImmutableText(final @Nonnull TextNode pNode) {
		mNode = checkNotNull(pNode);
	}

	/**
	 * Get an immutable text node instance.
	 * 
	 * @param pNode
	 *          the mutable {@link TextNode} to wrap
	 * @return immutable text node instance
	 */
	public static ImmutableText of(final @Nonnull TextNode pNode) {
		return new ImmutableText(pNode);
	}

	@Override
	public int getTypeKey() {
		return mNode.getTypeKey();
	}

	@Override
	public void setTypeKey(int pTypeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isSameItem(@Nullable INode pOther) {
		return mNode.isSameItem(pOther);
	}

	@Override
	public EVisitResult acceptVisitor(@Nonnull IVisitor pVisitor) {
		return pVisitor.visit(this);
	}

	@Override
	public void setHash(long pHash) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getHash() {
		return mNode.getHash();
	}

	@Override
	public void setParentKey(long pNodeKey) {
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
	public void setRightSiblingKey(long pNodeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setLeftSiblingKey(long pNodeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setFirstChildKey(long pNodeKey) {
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
	public void setDescendantCount(long pDescendantCount) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getRawValue() {
		return mNode.getRawValue();
	}

	@Override
	public void setValue(@Nonnull byte[] pValue) {
		throw new UnsupportedOperationException();
	}
}
