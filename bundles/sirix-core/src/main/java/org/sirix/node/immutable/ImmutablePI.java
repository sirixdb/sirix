package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.IVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.EKind;
import org.sirix.node.PINode;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.node.interfaces.IValNode;

/**
 * Immutable processing instruction wrapper.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class ImmutablePI implements IValNode, INameNode, IStructNode {
	/** Mutable {@link PINode}. */
	private final PINode mNode;

	/**
	 * Private constructor.
	 * 
	 * @param pNode
	 *          {@link PINode} to wrap
	 */
	private ImmutablePI(final @Nonnull PINode pNode) {
		mNode = checkNotNull(pNode);
	}

	/**
	 * Get an immutable processing instruction node instance.
	 * 
	 * @param pNode
	 *          the mutable {@link PINode} to wrap
	 * @return immutable processing instruction node instance
	 */
	public static ImmutablePI of(final @Nonnull PINode pNode) {
		return new ImmutablePI(pNode);
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
	public IVisitResult acceptVisitor(@Nonnull IVisitor pVisitor) {
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
	public int getNameKey() {
		return mNode.getNameKey();
	}

	@Override
	public int getURIKey() {
		return mNode.getURIKey();
	}

	@Override
	public void setNameKey(int pNameKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setURIKey(int pUriKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setPathNodeKey(long nodeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getPathNodeKey() {
		return mNode.getPathNodeKey();
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
