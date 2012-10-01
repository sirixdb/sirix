package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.settings.EFixed;

/**
 * Immutable document root node wrapper.
 * 
 * @author Johannes Lichtenberger
 */
public class ImmutableDocument implements IStructNode {

	/** Mutable {@link DocumentRootNode} instance. */
	private final DocumentRootNode mNode;

	/**
	 * Private constructor.
	 * 
	 * @param pNode
	 *          mutable {@link DocumentRootNode}
	 */
	private ImmutableDocument(final @Nonnull DocumentRootNode pNode) {
		mNode = checkNotNull(pNode);
	}

	/**
	 * Get an immutable document root node instance.
	 * 
	 * @param pNode
	 *          the mutable {@link DocumentRootNode} to wrap
	 * @return immutable document root node instance
	 */
	public static ImmutableDocument of(final @Nonnull DocumentRootNode pNode) {
		return new ImmutableDocument(pNode);
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
		return false;
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
		return false;
	}

	@Override
	public boolean hasRightSibling() {
		return false;
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
		return EFixed.NULL_NODE_KEY.getStandardProperty();
	}

	@Override
	public long getRightSiblingKey() {
		return EFixed.NULL_NODE_KEY.getStandardProperty();
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
}
