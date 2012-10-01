package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.EKind;
import org.sirix.node.NamespaceNode;
import org.sirix.node.PINode;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;

/**
 * Immutable namespace node wrapper.
 * 
 * @author Johannes Lichtenberger
 *
 */
public class ImmutableNamespace implements INameNode {

	/** Mutable {@link NamespaceNode}. */
	private final NamespaceNode mNode;

	/**
	 * Private constructor.
	 * 
	 * @param pNode
	 *          {@link NamespaceNode} to wrap
	 */
	private ImmutableNamespace(final @Nonnull NamespaceNode pNode) {
		mNode = checkNotNull(pNode);
	}
	
	/**
	 * Get an immutable namespace node instance.
	 * 
	 * @param pNode
	 *          the mutable {@link NamespaceNode} to wrap
	 * @return immutable namespace node instance
	 */
	public static ImmutableNamespace of(final @Nonnull NamespaceNode pNode) {
		return new ImmutableNamespace(pNode);
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

}
