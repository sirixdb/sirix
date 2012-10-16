package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.Kind;
import org.sirix.node.NamespaceNode;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;

/**
 * Immutable namespace node wrapper.
 * 
 * @author Johannes Lichtenberger
 *
 */
public class ImmutableNamespace implements NameNode {

	/** Mutable {@link NamespaceNode}. */
	private final NamespaceNode mNode;

	/**
	 * Private constructor.
	 * 
	 * @param node
	 *          {@link NamespaceNode} to wrap
	 */
	private ImmutableNamespace(final @Nonnull NamespaceNode node) {
		mNode = checkNotNull(node);
	}
	
	/**
	 * Get an immutable namespace node instance.
	 * 
	 * @param node
	 *          the mutable {@link NamespaceNode} to wrap
	 * @return immutable namespace node instance
	 */
	public static ImmutableNamespace of(final @Nonnull NamespaceNode node) {
		return new ImmutableNamespace(node);
	}

	@Override
	public int getTypeKey() {
		return mNode.getTypeKey();
	}

	@Override
	public void setTypeKey(int typeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isSameItem(final @Nullable Node pOther) {
		return mNode.isSameItem(pOther);
	}

	@Override
	public VisitResult acceptVisitor(final @Nonnull Visitor pVisitor) {
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
	public void setParentKey(long nodeKey) {
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
	public Kind getKind() {
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
	public void setNameKey(int nameKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setURIKey(int uriKey) {
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
