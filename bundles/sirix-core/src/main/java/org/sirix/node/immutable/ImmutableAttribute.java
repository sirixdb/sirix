package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.AttributeNode;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.ValNode;

/**
 * Immutable attribute node instance.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class ImmutableAttribute implements ValNode, NameNode {

	/** Mutable {@link AttributeNode}. */
	private final AttributeNode mNode;

	/**
	 * Private constructor.
	 * 
	 * @param pNode
	 *          mutable {@link AttributeNode}
	 */
	private ImmutableAttribute(final @Nonnull AttributeNode pNode) {
		mNode = checkNotNull(pNode);
	}

	/**
	 * Get an immutable attribute node.
	 * 
	 * @param pNode
	 *          the {@link AttributeNode} which should be immutable
	 * @return an immutable instance
	 */
	public static ImmutableAttribute of(final @Nonnull AttributeNode pNode) {
		return new ImmutableAttribute(pNode);
	}

	@Override
	public int getTypeKey() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTypeKey(final int pTypeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isSameItem(final @Nullable Node pOther) {
		return mNode.isSameItem(pOther);
	}

	@Override
	public VisitResult acceptVisitor(@Nonnull IVisitor pVisitor) {
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
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] getRawValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setValue(@Nonnull byte[] pValue) {
		throw new UnsupportedOperationException();
	}

}
