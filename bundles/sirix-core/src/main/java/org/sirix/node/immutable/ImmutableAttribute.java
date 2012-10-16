package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
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
	 * @param node
	 *          mutable {@link AttributeNode}
	 */
	private ImmutableAttribute(final @Nonnull AttributeNode node) {
		mNode = checkNotNull(node);
	}

	/**
	 * Get an immutable attribute node.
	 * 
	 * @param node
	 *          the {@link AttributeNode} which should be immutable
	 * @return an immutable instance
	 */
	public static ImmutableAttribute of(final @Nonnull AttributeNode node) {
		return new ImmutableAttribute(node);
	}

	@Override
	public int getTypeKey() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTypeKey(final int typeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isSameItem(final @Nullable Node other) {
		return mNode.isSameItem(other);
	}

	@Override
	public VisitResult acceptVisitor(final @Nonnull Visitor visitor) {
		return visitor.visit(this);
	}

	@Override
	public void setHash(long hash) {
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

	@Override
	public byte[] getRawValue() {
		return mNode.getRawValue();
	}

	@Override
	public void setValue(byte[] value) {
		throw new UnsupportedOperationException();
	}

}
