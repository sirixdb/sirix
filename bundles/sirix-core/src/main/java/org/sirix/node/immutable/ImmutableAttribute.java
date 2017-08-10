package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;

import javax.annotation.Nullable;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.AttributeNode;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.settings.Constants;

/**
 * Immutable attribute node wrapper.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class ImmutableAttribute implements ImmutableValueNode, ImmutableNameNode {

	/** Mutable {@link AttributeNode}. */
	private final AttributeNode mNode;

	/**
	 * Private constructor.
	 * 
	 * @param node mutable {@link AttributeNode}
	 */
	private ImmutableAttribute(final AttributeNode node) {
		mNode = checkNotNull(node);
	}

	/**
	 * Get an immutable attribute node.
	 * 
	 * @param node the {@link AttributeNode} which should be immutable
	 * @return an immutable instance
	 */
	public static ImmutableAttribute of(final AttributeNode node) {
		return new ImmutableAttribute(node);
	}

	@Override
	public int getTypeKey() {
		return mNode.getTypeKey();
	}

	@Override
	public boolean isSameItem(final @Nullable Node other) {
		return mNode.isSameItem(other);
	}

	@Override
	public VisitResult acceptVisitor(final Visitor visitor) {
		return visitor.visit(this);
	}

	@Override
	public long getHash() {
		return mNode.getHash();
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
	public int getLocalNameKey() {
		return mNode.getLocalNameKey();
	}

	@Override
	public int getPrefixKey() {
		return mNode.getPrefixKey();
	}

	@Override
	public int getURIKey() {
		return mNode.getURIKey();
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
	public Optional<SirixDeweyID> getDeweyID() {
		return mNode.getDeweyID();
	}

	@Override
	public boolean equals(Object obj) {
		return mNode.equals(obj);
	}

	@Override
	public int hashCode() {
		return mNode.hashCode();
	}

	@Override
	public String toString() {
		return mNode.toString();
	}

	@Override
	public QNm getName() {
		return mNode.getName();
	}

	@Override
	public String getValue() {
		return new String(((ValueNode) mNode).getRawValue(), Constants.DEFAULT_ENCODING);
	}
}
