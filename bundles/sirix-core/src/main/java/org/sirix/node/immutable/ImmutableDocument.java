package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;
import org.sirix.settings.Fixed;

import com.google.common.base.Optional;

/**
 * Immutable document root node wrapper.
 * 
 * @author Johannes Lichtenberger
 */
public class ImmutableDocument implements ImmutableStructNode {

	/** Mutable {@link DocumentRootNode} instance. */
	private final DocumentRootNode mNode;

	/**
	 * Private constructor.
	 * 
	 * @param node
	 *          mutable {@link DocumentRootNode}
	 */
	private ImmutableDocument(final @Nonnull DocumentRootNode node) {
		mNode = checkNotNull(node);
	}

	/**
	 * Get an immutable document root node instance.
	 * 
	 * @param node
	 *          the mutable {@link DocumentRootNode} to wrap
	 * @return immutable document root node instance
	 */
	public static ImmutableDocument of(final @Nonnull DocumentRootNode node) {
		return new ImmutableDocument(node);
	}

	@Override
	public int getTypeKey() {
		return mNode.getTypeKey();
	}

	@Override
	public boolean isSameItem(Node other) {
		return false;
	}

	@Override
	public VisitResult acceptVisitor(final @Nonnull Visitor visitor) {
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
		return Fixed.NULL_NODE_KEY.getStandardProperty();
	}

	@Override
	public long getRightSiblingKey() {
		return Fixed.NULL_NODE_KEY.getStandardProperty();
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
}
