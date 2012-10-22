package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.ElementNode;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;

import com.google.common.base.Optional;

/**
 * Immutable element wrapper.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class ImmutableElement implements ImmutableNameNode, ImmutableStructNode {

	/** Mutable {@link ElementNode}. */
	private final ElementNode mNode;

	/**
	 * Private constructor.
	 * 
	 * @param pNode
	 *          mutable {@link ElementNode}
	 */
	private ImmutableElement(final @Nonnull ElementNode pNode) {
		mNode = checkNotNull(pNode);
	}

	/**
	 * Get an immutable element node instance.
	 * 
	 * @param pNode
	 *          the mutable {@link ElementNode} to wrap
	 * @return immutable element instance
	 */
	public static ImmutableElement of(final @Nonnull ElementNode pNode) {
		return new ImmutableElement(pNode);
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
	public int getTypeKey() {
		return mNode.getTypeKey();
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
	public int getNameKey() {
		return mNode.getNameKey();
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
	public Optional<SirixDeweyID> getDeweyID() {
		return mNode.getDeweyID();
	}
}
