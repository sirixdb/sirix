package org.sirix.index.path.summary;

import javax.annotation.Nullable;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;

import com.google.common.base.Optional;

/**
 * Wraps a {@link PathNode} to provide immutability.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class ImmutablePathNode implements ImmutableNameNode,
		ImmutableStructNode {

	/** {@link PathNode} instance. */
	private final PathNode mNode;

	/**
	 * Private constructor.
	 * 
	 * @param node
	 *          the mutable path node
	 */
	public ImmutablePathNode(final PathNode node) {
		mNode = node;
	}
	
	/**
	 * Get an immutable path node instance.
	 * 
	 * @param node
	 *          the mutable {@link PathNode} to wrap
	 * @return immutable path node instance
	 */
	public static ImmutableNode of(final PathNode node) {
		return new ImmutablePathNode(node);
	}

	@Override
	public Kind getKind() {
		return mNode.getKind();
	}

	@Override
	public Optional<SirixDeweyID> getDeweyID() {
		return mNode.getDeweyID();
	}

	@Override
	public int getTypeKey() {
		return mNode.getTypeKey();
	}

	@Override
	public boolean isSameItem(@Nullable Node other) {
		return mNode.isSameItem(other);
	}

	@Override
	public VisitResult acceptVisitor(Visitor visitor) {
		return mNode.acceptVisitor(visitor);
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
	public QNm getName() {
		return mNode.getName();
	}
}
