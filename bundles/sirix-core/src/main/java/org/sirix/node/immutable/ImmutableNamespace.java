package org.sirix.node.immutable;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.Kind;
import org.sirix.node.NamespaceNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;

import com.google.common.base.Optional;

/**
 * Immutable namespace node wrapper.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class ImmutableNamespace implements ImmutableNameNode {

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
	public boolean isSameItem(final @Nullable Node pOther) {
		return mNode.isSameItem(pOther);
	}

	@Override
	public VisitResult acceptVisitor(final @Nonnull Visitor pVisitor) {
		return pVisitor.visit(this);
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
}
