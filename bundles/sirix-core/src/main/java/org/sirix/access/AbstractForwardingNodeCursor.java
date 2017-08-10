package org.sirix.access;

import org.sirix.api.NodeCursor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import com.google.common.collect.ForwardingObject;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractForwardingNodeCursor extends ForwardingObject implements NodeCursor {

	/** Constructor for use by subclasses. */
	protected AbstractForwardingNodeCursor() {}

	@Override
	protected abstract NodeCursor delegate();

	@Override
	public VisitResult acceptVisitor(Visitor visitor) {
		return delegate().acceptVisitor(visitor);
	}

	@Override
	public Kind getFirstChildKind() {
		return delegate().getFirstChildKind();
	};

	@Override
	public Kind getKind() {
		return delegate().getKind();
	}

	@Override
	public Kind getLastChildKind() {
		return delegate().getLastChildKind();
	}

	@Override
	public Kind getLeftSiblingKind() {
		return delegate().getLeftSiblingKind();
	}

	@Override
	public Kind getRightSiblingKind() {
		return delegate().getRightSiblingKind();
	}

	@Override
	public ImmutableNode getNode() {
		return delegate().getNode();
	}

	@Override
	public long getNodeKey() {
		return delegate().getNodeKey();
	}

	@Override
	public Kind getParentKind() {
		return delegate().getParentKind();
	}

	@Override
	public boolean hasFirstChild() {
		return delegate().hasFirstChild();
	}

	@Override
	public boolean hasLastChild() {
		return delegate().hasLastChild();
	}

	@Override
	public boolean hasLeftSibling() {
		return delegate().hasLeftSibling();
	}

	@Override
	public boolean hasNode(long key) {
		return delegate().hasNode(key);
	}

	@Override
	public boolean hasParent() {
		return delegate().hasParent();
	}

	@Override
	public boolean hasRightSibling() {
		return delegate().hasRightSibling();
	}

	@Override
	public Move<? extends NodeCursor> moveTo(long key) {
		return delegate().moveTo(key);
	}

	@Override
	public Move<? extends NodeCursor> moveToDocumentRoot() {
		return delegate().moveToDocumentRoot();
	}

	@Override
	public Move<? extends NodeCursor> moveToFirstChild() {
		return delegate().moveToFirstChild();
	}

	@Override
	public Move<? extends NodeCursor> moveToLastChild() {
		return delegate().moveToLastChild();
	}

	@Override
	public Move<? extends NodeCursor> moveToLeftSibling() {
		return delegate().moveToLeftSibling();
	}

	@Override
	public Move<? extends NodeCursor> moveToNext() {
		return delegate().moveToNext();
	}

	@Override
	public Move<? extends NodeCursor> moveToParent() {
		return delegate().moveToParent();
	}

	@Override
	public Move<? extends NodeCursor> moveToPrevious() {
		return delegate().moveToParent();
	}

	@Override
	public Move<? extends NodeCursor> moveToRightSibling() {
		return delegate().moveToRightSibling();
	}
}
