package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;

public class CommentNode extends AbsStructForwardingNode {

	private final StructNodeDelegate mStructDel;
	private final ValNodeDelegate mValDel;

	public CommentNode(final @Nonnull ValNodeDelegate pValDel,
			final @Nonnull StructNodeDelegate pStructDel) {
		mStructDel = checkNotNull(pStructDel);
		mValDel = checkNotNull(pValDel);
	}

	@Override
	public EVisitResult acceptVisitor(@Nonnull IVisitor pVisitor) {
		return pVisitor.visit(this);
	}

	@Override
	public EKind getKind() {
		return EKind.COMMENT;
	}

	@Override
	protected StructNodeDelegate structDelegate() {
		return mStructDel;
	}

	@Override
	protected NodeDelegate delegate() {
		return mStructDel.getNodeDelegate();
	}

}
