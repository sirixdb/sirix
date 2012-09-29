package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;

public class PINode extends AbsStructForwardingNode {

	private final StructNodeDelegate mStructDel;

	public PINode(@Nonnull final StructNodeDelegate pStructDel,
	    @Nonnull final NameNodeDelegate pNameDel,
	    @Nonnull final ValNodeDelegate pValDel) {
		mStructDel = checkNotNull(pStructDel);
	}
	
	@Override
	public EVisitResult acceptVisitor(final @Nonnull IVisitor pVisitor) {
		return pVisitor.visit(this);
	}

	@Override
	public EKind getKind() {
		return EKind.PROCESSING;
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
