package org.sirix.index;

import java.util.Set;

import javax.annotation.Nonnull;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.visitor.Visitor;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.NonStructuralWrapperAxis;

public final class IndexBuilder {

	public static void build(final NodeReadTrx rtx, final Set<Visitor> builders) {
		final long nodeKey = rtx.getNodeKey();
		rtx.moveToDocumentRoot();

		for (@SuppressWarnings("unused")
		final long key : new NonStructuralWrapperAxis(new DescendantAxis(rtx))) {
			for (final @Nonnull
			Visitor builder : builders) {
				rtx.acceptVisitor(builder);
			}
		}
		rtx.moveTo(nodeKey);
	}

}
