package org.sirix.index;

import java.util.Set;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.visitor.Visitor;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.NonStructuralWrapperAxis;

/**
 * Build an index by traversing the current revision.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class IndexBuilder {

	/**
	 * Build the index.
	 * 
	 * @param rtx
	 *          the current {@link NodeReadTrx}
	 * @param builders
	 *          the index builders
	 */
	public static void build(final NodeReadTrx rtx, final Set<Visitor> builders) {
		final long nodeKey = rtx.getNodeKey();
		rtx.moveToDocumentRoot();

		for (@SuppressWarnings("unused")
		final long key : new NonStructuralWrapperAxis(new DescendantAxis(rtx))) {
			for (final Visitor builder : builders) {
				rtx.acceptVisitor(builder);
			}
		}
		rtx.moveTo(nodeKey);
	}

}
