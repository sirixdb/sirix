package io.sirix.index;

import io.sirix.axis.DescendantAxis;
import io.sirix.axis.NonStructuralWrapperAxis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;

import java.util.Set;

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
	 *            the current {@link XmlNodeReadOnlyTrx}
	 * @param builders
	 *            the index builders
	 */
	public static void build(final XmlNodeReadOnlyTrx rtx, final Set<XmlNodeVisitor> builders) {
		final long nodeKey = rtx.getNodeKey();
		rtx.moveToDocumentRoot();

		var axis = new NonStructuralWrapperAxis(new DescendantAxis(rtx));
		while (axis.hasNext()) {
			axis.nextLong();

			for (final XmlNodeVisitor builder : builders) {
				rtx.acceptVisitor(builder);
			}
		}
		rtx.moveTo(nodeKey);
	}

	/**
	 * Build the index.
	 *
	 * @param rtx
	 *            the current {@link JsonNodeReadOnlyTrx}
	 * @param builders
	 *            the index builders
	 */
	public static void build(final JsonNodeReadOnlyTrx rtx, final Set<JsonNodeVisitor> builders) {
		final long nodeKey = rtx.getNodeKey();
		rtx.moveToDocumentRoot();

		final var axis = new DescendantAxis(rtx);
		while (axis.hasNext()) {
			axis.nextLong();
			for (final JsonNodeVisitor builder : builders) {
				rtx.acceptVisitor(builder);
			}
		}
		rtx.moveTo(nodeKey);
	}

}
