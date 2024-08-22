package io.sirix.index.cas.json;

import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.visitor.VisitResult;
import io.sirix.index.cas.CASIndexBuilder;
import io.sirix.node.immutable.json.*;
import io.sirix.node.interfaces.immutable.ImmutableNode;

/**
 * Builds a content-and-structure (CAS) index.
 *
 * @author Johannes Lichtenberger
 */
final class JsonCASIndexBuilder extends AbstractJsonNodeVisitor {

	private final CASIndexBuilder indexBuilderDelegate;

	private final JsonNodeReadOnlyTrx rtx;

	JsonCASIndexBuilder(final CASIndexBuilder indexBuilderDelegate, final JsonNodeReadOnlyTrx rtx) {
		this.indexBuilderDelegate = indexBuilderDelegate;
		this.rtx = rtx;
	}

	@Override
	public VisitResult visit(ImmutableStringNode node) {
		final long PCR = getPathClassRecord(node);

		return indexBuilderDelegate.process(node, PCR);
	}

	@Override
	public VisitResult visit(ImmutableObjectStringNode node) {
		final long PCR = getPathClassRecord(node);

		return indexBuilderDelegate.process(node, PCR);
	}

	@Override
	public VisitResult visit(ImmutableBooleanNode node) {
		final long PCR = getPathClassRecord(node);

		return indexBuilderDelegate.process(node, PCR);
	}

	@Override
	public VisitResult visit(ImmutableObjectBooleanNode node) {
		final long PCR = getPathClassRecord(node);

		return indexBuilderDelegate.process(node, PCR);
	}

	@Override
	public VisitResult visit(ImmutableNumberNode node) {
		final long PCR = getPathClassRecord(node);

		return indexBuilderDelegate.process(node, PCR);
	}

	@Override
	public VisitResult visit(ImmutableObjectNumberNode node) {
		final long PCR = getPathClassRecord(node);

		return indexBuilderDelegate.process(node, PCR);
	}

	private long getPathClassRecord(ImmutableNode node) {
		rtx.moveTo(node.getParentKey());

		final long pcr;

		if (rtx.isObjectKey()) {
			pcr = ((ImmutableObjectKeyNode) rtx.getNode()).getPathNodeKey();
		} else if (rtx.isArray()) {
			pcr = ((ImmutableArrayNode) rtx.getNode()).getPathNodeKey();
		} else {
			pcr = 0;
		}

		return pcr;
	}

}
