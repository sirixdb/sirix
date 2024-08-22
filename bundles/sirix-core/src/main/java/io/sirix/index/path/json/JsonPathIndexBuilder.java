package io.sirix.index.path.json;

import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.index.path.PathIndexBuilder;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;

public final class JsonPathIndexBuilder extends AbstractJsonNodeVisitor {

	private final PathIndexBuilder pathIndexBuilder;

	public JsonPathIndexBuilder(final PathIndexBuilder pathIndexBuilderDelegate) {
		pathIndexBuilder = pathIndexBuilderDelegate;
	}

	@Override
	public VisitResult visit(ImmutableObjectKeyNode node) {
		return pathIndexBuilder.process(node, node.getPathNodeKey());
	}

	@Override
	public VisitResult visit(ImmutableArrayNode node) {
		return pathIndexBuilder.process(node, node.getPathNodeKey());
	}
}
