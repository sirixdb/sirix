package io.sirix.index.path.xml;

import io.sirix.access.trx.node.xml.AbstractXmlNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.index.path.PathIndexBuilder;
import io.sirix.node.immutable.xml.ImmutableAttributeNode;
import io.sirix.node.immutable.xml.ImmutableElement;

public final class XmlPathIndexBuilder extends AbstractXmlNodeVisitor {

	private final PathIndexBuilder mPathIndexBuilder;

	public XmlPathIndexBuilder(final PathIndexBuilder pathIndexBuilderDelegate) {
		mPathIndexBuilder = pathIndexBuilderDelegate;
	}

	@Override
	public VisitResult visit(ImmutableElement node) {
		return mPathIndexBuilder.process(node, node.getPathNodeKey());
	}

	@Override
	public VisitResult visit(ImmutableAttributeNode node) {
		return mPathIndexBuilder.process(node, node.getPathNodeKey());
	}

}
