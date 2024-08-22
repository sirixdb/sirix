package io.sirix.index.name.xml;

import io.sirix.api.visitor.VisitResult;
import io.sirix.node.immutable.xml.ImmutableElement;
import io.brackit.query.atomic.QNm;
import io.sirix.access.trx.node.xml.AbstractXmlNodeVisitor;
import io.sirix.index.name.NameIndexBuilder;

final class XmlNameIndexBuilder extends AbstractXmlNodeVisitor {
	private final NameIndexBuilder builder;

	XmlNameIndexBuilder(final NameIndexBuilder builder) {
		this.builder = builder;
	}

	@Override
	public VisitResult visit(final ImmutableElement node) {
		final QNm name = node.getName();

		return builder.build(name, node);
	}
}
