package org.sirix.index.name.xml;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.xml.AbstractXmlNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.index.name.NameIndexBuilder;
import org.sirix.node.immutable.xml.ImmutableElement;

final class XmlNameIndexBuilder extends AbstractXmlNodeVisitor {
  private final NameIndexBuilder mBuilder;

  XmlNameIndexBuilder(final NameIndexBuilder builder) {
    mBuilder = builder;
  }

  @Override
  public VisitResult visit(final ImmutableElement node) {
    final QNm name = node.getName();

    return mBuilder.build(name, node);
  }
}
