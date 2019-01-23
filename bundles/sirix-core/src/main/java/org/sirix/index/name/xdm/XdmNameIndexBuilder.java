package org.sirix.index.name.xdm;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.xdm.AbstractXdmNodeVisitor;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.index.IndexDef;
import org.sirix.index.name.NameIndexBuilder;
import org.sirix.index.name.NameIndexBuilderFactory;
import org.sirix.node.immutable.xdm.ImmutableElement;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

final class XdmNameIndexBuilder extends AbstractXdmNodeVisitor {
  private final NameIndexBuilder mBuilder;

  XdmNameIndexBuilder(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDefinition, final NameIndexBuilderFactory builderFactory) {
    mBuilder = builderFactory.create(pageWriteTrx, indexDefinition);
  }

  @Override
  public VisitResult visit(final ImmutableElement node) {
    final QNm name = node.getName();

    return mBuilder.build(name, node);
  }
}
