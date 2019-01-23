package org.sirix.index.name.json;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.index.IndexDef;
import org.sirix.index.name.NameIndexBuilder;
import org.sirix.index.name.NameIndexBuilderFactory;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

final class JsonNameIndexBuilder extends AbstractJsonNodeVisitor {
  private final NameIndexBuilder mBuilder;

  public JsonNameIndexBuilder(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDefinition, final NameIndexBuilderFactory builderFactory) {
    mBuilder = builderFactory.create(pageWriteTrx, indexDefinition);
  }

  @Override
  public VisitResult visit(final ImmutableObjectKeyNode node) {
    final QNm name = new QNm(node.getName());

    return mBuilder.build(name, node);
  }
}
