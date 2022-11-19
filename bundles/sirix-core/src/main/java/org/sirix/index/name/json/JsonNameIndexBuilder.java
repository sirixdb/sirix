package org.sirix.index.name.json;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.index.name.NameIndexBuilder;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;

final class JsonNameIndexBuilder extends AbstractJsonNodeVisitor {
  private final NameIndexBuilder builder;

  public JsonNameIndexBuilder(final NameIndexBuilder builder) {
    this.builder = builder;
  }

  @Override
  public VisitResult visit(final ImmutableObjectKeyNode node) {
    final QNm name = node.getName();

    return builder.build(name, node);
  }
}
