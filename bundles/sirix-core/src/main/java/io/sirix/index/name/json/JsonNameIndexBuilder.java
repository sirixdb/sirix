package io.sirix.index.name.json;

import io.sirix.api.visitor.VisitResult;
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.brackit.xquery.atomic.QNm;
import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.index.name.NameIndexBuilder;

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
