package io.sirix.index.name.json;

import io.brackit.query.atomic.QNm;
import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.index.name.NameIndexBuilder;
import io.sirix.node.NodeKind;
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;

final class JsonNameIndexBuilder extends AbstractJsonNodeVisitor {
  private final NameIndexBuilder builder;

  public JsonNameIndexBuilder(final NameIndexBuilder builder) {
    this.builder = builder;
  }

  @Override
  public VisitResult visit(final ImmutableObjectKeyNode node) {
    QNm name = node.getName();

    if (name == null) {
      name = new QNm(builder.pageRtx.getName(node.getNameKey(), NodeKind.OBJECT_KEY));
    }

    return builder.build(name, node);
  }
}
