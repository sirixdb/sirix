package org.sirix.index.path.json;

import org.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.index.path.PathIndexBuilder;
import org.sirix.node.immutable.json.ImmutableArrayNode;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;

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
