package org.sirix.index.path.json;

import org.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.index.path.PathIndexBuilder;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;

public final class JsonPathIndexBuilder extends AbstractJsonNodeVisitor {

  private final PathIndexBuilder mPathIndexBuilder;

  public JsonPathIndexBuilder(final PathIndexBuilder pathIndexBuilderDelegate) {
    mPathIndexBuilder = pathIndexBuilderDelegate;
  }

  @Override
  public VisitResult visit(ImmutableObjectKeyNode node) {
    return mPathIndexBuilder.process(node, node.getPathNodeKey());
  }
}
