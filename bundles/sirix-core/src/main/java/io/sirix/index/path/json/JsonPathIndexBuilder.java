package io.sirix.index.path.json;

import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.index.path.PathIndexBuilder;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;

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

  // Fused OBJECT_NAMED_* records carry the OBJECT_KEY structural role — same PATH-index entry.

  @Override
  public VisitResult visit(ObjectNamedNumberNode node) {
    return pathIndexBuilder.process(node, node.getPathNodeKey());
  }

  @Override
  public VisitResult visit(ObjectNamedStringNode node) {
    return pathIndexBuilder.process(node, node.getPathNodeKey());
  }

  @Override
  public VisitResult visit(ObjectNamedBooleanNode node) {
    return pathIndexBuilder.process(node, node.getPathNodeKey());
  }

  @Override
  public VisitResult visit(ObjectNamedNullNode node) {
    return pathIndexBuilder.process(node, node.getPathNodeKey());
  }
}
