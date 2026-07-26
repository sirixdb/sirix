package io.sirix.index.path.json;

import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.index.path.PathIndexBuilder;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;

public final class JsonPathIndexBuilder extends AbstractJsonNodeVisitor {

  private final PathIndexBuilder pathIndexBuilder;

  public JsonPathIndexBuilder(final PathIndexBuilder pathIndexBuilderDelegate) {
    pathIndexBuilder = pathIndexBuilderDelegate;
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

  @Override
  public VisitResult visit(ObjectNamedObjectNode node) {
    // iter#32 P2 structural fusion: OBJECT_NAMED_OBJECT plays the OBJECT_KEY role.
    return pathIndexBuilder.process(node, node.getPathNodeKey());
  }

  @Override
  public VisitResult visit(ObjectNamedArrayNode node) {
    // iter#32 P2 structural fusion: OBJECT_NAMED_ARRAY plays BOTH the OBJECT_KEY (field name)
    // role AND the ARRAY (anonymous container) role. Its own pathNodeKey lives at the
    // {@code __array__/ARRAY} layer so child fields nest under "/.../tada/__array__/...". Legacy
    // had two physical nodes (OBJECT_KEY + ARRAY child) indexed under their respective PCRs;
    // path-index lookups for the OBJECT_KEY-level path ("/.../tada") expect that layer's entry
    // to be populated. We index the fused record under BOTH layers so either path resolves.
    final long arrayLayerPathNodeKey = node.getPathNodeKey();
    pathIndexBuilder.process(node, arrayLayerPathNodeKey);
    final var arrayPathNode =
        pathIndexBuilder.getPathSummaryReader().getPathNodeForPathNodeKey(arrayLayerPathNodeKey);
    if (arrayPathNode != null) {
      final long objectKeyLayerPathNodeKey = arrayPathNode.getParentKey();
      if (objectKeyLayerPathNodeKey >= 0) {
        return pathIndexBuilder.process(node, objectKeyLayerPathNodeKey);
      }
    }
    return VisitResultType.CONTINUE;
  }
}
