package io.sirix.index.name.json;

import io.brackit.query.atomic.QNm;
import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.index.name.NameIndexBuilder;
import io.sirix.node.NodeKind;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;

final class JsonNameIndexBuilder extends AbstractJsonNodeVisitor {
  private final NameIndexBuilder builder;

  public JsonNameIndexBuilder(final NameIndexBuilder builder) {
    this.builder = builder;
  }

  // Fused OBJECT_NAMED_* carries the field-name role.

  @Override
  public VisitResult visit(final ObjectNamedNumberNode node) {
    return builder.build(resolveName(node.getName(), node.getNameKey()), node);
  }

  @Override
  public VisitResult visit(final ObjectNamedStringNode node) {
    return builder.build(resolveName(node.getName(), node.getNameKey()), node);
  }

  @Override
  public VisitResult visit(final ObjectNamedBooleanNode node) {
    return builder.build(resolveName(node.getName(), node.getNameKey()), node);
  }

  @Override
  public VisitResult visit(final ObjectNamedNullNode node) {
    return builder.build(resolveName(node.getName(), node.getNameKey()), node);
  }

  @Override
  public VisitResult visit(final ObjectNamedObjectNode node) {
    // iter#32 P2 structural fusion: OBJECT_NAMED_OBJECT carries the OBJECT_KEY name.
    return builder.build(resolveName(node.getName(), node.getNameKey()), node);
  }

  @Override
  public VisitResult visit(final ObjectNamedArrayNode node) {
    // iter#32 P2 structural fusion: OBJECT_NAMED_ARRAY carries the OBJECT_KEY name.
    return builder.build(resolveName(node.getName(), node.getNameKey()), node);
  }

  private QNm resolveName(final QNm cachedName, final int nameKey) {
    if (cachedName != null) return cachedName;
    final String localName = builder.storageEngineReader.getName(nameKey, NodeKind.OBJECT_NAMED_OBJECT);
    return localName == null ? null : new QNm(localName);
  }
}
