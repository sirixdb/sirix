package io.sirix.index.name.xml;

import io.sirix.api.visitor.VisitResult;
import io.sirix.node.immutable.xml.ImmutableElement;
import io.sirix.node.immutable.xml.ImmutableAttributeNode;
import io.sirix.node.immutable.xml.ImmutablePI;
import io.brackit.query.atomic.QNm;
import io.sirix.access.trx.node.xml.AbstractXmlNodeVisitor;
import io.sirix.index.IndexBuildFinalizer;
import io.sirix.index.name.NameIndexBuilder;
import io.sirix.utils.XmlNameResolver;

final class XmlNameIndexBuilder extends AbstractXmlNodeVisitor implements IndexBuildFinalizer {
  private final NameIndexBuilder builder;

  XmlNameIndexBuilder(final NameIndexBuilder builder) {
    this.builder = builder;
  }

  @Override
  public VisitResult visit(final ImmutableElement node) {
    final QNm name = XmlNameResolver.resolveExpandedName(node, builder.storageEngineReader);

    return builder.build(name, node);
  }

  @Override
  public VisitResult visit(final ImmutableAttributeNode node) {
    return builder.build(XmlNameResolver.resolveExpandedName(node, builder.storageEngineReader), node);
  }

  @Override
  public VisitResult visit(final ImmutablePI node) {
    return builder.build(XmlNameResolver.resolveExpandedName(node, builder.storageEngineReader), node);
  }

  @Override
  public void finishIndexBuild() {
    builder.finish();
  }
}
