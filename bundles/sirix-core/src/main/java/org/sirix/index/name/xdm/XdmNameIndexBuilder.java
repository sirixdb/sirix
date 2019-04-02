package org.sirix.index.name.xdm;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.xdm.AbstractXdmNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.index.name.NameIndexBuilder;
import org.sirix.node.immutable.xdm.ImmutableElement;

final class XdmNameIndexBuilder extends AbstractXdmNodeVisitor {
  private final NameIndexBuilder mBuilder;

  XdmNameIndexBuilder(final NameIndexBuilder builder) {
    mBuilder = builder;
  }

  @Override
  public VisitResult visit(final ImmutableElement node) {
    final QNm name = node.getName();

    return mBuilder.build(name, node);
  }
}
