package org.sirix.index.name;

import static com.google.common.base.Preconditions.checkNotNull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;

public final class NameIndexBuilderFactory {

  public NameIndexBuilder create(final PageTrx pageTrx,
      final IndexDef indexDefinition) {
    final var includes = checkNotNull(indexDefinition.getIncluded());
    final var excludes = checkNotNull(indexDefinition.getExcluded());
    assert indexDefinition.getType() == IndexType.NAME;
    final var avlTreeWriter = RBTreeWriter.<QNm, NodeReferences>getInstance(pageTrx, indexDefinition.getType(),
                                                                            indexDefinition.getID());

    return new NameIndexBuilder(includes, excludes, avlTreeWriter);
  }
}
