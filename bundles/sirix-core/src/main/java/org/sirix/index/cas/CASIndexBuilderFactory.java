package org.sirix.index.cas;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.CASValue;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;

public final class CASIndexBuilderFactory {

  public CASIndexBuilder create(final PageTrx pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var avlTreeWriter =
        RBTreeWriter.<CASValue, NodeReferences>getInstance(pageTrx, indexDef.getType(), indexDef.getID());
    final var pathSummary = checkNotNull(pathSummaryReader);
    final var paths = checkNotNull(indexDef.getPaths());
    final var type = checkNotNull(indexDef.getContentType());

    return new CASIndexBuilder(avlTreeWriter, pathSummary, paths, type);
  }
}
