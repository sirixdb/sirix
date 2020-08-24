package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;

public final class PathIndexListenerFactory {
  public PathIndexListener create(final PageTrx pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var pathSummary = checkNotNull(pathSummaryReader);
    final var paths = checkNotNull(indexDef.getPaths());
    final var avlTreeWriter =
        RBTreeWriter.<Long, NodeReferences>getInstance(pageTrx, indexDef.getType(), indexDef.getID());

    return new PathIndexListener(paths, pathSummary, avlTreeWriter);
  }
}
