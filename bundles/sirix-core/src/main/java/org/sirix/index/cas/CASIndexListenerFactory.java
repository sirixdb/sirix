package org.sirix.index.cas;

import org.sirix.access.DatabaseType;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.CASValue;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;

import static com.google.common.base.Preconditions.checkNotNull;

public final class CASIndexListenerFactory {

  private final DatabaseType databaseType;

  public CASIndexListenerFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public CASIndexListener create(final PageTrx pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var pathSummary = checkNotNull(pathSummaryReader);
    final var avlTreeWriter =
        RBTreeWriter.<CASValue, NodeReferences>getInstance(
                this.databaseType,
                pageTrx,
                indexDef.getType(),
                indexDef.getID()
        );
    final var type = checkNotNull(indexDef.getContentType());
    final var paths = checkNotNull(indexDef.getPaths());

    return new CASIndexListener(pathSummary, avlTreeWriter, paths, type);
  }
}
