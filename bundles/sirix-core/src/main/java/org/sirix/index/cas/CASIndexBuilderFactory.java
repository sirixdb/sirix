package org.sirix.index.cas;

import org.sirix.access.DatabaseType;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.CASValue;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;

import static com.google.common.base.Preconditions.checkNotNull;

public final class CASIndexBuilderFactory {

  private final DatabaseType databaseType;

  public CASIndexBuilderFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public CASIndexBuilder create(final PageTrx pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var rbTreeWriter =
        RBTreeWriter.<CASValue, NodeReferences>getInstance(this.databaseType, pageTrx, indexDef.getType(), indexDef.getID());
    final var pathSummary = checkNotNull(pathSummaryReader);
    final var paths = checkNotNull(indexDef.getPaths());
    final var type = checkNotNull(indexDef.getContentType());

    return new CASIndexBuilder(rbTreeWriter, pathSummary, paths, type);
  }
}
