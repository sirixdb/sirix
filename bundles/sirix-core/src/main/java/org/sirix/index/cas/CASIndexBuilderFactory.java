package org.sirix.index.cas;

import static java.util.Objects.requireNonNull;

import org.sirix.access.DatabaseType;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.CASValue;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;


public final class CASIndexBuilderFactory {

  private final DatabaseType databaseType;

  public CASIndexBuilderFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public CASIndexBuilder create(final PageTrx pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var rbTreeWriter =
        RBTreeWriter.<CASValue, NodeReferences>getInstance(this.databaseType, pageTrx, indexDef.getType(), indexDef.getID());
    final var pathSummary = requireNonNull(pathSummaryReader);
    final var paths = requireNonNull(indexDef.getPaths());
    final var type = requireNonNull(indexDef.getContentType());

    return new CASIndexBuilder(rbTreeWriter, pathSummary, paths, type);
  }
}
