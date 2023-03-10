package org.sirix.index.path;

import static java.util.Objects.requireNonNull;

import org.sirix.access.DatabaseType;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;


public final class PathIndexBuilderFactory {

  private final DatabaseType databaseType;

  public PathIndexBuilderFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public PathIndexBuilder create(final PageTrx pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var pathSummary = requireNonNull(pathSummaryReader);
    final var paths = requireNonNull(indexDef.getPaths());
    assert indexDef.getType() == IndexType.PATH;
    final var rbTreeWriter = RBTreeWriter.<Long, NodeReferences>getInstance(
            this.databaseType, pageTrx, indexDef.getType(), indexDef.getID());

    return new PathIndexBuilder(rbTreeWriter, pathSummary, paths);
  }
}
