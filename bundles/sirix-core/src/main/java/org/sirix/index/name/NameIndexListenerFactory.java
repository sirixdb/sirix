package org.sirix.index.name;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.DatabaseType;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;

import static com.google.common.base.Preconditions.checkNotNull;

public final class NameIndexListenerFactory {

  private final DatabaseType databaseType;

  public NameIndexListenerFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public NameIndexListener create(final PageTrx pageWriteTrx,
      final IndexDef indexDefinition) {
    final var includes = checkNotNull(indexDefinition.getIncluded());
    final var excludes = checkNotNull(indexDefinition.getExcluded());
    assert indexDefinition.getType() == IndexType.NAME;
    final var avlTreeWriter = RBTreeWriter.<QNm, NodeReferences>getInstance(
            this.databaseType,
            pageWriteTrx,
            indexDefinition.getType(),
            indexDefinition.getID()
    );

    return new NameIndexListener(includes, excludes, avlTreeWriter);
  }
}
