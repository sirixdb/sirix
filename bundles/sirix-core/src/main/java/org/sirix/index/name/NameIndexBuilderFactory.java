package org.sirix.index.name;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.DatabaseType;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;

import static com.google.common.base.Preconditions.checkNotNull;

public final class NameIndexBuilderFactory {

  private final DatabaseType databaseType;

  public NameIndexBuilderFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public NameIndexBuilder create(final PageTrx pageTrx, final IndexDef indexDefinition) {

    final var includes = checkNotNull(indexDefinition.getIncluded());
    final var excludes = checkNotNull(indexDefinition.getExcluded());
    assert indexDefinition.getType() == IndexType.NAME;
    final var rbTreeWriter = RBTreeWriter.<QNm, NodeReferences>getInstance(
            this.databaseType,
            pageTrx,
            indexDefinition.getType(),
            indexDefinition.getID()
    );

    return new NameIndexBuilder(includes, excludes, rbTreeWriter);
  }
}
