package io.sirix.index.name;

import static java.util.Objects.requireNonNull;

import io.sirix.access.DatabaseType;
import io.sirix.api.PageTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.brackit.query.atomic.QNm;

public final class NameIndexBuilderFactory {

  private final DatabaseType databaseType;

  public NameIndexBuilderFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public NameIndexBuilder create(final PageTrx pageTrx, final IndexDef indexDefinition) {
    final var includes = requireNonNull(indexDefinition.getIncluded());
    final var excludes = requireNonNull(indexDefinition.getExcluded());
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
