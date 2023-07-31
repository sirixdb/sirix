package io.sirix.index.path.json;

import io.sirix.access.DatabaseType;
import io.sirix.api.PageTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.path.PathIndexBuilderFactory;
import io.sirix.index.path.PathIndexListenerFactory;
import io.sirix.index.path.summary.PathSummaryReader;

public final class JsonPathIndexImpl implements JsonPathIndex {

  private final PathIndexBuilderFactory pathIndexBuilderFactory;

  private final PathIndexListenerFactory pathIndexListenerFactory;

  public JsonPathIndexImpl() {
    pathIndexBuilderFactory = new PathIndexBuilderFactory(DatabaseType.JSON);
    pathIndexListenerFactory = new PathIndexListenerFactory(DatabaseType.JSON);
  }

  @Override
  public JsonPathIndexBuilder createBuilder(final PageTrx pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var indexBuilderDelegate = pathIndexBuilderFactory.create(pageTrx, pathSummaryReader, indexDef);
    return new JsonPathIndexBuilder(indexBuilderDelegate);
  }

  @Override
  public JsonPathIndexListener createListener(final PageTrx pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var indexListenerDelegate = pathIndexListenerFactory.create(pageTrx, pathSummaryReader, indexDef);
    return new JsonPathIndexListener(indexListenerDelegate);
  }

}
