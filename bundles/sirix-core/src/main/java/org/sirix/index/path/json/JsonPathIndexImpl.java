package org.sirix.index.path.json;

import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.path.PathIndexBuilderFactory;
import org.sirix.index.path.PathIndexListenerFactory;
import org.sirix.index.path.summary.PathSummaryReader;

public final class JsonPathIndexImpl implements JsonPathIndex {

  private final PathIndexBuilderFactory pathIndexBuilderFactory;

  private final PathIndexListenerFactory pathIndexListenerFactory;

  public JsonPathIndexImpl() {
    pathIndexBuilderFactory = new PathIndexBuilderFactory();
    pathIndexListenerFactory = new PathIndexListenerFactory();
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
