package org.sirix.index.cas.json;

import org.sirix.access.DatabaseType;
import org.sirix.api.PageTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.cas.CASIndexBuilderFactory;
import org.sirix.index.cas.CASIndexListenerFactory;
import org.sirix.index.path.summary.PathSummaryReader;

public final class JsonCASIndexImpl implements JsonCASIndex {

  private final CASIndexBuilderFactory casIndexBuilderFactory;

  private final CASIndexListenerFactory casIndexListenerFactory;

  public JsonCASIndexImpl() {
    casIndexBuilderFactory = new CASIndexBuilderFactory(DatabaseType.JSON);
    casIndexListenerFactory = new CASIndexListenerFactory(DatabaseType.JSON);
  }

  @Override
  public JsonCASIndexBuilder createBuilder(JsonNodeReadOnlyTrx rtx,
      PageTrx pageTrx, PathSummaryReader pathSummaryReader,
      IndexDef indexDef) {
    final var indexBuilderDelegate = casIndexBuilderFactory.create(pageTrx, pathSummaryReader, indexDef);
    return new JsonCASIndexBuilder(indexBuilderDelegate, rtx);
  }

  @Override
  public JsonCASIndexListener createListener(PageTrx pageTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef) {
    final var indexListenerDelegate = casIndexListenerFactory.create(pageTrx, pathSummaryReader, indexDef);
    return new JsonCASIndexListener(indexListenerDelegate);
  }
}
