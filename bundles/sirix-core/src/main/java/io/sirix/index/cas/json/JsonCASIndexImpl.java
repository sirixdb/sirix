package io.sirix.index.cas.json;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.cas.CASIndexBuilderFactory;
import io.sirix.index.cas.CASIndexListenerFactory;
import io.sirix.index.path.summary.PathSummaryReader;

public final class JsonCASIndexImpl implements JsonCASIndex {

  private final CASIndexBuilderFactory casIndexBuilderFactory;

  private final CASIndexListenerFactory casIndexListenerFactory;

  public JsonCASIndexImpl() {
    casIndexBuilderFactory = new CASIndexBuilderFactory(DatabaseType.JSON);
    casIndexListenerFactory = new CASIndexListenerFactory(DatabaseType.JSON);
  }

  @Override
  public JsonCASIndexBuilder createBuilder(JsonNodeReadOnlyTrx rtx,
      StorageEngineWriter pageTrx, PathSummaryReader pathSummaryReader,
      IndexDef indexDef) {
    final var indexBuilderDelegate = casIndexBuilderFactory.create(pageTrx, pathSummaryReader, indexDef);
    return new JsonCASIndexBuilder(indexBuilderDelegate, rtx);
  }

  @Override
  public JsonCASIndexListener createListener(StorageEngineWriter pageTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef) {
    final var indexListenerDelegate = casIndexListenerFactory.create(pageTrx, pathSummaryReader, indexDef);
    return new JsonCASIndexListener(indexListenerDelegate);
  }
}
