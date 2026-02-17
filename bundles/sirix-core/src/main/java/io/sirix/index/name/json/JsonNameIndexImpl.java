package io.sirix.index.name.json;

import io.sirix.access.DatabaseType;
import io.sirix.index.IndexDef;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.name.NameIndexBuilderFactory;
import io.sirix.index.name.NameIndexListenerFactory;

public final class JsonNameIndexImpl implements JsonNameIndex {

  private final NameIndexBuilderFactory nameIndexBuilderFactory;

  private final NameIndexListenerFactory nameIndexListenerFactory;

  public JsonNameIndexImpl() {
    nameIndexBuilderFactory = new NameIndexBuilderFactory(DatabaseType.JSON);
    nameIndexListenerFactory = new NameIndexListenerFactory(DatabaseType.JSON);
  }

  @Override
  public JsonNameIndexBuilder createBuilder(final StorageEngineWriter pageWriteTrx, final IndexDef indexDef) {
    final var nameIndexBuilderDelegate = nameIndexBuilderFactory.create(pageWriteTrx, indexDef);
    return new JsonNameIndexBuilder(nameIndexBuilderDelegate);
  }

  @Override
  public JsonNameIndexListener createListener(final StorageEngineWriter pageWriteTrx, final IndexDef indexDef) {
    final var nameIndexListenerDelegate = nameIndexListenerFactory.create(pageWriteTrx, indexDef);
    return new JsonNameIndexListener(nameIndexListenerDelegate);
  }
}
