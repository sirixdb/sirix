package org.sirix.index.name.json;

import org.sirix.access.DatabaseType;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.name.NameIndexBuilderFactory;
import org.sirix.index.name.NameIndexListenerFactory;

public final class JsonNameIndexImpl implements JsonNameIndex {

  private final NameIndexBuilderFactory nameIndexBuilderFactory;

  private final NameIndexListenerFactory nameIndexListenerFactory;

  public JsonNameIndexImpl() {
    nameIndexBuilderFactory = new NameIndexBuilderFactory(DatabaseType.JSON);
    nameIndexListenerFactory = new NameIndexListenerFactory(DatabaseType.JSON);
  }

  @Override
  public JsonNameIndexBuilder createBuilder(final PageTrx pageWriteTrx,
      final IndexDef indexDef) {
    final var nameIndexBuilderDelegate = nameIndexBuilderFactory.create(pageWriteTrx, indexDef);
    return new JsonNameIndexBuilder(nameIndexBuilderDelegate);
  }

  @Override
  public JsonNameIndexListener createListener(final PageTrx pageWriteTrx,
      final IndexDef indexDef) {
    final var nameIndexListenerDelegate = nameIndexListenerFactory.create(pageWriteTrx, indexDef);
    return new JsonNameIndexListener(nameIndexListenerDelegate);
  }
}
