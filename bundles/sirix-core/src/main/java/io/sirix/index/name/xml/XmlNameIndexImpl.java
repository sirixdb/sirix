package io.sirix.index.name.xml;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;
import io.sirix.index.name.NameIndexBuilderFactory;
import io.sirix.index.name.NameIndexListenerFactory;

public final class XmlNameIndexImpl implements XmlNameIndex {

  private final NameIndexBuilderFactory nameIndexBuilderFactory;

  private final NameIndexListenerFactory nameIndexListenerFactory;

  public XmlNameIndexImpl() {
    nameIndexBuilderFactory = new NameIndexBuilderFactory(DatabaseType.XML);
    nameIndexListenerFactory = new NameIndexListenerFactory(DatabaseType.XML);
  }

  @Override
  public XmlNameIndexBuilder createBuilder(final StorageEngineWriter storageEngineWriter, final IndexDef indexDef) {
    final var nameIndexBuilderDelegate = nameIndexBuilderFactory.create(storageEngineWriter, indexDef);
    return new XmlNameIndexBuilder(nameIndexBuilderDelegate);
  }

  @Override
  public XmlNameIndexListener createListener(final StorageEngineWriter storageEngineWriter, final IndexDef indexDef) {
    final var nameIndexListenerDelegate = nameIndexListenerFactory.create(storageEngineWriter, indexDef);
    return new XmlNameIndexListener(nameIndexListenerDelegate);
  }
}
