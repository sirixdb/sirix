package io.sirix.index.path.xml;

import io.sirix.index.IndexDef;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.path.PathIndexBuilderFactory;
import io.sirix.index.path.PathIndexListenerFactory;
import io.sirix.index.path.summary.PathSummaryReader;

public final class XmlPathIndexImpl implements XmlPathIndex {

  private final PathIndexBuilderFactory pathIndexBuilderFactory;

  private final PathIndexListenerFactory pathIndexListenerFactory;

  public XmlPathIndexImpl() {
    pathIndexBuilderFactory = new PathIndexBuilderFactory();
    pathIndexListenerFactory = new PathIndexListenerFactory();
  }

  @Override
  public XmlPathIndexBuilder createBuilder(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var builderDelegate = pathIndexBuilderFactory.create(storageEngineWriter, pathSummaryReader, indexDef);
    return new XmlPathIndexBuilder(builderDelegate);
  }

  @Override
  public XmlPathIndexListener createListener(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var listenerDelegate = pathIndexListenerFactory.create(storageEngineWriter, pathSummaryReader, indexDef);
    return new XmlPathIndexListener(listenerDelegate);
  }

}
