package io.sirix.index.cas.xml;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.cas.CASIndexBuilderFactory;
import io.sirix.index.cas.CASIndexListenerFactory;
import io.sirix.index.path.summary.PathSummaryReader;

public final class XmlCASIndexImpl implements XmlCASIndex {

  private final CASIndexBuilderFactory casIndexBuilderFactory;

  private final CASIndexListenerFactory casIndexListenerFactory;

  public XmlCASIndexImpl() {
    casIndexBuilderFactory = new CASIndexBuilderFactory(DatabaseType.XML);
    casIndexListenerFactory = new CASIndexListenerFactory(DatabaseType.XML);
  }

  @Override
  public XmlCASIndexBuilder createBuilder(XmlNodeReadOnlyTrx rtx, StorageEngineWriter storageEngineWriter,
      PathSummaryReader pathSummaryReader, IndexDef indexDef) {
    final var indexBuilderDelegate = casIndexBuilderFactory.create(storageEngineWriter, pathSummaryReader, indexDef);
    return new XmlCASIndexBuilder(indexBuilderDelegate, rtx);
  }

  @Override
  public XmlCASIndexListener createListener(StorageEngineWriter storageEngineWriter, PathSummaryReader pathSummaryReader,
      IndexDef indexDef) {
    final var indexListenerDelegate = casIndexListenerFactory.create(storageEngineWriter, pathSummaryReader, indexDef);
    return new XmlCASIndexListener(indexListenerDelegate);
  }
}
