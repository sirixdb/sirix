package io.sirix.access.trx.page;

import cn.danielw.fop.ObjectFactory;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.StorageEngineReader;

public final class StorageEngineReaderFactory implements ObjectFactory<StorageEngineReader> {

  private final AbstractResourceSession<?, ?> resourceSession;

  public StorageEngineReaderFactory(final AbstractResourceSession<?, ?> resourceSession) {
    this.resourceSession = resourceSession;
  }

  @Override
  public StorageEngineReader create() {
    return resourceSession.beginStorageEngineReader();
  }

  @Override
  public void destroy(StorageEngineReader storageEngineReader) {
    storageEngineReader.close();
  }

  @Override
  public boolean validate(StorageEngineReader storageEngineReader) {
    int mostRecentRevisionNumber = resourceSession.getMostRecentRevisionNumber();
    return !storageEngineReader.isClosed() || storageEngineReader.getRevisionNumber() != mostRecentRevisionNumber;
  }
}
