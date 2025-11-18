package io.sirix.access.trx.page;

import cn.danielw.fop.ObjectFactory;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.StorageEngineReader;

public final class StorageEngineReaderFactory implements ObjectFactory<StorageEngineReader> {

  private final AbstractResourceSession<?,?> resourceSession;

  public StorageEngineReaderFactory(final AbstractResourceSession<?, ?> resourceSession) {
    this.resourceSession = resourceSession;
  }
  @Override
  public StorageEngineReader create() {
    return resourceSession.beginPageReadOnlyTrx();
  }

  @Override
  public void destroy(StorageEngineReader pageReadOnlyTrx) {
    pageReadOnlyTrx.close();
  }

  @Override
  public boolean validate(StorageEngineReader pageReadOnlyTrx) {
    int mostRecentRevisionNumber = resourceSession.getMostRecentRevisionNumber();
    return !pageReadOnlyTrx.isClosed() || pageReadOnlyTrx.getRevisionNumber() != mostRecentRevisionNumber;
  }
}
