package io.sirix.access.trx.page;

import cn.danielw.fop.ObjectFactory;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.PageReadOnlyTrx;

public final class PageTrxReadOnlyFactory implements ObjectFactory<PageReadOnlyTrx> {

  private final AbstractResourceSession<?,?> resourceSession;

  public PageTrxReadOnlyFactory(final AbstractResourceSession<?, ?> resourceSession) {
    this.resourceSession = resourceSession;
  }
  @Override
  public PageReadOnlyTrx create() {
    return resourceSession.beginPageReadOnlyTrx();
  }

  @Override
  public void destroy(PageReadOnlyTrx pageReadOnlyTrx) {
    pageReadOnlyTrx.close();
  }

  @Override
  public boolean validate(PageReadOnlyTrx pageReadOnlyTrx) {
    int mostRecentRevisionNumber = resourceSession.getMostRecentRevisionNumber();
    return !pageReadOnlyTrx.isClosed() || pageReadOnlyTrx.getRevisionNumber() != mostRecentRevisionNumber;
  }
}
