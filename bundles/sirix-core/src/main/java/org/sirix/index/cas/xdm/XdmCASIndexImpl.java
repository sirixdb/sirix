package org.sirix.index.cas.xdm;

import org.sirix.api.PageWriteTrx;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.cas.CASIndexBuilderFactory;
import org.sirix.index.cas.CASIndexListenerFactory;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public final class XdmCASIndexImpl implements XdmCASIndex {

  private final CASIndexBuilderFactory mCASIndexBuilderFactory;

  private final CASIndexListenerFactory mCASIndexListenerFactory;

  public XdmCASIndexImpl() {
    mCASIndexBuilderFactory = new CASIndexBuilderFactory();
    mCASIndexListenerFactory = new CASIndexListenerFactory();
  }

  @Override
  public XdmCASIndexBuilder createBuilder(XdmNodeReadOnlyTrx rtx,
      PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, PathSummaryReader pathSummaryReader,
      IndexDef indexDef) {
    final var indexBuilderDelegate = mCASIndexBuilderFactory.create(pageWriteTrx, pathSummaryReader, indexDef);
    return new XdmCASIndexBuilder(indexBuilderDelegate, rtx);
  }

  @Override
  public XdmCASIndexListener createListener(PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef) {
    final var indexListenerDelegate = mCASIndexListenerFactory.create(pageWriteTrx, pathSummaryReader, indexDef);
    return new XdmCASIndexListener(indexListenerDelegate);
  }
}
