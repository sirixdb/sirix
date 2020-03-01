package org.sirix.index.cas.xml;

import org.sirix.api.PageTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.cas.CASIndexBuilderFactory;
import org.sirix.index.cas.CASIndexListenerFactory;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public final class XmlCASIndexImpl implements XmlCASIndex {

  private final CASIndexBuilderFactory mCASIndexBuilderFactory;

  private final CASIndexListenerFactory mCASIndexListenerFactory;

  public XmlCASIndexImpl() {
    mCASIndexBuilderFactory = new CASIndexBuilderFactory();
    mCASIndexListenerFactory = new CASIndexListenerFactory();
  }

  @Override
  public XmlCASIndexBuilder createBuilder(XmlNodeReadOnlyTrx rtx,
      PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, PathSummaryReader pathSummaryReader,
      IndexDef indexDef) {
    final var indexBuilderDelegate = mCASIndexBuilderFactory.create(pageWriteTrx, pathSummaryReader, indexDef);
    return new XmlCASIndexBuilder(indexBuilderDelegate, rtx);
  }

  @Override
  public XmlCASIndexListener createListener(PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef) {
    final var indexListenerDelegate = mCASIndexListenerFactory.create(pageWriteTrx, pathSummaryReader, indexDef);
    return new XmlCASIndexListener(indexListenerDelegate);
  }
}
