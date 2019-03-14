package org.sirix.index.path.xml;

import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.path.PathIndexBuilderFactory;
import org.sirix.index.path.PathIndexListenerFactory;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public final class XmlPathIndexImpl implements XmlPathIndex {

  private final PathIndexBuilderFactory mPathIndexBuilderFactory;

  private final PathIndexListenerFactory mPathIndexListenerFactory;

  public XmlPathIndexImpl() {
    mPathIndexBuilderFactory = new PathIndexBuilderFactory();
    mPathIndexListenerFactory = new PathIndexListenerFactory();
  }

  @Override
  public XmlPathIndexBuilder createBuilder(final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var builderDelegate = mPathIndexBuilderFactory.create(pageWriteTrx, pathSummaryReader, indexDef);
    return new XmlPathIndexBuilder(builderDelegate);
  }

  @Override
  public XmlPathIndexListener createListener(final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var listenerDelegate = mPathIndexListenerFactory.create(pageWriteTrx, pathSummaryReader, indexDef);
    return new XmlPathIndexListener(listenerDelegate);
  }

}
