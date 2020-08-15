package org.sirix.index.path.xml;

import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.path.PathIndexBuilderFactory;
import org.sirix.index.path.PathIndexListenerFactory;
import org.sirix.index.path.summary.PathSummaryReader;

public final class XmlPathIndexImpl implements XmlPathIndex {

  private final PathIndexBuilderFactory pathIndexBuilderFactory;

  private final PathIndexListenerFactory pathIndexListenerFactory;

  public XmlPathIndexImpl() {
    pathIndexBuilderFactory = new PathIndexBuilderFactory();
    pathIndexListenerFactory = new PathIndexListenerFactory();
  }

  @Override
  public XmlPathIndexBuilder createBuilder(final PageTrx pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var builderDelegate = pathIndexBuilderFactory.create(pageTrx, pathSummaryReader, indexDef);
    return new XmlPathIndexBuilder(builderDelegate);
  }

  @Override
  public XmlPathIndexListener createListener(final PageTrx pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var listenerDelegate = pathIndexListenerFactory.create(pageTrx, pathSummaryReader, indexDef);
    return new XmlPathIndexListener(listenerDelegate);
  }

}
