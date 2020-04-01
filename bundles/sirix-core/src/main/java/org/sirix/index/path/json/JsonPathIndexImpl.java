package org.sirix.index.path.json;

import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.path.PathIndexBuilderFactory;
import org.sirix.index.path.PathIndexListenerFactory;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.UnorderedKeyValuePage;

public final class JsonPathIndexImpl implements JsonPathIndex {

  private final PathIndexBuilderFactory mPathIndexBuilderFactory;

  private final PathIndexListenerFactory mPathIndexListenerFactory;

  public JsonPathIndexImpl() {
    mPathIndexBuilderFactory = new PathIndexBuilderFactory();
    mPathIndexListenerFactory = new PathIndexListenerFactory();
  }

  @Override
  public JsonPathIndexBuilder createBuilder(final PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var indexBuilderDelegate = mPathIndexBuilderFactory.create(pageWriteTrx, pathSummaryReader, indexDef);
    return new JsonPathIndexBuilder(indexBuilderDelegate);
  }

  @Override
  public JsonPathIndexListener createListener(final PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var indexListenerDelegate = mPathIndexListenerFactory.create(pageWriteTrx, pathSummaryReader, indexDef);
    return new JsonPathIndexListener(indexListenerDelegate);
  }

}
