package org.sirix.index.cas.json;

import org.sirix.api.PageTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.cas.CASIndexBuilderFactory;
import org.sirix.index.cas.CASIndexListenerFactory;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public final class JsonCASIndexImpl implements JsonCASIndex {

  private final CASIndexBuilderFactory mCASIndexBuilderFactory;

  private final CASIndexListenerFactory mCASIndexListenerFactory;

  public JsonCASIndexImpl() {
    mCASIndexBuilderFactory = new CASIndexBuilderFactory();
    mCASIndexListenerFactory = new CASIndexListenerFactory();
  }

  @Override
  public JsonCASIndexBuilder createBuilder(JsonNodeReadOnlyTrx rtx,
      PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, PathSummaryReader pathSummaryReader,
      IndexDef indexDef) {
    final var indexBuilderDelegate = mCASIndexBuilderFactory.create(pageWriteTrx, pathSummaryReader, indexDef);
    return new JsonCASIndexBuilder(indexBuilderDelegate, rtx);
  }

  @Override
  public JsonCASIndexListener createListener(PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef) {
    final var indexListenerDelegate = mCASIndexListenerFactory.create(pageWriteTrx, pathSummaryReader, indexDef);
    return new JsonCASIndexListener(indexListenerDelegate);
  }
}
