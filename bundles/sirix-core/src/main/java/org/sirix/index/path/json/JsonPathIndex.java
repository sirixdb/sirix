package org.sirix.index.path.json;

import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.path.PathIndex;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public interface JsonPathIndex extends PathIndex {
  JsonPathIndexBuilder createBuilder(PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef);

  JsonPathIndexListener createListener(PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef);
}
