package org.sirix.index.path.xdm;

import java.util.Iterator;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.index.path.PathFilter;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public interface XdmPathIndex<K extends Comparable<? super K>, V extends References> {
  XdmPathIndexBuilder createBuilder(PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef);

  XdmPathIndexListener createListener(PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef);

  Iterator<V> openIndex(PageReadTrx pageRtx, IndexDef indexDef, PathFilter filter);
}
