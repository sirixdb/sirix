package org.sirix.index.cas;

import java.util.Iterator;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public interface CASIndex<K extends Comparable<? super K>, V extends References> {

  XdmCASIndexBuilder createBuilder(XdmNodeReadTrx rtx,
      PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef);

  CASIndexListener createListener(PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef);

  Iterator<V> openIndex(PageReadTrx pageReadTrx, IndexDef indexDef, CASFilterRange filter);

  Iterator<V> openIndex(PageReadTrx pageReadTrx, IndexDef indexDef, CASFilter filter);
}
