package org.sirix.index.cas;

import java.util.Iterator;

import org.brackit.xquery.atomic.Atomic;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public interface CASIndex<K extends Comparable<? super K>, V extends References> {

	CASIndexBuilder createBuilder(NodeReadTrx rtx,
			PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			PathSummaryReader pathSummaryReader, IndexDef indexDef);

	CASIndexListener createListener(
			PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			PathSummaryReader pathSummaryReader, IndexDef indexDef);

	Iterator<V> openIndex(PageReadTrx pageReadTrx, IndexDef indexDef,
			SearchMode mode, CASFilterRange filter, Atomic low, Atomic high,
			boolean incLow, boolean incMax);

	Iterator<V> openIndex(PageReadTrx pageReadTrx, IndexDef indexDef,
			SearchMode mode, CASFilter filter, Atomic key, boolean incSelf);
}
