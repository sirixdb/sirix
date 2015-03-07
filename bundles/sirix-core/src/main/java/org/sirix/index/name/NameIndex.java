package org.sirix.index.name;

import java.util.Iterator;

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public interface NameIndex<K extends Comparable<? super K>, V extends References> {

	NameIndexBuilder createBuilder(
			PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			IndexDef indexDef);

	NameIndexListener createListener(
			PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			IndexDef indexDef);

	Iterator<NodeReferences> openIndex(PageReadTrx pageRtx, IndexDef indexDef,
			NameFilter filter);
}
