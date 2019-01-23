package org.sirix.index.name.json;

import java.util.Iterator;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.name.NameFilter;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public interface JsonNameIndex<K extends Comparable<? super K>, V extends References> {

  JsonNameIndexBuilder createBuilder(PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, IndexDef indexDef);

  JsonNameIndexListener createListener(PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      IndexDef indexDef);

  Iterator<NodeReferences> openIndex(PageReadTrx pageRtx, IndexDef indexDef, NameFilter filter);
}
