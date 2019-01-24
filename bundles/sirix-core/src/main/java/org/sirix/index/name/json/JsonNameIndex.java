package org.sirix.index.name.json;

import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.name.NameIndex;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public interface JsonNameIndex extends NameIndex {

  JsonNameIndexBuilder createBuilder(PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, IndexDef indexDef);

  JsonNameIndexListener createListener(PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      IndexDef indexDef);
}
