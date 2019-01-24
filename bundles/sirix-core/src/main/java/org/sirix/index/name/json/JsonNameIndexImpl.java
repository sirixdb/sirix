package org.sirix.index.name.json;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.name.NameIndexBuilderFactory;
import org.sirix.index.name.NameIndexListenerFactory;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public final class JsonNameIndexImpl implements JsonNameIndex<QNm, NodeReferences> {

  private final NameIndexBuilderFactory mNameIndexBuilderFactory;

  private final NameIndexListenerFactory mNameIndexListenerFactory;

  public JsonNameIndexImpl() {
    mNameIndexBuilderFactory = new NameIndexBuilderFactory();
    mNameIndexListenerFactory = new NameIndexListenerFactory();
  }

  @Override
  public JsonNameIndexBuilder createBuilder(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    final var nameIndexBuilderDelegate = mNameIndexBuilderFactory.create(pageWriteTrx, indexDef);
    return new JsonNameIndexBuilder(nameIndexBuilderDelegate);
  }

  @Override
  public JsonNameIndexListener createListener(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    final var nameIndexListenerDelegate = mNameIndexListenerFactory.create(pageWriteTrx, indexDef);
    return new JsonNameIndexListener(nameIndexListenerDelegate);
  }
}
