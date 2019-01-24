package org.sirix.index.name.xdm;

import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.name.NameIndexBuilderFactory;
import org.sirix.index.name.NameIndexListenerFactory;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public final class XdmNameIndexImpl implements XdmNameIndex {

  private final NameIndexBuilderFactory mNameIndexBuilderFactory;

  private final NameIndexListenerFactory mNameIndexListenerFactory;

  public XdmNameIndexImpl() {
    mNameIndexBuilderFactory = new NameIndexBuilderFactory();
    mNameIndexListenerFactory = new NameIndexListenerFactory();
  }

  @Override
  public XdmNameIndexBuilder createBuilder(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    final var nameIndexBuilderDelegate = mNameIndexBuilderFactory.create(pageWriteTrx, indexDef);
    return new XdmNameIndexBuilder(nameIndexBuilderDelegate);
  }

  @Override
  public XdmNameIndexListener createListener(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    final var nameIndexListenerDelegate = mNameIndexListenerFactory.create(pageWriteTrx, indexDef);
    return new XdmNameIndexListener(nameIndexListenerDelegate);
  }
}
