package org.sirix.index.name.xml;

import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.name.NameIndexBuilderFactory;
import org.sirix.index.name.NameIndexListenerFactory;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.UnorderedKeyValuePage;

public final class XmlNameIndexImpl implements XmlNameIndex {

  private final NameIndexBuilderFactory mNameIndexBuilderFactory;

  private final NameIndexListenerFactory mNameIndexListenerFactory;

  public XmlNameIndexImpl() {
    mNameIndexBuilderFactory = new NameIndexBuilderFactory();
    mNameIndexListenerFactory = new NameIndexListenerFactory();
  }

  @Override
  public XmlNameIndexBuilder createBuilder(final PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    final var nameIndexBuilderDelegate = mNameIndexBuilderFactory.create(pageWriteTrx, indexDef);
    return new XmlNameIndexBuilder(nameIndexBuilderDelegate);
  }

  @Override
  public XmlNameIndexListener createListener(final PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    final var nameIndexListenerDelegate = mNameIndexListenerFactory.create(pageWriteTrx, indexDef);
    return new XmlNameIndexListener(nameIndexListenerDelegate);
  }
}
