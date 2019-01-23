package org.sirix.index.name.json;

import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.IndexController.ChangeType;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.ChangeListener;
import org.sirix.index.IndexDef;
import org.sirix.index.name.NameIndexListener;
import org.sirix.index.name.NameIndexListenerFactory;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.page.UnorderedKeyValuePage;

final class JsonNameIndexListener implements ChangeListener {

  private final NameIndexListener mIndexListener;

  public JsonNameIndexListener(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDefinition, final NameIndexListenerFactory listenerFactory) {
    mIndexListener = listenerFactory.create(pageWriteTrx, indexDefinition);
  }

  @Override
  public void listen(ChangeType type, @Nonnull ImmutableNode node, long pathNodeKey) {
    if (node instanceof ObjectKeyNode) {
      final ObjectKeyNode objectKeyNode = (ObjectKeyNode) node;
      final QNm name = new QNm(objectKeyNode.getName());

      mIndexListener.listen(type, objectKeyNode, name);
    }
  }
}
