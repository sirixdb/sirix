package org.sirix.index.name.xdm;

import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.IndexController.ChangeType;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.ChangeListener;
import org.sirix.index.IndexDef;
import org.sirix.index.name.NameIndexListener;
import org.sirix.index.name.NameIndexListenerFactory;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.UnorderedKeyValuePage;

final class XdmNameIndexListener implements ChangeListener {

  private final NameIndexListener mIndexListener;

  XdmNameIndexListener(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDefinition, final NameIndexListenerFactory listenerFactory) {
    mIndexListener = listenerFactory.create(pageWriteTrx, indexDefinition);
  }

  @Override
  public void listen(ChangeType type, @Nonnull ImmutableNode node, long pathNodeKey) {
    if (node instanceof NameNode) {
      final NameNode nameNode = (NameNode) node;
      final QNm name = nameNode.getName();

      mIndexListener.listen(type, nameNode, name);
    }
  }
}
