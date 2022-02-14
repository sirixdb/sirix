package org.sirix.index.name.xml;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.name.NameIndexListener;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;

final class XmlNameIndexListener implements ChangeListener {

  private final NameIndexListener mIndexListener;

  XmlNameIndexListener(final NameIndexListener indexListener) {
    mIndexListener = indexListener;
  }

  @Override
  public void listen(ChangeType type, @NonNull ImmutableNode node, long pathNodeKey) {
    if (node instanceof NameNode) {
      final NameNode nameNode = (NameNode) node;
      final QNm name = nameNode.getName();

      mIndexListener.listen(type, nameNode, name);
    }
  }
}
