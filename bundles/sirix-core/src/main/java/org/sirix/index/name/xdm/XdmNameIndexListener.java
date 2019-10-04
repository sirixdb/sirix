package org.sirix.index.name.xdm;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.name.NameIndexListener;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import javax.annotation.Nonnull;

final class XdmNameIndexListener implements ChangeListener {

  private final NameIndexListener mIndexListener;

  XdmNameIndexListener(final NameIndexListener indexListener) {
    mIndexListener = indexListener;
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
