package org.sirix.index.path.json;

import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.path.PathIndexListener;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.immutable.ImmutableNode;

final class JsonPathIndexListener implements ChangeListener {

  private final PathIndexListener pathIndexListener;

  JsonPathIndexListener(final PathIndexListener pathIndexListenerDelegate) {
    pathIndexListener = pathIndexListenerDelegate;
  }

  @Override
  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    if (node.getKind() == NodeKind.OBJECT_KEY || node.getKind() == NodeKind.ARRAY) {
      pathIndexListener.listen(type, node, pathNodeKey);
    }
  }
}
