package io.sirix.index.path.json;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.ChangeListener;
import io.sirix.index.path.PathIndexListener;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;

final class JsonPathIndexListener implements ChangeListener {

  private final PathIndexListener pathIndexListener;

  JsonPathIndexListener(final PathIndexListener pathIndexListenerDelegate) {
    pathIndexListener = pathIndexListenerDelegate;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    if (node.getKind() == NodeKind.OBJECT_KEY || node.getKind() == NodeKind.ARRAY) {
      pathIndexListener.listen(type, node, pathNodeKey);
    }
  }
}
