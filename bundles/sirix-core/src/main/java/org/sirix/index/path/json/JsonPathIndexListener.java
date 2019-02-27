package org.sirix.index.path.json;

import org.sirix.access.trx.node.xdm.XdmIndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.path.PathIndexListener;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.immutable.ImmutableNode;

final class JsonPathIndexListener implements ChangeListener {

  private final PathIndexListener mPathIndexListener;

  JsonPathIndexListener(final PathIndexListener pathIndexListenerDelegate) {
    mPathIndexListener = pathIndexListenerDelegate;
  }

  @Override
  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    if (node.getKind() == Kind.OBJECT_RECORD) {
      mPathIndexListener.listen(type, node, pathNodeKey);
    }
  }
}
