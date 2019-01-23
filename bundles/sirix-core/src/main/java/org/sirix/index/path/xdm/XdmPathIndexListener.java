package org.sirix.index.path.xdm;

import org.sirix.access.trx.node.IndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.path.PathIndexListener;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;

final class XdmPathIndexListener implements ChangeListener {

  private final PathIndexListener mPathIndexListener;

  XdmPathIndexListener(final PathIndexListener pathIndexListenerDelegate) {
    mPathIndexListener = pathIndexListenerDelegate;
  }

  @Override
  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    if (node instanceof NameNode) {
      mPathIndexListener.listen(type, node, pathNodeKey);
    }
  }
}
