package io.sirix.index.path.xml;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.PathNodeKeyChangeListener;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.sirix.index.path.PathIndexListener;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import org.checkerframework.checker.nullness.qual.Nullable;

final class XmlPathIndexListener implements PathNodeKeyChangeListener {

  private final PathIndexListener mPathIndexListener;

  XmlPathIndexListener(final PathIndexListener pathIndexListenerDelegate) {
    mPathIndexListener = pathIndexListenerDelegate;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    listen(type, node.getNodeKey(), node.getKind(), pathNodeKey, null, null);
  }

  @Override
  public void listen(IndexController.ChangeType type, long nodeKey, NodeKind nodeKind, long pathNodeKey,
      @Nullable QNm name, @Nullable Str value) {
    switch (nodeKind) {
      case ELEMENT, ATTRIBUTE, NAMESPACE, PROCESSING_INSTRUCTION -> mPathIndexListener.listen(type, nodeKey, pathNodeKey);
      default -> {
      }
    }
  }
}
