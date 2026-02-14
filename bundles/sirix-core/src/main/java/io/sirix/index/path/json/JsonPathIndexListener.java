package io.sirix.index.path.json;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.PathNodeKeyChangeListener;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.sirix.index.path.PathIndexListener;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import org.checkerframework.checker.nullness.qual.Nullable;

final class JsonPathIndexListener implements PathNodeKeyChangeListener {

  private final PathIndexListener pathIndexListener;

  JsonPathIndexListener(final PathIndexListener pathIndexListenerDelegate) {
    pathIndexListener = pathIndexListenerDelegate;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    listen(type, node.getNodeKey(), node.getKind(), pathNodeKey, null, null);
  }

  @Override
  public void listen(IndexController.ChangeType type, long nodeKey, NodeKind nodeKind, long pathNodeKey,
      @Nullable QNm name, @Nullable Str value) {
    if (nodeKind == NodeKind.OBJECT_KEY || nodeKind == NodeKind.ARRAY) {
      pathIndexListener.listen(type, nodeKey, pathNodeKey);
    }
  }
}
