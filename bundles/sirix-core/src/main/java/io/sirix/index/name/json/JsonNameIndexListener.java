package io.sirix.index.name.json;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.PathNodeKeyChangeListener;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import io.sirix.index.name.NameIndexListener;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ObjectKeyNode;

final class JsonNameIndexListener implements PathNodeKeyChangeListener {

  private final NameIndexListener indexListener;

  public JsonNameIndexListener(final NameIndexListener listener) {
    indexListener = listener;
  }

  @Override
  public void listen(IndexController.ChangeType type, @NonNull ImmutableNode node, long pathNodeKey) {
    if (node instanceof final ObjectKeyNode objectKeyNode) {
      listen(type, objectKeyNode.getNodeKey(), objectKeyNode.getKind(), pathNodeKey, objectKeyNode.getName(), null);
    }
  }

  @Override
  public void listen(IndexController.ChangeType type, long nodeKey, NodeKind nodeKind, long pathNodeKey,
      @Nullable QNm name, @Nullable Str value) {
    if (nodeKind == NodeKind.OBJECT_KEY && name != null) {
      indexListener.listen(type, nodeKey, name);
    }
  }
}
