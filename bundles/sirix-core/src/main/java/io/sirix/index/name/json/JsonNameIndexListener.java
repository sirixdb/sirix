package io.sirix.index.name.json;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.ChangeListener;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.index.name.NameIndexListener;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ObjectKeyNode;

final class JsonNameIndexListener implements ChangeListener {

  private final NameIndexListener indexListener;

  public JsonNameIndexListener(final NameIndexListener listener) {
    indexListener = listener;
  }

  @Override
  public void listen(IndexController.ChangeType type, @NonNull ImmutableNode node, long pathNodeKey) {
    if (node instanceof final ObjectKeyNode objectKeyNode) {
      final QNm name = objectKeyNode.getName();

      indexListener.listen(type, objectKeyNode, name);
    }
  }
}
