package io.sirix.index.name.xml;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.node.interfaces.NameNode;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import io.sirix.index.name.NameIndexListener;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;

final class XmlNameIndexListener implements PathNodeKeyChangeListener {

  private final NameIndexListener indexListener;

  XmlNameIndexListener(final NameIndexListener indexListener) {
    this.indexListener = indexListener;
  }

  @Override
  public void listen(IndexController.ChangeType type, @NonNull ImmutableNode node, long pathNodeKey) {
    if (node instanceof final NameNode nameNode) {
      listen(type, nameNode.getNodeKey(), nameNode.getKind(), pathNodeKey, nameNode.getName(), null);
    }
  }

  @Override
  public void listen(IndexController.ChangeType type, long nodeKey, NodeKind nodeKind, long pathNodeKey,
      @Nullable QNm name, @Nullable Str value) {
    if (name == null) {
      return;
    }
    switch (nodeKind) {
      case ELEMENT, ATTRIBUTE, NAMESPACE, PROCESSING_INSTRUCTION -> indexListener.listen(type, nodeKey, name);
      default -> {
      }
    }
  }
}
