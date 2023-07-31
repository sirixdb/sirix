package io.sirix.index.name.xml;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.ChangeListener;
import io.sirix.node.interfaces.NameNode;
import io.brackit.query.atomic.QNm;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.index.name.NameIndexListener;
import io.sirix.node.interfaces.immutable.ImmutableNode;

final class XmlNameIndexListener implements ChangeListener {

  private final NameIndexListener indexListener;

  XmlNameIndexListener(final NameIndexListener indexListener) {
    this.indexListener = indexListener;
  }

  @Override
  public void listen(IndexController.ChangeType type, @NonNull ImmutableNode node, long pathNodeKey) {
    if (node instanceof final NameNode nameNode) {
      final QNm name = nameNode.getName();

      indexListener.listen(type, nameNode, name);
    }
  }
}
