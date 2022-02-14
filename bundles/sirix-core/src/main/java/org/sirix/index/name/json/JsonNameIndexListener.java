package org.sirix.index.name.json;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.name.NameIndexListener;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.ObjectKeyNode;

final class JsonNameIndexListener implements ChangeListener {

  private final NameIndexListener mIndexListener;

  public JsonNameIndexListener(final NameIndexListener listener) {
    mIndexListener = listener;
  }

  @Override
  public void listen(ChangeType type, @NonNull ImmutableNode node, long pathNodeKey) {
    if (node instanceof ObjectKeyNode) {
      final ObjectKeyNode objectKeyNode = (ObjectKeyNode) node;
      final QNm name = objectKeyNode.getName();

      mIndexListener.listen(type, objectKeyNode, name);
    }
  }
}
