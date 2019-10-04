package org.sirix.index.name.json;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.name.NameIndexListener;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.ObjectKeyNode;

import javax.annotation.Nonnull;

final class JsonNameIndexListener implements ChangeListener {

  private final NameIndexListener mIndexListener;

  public JsonNameIndexListener(final NameIndexListener listener) {
    mIndexListener = listener;
  }

  @Override
  public void listen(ChangeType type, @Nonnull ImmutableNode node, long pathNodeKey) {
    if (node instanceof ObjectKeyNode) {
      final ObjectKeyNode objectKeyNode = (ObjectKeyNode) node;
      final QNm name = new QNm(objectKeyNode.getName());

      mIndexListener.listen(type, objectKeyNode, name);
    }
  }
}
