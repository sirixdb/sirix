package org.sirix.index.cas.json;

import org.brackit.xquery.atomic.Str;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.cas.CASIndexListener;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;

public final class JsonCASIndexListener implements ChangeListener {

  private final CASIndexListener mIndexListenerDelegate;

  public JsonCASIndexListener(final CASIndexListener indexListenerDelegate) {
    mIndexListenerDelegate = indexListenerDelegate;
  }

  @Override
  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    if (node.getKind() == NodeKind.BOOLEAN_VALUE || node.getKind() == NodeKind.NUMBER_VALUE
        || node.getKind() == NodeKind.STRING_VALUE) {
      final ValueNode valueNode = ((ValueNode) node);

      mIndexListenerDelegate.listen(type, valueNode, pathNodeKey, new Str(valueNode.getValue()));
    }
  }
}
