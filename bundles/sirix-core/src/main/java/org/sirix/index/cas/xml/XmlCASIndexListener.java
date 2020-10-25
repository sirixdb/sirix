package org.sirix.index.cas.xml;

import org.brackit.xquery.atomic.Str;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.cas.CASIndexListener;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;

public final class XmlCASIndexListener implements ChangeListener {

  private final CASIndexListener indexListenerDelegate;

  public XmlCASIndexListener(final CASIndexListener indexListenerDelegate) {
    this.indexListenerDelegate = indexListenerDelegate;
  }

  @Override
  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    if (node instanceof ValueNode) {
      final ValueNode valueNode = ((ValueNode) node);

      indexListenerDelegate.listen(type, valueNode, pathNodeKey, new Str(valueNode.getValue()));
    } else if (node instanceof ImmutableValueNode) {
      final ImmutableValueNode valueNode = ((ImmutableValueNode) node);

      indexListenerDelegate.listen(type, node, pathNodeKey, new Str(valueNode.getValue()));
    }
  }
}
