package org.sirix.index.cas.xdm;

import org.brackit.xquery.atomic.Str;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.cas.CASIndexListener;
import org.sirix.node.immutable.xml.ImmutableNamespace;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;

public final class XmlCASIndexListener implements ChangeListener {

  private final CASIndexListener mIndexListenerDelegate;

  public XmlCASIndexListener(final CASIndexListener indexListenerDelegate) {
    mIndexListenerDelegate = indexListenerDelegate;
  }

  @Override
  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    if (node instanceof ValueNode) {
      final ValueNode valueNode = ((ValueNode) node);

      mIndexListenerDelegate.listen(type, valueNode, pathNodeKey, new Str(valueNode.getValue()));
    } else if (node instanceof ImmutableValueNode) {
      final ImmutableValueNode valueNode = ((ImmutableValueNode) node);

      mIndexListenerDelegate.listen(type, node, pathNodeKey, new Str(valueNode.getValue()));
    }
  }
}
