package org.sirix.index.cas.json;

import org.brackit.xquery.atomic.Str;
import org.sirix.access.trx.node.IndexController.ChangeType;
import org.sirix.index.ChangeListener;
import org.sirix.index.cas.CASIndexListener;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.BooleanNode;
import org.sirix.node.json.NumberNode;
import org.sirix.node.json.ObjectBooleanNode;
import org.sirix.node.json.ObjectNumberNode;

public final class JsonCASIndexListener implements ChangeListener {

  private final CASIndexListener mIndexListenerDelegate;

  public JsonCASIndexListener(final CASIndexListener indexListenerDelegate) {
    mIndexListenerDelegate = indexListenerDelegate;
  }

  @Override
  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    switch (node.getKind()) {
      case STRING_VALUE:
      case OBJECT_STRING_VALUE:
        final ValueNode valueNode = ((ValueNode) node);
        mIndexListenerDelegate.listen(type, valueNode, pathNodeKey, new Str(valueNode.getValue()));
        break;
      case BOOLEAN_VALUE:
        mIndexListenerDelegate.listen(type, node, pathNodeKey, new Str(String.valueOf(((BooleanNode) node).getValue())));
        break;
      case OBJECT_BOOLEAN_VALUE:
        mIndexListenerDelegate.listen(type, node, pathNodeKey, new Str(String.valueOf(((ObjectBooleanNode) node).getValue())));
        break;
      case NUMBER_VALUE:
        mIndexListenerDelegate.listen(type, node, pathNodeKey, new Str(String.valueOf(((NumberNode) node).getValue())));
        break;
      case OBJECT_NUMBER_VALUE:
        mIndexListenerDelegate.listen(type, node, pathNodeKey, new Str(String.valueOf(((ObjectNumberNode) node).getValue())));
        break;
    }
  }
}
