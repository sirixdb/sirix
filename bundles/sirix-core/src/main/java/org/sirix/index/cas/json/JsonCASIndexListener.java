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

  private final CASIndexListener indexListenerDelegate;

  public JsonCASIndexListener(final CASIndexListener indexListenerDelegate) {
    this.indexListenerDelegate = indexListenerDelegate;
  }

  @Override
  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    switch (node.getKind()) {
      case STRING_VALUE, OBJECT_STRING_VALUE -> {
        final ValueNode valueNode = ((ValueNode) node);
        indexListenerDelegate.listen(type, valueNode, pathNodeKey, new Str(valueNode.getValue()));
      }
      case BOOLEAN_VALUE -> indexListenerDelegate.listen(type,
                                                         node,
                                                         pathNodeKey,
                                                         new Str(String.valueOf(((BooleanNode) node).getValue())));
      case OBJECT_BOOLEAN_VALUE -> indexListenerDelegate.listen(type,
                                                                node,
                                                                pathNodeKey,
                                                                new Str(String.valueOf(((ObjectBooleanNode) node).getValue())));
      case NUMBER_VALUE -> indexListenerDelegate.listen(type,
                                                        node,
                                                        pathNodeKey,
                                                        new Str(String.valueOf(((NumberNode) node).getValue())));
      case OBJECT_NUMBER_VALUE -> indexListenerDelegate.listen(type,
                                                               node,
                                                               pathNodeKey,
                                                               new Str(String.valueOf(((ObjectNumberNode) node).getValue())));
    }
  }
}
