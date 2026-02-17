package io.sirix.index.cas.xml;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.interfaces.immutable.ImmutableValueNode;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.sirix.index.cas.CASIndexListener;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class XmlCASIndexListener implements PathNodeKeyChangeListener {

  private final CASIndexListener indexListenerDelegate;

  public XmlCASIndexListener(final CASIndexListener indexListenerDelegate) {
    this.indexListenerDelegate = indexListenerDelegate;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    final String value;
    if (node instanceof ValueNode) {
      final ValueNode valueNode = ((ValueNode) node);
      value = valueNode.getValue();
    } else if (node instanceof ImmutableValueNode) {
      final ImmutableValueNode valueNode = ((ImmutableValueNode) node);
      value = valueNode.getValue();
    } else {
      value = null;
    }
    listen(type, node.getNodeKey(), node.getKind(), pathNodeKey, null, value == null
        ? null
        : new Str(value));
  }

  @Override
  public void listen(IndexController.ChangeType type, long nodeKey, NodeKind nodeKind, long pathNodeKey,
      @Nullable QNm name, @Nullable Str value) {
    if (value == null) {
      return;
    }
    switch (nodeKind) {
      case ATTRIBUTE, COMMENT, PROCESSING_INSTRUCTION, TEXT ->
        indexListenerDelegate.listen(type, nodeKey, pathNodeKey, value);
      default -> {
      }
    }
  }
}
