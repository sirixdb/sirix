package io.sirix.index;

import io.sirix.access.trx.node.IndexController;
import io.sirix.node.interfaces.immutable.ImmutableNode;

public interface ChangeListener {
  void listen(IndexController.ChangeType type, ImmutableNode node, long pathNodeKey);
}
