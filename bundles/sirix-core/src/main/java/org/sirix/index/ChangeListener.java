package org.sirix.index;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.node.interfaces.immutable.ImmutableNode;

public interface ChangeListener {
  void listen(ChangeType type, @NonNull ImmutableNode node, long pathNodeKey);
}
