package org.sirix.index;

import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import javax.annotation.Nonnull;

public interface ChangeListener {
  void listen(ChangeType type, @Nonnull ImmutableNode node, long pathNodeKey);
}
