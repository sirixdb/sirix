package io.sirix.index;

import io.sirix.access.trx.node.IndexController;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.node.interfaces.immutable.ImmutableNode;

public interface ChangeListener {
	void listen(IndexController.ChangeType type, @NonNull ImmutableNode node, long pathNodeKey);
}
