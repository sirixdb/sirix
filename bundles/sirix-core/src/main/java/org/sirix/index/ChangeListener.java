package org.sirix.index;

import javax.annotation.Nonnull;

import org.sirix.access.IndexController.ChangeType;
import org.sirix.exception.SirixIOException;
import org.sirix.node.interfaces.immutable.ImmutableNode;

public interface ChangeListener {
	void listen(@Nonnull ChangeType type, @Nonnull ImmutableNode node, long pathNodeKey) throws SirixIOException;
}
