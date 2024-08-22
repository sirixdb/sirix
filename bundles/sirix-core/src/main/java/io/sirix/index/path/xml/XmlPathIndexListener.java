package io.sirix.index.path.xml;

import io.sirix.access.trx.node.IndexController;
import io.sirix.node.interfaces.NameNode;
import io.sirix.index.ChangeListener;
import io.sirix.index.path.PathIndexListener;
import io.sirix.node.interfaces.immutable.ImmutableNode;

final class XmlPathIndexListener implements ChangeListener {

	private final PathIndexListener mPathIndexListener;

	XmlPathIndexListener(final PathIndexListener pathIndexListenerDelegate) {
		mPathIndexListener = pathIndexListenerDelegate;
	}

	@Override
	public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
		if (node instanceof NameNode) {
			mPathIndexListener.listen(type, node, pathNodeKey);
		}
	}
}
