package io.sirix.index.cas.xml;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.ChangeListener;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.interfaces.immutable.ImmutableValueNode;
import io.brackit.query.atomic.Str;
import io.sirix.index.cas.CASIndexListener;

public final class XmlCASIndexListener implements ChangeListener {

	private final CASIndexListener indexListenerDelegate;

	public XmlCASIndexListener(final CASIndexListener indexListenerDelegate) {
		this.indexListenerDelegate = indexListenerDelegate;
	}

	@Override
	public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
		if (node instanceof ValueNode) {
			final ValueNode valueNode = ((ValueNode) node);

			indexListenerDelegate.listen(type, valueNode, pathNodeKey, new Str(valueNode.getValue()));
		} else if (node instanceof ImmutableValueNode) {
			final ImmutableValueNode valueNode = ((ImmutableValueNode) node);

			indexListenerDelegate.listen(type, node, pathNodeKey, new Str(valueNode.getValue()));
		}
	}
}
