package io.sirix.index.cas.json;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.ChangeListener;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.brackit.query.atomic.Str;
import io.sirix.index.cas.CASIndexListener;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.ObjectNumberNode;

public final class JsonCASIndexListener implements ChangeListener {

	private final CASIndexListener indexListenerDelegate;

	public JsonCASIndexListener(final CASIndexListener indexListenerDelegate) {
		this.indexListenerDelegate = indexListenerDelegate;
	}

	@Override
	public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
		switch (node.getKind()) {
			case STRING_VALUE, OBJECT_STRING_VALUE -> {
				final ValueNode valueNode = ((ValueNode) node);
				indexListenerDelegate.listen(type, valueNode, pathNodeKey, new Str(valueNode.getValue()));
			}
			case BOOLEAN_VALUE -> indexListenerDelegate.listen(type, node, pathNodeKey,
					new Str(String.valueOf(((BooleanNode) node).getValue())));
			case OBJECT_BOOLEAN_VALUE -> indexListenerDelegate.listen(type, node, pathNodeKey,
					new Str(String.valueOf(((ObjectBooleanNode) node).getValue())));
			case NUMBER_VALUE -> indexListenerDelegate.listen(type, node, pathNodeKey,
					new Str(String.valueOf(((NumberNode) node).getValue())));
			case OBJECT_NUMBER_VALUE -> indexListenerDelegate.listen(type, node, pathNodeKey,
					new Str(String.valueOf(((ObjectNumberNode) node).getValue())));
		}
	}
}
