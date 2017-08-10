package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.delegates.NodeDelegate;

/**
 * If a node is deleted, it will be encapsulated over this class.
 * 
 * @author Sebastian Graf
 * 
 */
public final class DeweyIDMappingNode extends AbstractForwardingNode {

	/**
	 * Delegate for common data.
	 */
	private final NodeDelegate mDelegate;

	/**
	 * Constructor.
	 * 
	 * @param nodeDelegate node delegate
	 */
	public DeweyIDMappingNode(final NodeDelegate nodeDelegate) {
		mDelegate = checkNotNull(nodeDelegate);
	}

	@Override
	public Kind getKind() {
		return Kind.DEWEYIDMAPPING;
	}

	@Override
	public VisitResult acceptVisitor(Visitor visitor) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected NodeDelegate delegate() {
		return mDelegate;
	}
}
