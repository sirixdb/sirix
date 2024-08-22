package io.sirix.node.interfaces.immutable;

import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;

public interface ImmutableJsonNode extends ImmutableNode {
	/**
	 * Accept a visitor and use double dispatching to invoke the visitor method.
	 *
	 * @param visitor
	 *            implementation of the {@link JsonNodeVisitor} interface
	 * @return the result of a visit
	 */
	VisitResult acceptVisitor(JsonNodeVisitor visitor);
}
