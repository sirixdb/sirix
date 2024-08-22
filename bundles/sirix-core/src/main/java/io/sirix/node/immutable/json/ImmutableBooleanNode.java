package io.sirix.node.immutable.json;

import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.StructNode;
import net.openhft.chronicle.bytes.Bytes;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.xml.TextNode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable JSONBooleanNode wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableBooleanNode extends AbstractImmutableJsonStructuralNode {
	/** Mutable {@link BooleanNode}. */
	private final BooleanNode node;

	/**
	 * Private constructor.
	 *
	 * @param node
	 *            {@link StringNode} to wrap
	 */
	private ImmutableBooleanNode(final BooleanNode node) {
		this.node = requireNonNull(node);
	}

	/**
	 * Get an immutable text node instance.
	 *
	 * @param node
	 *            the mutable {@link TextNode} to wrap
	 * @return immutable text node instance
	 */
	public static ImmutableBooleanNode of(final BooleanNode node) {
		return new ImmutableBooleanNode(node);
	}

	@Override
	public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
		return visitor.visit(this);
	}

	@Override
	public StructNode structDelegate() {
		return node.getStructNodeDelegate();
	}

	public boolean getValue() {
		return node.getValue();
	}

	@Override
	public NodeKind getKind() {
		return NodeKind.BOOLEAN_VALUE;
	}

	@Override
	public long computeHash(Bytes<ByteBuffer> bytes) {
		return node.computeHash(bytes);
	}

}
