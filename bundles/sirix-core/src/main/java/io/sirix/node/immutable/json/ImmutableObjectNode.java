package io.sirix.node.immutable.json;

import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.StructNode;
import net.openhft.chronicle.bytes.Bytes;
import io.sirix.node.json.ObjectNode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable JSONObject wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableObjectNode extends AbstractImmutableJsonStructuralNode {

	/** Mutable {@link ObjectNode}. */
	private final ObjectNode node;

	/**
	 * Private constructor.
	 *
	 * @param node
	 *            mutable {@link ObjectNode}
	 */
	private ImmutableObjectNode(final ObjectNode node) {
		this.node = requireNonNull(node);
	}

	/**
	 * Get an immutable JSON-array node instance.
	 *
	 * @param node
	 *            the mutable {@link ImmutableObjectNode} to wrap
	 * @return immutable JSON-array node instance
	 */
	public static ImmutableObjectNode of(final ObjectNode node) {
		return new ImmutableObjectNode(node);
	}

	@Override
	public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
		return visitor.visit(this);
	}

	@Override
	public StructNode structDelegate() {
		return node;
	}

	@Override
	public NodeKind getKind() {
		return NodeKind.ARRAY;
	}

	@Override
	public long computeHash(Bytes<ByteBuffer> bytes) {
		return node.computeHash(bytes);
	}
}
