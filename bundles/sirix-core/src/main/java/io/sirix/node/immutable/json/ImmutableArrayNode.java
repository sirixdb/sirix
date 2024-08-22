package io.sirix.node.immutable.json;

import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.xml.ElementNode;
import net.openhft.chronicle.bytes.Bytes;
import io.sirix.node.json.ArrayNode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable array node wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableArrayNode extends AbstractImmutableJsonStructuralNode {

	/** Mutable {@link ArrayNode}. */
	private final ArrayNode node;

	/**
	 * Private constructor.
	 *
	 * @param node
	 *            mutable {@link ElementNode}
	 */
	private ImmutableArrayNode(final ArrayNode node) {
		this.node = requireNonNull(node);
	}

	/**
	 * Get a path node key.
	 *
	 * @return path node key
	 */
	public long getPathNodeKey() {
		return node.getPathNodeKey();
	}

	@Override
	public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
		return visitor.visit(this);
	}

	/**
	 * Get an immutable JSON-array node instance.
	 *
	 * @param node
	 *            the mutable {@link ImmutableArrayNode} to wrap
	 * @return immutable JSON-array node instance
	 */
	public static ImmutableArrayNode of(final ArrayNode node) {
		return new ImmutableArrayNode(node);
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
