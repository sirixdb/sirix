package io.sirix.node.immutable.json;

import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.xml.ElementNode;
import net.openhft.chronicle.bytes.Bytes;
import io.sirix.node.json.NullNode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable element wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableNullNode extends AbstractImmutableJsonStructuralNode {

	/** Mutable {@link NullNode}. */
	private final NullNode node;

	/**
	 * Private constructor.
	 *
	 * @param node
	 *            mutable {@link ElementNode}
	 */
	private ImmutableNullNode(final NullNode node) {
		this.node = requireNonNull(node);
	}

	/**
	 * Get an immutable JSON-array node instance.
	 *
	 * @param node
	 *            the mutable {@link ImmutableNullNode} to wrap
	 * @return immutable JSON-array node instance
	 */
	public static ImmutableNullNode of(final NullNode node) {
		return new ImmutableNullNode(node);
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
		return NodeKind.NULL_VALUE;
	}

	@Override
	public long computeHash(Bytes<ByteBuffer> bytes) {
		return node.computeHash(bytes);
	}
}
