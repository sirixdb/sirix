package io.sirix.node.immutable.json;

import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.StructNode;
import net.openhft.chronicle.bytes.Bytes;
import io.sirix.node.json.JsonDocumentRootNode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable document root node wrapper.
 *
 * @author Johannes Lichtenberger
 */
public final class ImmutableJsonDocumentRootNode extends AbstractImmutableJsonStructuralNode {

	/** Mutable {@link JsonDocumentRootNode} instance. */
	private final JsonDocumentRootNode node;

	/**
	 * Private constructor.
	 *
	 * @param node
	 *            mutable {@link JsonDocumentRootNode}
	 */
	private ImmutableJsonDocumentRootNode(final JsonDocumentRootNode node) {
		this.node = requireNonNull(node);
	}

	/**
	 * Get an immutable document root node instance.
	 *
	 * @param node
	 *            the mutable {@link JsonDocumentRootNode} to wrap
	 * @return immutable document root node instance
	 */
	public static ImmutableJsonDocumentRootNode of(final JsonDocumentRootNode node) {
		return new ImmutableJsonDocumentRootNode(node);
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
		return NodeKind.XML_DOCUMENT;
	}

	@Override
	public long computeHash(Bytes<ByteBuffer> bytes) {
		return node.computeHash(bytes);
	}
}
