package org.sirix.node.immutable.json;

import net.openhft.chronicle.bytes.Bytes;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.node.json.ObjectNode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable JSONObject wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableObjectKeyNode extends AbstractImmutableJsonStructuralNode implements ImmutableNameNode {

  /** Mutable {@link ObjectNode}. */
  private final ObjectKeyNode node;

  /**
   * Private constructor.
   *
   * @param node mutable {@link ObjectNode}
   */
  private ImmutableObjectKeyNode(final ObjectKeyNode node) {
    this.node = requireNonNull(node);
  }

  /**
   * Get an immutable JSON-array node instance.
   *
   * @param node the mutable {@link ImmutableObjectKeyNode} to wrap
   * @return immutable JSON-array node instance
   */
  public static ImmutableObjectKeyNode of(final ObjectKeyNode node) {
    return new ImmutableObjectKeyNode(node);
  }

  @Override
  public int getLocalNameKey() {
    return node.getLocalNameKey();
  }

  @Override
  public int getPrefixKey() {
    return node.getPrefixKey();
  }

  @Override
  public int getURIKey() {
    return node.getURIKey();
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

  @Override
  public StructNode structDelegate() {
    return node;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT_KEY;
  }

  public int getNameKey() {
    return node.getNameKey();
  }

  @Override
  public QNm getName() {
    return node.getName();
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    return node.computeHash(bytes);
  }
}
