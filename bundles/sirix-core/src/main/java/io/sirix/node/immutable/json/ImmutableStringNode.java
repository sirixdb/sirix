package io.sirix.node.immutable.json;

import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableValueNode;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.nullness.qual.Nullable;
import io.sirix.node.json.StringNode;
import io.sirix.node.xml.TextNode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable JSONValueString wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableStringNode extends AbstractImmutableJsonStructuralNode implements ImmutableValueNode {
  /** Mutable {@link StringNode}. */
  private final StringNode node;

  /**
   * Private constructor.
   *
   * @param node {@link StringNode} to wrap
   */
  private ImmutableStringNode(final StringNode node) {
    this.node = requireNonNull(node);
  }

  /**
   * Get an immutable text node instance.
   *
   * @param node the mutable {@link TextNode} to wrap
   * @return immutable text node instance
   */
  public static ImmutableStringNode of(final StringNode node) {
    return new ImmutableStringNode(node);
  }

  @Override
  public ImmutableStringNode clone() {
    return new ImmutableStringNode(node.clone());
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return node.isSameItem(other);
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public long getParentKey() {
    return node.getParentKey();
  }

  @Override
  public boolean hasParent() {
    return node.hasParent();
  }

  @Override
  public long getNodeKey() {
    return node.getNodeKey();
  }

  @Override
  public NodeKind getKind() {
    return node.getKind();
  }

  @Override
  public int getPreviousRevisionNumber() {
    return node.getPreviousRevisionNumber();
  }

  @Override
  public boolean hasFirstChild() {
    return node.hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    return node.hasLastChild();
  }

  @Override
  public boolean hasLeftSibling() {
    return node.hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    return node.hasRightSibling();
  }

  @Override
  public long getChildCount() {
    return node.getChildCount();
  }

  @Override
  public long getDescendantCount() {
    return node.getDescendantCount();
  }

  @Override
  public long getFirstChildKey() {
    return node.getFirstChildKey();
  }

  @Override
  public long getLastChildKey() {
    return node.getLastChildKey();
  }

  @Override
  public long getLeftSiblingKey() {
    return node.getLeftSiblingKey();
  }

  @Override
  public long getRightSiblingKey() {
    return node.getRightSiblingKey();
  }

  @Override
  public byte[] getRawValue() {
    return node.getRawValue();
  }

  @Override
  public String getValue() {
    return node.getValue();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ImmutableStringNode)) {
      return false;
    }
    return node.equals(obj);
  }

  @Override
  public int hashCode() {
    return node.hashCode();
  }

  @Override
  public String toString() {
    return node.toString();
  }

  @Override
  public StructNode structDelegate() {
    return node.getStructNodeDelegate();
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    return node.computeHash(bytes);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return node.getDeweyID();
  }
}
