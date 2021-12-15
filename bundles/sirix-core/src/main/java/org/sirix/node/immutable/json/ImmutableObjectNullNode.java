package org.sirix.node.immutable.json;

import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.NullNode;
import org.sirix.node.json.ObjectNullNode;
import org.sirix.node.xml.ElementNode;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable element wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableObjectNullNode extends AbstractImmutableJsonStructuralNode {

  /** Mutable {@link ObjectNullNode}. */
  private final ObjectNullNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link ElementNode}
   */
  private ImmutableObjectNullNode(final ObjectNullNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable JSON-array node instance.
   *
   * @param node the mutable {@link ImmutableObjectNullNode} to wrap
   * @return immutable JSON-array node instance
   */
  public static ImmutableObjectNullNode of(final ObjectNullNode node) {
    return new ImmutableObjectNullNode(node);
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public StructNode structDelegate() {
    return mNode;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.NULL_VALUE;
  }

  @Override
  public BigInteger computeHash() {
    return mNode.computeHash();
  }
}
