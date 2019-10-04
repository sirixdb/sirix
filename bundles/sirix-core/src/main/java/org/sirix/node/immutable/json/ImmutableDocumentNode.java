package org.sirix.node.immutable.json;

import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.JsonDocumentRootNode;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable document root node wrapper.
 *
 * @author Johannes Lichtenberger
 */
public final class ImmutableDocumentNode extends AbstractImmutableJsonStructuralNode {

  /** Mutable {@link JsonDocumentRootNode} instance. */
  private final JsonDocumentRootNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link JsonDocumentRootNode}
   */
  private ImmutableDocumentNode(final JsonDocumentRootNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable document root node instance.
   *
   * @param node the mutable {@link JsonDocumentRootNode} to wrap
   * @return immutable document root node instance
   */
  public static ImmutableDocumentNode of(final JsonDocumentRootNode node) {
    return new ImmutableDocumentNode(node);
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
    return NodeKind.XDM_DOCUMENT;
  }

  @Override
  public BigInteger computeHash() {
    return mNode.computeHash();
  }
}
