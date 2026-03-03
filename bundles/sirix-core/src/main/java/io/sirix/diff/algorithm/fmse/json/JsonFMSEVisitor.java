package io.sirix.diff.algorithm.fmse.json;

import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.node.NodeKind;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import io.sirix.node.immutable.json.ImmutableNullNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectBooleanNode;
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.immutable.json.ImmutableObjectNullNode;
import io.sirix.node.immutable.json.ImmutableObjectNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectStringNode;
import io.sirix.node.immutable.json.ImmutableStringNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import it.unimi.dsi.fastutil.longs.Long2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import static java.util.Objects.requireNonNull;

/**
 * JSON FMSE visitor that initializes the {@code inOrder} and {@code descendants} data structures
 * during post-order traversal. Uses fastutil primitive collections for HFT-grade performance.
 */
public final class JsonFMSEVisitor extends AbstractJsonNodeVisitor {

  private final JsonNodeReadOnlyTrx rtx;
  private final Long2BooleanOpenHashMap inOrder;
  private final Long2LongOpenHashMap descendants;

  /**
   * Constructor.
   *
   * @param rtx         the read-only transaction cursor
   * @param inOrder     map tracking whether nodes are "in order"
   * @param descendants map tracking descendant counts per node
   */
  public JsonFMSEVisitor(final JsonNodeReadOnlyTrx rtx, final Long2BooleanOpenHashMap inOrder,
      final Long2LongOpenHashMap descendants) {
    this.rtx = requireNonNull(rtx);
    this.inOrder = requireNonNull(inOrder);
    this.descendants = requireNonNull(descendants);
  }

  // ==================== Inner node visitors ====================

  @Override
  public VisitResultType visit(final ImmutableObjectNode node) {
    return visitInnerNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableArrayNode node) {
    return visitInnerNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableObjectKeyNode node) {
    return visitInnerNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableJsonDocumentRootNode node) {
    return visitInnerNode(node);
  }

  // ==================== Leaf node visitors ====================

  @Override
  public VisitResultType visit(final ImmutableStringNode node) {
    return visitLeafNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableNumberNode node) {
    return visitLeafNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableBooleanNode node) {
    return visitLeafNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableNullNode node) {
    return visitLeafNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableObjectStringNode node) {
    return visitLeafNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableObjectNumberNode node) {
    return visitLeafNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableObjectBooleanNode node) {
    return visitLeafNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableObjectNullNode node) {
    return visitLeafNode(node);
  }

  // ==================== Internal methods ====================

  private VisitResultType visitInnerNode(final ImmutableNode node) {
    rtx.moveTo(node.getNodeKey());
    countDescendants();
    return VisitResultType.CONTINUE;
  }

  private VisitResultType visitLeafNode(final ImmutableNode node) {
    rtx.moveTo(node.getNodeKey());
    inOrder.put(rtx.getNodeKey(), false);
    descendants.put(rtx.getNodeKey(), 1L);
    return VisitResultType.CONTINUE;
  }

  /**
   * Count descendants of the current inner node. Inner node children (OBJECT, ARRAY, OBJECT_KEY)
   * get +1 for themselves, mirroring the XML pattern where ELEMENT children get +1.
   */
  private void countDescendants() {
    long desc = 0;
    final long nodeKey = rtx.getNodeKey();
    if (rtx.hasFirstChild()) {
      rtx.moveToFirstChild();
      do {
        desc += descendants.get(rtx.getNodeKey());
        final NodeKind kind = rtx.getKind();
        if (kind == NodeKind.OBJECT || kind == NodeKind.ARRAY || kind == NodeKind.OBJECT_KEY) {
          desc += 1;
        }
      } while (rtx.hasRightSibling() && rtx.moveToRightSibling());
    }
    rtx.moveTo(nodeKey);
    inOrder.put(rtx.getNodeKey(), false);
    descendants.put(rtx.getNodeKey(), desc);
  }
}
