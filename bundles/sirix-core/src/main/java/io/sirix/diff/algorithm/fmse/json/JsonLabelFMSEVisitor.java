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
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * JSON label visitor for FMSE. Collects inner and leaf node keys grouped by {@link NodeKind}.
 * Uses {@link EnumMap}{@code <NodeKind, LongArrayList>} for O(1) enum lookup and unboxed long
 * storage.
 */
public final class JsonLabelFMSEVisitor extends AbstractJsonNodeVisitor {

  private final JsonNodeReadOnlyTrx rtx;
  private final EnumMap<NodeKind, LongArrayList> labels;
  private final EnumMap<NodeKind, LongArrayList> leafLabels;

  public JsonLabelFMSEVisitor(final JsonNodeReadOnlyTrx rtx) {
    this.rtx = requireNonNull(rtx);
    this.labels = new EnumMap<>(NodeKind.class);
    this.leafLabels = new EnumMap<>(NodeKind.class);
  }

  // ==================== Inner node visitors ====================

  @Override
  public VisitResultType visit(final ImmutableObjectNode node) {
    addInnerLabel(node.getNodeKey(), NodeKind.OBJECT);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableArrayNode node) {
    addInnerLabel(node.getNodeKey(), NodeKind.ARRAY);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableObjectKeyNode node) {
    addInnerLabel(node.getNodeKey(), NodeKind.OBJECT_KEY);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableJsonDocumentRootNode node) {
    addInnerLabel(node.getNodeKey(), NodeKind.JSON_DOCUMENT);
    return VisitResultType.CONTINUE;
  }

  // ==================== Leaf node visitors ====================

  @Override
  public VisitResultType visit(final ImmutableStringNode node) {
    addLeafLabel(node.getNodeKey(), NodeKind.STRING_VALUE);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableNumberNode node) {
    addLeafLabel(node.getNodeKey(), NodeKind.NUMBER_VALUE);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableBooleanNode node) {
    addLeafLabel(node.getNodeKey(), NodeKind.BOOLEAN_VALUE);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableNullNode node) {
    addLeafLabel(node.getNodeKey(), NodeKind.NULL_VALUE);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableObjectStringNode node) {
    addLeafLabel(node.getNodeKey(), NodeKind.OBJECT_STRING_VALUE);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableObjectNumberNode node) {
    addLeafLabel(node.getNodeKey(), NodeKind.OBJECT_NUMBER_VALUE);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableObjectBooleanNode node) {
    addLeafLabel(node.getNodeKey(), NodeKind.OBJECT_BOOLEAN_VALUE);
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableObjectNullNode node) {
    addLeafLabel(node.getNodeKey(), NodeKind.OBJECT_NULL_VALUE);
    return VisitResultType.CONTINUE;
  }

  // ==================== Internal methods ====================

  private void addInnerLabel(final long nodeKey, final NodeKind kind) {
    labels.computeIfAbsent(kind, k -> new LongArrayList()).add(nodeKey);
  }

  private void addLeafLabel(final long nodeKey, final NodeKind kind) {
    leafLabels.computeIfAbsent(kind, k -> new LongArrayList()).add(nodeKey);
  }

  /**
   * Returns inner node labels grouped by kind.
   *
   * @return mutable map of inner labels
   */
  public Map<NodeKind, List<Long>> getLabels() {
    // EnumMap<NodeKind, LongArrayList> is compatible with Map<NodeKind, List<Long>>
    // because LongArrayList implements List<Long>
    @SuppressWarnings("unchecked")
    final Map<NodeKind, List<Long>> result = (Map<NodeKind, List<Long>>) (Map<?, ?>) labels;
    return result;
  }

  /**
   * Returns leaf node labels grouped by kind.
   *
   * @return mutable map of leaf labels
   */
  public Map<NodeKind, List<Long>> getLeafLabels() {
    @SuppressWarnings("unchecked")
    final Map<NodeKind, List<Long>> result = (Map<NodeKind, List<Long>>) (Map<?, ?>) leafLabels;
    return result;
  }
}
