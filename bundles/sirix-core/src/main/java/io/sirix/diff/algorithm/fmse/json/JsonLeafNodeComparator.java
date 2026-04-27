package io.sirix.diff.algorithm.fmse.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.diff.algorithm.fmse.NodeComparator;
import io.sirix.node.NodeKind;

/**
 * Compares JSON leaf nodes for the FMSE matching algorithm. Uses typed comparisons for booleans,
 * numbers, nulls, and Levenshtein similarity for strings.
 */
final class JsonLeafNodeComparator implements NodeComparator<Long> {

  /** Threshold for considering two leaf nodes as equal. */
  public static final double FMESF = 0.5;

  private final JsonNodeReadOnlyTrx oldRtx;
  private final JsonNodeReadOnlyTrx newRtx;
  private final JsonFMSENodeComparisonUtils comparisonUtils;

  JsonLeafNodeComparator(final JsonNodeReadOnlyTrx oldRtx, final JsonNodeReadOnlyTrx newRtx,
      final JsonFMSENodeComparisonUtils comparisonUtils) {
    this.oldRtx = oldRtx;
    this.newRtx = newRtx;
    this.comparisonUtils = comparisonUtils;
  }

  @Override
  public boolean isEqual(final Long firstNode, final Long secondNode) {
    assert firstNode != null;
    assert secondNode != null;

    oldRtx.moveTo(firstNode);
    newRtx.moveTo(secondNode);

    assert oldRtx.getKind() == newRtx.getKind();

    double ratio = computeValueRatio();

    if (ratio > FMESF) {
      // Check parent context: for OBJECT_* values, parent is OBJECT_KEY; for fused kinds
      // the NODE ITSELF carries the name so we use its name directly (parent is OBJECT).
      // Cursors already at firstNode/secondNode — computeValueRatio() does not move them.
      final NodeKind oldKind = oldRtx.getKind();
      if (oldKind == NodeKind.OBJECT_NAMED_BOOLEAN || oldKind == NodeKind.OBJECT_NAMED_NUMBER
          || oldKind == NodeKind.OBJECT_NAMED_STRING || oldKind == NodeKind.OBJECT_NAMED_NULL) {
        final var oldName = oldRtx.getName();
        final var newName = newRtx.getName();
        if (oldName != null && newName != null) {
          ratio = JsonFMSENodeComparisonUtils.calculateRatio(
              oldName.getLocalName(), newName.getLocalName());
        }
      } else if (oldRtx.hasParent() && newRtx.hasParent()) {
        oldRtx.moveToParent();
        newRtx.moveToParent();

        if (oldRtx.getKind().playsObjectKeyRole() && newRtx.getKind().playsObjectKeyRole()) {
          ratio = JsonFMSENodeComparisonUtils.calculateRatio(
              oldRtx.getName().getLocalName(), newRtx.getName().getLocalName());
        }
      }

      if (ratio > FMESF) {
        // Check ancestor path compatibility.
        oldRtx.moveTo(firstNode);
        newRtx.moveTo(secondNode);
        if (!comparisonUtils.checkAncestors(oldRtx.getNodeKey(), newRtx.getNodeKey())) {
          ratio = 0;
        }
      }
    }

    // Restore cursor positions.
    oldRtx.moveTo(firstNode);
    newRtx.moveTo(secondNode);

    return ratio > FMESF;
  }

  /**
   * Computes the value similarity ratio using typed comparison.
   */
  private double computeValueRatio() {
    final NodeKind kind = oldRtx.getKind();
    return switch (kind) {
      case NULL_VALUE, OBJECT_NAMED_NULL -> 1.0;
      case BOOLEAN_VALUE, OBJECT_NAMED_BOOLEAN ->
          oldRtx.getBooleanValue() == newRtx.getBooleanValue() ? 1.0 : 0.0;
      case NUMBER_VALUE, OBJECT_NAMED_NUMBER ->
          oldRtx.getNumberValue().doubleValue() == newRtx.getNumberValue().doubleValue() ? 1.0 : 0.0;
      case STRING_VALUE, OBJECT_NAMED_STRING ->
          JsonFMSENodeComparisonUtils.calculateRatio(oldRtx.getValue(), newRtx.getValue());
      default -> 0.0;
    };
  }
}
