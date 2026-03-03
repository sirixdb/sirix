package io.sirix.diff.algorithm.fmse.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.diff.algorithm.fmse.NodeComparator;
import io.sirix.node.NodeKind;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

/**
 * Compares JSON inner nodes (OBJECT, ARRAY, OBJECT_KEY) for the FMSE matching algorithm. Uses
 * descendant overlap ratio and key name similarity for OBJECT_KEY nodes.
 */
final class JsonInnerNodeComparator implements NodeComparator<Long> {

  private static final double FMESTHRESHOLD = 0.5;
  private static final double KEY_NAME_THRESHOLD = 0.7;

  private final JsonMatching matching;
  private final JsonNodeReadOnlyTrx oldRtx;
  private final JsonNodeReadOnlyTrx newRtx;
  private final JsonFMSENodeComparisonUtils comparisonUtils;
  private final Long2LongOpenHashMap descendantsOldRev;
  private final Long2LongOpenHashMap descendantsNewRev;

  JsonInnerNodeComparator(final JsonMatching matching, final JsonNodeReadOnlyTrx oldRtx,
      final JsonNodeReadOnlyTrx newRtx, final JsonFMSENodeComparisonUtils comparisonUtils,
      final Long2LongOpenHashMap descendantsOldRev, final Long2LongOpenHashMap descendantsNewRev) {
    this.matching = matching;
    this.oldRtx = oldRtx;
    this.newRtx = newRtx;
    this.comparisonUtils = comparisonUtils;
    this.descendantsOldRev = descendantsOldRev;
    this.descendantsNewRev = descendantsNewRev;
  }

  @Override
  public boolean isEqual(final Long firstNode, final Long secondNode) {
    assert firstNode != null;
    assert secondNode != null;

    oldRtx.moveTo(firstNode);
    newRtx.moveTo(secondNode);

    assert oldRtx.getKind() == newRtx.getKind();

    final NodeKind kind = oldRtx.getKind();

    if (kind == NodeKind.OBJECT_KEY) {
      return isObjectKeyEqual(firstNode, secondNode);
    }

    // OBJECT or ARRAY comparison: based on descendant overlap.
    final boolean oldHasChildren = oldRtx.hasFirstChild();
    final boolean newHasChildren = newRtx.hasFirstChild();

    if (oldHasChildren && newHasChildren) {
      return checkDescendantOverlap(firstNode, secondNode);
    } else if (!oldHasChildren && !newHasChildren) {
      // Both empty containers of the same kind → match.
      return true;
    } else {
      // One empty, one not → no match.
      return false;
    }
  }

  private boolean isObjectKeyEqual(final long firstNode, final long secondNode) {
    // Compare key names first.
    final double nameRatio = JsonFMSENodeComparisonUtils.calculateRatio(
        oldRtx.getName().getLocalName(), newRtx.getName().getLocalName());
    if (nameRatio < KEY_NAME_THRESHOLD) {
      return false;
    }

    // If both have children, check descendant overlap.
    final boolean oldHasChildren = oldRtx.hasFirstChild();
    final boolean newHasChildren = newRtx.hasFirstChild();

    if (oldHasChildren && newHasChildren) {
      return checkDescendantOverlap(firstNode, secondNode);
    }
    // If neither has children (or one has and one doesn't), match by name only.
    return true;
  }

  private boolean checkDescendantOverlap(final long firstNode, final long secondNode) {
    final long common = matching.containedDescendants(firstNode, secondNode);
    final long descOld = descendantsOldRev.get(firstNode);
    final long descNew = descendantsNewRev.get(secondNode);
    final long maxFamilySize = Math.max(descOld, descNew);

    // Division-by-zero guard: both containers are empty.
    if (maxFamilySize == 0) {
      return true;
    }

    return ((double) common / (double) maxFamilySize) >= FMESTHRESHOLD;
  }
}
