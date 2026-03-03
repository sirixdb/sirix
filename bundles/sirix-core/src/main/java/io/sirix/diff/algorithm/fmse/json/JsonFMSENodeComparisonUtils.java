package io.sirix.diff.algorithm.fmse.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.diff.algorithm.fmse.Levenshtein;
import io.sirix.diff.algorithm.fmse.Util;
import io.sirix.node.NodeKind;

/**
 * JSON-specific node comparison utilities for the FMSE algorithm. Provides typed value extraction,
 * ratio calculation, and ancestor path comparison.
 */
public final class JsonFMSENodeComparisonUtils {

  private static final int MAX_LENGTH = 50;
  private static final double ANCESTOR_NAME_THRESHOLD = 0.7;

  private final long oldStartKey;
  private final long newStartKey;
  private final JsonNodeReadOnlyTrx oldRtx;
  private final JsonNodeReadOnlyTrx newRtx;

  public JsonFMSENodeComparisonUtils(final long oldStartKey, final long newStartKey,
      final JsonNodeReadOnlyTrx oldRtx, final JsonNodeReadOnlyTrx newRtx) {
    this.oldStartKey = oldStartKey;
    this.newStartKey = newStartKey;
    this.oldRtx = oldRtx;
    this.newRtx = newRtx;
  }

  /**
   * Gets the string representation of a node's value for comparison purposes.
   *
   * @param nodeKey the node key
   * @param rtx     the read-only transaction
   * @return the node value as a string
   */
  public static String getNodeValue(final long nodeKey, final JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(nodeKey);
    return switch (rtx.getKind()) {
      case OBJECT_KEY -> rtx.getName().getLocalName();
      case STRING_VALUE, OBJECT_STRING_VALUE -> rtx.getValue();
      case NUMBER_VALUE, OBJECT_NUMBER_VALUE -> rtx.getValue();
      case BOOLEAN_VALUE, OBJECT_BOOLEAN_VALUE -> String.valueOf(rtx.getBooleanValue());
      case NULL_VALUE, OBJECT_NULL_VALUE -> "null";
      case OBJECT, ARRAY, JSON_DOCUMENT -> "";
      default -> "";
    };
  }

  /**
   * Checks if two nodes have equal typed values (faster than string comparison for typed nodes).
   *
   * @param firstKey  first node key
   * @param secondKey second node key
   * @param oldRtx    old revision transaction
   * @param newRtx    new revision transaction
   * @return true if values are equal
   */
  public static boolean typedValuesEqual(final long firstKey, final long secondKey,
      final JsonNodeReadOnlyTrx oldRtx, final JsonNodeReadOnlyTrx newRtx) {
    oldRtx.moveTo(firstKey);
    newRtx.moveTo(secondKey);

    if (oldRtx.getKind() != newRtx.getKind()) {
      return false;
    }

    return switch (oldRtx.getKind()) {
      case NULL_VALUE, OBJECT_NULL_VALUE -> true;
      case BOOLEAN_VALUE, OBJECT_BOOLEAN_VALUE -> oldRtx.getBooleanValue() == newRtx.getBooleanValue();
      case NUMBER_VALUE, OBJECT_NUMBER_VALUE ->
          oldRtx.getNumberValue().doubleValue() == newRtx.getNumberValue().doubleValue();
      case STRING_VALUE, OBJECT_STRING_VALUE -> oldRtx.getValue().equals(newRtx.getValue());
      case OBJECT_KEY -> oldRtx.getName().getLocalName().equals(newRtx.getName().getLocalName());
      default -> false;
    };
  }

  /**
   * Calculates the similarity ratio between two strings using Levenshtein distance or quick ratio.
   *
   * @param first  the first string
   * @param second the second string
   * @return similarity ratio in [0, 1]
   */
  public static double calculateRatio(final String first, final String second) {
    if (first == null || second == null) {
      return 0.0;
    }
    if (first.equals(second)) {
      return 1.0;
    }
    if (first.length() > MAX_LENGTH || second.length() > MAX_LENGTH) {
      return Util.quickRatio(first, second);
    }
    return Levenshtein.getSimilarity(first, second);
  }

  /**
   * Checks if the ancestor paths of two nodes are compatible. For OBJECT_KEY ancestors, compares
   * key names with a threshold of 0.7.
   *
   * @param nodeX node key in old revision
   * @param nodeY node key in new revision
   * @return true if ancestor paths are compatible
   */
  public boolean checkAncestors(final long nodeX, final long nodeY) {
    oldRtx.moveTo(nodeX);
    newRtx.moveTo(nodeY);

    // Walk up the ancestor chains in parallel.
    while (oldRtx.hasParent() && newRtx.hasParent()) {
      oldRtx.moveToParent();
      newRtx.moveToParent();

      // Reached start keys — ancestor paths match up to the root of interest.
      if (oldRtx.getNodeKey() == oldStartKey && newRtx.getNodeKey() == newStartKey) {
        return true;
      }

      if (oldRtx.getKind() != newRtx.getKind()) {
        return false;
      }

      // For OBJECT_KEY ancestors, compare key names.
      if (oldRtx.getKind() == NodeKind.OBJECT_KEY) {
        final double ratio = calculateRatio(
            oldRtx.getName().getLocalName(), newRtx.getName().getLocalName());
        if (ratio < ANCESTOR_NAME_THRESHOLD) {
          return false;
        }
      }
    }

    return !oldRtx.hasParent() && !newRtx.hasParent();
  }
}
