package io.sirix.diff.algorithm.fmse.json;

import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.diff.algorithm.fmse.ConnectionMap;
import io.sirix.node.NodeKind;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.checkerframework.checker.index.qual.NonNegative;

/**
 * HFT-grade matching data structure for JSON FMSE. Uses fastutil's {@link Long2LongOpenHashMap}
 * with {@code defaultReturnValue(-1L)} for zero-boxing partner lookups.
 */
public final class JsonMatching {

  /** Sentinel value indicating no partner exists. */
  public static final long NO_PARTNER = -1L;

  /** Forward mapping: old node key → new node key. */
  private final Long2LongOpenHashMap mapping;

  /** Reverse mapping: new node key → old node key. */
  private final Long2LongOpenHashMap reverseMapping;

  /**
   * Tracks the (grand-)parent-child relation of nodes for fast subtree containment checks.
   */
  private final ConnectionMap<Long> isInSubtree;

  /** Read-only transaction on old revision. */
  private final JsonNodeReadOnlyTrx rtxOld;

  /** Read-only transaction on new revision. */
  private final JsonNodeReadOnlyTrx rtxNew;

  /**
   * Creates a new matching with pre-sized maps.
   *
   * @param rtxOld read-only transaction on old revision
   * @param rtxNew read-only transaction on new revision
   */
  public JsonMatching(final JsonNodeReadOnlyTrx rtxOld, final JsonNodeReadOnlyTrx rtxNew) {
    final int capacity = (int) Math.min(
        Math.max(rtxOld.getMaxNodeKey(), rtxNew.getMaxNodeKey()), 1 << 20);
    this.mapping = new Long2LongOpenHashMap(capacity);
    this.mapping.defaultReturnValue(NO_PARTNER);
    this.reverseMapping = new Long2LongOpenHashMap(capacity);
    this.reverseMapping.defaultReturnValue(NO_PARTNER);
    this.isInSubtree = new ConnectionMap<>();
    this.rtxOld = rtxOld;
    this.rtxNew = rtxNew;
  }

  /**
   * Copy constructor. Creates a new matching with the same state.
   *
   * @param other the original matching to copy
   */
  public JsonMatching(final JsonMatching other) {
    this.mapping = new Long2LongOpenHashMap(other.mapping);
    this.mapping.defaultReturnValue(NO_PARTNER);
    this.reverseMapping = new Long2LongOpenHashMap(other.reverseMapping);
    this.reverseMapping.defaultReturnValue(NO_PARTNER);
    this.isInSubtree = new ConnectionMap<>(other.isInSubtree);
    this.rtxOld = other.rtxOld;
    this.rtxNew = other.rtxNew;
  }

  /**
   * Adds the matching x → y. Both nodes must have the same {@link NodeKind}.
   *
   * @param nodeX source node key (in old revision)
   * @param nodeY partner node key (in new revision)
   */
  public void add(final @NonNegative long nodeX, final @NonNegative long nodeY) {
    rtxOld.moveTo(nodeX);
    rtxNew.moveTo(nodeY);
    if (rtxOld.getKind() != rtxNew.getKind()) {
      throw new AssertionError(
          "Matched nodes must have same kind: " + rtxOld.getKind() + " vs " + rtxNew.getKind());
    }
    mapping.put(nodeX, nodeY);
    reverseMapping.put(nodeY, nodeX);
    updateSubtreeMap(nodeX, rtxOld);
    updateSubtreeMap(nodeY, rtxNew);
  }

  /**
   * Removes the matching for the given old-revision node.
   *
   * @param nodeX source node key
   * @return true if the matching was removed
   */
  public boolean remove(final @NonNegative long nodeX) {
    final long partner = mapping.remove(nodeX);
    if (partner != NO_PARTNER) {
      reverseMapping.remove(partner);
      return true;
    }
    return false;
  }

  /**
   * Updates the subtree map: for each ancestor of {@code key}, records that {@code key} is in its
   * subtree.
   */
  private void updateSubtreeMap(final @NonNegative long key, final JsonNodeReadOnlyTrx rtx) {
    isInSubtree.set(key, key, true);
    rtx.moveTo(key);
    while (rtx.hasParent()) {
      rtx.moveToParent();
      isInSubtree.set(rtx.getNodeKey(), key, true);
    }
    rtx.moveTo(key);
  }

  /**
   * Checks if the matching contains the pair (nodeX, nodeY).
   *
   * @param nodeX source node key
   * @param nodeY expected partner node key
   * @return true iff {@code add(nodeX, nodeY)} was called before
   */
  public boolean contains(final @NonNegative long nodeX, final @NonNegative long nodeY) {
    return mapping.get(nodeX) == nodeY;
  }

  /**
   * Counts the number of descendants in the subtrees of x and y that are also in the matching.
   * JSON-specific: no attribute/namespace iteration.
   *
   * @param nodeX first subtree root node (old revision)
   * @param nodeY second subtree root node (new revision)
   * @return number of matched descendants
   */
  public long containedDescendants(final @NonNegative long nodeX, final @NonNegative long nodeY) {
    long retVal = 0;
    rtxOld.moveTo(nodeX);
    for (final Axis axis = new DescendantAxis(rtxOld, IncludeSelf.YES); axis.hasNext();) {
      axis.nextLong();
      final long partnerKey = partner(rtxOld.getNodeKey());
      if (partnerKey != NO_PARTNER && isInSubtree.get(nodeY, partnerKey)) {
        retVal++;
      }
    }
    return retVal;
  }

  /**
   * Returns the partner (new revision) of the given old-revision node.
   *
   * @param node old-revision node key
   * @return partner node key, or {@link #NO_PARTNER} (-1L) if no partner exists
   */
  public long partner(final @NonNegative long node) {
    return mapping.get(node);
  }

  /**
   * Returns the reverse partner (old revision) of the given new-revision node.
   *
   * @param node new-revision node key
   * @return old-revision node key, or {@link #NO_PARTNER} (-1L) if no partner exists
   */
  public long reversePartner(final @NonNegative long node) {
    return reverseMapping.get(node);
  }

  /** Reset internal data structures. */
  public void reset() {
    mapping.clear();
    reverseMapping.clear();
    isInSubtree.reset();
  }
}
