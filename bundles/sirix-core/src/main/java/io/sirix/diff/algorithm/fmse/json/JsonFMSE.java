package io.sirix.diff.algorithm.fmse.json;

import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.LevelOrderAxis;
import io.sirix.axis.PostOrderAxis;
import io.sirix.axis.visitor.DeleteJsonFMSEVisitor;
import io.sirix.axis.visitor.VisitorDescendantAxis;
import io.sirix.diff.algorithm.JsonImportDiff;
import io.sirix.diff.algorithm.fmse.FMSEAlgorithm;
import io.sirix.diff.algorithm.fmse.NodeComparator;
import io.sirix.diff.algorithm.fmse.Util;
import io.sirix.exception.SirixException;
import io.sirix.node.NodeKind;
import io.sirix.utils.LogWrapper;
import io.sirix.utils.Pair;
import it.unimi.dsi.fastutil.longs.Long2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * JSON implementation of the Fast Match / Edit Script (FMSE) algorithm from Chawathe et al. 1996.
 * Computes a minimum-cost edit script (with move detection) between two JSON revisions. Uses
 * fastutil primitive collections throughout for HFT-grade zero-boxing performance.
 */
public final class JsonFMSE implements JsonImportDiff, AutoCloseable {

  private static final LogWrapper LOGWRAPPER =
      new LogWrapper(LoggerFactory.getLogger(JsonFMSE.class));

  private static final String NAME = "Fast Matching / Edit Script (JSON)";

  /**
   * Tracks which nodes have already been inserted (for subtree insertions — prevents double
   * insertion).
   */
  private Long2BooleanOpenHashMap alreadyInserted;

  /** The total matching M' between nodes as described in the paper. */
  private JsonMatching totalMatching;

  /** Stores the in-order property for each node in the old revision. */
  private Long2BooleanOpenHashMap inOrderOldRev;

  /** Stores the in-order property for each node in the new revision. */
  private Long2BooleanOpenHashMap inOrderNewRev;

  /** Number of descendants in subtree of node on old revision. */
  private Long2LongOpenHashMap descendantsOldRev;

  /** Number of descendants in subtree of node on new revision. */
  private Long2LongOpenHashMap descendantsNewRev;

  /** Label visitor for old revision. */
  private JsonLabelFMSEVisitor labelOldRevVisitor;

  /** Label visitor for new revision. */
  private JsonLabelFMSEVisitor labelNewRevVisitor;

  /** Write transaction on old revision. */
  private JsonNodeTrx wtx;

  /** Read-only transaction on new revision. */
  private JsonNodeReadOnlyTrx rtx;

  /** Start key in old revision. */
  private long oldStartKey;

  /** Start key in new revision. */
  private long newStartKey;

  private JsonFMSE() {
  }

  /**
   * Creates a new JsonFMSE instance.
   *
   * @return a new instance
   */
  public static JsonFMSE createInstance() {
    return new JsonFMSE();
  }

  @Override
  public void diff(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx) {
    this.wtx = requireNonNull(wtx);
    this.rtx = requireNonNull(rtx);
    oldStartKey = this.wtx.getNodeKey();
    newStartKey = this.rtx.getNodeKey();

    final int capacity = (int) Math.min(
        Math.max(this.wtx.getMaxNodeKey(), this.rtx.getMaxNodeKey()), 1 << 20);
    descendantsOldRev = new Long2LongOpenHashMap(capacity);
    descendantsNewRev = new Long2LongOpenHashMap(capacity);
    inOrderOldRev = new Long2BooleanOpenHashMap(capacity);
    inOrderNewRev = new Long2BooleanOpenHashMap(capacity);
    alreadyInserted = new Long2BooleanOpenHashMap(capacity);

    final var oldRevVisitor = new JsonFMSEVisitor(this.wtx, inOrderOldRev, descendantsOldRev);
    final var newRevVisitor = new JsonFMSEVisitor(this.rtx, inOrderNewRev, descendantsNewRev);

    labelOldRevVisitor = new JsonLabelFMSEVisitor(this.wtx);
    labelNewRevVisitor = new JsonLabelFMSEVisitor(this.rtx);

    postOrderVisit(this.wtx, oldRevVisitor);
    postOrderVisit(this.rtx, newRevVisitor);

    final var fastMatching = fastMatch(this.wtx, this.rtx);
    totalMatching = new JsonMatching(fastMatching);
    firstFMESStep(this.wtx, this.rtx);
    secondFMESStep(this.wtx, this.rtx);
  }

  // ==================== Initialization ====================

  /**
   * Performs a post-order traversal, visiting each descendant and then the root node itself.
   * Used for both initialization and label collection.
   */
  private static void postOrderVisit(final JsonNodeReadOnlyTrx rtx, final JsonNodeVisitor visitor) {
    assert rtx != null;
    assert visitor != null;

    final long nodeKey = rtx.getNodeKey();
    for (final Axis axis = new PostOrderAxis(rtx); axis.hasNext();) {
      axis.nextLong();
      if (rtx.getNodeKey() == nodeKey) {
        break;
      }
      rtx.acceptVisitor(visitor);
    }
    rtx.acceptVisitor(visitor);
  }

  // ==================== Fast Match ====================

  /**
   * The fast match algorithm. Try to resolve the "good matching problem".
   */
  private JsonMatching fastMatch(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx) {
    assert wtx != null;
    assert rtx != null;

    final var comparisonUtils =
        new JsonFMSENodeComparisonUtils(oldStartKey, newStartKey, this.wtx, this.rtx);

    // Chain all nodes with a given label together.
    postOrderVisit(wtx, labelOldRevVisitor);
    postOrderVisit(rtx, labelNewRevVisitor);

    // Do the matching job on the leaf nodes.
    final var matching = new JsonMatching(wtx, rtx);
    matching.reset();
    final NodeComparator<Long> leafComparator =
        new JsonLeafNodeComparator(this.wtx, this.rtx, comparisonUtils);
    FMSEAlgorithm.match(
        labelOldRevVisitor.getLeafLabels(), labelNewRevVisitor.getLeafLabels(),
        matching::add, leafComparator);

    // Remove roots from labels and add them to matching.
    final Map<NodeKind, List<Long>> oldLabels = labelOldRevVisitor.getLabels();
    final Map<NodeKind, List<Long>> newLabels = labelNewRevVisitor.getLabels();
    oldLabels.remove(NodeKind.JSON_DOCUMENT);
    newLabels.remove(NodeKind.JSON_DOCUMENT);

    wtx.moveTo(oldStartKey);
    rtx.moveTo(newStartKey);
    wtx.moveToParent();
    rtx.moveToParent();
    matching.add(wtx.getNodeKey(), rtx.getNodeKey());

    final NodeComparator<Long> innerComparator =
        new JsonInnerNodeComparator(matching, this.wtx, this.rtx, comparisonUtils,
            descendantsOldRev, descendantsNewRev);

    FMSEAlgorithm.match(oldLabels, newLabels, matching::add, innerComparator);

    return matching;
  }

  // ==================== First FMES Step (update/insert/align/move) ====================

  /**
   * First step of the edit script algorithm. Combines the update, insert, align and move phases.
   */
  private void firstFMESStep(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx) {
    assert wtx != null;
    assert rtx != null;

    wtx.moveTo(oldStartKey);
    rtx.moveTo(newStartKey);

    for (final Axis axis =
        new LevelOrderAxis.Builder(rtx).includeSelf().build(); axis.hasNext();) {
      axis.nextLong();
      final long nodeKey = rtx.getNodeKey();
      doFirstFSMEStep(wtx, rtx);
      rtx.moveTo(nodeKey);
    }
  }

  /**
   * Do the actual first step of FSME.
   */
  private void doFirstFSMEStep(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx) {
    assert wtx != null;
    assert rtx != null;

    // 2(a) - Parent of x.
    final long key = rtx.getNodeKey();
    final long x = rtx.getNodeKey();
    rtx.moveToParent();
    final long y = rtx.getNodeKey();

    final long z = totalMatching.reversePartner(y);
    long w = totalMatching.reversePartner(x);

    wtx.moveTo(oldStartKey);

    // 2(b) - insert
    if (w == JsonMatching.NO_PARTNER) {
      // x has no partner.
      assert z != JsonMatching.NO_PARTNER;
      inOrderNewRev.put(x, true);
      final int k = findPos(x, wtx, rtx);
      assert k > -1;
      w = emitInsert(x, z, k, wtx, rtx);
    } else if (x != oldStartKey) {
      // 2(c) not the root (x has a partner in M').
      if (wtx.moveTo(w) && rtx.moveTo(x) && wtx.getKind() == rtx.getKind()
          && !JsonFMSENodeComparisonUtils.typedValuesEqual(w, x, wtx, rtx)) {
        emitUpdate(w, x, wtx, rtx);
      }

      wtx.moveTo(w);
      wtx.moveToParent();
      final long v = wtx.getNodeKey();
      if (!totalMatching.contains(v, y) && wtx.moveTo(w) && rtx.moveTo(x)) {
        assert z != JsonMatching.NO_PARTNER;
        inOrderNewRev.put(x, true);
        final int k = findPos(x, wtx, rtx);
        assert k > -1;
        emitMove(w, z, k, wtx, rtx);
      }
    }

    alignChildren(w, x, wtx, rtx);
    rtx.moveTo(key);
  }

  // ==================== Second FMES Step (delete) ====================

  /**
   * Second step of the edit script algorithm. This is the delete phase.
   */
  private void secondFMESStep(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx)
      throws SirixException {
    assert wtx != null;
    assert rtx != null;
    wtx.moveTo(oldStartKey);
    // noinspection StatementWithEmptyBody
    for (@SuppressWarnings("unused")
    final long nodeKey : VisitorDescendantAxis.newBuilder(wtx)
                                              .includeSelf()
                                              .visitor(
                                                  new DeleteJsonFMSEVisitor(wtx, totalMatching,
                                                      oldStartKey))
                                              .build())
      ;
  }

  // ==================== Align Children ====================

  /**
   * Aligns the children of node w (old tree) according to the children of node x (new tree).
   */
  private void alignChildren(final long w, final long x, final JsonNodeTrx wtx,
      final JsonNodeReadOnlyTrx rtx) {
    assert w >= 0;
    assert x >= 0;
    assert wtx != null;
    assert rtx != null;

    wtx.moveTo(w);
    rtx.moveTo(x);

    // Mark all children of w and all children of x "out of order".
    markOutOfOrder(wtx, inOrderOldRev);
    markOutOfOrder(rtx, inOrderNewRev);

    // 2
    final List<Long> first = commonChildren(w, x, wtx, rtx, false);
    final List<Long> second = commonChildren(x, w, rtx, wtx, true);

    // 3 && 4
    final List<Pair<Long, Long>> s =
        Util.longestCommonSubsequence(first, second,
            (pX, pY) -> totalMatching.contains(pX, pY));

    // 5
    final Long2LongOpenHashMap seen = new Long2LongOpenHashMap(s.size());
    seen.defaultReturnValue(JsonMatching.NO_PARTNER);
    for (final Pair<Long, Long> p : s) {
      final long first0 = p.getFirst();
      final long second0 = p.getSecond();
      inOrderOldRev.put(first0, true);
      inOrderNewRev.put(second0, true);
      seen.put(first0, second0);
    }

    // 6
    for (final Long a : first) {
      final long aKey = a;
      wtx.moveTo(aKey);
      final long b = totalMatching.partner(aKey);
      if (seen.get(aKey) == JsonMatching.NO_PARTNER
          && b != JsonMatching.NO_PARTNER && rtx.moveTo(b)) {
        inOrderOldRev.put(aKey, true);
        inOrderNewRev.put(b, true);
        final int k = findPos(b, wtx, rtx);
        LOGWRAPPER.debug("Move in align children: {}", k);
        emitMove(aKey, w, k, wtx, rtx);
      }
    }
  }

  /**
   * Mark children out of order.
   */
  private static void markOutOfOrder(final JsonNodeReadOnlyTrx rtx,
      final Long2BooleanOpenHashMap inOrder) {
    for (final Axis axis = new ChildAxis(rtx); axis.hasNext();) {
      axis.nextLong();
      inOrder.put(rtx.getNodeKey(), false);
    }
  }

  /**
   * The sequence of children of n whose partners are children of o. This is used by
   * alignChildren().
   */
  private List<Long> commonChildren(final long n, final long o,
      final JsonNodeReadOnlyTrx firstRtx, final JsonNodeReadOnlyTrx secondRtx,
      final boolean reverse) {
    assert n >= 0;
    assert o >= 0;
    assert firstRtx != null;
    assert secondRtx != null;

    final LongArrayList retVal = new LongArrayList();
    firstRtx.moveTo(n);
    if (firstRtx.hasFirstChild()) {
      firstRtx.moveToFirstChild();
      do {
        final long partner;
        if (reverse) {
          partner = totalMatching.reversePartner(firstRtx.getNodeKey());
        } else {
          partner = totalMatching.partner(firstRtx.getNodeKey());
        }
        if (partner != JsonMatching.NO_PARTNER) {
          secondRtx.moveTo(partner);
          if (secondRtx.getParentKey() == o) {
            retVal.add(firstRtx.getNodeKey());
          }
        }
      } while (firstRtx.hasRightSibling() && firstRtx.moveToRightSibling());
    }
    return retVal;
  }

  // ==================== Emit Move ====================

  /**
   * Emits the move of node "child" to the pos-th child of node "parent".
   */
  private void emitMove(final long child, final long parent, final int pos, final JsonNodeTrx wtx,
      final JsonNodeReadOnlyTrx rtx) {
    assert child >= 0;
    assert parent >= 0;
    assert wtx != null;
    assert rtx != null;
    assert pos >= 0;

    boolean moved = wtx.moveTo(parent);
    assert moved;

    if (pos == 0) {
      if (wtx.getFirstChildKey() == child) {
        LOGWRAPPER.error("Something went wrong: First child and child may never be the same!");
      } else {
        if (wtx.moveTo(child)) {
          // JSON_DOCUMENT is in the allowed kinds for moveSubtreeToFirstChild,
          // so no special case is needed (unlike XML where document root required copy).
          wtx.moveTo(parent);
          wtx.moveTo(wtx.moveSubtreeToFirstChild(child).getNodeKey());
        }
      }
    } else {
      assert wtx.hasFirstChild();
      wtx.moveToFirstChild();

      for (int i = 1; i < pos; i++) {
        assert wtx.hasRightSibling();
        wtx.moveToRightSibling();
      }

      // Move.
      assert wtx.getNodeKey() != child;

      wtx.moveTo(wtx.moveSubtreeToRightSibling(child).getNodeKey());
    }
  }

  // ==================== Emit Update ====================

  /**
   * Emit an update.
   */
  private static void emitUpdate(final long fromNode, final long toNode, final JsonNodeTrx wtx,
      final JsonNodeReadOnlyTrx rtx) {
    assert fromNode >= 0;
    assert toNode >= 0;
    assert wtx != null;
    assert rtx != null;

    wtx.moveTo(fromNode);
    rtx.moveTo(toNode);

    switch (rtx.getKind()) {
      case OBJECT_KEY -> wtx.setObjectKeyName(rtx.getName().getLocalName());
      case STRING_VALUE, OBJECT_STRING_VALUE -> wtx.setStringValue(rtx.getValue());
      case NUMBER_VALUE, OBJECT_NUMBER_VALUE -> wtx.setNumberValue(rtx.getNumberValue());
      case BOOLEAN_VALUE, OBJECT_BOOLEAN_VALUE -> wtx.setBooleanValue(rtx.getBooleanValue());
      case NULL_VALUE, OBJECT_NULL_VALUE -> {
        // null = null, no-op
      }
      case OBJECT, ARRAY -> {
        // No intrinsic value to update.
      }
      // $CASES-OMITTED$
      default -> {
      }
    }
  }

  // ==================== Emit Insert ====================

  /**
   * Emit an insert operation.
   */
  private long emitInsert(final long child, final long parent, final int pos,
      final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx) {
    assert child >= 0;
    assert parent >= 0;
    assert wtx != null;
    assert rtx != null;

    // Determines if node has been already inserted (for subtrees).
    // Return the old-revision key (not the new-revision key "child") since the caller
    // uses this return value as an old-revision key (e.g., in alignChildren(w, x, wtx, rtx)).
    if (alreadyInserted.containsKey(child) && alreadyInserted.get(child)) {
      final long oldKey = totalMatching.reversePartner(child);
      assert oldKey != JsonMatching.NO_PARTNER : "Already-inserted node must have a partner";
      return oldKey;
    }

    wtx.moveTo(parent);
    rtx.moveTo(child);

    long oldKey;

    if (pos == 0) {
      oldKey = insertAsFirstChild(wtx, rtx);
    } else {
      assert wtx.hasFirstChild();
      wtx.moveToFirstChild();
      for (int i = 0; i < pos - 1; i++) {
        assert wtx.hasRightSibling();
        wtx.moveToRightSibling();
      }
      oldKey = insertAsRightSibling(wtx, rtx);
    }

    // Mark all nodes in subtree as inserted.
    wtx.moveTo(oldKey);
    rtx.moveTo(child);
    for (final Axis oldAxis = new DescendantAxis(wtx, IncludeSelf.YES),
        newAxis = new DescendantAxis(rtx, IncludeSelf.YES);
        oldAxis.hasNext() && newAxis.hasNext();) {
      oldAxis.nextLong();
      newAxis.nextLong();
      process(wtx.getNodeKey(), rtx.getNodeKey());
    }

    // Return the key of the inserted subtree root, not the last descendant.
    return oldKey;
  }

  /**
   * Insert a node as the first child of the current wtx position.
   */
  private static long insertAsFirstChild(final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx) {
    return switch (rtx.getKind()) {
      case OBJECT, ARRAY, OBJECT_KEY -> wtx.copySubtreeAsFirstChild(rtx).getNodeKey();
      case STRING_VALUE, OBJECT_STRING_VALUE ->
          wtx.insertStringValueAsFirstChild(rtx.getValue()).getNodeKey();
      case NUMBER_VALUE, OBJECT_NUMBER_VALUE ->
          wtx.insertNumberValueAsFirstChild(rtx.getNumberValue()).getNodeKey();
      case BOOLEAN_VALUE, OBJECT_BOOLEAN_VALUE ->
          wtx.insertBooleanValueAsFirstChild(rtx.getBooleanValue()).getNodeKey();
      case NULL_VALUE, OBJECT_NULL_VALUE ->
          wtx.insertNullValueAsFirstChild().getNodeKey();
      default -> throw new IllegalStateException("Unexpected node kind: " + rtx.getKind());
    };
  }

  /**
   * Insert a node as the right sibling of the current wtx position.
   */
  private static long insertAsRightSibling(final JsonNodeTrx wtx,
      final JsonNodeReadOnlyTrx rtx) {
    return switch (rtx.getKind()) {
      case OBJECT, ARRAY, OBJECT_KEY -> wtx.copySubtreeAsRightSibling(rtx).getNodeKey();
      case STRING_VALUE, OBJECT_STRING_VALUE ->
          wtx.insertStringValueAsRightSibling(rtx.getValue()).getNodeKey();
      case NUMBER_VALUE, OBJECT_NUMBER_VALUE ->
          wtx.insertNumberValueAsRightSibling(rtx.getNumberValue()).getNodeKey();
      case BOOLEAN_VALUE, OBJECT_BOOLEAN_VALUE ->
          wtx.insertBooleanValueAsRightSibling(rtx.getBooleanValue()).getNodeKey();
      case NULL_VALUE, OBJECT_NULL_VALUE ->
          wtx.insertNullValueAsRightSibling().getNodeKey();
      default -> throw new IllegalStateException("Unexpected node kind: " + rtx.getKind());
    };
  }

  // ==================== Find Position ====================

  /**
   * The position of node x in the destination tree (tree2).
   *
   * @param x   a node in the second (new) document
   * @param wtx write transaction on old revision
   * @param rtx read-only transaction on new revision
   * @return its position, with respect to already inserted/deleted nodes
   */
  private int findPos(final long x, final JsonNodeTrx wtx, final JsonNodeReadOnlyTrx rtx) {
    assert x >= 0;
    assert wtx != null;
    assert rtx != null;

    rtx.moveTo(x);
    final long nodeKey = rtx.getNodeKey();

    // 1 - Let y = p(x) in T2.
    rtx.moveToParent();

    // 2 - If x is the leftmost child of y that is marked "in order", return 0.
    if (rtx.hasFirstChild()) {
      rtx.moveToFirstChild();
      final long v = rtx.getNodeKey();
      if (inOrderNewRev.get(v) && v == x) {
        return 0;
      }
    }

    // 3 - Find v in T2 where v is the rightmost sibling of x that is to the left
    // of x and is marked "in order".
    rtx.moveTo(nodeKey);
    if (!rtx.hasLeftSibling()) {
      // x is the first child — no left sibling is "in order" to the left of x.
      return 0;
    }
    rtx.moveToLeftSibling();
    long v = rtx.getNodeKey();
    while (!inOrderNewRev.get(v) && rtx.hasLeftSibling()) {
      rtx.moveToLeftSibling();
      v = rtx.getNodeKey();
    }

    // Check if we found an "in order" node.
    if (!inOrderNewRev.get(v)) {
      // No left sibling is "in order" — insert at position 0.
      return 0;
    }

    // 4 - Let u be the partner of v in T1
    long u = totalMatching.reversePartner(v);
    int i = -1;
    if (u != JsonMatching.NO_PARTNER) {
      final boolean moved = wtx.moveTo(u);
      assert moved;

      // Suppose u is the i-th child of its parent (counting from left to right)
      // that is marked "in order". Return i+1.
      final long toNodeKey = u;
      wtx.moveToParent();
      wtx.moveToFirstChild();
      do {
        u = wtx.getNodeKey();
        i++;
      } while (u != toNodeKey && wtx.hasRightSibling() && wtx.moveToRightSibling());
    }

    return i + 1;
  }

  // ==================== Internal Bookkeeping ====================

  /**
   * Process nodes and update data structures.
   */
  private void process(final long oldKey, final long newKey) {
    alreadyInserted.put(newKey, true);
    final long partner = totalMatching.partner(oldKey);
    if (partner != JsonMatching.NO_PARTNER) {
      totalMatching.remove(oldKey);
    }
    final long reversePartner = totalMatching.reversePartner(newKey);
    if (reversePartner != JsonMatching.NO_PARTNER) {
      totalMatching.remove(reversePartner);
    }
    totalMatching.add(oldKey, newKey);
    inOrderOldRev.put(oldKey, true);
    inOrderNewRev.put(newKey, true);
  }

  // ==================== Lifecycle ====================

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() {
    if (wtx != null) {
      wtx.commit();
    }
  }
}
