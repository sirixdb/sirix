package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.Walker;
import io.brackit.query.util.Cmp;
import io.sirix.query.compiler.optimizer.join.AdaptiveJoinOrderOptimizer;
import io.sirix.query.compiler.optimizer.join.JoinGraph;
import io.sirix.query.compiler.optimizer.join.JoinPlan;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.compiler.optimizer.stats.JsonCostModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts join groups from the AST, runs DPhyp to find optimal join
 * orderings, and annotates the AST with ordering decisions.
 *
 * <p>A "join group" is a set of binary Join nodes that form a connected
 * subtree in the AST. The walker collects all such groups, builds a
 * {@link JoinGraph}, runs {@link AdaptiveJoinOrderOptimizer} (DPhyp),
 * and annotates each Join node with the optimal ordering.</p>
 *
 * <p>For join groups with 2+ joins, DPhyp may suggest a different tree
 * shape (left-deep vs right-deep vs bushy). The walker restructures
 * the AST according to the optimal plan.</p>
 *
 * <p>Left outer joins are excluded from reordering groups (their
 * commutativity constraints are different).</p>
 */
public final class CostBasedJoinReorder extends Walker {

  private static final Logger LOG = LoggerFactory.getLogger(CostBasedJoinReorder.class);

  private final JsonCostModel costModel;
  private boolean modified;

  public CostBasedJoinReorder(JsonCostModel costModel) {
    super();
    this.costModel = costModel;
  }

  public boolean wasModified() {
    return modified;
  }

  @Override
  protected AST prepare(AST ast) {
    modified = false;
    return ast;
  }

  @Override
  protected AST visit(AST node) {
    if (node.getType() != XQ.Join) {
      return node;
    }

    // Skip left outer joins
    if (CostProperties.isLeftJoin(node)) {
      return node;
    }

    // Skip already reordered joins
    if (node.getProperty(CostProperties.JOIN_REORDERED) != null) {
      return node;
    }

    // Collect the join group rooted at this node
    final List<AST> joinNodes = new ArrayList<>();
    final List<AST> baseInputs = new ArrayList<>();
    collectJoinGroup(node, joinNodes, baseInputs);

    if (joinNodes.size() < 2) {
      // Single join: just annotate with cardinality info
      annotateSingleJoin(node);
      return node;
    }

    // Build join graph from the collected group
    final int n = baseInputs.size();
    if (n > JoinGraph.MAX_RELATIONS) {
      LOG.warn("Join group has {} base relations (exceeds MAX_RELATIONS={}), "
          + "skipping join reorder for this group", n, JoinGraph.MAX_RELATIONS);
      return node;
    }

    final JoinGraph graph = buildJoinGraph(joinNodes, baseInputs);

    // Run DPhyp
    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan optimalPlan = optimizer.optimize();

    if (optimalPlan == null) {
      return node;
    }

    // Annotate join nodes with optimal ordering information
    annotateJoinGroup(node, optimalPlan, joinNodes);

    // Restructure: swap join inputs at each level based on DPhyp plan
    restructureJoinTree(node, optimalPlan);

    modified = true;
    return node;
  }

  /**
   * Collect all inner Join nodes forming a connected group, and their
   * base (non-join) inputs.
   */
  private static void collectJoinGroup(AST join, List<AST> joinNodes, List<AST> baseInputs) {
    if (join.getType() != XQ.Join || CostProperties.isLeftJoin(join)) {
      baseInputs.add(join);
      return;
    }

    joinNodes.add(join);

    // Child 0: left input (Start node wrapping a pipeline)
    final AST leftInput = join.getChild(0);
    final AST leftContent = findPipelineContent(leftInput);
    if (leftContent != null && leftContent.getType() == XQ.Join
        && !CostProperties.isLeftJoin(leftContent)) {
      collectJoinGroup(leftContent, joinNodes, baseInputs);
    } else {
      baseInputs.add(leftInput);
    }

    // Child 1: right input (Start node wrapping a pipeline)
    final AST rightInput = join.getChild(1);
    final AST rightContent = findPipelineContent(rightInput);
    if (rightContent != null && rightContent.getType() == XQ.Join
        && !CostProperties.isLeftJoin(rightContent)) {
      collectJoinGroup(rightContent, joinNodes, baseInputs);
    } else {
      baseInputs.add(rightInput);
    }
  }

  /**
   * Navigate through Start/End wrappers to find the actual content node.
   */
  private static AST findPipelineContent(AST node) {
    if (node == null || node.getChildCount() == 0) {
      return node;
    }
    // Start → [pipeline] → End → expr
    AST current = node;
    while (current.getChildCount() > 0) {
      final AST child = current.getChild(current.getChildCount() - 1);
      if (child.getType() == XQ.End || child.getType() == XQ.Start) {
        current = child;
      } else if (child.getType() == XQ.Join) {
        return child;
      } else if (child.getType() == XQ.ForBind || child.getType() == XQ.LetBind) {
        return child;
      } else {
        return current;
      }
    }
    return current;
  }

  /**
   * Build a JoinGraph from collected join nodes and base inputs.
   */
  private JoinGraph buildJoinGraph(List<AST> joinNodes, List<AST> baseInputs) {
    final int n = baseInputs.size();
    final JoinGraph graph = new JoinGraph(n);

    // Set base cardinalities from AST annotations
    for (int i = 0; i < n; i++) {
      final long card = extractCardinality(baseInputs.get(i));
      graph.setBaseCardinality(i, Math.max(1L, card));
      graph.setBaseCost(i, card > 0 ? costModel.estimateSequentialScanCost(card) : 1.0);
    }

    // Add edges based on join predicates
    // For a chain of binary joins, each join connects its left and right subtrees
    addEdgesFromJoinChain(graph, joinNodes, baseInputs);

    return graph;
  }

  /**
   * Add edges to the join graph based on the join predicates.
   * Each binary Join connects two base input groups.
   *
   * <p>Uses an IdentityHashMap for O(1) lookup of base input indices
   * instead of O(n) linear scan per join child.</p>
   */
  private static void addEdgesFromJoinChain(JoinGraph graph, List<AST> joinNodes,
                                            List<AST> baseInputs) {
    // Build identity-based index map for O(1) lookup (was O(n) per call)
    final Map<AST, Integer> baseInputIndex = new IdentityHashMap<>(baseInputs.size() * 2);
    for (int i = 0; i < baseInputs.size(); i++) {
      baseInputIndex.put(baseInputs.get(i), i);
    }

    // For each join node, determine which base inputs are on its left vs right
    for (final AST join : joinNodes) {
      final AST leftInput = join.getChild(0);
      final AST rightInput = join.getChild(1);

      final int leftIdx = findBaseInputIndex(leftInput, baseInputIndex);
      final int rightIdx = findBaseInputIndex(rightInput, baseInputIndex);

      if (leftIdx >= 0 && rightIdx >= 0 && leftIdx != rightIdx) {
        graph.addEdge(leftIdx, rightIdx, JsonCostModel.DEFAULT_JOIN_SELECTIVITY);
      }
    }

    // If no edges were added (e.g., nested joins), add chain edges
    if (graph.edgeCount() == 0 && baseInputs.size() >= 2) {
      for (int i = 0; i < baseInputs.size() - 1; i++) {
        graph.addEdge(i, i + 1, JsonCostModel.DEFAULT_JOIN_SELECTIVITY);
      }
    }
  }

  /**
   * Find which base input index corresponds to a join's left/right child.
   * Uses IdentityHashMap for O(1) lookup instead of O(n) linear scan.
   */
  private static int findBaseInputIndex(AST input, Map<AST, Integer> baseInputIndex) {
    final Integer directIdx = baseInputIndex.get(input);
    if (directIdx != null) {
      return directIdx;
    }
    // Check if the content of the input matches a base input
    final AST content = findPipelineContent(input);
    if (content != null) {
      final Integer contentIdx = baseInputIndex.get(content);
      if (contentIdx != null) {
        return contentIdx;
      }
    }
    return -1;
  }

  /**
   * Extract cardinality from an AST node or its descendants.
   * Returns at least 1 (never negative, unlike the raw utility).
   */
  private static long extractCardinality(AST node) {
    final long card = CostProperties.extractCardinality(node, 5);
    return card > 0 ? card : 1L;
  }

  /**
   * Annotate a single Join node with cost information.
   * Also swaps children if the left side is smaller (should be build side).
   */
  private void annotateSingleJoin(AST join) {
    final AST leftInput = join.getChild(0);
    final AST rightInput = join.getChild(1);

    final long leftCard = extractCardinality(leftInput);
    final long rightCard = extractCardinality(rightInput);

    final double joinCost = costModel.estimateHashJoinCost(leftCard, rightCard);
    final long joinCard = costModel.estimateJoinCardinality(leftCard, rightCard);

    join.setProperty(CostProperties.JOIN_REORDERED, true);
    join.setProperty(CostProperties.JOIN_LEFT_CARD, leftCard);
    join.setProperty(CostProperties.JOIN_RIGHT_CARD, rightCard);
    join.setProperty(CostProperties.JOIN_COST, joinCost);
    join.setProperty(CostProperties.ESTIMATED_CARDINALITY, joinCard);

    // Swap children if left is smaller (should be build side for hash join)
    if (leftCard > 0 && rightCard > 0 && leftCard < rightCard
        && join.getChildCount() >= 4) {
      swapJoinChildren(join);
      join.setProperty(CostProperties.JOIN_LEFT_CARD, rightCard);
      join.setProperty(CostProperties.JOIN_RIGHT_CARD, leftCard);
      modified = true;
    }
  }

  /**
   * Recursively restructure the join tree based on the DPhyp-optimal plan.
   *
   * <p>At each join node, if the plan indicates the left subtree has lower
   * cardinality than the right, the children are swapped so the larger
   * relation is on the probe side (left) and the smaller on the build side
   * (right) of a hash join.</p>
   *
   * <p>After swapping AST children, the plan references are also swapped
   * so that recursion into child joins uses the correct sub-plan for each
   * AST child direction. Left outer joins encountered during recursion
   * are skipped (their commutativity constraints differ).</p>
   */
  private void restructureJoinTree(AST join, JoinPlan plan) {
    if (plan.isBaseRelation()) {
      return;
    }

    JoinPlan leftPlan = plan.left();
    JoinPlan rightPlan = plan.right();

    if (leftPlan == null || rightPlan == null) {
      return;
    }

    // Swap children if left plan has lower cardinality (should be build side)
    final long leftCard = leftPlan.cardinality();
    final long rightCard = rightPlan.cardinality();

    if (leftCard < rightCard && join.getChildCount() >= 4) {
      swapJoinChildren(join);
      join.setProperty(CostProperties.JOIN_SWAPPED, true);
      join.setProperty(CostProperties.JOIN_LEFT_CARD, rightCard);
      join.setProperty(CostProperties.JOIN_RIGHT_CARD, leftCard);
      // After swapping AST children, swap plan references to stay in sync
      final JoinPlan tmp = leftPlan;
      leftPlan = rightPlan;
      rightPlan = tmp;
    }

    // Recurse into left child join
    if (!leftPlan.isBaseRelation()) {
      final AST leftContent = findPipelineContent(join.getChild(0));
      if (leftContent != null && leftContent.getType() == XQ.Join
          && !CostProperties.isLeftJoin(leftContent)) {
        restructureJoinTree(leftContent, leftPlan);
      }
    }

    // Recurse into right child join
    if (!rightPlan.isBaseRelation()) {
      final AST rightContent = findPipelineContent(join.getChild(1));
      if (rightContent != null && rightContent.getType() == XQ.Join
          && !CostProperties.isLeftJoin(rightContent)) {
        restructureJoinTree(rightContent, rightPlan);
      }
    }
  }

  /**
   * Swap left and right children of a Join node, adjusting the
   * comparison operator accordingly.
   */
  private static void swapJoinChildren(AST join) {
    final AST leftInput = join.getChild(0);
    final AST rightInput = join.getChild(1);
    join.replaceChild(0, rightInput);
    join.replaceChild(1, leftInput);

    // Adjust comparison operator
    final Object cmpObj = join.getProperty(CostProperties.CMP);
    if (cmpObj instanceof Cmp cmp) {
      join.setProperty(CostProperties.CMP, cmp.swap());
    }
  }

  /**
   * Annotate a join group with the optimal plan from DPhyp.
   */
  private static void annotateJoinGroup(AST rootJoin, JoinPlan plan,
                                        List<AST> joinNodes) {
    // Annotate the root join with the overall cost
    rootJoin.setProperty(CostProperties.JOIN_REORDERED, true);
    rootJoin.setProperty(CostProperties.JOIN_COST, plan.cost());
    rootJoin.setProperty(CostProperties.ESTIMATED_CARDINALITY, plan.cardinality());

    // Annotate individual join nodes based on the plan tree
    annotatePlanTree(plan, joinNodes, 0);
  }

  /**
   * Recursively annotate join nodes from the plan tree.
   * Returns the index of the next join node to annotate.
   */
  private static int annotatePlanTree(JoinPlan plan, List<AST> joinNodes, int joinIdx) {
    if (plan.isBaseRelation() || joinIdx >= joinNodes.size()) {
      return joinIdx;
    }

    final AST join = joinNodes.get(joinIdx);
    join.setProperty(CostProperties.JOIN_REORDERED, true);
    join.setProperty(CostProperties.JOIN_COST, plan.cost());
    join.setProperty(CostProperties.ESTIMATED_CARDINALITY, plan.cardinality());

    if (plan.left() != null) {
      join.setProperty(CostProperties.JOIN_LEFT_CARD, plan.left().cardinality());
    }
    if (plan.right() != null) {
      join.setProperty(CostProperties.JOIN_RIGHT_CARD, plan.right().cardinality());
    }

    int nextIdx = joinIdx + 1;
    if (plan.left() != null && !plan.left().isBaseRelation()) {
      nextIdx = annotatePlanTree(plan.left(), joinNodes, nextIdx);
    }
    if (plan.right() != null && !plan.right().isBaseRelation()) {
      nextIdx = annotatePlanTree(plan.right(), joinNodes, nextIdx);
    }
    return nextIdx;
  }
}
