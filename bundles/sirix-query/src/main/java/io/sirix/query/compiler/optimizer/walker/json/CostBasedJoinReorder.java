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

import java.util.ArrayList;
import java.util.List;

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
      return node; // Too many relations for bitmask DP
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
   */
  private static void addEdgesFromJoinChain(JoinGraph graph, List<AST> joinNodes,
                                            List<AST> baseInputs) {
    // For each join node, determine which base inputs are on its left vs right
    for (final AST join : joinNodes) {
      final AST leftInput = join.getChild(0);
      final AST rightInput = join.getChild(1);

      final int leftIdx = findBaseInputIndex(leftInput, baseInputs);
      final int rightIdx = findBaseInputIndex(rightInput, baseInputs);

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
   */
  private static int findBaseInputIndex(AST input, List<AST> baseInputs) {
    for (int i = 0; i < baseInputs.size(); i++) {
      if (baseInputs.get(i) == input) {
        return i;
      }
    }
    // Check if the content of the input matches a base input
    final AST content = findPipelineContent(input);
    for (int i = 0; i < baseInputs.size(); i++) {
      if (baseInputs.get(i) == content) {
        return i;
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
   * Restructure the join tree based on the DPhyp-optimal plan.
   *
   * <p>For each join node in the group, if the plan indicates the inputs
   * should be swapped (right subtree has lower cardinality), swap the
   * children to place the smaller relation as the build side.</p>
   *
   * <p>TODO: Currently only restructures the root join. Recursive restructuring
   * of nested joins within the group requires mapping plan sub-trees to
   * corresponding AST join nodes, which is deferred to a future iteration.</p>
   */
  private void restructureJoinTree(AST rootJoin, JoinPlan plan) {
    if (plan.isBaseRelation()) {
      return;
    }

    final JoinPlan leftPlan = plan.left();
    final JoinPlan rightPlan = plan.right();

    if (leftPlan == null || rightPlan == null) {
      return;
    }

    // For the root join: check if plan suggests swapping
    final long leftCard = leftPlan.cardinality();
    final long rightCard = rightPlan.cardinality();

    if (leftCard < rightCard && rootJoin.getChildCount() >= 4) {
      swapJoinChildren(rootJoin);
      rootJoin.setProperty(CostProperties.JOIN_SWAPPED, true);
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
