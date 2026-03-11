package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.walker.json.JoinCommutativityWalker;
import io.sirix.query.compiler.optimizer.walker.json.SelectAccessFusionWalker;
import io.sirix.query.compiler.optimizer.walker.json.SelectJoinFusionWalker;

/**
 * Applies JQGM rewrite rules adapted from Weiner et al. for JSON/JSONiq.
 *
 * <p>Currently implements:
 * <ul>
 *   <li><b>Rule 2</b>: Select-Join fusion — pushes WHERE predicates into
 *       adjacent Join nodes, reducing input cardinality before the join</li>
 *   <li><b>Rule 3</b>: Select-Access fusion — pushes WHERE predicates into
 *       ObjectAccess/ArrayAccess operators, enabling direct CAS/PATH index mapping</li>
 *   <li><b>Rule 4</b>: Join commutativity — swaps join inputs when the smaller
 *       relation should be the build side of a hash join</li>
 * </ul>
 *
 * <p>Future milestones will add:
 * <ul>
 *   <li>Rule 1: Fusion of adjacent JSON path joins</li>
 * </ul>
 *
 * <p>Rules are applied in a fixpoint loop (max 10 iterations) until no
 * walker produces a change. Identity comparison on the AST reference
 * detects whether a walker modified the tree (Brackit Walker contract).</p>
 */
public final class JqgmRewriteStage implements Stage {

  private static final int MAX_ITERATIONS = 10;

  @Override
  public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    // Reuse walker instances across fixpoint iterations (prepare() resets state)
    final var selectJoinWalker = new SelectJoinFusionWalker();
    final var selectAccessWalker = new SelectAccessFusionWalker();
    final var joinCommutativityWalker = new JoinCommutativityWalker();
    boolean modified = true;
    int iterations = 0;

    while (modified && iterations < MAX_ITERATIONS) {
      modified = false;

      // Rule 2: Select-Join fusion (predicate pushdown into joins)
      AST prev = ast;
      ast = selectJoinWalker.walk(ast);
      if (ast != prev || selectJoinWalker.wasModified()) {
        modified = true;
      }

      // Rule 3: Select-Access fusion (predicate pushdown into access operators)
      prev = ast;
      ast = selectAccessWalker.walk(ast);
      if (ast != prev || selectAccessWalker.wasModified()) {
        modified = true;
      }

      // Rule 4: Join commutativity (swap join inputs for hash join efficiency)
      prev = ast;
      ast = joinCommutativityWalker.walk(ast);
      if (ast != prev || joinCommutativityWalker.wasModified()) {
        modified = true;
      }

      // TODO: Wire CostBasedJoinReorder (DPhyp) here once AST restructuring is implemented.
      //        Currently it only annotates; it cannot yet rewrite the join tree shape.
      // Future: Rule 1 (JoinFusionWalker)

      iterations++;
    }

    return ast;
  }
}
