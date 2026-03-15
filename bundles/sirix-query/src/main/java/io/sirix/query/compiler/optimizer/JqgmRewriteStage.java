package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.walker.json.JoinFusionWalker;
import io.sirix.query.compiler.optimizer.walker.json.SelectAccessFusionWalker;
import io.sirix.query.compiler.optimizer.walker.json.SelectJoinFusionWalker;

/**
 * Applies JQGM rewrite rules adapted from Weiner et al. for JSON/JSONiq.
 *
 * <p>Currently implements:
 * <ul>
 *   <li><b>Rule 1</b>: Join fusion — fuses adjacent binary joins over
 *       DerefExpr chains into logically n-way joins</li>
 *   <li><b>Rule 2</b>: Select-Join fusion — annotates WHERE predicates as
 *       pushable into adjacent Join nodes (annotation-only; physical pushdown
 *       is a future extension)</li>
 *   <li><b>Rule 3</b>: Select-Access fusion — pushes WHERE predicates into
 *       ObjectAccess/ArrayAccess operators, enabling direct CAS/PATH index mapping</li>
 * </ul>
 *
 * <p><b>Note:</b> Join commutativity (Rule 4) was removed from this stage because
 * it reads cardinality annotations that are only available after CostBasedStage (Stage 2).
 * Join reordering is handled by JoinReorderStage (Stage 3) after cardinalities are set.</p>
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
    final var joinFusionWalker = new JoinFusionWalker();
    final var selectJoinWalker = new SelectJoinFusionWalker();
    final var selectAccessWalker = new SelectAccessFusionWalker();
    boolean modified = true;
    int iterations = 0;

    while (modified && iterations < MAX_ITERATIONS) {
      modified = false;

      // Rule 1: Join fusion (fuse adjacent binary joins into n-way joins)
      AST prev = ast;
      ast = joinFusionWalker.walk(ast);
      if (ast != prev || joinFusionWalker.wasModified()) {
        modified = true;
      }

      // Rule 2: Select-Join fusion (annotate pushable predicates on joins)
      prev = ast;
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

      iterations++;
    }

    return ast;
  }
}
