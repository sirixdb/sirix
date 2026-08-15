package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.compiler.optimizer.VectorizedScanAnnotation;
import io.brackit.query.module.StaticContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Withdraws a vectorized claim whose scan source is a variable that a {@code group by} has already
 * rebound.
 *
 * <p>
 * A {@code group by} rebinds every non-grouping variable of its FLWOR to the <em>sequence of that
 * group's values</em>. Brackit's detection walker resolves a scan source by following the variable
 * to its binding clause, and that walk does not stop at the grouping — so for
 *
 * <pre>
 *   for $h in jn:doc('db','res')[]
 *   let $k := $h.region
 *   group by $k
 *   return {"k": $k, "s": sum(for $x in $h return $x.width)}
 * </pre>
 *
 * the inner pipeline's source resolves back through {@code $h} to the whole document, and the
 * aggregate is answered with the GLOBAL fold — the same value in every group. Silently: the shape
 * looks like an ordinary per-group aggregate, and {@code count($h)} (which needs no field, so no
 * inner pipeline) stays correct, which is what makes the wrong ones hard to notice.
 *
 * <p>
 * This stage restores the invariant the source gate is built on — a {@link SourceRef} names the
 * document a scan will actually read — by replacing such a claim's source with
 * {@link SourceRef#unknown()}. Both pipeline strategies gate on that ref
 * ({@code acceptsSource(UNKNOWN)} is {@code false}), so the pipeline falls back to the generic
 * interpreter, which reads the grouped binding and stays correct. Nothing else is affected: the
 * post-group form {@code sum($h.width)} is not a pipeline of its own and never had a bogus source,
 * and the enclosing group-by pipeline is annotated before the grouping, where its own source is
 * exactly what it says it is.
 *
 * <p>
 * The root cause is in brackit's {@code VectorizedGroupByDetection#resolveSourceRef}; fixing it
 * there makes this stage redundant rather than wrong, so it stays as the backend's own guard: the
 * engine that would serve the wrong answer is the one that declines to.
 */
public final class RegroupedSourceGuardStage implements Stage {

  /** How far to unwrap {@code $v.a[[0]][?p]} layers when looking for the source variable. */
  private static final int MAX_UNWRAP_STEPS = 32;

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    if (ast != null) {
      guard(ast, Set.of(), List.of());
    }
    return ast;
  }

  /**
   * @param node the subtree to walk
   * @param regroupedVars variables an enclosing {@code group by} has rebound
   * @param chainVars variables bound so far by the current pipeline's own clauses
   */
  private void guard(final AST node, final Set<Object> regroupedVars, final List<Object> chainVars) {
    if (node.getType() == XQ.PipeExpr && !regroupedVars.isEmpty()) {
      withdrawIfSourceIsRegrouped(node, regroupedVars);
    }
    Set<Object> childRegrouped = regroupedVars;
    List<Object> childChainVars = chainVars;
    switch (node.getType()) {
      // A nested pipeline opens its own clause scope; its variables are tracked separately, and the
      // outer ones it can still see are already in regroupedVars if a grouping rebound them.
      case XQ.Start -> childChainVars = List.of();
      case XQ.ForBind, XQ.LetBind -> {
        final Object varKey = boundVariable(node);
        if (varKey != null) {
          final List<Object> extended = new ArrayList<>(chainVars.size() + 1);
          extended.addAll(chainVars);
          extended.add(varKey);
          childChainVars = extended;
        }
      }
      case XQ.GroupBy -> {
        if (!chainVars.isEmpty()) {
          final Set<Object> extended = new HashSet<>(regroupedVars);
          extended.addAll(chainVars);
          childRegrouped = extended;
        }
      }
      default -> {
        // no scope change
      }
    }
    for (int i = 0, n = node.getChildCount(); i < n; i++) {
      guard(node.getChild(i), childRegrouped, childChainVars);
    }
  }

  /**
   * Replaces the claim's source with {@link SourceRef#unknown()} when the pipeline scans a regrouped
   * variable. {@code unknown} rather than removing the property: a missing source ref means
   * "unannotated, admit by executor default", which is the opposite of what this has to say.
   */
  private void withdrawIfSourceIsRegrouped(final AST pipeExpr, final Set<Object> regroupedVars) {
    if (pipeExpr.getProperty(VectorizedScanAnnotation.SOURCE_REF) == null) {
      return;
    }
    final AST forBind = forBindOf(pipeExpr);
    if (forBind == null || forBind.getChildCount() < 2) {
      return;
    }
    final Object sourceVar = sourceVariable(forBind.getChild(1));
    if (sourceVar != null && regroupedVars.contains(sourceVar)) {
      pipeExpr.setProperty(VectorizedScanAnnotation.SOURCE_REF, SourceRef.unknown());
    }
  }

  /** The {@code ForBind} at the end of a pipeline's clause chain, or {@code null}. */
  private static AST forBindOf(final AST pipeExpr) {
    if (pipeExpr.getChildCount() < 1) {
      return null;
    }
    final AST chain = pipeExpr.getChild(0);
    if (chain.getType() != XQ.Start || chain.getChildCount() < 1) {
      return null;
    }
    AST forBind = chain.getLastChild();
    while (forBind != null && forBind.getType() == XQ.LetBind) {
      forBind = forBind.getLastChild();
    }
    return forBind != null && forBind.getType() == XQ.ForBind
        ? forBind
        : null;
  }

  /**
   * The variable a binding expression ultimately reads, looking through the deref / array-access /
   * filter layers a source path is written with ({@code $h.items[]}); {@code null} when the
   * expression is not rooted in a variable.
   */
  private static Object sourceVariable(final AST binding) {
    AST current = binding;
    for (int step = 0; current != null && step < MAX_UNWRAP_STEPS; step++) {
      switch (current.getType()) {
        case XQ.DerefExpr, XQ.ArrayAccess, XQ.FilterExpr -> {
          if (current.getChildCount() < 1) {
            return null;
          }
          current = current.getChild(0);
        }
        case XQ.VariableRef -> {
          return current.getValue();
        }
        default -> {
          return null;
        }
      }
    }
    return null;
  }

  /** The variable a {@code ForBind}/{@code LetBind} declares, or {@code null}. */
  private static Object boundVariable(final AST bindingClause) {
    if (bindingClause.getChildCount() < 2) {
      return null;
    }
    final AST typedVariableBinding = bindingClause.getChild(0);
    return typedVariableBinding.getChildCount() > 0
        ? typedVariableBinding.getChild(0).getValue()
        : typedVariableBinding.getValue();
  }
}
