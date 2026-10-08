package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.module.StaticContext;

import java.util.HashSet;
import java.util.Set;

/**
 * Admission of an INDEX-ROUTED row source for the projection-served pipeline shapes: a loop whose
 * source is a secondary-index function over a literal document rather than a plain document scan.
 *
 * <pre>
 *   for $r in jn:open-bitemporal('db', 'res', T, P) ...
 * </pre>
 *
 * Brackit's detection walker sees only {@code jn:doc}/{@code jn:open} documents: for the bitemporal
 * opener it records an EMPTY source path (the function call is the path's terminal) and an unknown
 * source ref, which every projection route declines. The rows the opener returns are the resource's
 * top-level array members at the revision current at {@code T} that are valid at {@code P}
 * (half-open), so this stage (1) sets the source path to the array members — the path a projection
 * over the array is declared on — and (2) records the opener's identity and its two instant
 * expressions. The translator then compiles the instants, resolves the revision from {@code T} per
 * evaluation, obtains the valid rows' record keys from the valid-time index, and hands the
 * projection executor that key set as a row mask — the same rows the opener would materialise, read
 * as columns instead.
 *
 * <p>
 * The instant expressions must be evaluable at the pipeline's entry: they may read prolog and outer
 * variables, never a variable the pipeline itself binds before this loop (those bindings do not
 * exist when the served expression evaluates). A dynamic database or resource name declines: the
 * executor is bound per resource.
 * </p>
 */
public final class IndexRoutedSourceStage implements Stage {

  /** {@code Boolean.TRUE} on a pipe whose loop source is an admitted index-routed opener. */
  public static final String ROUTED_SOURCE = "SIRIX_ROUTED_SOURCE";
  /** {@code String}: the literal database (collection) name. */
  public static final String ROUTED_SOURCE_DATABASE = "SIRIX_ROUTED_SOURCE_DATABASE";
  /** {@code String}: the literal resource name. */
  public static final String ROUTED_SOURCE_RESOURCE = "SIRIX_ROUTED_SOURCE_RESOURCE";
  /** {@code AST}: the transaction-time instant expression ({@code xs:dateTime}). */
  public static final String ROUTED_SOURCE_TX_TIME = "SIRIX_ROUTED_SOURCE_TX_TIME";
  /** {@code AST}: the valid-time instant expression ({@code xs:dateTime}). */
  public static final String ROUTED_SOURCE_VALID_TIME = "SIRIX_ROUTED_SOURCE_VALID_TIME";

  private static final String SOURCE_PATH = "VECTORIZED_SOURCE_PATH_PREFIX";
  /**
   * The members of the resource's top-level array — the path the kit's projections are declared on.
   */
  private static final String[] ARRAY_MEMBERS = {"[]"};

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    walk(ast);
    return ast;
  }

  private void walk(final AST node) {
    if (node == null) {
      return;
    }
    if (node.getType() == XQ.PipeExpr) {
      tryAnnotate(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      walk(node.getChild(i));
    }
  }

  private static void tryAnnotate(final AST pipeExpr) {
    annotate(pipeExpr);
  }

  /**
   * Annotate {@code pipeExpr} when its loop source is an admitted index-routed opener.
   *
   * @return whether the pipe was annotated
   */
  public static boolean annotate(final AST pipeExpr) {
    if (pipeExpr.getChildCount() < 1) {
      return false;
    }
    final AST chain = pipeExpr.getChild(0);
    if (chain.getType() != XQ.Start || chain.getChildCount() < 1) {
      return false;
    }
    // The same head the group-aggregate detection admits: leading lets, then the loop.
    final Set<Object> boundBefore = new HashSet<>(4);
    AST forBind = chain.getLastChild();
    while (forBind != null && forBind.getType() == XQ.LetBind) {
      if (forBind.getChildCount() > 0 && forBind.getChild(0).getChildCount() > 0) {
        boundBefore.add(forBind.getChild(0).getChild(0).getValue());
      }
      forBind = forBind.getLastChild();
    }
    if (forBind == null || forBind.getType() != XQ.ForBind || forBind.getChildCount() != 3
        || forBind.getChild(0).getType() != XQ.TypedVariableBinding) {
      return false;
    }
    final AST source = forBind.getChild(1);
    if (source.getType() != XQ.FunctionCall || !(source.getValue() instanceof QNm fn)
        || !JSONFun.JSON_NSURI.equals(fn.getNamespaceURI()) || !"open-bitemporal".equals(fn.getLocalName())
        || source.getChildCount() != 4) {
      return false;
    }
    final String database = stringLiteral(source.getChild(0));
    final String resource = stringLiteral(source.getChild(1));
    if (database == null || resource == null) {
      return false; // a dynamic name cannot be bound to one executor at compile time
    }
    final AST txTime = source.getChild(2);
    final AST validTime = source.getChild(3);
    if (referencesAny(txTime, boundBefore) || referencesAny(validTime, boundBefore)) {
      return false; // evaluated at the pipeline's entry, where those bindings do not exist
    }
    pipeExpr.setProperty(SOURCE_PATH, ARRAY_MEMBERS.clone());
    pipeExpr.setProperty(ROUTED_SOURCE, Boolean.TRUE);
    pipeExpr.setProperty(ROUTED_SOURCE_DATABASE, database);
    pipeExpr.setProperty(ROUTED_SOURCE_RESOURCE, resource);
    pipeExpr.setProperty(ROUTED_SOURCE_TX_TIME, txTime);
    pipeExpr.setProperty(ROUTED_SOURCE_VALID_TIME, validTime);
    return true;
  }

  private static String stringLiteral(final AST node) {
    if (node == null || node.getType() != XQ.Str) {
      return null;
    }
    final Object value = node.getValue();
    if (value instanceof Str str) {
      return str.stringValue();
    }
    return value instanceof String s
        ? s
        : null;
  }

  private static boolean referencesAny(final AST node, final Set<Object> vars) {
    if (node == null) {
      return false;
    }
    if (node.getType() == XQ.VariableRef && vars.contains(node.getValue())) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (referencesAny(node.getChild(i), vars)) {
        return true;
      }
    }
    return false;
  }
}
