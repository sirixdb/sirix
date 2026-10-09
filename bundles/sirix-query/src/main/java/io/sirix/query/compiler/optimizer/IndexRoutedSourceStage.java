package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.module.StaticContext;
import io.sirix.query.function.jn.temporal.OpenBitemporal;
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;

import org.jspecify.annotations.Nullable;

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

  public static final String ROUTED_SOURCE_EXPR = "SIRIX_ROUTED_SOURCE_EXPR";

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
    final Source routed = source(source);
    if (routed == null || referencesAny(source, boundBefore)) {
      return false;
    }
    pipeExpr.setProperty(SOURCE_PATH, ARRAY_MEMBERS.clone());
    pipeExpr.setProperty(ROUTED_SOURCE, Boolean.TRUE);
    pipeExpr.setProperty(ROUTED_SOURCE_DATABASE, routed.database());
    pipeExpr.setProperty(ROUTED_SOURCE_RESOURCE, routed.resource());
    pipeExpr.setProperty(ROUTED_SOURCE_TX_TIME, routed.txTime());
    pipeExpr.setProperty(ROUTED_SOURCE_VALID_TIME, routed.validTime());
    pipeExpr.setProperty(ROUTED_SOURCE_EXPR, routed.indexed());
    return true;
  }

  public record Source(String database, String resource, @Nullable AST txTime, AST validTime, @Nullable AST indexed) {
  }

  public static @Nullable Source source(final AST expression) {
    AST source = expression;
    while (source.getType() == XQ.ParenthesizedExpr && source.getChildCount() == 1) {
      source = source.getChild(0);
    }
    if (source.getType() != XQ.FunctionCall || !(source.getValue() instanceof QNm fn)
        || !JSONFun.JSON_NSURI.equals(fn.getNamespaceURI())) {
      return null;
    }
    final boolean opener = isOpener(expression, source, fn);
    final boolean slice = isSlice(source, fn);
    final boolean scan = isScan(source, fn);
    if (!opener && !slice && !scan) {
      return null;
    }
    final AST doc = scan
        ? source.getChild(0)
        : source;
    if (scan && !isDocumentCall(doc)) {
      return null;
    }
    return resolvedSource(source, doc, scan, opener);
  }

  private static @Nullable String stringLiteral(final AST node) {
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

  private static boolean isDocumentCall(final AST doc) {
    return doc.getType() == XQ.FunctionCall && doc.getValue() instanceof QNm docFn
        && JSONFun.JSON_NSURI.equals(docFn.getNamespaceURI())
        && ("doc".equals(docFn.getLocalName()) || "open".equals(docFn.getLocalName())) && doc.getChildCount() >= 2
        && doc.getChildCount() <= 3;
  }

  private static boolean isOpener(final AST expression, final AST source, final QNm fn) {
    return expression.getType() != XQ.ParenthesizedExpr && "open-bitemporal".equals(fn.getLocalName())
        && source.getChildCount() == 4;
  }

  private static boolean isSlice(final AST source, final QNm fn) {
    return OpenBitemporal.OPEN_BITEMPORAL_SLICE.equals(fn) && source.checkProperty(OpenBitemporal.INTERNAL_SLICE)
        && source.getChildCount() == 7;
  }

  private static boolean isScan(final AST source, final QNm fn) {
    return ScanValidTimeIndex.SCAN_VALID_TIME_INDEX.equals(fn) && (source.getChildCount() == 2
        || (source.getChildCount() == 5 && source.checkProperty(ScanValidTimeIndex.DEFERRED_POINT)));
  }

  private static @Nullable Source resolvedSource(final AST source, final AST doc, final boolean scan,
      final boolean opener) {
    final String database = stringLiteral(doc.getChild(0));
    final String resource = stringLiteral(doc.getChild(1));
    return database == null || resource == null
        ? null
        : new Source(database, resource, scan
            ? null
            : source.getChild(2),
            source.getChild(scan
                ? 1
                : 3),
            opener
                ? null
                : source);
  }

}
