package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.module.StaticContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Confines vectorized/projection serving to the resource the executor is bound to.
 *
 * <p>Brackit's {@code VectorizedGroupByDetection} annotates a vectorizable scan with a family of
 * {@code VECTORIZED_*} AST properties that the translator later reads to build a
 * {@code VectorizedGroupByExpr}. The detection captures the scan's source <em>path</em> but never
 * which document it dereferences, and a {@code SirixVectorizedExecutor} is bound at construction
 * to a single {@code (session, revision)}. Left alone, that executor would answer a same-shaped
 * query over a <em>different</em> resource with the wrong data (its own projection/columns).
 *
 * <p>This stage runs after the detection and before translation. When the compile chain auto-wired
 * an executor (a non-null {@link BoundResource}), it fails <b>closed</b>: if any document scan in
 * the query provably targets a resource other than the bound one — a different database/resource, a
 * revision the bound executor does not answer, or an unprovable/dynamic {@code jn:doc} — it removes
 * the {@code VECTORIZED_*} properties from every annotated node, so the translator builds the
 * generic (always-correct) pipeline instead. Context-item scans (the request's own read
 * transaction) and same-resource, same-revision {@code jn:doc} scans keep serving.
 *
 * <p>A refusal only ever costs the fast path, never correctness — so uncertainty resolves to
 * stripping. The stage is a no-op when no executor was auto-wired (bench/test registrations that
 * install an executor directly are out of scope; they carry their own resource contract).
 */
public final class VectorizedResourceScopeStage implements Stage {

  /**
   * The Brackit AST property the translator gates on to build a vectorized expression; its
   * presence marks a vectorized-annotated node.
   */
  private static final String VECTORIZED_MARKER = "VECTORIZED_SOURCE_PATH_PREFIX";

  /**
   * Every {@code VECTORIZED_*} property Brackit's detection sets and its translator reads. All are
   * cleared together so a partially-stripped node can never reach the translator.
   */
  private static final String[] VECTORIZED_PROPERTIES = {
      "VECTORIZED_SOURCE_PATH_PREFIX",
      "VECTORIZED_GROUPBY",
      "VECTORIZED_GROUPBY_FIELD",
      "VECTORIZED_GROUPBY_FIELDS",
      "VECTORIZED_GROUPBY_OUT_NAMES",
      "VECTORIZED_GROUPBY_COUNT_NAME",
      "VECTORIZED_GROUPBY_MULTI",
      "VECTORIZED_AGGREGATE",
      "VECTORIZED_AGGREGATE_FIELD",
      "VECTORIZED_AGGREGATE_FUNC",
      "VECTORIZED_COUNT",
      "VECTORIZED_COUNT_DISTINCT",
      "VECTORIZED_COUNT_DISTINCT_FIELD",
      "VECTORIZED_ORDERBY",
      "VECTORIZED_ORDER_FIELD",
      "VECTORIZED_ORDER_DIRECTION",
      "VECTORIZED_PREDICATE_TREE"
  };

  /** Guard against pathological ASTs in the variable-resolution walk. */
  private static final int MAX_UNWRAP_STEPS = 64;

  /** Classification of a {@code ForBind}'s binding expression relative to the bound resource. */
  private enum Scope {
    /** A context-item scan, or a same-resource same-revision {@code jn:doc} scan. */
    IN_SCOPE,
    /** A document scan that provably targets a different resource/revision, or is unprovable. */
    OUT_OF_SCOPE,
    /** Not a document scan at all — irrelevant to serving (e.g. a literal or arithmetic bind). */
    NOT_A_SCAN
  }

  private final BoundResource bound;

  /**
   * @param bound the resource the auto-wired executor is bound to, or {@code null} when no executor
   *              was auto-wired (the stage is then a no-op)
   */
  public VectorizedResourceScopeStage(final BoundResource bound) {
    this.bound = bound;
  }

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) throws QueryException {
    if (bound == null || ast == null) {
      return ast;
    }
    final List<AST> vectorizedNodes = new ArrayList<>(4);
    collectVectorizedNodes(ast, vectorizedNodes);
    if (vectorizedNodes.isEmpty()) {
      return ast;
    }
    final Map<Object, AST> variableBindings = new HashMap<>(16);
    collectVariableBindings(ast, variableBindings);
    if (hasOutOfScopeScan(ast, variableBindings)) {
      for (final AST node : vectorizedNodes) {
        for (final String property : VECTORIZED_PROPERTIES) {
          node.delProperty(property);
        }
      }
    }
    return ast;
  }

  private static void collectVectorizedNodes(final AST node, final List<AST> out) {
    // getProperty(...) != null, NOT checkProperty(...): the marker's value is the source-path
    // String[], and checkProperty casts the value to Boolean (ClassCastException here).
    if (node.getProperty(VECTORIZED_MARKER) != null) {
      out.add(node);
    }
    for (int i = 0, n = node.getChildCount(); i < n; i++) {
      collectVectorizedNodes(node.getChild(i), out);
    }
  }

  private static void collectVariableBindings(final AST node, final Map<Object, AST> out) {
    if ((node.getType() == XQ.ForBind || node.getType() == XQ.LetBind) && node.getChildCount() >= 2) {
      final Object varKey = bindingVariableKey(node.getChild(0));
      if (varKey != null) {
        out.putIfAbsent(varKey, node.getChild(1));
      }
    }
    for (int i = 0, n = node.getChildCount(); i < n; i++) {
      collectVariableBindings(node.getChild(i), out);
    }
  }

  /**
   * The variable QNm bound by a {@code For}/{@code LetBind}'s first child (a
   * {@code TypedVariableBinding} whose own first child, the {@code Variable}, carries the QNm that a
   * {@code VariableRef} later resolves against). Falls back to the node's own value defensively.
   */
  private static Object bindingVariableKey(final AST typedVariableBinding) {
    if (typedVariableBinding.getChildCount() > 0) {
      return typedVariableBinding.getChild(0).getValue();
    }
    return typedVariableBinding.getValue();
  }

  /** {@code true} as soon as one {@code ForBind} binds a provably out-of-scope document scan. */
  private boolean hasOutOfScopeScan(final AST node, final Map<Object, AST> variableBindings) {
    if (node.getType() == XQ.ForBind && node.getChildCount() >= 2
        && classify(node.getChild(1), variableBindings) == Scope.OUT_OF_SCOPE) {
      return true;
    }
    for (int i = 0, n = node.getChildCount(); i < n; i++) {
      if (hasOutOfScopeScan(node.getChild(i), variableBindings)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Unwraps deref/array/filter/variable layers of a binding expression down to its scan source and
   * classifies it against {@link #bound}.
   */
  private Scope classify(final AST binding, final Map<Object, AST> variableBindings) {
    final Set<Object> resolvingVars = new HashSet<>(4);
    AST current = binding;
    for (int step = 0; current != null && step < MAX_UNWRAP_STEPS; step++) {
      switch (current.getType()) {
        case XQ.DerefExpr, XQ.ArrayAccess, XQ.FilterExpr -> {
          if (current.getChildCount() < 1) {
            return Scope.NOT_A_SCAN;
          }
          current = current.getChild(0);
        }
        case XQ.VariableRef -> {
          final Object varKey = current.getValue();
          if (varKey == null || !resolvingVars.add(varKey)) {
            return Scope.OUT_OF_SCOPE; // unresolved or cyclic — cannot prove it targets bound
          }
          final AST resolved = variableBindings.get(varKey);
          if (resolved == null) {
            return Scope.NOT_A_SCAN; // an outer/for-loop variable, not a document binding
          }
          current = resolved;
        }
        case XQ.ContextItemExpr -> {
          return Scope.IN_SCOPE; // the request's own bound read transaction
        }
        case XQ.FunctionCall -> {
          return classifyFunctionCall(current);
        }
        default -> {
          return Scope.NOT_A_SCAN;
        }
      }
    }
    return Scope.NOT_A_SCAN;
  }

  private Scope classifyFunctionCall(final AST call) {
    if (!(call.getValue() instanceof final QNm qnm)) {
      return Scope.NOT_A_SCAN;
    }
    final boolean jsonNamespace = JSONFun.JSON_NSURI.equals(qnm.getNamespaceURI());
    if (!jsonNamespace) {
      return Scope.NOT_A_SCAN; // not a Sirix document scan the executor could serve from
    }
    final String local = qnm.getLocalName();
    if ("doc".equals(local) || "open".equals(local)) {
      return classifyDocCall(call);
    }
    // Any other jn: opener (collection, load, store, open-bitemporal, open-revisions, …) spans
    // more than a single (resource, revision): it cannot be proven to be the bound one.
    return Scope.OUT_OF_SCOPE;
  }

  private Scope classifyDocCall(final AST call) {
    if (call.getChildCount() < 2) {
      return Scope.OUT_OF_SCOPE;
    }
    final String databaseName = call.getChild(0).getStringValue();
    final String resourceName = call.getChild(1).getStringValue();
    if (databaseName == null || resourceName == null) {
      return Scope.OUT_OF_SCOPE; // dynamic (non-literal) arguments — unprovable
    }
    if (!databaseName.equals(bound.databaseName()) || !resourceName.equals(bound.resourceName())) {
      return Scope.OUT_OF_SCOPE; // a different resource
    }
    final int explicitRevision = explicitRevision(call);
    if (explicitRevision < 0) {
      // No revision argument — jn:doc opens the most-recent revision. It may only serve when the
      // bound executor is itself pinned to that most-recent revision.
      return bound.revisionIsLatest() ? Scope.IN_SCOPE : Scope.OUT_OF_SCOPE;
    }
    return explicitRevision == bound.revision() ? Scope.IN_SCOPE : Scope.OUT_OF_SCOPE;
  }

  /** The explicit revision argument of a {@code jn:doc}/{@code jn:open} call, or -1 if absent. */
  private static int explicitRevision(final AST call) {
    if (call.getChildCount() <= 2) {
      return -1;
    }
    return call.getChild(2).getValue() instanceof final Number number ? number.intValue() : -1;
  }
}
