package io.sirix.query.compiler.translator;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.expr.PipeExpr;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.compiler.operator.HashMembershipJoin;
import java.util.List;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.compiler.translator.Compiler;
import io.brackit.query.expr.VectorizedGroupByExpr;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Expr;
import io.brackit.query.operator.Operator;
import io.sirix.query.compiler.optimizer.CorrelatedGroupAggregateDetectionStage;
import io.sirix.query.compiler.optimizer.GroupAggregateDetectionStage;
import io.sirix.query.compiler.optimizer.IndexRoutedSourceStage;
import io.sirix.query.compiler.optimizer.JoinedGroupAggregateDetectionStage;
import io.sirix.query.compiler.optimizer.RowMaterializeDetectionStage;
import io.sirix.query.compiler.optimizer.SortedScanDetectionStage;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.scan.SirixExecutorProvider;
import io.sirix.query.scan.SirixVectorizedExecutor;

/**
 * Sirix-aware pipeline strategy that extends Brackit's sequential strategy with support for
 * optimizer annotations.
 *
 * <p>
 * When the optimizer marks a join as an intersection join ({@code INTERSECTION_JOIN=true}), this
 * strategy forces hash-based execution ({@code skipSort=true}) to avoid unnecessary post-probe
 * sorting. Index intersection results are already unique by nodeKey, so sort+dedup is wasted work.
 * </p>
 */
public final class SirixPipelineStrategy extends SequentialPipelineStrategy {

  static boolean hasMembershipJoin(final AST node) {
    if (node.getType() == XQExt.HashMembershipJoin) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasMembershipJoin(node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Operator select(final Operator in, final AST node, final Compiler compiler) {
    if (node.getChild(0).getType() != XQExt.HashMembershipJoin) {
      return super.select(in, node, compiler);
    }
    final HashMembershipJoin join = ((SirixTranslator) compiler).membershipJoin(in, node.getChild(0));
    addChecks(join, (List<QNm>) node.getProperty("check"), compiler);
    return anyOp(join, node.getLastChild(), compiler);
  }

  /**
   * P5b stage 7a: consume the {@code SIRIX_GROUP_AGG_*} annotations from
   * {@link GroupAggregateDetectionStage}. For pipelines without membership joins, the generic
   * pipeline is always compiled (via {@code super}) and rides along as the runtime fallback, so
   * serving declines can never change an answer — only its cost.
   */
  @Override
  public Expr compilePipeExpr(AST node, Compiler compiler) throws QueryException {
    final Expr generic;
    if (hasMembershipJoin(node)) {
      // Membership tables have one cursor owner. Do not split this pipeline into morsels or
      // block workers, each of which would otherwise build the same inner relation again.
      final SirixTranslator translator = (SirixTranslator) compiler;
      final int initialBindings = translator.bindingCount();
      final Operator root = anyOp(null, node.getChild(0), compiler);
      AST end = node.getChild(0);
      while (end.getType() != XQ.End) {
        end = end.getLastChild();
      }
      generic = new PipeExpr(root, translator.pipelineReturn(end.getChild(0), initialBindings));
      // A membership filter over an index-routed opener under a routed grouping is served as a
      // row-key subtraction before the grouping; the membership pipeline is its fallback.
      if (!Boolean.TRUE.equals(node.getProperty(GroupAggregateDetectionStage.GROUP_AGG))
          || !Boolean.TRUE.equals(node.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE))
          || node.getProperty(GroupAggregateDetectionStage.MEMBERSHIP_DATABASE) == null) {
        return generic;
      }
    } else {
      generic = Boolean.TRUE.equals(node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_CONST))
          ? super.compileGenericPipeExpr(node, compiler)
          : super.compilePipeExpr(node, compiler);
    }
    // P5b stage 7b (+gap 1b multi-key): sorted-scan serving. Brackit's own
    // supportsSortedScan hook DROPS the predicate (its sorted() factory never receives
    // it), so sirix consumes its OWN SortedScanDetectionStage annotations here instead —
    // predicate included — and keeps supportsSortedScan false.
    if (Boolean.TRUE.equals(node.getProperty(SortedScanDetectionStage.SORTED_SCAN))
        && !(generic instanceof VectorizedGroupByExpr)
        && SequentialPipelineStrategy.getVectorizedExecutor() instanceof SirixExecutorProvider sortExecutor) {
      final String[] sortSourcePath = (String[]) node.getProperty("VECTORIZED_SOURCE_PATH_PREFIX");
      final String[] orderFields = (String[]) node.getProperty(SortedScanDetectionStage.SORTED_FIELDS);
      final boolean[] descending = (boolean[]) node.getProperty(SortedScanDetectionStage.SORTED_DESC);
      final SourceRef sortSourceRef = (SourceRef) node.getProperty("VECTORIZED_SOURCE_REF");
      // Gap 3: a sole-consumer fn:subsequence over this pipe caps how many sorted rows
      // can ever be pulled — the executor then heap-selects top-K instead of full-sorting.
      final Long topK = (Long) node.getProperty(SortedScanDetectionStage.SORTED_LIMIT);
      final String returnField = (String) node.getProperty(SortedScanDetectionStage.SORTED_RETURN_FIELD);
      if (sortSourcePath != null && orderFields != null && descending != null && orderFields.length == descending.length
          && acceptsOrRuntimeCheckable(sortExecutor, sortSourceRef)) {
        return new SirixSortedScanExpr(sortExecutor, sortSourcePath,
            (PredicateNode) node.getProperty("VECTORIZED_PREDICATE_TREE"), orderFields, descending, topK == null
                ? -1L
                : topK,
            returnField, sortSourceRef, generic);
      }
    }
    // P5b stage 7d: predicate scan — filtered rows (or one field of them) in document order,
    // record keys straight from the projection's predicate mask. Null orderFields selects the
    // scan-without-sort branch inside the shared expr.
    if (Boolean.TRUE.equals(node.getProperty(SortedScanDetectionStage.PREDICATE_SCAN))
        && !(generic instanceof VectorizedGroupByExpr)
        && SequentialPipelineStrategy.getVectorizedExecutor() instanceof SirixExecutorProvider predExecutor) {
      final String[] predSourcePath = (String[]) node.getProperty("VECTORIZED_SOURCE_PATH_PREFIX");
      final PredicateNode predTree = (PredicateNode) node.getProperty("VECTORIZED_PREDICATE_TREE");
      final SourceRef predSourceRef = (SourceRef) node.getProperty("VECTORIZED_SOURCE_REF");
      final Long predTopK = (Long) node.getProperty(SortedScanDetectionStage.SORTED_LIMIT);
      final String predReturnField = (String) node.getProperty(SortedScanDetectionStage.SORTED_RETURN_FIELD);
      if (predSourcePath != null && predTree != null && acceptsOrRuntimeCheckable(predExecutor, predSourceRef)) {
        return new SirixSortedScanExpr(predExecutor, predSourcePath, predTree, null, null, predTopK == null
            ? -1L
            : predTopK, predReturnField, predSourceRef, generic);
      }
    }
    // P5b stage 7c: covered-row serving (record-constructor returns over covered fields).
    if (Boolean.TRUE.equals(node.getProperty(RowMaterializeDetectionStage.ROW_MAT))
        && !(generic instanceof VectorizedGroupByExpr)
        && SequentialPipelineStrategy.getVectorizedExecutor() instanceof SirixExecutorProvider rowExecutor) {
      final String[] rowSourcePath = (String[]) node.getProperty("VECTORIZED_SOURCE_PATH_PREFIX");
      final String[] rowFields = (String[]) node.getProperty(RowMaterializeDetectionStage.ROW_MAT_FIELDS);
      final String[] rowOutNames = (String[]) node.getProperty(RowMaterializeDetectionStage.ROW_MAT_OUT_NAMES);
      final int[] rowDirect = (int[]) node.getProperty(RowMaterializeDetectionStage.ROW_MAT_DIRECT);
      final int[][] rowCodes = (int[][]) node.getProperty(RowMaterializeDetectionStage.ROW_MAT_CODES);
      final long[][] rowConsts = (long[][]) node.getProperty(RowMaterializeDetectionStage.ROW_MAT_CONSTS);
      final SourceRef rowSourceRef = (SourceRef) node.getProperty("VECTORIZED_SOURCE_REF");
      if (rowSourcePath != null && rowFields != null && rowOutNames != null && rowDirect != null && rowCodes != null
          && rowConsts != null && acceptsOrRuntimeCheckable(rowExecutor, rowSourceRef)) {
        return new SirixRowMaterializeExpr(rowExecutor, rowSourcePath,
            (PredicateNode) node.getProperty("VECTORIZED_PREDICATE_TREE"), rowFields, rowOutNames, rowDirect, rowCodes,
            rowConsts, rowSourceRef, generic);
      }
    }
    // Column-side equality join with a grouped aggregate over the pairs.
    if (Boolean.TRUE.equals(node.getProperty(JoinedGroupAggregateDetectionStage.JOIN_GROUP))
        && !(generic instanceof VectorizedGroupByExpr)
        && SequentialPipelineStrategy.getVectorizedExecutor() instanceof SirixExecutorProvider joinExecutor) {
      final Expr joined = joined(node, compiler, joinExecutor, generic);
      if (joined != null) {
        return joined;
      }
    }
    // Correlated index-routed grouping (an outer loop supplying the opener's instants and some keys).
    if (Boolean.TRUE.equals(node.getProperty(CorrelatedGroupAggregateDetectionStage.CORRELATED))
        && !(generic instanceof VectorizedGroupByExpr)
        && SequentialPipelineStrategy.getVectorizedExecutor() instanceof SirixExecutorProvider corrExecutor) {
      final Expr correlated = correlated(node, compiler, corrExecutor, generic);
      if (correlated != null) {
        return correlated;
      }
      if (Boolean.getBoolean("sirix.projDiag")) {
        System.err.println("[corr-translate] annotations did not yield a serving expression");
      }
    }
    // Constant-key grouping (Q29's `let $g := 1 ... group by $g`): one scalar pass, one record.
    if (Boolean.TRUE.equals(node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_CONST))
        && !Boolean.TRUE.equals(node.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE))
        && !(generic instanceof VectorizedGroupByExpr)
        && SequentialPipelineStrategy.getVectorizedExecutor() instanceof SirixExecutorProvider constExecutor) {
      final SourceRef constRef = (SourceRef) node.getProperty("VECTORIZED_SOURCE_REF");
      final String[] constSourcePath = (String[]) node.getProperty("VECTORIZED_SOURCE_PATH_PREFIX");
      final String[] constFuncs = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_FUNCS);
      final String[] constFields = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_FIELDS);
      final String[] constOutNames = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_OUT_NAMES);
      final long[] constOffsets = (long[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_OFFSETS);
      if (constSourcePath != null && constFuncs != null && constFields != null && constOutNames != null
          && constOffsets != null && constOffsets.length == constFuncs.length
          && acceptsOrRuntimeCheckable(constExecutor, constRef)) {
        return new SirixConstGroupAggregateExpr(constExecutor, constSourcePath, servedPredicate(node), constFuncs,
            constFields, constOffsets, constOutNames, constRef, generic);
      }
      return generic;
    }
    if (!Boolean.TRUE.equals(node.getProperty(GroupAggregateDetectionStage.GROUP_AGG))) {
      return generic;
    }
    final Long groupCap = (Long) node.getProperty(SortedScanDetectionStage.SORTED_LIMIT);
    if (generic instanceof VectorizedGroupByExpr && groupCap == null) {
      // Overlap shape (canonical count return matches BOTH detections): brackit's expr
      // THROWS on decline instead of falling back, so it must not become our "generic
      // fallback" — leave it as-is (pre-stage-7a behavior for that shape). With a
      // subsequence CAP the flat top-K route claims first: brackit's expr stays the
      // fallback, and on our decline it serves exactly as it does today (it claimed the
      // shape at compile time, so its own runtime path exists).
      return generic;
    }
    if (!(SequentialPipelineStrategy.getVectorizedExecutor() instanceof SirixExecutorProvider sirixExecutor)) {
      return generic;
    }
    // Source identity/revision gate — the same check brackit's own vectorized dispatch
    // applies: an executor bound to another resource, or pinned to an older revision
    // while the query opens latest, must not serve. VARIABLE refs (external variables,
    // compile-time-unresolvable since brackit 1.0-alpha9) pass through with the ref
    // attached; the expr verifies the ACTUAL binding at evaluation time and falls back
    // to the generic pipeline when it is not this executor's resource/revision.
    final SourceRef sourceRef = (SourceRef) node.getProperty("VECTORIZED_SOURCE_REF");
    // An index-routed source names its document itself and resolves its revision per evaluation,
    // so the executor it needs is acquired at run time; the compile-time ref (unknown — Brackit's
    // walker does not know the opener) is not consulted for it.
    final SirixGroupAggregateExpr.RoutedSource routedSource = routedSource(node, compiler);
    if (routedSource == null && !acceptsOrRuntimeCheckable(sirixExecutor, sourceRef)) {
      return generic;
    }
    final String[] sourcePath = (String[]) node.getProperty("VECTORIZED_SOURCE_PATH_PREFIX");
    final PredicateNode predicate = servedPredicate(node);
    final String[] groupFields = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_GROUP_FIELDS);
    final String[] keyNames = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_NAMES);
    final String[] funcs = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_FUNCS);
    final String[] aggFields = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_FIELDS);
    final String[] outNames = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_OUT_NAMES);
    if (sourcePath == null || groupFields == null || keyNames == null || funcs == null || aggFields == null
        || outNames == null) {
      return generic;
    }
    // Order-by is optional; when present all three descriptors must agree in length, or the
    // annotation is not one this strategy wrote and must not be acted on.
    final int[] orderIndexes = (int[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_ORDER_INDEXES);
    final boolean[] orderAsc = (boolean[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_ORDER_ASC);
    final boolean[] orderEmptyLeast =
        (boolean[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_ORDER_EMPTY_LEAST);
    if (orderIndexes != null && (orderAsc == null || orderEmptyLeast == null || orderAsc.length != orderIndexes.length
        || orderEmptyLeast.length != orderIndexes.length)) {
      return generic;
    }
    // Same sole-consumer fn:subsequence cap the sorted-scan route uses (gap 3): an ordered
    // group-by whose only consumer slices the first K records never needs more than K groups
    // materialized — the executor then heap-selects top-K instead of sort-all + emit-all.
    final Long groupTopK = (Long) node.getProperty(SortedScanDetectionStage.SORTED_LIMIT);
    final long[] keyOffsets = (long[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_OFFSETS);
    final int[] keySubstr = (int[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_SUBSTR);
    if (keyOffsets != null && (keySubstr == null || keyOffsets.length != groupFields.length
        || keySubstr.length != 2 * groupFields.length)) {
      return generic; // not an annotation this strategy wrote
    }
    final String[] keyRegexPattern =
        (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_REGEX_PATTERN);
    final String[] keyRegexRepl = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_REGEX_REPL);
    if (keyRegexPattern != null && (keyRegexRepl == null || keyRegexPattern.length != groupFields.length
        || keyRegexRepl.length != groupFields.length)) {
      return generic; // not an annotation this strategy wrote
    }
    final long[] keyDivMod = (long[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_DIVMOD);
    if (keyDivMod != null && (keyOffsets == null || keyDivMod.length != 2 * groupFields.length)) {
      // A divmod key is a key TRANSFORM: without the offsets/substr pair the executor never takes
      // the transform-carrying arm, and the divisor would silently vanish.
      return generic;
    }
    final boolean[] keyStringify = (boolean[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_STRINGIFY);
    if (keyStringify != null && (keyOffsets == null || keyStringify.length != groupFields.length)) {
      return generic;
    }
    final long[] having = (long[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_HAVING);
    if (having != null && having.length != 2) {
      return generic; // not an annotation this strategy wrote
    }
    final String[] keyCondFields = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_COND_FIELDS);
    final long[] keyCondLits = (long[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_COND_LITS);
    final String[] keyCondElse = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_COND_ELSE);
    if (keyCondElse != null && (keyCondFields == null || keyCondLits == null || keyCondElse.length != groupFields.length
        || keyCondFields.length != 2 * groupFields.length || keyCondLits.length != 2 * groupFields.length)) {
      return generic; // not an annotation this strategy wrote
    }
    final int[] decorPos = (int[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_DECOR_POS);
    final String[] decorPrefix = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_DECOR_PREFIX);
    final String[] decorSuffix = (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_DECOR_SUFFIX);
    if (decorPos != null && (decorPrefix == null || decorSuffix == null || decorPrefix.length != decorPos.length
        || decorSuffix.length != decorPos.length)) {
      return generic;
    }
    final int[] constEntryPos = (int[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_CONST_ENTRY_POS);
    final String[] constEntryNames =
        (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_CONST_ENTRY_NAMES);
    final long[] constEntryValues =
        (long[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_CONST_ENTRY_VALUES);
    if (constEntryPos != null && (constEntryNames == null || constEntryValues == null
        || constEntryNames.length != constEntryPos.length || constEntryValues.length != constEntryPos.length)) {
      return generic;
    }
    final SirixVectorizedExecutor.ComputedLane[] computedLanes = computedLanes(node);
    if (computedLanes == null && hasComputedAggregate(aggFields)) {
      return generic; // a prog: operand without its program annotations is not ours
    }
    final RoutedGroupRequest.MembershipFilter membership = membershipFilter(node, compiler);
    if (node.getProperty(GroupAggregateDetectionStage.MEMBERSHIP_DATABASE) != null
        && (membership == null || routedSource == null)) {
      return generic; // a membership filter serves only under a routed source, with its own source compiled
    }
    if (routedSource != null && !ordersEveryKey(orderIndexes, groupFields.length)) {
      // The opener yields its rows in record-key order while the projection folds them in physical
      // row order; the two agree except at order exceptions, so a routed grouping is served only
      // under an order-by that totally orders the groups — every key named — which the wrapper
      // applies. Without one the generic pipeline keeps its first-appearance order.
      return generic;
    }
    return new SirixGroupAggregateExpr(sirixExecutor, sourcePath, predicate, groupFields, keyNames, funcs, aggFields,
        outNames, orderIndexes, orderAsc, orderEmptyLeast, groupTopK != null
            ? groupTopK
            : -1L,
        keyOffsets, keySubstr, keyCondFields, keyCondLits, keyCondElse, keyRegexPattern, keyRegexRepl, keyDivMod,
        keyStringify, having, decorPos, decorPrefix, decorSuffix, constEntryPos, constEntryNames, constEntryValues,
        sourceRef, generic, routedSource, computedLanes, membership);
  }

  /** The membership filter the detection stage annotated, its opener's instants compiled, or null. */
  private static RoutedGroupRequest.@org.jspecify.annotations.Nullable MembershipFilter membershipFilter(final AST node,
      final Compiler compiler) throws QueryException {
    final String database = (String) node.getProperty(GroupAggregateDetectionStage.MEMBERSHIP_DATABASE);
    final String resource = (String) node.getProperty(GroupAggregateDetectionStage.MEMBERSHIP_RESOURCE);
    final AST txTime = (AST) node.getProperty(GroupAggregateDetectionStage.MEMBERSHIP_TX_TIME);
    final AST validTime = (AST) node.getProperty(GroupAggregateDetectionStage.MEMBERSHIP_VALID_TIME);
    final String innerField = (String) node.getProperty(GroupAggregateDetectionStage.MEMBERSHIP_INNER_FIELD);
    final String outerField = (String) node.getProperty(GroupAggregateDetectionStage.MEMBERSHIP_OUTER_FIELD);
    if (database == null || resource == null || txTime == null || validTime == null || innerField == null
        || outerField == null || !(compiler instanceof SirixTranslator translator)) {
      return null;
    }
    return new RoutedGroupRequest.MembershipFilter(
        new SirixGroupAggregateExpr.RoutedSource(database, resource, translator.routedInstant(txTime),
            translator.routedInstant(validTime)),
        innerField, outerField, Boolean.TRUE.equals(node.getProperty(GroupAggregateDetectionStage.MEMBERSHIP_ANTI)));
  }

  /**
   * The correlated serving expression, or {@code null} when the annotations are not this strategy's.
   * The outer prefix (the outer loop and its selections) compiles as operators; while its variables
   * are bound, the outer key expressions and the inner opener's instants compile in that scope.
   */
  private Expr correlated(final AST node, final Compiler compiler, final SirixExecutorProvider executor,
      final Expr generic) throws QueryException {
    final AST innerPipe = (AST) node.getProperty(CorrelatedGroupAggregateDetectionStage.INNER_PIPE);
    final AST[] outerKeyAsts = (AST[]) node.getProperty(CorrelatedGroupAggregateDetectionStage.OUTER_KEY_EXPRS);
    final String[] outerKeyNames = (String[]) node.getProperty(CorrelatedGroupAggregateDetectionStage.OUTER_KEY_NAMES);
    final int[] entryKinds = (int[]) node.getProperty(CorrelatedGroupAggregateDetectionStage.ENTRY_KINDS);
    final int[] orderIndexes = (int[]) node.getProperty(CorrelatedGroupAggregateDetectionStage.ORDER_INDEXES);
    final boolean[] orderAsc = (boolean[]) node.getProperty(CorrelatedGroupAggregateDetectionStage.ORDER_ASC);
    final boolean[] orderEmptyLeast =
        (boolean[]) node.getProperty(CorrelatedGroupAggregateDetectionStage.ORDER_EMPTY_LEAST);
    if (innerPipe == null || outerKeyAsts == null || outerKeyNames == null || entryKinds == null || orderIndexes == null
        || orderAsc == null || orderEmptyLeast == null || outerKeyNames.length != outerKeyAsts.length
        || orderAsc.length != orderIndexes.length || orderEmptyLeast.length != orderIndexes.length
        || !(compiler instanceof SirixTranslator translator)) {
      return null;
    }
    final RoutedGroupRequest inner = routedGroupRequest(innerPipe);
    final String database = (String) innerPipe.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE_DATABASE);
    final String resource = (String) innerPipe.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE_RESOURCE);
    final AST txTime = (AST) innerPipe.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE_TX_TIME);
    final AST validTime = (AST) innerPipe.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE_VALID_TIME);
    if (inner == null || database == null || resource == null || txTime == null || validTime == null) {
      return null;
    }
    // The real record's entry names, in order.
    final AST returnRecord = returnRecord(node);
    if (returnRecord == null || returnRecord.getChildCount() != entryKinds.length) {
      return null;
    }
    final String[] entryNames = new String[entryKinds.length];
    for (int i = 0; i < entryNames.length; i++) {
      final AST nameNode = returnRecord.getChild(i).getChild(0);
      entryNames[i] = nameNode.getValue() instanceof Str str
          ? str.stringValue()
          : String.valueOf(nameNode.getValue());
    }
    // The outer prefix: the chain up to (excluding) the inner loop, ended with a bare End.
    final AST prefix = node.getChild(0).copyTree();
    AST parent = prefix;
    AST current = prefix.getLastChild(); // the outer ForBind
    while (current != null && current.getType() != XQ.ForBind) {
      parent = current;
      current = current.getLastChild();
    }
    if (current == null) {
      return null;
    }
    parent = current;
    current = current.getLastChild();
    while (current != null && current.getType() == XQ.Selection) {
      parent = current;
      current = current.getLastChild();
    }
    if (current == null || current.getType() != XQ.ForBind) {
      return null;
    }
    parent.replaceChild(parent.getChildCount() - 1, new AST(XQ.End));
    final int initialBindings = translator.bindingCount();
    final Operator outer = anyOp(null, prefix, compiler);
    final Expr[] outerKeyExprs = new Expr[outerKeyAsts.length];
    final SirixGroupAggregateExpr.RoutedSource routed;
    try {
      for (int k = 0; k < outerKeyAsts.length; k++) {
        outerKeyExprs[k] = translator.routedInstant(outerKeyAsts[k]);
      }
      routed = new SirixGroupAggregateExpr.RoutedSource(database, resource, translator.routedInstant(txTime),
          translator.routedInstant(validTime));
    } finally {
      translator.unbindTo(initialBindings);
    }
    return new SirixCorrelatedGroupAggregateExpr(executor, outer, outerKeyExprs, outerKeyNames, routed, inner,
        entryKinds, entryNames, orderIndexes, orderAsc, orderEmptyLeast, generic);
  }

  /** The joined serving expression, or {@code null} when the annotations are not this strategy's. */
  private static Expr joined(final AST node, final Compiler compiler, final SirixExecutorProvider executor,
      final Expr generic) throws QueryException {
    final String[] databases = (String[]) node.getProperty(JoinedGroupAggregateDetectionStage.SIDE_DATABASES);
    final String[] resources = (String[]) node.getProperty(JoinedGroupAggregateDetectionStage.SIDE_RESOURCES);
    final AST[] txTimes = (AST[]) node.getProperty(JoinedGroupAggregateDetectionStage.SIDE_TX_TIMES);
    final AST[] validTimes = (AST[]) node.getProperty(JoinedGroupAggregateDetectionStage.SIDE_VALID_TIMES);
    final int[] revisions = (int[]) node.getProperty(JoinedGroupAggregateDetectionStage.SIDE_REVISIONS);
    final String[] joinFields = (String[]) node.getProperty(JoinedGroupAggregateDetectionStage.JOIN_FIELDS);
    final int[] keySides = (int[]) node.getProperty(JoinedGroupAggregateDetectionStage.KEY_SIDES);
    final String[] keyFields = (String[]) node.getProperty(JoinedGroupAggregateDetectionStage.KEY_FIELDS);
    final String[] aggFuncs = (String[]) node.getProperty(JoinedGroupAggregateDetectionStage.AGG_FUNCS);
    final int[] aggSides = (int[]) node.getProperty(JoinedGroupAggregateDetectionStage.AGG_SIDES);
    final String[] aggFields = (String[]) node.getProperty(JoinedGroupAggregateDetectionStage.AGG_FIELDS);
    final int[] progSides = (int[]) node.getProperty(JoinedGroupAggregateDetectionStage.PROG_SIDES);
    final String[][] progFields = (String[][]) node.getProperty(JoinedGroupAggregateDetectionStage.PROG_FIELDS);
    final int[][] progCode = (int[][]) node.getProperty(JoinedGroupAggregateDetectionStage.PROG_CODE);
    final long[][] progConsts = (long[][]) node.getProperty(JoinedGroupAggregateDetectionStage.PROG_CONSTS);
    final int[] entryKinds = (int[]) node.getProperty(JoinedGroupAggregateDetectionStage.ENTRY_KINDS);
    final int[] orderIndexes = (int[]) node.getProperty(JoinedGroupAggregateDetectionStage.ORDER_INDEXES);
    final boolean[] orderAsc = (boolean[]) node.getProperty(JoinedGroupAggregateDetectionStage.ORDER_ASC);
    final boolean[] orderEmptyLeast =
        (boolean[]) node.getProperty(JoinedGroupAggregateDetectionStage.ORDER_EMPTY_LEAST);
    if (databases == null || resources == null || txTimes == null || validTimes == null || revisions == null
        || joinFields == null || keySides == null || keyFields == null || aggFuncs == null || aggSides == null
        || aggFields == null || progSides == null || progFields == null || progCode == null || progConsts == null
        || entryKinds == null || orderIndexes == null || orderAsc == null || orderEmptyLeast == null
        || databases.length != 2 || keySides.length != keyFields.length || aggSides.length != aggFuncs.length
        || aggFields.length != aggFuncs.length || progFields.length != progSides.length
        || progCode.length != progSides.length || progConsts.length != progSides.length
        || orderAsc.length != orderIndexes.length || orderEmptyLeast.length != orderIndexes.length
        || !(compiler instanceof SirixTranslator translator)) {
      return null;
    }
    final AST returnRecord = returnRecord(node);
    if (returnRecord == null || returnRecord.getChildCount() != entryKinds.length) {
      return null;
    }
    final String[] entryNames = new String[entryKinds.length];
    for (int i = 0; i < entryNames.length; i++) {
      final AST nameNode = returnRecord.getChild(i).getChild(0);
      entryNames[i] = nameNode.getValue() instanceof Str str
          ? str.stringValue()
          : String.valueOf(nameNode.getValue());
    }
    final SirixJoinedGroupAggregateExpr.Side[] sides = new SirixJoinedGroupAggregateExpr.Side[2];
    for (int side = 0; side < 2; side++) {
      final SirixGroupAggregateExpr.RoutedSource routed = txTimes[side] == null
          ? null
          : new SirixGroupAggregateExpr.RoutedSource(databases[side], resources[side],
              translator.routedInstant(txTimes[side]), translator.routedInstant(validTimes[side]));
      sides[side] = new SirixJoinedGroupAggregateExpr.Side(databases[side], resources[side], routed, revisions[side],
          joinFields[side]);
    }
    return new SirixJoinedGroupAggregateExpr(executor, sides, keySides, keyFields, aggFuncs, aggSides, aggFields,
        progSides, progFields, progCode, progConsts, entryKinds, entryNames, orderIndexes, orderAsc, orderEmptyLeast,
        generic);
  }

  /** The {@code ObjectConstructor} of a pipe's return, or {@code null}. */
  private static AST returnRecord(final AST pipe) {
    AST end = pipe.getChild(0);
    while (end != null && end.getType() != XQ.End) {
      end = end.getLastChild();
    }
    if (end == null || end.getChildCount() < 1 || end.getChild(0).getType() != XQ.ObjectConstructor) {
      return null;
    }
    return end.getChild(0);
  }

  /**
   * The executor-side request an annotated plain pipe describes, or {@code null} when the annotations
   * are not this strategy's or the shape carries what the correlated route declines.
   */
  private static RoutedGroupRequest routedGroupRequest(final AST pipe) {
    final String[] sourcePath = (String[]) pipe.getProperty("VECTORIZED_SOURCE_PATH_PREFIX");
    final String[] groupFields = (String[]) pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_GROUP_FIELDS);
    final String[] keyNames = (String[]) pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_NAMES);
    final String[] funcs = (String[]) pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_FUNCS);
    final String[] aggFields = (String[]) pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_FIELDS);
    final String[] outNames = (String[]) pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_OUT_NAMES);
    if (sourcePath == null || groupFields == null || keyNames == null || funcs == null || aggFields == null
        || outNames == null || pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_OFFSETS) != null
        || pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_COND_ELSE) != null
        || pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_REGEX_PATTERN) != null
        || pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_STRINGIFY) != null
        || pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_HAVING) != null
        || pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_DECOR_POS) != null
        || pipe.getProperty(GroupAggregateDetectionStage.GROUP_AGG_CONST_ENTRY_POS) != null) {
      return null;
    }
    final SirixVectorizedExecutor.ComputedLane[] lanes = computedLanes(pipe);
    if (lanes == null && hasComputedAggregate(aggFields)) {
      return null;
    }
    return new RoutedGroupRequest(sourcePath, servedPredicate(pipe), groupFields, keyNames, funcs, aggFields, outNames,
        lanes);
  }

  /**
   * The index-routed source of {@code node}, with its two instant expressions compiled at the
   * pipeline's entry scope, or {@code null} when the pipe is an ordinary document scan.
   */
  private static SirixGroupAggregateExpr.@org.jspecify.annotations.Nullable RoutedSource routedSource(final AST node,
      final Compiler compiler) throws QueryException {
    if (!Boolean.TRUE.equals(node.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE))) {
      return null;
    }
    final String database = (String) node.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE_DATABASE);
    final String resource = (String) node.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE_RESOURCE);
    final AST txTime = (AST) node.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE_TX_TIME);
    final AST validTime = (AST) node.getProperty(IndexRoutedSourceStage.ROUTED_SOURCE_VALID_TIME);
    if (database == null || resource == null || txTime == null || validTime == null
        || !(compiler instanceof SirixTranslator translator)) {
      return null;
    }
    return new SirixGroupAggregateExpr.RoutedSource(database, resource, translator.routedInstant(txTime),
        translator.routedInstant(validTime));
  }

  /** Whether the order-by specs name every group key (positions {@code 0..keyCount-1}). */
  private static boolean ordersEveryKey(final int[] orderIndexes, final int keyCount) {
    if (orderIndexes == null) {
      return false;
    }
    for (int key = 0; key < keyCount; key++) {
      boolean named = false;
      for (final int index : orderIndexes) {
        named |= index == key;
      }
      if (!named) {
        return false;
      }
    }
    return true;
  }

  private static boolean hasComputedAggregate(final String[] aggFields) {
    for (final String field : aggFields) {
      if (field != null && field.startsWith(GroupAggregateDetectionStage.COMPUTED_FIELD_PREFIX)) {
        return true;
      }
    }
    return false;
  }

  /** The computed pre-group programs the detection stage annotated, or {@code null} for none. */
  private static SirixVectorizedExecutor.ComputedLane @org.jspecify.annotations.Nullable [] computedLanes(
      final AST node) {
    final String[][] fields = (String[][]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_PROG_FIELDS);
    final int[][] code = (int[][]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_PROG_CODE);
    final long[][] consts = (long[][]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_PROG_CONSTS);
    if (fields == null || code == null || consts == null || code.length != fields.length
        || consts.length != fields.length) {
      return null;
    }
    final SirixVectorizedExecutor.ComputedLane[] lanes = new SirixVectorizedExecutor.ComputedLane[fields.length];
    for (int i = 0; i < fields.length; i++) {
      lanes[i] = new SirixVectorizedExecutor.ComputedLane(fields[i], code[i], consts[i]);
    }
    return lanes;
  }

  /**
   * The predicate a group-aggregate serving expr applies: Brackit's own tree when it represented the
   * {@code where}, else the chain-aware tree {@link GroupAggregateDetectionStage} built for a
   * nested-deref filter Brackit's direct-deref leaves cannot name. Never both — the detection stage
   * writes its own only when Brackit's is absent — and never neither when the pipeline HAS a
   * selection: the stage declines instead, so a served pipeline always carries the filter.
   */
  private static PredicateNode servedPredicate(final AST node) {
    final PredicateNode brackitTree = (PredicateNode) node.getProperty("VECTORIZED_PREDICATE_TREE");
    return brackitTree != null
        ? brackitTree
        : (PredicateNode) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_PREDICATE);
  }

  /**
   * Compile-time admission for a serving expr: no ref (older brackit / unannotated), a ref the
   * executor accepts outright, or a {@link SourceRef.Kind#VARIABLE} ref — which cannot be judged at
   * compile time but IS verifiable at evaluation time via
   * {@code VectorizedExecutor#acceptsSource(SourceRef, QueryContext)}; the expr carries the ref and
   * declines to its generic fallback when the runtime binding is foreign.
   */
  static boolean acceptsOrRuntimeCheckable(final SirixExecutorProvider executor, final SourceRef ref) {
    return ref == null || ref.kind() == SourceRef.Kind.VARIABLE || executor.acceptsSource(ref);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Operator join(Operator in, AST node, Compiler compiler) throws QueryException {
    // Check for intersection join annotation from IndexDecompositionStage
    final boolean isIntersectionJoin = Boolean.TRUE.equals(node.getProperty(CostProperties.INTERSECTION_JOIN));

    if (isIntersectionJoin) {
      // Force skipSort=true for hash-based intersection
      node.setProperty("skipSort", true);
    }

    return super.join(in, node, compiler);
  }
}
