package io.sirix.query.compiler.translator;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.compiler.translator.Compiler;
import io.brackit.query.expr.VectorizedGroupByExpr;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Expr;
import io.brackit.query.operator.Operator;
import io.brackit.query.operator.Start;
import io.brackit.query.operator.TableJoin;
import io.brackit.query.util.Cmp;
import io.sirix.query.compiler.optimizer.GroupAggregateDetectionStage;
import io.sirix.query.compiler.optimizer.RowMaterializeDetectionStage;
import io.sirix.query.compiler.optimizer.SortedScanDetectionStage;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.scan.SirixVectorizedExecutor;

/**
 * Sirix-aware pipeline strategy that extends Brackit's sequential strategy
 * with support for optimizer annotations.
 *
 * <p>When the optimizer marks a join as an intersection join
 * ({@code INTERSECTION_JOIN=true}), this strategy forces hash-based
 * execution ({@code skipSort=true}) to avoid unnecessary post-probe
 * sorting. Index intersection results are already unique by nodeKey,
 * so sort+dedup is wasted work.</p>
 */
public final class SirixPipelineStrategy extends SequentialPipelineStrategy {

  /**
   * P5b stage 7a: consume the {@code SIRIX_GROUP_AGG_*} annotations from
   * {@link io.sirix.query.compiler.optimizer.GroupAggregateDetectionStage}. The generic
   * pipeline is ALWAYS compiled (via {@code super}) and rides along as the runtime
   * fallback, so serving declines can never change an answer — only its cost.
   */
  @Override
  public Expr compilePipeExpr(AST node, Compiler compiler) throws QueryException {
    final Expr generic = super.compilePipeExpr(node, compiler);
    // P5b stage 7b (+gap 1b multi-key): sorted-scan serving. Brackit's own
    // supportsSortedScan hook DROPS the predicate (its sorted() factory never receives
    // it), so sirix consumes its OWN SortedScanDetectionStage annotations here instead —
    // predicate included — and keeps supportsSortedScan false.
    if (Boolean.TRUE.equals(node.getProperty(SortedScanDetectionStage.SORTED_SCAN))
        && !(generic instanceof VectorizedGroupByExpr)
        && SequentialPipelineStrategy.getVectorizedExecutor()
            instanceof SirixVectorizedExecutor sortExecutor) {
      final String[] sortSourcePath = (String[]) node.getProperty("VECTORIZED_SOURCE_PATH_PREFIX");
      final String[] orderFields =
          (String[]) node.getProperty(SortedScanDetectionStage.SORTED_FIELDS);
      final boolean[] descending =
          (boolean[]) node.getProperty(SortedScanDetectionStage.SORTED_DESC);
      final SourceRef sortSourceRef = (SourceRef) node.getProperty("VECTORIZED_SOURCE_REF");
      // Gap 3: a sole-consumer fn:subsequence over this pipe caps how many sorted rows
      // can ever be pulled — the executor then heap-selects top-K instead of full-sorting.
      final Long topK = (Long) node.getProperty(SortedScanDetectionStage.SORTED_LIMIT);
      if (sortSourcePath != null && orderFields != null && descending != null
          && orderFields.length == descending.length
          && (sortSourceRef == null || sortExecutor.acceptsSource(sortSourceRef))) {
        return new SirixSortedScanExpr(sortExecutor, sortSourcePath,
            (PredicateNode) node.getProperty("VECTORIZED_PREDICATE_TREE"), orderFields,
            descending, topK == null ? -1L : topK, sortExecutor.boundDatabaseName(), generic);
      }
    }
    // P5b stage 7c: covered-row serving (record-constructor returns over covered fields).
    if (Boolean.TRUE.equals(node.getProperty(RowMaterializeDetectionStage.ROW_MAT))
        && !(generic instanceof VectorizedGroupByExpr)
        && SequentialPipelineStrategy.getVectorizedExecutor()
            instanceof SirixVectorizedExecutor rowExecutor) {
      final String[] rowSourcePath = (String[]) node.getProperty("VECTORIZED_SOURCE_PATH_PREFIX");
      final String[] rowFields =
          (String[]) node.getProperty(RowMaterializeDetectionStage.ROW_MAT_FIELDS);
      final String[] rowOutNames =
          (String[]) node.getProperty(RowMaterializeDetectionStage.ROW_MAT_OUT_NAMES);
      final int[] rowDirect = (int[]) node.getProperty(RowMaterializeDetectionStage.ROW_MAT_DIRECT);
      final int[][] rowCodes =
          (int[][]) node.getProperty(RowMaterializeDetectionStage.ROW_MAT_CODES);
      final long[][] rowConsts =
          (long[][]) node.getProperty(RowMaterializeDetectionStage.ROW_MAT_CONSTS);
      final SourceRef rowSourceRef = (SourceRef) node.getProperty("VECTORIZED_SOURCE_REF");
      if (rowSourcePath != null && rowFields != null && rowOutNames != null && rowDirect != null
          && rowCodes != null && rowConsts != null
          && (rowSourceRef == null || rowExecutor.acceptsSource(rowSourceRef))) {
        return new SirixRowMaterializeExpr(rowExecutor, rowSourcePath,
            (PredicateNode) node.getProperty("VECTORIZED_PREDICATE_TREE"), rowFields,
            rowOutNames, rowDirect, rowCodes, rowConsts, generic);
      }
    }
    if (!Boolean.TRUE.equals(node.getProperty(GroupAggregateDetectionStage.GROUP_AGG))) {
      return generic;
    }
    if (generic instanceof VectorizedGroupByExpr) {
      // Overlap shape (canonical count return matches BOTH detections): brackit's expr
      // THROWS on decline instead of falling back, so it must not become our "generic
      // fallback" — leave it as-is (pre-stage-7a behavior for that shape).
      return generic;
    }
    if (!(SequentialPipelineStrategy.getVectorizedExecutor()
        instanceof SirixVectorizedExecutor sirixExecutor)) {
      return generic;
    }
    // Source identity/revision gate — the same check brackit's own vectorized dispatch
    // applies: an executor bound to another resource, or pinned to an older revision
    // while the query opens latest, must not serve.
    final SourceRef sourceRef = (SourceRef) node.getProperty("VECTORIZED_SOURCE_REF");
    if (sourceRef != null && !sirixExecutor.acceptsSource(sourceRef)) {
      return generic;
    }
    final String[] sourcePath = (String[]) node.getProperty("VECTORIZED_SOURCE_PATH_PREFIX");
    final PredicateNode predicate = (PredicateNode) node.getProperty("VECTORIZED_PREDICATE_TREE");
    final String[] groupFields =
        (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_GROUP_FIELDS);
    final String[] keyNames =
        (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_KEY_NAMES);
    final String[] funcs =
        (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_FUNCS);
    final String[] aggFields =
        (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_FIELDS);
    final String[] outNames =
        (String[]) node.getProperty(GroupAggregateDetectionStage.GROUP_AGG_OUT_NAMES);
    if (sourcePath == null || groupFields == null || keyNames == null || funcs == null
        || aggFields == null || outNames == null) {
      return generic;
    }
    return new SirixGroupAggregateExpr(sirixExecutor, sourcePath, predicate, groupFields, keyNames,
        funcs, aggFields, outNames, generic);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Operator join(Operator in, AST node, Compiler compiler) throws QueryException {
    // Check for intersection join annotation from IndexDecompositionStage
    final boolean isIntersectionJoin = Boolean.TRUE.equals(
        node.getProperty(CostProperties.INTERSECTION_JOIN));

    if (isIntersectionJoin) {
      // Force skipSort=true for hash-based intersection
      node.setProperty("skipSort", true);
    }

    return super.join(in, node, compiler);
  }
}
