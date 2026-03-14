package io.sirix.query.compiler.expression;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.ExprUtil;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.optimizer.VectorizedRoutingStage;
import io.sirix.query.compiler.vectorized.ColumnBatch;
import io.sirix.query.compiler.vectorized.ColumnarScanAxis;
import io.sirix.query.compiler.vectorized.ColumnarStringFilter;
import io.sirix.query.compiler.vectorized.ComparisonOperator;
import io.sirix.query.compiler.vectorized.VectorizedPredicate;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonItemFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Physical operator for vectorized pipeline execution.
 *
 * <p>Follows the {@link IndexExpr} pattern: constructed from AST properties
 * during translation, evaluates by opening a storage engine reader and
 * scanning columnar batches with SIMD-accelerated filtering.</p>
 *
 * <h3>Evaluate flow (columnar route):</h3>
 * <ol>
 *   <li>Look up database → JsonDBCollection</li>
 *   <li>Open resource session → JsonResourceSession</li>
 *   <li>Begin read-only transaction → JsonNodeReadOnlyTrx</li>
 *   <li>Get storage engine reader → StorageEngineReader (bridge to columnar scan)</li>
 *   <li>Create ColumnarScanAxis for multi-page columnar scan</li>
 *   <li>Loop: while nextBatch() != null
 *     <ul>
 *       <li>Apply string EQ/NE predicates via ColumnarStringFilter (FSST-aware)</li>
 *       <li>Apply numeric and string range predicates via scalar fallback
 *           (materialize string value → parse → compare row-by-row)</li>
 *       <li>Skip batch if selectionCount == 0</li>
 *       <li>Materialize surviving rows → moveTo(nodeKey) → add to result</li>
 *     </ul>
 *   </li>
 *   <li>Return ItemSequence of all surviving items</li>
 * </ol>
 *
 * <h3>HFT-grade analysis:</h3>
 * <p>No hot-path allocation violations: ColumnarScanAxis pre-allocates extraction
 * arrays, ColumnBatch reuses arrays via reset(). String EQ/NE uses FSST-aware
 * compressed comparison via ColumnarStringFilter. Numeric predicates currently use
 * scalar fallback (parse-then-compare) — SIMD acceleration via ColumnBatchFilter
 * will be available when per-field typed columns are added to ColumnarScanAxis.
 * The only per-batch allocation is JsonDBItem wrappers for surviving rows
 * (unavoidable — Brackit requires Item objects).</p>
 */
public final class VectorizedPipelineExpr implements Expr {

  private final Map<String, Object> properties;
  private final String databaseName;
  private final String resourceName;
  private final Integer revision;
  @SuppressWarnings("unchecked")
  private final List<VectorizedPredicate> predicates;
  private final String route;

  @SuppressWarnings("unchecked")
  public VectorizedPipelineExpr(final Map<String, Object> properties) {
    this.properties = properties;
    this.databaseName = (String) properties.get("databaseName");
    this.resourceName = (String) properties.get("resourceName");
    this.revision = (Integer) properties.get("revision");
    this.predicates = (List<VectorizedPredicate>) properties.get(VectorizedRoutingStage.VECTORIZED_PREDICATES);
    this.route = (String) properties.get(VectorizedRoutingStage.VECTORIZED_ROUTE);
  }

  @Override
  public Sequence evaluate(QueryContext ctx, Tuple tuple) throws QueryException {
    final var jsonItemStore = ((SirixQueryContext) ctx).getJsonItemStore();

    final JsonDBCollection jsonCollection = jsonItemStore.lookup(databaseName);
    final var database = jsonCollection.getDatabase();

    final var resourceSession = database.beginResourceSession(resourceName);
    final JsonNodeReadOnlyTrx rtx;
    try {
      rtx = revision == null || revision == -1
          ? resourceSession.beginNodeReadOnlyTrx()
          : resourceSession.beginNodeReadOnlyTrx(revision);
    } catch (final Exception e) {
      resourceSession.close();
      throw e;
    }

    try {
      return evaluateColumnar(rtx, resourceSession, jsonCollection);
    } finally {
      // Resources can be closed here because evaluateColumnar() fully materializes
      // all surviving rows into an ArrayList before returning. The returned
      // ItemSequence does not hold references back to the transaction.
      rtx.close();
      resourceSession.close();
    }
  }

  /**
   * Columnar scan-filter-materialize loop.
   *
   * <p>Hot path: per-batch filtering uses pre-allocated arrays from ColumnarScanAxis
   * and in-place selection vector compaction from ColumnBatchFilter. No allocations
   * in the inner loop except for JsonDBItem wrappers on surviving rows.</p>
   */
  private Sequence evaluateColumnar(JsonNodeReadOnlyTrx rtx, JsonResourceSession resourceSession,
      JsonDBCollection jsonCollection) throws QueryException {
    final var sequence = new ArrayList<Item>();
    final var jsonItemFactory = new JsonItemFactory();

    try (final var axis = new ColumnarScanAxis(rtx.getStorageEngineReader(), false)) {
      ColumnBatch batch;
      while ((batch = axis.nextBatch()) != null) {
        // Apply predicates to narrow the selection vector
        applyPredicates(batch);

        // Skip batch if no rows survived filtering
        if (batch.selectionCount() == 0) {
          continue;
        }

        // Materialize surviving rows
        for (int i = 0; i < batch.selectionCount(); i++) {
          final int row = batch.selectedRow(i);
          final long nodeKey = batch.getLong(ColumnarScanAxis.COL_NODE_KEY, row);
          rtx.moveTo(nodeKey);
          sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
        }
      }
    } catch (final Exception e) {
      throw new QueryException(new QNm(e.getMessage()), e);
    }

    if (sequence.isEmpty()) {
      return null;
    }
    return new ItemSequence(sequence.toArray(new Item[0]));
  }

  /**
   * Apply all extracted predicates to a batch.
   *
   * <p>Each predicate narrows the selection vector in-place. String EQ/NE predicates
   * use ColumnarStringFilter with FSST-aware compressed comparison. All other predicates
   * (numeric and string range) fall back to scalar evaluation: materialize the string
   * value from {@code COL_STRING_VALUE}, parse to the target type, then compare.</p>
   *
   * <p><b>Why scalar fallback for numerics:</b> ColumnarScanAxis has fixed columns
   * (nodeKey, parentKey, stringValue, deweyId) — there are no per-field typed columns.
   * SIMD filtering via {@link ColumnBatchFilter} requires a dedicated typed column, which
   * doesn't exist for arbitrary JSON fields yet. When per-field columnar storage is added,
   * this method should resolve field names to column indices and use SIMD paths.</p>
   */
  private void applyPredicates(ColumnBatch batch) {
    if (predicates == null) {
      return;
    }
    for (final VectorizedPredicate pred : predicates) {
      switch (pred.type()) {
        case INT64 -> filterLongScalar(batch, pred);
        case FLOAT64 -> filterDoubleScalar(batch, pred);
        case STRING -> {
          if (pred.isStringFilterable()) {
            // EQ/NE: use FSST-aware compressed comparison
            if (pred.op() == ComparisonOperator.EQ) {
              ColumnarStringFilter.filterStringEqual(
                  batch, ColumnarScanAxis.COL_STRING_VALUE, (String) pred.constant());
            } else {
              ColumnarStringFilter.filterStringNotEqual(
                  batch, ColumnarScanAxis.COL_STRING_VALUE, (String) pred.constant());
            }
          } else {
            // String range predicates: materialize and compare row-by-row
            filterStringScalar(batch, pred);
          }
        }
        default -> {
          // Unsupported column type — skip predicate (conservative)
        }
      }
    }
  }

  /**
   * Scalar fallback for string range predicates (LT/LE/GT/GE).
   * Materializes string values and compares row-by-row.
   */
  private static void filterStringScalar(ColumnBatch batch, VectorizedPredicate pred) {
    final String constant = (String) pred.constant();
    final String[] values = batch.materializeAllSelected(ColumnarScanAxis.COL_STRING_VALUE);
    final int[] sel = batch.selectionVector();
    final int inputCount = batch.selectionCount();
    int outPos = 0;

    for (int i = 0; i < inputCount; i++) {
      final int row = sel[i];
      final String value = values[i];
      if (value != null && compareString(value, constant, pred.op())) {
        sel[outPos++] = row;
      }
    }
    batch.setSelectionCount(outPos);
  }

  /**
   * Scalar fallback for INT64 predicates. Materializes string values,
   * parses each to long, and compares row-by-row.
   *
   * <p>Required because ColumnarScanAxis has no per-field typed column for
   * arbitrary JSON fields. Values are stored as strings in COL_STRING_VALUE.</p>
   */
  private static void filterLongScalar(ColumnBatch batch, VectorizedPredicate pred) {
    final long constant = ((Number) pred.constant()).longValue();
    final ComparisonOperator op = pred.op();
    final String[] values = batch.materializeAllSelected(ColumnarScanAxis.COL_STRING_VALUE);
    final int[] sel = batch.selectionVector();
    final int inputCount = batch.selectionCount();
    int outPos = 0;

    for (int i = 0; i < inputCount; i++) {
      final int row = sel[i];
      final String value = values[i];
      if (value != null) {
        try {
          if (op.testLong(Long.parseLong(value), constant)) {
            sel[outPos++] = row;
          }
        } catch (NumberFormatException ignored) {
          // Non-numeric value — exclude from results
        }
      }
    }
    batch.setSelectionCount(outPos);
  }

  /**
   * Scalar fallback for FLOAT64 predicates. Materializes string values,
   * parses each to double, and compares row-by-row.
   */
  private static void filterDoubleScalar(ColumnBatch batch, VectorizedPredicate pred) {
    final double constant = ((Number) pred.constant()).doubleValue();
    final ComparisonOperator op = pred.op();
    final String[] values = batch.materializeAllSelected(ColumnarScanAxis.COL_STRING_VALUE);
    final int[] sel = batch.selectionVector();
    final int inputCount = batch.selectionCount();
    int outPos = 0;

    for (int i = 0; i < inputCount; i++) {
      final int row = sel[i];
      final String value = values[i];
      if (value != null) {
        try {
          if (op.testDouble(Double.parseDouble(value), constant)) {
            sel[outPos++] = row;
          }
        } catch (NumberFormatException ignored) {
          // Non-numeric value — exclude from results
        }
      }
    }
    batch.setSelectionCount(outPos);
  }

  /**
   * Scalar string comparison for range predicates.
   */
  private static boolean compareString(String value, String constant, ComparisonOperator op) {
    final int cmp = value.compareTo(constant);
    return switch (op) {
      case EQ -> cmp == 0;
      case NE -> cmp != 0;
      case LT -> cmp < 0;
      case LE -> cmp <= 0;
      case GT -> cmp > 0;
      case GE -> cmp >= 0;
    };
  }

  @Override
  public Item evaluateToItem(QueryContext ctx, Tuple tuple) throws QueryException {
    return ExprUtil.asItem(evaluate(ctx, tuple));
  }

  @Override
  public boolean isUpdating() {
    return false;
  }

  @Override
  public boolean isVacuous() {
    return false;
  }
}
