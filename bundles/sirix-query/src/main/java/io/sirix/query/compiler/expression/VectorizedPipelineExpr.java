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

  private final String databaseName;
  private final String resourceName;
  private final Integer revision;
  @SuppressWarnings("unchecked")
  private final List<VectorizedPredicate> predicates;
  private final String route;

  @SuppressWarnings("unchecked")
  public VectorizedPipelineExpr(final Map<String, Object> properties) {
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

    // Resources can be closed after evaluateColumnar() because it fully materializes
    // all surviving rows into an ArrayList. The returned ItemSequence does not hold
    // references back to the transaction. The primary-exception tracking below mirrors
    // the try-with-resources pattern to avoid masking the original exception.
    Throwable primaryEx = null;
    try {
      return evaluateColumnar(rtx, resourceSession, jsonCollection);
    } catch (final Throwable t) {
      primaryEx = t;
      throw t;
    } finally {
      if (primaryEx != null) {
        try { rtx.close(); } catch (final Throwable s) { primaryEx.addSuppressed(s); }
        try { resourceSession.close(); } catch (final Throwable s) { primaryEx.addSuppressed(s); }
      } else {
        rtx.close();
        resourceSession.close();
      }
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
   * use ColumnarStringFilter with FSST-aware compressed comparison. Numeric predicates
   * use a combined filter that handles both number-type rows (via COL_NUMERIC_VALUE)
   * and string-encoded numbers (via COL_STRING_VALUE) in a single pass.</p>
   *
   * <p><b>Design note:</b> SIMD and scalar fallback cannot be chained sequentially because
   * {@code ColumnBatchFilter.filterDouble} removes rows with null in COL_NUMERIC_VALUE
   * (string rows). A combined pass preserves both populations correctly.</p>
   */
  private void applyPredicates(ColumnBatch batch) {
    if (predicates == null) {
      return;
    }
    for (final VectorizedPredicate pred : predicates) {
      switch (pred.type()) {
        case INT64 -> filterNumericCombined(batch, pred, true);
        case FLOAT64 -> filterNumericCombined(batch, pred, false);
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
      final String value = values[row];
      if (value != null && compareString(value, constant, pred.op())) {
        sel[outPos++] = row;
      }
    }
    batch.setSelectionCount(outPos);
  }

  /**
   * Combined numeric predicate filter for batches containing both number-type
   * and string-type rows.
   *
   * <p>Number rows (non-null in COL_NUMERIC_VALUE) are compared directly via the
   * double column — fast scalar path (SIMD can be added by splitting populations).
   * String rows (null in COL_NUMERIC_VALUE, non-null in COL_STRING_VALUE) are
   * materialized, parsed to number, and compared row-by-row.</p>
   *
   * <p><b>Why not chain SIMD + scalar?</b> {@code ColumnBatchFilter.filterDouble}
   * removes null rows from the selection vector. String rows have null in
   * COL_NUMERIC_VALUE, so they'd be removed before the scalar fallback runs.
   * This combined pass handles both populations correctly in one iteration.</p>
   *
   * @param batch  the column batch to filter in-place
   * @param pred   the numeric predicate
   * @param isLong true for INT64 predicates (parse as long), false for FLOAT64
   */
  private static void filterNumericCombined(ColumnBatch batch, VectorizedPredicate pred, boolean isLong) {
    final double doubleConstant = ((Number) pred.constant()).doubleValue();
    final long longConstant = isLong ? ((Number) pred.constant()).longValue() : 0;
    final ComparisonOperator op = pred.op();

    final double[] numColumn = batch.doubleColumn(ColumnarScanAxis.COL_NUMERIC_VALUE);
    final boolean[] numNulls = batch.nullFlags(ColumnarScanAxis.COL_NUMERIC_VALUE);
    final int[] sel = batch.selectionVector();
    final int inputCount = batch.selectionCount();

    // Check if any selected rows are string rows (null in COL_NUMERIC_VALUE).
    // Skip expensive string materialization when all rows are number-type.
    boolean hasStringRows = false;
    for (int i = 0; i < inputCount; i++) {
      if (numNulls[sel[i]]) {
        hasStringRows = true;
        break;
      }
    }

    // Only materialize string values when needed (FSST decompression is expensive)
    final String[] strValues = hasStringRows
        ? batch.materializeAllSelected(ColumnarScanAxis.COL_STRING_VALUE) : null;

    int outPos = 0;
    for (int i = 0; i < inputCount; i++) {
      final int row = sel[i];
      if (!numNulls[row]) {
        // Number row: compare via double column (fast path)
        if (op.testDouble(numColumn[row], doubleConstant)) {
          sel[outPos++] = row;
        }
      } else if (strValues != null) {
        // String row: parse and compare (slow path)
        final String value = strValues[row];
        if (value != null) {
          try {
            if (isLong) {
              if (op.testLong(Long.parseLong(value), longConstant)) {
                sel[outPos++] = row;
              }
            } else {
              if (op.testDouble(Double.parseDouble(value), doubleConstant)) {
                sel[outPos++] = row;
              }
            }
          } catch (NumberFormatException e) {
            // Not a valid number — exclude from results
          }
        }
      }
    }
    batch.setSelectionCount(outPos);
  }

  /**
   * Scalar string comparison for range predicates.
   */
  private static boolean compareString(String value, String constant, ComparisonOperator op) {
    return op.testCompareTo(value.compareTo(constant));
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
