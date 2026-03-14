package io.sirix.query.compiler.vectorized;

/**
 * Extracted predicate for vectorized pipeline execution.
 *
 * <p>Represents a single scalar comparison predicate of the form
 * {@code field op constant} that can be evaluated in a columnar
 * batch scan via SIMD ({@link ColumnBatchFilter}) or string
 * filtering ({@link ColumnarStringFilter}).</p>
 *
 * <p><b>Limitation:</b> {@link ColumnarStringFilter} only supports
 * {@code filterStringEqual} and {@code filterStringNotEqual}.
 * String range predicates (LT, GT, etc.) fall back to row-by-row
 * evaluation in {@code VectorizedPipelineExpr}.</p>
 *
 * @param fieldName the JSON field name (e.g., "price", "category")
 * @param op        the comparison operator
 * @param constant  the constant value (Long, Double, or String)
 * @param type      the column type for SIMD dispatch
 */
public record VectorizedPredicate(
    String fieldName,
    ComparisonOperator op,
    Object constant,
    ColumnType type
) {

  public VectorizedPredicate {
    if (fieldName == null || fieldName.isEmpty()) {
      throw new IllegalArgumentException("fieldName must not be null or empty");
    }
    if (op == null) {
      throw new IllegalArgumentException("op must not be null");
    }
    if (constant == null) {
      throw new IllegalArgumentException("constant must not be null");
    }
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
  }

  /**
   * Whether this predicate can be evaluated by {@link ColumnarStringFilter}.
   * Only EQ and NE are supported for string columns.
   */
  public boolean isStringFilterable() {
    return type == ColumnType.STRING && (op == ComparisonOperator.EQ || op == ComparisonOperator.NE);
  }
}
