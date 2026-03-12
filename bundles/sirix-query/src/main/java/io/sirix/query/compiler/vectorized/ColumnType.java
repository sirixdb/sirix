package io.sirix.query.compiler.vectorized;

/**
 * Supported column types for vectorized batch processing.
 *
 * <p>Each type maps to a primitive array representation in {@link ColumnBatch}
 * for cache-friendly, SIMD-compatible columnar storage.</p>
 */
public enum ColumnType {

  /** 64-bit signed integer (stored as {@code long[]}). */
  INT64,

  /** 64-bit IEEE 754 floating point (stored as {@code double[]}). */
  FLOAT64,

  /** Variable-length UTF-8 string (stored as {@code String[]}). */
  STRING,

  /** Boolean flag (stored as {@code boolean[]}). */
  BOOLEAN
}
