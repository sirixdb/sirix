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
  BOOLEAN,

  /**
   * Deferred byte reference into a backing MemorySegment (late materialization).
   *
   * <p>Stored as offset + length + page index per row. The actual bytes are
   * only decoded (e.g., FSST decompression) when explicitly materialized,
   * enabling SIMD filtering on compressed data without decompression.</p>
   */
  DEFERRED_BYTES,

  /** Raw byte array column (e.g., for DeweyIDs). Stored as {@code byte[][]}. */
  BYTES
}
