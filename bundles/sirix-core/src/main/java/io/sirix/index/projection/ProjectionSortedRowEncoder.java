/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.IndexDef;
import io.sirix.index.ProjectionSortedSpec;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Compiled, transaction-confined row admission and key encoding for a sorted projection view.
 *
 * <p>The index definition holds column positions and literal text; binding converts those literals
 * once into primitive longs, booleans, or UTF-8. Every matching row is then checked against the
 * extractor's reusable primitive buffers without allocating or probing a dictionary. Sort keys use
 * the original UTF-8 bytes, since dictionary ids reflect insertion order rather than value order.
 * Callers must copy the borrowed key before extracting the next row.</p>
 */
final class ProjectionSortedRowEncoder {

  private final ProjectionIndexRowExtractor extractor;
  private final int[] keyColumns;
  private final byte[] keyKinds;
  private final int[] equalityColumns;
  private final byte[] equalityKinds;
  private final long[] equalityLongs;
  private final boolean[] equalityBooleans;
  private final byte[][] equalityUtf8;
  private final ProjectionSortKeyCodec.Writer keyWriter = new ProjectionSortKeyCodec.Writer();

  ProjectionSortedRowEncoder(final IndexDef definition, final ProjectionIndexRowExtractor extractor) {
    Objects.requireNonNull(definition, "definition");
    this.extractor = Objects.requireNonNull(extractor, "extractor");
    final ProjectionSortedSpec spec = Objects.requireNonNull(definition.getProjectionSortedSpec(),
        "projection has no sorted-view declaration");
    final byte[] columnKinds = extractor.columnKindsRef();
    final List<Integer> declaredKeys = spec.keyColumns();
    keyColumns = new int[declaredKeys.size()];
    keyKinds = new byte[declaredKeys.size()];
    for (int i = 0; i < keyColumns.length; i++) {
      final int column = declaredKeys.get(i);
      final byte kind = columnKinds[column];
      requireSupported(kind, column);
      keyColumns[i] = column;
      keyKinds[i] = kind;
    }
    final List<ProjectionSortedSpec.Equality> equalities = spec.equalities();
    equalityColumns = new int[equalities.size()];
    equalityKinds = new byte[equalities.size()];
    equalityLongs = new long[equalities.size()];
    equalityBooleans = new boolean[equalities.size()];
    equalityUtf8 = new byte[equalities.size()][];
    for (int i = 0; i < equalities.size(); i++) {
      final ProjectionSortedSpec.Equality equality = equalities.get(i);
      final int column = equality.column();
      final byte kind = columnKinds[column];
      requireSupported(kind, column);
      equalityColumns[i] = column;
      equalityKinds[i] = kind;
      if (isString(kind)) {
        equalityUtf8[i] = equality.literal().getBytes(StandardCharsets.UTF_8);
      } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
        if (!"true".equals(equality.literal()) && !"false".equals(equality.literal())) {
          throw new IllegalArgumentException("boolean equality literal must be true or false");
        }
        equalityBooleans[i] = "true".equals(equality.literal());
      } else if (ProjectionIndexRowGroupPage.isTemporalKind(kind)) {
        final long parsed = ProjectionTemporalCodec.parse(kind, equality.literal());
        if (parsed == ProjectionTemporalCodec.NOT_CANONICAL) {
          throw new IllegalArgumentException("temporal equality literal is not canonical");
        }
        equalityLongs[i] = parsed;
      } else {
        equalityLongs[i] = Long.parseLong(equality.literal());
      }
    }
  }

  /** Evaluate the partial-view predicate against the extractor's current row. */
  boolean matches() {
    for (int i = 0; i < equalityColumns.length; i++) {
      final int column = equalityColumns[i];
      if (!extractor.rowPresent(column) || extractor.rowUnrepresentable(column)) {
        return false;
      }
      final byte kind = equalityKinds[i];
      if (isString(kind)) {
        final byte[] value = extractor.rowStringUtf8(column);
        final byte[] expected = equalityUtf8[i];
        if (value == null || extractor.rowStringUtf8Length(column) != expected.length
            || !Arrays.equals(value, 0, expected.length, expected, 0, expected.length)) {
          return false;
        }
      } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
        if (extractor.rowBoolean(column) != equalityBooleans[i]) {
          return false;
        }
      } else if ((kind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG
          && extractor.rowNonIntegral(column)) || extractor.rowLong(column) != equalityLongs[i]) {
        return false;
      }
    }
    return true;
  }

  /** Write one matching row's ordered key to reusable scratch; return false for a filtered row. */
  boolean writeKeyIfMatching(final long recordKey) {
    if (!matches()) {
      return false;
    }
    keyWriter.reset();
    for (int i = 0; i < keyColumns.length; i++) {
      final int column = keyColumns[i];
      if (!extractor.rowPresent(column)) {
        keyWriter.appendMissing();
        continue;
      }
      if (extractor.rowUnrepresentable(column)
          || (keyKinds[i] == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG
              && extractor.rowNonIntegral(column))) {
        throw new IllegalStateException("sorted projection key column " + column
            + " cannot represent record " + recordKey + " exactly");
      }
      final byte kind = keyKinds[i];
      if (isString(kind)) {
        final byte[] value = extractor.rowStringUtf8(column);
        if (value == null) {
          throw new IllegalStateException("sorted projection string key column " + column + " has no UTF-8 bytes");
        }
        keyWriter.appendUtf8(value, 0, extractor.rowStringUtf8Length(column));
      } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
        keyWriter.appendBoolean(extractor.rowBoolean(column));
      } else {
        keyWriter.appendLong(extractor.rowLong(column));
      }
    }
    keyWriter.appendRecordKey(recordKey);
    return true;
  }

  byte[] keyBytesRef() {
    return keyWriter.bytesRef();
  }

  int keyLength() {
    return keyWriter.length();
  }

  byte[] copyKey() {
    return keyWriter.copyKey();
  }

  private static boolean isString(final byte kind) {
    return kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
        || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
        || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT;
  }

  private static void requireSupported(final byte kind, final int column) {
    if (!isString(kind) && !ProjectionIndexRowGroupPage.isOrderedLongKind(kind)
        && kind != ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
      throw new IllegalArgumentException("sorted projection column " + column + " has unsupported kind " + kind);
    }
  }
}
