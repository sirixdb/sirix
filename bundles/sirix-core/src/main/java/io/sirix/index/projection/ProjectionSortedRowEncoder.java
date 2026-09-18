/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.IndexDef;
import io.sirix.index.ProjectionSortedSpec;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Compiled, transaction-confined key encoding for a sorted projection view.
 *
 * <p>
 * Every extracted row has exactly one key, read from the extractor's reusable primitive buffers
 * without allocating or probing a dictionary. Sort keys use the original UTF-8 bytes, since
 * dictionary ids reflect insertion order rather than value order. A row whose key field cannot be
 * represented exactly, or whose whole key exceeds {@link ProjectionSortKeyCodec#MAX_KEY_BYTES},
 * receives the reserved unencodable key instead of failing its load or commit; the view then
 * declines to serve until no such row remains. An encoder bound to a view whose persisted layout
 * the extractor's column kinds no longer produce writes only reserved keys, so a view never mixes
 * two encodings. Callers must copy the borrowed key before extracting the next row.
 * </p>
 */
final class ProjectionSortedRowEncoder {

  private final ProjectionIndexRowExtractor extractor;
  private final int[] keyColumns;
  private final byte[] keyKinds;
  private final ProjectionSortKeyCodec.Layout layout;
  private final ProjectionSortKeyCodec.Writer keyWriter = new ProjectionSortKeyCodec.Writer();
  private final boolean encodesTargetLayout;
  private boolean unencodable;

  /** An encoder for a new view, whose layout is the one the extractor's column kinds produce. */
  ProjectionSortedRowEncoder(final IndexDef definition, final ProjectionIndexRowExtractor extractor) {
    this(definition, extractor, null);
  }

  /**
   * An encoder for an existing view persisted with {@code targetLayout}.
   *
   * @param targetLayout the view's persisted layout, or {@code null} for a new view
   */
  ProjectionSortedRowEncoder(final IndexDef definition, final ProjectionIndexRowExtractor extractor,
      final ProjectionSortKeyCodec.@Nullable Layout targetLayout) {
    Objects.requireNonNull(definition, "definition");
    this.extractor = Objects.requireNonNull(extractor, "extractor");
    final ProjectionSortedSpec spec =
        Objects.requireNonNull(definition.getProjectionSortedSpec(), "projection has no sorted-view declaration");
    final byte[] columnKinds = extractor.columnKindsRef();
    keyColumns = keyColumns(spec);
    keyKinds = new byte[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      keyKinds[i] = columnKinds[keyColumns[i]];
    }
    final ProjectionSortKeyCodec.Layout derived = ProjectionSortKeyCodec.Layout.of(columnKinds, keyColumns);
    encodesTargetLayout = targetLayout == null || targetLayout.equals(derived);
    layout = targetLayout == null
        ? derived
        : targetLayout;
  }

  /** Key layout of {@code definition}'s sorted view over a projection with {@code columnKinds}. */
  static ProjectionSortKeyCodec.Layout layoutOf(final IndexDef definition, final byte[] columnKinds) {
    final ProjectionSortedSpec spec =
        Objects.requireNonNull(definition.getProjectionSortedSpec(), "projection has no sorted-view declaration");
    return ProjectionSortKeyCodec.Layout.of(columnKinds, keyColumns(spec));
  }

  private static int[] keyColumns(final ProjectionSortedSpec spec) {
    final List<Integer> declaredKeys = spec.keyColumns();
    final int[] columns = new int[declaredKeys.size()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = declaredKeys.get(i);
    }
    return columns;
  }

  ProjectionSortKeyCodec.Layout layout() {
    return layout;
  }

  /** Whether the extractor's column kinds produce {@link #layout()}; otherwise every key is reserved. */
  boolean encodesTargetLayout() {
    return encodesTargetLayout;
  }

  /** Write the current row's ordered key, or its reserved unencodable key, to reusable scratch. */
  void writeKey(final long recordKey) {
    if (!encodesTargetLayout) {
      markUnencodable(recordKey);
      return;
    }
    unencodable = false;
    keyWriter.reset();
    for (int i = 0; i < keyColumns.length; i++) {
      final int column = keyColumns[i];
      if (!extractor.rowPresent(column)) {
        keyWriter.appendMissing();
        continue;
      }
      final byte kind = keyKinds[i];
      if (extractor.rowUnrepresentable(column)
          || (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG && extractor.rowNonIntegral(column))) {
        markUnencodable(recordKey);
        return;
      }
      if (ProjectionSortKeyCodec.Layout.isStringKind(kind)) {
        final byte[] value = extractor.rowStringUtf8(column);
        final int valueLength = extractor.rowStringUtf8Length(column);
        if (value == null || valueLength > ProjectionSortKeyCodec.MAX_KEY_BYTES) {
          markUnencodable(recordKey);
          return;
        }
        keyWriter.appendUtf8(value, 0, valueLength);
      } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
        keyWriter.appendBoolean(extractor.rowBoolean(column));
      } else {
        keyWriter.appendLong(extractor.rowLong(column));
      }
    }
    if (keyWriter.length() > ProjectionSortKeyCodec.MAX_KEY_BYTES - Long.BYTES) {
      markUnencodable(recordKey);
      return;
    }
    keyWriter.appendRecordKey(recordKey);
  }

  /** Whether the last written key is the reserved key of a row the view cannot order exactly. */
  boolean unencodable() {
    return unencodable;
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

  private void markUnencodable(final long recordKey) {
    keyWriter.writeUnencodable(recordKey);
    unencodable = true;
  }
}
