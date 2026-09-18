/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.index.projection.ProjectionIndexBuilder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An independently sorted access path over the fields of one covering projection, in the manner of
 * a table's {@code ORDER BY} key.
 *
 * <p>Column numbers refer to the projection definition's ordered field list. Every projected record
 * is kept in the view, ordered by the key columns and then by its stable record key. The
 * declaration names columns only: a query's equality filter on a leading prefix of the key columns
 * is answered as a key range at query time. This declaration is persisted with the index catalogue
 * and applies identically to initial construction and fine-grained maintenance.</p>
 */
public record ProjectionSortedSpec(List<Integer> keyColumns) {

  public ProjectionSortedSpec {
    keyColumns = List.copyOf(keyColumns);
    if (keyColumns.isEmpty()) {
      throw new IllegalArgumentException("sorted projection needs at least one key column");
    }
    final Set<Integer> seenKeys = new HashSet<>(keyColumns.size());
    for (final Integer column : keyColumns) {
      if (column < 0 || !seenKeys.add(column)) {
        throw new IllegalArgumentException("sorted projection key columns must be distinct and nonnegative");
      }
    }
  }

  /**
   * Reject key columns outside the projection's field list, or whose declared type a sort key cannot
   * order exactly. String, long, boolean and temporal columns are sortable; floating, decimal and
   * array-element (set) columns are not.
   *
   * @throws IllegalArgumentException naming the offending column
   */
  public void validate(final List<Path<QNm>> fieldPaths, final List<Type> fieldTypes) {
    if (fieldPaths.isEmpty() || fieldPaths.size() != fieldTypes.size()) {
      throw new IllegalArgumentException("sorted projection needs declared, typed fields");
    }
    for (final int column : keyColumns) {
      if (column >= fieldPaths.size()) {
        throw new IllegalArgumentException("sorted projection key column " + column + " is outside the field list");
      }
      if (!ProjectionIndexBuilder.isSortKeyType(fieldTypes.get(column), fieldPaths.get(column))) {
        throw new IllegalArgumentException("sorted projection key column " + column + " (" + fieldPaths.get(column)
            + ") has type " + fieldTypes.get(column)
            + ", which a sort key cannot order exactly; use a string, long, boolean or temporal column");
      }
    }
  }
}
