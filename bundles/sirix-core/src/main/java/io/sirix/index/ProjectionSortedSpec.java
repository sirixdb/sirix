/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index;

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

  void validateFieldCount(final int fieldCount) {
    if (fieldCount < 1) {
      throw new IllegalArgumentException("sorted projection needs declared fields");
    }
    for (final int column : keyColumns) {
      if (column >= fieldCount) {
        throw new IllegalArgumentException("sorted projection key column " + column + " is outside the field list");
      }
    }
  }
}
