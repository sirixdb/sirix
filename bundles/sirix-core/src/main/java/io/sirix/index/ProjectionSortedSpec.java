/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A partial, independently sorted access path over fields of one covering projection.
 *
 * <p>Column numbers refer to the projection definition's ordered field list. The equality
 * predicates select membership; key columns determine the order of matching rows, followed by the
 * stable record key. Literal text is interpreted under each field's declared type when the row
 * encoder binds. This declaration is persisted with the index catalogue and applies identically to
 * initial construction and fine-grained maintenance.</p>
 */
public record ProjectionSortedSpec(List<Integer> keyColumns, List<Equality> equalities) {

  public record Equality(int column, String literal) {
    public Equality {
      if (column < 0) {
        throw new IllegalArgumentException("equality column must be nonnegative");
      }
      Objects.requireNonNull(literal, "literal");
    }
  }

  public ProjectionSortedSpec {
    keyColumns = List.copyOf(keyColumns);
    equalities = List.copyOf(equalities);
    if (keyColumns.isEmpty()) {
      throw new IllegalArgumentException("sorted projection needs at least one key column");
    }
    final Set<Integer> seenKeys = new HashSet<>(keyColumns.size());
    for (final Integer column : keyColumns) {
      if (column < 0 || !seenKeys.add(column)) {
        throw new IllegalArgumentException("sorted projection key columns must be distinct and nonnegative");
      }
    }
    final Set<Integer> seenEqualities = new HashSet<>(equalities.size());
    for (final Equality equality : equalities) {
      if (!seenEqualities.add(equality.column())) {
        throw new IllegalArgumentException("sorted projection equality columns must be distinct");
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
    for (final Equality equality : equalities) {
      if (equality.column() >= fieldCount) {
        throw new IllegalArgumentException(
            "sorted projection equality column " + equality.column() + " is outside the field list");
      }
    }
  }
}
