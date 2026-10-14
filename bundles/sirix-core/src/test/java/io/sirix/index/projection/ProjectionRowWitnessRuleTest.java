package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_UTC_TIMESTAMP;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_KIND_DATE;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.ROW_WITNESS_LEXICAL_UTC;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.ROW_WITNESS_NONE;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.ROW_WITNESS_NO_UTC;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.ROW_WITNESS_SOME_IMPURE;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.ROW_WITNESS_UNKNOWN;
import static io.sirix.index.projection.ProjectionIndexRowGroupPage.rowWitnessRule;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The table that decides whether a row can be carried into a rebuilt row group without reading its
 * record: what a column's persisted sticky flags prove about every present cell.
 *
 * <p>
 * It is the one place where a wrong entry turns a carried row into a wrong zone map or a wrong
 * provenance flag, so every kind and every flag combination is pinned here.
 * {@link ProjectionUnchangedRowCarryOverTest} checks the consequences end to end.
 * </p>
 */
final class ProjectionRowWitnessRuleTest {

  private static final byte NONE = 0;

  private static final byte[] VALUE_KINDS = {COLUMN_KIND_NUMERIC_LONG, COLUMN_KIND_BOOLEAN, COLUMN_KIND_STRING_DICT,
      COLUMN_KIND_STRING_SET, COLUMN_KIND_STRING_GLOBAL, COLUMN_KIND_DATE};

  @Test
  void aColumnWithoutARaisedFlagHasNoWitness() {
    for (final byte kind : VALUE_KINDS) {
      assertEquals(ROW_WITNESS_NONE, rowWitnessRule(kind, NONE), "kind " + kind);
    }
    // Purity is a positive assertion: a double or timestamp column that raised nothing carries it.
    assertEquals(ROW_WITNESS_NONE, rowWitnessRule(COLUMN_KIND_NUMERIC_DOUBLE, COLUMN_FLAG_PURE_DOUBLE_SOURCE));
    assertEquals(ROW_WITNESS_NONE, rowWitnessRule(COLUMN_KIND_TIMESTAMP, COLUMN_FLAG_PURE_UTC_TIMESTAMP));
  }

  @Test
  void anUnrepresentableOrTruncatedCellNamesNoRow() {
    for (final byte kind : VALUE_KINDS) {
      assertEquals(ROW_WITNESS_UNKNOWN, rowWitnessRule(kind, COLUMN_FLAG_UNREPRESENTABLE), "kind " + kind);
      assertEquals(ROW_WITNESS_UNKNOWN, rowWitnessRule(kind, COLUMN_FLAG_NON_INTEGRAL), "kind " + kind);
    }
    for (final byte purity : new byte[] {NONE, COLUMN_FLAG_PURE_DOUBLE_SOURCE}) {
      assertEquals(ROW_WITNESS_UNKNOWN,
          rowWitnessRule(COLUMN_KIND_NUMERIC_DOUBLE, (byte) (COLUMN_FLAG_UNREPRESENTABLE | purity)));
      assertEquals(ROW_WITNESS_UNKNOWN,
          rowWitnessRule(COLUMN_KIND_NUMERIC_DOUBLE, (byte) (COLUMN_FLAG_NON_INTEGRAL | purity)));
      assertEquals(ROW_WITNESS_UNKNOWN,
          rowWitnessRule(COLUMN_KIND_TIMESTAMP, (byte) (COLUMN_FLAG_NON_INTEGRAL | purity)));
    }
  }

  @Test
  void aDoubleColumnThatOnlyLostPurityCarriesItsCellsByValue() {
    assertEquals(ROW_WITNESS_SOME_IMPURE, rowWitnessRule(COLUMN_KIND_NUMERIC_DOUBLE, NONE));
  }

  @Test
  void aTimestampColumnThatRaisedExactlyOneFlagRaisedItForEveryCell() {
    assertEquals(ROW_WITNESS_LEXICAL_UTC,
        rowWitnessRule(COLUMN_KIND_TIMESTAMP, (byte) (COLUMN_FLAG_UNREPRESENTABLE | COLUMN_FLAG_PURE_UTC_TIMESTAMP)));
    assertEquals(ROW_WITNESS_NO_UTC, rowWitnessRule(COLUMN_KIND_TIMESTAMP, NONE));
    // Both raised: suffixed and unsuffixed values, or a poisoned cell, share the row group.
    assertEquals(ROW_WITNESS_UNKNOWN, rowWitnessRule(COLUMN_KIND_TIMESTAMP, COLUMN_FLAG_UNREPRESENTABLE));
  }

  @Test
  void segmentScopedIdsAreNeverCarriedOver() {
    assertEquals(ROW_WITNESS_UNKNOWN, rowWitnessRule(COLUMN_KIND_STRING_SEGMENT, NONE));
    assertEquals(ROW_WITNESS_UNKNOWN, rowWitnessRule(COLUMN_KIND_STRING_SEGMENT, COLUMN_FLAG_UNREPRESENTABLE));
  }
}
