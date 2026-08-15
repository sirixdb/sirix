package io.sirix.query.bench.clickbench;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The projected column set is DERIVED from the query text, so these pin the derivation rather than
 * a hand-kept list. A query edit that reaches for a new column must widen the projection
 * automatically; the alternative is a silent decline at benchmark time, which reads as "the
 * projection does not help" instead of "the projection is missing a column".
 */
final class ClickBenchProjectionTest {

  @Test
  void everyProjectedColumnIsARealSchemaColumn() {
    final Set<String> schema = new HashSet<>(ClickBenchSchema.COLUMNS);
    for (final String column : ClickBenchProjection.PROJECTED_COLUMNS) {
      Assertions.assertTrue(schema.contains(column), "projected column is not in the schema: " + column);
    }
  }

  @Test
  void projectsEveryColumnTheQueriesDereference() {
    final Set<String> schema = new HashSet<>(ClickBenchSchema.COLUMNS);
    final Set<String> projected = new HashSet<>(ClickBenchProjection.PROJECTED_COLUMNS);
    for (final String referenced : ClickBenchQueries.referencedColumns()) {
      if (schema.contains(referenced)) {
        Assertions.assertTrue(projected.contains(referenced), "query text dereferences '" + referenced
            + "' but the projection does not cover it — " + "every query touching it would decline");
      }
    }
  }

  @Test
  void projectsFewerThanAllColumns() {
    // The point of deriving the set is to pay for the working set only. If this ever equals 105 the
    // derivation has stopped discriminating and the benchmark is paying ingest for dead columns.
    Assertions.assertTrue(ClickBenchProjection.PROJECTED_COLUMNS.size() < ClickBenchSchema.COLUMNS.size(),
        "projection should cover the queried subset, not the whole schema");
    Assertions.assertFalse(ClickBenchProjection.PROJECTED_COLUMNS.isEmpty());
  }

  @Test
  void keepsSchemaOrderSoColumnIndexesAreStable() {
    // Persisted stores address columns positionally, so discovery order (which changes whenever a
    // query is edited) would silently invalidate every store built before the edit.
    final List<String> projected = ClickBenchProjection.PROJECTED_COLUMNS;
    int previous = -1;
    for (final String column : projected) {
      final int at = ClickBenchSchema.COLUMNS.indexOf(column);
      Assertions.assertTrue(at > previous, "projected columns must follow create.sql order, " + column + " does not");
      previous = at;
    }
  }

  @Test
  void everyColumnGetsATypeTheProjectionUnderstands() {
    for (final String column : ClickBenchProjection.PROJECTED_COLUMNS) {
      final String type = ClickBenchProjection.projectionType(column);
      Assertions.assertTrue("long".equals(type) || "string".equals(type),
          column + " mapped to an unsupported projection type: " + type);
    }
  }

  @Test
  void createQueryNamesEveryProjectedColumnUnderTheRootPath() {
    final String query = ClickBenchProjection.createQuery();
    Assertions.assertTrue(query.contains("jn:create-projection-index"), query);
    for (final String column : ClickBenchProjection.PROJECTED_COLUMNS) {
      Assertions.assertTrue(query.contains("'" + ClickBenchProjection.ROOT_PATH + "/" + column + "'"),
          "create query omits " + column);
    }
  }
}
