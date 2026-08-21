package io.sirix.query.bench.clickbench;

import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Reader;
import java.nio.file.Path;
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
  void createQueryPersistsTheDeclaredProjection(@TempDir final Path directory) throws Exception {
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                     .location(directory)
                                                     .buildPathSummary(true)
                                                     .build();
        Reader source = ClickBenchSource.open("generate:4");
        JsonReader jsonReader = new JsonReader(source)) {
      store.create(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, jsonReader);
      try (SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
          SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
        new Query(chain, ClickBenchProjection.createQuery()).evaluate(context);
      }
    }

    try (Database<JsonResourceSession> database =
             Databases.openJsonDatabase(directory.resolve(ClickBenchSchema.DATABASE));
        JsonResourceSession session = database.beginResourceSession(ClickBenchSchema.RESOURCE)) {
      final List<IndexDef> definitions = session.getRtxIndexController(session.getMostRecentRevisionNumber())
                                                   .getIndexes()
                                                   .getIndexDefs()
                                                   .stream()
                                                   .filter(IndexDef::isProjectionIndex)
                                                   .toList();
      Assertions.assertEquals(1, definitions.size());
      final IndexDef expected = ClickBenchProjection.spec().toIndexDef();
      final IndexDef actual = definitions.getFirst();
      Assertions.assertEquals(expected.getProjectionRootPath().toString(),
          actual.getProjectionRootPath().toString());
      Assertions.assertEquals(expected.getProjectionFields().stream().map(Object::toString).toList(),
          actual.getProjectionFields().stream().map(Object::toString).toList());
      Assertions.assertEquals(expected.getProjectionFieldTypes(), actual.getProjectionFieldTypes());
    }
  }
}
