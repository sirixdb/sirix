package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A predicate, a group key and an aggregate may share slices without sharing the query's mask. */
final class CompositePredicateSliceReuseTest {
  private static final String DATABASE = "predicate-reuse";
  private static final String RESOURCE = "events.jn";
  private static final String GROUP = """
      for $e in jn:doc('predicate-reuse','events.jn')[]
      %s
      let $cat := $e.cat, $kind := $e.kind + 1
      group by $cat, $kind
      let $count := count($e), $first := min($e.time_us), $last := max($e.time_us)
      order by $first
      return {"cat":$cat,"kind":$kind,"count":$count,"first":$first,"last":$last}
      """;

  @TempDir
  Path directory;

  @AfterEach
  void clearState() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
  }

  @ParameterizedTest
  @ValueSource(strings = {"where $e.cat = 'keep' and $e.time_us ge 1024",
      "where ($e.cat = 'keep' or $e.cat = 'also') and $e.time_us ge 1024",
      "where ($e.cat = 'missing' or $e.cat = 'also') and $e.time_us ge 2048"})
  void filteredOperandsAreSharedWithoutPoisoningTheNextQuery(final String where) throws IOException {
    clearState();
    final StringBuilder data = new StringBuilder(200_000).append('[');
    final String[] categories = {"drop", "keep", "also"};
    for (int row = 0; row < 3072; row++) {
      if (row != 0) {
        data.append(',');
      }
      data.append("{\"cat\":\"")
          .append(categories[row / 1024])
          .append("\",\"kind\":")
          .append(row % 2)
          .append(",\"time_us\":")
          .append(row)
          .append('}');
    }
    data.append(']');
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        SirixCompileChain vectorized = SirixCompileChain.createWithJsonStore(store)) {
      new Query(generic, "jn:store('predicate-reuse','events.jn','" + data + "')").evaluate(context);
      new Query(generic, """
          let $doc := jn:doc('predicate-reuse','events.jn')
          let $index := jn:create-projection-index($doc, '/[]',
              ('/[]/cat', '/[]/kind', '/[]/time_us'), ('string', 'long', 'long'))
          return sdb:commit($doc)
          """).evaluate(context);
      ProjectionIndexCatalog.clearCache();
      ProjectionIndexRegistry.clear();
      final String filtered = GROUP.formatted(where);
      final String all = GROUP.formatted("");
      final String expectedFiltered = evaluate(generic, context, filtered);
      final String expectedAll = evaluate(generic, context, all);
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(DATABASE);
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession(RESOURCE)) {
        final SirixVectorizedExecutor executor =
            new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          final long before = SirixVectorizedExecutor.groupAggSlicedServedCount();
          assertEquals(expectedFiltered, evaluate(vectorized, context, filtered));
          assertEquals(before + 1, SirixVectorizedExecutor.groupAggSlicedServedCount());
          final ProjectionIndexRegistry.Handle handle =
              ProjectionIndexCatalog.lookupCovering(session, session.getResourceConfig().getResource().toString(),
                  session.getMostRecentRevisionNumber(), new String[] {"[]"}, new String[] {"cat", "kind", "time_us"});
          assertNotNull(handle);
          final ProjectionColumnStore columns = handle.columnStoreOrNull();
          assertNotNull(columns);
          assertEquals(3, columns.rowGroupCount());
          assertFalse(columns.columnFilled(handle.columnOf("cat")),
              "the filtered group key must not trigger a second, full-column fill");
          assertFalse(columns.columnFilled(handle.columnOf("time_us")),
              "the filtered aggregate operand must reuse the predicate slices");
          assertTrue(columns.columnFilled(handle.columnOf("kind")),
              "a key outside the predicate still needs a full-column fill");
          assertEquals(expectedAll, evaluate(vectorized, context, all),
              "a later unfiltered query must see the leaves the first query pruned");
          assertTrue(columns.columnFilled(handle.columnOf("cat")));
          assertTrue(columns.columnFilled(handle.columnOf("time_us")));
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext context, final String query)
      throws IOException {
    final Sequence result = new Query(chain, query).execute(context);
    final StringWriter output = new StringWriter();
    try (PrintWriter writer = new PrintWriter(output)) {
      new StringSerializer(writer).serialize(result);
    }
    return output.toString();
  }
}
