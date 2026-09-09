package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PartialGroupTopKTest {
  @TempDir
  Path directory;

  @Test
  void highCardinalityPartialGroupsPreserveOrderedAnswersAndDistinctStaysExact() throws Exception {
    final StringBuilder rows = new StringBuilder(1_000_000).append('[');
    for (int row = 0; row < 24_000; row++) {
      if (row != 0) {
        rows.append(',');
      }
      final int group = row % 11_000;
      rows.append("{\"id\":").append(group).append(",\"bucket\":").append(group % 101);
      if (group % 7 != 0) {
        rows.append(",\"value\":").append(row % 31 - 15);
      }
      rows.append('}');
    }
    rows.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('partial','records','" + rows + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('partial','records')
          let $index := jn:create-projection-index($doc, '/[]',
            ('/[]/id', '/[]/bucket', '/[]/value'), ('long', 'long', 'long'))
          return sdb:commit($doc)
          """).evaluate(ctx);
    }
    ProjectionIndexCatalog.clearCache();
    final String prefix = "subsequence(for $r in jn:doc('partial','records')[] "
        + "let $id := $r.id, $bucket := $r.bucket group by $id, $bucket ";
    try {
      // Sparse SUM ordering is also covered by the enabled SparseSumOrderingOriginTest.
      for (final String suffix : new String[] {
          "let $n := count($r) order by $n descending return {\"id\":$id,\"bucket\":$bucket,\"n\":$n},1,13)",
          "let $n := count($r) order by $n descending return {\"id\":$id,\"bucket\":$bucket,\"n\":$n,\"s\":sum($r.value),\"a\":avg($r.value)},1,13)",
          "let $a := avg($r.value) order by $a ascending empty greatest return {\"id\":$id,\"bucket\":$bucket,\"a\":$a},1,19)"}) {
        final String query = prefix + suffix;
        final String expected = run(query, false);
        final long serves = SirixVectorizedExecutor.groupAggServedCount();
        assertEquals(expected, run(query, true), query);
        assertTrue(SirixVectorizedExecutor.groupAggServedCount() > serves,
            "must serve the vectorized group route: " + query);
      }
      final String distinct = prefix + "let $n := count(distinct-values($r.value)) "
          + "order by $n descending return {\"id\":$id,\"bucket\":$bucket,\"n\":$n},1,13)";
      final String expected = run(distinct, false);
      final long serves = SirixVectorizedExecutor.groupAggServedCount();
      assertEquals(expected, run(distinct, true));
      assertTrue(SirixVectorizedExecutor.groupAggServedCount() > serves,
          "distinct aggregation must serve with exact worker tables");
    } finally {
      ProjectionIndexCatalog.clearCache();
    }
  }

  private String run(final String text, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor executor = null;
      try {
        if (vectorized) {
          final var database = Databases.openJsonDatabase(directory.resolve("partial"));
          final JsonResourceSession session = database.beginResourceSession("records");
          executor = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 1);
          SequentialPipelineStrategy.setVectorizedExecutor(executor);
        }
        final Sequence result = new Query(chain, text).execute(ctx);
        final StringWriter output = new StringWriter();
        try (PrintWriter writer = new PrintWriter(output)) {
          new StringSerializer(writer).serialize(result);
        }
        return output.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (executor != null) {
          executor.close();
        }
      }
    }
  }
}
