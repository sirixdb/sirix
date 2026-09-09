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
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SparseSumOrderingOriginTest {
  @TempDir
  Path directory;

  @Test
  void sparseSumDescendingMatchesInterpreter() throws Exception {
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
      // Group 0 has no operands; every other group mixes one present and one missing operand.
      new Query(chain,
          "jn:store('partial','mixed','[{\"g\":0},{\"g\":0},"
              + "{\"g\":1,\"value\":10},{\"g\":1},{\"g\":2,\"value\":-5},{\"g\":2},"
              + "{\"g\":3,\"value\":0},{\"g\":3}]',false())").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('partial','records')
          let $index := jn:create-projection-index($doc, '/[]',
            ('/[]/id', '/[]/bucket', '/[]/value'), ('long', 'long', 'long'))
          return sdb:commit($doc)
          """).evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('partial','mixed')
          let $index := jn:create-projection-index($doc, '/[]',
            ('/[]/g', '/[]/value'), ('long', 'long'))
          return sdb:commit($doc)
          """).evaluate(ctx);
    }
    ProjectionIndexCatalog.clearCache();
    final String prefix = "subsequence(for $r in jn:doc('partial','records')[] "
        + "let $id := $r.id, $bucket := $r.bucket group by $id, $bucket ";
    try {
      int ordinal = 0;
      for (final String suffix : new String[] {
          "let $n := count($r) order by $n descending return {\"id\":$id,\"bucket\":$bucket,\"n\":$n},1,13)",
          "let $n := count($r) order by $n descending return {\"id\":$id,\"bucket\":$bucket,\"n\":$n,\"s\":sum($r.value),\"a\":avg($r.value)},1,13)",
          "let $s := sum($r.value) order by $s descending return {\"id\":$id,\"bucket\":$bucket,\"s\":$s,\"lo\":min($r.value),\"hi\":max($r.value)},1,17)",
          "let $a := avg($r.value) order by $a ascending empty greatest return {\"id\":$id,\"bucket\":$bucket,\"a\":$a},1,19)"}) {
        final String query = prefix + suffix;
        assertMatchesInterpreter(query, "records", ordinal++);
      }
      final String mixedPrefix = "subsequence(for $r in jn:doc('partial','mixed')[] let $g := $r.g group by $g ";
      for (final String suffix : new String[] {
          // SUM is always present, so explicit empty placement must not move its zero-valued group.
          "let $s := sum($r.value) order by $s ascending empty greatest return {\"g\":$g,\"s\":$s},1,4)",
          "let $s := sum($r.value) order by $s descending empty least return {\"g\":$g,\"s\":$s},1,4)",
          // MIN, MAX, and AVG remain empty for group 0 and must retain empty-key placement.
          "let $m := min($r.value) order by $m descending empty least return {\"g\":$g,\"m\":$m},1,4)",
          "let $m := max($r.value) order by $m ascending empty greatest return {\"g\":$g,\"m\":$m},1,4)",
          "let $a := avg($r.value) order by $a descending empty greatest return {\"g\":$g,\"a\":$a},1,4)"}) {
        assertMatchesInterpreter(mixedPrefix + suffix, "mixed", ordinal++);
      }
    } finally {
      ProjectionIndexCatalog.clearCache();
    }
  }

  private void assertMatchesInterpreter(final String query, final String resource, final int ordinal) throws Exception {
    final String expected = run(query, resource, false);
    final long serves = SirixVectorizedExecutor.groupAggServedCount();
    final String actual = run(query, resource, true);
    final Path evidence = directory.resolve("reproduction");
    Files.createDirectories(evidence);
    final String name = "query-" + ordinal;
    Files.writeString(evidence.resolve(name + "-query.txt"), query);
    Files.writeString(evidence.resolve(name + "-expected.txt"), expected);
    Files.writeString(evidence.resolve(name + "-actual.txt"), actual);
    assertEquals(expected, actual, query);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > serves,
        "must serve the vectorized group route: " + query);
  }

  private String run(final String text, final String resource, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor executor = null;
      try {
        if (vectorized) {
          final var database = Databases.openJsonDatabase(directory.resolve("partial"));
          final JsonResourceSession session = database.beginResourceSession(resource);
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
