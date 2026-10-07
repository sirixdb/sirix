package io.sirix.query.scan;

import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The long lane cannot reconstruct a Z suffix: every text-sensitive route must stay exact. */
final class UtcTimestampProjectionTest {
  @TempDir
  Path directory;

  @AfterEach
  void resetExecutor() {
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @Test
  void mixedSuffixesStayDistinctAfterBuildReopenAndUpdate() throws Exception {
    SirixVectorizedExecutor.STRICT_SERVING = true;
    final String json = """
        [{"t":"2024-01-01T00:00:00","d":"2024-01-01","u":"a","v":1},
         {"t":"2024-01-01T00:00:00Z","d":"2024-01-01","u":"b","v":2},
         {"t":"2024-01-02T00:00:00Z","d":"2024-01-02","u":"c","v":3}]
        """;
    TemporalColumnDifferentialTest.createFixture(directory, json);
    assertQueries();
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("temporal-col-db");
      try (final var session = collection.getDatabase().beginResourceSession("records.jn");
          final var writer = session.beginNodeTrx()) {
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.setStringValue("2024-01-03T00:00:00Z");
        writer.commit();
      }
    }
    assertQueries();
  }

  private void assertQueries() throws Exception {
    final String source = "jn:doc('temporal-col-db','records.jn')[]";
    for (final String query : List.of("for $h in " + source + " where $h.v ge 0 return $h.t",
        "min(for $h in " + source + " return $h.t)", "max(for $h in " + source + " return $h.t)",
        "subsequence(for $h in " + source + " order by $h.t return $h.t, 1, 3)",
        "for $h in " + source
            + " let $k := $h.t group by $k let $c := count($h) order by $k return {\"k\":$k,\"c\":$c}",
        "count(for $h in " + source + " where $h.t eq '2024-01-01T00:00:00' return $h)",
        "count(for $h in " + source + " where $h.t eq '2024-01-01T00:00:00Z' return $h)",
        "count(for $h in " + source + " where $h.t gt '2024-01-01T00:00:00' return $h)",
        "count(for $h in " + source + " where $h.t lt '2024-01-01T00:00:00Z' return $h)",
        "count(for $h in " + source + " where $h.t ge '2024-01' return $h)", "for $h in " + source
            + " let $k := substring($h.t, 1, 19) group by $k let $c := count($h) order by $k return $c")) {
      assertEquals(TemporalColumnDifferentialTest.run(directory, query, false),
          TemporalColumnDifferentialTest.run(directory, query, true), query);
    }
  }
}
