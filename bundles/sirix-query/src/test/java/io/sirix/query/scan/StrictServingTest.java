package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The fail-soft contract of the serving arms, both ways.
 *
 * <p>
 * An exception inside a serving arm (not a gate decline) is counted and answered by the generic
 * pipeline — the production contract, exercised through a deterministic fault at the
 * group-aggregate arm's entry. Under {@link SirixVectorizedExecutor#STRICT_SERVING}, which a
 * serving-proof run switches on, the same fault must SURFACE: at 100M rows a silent fallback is an
 * hour of interpreter time that looks like a hang, and the proof flag exists to make exactly that
 * visible.
 * </p>
 */
final class StrictServingTest {
  private static final String DB = "strict-serving-db";
  private static final String RES = "records.jn";
  private static final int N = 3_000;
  private static final String QUERY = "for $h in jn:doc('" + DB + "','" + RES + "')[] let $k := $h.k7 group by $k "
      + "order by $k return {\"k7\": $k, \"c\": count($h)}";

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    resetSeams();
    dbDir = Files.createTempDirectory("sirix-strict-serving-");
    final StringBuilder sb = new StringBuilder(N * 32);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i).append(",\"k7\":").append(i % 7).append(",\"amount\":").append(i).append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/id', '/[]/k7', '/[]/amount'),
            ('long', 'long', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    resetSeams();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  private static void resetSeams() {
    SirixVectorizedExecutor.GROUP_AGG_TEST_FAULT = null;
    SirixVectorizedExecutor.STRICT_SERVING = false;
  }

  @Test
  @DisplayName("a failing arm falls back to the generic pipeline by default, counted")
  void failSoftFallsBackAndCounts() throws Exception {
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final String served = run(QUERY);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore,
        "the group-aggregate arm did not serve the control query, so the fault below would be unreachable");
    final long failedBefore = SirixVectorizedExecutor.groupAggFailedCount();
    final long servedAfterControl = SirixVectorizedExecutor.groupAggServedCount();
    SirixVectorizedExecutor.GROUP_AGG_TEST_FAULT = new IllegalStateException("injected arm failure");
    final String fallback = run(QUERY);
    assertEquals(served, fallback, "the generic pipeline must answer exactly what the arm answered");
    assertEquals(failedBefore + 1, SirixVectorizedExecutor.groupAggFailedCount(), "the failure is counted");
    assertEquals(servedAfterControl, SirixVectorizedExecutor.groupAggServedCount(), "and not reported as served");
  }

  @Test
  @DisplayName("under strict serving the same failure surfaces instead of falling back")
  void strictServingSurfacesTheFailure() {
    SirixVectorizedExecutor.GROUP_AGG_TEST_FAULT = new IllegalStateException("injected arm failure");
    SirixVectorizedExecutor.STRICT_SERVING = true;
    final long failedBefore = SirixVectorizedExecutor.groupAggFailedCount();
    final RuntimeException raised = assertThrows(RuntimeException.class, () -> run(QUERY));
    assertTrue(mentionsStrictServing(raised), "the raised failure must name strict serving: " + raised);
    assertEquals(failedBefore + 1, SirixVectorizedExecutor.groupAggFailedCount(), "still counted");
  }

  private static boolean mentionsStrictServing(final Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c.getMessage() != null && c.getMessage().contains("strict serving")) {
        return true;
      }
    }
    return false;
  }

  private String run(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
        final JsonResourceSession session = db.beginResourceSession(RES);
        exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        final Sequence result = new Query(chain, query).execute(ctx);
        final StringWriter out = new StringWriter();
        try (PrintWriter pw = new PrintWriter(out)) {
          new StringSerializer(pw).serialize(result);
        }
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (exec != null) {
          exec.close();
        }
      }
    }
  }
}
