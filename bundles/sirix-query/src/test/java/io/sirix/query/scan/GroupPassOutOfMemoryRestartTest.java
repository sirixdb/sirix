package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.GroupTableSpill;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A worker that runs out of memory inside a hash-range pass aborts the pass like an over-budget table
 * does: the arm restarts with more passes, each keeping fewer groups, and still serves exactly.
 *
 * <p>
 * q32 at 100M/8 GB: served in 46 s on its first try, then died on the second with
 * {@code Parallel scan failed — OutOfMemoryError: Java heap space} once earlier queries' retained state
 * left too little heap for a pass planned against the maximum heap. Here the spill's first flush throws
 * a synthetic {@link OutOfMemoryError} (a seam — no heap is exhausted); strict serving is on, so an arm
 * that let the failure stand would fail the test instead of falling back to the interpreter.
 * </p>
 */
final class GroupPassOutOfMemoryRestartTest {
  private static final String DB = "group-oom-db";
  private static final String RES = "records.jn";
  private static final int N = 8_000;
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";

  private static final List<String> QUERIES = List.of(
      // numeric arm
      "subsequence(for $h in " + DOC + " let $k := $h.k40 group by $k let $c := count($h) "
          + "order by $c descending return {\"k40\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
      // string arm
      "subsequence(for $h in " + DOC + " let $k := $h.s group by $k let $c := count($h) "
          + "order by $c descending return {\"s\": $k, \"c\": $c}, 1, 12)",
      // composite arm with a grouped COUNT(DISTINCT)
      "subsequence(for $h in " + DOC + " let $a := $h.k7, $b := $h.k40 group by $a, $b "
          + "let $u := count(distinct-values($h.u)) order by $u descending return {\"k7\": $a, \"k40\": $b, \"u\": $u}, 1, 12)",
      // packed ISO-minute substring key
      "subsequence(for $h in " + DOC + " let $m := substring($h.t, 1, 16) group by $m let $c := count($h) "
          + "order by $c descending return {\"m\": $m, \"c\": $c}, 1, 12)");

  private Path dbDir;
  private int previousThreshold;
  private int previousSubChunk;

  @BeforeEach
  void setUp() throws Exception {
    previousThreshold = GroupTableSpill.setFlushGroupsForTesting(8);
    previousSubChunk = GroupTableSpill.setSubChunkLeavesForTesting(1); // several sub-chunks per worker: flushes happen
    SirixVectorizedExecutor.STRICT_SERVING = true;
    dbDir = Files.createTempDirectory("sirix-group-oom-");
    final StringBuilder sb = new StringBuilder(N * 96);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final int minute = (i * 7919) % 240;
      sb.append("{\"id\":").append(i).append(",\"k7\":").append(i % 7).append(",\"k40\":").append(i % 40)
        .append(",\"s\":\"s").append(i % 50).append("\",\"u\":").append(i % 97).append(",\"amount\":").append(i)
        .append(",\"t\":\"2024-01-01T").append(String.format("%02d", minute / 60)).append(':')
        .append(String.format("%02d", minute % 60)).append(":00\"}");
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/id', '/[]/k7', '/[]/k40', '/[]/s', '/[]/u', '/[]/amount', '/[]/t'),
            ('long', 'long', 'long', 'string', 'long', 'long', 'string'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    GroupTableSpill.setSimulateOutOfMemoryOnFlushForTesting(false);
    GroupTableSpill.setFlushGroupsForTesting(previousThreshold);
    GroupTableSpill.setSubChunkLeavesForTesting(previousSubChunk);
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("a worker OutOfMemoryError aborts the pass and the arm restarts with more passes, exactly")
  void outOfMemoryInAPassRestartsWithMorePasses() throws Exception {
    for (final String query : QUERIES) {
      final String generic = run(query, false);
      final long abortsBefore = GroupTableSpill.outOfMemoryAbortsCount();
      final long restartsBefore = SirixVectorizedExecutor.groupPassRestartsCount();
      final long releasesBefore = GroupTableSpill.releaseCount();
      final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
      GroupTableSpill.setSimulateOutOfMemoryOnFlushForTesting(true);
      final String served = run(query, true);
      assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore, "not served by a group arm: " + query);
      assertEquals(abortsBefore + 1, GroupTableSpill.outOfMemoryAbortsCount(),
          "the synthetic OutOfMemoryError must have aborted exactly one pass for: " + query);
      assertTrue(SirixVectorizedExecutor.groupPassRestartsCount() > restartsBefore,
          "the arm must have restarted with more passes for: " + query);
      assertEquals(generic, served, "the restarted arm diverges from the interpreter for: " + query);
      // Each arm releases the aborted pass's tables before it re-plans — the budget refresh must not
      // read the pass it is replacing as live heap.
      assertEquals(SirixVectorizedExecutor.groupPassRestartsCount() - restartsBefore,
          GroupTableSpill.releaseCount() - releasesBefore, "one table release per restart for: " + query);
    }
  }

  private String run(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final JsonResourceSession session = db.beginResourceSession(RES);
          exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
          SequentialPipelineStrategy.setVectorizedExecutor(exec);
        }
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
