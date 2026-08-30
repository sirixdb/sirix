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
 * Hash-range passes of the composite group arm against the interpreter, with the per-pass group
 * budget pinned so low that the first pass aborts and the arm restarts with several passes.
 *
 * <p>
 * A pass keeps only the groups of its partitions; a group that belongs to another pass folds into
 * the table's discard block and the accumulator's discard sink. Exactness therefore rests on every
 * partition completing within exactly one pass, the top-k selectors surviving across passes, the
 * missing-key rows and the zero key being owned by the pass holding partition 0, and the grouped
 * COUNT(DISTINCT) state being reset between passes. Strict serving is on, so an arm that failed and
 * fell back would fail the test instead of agreeing through the interpreter.
 * </p>
 */
final class GroupHashRangePassTest {
  private static final String DB = "group-pass-db";
  private static final String RES = "records.jn";
  private static final int N = 8_000;
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";

  private static final List<String> QUERIES = List.of(
      // composite key (k7 × k40 = 280 groups), top-k by count
      "subsequence(for $h in " + DOC + " let $a := $h.k7, $b := $h.k40 group by $a, $b let $c := count($h) "
          + "order by $c descending return {\"k7\": $a, \"k40\": $b, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
      // composite key with a grouped COUNT(DISTINCT), top-k by it
      "subsequence(for $h in " + DOC + " let $a := $h.k7, $b := $h.k40 group by $a, $b "
          + "let $u := count(distinct-values($h.u)) order by $u descending return {\"k7\": $a, \"k40\": $b, \"u\": $u}, 1, 12)",
      // numeric key, top-k by count (numeric arm)
      "subsequence(for $h in " + DOC + " let $k := $h.k40 group by $k let $c := count($h) "
          + "order by $c descending return {\"k40\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
      // numeric key with a grouped COUNT(DISTINCT) and rows missing the key (numeric arm)
      "subsequence(for $h in " + DOC + " let $k := $h.k40 group by $k let $u := count(distinct-values($h.u)) "
          + "order by $u descending return {\"k40\": $k, \"u\": $u}, 1, 12)",
      // string key, top-k (string arm)
      "subsequence(for $h in " + DOC + " let $k := $h.s group by $k let $c := count($h) "
          + "order by $c descending return {\"s\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
      // string key with a grouped COUNT(DISTINCT) (string arm)
      "subsequence(for $h in " + DOC + " let $k := $h.s group by $k let $u := count(distinct-values($h.u)) "
          + "order by $u descending return {\"s\": $k, \"u\": $u}, 1, 12)",
      // composite key with a string component and a predicate
      "subsequence(for $h in " + DOC + " where $h.amount ge 2000 let $a := $h.k7, $s := $h.s group by $a, $s "
          + "let $c := count($h) order by $c descending return {\"k7\": $a, \"s\": $s, \"c\": $c}, 1, 12)");

  private Path dbDir;
  private long previousBudget;
  private int previousThreshold;
  private int previousSubChunk;

  @BeforeEach
  void setUp() throws Exception {
    previousBudget = GroupTableSpill.setGroupBudgetForTesting(32L);
    previousThreshold = GroupTableSpill.setFlushGroupsForTesting(8);
    previousSubChunk = GroupTableSpill.setSubChunkLeavesForTesting(1);
    SirixVectorizedExecutor.STRICT_SERVING = true;
    dbDir = Files.createTempDirectory("sirix-group-pass-");
    final StringBuilder sb = new StringBuilder(N * 64);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i).append(",\"k7\":").append(i % 7).append(",\"k40\":").append(i % 40)
        .append(",\"s\":\"s").append(i % 50).append("\",\"u\":").append(i % 97).append(",\"amount\":").append(i)
        .append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/id', '/[]/k7', '/[]/k40', '/[]/s', '/[]/u', '/[]/amount'),
            ('long', 'long', 'long', 'string', 'long', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    GroupTableSpill.setGroupBudgetForTesting(previousBudget);
    GroupTableSpill.setFlushGroupsForTesting(previousThreshold);
    GroupTableSpill.setSubChunkLeavesForTesting(previousSubChunk);
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("a group state over the per-pass budget is served exactly in several hash-range passes")
  void passesAgreeWithTheInterpreter() throws Exception {
    final long restartsBefore = SirixVectorizedExecutor.groupPassRestartsCount();
    for (final String query : QUERIES) {
      final String generic = run(query, false);
      final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
      final String served = run(query, true);
      assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore, "not served by a group arm: " + query);
      assertEquals(generic, served, "hash-range passes diverge from the interpreter for: " + query);
    }
    assertTrue(SirixVectorizedExecutor.groupPassRestartsCount() > restartsBefore,
        "no arm ever restarted with more passes: the budget seam did not take, the agreement above is vacuous");
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
