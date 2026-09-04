package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.GroupTableSpill;
import io.sirix.index.projection.LongChunkPool;
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
      sb.append("{\"id\":")
        .append(i)
        .append(",\"k7\":")
        .append(i % 7)
        .append(",\"k40\":")
        .append(i % 40)
        .append(",\"s\":\"s")
        .append(i % 50)
        .append("\",\"u\":")
        .append(i % 97)
        .append(",\"amount\":")
        .append(i)
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
    final long releasesBefore = GroupTableSpill.releaseCount();
    final long poolHitsBefore = LongChunkPool.totalHits();
    final long presizedBefore = GroupTableSpill.presizedSharedCount();
    for (final String query : QUERIES) {
      final String generic = run(query, false);
      final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
      final String served = run(query, true);
      assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore, "not served by a group arm: " + query);
      assertEquals(generic, served, "hash-range passes diverge from the interpreter for: " + query);
    }
    assertTrue(GroupTableSpill.presizedSharedCount() > presizedBefore,
        "a restarted pass plans from the abort estimate: its shared tables must be created at that share");
    assertTrue(SirixVectorizedExecutor.groupPassRestartsCount() > restartsBefore,
        "no arm ever restarted with more passes: the budget seam did not take, the agreement above is vacuous");
    // Every restart released the aborted pass's tables BEFORE re-planning — a budget refreshed by a
    // forced collection must not read the pass it is replacing as live heap.
    assertEquals(SirixVectorizedExecutor.groupPassRestartsCount() - restartsBefore,
        GroupTableSpill.releaseCount() - releasesBefore, "one table release per restart, in every arm");
    assertTrue(LongChunkPool.totalHits() > poolHitsBefore,
        "a later pass never took a chunk an earlier pass released: the pool does not span the passes");
  }

  @Test
  @DisplayName("a per-pass budget raised the way more headroom raises it costs no pass restart")
  void aRaisedPerPassBudgetCostsNoRestart() throws Exception {
    // The budget -> pass-count half of the chain R1 shortens. The other half (more headroom raises
    // the shared figure, which raises this budget) is HeapHeadroomBudgetTest's; end to end the
    // effect needs a group state above the 2^20-group floor, which is the 100M leg, not a fixture.
    final String query = QUERIES.get(0);
    final String generic = run(query, false);

    GroupTableSpill.setGroupBudgetForTesting(32L);
    final long starvedBefore = SirixVectorizedExecutor.groupPassRestartsCount();
    final String starved = run(query, true);
    assertEquals(generic, starved, "the starved budget's passes must agree with the interpreter");
    assertTrue(SirixVectorizedExecutor.groupPassRestartsCount() > starvedBefore,
        "the starved budget must force at least one restart, or the comparison below is vacuous");

    // Exactly what an empty heap plans through the shared headroom share.
    final long maxMemory = Runtime.getRuntime().maxMemory();
    GroupTableSpill.setGroupBudgetForTesting(GroupTableSpill.groupBudgetFor(maxMemory, maxMemory));
    final long roomyBefore = SirixVectorizedExecutor.groupPassRestartsCount();
    final String roomy = run(query, true);
    assertEquals(generic, roomy, "the roomy budget must answer the same");
    assertEquals(roomyBefore, SirixVectorizedExecutor.groupPassRestartsCount(),
        "a budget raised the way released residency raises it must cost no restart at all");
  }

  @Test
  @DisplayName("the exact group count of a completed scan seeds the next execution of its shape: no restart")
  void anExactCountSeedsTheNextExecution() throws Exception {
    // Every query of the suite, so all three arms with a memo (composite, numeric, string) prove it:
    // the first execution starts at one pass, aborts past the starved budget and restarts; the second
    // execution of the SAME shape in the same session must start with the passes the exact count
    // calls for and never abort. The abort-time estimate alone cannot promise this — it extrapolates
    // the distinct arrival rate and overshoots, and the budget it seeds against is noisy.
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var db = Databases.openJsonDatabase(dbDir.resolve(DB));
        var session = db.beginResourceSession(RES)) {
      for (final String query : QUERIES) {
        final String generic = run(query, false);
        final long firstBefore = SirixVectorizedExecutor.groupPassRestartsCount();
        final String first = runWith(session, chain, ctx, query);
        assertEquals(generic, first, "first execution diverges for: " + query);
        assertTrue(SirixVectorizedExecutor.groupPassRestartsCount() > firstBefore,
            "the first execution must abort its single pass, or there is no completed pass set to count: " + query);
        final long secondBefore = SirixVectorizedExecutor.groupPassRestartsCount();
        final long presizedBefore = GroupTableSpill.presizedSharedCount();
        final String second = runWith(session, chain, ctx, query);
        assertEquals(generic, second, "seeded execution diverges for: " + query);
        assertEquals(secondBefore, SirixVectorizedExecutor.groupPassRestartsCount(),
            "the second execution must be seeded from the exact count and never restart: " + query);
        assertTrue(GroupTableSpill.presizedSharedCount() > presizedBefore,
            "the seeded execution knows the count: its shared tables must be created at their share: " + query);
      }
    }
  }

  @Test
  @DisplayName("a completed pass count caps what its exact count implies — a pass is judged by what it flushed")
  void aCompletedPassCountCapsWhatTheCountImplies() {
    // q16 at 100M: 28M groups completed in TWO passes at a budget of 12,582,912 (each pass held 14M,
    // the workers' final local tables never flush, so the abort never saw them). The count alone
    // implies three balanced passes at the tolerant budget (the largest of three shares holds eleven
    // partitions, 9.6M groups); the completed two win, at a pass budget that holds the fourteen
    // million plus the skew margin.
    final long budget = 12_582_912L;
    final long groups = 28_000_000L;
    assertEquals(2, SirixVectorizedExecutor.GroupPasses.seededPasses(groups, 2, budget, 32));
    final long perPass = SirixVectorizedExecutor.GroupPasses.perPassBudget(groups, 2, 32);
    assertTrue(perPass > 14_000_000L && perPass < 14_000_000L * 107L / 100L, "pass budget: " + perPass);
    // The same count completed in four passes (an overshooting estimate seeded them): the count says
    // three, and three it is — the completed count only ever caps.
    assertEquals(3, SirixVectorizedExecutor.GroupPasses.seededPasses(groups, 4, budget, 32));
    // A count that fits ONE pass at the budget seeds one pass however many passes completed: the
    // completed count only ever caps, it never inflates.
    assertEquals(1, SirixVectorizedExecutor.GroupPasses.seededPasses(6_000_000L, 4, budget, 32));
    // The budget has since collapsed to five million: replaying two passes of fourteen million groups
    // each would plan almost three times the budget, so the count plans against the budget it has —
    // six balanced passes of at most six partitions (5.25M groups) at the tolerant 5.5M.
    assertEquals(6, SirixVectorizedExecutor.GroupPasses.seededPasses(groups, 2, 5_000_000L, 32));
    // Exactly at the replay limit the completed count still wins.
    assertEquals(2, SirixVectorizedExecutor.GroupPasses.seededPasses(groups, 2, 7_000_000L, 32));
    // A fixture: 280 groups at a budget of 32 completed in sixteen passes; the count says eleven
    // (three partitions per pass, 27 expected groups), and eleven it is.
    assertEquals(11, SirixVectorizedExecutor.GroupPasses.seededPasses(280L, 16, 32L, 32));
  }

  private static String runWith(final JsonResourceSession session, final SirixCompileChain chain,
      final SirixQueryContext ctx, final String query) throws Exception {
    final SirixVectorizedExecutor exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
    try {
      SequentialPipelineStrategy.setVectorizedExecutor(exec);
      final Sequence result = new Query(chain, query).execute(ctx);
      final StringWriter out = new StringWriter();
      try (PrintWriter pw = new PrintWriter(out)) {
        new StringSerializer(pw).serialize(result);
      }
      return out.toString();
    } finally {
      SequentialPipelineStrategy.setVectorizedExecutor(null);
      exec.close();
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
