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
 * The group-table spill against the interpreter, with the threshold pinned so low that every
 * worker flushes into the shared partition tables many times per query.
 *
 * <p>
 * Spilling changes WHERE a group's state lives mid-scan, not what it is: first-seen ordinals (the
 * interpreter's first-appearance emission order), exact sums, identity lanes of composite keys, the
 * zero-key group and the grouped COUNT(DISTINCT) lane must all come out of the shared tables exactly
 * as they came out of one table per worker. Strict serving is on, so an arm that failed and fell back
 * would fail the test rather than agree by accident through the interpreter.
 * </p>
 */
final class GroupTableSpillDifferentialTest {
  private static final String DB = "group-spill-db";
  private static final String RES = "records.jn";
  private static final int N = 6_000;
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";

  private static final List<String> QUERIES = List.of(
      // numeric key, top-k by count with a deterministic tie-break
      "subsequence(for $h in " + DOC + " let $k := $h.k40 group by $k let $c := count($h) "
          + "order by $c descending, $k ascending return {\"k40\": $k, \"c\": $c}, 1, 10)",
      // numeric key, first-appearance order, exact sums
      "for $h in " + DOC + " let $k := $h.k40 group by $k return {\"k40\": $k, \"sum\": sum($h.amount)}",
      // a zero group key beside the others (with a sum, so the table arm serves it rather than the
      // count-only group-count arm)
      "for $h in " + DOC + " let $k := $h.k7 group by $k return {\"k7\": $k, \"c\": count($h), \"sum\": sum($h.amount)}",
      // string key with grouped COUNT(DISTINCT) and a top-k (an order spec ON a string key declines the
      // plan, so ties resolve by the selector's stable first-appearance order, as the interpreter's do)
      "subsequence(for $h in " + DOC + " let $k := $h.s group by $k let $u := count(distinct-values($h.u)) "
          + "order by $u descending return {\"s\": $k, \"u\": $u}, 1, 10)",
      // string key, first-appearance order, with a sum (the count-only string shape takes no table arm)
      "for $h in " + DOC + " let $k := $h.s group by $k return {\"s\": $k, \"sum\": sum($h.amount)}",
      // composite key with identity lanes, top-k
      "subsequence(for $h in " + DOC + " let $a := $h.k7, $b := $h.k40 group by $a, $b let $c := count($h) "
          + "order by $c descending "
          + "return {\"k7\": $a, \"k40\": $b, \"c\": $c, \"sum\": sum($h.amount)}, 1, 10)",
      // a predicate before the group-by, ordered (the count-only numeric arm emits an UNORDERED group-by
      // in hash order, which the spec leaves implementation-dependent — not a table-arm shape)
      "subsequence(for $h in " + DOC + " where $h.amount ge 3000 let $k := $h.k40 group by $k let $c := count($h) "
          + "order by $c descending, $k ascending return {\"k40\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 10)");

  private Path dbDir;
  private int previousThreshold;
  private int previousSubChunk;

  @BeforeEach
  void setUp() throws Exception {
    previousThreshold = GroupTableSpill.setFlushGroupsForTesting(16);
    // One row group per sub-chunk: a six-leaf corpus then reaches a flush point after every leaf.
    previousSubChunk = GroupTableSpill.setSubChunkLeavesForTesting(1);
    SirixVectorizedExecutor.STRICT_SERVING = true;
    dbDir = Files.createTempDirectory("sirix-group-spill-");
    final StringBuilder sb = new StringBuilder(N * 64);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i).append(",\"k7\":").append(i % 7).append(",\"k40\":").append(i % 40)
        .append(",\"s\":\"s").append(i % 300).append("\",\"u\":").append(i % 97).append(",\"amount\":").append(i)
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
    GroupTableSpill.setFlushGroupsForTesting(previousThreshold);
    GroupTableSpill.setSubChunkLeavesForTesting(previousSubChunk);
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("every group shape agrees with the interpreter while spilling constantly")
  void spillingAgreesWithTheInterpreter() throws Exception {
    final long flushesBefore = GroupTableSpill.flushCount();
    final long poolHitsBefore = LongChunkPool.totalHits();
    for (final String query : QUERIES) {
      final String generic = run(query, false);
      final long servedBefore = groupArmsServed();
      final String served = run(query, true);
      assertTrue(groupArmsServed() > servedBefore,
          "not served by a group arm, so the spill was not exercised: " + query);
      assertEquals(generic, served, "spilled group tables diverge from the interpreter for: " + query);
    }
    assertTrue(GroupTableSpill.flushCount() > flushesBefore,
        "no worker table was ever flushed: the threshold seam did not take, the agreement above is vacuous");
    // The flushed tables' chunks must have been RECYCLED into later tables — the pool engaged, the
    // agreement above ran over recycled (zeroed) storage, and the winners survived the pass-end release.
    assertTrue(LongChunkPool.totalHits() > poolHitsBefore, "no recycled chunk was ever taken: the chunk pool is idle");
  }

  private static long groupArmsServed() {
    return SirixVectorizedExecutor.groupAggServedCount() + SirixVectorizedExecutor.numericGroupByServedCount()
        + SirixVectorizedExecutor.constGroupAggServedCount();
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
