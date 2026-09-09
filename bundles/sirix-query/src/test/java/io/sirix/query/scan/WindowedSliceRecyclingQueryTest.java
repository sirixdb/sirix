package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.GroupTableSpill;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end witness for the windowed access's slice recycling: a store of 200 leaves (past the
 * two-window cache, so eviction — and therefore reuse — actually happens), a composite group-by
 * driven windowed by a one-byte fill budget and into several hash-range passes by a tight group
 * budget, so the same leaves are decoded into recycled arrays pass after pass. The answers must
 * equal the resident arm's and the interpreter's, and the recycling counter must have moved — a
 * pool that is wired to nothing passes every correctness check and this test alone would notice.
 */
final class WindowedSliceRecyclingQueryTest {
  private static final String DB = "slice-recycling-db";
  private static final String RES = "records.jn";
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";
  /** Group g (of GROUPS) holds g + 1 rows, so every count is unique and the top-k order is total. */
  private static final int GROUPS = 640;
  /** 640 × 641 / 2 = 205,120 rows: 200 leaves of 1,024 rows plus a short tail leaf. */
  private static final int N = GROUPS * (GROUPS + 1) / 2;

  private Path dbDir;
  private String previousGlobalDictMode;
  private String previousThreads;
  private long previousBudget = -1L;
  private long previousGroupBudget = -1L;

  @BeforeEach
  void setUp() throws Exception {
    previousGlobalDictMode = System.getProperty("sirix.projection.globalDict");
    System.setProperty("sirix.projection.globalDict", "never");
    // One worker owns every morsel: 201 leaves against a 128-leaf per-column cache is four windows, and
    // only
    // a worker that outgrows its cache ever evicts — twenty workers with ten leaves each never would.
    previousThreads = System.getProperty("sirix.vec.threads");
    System.setProperty("sirix.vec.threads", "1");
    SirixVectorizedExecutor.STRICT_SERVING = true;
    dbDir = Files.createTempDirectory("sirix-slice-recycling-");
    final StringBuilder sb = new StringBuilder(N * 48);
    sb.append('[');
    int i = 0;
    for (int g = 0; g < GROUPS; g++) {
      for (int r = 0; r <= g; r++, i++) {
        if (i > 0) {
          sb.append(',');
        }
        // key (k, a) = (g mod 100, g div 100); `m` is absent on every other row so presence words are mixed
        sb.append("{\"id\":")
          .append(i)
          .append(",\"k\":")
          .append(g % 100)
          .append(",\"a\":")
          .append(g / 100)
          .append(",\"amount\":")
          .append(i % 10_007);
        if ((i & 1) == 0) {
          sb.append(",\"m\":").append(i % 13);
        }
        sb.append('}');
      }
    }
    assertEquals(N, i);
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/id', '/[]/k', '/[]/a', '/[]/amount', '/[]/m'),
            ('long', 'long', 'long', 'long', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    if (previousGlobalDictMode == null) {
      System.clearProperty("sirix.projection.globalDict");
    } else {
      System.setProperty("sirix.projection.globalDict", previousGlobalDictMode);
    }
    if (previousThreads == null) {
      System.clearProperty("sirix.vec.threads");
    } else {
      System.setProperty("sirix.vec.threads", previousThreads);
    }
    if (previousBudget >= 0L) {
      ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
    }
    if (previousGroupBudget >= 0L) {
      GroupTableSpill.setGroupBudgetForTesting(previousGroupBudget);
    }
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  void aMultiPassWindowedCompositeGroupByRecyclesItsSliceArraysAndAnswersExactly() throws Exception {
    final String query = "subsequence(for $h in " + DOC + " where $h.amount ge 100 "
        + "let $k := $h.k, $a := $h.a group by $k, $a let $c := count($h) " + "order by $c descending "
        + "return {\"k\": $k, \"a\": $a, \"c\": $c, \"sum\": sum($h.amount), \"m\": sum($h.m)}, 1, 25)";
    final String generic = run(query, false);
    final String resident = run(query, true);
    assertEquals(generic, resident, "the resident arm diverges from the interpreter");

    previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(1L);
    previousGroupBudget = GroupTableSpill.setGroupBudgetForTesting(200L); // 640 groups → several passes
    final long windowedBefore = SirixVectorizedExecutor.groupWindowedSlicesCount();
    final long recycledBefore = ProjectionColumnStore.recycledSliceArraysCount();
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final String windowed = run(query, true);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore, "not served by the group arm");
    assertTrue(SirixVectorizedExecutor.groupWindowedSlicesCount() > windowedBefore, "the windowed route never engaged");
    final long recycled = ProjectionColumnStore.recycledSliceArraysCount() - recycledBefore;
    // Five columns × 201 leaves per pass in 64-leaf windows against a two-window cache: the third and
    // fourth
    // window of every pass decode into the arrays the first two gave up — 73 leaves × 5 columns × 2
    // arrays
    // per pass, three passes.
    assertTrue(recycled > 1_000L, "slice arrays recycled: " + recycled);
    assertEquals(generic, windowed, "the recycling windowed arm diverges from the interpreter");
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
