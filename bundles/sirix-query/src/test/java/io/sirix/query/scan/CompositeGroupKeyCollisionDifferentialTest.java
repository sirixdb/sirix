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
import it.unimi.dsi.fastutil.HashCommon;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end adversarial gate for {@code group by a, b} over the composite serving kernel.
 *
 * <p>
 * The corpus carries two composite key tuples that are ENGINEERED to share the kernel's 64-bit
 * composite probe hash. The construction is closed-form rather than a search: the per-component
 * hash is {@code HashCommon.mix}, a bijection, and the components chain through
 * {@code h = h * FNV_PRIME ^ mix(c)}, so fixing the first component and solving for the second
 * lands on any chosen hash. If group identity were the probe hash, the two tuples' counts would
 * fold into one group and the query would return a silently wrong answer.
 *
 * <p>
 * The test asserts the collision precondition itself, so a change to the kernel's constants makes
 * it fail loudly rather than quietly stop testing anything.
 */
public final class CompositeGroupKeyCollisionDifferentialTest {

  private static final String DB = "composite-collision-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";

  private static final long FNV_SEED = 0xcbf29ce484222325L;
  private static final long FNV_PRIME = 0x100000001b3L;

  private static final long A0 = 1_234_567L;
  private static final long A1 = 987_654_321L;
  private static final long B0 = 42L;
  private static final long B1 = collidingSecondComponent();

  /** Rows carrying tuple A, and rows carrying tuple B — deliberately different counts. */
  private static final int A_ROWS = 5;
  private static final int B_ROWS = 3;

  private Path dbDir;

  private static long compositeKey(final long c0, final long c1) {
    long h = FNV_SEED;
    h = h * FNV_PRIME ^ HashCommon.mix(c0);
    h = h * FNV_PRIME ^ HashCommon.mix(c1);
    return h;
  }

  private static long collidingSecondComponent() {
    final long target = compositeKey(A0, A1);
    final long partial = FNV_SEED * FNV_PRIME ^ HashCommon.mix(B0);
    return HashCommon.invMix(partial * FNV_PRIME ^ target);
  }

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-composite-collision-");
    final StringBuilder sb = new StringBuilder(4096);
    sb.append('[');
    int row = 0;
    for (int i = 0; i < A_ROWS; i++) {
      appendRow(sb, row++, A0, A1, 10 + i);
    }
    for (int i = 0; i < B_ROWS; i++) {
      appendRow(sb, row++, B0, B1, 100 + i);
    }
    // Filler groups so the composite arm has a real table to grow and partition.
    for (int i = 0; i < 400; i++) {
      appendRow(sb, row++, 7_000L + i % 40, 9_000L + i % 31, i);
    }
    sb.append(']');

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/id', '/[]/ca', '/[]/cb', '/[]/amount'),
            ('long', 'long', 'long', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
    ProjectionIndexCatalog.clearCache();
  }

  private static void appendRow(final StringBuilder sb, final int id, final long ca, final long cb, final long amount) {
    if (id > 0) {
      sb.append(',');
    }
    sb.append("{\"id\":")
      .append(id)
      .append(",\"ca\":")
      .append(ca)
      .append(",\"cb\":")
      .append(cb)
      .append(",\"amount\":")
      .append(amount)
      .append('}');
  }

  @AfterEach
  void tearDown() {
    ProjectionIndexCatalog.clearCache();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
  }

  @Test
  @DisplayName("the two corpus tuples really do share the kernel's composite probe hash")
  void collisionPreconditionHolds() {
    assertTrue(A0 != B0 || A1 != B1, "the tuples must be distinct");
    assertEquals(compositeKey(A0, A1), compositeKey(B0, B1),
        "witness is stale: the corpus tuples no longer collide under the kernel's constants");
  }

  @Test
  @DisplayName("colliding composite groups keep their own counts through the served route")
  void collidingCompositeGroupsDoNotMergeTheirCounts() throws Exception {
    assertServedDifferential("subsequence(for $u in " + SRC + " let $a := $u.ca, $b := $u.cb group by $a, $b "
        + "let $c := count($u) order by $c descending " + "return {\"a\": $a, \"b\": $b, \"c\": $c}, 1, 500)");
  }

  @Test
  @DisplayName("colliding composite groups keep their own sums through the served route")
  void collidingCompositeGroupsDoNotMergeTheirSums() throws Exception {
    assertServedDifferential("subsequence(for $u in " + SRC + " let $a := $u.ca, $b := $u.cb group by $a, $b "
        + "let $s := sum($u.amount) order by $s descending " + "return {\"a\": $a, \"b\": $b, \"s\": $s}, 1, 500)");
  }

  @Test
  @DisplayName("the colliding pair survives a narrow top-K window that only the true counts can order")
  void collidingPairSurvivesTopKWindow() throws Exception {
    assertServedDifferential("subsequence(for $u in " + SRC + " let $a := $u.ca, $b := $u.cb group by $a, $b "
        + "let $c := count($u) order by $c descending " + "return {\"a\": $a, \"b\": $b, \"c\": $c}, 1, 3)");
  }

  @Test
  @DisplayName("both colliding groups are present with their own counts, not one fused group")
  void bothGroupsSurviveIndividually() throws Exception {
    // Top-2 by count: the two colliding tuples are the ONLY groups with counts above 1, so a
    // merged pair would surface as one group of 8 instead of 5 then 3.
    final String query = "subsequence(for $u in " + SRC + " let $a := $u.ca, $b := $u.cb group by $a, $b "
        + "let $c := count($u) order by $c descending " + "return {\"a\": $a, \"b\": $b, \"c\": $c}, 1, 2)";
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final String served = run(query, true);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore,
        "query was NOT served by the group-aggregate route: " + query);
    assertTrue(served.contains("\"c\":" + A_ROWS), "group A lost its own count: " + served);
    assertTrue(served.contains("\"c\":" + B_ROWS), "group B lost its own count: " + served);
    assertFalse(served.contains("\"c\":" + (A_ROWS + B_ROWS)), "the colliding groups fused into one: " + served);
    assertEquals(run(query, false), served);
  }

  private void assertServedDifferential(final String query) throws Exception {
    final String interpreted = run(query, false);
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final String vectorized = run(query, true);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore,
        "query was NOT served by the group-aggregate route (agreement would be vacuous): " + query);
    assertEquals(interpreted, vectorized, "served result differs from interpreted for: " + query);
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
