package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential gate for ORDERED, subsequence-capped group-by serving (the in-kernel top-K route):
 * the served result must equal the interpreter's — INCLUDING emission order, which the
 * sort-normalizing sibling test deliberately ignores. Every vectorized run also asserts the
 * group-aggregate served counter moved, so a silent decline (both arms interpreted, agreement
 * vacuous) fails the test rather than proving nothing.
 *
 * <p>
 * The corpus is engineered for the selection's failure modes: a giant count-tie plateau exactly at
 * the K boundary (stable-order tiebreak), groups whose aggregate key is EMPTY (sparse field) under
 * ascending/descending × empty least/greatest, a missing-group-key population large enough to WIN
 * the window, group value {@code 0} (the flat table's sentinel side slot), sums adjacent to 2^53
 * where a double-rounded {@code avg} comparison would collapse an order the interpreter's exact
 * decimal division defines, and enough distinct keys to force table growth and the partitioned
 * merge.
 */
public final class GroupTopKDifferentialTest {

  private static final int N = 2_003;
  private static final String DB = "topk-gb-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";

  private static final String[] DEPTS = {"Eng", "Sales", "Mkt", "Ops"};
  private static final String[] TIERS = {"gold", "silver", "bronze"};

  private java.nio.file.Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-topk-gb-");
    final Random rng = new Random(41);
    final StringBuilder sb = new StringBuilder(N * 96);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i); // unique numeric key, includes 0 (zero side slot)
      sb.append(",\"k7\":").append(i % 7); // few groups, near-equal counts (tie plateaus)
      sb.append(",\"k40\":").append(i % 40);
      sb.append(",\"amount\":").append(rng.nextInt(1000));
      sb.append(",\"dept\":\"").append(DEPTS[rng.nextInt(DEPTS.length)]).append('"');
      sb.append(",\"name\":\"n").append(i % 401).append('"'); // 401 distinct strings
      if (i % 3 != 0) {
        // ~2/3 present: the MISSING-tier population (~1/3) outcounts every tier group,
        // so under count-descending the null-key group must WIN the window.
        sb.append(",\"tier\":\"").append(TIERS[rng.nextInt(TIERS.length)]).append('"');
      }
      if (i % 40 < 3) {
        // bonus exists ONLY for k40 groups 0..2 — every other k40 group has an EMPTY
        // sum/avg/min/max key, exercising empty placement in the heap comparator.
        sb.append(",\"bonus\":").append(rng.nextInt(500));
      }
      if (i >= 200 && i < 300) {
        // Group bk=2: avg = 4.5e14 + 0.02. At this magnitude a double's ulp is ~0.06, so
        // the two groups' avgs COLLAPSE after rounding while the exact decimal division
        // orders them strictly — and bk=2 (the LARGER avg) appears FIRST in document
        // order, so an ordinal fallback (what a rounded tie would trigger under
        // ascending) inverts the exact order. Magnitudes stay far below the group-sum
        // pre-flight bound, so the query SERVES instead of declining.
        sb.append(",\"bk\":2,\"big\":").append(i == 200
            ? 450000000000002L
            : 450000000000000L);
      } else if (i >= 500 && i < 600) {
        // Group bk=1: avg = 4.5e14 + 0.01 — exactly BELOW bk=2's, double-equal to it.
        sb.append(",\"bk\":1,\"big\":").append(i == 500
            ? 450000000000001L
            : 450000000000000L);
      }
      sb.append('}');
    }
    sb.append(']');

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
  }

  // ---- numeric single key -------------------------------------------------------------------

  @Test
  void countDescTiePlateauAtTheBoundary() throws Exception {
    // Every id group has count 1 — the ENTIRE result is one tie plateau, so the first K of
    // the stable order is pure first-appearance. Any instability in the heap shows here.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.id group by $k "
        + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 1, 12)");
  }

  @Test
  void countDescFewGroups() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k7 group by $k "
        + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 1, 3)");
  }

  @Test
  void countAscending() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k7 group by $k "
        + "let $c := count($u) order by $c ascending return {\"k\": $k, \"c\": $c}, 1, 4)");
  }

  @Test
  void limitBeyondGroupCountEmitsAllOrdered() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k7 group by $k "
        + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 1, 100000)");
  }

  @Test
  void limitExactlyGroupCount() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k7 group by $k "
        + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 1, 7)");
  }

  @Test
  void subsequenceWithOffsetWindow() throws Exception {
    // start > 1: the annotated cap is start+length-1 = 9; the served prefix feeds the same
    // downstream subsequence, so the window [4, 9] must match exactly.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 4, 6)");
  }

  @Test
  void orderBySumDescending() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $s := sum($u.amount) order by $s descending return {\"k\": $k, \"s\": $s}, 1, 5)");
  }

  @Test
  void orderByMinAndMaxAscending() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $m := min($u.amount) order by $m ascending return {\"k\": $k, \"m\": $m}, 1, 6)");
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $m := max($u.amount) order by $m ascending return {\"k\": $k, \"m\": $m}, 1, 6)");
  }

  @Test
  void twoSpecOrdering() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.id group by $k "
        + "let $c := count($u), $s := sum($u.amount) order by $c descending, $s ascending "
        + "return {\"k\": $k, \"c\": $c, \"s\": $s}, 1, 9)");
  }

  @Test
  void orderByTheNumericKeyItself() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.id group by $k "
        + "let $c := count($u) order by $k descending return {\"k\": $k, \"c\": $c}, 1, 8)");
  }

  // ---- empty aggregate keys (sparse bonus) ---------------------------------------------------

  @Test
  void emptyAggregateKeysDefaultEmptyLeast() throws Exception {
    // 37 of 40 k40 groups have NO bonus at all: their avg key is the empty sequence. Default
    // ordering is empty-least, so ascending must front-load them (ordinal-stable among
    // themselves), descending must push them to the back UNMOVED by the direction flip.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $a := avg($u.bonus) order by $a ascending return {\"k\": $k, \"a\": $a}, 1, 10)");
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $a := avg($u.bonus) order by $a descending return {\"k\": $k, \"a\": $a}, 1, 10)");
  }

  @Test
  void emptyAggregateKeysExplicitEmptyGreatest() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $a := avg($u.bonus) order by $a ascending empty greatest return {\"k\": $k, \"a\": $a}, 1, 10)");
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $s := sum($u.bonus) order by $s descending empty greatest return {\"k\": $k, \"s\": $s}, 1, 10)");
  }

  @Test
  void avgOrderingIsExactWhereDoublesRound() throws Exception {
    // bk groups: avg 9007199254740993 vs 9007199254740992 — equal as doubles, strictly
    // ordered exactly; the larger avg is FIRST in document order so a rounded tie inverts
    // the ascending answer.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.bk group by $k "
        + "let $a := avg($u.big) order by $a ascending return {\"k\": $k, \"a\": $a}, 1, 3)");
  }

  // ---- missing group key --------------------------------------------------------------------

  @Test
  void missingGroupKeyWinsTheWindow() throws Exception {
    // ~1/3 of records lack tier — the null-key group outcounts every real tier, so it must
    // occupy position 1 under count-descending even at K=1.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $t := $u.tier group by $t "
        + "let $c := count($u) order by $c descending return {\"t\": $t, \"c\": $c}, 1, 1)");
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $t := $u.tier group by $t "
        + "let $c := count($u) order by $c descending return {\"t\": $t, \"c\": $c}, 1, 4)");
  }

  @Test
  void missingGroupKeyOutsideTheWindow() throws Exception {
    // Ascending: the null-key group (largest count) must fall OFF a K=2 window.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $t := $u.tier group by $t "
        + "let $c := count($u) order by $c ascending return {\"t\": $t, \"c\": $c}, 1, 2)");
  }

  // ---- string keys (flat hash route, winners materialized from dict bytes) -------------------

  @Test
  void stringKeyCountDesc() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $d := $u.dept group by $d "
        + "let $c := count($u) order by $c descending return {\"d\": $d, \"c\": $c}, 1, 2)");
  }

  @Test
  void highCardinalityStringKeyAcrossLeaves() throws Exception {
    // 401 distinct names spread over every leaf: winners' strings materialize from a leaf
    // OTHER than the last scanned, and the count plateau makes order = first appearance.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $d := $u.name group by $d "
        + "let $c := count($u) order by $c descending return {\"d\": $d, \"c\": $c}, 1, 11)");
  }

  @Test
  void stringKeyAggregateOrderings() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $d := $u.dept group by $d "
        + "let $s := sum($u.amount) order by $s ascending return {\"d\": $d, \"s\": $s}, 1, 3)");
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $d := $u.name group by $d "
        + "let $a := avg($u.bonus) order by $a descending return {\"d\": $d, \"a\": $a}, 1, 9)");
  }

  // ---- grouped COUNT(DISTINCT) ----------------------------------------------------------------

  @Test
  void countDistinctOrderedByItself() throws Exception {
    final long before = SirixVectorizedExecutor.groupDistinctServedCount();
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $d := count(distinct-values($u.amount)) order by $d descending return {\"k\": $k, \"d\": $d}, 1, 8)");
    assertTrue(SirixVectorizedExecutor.groupDistinctServedCount() > before,
        "the distinct route did not serve — agreement would be vacuous");
  }

  @Test
  void countDistinctAlongsideValueAggregates() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $c := count($u), $s := sum($u.amount), $d := count(distinct-values($u.id)) "
        + "order by $c descending return {\"k\": $k, \"c\": $c, \"s\": $s, \"d\": $d}, 1, 6)");
  }

  @Test
  void countDistinctOverSparseOperandAnswersZeroNotNull() throws Exception {
    // 37 of 40 k40 groups have NO bonus: fn:count of the empty distinct-values sequence is 0 —
    // an Int64 zero, never the JSON null avg/min/max emit for the same emptiness.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $c := count($u), $d := count(distinct-values($u.bonus)) order by $c descending "
        + "return {\"k\": $k, \"c\": $c, \"d\": $d}, 1, 40)");
  }

  @Test
  void countDistinctUnderStringGroupKey() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $t := $u.tier group by $t "
        + "let $d := count(distinct-values($u.id)) order by $d descending return {\"t\": $t, \"d\": $d}, 1, 4)");
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $n := $u.name group by $n "
        + "let $d := count(distinct-values($u.k7)) order by $d descending return {\"n\": $n, \"d\": $d}, 1, 9)");
  }

  @Test
  void doubleCastAvgServes() throws Exception {
    // The SQL-AVG idiom: xs:double(avg(...)) must serve, cast digit-for-digit like the
    // interpreter's constructor function.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $c := count($u) order by $c descending "
        + "return {\"k\": $k, \"a\": xs:double(avg($u.amount)), \"c\": $c}, 1, 7)");
  }

  // ---- unlimited ordered group-by must still match (expression-level sort path) --------------

  @Test
  void orderedWithoutSubsequenceStillMatchesExactly() throws Exception {
    assertOrderedDifferential("for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}", true);
  }

  // ---- harness --------------------------------------------------------------------------------

  private void assertOrderedDifferentialServed(final String query) throws Exception {
    assertOrderedDifferential(query, true);
  }

  private void assertOrderedDifferential(final String query, final boolean requireServed) throws Exception {
    final String interpreted = run(query, false);
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final String vectorized = run(query, true);
    if (requireServed) {
      assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore,
          "query was NOT served by the group-aggregate route (agreement would be vacuous): " + query);
    }
    assertEquals(interpreted, vectorized, "served ordered result differs from interpreted for: " + query);
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
          installProjection(session);
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

  private static void installProjection(final JsonResourceSession session) {
    final Path<QNm> rootPath = Path.parse("/[]", PathParser.Type.JSON);
    final String[] fields = {"id", "k7", "k40", "amount", "dept", "name", "tier", "bonus", "bk", "big"};
    final Type[] types =
        {Type.LON, Type.LON, Type.LON, Type.LON, Type.STR, Type.STR, Type.STR, Type.LON, Type.LON, Type.LON};
    final List<Path<QNm>> fieldPaths = new ArrayList<>(fields.length);
    final List<Type> typeList = new ArrayList<>(fields.length);
    for (int i = 0; i < fields.length; i++) {
      fieldPaths.add(Path.parse("/[]/" + fields[i], PathParser.Type.JSON));
      typeList.add(types[i]);
    }
    final var def = IndexDefs.createProjectionIdxDef(rootPath, fieldPaths, typeList, 7, IndexDef.DbType.JSON);
    final List<byte[]> leaves = new ArrayList<>();
    final ProjectionIndexBuilder builder;
    final int revision = session.getMostRecentRevisionNumber();
    try (var rtx = session.beginNodeReadOnlyTrx(revision); var pathSummary = session.openPathSummary(revision)) {
      builder = new ProjectionIndexBuilder(def, pathSummary, leaves::add);
      builder.build(rtx);
    }
    ProjectionIndexRegistry.installWildcard(session.getResourceConfig().getResource().toString(), fields, leaves,
        builder.numericColumnNonIntegralFlags());
  }
}
