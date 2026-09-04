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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
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
      final String dept = DEPTS[rng.nextInt(DEPTS.length)];
      sb.append(",\"dept\":\"").append(dept).append('"');
      sb.append(",\"name\":\"n").append(i % 401).append('"'); // 401 distinct strings
      if (!"Ops".equals(dept)) {
        // Sparse STRING operand for the deferred-extremum route: absent for the ENTIRE Ops
        // dept (an all-missing group's min/max is the EMPTY sequence), and salted with the
        // collation adversary pair — U+FF01 vs U+10400 order OPPOSITE ways under raw UTF-8
        // bytes vs the interpreter's UTF-16 units, so max(nick) catches a byte-order kernel.
        final String nick = i % 97 == 0
            ? "！"
            : i % 89 == 0
                ? "𐐀"
                : "m" + i % 53;
        sb.append(",\"nick\":\"").append(nick).append('"');
      }
      // ISO timestamps across two days, minute-granular — the packed-substring key's corpus.
      sb.append(",\"ts\":\"2013-07-")
        .append(String.format("%02d", 14 + i % 2))
        .append('T')
        .append(String.format("%02d", i / 60 % 24))
        .append(':')
        .append(String.format("%02d", i % 60))
        .append(":30\"");
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
        sb.append(",\"bk\":2,\"big\":")
          .append(i == 200
              ? 450000000000002L
              : 450000000000000L);
      } else if (i >= 500 && i < 600) {
        // Group bk=1: avg = 4.5e14 + 0.01 — exactly BELOW bk=2's, double-equal to it.
        sb.append(",\"bk\":1,\"big\":")
          .append(i == 500
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
      createProjectionIndex(chain, ctx);
    }
    ProjectionIndexCatalog.clearCache();
  }

  @AfterEach
  void tearDown() {
    ProjectionIndexCatalog.clearCache();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
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

  // ---- packed substring keys and concat-decorated emission -----------------------------------

  @Test
  void substringKeyOrderedByItselfWithConcatEmission() throws Exception {
    // Q42's exact shape: DATE_TRUNC-minute key, ordered BY the key, emitted through concat,
    // with an OFFSET window. The kernel groups and orders on the ISO-minute digit pack;
    // lexicographic order over validated windows must equal numeric order over packs.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $m := substring($u.ts, 1, 16) "
        + "group by $m let $c := count($u) order by $m " + "return {\"M\": concat($m, \":00\"), \"c\": $c}, 5, 9)");
  }

  @Test
  void substringKeyDescendingAndPlainEmission() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $m := substring($u.ts, 1, 16) "
        + "group by $m let $c := count($u) order by $m descending return {\"M\": $m, \"c\": $c}, 1, 7)");
  }

  @Test
  void concatDecorationOnAPlainStringKey() throws Exception {
    // The decoration is emission-only and general: a plain dict key served by the string flat
    // arm, emitted through concat with a prefix.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $d := $u.dept group by $d "
        + "let $c := count($u) order by $c descending return {\"d\": concat(\"dept-\", $d), \"c\": $c}, 1, 3)");
  }

  // ---- unlimited ordered group-by must still match (expression-level sort path) --------------

  @Test
  void orderedWithoutSubsequenceStillMatchesExactly() throws Exception {
    assertOrderedDifferential("for $u in " + SRC + " let $k := $u.k40 group by $k "
        + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}", true);
  }

  // ---- harness --------------------------------------------------------------------------------

  // ---- regex keys (the Q28 REGEXP_REPLACE shape) ----------------------------------------------

  @Test
  void regexKeyGroupsOnTheTransformedString() throws Exception {
    // name n0..n400 → first digit: 401 sources merge into 10 transformed groups — grouping
    // on the RAW value over-partitions and every count differs.
    assertOrderedDifferentialServed(
        "subsequence(for $u in " + SRC + " let $k := replace($u.name, \"^n(.).*$\", \"$1\") group by $k "
            + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 1, 6)");
  }

  @Test
  void q28ShapeRegexHavingStrlenCastAvgAndDeferredMin() throws Exception {
    // The full Q28 composition: regex key + HAVING + strlen operand + cast-avg ordering +
    // a deferred string extremum whose pass-2 row matching must hash the TRANSFORMED key.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " where $u.name != \"\" "
        + "let $k := replace($u.name, \"^n(.).*$\", \"$1\"), $len := string-length($u.nick) group by $k "
        + "let $c := count($u) where $c > 100 let $l := xs:double(avg($len)) order by $l descending "
        + "return {\"k\": $k, \"l\": $l, \"c\": $c, \"m\": min($u.dept)}, 1, 25)");
  }

  @Test
  void regexKeyOverAMissingFieldFallsBack() throws Exception {
    // nick is absent for the whole Ops dept: fn:replace over the empty sequence is "" — a
    // REAL group key the kernel's missing-key arm must not absorb. The serve declines
    // mid-scan and the interpreter answers; agreement is the contract.
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    assertOrderedDifferential(
        "subsequence(for $u in " + SRC + " let $k := replace($u.nick, \"^m(.).*$\", \"$1\") group by $k "
            + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 1, 6)",
        false);
    assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(),
        "a regex key over a field with missing rows must DECLINE to the interpreter");
  }

  // ---- HAVING, strlen operands, cast-avg ordering (the Q27 shape) -----------------------------

  @Test
  void havingCountThresholdSweep() throws Exception {
    // ~2003 rows over 7 k7 groups (~286 each): thresholds that pass all, some, none; both
    // operand orders; ops beyond >. A group failing HAVING must never occupy a window slot.
    for (final String h : new String[] {"$c > 100", "$c > 280", "$c > 100000", "$c >= 287", "$c < 287", "300 > $c",
        "$c != 286"}) {
      assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.k7 group by $k "
          + "let $c := count($u) where " + h + " order by $c descending return {\"k\": $k, \"c\": $c}, 1, 5)");
    }
  }

  @Test
  void havingOnStringKeyAndTheMissingKeyGroup() throws Exception {
    // String-key arm; and the null-key group (missing tier, ~1/3 of rows) is the LARGEST —
    // a threshold only IT passes proves the missing-group offer is filtered symmetrically.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $d := $u.dept group by $d "
        + "let $c := count($u) where $c >= 400 order by $c descending return {\"d\": $d, \"c\": $c}, 1, 4)");
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $t := $u.tier group by $t "
        + "let $c := count($u) where $c > 500 order by $c descending return {\"t\": $t, \"c\": $c}, 1, 4)");
  }

  @Test
  void havingOverANonCountAggregateDeclines() throws Exception {
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    assertOrderedDifferential("subsequence(for $u in " + SRC + " let $k := $u.k7 group by $k "
        + "let $s := sum($u.amount) where $s > 1000 order by $s descending " + "return {\"k\": $k, \"s\": $s}, 1, 5)",
        false);
    assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(),
        "HAVING over a non-count aggregate must DECLINE to the interpreter");
  }

  @Test
  void strlenOperandFoldsCodepointsAndZeroForMissing() throws Exception {
    // nick is ABSENT for the whole Ops dept and carries the U+FF01 (1 codepoint, 3 UTF-8
    // bytes) / U+10400 (1 codepoint, 4 bytes) salts: byte-count-as-length inflates both,
    // and fn:string-length(()) is 0 — SKIPPING missing rows (instead of folding 0) shifts
    // every average and count.
    assertOrderedDifferentialServed(
        "subsequence(for $u in " + SRC + " let $k := $u.k7, $len := string-length($u.name) group by $k "
            + "let $c := count($u) let $l := xs:double(avg($len)) order by $l descending "
            + "return {\"k\": $k, \"l\": $l, \"c\": $c}, 1, 4)");
    assertOrderedDifferentialServed(
        "subsequence(for $u in " + SRC + " let $k := $u.k7, $len := string-length($u.nick) group by $k "
            + "let $c := count($u) let $l := xs:double(avg($len)) order by $l descending "
            + "return {\"k\": $k, \"l\": $l, \"c\": $c}, 1, 4)");
    // min/max over the strlen operand read the same lanes the ordering trusts.
    assertOrderedDifferentialServed(
        "subsequence(for $u in " + SRC + " let $k := $u.k7, $len := string-length($u.nick) group by $k "
            + "let $c := count($u) order by $c descending "
            + "return {\"k\": $k, \"lo\": min($len), \"hi\": max($len), \"c\": $c}, 1, 4)");
  }

  @Test
  void utf8LengthOperandFoldsBytesAndZeroForMissing() throws Exception {
    // The same salted nick dictionary distinguishes this mode from fn:string-length: U+FF01 is
    // three bytes and U+10400 is four, while both are one code point. Missing nick values still
    // contribute zero, matching jn:utf8-length(()).
    assertOrderedDifferentialServed(
        "subsequence(for $u in " + SRC + " let $k := $u.k7, $len := jn:utf8-length($u.nick) group by $k "
            + "let $c := count($u) let $l := xs:double(avg($len)) order by $l descending "
            + "return {\"k\": $k, \"l\": $l, \"c\": $c}, 1, 7)");
    assertOrderedDifferentialServed(
        "subsequence(for $u in " + SRC + " let $k := $u.dept, $len := jn:utf8-length($u.nick) group by $k "
            + "let $c := count($u) order by $c descending "
            + "return {\"k\": $k, \"lo\": min($len), \"hi\": max($len), \"c\": $c}, 1, 4)");
  }

  @Test
  void orderByCastAvgTiesWhereDoublesCollapse() throws Exception {
    // bk groups 2 and 1 have exact avgs 4.5e14+0.02 vs +0.01 — DISTINCT exactly, EQUAL as
    // doubles. Ordering by xs:double(avg) must treat them as a TIE (first-appearance:
    // bk=2 first); the exact-fraction comparator orders them and diverges. The plain-avg
    // ordering test elsewhere pins the opposite direction.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.bk group by $k "
        + "let $a := xs:double(avg($u.big)) order by $a ascending return {\"k\": $k, \"a\": $a}, 1, 3)");
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $k := $u.bk group by $k "
        + "let $a := xs:double(avg($u.big)) order by $a descending return {\"k\": $k, \"a\": $a}, 1, 3)");
  }

  @Test
  void q27ShapeAllThreePiecesCompose() throws Exception {
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC
        + " where $u.name != \"\" let $k := $u.k7, $len := string-length($u.name) group by $k "
        + "let $c := count($u) where $c > 100 let $l := xs:double(avg($len)) order by $l descending "
        + "return {\"k\": $k, \"l\": $l, \"c\": $c}, 1, 25)");
  }

  // ---- conditional keys (the Q39 CASE WHEN shape) ---------------------------------------------

  @Test
  void conditionalKeyAmongPlainCompositeKeys() throws Exception {
    // The Q39 shape: numeric keys + a two-conjunct conditional string key + a plain string key,
    // ordered by count under a subsequence cap.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC
        + " let $t := $u.k7, $src := (if ($u.k7 = 0 and $u.k40 = 0) then $u.name else \"\"), $d := $u.dept "
        + "group by $t, $src, $d let $c := count($u) order by $c descending "
        + "return {\"t\": $t, \"src\": $src, \"d\": $d, \"c\": $c}, 1, 8)");
  }

  @Test
  void conditionalKeyElseLiteralMergesWithStoredValues() throws Exception {
    // else "Eng" COLLIDES with a stored dept: rows failing the condition and rows passing it
    // with dept = "Eng" are ONE group to the interpreter — the else branch must hash into the
    // same domain as dict entries or the served route splits them.
    assertOrderedDifferentialServed(
        "subsequence(for $u in " + SRC + " let $k := (if ($u.k7 = 0) then $u.dept else \"Eng\") group by $k "
            + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 1, 5)");
  }

  @Test
  void conditionalKeyOverMissingOperands() throws Exception {
    // bonus is missing on ~92% of rows: a missing condition operand is FALSE (else branch).
    // tier is missing on ~1/3: a TRUE condition over a missing then-field is the
    // empty-sequence key — the same null-key group a plain deref produces.
    assertOrderedDifferentialServed(
        "subsequence(for $u in " + SRC + " let $k := (if ($u.bonus = 100) then $u.dept else \"nb\") group by $k "
            + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 1, 5)");
    assertOrderedDifferentialServed(
        "subsequence(for $u in " + SRC + " let $k := (if ($u.k7 = 1) then $u.tier else \"other\") group by $k "
            + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 1, 6)");
  }

  @Test
  void aggregateOverConditionalLetDeclines() throws Exception {
    // min($src) would fold the RAW column, ignoring the condition — must decline whole.
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    assertOrderedDifferential("subsequence(for $u in " + SRC
        + " let $d := $u.dept, $src := (if ($u.k7 = 0) then $u.name else \"\") group by $d, $src "
        + "let $c := count($u) order by $c descending "
        + "return {\"d\": $d, \"src\": $src, \"m\": min($src), \"c\": $c}, 1, 3)", false);
    assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(),
        "an aggregate over a conditional let must DECLINE to the interpreter");
  }

  // ---- deferred string extrema (pass 2) -----------------------------------------------------

  @Test
  void stringMinByStringKeyOrderedByCount() throws Exception {
    // The Q21 shape: a conjunctive predicate, a string group key, min over a second string
    // column appearing ONLY in the emission record, ordered by count.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC
        + " where contains($u.name, \"1\") and $u.dept != \"\" let $d := $u.dept group by $d "
        + "let $c := count($u) order by $c descending " + "return {\"d\": $d, \"m\": min($u.name), \"c\": $c}, 1, 3)");
  }

  @Test
  void stringMinAndMaxWithCollationAdversariesAndAllMissingGroup() throws Exception {
    // All four depts win: Ops carries NO nick at all (empty min/max → JSON null), and the
    // other depts contain both U+FF01 and U+10400 — max(nick) inverts if the kernel compares
    // raw UTF-8 bytes without the UTF-16 fallback.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $d := $u.dept group by $d "
        + "let $c := count($u) order by $c descending "
        + "return {\"d\": $d, \"lo\": min($u.nick), \"hi\": max($u.nick), \"c\": $c}, 1, 4)");
  }

  @Test
  void stringMinUnderTheMissingKeyWinner() throws Exception {
    // tier is missing on ~1/3 of rows, so the null-key group WINS count-descending: its
    // extremum folds through the kernel's missing-key slot, not a hash match.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $t := $u.tier group by $t "
        + "let $c := count($u) order by $c descending " + "return {\"t\": $t, \"m\": min($u.name), \"c\": $c}, 1, 2)");
  }

  @Test
  void stringMinWithCountDistinctRides() throws Exception {
    // The Q22 emission shape: a deferred extremum and an exact COUNT(DISTINCT) in one record.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $d := $u.dept group by $d "
        + "let $c := count($u) order by $c descending " + "return {\"d\": $d, \"m\": min($u.name), \"c\": $c, "
        + "\"uniq\": count(distinct-values($u.k7))}, 1, 3)");
  }

  @Test
  void stringMinUnderOrdinalOnlyLimit() throws Exception {
    // No order-by at all: the ordinal-only plan takes the first K groups by appearance and
    // the deferred pass still fills their extrema.
    assertOrderedDifferentialServed("subsequence(for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"d\": $d, \"m\": min($u.name)}, 1, 3)");
  }

  @Test
  void orderByStringMinDeclines() throws Exception {
    // Ordering BY the string extremum would need the selector to compare strings — the plan
    // must fail to resolve and the whole query fall back, never half-serve.
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    assertOrderedDifferential("subsequence(for $u in " + SRC + " let $d := $u.dept group by $d "
        + "let $m := min($u.name) order by $m descending return {\"d\": $d, \"m\": $m}, 1, 3)", false);
    assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(),
        "ordering BY a string extremum must DECLINE to the interpreter");
  }

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

  private static void createProjectionIndex(final SirixCompileChain chain, final SirixQueryContext context) {
    new Query(chain, """
        let $doc := jn:doc('%s','%s')
        let $stats := jn:create-projection-index($doc, '/[]',
          ('/[]/id', '/[]/k7', '/[]/k40', '/[]/amount', '/[]/dept', '/[]/name',
           '/[]/tier', '/[]/bonus', '/[]/bk', '/[]/big', '/[]/ts', '/[]/nick'),
          ('long', 'long', 'long', 'long', 'string', 'string',
           'string', 'long', 'long', 'long', 'string', 'string'))
        return sdb:commit($doc)
        """.formatted(DB, RES)).evaluate(context);
  }
}
