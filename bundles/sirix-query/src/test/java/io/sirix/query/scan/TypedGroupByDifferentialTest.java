package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
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
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Differential gate for the vectorized group-by paths: every query runs through
 * the interpreted pipeline AND the vectorized executor; the serialized results
 * (row-order normalized) must be IDENTICAL. Covers what the scale bench cannot:
 * numeric / boolean / double-typed group keys (historically returned EMPTY —
 * the kernels required string values), multi-key grouping, query-renamed output
 * fields, and predicated variants.
 */
public final class TypedGroupByDifferentialTest {

  // Prime record count: every integer-avg division is non-terminating, so any
  // double-vs-xs:decimal divergence in aggregate emission surfaces immediately.
  private static final int N = 1_999;
  private static final String DB = "typed-gb-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";
  /**
   * Second resource ingested via the REST-path {@code JsonShredder}: its
   * fractional numbers are stored as GENUINE doubles (JsonNumber round-trip),
   * unlike jn:store which keeps them as BigDecimal — exercising the
   * double-row (FK_DOUBLE) predicate arms incl. the generated batch kernels.
   */
  private static final String RES2 = "shredded.jn";
  private static final String SRC2 = "jn:doc('" + DB + "','" + RES2 + "')[]";
  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops" };
  private static final String[] CITIES = { "NYC", "LA", "SF" };

  private Path dbDir;

  private static final String[] TIERS = { "gold", "silver", "bronze" };

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-typed-gb-");
    final Random rng = new Random(7);
    final StringBuilder sb = new StringBuilder(N * 128);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0)
        sb.append(',');
      final String dept = DEPTS[rng.nextInt(DEPTS.length)];
      final String city = CITIES[rng.nextInt(CITIES.length)];
      final int age = 18 + rng.nextInt(8);
      final double score = (rng.nextInt(7) + 1) / 2.0; // 0.5 .. 3.5 — non-integral doubles
      final boolean active = rng.nextBoolean();
      sb.append("{\"id\":").append(i)
        .append(",\"dept\":\"").append(dept)
        .append("\",\"city\":\"").append(city)
        .append("\",\"age\":").append(age)
        .append(",\"score\":").append(score)
        .append(",\"active\":").append(active)
        // "amount" hashes NEGATIVE (like "active") — regression coverage for the
        // nameKey-sentinel family ('< 0' treated legitimate negative hashes as missing).
        .append(",\"amount\":").append(rng.nextInt(1000));
      // ---- adversarial sparse / typed fields ----
      // "bonus": numeric, MISSING on ~30% of records.
      if (i % 10 < 7) {
        sb.append(",\"bonus\":").append(rng.nextInt(1000));
      }
      // "tier": string group key, MISSING on ~third of records.
      if (i % 3 != 0) {
        sb.append(",\"tier\":\"").append(TIERS[rng.nextInt(TIERS.length)]).append('"');
      }
      // "flag": boolean, MISSING on half the records.
      if (i % 2 == 0) {
        sb.append(",\"flag\":").append(rng.nextBoolean());
      }
      // "nully": present-but-NULL on some records, a string on others, missing on the rest.
      if (i % 5 == 0) {
        sb.append(",\"nully\":null");
      } else if (i % 5 < 3) {
        sb.append(",\"nully\":\"n").append(i % 4).append('"');
      }
      // "mixed": NUMBER on some records, STRING on others — a projection column
      // cannot represent both kinds; it must fail closed to the typed kernel.
      if (i % 2 == 0) {
        sb.append(",\"mixed\":").append(i % 7);
      } else {
        sb.append(",\"mixed\":\"m").append(i % 7).append('"');
      }
      // "rating": the famous mixed int/double column — 3 on half the rows, 3.7-style on the rest.
      if (i % 2 == 0) {
        sb.append(",\"rating\":").append(1 + rng.nextInt(5));
      } else {
        sb.append(",\"rating\":").append(1 + rng.nextInt(5)).append('.').append(1 + rng.nextInt(9));
      }
      sb.append('}');
    }
    sb.append(']');

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }

    // Second resource via the REST-path shredder: fractional values become
    // genuine DOUBLES (JsonNumber), not BigDecimals.
    final Random rng2 = new Random(11);
    final StringBuilder sb2 = new StringBuilder(N * 64);
    sb2.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0)
        sb2.append(',');
      sb2.append("{\"id\":").append(i)
         .append(",\"dept\":\"").append(DEPTS[rng2.nextInt(DEPTS.length)]).append('"');
      // rating: int on half the records, genuine DOUBLE on the rest. The
      // shredder keeps a compact double only for EXPONENT-form literals that
      // round-trip (plain "3.7" stays BigDecimal!), so write x.5e0/x.25e0 —
      // exact binary fractions, which also keep parallel double sums
      // order-free vs the interpreter's sequential fold.
      if (i % 2 == 0) {
        sb2.append(",\"rating\":").append(1 + rng2.nextInt(5));
      } else {
        sb2.append(",\"rating\":").append(1 + rng2.nextInt(5)).append(i % 4 == 1 ? ".5e0" : ".25e0");
      }
      // amount: pure-double column (exact quarters).
      sb2.append(",\"amount\":").append(rng2.nextInt(100)).append(".25e0");
      // mix: the SAME small values written as int, exponent-double and decimal —
      // XQuery group-by merges 18, 18.0e0 and 18.00 into ONE group.
      final int mixVal = 10 + (i % 4);
      switch (i % 3) {
        case 0 -> sb2.append(",\"mix\":").append(mixVal);
        case 1 -> sb2.append(",\"mix\":").append(mixVal).append(".0e0");
        default -> sb2.append(",\"mix\":").append(mixVal).append(".00");
      }
      // fracmix: fractional values as decimal vs exponent-double vs scaled decimal.
      final int fracBase = 1 + (i % 3);
      switch (i % 3) {
        case 0 -> sb2.append(",\"fracmix\":").append(fracBase).append(".5");
        case 1 -> sb2.append(",\"fracmix\":").append(fracBase).append(".5e0");
        default -> sb2.append(",\"fracmix\":").append(fracBase).append(".50");
      }
      sb2.append('}');
    }
    sb2.append(']');
    try (final var db = Databases.openJsonDatabase(dbDir.resolve(DB))) {
      db.createResource(io.sirix.access.ResourceConfiguration.newBuilder(RES2).buildPathSummary(true).build());
      try (final var session = db.beginResourceSession(RES2);
           final io.sirix.api.json.JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            io.sirix.service.json.shredder.JsonShredder.createStringReader(sb2.toString()));
        wtx.commit();
      }
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null)
      Databases.removeDatabase(dbDir);
  }

  // ==================== single-key, typed values ====================

  @Test
  void stringKeyCanonical() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"dept\": $d, \"count\": count($u)}");
  }

  @Test
  void intKeyCanonical() throws Exception {
    // Historically the vectorized kernel required STRING group values and
    // silently returned an EMPTY sequence for numeric keys.
    assertDifferential("for $u in " + SRC + " let $a := $u.age group by $a "
        + "return {\"age\": $a, \"count\": count($u)}");
  }

  @Test
  void booleanKeyCanonical() throws Exception {
    assertDifferential("for $u in " + SRC + " let $b := $u.active group by $b "
        + "return {\"active\": $b, \"count\": count($u)}");
  }

  @Test
  void doubleKeyCanonical() throws Exception {
    assertDifferential("for $u in " + SRC + " let $s := $u.score group by $s "
        + "return {\"score\": $s, \"count\": count($u)}");
  }

  @Test
  void renamedStringKey() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"d\": $d, \"n\": count($u)}");
  }

  // ==================== multi-key ====================

  @Test
  void twoStringKeys() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $c := $u.city group by $d, $c "
        + "return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void stringAndIntKeys() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $a := $u.age group by $d, $a "
        + "return {\"dept\": $d, \"age\": $a, \"count\": count($u)}");
  }

  @Test
  void threeKeysMixedTypes() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $b := $u.active, $a := $u.age "
        + "group by $d, $b, $a return {\"d\": $d, \"b\": $b, \"a\": $a, \"n\": count($u)}");
  }

  @Test
  void returnOrderDiffersFromSpecOrder() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $c := $u.city group by $d, $c "
        + "return {\"city\": $c, \"dept\": $d, \"count\": count($u)}");
  }

  // ==================== predicated ====================

  @Test
  void predicatedTwoKeys() throws Exception {
    assertDifferential("for $u in " + SRC + " where $u.active let $d := $u.dept, $c := $u.city "
        + "group by $d, $c return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void predicatedIntKey() throws Exception {
    assertDifferential("for $u in " + SRC + " where $u.age gt 20 let $a := $u.age group by $a "
        + "return {\"age\": $a, \"count\": count($u)}");
  }

  @Test
  void numCmpPredicateMultiKey() throws Exception {
    assertDifferential("for $u in " + SRC + " where $u.age gt 19 and $u.active "
        + "let $d := $u.dept, $a := $u.age group by $d, $a "
        + "return {\"d\": $d, \"a\": $a, \"n\": count($u)}");
  }

  // ==================== negative-hash nameKey regressions ====================
  // nameKeys are String hashes; 'active' and 'amount' hash NEGATIVE. The scan
  // kernels treated `nameKey < 0` as the missing-field sentinel (-1), silently
  // emptying group-by / count-distinct / aggregates over such fields.

  @Test
  void countDistinctOverNegativeHashField() throws Exception {
    assertDifferential("count(for $u in " + SRC + " let $b := $u.active group by $b return $b)");
  }

  @Test
  void sumOverNegativeHashField() throws Exception {
    assertDifferential("sum(for $u in " + SRC + " return $u.amount)");
  }

  @Test
  void sumOverDoubleField() throws Exception {
    // Probes the aggregate path's number typing: score holds non-integral doubles.
    assertDifferential("sum(for $u in " + SRC + " return $u.score)");
  }

  @Test
  void predicatedSumOverDoubleField() throws Exception {
    assertDifferential("sum(for $u in " + SRC + " where $u.active return $u.score)");
  }

  @Test
  void predicatedAvgOverIntField() throws Exception {
    assertDifferential("avg(for $u in " + SRC + " where $u.age gt 20 return $u.age)");
  }

  @Test
  void avgOverIntField() throws Exception {
    // Integer avg is xs:decimal — must match the generic pipeline digit-for-digit.
    assertDifferential("avg(for $u in " + SRC + " return $u.age)");
  }

  @Test
  void groupByNegativeHashIntField() throws Exception {
    assertDifferential("for $u in " + SRC + " let $a := $u.amount group by $a "
        + "return {\"amount\": $a, \"count\": count($u)}");
  }

  // ==================== projection-backed paths ====================
  // Installing the wildcard projection on this resource routes the vectorized
  // runs through the columnar fast paths (incl. the multi-key composite
  // kernel); interpreted results remain the oracle. The registry is keyed by
  // the per-test temp resource, so no cross-test leakage.

  @Test
  void projectionTwoStringKeys() throws Exception {
    assertDifferentialWithProjection("for $u in " + SRC + " let $d := $u.dept, $c := $u.city group by $d, $c "
        + "return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void projectionPredicatedTwoStringKeys() throws Exception {
    assertDifferentialWithProjection("for $u in " + SRC + " where $u.age gt 20 and $u.active "
        + "let $d := $u.dept, $c := $u.city group by $d, $c "
        + "return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void projectionRenamedSingleKeyViaMultiPath() throws Exception {
    assertDifferentialWithProjection("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"d\": $d, \"n\": count($u)}");
  }

  @Test
  void projectionMixedTypeKeysFallBack() throws Exception {
    // age is NUMERIC_LONG in the projection — the composite kernel requires
    // STRING_DICT, so this must fall back to the typed kernel and stay correct.
    assertDifferentialWithProjection("for $u in " + SRC + " let $d := $u.dept, $a := $u.age group by $d, $a "
        + "return {\"dept\": $d, \"age\": $a, \"count\": count($u)}");
  }

  @Test
  void projectionSumOverIntField() throws Exception {
    assertDifferentialWithProjection("sum(for $u in " + SRC + " return $u.amount)");
  }

  @Test
  void projectionAvgOverIntField() throws Exception {
    assertDifferentialWithProjection("avg(for $u in " + SRC + " return $u.age)");
  }

  @Test
  void projectionSumOverDoubleField() throws Exception {
    // score is non-integral: the builder truncates doubles into NUMERIC_LONG, so
    // the integrality gate MUST decline and fall back to the typed double path.
    assertDifferentialWithProjection("sum(for $u in " + SRC + " return $u.score)");
  }

  @Test
  void projectionPredicatedSumOverIntField() throws Exception {
    assertDifferentialWithProjection("sum(for $u in " + SRC + " where $u.active return $u.amount)");
  }

  // ==================== sparse fields (presence bitmaps) ====================
  // bonus/tier/flag are MISSING on a chunk of records; nully mixes null with
  // strings; mixed mixes numbers with strings; ghost exists on NO record.
  // The interpreter remains the oracle for every case.

  @Test
  void sparseGroupKeyScanPath() throws Exception {
    // Records lacking `tier` group under the empty key — historically a loud
    // QueryException; now the typed kernel synthesizes the missing bucket.
    assertDifferential("for $u in " + SRC + " let $t := $u.tier group by $t "
        + "return {\"tier\": $t, \"count\": count($u)}");
  }

  @Test
  void sparseGroupKeyProjectionPath() throws Exception {
    assertDifferentialWithSparseProjection("for $u in " + SRC + " let $t := $u.tier group by $t "
        + "return {\"tier\": $t, \"count\": count($u)}");
  }

  @Test
  void sparseNumericGroupKeyScanPath() throws Exception {
    // Numeric sparse group key routes through the typed-primitive probe and
    // the typed kernel — the missing bucket must still be synthesized.
    assertDifferential("for $u in " + SRC + " let $b := $u.bonus group by $b "
        + "return {\"bonus\": $b, \"count\": count($u)}");
  }

  @Test
  void groupKeyMissingOnAllRecords() throws Exception {
    // `ghost` exists on NO record: ONE empty-key group covering everything.
    assertDifferential("for $u in " + SRC + " let $g := $u.ghost group by $g "
        + "return {\"g\": $g, \"count\": count($u)}");
  }

  @Test
  void presentButNullGroupKey() throws Exception {
    // null and MISSING are distinct buckets; the projection column cannot
    // represent null (fails closed to the typed kernel).
    assertDifferential("for $u in " + SRC + " let $x := $u.nully group by $x "
        + "return {\"x\": $x, \"count\": count($u)}");
  }

  @Test
  void presentButNullGroupKeyWithSparseProjectionInstalled() throws Exception {
    // The installed projection carries `nully` but flags it unrepresentable —
    // the projection path must decline and the fallback stays correct.
    assertDifferentialWithSparseProjection("for $u in " + SRC + " let $x := $u.nully group by $x "
        + "return {\"x\": $x, \"count\": count($u)}");
  }

  @Test
  void mixedKindGroupKeyFailsClosedToTypedKernel() throws Exception {
    // `mixed` holds numbers AND strings — STRING_DICT cannot represent the
    // numeric rows; the typed kernel groups them per-type like the interpreter.
    assertDifferentialWithSparseProjection("for $u in " + SRC + " let $m := $u.mixed group by $m "
        + "return {\"m\": $m, \"count\": count($u)}");
  }

  @Test
  void multiKeyWithSparseSecondKey() throws Exception {
    // Dense anchor (dept) + sparse second key (tier) — scan path encodes 'm'.
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $t := $u.tier group by $d, $t "
        + "return {\"d\": $d, \"t\": $t, \"n\": count($u)}");
  }

  @Test
  void multiKeyWithSparseSecondKeyProjection() throws Exception {
    assertDifferentialWithSparseProjection("for $u in " + SRC + " let $d := $u.dept, $t := $u.tier group by $d, $t "
        + "return {\"d\": $d, \"t\": $t, \"n\": count($u)}");
  }

  @Test
  void multiKeySparseAnchorViaProjection() throws Exception {
    // Sparse FIRST key: the anchor-based slot walk cannot see records missing
    // `tier`, but the projection visits every record and emits 'm' segments.
    assertDifferentialWithSparseProjection("for $u in " + SRC + " let $t := $u.tier, $d := $u.dept group by $t, $d "
        + "return {\"t\": $t, \"d\": $d, \"n\": count($u)}");
  }

  @Test
  void multiKeySparseAnchorWithoutProjectionFailsLoud() {
    // No projection installed: the typed slot-walk cannot reconstruct the
    // secondary keys of records missing the sparse anchor — it must FAIL
    // LOUDLY, never return silently-partial groups.
    final var ex = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
        () -> run("for $u in " + SRC + " let $t := $u.tier, $d := $u.dept group by $t, $d "
            + "return {\"t\": $t, \"d\": $d, \"n\": count($u)}", true));
    final String msg = String.valueOf(ex.getMessage()) + " " + String.valueOf(ex.getCause());
    org.junit.jupiter.api.Assertions.assertTrue(msg.contains("tier"),
        "loud error should name the sparse group field, got: " + msg);
  }

  @Test
  void stringMinMaxAggregatesFailLoud() {
    // min/max over a STRING field use string comparison in the interpreter —
    // the numeric kernels cannot reproduce it. Historically this silently
    // returned 0; it must now fail LOUDLY (never fabricate a value).
    for (final String fn : new String[] { "min", "max", "sum", "avg" }) {
      final var ex = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
          () -> run(fn + "(for $u in " + SRC + " return $u.dept)", true), fn);
      final String msg = String.valueOf(ex.getMessage());
      org.junit.jupiter.api.Assertions.assertTrue(msg.contains("dept") || msg.contains("numeric"),
          fn + " error should explain the non-numeric field, got: " + msg);
    }
  }

  @Test
  void countOverStringField() throws Exception {
    // count(... return $u.dept) counts non-empty derefs — type-agnostic.
    // Historically the numeric accumulator returned 0 for string fields.
    assertDifferential("count(for $u in " + SRC + " return $u.dept)");
  }

  @Test
  void countOverSparseField() throws Exception {
    assertDifferential("count(for $u in " + SRC + " return $u.bonus)");
    assertDifferential("count(for $u in " + SRC + " return $u.tier)");
    assertDifferential("count(for $u in " + SRC + " return $u.ghost)");
  }

  @Test
  void predicatedCountOverSparseField() throws Exception {
    // count(matches WITH the field) — a matching record missing `bonus`
    // contributes zero items. Historically counted ALL matches.
    assertDifferential("count(for $u in " + SRC + " where $u.age gt 20 return $u.bonus)");
    assertDifferential("count(for $u in " + SRC + " where $u.active return $u.tier)");
  }

  @Test
  void sparseAggregates() throws Exception {
    // Anchored on the aggregated field — records missing it contribute nothing.
    assertDifferential("sum(for $u in " + SRC + " return $u.bonus)");
    assertDifferential("avg(for $u in " + SRC + " return $u.bonus)");
    assertDifferential("min(for $u in " + SRC + " return $u.bonus)");
    assertDifferential("max(for $u in " + SRC + " return $u.bonus)");
  }

  @Test
  void sparseAggregatesViaProjection() throws Exception {
    assertDifferentialWithSparseProjection("sum(for $u in " + SRC + " return $u.bonus)");
    assertDifferentialWithSparseProjection("avg(for $u in " + SRC + " return $u.bonus)");
    assertDifferentialWithSparseProjection("min(for $u in " + SRC + " return $u.bonus)");
    assertDifferentialWithSparseProjection("max(for $u in " + SRC + " return $u.bonus)");
  }

  @Test
  void aggregatesOverAllMissingField() throws Exception {
    // sum(()) = 0; avg/min/max(()) = () — the executor used to fabricate 0.
    assertDifferential("sum(for $u in " + SRC + " return $u.ghost)");
    assertDifferential("avg(for $u in " + SRC + " return $u.ghost)");
    assertDifferential("min(for $u in " + SRC + " return $u.ghost)");
  }

  @Test
  void aggregateOverEmptyMatchSet() throws Exception {
    // Predicate matches nothing → avg over zero rows is the empty sequence.
    assertDifferential("avg(for $u in " + SRC + " where $u.age gt 99999 return $u.age)");
    assertDifferential("min(for $u in " + SRC + " where $u.age gt 99999 return $u.age)");
    assertDifferential("sum(for $u in " + SRC + " where $u.age gt 99999 return $u.age)");
  }

  @Test
  void predicateOverSparseField() throws Exception {
    // Comparison over a missing field is FALSE — those records are excluded.
    assertDifferential("count(for $u in " + SRC + " where $u.bonus gt 500 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.bonus lt 500 return $u)");
  }

  @Test
  void predicateOverSparseFieldViaProjection() throws Exception {
    // The projection's presence bitmap must exclude missing rows — the
    // historical layout matched them via the phantom default 0 on `lt`.
    assertDifferentialWithSparseProjection("count(for $u in " + SRC + " where $u.bonus lt 500 return $u)");
    assertDifferentialWithSparseProjection("count(for $u in " + SRC + " where $u.bonus gt 500 return $u)");
  }

  @Test
  void sparseAndDensePredicatesCombined() throws Exception {
    assertDifferential("count(for $u in " + SRC + " where $u.bonus gt 500 and $u.age gt 20 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.age gt 20 and $u.bonus gt 500 return $u)");
  }

  @Test
  void orOverSameSparseField() throws Exception {
    // Same-field OR is claimable (sound anchor) and must exclude missing rows.
    assertDifferential("count(for $u in " + SRC + " where $u.bonus gt 900 or $u.bonus lt 50 return $u)");
  }

  @Test
  void orAcrossFieldsWithSparseSideFallsBack() throws Exception {
    // No sound anchor → detection leaves it to the generic pipeline (both runs
    // identical by construction — this guards the FAIL-CLOSED veto).
    assertDifferential("count(for $u in " + SRC + " where $u.bonus gt 1 or $u.age gt 100 return $u)");
  }

  @Test
  void notOverSparseFieldFallsBack() throws Exception {
    // not($u.flag) over a record missing `flag` is TRUE — not representable by
    // an anchor-based scan; detection must leave it to the generic pipeline.
    assertDifferential("count(for $u in " + SRC + " where not($u.flag) return $u)");
  }

  @Test
  void countDistinctOverSparseField() throws Exception {
    // Missing records emit ZERO items under `return $t` — not a distinct value.
    assertDifferential("count(for $u in " + SRC + " let $t := $u.tier group by $t return $t)");
  }

  @Test
  void countDistinctOverSparseFieldViaProjection() throws Exception {
    assertDifferentialWithSparseProjection(
        "count(for $u in " + SRC + " let $t := $u.tier group by $t return $t)");
  }

  @Test
  void predicatedGroupByWithSparseGroupKey() throws Exception {
    // Dense predicate anchor (age) + sparse group key — visited records missing
    // `tier` encode the missing bucket.
    assertDifferential("for $u in " + SRC + " where $u.age gt 20 let $t := $u.tier group by $t "
        + "return {\"tier\": $t, \"count\": count($u)}");
  }

  // ==================== double-typed predicates (FpCmp) ====================

  // ---- genuine-DOUBLE rows (REST-shredder provenance, resource 2) ----

  @Test
  void doubleRowsIntegerLiteral() throws Exception {
    // The demo's famous rating 3-vs-3.7 with REAL double rows: the integer
    // literal promotes to double per row (no decimal rows on these pages, so
    // the GENERATED batch kernels execute the double arm).
    assertDifferential2("count(for $u in " + SRC2 + " where $u.rating gt 3 return $u)");
    assertDifferential2("count(for $u in " + SRC2 + " where $u.rating le 3 return $u)");
    assertDifferential2("count(for $u in " + SRC2 + " where $u.rating eq 3 return $u)");
  }

  @Test
  void doubleRowsDecimalLiteral() throws Exception {
    assertDifferential2("count(for $u in " + SRC2 + " where $u.rating gt 3.5 return $u)");
    assertDifferential2("count(for $u in " + SRC2 + " where $u.rating eq 3.5 return $u)");
    assertDifferential2("count(for $u in " + SRC2 + " where $u.amount lt 50.25 return $u)");
  }

  // ---- exact-DECIMAL rows (jn:store provenance, resource 1) ----

  @Test
  void decimalRowAggregates() throws Exception {
    // score/rating on resource 1 are BigDecimal rows — the interpreter sums
    // them EXACTLY and divides via Dec#div; the vectorized accumulator must
    // match digit-for-digit (the historical parseDouble fold could not, and
    // brackit's decimal division itself rounded terminating quotients before
    // the 1.0-div-2.0 fix).
    assertDifferential("avg(for $u in " + SRC + " return $u.score)");
    assertDifferential("min(for $u in " + SRC + " return $u.score)");
    assertDifferential("max(for $u in " + SRC + " return $u.score)");
    // Mixed long+decimal column: Int + Dec folds stay exact decimals.
    assertDifferential("avg(for $u in " + SRC + " return $u.rating)");
    assertDifferential("sum(for $u in " + SRC + " return $u.rating)");
  }

  @Test
  void predicatedDecimalRowAggregates() throws Exception {
    assertDifferential("avg(for $u in " + SRC + " where $u.active return $u.score)");
    assertDifferential("min(for $u in " + SRC + " where $u.age gt 20 return $u.score)");
  }

  @Test
  void doubleRowsDoubleLiteral() throws Exception {
    // xs:double literal — the generated FP_CMP kernel runs on double rows.
    assertDifferential2("count(for $u in " + SRC2 + " where $u.rating ge 3.5e0 return $u)");
    assertDifferential2("count(for $u in " + SRC2 + " where $u.amount lt 5.025e1 return $u)");
  }

  @Test
  void doubleRowsRangeAndGroupBy() throws Exception {
    assertDifferential2("count(for $u in " + SRC2
        + " where $u.rating ge 1.5 and $u.rating le 3.5 return $u)");
    assertDifferential2("for $u in " + SRC2 + " where $u.rating gt 2.5 let $d := $u.dept group by $d "
        + "return {\"dept\": $d, \"count\": count($u)}");
    assertDifferential2("for $u in " + SRC2 + " let $r := $u.rating group by $r "
        + "return {\"rating\": $r, \"count\": count($u)}");
  }

  // ==================== mixed-provenance numeric group keys ====================

  @Test
  void mixedProvenanceIntegralGroupKeysMerge() throws Exception {
    // 18 (int), 18.0e0 (genuine double) and 18.00 (decimal) are ONE group under
    // the interpreter's eq-based grouping — the typed kernel historically split
    // them by type tag into three buckets.
    assertDifferential2("for $u in " + SRC2 + " let $m := $u.mix group by $m "
        + "return {\"m\": $m, \"n\": count($u)}");
  }

  @Test
  void mixedProvenanceFractionalGroupKeysMerge() throws Exception {
    // 1.5 (decimal), 1.5e0 (double) and 1.50 (decimal, different scale) merge.
    assertDifferential2("for $u in " + SRC2 + " let $f := $u.fracmix group by $f "
        + "return {\"f\": $f, \"n\": count($u)}");
  }

  @Test
  void mixedProvenanceCountDistinct() throws Exception {
    // Count-distinct rides the same typed grouping — merged keys count once.
    assertDifferential2("count(for $u in " + SRC2 + " let $m := $u.mix group by $m return $m)");
    assertDifferential2("count(for $u in " + SRC2 + " let $f := $u.fracmix group by $f return $f)");
  }

  @Test
  void mixedProvenanceMultiKeyGroupBy() throws Exception {
    assertDifferential2("for $u in " + SRC2 + " let $d := $u.dept, $m := $u.mix group by $d, $m "
        + "return {\"d\": $d, \"m\": $m, \"n\": count($u)}");
  }

  @Test
  void negativeZeroGroupKeyMergesWithZero() throws Exception {
    // JSON ingestion loses the sign of zero: "-0.0e0" routes through
    // BigDecimal (which has no signed zero) and stores +0.0, so document data
    // can never carry a -0.0 group key — both engines group it with 0/0.0.
    // (The executor still guards loudly against a true -0.0 key from other
    // value sources: the interpreter would merge it with 0 but render the
    // FIRST tuple's lexical, which a parallel scan cannot reproduce.)
    shredExtraResource("negzero.jn", "[{\"v\":0},{\"v\":-0.0e0},{\"v\":0.0e0},{\"v\":1}]");
    final String q = "for $u in jn:doc('" + DB + "','negzero.jn')[] let $v := $u.v group by $v "
        + "return {\"v\": $v, \"n\": count($u)}";
    assertEquals(normalize(run2On("negzero.jn", q, false)), normalize(run2On("negzero.jn", q, true)),
        "zero-family group keys must merge identically");
  }

  @Test
  void plateauLongDoubleMixFailsLoud() throws Exception {
    // Above 2^53 one double is atomicCmp-equal to SEVERAL distinct longs —
    // the interpreter's grouping is order-dependent there (verified by probe:
    // the same values group differently depending on arrival order).
    shredExtraResource("plateau.jn",
        "[{\"v\":9007199254740993},{\"v\":9007199254740992.0e0},{\"v\":9007199254740992}]");
    final var ex = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
        () -> run2On("plateau.jn", "for $u in jn:doc('" + DB + "','plateau.jn')[] let $v := $u.v group by $v "
            + "return {\"v\": $v, \"n\": count($u)}", true));
    org.junit.jupiter.api.Assertions.assertTrue(String.valueOf(ex.getMessage()).contains("2^53"),
        "loud error should explain the plateau hazard, got: " + ex.getMessage());
  }

  @Test
  void inexactDecimalImageCollisionFailsLoud() throws Exception {
    // A decimal that is NOT the shortest double form but whose double image
    // collides with a present double key: the interpreter merges them while
    // their lexical renderings differ — order-dependent rendering.
    shredExtraResource("inexact.jn",
        "[{\"v\":0.1e0},{\"v\":0.1000000000000000055511151231257827021181583404541015625}]");
    final var ex = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
        () -> run2On("inexact.jn", "for $u in jn:doc('" + DB + "','inexact.jn')[] let $v := $u.v group by $v "
            + "return {\"v\": $v, \"n\": count($u)}", true));
    org.junit.jupiter.api.Assertions.assertTrue(String.valueOf(ex.getMessage()).contains("decimal"),
        "loud error should explain the decimal-image hazard, got: " + ex.getMessage());
  }

  @Test
  void doubleRowsAggregates() throws Exception {
    assertDifferential2("sum(for $u in " + SRC2 + " return $u.amount)");
    assertDifferential2("avg(for $u in " + SRC2 + " return $u.rating)");
    assertDifferential2("sum(for $u in " + SRC2 + " where $u.rating gt 2.5 return $u.amount)");
  }

  @Test
  void doublePredicateOnDoubleColumn() throws Exception {
    // Historically TRUNCATED: `score gt 2.5` was evaluated as `score gt 2`.
    assertDifferential("count(for $u in " + SRC + " where $u.score gt 2.5 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.score le 0.5 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.score eq 2.5 return $u)");
  }

  @Test
  void doubleRangePredicateOnDoubleColumn() throws Exception {
    assertDifferential("count(for $u in " + SRC + " where $u.score ge 1.5 and $u.score lt 3.5 return $u)");
  }

  @Test
  void fractionalThresholdOnIntegerColumn() throws Exception {
    // x > 20.5 ⟺ x >= 21 on integers; equality against a fraction is empty.
    assertDifferential("count(for $u in " + SRC + " where $u.age gt 20.5 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.age ge 20.5 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.age lt 20.5 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.age le 20.5 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.age eq 20.999 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.age eq 21.0 return $u)");
  }

  @Test
  void doubleLiteralFormPredicates() throws Exception {
    // xs:double literals (exponent form) take the same FpCmp path.
    assertDifferential("count(for $u in " + SRC + " where $u.score gt 2.5e0 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.age ge 2.05e1 return $u)");
  }

  @Test
  void mixedIntDoubleColumnPredicates() throws Exception {
    // The famous rating 3-vs-3.7 family: integer literal over a mixed column
    // must promote double rows (NOT truncate them), and the zone-map prune
    // must not skip pages whose NumberRegion only covers the long rows.
    assertDifferential("count(for $u in " + SRC + " where $u.rating gt 3 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.rating le 3 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.rating eq 3 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.rating gt 3.5 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.rating eq 3.7 return $u)");
  }

  @Test
  void mixedColumnGroupByWithDoublePredicate() throws Exception {
    assertDifferential("for $u in " + SRC + " where $u.rating gt 2.5 let $d := $u.dept group by $d "
        + "return {\"dept\": $d, \"count\": count($u)}");
  }

  @Test
  void doublePredicateWithAggregate() throws Exception {
    assertDifferential("sum(for $u in " + SRC + " where $u.score gt 2.5 return $u.amount)");
    assertDifferential("avg(for $u in " + SRC + " where $u.score le 1.5 return $u.age)");
  }

  @Test
  void sparseFieldWithDoubleThreshold() throws Exception {
    assertDifferential("count(for $u in " + SRC + " where $u.bonus gt 500.5 return $u)");
    assertDifferential("count(for $u in " + SRC + " where $u.bonus le 499.5 return $u)");
  }

  @Test
  void projectionIntegralRewriteForDoubleThresholds() throws Exception {
    // amount is a PROVABLY-INTEGRAL projection column: the fractional
    // thresholds rewrite into exact long-space predicates (gt 99.5 ⟺ ge 100).
    assertDifferentialWithProjection("count(for $u in " + SRC + " where $u.amount gt 99.5 return $u)");
    assertDifferentialWithProjection("count(for $u in " + SRC + " where $u.amount le 99.5 return $u)");
    assertDifferentialWithProjection("count(for $u in " + SRC + " where $u.amount eq 99.5 return $u)");
    assertDifferentialWithProjection("count(for $u in " + SRC + " where $u.amount eq 100.0 return $u)");
    assertDifferentialWithProjection(
        "count(for $u in " + SRC + " where $u.amount ge 100.5 and $u.amount lt 900.5 return $u)");
  }

  @Test
  void projectionDeclinesDoubleThresholdOnNonIntegralColumn() throws Exception {
    // score is KNOWN non-integral in the projection — the rewrite must fail
    // closed and the scan path still answers correctly.
    assertDifferentialWithProjection("count(for $u in " + SRC + " where $u.score gt 2.5 return $u)");
  }

  @Test
  void projectionPredicatedGroupByWithDoubleThreshold() throws Exception {
    assertDifferentialWithProjection("for $u in " + SRC + " where $u.amount gt 500.5 "
        + "let $d := $u.dept, $c := $u.city group by $d, $c "
        + "return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  private void assertDifferentialWithProjection(final String query) throws Exception {
    final String interpreted = normalize(run(query, false));
    final String vectorized = normalize(runWithProjection(query));
    assertEquals(interpreted, vectorized, "projection-backed vectorized result differs for: " + query);
  }

  /** Differential harness with a SPARSE-field wildcard projection installed. */
  private void assertDifferentialWithSparseProjection(final String query) throws Exception {
    final String interpreted = normalize(run(query, false));
    final String vectorized = normalize(runWithSparseProjection(query));
    assertEquals(interpreted, vectorized, "sparse-projection vectorized result differs for: " + query);
  }

  private String runWithSparseProjection(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
      final var session = db.beginResourceSession(RES);
      SirixVectorizedExecutor exec = null;
      try {
        installSparseWildcardProjection(session);
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
        if (exec != null) exec.close();
      }
    }
  }

  /**
   * Install an in-memory wildcard projection over the SPARSE/typed fields:
   * tier (string, sparse), bonus (numeric, sparse), nully (string column fed
   * nulls — must be flagged unrepresentable), mixed (string column fed
   * numbers — likewise), plus the dense dept/age columns. Mirrors
   * {@code ScaleBenchProjectionSetup}'s slow path without HOT persistence.
   */
  private static void installSparseWildcardProjection(final io.sirix.api.json.JsonResourceSession session) {
    final var rootPath = io.brackit.query.util.path.Path.parse("/[]",
        io.brackit.query.util.path.PathParser.Type.JSON);
    final java.util.List<io.brackit.query.util.path.Path<io.brackit.query.atomic.QNm>> fieldPaths =
        java.util.List.of(
            io.brackit.query.util.path.Path.parse("/[]/dept", io.brackit.query.util.path.PathParser.Type.JSON),
            io.brackit.query.util.path.Path.parse("/[]/tier", io.brackit.query.util.path.PathParser.Type.JSON),
            io.brackit.query.util.path.Path.parse("/[]/bonus", io.brackit.query.util.path.PathParser.Type.JSON),
            io.brackit.query.util.path.Path.parse("/[]/age", io.brackit.query.util.path.PathParser.Type.JSON),
            io.brackit.query.util.path.Path.parse("/[]/nully", io.brackit.query.util.path.PathParser.Type.JSON),
            io.brackit.query.util.path.Path.parse("/[]/mixed", io.brackit.query.util.path.PathParser.Type.JSON));
    final var def = io.sirix.index.IndexDefs.createProjectionIdxDef(rootPath, fieldPaths,
        java.util.List.of(io.brackit.query.jdm.Type.STR, io.brackit.query.jdm.Type.STR,
            io.brackit.query.jdm.Type.LON, io.brackit.query.jdm.Type.LON,
            io.brackit.query.jdm.Type.STR, io.brackit.query.jdm.Type.STR),
        7, io.sirix.index.IndexDef.DbType.JSON);
    final java.util.List<byte[]> leaves = new java.util.ArrayList<>();
    final io.sirix.index.projection.ProjectionIndexBuilder builder;
    final int revision = session.getMostRecentRevisionNumber();
    try (var rtx = session.beginNodeReadOnlyTrx(revision);
         var pathSummary = session.openPathSummary(revision)) {
      builder = new io.sirix.index.projection.ProjectionIndexBuilder(def, pathSummary, leaves::add);
      builder.build(rtx);
    }
    io.sirix.index.projection.ProjectionIndexRegistry.installWildcard(
        session.getResourceConfig().getResource().toString(),
        new String[] { "dept", "tier", "bonus", "age", "nully", "mixed" },
        leaves,
        builder.numericColumnNonIntegralFlags());
  }

  private String runWithProjection(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
      final var session = db.beginResourceSession(RES);
      SirixVectorizedExecutor exec = null;
      try {
        io.sirix.query.bench.ScaleBenchProjectionSetupAccess.installWildcard(session);
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
        if (exec != null) exec.close();
      }
    }
  }

  // ==================== harness ====================

  private void assertDifferential(final String query) throws Exception {
    final String interpreted = normalize(run(query, false));
    final String vectorized = normalize(run(query, true));
    assertEquals(interpreted, vectorized, "vectorized result differs from interpreted for: " + query);
  }

  /** Differential harness bound to the shredder-built second resource. */
  private void assertDifferential2(final String query) throws Exception {
    final String interpreted = normalize(run2(query, false));
    final String vectorized = normalize(run2(query, true));
    assertEquals(interpreted, vectorized, "vectorized result differs from interpreted for: " + query);
  }

  /** Shred an extra resource into the test database via the REST-path shredder. */
  private void shredExtraResource(final String resource, final String json) throws Exception {
    try (final var db = Databases.openJsonDatabase(dbDir.resolve(DB))) {
      db.createResource(io.sirix.access.ResourceConfiguration.newBuilder(resource).buildPathSummary(true).build());
      try (final var session = db.beginResourceSession(resource);
           final io.sirix.api.json.JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(io.sirix.service.json.shredder.JsonShredder.createStringReader(json));
        wtx.commit();
      }
    }
  }

  /** {@link #run2} against an arbitrary resource of the test database. */
  private String run2On(final String resource, final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final var session = db.beginResourceSession(resource);
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
        if (exec != null) exec.close();
      }
    }
  }

  private String run2(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final var session = db.beginResourceSession(RES2);
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

  private String run(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final var session = db.beginResourceSession(RES);
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

  /** Group emission order is engine-specific — compare as sorted record lines. */
  private static String normalize(final String s) {
    return s.replace("} {", "}\n{").lines().map(String::strip).filter(l -> !l.isEmpty()).sorted()
            .reduce("", (a, b) -> a + "\n" + b);
  }
}
