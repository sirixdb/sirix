package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
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
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential gate for the vectorized group-by paths: every query runs through the interpreted
 * pipeline AND the vectorized executor; the serialized results (row-order normalized) must be
 * IDENTICAL. Covers what the scale bench cannot: numeric / boolean / double-typed group keys
 * (historically returned EMPTY — the kernels required string values), multi-key grouping,
 * query-renamed output fields, and predicated variants.
 */
public final class TypedGroupByDifferentialTest {

  // Prime record count: every integer-avg division is non-terminating, so any
  // double-vs-xs:decimal divergence in aggregate emission surfaces immediately.
  private static final int N = 1_999;
  private static final String DB = "typed-gb-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";
  /**
   * Second resource ingested via the REST-path {@code JsonShredder}: its fractional numbers are
   * stored as GENUINE doubles (JsonNumber round-trip), unlike jn:store which keeps them as BigDecimal
   * — exercising the double-row (FK_DOUBLE) predicate arms incl. the generated batch kernels.
   */
  private static final String RES2 = "shredded.jn";
  private static final String SRC2 = "jn:doc('" + DB + "','" + RES2 + "')[]";
  private static final String[] DEPTS = {"Eng", "Sales", "Mkt", "Ops"};
  private static final String[] CITIES = {"NYC", "LA", "SF"};

  private Path dbDir;
  private boolean sparseProjectionReady;

  private static final String[] TIERS = {"gold", "silver", "bronze"};

  @BeforeEach
  void setUp() throws Exception {
    sparseProjectionReady = false;
    dbDir = Files.createTempDirectory("sirix-typed-gb-");
    final Random rng = new Random(7);
    final StringBuilder sb = new StringBuilder(N * 128);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0)
        sb.append(',');
      appendRecord(sb, rng, i);
    }
    sb.append(']');

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }

    setUpSecondResource();
  }

  /**
   * One record of the main corpus: dense typed fields plus the adversarial sparse / mixed-type ones
   * the kernels have to fail closed on.
   */
  private static void appendRecord(final StringBuilder sb, final Random rng, final int i) {
    final String dept = DEPTS[rng.nextInt(DEPTS.length)];
    final String city = CITIES[rng.nextInt(CITIES.length)];
    final int age = 18 + rng.nextInt(8);
    final double score = (rng.nextInt(7) + 1) / 2.0; // 0.5 .. 3.5 — non-integral doubles
    final boolean active = rng.nextBoolean();
    sb.append("{\"id\":")
      .append(i)
      .append(",\"dept\":\"")
      .append(dept)
      .append("\",\"city\":\"")
      .append(city)
      .append("\",\"age\":")
      .append(age)
      .append(",\"score\":")
      .append(score)
      .append(",\"active\":")
      .append(active)
      // "amount" hashes NEGATIVE (like "active") — regression coverage for the
      // nameKey-sentinel family ('< 0' treated legitimate negative hashes as missing).
      .append(",\"amount\":")
      .append(rng.nextInt(1000));
    // ---- adversarial sparse / typed fields ----
    // "bonus": numeric, MISSING on ~30% of records.
    if (i % 10 < 7) {
      sb.append(",\"bonus\":").append(rng.nextInt(1000));
    }
    // "tier": string group key, MISSING on ~third of records.
    if (i % 3 != 0) {
      sb.append(",\"tier\":\"").append(TIERS[rng.nextInt(TIERS.length)]).append('"');
    }
    // "region": a SECOND sparse string group key, present on ~half the records
    // with sparsity DISJOINT from tier — a both-sparse multi-key combination
    // (tier, region) anchored neither field densely.
    if (i % 2 == 1) {
      sb.append(",\"region\":\"").append(CITIES[rng.nextInt(CITIES.length)]).append("-r\"");
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

  /**
   * Second resource via the REST-path shredder: fractional values become genuine DOUBLES
   * (JsonNumber), not BigDecimals.
   */
  private void setUpSecondResource() throws Exception {
    final Random rng2 = new Random(11);
    final StringBuilder sb2 = new StringBuilder(N * 64);
    sb2.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0)
        sb2.append(',');
      sb2.append("{\"id\":").append(i).append(",\"dept\":\"").append(DEPTS[rng2.nextInt(DEPTS.length)]).append('"');
      // rating: int on half the records, genuine DOUBLE on the rest. The
      // shredder keeps a compact double only for EXPONENT-form literals that
      // round-trip (plain "3.7" stays BigDecimal!), so write x.5e0/x.25e0 —
      // exact binary fractions, which also keep parallel double sums
      // order-free vs the interpreter's sequential fold.
      if (i % 2 == 0) {
        sb2.append(",\"rating\":").append(1 + rng2.nextInt(5));
      } else {
        sb2.append(",\"rating\":")
           .append(1 + rng2.nextInt(5))
           .append(i % 4 == 1
               ? ".5e0"
               : ".25e0");
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
        wtx.insertSubtreeAsFirstChild(io.sirix.service.json.shredder.JsonShredder.createStringReader(sb2.toString()));
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
    assertDifferential(
        "for $u in " + SRC + " let $d := $u.dept group by $d " + "return {\"dept\": $d, \"count\": count($u)}");
  }

  @Test
  void intKeyCanonical() throws Exception {
    // Historically the vectorized kernel required STRING group values and
    // silently returned an EMPTY sequence for numeric keys.
    assertDifferential(
        "for $u in " + SRC + " let $a := $u.age group by $a " + "return {\"age\": $a, \"count\": count($u)}");
  }

  @Test
  void booleanKeyCanonical() throws Exception {
    assertDifferential(
        "for $u in " + SRC + " let $b := $u.active group by $b " + "return {\"active\": $b, \"count\": count($u)}");
  }

  @Test
  void doubleKeyCanonical() throws Exception {
    assertDifferential(
        "for $u in " + SRC + " let $s := $u.score group by $s " + "return {\"score\": $s, \"count\": count($u)}");
  }

  @Test
  void renamedStringKey() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept group by $d " + "return {\"d\": $d, \"n\": count($u)}");
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

  /**
   * A predicate over the group-by field itself must be applied by the column kernel, not per record.
   *
   * <p>
   * {@code regionEligible} used to read {@code predicateOrNull == null}, so every predicated group-by
   * — including this one, where the filter and the key are the same column — reconstructed records
   * through {@code rtx.moveTo}. The interval is now handed to the page kernel, which turns it into a
   * selection vector over the encoded column
   * ({@link io.sirix.page.pax.NumberRegionSimd#selectMatching}) and tallies only the survivors.
   *
   * <p>
   * {@link #predicatedIntKey} already gates the ANSWER differentially. What it cannot see is which
   * path produced it: the record walk returns the same groups, so a regression that quietly
   * disqualifies the columnar path would leave every assertion green. Hence the page counter.
   */
  @Test
  void predicateOnTheGroupKeyIsAppliedInTheColumn() throws Exception {
    final String query = "for $u in " + SRC + " where $u.age gt 20 let $a := $u.age group by $a "
        + "return {\"age\": $a, \"count\": count($u)}";
    final String interpreted = normalize(run(query, false));
    SirixVectorizedExecutor.resetRegionGroupByPages();
    final String vectorized = normalize(run(query, true));
    assertEquals(interpreted, vectorized, "vectorized result differs from interpreted for: " + query);
    assertTrue(SirixVectorizedExecutor.regionGroupByPagesServed() > 0,
        "no page answered from the number column for a predicate over the group-by field");
    // `age` spans 18..25 on every page, so the interval [21, +inf) is neither disjoint from a
    // page's zone map nor contained in it — the selection kernel is the only thing that can
    // answer these pages, and this is what distinguishes it from the zone-map shortcuts.
    assertTrue(SirixVectorizedExecutor.regionGroupBySelectionPages() > 0,
        "the interval was settled by zone maps alone — selectMatching never ran");
  }

  /**
   * The same, for an interval that rules whole pages out and one that lets everything through.
   *
   * <p>
   * Both ends are answered by the tag's zone map rather than by the selection kernel — one returning
   * "nothing on this page survives", the other "every value does, take the unfiltered tally". They
   * are the arms most easily got backwards, and a wrong one shows up as extra or missing groups
   * rather than as a crash.
   */
  @Test
  void zoneMapEndsOfTheGroupKeyFilter() throws Exception {
    // `age` is 18..25 on every record: the first interval excludes all of them, the second
    // includes all of them, and the third cuts through the middle.
    for (final String bound : new String[] {"gt 1000", "ge 0", "gt 21"}) {
      final String query = "for $u in " + SRC + " where $u.age " + bound + " let $a := $u.age "
          + "group by $a return {\"age\": $a, \"count\": count($u)}";
      assertDifferential(query);
    }
  }

  /**
   * A predicate on a DIFFERENT field than the group key must keep going through the records.
   *
   * <p>
   * The columnar kernel tallies a tag's values with no notion of which record each belongs to, so it
   * cannot intersect one column's survivors with another's. Claiming the page here would count every
   * {@code age} on it regardless of {@code amount} — the answer would simply be the unfiltered
   * grouping. The differential assertion is the real gate; the counter pins that the page kernel
   * declined rather than accidentally agreeing.
   */
  @Test
  void predicateOnAnotherFieldStillGoesThroughRecords() throws Exception {
    final String query = "for $u in " + SRC + " where $u.amount gt 500 let $a := $u.age group by $a "
        + "return {\"age\": $a, \"count\": count($u)}";
    final String interpreted = normalize(run(query, false));
    SirixVectorizedExecutor.resetRegionGroupByPages();
    final String vectorized = normalize(run(query, true));
    assertEquals(interpreted, vectorized, "vectorized result differs from interpreted for: " + query);
    assertEquals(0, SirixVectorizedExecutor.regionGroupByPagesServed(),
        "a cross-field predicate cannot be applied by a per-field column kernel");
  }

  @Test
  void numCmpPredicateMultiKey() throws Exception {
    assertDifferential("for $u in " + SRC + " where $u.age gt 19 and $u.active "
        + "let $d := $u.dept, $a := $u.age group by $d, $a " + "return {\"d\": $d, \"a\": $a, \"n\": count($u)}");
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
    assertDifferential(
        "for $u in " + SRC + " let $a := $u.amount group by $a " + "return {\"amount\": $a, \"count\": count($u)}");
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
        + "let $d := $u.dept, $c := $u.city group by $d, $c " + "return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void projectionRenamedSingleKeyViaMultiPath() throws Exception {
    assertDifferentialWithProjection(
        "for $u in " + SRC + " let $d := $u.dept group by $d " + "return {\"d\": $d, \"n\": count($u)}");
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
    assertDifferential(
        "for $u in " + SRC + " let $t := $u.tier group by $t " + "return {\"tier\": $t, \"count\": count($u)}");
  }

  @Test
  void sparseGroupKeyProjectionPath() throws Exception {
    assertDifferentialWithSparseProjection(
        "for $u in " + SRC + " let $t := $u.tier group by $t " + "return {\"tier\": $t, \"count\": count($u)}");
  }

  @Test
  void sparseNumericGroupKeyScanPath() throws Exception {
    // Numeric sparse group key routes through the typed-primitive probe and
    // the typed kernel — the missing bucket must still be synthesized.
    assertDifferential(
        "for $u in " + SRC + " let $b := $u.bonus group by $b " + "return {\"bonus\": $b, \"count\": count($u)}");
  }

  @Test
  void groupKeyMissingOnAllRecords() throws Exception {
    // `ghost` exists on NO record: ONE empty-key group covering everything.
    assertDifferential(
        "for $u in " + SRC + " let $g := $u.ghost group by $g " + "return {\"g\": $g, \"count\": count($u)}");
  }

  @Test
  void presentButNullGroupKey() throws Exception {
    // null and MISSING are distinct buckets; the projection column cannot
    // represent null (fails closed to the typed kernel).
    assertDifferential(
        "for $u in " + SRC + " let $x := $u.nully group by $x " + "return {\"x\": $x, \"count\": count($u)}");
  }

  @Test
  void presentButNullGroupKeyWithSparseProjectionInstalled() throws Exception {
    // The installed projection carries `nully` but flags it unrepresentable —
    // the projection path must decline and the fallback stays correct.
    assertDifferentialWithSparseProjection(
        "for $u in " + SRC + " let $x := $u.nully group by $x " + "return {\"x\": $x, \"count\": count($u)}");
  }

  @Test
  void mixedKindGroupKeyFailsClosedToTypedKernel() throws Exception {
    // `mixed` holds numbers AND strings — STRING_DICT cannot represent the
    // numeric rows; the typed kernel groups them per-type like the interpreter.
    assertDifferentialWithSparseProjection(
        "for $u in " + SRC + " let $m := $u.mixed group by $m " + "return {\"m\": $m, \"count\": count($u)}");
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
  void multiKeySparseFirstKeyDenseSecondReAnchorsToScan() throws Exception {
    // Sparse FIRST key (tier) + DENSE second key (dept): no projection installed.
    // The scan re-anchors on the provably-dense `dept` (path-summary reference
    // count == record total), visiting EVERY record, so the sparse `tier` —
    // including its missing 'm' bucket — falls out exactly. No fallback, no bail.
    assertDifferential("for $u in " + SRC + " let $t := $u.tier, $d := $u.dept group by $t, $d "
        + "return {\"t\": $t, \"d\": $d, \"n\": count($u)}");
  }

  @Test
  void multiKeyDenseFirstSparseSecondScanPath() throws Exception {
    // Mirror order: dense anchor already at index 0 — the existing happy path,
    // verified on the SCAN path (no projection).
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $t := $u.tier group by $d, $t "
        + "return {\"d\": $d, \"t\": $t, \"n\": count($u)}");
  }

  @Test
  void multiKeySparseFirstDenseSecondNumericKeysScanPath() throws Exception {
    // Sparse string first key (tier) + dense NUMERIC second key (age): re-anchor
    // on the numeric dense field; the typed kernel encodes long + 'm' keys.
    assertDifferential("for $u in " + SRC + " let $t := $u.tier, $a := $u.age group by $t, $a "
        + "return {\"t\": $t, \"a\": $a, \"n\": count($u)}");
  }

  @Test
  void multiKeyDenseAnchorIsThirdKeyScanPath() throws Exception {
    // Two sparse keys (tier, flag) FIRST, dense key (city) THIRD. The dense
    // anchor is not at the front — the selector must scan it regardless of
    // group-field position, and both sparse keys fall out (incl. 'm').
    assertDifferential("for $u in " + SRC + " let $t := $u.tier, $f := $u.flag, $c := $u.city "
        + "group by $t, $f, $c return {\"t\": $t, \"f\": $f, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void multiKeyAbsentFirstKeyFailsLoudEvenWithDenseSecond() {
    // First key is absent on EVERY record (ghost). The interpreter has an
    // order-dependent quirk: an empty FIRST grouping variable collapses the
    // whole grouping to ONE all-null tuple ({g:null,d:null,n:N}) rather than
    // per-dept groups. A dense-anchored scan would produce per-dept groups, so
    // the dense-anchor selector VETOES re-anchoring here and the path fails
    // LOUDLY (fail-closed) instead of returning a divergent result.
    final var ex = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
        () -> run("for $u in " + SRC + " let $g := $u.ghost, $d := $u.dept group by $g, $d "
            + "return {\"g\": $g, \"d\": $d, \"n\": count($u)}", true));
    final String msg = String.valueOf(ex.getMessage()) + " " + String.valueOf(ex.getCause());
    org.junit.jupiter.api.Assertions.assertTrue(msg.contains("ghost"),
        "loud error should name the absent first group field, got: " + msg);
  }

  @Test
  void multiKeyAbsentSecondKeyDenseFirstScanPath() throws Exception {
    // Mirror: absent key SECOND, dense key FIRST. The interpreter does NOT
    // collapse here (empty key is not first) — it yields proper per-dept groups
    // with the absent key rendered null. The dense anchor (dept) is already
    // first, so the scan visits every record and emits the all-'m' second
    // segment, matching the interpreter exactly (no bail).
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $g := $u.ghost group by $d, $g "
        + "return {\"d\": $d, \"g\": $g, \"n\": count($u)}");
  }

  @Test
  void multiKeySparseFirstAbsentSecondDenseThirdScanPath() throws Exception {
    // Sparse-but-present first key (tier), absent second key (ghost), dense
    // third key (city). The first key is present on some records (no collapse),
    // so re-anchoring on the dense city is sound; tier emits its missing bucket
    // and ghost emits all-'m'.
    assertDifferential("for $u in " + SRC + " let $t := $u.tier, $g := $u.ghost, $c := $u.city "
        + "group by $t, $g, $c return {\"t\": $t, \"g\": $g, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void multiKeyBothSparseWithoutProjectionIsCompletedNotRefused() throws Exception {
    // No group field is provably dense (tier ~67% present, region ~50% present, sparsity DISJOINT)
    // and no projection is installed. The typed slot-walk anchors on one sparse field and cannot
    // see the records missing it, which used to be a loud failure — the secondary keys of those
    // records are unknowable from that one walk.
    //
    // They are knowable from the OTHER key's walk. Running a pass per key field visits every record
    // carrying at least one of them, each record counted by the pass for the first key it carries;
    // the records carrying neither (i % 6 == 0 here) are one all-missing group sized from the record
    // total. The corpus covers all four combinations — tier only, region only, both, neither — so
    // the de-duplication rule is exercised, not merely the happy path.
    assertDifferential("for $u in " + SRC + " let $t := $u.tier, $r := $u.region group by $t, $r "
        + "return {\"t\": $t, \"r\": $r, \"n\": count($u)}");
  }

  @Test
  void multiKeyBothSparsePlusAbsentKeyIsCompleted() throws Exception {
    // Same both-sparse pair with a third key no record carries at all ('ghost' is absent from the
    // name dictionary). It gets no pass of its own and can never be the first key a record carries,
    // so it must neither claim records nor block the passes that should.
    assertDifferential("for $u in " + SRC + " let $t := $u.tier, $r := $u.region, $g := $u.ghost "
        + "group by $t, $r, $g return {\"t\": $t, \"r\": $r, \"g\": $g, \"n\": count($u)}");
  }

  @Test
  void multiKeySparseKeyOrderDoesNotChangeTheGrouping() throws Exception {
    // The completion counts each record under the FIRST key it carries, so swapping the key order
    // swaps which pass owns which record. The grouping must not notice.
    assertDifferential("for $u in " + SRC + " let $r := $u.region, $t := $u.tier group by $r, $t "
        + "return {\"r\": $r, \"t\": $t, \"n\": count($u)}");
  }

  @Test
  void multiKeyBothSparseFallbackViaProjectionStaysCorrect() throws Exception {
    // The SAME both-sparse query that fails loud on the bare scan path returns
    // the CORRECT (interpreter-identical) result once a covering projection is
    // installed — proving the loud bail is a fail-closed gate, not a dead end.
    // Both tier and region are sparse STRING_DICT columns in the projection.
    assertDifferentialWithSparseProjection("for $u in " + SRC + " let $t := $u.tier, $r := $u.region "
        + "group by $t, $r return {\"t\": $t, \"r\": $r, \"n\": count($u)}");
  }

  @Test
  void multiKeySparseFirstDenseSecondMixedProvenanceScanPath() throws Exception {
    // SHREDDER-built resource (genuine double rows): sparse first key (xtra,
    // ~half present) + dense second key (dept). Re-anchor on dept; the missing
    // first key emits 'm'. Exercises a different value provenance than jn:store.
    shredExtraResource("provmix.jn", provMixJson());
    assertDifferentialOn("provmix.jn",
        "for $u in jn:doc('" + DB + "','provmix.jn')[] let $x := $u.xtra, $d := $u.dept group by $x, $d "
            + "return {\"x\": $x, \"d\": $d, \"n\": count($u)}");
  }

  /** A small shredder corpus: dense `dept`, sparse `xtra` (present on ~half). */
  private static String provMixJson() {
    final StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = 0; i < 200; i++) {
      if (i > 0)
        sb.append(',');
      sb.append("{\"id\":").append(i).append(",\"dept\":\"").append(DEPTS[i % DEPTS.length]).append('"');
      if (i % 2 == 0) {
        // genuine double via exponent form on the sparse key
        sb.append(",\"xtra\":").append(1 + (i % 3)).append(".5e0");
      }
      sb.append('}');
    }
    sb.append(']');
    return sb.toString();
  }

  @Test
  void aggregateOverANestedForOnAGroupedVariableStaysPerGroup() throws Exception {
    // The grouped variable binds to the sequence of THIS group's tuples, so an aggregate over a
    // nested `for` on it must fold that group and nothing else. The detector used to claim the inner
    // FLWOR and trace its source variable back through the enclosing `for` to the whole document,
    // which answered every group with the GLOBAL fold — silently, and only for the shapes where the
    // aggregate reads a field (count($g) was unaffected, which is what made it hard to notice).
    assertDifferential("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"d\": $d, \"s\": sum(for $x in $u return $x.age)}");
    assertDifferential("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"d\": $d, \"a\": avg(for $x in $u return $x.age)}");
    assertDifferential("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"d\": $d, \"m\": min(for $x in $u return $x.age), " + "\"x\": max(for $x in $u return $x.age)}");
    assertDifferential("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"d\": $d, \"u\": count(distinct-values(for $x in $u return $x.age))}");
    assertDifferential("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"d\": $d, \"n\": count(for $x in $u return $x.age)}");
    // Two grouping keys, and a nested for over a SPARSE field.
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $c := $u.city group by $d, $c "
        + "return {\"d\": $d, \"c\": $c, \"s\": sum(for $x in $u return $x.bonus)}");
  }

  @Test
  void stringMinMaxAggregatesMatchTheInterpreter() throws Exception {
    // min/max over a STRING field are well defined — fn:min/fn:max order xs:string by codepoint —
    // and the interpreter answers them, so the fast path has to produce the same answer rather than
    // fail. (It used to fail loudly here, which made ClickBench Q6's MIN(EventDate)/MAX(EventDate)
    // unrunnable in the default configuration.)
    assertDifferential("min(for $u in " + SRC + " return $u.dept)");
    assertDifferential("max(for $u in " + SRC + " return $u.dept)");
  }

  @Test
  void stringSumAndAvgAggregatesFailLoud() {
    // sum/avg over a STRING field are a type error in the interpreter too, so the numeric kernels
    // must never fabricate a value for them — they stay loud.
    for (final String fn : new String[] {"sum", "avg"}) {
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
    // A third of the records MISS `tier` and intern the "" default into their leaf dictionaries: the
    // hashed dictionary union must count the present tiers and no phantom — and it, not the bounded
    // content-based union or the row-wise group count, must be the route that answered.
    final long servedBefore = SirixVectorizedExecutor.projectionCountDistinctServedCount();
    final long unionBefore = SirixVectorizedExecutor.projectionCountDistinctDictUnionServedCount();
    assertDifferentialWithSparseProjection("count(for $u in " + SRC + " let $t := $u.tier group by $t return $t)");
    assertEquals(servedBefore + 1, SirixVectorizedExecutor.projectionCountDistinctServedCount(),
        "a correct answer is not route evidence: the projection count-distinct outcome counter must move");
    assertEquals(unionBefore + 1, SirixVectorizedExecutor.projectionCountDistinctDictUnionServedCount(),
        "…and the hashed dictionary union must be the implementation that produced it");
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
    assertDifferential2("count(for $u in " + SRC2 + " where $u.rating ge 1.5 and $u.rating le 3.5 return $u)");
    assertDifferential2("for $u in " + SRC2 + " where $u.rating gt 2.5 let $d := $u.dept group by $d "
        + "return {\"dept\": $d, \"count\": count($u)}");
    assertDifferential2(
        "for $u in " + SRC2 + " let $r := $u.rating group by $r " + "return {\"rating\": $r, \"count\": count($u)}");
  }

  // ==================== mixed-provenance numeric group keys ====================

  @Test
  void mixedProvenanceIntegralGroupKeysMerge() throws Exception {
    // 18 (int), 18.0e0 (genuine double) and 18.00 (decimal) are ONE group under
    // the interpreter's eq-based grouping — the typed kernel historically split
    // them by type tag into three buckets.
    assertDifferential2(
        "for $u in " + SRC2 + " let $m := $u.mix group by $m " + "return {\"m\": $m, \"n\": count($u)}");
  }

  @Test
  void mixedProvenanceFractionalGroupKeysMerge() throws Exception {
    // 1.5 (decimal), 1.5e0 (double) and 1.50 (decimal, different scale) merge.
    assertDifferential2(
        "for $u in " + SRC2 + " let $f := $u.fracmix group by $f " + "return {\"f\": $f, \"n\": count($u)}");
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
    final var ex =
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> run2On("plateau.jn", "for $u in jn:doc('"
            + DB + "','plateau.jn')[] let $v := $u.v group by $v " + "return {\"v\": $v, \"n\": count($u)}", true));
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
    final var ex =
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> run2On("inexact.jn", "for $u in jn:doc('"
            + DB + "','inexact.jn')[] let $v := $u.v group by $v " + "return {\"v\": $v, \"n\": count($u)}", true));
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
        + "let $d := $u.dept, $c := $u.city group by $d, $c " + "return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  // ==================== NUMERIC_LONG group keys via the projection ====================
  // A third resource whose numeric columns are built to exercise every arm of the
  // NUMERIC_LONG group-by kernel and every gate that must decline it. The projection is
  // CATALOGUED (jn:create-projection-index + commit), not registry-installed, so these
  // cases run against a column-LAZY handle — the arm that resolves the zone-map range from
  // leaf descriptors rather than from leaf headers.

  private static final String RES3 = "numeric-groupby.jn";
  private static final String SRC3 = "jn:doc('" + DB + "','" + RES3 + "')[]";

  private boolean numericResourceReady;

  /**
   * Corpus for the numeric group-by cases. Per record:
   * <ul>
   * <li>{@code small} — 8 dense values: the DENSE accumulator arm;</li>
   * <li>{@code neg} — straddles zero: the {@code v - base} sign trap;</li>
   * <li>{@code wide} — span far beyond the row count: forced onto the HASH arm by the "never more
   * cells than rows" guard;</li>
   * <li>{@code sparse} — absent on ~30% of records: the missing-field group;</li>
   * <li>{@code decint} — the SAME integral value written as {@code 20} and as {@code 20.00}; the
   * decimal is integral and long-representable, so it is NOT flagged and the column SERVES, merging
   * both spellings into one group (the one non-obvious serve);</li>
   * <li>{@code hasnull} — integers plus JSON {@code null}: present-but-unrepresentable, gate 2;</li>
   * <li>{@code realmix} — {@code 3} mixed with the genuine double {@code 3.0e0}: every Double source
   * flags the column non-integral, gate 3;</li>
   * <li>{@code dbl} — declared {@code double}, hence a NUMERIC_DOUBLE column, gate 1;</li>
   * <li>{@code tags} — an array layer, hence a STRING_SET column, gate 1;</li>
   * <li>{@code flag} — a BOOLEAN column, gate 1;</li>
   * <li>{@code dept} — a string key, for the mixed composite that must still decline.</li>
   * </ul>
   */
  private void ensureNumericResource() throws Exception {
    if (numericResourceReady) {
      return;
    }
    final StringBuilder sb = new StringBuilder(N * 128);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i);
      sb.append(",\"small\":").append(100 + i % 8);
      sb.append(",\"neg\":").append(-4 + i % 9);
      sb.append(",\"wide\":").append(i * 7);
      sb.append(",\"dept\":\"").append(DEPTS[i % DEPTS.length]).append('"');
      if (i % 10 < 7) {
        sb.append(",\"sparse\":").append(i % 5);
      }
      sb.append(",\"decint\":")
        .append(20 + i % 3)
        .append(i % 2 == 0
            ? ""
            : ".00");
      sb.append(",\"hasnull\":")
        .append(i % 3 == 0
            ? "null"
            : String.valueOf(i % 4));
      sb.append(",\"realmix\":")
        .append(i % 3)
        .append(i % 2 == 0
            ? ""
            : ".0e0");
      sb.append(",\"dbl\":").append(i % 4).append(".5e0");
      sb.append(",\"flag\":").append(i % 2 == 0);
      sb.append(",\"tags\":[\"t").append(i % 3).append("\"]}");
    }
    sb.append(']');
    shredExtraResource(RES3, sb.toString());
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      // The commit is load-bearing: without it the definition is never catalogued and every
      // lookup reports "no covering handle", so both arms quietly run through the records.
      new Query(chain,
          "let $doc := jn:doc('" + DB + "','" + RES3 + "') " + "let $i := jn:create-projection-index($doc, '/[]', "
              + "('/[]/small','/[]/neg','/[]/wide','/[]/sparse','/[]/decint','/[]/hasnull',"
              + "'/[]/realmix','/[]/dbl','/[]/flag','/[]/dept','/[]/tags/[]'), "
              + "('long','long','long','long','long','long','long','double','boolean','string','string')) "
              + "return {\"revision\": sdb:commit($doc)}").evaluate(ctx);
    }
    ProjectionIndexCatalog.clearCache();
    numericResourceReady = true;
  }

  /** The vectorized answer must match the interpreter AND the numeric route must have taken it. */
  private String assertNumericProjectionServes(final String query) throws Exception {
    ensureNumericResource();
    final String interpreted = normalize(run2On(RES3, query, false));
    final long before = SirixVectorizedExecutor.numericGroupByServedCount();
    final String vectorized = normalize(run2On(RES3, query, true));
    assertEquals(interpreted, vectorized, "numeric projection result differs for: " + query);
    assertTrue(SirixVectorizedExecutor.numericGroupByServedCount() > before,
        "the numeric group-by route DECLINED, so the agreement above is vacuous: " + query);
    return vectorized;
  }

  /** The gate must decline AND the fallback must still be right — a decline is not an error. */
  private void assertNumericProjectionDeclines(final String query) throws Exception {
    ensureNumericResource();
    final String interpreted = normalize(run2On(RES3, query, false));
    final long before = SirixVectorizedExecutor.numericGroupByServedCount();
    final String vectorized = normalize(run2On(RES3, query, true));
    assertEquals(interpreted, vectorized, "fallback result differs for the declined shape: " + query);
    assertEquals(before, SirixVectorizedExecutor.numericGroupByServedCount(),
        "the numeric group-by route SERVED a shape it must decline: " + query);
  }

  private static String groupByCount(final String field) {
    return "for $u in " + SRC3 + " let $k := $u." + field + " group by $k " + "return {\"" + field
        + "\": $k, \"count\": count($u)}";
  }

  @Test
  void projectionNumericGroupKeyDenseArm() throws Exception {
    // 8 distinct values over ~2k rows: cells <= rows and cells <= the cap, so the dense
    // index-by-subtraction accumulator is chosen.
    assertNumericProjectionServes(groupByCount("small"));
  }

  @Test
  void projectionNumericGroupKeyHashArm() throws Exception {
    // Same query, dense arm switched OFF in the SAME JVM — the two accumulators must be
    // indistinguishable by result. (Comparing them across builds is what the box's thermal
    // throttling makes untrustworthy; this is the correctness half of that discipline.)
    ensureNumericResource();
    SirixVectorizedExecutor.setNumericDenseGroupByEnabled(false);
    try {
      assertNumericProjectionServes(groupByCount("small"));
    } finally {
      SirixVectorizedExecutor.setNumericDenseGroupByEnabled(true);
    }
    // And a column the guards push onto the hash arm on their own: span 13,986 over 1,999 rows.
    assertNumericProjectionServes(groupByCount("wide"));
  }

  @Test
  void projectionNumericGroupKeyNegativeValuesStraddlingZero() throws Exception {
    assertNumericProjectionServes(groupByCount("neg"));
  }

  @Test
  void projectionNumericGroupKeySparseMissingGroup() throws Exception {
    // Records lacking `sparse` group under the empty key; the stored default 0 is ALSO a real
    // value of this column, so a phantom would merge two groups rather than add one.
    assertNumericProjectionServes(groupByCount("sparse"));
  }

  @Test
  void projectionNumericGroupKeyIntegralDecimalMergesWithLong() throws Exception {
    // 20 and 20.00 are the same xs:integer-representable value: ONE group, keyed xs:integer.
    final String served = assertNumericProjectionServes(groupByCount("decint"));
    assertEquals(3, served.lines().filter(l -> !l.isEmpty()).count(),
        "20/20.00, 21/21.00 and 22/22.00 must merge into three groups, not six: " + served);
  }

  @Test
  void projectionNumericGroupKeyPredicatedSelectivitySweep() throws Exception {
    // Common / mid / rare / none. A wrong-answer bug once hid behind a common literal for a
    // whole session — the error scaled with rarity, not corpus size.
    for (final String bound : new String[] {"-1", "6993", "13900", "20000"}) {
      assertNumericProjectionServes("for $u in " + SRC3 + " where $u.wide gt " + bound + " let $k := $u.small "
          + "group by $k return {\"small\": $k, \"count\": count($u)}");
    }
  }

  @Test
  void projectionNumericGroupKeyRenamedRoutesThroughTheMultiPath() throws Exception {
    // Renamed output field: the single-key composite entry point, which encodes 'l<value>' and
    // decodes it back to xs:integer through the shared typed-group decoder.
    assertNumericProjectionServes(
        "for $u in " + SRC3 + " let $k := $u.small group by $k return {\"g\": $k, \"n\": count($u)}");
  }

  @Test
  void projectionNumericGroupAggregate() throws Exception {
    ensureNumericResource();
    final String query = "for $u in " + SRC3 + " let $k := $u.small group by $k "
        + "return {\"small\": $k, \"n\": count($u), \"total\": sum($u.wide), \"top\": max($u.wide), "
        + "\"lo\": min($u.wide), \"mean\": avg($u.wide)}";
    final long aggBefore = SirixVectorizedExecutor.groupAggServedCount();
    assertNumericProjectionServes(query);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > aggBefore,
        "the per-group aggregate route must have served, not just the count route");
  }

  @Test
  void projectionNumericGroupAggregateWithSparseKeyAndSparseAggregate() throws Exception {
    assertNumericProjectionServes("for $u in " + SRC3 + " let $k := $u.sparse group by $k "
        + "return {\"sparse\": $k, \"n\": count($u), \"total\": sum($u.wide)}");
  }

  @Test
  void projectionNumericGroupKeyDeclinesOnDoubleColumn() throws Exception {
    // NUMERIC_DOUBLE is byte-identical to NUMERIC_LONG on disk but holds the order-preserving
    // transform of the double bits: reading it as a long would emit garbage keys.
    assertNumericProjectionDeclines(groupByCount("dbl"));
  }

  @Test
  void projectionNumericGroupKeyDeclinesOnJsonNullColumn() throws Exception {
    assertNumericProjectionDeclines(groupByCount("hasnull"));
  }

  @Test
  void projectionNumericGroupKeyDeclinesOnMixedIntAndDoubleColumn() throws Exception {
    assertNumericProjectionDeclines(groupByCount("realmix"));
  }

  @Test
  void projectionNumericGroupKeyDeclinesOnBooleanColumn() throws Exception {
    assertNumericProjectionDeclines(groupByCount("flag"));
  }

  @Test
  void projectionNumericGroupKeyDeclinesOnStringSetColumn() throws Exception {
    ensureNumericResource();
    final String query = groupByCount("tags");
    // Grouping by an ARRAY is UNDEFINED in JSONiq — the interpreter raises FOTY0012 — so this
    // shape has no correct answer for a fast path to reproduce, and the differential comparison
    // the other decline tests make is not available here. What is asserted is the gate: the
    // numeric route must not claim a STRING_SET column. The arm itself is pinned at kernel level
    // by ProjectionIndexByteScanTest#numericGroupByRejectsAStringSetColumn.
    //
    // NOT blessed here: the vectorized run ANSWERS this query (the typed slot-walk kernel
    // flattens a one-element array to its element) where the interpreter errors. That divergence
    // predates the numeric group-by route and reproduces with no projection installed at all —
    // it belongs to the typed kernel, not to this gate, so this test deliberately does not
    // compare the two outputs.
    final Exception interpreted =
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> run2On(RES3, query, false));
    assertTrue(causeChain(interpreted).contains("FOTY0012"), causeChain(interpreted));
    final long before = SirixVectorizedExecutor.numericGroupByServedCount();
    run2On(RES3, query, true);
    assertEquals(before, SirixVectorizedExecutor.numericGroupByServedCount(),
        "the numeric group-by route must never claim a STRING_SET column");
  }

  /** Flatten an exception chain to one string — wrappers move around, the code does not. */
  private static String causeChain(final Throwable t) {
    final StringBuilder sb = new StringBuilder();
    for (Throwable c = t; c != null && sb.length() < 4096; c = c.getCause() == c
        ? null
        : c.getCause()) {
      sb.append(c).append(" | ");
    }
    return sb.toString();
  }

  @Test
  void projectionMixedStringAndNumericCompositeDeclines() throws Exception {
    // A composite key mixing a string and a numeric column needs a typed multi-kernel; until
    // then the shape must decline as a whole, never serve the numeric half as a string.
    assertNumericProjectionDeclines("for $u in " + SRC3 + " let $d := $u.dept, $k := $u.small group by $d, $k "
        + "return {\"dept\": $d, \"small\": $k, \"count\": count($u)}");
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
      // Creating the projection commits a new resource revision. Open the serving session only
      // afterwards: a session opened before that separate transaction is revision-pinned to the
      // old catalog and must not be used to construct the executor for the just-committed index.
      ensureSparseProjection(chain, ctx);
      final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
      final var session = db.beginResourceSession(RES);
      SirixVectorizedExecutor exec = null;
      try {
        final int revision = session.getMostRecentRevisionNumber();
        assertTrue(ProjectionIndexCatalog.hasProjections(session,
            session.getResourceConfig().getResource().toString(), revision),
            "fresh serving session must observe the committed sparse projection catalog");
        exec = new SirixVectorizedExecutor(session, revision);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        final Sequence result = new Query(chain, query).execute(ctx);
        final StringWriter out = new StringWriter();
        try (PrintWriter pw = new PrintWriter(out)) {
          new StringSerializer(pw).serialize(result);
        }
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (exec != null)
          exec.close();
      }
    }
  }

  /** Persist the sparse/typed projection through the same catalogued lifecycle used in production. */
  private void ensureSparseProjection(final SirixCompileChain chain, final SirixQueryContext ctx) {
    if (sparseProjectionReady) {
      return;
    }
    new Query(chain, """
        let $doc := jn:doc('typed-gb-db','records.jn')
        let $index := jn:create-projection-index($doc, '/[]',
          ('/[]/dept','/[]/tier','/[]/bonus','/[]/age','/[]/nully','/[]/mixed','/[]/region'),
          ('string','string','long','long','string','string','string'))
        return {"revision": sdb:commit($doc)}
        """).evaluate(ctx);
    sparseProjectionReady = true;
  }

  private String runWithProjection(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
      final var session = db.beginResourceSession(RES);
      SirixVectorizedExecutor exec = null;
      try {
        io.sirix.query.bench.ScaleBenchProjectionSetupAccess.ensureProjection(session);
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
        if (exec != null)
          exec.close();
      }
    }
  }

  // ============ post-group aggregate let + order by (the ClickBench shape) ============

  @Test
  void postGroupLetOrderedDescending() throws Exception {
    // Verbatim ClickBench Q7 shape: pre-group key let, group by, post-group aggregate let,
    // order by that let, return referencing it. Predicated, so the widened detection has to
    // survive the filter-safety gate as well.
    assertOrderedDifferentialServed("""
        for $u in %s
        where $u.amount > 0
        let $k := $u.dept
        group by $k
        let $c := count($u)
        order by $c descending
        return {"dept": $k, "count": $c}""".formatted(SRC));
  }

  @Test
  void notEqualPredicateIsServed() throws Exception {
    // ClickBench writes this filter as `<> 0` — the most common predicate in the set. It used to
    // decline all the way down: brackit's getComparisonOp did not map NE, so the predicate was
    // unrepresentable and no tree reached sirix at all.
    assertOrderedDifferentialServed("""
        for $u in %s
        where $u.amount != 0
        let $k := $u.dept
        group by $k
        let $c := count($u)
        order by $c descending
        return {"dept": $k, "count": $c}""".formatted(SRC));
  }

  @Test
  void notEqualOverASparseFieldExcludesTheMissingRows() throws Exception {
    // The semantic that makes NE its own operator rather than a negated equality. "bonus" is
    // MISSING on ~30% of records; `$u.bonus != 0` must not match those, because the interpreter
    // dereferences a missing field to the empty sequence and `where` reads that as false. A
    // complement of the equality mask would match every one of them.
    assertOrderedDifferentialServedSparse("""
        for $u in %s
        where $u.bonus != 0
        let $k := $u.dept
        group by $k
        let $c := count($u)
        order by $c descending
        return {"dept": $k, "count": $c}""".formatted(SRC));
  }

  // The JSON-null-vs-`!=` divergence is pinned by NotEqualNullGateTest, not here: the wildcard
  // projection does not cover "nully", so a differential over it declines for an unrelated reason
  // and both arms end up interpreted — agreement that proves nothing. Verified by re-introducing
  // the defect and watching this file stay green.

  @Test
  void notEqualOnAStringFieldIsServed() throws Exception {
    assertOrderedDifferentialServed("""
        for $u in %s
        where $u.dept != "Eng"
        let $k := $u.city
        group by $k
        let $c := count($u)
        order by $c descending
        return {"city": $k, "count": $c}""".formatted(SRC));
  }

  @Test
  void stringNotEqualWithTheLiteralOnTheLeftIsServed() throws Exception {
    // The mirrored operand order takes the other arm of extractPredicate. `ne` is symmetric, so it
    // must produce the identical leaf rather than fall through unrepresentable.
    assertOrderedDifferentialServed("""
        for $u in %s
        where "Eng" != $u.dept
        let $k := $u.city
        group by $k
        let $c := count($u)
        order by $c descending
        return {"city": $k, "count": $c}""".formatted(SRC));
  }

  @Test
  void stringNotEqualAgainstTheEmptyStringIsServed() throws Exception {
    // The ClickBench idiom itself: `<> ''`. Distinct from "field is absent", which is the case the
    // kernels must NOT fold into it.
    assertOrderedDifferentialServed("""
        for $u in %s
        where $u.dept != ""
        let $k := $u.city
        group by $k
        let $c := count($u)
        return {"city": $k, "count": $c}""".formatted(SRC));
  }

  @Test
  void stringNotEqualOverASparseFieldExcludesTheMissingRows() throws Exception {
    // "tier" is MISSING on about a third of the records. Those rows must NOT match `!= "gold"`:
    // the deref is the empty sequence and a general comparison over it is false. Complementing an
    // equality mask would match every one of them, which is the whole reason StrNe exists as its
    // own node rather than as Not(StrEq).
    assertOrderedDifferentialServedSparse("""
        for $u in %s
        where $u.tier != "gold"
        let $k := $u.dept
        group by $k
        let $c := count($u)
        order by $c descending
        return {"dept": $k, "count": $c}""".formatted(SRC));
  }

  @Test
  void postGroupLetOrderedAscending() throws Exception {
    assertOrderedDifferentialServed("""
        for $u in %s
        let $k := $u.dept
        group by $k
        let $c := count($u)
        order by $c ascending
        return {"dept": $k, "count": $c}""".formatted(SRC));
  }

  @Test
  void orderedByGroupKeyItself() throws Exception {
    assertOrderedDifferentialServed("""
        for $u in %s
        let $k := $u.dept
        group by $k
        order by $k descending
        return {"dept": $k, "n": count($u)}""".formatted(SRC));
  }

  @Test
  void orderedWithManyTies() throws Exception {
    // The tie case is the whole reason this sorts with Brackit's own stable TupleSort rather
    // than a fresh comparator: grouping on `active` gives two groups, and ordering on a
    // CONSTANT-valued aggregate makes every group tie, so the answer is entirely determined by
    // whether ties keep document first-appearance order.
    assertOrderedDifferentialServed("""
        for $u in %s
        let $k := $u.city
        group by $k
        let $m := max($u.age)
        order by $m descending
        return {"city": $k, "m": $m}""".formatted(SRC));
  }

  @Test
  void orderedOnSparseKeyWithNullGroup() throws Exception {
    // "tier" is MISSING on ~a third of the records, so the null-key group participates in the
    // ordering — the empty-least default has to match, and it is the default on BOTH sides only
    // if the modifier is carried through rather than assumed. Needs the SPARSE projection: the
    // bench wildcard covers only age/active/dept/city/amount/score, so on that one the route
    // would decline for lack of a column and prove nothing.
    assertOrderedDifferentialServedSparse("""
        for $u in %s
        let $k := $u.tier
        group by $k
        let $c := count($u)
        order by $c descending
        return {"tier": $k, "count": $c}""".formatted(SRC));
  }

  @Test
  void orderedOnSparseNumericAggregate() throws Exception {
    // "bonus" is missing on ~30% of records: the aggregate column is sparse while the key is not.
    assertOrderedDifferentialServedSparse("""
        for $u in %s
        let $k := $u.dept
        group by $k
        let $s := sum($u.bonus)
        order by $s descending
        return {"dept": $k, "bonus": $s}""".formatted(SRC));
  }

  @Test
  void orderedTwoSpecs() throws Exception {
    assertOrderedDifferentialServed("""
        for $u in %s
        let $d := $u.dept, $c := $u.city
        group by $d, $c
        let $n := count($u)
        order by $n descending, $d ascending
        return {"dept": $d, "city": $c, "n": $n}""".formatted(SRC));
  }

  @Test
  void preGroupLetVarAsAggregateArgument() throws Exception {
    // `sum($a)` where $a is a PRE-group let bound to $u.amount. After the group-by $a is the
    // grouped sequence of that field, so it denotes exactly sum($u.amount).
    assertOrderedDifferentialServed("""
        for $u in %s
        let $k := $u.dept, $a := $u.amount
        group by $k
        let $s := sum($a)
        order by $s descending
        return {"dept": $k, "total": $s}""".formatted(SRC));
  }

  @Test
  void numericGroupKeyOrderedByCount() throws Exception {
    // The NUMERIC_LONG group key through the widened detection — the combination the
    // ClickBench queries need and neither half delivered alone.
    assertOrderedDifferentialServed("""
        for $u in %s
        let $k := $u.age
        group by $k
        let $c := count($u)
        order by $c descending
        return {"age": $k, "count": $c}""".formatted(SRC));
  }

  // ---- shapes that must DECLINE, and still answer correctly ----

  @Test
  void orderByAnExpressionDeclines() throws Exception {
    assertOrderedDifferential("""
        for $u in %s
        let $k := $u.dept
        group by $k
        order by count($u) * -1
        return {"dept": $k, "n": count($u)}""".formatted(SRC));
  }

  @Test
  void orderByAVariableTheReturnDoesNotEmitDeclines() throws Exception {
    assertOrderedDifferential("""
        for $u in %s
        let $k := $u.dept
        group by $k
        let $c := count($u)
        order by $c descending
        return {"dept": $k}""".formatted(SRC));
  }

  @Test
  void postGroupNonAggregateLetDeclines() throws Exception {
    assertOrderedDifferential("""
        for $u in %s
        let $k := $u.dept
        group by $k
        let $c := 42
        order by $c descending
        return {"dept": $k, "c": $c}""".formatted(SRC));
  }

  // ==================== order-sensitive harness ====================

  /**
   * Differential for {@code order by} shapes. {@link #normalize} SORTS its lines, which is right for
   * a group-by (emission order is implementation-defined) and useless here — it would pass no matter
   * what order the served path produced. This one compares the sequence as emitted.
   *
   * <p>
   * Also asserts the projection ACTUALLY served, so a decline cannot masquerade as agreement: with
   * both arms falling back to the interpreter the comparison is a tautology.
   */
  private void assertOrderedDifferentialServed(final String query) throws Exception {
    final String interpreted = flatten(run(query, false));
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final String served = flatten(runWithProjection(query));
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore,
        "the group-aggregate must be SERVED from the projection, else this asserts nothing: " + query);
    assertEquals(interpreted, served, "ordered projection-served result differs for: " + query);
  }

  /** {@link #assertOrderedDifferentialServed} against the SPARSE/typed wildcard projection. */
  private void assertOrderedDifferentialServedSparse(final String query) throws Exception {
    final String interpreted = flatten(run(query, false));
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final String served = flatten(runWithSparseProjection(query));
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore,
        "the group-aggregate must be SERVED from the sparse projection: " + query);
    assertEquals(interpreted, served, "ordered sparse-projection result differs for: " + query);
  }

  /** Order-sensitive differential WITHOUT requiring a serve — for shapes expected to decline. */
  private void assertOrderedDifferential(final String query) throws Exception {
    final String interpreted = flatten(run(query, false));
    final String vectorized = flatten(runWithProjection(query));
    assertEquals(interpreted, vectorized, "ordered vectorized result differs for: " + query);
  }

  /** One record per line, in EMISSION order — whitespace only, never sorted. */
  private static String flatten(final String s) {
    return s.replace("} {", "}\n{")
            .lines()
            .map(String::strip)
            .filter(l -> !l.isEmpty())
            .reduce("", (a, b) -> a + "\n" + b);
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

  /** Differential harness ({@code run2On}-based) against an arbitrary resource. */
  private void assertDifferentialOn(final String resource, final String query) throws Exception {
    final String interpreted = normalize(run2On(resource, query, false));
    final String vectorized = normalize(run2On(resource, query, true));
    assertEquals(interpreted, vectorized, "vectorized result differs from interpreted for: " + query);
  }

  /** {@link #run2} against an arbitrary resource of the test database. */
  private String run2On(final String resource, final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        // The interpreted arm is this test's ground truth, so it has to STAY interpreted: a chain
        // that auto-wires an executor would compare the fast path against itself and prove nothing.
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
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
        if (exec != null)
          exec.close();
      }
    }
  }

  private String run2(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        // The interpreted arm is this test's ground truth, so it has to STAY interpreted: a chain
        // that auto-wires an executor would compare the fast path against itself and prove nothing.
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
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
        // The interpreted arm is this test's ground truth, so it has to STAY interpreted: a chain
        // that auto-wires an executor would compare the fast path against itself and prove nothing.
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
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
    return s.replace("} {", "}\n{")
            .lines()
            .map(String::strip)
            .filter(l -> !l.isEmpty())
            .sorted()
            .reduce("", (a, b) -> a + "\n" + b);
  }
}
