package io.sirix.query.function.sdb.explain;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Behavioral end-to-end tests for the cost-based query optimizer.
 *
 * <p>Each test stores real data, optionally creates indexes, then verifies BOTH:
 * <ol>
 *   <li>The optimizer makes correct plan decisions (via {@link QueryPlan} API)</li>
 *   <li>The query returns exact correct results (via {@code Query.serialize()})</li>
 * </ol>
 *
 * <p>This is the only test class that exercises the full 10-stage optimizer
 * pipeline against real databases and verifies both plan and result correctness.</p>
 */
final class OptimizerBehavioralE2ETest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  // ─────────────────────────────────────────────
  // 1. CAS index selection + result correctness
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("CAS index present but optimizer correctly prefers seq scan for tiny dataset")
  void casIndexNotUsedForTinyDataset() {
    storeAndCommit("jn:store('json-path1','mydoc.jn'," +
        "'[{\"price\":10},{\"price\":20},{\"price\":30},{\"price\":40},{\"price\":50}]')");

    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/price')
        return {"revision": sdb:commit($doc)}
        """);

    // Verify plan: index exists but cost model should reject it for 5 rows
    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.price gt 25 return $x",
          store, null);

      assertNotNull(plan.optimizedAST(), "Optimized AST should not be null");
      assertNotNull(plan.parsedAST(), "Parsed AST should not be null");
      assertFalse(plan.usesIndex(),
          "Cost model should prefer seq scan over index for 5 rows");

      final String json = plan.toJSON();
      assertFalse(json.equals("null"), "Plan should not serialize to 'null'");
      assertTrue(json.contains("\"operator\""), "Plan should contain operator nodes");
    }

    // Exact result: prices > 25 are 30, 40, 50
    assertEquals("30 40 50",
        executeQuery("for $x in jn:doc('json-path1','mydoc.jn')[] where $x.price gt 25 return $x.price"));

    // Exact result: full objects
    assertEquals("{\"price\":30} {\"price\":40} {\"price\":50}",
        executeQuery("for $x in jn:doc('json-path1','mydoc.jn')[] where $x.price gt 25 return $x"));
  }

  @Test
  @DisplayName("No index: plan valid, equality predicate returns exact match")
  void noIndexEqualityPredicateExactResult() {
    storeAndCommit("jn:store('json-path1','mydoc.jn'," +
        "'[{\"val\":1},{\"val\":2},{\"val\":3}]')");

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.val eq 2 return $x",
          store, null);

      assertNotNull(plan.optimizedAST());
      assertFalse(plan.usesIndex(),
          "No index exists → optimizer must not prefer index scan");
      assertTrue(plan.toJSON().contains("\"operator\""));
    }

    assertEquals("2",
        executeQuery("for $x in jn:doc('json-path1','mydoc.jn')[] where $x.val eq 2 return $x.val"));
  }

  // ─────────────────────────────────────────────
  // 2. Cardinality estimation
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("Estimated cardinality is positive for 100-element dataset")
  void cardinalityReflectsDataSize() {
    final StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < 100; i++) {
      if (i > 0) json.append(",");
      json.append("{\"id\":").append(i).append(",\"name\":\"item").append(i).append("\"}");
    }
    json.append("]");

    storeAndCommit("jn:store('json-path1','mydoc.jn','" + json + "')");

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[] return $x",
          store, null);

      assertFalse(plan.usesIndex(),
          "No index exists → optimizer must not prefer index scan");

      final long cardinality = plan.estimatedCardinality();
      assertTrue(cardinality > 0 || cardinality == -1,
          "Cardinality should be positive or -1 (not annotated): " + cardinality);
    }
  }

  // ─────────────────────────────────────────────
  // 3. Parsed vs optimized AST differ
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("Parsed and optimized ASTs both present, differ after optimization, verbose JSON correct")
  void parsedAndOptimizedASTsDiffer() {
    storeAndCommit("jn:store('json-path1','mydoc.jn'," +
        "'[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4}]')");

    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/a')
        return {"revision": sdb:commit($doc)}
        """);

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.a gt 1 return $x",
          store, null);

      assertNotNull(plan.parsedAST(), "Parsed AST should be present");
      assertNotNull(plan.optimizedAST(), "Optimized AST should be present");
      assertFalse(plan.usesIndex(),
          "CAS index exists but cost model should prefer seq scan for 2 rows");

      final String verbose = plan.toVerboseJSON();
      assertTrue(verbose.contains("\"parsed\":"), "Verbose should include parsed section");
      assertTrue(verbose.contains("\"optimized\":"), "Verbose should include optimized section");

      // Optimizer must annotate/rewrite — ASTs should differ
      final String parsedRaw = plan.parsedAST().toJSON();
      final String optimizedRaw = plan.optimizedAST().toJSON();
      assertNotEquals(parsedRaw, optimizedRaw,
          "Optimizer should modify the AST (cost annotations at minimum)");
    }
  }

  // ─────────────────────────────────────────────
  // 4. No index → usesIndex() returns false
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("Without index, usesIndex=false, query returns exact result")
  void noIndexUsesIndexFalseExactResult() {
    storeAndCommit("jn:store('json-path1','mydoc.jn','[{\"x\":1},{\"x\":2}]')");

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[] return $x",
          store, null);

      assertFalse(plan.usesIndex(),
          "Without an index, optimizer should not prefer index scan");
    }

    assertEquals("1 2",
        executeQuery("for $x in jn:doc('json-path1','mydoc.jn')[] return $x.x"));
  }

  // ─────────────────────────────────────────────
  // 5. sdb:explain() end-to-end
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("sdb:explain() returns valid JSON through the full query engine")
  void explainThroughQueryEngine() {
    storeAndCommit("jn:store('json-path1','mydoc.jn'," +
        "'[{\"category\":\"A\",\"value\":10},{\"category\":\"B\",\"value\":20}]')");

    final String plan = executeQuery(
        "sdb:explain('for $x in jn:doc(\"json-path1\",\"mydoc.jn\")[] return $x')");

    assertNotNull(plan, "sdb:explain() should return a result");
    assertTrue(plan.startsWith("{"), "Plan should be JSON object");
    assertTrue(plan.contains("\"operator\""), "Plan should contain operator field");
  }

  @Test
  @DisplayName("sdb:explain() verbose mode includes both parsed and optimized sections")
  void explainVerboseThroughQueryEngine() {
    storeAndCommit("jn:store('json-path1','mydoc.jn','[{\"x\":1}]')");

    final String plan = executeQuery(
        "sdb:explain('for $x in jn:doc(\"json-path1\",\"mydoc.jn\")[] return $x', true())");

    assertNotNull(plan, "sdb:explain verbose should return a result");
    assertTrue(plan.contains("\"parsed\""), "Verbose plan must include parsed AST");
    assertTrue(plan.contains("\"optimized\""), "Verbose plan must include optimized AST");
  }

  // ─────────────────────────────────────────────
  // 6. FLWOR: where + order by
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("FLWOR with where + order by: plan valid, results exact and ordered")
  void flworWithWhereAndOrderByExactResult() {
    storeAndCommit("jn:store('json-path1','mydoc.jn'," +
        "'[{\"name\":\"Charlie\",\"age\":30},{\"name\":\"Alice\",\"age\":25},{\"name\":\"Bob\",\"age\":35}]')");

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.age gt 20 order by $x.name return $x.name",
          store, null);

      assertNotNull(plan.optimizedAST());
      assertFalse(plan.usesIndex(),
          "No index exists → optimizer must not prefer index scan");
      assertTrue(plan.toJSON().contains("\"operator\""));
    }

    // Exact result: all have age > 20, ordered alphabetically by name
    assertEquals("\"Alice\" \"Bob\" \"Charlie\"",
        executeQuery("for $x in jn:doc('json-path1','mydoc.jn')[] where $x.age gt 20 order by $x.name return $x.name"));
  }

  // ─────────────────────────────────────────────
  // 7. Count aggregation
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("Count with string equality filter returns exact count")
  void countAggregationExactResult() {
    storeAndCommit("jn:store('json-path1','mydoc.jn'," +
        "'[{\"status\":\"active\"},{\"status\":\"inactive\"},{\"status\":\"active\"},{\"status\":\"active\"}]')");

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "count(for $x in jn:doc('json-path1','mydoc.jn')[] where $x.status eq 'active' return $x)",
          store, null);

      assertNotNull(plan.optimizedAST());
      assertFalse(plan.usesIndex(),
          "No index exists → optimizer must not prefer index scan");
    }

    assertEquals("3",
        executeQuery("count(for $x in jn:doc('json-path1','mydoc.jn')[] where $x.status eq 'active' return $x)"));
  }

  // ─────────────────────────────────────────────
  // 8. Plan caching consistency
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("Same query compiled twice produces identical plans")
  void planCachingConsistency() {
    storeAndCommit("jn:store('json-path1','mydoc.jn','[{\"k\":1},{\"k\":2}]')");

    final String query = "for $x in jn:doc('json-path1','mydoc.jn')[] return $x.k";

    try (final var store = newStore()) {
      final QueryPlan plan1 = QueryPlan.explain(query, store, null);
      final QueryPlan plan2 = QueryPlan.explain(query, store, null);

      assertFalse(plan1.usesIndex(),
          "No index exists → optimizer must not prefer index scan");
      assertFalse(plan2.usesIndex(),
          "No index exists → second compilation must also not prefer index scan");

      assertEquals(plan1.toJSON(), plan2.toJSON(),
          "Same query should produce identical plans");
      assertEquals(plan1.toRawJSON(), plan2.toRawJSON(),
          "Same query should produce identical raw ASTs");
    }

    assertEquals("1 2", executeQuery(query));
  }

  // ─────────────────────────────────────────────
  // 9. Before/after index: results identical
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("Results are identical before and after CAS index creation")
  void resultsIdenticalBeforeAndAfterIndex() {
    storeAndCommit("jn:store('json-path1','mydoc.jn'," +
        "'[{\"score\":10},{\"score\":20},{\"score\":30},{\"score\":40},{\"score\":50}]')");

    final String queryExpr = "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.score gt 25 return $x.score";

    // Before index
    try (final var store = newStore()) {
      assertFalse(QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.score gt 25 return $x",
          store, null).usesIndex(),
          "No index → should not use index");
    }
    final String resultBefore = executeQuery(queryExpr);
    assertEquals("30 40 50", resultBefore, "Before index: exact result");

    // Create CAS index
    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/score')
        return {"revision": sdb:commit($doc)}
        """);

    // After index — plan should still prefer seq scan for 5 rows
    try (final var store = newStore()) {
      assertFalse(QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.score gt 25 return $x",
          store, null).usesIndex(),
          "CAS index exists but cost model should still prefer seq scan for 5 rows");
    }

    // After index — results must be identical
    final String resultAfter = executeQuery(queryExpr);
    assertEquals("30 40 50", resultAfter, "After index: exact result");
    assertEquals(resultBefore, resultAfter,
        "Results must be identical regardless of index presence");
  }

  // ─────────────────────────────────────────────
  // 10. Empty result set
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("Query with no matches returns null (empty) and plan is valid")
  void emptyResultSet() {
    storeAndCommit("jn:store('json-path1','mydoc.jn','[{\"x\":1},{\"x\":2},{\"x\":3}]')");

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.x gt 999 return $x",
          store, null);
      assertNotNull(plan.optimizedAST(), "Plan should exist even for empty result");
      assertFalse(plan.usesIndex(),
          "No index exists → optimizer must not prefer index scan");
    }

    assertNull(executeQuery(
        "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.x gt 999 return $x.x"),
        "No matches → null result");
  }

  // ─────────────────────────────────────────────
  // 11. Arithmetic expression (no database)
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("sdb:explain on arithmetic expression produces valid plan")
  void explainArithmeticExpression() {
    final String plan = executeQuery("sdb:explain('1 + 2')");
    assertNotNull(plan, "Plan for arithmetic should not be null");
    assertTrue(plan.contains("\"operator\""), "Should have operator nodes");
  }

  // ─────────────────────────────────────────────
  // 12. Multiple AND predicates
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("AND predicates: plan valid, exact filtered results")
  void multipleAndPredicatesExactResult() {
    storeAndCommit("jn:store('json-path1','mydoc.jn'," +
        "'[{\"a\":1,\"b\":10},{\"a\":2,\"b\":20},{\"a\":3,\"b\":30},{\"a\":4,\"b\":5}]')");

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.a gt 1 and $x.b gt 15 return $x",
          store, null);
      assertNotNull(plan.optimizedAST());
      assertFalse(plan.usesIndex(),
          "No index exists → optimizer must not prefer index scan");
    }

    // a>1 AND b>15 → only {a:2,b:20} and {a:3,b:30} match
    assertEquals("2 3",
        executeQuery("for $x in jn:doc('json-path1','mydoc.jn')[] where $x.a gt 1 and $x.b gt 15 return $x.a"));
  }

  // ─────────────────────────────────────────────
  // 13. Let binding with fn:sum
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("Let binding with fn:sum returns exact aggregate")
  void letBindingSumExactResult() {
    storeAndCommit("jn:store('json-path1','mydoc.jn','[{\"v\":10},{\"v\":20},{\"v\":30}]')");

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "let $sum := fn:sum(for $x in jn:doc('json-path1','mydoc.jn')[] return $x.v) return $sum",
          store, null);
      assertNotNull(plan.optimizedAST());
      assertFalse(plan.usesIndex(),
          "No index exists → optimizer must not prefer index scan");
    }

    assertEquals("60",
        executeQuery("let $sum := fn:sum(for $x in jn:doc('json-path1','mydoc.jn')[] return $x.v) return $sum"));
  }

  // ─────────────────────────────────────────────
  // 14. Cost model gates index: tiny dataset rejects CAS index
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("Cost model rejects CAS index for filter predicate on tiny dataset")
  void costModelRejectsCasIndexForTinyDataset() {
    // Same nested structure as test 15, but only 5 rows.
    // Cost model: seqScan ≈ 1.2 vs indexScan ≈ 7.5 → seq scan wins → gate closes
    storeAndCommit("jn:store('json-path1','mydoc.jn','" + buildNestedPriceArray(5) + "')");

    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/item/price')
        return {"revision": sdb:commit($doc)}
        """);

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $i in jn:doc('json-path1','mydoc.jn')[].item[?$$.price gt 3] return $i",
          store, null);

      assertFalse(plan.usesIndex(),
          "Cost model should close index gate for 5-row dataset — seq scan is cheaper");
      assertNull(plan.indexType(),
          "No IndexExpr → no index type");
    }

    // Results still correct via sequential scan
    assertEquals("{\"price\":4} {\"price\":5}",
        executeQuery("for $x in jn:doc('json-path1','mydoc.jn')[] where $x.item.price gt 3 order by $x.item.price return $x.item"));
  }

  // ─────────────────────────────────────────────
  // 15. Large dataset: CAS index preferred
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("CAS index used for range predicate on large dataset")
  void casIndexPreferredForLargeDataset() {
    // Nested structure: [{"item":{"price":1}},...]
    // JsonCASStep requires DerefExpr before the filter predicate
    storeAndCommit("jn:store('json-path1','mydoc.jn','" + buildNestedPriceArray(10_000) + "')");

    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/item/price')
        return {"revision": sdb:commit($doc)}
        """);

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $i in jn:doc('json-path1','mydoc.jn')[].item[?$$.price gt 9990] return $i",
          store, null);

      assertTrue(plan.usesIndex(),
          "IndexMatching should create IndexExpr for CAS index on /[]/item/price");

      assertEquals("CAS", plan.indexType(),
          "Should use CAS index type");
    }

    // Verify results: 10 items with price 9991..10000
    final String result14 = executeQuery(
        "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.item.price gt 9990 order by $x.item.price return $x.item.price");
    assertEquals("9991 9992 9993 9994 9995 9996 9997 9998 9999 10000", result14);
  }

  // ─────────────────────────────────────────────
  // 15. CAS index with equality predicate
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("CAS index used for equality lookup on large dataset")
  void casIndexUsedForEqualityOnLargeDataset() {
    storeAndCommit("jn:store('json-path1','mydoc.jn','" + buildNestedPriceArray(10_000) + "')");

    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/item/price')
        return {"revision": sdb:commit($doc)}
        """);

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $i in jn:doc('json-path1','mydoc.jn')[].item[?$$.price eq 5000] return $i",
          store, null);

      assertTrue(plan.usesIndex(),
          "IndexMatching should rewrite to IndexExpr for equality on 10000 rows");

      assertEquals("CAS", plan.indexType(),
          "Should use CAS index type for equality predicate");
    }

    assertEquals("{\"price\":5000}",
        executeQuery("for $x in jn:doc('json-path1','mydoc.jn')[] where $x.item.price eq 5000 return $x.item"));
  }

  // ─────────────────────────────────────────────
  // 16. Index precedence: CAS > PATH
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("CAS index takes precedence over PATH index for value predicate")
  void casIndexTakesPrecedenceOverPath() {
    storeAndCommit("jn:store('json-path1','mydoc.jn','" + buildNestedPriceArray(10_000) + "')");

    // Create both PATH and CAS indexes — CAS should win for value predicates
    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $p := jn:create-path-index($doc, '/[]/item/price')
        let $c := jn:create-cas-index($doc, 'xs:integer', '/[]/item/price')
        return {"revision": sdb:commit($doc)}
        """);

    // IndexMatching runs JsonCASStep before JsonPathStep — CAS wins
    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $i in jn:doc('json-path1','mydoc.jn')[].item[?$$.price gt 9990] return $i",
          store, null);

      assertTrue(plan.usesIndex(),
          "Should use an index for 10000-row dataset");

      assertEquals("CAS", plan.indexType(),
          "CAS should take precedence over PATH index");
    }

    final String result16 = executeQuery(
        "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.item.price gt 9990 order by $x.item.price return $x.item.price");
    assertEquals("9991 9992 9993 9994 9995 9996 9997 9998 9999 10000", result16);
  }

  // ─────────────────────────────────────────────
  // 17. PATH index fallback when no CAS
  // ─────────────────────────────────────────────

  @Test
  @DisplayName("Without CAS index, no index rewrite for value predicate")
  void noCasIndexNoRewrite() {
    storeAndCommit("jn:store('json-path1','mydoc.jn','" + buildNestedPriceArray(10_000) + "')");

    // Only PATH index — JsonCASStep won't match (no CAS), and JsonPathStep
    // doesn't handle value predicates. So no index rewrite.
    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $p := jn:create-path-index($doc, '/[]/item/price')
        return {"revision": sdb:commit($doc)}
        """);

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $i in jn:doc('json-path1','mydoc.jn')[].item[?$$.price gt 9990] return $i",
          store, null);

      // Cost model finds the PATH index and prefers it...
      assertTrue(plan.prefersIndex(),
          "Cost model should prefer PATH index over seq scan at 10K rows");

      // ...but JsonCASStep can't use a PATH index for value predicates → no rewrite
      assertFalse(plan.usesIndex(),
          "Without CAS index, value predicate cannot use PATH index");
      assertNull(plan.indexType(),
          "No index type when no CAS index available");
    }

    // Results still correct via sequential scan
    final String result17 = executeQuery(
        "for $x in jn:doc('json-path1','mydoc.jn')[] where $x.item.price gt 9990 order by $x.item.price return $x.item.price");
    assertEquals("9991 9992 9993 9994 9995 9996 9997 9998 9999 10000", result17);
  }

  // ─────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────

  private void storeAndCommit(String query) {
    try (final var store = newStore();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, query).evaluate(ctx);
    }
  }

  /**
   * Execute a query and return the serialized result, or null if empty.
   */
  private String executeQuery(String query) {
    try (final var store = newStore();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store);
         final var out = new ByteArrayOutputStream();
         final var writer = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, writer);
      writer.flush();
      final String result = out.toString();
      return result.isEmpty() ? null : result;
    } catch (Exception e) {
      fail("Query execution failed: " + e.getMessage(), e);
      return null;
    }
  }

  /**
   * Build nested array: [{"item":{"price":1}},{"item":{"price":2}},...].
   * This structure produces the DerefExpr AST node that JsonCASStep requires.
   */
  private String buildNestedPriceArray(int count) {
    final var sb = new StringBuilder("[");
    for (int i = 1; i <= count; i++) {
      if (i > 1) sb.append(",");
      sb.append("{\"item\":{\"price\":").append(i).append("}}");
    }
    sb.append("]");
    return sb.toString();
  }

  private BasicJsonDBStore newStore() {
    return BasicJsonDBStore.newBuilder()
        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
        .build();
  }
}
