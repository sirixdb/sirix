package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.JsonTestHelper;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link SirixStatisticsProvider} against a real SirixDB database.
 * Verifies that PathSummary-based cardinality estimation and index detection work correctly.
 */
final class SirixStatisticsProviderTest {

  private static final String DB_NAME = "json-path1";
  private static final String RESOURCE_NAME = "mydoc.jn";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void pathCardinalityReflectsActualNodeCount() {
    // Store JSON with 3 objects each having a "name" field
    storeAndCommit("""
        jn:store('json-path1','mydoc.jn',
          '[{"name":"Alice","age":30},{"name":"Bob","age":25},{"name":"Charlie","age":35}]')
        """);

    try (final var store = newStore();
         final var statsProvider = new SirixStatisticsProvider(store)) {

      // PathSummary for [{"name":"..."}] stores path as: childArray() / childObjectField("name")
      final var namePath = new Path<QNm>();
      namePath.childArray().childObjectField(new QNm("name"));
      final long nameCardinality = statsProvider.getPathCardinality(
          namePath, DB_NAME, RESOURCE_NAME, -1);
      assertEquals(3L, nameCardinality, "Expected 3 nodes at path /[]/name");

      // Path /[]/age should match 3 nodes
      final var agePath = new Path<QNm>();
      agePath.childArray().childObjectField(new QNm("age"));
      final long ageCardinality = statsProvider.getPathCardinality(
          agePath, DB_NAME, RESOURCE_NAME, -1);
      assertEquals(3L, ageCardinality, "Expected 3 nodes at path /[]/age");
    }
  }

  @Test
  void totalNodeCountReflectsDocumentSize() {
    storeAndCommit("""
        jn:store('json-path1','mydoc.jn',
          '[{"name":"Alice","age":30},{"name":"Bob","age":25}]')
        """);

    try (final var store = newStore();
         final var statsProvider = new SirixStatisticsProvider(store)) {

      final long totalNodes = statsProvider.getTotalNodeCount(DB_NAME, RESOURCE_NAME, -1);
      // Array(1) + Object(2) + name(2) + "Alice"/"Bob"(2) + age(2) + 30/25(2) = 11
      assertTrue(totalNodes > 0, "Total node count should be positive, got " + totalNodes);
      assertTrue(totalNodes >= 10, "Should have at least 10 nodes for 2 objects with 2 fields each");
    }
  }

  @Test
  void indexInfoDetectsCASIndex() {
    // Store data, then create a CAS index on /[]/age
    storeAndCommit("""
        jn:store('json-path1','mydoc.jn',
          '[{"name":"Alice","age":30},{"name":"Bob","age":25}]')
        """);
    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/age')
        return {"revision": sdb:commit($doc)}
        """);

    try (final var store = newStore();
         final var statsProvider = new SirixStatisticsProvider(store)) {

      // Build path matching the CAS index definition: /[]/age
      final var agePath = new Path<QNm>();
      agePath.childArray().childObjectField(new QNm("age"));

      final IndexInfo indexInfo = statsProvider.getIndexInfo(
          agePath, DB_NAME, RESOURCE_NAME, -1);

      assertTrue(indexInfo.exists(), "CAS index on /[]/age should be detected");
      assertEquals(IndexType.CAS, indexInfo.type(), "Index type should be CAS");
      assertTrue(indexInfo.indexId() >= 0, "Index ID should be non-negative");
    }
  }

  @Test
  void indexInfoReturnsNoIndexWhenNoneCreated() {
    storeAndCommit("""
        jn:store('json-path1','mydoc.jn', '{"name":"Alice","age":30}')
        """);

    try (final var store = newStore();
         final var statsProvider = new SirixStatisticsProvider(store)) {

      final var agePath = new Path<QNm>();
      agePath.childObjectField(new QNm("age"));

      final IndexInfo indexInfo = statsProvider.getIndexInfo(
          agePath, DB_NAME, RESOURCE_NAME, -1);

      assertFalse(indexInfo.exists(), "No index should be found when none is created");
      assertEquals(IndexType.NONE, indexInfo.type());
    }
  }

  @Test
  void baseProfileCombinesCardinalityAndIndexInfo() {
    storeAndCommit("""
        jn:store('json-path1','mydoc.jn',
          '[{"price":10},{"price":20},{"price":30},{"price":40},{"price":50}]')
        """);
    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/price')
        return {"revision": sdb:commit($doc)}
        """);

    try (final var store = newStore();
         final var statsProvider = new SirixStatisticsProvider(store)) {

      final var pricePath = new Path<QNm>();
      pricePath.childArray().childObjectField(new QNm("price"));

      final BaseProfile profile = statsProvider.buildBaseProfile(
          pricePath, DB_NAME, RESOURCE_NAME, -1);

      assertEquals(5L, profile.nodeCount(), "5 price nodes expected");
      assertTrue(profile.pathLevel() > 0, "Path level should be > 0");
      assertTrue(profile.hasIndex(), "CAS index should be detected");
      assertEquals(IndexType.CAS, profile.indexType());
    }
  }

  @Test
  void costModelPrefersIndexForSelectivePath() {
    // Create a document with many nodes and an index on a selective path
    storeAndCommit("""
        jn:store('json-path1','mydoc.jn',
          '[{"a":1,"b":2,"c":3,"d":4,"e":5},{"a":6,"b":7,"c":8,"d":9,"e":10},
            {"a":11,"b":12,"c":13,"d":14,"e":15},{"a":16,"b":17,"c":18,"d":19,"e":20}]')
        """);
    storeAndCommit("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/a')
        return {"revision": sdb:commit($doc)}
        """);

    try (final var store = newStore();
         final var statsProvider = new SirixStatisticsProvider(store)) {

      final var aPath = new Path<QNm>();
      aPath.childArray().childObjectField(new QNm("a"));

      final long pathCard = statsProvider.getPathCardinality(aPath, DB_NAME, RESOURCE_NAME, -1);
      final long totalNodes = statsProvider.getTotalNodeCount(DB_NAME, RESOURCE_NAME, -1);

      assertTrue(pathCard > 0, "Path cardinality should be positive");
      assertTrue(totalNodes > pathCard,
          "Total nodes (" + totalNodes + ") should exceed path cardinality (" + pathCard + ")");

      // Verify statistics correctness: 4 objects with "a" field → pathCard = 4
      assertEquals(4L, pathCard, "Expected 4 'a' nodes");

      // The test document is small (~45 nodes, 1 page), so the B-tree overhead
      // makes index scan more expensive than a single-page sequential scan.
      // Verify the cost model correctly reflects this:
      final var costModel = new JsonCostModel();
      final double seqCost = costModel.estimateSequentialScanCost(totalNodes);
      final double idxCost = costModel.estimateIndexScanCost(pathCard);
      assertTrue(seqCost > 0, "Sequential scan cost should be positive");
      assertTrue(idxCost > 0, "Index scan cost should be positive");
      assertFalse(costModel.isIndexScanCheaper(idxCost, seqCost),
          "For tiny documents, B-tree overhead should make index scan more expensive");

      // Now verify with the SAME selectivity ratio (4/45 ≈ 9%) but at realistic scale,
      // the index wins. Scale both counts by 1000x → ~45K total, ~4K matching.
      final double scaledSeqCost = costModel.estimateSequentialScanCost(totalNodes * 1000L);
      final double scaledIdxCost = costModel.estimateIndexScanCost(pathCard * 1000L);
      assertTrue(costModel.isIndexScanCheaper(scaledIdxCost, scaledSeqCost),
          "At realistic scale, index scan (cost=" + scaledIdxCost
              + ") should be cheaper than seq scan (cost=" + scaledSeqCost + ")");
    }
  }

  @Test
  void cacheReturnsSameResultOnSecondCall() {
    storeAndCommit("""
        jn:store('json-path1','mydoc.jn', '[{"x":1},{"x":2},{"x":3}]')
        """);

    try (final var store = newStore();
         final var statsProvider = new SirixStatisticsProvider(store)) {

      final var xPath = new Path<QNm>();
      xPath.childArray().childObjectField(new QNm("x"));

      final long firstCall = statsProvider.getPathCardinality(xPath, DB_NAME, RESOURCE_NAME, -1);
      final long secondCall = statsProvider.getPathCardinality(xPath, DB_NAME, RESOURCE_NAME, -1);

      assertEquals(firstCall, secondCall, "Cached result should match first call");
      assertEquals(3L, firstCall, "Expected 3 nodes at path /[]/x");
    }
  }

  private void storeAndCommit(String query) {
    try (final var store = newStore();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, query).evaluate(ctx);
    }
  }

  private BasicJsonDBStore newStore() {
    return BasicJsonDBStore.newBuilder()
        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
        .build();
  }
}
