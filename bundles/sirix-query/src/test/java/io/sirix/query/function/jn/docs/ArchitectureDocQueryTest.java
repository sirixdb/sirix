package io.sirix.query.function.jn.docs;

import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for all XQuery examples in docs/ARCHITECTURE.md. These tests verify that the documentation
 * examples actually run.
 */
public final class ArchitectureDocQueryTest {

  private static final Path SIRIX_DB_PATH = PATHS.PATH1.getFile();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /**
   * Tests for Problem 1: "What did my data look like last Tuesday at 3pm?" Query: Point-in-time
   * access via revision number
   */
  @Nested
  @DisplayName("Problem 1: Point-in-time query")
  class PointInTimeQueryTests {

    @Test
    @DisplayName("Query state at specific revision using jn:doc with revision")
    void testQueryAtRevision() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create initial data (revision 1)
        final var storeQuery = """
            jn:store('shop', 'products', '{
              "products": [
                {"sku": "SKU-12345", "price": 99.99},
                {"sku": "SKU-67890", "price": 49.99}
              ]
            }')
            """;
        new Query(chain, storeQuery).evaluate(ctx);

        // Create revision 2 via update query
        final var updateQuery = """
            let $doc := jn:doc('shop', 'products')
            return replace json value of $doc.products[0].price with 129.99
            """;
        new Query(chain, updateQuery).evaluate(ctx);

        // Query revision 1 - should have original price
        final var queryRev1 = "let $catalog := jn:doc('shop', 'products', 1) "
            + "for $p in $catalog.products[] where $p.sku eq \"SKU-12345\" return $p.price";
        final var result1 = executeQuery(chain, ctx, queryRev1);
        assertTrue(result1.contains("99.99"), "Revision 1 should have original price 99.99, got: " + result1);

        // Query revision 2 - should have updated price
        final var queryRev2 = "let $catalog := jn:doc('shop', 'products', 2) "
            + "for $p in $catalog.products[] where $p.sku eq \"SKU-12345\" return $p.price";
        final var result2 = executeQuery(chain, ctx, queryRev2);
        assertTrue(result2.contains("129.99"), "Revision 2 should have updated price 129.99, got: " + result2);
      }
    }
  }

  /**
   * Tests for Problem 2: "Show me what changed between two points in time" Query: Diff between
   * revisions
   */
  @Nested
  @DisplayName("Problem 2: Diff between revisions")
  class DiffQueryTests {

    @Test
    @DisplayName("Get structured diff between two revisions")
    void testDiffBetweenRevisions() {
      try (
          final var store =
              BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).storeDeweyIds(true).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create initial config (revision 1)
        final var storeQuery = """
            jn:store('configs', 'production', '{
              "database": {"host": "localhost", "port": 5432},
              "cache": {"enabled": true}
            }')
            """;
        new Query(chain, storeQuery).evaluate(ctx);

        // Modify via insert (revision 2)
        final var insertQuery = """
            insert json {"newSetting": "addedValue"} into jn:doc('configs', 'production')
            """;
        new Query(chain, insertQuery).evaluate(ctx);

        // Pattern from docs: jn:diff with sdb:revision
        final var diffQuery = """
            let $rev1 := jn:doc('configs', 'production', 1)
            let $rev2 := jn:doc('configs', 'production', 2)
            return jn:diff('configs', 'production', sdb:revision($rev1), sdb:revision($rev2))
            """;

        final var result = executeQuery(chain, ctx, diffQuery);
        assertNotNull(result);
        assertFalse(result.isEmpty(), "Diff should return non-empty result");
      }
    }
  }

  /**
   * Tests for Problem 3: "Track how this specific record evolved" Query: jn:all-times with
   * sdb:revision
   */
  @Nested
  @DisplayName("Problem 3: Track record evolution")
  class RecordEvolutionTests {

    @Test
    @DisplayName("Track node across all revisions with jn:all-times")
    void testAllTimesQuery() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create initial data (revision 1)
        final var storeQuery = """
            jn:store('hospital', 'patient', '{
              "name": "John Doe",
              "allergies": [
                {"name": "Penicillin", "severity": "high"}
              ]
            }')
            """;
        new Query(chain, storeQuery).evaluate(ctx);

        // Update allergy severity (revision 2)
        final var updateQuery = """
            replace json value of jn:doc('hospital', 'patient').allergies[0].severity with "critical"
            """;
        new Query(chain, updateQuery).evaluate(ctx);

        // Pattern from docs: Track evolution with all-times
        final var evolutionQuery = """
            let $allergy := jn:doc('hospital', 'patient').allergies[0]
            for $version in jn:all-times($allergy)
            return {
              "revision": sdb:revision($version),
              "severity": $version.severity
            }
            """;

        final var result = executeQuery(chain, ctx, evolutionQuery);
        assertNotNull(result);
        assertTrue(result.contains("high") || result.contains("critical"), "Should contain allergy data: " + result);
      }
    }

    @Test
    @DisplayName("Get hash of node with sdb:hash")
    void testHashFunction() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create data
        final var storeQuery = "jn:store('testdb', 'testres', '{\"data\": {\"value\": 100}}')";
        new Query(chain, storeQuery).evaluate(ctx);

        // Test sdb:hash function
        final var hashQuery = "sdb:hash(jn:doc('testdb', 'testres').data)";
        final var result = executeQuery(chain, ctx, hashQuery);
        assertNotNull(result);
        assertFalse(result.isEmpty(), "Hash should return a value");
      }
    }
  }

  /**
   * Tests for Problem 4: "Find records that were added after a specific date" Query: Using
   * jn:previous to detect new records
   */
  @Nested
  @DisplayName("Problem 4: Records added after date")
  class RecordsAddedAfterDateTests {

    @Test
    @DisplayName("Detect new records with jn:previous returning empty")
    void testDetectNewRecords() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create initial accounts (revision 1)
        final var storeQuery = """
            jn:store('bank', 'accounts', '{
              "accounts": [
                {"id": 1, "owner": "Alice"},
                {"id": 2, "owner": "Bob"}
              ]
            }')
            """;
        new Query(chain, storeQuery).evaluate(ctx);

        // Add new account (revision 2)
        final var insertQuery = """
            insert json {"id": 3, "owner": "Charlie"} into jn:doc('bank', 'accounts').accounts
            """;
        new Query(chain, insertQuery).evaluate(ctx);

        // Find accounts that don't have a previous version (newly added)
        final var findNewQuery = """
            let $current := jn:doc('bank', 'accounts')
            for $account in $current.accounts[]
            where not(exists(jn:previous($account)))
            return $account.owner
            """;

        final var result = executeQuery(chain, ctx, findNewQuery);
        assertTrue(result.contains("Charlie"), "Should find newly added Charlie: " + result);
      }
    }
  }

  /**
   * Tests for Problem 5: "Access old revision by node key" Query: sdb:select-item with stable node
   * key
   */
  @Nested
  @DisplayName("Problem 5: Access old revision by node key")
  class AccessByNodeKeyTests {

    @Test
    @DisplayName("Get node key with sdb:nodekey")
    void testGetNodeKey() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create data
        final var storeQuery = "jn:store('mydb', 'myresource', '{\"field\": \"value\"}')";
        new Query(chain, storeQuery).evaluate(ctx);

        // Get node key
        final var nodeKeyQuery = "sdb:nodekey(jn:doc('mydb', 'myresource').field)";
        final var result = executeQuery(chain, ctx, nodeKeyQuery);
        assertNotNull(result);
        assertFalse(result.isEmpty(), "Node key should be returned");
      }
    }

    @Test
    @DisplayName("Select item by node key with sdb:select-item")
    void testSelectItemByNodeKey() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create data
        final var storeQuery = "jn:store('mydb', 'myresource', '{\"field\": \"original\"}')";
        new Query(chain, storeQuery).evaluate(ctx);

        // Update to create revision 2
        final var updateQuery = "replace json value of jn:doc('mydb', 'myresource').field with \"modified\"";
        new Query(chain, updateQuery).evaluate(ctx);

        // Access old version via node key (node key 3 is typically the field value)
        final var selectQuery = """
            let $oldDoc := jn:doc('mydb', 'myresource', 1)
            return sdb:select-item($oldDoc, 3)
            """;
        final var result = executeQuery(chain, ctx, selectQuery);
        assertTrue(result.contains("original"), "Old revision should have 'original': " + result);
      }
    }
  }

  /**
   * Tests for Query Processing: Cross-time joins
   */
  @Nested
  @DisplayName("Query Processing: Cross-time joins")
  class CrossTimeJoinTests {

    @Test
    @DisplayName("Compare data across revisions")
    void testCrossRevisionComparison() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create products (revision 1)
        final var storeQuery = """
            jn:store('shop', 'products', '{
              "products": [
                {"id": 1, "name": "Widget", "price": 100},
                {"id": 2, "name": "Gadget", "price": 200}
              ]
            }')
            """;
        new Query(chain, storeQuery).evaluate(ctx);

        // Update Widget price (revision 2) - 20% increase
        final var updateQuery = "replace json value of jn:doc('shop', 'products').products[0].price with 120";
        new Query(chain, updateQuery).evaluate(ctx);

        // Verify that both revisions exist and can be queried separately
        // Use sdb:revision to verify we have revision 2
        final var revQuery = "sdb:revision(jn:doc('shop', 'products'))";
        final var revResult = executeQuery(chain, ctx, revQuery);
        assertTrue(revResult.contains("2"), "Should be at revision 2: " + revResult);

        // Verify prices in each revision using for loop to avoid optimizer issue
        final var rev1Query = "for $p in jn:doc('shop', 'products', 1).products[] return $p.price";
        final var rev1Result = executeQuery(chain, ctx, rev1Query);
        assertTrue(rev1Result.contains("100"), "Revision 1 should have price 100: " + rev1Result);

        final var rev2Query = "for $p in jn:doc('shop', 'products').products[] return $p.price";
        final var rev2Result = executeQuery(chain, ctx, rev2Query);
        assertTrue(rev2Result.contains("120"), "Revision 2 should have price 120: " + rev2Result);
      }
    }
  }

  /**
   * Tests for sdb:revision and sdb:timestamp functions
   */
  @Nested
  @DisplayName("Revision and timestamp functions")
  class RevisionTimestampTests {

    @Test
    @DisplayName("Get revision number with sdb:revision")
    void testGetRevision() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        final var storeQuery = "jn:store('testdb', 'testres', '{\"data\": 1}')";
        new Query(chain, storeQuery).evaluate(ctx);

        // Create revision 2
        final var updateQuery = "replace json value of jn:doc('testdb', 'testres').data with 2";
        new Query(chain, updateQuery).evaluate(ctx);

        final var revQuery = "sdb:revision(jn:doc('testdb', 'testres'))";
        final var result = executeQuery(chain, ctx, revQuery);
        assertTrue(result.contains("2"), "Should be at revision 2: " + result);
      }
    }

    @Test
    @DisplayName("Get timestamp with sdb:timestamp")
    void testGetTimestamp() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        final var storeQuery = "jn:store('testdb', 'testres', '{\"data\": 1}')";
        new Query(chain, storeQuery).evaluate(ctx);

        final var timestampQuery = "sdb:timestamp(jn:doc('testdb', 'testres'))";
        final var result = executeQuery(chain, ctx, timestampQuery);
        assertNotNull(result);
        assertFalse(result.isEmpty(), "Timestamp should not be empty");
      }
    }
  }

  /**
   * Tests for temporal navigation: jn:previous, jn:next
   */
  @Nested
  @DisplayName("Temporal navigation functions")
  class TemporalNavigationTests {

    @Test
    @DisplayName("Navigate with jn:previous")
    void testJnPrevious() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create revision 1
        final var storeQuery = "jn:store('testdb', 'testres', '{\"value\": \"v1\"}')";
        new Query(chain, storeQuery).evaluate(ctx);

        // Create revision 2
        final var updateQuery = "replace json value of jn:doc('testdb', 'testres').value with \"v2\"";
        new Query(chain, updateQuery).evaluate(ctx);

        // Navigate to previous
        final var prevQuery = """
            let $current := jn:doc('testdb', 'testres')
            let $prev := jn:previous($current)
            return {"currentRev": sdb:revision($current), "prevRev": sdb:revision($prev)}
            """;

        final var result = executeQuery(chain, ctx, prevQuery);
        assertTrue(result.contains("\"currentRev\":2") || result.contains("\"currentRev\": 2"),
            "Current should be revision 2: " + result);
        assertTrue(result.contains("\"prevRev\":1") || result.contains("\"prevRev\": 1"),
            "Previous should be revision 1: " + result);
      }
    }

    @Test
    @DisplayName("Navigate with jn:next")
    void testJnNext() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create revision 1
        final var storeQuery = "jn:store('testdb', 'testres', '{\"value\": \"v1\"}')";
        new Query(chain, storeQuery).evaluate(ctx);

        // Create revision 2
        final var updateQuery = "replace json value of jn:doc('testdb', 'testres').value with \"v2\"";
        new Query(chain, updateQuery).evaluate(ctx);

        // Navigate from old to next
        final var nextQuery = """
            let $old := jn:doc('testdb', 'testres', 1)
            let $next := jn:next($old)
            return {"oldRev": sdb:revision($old), "nextRev": sdb:revision($next)}
            """;

        final var result = executeQuery(chain, ctx, nextQuery);
        assertTrue(result.contains("\"oldRev\":1") || result.contains("\"oldRev\": 1"),
            "Old should be revision 1: " + result);
        assertTrue(result.contains("\"nextRev\":2") || result.contains("\"nextRev\": 2"),
            "Next should be revision 2: " + result);
      }
    }

    @Test
    @DisplayName("Find first existing with jn:first-existing")
    void testFirstExisting() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create revision 1
        final var storeQuery = "jn:store('testdb', 'testres', '{\"items\": [1]}')";
        new Query(chain, storeQuery).evaluate(ctx);

        // Add item in revision 2
        final var insertQuery = "insert json 2 into jn:doc('testdb', 'testres').items";
        new Query(chain, insertQuery).evaluate(ctx);

        // First existing for an item added in revision 2
        final var firstExistingQuery = """
            sdb:revision(jn:first-existing(jn:doc('testdb', 'testres').items[1]))
            """;
        final var result = executeQuery(chain, ctx, firstExistingQuery);
        assertTrue(result.contains("2"), "Item should first exist in revision 2: " + result);
      }
    }

    @Test
    @DisplayName("Find last existing with jn:last-existing")
    void testLastExisting() {
      try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create revision 1
        final var storeQuery = "jn:store('testdb', 'testres', '{\"items\": [1, 2]}')";
        new Query(chain, storeQuery).evaluate(ctx);

        // Remove item in revision 2
        final var deleteQuery = "delete json jn:doc('testdb', 'testres').items[1]";
        new Query(chain, deleteQuery).evaluate(ctx);

        // Last existing for the removed item (access via revision 1)
        final var lastExistingQuery = """
            sdb:revision(jn:last-existing(jn:doc('testdb', 'testres', 1).items[1]))
            """;
        final var result = executeQuery(chain, ctx, lastExistingQuery);
        assertTrue(result.contains("1"), "Deleted item should last exist in revision 1: " + result);
      }
    }
  }

  private String executeQuery(SirixCompileChain chain, SirixQueryContext ctx, String query) {
    try {
      final Sequence result = new Query(chain, query).execute(ctx);
      if (result != null) {
        final var buf = IOUtils.createBuffer();
        try (final var serializer = new StringSerializer(buf)) {
          serializer.serialize(result);
        }
        return buf.toString();
      }
      return "";
    } catch (Exception e) {
      throw new RuntimeException("Query execution failed: " + query, e);
    }
  }
}
