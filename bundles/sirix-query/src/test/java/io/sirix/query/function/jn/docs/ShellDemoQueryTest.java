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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end test that executes all queries from the sirix.io terminal demo
 * sequentially, validating each step. This mirrors the "See it in action"
 * shell demo on the website: store → update → append → delete → time travel → item-history.
 */
public final class ShellDemoQueryTest {

  private static final Path SIRIX_DB_PATH = PATHS.PATH1.getFile();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("Execute all shell demo queries sequentially")
  void testShellDemoQueriesSequentially() {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      // === Rev 1 — store initial data ===
      final var storeQuery = "jn:store('json-path1','products','[{\"name\":\"Laptop\",\"price\":999},{\"name\":\"Phone\",\"price\":699}]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Verify Rev 1 content
      final var rev1Check = serializeQuery(chain, ctx, "jn:doc('json-path1','products')");
      assertTrue(rev1Check.contains("Laptop"), "Rev 1 should contain Laptop: " + rev1Check);
      assertTrue(rev1Check.contains("999"), "Rev 1 should contain price 999: " + rev1Check);
      assertTrue(rev1Check.contains("Phone"), "Rev 1 should contain Phone: " + rev1Check);
      assertTrue(rev1Check.contains("699"), "Rev 1 should contain price 699: " + rev1Check);

      // === Rev 2 — price drop on the laptop ===
      final var updateQuery = """
          let $doc := jn:doc('json-path1','products')
          return replace json value of $doc[0].price with 899""";
      new Query(chain, updateQuery).evaluate(ctx);

      // Verify Rev 2 content — laptop price changed
      final var rev2Check = serializeQuery(chain, ctx, "jn:doc('json-path1','products')");
      assertTrue(rev2Check.contains("899"), "Rev 2 should contain updated price 899: " + rev2Check);
      assertTrue(rev2Check.contains("Laptop"), "Rev 2 should still contain Laptop: " + rev2Check);
      assertTrue(rev2Check.contains("Phone"), "Rev 2 should still contain Phone: " + rev2Check);

      // === Rev 3 — add a new product ===
      final var appendQuery = """
          let $doc := jn:doc('json-path1','products')
          return append json {"name":"Tablet","price":449} into $doc""";
      new Query(chain, appendQuery).evaluate(ctx);

      // Verify Rev 3 content — tablet added
      final var rev3Check = serializeQuery(chain, ctx, "jn:doc('json-path1','products')");
      assertTrue(rev3Check.contains("Tablet"), "Rev 3 should contain Tablet: " + rev3Check);
      assertTrue(rev3Check.contains("449"), "Rev 3 should contain price 449: " + rev3Check);

      // === Rev 4 — discontinue the phone ===
      final var deleteQuery = """
          let $doc := jn:doc('json-path1','products')
          return delete json $doc[1]""";
      new Query(chain, deleteQuery).evaluate(ctx);

      // Verify Rev 4 content — phone removed
      final var rev4Check = serializeQuery(chain, ctx, "jn:doc('json-path1','products')");
      assertFalse(rev4Check.contains("Phone"), "Rev 4 should not contain Phone: " + rev4Check);
      assertTrue(rev4Check.contains("Laptop"), "Rev 4 should still contain Laptop: " + rev4Check);
      assertTrue(rev4Check.contains("Tablet"), "Rev 4 should still contain Tablet: " + rev4Check);

      // === Time travel — what was the original catalog? ===
      final var timeTravelQuery = "jn:doc('json-path1','products', 1)";
      final var timeTravelResult = serializeQuery(chain, ctx, timeTravelQuery);
      assertTrue(timeTravelResult.contains("Laptop"), "Rev 1 time travel should contain Laptop: " + timeTravelResult);
      assertTrue(timeTravelResult.contains("999"), "Rev 1 time travel should have original price 999: " + timeTravelResult);
      assertTrue(timeTravelResult.contains("Phone"), "Rev 1 time travel should contain Phone: " + timeTravelResult);
      assertTrue(timeTravelResult.contains("699"), "Rev 1 time travel should have original price 699: " + timeTravelResult);

      // === Discover the node key for the laptop price value ===
      // We need to find the right node key for item-history tracking.
      // First, find what node key the laptop object has:
      final var laptopNodeKeyQuery = "sdb:nodekey(jn:doc('json-path1','products')[0])";
      final var laptopNodeKey = serializeQuery(chain, ctx, laptopNodeKeyQuery);
      assertFalse(laptopNodeKey.isEmpty(), "Laptop object node key should not be empty");

      // Find the node key for the laptop price:
      final var priceNodeKeyQuery = "sdb:nodekey(jn:doc('json-path1','products')[0].price)";
      final var priceNodeKey = serializeQuery(chain, ctx, priceNodeKeyQuery);
      assertFalse(priceNodeKey.isEmpty(), "Price node key should not be empty");

      // === Track how the laptop price evolved across revisions ===
      // Use the discovered node key for item-history
      final var itemHistoryQuery = String.format("""
          let $item := sdb:select-item(jn:doc('json-path1','products'), %s)
          for $v in sdb:item-history($item)
          return {"rev": sdb:revision($v), "price": $v}""", priceNodeKey);
      final var historyResult = serializeQuery(chain, ctx, itemHistoryQuery);
      // The price node was modified in rev 2 (999 → 899), so history should show both revisions
      assertTrue(historyResult.contains("999"), "Item history should contain original price 999: " + historyResult);
      assertTrue(historyResult.contains("899"), "Item history should contain updated price 899: " + historyResult);

      // Also test item-history on the laptop object itself
      final var objectHistoryQuery = String.format("""
          let $item := sdb:select-item(jn:doc('json-path1','products'), %s)
          for $v in sdb:item-history($item)
          return {"rev": sdb:revision($v), "price": $v.price}""", laptopNodeKey);
      final var objectHistoryResult = serializeQuery(chain, ctx, objectHistoryQuery);
      assertTrue(objectHistoryResult.contains("999") || objectHistoryResult.contains("899"),
          "Object history should contain laptop prices: " + objectHistoryResult);

      // === Catalog evolution via jn:all-times ===
      final var allTimesQuery = """
          for $v in jn:all-times(jn:doc('json-path1','products'))
          return {"rev": sdb:revision($v), "products": count($v[])}""";
      final var allTimesResult = serializeQuery(chain, ctx, allTimesQuery);
      // Should show 4 revisions with product counts: 2, 2, 3, 2
      assertTrue(allTimesResult.contains("\"rev\":1"), "all-times should include rev 1: " + allTimesResult);
      assertTrue(allTimesResult.contains("\"rev\":4"), "all-times should include rev 4: " + allTimesResult);

      // === Structural diff via jn:diff ===
      final var diffQuery = "jn:diff('json-path1','products', 1, 4)";
      final var diffResult = serializeQuery(chain, ctx, diffQuery);
      // Should contain metadata and diff entries
      assertTrue(diffResult.contains("diffs"), "diff should contain 'diffs' array: " + diffResult);
      assertTrue(diffResult.contains("old-revision"), "diff should contain old-revision: " + diffResult);

      // Also test a smaller diff (rev 1 to 2, just the price update)
      final var smallDiffQuery = "jn:diff('json-path1','products', 1, 2)";
      final var smallDiffResult = serializeQuery(chain, ctx, smallDiffQuery);
      assertTrue(smallDiffResult.contains("diffs"), "small diff should contain 'diffs': " + smallDiffResult);

      // Print discovered outputs for reference
      System.out.println("=== Shell Demo Output Discovery ===");
      System.out.println("Laptop object node key: " + laptopNodeKey);
      System.out.println("Laptop price node key: " + priceNodeKey);
      System.out.println("Item history (price node): " + historyResult);
      System.out.println("Item history (laptop object): " + objectHistoryResult);
      System.out.println("jn:all-times result: " + allTimesResult);
      System.out.println("jn:diff(1,4) result: " + diffResult);
      System.out.println("jn:diff(1,2) result: " + smallDiffResult);
    }
  }

  @Test
  @DisplayName("Verify revision numbers are correct after sequential updates")
  void testRevisionNumbersAfterUpdates() {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      // Rev 1 — store
      new Query(chain, "jn:store('json-path1','products','[{\"name\":\"Laptop\",\"price\":999},{\"name\":\"Phone\",\"price\":699}]')").evaluate(ctx);
      assertEquals("1", serializeQuery(chain, ctx, "sdb:revision(jn:doc('json-path1','products'))"));

      // Rev 2 — update
      new Query(chain, "let $doc := jn:doc('json-path1','products') return replace json value of $doc[0].price with 899").evaluate(ctx);
      assertEquals("2", serializeQuery(chain, ctx, "sdb:revision(jn:doc('json-path1','products'))"));

      // Rev 3 — append
      new Query(chain, "let $doc := jn:doc('json-path1','products') return append json {\"name\":\"Tablet\",\"price\":449} into $doc").evaluate(ctx);
      assertEquals("3", serializeQuery(chain, ctx, "sdb:revision(jn:doc('json-path1','products'))"));

      // Rev 4 — delete
      new Query(chain, "let $doc := jn:doc('json-path1','products') return delete json $doc[1]").evaluate(ctx);
      assertEquals("4", serializeQuery(chain, ctx, "sdb:revision(jn:doc('json-path1','products'))"));
    }
  }

  @Test
  @DisplayName("jn:all-times shows catalog evolution across all revisions")
  void testAllTimesQuery() {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      // Build 4 revisions
      new Query(chain, "jn:store('json-path1','products','[{\"name\":\"Laptop\",\"price\":999},{\"name\":\"Phone\",\"price\":699}]')").evaluate(ctx);
      new Query(chain, "let $doc := jn:doc('json-path1','products') return replace json value of $doc[0].price with 899").evaluate(ctx);
      new Query(chain, "let $doc := jn:doc('json-path1','products') return append json {\"name\":\"Tablet\",\"price\":449} into $doc").evaluate(ctx);
      new Query(chain, "let $doc := jn:doc('json-path1','products') return delete json $doc[1]").evaluate(ctx);

      // Query all revisions via jn:all-times
      final var query = """
          for $v in jn:all-times(jn:doc('json-path1','products'))
          return {"rev": sdb:revision($v), "products": count($v[])}""";
      final var result = serializeQuery(chain, ctx, query);

      assertTrue(result.contains("\"rev\":1"), "Should contain rev 1: " + result);
      assertTrue(result.contains("\"rev\":2"), "Should contain rev 2: " + result);
      assertTrue(result.contains("\"rev\":3"), "Should contain rev 3: " + result);
      assertTrue(result.contains("\"rev\":4"), "Should contain rev 4: " + result);
    }
  }

  @Test
  @DisplayName("jn:diff shows structural changes between revisions")
  void testDiffQuery() {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      // Build 4 revisions
      new Query(chain, "jn:store('json-path1','products','[{\"name\":\"Laptop\",\"price\":999},{\"name\":\"Phone\",\"price\":699}]')").evaluate(ctx);
      new Query(chain, "let $doc := jn:doc('json-path1','products') return replace json value of $doc[0].price with 899").evaluate(ctx);
      new Query(chain, "let $doc := jn:doc('json-path1','products') return append json {\"name\":\"Tablet\",\"price\":449} into $doc").evaluate(ctx);
      new Query(chain, "let $doc := jn:doc('json-path1','products') return delete json $doc[1]").evaluate(ctx);

      // Diff between rev 1 and rev 4 — should show all changes
      final var fullDiffQuery = "jn:diff('json-path1','products', 1, 4)";
      final var fullDiffResult = serializeQuery(chain, ctx, fullDiffQuery);
      assertTrue(fullDiffResult.contains("\"database\""), "Should contain database field: " + fullDiffResult);
      assertTrue(fullDiffResult.contains("\"diffs\""), "Should contain diffs array: " + fullDiffResult);
      assertTrue(fullDiffResult.contains("\"old-revision\":1"), "Should show old-revision 1: " + fullDiffResult);
      assertTrue(fullDiffResult.contains("\"new-revision\":4"), "Should show new-revision 4: " + fullDiffResult);

      // Diff between rev 1 and rev 2 — just the price update
      final var smallDiffQuery = "jn:diff('json-path1','products', 1, 2)";
      final var smallDiffResult = serializeQuery(chain, ctx, smallDiffQuery);
      assertTrue(smallDiffResult.contains("\"old-revision\":1"), "Should show old-revision 1: " + smallDiffResult);
      assertTrue(smallDiffResult.contains("\"new-revision\":2"), "Should show new-revision 2: " + smallDiffResult);
    }
  }

  private String serializeQuery(SirixCompileChain chain, SirixQueryContext ctx, String query) {
    try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, printWriter);
      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("Query serialization failed: " + query, e);
    }
  }
}
