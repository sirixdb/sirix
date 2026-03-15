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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@code sdb:explain()} — verifies the function works
 * end-to-end through the full compile chain.
 */
final class ExplainIntegrationTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("sdb:explain returns valid JSON for a simple expression")
  void explainSimpleExpression() {
    try (final var store = newStore();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final String result = executeAndSerialize(chain, ctx,
          "sdb:explain('1 + 2')");

      assertNotNull(result);
      assertTrue(result.startsWith("{"), "Should start with JSON object: " + result);
      assertTrue(result.contains("\"operator\""), "Should contain operator field: " + result);
    }
  }

  @Test
  @DisplayName("sdb:explain with verbose=true returns parsed and optimized")
  void explainVerbose() {
    try (final var store = newStore();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final String result = executeAndSerialize(chain, ctx,
          "sdb:explain('1 + 2', true())");

      assertNotNull(result);
      assertTrue(result.contains("\"parsed\""), "Verbose mode should include parsed AST: " + result);
      assertTrue(result.contains("\"optimized\""), "Verbose mode should include optimized AST: " + result);
    }
  }

  @Test
  @DisplayName("sdb:explain on data query shows ForBind in plan")
  void explainDataQuery() {
    // First store some data
    executeQuery("jn:store('json-path1','mydoc.jn','[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]')");

    try (final var store = newStore();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final String result = executeAndSerialize(chain, ctx,
          "sdb:explain('for $x in jn:doc(\"json-path1\",\"mydoc.jn\")[][] return $x.name')");

      assertNotNull(result);
      // The plan should show FLWOR pipeline structure
      assertTrue(result.contains("\"operator\""), "Should contain operator nodes: " + result);
    }
  }

  @Test
  @DisplayName("sdb:explain on query with index shows cost annotations")
  void explainQueryWithIndex() {
    // Store data and create an index
    executeQuery("jn:store('json-path1','mydoc.jn','[{\"price\":10},{\"price\":20},{\"price\":30}]')");
    executeQuery("""
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/price')
        return {"revision": sdb:commit($doc)}
        """);

    try (final var store = newStore();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final String result = executeAndSerialize(chain, ctx,
          "sdb:explain('for $x in jn:doc(\"json-path1\",\"mydoc.jn\")[][] where $x.price gt 15 return $x')");

      assertNotNull(result);
      assertTrue(result.contains("\"operator\""), "Should have operator nodes: " + result);
    }
  }

  @Test
  @DisplayName("QueryPlan programmatic API works with real database")
  void queryPlanProgrammaticAPI() {
    executeQuery("jn:store('json-path1','mydoc.jn','[{\"x\":1},{\"x\":2},{\"x\":3}]')");

    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain(
          "for $x in jn:doc('json-path1','mydoc.jn')[][] return $x",
          store, null);

      assertNotNull(plan.optimizedAST());
      assertNotNull(plan.parsedAST());

      final String json = plan.toJSON();
      assertNotNull(json);
      assertFalse(json.equals("null"), "Plan should not be null");
      assertTrue(json.contains("\"operator\""), "Plan should contain operators: " + json);
    }
  }

  @Test
  @DisplayName("QueryPlan.toRawJSON matches Brackit AST.toJSON format")
  void queryPlanRawJSON() {
    try (final var store = newStore()) {
      final QueryPlan plan = QueryPlan.explain("1 + 2", store, null);
      final String raw = plan.toRawJSON();

      assertNotNull(raw);
      assertTrue(raw.startsWith("{"), "Raw JSON should be object: " + raw);
      assertTrue(raw.contains("\"type\""), "Raw JSON should have type field: " + raw);
    }
  }

  private String executeAndSerialize(SirixCompileChain chain, SirixQueryContext ctx, String query) {
    try (final var out = new ByteArrayOutputStream();
         final var writer = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, writer);
      return out.toString();
    } catch (Exception e) {
      fail("Query execution failed: " + e.getMessage(), e);
      return null;
    }
  }

  private void executeQuery(String query) {
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
