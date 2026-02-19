package io.sirix.query.function.sdb.trx;

import io.sirix.JsonTestHelper;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.brackit.query.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.*;

public final class LevelOrderTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void testLevelOrderTraversal() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"a\", \"b\", \"c\"]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Traverse all nodes in level order
      final String query = "for $node in sdb:level-order(jn:doc('json-path1','mydoc.jn')) return $node";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        final String result = out.toString();
        assertFalse(result.isEmpty(), "Level order traversal should return nodes");
        // The result should contain the string values
        assertTrue(result.contains("a"), "Should contain first element");
        assertTrue(result.contains("b"), "Should contain second element");
        assertTrue(result.contains("c"), "Should contain third element");
      }
    }
  }

  @Test
  public void testLevelOrderWithDepthLimit() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Nested structure: array -> objects -> nested values
      final String storeQuery = "jn:store('json-path1','mydoc.jn','[{\"a\": {\"deep\": 1}}, {\"b\": 2}]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Traverse with depth limit of 1 (only direct children)
      final String query = "count(sdb:level-order(jn:doc('json-path1','mydoc.jn'), 1))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        final int countDepth1 = Integer.parseInt(out.toString());
        assertTrue(countDepth1 > 0, "Should have at least 1 node at depth 1");
      }

      // Traverse with unlimited depth
      final String queryAll = "count(sdb:level-order(jn:doc('json-path1','mydoc.jn')))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, queryAll).serialize(ctx, printWriter);
        final int countAll = Integer.parseInt(out.toString());
        assertTrue(countAll > 0, "Should have nodes in full traversal");
      }
    }
  }
}
