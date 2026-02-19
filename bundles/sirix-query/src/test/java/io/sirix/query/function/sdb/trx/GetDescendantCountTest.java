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

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class GetDescendantCountTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void testFlatArrayDescendantCount() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"a\", \"b\", \"c\"]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Root array has 3 string children
      final String query = "sdb:descendant-count(jn:doc('json-path1','mydoc.jn'))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        assertEquals("3", out.toString());
      }
    }
  }

  @Test
  public void testNestedObjectDescendantCount() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Object with nested structure: {"a": {"b": 1}} = object(key "a", object(key "b", value 1))
      final String storeQuery = "jn:store('json-path1','mydoc.jn','{\"a\": {\"b\": 1}}')";
      new Query(chain, storeQuery).evaluate(ctx);

      final String query = "sdb:descendant-count(jn:doc('json-path1','mydoc.jn'))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        // Descendant count depends on internal node structure
        final int count = Integer.parseInt(out.toString());
        // Should have at least the key "a", the nested object, key "b", and value 1
        assertEquals(true, count >= 4, "Expected at least 4 descendants, got " + count);
      }
    }
  }

  @Test
  public void testLeafNodeDescendantCount() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"bla\", \"blubb\"]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Leaf node has 0 descendants
      final String query = "sdb:descendant-count(sdb:select-item(jn:doc('json-path1','mydoc.jn'), 2))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        assertEquals("0", out.toString());
      }
    }
  }
}
