package io.sirix.query.function.jn.trx;

import io.sirix.JsonTestHelper;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.*;

public final class SelectJsonItemTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void testSelectRootArray() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"bla\", \"blubb\"]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Select the root node (node key 1)
      final String query = "jn:select-json-item(jn:doc('json-path1','mydoc.jn'), 1)";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        assertEquals("[\"bla\",\"blubb\"]", out.toString());
      }
    }
  }

  @Test
  public void testSelectStringItem() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"bla\", \"blubb\"]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Select the first string element (node key 2)
      final String query = "jn:select-json-item(jn:doc('json-path1','mydoc.jn'), 2)";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        assertEquals("\"bla\"", out.toString());
      }
    }
  }

  @Test
  public void testSelectNestedObject() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String storeQuery = "jn:store('json-path1','mydoc.jn','[{\"name\":\"Alice\"}, {\"name\":\"Bob\"}]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Select the first object (node key 2)
      final String query = "jn:select-json-item(jn:doc('json-path1','mydoc.jn'), 2)";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        assertEquals("{\"name\":\"Alice\"}", out.toString());
      }
    }
  }

  @Test
  public void testSelectInvalidNodeKeyThrows() {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"bla\"]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Select a non-existent node key
      final String query = "jn:select-json-item(jn:doc('json-path1','mydoc.jn'), 999)";

      assertThrows(QueryException.class, () -> new Query(chain, query).execute(ctx));
    }
  }
}
