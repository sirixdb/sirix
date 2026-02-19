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

public final class GetChildCountTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void testArrayChildCount() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"a\", \"b\", \"c\"]')";
      new Query(chain, storeQuery).evaluate(ctx);

      final String query = "sdb:child-count(jn:doc('json-path1','mydoc.jn'))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        assertEquals("3", out.toString());
      }
    }
  }

  @Test
  public void testObjectChildCount() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String storeQuery = "jn:store('json-path1','mydoc.jn','{\"a\":1, \"b\":2}')";
      new Query(chain, storeQuery).evaluate(ctx);

      final String query = "sdb:child-count(jn:doc('json-path1','mydoc.jn'))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        assertEquals("2", out.toString());
      }
    }
  }

  @Test
  public void testLeafNodeChildCount() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"bla\", \"blubb\"]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Select a leaf string node (node key 2 = first string "bla")
      final String query = "sdb:child-count(sdb:select-item(jn:doc('json-path1','mydoc.jn'), 2))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        assertEquals("0", out.toString());
      }
    }
  }
}
