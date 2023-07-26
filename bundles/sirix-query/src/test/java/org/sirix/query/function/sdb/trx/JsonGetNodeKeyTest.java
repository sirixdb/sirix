package org.sirix.query.function.sdb.trx;

import org.brackit.xquery.XQuery;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.sirix.JsonTestHelper;
import org.sirix.query.SirixCompileChain;
import org.sirix.query.SirixQueryContext;
import org.sirix.query.json.BasicJsonDBStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonGetNodeKeyTest {

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test() throws IOException {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"bla\", \"blubb\"]')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      // Use XQuery to load a JSON database/resource.
      final String openQuery = "sdb:nodekey(jn:doc('json-path1','mydoc.jn')[[1]])";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, openQuery).serialize(ctx, printWriter);
        assertEquals("3", out.toString());
      }
    }
  }
}
