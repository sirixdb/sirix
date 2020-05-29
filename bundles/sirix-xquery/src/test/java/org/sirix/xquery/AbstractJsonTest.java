package org.sirix.xquery;

import org.brackit.xquery.XQuery;
import org.sirix.JsonTestHelper;
import org.sirix.xquery.json.BasicJsonDBStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;

public abstract class AbstractJsonTest {
  protected void setUp() {
    JsonTestHelper.deleteEverything();
  }

  protected void tearDown() {
    JsonTestHelper.closeEverything();
  }

  public void test(String storeQuery, String query, String assertion) throws IOException {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      new XQuery(chain, storeQuery).evaluate(ctx);

      // Use XQuery to load a JSON database/resource.
      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, query).serialize(ctx, printWriter);
        assertEquals(assertion, out.toString());
      }
    }
  }
}
