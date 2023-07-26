package io.sirix.query.function.jn.io;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import junit.framework.TestCase;
import org.brackit.xquery.XQuery;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public final class DocIntegrationTest extends TestCase {

  @Override
  protected void setUp() throws Exception {
    JsonTestHelper.deleteEverything();
  }

  @Override
  protected void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test() throws IOException {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"bla\", \"blubb\"]')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      // Use XQuery to load a JSON database/resource.
      final String openQuery = "jn:doc('json-path1','mydoc.jn')";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, openQuery).serialize(ctx, printWriter);
        assertEquals("[\"bla\",\"blubb\"]", out.toString());
      }
    }
  }
}
