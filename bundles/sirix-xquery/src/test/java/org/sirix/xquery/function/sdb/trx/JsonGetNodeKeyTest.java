package org.sirix.xquery.function.sdb.trx;

import junit.framework.TestCase;
import org.brackit.xquery.XQuery;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class JsonGetNodeKeyTest extends TestCase {
  @Override
  protected void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @Override
  protected void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[\"bla\", \"blubb\"]')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      // Use XQuery to load a JSON database/resource.
      final String openQuery = "sdb:nodekey(jn:doc('mycol.jn','mydoc.jn')[[1]])";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, openQuery).serialize(ctx, printWriter);
        assertEquals("3", out.toString());
      }
    }
  }
}
