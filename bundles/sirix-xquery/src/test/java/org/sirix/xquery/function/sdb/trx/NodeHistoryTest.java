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

public class NodeHistoryTest extends TestCase {
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
      final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','["bla", "blubb"]')
        """.strip();
      new XQuery(chain, storeQuery).evaluate(ctx);

      final String updateQuery1 = """
          replace json value of jn:doc('mycol.jn','mydoc.jn')[[0]] with 'blafasel'
        """.strip();

      new XQuery(chain, updateQuery1).evaluate(ctx);

      final String updateQuery2 = """
          replace json value of jn:doc('mycol.jn','mydoc.jn')[[0]] with 'blablabla'
        """.strip();

      new XQuery(chain, updateQuery2).evaluate(ctx);

      // Use XQuery to load a JSON database/resource.
      final String openQuery = "sdb:node-history(jn:doc('mycol.jn','mydoc.jn')[[0]])";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, openQuery).serialize(ctx, printWriter);
        assertEquals("blablabla", out.toString());
      }
    }
  }
}
