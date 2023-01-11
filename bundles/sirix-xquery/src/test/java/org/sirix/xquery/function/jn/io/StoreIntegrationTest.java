package org.sirix.xquery.function.jn.io;

import java.nio.file.Path;
import org.brackit.xquery.XQuery;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;
import junit.framework.TestCase;

public final class StoreIntegrationTest extends TestCase {

  private Path sirixPath = PATHS.PATH1.getFile();

  @Override
  protected void setUp() throws Exception {
    JsonTestHelper.deleteEverything();
  }

  @Override
  protected void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      final String query = "jn:store('json-path1','mydoc.jn','[\"bla\", \"blubb\"]')";
      new XQuery(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testMultipleStrings() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store multiple JSON strings into the store.
      final String query = "jn:store('json-path1',(),('[\"bla\", \"blubb\"]','{\"foo\": true}'))";
      new XQuery(chain, query).evaluate(ctx);

      // Use XQuery to add a JSON string to the collection.
      final String queryAdd = "jn:store('json-path1',(),'[\"bla\", \"blubb\"]',false())";
      new XQuery(chain, queryAdd).evaluate(ctx);

      // Use XQuery to add a JSON string to the collection.
      final String queryAddStrings = "jn:store('json-path1',(),('[\"bla\", \"blubb\"]','{\"foo\": true}'),false())";
      new XQuery(chain, queryAddStrings).evaluate(ctx);
    }
  }
}
