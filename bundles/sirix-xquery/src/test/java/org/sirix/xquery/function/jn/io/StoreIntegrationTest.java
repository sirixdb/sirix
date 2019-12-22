package org.sirix.xquery.function.jn.io;

import java.nio.file.Files;
import java.nio.file.Path;
import org.brackit.xquery.XQuery;
import org.junit.Test;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;
import junit.framework.TestCase;

public final class StoreIntegrationTest extends TestCase {

  private Path sirixPath;

  @Override
  protected void setUp() throws Exception {
    sirixPath = Files.createTempDirectory("sirix");
  }

  @Override
  protected void tearDown() {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.drop("mycol.jn");
    }
  }

  @Test
  public void test() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      final String query = "jn:store('mycol.jn','mydoc.jn','[\"bla\", \"blubb\"]')";
      new XQuery(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testMultipleStrings() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store multiple JSON strings into the store.
      final String query = "jn:store('mycol.jn',(),('[\"bla\", \"blubb\"]','{\"foo\": true}'))";
      new XQuery(chain, query).evaluate(ctx);

      // Use XQuery to add a JSON string to the collection.
      final String queryAdd = "jn:store('mycol.jn',(),'[\"bla\", \"blubb\"]',false())";
      new XQuery(chain, queryAdd).evaluate(ctx);

      // Use XQuery to add a JSON string to the collection.
      final String queryAddStrings = "jn:store('mycol.jn',(),('[\"bla\", \"blubb\"]','{\"foo\": true}'),false())";
      new XQuery(chain, queryAddStrings).evaluate(ctx);
    }
  }
}
