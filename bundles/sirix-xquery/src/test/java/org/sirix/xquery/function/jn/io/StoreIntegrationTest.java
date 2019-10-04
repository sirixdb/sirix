package org.sirix.xquery.function.jn.io;

import junit.framework.TestCase;
import org.brackit.xquery.XQuery;
import org.junit.Test;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;

public final class StoreIntegrationTest extends TestCase {

  @Test
  public void test() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      System.out.println("Storing string:");
      final String query = "jn:store('mycol.jn','mydoc.jn','[\"bla\", \"blubb\"]')";
      System.out.println(query);
      new XQuery(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testMultipleStrings() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store multiple JSON strings into the store.
      System.out.println("Storing strings:");
      final String query = "jn:store('mycol.jn',(),('[\"bla\", \"blubb\"]','{\"foo\": true}'))";
      System.out.println(query);
      new XQuery(chain, query).evaluate(ctx);

      // Use XQuery to add a JSON string to the collection.
      System.out.println("Storing strings:");
      final String queryAdd = "jn:store('mycol.jn',(),'[\"bla\", \"blubb\"]',false())";
      System.out.println(queryAdd);
      new XQuery(chain, queryAdd).evaluate(ctx);

      // Use XQuery to add a JSON string to the collection.
      System.out.println("Storing strings:");
      final String queryAddStrings = "jn:store('mycol.jn',(),('[\"bla\", \"blubb\"]','{\"foo\": true}'),false())";
      System.out.println(queryAddStrings);
      new XQuery(chain, queryAddStrings).evaluate(ctx);
    }
  }
}
