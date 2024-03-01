package io.sirix.query.function.jn.io;

import java.nio.file.Path;

import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.brackit.query.Query;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import junit.framework.TestCase;

public final class StoreIntegrationTest extends TestCase {

  private final Path sirixPath = PATHS.PATH1.getFile();

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

      // Use Query to store a JSON string into the store.
      final String query = "jn:store('json-path1','mydoc.jn','[\"bla\", \"blubb\"]')";
      new Query(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testMultipleStrings() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use Query to store multiple JSON strings into the store.
      final String query = "jn:store('json-path1',(),('[\"bla\", \"blubb\"]','{\"foo\": true}'))";
      new Query(chain, query).evaluate(ctx);

      // Use Query to add a JSON string to the collection.
      final String queryAdd = "jn:store('json-path1',(),'[\"bla\", \"blubb\"]',false())";
      new Query(chain, queryAdd).evaluate(ctx);

      // Use Query to add a JSON string to the collection.
      final String queryAddStrings = "jn:store('json-path1',(),('[\"bla\", \"blubb\"]','{\"foo\": true}'),false())";
      new Query(chain, queryAddStrings).evaluate(ctx);

      // Use Query to add a JSON string to the collection.
      final String queryAddWithOptions = "jn:store('json-path1',(),('[\"bla\", \"blubb\"]','{\"foo\": true}'),false(),{\"commitMessage\": \"this is the first commit\"})";
      new Query(chain, queryAddWithOptions).evaluate(ctx);
    }
  }
}
