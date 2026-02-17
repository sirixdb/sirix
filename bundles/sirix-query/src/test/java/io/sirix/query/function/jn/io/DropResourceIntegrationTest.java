package io.sirix.query.function.jn.io;

import io.brackit.query.Query;
import io.brackit.query.atomic.Bool;
import io.brackit.query.jdm.Sequence;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.Database;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;

public final class DropResourceIntegrationTest extends TestCase {

  @Override
  protected void setUp() throws Exception {
    JsonTestHelper.deleteEverything();
    JsonTestHelper.createDatabase(PATHS.PATH1.getFile());
  }

  @Override
  protected void tearDown() {
    JsonTestHelper.closeEverything();
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test() throws IOException {
    // Initialize query context and store.
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use Query to store a JSON string into the store.
      final String storeQuery = "jn:store('json-path1','mydoc', '{\"foo\":\"bar\"}')";
      new Query(chain, storeQuery).evaluate(ctx);

      final String removeQuery = "jn:drop-resource('json-path1','mydoc')";
      new Query(chain, removeQuery).evaluate(ctx);

      final String existsResourceQuery = "jn:exists-resource('json-path1','mydoc')";
      Sequence result = new Query(chain, existsResourceQuery).evaluate(ctx);

      assertTrue(result instanceof Bool);

      assertEquals(result, Bool.FALSE);

      final String existsDatabaseQuery = "jn:exists-database('json-path1')";
      result = new Query(chain, existsDatabaseQuery).evaluate(ctx);

      assertTrue(result instanceof Bool);

      assertEquals(result, Bool.TRUE);
    }
  }
}
