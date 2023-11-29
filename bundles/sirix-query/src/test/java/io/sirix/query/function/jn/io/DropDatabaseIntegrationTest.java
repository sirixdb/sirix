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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public final class DropDatabaseIntegrationTest extends TestCase {

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
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String dropQuery = "jn:drop-database('json-path1')";
      new Query(chain, dropQuery).evaluate(ctx);

      final String hasQuery = "jn:exists-database('json-path1')";
      final Sequence result = new Query(chain, hasQuery).evaluate(ctx);

      assertTrue(result instanceof Bool);

      assertEquals(result, Bool.FALSE);
    }
  }
}
