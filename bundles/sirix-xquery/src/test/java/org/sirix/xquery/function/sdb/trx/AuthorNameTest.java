package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.XQuery;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.User;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

public class AuthorNameTest {
  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder("mydoc.jn").build());

      try (final var manager = database.beginResourceSession("mydoc.jn"); final var wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"bla\", \"blubb\"]"));
      }
    }

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile(),
                                                         new User("johannes", UUID.randomUUID()));
         final var manager = database.beginResourceSession("mydoc.jn");
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(2);
      wtx.setStringValue("blabla").commit();
    }

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile(),
                                                         new User("moshe", UUID.randomUUID()));
         final var manager = database.beginResourceSession("mydoc.jn");
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(2);
      wtx.setStringValue("blablabla").commit();
    }

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile(),
                                                         new User("carolin", UUID.randomUUID()));
         final var manager = database.beginResourceSession("mydoc.jn");
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(2);
      wtx.remove().commit();
    }

    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      // Use XQuery to load a JSON database/resource.
      query(ctx, chain, "sdb:author-name(jn:doc('json-path1','mydoc.jn', 1))", "admin");
      query(ctx, chain, "sdb:author-name(jn:doc('json-path1','mydoc.jn', 2))", "johannes");
      query(ctx, chain, "sdb:author-name(jn:doc('json-path1','mydoc.jn', 3))", "moshe");
      query(ctx, chain, "sdb:author-name(jn:doc('json-path1','mydoc.jn', 4))", "carolin");
    }
  }

  private void query(SirixQueryContext ctx, SirixCompileChain chain, String openQueryRevisionOne, String author)
      throws IOException {
    try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
      new XQuery(chain, openQueryRevisionOne).serialize(ctx, printWriter);
      Assert.assertEquals(author, out.toString());
    }
  }
}
