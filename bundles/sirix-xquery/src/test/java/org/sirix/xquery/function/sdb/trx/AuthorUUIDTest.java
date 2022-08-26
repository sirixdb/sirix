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

public class AuthorUUIDTest {
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

    final var johannesUUID = UUID.randomUUID();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile(),
                                                         new User("johannes", johannesUUID));
         final var manager = database.beginResourceSession("mydoc.jn");
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(2);
      wtx.setStringValue("blabla").commit();
    }

    final var mosheUUID = UUID.randomUUID();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile(),
                                                         new User("moshe", mosheUUID));
         final var manager = database.beginResourceSession("mydoc.jn");
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(2);
      wtx.setStringValue("blablabla").commit();
    }

    final var carolinUUID = UUID.randomUUID();

    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile(),
                                                         new User("carolin", carolinUUID));
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
      query(ctx, chain, "sdb:author-id(jn:doc('json-path1','mydoc.jn', 2))", johannesUUID.toString());
      query(ctx, chain, "sdb:author-id(jn:doc('json-path1','mydoc.jn', 3))", mosheUUID.toString());
      query(ctx, chain, "sdb:author-id(jn:doc('json-path1','mydoc.jn', 4))", carolinUUID.toString());
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
