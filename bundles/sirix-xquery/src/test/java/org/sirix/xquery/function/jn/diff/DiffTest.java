package org.sirix.xquery.function.jn.diff;

import org.brackit.xquery.XQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.access.trx.node.json.objectvalue.StringValue;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public final class DiffTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    assert database != null;
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("tadaaa", new StringValue("todooo"));
      wtx.moveTo(5);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"test\":1}"));
      wtx.moveTo(5);
      wtx.remove();
      wtx.moveTo(4);
      wtx.insertBooleanValueAsRightSibling(true);
      wtx.setBooleanValue(false);
      wtx.moveTo(6);
      wtx.setNumberValue(1.2);
      wtx.moveTo(9);
      wtx.remove();
      wtx.moveTo(13);
      wtx.remove();
      wtx.moveTo(15);
      wtx.setObjectKeyName("tadaa");
      wtx.moveTo(22);
      wtx.setBooleanValue(true);
      wtx.commit();
    }

    // Initialize query context and store.
    try (final var store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      // Use XQuery to store a JSON string into the store.
      final var databaseName = PATHS.PATH1.getFile().getName(PATHS.PATH1.getFile().getNameCount() - 1).toString();
      final var resourceName = JsonTestHelper.RESOURCE;

      final var queryBuilder = new StringBuilder();
      queryBuilder.append("jn:diff('");
      queryBuilder.append(databaseName);
      queryBuilder.append("','");
      queryBuilder.append(resourceName);
      queryBuilder.append("',1,3)");

      try (final var out = new ByteArrayOutputStream()) {
        new XQuery(chain, queryBuilder.toString()).serialize(ctx, new PrintStream(out));
        final var content = out.toString(StandardCharsets.UTF_8);
        assertEquals(Files.readString(JSON.resolve("diff.json"), StandardCharsets.UTF_8), content);
      }

      queryBuilder.setLength(0);
      queryBuilder.append("jn:diff('");
      queryBuilder.append(databaseName);
      queryBuilder.append("','");
      queryBuilder.append(resourceName);
      queryBuilder.append("',1,3,3,0)");

      try (final var out = new ByteArrayOutputStream()) {
        new XQuery(chain, queryBuilder.toString()).serialize(ctx, new PrintStream(out));
        final var content = out.toString(StandardCharsets.UTF_8);
        assertEquals(Files.readString(JSON.resolve("diff-with-startnodekey.json"), StandardCharsets.UTF_8), content);
      }

      queryBuilder.setLength(0);
      queryBuilder.append("jn:diff('");
      queryBuilder.append(databaseName);
      queryBuilder.append("','");
      queryBuilder.append(resourceName);
      queryBuilder.append("',1,3,0,2)");

      try (final var out = new ByteArrayOutputStream()) {
        new XQuery(chain, queryBuilder.toString()).serialize(ctx, new PrintStream(out));
        final var content = out.toString(StandardCharsets.UTF_8);
        assertEquals(Files.readString(JSON.resolve("diff-with-maxlevel.json"), StandardCharsets.UTF_8), content);
      }
    }
  }
}

