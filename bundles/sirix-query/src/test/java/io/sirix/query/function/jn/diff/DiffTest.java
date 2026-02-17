package io.sirix.query.function.jn.diff;

import com.google.gson.JsonParser;
import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.query.JsonDBSerializer;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
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
    try (
        final var store =
            BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).storeDeweyIds(true).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      // Use Query to store a JSON string into the store.
      final var databaseName = PATHS.PATH1.getFile().getName(PATHS.PATH1.getFile().getNameCount() - 1).toString();
      final var resourceName = JsonTestHelper.RESOURCE;

      final var queryBuilder = new StringBuilder();
      queryBuilder.append("jn:diff('");
      queryBuilder.append(databaseName);
      queryBuilder.append("','");
      queryBuilder.append(resourceName);
      queryBuilder.append("',1,3)");

      try (final var out = new ByteArrayOutputStream()) {
        new Query(chain, queryBuilder.toString()).serialize(ctx, new PrintStream(out));
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
        new Query(chain, queryBuilder.toString()).serialize(ctx, new PrintStream(out));
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
        new Query(chain, queryBuilder.toString()).serialize(ctx, new PrintStream(out));
        final var content = out.toString(StandardCharsets.UTF_8);
        assertEquals(Files.readString(JSON.resolve("diff-with-maxlevel.json"), StandardCharsets.UTF_8), content);
      }
    }
  }

  @Test
  public void test_whenDiffSerializedWithJsonDBSerializer_thenDiffIsNotQuotedAsString() throws IOException {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("newKey", new StringValue("newValue"));
      wtx.commit();
    }

    // Initialize query context and store.
    try (
        final var store =
            BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).storeDeweyIds(true).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final var databaseName = PATHS.PATH1.getFile().getName(PATHS.PATH1.getFile().getNameCount() - 1).toString();
      final var resourceName = JsonTestHelper.RESOURCE;

      final var queryBuilder = new StringBuilder();
      queryBuilder.append("jn:diff('");
      queryBuilder.append(databaseName);
      queryBuilder.append("','");
      queryBuilder.append(resourceName);
      queryBuilder.append("',1,2)");

      // Use JsonDBSerializer like the REST API does - this wraps the output in {"rest":[...]}
      final var stringBuilder = new StringBuilder();
      final var serializer = new JsonDBSerializer(stringBuilder, false);
      new Query(chain, queryBuilder.toString()).serialize(ctx, serializer);

      final var result = stringBuilder.toString();

      // Parse the result as JSON to verify it's valid JSON
      final var jsonResult = JsonParser.parseString(result).getAsJsonObject();

      // Verify the structure is {"rest":[...]} with an object inside, not a string
      assertTrue("Result should contain 'rest' array", jsonResult.has("rest"));
      final var restArray = jsonResult.getAsJsonArray("rest");
      assertEquals("Rest array should have one element", 1, restArray.size());

      // The key assertion: the first element should be an object, not a string
      // Before the fix, this would fail because the diff was serialized as a quoted string
      assertTrue("Diff should be a JSON object, not a quoted string", restArray.get(0).isJsonObject());

      // Verify the diff object has expected fields
      final var diffObject = restArray.get(0).getAsJsonObject();
      assertTrue("Diff object should have 'database' field", diffObject.has("database"));
      assertTrue("Diff object should have 'resource' field", diffObject.has("resource"));
      assertTrue("Diff object should have 'old-revision' field", diffObject.has("old-revision"));
      assertTrue("Diff object should have 'new-revision' field", diffObject.has("new-revision"));
      assertTrue("Diff object should have 'diffs' field", diffObject.has("diffs"));
    }
  }
}

