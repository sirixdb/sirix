package io.sirix.query.function.jn.diff;

import com.google.gson.JsonParser;
import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
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
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class DiffTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  /**
   * Fusion-mode flag. When {@code -Dsirix.json.fuseNamedPrimitives=true}, the
   * shredder collapses {@code "name":primitive} object fields into a single fused
   * record, which removes one node per such field and shifts subsequent nodeKeys
   * downward. The diff serializer prints raw nodeKeys, so any test that compares
   * a serialized diff against a fixture file with literal nodeKeys must strip
   * the integer values before comparing in fused mode.
   */
  private static final boolean FUSED_NAMED_PRIMITIVES = true;

  /** Matches {@code "nodeKey":<int>}, {@code "oldNodeKey":<int>}, {@code "newNodeKey":<int>}, {@code "insertPositionNodeKey":<int>}. */
  private static final Pattern NODE_KEY_NUMERIC = Pattern.compile(
      "(\"(?:nodeKey|oldNodeKey|newNodeKey|insertPositionNodeKey)\"\\s*:\\s*)(-?\\d+)");

  /**
   * Strips integer nodeKey values when the fusion flag is enabled so a single
   * fixture file can match both legacy and fused outputs whose only difference
   * is downward-shifted nodeKey integers.
   */
  private static String normalize(final String s) {
    if (s == null || !FUSED_NAMED_PRIMITIVES) {
      return s;
    }
    return NODE_KEY_NUMERIC.matcher(s).replaceAll("$1<nk>");
  }

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
    try (final var resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = resourceSession.beginNodeTrx()) {
      // iter#32 P2 fusion key map (initial doc, see JsonDocumentCreator.JSON):
      //   1=outer OBJ, 2=foo OBJ_NAMED_ARR, 3=str "bar", 4=null, 5=num 2.33,
      //   6=bar OBJ_NAMED_OBJ, 7=hello OBJ_NAMED_STR, 8=helloo OBJ_NAMED_BOOL,
      //   9=baz OBJ_NAMED_STR, 10=tada OBJ_NAMED_ARR, 11=tada[0] OBJ,
      //   12=tada[0].foo OBJ_NAMED_STR, 13=tada[1] OBJ, 14=tada[1].baz OBJ_NAMED_BOOL,
      //   15=tada[2] str "boo", 16=tada[3] empty OBJ, 17=tada[4] empty ARR.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("tadaaa", new StringValue("todooo"));
      wtx.moveTo(4);  // legacy 5 → fused 4 (NULL_VALUE foo[1])
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"test\":1}"));
      wtx.moveTo(4);  // still NULL_VALUE
      wtx.remove();
      wtx.moveTo(3);  // legacy 4 → fused 3 (STRING_VALUE foo[0]="bar")
      wtx.insertBooleanValueAsRightSibling(true);
      wtx.setBooleanValue(false);
      wtx.moveTo(5);  // legacy 6 → fused 5 (NUMBER_VALUE 2.33)
      wtx.setNumberValue(1.2);
      wtx.moveTo(7);  // legacy 9 → fused 7 (hello OBJ_NAMED_STR)
      wtx.remove();
      wtx.moveTo(9);  // legacy 13 → fused 9 (baz OBJ_NAMED_STR)
      wtx.remove();
      wtx.moveTo(10); // legacy 15 → fused 10 (tada OBJ_NAMED_ARR)
      wtx.setObjectKeyName("tadaa");
      wtx.moveTo(14); // legacy 22 → fused 14 (tada[1].baz OBJ_NAMED_BOOL)
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
        assertEquals(normalize(Files.readString(JSON.resolve("diff.json"), StandardCharsets.UTF_8)), normalize(content));
      }

      queryBuilder.setLength(0);
      queryBuilder.append("jn:diff('");
      queryBuilder.append(databaseName);
      queryBuilder.append("','");
      queryBuilder.append(resourceName);
      // iter#32 P2 fusion: legacy startNodeKey 3 = foo OBJECT_KEY → fused 2 = foo OBJ_NAMED_ARR.
      queryBuilder.append("',1,3,2,0)");

      try (final var out = new ByteArrayOutputStream()) {
        new Query(chain, queryBuilder.toString()).serialize(ctx, new PrintStream(out));
        final var content = out.toString(StandardCharsets.UTF_8);
        assertEquals(
            normalize(Files.readString(JSON.resolve("diff-with-startnodekey.json"), StandardCharsets.UTF_8)),
            normalize(content));
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
        assertEquals(
            normalize(Files.readString(JSON.resolve("diff-with-maxlevel.json"), StandardCharsets.UTF_8)),
            normalize(content));
      }
    }
  }

  /**
   * The dewey-ID fast path of {@code jn:diff} reads the pre-computed update-operations file. A
   * crash while that file was written (after the storage commit was already durable) may leave a
   * torn/garbage file behind — the fast path must detect that and fall back to computing the
   * diff, instead of failing on (or serving) the garbage. Mirrors the REST {@code DiffHandler}
   * behavior.
   */
  @Test
  public void test_whenUpdateOperationsFileIsTorn_thenFallBackToComputedDiff() throws IOException {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    final Path updateOperationsFile;
    try (final var resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = resourceSession.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertObjectRecordAsFirstChild("newKey", new StringValue("newValue"));
        wtx.commit();
      }
      updateOperationsFile = resourceSession.getResourceConfig()
                                            .getResource()
                                            .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                            .resolve("diffFromRev1toRev2.json");
    }
    assertTrue("pre-computed update-operations file must exist after the commit",
               Files.exists(updateOperationsFile));

    // Simulate the crash: replace the pre-computed diff file with a torn (truncated,
    // unparseable) write of itself.
    Files.writeString(updateOperationsFile,
                      "{\"database\":\"json-path1\",\"resource\":\"shredded\",\"diffs\":[{\"insert\":{\"nodeKey\":",
                      StandardCharsets.UTF_8);

    try (
        final var store =
            BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).storeDeweyIds(true).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var databaseName = PATHS.PATH1.getFile().getName(PATHS.PATH1.getFile().getNameCount() - 1).toString();
      final var query = "jn:diff('" + databaseName + "','" + JsonTestHelper.RESOURCE + "',1,2)";

      try (final var out = new ByteArrayOutputStream()) {
        new Query(chain, query).serialize(ctx, new PrintStream(out));
        final var content = out.toString(StandardCharsets.UTF_8);

        // The result must be a correct, parseable diff document computed via the fallback — not
        // the torn file content and not an exception.
        final var diffObject = JsonParser.parseString(content).getAsJsonObject();
        assertEquals(databaseName, diffObject.get("database").getAsString());
        assertEquals(JsonTestHelper.RESOURCE, diffObject.get("resource").getAsString());
        assertEquals(1, diffObject.get("old-revision").getAsInt());
        assertEquals(2, diffObject.get("new-revision").getAsInt());
        final var diffs = diffObject.getAsJsonArray("diffs");
        assertEquals("exactly one update operation (the inserted record)", 1, diffs.size());
        assertTrue("the single update operation must be an insert",
                   diffs.get(0).getAsJsonObject().has("insert"));
      }
    }
  }

  @Test
  public void test_whenDiffSerializedWithJsonDBSerializer_thenDiffIsNotQuotedAsString() throws IOException {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    try (final var resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = resourceSession.beginNodeTrx()) {
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

