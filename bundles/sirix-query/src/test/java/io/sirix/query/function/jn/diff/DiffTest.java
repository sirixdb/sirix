package io.sirix.query.function.jn.diff;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jsonitem.array.DArray;
import io.brackit.query.jsonitem.object.CompactObject;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.diff.JsonDiffIntegrity;
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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class DiffTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  /**
   * Fusion-mode flag. When {@code -Dsirix.json.fuseNamedPrimitives=true}, the shredder collapses
   * {@code "name":primitive} object fields into a single fused record, which removes one node per
   * such field and shifts subsequent nodeKeys downward. The diff serializer prints raw nodeKeys, so
   * any test that compares a serialized diff against a fixture file with literal nodeKeys must strip
   * the integer values before comparing in fused mode.
   */
  private static final boolean FUSED_NAMED_PRIMITIVES = true;

  /**
   * Matches {@code "nodeKey":<int>}, {@code "oldNodeKey":<int>}, {@code "newNodeKey":<int>},
   * {@code "insertPositionNodeKey":<int>}.
   */
  private static final Pattern NODE_KEY_NUMERIC =
      Pattern.compile("(\"(?:nodeKey|oldNodeKey|newNodeKey|insertPositionNodeKey)\"\\s*:\\s*)(-?\\d+)");

  /**
   * Strips integer nodeKey values when the fusion flag is enabled so a single fixture file can match
   * both legacy and fused outputs whose only difference is downward-shifted nodeKey integers.
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
      // 1=outer OBJ, 2=foo OBJ_NAMED_ARR, 3=str "bar", 4=null, 5=num 2.33,
      // 6=bar OBJ_NAMED_OBJ, 7=hello OBJ_NAMED_STR, 8=helloo OBJ_NAMED_BOOL,
      // 9=baz OBJ_NAMED_STR, 10=tada OBJ_NAMED_ARR, 11=tada[0] OBJ,
      // 12=tada[0].foo OBJ_NAMED_STR, 13=tada[1] OBJ, 14=tada[1].baz OBJ_NAMED_BOOL,
      // 15=tada[2] str "boo", 16=tada[3] empty OBJ, 17=tada[4] empty ARR.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("tadaaa", new StringValue("todooo"));
      wtx.moveTo(4); // legacy 5 → fused 4 (NULL_VALUE foo[1])
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"test\":1}"));
      wtx.moveTo(4); // still NULL_VALUE
      wtx.remove();
      wtx.moveTo(3); // legacy 4 → fused 3 (STRING_VALUE foo[0]="bar")
      wtx.insertBooleanValueAsRightSibling(true);
      wtx.setBooleanValue(false);
      wtx.moveTo(5); // legacy 6 → fused 5 (NUMBER_VALUE 2.33)
      wtx.setNumberValue(1.2);
      wtx.moveTo(7); // legacy 9 → fused 7 (hello OBJ_NAMED_STR)
      wtx.remove();
      wtx.moveTo(9); // legacy 13 → fused 9 (baz OBJ_NAMED_STR)
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
        assertEquals(normalize(Files.readString(JSON.resolve("diff.json"), StandardCharsets.UTF_8)),
            normalize(content));

        final var diffs = JsonParser.parseString(content).getAsJsonObject().getAsJsonArray("diffs");
        assertEquals("{\"tadaaa\":\"todooo\"}",
            diffs.get(0).getAsJsonObject().getAsJsonObject("insert").get("data").getAsString());
        assertEquals("{\"test\":1}",
            diffs.get(2).getAsJsonObject().getAsJsonObject("replace").get("data").getAsString());
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
        assertEquals(normalize(Files.readString(JSON.resolve("diff-with-startnodekey.json"), StandardCharsets.UTF_8)),
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
        assertEquals(normalize(Files.readString(JSON.resolve("diff-with-maxlevel.json"), StandardCharsets.UTF_8)),
            normalize(content));
      }
    }
  }

  @Test
  public void jsonBridgeDecodesEscapesAndPreservesUnicodeAndNumbers() throws IOException {
    final var expected = new JsonObject();
    expected.addProperty("quote", "say \"hello\"");
    expected.addProperty("backslash", "C:\\tmp\\file.json");
    expected.addProperty("controls", "line one\nline two\tend\b");
    expected.addProperty("unicode", "Grüße 𐐷");
    expected.addProperty("mixed", "\"quoted\" then Grüße 𐐷 and \\ a slash");

    final var array = new JsonArray();
    array.add(JsonNull.INSTANCE);
    array.add(true);
    array.add(false);
    array.add(new BigDecimal("-9223372036854775809"));
    array.add(new BigDecimal("1.25"));
    array.add(new BigDecimal("6.02E+23"));
    array.add(new BigDecimal("1.234567890123456789012345678901E+100"));
    array.add(new BigDecimal("1E+309"));
    array.add(new BigDecimal("1E-400"));
    array.add(new BigDecimal("1E+1000000000"));
    array.add(JsonParser.parseString("-0e0"));
    final var nested = new JsonObject();
    nested.addProperty("value", "\\\"ü𐐷\n");
    array.add(nested);
    expected.add("array", array);

    final var item = Diff.parseJsonToBrackitItem(expected.toString());
    try (final var out = new ByteArrayOutputStream();
        final var serializer = new StringSerializer(new PrintStream(out, true, StandardCharsets.UTF_8))) {
      serializer.serialize(item);
      final String serialized = out.toString(StandardCharsets.UTF_8);
      final var actual = JsonParser.parseString(serialized).getAsJsonObject();
      assertEquals(expected, actual);

      final var actualArray = actual.getAsJsonArray("array");
      assertExactDecimal("1.234567890123456789012345678901E+100", actualArray, 6);
      assertExactDecimal("1E+309", actualArray, 7);
      assertExactDecimal("1E-400", actualArray, 8);
      assertEquals("huge exponents must remain compact instead of expanding to a giant string", "1E+1000000000",
          actualArray.get(9).getAsString());
      assertEquals("the exactly representable exponent-form negative zero keeps double semantics", "-0",
          actualArray.get(10).getAsString());

      // Parsing the serialized result through the same strict bridge proves that the output is
      // valid JSON (in particular, no Brackit INF/-INF token escaped from a large exponent).
      Diff.parseJsonToBrackitItem(serialized);
    }
  }

  @Test
  public void jsonBridgeRejectsNonStrictIncompleteOrUnicodeCorruptJson() {
    final var invalidInputs = List.of("", " \r\n\t", "{} {}", "{\"a\":1} trailing", "{unquoted:1}",
        "{'singleQuoted':1}", "{\"a\":01}", "{\"a\":NaN}", "{\"a\":Infinity}", "{\"a\":1,}", "{\"a\":/* comment */1}",
        "{\"a\":\"\\x\"}", "\"literal\nnewline\"", "{", "[1,", "\"unterminated", "1e", "tru", "1e2147483649",
        "\"\\uD800\"", "{\"\\uDC00\":1}");

    for (final String input : invalidInputs) {
      assertStrictParseFailure(input);
    }
    assertStrictParseFailure(null);
  }

  @Test
  public void jsonBridgePreservesSirixNumericTypesAndObjectOrder() {
    final var values = (DArray) Diff.parseJsonToBrackitItem(
        "[2147483647,2147483648," + "99999999999999999,9223372036854775807,0.123456789012345678901234567890,"
            + "1e0,6.02e23,-0e0,9007199254740993e0,5e-324,1e-325,1e309," + "1.234567890123456789012345678901e100]");

    assertTrue(values.at(0) instanceof Int32);
    assertTrue(values.at(1) instanceof Int64);
    assertTrue(values.at(2) instanceof Int64);
    assertTrue(values.at(3) instanceof io.brackit.query.atomic.Int);
    assertTrue(values.at(4) instanceof Dec);
    assertTrue(values.at(5) instanceof Dbl);
    assertTrue(values.at(6) instanceof Dbl);
    assertTrue(values.at(7) instanceof Dbl);
    assertEquals(Double.doubleToRawLongBits(-0.0d), Double.doubleToRawLongBits(((Dbl) values.at(7)).doubleValue()));
    for (int index = 8; index < values.len(); index++) {
      assertTrue("lossy/underflowing/overflowing exponent at index " + index + " must remain exact decimal",
          values.at(index) instanceof Dec);
    }
    assertEquals(0, new BigDecimal("9007199254740993e0").compareTo(((Dec) values.at(8)).decimalValue()));
    assertEquals(0, new BigDecimal("5e-324").compareTo(((Dec) values.at(9)).decimalValue()));
    assertEquals(0, new BigDecimal("1e-325").compareTo(((Dec) values.at(10)).decimalValue()));
    assertEquals(0, new BigDecimal("1e309").compareTo(((Dec) values.at(11)).decimalValue()));
    assertEquals("xs:decimal string semantics must retain the ordinary non-exponent lexical form",
        new BigDecimal("1e309").toPlainString(), ((Dec) values.at(11)).stringValue());
    assertEquals("JSON serialization may use compact exponent notation without changing stringValue()", "1E+309",
        values.at(11).toString());
    assertEquals(0,
        new BigDecimal("1.234567890123456789012345678901e100").compareTo(((Dec) values.at(12)).decimalValue()));

    final var ordered = (CompactObject) Diff.parseJsonToBrackitItem("{\"z\":0,\"a:b\":1,\"\":2,\"m\":3}");
    assertEquals("z", ordered.name(0).stringValue());
    assertEquals("a:b", ordered.name(1).stringValue());
    assertEquals("", ordered.name(2).stringValue());
    assertEquals("m", ordered.name(3).stringValue());

    final var duplicate = (CompactObject) Diff.parseJsonToBrackitItem("{\"key\":1,\"key\":2}");
    assertEquals(1, duplicate.len());
    assertEquals("2", duplicate.value(0).toString());
  }

  private static void assertExactDecimal(final String expected, final JsonArray values, final int index) {
    assertEquals("numeric value at array index " + index + " must be exact", 0,
        new BigDecimal(expected).compareTo(values.get(index).getAsBigDecimal()));
  }

  private static void assertStrictParseFailure(final String input) {
    try {
      Diff.parseJsonToBrackitItem(input);
      fail("Expected strict parse failure for: " + input);
    } catch (final QueryException e) {
      assertEquals(JSONFun.ERR_PARSING_ERROR, e.getCode());
    }
  }

  /**
   * The dewey-ID fast path of {@code jn:diff} reads the pre-computed update-operations file. A crash
   * while that file was written (after the storage commit was already durable) may leave a
   * torn/garbage file behind — the fast path must detect that and fall back to computing the diff,
   * instead of failing on (or serving) the garbage. Mirrors the REST {@code DiffHandler} behavior.
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
    assertTrue("pre-computed update-operations file must exist after the commit", Files.exists(updateOperationsFile));
    final var writtenSidecar = JsonParser.parseString(Files.readString(updateOperationsFile)).getAsJsonObject();
    JsonDiffIntegrity.validate(writtenSidecar);
    assertEquals(1, writtenSidecar.get(JsonDiffIntegrity.OPERATION_COUNT_FIELD).getAsInt());
    final JsonObject omittedOperationSidecar = writtenSidecar.deepCopy();
    omittedOperationSidecar.getAsJsonArray("diffs").remove(0);

    try (
        final var store =
            BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).storeDeweyIds(true).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var databaseName = PATHS.PATH1.getFile().getName(PATHS.PATH1.getFile().getNameCount() - 1).toString();
      final var query = "jn:diff('" + databaseName + "','" + JsonTestHelper.RESOURCE + "',1,2)";
      final var validPrefix = "{\"database\":\"" + databaseName + "\",\"resource\":\"" + JsonTestHelper.RESOURCE
          + "\",\"old-revision\":1,\"new-revision\":2,\"diffs\":";

      final var invalidFiles = List.of(
          // Torn/truncated durable write.
          "{\"database\":\"json-path1\",\"resource\":\"shredded\",\"diffs\":[{\"insert\":{\"nodeKey\":",
          // Syntactically complete but Gson-lenient-only. Accepting this would incorrectly serve
          // an empty pre-computed diff instead of the real insert below.
          "{'database':'" + databaseName + "','resource':'" + JsonTestHelper.RESOURCE
              + "','old-revision':1,'new-revision':2,'diffs':[]}",
          // Strict and full-shaped, but missing the mandatory format/count/digest metadata.
          validPrefix + "[]}",
          // Syntactically valid omission with the writer's original count/digest left in place.
          omittedOperationSidecar.toString(),
          // These carry a valid integrity marker, so schema and node-resolution validation — not
          // merely a missing digest — must reject them.
          withIntegrity(validPrefix + "[{\"bogus\":{}}]}"),
          withIntegrity(validPrefix + "[{\"update\":{\"deweyID\":\"1.17.9\",\"depth\":2}}]}"),
          withIntegrity(validPrefix + "[{\"update\":{\"nodeKey\":7,\"name\":\"renamed\","
              + "\"type\":\"bogus\",\"value\":\"invalid\",\"deweyID\":\"1.17.33.17\"," + "\"depth\":3}}]}"),
          withIntegrity(validPrefix + "[{\"insert\":{\"nodeKey\":9223372036854775807,"
              + "\"insertPositionNodeKey\":1,\"insertPosition\":\"asFirstChild\","
              + "\"deweyID\":\"1.17.9\",\"depth\":2,\"type\":\"jsonFragment\"}}]}"),
          withIntegrity("{\"database\":\"" + databaseName + "\",\"resource\":\"wrong-resource\","
              + "\"old-revision\":1,\"new-revision\":2,\"diffs\":[]}"));

      for (final String invalidFile : invalidFiles) {
        Files.writeString(updateOperationsFile, invalidFile, StandardCharsets.UTF_8);

        try (final var out = new ByteArrayOutputStream()) {
          new Query(chain, query).serialize(ctx, new PrintStream(out));
          final var content = out.toString(StandardCharsets.UTF_8);

          // The result must be a correct, parseable diff document computed via the fallback —
          // not the invalid file content and not an exception.
          final var diffObject = JsonParser.parseString(content).getAsJsonObject();
          assertEquals(databaseName, diffObject.get("database").getAsString());
          assertEquals(JsonTestHelper.RESOURCE, diffObject.get("resource").getAsString());
          assertEquals(1, diffObject.get("old-revision").getAsInt());
          assertEquals(2, diffObject.get("new-revision").getAsInt());
          final var diffs = diffObject.getAsJsonArray("diffs");
          assertEquals("exactly one update operation (the inserted record)", 1, diffs.size());
          assertTrue("the single update operation must be an insert", diffs.get(0).getAsJsonObject().has("insert"));
        }
      }
    }
  }

  private static String withIntegrity(final String document) {
    final JsonObject sidecar = JsonParser.parseString(document).getAsJsonObject();
    JsonDiffIntegrity.add(sidecar);
    return sidecar.toString();
  }

  @Test
  public void test_whenCachedDiffRootWasDeleted_thenFilterFromOldRevisionDeweyId() throws IOException {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    try (final var resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = resourceSession.beginNodeTrx()) {
      // Delete top-level fused field "baz" (node 9), then update another top-level field outside
      // that subtree. Filtering at node 9 must return only its own delete even though node 9 no
      // longer exists in the new revision used to hydrate cached fragments.
      assertTrue(wtx.moveTo(9));
      wtx.remove();
      assertTrue(wtx.moveTo(7));
      wtx.setStringValue("outside-update");
      wtx.commit();

      final Path sidecarPath = resourceSession.getResourceConfig()
                                              .getResource()
                                              .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                              .resolve("diffFromRev1toRev2.json");
      final JsonObject sidecar = JsonParser.parseString(Files.readString(sidecarPath)).getAsJsonObject();
      for (final var operation : sidecar.getAsJsonArray("diffs")) {
        if (operation.getAsJsonObject().has("delete")) {
          operation.getAsJsonObject().getAsJsonObject("delete").addProperty("cache-marker", "old-anchor");
        }
      }
      JsonDiffIntegrity.add(sidecar);
      Files.writeString(sidecarPath, sidecar.toString(), StandardCharsets.UTF_8);
    }

    try (
        final var store =
            BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).storeDeweyIds(true).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store);
        final var out = new ByteArrayOutputStream()) {
      final var databaseName = PATHS.PATH1.getFile().getFileName().toString();
      final var query = "jn:diff('" + databaseName + "','" + JsonTestHelper.RESOURCE + "',1,2,9)";
      new Query(chain, query).serialize(ctx, new PrintStream(out));

      final JsonArray diffs =
          JsonParser.parseString(out.toString(StandardCharsets.UTF_8)).getAsJsonObject().getAsJsonArray("diffs");
      assertEquals(1, diffs.size());
      final JsonObject delete = diffs.get(0).getAsJsonObject().getAsJsonObject("delete");
      assertEquals(9, delete.get("nodeKey").getAsLong());
      assertEquals("old-anchor", delete.get("cache-marker").getAsString());
    }
  }

  @Test
  public void test_whenCachedReplaceReferencesJsonFragment_thenNewNodeIsSerialized() throws IOException {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    assert database != null;
    try (final var resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = resourceSession.beginNodeTrx()) {
      // The fused "baz":"hello" record is node 9. Replacing its primitive value with an object
      // produces a REPLACEDNEW tuple whose cache payload uses newNodeKey (not insert's nodeKey).
      assertTrue(wtx.moveTo(9));
      wtx.replaceObjectRecordValue(new ObjectValue());
      wtx.commit();

      // A cache-only marker proves the query below really took the sidecar path. Recompute the
      // integrity metadata after adding it; an authoritative BasicJsonDiff fallback cannot emit it.
      final Path sidecarPath = resourceSession.getResourceConfig()
                                              .getResource()
                                              .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                              .resolve("diffFromRev1toRev2.json");
      final JsonObject sidecar = JsonParser.parseString(Files.readString(sidecarPath)).getAsJsonObject();
      sidecar.getAsJsonArray("diffs")
             .get(0)
             .getAsJsonObject()
             .getAsJsonObject("replace")
             .addProperty("cache-marker", "new-node-hydrated");
      JsonDiffIntegrity.add(sidecar);
      Files.writeString(sidecarPath, sidecar.toString(), StandardCharsets.UTF_8);
    }

    try (
        final var store =
            BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).storeDeweyIds(true).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store);
        final var out = new ByteArrayOutputStream()) {
      final var databaseName = PATHS.PATH1.getFile().getName(PATHS.PATH1.getFile().getNameCount() - 1).toString();
      final var query = "jn:diff('" + databaseName + "','" + JsonTestHelper.RESOURCE + "',1,2)";
      new Query(chain, query).serialize(ctx, new PrintStream(out));

      final var diffs =
          JsonParser.parseString(out.toString(StandardCharsets.UTF_8)).getAsJsonObject().getAsJsonArray("diffs");
      assertEquals(1, diffs.size());
      final var replace = diffs.get(0).getAsJsonObject().getAsJsonObject("replace");
      assertTrue(replace.has("newNodeKey"));
      assertEquals("new-node-hydrated", replace.get("cache-marker").getAsString());
      final var fragment = JsonParser.parseString(replace.get("data").getAsString()).getAsJsonObject();
      assertTrue(fragment.has("baz"));
      assertTrue(fragment.get("baz").isJsonObject());
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
