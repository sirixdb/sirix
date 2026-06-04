package io.sirix.service.json.serialize;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.utils.JsonDocumentCreator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public final class JsonSerializerTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  /**
   * When {@code sirix.json.fuseNamedPrimitives=true} the shredder collapses object
   * fields whose value is a primitive into a single {@code OBJECT_NAMED_*} record
   * instead of emitting an {@code OBJECT_KEY} plus a value child. That shrinks the
   * persisted record count for those fields by one, so descendantCount and nodeKey
   * annotations in the serialized output shift accordingly. We keep two parallel
   * expected-JSON fixtures per test ({@code name.json} for legacy, {@code
   * name-fused.json} for fusion) and {@link #expectedFor(String)} picks the right one.
   */
  private static final boolean FUSE_NAMED_PRIMITIVES =
      true;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void metadataSerializationAlwaysProducesValidJson() throws Exception {
    // The Explorer fetches every resource WITH metadata and a maxLevel (delegating to
    // JsonLimitedSerializer). Regression sweep: nested objects, objects-in-arrays, empties,
    // mixed primitives and array roots must all serialize to VALID JSON across both metadata
    // modes and every depth — not just the unbounded JsonSerializer path.
    final String[] docs = {
        "{\"store\":{\"name\":\"Test Store\",\"products\":[{\"id\":1}],\"metadata\":{\"version\":\"1.0\"}}}",
        "{\"a\":{\"b\":{\"c\":{\"d\":1}}}}",
        "{\"arr\":[{\"x\":1},{\"y\":{\"z\":2}}]}",
        "{\"empty\":{},\"emptyArr\":[],\"mixed\":[1,\"two\",true,null,{\"k\":\"v\"}]}",
        "[{\"obj\":{\"nested\":{}}},[1,2],{}]",
        "{\"o\":{\"p\":{\"q\":[{\"r\":{\"s\":\"t\"}}]}}}",
        "{\"users\":[{\"name\":\"a\",\"roles\":[\"x\",\"y\"],\"meta\":{\"active\":true}}]}"
    };
    int caseNum = 0;
    for (final String json : docs) {
      caseNum++;
      JsonTestHelper.deleteEverything();
      final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = manager.beginNodeTrx()) {
        new JsonShredder.Builder(trx, JsonShredder.createStringReader(json),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();

        for (final boolean nodeKeyAndChildCount : new boolean[] {true, false}) {
          for (final int level : new int[] {1, 2, 3, 4, 5, Integer.MAX_VALUE}) {
            final Writer w = new StringWriter();
            final JsonSerializer ser = nodeKeyAndChildCount
                ? new JsonSerializer.Builder(manager, w).maxLevel(level)
                      .withNodeKeyAndChildCountMetaData(true).build()
                : new JsonSerializer.Builder(manager, w).maxLevel(level).withMetaData(true).build();
            ser.call();
            final String actual = w.toString();
            try {
              if (actual.trim().startsWith("[")) {
                new org.json.JSONArray(actual);
              } else {
                new org.json.JSONObject(actual);
              }
            } catch (final Exception e) {
              throw new AssertionError("INVALID JSON  case=" + caseNum + "  mode="
                  + (nodeKeyAndChildCount ? "nodeKeyAndChildCount" : "metaData") + "  maxLevel=" + level
                  + "\n  doc: " + json + "\n  out: " + actual + "\n  err: " + e.getMessage());
            }
          }
        }
      }
    }
    System.out.println(">>> SWEEP PASSED: " + caseNum + " docs x 2 modes x 6 levels all valid JSON");
  }

  /**
   * Resolves the expected-JSON fixture for the current shredder fusion mode.
   *
   * @param baseFileName file name relative to the {@code json} resource root for the
   *                     legacy (non-fused) expected output, e.g.
   *                     {@code "simple-testdoc-withmetadata-withmaxlevel.json"}
   * @return the {@link Path} of the matching fused fixture when
   *         {@code sirix.json.fuseNamedPrimitives=true} and a {@code *-fused.json}
   *         sibling exists, otherwise the legacy path
   */
  private static Path expectedFor(final String baseFileName) {
    if (FUSE_NAMED_PRIMITIVES) {
      final int dot = baseFileName.lastIndexOf('.');
      final String fusedName = baseFileName.substring(0, dot) + "-fused" + baseFileName.substring(dot);
      final Path fusedPath = JSON.resolve(fusedName);
      if (Files.exists(fusedPath)) {
        return fusedPath;
      }
    }
    return JSON.resolve(baseFileName);
  }

  @Test
  public void test() throws Exception {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx();
        final var writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"test\",\"test\"]"));
      wtx.moveTo(2);
      wtx.remove();
      wtx.moveTo(1);
      wtx.insertStringValueAsFirstChild("diff");
      wtx.moveTo(3);
      wtx.remove();
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"interesting\": \"!\"}]"));

      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      final var expected = Files.readString(JSON.resolve("array-with-right-sibling.json"), StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, writer.toString(), true);
    }
  }

  @Test
  public void testJsonDocumentPrettyPrinted() throws Exception {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).prettyPrint().build();
      serializer.call();
      final var expected = Files.readString(JSON.resolve("pretty-printed-test-doc.json"), StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, writer.toString(), true);
    }
  }

  @Test
  public void testJsonDocument() throws Exception {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      Assert.assertEquals(JsonDocumentCreator.JSON, writer.toString());
    }
  }

  @Test
  public void testMultipleRevisionsJsonDocument() throws Exception {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var writer = new StringWriter();
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("tadaaa", new StringValue("todooo"));
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(manager, writer, 1, 2).build();
      serializer.call();

      final var expected = Files.readString(JSON.resolve("multiple-revisions.json"), StandardCharsets.UTF_8);
      final var actual = writer.toString();
      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMaxChildren1() throws Exception {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      var serializedString = getSerializedStringWithMaxChildren(manager, 1);
      var expected = Files.readString(JSON.resolve("jsonSerializer")
                                          .resolve("testJsonDocumentWithMaxChildren1")
                                          .resolve("document-with-1-maxChildren.json"),
          StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, serializedString, true);

      serializedString = getSerializedStringWithMaxChildren(manager, 2);
      expected = Files.readString(JSON.resolve("jsonSerializer")
                                      .resolve("testJsonDocumentWithMaxChildren1")
                                      .resolve("document-with-2-maxChildren.json"),
          StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, serializedString, true);

      serializedString = getSerializedStringWithMaxChildren(manager, 3);
      expected = Files.readString(JSON.resolve("jsonSerializer")
                                      .resolve("testJsonDocumentWithMaxChildren1")
                                      .resolve("document-with-3-maxChildren.json"),
          StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, serializedString, true);

      serializedString = getSerializedStringWithMaxChildren(manager, 4);
      expected = Files.readString(JSON.resolve("jsonSerializer")
                                      .resolve("testJsonDocumentWithMaxChildren1")
                                      .resolve("document-with-4-maxChildren.json"),
          StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, serializedString, true);
    }
  }

  private String getSerializedStringWithMaxChildren(final JsonResourceSession manager, final int maxChildren)
      throws Exception {
    try (final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).maxChildren(maxChildren).build();
      serializer.call();

      return writer.toString();
    }
  }

  @Test
  public void testJsonDocumentWithMaxChildren2() throws Exception {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).useDeweyIDs(true).build());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(JSON.resolve("complex3.json")));

      var serializedString = getSerializedStringWithMaxChildren(manager, 2);
      var expected = Files.readString(JSON.resolve("jsonSerializer")
                                          .resolve("testJsonDocumentWithMaxChildren2")
                                          .resolve("document-with-1-maxChildren.json"),
          StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, serializedString, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadata() throws Exception {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).withMetaData(true).build();
      serializer.call();

      final var expected = Files.readString(JSON.resolve("document-with-metadata.json"), StandardCharsets.UTF_8);
      final var actual = writer.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000");

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndMaxLevelAndPrettyPrinting() throws Exception {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final Writer writer = new StringWriter()) {
      final var serializer =
          new JsonSerializer.Builder(manager, writer).withMetaData(true).maxLevel(2).prettyPrint().build();
      serializer.call();

      final var expected =
          Files.readString(expectedFor("testdoc-withmetadata-withmaxlevel.json"), StandardCharsets.UTF_8);
      final var actual = writer.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000");

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndMaxLevelSecond() throws Exception {
    final var jsonPath = JSON.resolve("simple-testdoc.json");
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var serializer = new JsonSerializer.Builder(manager, writer).withMetaData(true).maxLevel(2).build();
      serializer.call();

      final var expected = Files.readString(expectedFor("simple-testdoc-withmetadata-withmaxlevel.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000");

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndPrettyPrintingAndObjectKeyStartNodeKey() throws Exception {
    final var jsonPath = JSON.resolve("simple-testdoc.json");
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var serializer =
          new JsonSerializer.Builder(manager, writer).withMetaData(true).startNodeKey(3).prettyPrint().build();
      serializer.call();

      final var expected = Files.readString(expectedFor("simple-testdoc-withmetadata-withstartnodekey-objectkey.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000");

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndPrettyPrintingAndObjectStartNodeKey() throws Exception {
    final var jsonPath = JSON.resolve("simple-testdoc.json");
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      // iter#32 fusion: legacy key 4 was the inner OBJECT (head's value); under fusion the
      // OBJECT_KEY+OBJECT pair collapsed into OBJECT_NAMED_OBJECT at key 3. Starting on the
      // fused record reproduces the same logical "head's body" subtree.
      final var serializer =
          new JsonSerializer.Builder(manager, writer).withMetaData(true).startNodeKey(3).prettyPrint().build();
      serializer.call();

      final var expected = Files.readString(expectedFor("simple-testdoc-withmetadata-withstartnodekey-object.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000");

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndPrettyPrintingAndArrayStartNodeKey() throws Exception {
    final var jsonPath = JSON.resolve("simple-testdoc.json");
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      // iter#32 fusion: legacy key 6 was the inner ARRAY (test's value); under fusion the
      // OBJECT_KEY+ARRAY pair collapsed into OBJECT_NAMED_ARRAY at key 4. Starting on the
      // fused record reproduces the same logical "test's array body" subtree.
      final var serializer =
          new JsonSerializer.Builder(manager, writer).withMetaData(true).startNodeKey(4).prettyPrint().build();
      serializer.call();

      final var expected = Files.readString(expectedFor("simple-testdoc-withmetadata-withstartnodekey-array.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000");

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndPrettyPrintingAndObjectStartNodeKeyAndMaxLevelTwo() throws Exception {
    JsonTestHelper.createTestDocument();
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final Writer writer = new StringWriter()) {

      // iter#32 full fusion: "tada" OBJECT_KEY+ARRAY pair moved from legacy 15+16 -> 10
      // (single fused OBJECT_NAMED_ARRAY).
      final var serializer = new JsonSerializer.Builder(manager, writer).withMetaData(true)
                                                                        .startNodeKey(10)
                                                                        .maxLevel(2)
                                                                        .prettyPrint()
                                                                        .build();
      serializer.call();

      final var expected =
          Files.readString(expectedFor("test-withmetadata-withprettyprinting-withstartnodekey-withmaxlevel2.json"),
              StandardCharsets.UTF_8);
      final var actual = writer.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000");

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJsonDocumentWithNodeKeyMetadataAndPrettyPrintingAndObjectStartNodeKeyAndMaxLevel()
      throws Exception {
    JsonTestHelper.createTestDocument();
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final Writer writer = new StringWriter()) {
      // iter#32 full fusion: "tada" OBJECT_KEY+ARRAY pair moved from legacy 15+16 -> 10
      // (single fused OBJECT_NAMED_ARRAY).
      final var serializer = new JsonSerializer.Builder(manager, writer).withNodeKeyMetaData(true)
                                                                        .startNodeKey(10)
                                                                        .maxLevel(3)
                                                                        .prettyPrint()
                                                                        .build();
      serializer.call();

      final var expected = Files.readString(
          expectedFor("test-withnodekeymetadata-withprettyprinting-withstartnodekey-withmaxlevel.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJsonDocumentWithChildCountMetadataAndPrettyPrintingAndObjectStartNodeKeyAndMaxLevel()
      throws Exception {
    JsonTestHelper.createTestDocument();
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final Writer writer = new StringWriter()) {
      // iter#32 full fusion: "tada" OBJECT_KEY+ARRAY pair moved from legacy 15+16 -> 10
      // (single fused OBJECT_NAMED_ARRAY).
      final var serializer = new JsonSerializer.Builder(manager, writer).withNodeKeyAndChildCountMetaData(true)
                                                                        .startNodeKey(10)
                                                                        .maxLevel(3)
                                                                        .prettyPrint()
                                                                        .build();
      serializer.call();

      final var expected = Files.readString(
          expectedFor("test-withnodekeyandchildcountmetadata-withprettyprinting-withstartnodekey-withmaxlevel.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndPrettyPrintingAndObjectStartNodeKeyAndMaxLevelThree() throws Exception {
    JsonTestHelper.createTestDocument();
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final Writer writer = new StringWriter()) {
      // iter#32 full fusion: "tada" OBJECT_KEY+ARRAY pair moved from legacy 15+16 -> 10.
      final var serializer = new JsonSerializer.Builder(manager, writer).withMetaData(true)
                                                                        .startNodeKey(10)
                                                                        .maxLevel(3)
                                                                        .prettyPrint()
                                                                        .build();
      serializer.call();

      final var expected =
          Files.readString(expectedFor("test-withmetadata-withprettyprinting-withstartnodekey-withmaxlevel3.json"),
              StandardCharsets.UTF_8);
      final var actual = writer.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000");

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndMaxLevelSecondAndPrettyPrinting() throws Exception {
    final var jsonPath = JSON.resolve("simple-testdoc.json");
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var serializer =
          new JsonSerializer.Builder(manager, writer).withMetaData(true).maxLevel(3).prettyPrint().build();
      serializer.call();

      final var expected = Files.readString(
          expectedFor("simple-testdoc-withmetadata-withmaxlevel-withprettyprint.json"), StandardCharsets.UTF_8);
      final var actual = writer.toString().replaceAll("[0-9a-fA-F]{16}", "0000000000000000");

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMaxLevel() throws Exception {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      var serializedString = getSerializedStringWithMaxLevel(manager, 1);
      var expected = "{}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevel(manager, 2);
      expected = "{\"foo\":[],\"bar\":{},\"baz\":\"hello\",\"tada\":[]}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevel(manager, 3);
      expected =
          "{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{},{},\"boo\",{},[]]}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevel(manager, 4);
      expected =
          "{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";
      assertEquals(expected, serializedString);
    }
  }

  private String getSerializedStringWithMaxLevel(final JsonResourceSession manager, final int maxLevel)
      throws Exception {
    try (final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(maxLevel).build();
      serializer.call();

      return writer.toString();
    }
  }

  @Test
  public void testArrayWithNumberOfNodesEmitsExactLimit() throws Exception {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx();
        final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2]"));
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(manager, writer).numberOfNodes(2).build();
      serializer.call();

      assertEquals("[1]", writer.toString());
    }
  }

  @Test
  public void testJsonDocumentWithMaxLevelAndNumberOfNodes() throws Exception {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      var serializedString = getSerializedStringWithMaxLevelAndNumberOfNodes(manager, 2, 3);
      var expected = "{\"foo\":[]}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndNumberOfNodes(manager, 2, 4);
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndNumberOfNodes(manager, 2, 5);
      expected = "{\"foo\":[],\"bar\":{}}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndNumberOfNodes(manager, 2, 6);
      assertEquals(expected, serializedString);
    }
  }

  private String getSerializedStringWithMaxLevelAndNumberOfNodes(final JsonResourceSession manager, final int maxLevel,
      final int numberOfNodes) throws Exception {
    try (final Writer writer = new StringWriter()) {
      final var serializer =
          new JsonSerializer.Builder(manager, writer).maxLevel(maxLevel).numberOfNodes(numberOfNodes).build();
      serializer.call();

      return writer.toString();
    }
  }

  @Test
  public void testJsonDocumentWithMaxLevelAndStartNodeKey() throws Exception {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // iter#32 full fusion: every primitive-valued OBJECT_KEY collapses with its value
      // into a single fused OBJECT_NAMED_* record; structural-valued OBJECT_KEYs collapse
      // with their inner OBJECT/ARRAY into OBJECT_NAMED_OBJECT/OBJECT_NAMED_ARRAY. Result:
      //  1: root OBJECT
      //  2: "foo" OBJECT_NAMED_ARRAY  (legacy 2+3 collapsed)
      //  3: "bar" STRING_VALUE        (legacy 4)
      //  4: null  NULL_VALUE          (legacy 5)
      //  5: 2.33  NUMBER_VALUE        (legacy 6)
      //  6: "bar" OBJECT_NAMED_OBJECT (legacy 7+8 collapsed)
      //  7: "hello"  OBJECT_NAMED_STRING  (legacy 9+10)
      //  8: "helloo" OBJECT_NAMED_BOOLEAN (legacy 11+12)
      //  9: "baz"  OBJECT_NAMED_STRING (legacy 13+14)
      // 10: "tada" OBJECT_NAMED_ARRAY  (legacy 15+16)
      // 11: OBJECT first tada element  (legacy 17)
      // 12: "foo"  OBJECT_NAMED_STRING (legacy 18+19, inside element 11)
      // 13: OBJECT second tada element (legacy 20)
      // 14: "baz"  OBJECT_NAMED_BOOLEAN (legacy 21+22, inside element 13)
      // 15: "boo"  STRING_VALUE        (legacy 23, third element of tada)
      // 16: OBJECT fourth tada element (legacy 24)
      // 17: ARRAY  fifth tada element  (legacy 25)
      var serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 2);
      var expected = "{\"foo\":[]}";
      assertEquals(expected, serializedString);

      // bar OBJECT_NAMED_OBJECT moved 7 -> 6.
      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 6);
      expected = "{\"bar\":{}}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 2, 6);
      expected = "{\"bar\":{\"hello\":\"world\",\"helloo\":true}}";
      assertEquals(expected, serializedString);

      // {"foo":"bar"} OBJECT (first tada element) moved 17 -> 11.
      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 2, 11);
      expected = "{\"foo\":\"bar\"}";
      assertEquals(expected, serializedString);

      // tada OBJECT_NAMED_ARRAY moved 15 (OBJECT_KEY) / 16 (ARRAY) -> 10.
      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 2, 10);
      expected = "{\"tada\":[{},{},\"boo\",{},[]]}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 3, 10);
      expected = "{\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";
      assertEquals(expected, serializedString);

      // foo's array elements: "bar" 4 -> 3, null 5 -> 4, 2.33 6 -> 5.
      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 3);
      expected = "\"bar\"";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 4);
      expected = "null";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 5);
      expected = "2.33";
      assertEquals(expected, serializedString);
    }
  }

  private String getSerializedStringWithMaxLevelAndStartNodeKey(final JsonResourceSession manager, final int maxLevel,
      final int startNodeKey) throws Exception {
    try (final Writer writer = new StringWriter()) {
      final var serializer =
          new JsonSerializer.Builder(manager, writer).maxLevel(maxLevel).startNodeKey(startNodeKey).build();
      serializer.call();

      return writer.toString();
    }
  }

  @Test
  public void testJsonDocumentWithMaxLevelAndNumberOfNodesAndStartNodeKey() throws Exception {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = manager.beginNodeReadOnlyTrx()) {

      rtx.moveTo(2);
      final var level = rtx.getDeweyID().getLevel();

      var serializedString = getSerializedStringWithMaxLevelAndNumberOfNodesAndStartNodeKey(manager, 2, level + 1, 3);
      var expected = """
          {"foo":["bar"]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndNumberOfNodesAndStartNodeKey(manager, 2, level + 1, 4);
      expected = """
          {"foo":["bar",null]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndNumberOfNodesAndStartNodeKey(manager, 2, level + 1, 5);
      expected = """
          {"foo":["bar",null,2.33]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndNumberOfNodesAndStartNodeKey(manager, 2, level + 1, 6);
      expected = """
          {"foo":["bar",null,2.33]}
          """.trim();
      assertEquals(expected, serializedString);
    }
  }

  private String getSerializedStringWithMaxLevelAndNumberOfNodesAndStartNodeKey(final JsonResourceSession manager,
      final long startNodeKey, final int maxLevel, final int numberOfNodes) throws Exception {
    try (final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).startNodeKey(startNodeKey)
                                                                        .maxLevel(maxLevel)
                                                                        .numberOfNodes(numberOfNodes)
                                                                        .build();
      serializer.call();

      return writer.toString();
    }
  }

  @Test
  public void testJsonDocumentWithMaxLevelAndNumberOfNodesAndStartNodeKeyAndMaxChildren() throws Exception {
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = manager.beginNodeReadOnlyTrx()) {

      rtx.moveTo(2);
      final var level = rtx.getDeweyID().getLevel();

      var serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 2, level, 3, 1);
      var expected = """
          {"foo":["bar"]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 2, level, 4, 1);
      expected = """
          {"foo":["bar"]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 2, level, 5, 2);
      expected = """
          {"foo":["bar",null]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 2, level, 6, 3);
      expected = """
          {"foo":["bar",null,2.33]}
          """.trim();
      assertEquals(expected, serializedString);

      // With scheme B counting: Root(1) + foo+Array(2) + bar,null,2.33(3) = 6
      // bar+Object would add 2 more = 8, which exceeds 7
      serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 1, 3, 7, 3);
      expected = """
          {"foo":["bar",null,2.33]}
          """.trim();
      assertEquals(expected, serializedString);

      // With numberOfNodes=8, bar+Object (2) fits: 6 + 2 = 8 <= 8
      serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 1, 3, 8, 3);
      expected = """
          {"foo":["bar",null,2.33],"bar":{}}
          """.trim();
      assertEquals(expected, serializedString);
    }
  }

  private String getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(
      final JsonResourceSession manager, final long startNodeKey, final int maxLevel, final int numberOfNodes,
      final int maxChildren) throws Exception {
    try (final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).startNodeKey(startNodeKey)
                                                                        .maxLevel(maxLevel)
                                                                        .numberOfNodes(numberOfNodes)
                                                                        .maxChildren(maxChildren)
                                                                        .build();
      serializer.call();

      return writer.toString();
    }
  }
}
