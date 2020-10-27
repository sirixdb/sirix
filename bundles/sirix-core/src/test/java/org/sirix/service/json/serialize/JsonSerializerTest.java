package org.sirix.service.json.serialize;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.access.trx.node.json.objectvalue.StringValue;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.service.xml.shredder.InsertPosition;
import org.sirix.utils.JsonDocumentCreator;
import org.skyscreamer.jsonassert.JSONAssert;

public final class JsonSerializerTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
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
  public void testJsonDocumentPrettyPrinted() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).prettyPrint().build();
      serializer.call();
      final var expected = Files.readString(JSON.resolve("pretty-printed-test-doc.json"), StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, writer.toString(), true);
    }
  }

  @Test
  public void testJsonDocument() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).build();
      serializer.call();
      assertEquals(JsonDocumentCreator.JSON, writer.toString());
    }
  }

  @Test
  public void testMultipleRevisionsJsonDocument() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var writer = new StringWriter();
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveToDocumentRoot().trx().moveToFirstChild();
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
  public void testJsonDocumentWithMaxChildren() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE)) {
      var serializedString = getSerializedStringWithMaxChildren(manager, 1);
      var expected = Files.readString(JSON.resolve("jsonSerializer").resolve("document-with-1-maxChildren.json"),
          StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, serializedString, true);

      serializedString = getSerializedStringWithMaxChildren(manager, 2);
      expected = Files.readString(JSON.resolve("jsonSerializer").resolve("document-with-2-maxChildren.json"),
          StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, serializedString, true);

      serializedString = getSerializedStringWithMaxChildren(manager, 3);
      expected = Files.readString(JSON.resolve("jsonSerializer").resolve("document-with-3-maxChildren.json"),
          StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, serializedString, true);

      serializedString = getSerializedStringWithMaxChildren(manager, 4);
      expected = Files.readString(JSON.resolve("jsonSerializer").resolve("document-with-4-maxChildren.json"),
          StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expected, serializedString, true);
    }
  }

  private String getSerializedStringWithMaxChildren(final JsonResourceManager manager, final int maxChildren)
      throws IOException {
    try (final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).maxChildren(maxChildren).build();
      serializer.call();

      return writer.toString();
    }
  }

  @Test
  public void testJsonDocumentWithMetadata() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).withMetaData(true).build();
      serializer.call();

      final var expected = Files.readString(JSON.resolve("document-with-metadata.json"), StandardCharsets.UTF_8);
      final var actual = writer.toString();
      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndMaxLevelAndPrettyPrinting() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final Writer writer = new StringWriter()) {
      final var serializer =
          new JsonSerializer.Builder(manager, writer).withMetaData(true).maxLevel(2).prettyPrint().build();
      serializer.call();

      final var expected =
          Files.readString(JSON.resolve("testdoc-withmetadata-withmaxlevel.json"), StandardCharsets.UTF_8);
      final var actual = writer.toString();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndMaxLevelSecond() throws IOException {
    final var jsonPath = JSON.resolve("simple-testdoc.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var serializer = new JsonSerializer.Builder(manager, writer).withMetaData(true).maxLevel(2).build();
      serializer.call();

      final var expected =
          Files.readString(JSON.resolve("simple-testdoc-withmetadata-withmaxlevel.json"), StandardCharsets.UTF_8);
      final var actual = writer.toString();

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndPrettyPrintingAndObjectKeyStartNodeKey() throws IOException {
    final var jsonPath = JSON.resolve("simple-testdoc.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var serializer =
          new JsonSerializer.Builder(manager, writer).withMetaData(true).startNodeKey(3).prettyPrint().build();
      serializer.call();

      final var expected = Files.readString(JSON.resolve("simple-testdoc-withmetadata-withstartnodekey-objectkey.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString();

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndPrettyPrintingAndObjectStartNodeKey() throws IOException {
    final var jsonPath = JSON.resolve("simple-testdoc.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var serializer =
          new JsonSerializer.Builder(manager, writer).withMetaData(true).startNodeKey(4).prettyPrint().build();
      serializer.call();

      final var expected = Files.readString(JSON.resolve("simple-testdoc-withmetadata-withstartnodekey-object.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString();

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndPrettyPrintingAndArrayStartNodeKey() throws IOException {
    final var jsonPath = JSON.resolve("simple-testdoc.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var serializer =
          new JsonSerializer.Builder(manager, writer).withMetaData(true).startNodeKey(6).prettyPrint().build();
      serializer.call();

      final var expected = Files.readString(JSON.resolve("simple-testdoc-withmetadata-withstartnodekey-array.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString();

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndPrettyPrintingAndObjectStartNodeKeyAndMaxLevelTwo() throws IOException {
    JsonTestHelper.createTestDocument();
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final Writer writer = new StringWriter()) {

      final var serializer = new JsonSerializer.Builder(manager, writer).withMetaData(true)
                                                                        .startNodeKey(15)
                                                                        .maxLevel(2)
                                                                        .prettyPrint()
                                                                        .build();
      serializer.call();

      final var expected =
          Files.readString(JSON.resolve("test-withmetadata-withprettyprinting-withstartnodekey-withmaxlevel2.json"),
              StandardCharsets.UTF_8);
      final var actual = writer.toString();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJsonDocumentWithNodeKeyMetadataAndPrettyPrintingAndObjectStartNodeKeyAndMaxLevel()
      throws IOException {
    JsonTestHelper.createTestDocument();
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).withNodeKeyMetaData(true)
                                                                        .startNodeKey(15)
                                                                        .maxLevel(3)
                                                                        .prettyPrint()
                                                                        .build();
      serializer.call();

      final var expected = Files.readString(
          JSON.resolve("test-withnodekeymetadata-withprettyprinting-withstartnodekey-withmaxlevel.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJsonDocumentWithChildCountMetadataAndPrettyPrintingAndObjectStartNodeKeyAndMaxLevel()
      throws IOException {
    JsonTestHelper.createTestDocument();
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).withNodeKeyAndChildCountMetaData(true)
                                                                        .startNodeKey(15)
                                                                        .maxLevel(3)
                                                                        .prettyPrint()
                                                                        .build();
      serializer.call();

      final var expected = Files.readString(
          JSON.resolve("test-withnodekeyandchildcountmetadata-withprettyprinting-withstartnodekey-withmaxlevel.json"),
          StandardCharsets.UTF_8);
      final var actual = writer.toString();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndPrettyPrintingAndObjectStartNodeKeyAndMaxLevelThree() throws IOException {
    JsonTestHelper.createTestDocument();
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).withMetaData(true)
                                                                        .startNodeKey(15)
                                                                        .maxLevel(3)
                                                                        .prettyPrint()
                                                                        .build();
      serializer.call();

      final var expected =
          Files.readString(JSON.resolve("test-withmetadata-withprettyprinting-withstartnodekey-withmaxlevel3.json"),
              StandardCharsets.UTF_8);
      final var actual = writer.toString();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testJsonDocumentWithMetadataAndMaxLevelSecondAndPrettyPrinting() throws IOException {
    final var jsonPath = JSON.resolve("simple-testdoc.json");
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      final var serializer =
          new JsonSerializer.Builder(manager, writer).withMetaData(true).maxLevel(3).prettyPrint().build();
      serializer.call();

      final var expected =
          Files.readString(JSON.resolve("simple-testdoc-withmetadata-withmaxlevel-withprettyprint.json"),
              StandardCharsets.UTF_8);
      final var actual = writer.toString();

      JSONAssert.assertEquals(expected, actual, true);
    }
  }

  @Test
  public void testJsonDocumentWithMaxLevel() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE)) {
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

  private String getSerializedStringWithMaxLevel(final JsonResourceManager manager, final int maxLevel)
      throws IOException {
    try (final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(maxLevel).build();
      serializer.call();

      return writer.toString();
    }
  }

  @Test
  public void testJsonDocumentWithMaxLevelAndNumberOfNodes() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE)) {
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

  private String getSerializedStringWithMaxLevelAndNumberOfNodes(final JsonResourceManager manager, final int maxLevel,
      final int numberOfNodes) throws IOException {
    try (final Writer writer = new StringWriter()) {
      final var serializer =
          new JsonSerializer.Builder(manager, writer).maxLevel(maxLevel).numberOfNodes(numberOfNodes).build();
      serializer.call();

      return writer.toString();
    }
  }

  @Test
  public void testJsonDocumentWithMaxLevelAndStartNodeKey() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE)) {
      var serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 2);
      var expected = "{\"foo\":[]}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 7);
      expected = "{\"bar\":{}}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 2, 7);
      expected = "{\"bar\":{\"hello\":\"world\",\"helloo\":true}}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 2, 17);
      expected = "{\"foo\":\"bar\"}";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 2, 16);
      expected = "[{},{},\"boo\",{},[]]";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 3, 16);
      expected = "[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 3);
      expected = "[]";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 4);
      expected = "\"bar\"";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 5);
      expected = "null";
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndStartNodeKey(manager, 1, 6);
      expected = "2.33";
      assertEquals(expected, serializedString);
    }
  }

  private String getSerializedStringWithMaxLevelAndStartNodeKey(final JsonResourceManager manager, final int maxLevel,
      final int startNodeKey) throws IOException {
    try (final Writer writer = new StringWriter()) {
      final var serializer =
          new JsonSerializer.Builder(manager, writer).maxLevel(maxLevel).startNodeKey(startNodeKey).build();
      serializer.call();

      return writer.toString();
    }
  }

  @Test
  public void testJsonDocumentWithMaxLevelAndNumberOfNodesAndStartNodeKey() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx()) {

      rtx.moveTo(2);
      final var level = rtx.getDeweyID().getLevel();

      var serializedString = getSerializedStringWithMaxLevelAndNumberOfNodesAndStartNodeKey(manager, 2, level + 1, 3);
      var expected = """
          {"foo":[]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndNumberOfNodesAndStartNodeKey(manager, 2, level + 1, 4);
      expected = """
          {"foo":["bar"]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndNumberOfNodesAndStartNodeKey(manager, 2, level + 1, 5);
      expected = """
          {"foo":["bar",null]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString = getSerializedStringWithMaxLevelAndNumberOfNodesAndStartNodeKey(manager, 2, level + 1, 6);
      expected = """
          {"foo":["bar",null,2.33]}
          """.trim();
      assertEquals(expected, serializedString);
    }
  }

  private String getSerializedStringWithMaxLevelAndNumberOfNodesAndStartNodeKey(final JsonResourceManager manager,
      final long startNodeKey, final int maxLevel, final int numberOfNodes) throws IOException {
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
  public void testJsonDocumentWithMaxLevelAndNumberOfNodesAndStartNodeKeyAndMaxChildren() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx()) {

      rtx.moveTo(2);
      final var level = rtx.getDeweyID().getLevel();

      var serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 2, level + 1, 3, 1);
      var expected = """
          {"foo":[]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 2, level + 1, 4, 1);
      expected = """
          {"foo":["bar"]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 2, level + 1, 5, 2);
      expected = """
          {"foo":["bar",null]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 2, level + 1, 6, 3);
      expected = """
          {"foo":["bar",null,2.33]}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 1, 3, 7, 3);
      expected = """
          {"foo":["bar",null,2.33],"bar":{}}
          """.trim();
      assertEquals(expected, serializedString);

      serializedString =
          getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(manager, 1, 3, 8, 3);
      expected = """
          {"foo":["bar",null,2.33],"bar":{}}
          """.trim();
      assertEquals(expected, serializedString);
    }
  }

  private String getSerializedStringWithMaxLevelAndNumberOfNodesAndMaxChildrenAndStartNodeKey(
      final JsonResourceManager manager, final long startNodeKey, final int maxLevel, final int numberOfNodes,
      final int maxChildren) throws IOException {
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
