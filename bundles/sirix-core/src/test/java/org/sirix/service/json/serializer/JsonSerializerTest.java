package org.sirix.service.json.serializer;

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
import org.sirix.exception.SirixException;
import org.sirix.service.json.serialize.JsonSerializer;
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
  public void testJsonDocumentWithMaxLevel() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE)) {
      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(1).build();
        serializer.call();

        final var expected = "{}";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(2).build();
        serializer.call();

        final var expected = "{\"foo\":[],\"bar\":{},\"baz\":\"hello\",\"tada\":[]}";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(3).build();
        serializer.call();

        final var expected =
            "{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{},{},\"boo\",{},[]]}";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(4).build();
        serializer.call();

        final var expected =
            "{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";
        assertEquals(expected, writer.toString());
      }
    }
  }

  @Test
  public void testJsonDocumentWithMaxLevelAndStartNodeKey() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE)) {
      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(1).startNodeKey(2).build();
        serializer.call();

        final var expected = "{\"foo\":[]}";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(1).startNodeKey(7).build();
        serializer.call();

        final var expected = "{\"bar\":{}}";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(2).startNodeKey(7).build();
        serializer.call();

        final var expected = "{\"bar\":{\"hello\":\"world\",\"helloo\":true}}";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(2).startNodeKey(17).build();
        serializer.call();

        final var expected = "{\"foo\":\"bar\"}";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(2).startNodeKey(16).build();
        serializer.call();

        final var expected = "[{},{},\"boo\",{},[]]";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(3).startNodeKey(16).build();
        serializer.call();

        final var expected = "[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(1).startNodeKey(3).build();
        serializer.call();

        final var expected = "[]";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(1).startNodeKey(4).build();
        serializer.call();

        final var expected = "\"bar\"";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(1).startNodeKey(5).build();
        serializer.call();

        final var expected = "null";
        assertEquals(expected, writer.toString());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).maxLevel(1).startNodeKey(6).build();
        serializer.call();

        final var expected = "2.33";
        assertEquals(expected, writer.toString());
      }
    }
  }
}
