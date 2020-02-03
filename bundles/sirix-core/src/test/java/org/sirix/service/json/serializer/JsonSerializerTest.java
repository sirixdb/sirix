package org.sirix.service.json.serializer;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.access.trx.node.json.objectvalue.StringValue;
import org.sirix.exception.SirixException;
import org.sirix.service.json.serialize.JsonSerializer;
import org.sirix.utils.JsonDocumentCreator;

public final class JsonSerializerTest {
  private static final String mJson =
      "{\"sirix\":[{\"revisionNumber\":1,\"revision\":{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}},{\"revisionNumber\":2,\"revision\":{\"tadaaa\":\"todooo\",\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}}]}";
  private static final String mJsonWithMetaData =
      "{\"metadata\":{\"nodeKey\":1,\"hash\":265733614641353216334192177030989802675,\"type\":\"OBJECT\",\"descendantCount\":24,\"childCount\":4},\"value\":[{\"key\":\"foo\",\"metadata\":{\"nodeKey\":2,\"hash\":87588092415439905136780802494009113124,\"descendantCount\":4},\"value\":{\"metadata\":{\"nodeKey\":3,\"hash\":244764497419664092925804987471584412159,\"type\":\"ARRAY\",\"descendantCount\":3,\"childCount\":3},\"value\":[{\"metadata\":{\"nodeKey\":4,\"hash\":43123621054888476341947620158447110518,\"type\":\"STRING_VALUE\"},\"value\":\"bar\"},{\"metadata\":{\"nodeKey\":5,\"hash\":228744900773092343581338760875554577454,\"type\":\"NULL_VALUE\"},\"value\":null},{\"metadata\":{\"nodeKey\":6,\"hash\":57693461871242985379749256258219263973,\"type\":\"NUMBER_VALUE\"},\"value\":2.33}]}},{\"key\":\"bar\",\"metadata\":{\"nodeKey\":7,\"hash\":7925123140688442101064701619077463369,\"descendantCount\":5},\"value\":{\"metadata\":{\"nodeKey\":8,\"hash\":205218170791476174913423894685762663916,\"type\":\"OBJECT\",\"descendantCount\":4,\"childCount\":2},\"value\":[{\"key\":\"hello\",\"metadata\":{\"nodeKey\":9,\"hash\":211455308309125107563287437944615474567,\"descendantCount\":1},\"value\":{\"metadata\":{\"nodeKey\":10,\"hash\":10169020970736956437540285487462263757,\"type\":\"STRING_VALUE\"},\"value\":\"world\"}},{\"key\":\"helloo\",\"metadata\":{\"nodeKey\":11,\"hash\":175085324914471279448241792209155588623,\"descendantCount\":1},\"value\":{\"metadata\":{\"nodeKey\":12,\"hash\":183967672696861437421468697841748109131,\"type\":\"BOOLEAN_VALUE\"},\"value\":true}}]}},{\"key\":\"baz\",\"metadata\":{\"nodeKey\":13,\"hash\":16888007229856707439877543771731703081,\"descendantCount\":1},\"value\":{\"metadata\":{\"nodeKey\":14,\"hash\":74887500757223809897082915624104181894,\"type\":\"STRING_VALUE\"},\"value\":\"hello\"}},{\"key\":\"tada\",\"metadata\":{\"nodeKey\":15,\"hash\":126864415688901173442808008598921089822,\"descendantCount\":10},\"value\":{\"metadata\":{\"nodeKey\":16,\"hash\":134129365758417052208347420546217650660,\"type\":\"ARRAY\",\"descendantCount\":9,\"childCount\":5},\"value\":[{\"metadata\":{\"nodeKey\":17,\"hash\":153666527387686514098358391758869655546,\"type\":\"OBJECT\",\"descendantCount\":2,\"childCount\":1},\"value\":[{\"key\":\"foo\",\"metadata\":{\"nodeKey\":18,\"hash\":3914797534298766484033683061514042451,\"descendantCount\":1},\"value\":{\"metadata\":{\"nodeKey\":19,\"hash\":53672668317813595267798645916889119646,\"type\":\"STRING_VALUE\"},\"value\":\"bar\"}}]},{\"metadata\":{\"nodeKey\":20,\"hash\":206178738362136863275169953919675494156,\"type\":\"OBJECT\",\"descendantCount\":2,\"childCount\":1},\"value\":[{\"key\":\"baz\",\"metadata\":{\"nodeKey\":21,\"hash\":232905588279019153068703577838761725617,\"descendantCount\":1},\"value\":{\"metadata\":{\"nodeKey\":22,\"hash\":224947326041181808720699945958599690628,\"type\":\"BOOLEAN_VALUE\"},\"value\":false}}]},{\"metadata\":{\"nodeKey\":23,\"hash\":22439358804012550367149504983891574382,\"type\":\"STRING_VALUE\"},\"value\":\"boo\"},{\"metadata\":{\"nodeKey\":24,\"hash\":6942659529142876751396473522407258578,\"type\":\"OBJECT\",\"descendantCount\":0,\"childCount\":0},\"value\":{}},{\"metadata\":{\"nodeKey\":25,\"hash\":324772884752533378938483264104493411865,\"type\":\"ARRAY\",\"descendantCount\":0,\"childCount\":0},\"value\":[]}]}}]}";

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
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
      assertEquals(mJson, writer.toString());
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
//      System.out.println(writer.toString());
      assertEquals(mJsonWithMetaData, writer.toString());
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
