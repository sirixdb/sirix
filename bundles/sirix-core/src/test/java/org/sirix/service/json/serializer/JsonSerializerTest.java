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
      "{\"foo\":{\"metadata\":{\"nodeKey\":2,\"hash\":87588092415439905136780802494009113124,\"descendantCount\":4},\"children\":[\"bar\",null,2.33]},\"bar\":{\"metadata\":{\"nodeKey\":7,\"hash\":7925123140688442101064701619077463369,\"descendantCount\":5},\"children\":{\"hello\":{\"metadata\":{\"nodeKey\":9,\"hash\":211455308309125107563287437944615474567,\"descendantCount\":1},\"children\":\"world\"},\"helloo\":{\"metadata\":{\"nodeKey\":11,\"hash\":175085324914471279448241792209155588623,\"descendantCount\":1},\"children\":true}}},\"baz\":{\"metadata\":{\"nodeKey\":13,\"hash\":16888007229856707439877543771731703081,\"descendantCount\":1},\"children\":\"hello\"},\"tada\":{\"metadata\":{\"nodeKey\":15,\"hash\":126864415688901173442808008598921089822,\"descendantCount\":10},\"children\":[{\"foo\":{\"metadata\":{\"nodeKey\":18,\"hash\":3914797534298766484033683061514042451,\"descendantCount\":1},\"children\":\"bar\"}},{\"baz\":{\"metadata\":{\"nodeKey\":21,\"hash\":232905588279019153068703577838761725617,\"descendantCount\":1},\"children\":false}},\"boo\",{},[]]}}";

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
      System.out.println(writer.toString());
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
      System.out.println(writer.toString());
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
      System.out.println(writer.toString());
      assertEquals(mJsonWithMetaData, writer.toString());
    }
  }
}
