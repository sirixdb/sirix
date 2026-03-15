package io.sirix.service.json.serialize;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.io.Writer;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trip tests for JSON serialization: shred JSON string into database,
 * commit, serialize back, and compare with original.
 */
public final class JsonSerializationRoundTripTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testRoundTripEmptyObject() throws Exception {
    assertRoundTrip("{}");
  }

  @Test
  void testRoundTripEmptyArray() throws Exception {
    assertRoundTrip("[]");
  }

  @Test
  void testRoundTripNestedEmptyStructures() throws Exception {
    assertRoundTrip("{\"a\":{},\"b\":[]}");
  }

  @Test
  void testRoundTripAllValueTypes() throws Exception {
    assertRoundTrip("[1,\"hello\",true,false,null,3.14]");
  }

  @Test
  void testRoundTripDeepNesting20Levels() throws Exception {
    final int depth = 20;
    final var sb = new StringBuilder(depth * 2);
    for (int i = 0; i < depth; i++) {
      sb.append('[');
    }
    sb.append("1");
    for (int i = 0; i < depth; i++) {
      sb.append(']');
    }
    assertRoundTrip(sb.toString());
  }

  @Test
  void testRoundTripDeepNesting50Levels() throws Exception {
    final int depth = 50;
    final var sb = new StringBuilder(depth * 32);
    for (int i = 0; i < depth; i++) {
      sb.append("{\"k").append(i).append("\":");
    }
    sb.append("\"leaf\"");
    for (int i = 0; i < depth; i++) {
      sb.append('}');
    }
    assertRoundTrip(sb.toString());
  }

  @Test
  void testRoundTripLargeArray100Elements() throws Exception {
    final var sb = new StringBuilder(512);
    sb.append('[');
    for (int i = 0; i < 100; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(i);
    }
    sb.append(']');
    assertRoundTrip(sb.toString());
  }

  @Test
  void testRoundTripLargeObject50Keys() throws Exception {
    final var sb = new StringBuilder(1024);
    sb.append('{');
    for (int i = 0; i < 50; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("\"key").append(i).append("\":").append(i);
    }
    sb.append('}');
    assertRoundTrip(sb.toString());
  }

  @Test
  void testRoundTripMixedNestedDoc() throws Exception {
    final String json =
        "{\"name\":\"test\",\"active\":true,\"count\":42,\"rate\":3.14,\"tags\":[\"a\",\"b\",\"c\"],"
            + "\"nested\":{\"x\":1,\"y\":null,\"z\":false},\"matrix\":[[1,2],[3,4]]}";
    assertRoundTrip(json);
  }

  @Test
  void testRoundTripAfterModification() throws Exception {
    final String initialJson = "{\"a\":\"original\",\"b\":100}";
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(initialJson));
      wtx.commit();

      // Modify: navigate to "a" value node and change it
      wtx.moveTo(1); // object node
      wtx.moveToFirstChild(); // "a" key
      wtx.moveToFirstChild(); // "original" value
      wtx.setStringValue("modified");
      wtx.commit();

      final Writer writer = new StringWriter();
      new JsonSerializer.Builder(session, writer).build().call();
      final String result = writer.toString();

      assertEquals("{\"a\":\"modified\",\"b\":100}", result);
    }
  }

  @Test
  void testRoundTripTopLevelString() throws Exception {
    // Top-level scalars are not supported by insertSubtreeAsFirstChild
    // (requires array or object root), so we wrap in an array.
    assertRoundTrip("[\"hello\"]");
  }

  @Test
  void testRoundTripTopLevelNumber() throws Exception {
    assertRoundTrip("[42]");
  }

  @Test
  void testRoundTripTopLevelBoolean() throws Exception {
    assertRoundTrip("[true]");
  }

  @Test
  void testRoundTripTopLevelNull() throws Exception {
    assertRoundTrip("[null]");
  }

  @Test
  void testRoundTripComplexRealWorldLike() throws Exception {
    final String json =
        "{\"user\":{\"id\":12345,\"name\":\"John Doe\",\"email\":\"john@example.com\","
            + "\"active\":true,\"score\":98.6,\"metadata\":null,"
            + "\"addresses\":[{\"type\":\"home\",\"street\":\"123 Main St\","
            + "\"city\":\"Springfield\",\"zip\":\"62701\"},"
            + "{\"type\":\"work\",\"street\":\"456 Oak Ave\","
            + "\"city\":\"Shelbyville\",\"zip\":\"62702\"}],"
            + "\"orders\":[{\"orderId\":1001,\"items\":[{\"sku\":\"A1\",\"qty\":2,\"price\":9.99},"
            + "{\"sku\":\"B2\",\"qty\":1,\"price\":24.5}],\"total\":44.48},"
            + "{\"orderId\":1002,\"items\":[{\"sku\":\"C3\",\"qty\":3,\"price\":5.0}],"
            + "\"total\":15.0}],\"preferences\":{\"theme\":\"dark\","
            + "\"notifications\":true,\"languages\":[\"en\",\"de\"]}}}";
    assertRoundTrip(json);
  }

  /**
   * Shreds the given compact JSON string into the database, commits,
   * serializes it back, and asserts the output matches the input.
   *
   * @param json compact JSON string (no extra whitespace)
   */
  private void assertRoundTrip(final String json) throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
      wtx.commit();

      final Writer writer = new StringWriter();
      new JsonSerializer.Builder(session, writer).build().call();
      final String result = writer.toString();

      assertEquals(json, result);
    }
  }
}
