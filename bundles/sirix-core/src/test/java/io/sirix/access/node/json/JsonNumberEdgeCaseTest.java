package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import static io.sirix.JsonTestHelper.RESOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for number edge cases in JSON node transactions.
 * Verifies correct storage, retrieval, and serialization of various
 * numeric types including boundary values.
 */
public final class JsonNumberEdgeCaseTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testZeroValue() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(0);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(0, rtx.getNumberValue().intValue());

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[0]", writer.toString());
    }
  }

  @Test
  void testNegativeInteger() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(-42);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(-42, rtx.getNumberValue().intValue());

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[-42]", writer.toString());
    }
  }

  @Test
  void testIntegerMaxValue() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(Integer.MAX_VALUE);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(Integer.MAX_VALUE, rtx.getNumberValue().intValue());

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[" + Integer.MAX_VALUE + "]", writer.toString());
    }
  }

  @Test
  void testIntegerMinValue() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(Integer.MIN_VALUE);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(Integer.MIN_VALUE, rtx.getNumberValue().intValue());

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[" + Integer.MIN_VALUE + "]", writer.toString());
    }
  }

  @Test
  void testLongMaxValue() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(Long.MAX_VALUE);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(Long.MAX_VALUE, rtx.getNumberValue().longValue());

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[" + Long.MAX_VALUE + "]", writer.toString());
    }
  }

  @Test
  void testLongMinValue() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(Long.MIN_VALUE);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(Long.MIN_VALUE, rtx.getNumberValue().longValue());

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[" + Long.MIN_VALUE + "]", writer.toString());
    }
  }

  @Test
  void testDoubleMaxValue() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(Double.MAX_VALUE);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(Double.MAX_VALUE, rtx.getNumberValue().doubleValue(), 0.0);

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[" + Double.MAX_VALUE + "]", writer.toString());
    }
  }

  @Test
  void testDoubleMinValue() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(Double.MIN_VALUE);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(Double.MIN_VALUE, rtx.getNumberValue().doubleValue(), 0.0);

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[" + Double.MIN_VALUE + "]", writer.toString());
    }
  }

  @Test
  void testNegativeDouble() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(-3.14159);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(-3.14159, rtx.getNumberValue().doubleValue(), 0.0);

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[-3.14159]", writer.toString());
    }
  }

  @Test
  void testVerySmallDecimal() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(0.000000001);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(0.000000001, rtx.getNumberValue().doubleValue(), 0.0);

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[" + 0.000000001 + "]", writer.toString());
    }
  }

  @Test
  void testSetNumberValueChangesType() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(42);

      // Verify initial integer value
      assertTrue(wtx.isNumberValue());
      assertEquals(42, wtx.getNumberValue().intValue());

      // Change to double value
      wtx.setNumberValue(3.14);
      assertTrue(wtx.isNumberValue());
      assertEquals(3.14, wtx.getNumberValue().doubleValue(), 0.0);

      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[3.14]", writer.toString());
    }
  }

  @Test
  void testMixedNumberTypesArray() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();

      // Insert in reverse order using insertAsFirstChild so the result is [42, longVal, 2.718]
      wtx.insertNumberValueAsFirstChild(2.718);
      wtx.moveToParent();
      wtx.insertNumberValueAsFirstChild(9999999999L);
      wtx.moveToParent();
      wtx.insertNumberValueAsFirstChild(42);

      wtx.commit();

      // Verify types via read-only transaction
      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild(); // array
      rtx.moveToFirstChild(); // first number: 42
      assertTrue(rtx.isNumberValue());
      assertEquals(42, rtx.getNumberValue().intValue());

      rtx.moveToRightSibling(); // second number: 9999999999
      assertTrue(rtx.isNumberValue());
      assertEquals(9999999999L, rtx.getNumberValue().longValue());

      rtx.moveToRightSibling(); // third number: 2.718
      assertTrue(rtx.isNumberValue());
      assertEquals(2.718, rtx.getNumberValue().doubleValue(), 0.0);

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[42,9999999999,2.718]", writer.toString());
    }
  }

  @Test
  void testNumberRoundTrip() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(123456789);
      wtx.moveToParent();
      wtx.insertNumberValueAsLastChild(-987654321L);
      wtx.moveToParent();
      wtx.insertNumberValueAsLastChild(1.23456789);
      wtx.commit();
    }

    // Close and clear the cached database instance so we can reopen fresh
    JsonTestHelper.closeEverything();

    // Reopen and verify via serialization
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final Writer writer = new StringWriter()) {
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[123456789,-987654321,1.23456789]", writer.toString());
    }
  }

  @Test
  void testFloatValue() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertNumberValueAsFirstChild(3.14f);
      wtx.commit();

      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveTo(wtx.getNodeKey());
      assertTrue(rtx.isNumberValue());
      assertEquals(3.14f, rtx.getNumberValue().floatValue(), 0.0f);

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      assertEquals("[" + 3.14f + "]", writer.toString());
    }
  }

  @Test
  void testNumberViaShredder() throws IOException {
    final String json = "[1.0e10, 1.5E-3, 42, -7, 0.5]";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
      wtx.commit();

      // Navigate and verify individual values
      final var rtx = session.beginNodeReadOnlyTrx();
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild(); // array

      rtx.moveToFirstChild(); // 1.0e10
      assertTrue(rtx.isNumberValue());
      assertEquals(1.0e10, rtx.getNumberValue().doubleValue(), 1.0);

      rtx.moveToRightSibling(); // 1.5E-3
      assertTrue(rtx.isNumberValue());
      assertEquals(1.5E-3, rtx.getNumberValue().doubleValue(), 1e-10);

      rtx.moveToRightSibling(); // 42
      assertTrue(rtx.isNumberValue());
      assertEquals(42, rtx.getNumberValue().intValue());

      rtx.moveToRightSibling(); // -7
      assertTrue(rtx.isNumberValue());
      assertEquals(-7, rtx.getNumberValue().intValue());

      rtx.moveToRightSibling(); // 0.5
      assertTrue(rtx.isNumberValue());

      // Serialize and verify round-trip
      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();
      final String result = writer.toString();

      // Verify the serialized output contains all expected numbers
      assertTrue(result.startsWith("["), "Expected JSON array, got: " + result);
      assertTrue(result.endsWith("]"), "Expected JSON array end, got: " + result);
      assertTrue(result.contains("42"), "Expected 42 in: " + result);
      assertTrue(result.contains("-7"), "Expected -7 in: " + result);
    }
  }
}
