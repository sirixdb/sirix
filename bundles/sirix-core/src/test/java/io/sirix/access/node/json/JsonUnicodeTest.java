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
 * Tests for unicode and special character handling in JSON string values.
 * Verifies that CJK, emoji, RTL scripts, control characters, and other
 * special strings survive the insert-commit-serialize round-trip.
 */
public final class JsonUnicodeTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testCjkCharacters() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("你好世界");
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[\"你好世界\"]", writer.toString());
    }
  }

  @Test
  void testEmojiSurrogatePairs() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("Hello \uD83D\uDE00\uD83C\uDF89\uD83D\uDE80");
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[\"Hello \uD83D\uDE00\uD83C\uDF89\uD83D\uDE80\"]", writer.toString());
    }
  }

  @Test
  void testRtlScript() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("\u0645\u0631\u062D\u0628\u0627");
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[\"\u0645\u0631\u062D\u0628\u0627\"]", writer.toString());
    }
  }

  @Test
  void testZeroWidthChars() {
    final var zeroWidthStr = "a\u200Bb\u200Cc\u200Dd";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild(zeroWidthStr);
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();
        rtx.moveToFirstChild();
        assertEquals(zeroWidthStr, rtx.getValue());
      }
    }
  }

  @Test
  void testControlCharsTabNewline() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("line1\tcolumn2\nline2");
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();
        rtx.moveToFirstChild();
        assertEquals("line1\tcolumn2\nline2", rtx.getValue());
      }
    }
  }

  @Test
  void testEmptyString() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("");
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[\"\"]", writer.toString());
    }
  }

  @Test
  void testUnicodeObjectKeys() throws IOException {
    final var json = "{\"日本語\":\"value\"}";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals(json, writer.toString());
    }
  }

  @Test
  void testJsonEscapeSequences() throws IOException {
    final var json = "[\"hello\\nworld\",\"tab\\there\",\"quote\\\"inside\",\"back\\\\slash\"]";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();
        rtx.moveToFirstChild();
        assertEquals("hello\nworld", rtx.getValue());

        rtx.moveToRightSibling();
        assertEquals("tab\there", rtx.getValue());

        rtx.moveToRightSibling();
        assertEquals("quote\"inside", rtx.getValue());

        rtx.moveToRightSibling();
        assertEquals("back\\slash", rtx.getValue());
      }
    }
  }

  @Test
  void testMixedScripts() throws IOException {
    final var mixed = "Hello\u4F60\u597D\u0645\u0631\u062D\u0628\u0627\u0410\u043B\u043B\u043E";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild(mixed);
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals("[\"" + mixed + "\"]", writer.toString());
    }
  }

  @Test
  void testSetStringValueWithUnicode() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("initial");
      wtx.commit();

      final long nodeKey = wtx.getNodeKey();

      wtx.moveTo(nodeKey);
      wtx.setStringValue("\u4F60\u597D\u4E16\u754C\uD83D\uDE00");
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveTo(nodeKey);
        assertEquals("\u4F60\u597D\u4E16\u754C\uD83D\uDE00", rtx.getValue());
      }
    }
  }

  @Test
  void testLongString10KB() {
    final var sb = new StringBuilder(10 * 1024);
    for (int i = 0; i < 10 * 1024; i++) {
      sb.append((char) ('A' + (i % 26)));
    }
    final var longStr = sb.toString();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild(longStr);
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();
        rtx.moveToFirstChild();
        assertEquals(longStr, rtx.getValue());
      }
    }
  }

  @Test
  void testLongString100KB() {
    final var sb = new StringBuilder(100 * 1024);
    for (int i = 0; i < 100 * 1024; i++) {
      sb.append((char) ('a' + (i % 26)));
    }
    final var longStr = sb.toString();

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild(longStr);
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();
        rtx.moveToFirstChild();
        assertEquals(longStr, rtx.getValue());
      }
    }
  }

  @Test
  void testUnicodeViaShredder() throws IOException {
    final var json = "{\"greeting\":\"\u4F60\u597D\",\"emoji\":\"\uD83D\uDE00\uD83C\uDF89\",\"arabic\":\"\u0645\u0631\u062D\u0628\u0627\"}";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals(json, writer.toString());
    }
  }

  @Test
  void testSpecialJsonChars() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("quote\"here");
      wtx.moveToParent();
      wtx.insertStringValueAsFirstChild("back\\slash");
      wtx.moveToParent();
      wtx.insertStringValueAsFirstChild("forward/slash");
      wtx.commit();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();
        rtx.moveToFirstChild();
        assertEquals("forward/slash", rtx.getValue());
        assertTrue(rtx.moveToRightSibling());
        assertEquals("back\\slash", rtx.getValue());
        assertTrue(rtx.moveToRightSibling());
        assertEquals("quote\"here", rtx.getValue());
      }
    }
  }

  @Test
  void testMultipleUnicodeStringsInArray() throws IOException {
    final var json = "[\"\u4F60\u597D\",\"\u0645\u0631\u062D\u0628\u0627\",\"\u0410\u043B\u043B\u043E\",\"\u3053\u3093\u306B\u3061\u306F\",\"\uC548\uB155\uD558\uC138\uC694\"]";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx();
         final Writer writer = new StringWriter()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
      wtx.commit();

      final var serializer = new JsonSerializer.Builder(session, writer).build();
      serializer.call();

      assertEquals(json, writer.toString());
    }
  }
}
