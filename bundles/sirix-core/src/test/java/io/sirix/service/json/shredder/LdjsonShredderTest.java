/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package io.sirix.service.json.shredder;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonNodeTrx.LdjsonResult;
import io.sirix.exception.SirixUsageException;
import io.sirix.node.NodeKind;
import io.sirix.service.json.serialize.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for LDJSON (line-delimited JSON) shredding via
 * {@link JsonNodeTrx#insertLdjsonAsFirstChild} and {@link JsonNodeTrx#insertLdjsonAsLastChild}.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class LdjsonShredderTest {

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  // ==================== Empty Input ====================

  @Test
  public void testEmptyInput() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser("")) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(0, result.documentCount());
      assertTrue(result.arrayNodeKey() > 0);

      // The wrapper array should exist and be empty
      trx.moveTo(result.arrayNodeKey());
      assertEquals(NodeKind.ARRAY, trx.getKind());
      assertEquals(0, trx.getChildCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[]", writer.toString(), true);
      }
    }
  }

  // ==================== Single Document ====================

  @Test
  public void testSingleObject() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser("{\"name\":\"Alice\",\"age\":30}")) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(1, result.documentCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[{\"name\":\"Alice\",\"age\":30}]", writer.toString(), true);
      }
    }
  }

  @Test
  public void testSingleArray() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser("[1,2,3]")) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(1, result.documentCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[[1,2,3]]", writer.toString(), true);
      }
    }
  }

  // ==================== Multiple Documents ====================

  @Test
  public void testTwoObjects() throws IOException {
    final var ldjson = "{\"a\":1}\n{\"b\":2}";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(2, result.documentCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[{\"a\":1},{\"b\":2}]", writer.toString(), true);
      }
    }
  }

  @Test
  public void testThreeObjects() throws IOException {
    final var ldjson = "{\"x\":1}\n{\"y\":2}\n{\"z\":3}";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(3, result.documentCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[{\"x\":1},{\"y\":2},{\"z\":3}]", writer.toString(), true);
      }
    }
  }

  @Test
  public void testManyDocuments() throws IOException {
    final var sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      if (i > 0) {
        sb.append('\n');
      }
      sb.append("{\"id\":").append(i).append('}');
    }
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(sb.toString())) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(1000, result.documentCount());

      // Verify the wrapper array has 1000 children
      trx.moveTo(result.arrayNodeKey());
      assertEquals(NodeKind.ARRAY, trx.getKind());
      assertEquals(1000, trx.getChildCount());
    }
  }

  // ==================== Nested Structures ====================

  @Test
  public void testNestedObjects() throws IOException {
    final var ldjson = "{\"a\":{\"b\":{\"c\":1}}}\n{\"d\":[1,[2,3]]}";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(2, result.documentCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[{\"a\":{\"b\":{\"c\":1}}},{\"d\":[1,[2,3]]}]", writer.toString(), true);
      }
    }
  }

  // ==================== Mixed Container Types ====================

  @Test
  public void testMixedObjectsAndArrays() throws IOException {
    final var ldjson = "{\"a\":1}\n[1,2]\n{\"b\":2}\n[3,4]";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(4, result.documentCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[{\"a\":1},[1,2],{\"b\":2},[3,4]]", writer.toString(), true);
      }
    }
  }

  // ==================== Trailing Newlines ====================

  @Test
  public void testTrailingNewlines() throws IOException {
    final var ldjson = "{\"a\":1}\n{\"b\":2}\n\n\n";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(2, result.documentCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[{\"a\":1},{\"b\":2}]", writer.toString(), true);
      }
    }
  }

  // ==================== Tree Structure Verification ====================

  @Test
  public void testTreeStructure() throws IOException {
    final var ldjson = "{\"k\":\"v\"}\n{\"k2\":\"v2\"}";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);

      // Wrapper array
      trx.moveTo(result.arrayNodeKey());
      assertEquals(NodeKind.ARRAY, trx.getKind());
      assertEquals(2, trx.getChildCount());

      // First child: first object
      trx.moveToFirstChild();
      assertEquals(NodeKind.OBJECT, trx.getKind());
      final long firstObjKey = trx.getNodeKey();

      // First object has child object key "k"
      trx.moveToFirstChild();
      assertEquals(NodeKind.OBJECT_KEY, trx.getKind());

      // Right sibling of first object: second object
      trx.moveTo(firstObjKey);
      trx.moveToRightSibling();
      assertEquals(NodeKind.OBJECT, trx.getKind());

      // Second object has child object key "k2"
      trx.moveToFirstChild();
      assertEquals(NodeKind.OBJECT_KEY, trx.getKind());

      // No more siblings after second object
      trx.moveToParent(); // back to second object
      assertEquals(false, trx.hasRightSibling());
    }
  }

  // ==================== insertLdjsonAsLastChild ====================

  @Test
  public void testInsertAsLastChild() throws IOException {
    final var ldjson = "{\"a\":1}\n{\"b\":2}";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsLastChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(2, result.documentCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[{\"a\":1},{\"b\":2}]", writer.toString(), true);
      }
    }
  }

  // ==================== Auto-Commit Integration ====================

  @Test
  public void testAutoCommit() throws IOException {
    final var ldjson = buildLdjson(500);
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx(100);
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.NO);
      assertEquals(500, result.documentCount());
      trx.commit();

      trx.moveTo(result.arrayNodeKey());
      assertEquals(NodeKind.ARRAY, trx.getKind());
      assertEquals(500, trx.getChildCount());
    }
  }

  // ==================== Scalar Rejection ====================

  @Test
  public void testScalarStringRejected() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser("\"hello\"")) {
      assertThrows(SirixUsageException.class, () -> trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT));
      trx.rollback();
    }
  }

  @Test
  public void testScalarNumberRejected() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser("42")) {
      assertThrows(SirixUsageException.class, () -> trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT));
      trx.rollback();
    }
  }

  @Test
  public void testScalarBooleanRejected() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser("true")) {
      assertThrows(SirixUsageException.class, () -> trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT));
      trx.rollback();
    }
  }

  @Test
  public void testScalarNullRejected() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser("null")) {
      assertThrows(SirixUsageException.class, () -> trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT));
      trx.rollback();
    }
  }

  @Test
  public void testScalarAfterObjectRejected() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser("{\"a\":1}\n42")) {
      assertThrows(SirixUsageException.class, () -> trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT));
      trx.rollback();
    }
  }

  // ==================== Parent Node Validation ====================

  @Test
  public void testInsertUnderArrayParent() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx()) {
      // First insert a root array
      try (final var arrayParser = JacksonJsonShredder.createStringParser("[1]")) {
        trx.insertSubtreeAsFirstChild(arrayParser);
      }
      // Now move to the array node and insert LDJSON as last child
      trx.moveTo(1);
      assertEquals(NodeKind.ARRAY, trx.getKind());

      try (final var parser = JacksonJsonShredder.createStringParser("{\"a\":1}\n{\"b\":2}")) {
        final var result = trx.insertLdjsonAsLastChild(parser, JsonNodeTrx.Commit.IMPLICIT);
        assertEquals(2, result.documentCount());
      }

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        // The existing array [1] now has the LDJSON wrapper array as a last child
        JSONAssert.assertEquals("[1,[{\"a\":1},{\"b\":2}]]", writer.toString(), true);
      }
    }
  }

  // ==================== Round-Trip Test ====================

  @Test
  public void testRoundTripComplexDocuments() throws IOException {
    final var ldjson = "{\"users\":[{\"name\":\"Alice\",\"tags\":[\"admin\",\"user\"]}]}\n"
        + "{\"users\":[{\"name\":\"Bob\",\"tags\":[\"user\"]}]}\n"
        + "{\"meta\":{\"count\":2,\"active\":true}}";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(3, result.documentCount());

      final var expected = "[{\"users\":[{\"name\":\"Alice\",\"tags\":[\"admin\",\"user\"]}]},"
          + "{\"users\":[{\"name\":\"Bob\",\"tags\":[\"user\"]}]},"
          + "{\"meta\":{\"count\":2,\"active\":true}}]";

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals(expected, writer.toString(), true);
      }
    }
  }

  // ==================== Builder Validation ====================

  @Test
  public void testLdjsonModeIncompatibleWithSkipRootToken() throws IOException {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser("{}")) {
      assertThrows(SirixUsageException.class, () -> {
        new JacksonJsonShredder.Builder(trx, parser, io.sirix.service.InsertPosition.AS_FIRST_CHILD)
            .skipRootJsonToken()
            .ldjsonMode()
            .build();
      });
    }
  }

  // ==================== Empty Objects/Arrays ====================

  @Test
  public void testEmptyObjects() throws IOException {
    final var ldjson = "{}\n{}\n{}";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(3, result.documentCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[{},{},{}]", writer.toString(), true);
      }
    }
  }

  @Test
  public void testEmptyArrayDocuments() throws IOException {
    final var ldjson = "[]\n[]\n[]";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.IMPLICIT);
      assertEquals(3, result.documentCount());

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[[],[],[]]", writer.toString(), true);
      }
    }
  }

  // ==================== Commit.NO Semantics ====================

  @Test
  public void testCommitNo() throws IOException {
    final var ldjson = "{\"a\":1}\n{\"b\":2}";
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = manager.beginNodeTrx();
        final var parser = JacksonJsonShredder.createStringParser(ldjson)) {
      final var result = trx.insertLdjsonAsFirstChild(parser, JsonNodeTrx.Commit.NO);
      assertEquals(2, result.documentCount());
      trx.commit();

      try (final Writer writer = new StringWriter()) {
        final var serializer = new JsonSerializer.Builder(manager, writer).build();
        serializer.call();
        JSONAssert.assertEquals("[{\"a\":1},{\"b\":2}]", writer.toString(), true);
      }
    }
  }

  // ==================== Helper Methods ====================

  private static String buildLdjson(final int count) {
    final var sb = new StringBuilder(count * 16);
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        sb.append('\n');
      }
      sb.append("{\"id\":").append(i).append('}');
    }
    return sb.toString();
  }
}
