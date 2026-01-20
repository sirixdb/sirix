package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.JsonTestHelper;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Smoke tests for GraalVM native image compilation.
 * These tests verify that core functionality works correctly when compiled to native image.
 *
 * <p>Run with: {@code ./gradlew :sirix-query:nativeTest}
 */
@Tag("native-image")
@DisplayName("Native Image Smoke Tests")
public class NativeImageSmokeTest {

  private Path sirixPath;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    sirixPath = JsonTestHelper.PATHS.PATH1.getFile();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  @DisplayName("Basic arithmetic query")
  void testBasicArithmetic() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final var query = "1 + 1";
      final var seq = new Query(chain, query).evaluate(ctx);

      assertNotNull(seq);
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("2", buf.toString());
    }
  }

  @Test
  @DisplayName("String manipulation query")
  void testStringManipulation() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final var query = "concat('Hello', ' ', 'World')";
      final var seq = new Query(chain, query).evaluate(ctx);

      assertNotNull(seq);
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("Hello World", buf.toString());
    }
  }

  @Test
  @DisplayName("JSON store and retrieve")
  void testJsonStoreAndRetrieve() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      // Store JSON document
      final var storeQuery = "jn:store('json-path1','test.jn','[1, 2, 3]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Retrieve and verify
      final var openQuery = "jn:doc('json-path1','test.jn')";
      final var seq = new Query(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("[1,2,3]", buf.toString());
    }
  }

  @Test
  @DisplayName("JSON object navigation")
  void testJsonObjectNavigation() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      // Store JSON document
      final var storeQuery = "jn:store('json-path1','test.jn','{\"name\":\"sirix\",\"version\":1}')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Navigate to field
      final var openQuery = "jn:doc('json-path1','test.jn').name";
      final var seq = new Query(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("\"sirix\"", buf.toString());
    }
  }

  @Test
  @DisplayName("JSON array indexing")
  void testJsonArrayIndexing() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      // Store JSON array
      final var storeQuery = "jn:store('json-path1','test.jn','[\"a\", \"b\", \"c\"]')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Access by index
      final var openQuery = "jn:doc('json-path1','test.jn')[1]";
      final var seq = new Query(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("\"b\"", buf.toString());
    }
  }

  @Test
  @DisplayName("FLWOR expression")
  void testFlworExpression() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final var query = "for $i in (1, 2, 3) return $i * 2";
      final var seq = new Query(chain, query).evaluate(ctx);

      assertNotNull(seq);
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("2 4 6", buf.toString());
    }
  }

  @Test
  @DisplayName("Conditional expression")
  void testConditionalExpression() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final var query = "if (1 < 2) then 'yes' else 'no'";
      final var seq = new Query(chain, query).evaluate(ctx);

      assertNotNull(seq);
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("yes", buf.toString());
    }
  }

  @Test
  @DisplayName("JSON update operation")
  void testJsonUpdate() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      // Store initial JSON
      final var storeQuery = "jn:store('json-path1','test.jn','{\"count\":0}')";
      new Query(chain, storeQuery).evaluate(ctx);

      // Update the document
      final var updateQuery = """
          let $doc := jn:doc('json-path1','test.jn')
          return replace json value of $doc.count with 42
          """;
      new Query(chain, updateQuery).evaluate(ctx);

      // Verify update
      final var verifyQuery = "jn:doc('json-path1','test.jn').count";
      final var seq = new Query(chain, verifyQuery).evaluate(ctx);

      assertNotNull(seq);
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("42", buf.toString());
    }
  }
}
