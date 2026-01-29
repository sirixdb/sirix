package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Smoke tests for GraalVM native image compilation.
 * These tests verify that the query engine works correctly when compiled to native image.
 *
 * <p>Tests use store-free query evaluation (no database I/O) to avoid requiring the
 * FFM-based LinuxMemorySegmentAllocator which is not yet supported in native images.
 *
 * <p>Run with: {@code ./gradlew :sirix-query:nativeSmokeTest}
 */
@Tag("native-image")
@DisplayName("Native Image Smoke Tests")
public class NativeImageSmokeTest {

  private String evaluate(String queryStr) {
    try (final var ctx = SirixQueryContext.create();
         final var chain = SirixCompileChain.create()) {
      final var seq = new Query(chain, queryStr).evaluate(ctx);
      assertNotNull(seq);
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      return buf.toString();
    }
  }

  @Test
  @DisplayName("Basic arithmetic query")
  void testBasicArithmetic() {
    assertEquals("2", evaluate("1 + 1"));
  }

  @Test
  @DisplayName("String manipulation query")
  void testStringManipulation() {
    assertEquals("Hello World", evaluate("concat('Hello', ' ', 'World')"));
  }

  @Test
  @DisplayName("FLWOR expression")
  void testFlworExpression() {
    assertEquals("2 4 6", evaluate("for $i in (1, 2, 3) return $i * 2"));
  }

  @Test
  @DisplayName("Conditional expression")
  void testConditionalExpression() {
    assertEquals("yes", evaluate("if (1 < 2) then 'yes' else 'no'"));
  }

  @Test
  @DisplayName("String length function")
  void testStringLength() {
    assertEquals("5", evaluate("string-length('hello')"));
  }

  @Test
  @DisplayName("Nested arithmetic")
  void testNestedArithmetic() {
    assertEquals("42", evaluate("(6 * 7)"));
  }

  @Test
  @DisplayName("Sequence operations")
  void testSequenceOperations() {
    assertEquals("6", evaluate("count((1, 2, 3, 4, 5, 6))"));
  }

  @Test
  @DisplayName("Let expression with computation")
  void testLetExpression() {
    assertEquals("100", evaluate("let $x := 10 return $x * $x"));
  }
}
