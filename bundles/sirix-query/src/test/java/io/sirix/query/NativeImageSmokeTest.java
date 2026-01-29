package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke tests for GraalVM native image compilation.
 * These tests verify that the query engine and FFM-based native memory
 * allocation work correctly when compiled to native image.
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

  @Test
  @DisplayName("LinuxMemorySegmentAllocator mmap/munmap via FFM")
  void testLinuxMemorySegmentAllocator() {
    Assumptions.assumeTrue(
        System.getProperty("os.name").toLowerCase().contains("linux"),
        "LinuxMemorySegmentAllocator requires Linux");

    var allocator = LinuxMemorySegmentAllocator.getInstance();
    // mapMemory calls mmap, releaseMemory calls munmap
    MemorySegment segment = allocator.mapMemory(4096);
    assertNotNull(segment);
    assertEquals(4096, segment.byteSize());

    // Write and read back to verify the mapped memory is usable
    segment.set(ValueLayout.JAVA_INT, 0, 42);
    assertEquals(42, segment.get(ValueLayout.JAVA_INT, 0));

    // Release via munmap
    allocator.releaseMemory(segment, 4096);
  }

  @Test
  @DisplayName("LinuxMemorySegmentAllocator pool allocation via FFM")
  void testLinuxMemorySegmentAllocatorPool() {
    Assumptions.assumeTrue(
        System.getProperty("os.name").toLowerCase().contains("linux"),
        "LinuxMemorySegmentAllocator requires Linux");

    var allocator = LinuxMemorySegmentAllocator.getInstance();
    // init() exercises mmap (preAllocateVirtualRegion) and sysconf (detectBasePageSize)
    allocator.init(1L << 30); // 1GB budget
    assertTrue(allocator.isInitialized());

    // allocate() gets a segment from the mmap'd pool
    MemorySegment segment = allocator.allocate(4096);
    assertNotNull(segment);

    // Write and read back
    segment.set(ValueLayout.JAVA_LONG, 0, 0xDEADBEEFL);
    assertEquals(0xDEADBEEFL, segment.get(ValueLayout.JAVA_LONG, 0));

    // release() exercises madvise(MADV_DONTNEED)
    allocator.release(segment);

    // free() exercises munmap
    allocator.free();
  }
}
