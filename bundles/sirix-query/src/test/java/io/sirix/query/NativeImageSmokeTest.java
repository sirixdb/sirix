package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Smoke tests for GraalVM native image compilation. These tests verify that the query engine and
 * FFM-based native memory allocation work correctly when compiled to native image.
 *
 * <p>
 * Run with: {@code ./gradlew :sirix-query:nativeSmokeTest}
 */
@Tag("native-image")
@DisplayName("Native Image Smoke Tests")
public class NativeImageSmokeTest {

  private String evaluate(String queryStr) {
    try (final var ctx = SirixQueryContext.create(); final var chain = SirixCompileChain.create()) {
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

  /**
   * Tests FFM downcall stubs registered in reachability-metadata.json. Calls mmap/munmap directly via
   * FFM to verify the native image has the downcall stubs the off-heap allocator maps its memory
   * with.
   */
  @Test
  @DisplayName("FFM downcall - mmap/munmap")
  void testFfmMmapMunmap() throws Throwable {
    Assumptions.assumeTrue(System.getProperty("os.name").toLowerCase().contains("linux"), "mmap/munmap requires Linux");

    var linker = Linker.nativeLinker();

    // Same FunctionDescriptors as PosixVirtualMemory, the allocator's mapping layer
    var mmap = linker.downcallHandle(linker.defaultLookup().find("mmap").orElseThrow(),
        FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG, JAVA_INT, JAVA_INT, JAVA_INT, JAVA_LONG));

    var munmap = linker.downcallHandle(linker.defaultLookup().find("munmap").orElseThrow(),
        FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG));

    // mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0)
    MemorySegment addr = (MemorySegment) mmap.invokeExact(MemorySegment.NULL, 4096L, 0x1 | 0x2, 0x02 | 0x20, -1, 0L);
    assertNotNull(addr);
    assertNotEquals(0L, addr.address());

    // Write and read back
    var segment = addr.reinterpret(4096);
    segment.set(JAVA_INT, 0, 42);
    assertEquals(42, segment.get(JAVA_INT, 0));

    // munmap
    int result = (int) munmap.invokeExact(segment, 4096L);
    assertEquals(0, result);
  }

  /**
   * The configured allocator inside the image: it reserves its regions, lends a page frame that can
   * be written and read, and takes it back.
   */
  @Test
  @DisplayName("Configured allocator lends and takes back a page frame")
  void testConfiguredAllocator() {
    final MemorySegmentAllocator allocator = Allocators.getInstance();
    // The first call fixes the budget for the process. Asking for what a database asks for leaves
    // the tests that open one unaffected, whichever runs first.
    allocator.init(new DatabaseConfiguration(Path.of("unused")).getMaxSegmentAllocationSize());

    final MemorySegment segment = allocator.allocate(4096);
    try {
      assertNotNull(segment);
      assertEquals(4096, segment.byteSize());

      segment.set(ValueLayout.JAVA_INT, 0, 42);
      assertEquals(42, segment.get(ValueLayout.JAVA_INT, 0));
    } finally {
      allocator.release(segment);
    }
  }

}
