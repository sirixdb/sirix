package io.sirix.io.bytepipe;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for empty ByteHandlerPipeline (Umbra-style identity operation).
 * 
 * <p>When no handlers are configured, the pipeline should act as identity:
 * no transformation is applied. This enables zero-copy I/O paths.</p>
 */
class EmptyPipelineTest {

  @Test
  void isEmpty_withNoHandlers_returnsTrue() {
    final var pipeline = new ByteHandlerPipeline();
    assertTrue(pipeline.isEmpty(), "Empty pipeline should return true for isEmpty()");
  }

  @Test
  void isEmpty_withHandlers_returnsFalse() {
    final var pipeline = new ByteHandlerPipeline(new LZ4Compressor());
    assertFalse(pipeline.isEmpty(), "Pipeline with handlers should return false for isEmpty()");
  }

  @Test
  void supportsMemorySegments_whenEmpty_returnsTrue() {
    final var pipeline = new ByteHandlerPipeline();
    assertTrue(pipeline.supportsMemorySegments(), 
        "Empty pipeline should support MemorySegments (identity operation)");
  }

  @Test
  void compress_whenEmpty_returnsInputUnchanged() {
    final byte[] data = new byte[1024];
    new Random(42).nextBytes(data);
    final MemorySegment input = MemorySegment.ofArray(data);
    
    final var pipeline = new ByteHandlerPipeline();
    final MemorySegment output = pipeline.compress(input);
    
    // Identity: should return exact same segment
    assertSame(input, output, "Empty pipeline compress should return same segment (identity)");
  }

  @Test
  void decompress_whenEmpty_returnsInputUnchanged() {
    final byte[] data = new byte[1024];
    new Random(42).nextBytes(data);
    final MemorySegment input = MemorySegment.ofArray(data);
    
    final var pipeline = new ByteHandlerPipeline();
    final MemorySegment output = pipeline.decompress(input);
    
    // Identity: should return exact same segment
    assertSame(input, output, "Empty pipeline decompress should return same segment (identity)");
  }

  @Test
  void decompressScoped_whenEmpty_returnsIdentityResult() {
    final byte[] data = new byte[1024];
    new Random(42).nextBytes(data);
    final MemorySegment input = MemorySegment.ofArray(data);
    
    final var pipeline = new ByteHandlerPipeline();
    
    try (var result = pipeline.decompressScoped(input)) {
      // Empty pipeline now COPIES data to a new buffer to avoid use-after-free issues
      // with reusable buffers (e.g., FileChannelReader's striped buffers)
      assertEquals(input.byteSize(), result.segment().byteSize(),
          "Copied segment should have same size as input");
      
      // Verify content is identical
      for (long i = 0; i < input.byteSize(); i++) {
        assertEquals(input.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i),
                    result.segment().get(java.lang.foreign.ValueLayout.JAVA_BYTE, i),
                    "Content at offset " + i + " should match");
      }
      
      // Releaser should not be null
      assertNotNull(result.releaser(), "Releaser should not be null");
      
      // Calling releaser should not throw
      result.releaser().run();
    }
  }

  @Test
  void decompressScoped_whenEmpty_releaserIsSafe() {
    final byte[] data = new byte[256];
    new Random(123).nextBytes(data);
    final MemorySegment input = MemorySegment.ofArray(data);
    
    final var pipeline = new ByteHandlerPipeline();
    
    // Multiple decompress/release cycles should work (with heap fallback since allocator not init'd)
    for (int i = 0; i < 10; i++) {
      var result = pipeline.decompressScoped(input);
      assertEquals(input.byteSize(), result.segment().byteSize(),
          "Copied segment should have same size");
      result.close();  // Safe to call - either returns to pool or is GC'd
    }
  }

  @Test
  void streamMethods_whenEmpty_actAsIdentity() throws Exception {
    final byte[] data = new byte[2048];
    new Random(99).nextBytes(data);
    
    final var pipeline = new ByteHandlerPipeline();
    
    // Serialize (should wrap without transformation)
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (OutputStream out = pipeline.serialize(baos)) {
      out.write(data);
    }
    byte[] serialized = baos.toByteArray();
    
    // For empty pipeline, serialized should be same as input
    // (no framing or headers added)
    assertArrayEquals(data, serialized, 
        "Empty pipeline serialize should not transform data");
    
    // Deserialize (should return data unchanged)
    try (InputStream in = pipeline.deserialize(new ByteArrayInputStream(serialized))) {
      byte[] deserialized = in.readAllBytes();
      assertArrayEquals(data, deserialized, 
          "Empty pipeline deserialize should not transform data");
    }
  }

  @Test
  void getComponents_whenEmpty_returnsEmptyList() {
    final var pipeline = new ByteHandlerPipeline();
    assertTrue(pipeline.getComponents().isEmpty(), 
        "Empty pipeline should have no components");
  }

  @Test
  void roundTrip_withEmptyPipeline_preservesData() {
    final byte[] original = new byte[4096];
    new Random(777).nextBytes(original);
    final MemorySegment input = MemorySegment.ofArray(original);
    
    final var pipeline = new ByteHandlerPipeline();
    
    // Compress
    MemorySegment compressed = pipeline.compress(input);
    
    // Decompress
    var result = pipeline.decompressScoped(compressed);
    try {
      // Verify data unchanged
      byte[] output = new byte[(int) result.segment().byteSize()];
      MemorySegment.copy(result.segment(), 0, MemorySegment.ofArray(output), 0, output.length);
      assertArrayEquals(original, output, "Round-trip through empty pipeline should preserve data");
    } finally {
      result.close();
    }
  }

  @Test
  void getInstance_whenEmpty_createsNewEmptyPipeline() {
    final var pipeline = new ByteHandlerPipeline();
    final var newInstance = (ByteHandlerPipeline) pipeline.getInstance();
    
    assertTrue(newInstance.isEmpty(), "getInstance should create empty pipeline");
    assertEquals(0, newInstance.getComponents().size());
  }
}
