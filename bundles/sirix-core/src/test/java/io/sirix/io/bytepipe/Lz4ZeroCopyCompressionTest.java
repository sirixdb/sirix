package io.sirix.io.bytepipe;

import io.sirix.node.MemorySegmentBytesOut;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class Lz4ZeroCopyCompressionTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(Lz4ZeroCopyCompressionTest.class);

  @SuppressWarnings("deprecation")
  @Test
  void nativeFFIZeroCopyRoundTrip() {
    assumeTrue(FFILz4Compressor.isNativeAvailable());

    final byte[] payload = new byte[1024];
    new Random(1).nextBytes(payload);

    final var compressor = new FFILz4Compressor();
    final MemorySegment compressed = compressor.compress(MemorySegment.ofArray(payload));

    // Use deprecated decompress method for testing (avoids allocator initialization issues)
    final MemorySegment decompressedSegment = compressor.decompress(compressed);
    final byte[] decompressed = new byte[payload.length];
    MemorySegment.copy(decompressedSegment, 0, MemorySegment.ofArray(decompressed), 0, decompressed.length);
    assertArrayEquals(payload, decompressed);
  }

  @Test
  void streamFallbackRoundTrip() throws Exception {
    final byte[] payload = new byte[2048];
    new Random(2).nextBytes(payload);

    final var pipeline = new ByteHandlerPipeline(new LZ4Compressor());

    final byte[] compressed;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         net.jpountz.lz4.LZ4BlockOutputStream lzOut = new net.jpountz.lz4.LZ4BlockOutputStream(out)) {
      lzOut.write(payload);
      lzOut.finish();
      compressed = out.toByteArray();
    }

    final byte[] decompressed;
    try (InputStream in = pipeline.deserialize(new ByteArrayInputStream(compressed))) {
      decompressed = in.readAllBytes();
    }

    assertArrayEquals(payload, decompressed);
  }

  @Test
  void memorySegmentBytesOutCopiesSegment() {
    final byte[] payload = new byte[]{1, 2, 3, 4};
    final MemorySegment segment = MemorySegment.ofArray(payload);

    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut()) {
      out.write(segment);
      assertArrayEquals(payload, out.bytesForRead().toByteArray());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  void ffiCompressionRatioComparableToLibrary() throws Exception {
    assumeTrue(FFILz4Compressor.isNativeAvailable());

    // Create compressible test data (repeated text patterns compress well)
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      sb.append("This is a test string that should compress well because it has repeated patterns. ");
      sb.append("JSON data often has similar structures: {\"key\": \"value\", \"count\": ").append(i).append("} ");
    }
    final byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
    
    // Compress with FFI (HC mode)
    final var ffiCompressor = new FFILz4Compressor();
    final MemorySegment ffiCompressed = ffiCompressor.compress(MemorySegment.ofArray(payload));
    final long ffiCompressedSize = ffiCompressed.byteSize();
    
    // Compress with Java library (LZ4BlockOutputStream)
    final byte[] libraryCompressed;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         net.jpountz.lz4.LZ4BlockOutputStream lzOut = new net.jpountz.lz4.LZ4BlockOutputStream(out)) {
      lzOut.write(payload);
      lzOut.finish();
      libraryCompressed = out.toByteArray();
    }
    final long libraryCompressedSize = libraryCompressed.length;
    
    // Calculate compression ratios
    final double ffiRatio = (double) payload.length / ffiCompressedSize;
    final double libraryRatio = (double) payload.length / libraryCompressedSize;
    
    LOGGER.info("Original size: {} bytes", payload.length);
    LOGGER.info("FFI HC compressed size: {} bytes (ratio: {}x)", ffiCompressedSize, String.format("%.2f", ffiRatio));
    LOGGER.info("Library compressed size: {} bytes (ratio: {}x)", libraryCompressedSize, String.format("%.2f", libraryRatio));
    
    // FFI with HC mode should achieve at least 80% of the library's compression ratio
    // (allowing some tolerance for format differences)
    final double ratioComparison = ffiRatio / libraryRatio;
    LOGGER.info("FFI/Library ratio comparison: {}", String.format("%.2f", ratioComparison));
    
    assertTrue(ratioComparison >= 0.8, 
        String.format("FFI compression ratio (%.2fx) should be at least 80%% of library ratio (%.2fx), but was %.2f%%",
            ffiRatio, libraryRatio, ratioComparison * 100));
    
    // Verify FFI round-trip works correctly (using deprecated decompress for test simplicity)
    final MemorySegment decompressedSegment = ffiCompressor.decompress(ffiCompressed);
    final byte[] decompressed = new byte[payload.length];
    MemorySegment.copy(decompressedSegment, 0, MemorySegment.ofArray(decompressed), 0, decompressed.length);
    assertArrayEquals(payload, decompressed, "FFI round-trip should produce identical data");
  }

  @SuppressWarnings("deprecation")
  @Test
  void ffiCompressionWithRandomData() throws Exception {
    assumeTrue(FFILz4Compressor.isNativeAvailable());

    // Random data compresses poorly - tests that FFI handles this correctly
    final byte[] payload = new byte[64 * 1024]; // 64KB
    new Random(42).nextBytes(payload);
    
    // Compress with FFI
    final var ffiCompressor = new FFILz4Compressor();
    final MemorySegment ffiCompressed = ffiCompressor.compress(MemorySegment.ofArray(payload));
    
    // Random data typically doesn't compress well, but it should still work
    // LZ4 may produce output larger than input for incompressible data (up to compressBound)
    assertTrue(ffiCompressed.byteSize() > 0, "Compressed size should be positive");
    
    // Verify round-trip (using deprecated decompress for test simplicity)
    final MemorySegment decompressedSegment = ffiCompressor.decompress(ffiCompressed);
    final byte[] decompressed = new byte[payload.length];
    MemorySegment.copy(decompressedSegment, 0, MemorySegment.ofArray(decompressed), 0, decompressed.length);
    assertArrayEquals(payload, decompressed, "Random data round-trip should produce identical data");
    
    LOGGER.info("Random data: {} bytes -> {} bytes (ratio: {}x)", 
        payload.length, ffiCompressed.byteSize(), 
        String.format("%.2f", (double) payload.length / ffiCompressed.byteSize()));
  }
}

