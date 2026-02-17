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
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    final byte[] payload = new byte[] {1, 2, 3, 4};
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
    LOGGER.info("Library compressed size: {} bytes (ratio: {}x)", libraryCompressedSize,
        String.format("%.2f", libraryRatio));

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

    LOGGER.info("Random data: {} bytes -> {} bytes (ratio: {}x)", payload.length, ffiCompressed.byteSize(),
        String.format("%.2f", (double) payload.length / ffiCompressed.byteSize()));
  }

  @Test
  void testAdaptiveCompressionSkipsSmallData() {
    // Data smaller than MIN_COMPRESSION_SIZE (64 bytes) should be stored uncompressed
    final byte[] smallPayload = "Short text".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    final MemorySegment source = MemorySegment.ofArray(smallPayload);

    final FFILz4Compressor compressor = new FFILz4Compressor();
    final MemorySegment compressed = compressor.compress(source);

    // Check header - negative size indicates uncompressed
    int sizeHeader = compressed.get(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, 0);
    assertTrue(sizeHeader < 0, "Small data should have negative size header (uncompressed)");
    assertEquals(-smallPayload.length, sizeHeader, "Header should be negated original size");

    // Verify round-trip
    final MemorySegment decompressed = compressor.decompress(compressed);
    byte[] result = new byte[smallPayload.length];
    MemorySegment.copy(decompressed, 0, MemorySegment.ofArray(result), 0, result.length);
    assertArrayEquals(smallPayload, result, "Small data round-trip should be identical");
  }

  @Test
  void testAdaptiveCompressionUsesCorrectModeForSize() {
    // Test that different sizes use appropriate compression modes
    final FFILz4Compressor compressor = new FFILz4Compressor();

    // Small page (< 16KB) - should use fast mode
    final byte[] smallPage = new byte[8 * 1024];
    java.util.Arrays.fill(smallPage, (byte) 'A'); // Highly compressible
    final MemorySegment smallCompressed = compressor.compress(MemorySegment.ofArray(smallPage));

    // Large page (> 16KB) - should use HC mode
    final byte[] largePage = new byte[32 * 1024];
    java.util.Arrays.fill(largePage, (byte) 'A'); // Highly compressible
    final MemorySegment largeCompressed = compressor.compress(MemorySegment.ofArray(largePage));

    // Both should compress well (we can't directly verify which mode was used,
    // but we can verify they work correctly)
    assertTrue(smallCompressed.byteSize() < smallPage.length, "Small page should compress");
    assertTrue(largeCompressed.byteSize() < largePage.length, "Large page should compress");

    // Verify round-trips
    MemorySegment smallDecompressed = compressor.decompress(smallCompressed);
    MemorySegment largeDecompressed = compressor.decompress(largeCompressed);

    byte[] smallResult = new byte[smallPage.length];
    byte[] largeResult = new byte[largePage.length];
    MemorySegment.copy(smallDecompressed, 0, MemorySegment.ofArray(smallResult), 0, smallResult.length);
    MemorySegment.copy(largeDecompressed, 0, MemorySegment.ofArray(largeResult), 0, largeResult.length);

    assertArrayEquals(smallPage, smallResult);
    assertArrayEquals(largePage, largeResult);

    LOGGER.info("Small page (8KB): {} -> {} bytes ({}%)", smallPage.length, smallCompressed.byteSize(),
        String.format("%.1f", 100.0 * smallCompressed.byteSize() / smallPage.length));
    LOGGER.info("Large page (32KB): {} -> {} bytes ({}%)", largePage.length, largeCompressed.byteSize(),
        String.format("%.1f", 100.0 * largeCompressed.byteSize() / largePage.length));
  }

  @Test
  void testAdaptiveCompressionSkipsIncompressibleData() {
    // Random data should be stored uncompressed if compression isn't beneficial
    final byte[] randomData = new byte[4096];
    new java.util.Random(42).nextBytes(randomData);

    final FFILz4Compressor compressor = new FFILz4Compressor();
    final MemorySegment compressed = compressor.compress(MemorySegment.ofArray(randomData));

    // Verify round-trip regardless of storage mode
    final MemorySegment decompressed = compressor.decompress(compressed);
    byte[] result = new byte[randomData.length];
    MemorySegment.copy(decompressed, 0, MemorySegment.ofArray(result), 0, result.length);
    assertArrayEquals(randomData, result, "Random data round-trip should be identical");

    LOGGER.info("Random data (4KB): {} -> {} bytes (header check: {})", randomData.length, compressed.byteSize(),
        compressed.get(java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED, 0) < 0
            ? "uncompressed"
            : "compressed");
  }

  @Test
  void testCompressionPerformanceComparison() {
    // Performance test comparing different compression modes
    final FFILz4Compressor compressor = new FFILz4Compressor();

    // Create test data that compresses well - repetitive JSON-like pattern
    final String pattern = "JSON data pattern: {\"key\": \"value_99\"}";
    final byte[] testData = new byte[64 * 1024]; // 64KB
    byte[] patternBytes = pattern.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    for (int i = 0; i < testData.length; i++) {
      testData[i] = patternBytes[i % patternBytes.length];
    }
    final MemorySegment source = MemorySegment.ofArray(testData);

    // Warmup
    for (int i = 0; i < 100; i++) {
      compressor.compress(source);
    }

    // Benchmark
    final int iterations = 1000;
    long startTime = System.nanoTime();
    for (int i = 0; i < iterations; i++) {
      compressor.compress(source);
    }
    long endTime = System.nanoTime();

    double avgTimeMs = (endTime - startTime) / 1_000_000.0 / iterations;
    double throughputMBps = (testData.length / 1024.0 / 1024.0) / (avgTimeMs / 1000.0);

    LOGGER.info("Compression performance: {}/iteration, {} MB/s throughput", String.format("%.3f ms", avgTimeMs),
        String.format("%.1f", throughputMBps));

    // Sanity check - should be reasonably fast
    assertTrue(avgTimeMs < 10, "Compression should complete in < 10ms per 64KB");
  }
}

