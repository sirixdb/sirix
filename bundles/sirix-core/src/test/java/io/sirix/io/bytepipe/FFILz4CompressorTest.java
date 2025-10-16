package io.sirix.io.bytepipe;

import io.sirix.cache.LinuxMemorySegmentAllocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class FFILz4CompressorTest {

  private FFILz4Compressor compressor;

  @BeforeEach
  public void setUp() {
    compressor = new FFILz4Compressor();
    LinuxMemorySegmentAllocator.getInstance().init(1L << 30);
  }

  @Test
  public void testNativeAvailability() {
    System.out.println("Native LZ4 available: " + FFILz4Compressor.isNativeAvailable());
    // Test should still pass even if native not available (fallback)
  }

  @Test
  public void testCompressDecompress() {
    if (!FFILz4Compressor.isNativeAvailable()) {
      System.out.println("Skipping test - native LZ4 not available");
      return;
    }

    // Create test data
    byte[] testData = "Hello, World! This is a test of LZ4 compression using FFI.".getBytes();
    MemorySegment source = MemorySegment.ofArray(testData);

    // Compress
    MemorySegment compressed = compressor.compress(source);
    assertNotNull(compressed);
    assertTrue(compressed.byteSize() > 0);
    System.out.println("Original: " + testData.length + " bytes, Compressed: " + compressed.byteSize() + " bytes");

    // Decompress
    MemorySegment decompressed = compressor.decompress(compressed);
    assertNotNull(decompressed);
    assertEquals(testData.length, decompressed.byteSize());

    // Verify data matches
    byte[] result = decompressed.toArray(ValueLayout.JAVA_BYTE);
    assertArrayEquals(testData, result);
  }

  @Test
  public void testLargeData() {
    if (!FFILz4Compressor.isNativeAvailable()) {
      System.out.println("Skipping test - native LZ4 not available");
      return;
    }

    // Create large test data (1MB of repeated pattern)
    byte[] testData = new byte[1024 * 1024];
    for (int i = 0; i < testData.length; i++) {
      testData[i] = (byte) (i % 256);
    }
    MemorySegment source = MemorySegment.ofArray(testData);

    // Compress
    MemorySegment compressed = compressor.compress(source);
    assertNotNull(compressed);
    System.out.println("Large data - Original: " + testData.length + " bytes, Compressed: " + 
                      compressed.byteSize() + " bytes, Ratio: " + 
                      String.format("%.2f%%", 100.0 * compressed.byteSize() / testData.length));

    // Decompress
    MemorySegment decompressed = compressor.decompress(compressed);
    assertNotNull(decompressed);
    assertEquals(testData.length, decompressed.byteSize());

    // Verify data matches
    byte[] result = decompressed.toArray(ValueLayout.JAVA_BYTE);
    assertArrayEquals(testData, result);
  }

  @Test
  public void testSupportsMemorySegments() {
    assertEquals(FFILz4Compressor.isNativeAvailable(), compressor.supportsMemorySegments());
  }
}


