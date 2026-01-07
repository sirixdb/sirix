/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.utils;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FSSTCompressor}.
 * 
 * Verifies the formal correctness properties:
 * P1: ∀ string s, table t: decode(encode(s, t), t) == s  (roundtrip correctness)
 * P2: ∀ input i: encode(i, emptyTable) == escape(i)     (graceful degradation)
 */
class FSSTCompressorTest {

  @Test
  void testBuildSymbolTableWithSimilarStrings() {
    // Given: similar JSON-like strings with common patterns
    // Need at least 64 samples with 32+ bytes each, totaling 4KB+
    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      // Each string is ~50 bytes, well over MIN_COMPRESSION_SIZE of 32
      samples.add(("{\"name\":\"Person" + i + "\",\"age\":" + (20 + i % 50) + ",\"city\":\"City" + i + "\"}").getBytes(StandardCharsets.UTF_8));
    }

    // When: building symbol table
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    // Then: table should be non-empty
    assertNotNull(symbolTable);
    assertTrue(symbolTable.length > 0, "Symbol table should be non-empty for similar strings");

    // And: should contain common patterns like "name", "age"
    final byte[][] symbols = FSSTCompressor.parseSymbolTable(symbolTable);
    assertTrue(symbols.length > 0, "Should have extracted symbols");
  }

  @Test
  void testRoundtripCorrectness() {
    // Given: similar strings to build table
    final List<byte[]> samples = Arrays.asList(
        "The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8),
        "The quick brown cat jumps over the lazy dog".getBytes(StandardCharsets.UTF_8),
        "The slow brown fox walks over the lazy dog".getBytes(StandardCharsets.UTF_8),
        "The quick brown fox runs over the lazy cat".getBytes(StandardCharsets.UTF_8)
    );
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    // When: encoding and decoding each sample
    for (final byte[] original : samples) {
      final byte[] encoded = FSSTCompressor.encode(original, symbolTable);
      final byte[] decoded = FSSTCompressor.decode(encoded, symbolTable);

      // Then: P1 holds - roundtrip correctness
      assertArrayEquals(original, decoded, 
          "Roundtrip should preserve original data: " + new String(original, StandardCharsets.UTF_8));
    }
  }

  @Test
  void testRoundtripWithNewString() {
    // Given: table built from samples
    final List<byte[]> samples = Arrays.asList(
        "{\"id\":123,\"value\":\"test\"}".getBytes(StandardCharsets.UTF_8),
        "{\"id\":456,\"value\":\"data\"}".getBytes(StandardCharsets.UTF_8),
        "{\"id\":789,\"value\":\"info\"}".getBytes(StandardCharsets.UTF_8),
        "{\"id\":101,\"value\":\"more\"}".getBytes(StandardCharsets.UTF_8)
    );
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    // When: encoding a new string (not in samples but with similar patterns)
    final byte[] newString = "{\"id\":999,\"value\":\"new\"}".getBytes(StandardCharsets.UTF_8);
    final byte[] encoded = FSSTCompressor.encode(newString, symbolTable);
    final byte[] decoded = FSSTCompressor.decode(encoded, symbolTable);

    // Then: roundtrip should work
    assertArrayEquals(newString, decoded);
  }

  @Test
  void testEmptyStringRoundtrip() {
    // Given: table and empty string
    final List<byte[]> samples = Arrays.asList(
        "test1".getBytes(StandardCharsets.UTF_8),
        "test2".getBytes(StandardCharsets.UTF_8),
        "test3".getBytes(StandardCharsets.UTF_8),
        "test4".getBytes(StandardCharsets.UTF_8)
    );
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    // When: encoding empty string
    final byte[] empty = new byte[0];
    final byte[] encoded = FSSTCompressor.encode(empty, symbolTable);
    final byte[] decoded = FSSTCompressor.decode(encoded, symbolTable);

    // Then: should preserve empty array
    assertArrayEquals(empty, decoded);
  }

  @Test
  void testNullSymbolTable() {
    // Given: null symbol table (no compression)
    final byte[] input = "test data".getBytes(StandardCharsets.UTF_8);

    // When: encoding with null table
    final byte[] encoded = FSSTCompressor.encode(input, null);
    final byte[] decoded = FSSTCompressor.decode(encoded, null);

    // Then: P2 - graceful degradation
    assertArrayEquals(input, decoded);
  }

  @Test
  void testEmptySymbolTable() {
    // Given: empty symbol table
    final byte[] input = "test data".getBytes(StandardCharsets.UTF_8);
    final byte[] emptyTable = new byte[0];

    // When: encoding with empty table
    final byte[] encoded = FSSTCompressor.encode(input, emptyTable);
    final byte[] decoded = FSSTCompressor.decode(encoded, emptyTable);

    // Then: P2 - graceful degradation
    assertArrayEquals(input, decoded);
  }

  @Test
  void testUnicodeRoundtrip() {
    // Test various unicode strings
    final String[] inputs = {
        "Hello, World!",
        "こんにちは世界",  // Japanese
        "🎉🚀🔥",        // Emoji
        "Ñoño señor",    // Spanish with accents
        "Mixed 日本語 and English",
        "\u0000\u0001\u00FF"  // Edge case bytes
    };

    for (final String input : inputs) {
      // Given: unicode string and a table
      final byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
      final List<byte[]> samples = Arrays.asList(
          inputBytes,
          (input + " copy").getBytes(StandardCharsets.UTF_8),
          (input + " another").getBytes(StandardCharsets.UTF_8),
          (input + " more").getBytes(StandardCharsets.UTF_8)
      );
      final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

      // When: encoding and decoding
      final byte[] encoded = FSSTCompressor.encode(inputBytes, symbolTable);
      final byte[] decoded = FSSTCompressor.decode(encoded, symbolTable);

      // Then: roundtrip preserves unicode
      assertArrayEquals(inputBytes, decoded, "Failed for input: " + input);
      assertEquals(input, new String(decoded, StandardCharsets.UTF_8));
    }
  }

  @Test
  void testCompressionBeneficial() {
    // Given: highly repetitive data with sufficient samples and size
    // Need at least 64 samples with 32+ bytes each, totaling 4KB+
    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      // Each string is ~60 bytes with repetitive patterns
      samples.add(("{\"type\":\"event\",\"id\":" + i + ",\"data\":\"common_value_pattern_" + i + "\"}").getBytes(StandardCharsets.UTF_8));
    }
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);
    assertNotNull(symbolTable);
    assertTrue(symbolTable.length > 0, "Symbol table should be built for sufficient data");

    // When: encoding a similar string (must be 32+ bytes)
    final byte[] input = "{\"type\":\"event\",\"id\":999,\"data\":\"common_value_pattern_999\"}".getBytes(StandardCharsets.UTF_8);
    final byte[] encoded = FSSTCompressor.encode(input, symbolTable);

    // Then: encoded should be smaller than input (compression beneficial)
    assertTrue(encoded.length < input.length, 
        "Compression should reduce size for repetitive data. Original: " + input.length + ", Encoded: " + encoded.length);

    // And: roundtrip should work
    final byte[] decoded = FSSTCompressor.decode(encoded, symbolTable);
    assertArrayEquals(input, decoded);
  }

  @Test
  void testIsCompressibleWithSimilarData() {
    // Given: similar data with common patterns
    // Need at least 64 samples with 32+ bytes each, totaling 4KB+
    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      // Each string is 50+ bytes with common prefix/suffix patterns (100 * 50 = 5KB > 4KB)
      samples.add(("prefix_common_pattern_" + String.format("%03d", i) + "_suffix_end_with_more_data").getBytes(StandardCharsets.UTF_8));
    }

    // When/Then
    assertTrue(FSSTCompressor.isCompressible(samples));
  }

  @Test
  void testIsCompressibleWithRandomData() {
    // Given: random data with no patterns
    final Random random = new Random(42);
    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      final byte[] randomBytes = new byte[32];
      random.nextBytes(randomBytes);
      samples.add(randomBytes);
    }

    // When/Then: random data may or may not be compressible
    // Just verify it doesn't throw
    FSSTCompressor.isCompressible(samples);
  }

  @Test
  void testTooFewSamples() {
    // Given: too few samples
    final List<byte[]> samples = Arrays.asList(
        "one".getBytes(StandardCharsets.UTF_8),
        "two".getBytes(StandardCharsets.UTF_8)
    );

    // When: building table
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    // Then: should return empty table (not enough data)
    assertEquals(0, symbolTable.length);
  }

  @Test
  void testMemorySegmentEncode() {
    // Given: memory segment and table
    final List<byte[]> samples = Arrays.asList(
        "memory segment test one".getBytes(StandardCharsets.UTF_8),
        "memory segment test two".getBytes(StandardCharsets.UTF_8),
        "memory segment test three".getBytes(StandardCharsets.UTF_8),
        "memory segment test four".getBytes(StandardCharsets.UTF_8)
    );
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    final byte[] input = "memory segment test five".getBytes(StandardCharsets.UTF_8);
    final MemorySegment segment = MemorySegment.ofArray(input);

    // When: encoding from memory segment
    final byte[] encoded = FSSTCompressor.encode(segment, symbolTable);
    final byte[] decoded = FSSTCompressor.decode(encoded, symbolTable);

    // Then: roundtrip works
    assertArrayEquals(input, decoded);
  }

  @Test
  void testMemorySegmentDecode() {
    // Given: encoded data as memory segment
    final List<byte[]> samples = Arrays.asList(
        "decode from segment one".getBytes(StandardCharsets.UTF_8),
        "decode from segment two".getBytes(StandardCharsets.UTF_8),
        "decode from segment three".getBytes(StandardCharsets.UTF_8),
        "decode from segment four".getBytes(StandardCharsets.UTF_8)
    );
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    final byte[] input = "decode from segment five".getBytes(StandardCharsets.UTF_8);
    final byte[] encoded = FSSTCompressor.encode(input, symbolTable);
    final MemorySegment encodedSegment = MemorySegment.ofArray(encoded);

    // When: decoding from memory segment
    final byte[] decoded = FSSTCompressor.decode(encodedSegment, symbolTable);

    // Then: roundtrip works
    assertArrayEquals(input, decoded);
  }

  @Test
  void testParseSymbolTableRoundtrip() {
    // Given: samples to build table
    final List<byte[]> samples = Arrays.asList(
        "symbol table parse test".getBytes(StandardCharsets.UTF_8),
        "symbol table parse another".getBytes(StandardCharsets.UTF_8),
        "symbol table parse more".getBytes(StandardCharsets.UTF_8),
        "symbol table parse data".getBytes(StandardCharsets.UTF_8)
    );

    // When: building and parsing table
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);
    final byte[][] symbols = FSSTCompressor.parseSymbolTable(symbolTable);

    // Then: symbols should be valid
    if (symbolTable.length > 0) {
      assertTrue(symbols.length > 0);
      for (final byte[] symbol : symbols) {
        assertNotNull(symbol);
        assertTrue(symbol.length >= 2, "Symbols should be at least 2 bytes");
      }
    }
  }

  @Test
  void testEscapeByteHandling() {
    // Given: input containing 0xFF bytes
    final byte[] input = new byte[]{0x00, (byte) 0xFF, 0x01, (byte) 0xFF, (byte) 0xFF, 0x02};
    final List<byte[]> samples = Arrays.asList(
        input,
        new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
        input,
        new byte[]{0x00, (byte) 0xFF, 0x00, (byte) 0xFF}
    );
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    // When: encoding and decoding
    final byte[] encoded = FSSTCompressor.encode(input, symbolTable);
    final byte[] decoded = FSSTCompressor.decode(encoded, symbolTable);

    // Then: should handle 0xFF correctly
    assertArrayEquals(input, decoded);
  }

  @Test
  void testLargeInput() {
    // Given: large input
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append("{\"index\":").append(i).append(",\"value\":\"data_").append(i).append("\"},");
    }
    final byte[] largeInput = sb.toString().getBytes(StandardCharsets.UTF_8);

    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      samples.add(("{\"index\":" + i + ",\"value\":\"data_" + i + "\"}").getBytes(StandardCharsets.UTF_8));
    }
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    // When: encoding large input
    final byte[] encoded = FSSTCompressor.encode(largeInput, symbolTable);
    final byte[] decoded = FSSTCompressor.decode(encoded, symbolTable);

    // Then: roundtrip should work
    assertArrayEquals(largeInput, decoded);
  }

  @Test
  void testIsCompressionBeneficialWithHighlyRepetitiveData() {
    // Given: highly repetitive data that should compress well (>= 15% savings)
    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      // Each string is ~70 bytes with very repetitive patterns
      samples.add(("{\"type\":\"event\",\"action\":\"click\",\"target\":\"button_" + i + "\"}").getBytes(StandardCharsets.UTF_8));
    }
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    // When: checking if compression is beneficial
    boolean beneficial = FSSTCompressor.isCompressionBeneficial(samples, symbolTable);

    // Then: should be beneficial for highly repetitive data
    assertTrue(beneficial, "Compression should be beneficial for highly repetitive JSON patterns");
  }

  @Test
  void testIsCompressionBeneficialWithRandomData() {
    // Given: random data with no patterns
    final Random random = new Random(42);
    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      byte[] randomBytes = new byte[64];
      random.nextBytes(randomBytes);
      samples.add(randomBytes);
    }
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);

    // When: checking if compression is beneficial
    boolean beneficial = FSSTCompressor.isCompressionBeneficial(samples, symbolTable);

    // Then: should NOT be beneficial for random data (no patterns to exploit)
    assertFalse(beneficial, "Compression should NOT be beneficial for random data");
  }

  @Test
  void testIsCompressionBeneficialWithNullSymbolTable() {
    // Given: null symbol table
    final List<byte[]> samples = Arrays.asList(
        "test data one".getBytes(StandardCharsets.UTF_8),
        "test data two".getBytes(StandardCharsets.UTF_8)
    );

    // When: checking with null table
    boolean beneficial = FSSTCompressor.isCompressionBeneficial(samples, null);

    // Then: should return false
    assertFalse(beneficial);
  }

  @Test
  void testIsCompressionBeneficialWithEmptySymbolTable() {
    // Given: empty symbol table
    final List<byte[]> samples = Arrays.asList(
        "test data one".getBytes(StandardCharsets.UTF_8),
        "test data two".getBytes(StandardCharsets.UTF_8)
    );

    // When: checking with empty table
    boolean beneficial = FSSTCompressor.isCompressionBeneficial(samples, new byte[0]);

    // Then: should return false
    assertFalse(beneficial);
  }

  @Test
  void testIsCompressionBeneficialWithEmptySamples() {
    // Given: empty samples list
    final List<byte[]> samples = new ArrayList<>();
    final byte[] symbolTable = new byte[]{1, 2, 3}; // Dummy table

    // When: checking with empty samples
    boolean beneficial = FSSTCompressor.isCompressionBeneficial(samples, symbolTable);

    // Then: should return false
    assertFalse(beneficial);
  }

  @Test
  void testDecodePerformanceNoPrimitiveBoxing() {
    // Given: data that will be encoded and decoded
    // Need enough samples to exceed MIN_TOTAL_BYTES_FOR_TABLE (4096 bytes)
    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 150; i++) {
      // Each sample is ~50 bytes, 150 samples = 7500 bytes > 4096
      samples.add(("{\"id\":" + i + ",\"value\":\"test_data_item_number_" + i + "\"}").getBytes(StandardCharsets.UTF_8));
    }
    final byte[] symbolTable = FSSTCompressor.buildSymbolTable(samples);
    assertNotNull(symbolTable, "Symbol table should not be null with 150 samples");
    assertTrue(symbolTable.length > 0, "Symbol table should have entries");

    // Encode a sample
    final byte[] input = "{\"id\":999,\"value\":\"test_data_item_999\"}".getBytes(StandardCharsets.UTF_8);
    final byte[] encoded = FSSTCompressor.encode(input, symbolTable);

    // When: decoding many times (performance test)
    long startTime = System.nanoTime();
    byte[] decoded = null;
    for (int i = 0; i < 10000; i++) {
      decoded = FSSTCompressor.decode(encoded, symbolTable);
    }
    long endTime = System.nanoTime();
    long durationMs = (endTime - startTime) / 1_000_000;

    // Then: should complete quickly (< 1 second for 10K decodes) and be correct
    assertArrayEquals(input, decoded);
    assertTrue(durationMs < 1000, "10K decodes should complete in < 1 second, took " + durationMs + "ms");
  }

  @Test
  void testMinCompressionRatioConstant() {
    // Verify the constant is set correctly
    assertEquals(0.15, FSSTCompressor.MIN_COMPRESSION_RATIO, 0.001);
  }
}

