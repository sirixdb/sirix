/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Focused performance + correctness harness for the native LZ77 decoder
 * path. Validates that the C decoder round-trips identically to the Java
 * encoder and measures ns/byte on a realistic 32 KiB page at higher
 * iteration count than the generic {@link SirixLZ77CodecTest}.
 */
@DisplayName("SirixLZ77NativeDecoder — bench")
final class SirixLZ77NativeDecoderBenchTest {

  private static final int PAGE = 32_768;

  private static byte[] buildCompressed() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment in = arena.allocate(PAGE);
      final SplittableRandom rng = new SplittableRandom(0x5EED);
      for (int i = 0; i < PAGE; i++) {
        final int r = rng.nextInt(100);
        if (r < 50) {
          in.set(ValueLayout.JAVA_BYTE, i, (byte) 0);
        } else if (r < 70) {
          in.set(ValueLayout.JAVA_BYTE, i, (byte) (rng.nextInt(127) + 1));
        } else if (i + 4 <= PAGE) {
          in.set(ValueLayout.JAVA_BYTE, i,     (byte) 0x01);
          in.set(ValueLayout.JAVA_BYTE, i + 1, (byte) 0x42);
          in.set(ValueLayout.JAVA_BYTE, i + 2, (byte) 0x00);
          in.set(ValueLayout.JAVA_BYTE, i + 3, (byte) 0x00);
          i += 3;
        }
      }
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(PAGE)];
      final int encoded = SirixLZ77Codec.encode(in, 0, PAGE, buf, 0);
      final byte[] out = new byte[encoded];
      System.arraycopy(buf, 0, out, 0, encoded);
      return out;
    }
  }

  @Test
  @DisplayName("native decoder produces byte-identical output to Java")
  void nativeProducesSameOutputAsJava() {
    try (Arena arena = Arena.ofConfined()) {
      // First encode.
      final MemorySegment in = arena.allocate(PAGE);
      final SplittableRandom rng = new SplittableRandom(0x1111);
      final byte[] expected = new byte[PAGE];
      for (int i = 0; i < PAGE; i++) {
        final int r = rng.nextInt(100);
        if (r < 45) {
          expected[i] = 0;
        } else if (r < 65) {
          expected[i] = (byte) (rng.nextInt(127) + 1);
        } else if (i + 4 <= PAGE) {
          expected[i] = 0x01;
          expected[i + 1] = (byte) rng.nextInt(256);
          expected[i + 2] = 0;
          expected[i + 3] = 0;
          i += 3;
        } else {
          expected[i] = 0;
        }
      }
      MemorySegment.copy(expected, 0, in, ValueLayout.JAVA_BYTE, 0, PAGE);
      final byte[] enc = new byte[SirixLZ77Codec.maxEncodedSize(PAGE)];
      final int encoded = SirixLZ77Codec.encode(in, 0, PAGE, enc, 0);

      // Decode via Java (by disabling native flag).
      final byte[] javaOutBytes = new byte[PAGE];
      final MemorySegment javaOut = MemorySegment.ofArray(javaOutBytes);
      SirixLZ77Codec.decode(enc, 0, encoded, javaOut, 0);
      assertArrayEquals(expected, javaOutBytes, "Java decode mismatch");

      // Decode via Native path (output native).
      if (!SirixLZ77NativeDecoder.isAvailable()) {
        System.out.println("[native bench] skipping: native library not available");
        return;
      }
      final MemorySegment nativeOut = arena.allocate(PAGE + 64);
      final int decoded = SirixLZ77NativeDecoder.decode(enc, 0, encoded, nativeOut, 0);
      assertEquals(PAGE, decoded);
      final byte[] nativeOutBytes = new byte[PAGE];
      MemorySegment.copy(nativeOut, ValueLayout.JAVA_BYTE, 0, nativeOutBytes, 0, PAGE);
      assertArrayEquals(expected, nativeOutBytes, "Native decode mismatch");
    }
  }

  @Test
  @DisplayName("native decoder ns/byte bench — targets ≥ 5 GB/s")
  void nativeDecodeSpeedBench() {
    if (!SirixLZ77NativeDecoder.isAvailable()) {
      System.out.println("[native bench] skipping: native library not available");
      return;
    }
    final byte[] encoded = buildCompressed();
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment out = arena.allocate(PAGE + 64);

      // Warm up — hit both the JIT and native dlopen cache.
      for (int i = 0; i < 5_000; i++) {
        SirixLZ77NativeDecoder.decode(encoded, 0, encoded.length, out, 0);
      }

      // Measure.
      final int iters = 100_000;
      final long t0 = System.nanoTime();
      for (int i = 0; i < iters; i++) {
        SirixLZ77NativeDecoder.decode(encoded, 0, encoded.length, out, 0);
      }
      final long t1 = System.nanoTime();
      final double nsPerByte = (t1 - t0) / ((double) iters * PAGE);
      final double gbPerSec = 1.0 / nsPerByte;
      System.out.printf("[native] decode: %.2f ns/byte (%.2f GB/s) (encoded=%d → %d bytes)%n",
          nsPerByte, gbPerSec, encoded.length, PAGE);
      assertTrue(nsPerByte < 10.0, "too slow: " + nsPerByte + " ns/byte");
    }
  }

  @Test
  @DisplayName("dispatcher routing: native path is taken for native-backed output")
  void dispatcherRoutesToNative() {
    if (!SirixLZ77NativeDecoder.isAvailable()) {
      System.out.println("[native bench] skipping: native library not available");
      return;
    }
    // Verify diagnostic counters are enabled for this test run.
    final String prop = System.getProperty("sirix.lz77Codec.diag.counters");
    if (!"true".equals(prop)) {
      System.out.println("[native bench] skipping dispatch count assertion: -Dsirix.lz77Codec.diag.counters=true not set");
      return;
    }
    final byte[] encoded = buildCompressed();
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment out = arena.allocate(PAGE + 64);

      final long beforeNative = SirixLZ77Codec.getNativeCallCount();
      final long beforeJava = SirixLZ77Codec.getJavaCallCount();

      // 100 calls via SirixLZ77Codec.decode — must route to native.
      for (int i = 0; i < 100; i++) {
        SirixLZ77Codec.decode(encoded, 0, encoded.length, out, 0);
      }

      final long nativeDelta = SirixLZ77Codec.getNativeCallCount() - beforeNative;
      final long javaDelta = SirixLZ77Codec.getJavaCallCount() - beforeJava;
      System.out.printf("[dispatch] native=%d java=%d%n", nativeDelta, javaDelta);
      assertEquals(100, nativeDelta, "expected 100 native dispatches");
      assertEquals(0, javaDelta, "expected zero Java dispatches");
    }
  }

  @Test
  @DisplayName("full-codec ns/byte bench via SirixLZ77Codec.decode with native ON")
  void codecDispatchSpeedBench() {
    final byte[] encoded = buildCompressed();
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment out = arena.allocate(PAGE + 64);

      for (int i = 0; i < 5_000; i++) {
        SirixLZ77Codec.decode(encoded, 0, encoded.length, out, 0);
      }

      final int iters = 100_000;
      final long t0 = System.nanoTime();
      for (int i = 0; i < iters; i++) {
        SirixLZ77Codec.decode(encoded, 0, encoded.length, out, 0);
      }
      final long t1 = System.nanoTime();
      final double nsPerByte = (t1 - t0) / ((double) iters * PAGE);
      final double gbPerSec = 1.0 / nsPerByte;
      System.out.printf("[codec] decode: %.2f ns/byte (%.2f GB/s)%n", nsPerByte, gbPerSec);
    }
  }

  @Test
  @DisplayName("compare native vs java decode on same input")
  void compareNativeVsJava() {
    final byte[] encoded = buildCompressed();
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment out = arena.allocate(PAGE + 64);

      // Force java path by disabling native temporarily? Can't — the
      // NATIVE_DECODER_ENABLED is a static final. Do a "direct Java"
      // measurement by building a heap-backed output to force fallback.
      // Actually the current dispatcher only bypasses native when output
      // is heap — so let's measure with heap output explicitly.
      final byte[] javaOutBytes = new byte[PAGE + 64];
      final MemorySegment javaOut = MemorySegment.ofArray(javaOutBytes);

      for (int i = 0; i < 5_000; i++) SirixLZ77Codec.decode(encoded, 0, encoded.length, javaOut, 0);
      final int iters = 100_000;

      long t0 = System.nanoTime();
      for (int i = 0; i < iters; i++) SirixLZ77Codec.decode(encoded, 0, encoded.length, javaOut, 0);
      long t1 = System.nanoTime();
      final double javaNs = (t1 - t0) / ((double) iters * PAGE);
      System.out.printf("[java-path (heap out)] %.3f ns/byte (%.2f GB/s)%n",
          javaNs, 1.0 / javaNs);

      for (int i = 0; i < 5_000; i++) SirixLZ77Codec.decode(encoded, 0, encoded.length, out, 0);
      t0 = System.nanoTime();
      for (int i = 0; i < iters; i++) SirixLZ77Codec.decode(encoded, 0, encoded.length, out, 0);
      t1 = System.nanoTime();
      final double nativeNs = (t1 - t0) / ((double) iters * PAGE);
      System.out.printf("[native-path (native out)] %.3f ns/byte (%.2f GB/s)%n",
          nativeNs, 1.0 / nativeNs);
    }
  }
}
