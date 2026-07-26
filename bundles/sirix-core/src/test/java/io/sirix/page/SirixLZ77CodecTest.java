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
 * Round-trip + compression-ratio tests for {@link SirixLZ77Codec} — the
 * schema-tuned single-pass LZ77 variant used per-page on the Sirix
 * {@link KeyValueLeafPage} write path.
 */
@DisplayName("SirixLZ77Codec")
final class SirixLZ77CodecTest {

  @Test
  @DisplayName("empty input encodes to frame header only, decodes to empty")
  void emptyRoundTrip() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment in = arena.allocate(0);
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(0)];
      final int encoded = SirixLZ77Codec.encode(in, 0, 0, buf, 0);
      // 1 marker + 1 varint(0) + 1 token(0)
      assertEquals(3, encoded);
      final MemorySegment out = arena.allocate(1);
      final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      assertEquals(0, decoded);
    }
  }

  @Test
  @DisplayName("single byte round-trip")
  void singleByte() {
    try (Arena arena = Arena.ofConfined()) {
      for (int v = 0; v < 256; v++) {
        final MemorySegment in = arena.allocate(1);
        in.set(ValueLayout.JAVA_BYTE, 0, (byte) v);
        final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(1)];
        final int encoded = SirixLZ77Codec.encode(in, 0, 1, buf, 0);
        final MemorySegment out = arena.allocate(1);
        final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
        assertEquals(1, decoded);
        assertEquals((byte) v, out.get(ValueLayout.JAVA_BYTE, 0), "v=" + v);
      }
    }
  }

  @Test
  @DisplayName("short input below LZ77_MIN_LENGTH emits as pure literal token")
  void shortInputLiteralOnly() {
    try (Arena arena = Arena.ofConfined()) {
      // Test every length from 1 to LZ77_MIN_LENGTH - 1.
      final SplittableRandom rng = new SplittableRandom(0xABCDEF);
      for (int n = 1; n < SirixLZ77Codec.LZ77_MIN_LENGTH; n++) {
        final byte[] expected = new byte[n];
        final MemorySegment in = arena.allocate(n);
        for (int i = 0; i < n; i++) {
          expected[i] = (byte) rng.nextInt(256);
          in.set(ValueLayout.JAVA_BYTE, i, expected[i]);
        }
        final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
        final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
        final MemorySegment out = arena.allocate(n);
        final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
        assertEquals(n, decoded, "n=" + n);
        final byte[] actual = new byte[n];
        MemorySegment.copy(out, ValueLayout.JAVA_BYTE, 0L, actual, 0, n);
        assertArrayEquals(expected, actual, "n=" + n);
      }
    }
  }

  @Test
  @DisplayName("all-zero input compresses to tiny token stream")
  void allZerosCompressesAggressively() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 4096;
      final MemorySegment in = arena.allocate(n);
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
      // A single long match covers 4084 bytes (n - MFLIMIT), 12 literals tail,
      // + some framing. Expect far below 50 bytes.
      assertTrue(encoded < 50,
          "all-zero " + n + " should encode to < 50 bytes, got " + encoded);
      final MemorySegment out = arena.allocate(n);
      final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      for (int i = 0; i < n; i++) {
        assertEquals(0, out.get(ValueLayout.JAVA_BYTE, i), "byte " + i);
      }
    }
  }

  @Test
  @DisplayName("all-non-zero random bytes round-trip with little overhead")
  void randomNonZeroRoundTrip() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 1024;
      final MemorySegment in = arena.allocate(n);
      final SplittableRandom rng = new SplittableRandom(0xDEADBEEF);
      final byte[] expected = new byte[n];
      for (int i = 0; i < n; i++) {
        expected[i] = (byte) (rng.nextInt(255) + 1); // 1..255
        in.set(ValueLayout.JAVA_BYTE, i, expected[i]);
      }
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
      // Random data should be near-incompressible; expect overhead within ~5%.
      assertTrue(encoded < n + 80, "overhead too large: encoded=" + encoded + " n=" + n);
      final MemorySegment out = arena.allocate(n);
      final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      for (int i = 0; i < n; i++) {
        assertEquals(expected[i], out.get(ValueLayout.JAVA_BYTE, i), "byte " + i);
      }
    }
  }

  @Test
  @DisplayName("repeating 4-byte pattern catches LZ77 back-references")
  void repeatingPatternCompresses() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 4096;
      final MemorySegment in = arena.allocate(n);
      // Repeat a 4-byte pattern.
      final byte[] pattern = {0x01, 0x02, 0x03, 0x04};
      for (int i = 0; i < n; i++) {
        in.set(ValueLayout.JAVA_BYTE, i, pattern[i % 4]);
      }
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
      // Should compress to well under 100 bytes: first 4 literal, one long match.
      assertTrue(encoded < 100,
          "repeating pattern " + n + " should encode to < 100 bytes, got " + encoded);
      final MemorySegment out = arena.allocate(n);
      final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      for (int i = 0; i < n; i++) {
        assertEquals(pattern[i % 4], out.get(ValueLayout.JAVA_BYTE, i), "byte " + i);
      }
    }
  }

  @Test
  @DisplayName("overlapping match (distance < matchLen) round-trips correctly")
  void overlappingMatch() {
    // Construct an input where LZ77 naturally finds overlapping matches:
    // a short cycle repeated, e.g. "ABCDABCDABCD..." The encoder finds
    // a 4-byte match at offset 4 with distance 4 and matchLen grows past 4.
    // This tests the decoder's overlapping-match byte-by-byte copy path.
    try (Arena arena = Arena.ofConfined()) {
      final int n = 1024;
      final MemorySegment in = arena.allocate(n);
      final byte[] cycle = {'A', 'B', 'C', 'D'};
      for (int i = 0; i < n; i++) {
        in.set(ValueLayout.JAVA_BYTE, i, cycle[i % 4]);
      }
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
      final MemorySegment out = arena.allocate(n);
      final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      for (int i = 0; i < n; i++) {
        assertEquals(cycle[i % 4], out.get(ValueLayout.JAVA_BYTE, i), "byte " + i);
      }
    }
  }

  @Test
  @DisplayName("single long match with overlap distance=1 (RLE-like run)")
  void longOverlapDist1() {
    // Distance-1 match is the degenerate RLE case: same byte repeated.
    // The decoder MUST use byte-by-byte copy (dist < matchLen) or the output
    // corrupts (MemorySegment.copy with overlap is undefined).
    try (Arena arena = Arena.ofConfined()) {
      final int n = 2048;
      final MemorySegment in = arena.allocate(n);
      for (int i = 0; i < n; i++) {
        in.set(ValueLayout.JAVA_BYTE, i, (byte) 0x42);
      }
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
      final MemorySegment out = arena.allocate(n);
      final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      for (int i = 0; i < n; i++) {
        assertEquals(0x42, out.get(ValueLayout.JAVA_BYTE, i) & 0xFF, "byte " + i);
      }
    }
  }

  @Test
  @DisplayName("mixed literals + matches + long-run tail round-trip identity")
  void mixedRoundTrip() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 8192;
      final MemorySegment in = arena.allocate(n);
      final SplittableRandom rng = new SplittableRandom(0xCAFEBABE);
      final byte[] expected = new byte[n];
      // First 2 KB: random bytes (incompressible).
      for (int i = 0; i < 2048; i++) {
        expected[i] = (byte) rng.nextInt(256);
      }
      // Next 2 KB: repeat the first 256 bytes 8 times (back-references).
      for (int i = 0; i < 2048; i++) {
        expected[2048 + i] = expected[i % 256];
      }
      // Next 2 KB: zero run.
      for (int i = 0; i < 2048; i++) {
        expected[4096 + i] = 0;
      }
      // Last 2 KB: realistic record-header pattern —
      // 0x01, templateId, 0x00, 0x00 repeating.
      for (int i = 0; i < 2048; i++) {
        expected[6144 + i] = (byte) ((i & 3) == 0 ? 0x01 : (i & 3) == 1 ? 0x42 : 0x00);
      }
      MemorySegment.copy(expected, 0, in, ValueLayout.JAVA_BYTE, 0L, n);
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
      // The compressible sections should dominate; expect ≤ 40% of n.
      assertTrue(encoded < (n * 4) / 10,
          "mixed input " + n + " should compress to < 40%, got " + encoded);
      final MemorySegment out = arena.allocate(n);
      final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      final byte[] actual = new byte[n];
      MemorySegment.copy(out, ValueLayout.JAVA_BYTE, 0L, actual, 0, n);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  @DisplayName("round-trip across broad length range with realistic record patterns")
  void fuzzRealisticPatternsAcrossLengths() {
    try (Arena arena = Arena.ofConfined()) {
      final SplittableRandom rng = new SplittableRandom(0x1234567890ABCDEFL);
      // Cover: tiny, small, medium, 32 KiB (typical page), slightly larger.
      final int[] sizes = {13, 50, 200, 1000, 4000, 16000, 32_000, 40_000};
      for (int n : sizes) {
        final MemorySegment in = arena.allocate(n);
        final byte[] expected = new byte[n];
        // 40% zero, 20% small varint leading byte, 40% repeating record headers.
        int i = 0;
        while (i < n) {
          final int r = rng.nextInt(100);
          if (r < 40) {
            expected[i++] = 0;
          } else if (r < 60) {
            expected[i++] = (byte) (rng.nextInt(127) + 1);
          } else {
            // 4-byte record header pattern.
            if (i + 4 <= n) {
              expected[i++] = 0x01;
              expected[i++] = (byte) (r & 0x0F);
              expected[i++] = 0;
              expected[i++] = 0;
            } else {
              expected[i++] = 0;
            }
          }
        }
        MemorySegment.copy(expected, 0, in, ValueLayout.JAVA_BYTE, 0L, n);
        final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
        final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
        final MemorySegment out = arena.allocate(n);
        final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
        assertEquals(n, decoded, "n=" + n);
        final byte[] actual = new byte[n];
        MemorySegment.copy(out, ValueLayout.JAVA_BYTE, 0L, actual, 0, n);
        assertArrayEquals(expected, actual, "n=" + n);
      }
    }
  }

  @Test
  @DisplayName("encoder output is framed with 0xFD marker")
  void frameMarker() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment in = arena.allocate(100);
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(100)];
      final int encoded = SirixLZ77Codec.encode(in, 0, 100, buf, 0);
      assertEquals((byte) 0xFD, buf[0]);
      assertTrue(encoded > 1);
    }
  }

  @Test
  @DisplayName("decoder rejects wrong frame marker")
  void rejectsWrongFrameMarker() {
    try (Arena arena = Arena.ofConfined()) {
      final byte[] bad = new byte[] { 0x00, 0x00 };
      final MemorySegment out = arena.allocate(1);
      try {
        SirixLZ77Codec.decode(bad, 0, bad.length, out, 0);
      } catch (IllegalStateException expected) {
        // expected
        return;
      }
      org.junit.jupiter.api.Assertions.fail("decoder accepted invalid frame marker");
    }
  }

  @Test
  @DisplayName("fuzz: 200 random inputs of varying length and content round-trip identity")
  void randomFuzz() {
    try (Arena arena = Arena.ofConfined()) {
      final SplittableRandom rng = new SplittableRandom(0xFEEDFACECAFEBEEFL);
      for (int trial = 0; trial < 200; trial++) {
        final int n = 1 + rng.nextInt(40_000);
        final int zeroProb = rng.nextInt(100); // % of bytes that are zero
        final MemorySegment in = arena.allocate(n);
        final byte[] expected = new byte[n];
        for (int i = 0; i < n; i++) {
          if (rng.nextInt(100) < zeroProb) {
            expected[i] = 0;
          } else {
            expected[i] = (byte) rng.nextInt(256);
          }
        }
        MemorySegment.copy(expected, 0, in, ValueLayout.JAVA_BYTE, 0L, n);
        final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
        final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
        final MemorySegment out = arena.allocate(n);
        final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
        assertEquals(n, decoded, "trial=" + trial + " n=" + n);
        final byte[] actual = new byte[n];
        MemorySegment.copy(out, ValueLayout.JAVA_BYTE, 0L, actual, 0, n);
        assertArrayEquals(expected, actual, "trial=" + trial + " n=" + n + " zeroProb=" + zeroProb);
      }
    }
  }

  @Test
  @DisplayName("long literal run overflow (> 15 + 255 bytes)")
  void longLiteralOverflow() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 3000;
      final MemorySegment in = arena.allocate(n);
      final SplittableRandom rng = new SplittableRandom(0x13371337);
      final byte[] expected = new byte[n];
      // Fill with non-repeating bytes so encoder can't find matches and
      // the whole thing becomes one giant literal run.
      for (int i = 0; i < n; i++) {
        expected[i] = (byte) ((i * 0x9E3779B1) >>> 24);
      }
      // Shuffle slightly to prevent any incidental 4-byte matches.
      for (int i = 0; i < n - 4; i += 4) {
        expected[i] = (byte) (rng.nextInt(256));
      }
      MemorySegment.copy(expected, 0, in, ValueLayout.JAVA_BYTE, 0L, n);
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
      final MemorySegment out = arena.allocate(n);
      final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      final byte[] actual = new byte[n];
      MemorySegment.copy(out, ValueLayout.JAVA_BYTE, 0L, actual, 0, n);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  @DisplayName("long match overflow (match extending past 15 + 255 bytes)")
  void longMatchOverflow() {
    try (Arena arena = Arena.ofConfined()) {
      // 2 KB of random "prefix" then 10 KB repeating that prefix —
      // should find a long match that requires overflow encoding.
      final int prefixLen = 2048;
      final int repeatLen = 10_000;
      final int n = prefixLen + repeatLen;
      final MemorySegment in = arena.allocate(n);
      final SplittableRandom rng = new SplittableRandom(0xAAA);
      final byte[] expected = new byte[n];
      for (int i = 0; i < prefixLen; i++) {
        expected[i] = (byte) rng.nextInt(256);
      }
      for (int i = 0; i < repeatLen; i++) {
        expected[prefixLen + i] = expected[i % prefixLen];
      }
      MemorySegment.copy(expected, 0, in, ValueLayout.JAVA_BYTE, 0L, n);
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
      final MemorySegment out = arena.allocate(n);
      final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      final byte[] actual = new byte[n];
      MemorySegment.copy(out, ValueLayout.JAVA_BYTE, 0L, actual, 0, n);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  @DisplayName("oversized input (> 64 KiB) falls back to literal-only and round-trips")
  void oversizedInputLiteralFallback() {
    try (Arena arena = Arena.ofConfined()) {
      // 128 KiB input — exceeds the 16-bit offset field used by the
      // generation-tagged hash table, so the encoder falls back to a
      // single literal-only token stream.
      final int n = 128 * 1024;
      final MemorySegment in = arena.allocate(n);
      final SplittableRandom rng = new SplittableRandom(0xABCD1234);
      final byte[] expected = new byte[n];
      for (int i = 0; i < n; i++) {
        expected[i] = (byte) rng.nextInt(256);
      }
      MemorySegment.copy(expected, 0, in, ValueLayout.JAVA_BYTE, 0L, n);
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
      // Because we fell back to literal-only, expect encoded ~= input + overhead.
      assertTrue(encoded >= n, "literal fallback should encode at least input size, got " + encoded);
      assertTrue(encoded < n + 1024, "literal fallback overhead too large: " + encoded);
      final MemorySegment out = arena.allocate(n);
      final int decoded = SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      final byte[] actual = new byte[n];
      MemorySegment.copy(out, ValueLayout.JAVA_BYTE, 0L, actual, 0, n);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  @DisplayName("back-to-back encodes reuse hash-table scratch without state bleed")
  void generationTagPreventsBleed() {
    try (Arena arena = Arena.ofConfined()) {
      // Encode two DIFFERENT inputs back-to-back on the same thread. If the
      // generation-tagged hash table wasn't reset properly, offsets from the
      // first encode's positions could be "found" by the second's hash
      // lookups, producing bogus matches that corrupt the output.
      final int n = 4096;
      final byte[] a = new byte[n];
      final byte[] b = new byte[n];
      final SplittableRandom rng = new SplittableRandom(0xEEEE);
      for (int i = 0; i < n; i++) {
        a[i] = (byte) rng.nextInt(256);
        b[i] = (byte) rng.nextInt(256);
      }
      final MemorySegment inA = arena.allocate(n);
      final MemorySegment inB = arena.allocate(n);
      MemorySegment.copy(a, 0, inA, ValueLayout.JAVA_BYTE, 0L, n);
      MemorySegment.copy(b, 0, inB, ValueLayout.JAVA_BYTE, 0L, n);

      final byte[] bufA = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final byte[] bufB = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encodedA = SirixLZ77Codec.encode(inA, 0, n, bufA, 0);
      final int encodedB = SirixLZ77Codec.encode(inB, 0, n, bufB, 0);

      // Decode B — the critical test. If encoder inadvertently matched into
      // A's state, the output will disagree with b.
      final MemorySegment outB = arena.allocate(n);
      final int decodedB = SirixLZ77Codec.decode(bufB, 0, encodedB, outB, 0);
      assertEquals(n, decodedB);
      final byte[] actualB = new byte[n];
      MemorySegment.copy(outB, ValueLayout.JAVA_BYTE, 0L, actualB, 0, n);
      assertArrayEquals(b, actualB);

      // For good measure also decode A and confirm correctness.
      final MemorySegment outA = arena.allocate(n);
      final int decodedA = SirixLZ77Codec.decode(bufA, 0, encodedA, outA, 0);
      assertEquals(n, decodedA);
      final byte[] actualA = new byte[n];
      MemorySegment.copy(outA, ValueLayout.JAVA_BYTE, 0L, actualA, 0, n);
      assertArrayEquals(a, actualA);
    }
  }

  @Test
  @DisplayName("decode speed micro-bench: ns/byte on realistic mixed input")
  void decodeSpeedMicrobench() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 32_768; // typical page size
      final MemorySegment in = arena.allocate(n);
      final SplittableRandom rng = new SplittableRandom(0x5EED);
      // Realistic mix.
      for (int i = 0; i < n; i++) {
        final int r = rng.nextInt(100);
        if (r < 50) {
          in.set(ValueLayout.JAVA_BYTE, i, (byte) 0);
        } else if (r < 70) {
          in.set(ValueLayout.JAVA_BYTE, i, (byte) (rng.nextInt(127) + 1));
        } else if (i + 4 <= n) {
          // Record header repeat.
          in.set(ValueLayout.JAVA_BYTE, i,     (byte) 0x01);
          in.set(ValueLayout.JAVA_BYTE, i + 1, (byte) 0x42);
          in.set(ValueLayout.JAVA_BYTE, i + 2, (byte) 0x00);
          in.set(ValueLayout.JAVA_BYTE, i + 3, (byte) 0x00);
          i += 3;
        }
      }
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);

      // Allocate output with a 32-byte wildCopy slack so the decoder
      // exercises its hot fast path (8-byte Unsafe strides). Production
      // call sites always allocate oversized staging buffers, so this
      // mirrors the real hot-path measurement.
      final MemorySegment out = arena.allocate(n + 32);

      // Warm up JIT.
      for (int i = 0; i < 2000; i++) {
        SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      }
      // Measure.
      final int iters = 50_000;
      final long t0 = System.nanoTime();
      for (int i = 0; i < iters; i++) {
        SirixLZ77Codec.decode(buf, 0, encoded, out, 0);
      }
      final long t1 = System.nanoTime();
      final double nsPerByte = (t1 - t0) / ((double) iters * n);
      final double gbPerSec = 1.0 / nsPerByte;
      System.out.printf("[SirixLZ77Codec] decode: %.2f ns/byte (%.2f GB/s) "
          + "(encoded=%d, uncompressed=%d, ratio=%.2fx)%n",
          nsPerByte, gbPerSec, encoded, n, (double) n / encoded);
      // HFT-grade target: ≥ 2 GB/s (≤ 0.5 ns/byte) on modern hardware.
      // CI boxes can be ~5× slower — use a loose sanity ceiling instead.
      assertTrue(nsPerByte < 20.0, "decode too slow: " + nsPerByte + " ns/byte");
    }
  }

  @Test
  @DisplayName("encode speed micro-bench: MB/s on realistic mixed input")
  void encodeSpeedMicrobench() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 32_768;
      final MemorySegment in = arena.allocate(n);
      final SplittableRandom rng = new SplittableRandom(0x5EED);
      for (int i = 0; i < n; i++) {
        final int r = rng.nextInt(100);
        if (r < 50) {
          in.set(ValueLayout.JAVA_BYTE, i, (byte) 0);
        } else if (r < 70) {
          in.set(ValueLayout.JAVA_BYTE, i, (byte) (rng.nextInt(127) + 1));
        } else if (i + 4 <= n) {
          in.set(ValueLayout.JAVA_BYTE, i,     (byte) 0x01);
          in.set(ValueLayout.JAVA_BYTE, i + 1, (byte) 0x42);
          in.set(ValueLayout.JAVA_BYTE, i + 2, (byte) 0x00);
          in.set(ValueLayout.JAVA_BYTE, i + 3, (byte) 0x00);
          i += 3;
        }
      }
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];

      for (int i = 0; i < 2000; i++) {
        SirixLZ77Codec.encode(in, 0, n, buf, 0);
      }
      final int iters = 20_000;
      final long t0 = System.nanoTime();
      for (int i = 0; i < iters; i++) {
        SirixLZ77Codec.encode(in, 0, n, buf, 0);
      }
      final long t1 = System.nanoTime();
      final double nsPerByte = (t1 - t0) / ((double) iters * n);
      final double mbPerSec = 1000.0 / nsPerByte;
      System.out.printf("[SirixLZ77Codec] encode: %.2f ns/byte (%.0f MB/s)%n",
          nsPerByte, mbPerSec);
      // HFT-grade target: ≥ 500 MB/s (≤ 2.0 ns/byte) on modern hardware.
      // CI boxes can be ~5× slower — use a loose sanity ceiling.
      assertTrue(nsPerByte < 50.0, "encode too slow: " + nsPerByte + " ns/byte");
    }
  }
}
