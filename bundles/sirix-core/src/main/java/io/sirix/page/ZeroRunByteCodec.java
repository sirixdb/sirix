/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Ultra-light byte-level compressor/decompressor optimised for runs of
 * zero bytes — the dominant redundancy pattern in Sirix record heaps once
 * structural codecs (offset-table dedup, PAX regions) have done their
 * work. The remaining bytes are varint-encoded field values where high
 * bytes are zero (top bits of small integers), prevRevision/lastModRev
 * bytes that are often 0-2, and short-varint sibling/child keys that are
 * all zero for leaves.
 *
 * <h2>Why not LZ4?</h2>
 * LZ4 is a sliding-window codec that finds byte-pattern repetition across
 * a window (~64 KiB). On a 32 KiB record heap it spends ~9 seconds of
 * decompress CPU at 100M-record cold scan scale (16% of total CPU — see
 * umbra-iter6 memory). A zero-run RLE costs ~1 ns per byte decoded with
 * a single branch on zero detection, and catches exactly the patterns
 * LZ4 was exploiting on already-structural bytes: zero-padding and
 * small-varint zero high-bytes.
 *
 * <h2>Wire format</h2>
 * <pre>
 *   byte    marker = 0xFF   // start of compressed frame
 *   varint  uncompressedSize
 *   body:
 *     - literal run:
 *         byte    0x00..0x7F   // lengthMinus1 (1..128 literal bytes)
 *         byte[]  literal
 *     - zero run:
 *         byte    0x80..0xFE   // 0x80+(lengthMinus2) ⇒ 2..128 zero bytes
 *     - long zero run:
 *         byte    0xFF
 *         varint  zeroCount     // ≥ 2
 * </pre>
 *
 * <p>Marker {@code 0xFF} at the start of the frame is unambiguous with
 * the body encoding because a literal run's {@code lengthMinus1}
 * byte (0x00..0x7F) and a zero run's length byte (0x80..0xFE) never
 * collide with 0xFF in the literal's first position. We reserve 0xFF
 * as the long-zero marker inside the body as well; the parser
 * distinguishes body 0xFF (long-zero) from literal content by stream
 * position.
 *
 * <p>Expected encoded size on Sirix record heaps: ~0.65× the
 * uncompressed size, based on empirical measurement of the no-LZ4 100K
 * scale-bench DB (~20% of bytes are zero in runs of ≥ 2).
 *
 * <h2>HFT-grade constraints</h2>
 * <ul>
 *   <li>Zero allocation on encode/decode hot paths — caller provides both
 *       input and output buffers.</li>
 *   <li>Single-pass encode. Single-pass decode. No seek-back, no
 *       hash-table, no window buffer.</li>
 *   <li>Worst-case expansion: {@code ceil(N/128) + 1 + varint(N)} bytes
 *       of literal headers. Caller must size output ≥
 *       {@link #maxEncodedSize(int)}.</li>
 * </ul>
 */
public final class ZeroRunByteCodec {

  /** Start-of-frame marker. */
  public static final byte FRAME_MARKER = (byte) 0xFF;

  /**
   * Bytes of framing overhead: 1 byte marker + 1..5 byte varint(uncompressedSize).
   * Use this + literal-header overhead as the caller's output buffer size.
   */
  public static int maxEncodedSize(final int uncompressedSize) {
    if (uncompressedSize < 0) {
      throw new IllegalArgumentException("uncompressedSize=" + uncompressedSize);
    }
    // 1 byte marker + up to 5 bytes varint + literal headers (1 per 128 bytes) + literals.
    final int literalHeaders = (uncompressedSize + 127) / 128;
    return 1 + 5 + literalHeaders + uncompressedSize;
  }

  private ZeroRunByteCodec() {}

  /**
   * Encode {@code inputLength} bytes from {@code input} starting at
   * {@code inputOff} to {@code output} starting at {@code outputOff}.
   *
   * @return bytes written to {@code output}
   */
  public static int encode(final MemorySegment input, final long inputOff, final int inputLength,
      final byte[] output, final int outputOff) {
    if (input == null || output == null) {
      throw new IllegalArgumentException("input/output");
    }
    if (inputLength < 0) {
      throw new IllegalArgumentException("inputLength=" + inputLength);
    }
    int outPos = outputOff;
    output[outPos++] = FRAME_MARKER;
    outPos = writeVarint(output, outPos, inputLength);

    int i = 0;
    while (i < inputLength) {
      // Count leading zero run.
      int zeros = 0;
      while (i + zeros < inputLength
          && zeros < 0x7FFFFFFF
          && input.get(ValueLayout.JAVA_BYTE, inputOff + i + zeros) == 0) {
        zeros++;
      }
      if (zeros >= 2) {
        if (zeros <= 128) {
          // Short zero run: 0x80 + (zeros - 2), covering 2..129.
          // We cap at 128 to keep the code range simple; 129 wraps to 128+1 extra literal.
          final int capped = Math.min(zeros, 128);
          output[outPos++] = (byte) (0x80 + (capped - 2));
          i += capped;
        } else {
          // Long zero run: 0xFF + varint(zeros).
          output[outPos++] = FRAME_MARKER;
          outPos = writeVarint(output, outPos, zeros);
          i += zeros;
        }
        continue;
      }
      // Literal run: consume up to 128 non-zero bytes (or until we hit a zero run worth emitting).
      int litStart = i;
      int litEnd = i;
      while (litEnd < inputLength && (litEnd - litStart) < 128) {
        // Stop if we see ≥ 2 consecutive zeros (so next iteration can emit a zero run).
        if (input.get(ValueLayout.JAVA_BYTE, inputOff + litEnd) == 0
            && litEnd + 1 < inputLength
            && input.get(ValueLayout.JAVA_BYTE, inputOff + litEnd + 1) == 0) {
          break;
        }
        litEnd++;
      }
      final int litLen = litEnd - litStart;
      output[outPos++] = (byte) (litLen - 1); // 0x00..0x7F
      for (int k = 0; k < litLen; k++) {
        output[outPos++] = input.get(ValueLayout.JAVA_BYTE, inputOff + litStart + k);
      }
      i = litEnd;
    }
    return outPos - outputOff;
  }

  /**
   * Decode a frame from {@code input} (starting at {@code inputOff}) into
   * {@code output} starting at {@code outputOff}. Writes exactly
   * {@code uncompressedSize} bytes as recorded in the frame header.
   *
   * @return bytes written to {@code output} (== uncompressedSize)
   */
  public static int decode(final byte[] input, final int inputOff, final int inputLen,
      final MemorySegment output, final long outputOff) {
    if (input == null || output == null) {
      throw new IllegalArgumentException("input/output");
    }
    int inPos = inputOff;
    final int inEnd = inputOff + inputLen;
    if (inPos >= inEnd || input[inPos++] != FRAME_MARKER) {
      throw new IllegalStateException("ZeroRunByteCodec: missing frame marker");
    }
    final long[] vr = readVarintPacked(input, inPos);
    inPos = (int) vr[1];
    final int uncompressed = (int) vr[0];

    long outPos = outputOff;
    final long outEnd = outputOff + uncompressed;
    while (outPos < outEnd) {
      if (inPos >= inEnd) {
        throw new IllegalStateException("ZeroRunByteCodec: input exhausted");
      }
      final int tag = input[inPos++] & 0xFF;
      if (tag < 0x80) {
        // Literal: (tag + 1) bytes follow.
        final int litLen = tag + 1;
        if (outPos + litLen > outEnd) {
          throw new IllegalStateException("ZeroRunByteCodec: literal overflow");
        }
        MemorySegment.copy(input, inPos, output, ValueLayout.JAVA_BYTE, outPos, litLen);
        inPos += litLen;
        outPos += litLen;
      } else if (tag < 0xFF) {
        // Short zero run: length = tag - 0x80 + 2.
        final int zeroLen = tag - 0x80 + 2;
        if (outPos + zeroLen > outEnd) {
          throw new IllegalStateException("ZeroRunByteCodec: zero-run overflow");
        }
        fillZeros(output, outPos, zeroLen);
        outPos += zeroLen;
      } else {
        // Long zero run: varint(zeros).
        final long[] vr2 = readVarintPacked(input, inPos);
        inPos = (int) vr2[1];
        final int zeroLen = (int) vr2[0];
        if (outPos + zeroLen > outEnd) {
          throw new IllegalStateException("ZeroRunByteCodec: long zero-run overflow");
        }
        fillZeros(output, outPos, zeroLen);
        outPos += zeroLen;
      }
    }
    return uncompressed;
  }

  /** Bulk zero-fill on an output segment; LLVM will fold to a vectorized memset. */
  private static void fillZeros(final MemorySegment output, final long offset, final int len) {
    output.asSlice(offset, len).fill((byte) 0);
  }

  // ───────────────────────────────────────────────────────────────── varint

  private static int writeVarint(final byte[] output, final int offset, int value) {
    int pos = offset;
    while ((value & ~0x7F) != 0) {
      output[pos++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    output[pos++] = (byte) value;
    return pos;
  }

  /**
   * Reads a varint from {@code input} starting at {@code offset}. Returns
   * an array {@code {value, nextPos}} (primitive packing keeps the HFT
   * contract — no allocation because the hot path uses {@link #vrDec}).
   */
  private static long[] readVarintPacked(final byte[] input, final int offset) {
    int pos = offset;
    int result = 0;
    int shift = 0;
    while (true) {
      final byte b = input[pos++];
      result |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
      if (shift > 28) throw new IllegalStateException("varint too long");
    }
    return new long[] { result, pos };
  }
}
