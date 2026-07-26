/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Byte-level RLE compressor/decompressor optimised for runs of <em>any</em>
 * repeating byte — not just zero. Strengthens the
 * {@link ZeroRunByteCodec} when the input contains long constant-byte runs
 * of non-zero values. The dominant remaining redundancy pattern in Sirix
 * record heaps once the structural codecs (offset-table dedup, PAX regions,
 * hash elision) have done their work is:
 *
 * <ul>
 *   <li>Zero-byte runs (varint high bytes of small integers, NULL sibling
 *       keys, zero revision counters, null descendant counts). The existing
 *       {@link ZeroRunByteCodec} already catches these.</li>
 *   <li><b>Non-zero repeating bytes</b>: sequences like
 *       {@code 0x01 0x01 0x01 ...} from {@code prev_revision} or
 *       {@code last_mod_revision} varints encoding a constant revision
 *       number across many slots; {@code 0xFF 0xFF ...} from sentinel
 *       deltas; kindId bytes when a page is dominated by a single kind
 *       (common for PathSummary / index pages). {@link ZeroRunByteCodec}
 *       treats these as literals — this codec catches them.</li>
 * </ul>
 *
 * <h2>Wire format</h2>
 * <pre>
 *   byte    marker = 0xFE   // distinguishes V2 frames from V1 (0xFF)
 *   varint  uncompressedSize
 *   body:
 *     - literal run:
 *         byte    0x00..0x7F   // lengthMinus1 (1..128 literal bytes)
 *         byte[]  literal
 *     - short zero run:
 *         byte    0x80..0xBF   // 0x80+(lengthMinus2) ⇒ 2..65 zero bytes
 *     - short constant-byte run (non-zero):
 *         byte    0xC0..0xFC   // 0xC0+(lengthMinus2) ⇒ 2..62
 *         byte    value        // the repeated byte (non-zero)
 *     - long zero run:
 *         byte    0xFD
 *         varint  zeroCount     // ≥ 2
 *     - long constant-byte run (non-zero):
 *         byte    0xFE
 *         varint  runCount      // ≥ 2
 *         byte    value         // the repeated byte
 *     - unused:
 *         byte    0xFF         // reserved — future use
 * </pre>
 *
 * <p>Encoder strategy: single-pass greedy — at each position, detect the
 * longest run of identical bytes starting there. If the run is long enough
 * (≥ 2), emit as a zero-run or constant-byte-run. Otherwise accumulate
 * into a literal run until the next worthwhile run is encountered.
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
public final class ByteRunCodec {

  /** Start-of-frame marker (distinct from {@link ZeroRunByteCodec#FRAME_MARKER}). */
  public static final byte FRAME_MARKER = (byte) 0xFE;

  /** Worst-case encoded size for {@code uncompressedSize} bytes of input. */
  public static int maxEncodedSize(final int uncompressedSize) {
    if (uncompressedSize < 0) {
      throw new IllegalArgumentException("uncompressedSize=" + uncompressedSize);
    }
    // 1 byte marker + up to 5 bytes varint + literal headers (1 per 128 bytes) + literals.
    final int literalHeaders = (uncompressedSize + 127) / 128;
    return 1 + 5 + literalHeaders + uncompressedSize;
  }

  private ByteRunCodec() {}

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
      // Detect run of identical bytes starting at i.
      final byte first = input.get(ValueLayout.JAVA_BYTE, inputOff + i);
      int runLen = 1;
      // Scan for up to some sane bound; long runs are capped by encoder
      // (long-run marker handles any length).
      while (i + runLen < inputLength
          && input.get(ValueLayout.JAVA_BYTE, inputOff + i + runLen) == first) {
        runLen++;
      }

      if (runLen >= 2) {
        if (first == 0) {
          // Zero run.
          if (runLen <= 65) {
            output[outPos++] = (byte) (0x80 + (runLen - 2)); // 0x80..0xBF
            i += runLen;
          } else {
            output[outPos++] = (byte) 0xFD;
            outPos = writeVarint(output, outPos, runLen);
            i += runLen;
          }
        } else {
          // Non-zero constant-byte run.
          if (runLen <= 62) {
            output[outPos++] = (byte) (0xC0 + (runLen - 2)); // 0xC0..0xFC
            output[outPos++] = first;
            i += runLen;
          } else {
            output[outPos++] = (byte) 0xFE;
            outPos = writeVarint(output, outPos, runLen);
            output[outPos++] = first;
            i += runLen;
          }
        }
        continue;
      }
      // Literal run. Consume up to 128 non-run bytes (stop if we see ≥ 2
      // consecutive identical bytes that's worth emitting as a run).
      int litStart = i;
      int litEnd = i;
      while (litEnd < inputLength && (litEnd - litStart) < 128) {
        // Stop if we see ≥ 2 consecutive identical bytes (so next iteration can
        // emit a run).
        if (litEnd + 1 < inputLength
            && input.get(ValueLayout.JAVA_BYTE, inputOff + litEnd)
                == input.get(ValueLayout.JAVA_BYTE, inputOff + litEnd + 1)) {
          break;
        }
        litEnd++;
      }
      final int litLen = litEnd - litStart;
      if (litLen == 0) {
        // Defensive: should not happen because runLen would have been ≥ 2.
        break;
      }
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
      throw new IllegalStateException("ByteRunCodec: missing frame marker");
    }
    final long[] vr = readVarintPacked(input, inPos);
    inPos = (int) vr[1];
    final int uncompressed = (int) vr[0];

    long outPos = outputOff;
    final long outEnd = outputOff + uncompressed;
    while (outPos < outEnd) {
      if (inPos >= inEnd) {
        throw new IllegalStateException("ByteRunCodec: input exhausted");
      }
      final int tag = input[inPos++] & 0xFF;
      if (tag < 0x80) {
        // Literal: (tag + 1) bytes follow.
        final int litLen = tag + 1;
        if (outPos + litLen > outEnd) {
          throw new IllegalStateException("ByteRunCodec: literal overflow");
        }
        MemorySegment.copy(input, inPos, output, ValueLayout.JAVA_BYTE, outPos, litLen);
        inPos += litLen;
        outPos += litLen;
      } else if (tag < 0xC0) {
        // Short zero run: length = tag - 0x80 + 2.
        final int runLen = tag - 0x80 + 2;
        if (outPos + runLen > outEnd) {
          throw new IllegalStateException("ByteRunCodec: zero-run overflow");
        }
        fillByte(output, outPos, runLen, (byte) 0);
        outPos += runLen;
      } else if (tag <= 0xFC) {
        // Short non-zero constant-byte run: length = tag - 0xC0 + 2.
        final int runLen = tag - 0xC0 + 2;
        if (inPos >= inEnd) {
          throw new IllegalStateException("ByteRunCodec: missing value byte for run");
        }
        final byte value = input[inPos++];
        if (outPos + runLen > outEnd) {
          throw new IllegalStateException("ByteRunCodec: const-run overflow");
        }
        fillByte(output, outPos, runLen, value);
        outPos += runLen;
      } else if (tag == 0xFD) {
        // Long zero run: varint(runLen).
        final long[] vr2 = readVarintPacked(input, inPos);
        inPos = (int) vr2[1];
        final int runLen = (int) vr2[0];
        if (outPos + runLen > outEnd) {
          throw new IllegalStateException("ByteRunCodec: long zero-run overflow");
        }
        fillByte(output, outPos, runLen, (byte) 0);
        outPos += runLen;
      } else if (tag == 0xFE) {
        // Long non-zero constant-byte run: varint(runLen) + byte.
        final long[] vr2 = readVarintPacked(input, inPos);
        inPos = (int) vr2[1];
        final int runLen = (int) vr2[0];
        if (inPos >= inEnd) {
          throw new IllegalStateException("ByteRunCodec: missing value byte for long run");
        }
        final byte value = input[inPos++];
        if (outPos + runLen > outEnd) {
          throw new IllegalStateException("ByteRunCodec: long const-run overflow");
        }
        fillByte(output, outPos, runLen, value);
        outPos += runLen;
      } else {
        // 0xFF reserved.
        throw new IllegalStateException("ByteRunCodec: reserved tag 0xFF");
      }
    }
    return uncompressed;
  }

  /** Bulk byte-fill on an output segment; LLVM folds to a vectorised memset. */
  private static void fillByte(final MemorySegment output, final long offset, final int len,
      final byte value) {
    output.asSlice(offset, len).fill(value);
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
