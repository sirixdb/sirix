/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * Byte-level RLE compressor/decompressor optimised for runs of <em>any</em> repeating byte — not
 * just zero. Strengthens the {@link ZeroRunByteCodec} when the input contains long constant-byte
 * runs of non-zero values. The dominant remaining redundancy pattern in Sirix record heaps once the
 * structural codecs (offset-table dedup, PAX regions, hash elision) have done their work is:
 *
 * <ul>
 * <li>Zero-byte runs (varint high bytes of small integers, NULL sibling keys, zero revision
 * counters, null descendant counts). The existing {@link ZeroRunByteCodec} already catches
 * these.</li>
 * <li><b>Non-zero repeating bytes</b>: sequences like {@code 0x01 0x01 0x01 ...} from
 * {@code prev_revision} or {@code last_mod_revision} varints encoding a constant revision number
 * across many slots; {@code 0xFF 0xFF ...} from sentinel deltas; kindId bytes when a page is
 * dominated by a single kind (common for PathSummary / index pages). {@link ZeroRunByteCodec}
 * treats these as literals — this codec catches them.</li>
 * </ul>
 *
 * <h2>Wire format</h2>
 *
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
 * <p>
 * Encoder strategy: single-pass greedy — at each position, detect the longest run of identical
 * bytes starting there. If the run is long enough (≥ 2), emit as a zero-run or constant-byte-run.
 * Otherwise accumulate into a literal run until the next worthwhile run is encountered.
 *
 * <h2>HFT-grade constraints</h2>
 * <ul>
 * <li>Zero allocation on encode/decode hot paths — caller provides both input and output
 * buffers.</li>
 * <li>Single-pass encode. Single-pass decode. No seek-back, no hash-table, no window buffer.</li>
 * <li>Worst-case expansion: about {@code 1.34 * N}, reached by input shaped {@code x y y x y y …}.
 * Caller must size output ≥ {@link #maxEncodedSize(int)}, which documents the derivation.</li>
 * </ul>
 */
public final class ByteRunCodec {

  /** Start-of-frame marker (distinct from {@link ZeroRunByteCodec#FRAME_MARKER}). */
  public static final byte FRAME_MARKER = (byte) 0xFE;

  /**
   * Worst-case encoded size for {@code uncompressedSize} bytes of input.
   *
   * <p>
   * The bound is {@code 1 + 5 + ceil(N/3) * 4}, i.e. roughly {@code 1.34 * N}, because a literal
   * token is not guaranteed to carry 128 bytes. The literal scan stops as soon as it sees two equal
   * bytes in a row so the next iteration can emit them as a run, which means a literal token can be
   * as short as a single byte — one header plus one literal, two bytes of output for one of input.
   * That lone literal is by construction followed by a run of at least two equal bytes, and a
   * non-zero run costs a marker plus the byte value, two more bytes of output for two of input. So an
   * input shaped {@code x y y x y y …} produces four output bytes for every three input bytes, and
   * that ratio is the maximum: any longer run or any zero run amortises better.
   *
   * <p>
   * An earlier version of this bound assumed one literal header per 128 input bytes and returned
   * about {@code 1.008 * N}. That holds for input with no adjacent duplicates, which is what random
   * data mostly looks like, so it survived until a benchmark fed the codec the adversarial pattern:
   * at 64 KiB the encoder wrote 87 034 bytes into a buffer sized 66 054 for it.
   */
  public static int maxEncodedSize(final int uncompressedSize) {
    if (uncompressedSize < 0) {
      throw new IllegalArgumentException("uncompressedSize=" + uncompressedSize);
    }
    // 1 byte marker + up to 5 bytes varint + 4 output bytes per 3 input bytes (see above).
    return 1 + 5 + ((uncompressedSize + 2) / 3) * 4;
  }

  private ByteRunCodec() {}

  /**
   * Encode {@code inputLength} bytes from {@code input} starting at {@code inputOff} to
   * {@code output} starting at {@code outputOff}.
   *
   * @return bytes written to {@code output}
   */
  public static int encode(final MemorySegment input, final long inputOff, final int inputLength, final byte[] output,
      final int outputOff) {
    if (input == null || output == null) {
      throw new IllegalArgumentException("input/output");
    }
    if (inputLength < 0) {
      throw new IllegalArgumentException("inputLength=" + inputLength);
    }
    // Scan a heap mirror rather than the segment: a run detector reads every byte, and the
    // foreign-memory checks behind each read cost more than the encoding does. See SegmentByteMirror.
    return encode(SegmentByteMirror.of(input, inputOff, inputLength), 0, inputLength, output, outputOff);
  }

  /**
   * Encode {@code inputLength} bytes from the array {@code input} starting at {@code inputOff} to
   * {@code output} starting at {@code outputOff}.
   *
   * @return bytes written to {@code output}
   */
  public static int encode(final byte[] input, final int inputOff, final int inputLength, final byte[] output,
      final int outputOff) {
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
      final byte first = input[inputOff + i];
      int runLen = 1;
      // Scan for up to some sane bound; long runs are capped by encoder
      // (long-run marker handles any length).
      while (i + runLen < inputLength && input[inputOff + i + runLen] == first) {
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
        if (litEnd + 1 < inputLength && input[inputOff + litEnd] == input[inputOff + litEnd + 1]) {
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
      System.arraycopy(input, inputOff + litStart, output, outPos, litLen);
      outPos += litLen;
      i = litEnd;
    }
    return outPos - outputOff;
  }

  /**
   * Decode a frame from {@code input} (starting at {@code inputOff}) into {@code output} starting at
   * {@code outputOff}. Writes exactly {@code uncompressedSize} bytes as recorded in the frame header.
   *
   * <p>
   * Every token span is bounded by the frame, never by the backing array: an input buffer is reusable
   * scratch whose bytes past {@code inputLen} belong to an earlier payload, so a frame that asks for
   * them is corrupt rather than a frame with extra input. The frame must also be consumed exactly.
   * Both are what {@link ZeroRunByteCodec#decode(byte[], int, int, byte[], int)} requires of its own
   * frames.
   *
   * @return bytes written to {@code output} (== uncompressedSize)
   */
  public static int decode(final byte[] input, final int inputOff, final int inputLen, final MemorySegment output,
      final long outputOff) {
    if (input == null || output == null) {
      throw new IllegalArgumentException("input/output");
    }
    Objects.checkFromIndexSize(inputOff, inputLen, input.length);
    int inPos = inputOff;
    final int inEnd = inputOff + inputLen;
    if (inPos >= inEnd || input[inPos++] != FRAME_MARKER) {
      throw new IllegalStateException("ByteRunCodec: missing frame marker");
    }
    final long vr = readVarintPacked(input, inPos, inEnd);
    inPos = (int) (vr >>> Integer.SIZE);
    final int uncompressed = (int) vr;
    if (uncompressed > output.byteSize() - outputOff) {
      throw new IllegalStateException("ByteRunCodec: frame declares " + uncompressed + " bytes, the output holds "
          + (output.byteSize() - outputOff));
    }

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
        if (litLen > inEnd - inPos || outPos + litLen > outEnd) {
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
        if (value == 0) {
          throw new IllegalStateException("ByteRunCodec: zero repeat byte in a const-run");
        }
        if (outPos + runLen > outEnd) {
          throw new IllegalStateException("ByteRunCodec: const-run overflow");
        }
        fillByte(output, outPos, runLen, value);
        outPos += runLen;
      } else if (tag == 0xFD) {
        // Long zero run: varint(runLen).
        final long vr2 = readVarintPacked(input, inPos, inEnd);
        inPos = (int) (vr2 >>> Integer.SIZE);
        final int runLen = (int) vr2;
        if (runLen < 2 || outPos + runLen > outEnd) {
          throw new IllegalStateException("ByteRunCodec: long zero-run overflow");
        }
        fillByte(output, outPos, runLen, (byte) 0);
        outPos += runLen;
      } else if (tag == 0xFE) {
        // Long non-zero constant-byte run: varint(runLen) + byte.
        final long vr2 = readVarintPacked(input, inPos, inEnd);
        inPos = (int) (vr2 >>> Integer.SIZE);
        final int runLen = (int) vr2;
        if (inPos >= inEnd) {
          throw new IllegalStateException("ByteRunCodec: missing value byte for long run");
        }
        final byte value = input[inPos++];
        if (value == 0) {
          throw new IllegalStateException("ByteRunCodec: zero repeat byte in a long const-run");
        }
        if (runLen < 2 || outPos + runLen > outEnd) {
          throw new IllegalStateException("ByteRunCodec: long const-run overflow");
        }
        fillByte(output, outPos, runLen, value);
        outPos += runLen;
      } else {
        // 0xFF reserved.
        throw new IllegalStateException("ByteRunCodec: reserved tag 0xFF");
      }
    }
    if (inPos != inEnd) {
      throw new IllegalStateException("ByteRunCodec: trailing frame bytes");
    }
    return uncompressed;
  }

  /** Bulk byte-fill on an output segment; LLVM folds to a vectorised memset. */
  private static void fillByte(final MemorySegment output, final long offset, final int len, final byte value) {
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

  /**
   * Reads a varint bounded by the frame. The low 32 bits hold the value, the high 32 bits the next
   * input offset, so a token costs no allocation. The five-byte cap keeps the value non-negative.
   */
  private static long readVarintPacked(final byte[] input, final int offset, final int end) {
    int position = offset;
    int result = 0;
    for (int shift = 0; shift <= 28; shift += 7) {
      if (position == end) {
        throw new IllegalStateException("ByteRunCodec: truncated varint");
      }
      final int value = input[position++] & 0xFF;
      if (shift == 28 && (value & 0xF8) != 0) {
        throw new IllegalStateException("ByteRunCodec: varint length overflow");
      }
      result |= (value & 0x7F) << shift;
      if ((value & 0x80) == 0) {
        return ((long) position << Integer.SIZE) | result;
      }
    }
    throw new IllegalStateException("ByteRunCodec: varint too long");
  }
}
