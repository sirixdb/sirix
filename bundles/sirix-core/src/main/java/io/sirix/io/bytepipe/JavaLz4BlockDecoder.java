/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.bytepipe;

import io.sirix.exception.SirixIOException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Pure-Java safe decoder for the LZ4 <em>block</em> format (the format produced by
 * {@code LZ4_compress_default}/{@code LZ4_compress_HC} and consumed by
 * {@code LZ4_decompress_safe}).
 *
 * <p>
 * Pages whose body carries codec id 1 — and resources whose byte-handler pipeline contains
 * {@link FFILz4Compressor} — used to be unreadable on hosts without the native {@code liblz4}. That
 * made on-disk portability depend on a runtime library probe. This decoder removes the read-side
 * dependency: it is the fallback whenever the FFI probe fails. Write-side LZ4 still requires the
 * native library (the page-body writer simply picks a pure-Java codec when LZ4 is unavailable, so
 * nothing unwritable is ever produced).
 *
 * <p>
 * Bounds-safe: every read from the source and write to the destination is validated, so a corrupt
 * or malicious block raises {@link SirixIOException} instead of reading/writing out of bounds.
 * Match copies honor LZ4 overlap semantics (a distance smaller than the match length replicates the
 * just-written pattern, never memmove semantics).
 *
 * <p>
 * <b>Two loops.</b> A block that fits {@link #HEAP_DECODE_LIMIT} is copied to a thread-owned heap
 * array, decoded there and copied out once. A page body decodes to hundreds of sequences of a few
 * bytes each, and a foreign-memory copy per sequence costs more than the bytes it moves: on 8,192
 * HOT leaf payloads the segment loop took 24.1 µs per leaf and this one 10.6 µs, against 4.8 µs for
 * the native decoder. Nothing reaches the destination unless the whole block decodes. Larger
 * blocks, which are overflow values, keep the segment loop so that no value-sized array is ever
 * retained by a thread.
 */
public final class JavaLz4BlockDecoder {

  private static final ValueLayout.OfShort LE_SHORT =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  /**
   * Little-endian words of a heap array; byte order is irrelevant to a copy, it only has to match.
   */
  private static final VarHandle WORD = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private static final int MIN_MATCH = 4;

  /**
   * Largest compressed length and largest destination capacity decoded on the heap: the largest
   * frame-slot size class, which bounds every page body. The two scratch arrays a thread keeps never
   * grow past this plus {@link #WORD_SLACK}.
   */
  static final int HEAP_DECODE_LIMIT = 256 * 1024;

  /** Spare bytes after each scratch array, so a run of up to sixteen bytes moves as whole words. */
  static final int WORD_SLACK = Long.BYTES;

  /** Runs up to this length are moved as one or two words instead of an array copy. */
  private static final int SHORT_RUN = 2 * Long.BYTES;

  private static final int MIN_SCRATCH = 4096;

  private static final byte[] NO_BYTES = new byte[0];

  /** Thread-owned decode buffers, grown on demand and reused; no allocation per block once sized. */
  private static final class Scratch {
    private byte[] input = NO_BYTES;
    private byte[] output = NO_BYTES;
  }

  // Thread-lifetime scratch, bounded by HEAP_DECODE_LIMIT: removing it per call would allocate two
  // arrays for every page decoded on a host without the native library.
  @SuppressWarnings("java:S5164")
  private static final ThreadLocal<Scratch> SCRATCH = ThreadLocal.withInitial(Scratch::new);

  private JavaLz4BlockDecoder() {}

  /**
   * Decompresses one LZ4 block.
   *
   * @param src segment holding the compressed block
   * @param srcOff offset of the block within {@code src}
   * @param srcLen compressed length in bytes
   * @param dst destination segment
   * @param dstOff offset within {@code dst} to write to
   * @param dstCapacity maximum number of bytes the block may decompress to
   * @return the number of decompressed bytes written
   * @throws SirixIOException if the block is malformed or exceeds {@code dstCapacity}
   */
  public static int decompressSafe(final MemorySegment src, final long srcOff, final int srcLen,
      final MemorySegment dst, final long dstOff, final int dstCapacity) {
    if (srcLen < 0 || dstCapacity < 0) {
      throw new SirixIOException(
          "LZ4 block decode: negative length (srcLen=" + srcLen + ", dstCapacity=" + dstCapacity + ")");
    }
    if (srcLen == 0) {
      return 0;
    }
    if (srcLen > HEAP_DECODE_LIMIT || dstCapacity > HEAP_DECODE_LIMIT) {
      return decompressSegments(src, srcOff, srcLen, dst, dstOff, dstCapacity);
    }
    final Scratch scratch = SCRATCH.get();
    byte[] in = scratch.input;
    if (in.length < srcLen + WORD_SLACK) {
      in = new byte[scratchCapacity(srcLen)];
      scratch.input = in;
    }
    byte[] out = scratch.output;
    if (out.length < dstCapacity + WORD_SLACK) {
      out = new byte[scratchCapacity(dstCapacity)];
      scratch.output = out;
    }
    MemorySegment.copy(src, ValueLayout.JAVA_BYTE, srcOff, in, 0, srcLen);
    final int produced = decodeHeap(in, srcLen, out, dstCapacity);
    MemorySegment.copy(out, 0, dst, ValueLayout.JAVA_BYTE, dstOff, produced);
    return produced;
  }

  /**
   * Length of the scratch array for a block side of {@code length} bytes: the next power of two that
   * holds it and its word slack, at least 4 KiB, and never more than the limit needs. Doubling keeps
   * a thread that sees growing pages from reallocating for each of them.
   */
  static int scratchCapacity(final int length) {
    final int needed = length + WORD_SLACK;
    final int rounded = Math.max(MIN_SCRATCH, Integer.highestOneBit(needed - 1) << 1);
    return Math.min(rounded, HEAP_DECODE_LIMIT + WORD_SLACK);
  }

  /**
   * Decode {@code in[0, sEnd)} into {@code out[0, dEnd)}. Both arrays extend at least
   * {@link #WORD_SLACK} bytes past those ends: a short run is moved as one or two whole words, which
   * may read and write up to seven bytes beyond the run. Those bytes lie inside the slack or are
   * overwritten by the sequences that follow, and no later read reaches them first, because a match
   * only ever reads output that is already complete.
   *
   * @return the number of bytes decoded
   */
  private static int decodeHeap(final byte[] in, final int sEnd, final byte[] out, final int dEnd) {
    int s = 0;
    int d = 0;
    while (true) {
      if (s >= sEnd) {
        throw new SirixIOException("LZ4 block decode: truncated block (missing token)");
      }
      final int token = in[s++] & 0xFF;

      // Literal run.
      int litLen = token >>> 4;
      if (litLen == 15) {
        int b;
        do {
          if (s >= sEnd) {
            throw new SirixIOException("LZ4 block decode: truncated literal-length chain");
          }
          b = in[s++] & 0xFF;
          litLen += b;
        } while (b == 255);
      }
      if (litLen > sEnd - s) {
        throw new SirixIOException("LZ4 block decode: literal run exceeds input");
      }
      if (litLen > dEnd - d) {
        throw new SirixIOException("LZ4 block decode: output exceeds capacity " + dEnd);
      }
      if (litLen <= SHORT_RUN) {
        WORD.set(out, d, (long) WORD.get(in, s));
        if (litLen > Long.BYTES) {
          WORD.set(out, d + Long.BYTES, (long) WORD.get(in, s + Long.BYTES));
        }
      } else {
        System.arraycopy(in, s, out, d, litLen);
      }
      s += litLen;
      d += litLen;

      // The final sequence is literals-only and ends exactly at the input boundary.
      if (s == sEnd) {
        return d;
      }

      // Match.
      if (sEnd - s < 2) {
        throw new SirixIOException("LZ4 block decode: truncated match offset");
      }
      final int distance = (in[s] & 0xFF) | (in[s + 1] & 0xFF) << 8;
      s += 2;
      if (distance == 0) {
        throw new SirixIOException("LZ4 block decode: zero match distance");
      }
      int matchLen = token & 0x0F;
      if (matchLen == 15) {
        int b;
        do {
          if (s >= sEnd) {
            throw new SirixIOException("LZ4 block decode: truncated match-length chain");
          }
          b = in[s++] & 0xFF;
          matchLen += b;
        } while (b == 255);
      }
      matchLen += MIN_MATCH;

      final int matchPos = d - distance;
      if (matchPos < 0) {
        throw new SirixIOException("LZ4 block decode: match distance " + distance + " reaches before the output start");
      }
      if (matchLen > dEnd - d) {
        throw new SirixIOException("LZ4 block decode: output exceeds capacity " + dEnd);
      }
      if (distance >= Long.BYTES && matchLen <= SHORT_RUN) {
        // A word never reads output it has not yet written when the distance is at least a word;
        // the second word may read the first, which is exactly the pattern continuing.
        WORD.set(out, d, (long) WORD.get(out, matchPos));
        if (matchLen > Long.BYTES) {
          WORD.set(out, d + Long.BYTES, (long) WORD.get(out, matchPos + Long.BYTES));
        }
      } else if (distance >= matchLen) {
        System.arraycopy(out, matchPos, out, d, matchLen);
      } else if (distance == 1) {
        Arrays.fill(out, d, d + matchLen, out[matchPos]);
      } else {
        // The pattern out[matchPos, d) repeats with period `distance`. Each copy reads only bytes
        // already written, and the written part stays a whole number of periods long, so the
        // bytes it reads are the ones the pattern continues with.
        int available = distance;
        int remaining = matchLen;
        int to = d;
        while (remaining > 0) {
          final int chunk = Math.min(available, remaining);
          System.arraycopy(out, matchPos, out, to, chunk);
          to += chunk;
          remaining -= chunk;
          available += chunk;
        }
      }
      d += matchLen;
    }
  }

  /**
   * The segment loop: no scratch, one foreign-memory copy per run. Used above the heap limit, and
   * package-private so that tests can hold both loops to the same blocks.
   */
  static int decompressSegments(final MemorySegment src, final long srcOff, final int srcLen, final MemorySegment dst,
      final long dstOff, final int dstCapacity) {
    long sPos = srcOff;
    final long sEnd = srcOff + srcLen;
    long dPos = dstOff;
    final long dEnd = dstOff + dstCapacity;

    while (true) {
      if (sPos >= sEnd) {
        throw new SirixIOException("LZ4 block decode: truncated block (missing token)");
      }
      final int token = src.get(ValueLayout.JAVA_BYTE, sPos++) & 0xFF;

      // Literal run.
      long litLen = token >>> 4;
      if (litLen == 15) {
        int b;
        do {
          if (sPos >= sEnd) {
            throw new SirixIOException("LZ4 block decode: truncated literal-length chain");
          }
          b = src.get(ValueLayout.JAVA_BYTE, sPos++) & 0xFF;
          litLen += b;
        } while (b == 255);
      }
      if (sPos + litLen > sEnd) {
        throw new SirixIOException("LZ4 block decode: literal run exceeds input");
      }
      if (dPos + litLen > dEnd) {
        throw new SirixIOException("LZ4 block decode: output exceeds capacity " + dstCapacity);
      }
      if (litLen > 0) {
        MemorySegment.copy(src, sPos, dst, dPos, litLen);
        sPos += litLen;
        dPos += litLen;
      }

      // The final sequence is literals-only and ends exactly at the input boundary.
      if (sPos == sEnd) {
        return (int) (dPos - dstOff);
      }

      // Match.
      if (sPos + 2 > sEnd) {
        throw new SirixIOException("LZ4 block decode: truncated match offset");
      }
      final int distance = src.get(LE_SHORT, sPos) & 0xFFFF;
      sPos += 2;
      if (distance == 0) {
        throw new SirixIOException("LZ4 block decode: zero match distance");
      }
      long matchLen = token & 0x0F;
      if (matchLen == 15) {
        int b;
        do {
          if (sPos >= sEnd) {
            throw new SirixIOException("LZ4 block decode: truncated match-length chain");
          }
          b = src.get(ValueLayout.JAVA_BYTE, sPos++) & 0xFF;
          matchLen += b;
        } while (b == 255);
      }
      matchLen += MIN_MATCH;

      long matchPos = dPos - distance;
      if (matchPos < dstOff) {
        throw new SirixIOException("LZ4 block decode: match distance " + distance + " reaches before the output start");
      }
      if (dPos + matchLen > dEnd) {
        throw new SirixIOException("LZ4 block decode: output exceeds capacity " + dstCapacity);
      }
      if (distance >= matchLen) {
        // Non-overlapping — bulk copy.
        MemorySegment.copy(dst, matchPos, dst, dPos, matchLen);
        dPos += matchLen;
      } else {
        // Overlapping pattern replication — must propagate forward byte-by-byte.
        for (long i = 0; i < matchLen; i++) {
          dst.set(ValueLayout.JAVA_BYTE, dPos + i, dst.get(ValueLayout.JAVA_BYTE, matchPos + i));
        }
        dPos += matchLen;
      }
    }
  }
}
