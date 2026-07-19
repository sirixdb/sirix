/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.bytepipe;

import io.sirix.exception.SirixIOException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Pure-Java safe decoder for the LZ4 <em>block</em> format (the format produced by
 * {@code LZ4_compress_default}/{@code LZ4_compress_HC} and consumed by
 * {@code LZ4_decompress_safe}).
 *
 * <p>Pages whose body carries codec id 1 — and resources whose byte-handler pipeline contains
 * {@link FFILz4Compressor} — used to be unreadable on hosts without the native {@code liblz4}.
 * That made on-disk portability depend on a runtime library probe. This decoder removes the
 * read-side dependency: it is the fallback whenever the FFI probe fails. Write-side LZ4 still
 * requires the native library (the page-body writer simply picks a pure-Java codec when LZ4 is
 * unavailable, so nothing unwritable is ever produced).
 *
 * <p>Bounds-safe: every read from the source and write to the destination is validated, so a
 * corrupt or malicious block raises {@link SirixIOException} instead of reading/writing out of
 * bounds. Match copies honor LZ4 overlap semantics (a distance smaller than the match length
 * replicates the just-written pattern byte-by-byte, never memmove semantics).
 */
public final class JavaLz4BlockDecoder {

  private static final ValueLayout.OfShort LE_SHORT =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  private static final int MIN_MATCH = 4;

  private JavaLz4BlockDecoder() {
  }

  /**
   * Decompresses one LZ4 block.
   *
   * @param src         segment holding the compressed block
   * @param srcOff      offset of the block within {@code src}
   * @param srcLen      compressed length in bytes
   * @param dst         destination segment
   * @param dstOff      offset within {@code dst} to write to
   * @param dstCapacity maximum number of bytes the block may decompress to
   * @return the number of decompressed bytes written
   * @throws SirixIOException if the block is malformed or exceeds {@code dstCapacity}
   */
  public static int decompressSafe(final MemorySegment src, final long srcOff, final int srcLen,
      final MemorySegment dst, final long dstOff, final int dstCapacity) {
    if (srcLen < 0 || dstCapacity < 0) {
      throw new SirixIOException("LZ4 block decode: negative length (srcLen=" + srcLen
          + ", dstCapacity=" + dstCapacity + ")");
    }
    if (srcLen == 0) {
      return 0;
    }
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
        throw new SirixIOException("LZ4 block decode: match distance " + distance
            + " reaches before the output start");
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
