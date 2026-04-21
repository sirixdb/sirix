/*
 * Copyright (c) 2024, SirixDB. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */
package io.sirix.page;

import io.sirix.utils.FSSTCompressor;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Per-source-fragment slot copier that transparently decodes FSST-compressed
 * string values using the <em>source</em> fragment's symbol table. Used by
 * multi-fragment {@code combineRecordPages} implementations so that the
 * resulting target page contains no compressed string bytes and no FSST
 * symbol table of its own. The subsequent commit cycle runs
 * {@code buildFsstSymbolTable} + {@code compressStringValues} to produce a
 * single coherent page-level table before writing to disk, so the
 * decompress-on-merge step is write/disk neutral — it only shifts work from
 * read-time (where it would otherwise be repeated for each read) to
 * combine-time (once).
 *
 * <p><b>Correctness invariant.</b> A compressed string slot's bytes are the
 * output of {@code FSSTCompressor.encode(value, srcTable)}; decoding with any
 * other table corrupts the value. This copier guarantees that every slot
 * handed off to the target is either (a) not a string slot — raw-copied; or
 * (b) a string slot whose compressed-flag byte is {@code 0} after rewrite.
 * There is no scenario in which a compressed-flag byte survives to the target
 * page — so the target may safely carry {@code fsstSymbolTable = null}.
 *
 * <p><b>Wire formats handled.</b>
 * <ul>
 *   <li>Flyweight format (dir {@code nodeKindId} in {@code {30, 40}}): slot is
 *       {@code [kindByte][offsetTable: fieldCount bytes][data region]}. We
 *       locate the payload via the offset-table byte and do not touch the
 *       structural varints.</li>
 *   <li>Legacy format (dir {@code nodeKindId == 0}): slot is
 *       {@code [kindByte][NodeKind.serialize output]}. We skip structural
 *       varints byte-by-byte using the LEB128 continuation-bit rule.</li>
 * </ul>
 *
 * <p>Since only the payload is rewritten and all bytes preceding it are
 * preserved byte-for-byte, any offset table remains valid without
 * modification.
 */
public final class FsstAwareSlotCopier {

  /** NodeKind.STRING_VALUE id. Duplicated to avoid a dependency cycle. */
  private static final int STRING_VALUE_KIND_ID = 30;

  /** NodeKind.OBJECT_STRING_VALUE id. */
  private static final int OBJECT_STRING_VALUE_KIND_ID = 40;

  /** Field count of the STRING_VALUE flyweight offset table. */
  private static final int STRING_VALUE_FIELD_COUNT = 6;

  /** Field count of the OBJECT_STRING_VALUE flyweight offset table. */
  private static final int OBJECT_STRING_VALUE_FIELD_COUNT = 4;

  /** Payload field index within STRING_VALUE's offset table. */
  private static final int STRING_VALUE_PAYLOAD_FIELD = 5;

  /** Payload field index within OBJECT_STRING_VALUE's offset table. */
  private static final int OBJECT_STRING_VALUE_PAYLOAD_FIELD = 3;

  /**
   * Structural delta-varints preceding {@code prevRev} in the STRING_VALUE
   * legacy wire format (parent, rightSib, leftSib).
   */
  private static final int STRING_VALUE_STRUCTURAL_VARINTS = 3;

  /** Structural delta-varint preceding {@code prevRev} for OBJECT_STRING_VALUE (parent only). */
  private static final int OBJECT_STRING_VALUE_STRUCTURAL_VARINTS = 1;

  /** Parsed symbol table — {@code null} when the source fragment has no FSST table. */
  private final byte[][] parsedSymbols;

  /** True iff {@link #parsedSymbols} is non-null and non-empty. */
  private final boolean active;

  /**
   * Build a copier for a source fragment's FSST symbol table. A {@code null}
   * or empty {@code fsstSymbolTable} yields an inactive copier whose
   * {@link #active()} returns {@code false} — callers can fast-path to a raw
   * byte copy in that case.
   */
  public FsstAwareSlotCopier(final byte[] fsstSymbolTable) {
    if (fsstSymbolTable == null || fsstSymbolTable.length == 0) {
      this.parsedSymbols = null;
      this.active = false;
      return;
    }
    final byte[][] parsed = FSSTCompressor.parseSymbolTable(fsstSymbolTable);
    if (parsed.length == 0) {
      this.parsedSymbols = null;
      this.active = false;
    } else {
      this.parsedSymbols = parsed;
      this.active = true;
    }
  }

  /**
   * @return {@code true} iff this copier can actually decompress. Callers may
   *     skip invoking {@link #decompressSlot} entirely when {@code false}.
   */
  public boolean active() {
    return active;
  }

  /**
   * Try to rewrite {@code slot} into a freshly-allocated byte array whose
   * compressed-flag byte is {@code 0} and whose value bytes are the
   * uncompressed form of the original. Returns {@code null} when no rewrite
   * is required — either the slot is not a string kind, or the slot's
   * compressed flag is already {@code 0}, or the wire layout is malformed
   * (caller should fall back to raw copy, which then requires the target
   * keep the source's FSST table — so callers must verify {@link #active()}
   * before deciding whether raw copy is safe in a multi-fragment combine).
   *
   * <p>When this returns a non-null array, the returned layout is a
   * byte-for-byte copy of the input up to (but not including) the
   * compressed-flag byte, followed by {@code [0][lengthVarint][value]} and
   * then any trailing bytes. For slots whose payload is the last field
   * (the normal case) the tail is empty.
   *
   * @param slot slot bytes as returned by {@code KeyValueLeafPage.getSlot(offset)}
   * @param dirNodeKindId directory {@code nodeKindId} — distinguishes flyweight from legacy
   * @return rewritten slot bytes, or {@code null} when no rewrite is needed
   */
  public byte[] decompressSlot(final MemorySegment slot, final int dirNodeKindId) {
    if (!active || slot == null) {
      return null;
    }
    final int slotLen = (int) slot.byteSize();
    if (dirNodeKindId == STRING_VALUE_KIND_ID) {
      return decompressFlyweight(slot, slotLen, STRING_VALUE_FIELD_COUNT, STRING_VALUE_PAYLOAD_FIELD);
    }
    if (dirNodeKindId == OBJECT_STRING_VALUE_KIND_ID) {
      return decompressFlyweight(slot, slotLen, OBJECT_STRING_VALUE_FIELD_COUNT,
          OBJECT_STRING_VALUE_PAYLOAD_FIELD);
    }
    if (dirNodeKindId == 0 && slotLen > 0) {
      final int kindByte = slot.get(ValueLayout.JAVA_BYTE, 0) & 0xFF;
      if (kindByte == STRING_VALUE_KIND_ID) {
        return decompressLegacy(slot, slotLen, STRING_VALUE_STRUCTURAL_VARINTS);
      }
      if (kindByte == OBJECT_STRING_VALUE_KIND_ID) {
        return decompressLegacy(slot, slotLen, OBJECT_STRING_VALUE_STRUCTURAL_VARINTS);
      }
    }
    return null;
  }

  private byte[] decompressFlyweight(final MemorySegment slot, final int slotLen,
      final int fieldCount, final int payloadField) {
    final int dataStart = 1 + fieldCount;
    if (dataStart > slotLen) {
      return null;
    }
    final int payloadOffset = slot.get(ValueLayout.JAVA_BYTE, 1 + payloadField) & 0xFF;
    final int payloadAbs = dataStart + payloadOffset;
    if (payloadAbs >= slotLen) {
      return null;
    }
    return rewriteFromCompressedFlag(slot, slotLen, payloadAbs);
  }

  private byte[] decompressLegacy(final MemorySegment slot, final int slotLen, final int structuralVarints) {
    int pos = 1; // skip kind byte
    for (int i = 0; i < structuralVarints; i++) {
      pos = skipVarint(slot, pos, slotLen);
    }
    pos = skipVarint(slot, pos, slotLen); // prevRev
    pos = skipVarint(slot, pos, slotLen); // lastModRev
    if (pos >= slotLen) {
      return null;
    }
    return rewriteFromCompressedFlag(slot, slotLen, pos);
  }

  /**
   * Given the absolute byte offset of the isCompressed flag within the slot,
   * decode the FSST-compressed payload and return a rewritten slot. Returns
   * {@code null} if the flag is already 0 or the length varint cannot be
   * decoded within the slot bounds.
   */
  private byte[] rewriteFromCompressedFlag(final MemorySegment slot, final int slotLen, final int flagPos) {
    final byte isCompressed = slot.get(ValueLayout.JAVA_BYTE, flagPos);
    if (isCompressed != 1) {
      return null;
    }
    // Read length varint.
    long readResult = readSignedVarint(slot, flagPos + 1, slotLen);
    if (readResult == READ_FAILED) {
      return null;
    }
    final int lenVarEnd = (int) (readResult >>> 32);
    final int compressedLen = (int) (readResult & 0xFFFFFFFFL);
    if (compressedLen < 0 || lenVarEnd + compressedLen > slotLen) {
      return null;
    }
    // Materialize the compressed blob. Unavoidable since FSSTCompressor.decode
    // takes a byte[] — but we allocate exactly once per compressed slot.
    final byte[] compressed = new byte[compressedLen];
    if (compressedLen > 0) {
      MemorySegment.copy(slot, ValueLayout.JAVA_BYTE, lenVarEnd, compressed, 0, compressedLen);
    }
    final byte[] decompressed = FSSTCompressor.decode(compressed, parsedSymbols);

    // Preserve any trailing bytes after the compressed payload. For well-formed
    // STRING_VALUE / OBJECT_STRING_VALUE slots the payload is the last field, so
    // tailLen == 0; we still handle the general case defensively.
    final int tailStart = lenVarEnd + compressedLen;
    final int tailLen = slotLen - tailStart;

    final int prefixLen = flagPos; // bytes strictly before the flag
    final int lenVarBytes = varintByteLength(zigzagEncodeInt(decompressed.length));
    final int totalLen = prefixLen + 1 /* new flag byte */ + lenVarBytes + decompressed.length + tailLen;

    final byte[] out = new byte[totalLen];
    MemorySegment.copy(slot, ValueLayout.JAVA_BYTE, 0, out, 0, prefixLen);
    out[prefixLen] = 0;
    writeSignedVarint(out, prefixLen + 1, decompressed.length);
    System.arraycopy(decompressed, 0, out, prefixLen + 1 + lenVarBytes, decompressed.length);
    if (tailLen > 0) {
      MemorySegment.copy(slot, ValueLayout.JAVA_BYTE, tailStart, out, totalLen - tailLen, tailLen);
    }
    return out;
  }

  // =========================================================================
  // Varint helpers — LEB128 with zigzag encoding for signed values, matching
  // DeltaVarIntCodec's byte-level wire format. Duplicated here so the helper
  // class has no data dependency on DeltaVarIntCodec's BytesIn/BytesOut API
  // and can operate directly on MemorySegment + byte[].
  // =========================================================================

  /** Sentinel returned by {@link #readSignedVarint} on failure. */
  private static final long READ_FAILED = -1L;

  /**
   * Read a signed varint (zigzag + LEB128) from {@code slot} starting at
   * {@code pos}, stopping before {@code limit}. Returns a packed {@code long}
   * where the high 32 bits hold the end-position (exclusive) and the low 32
   * bits hold the decoded int value. Returns {@link #READ_FAILED} if the
   * varint runs past {@code limit} or exceeds 5 continuation bytes.
   */
  private static long readSignedVarint(final MemorySegment slot, int pos, final int limit) {
    long result = 0;
    int shift = 0;
    while (pos < limit) {
      final int b = slot.get(ValueLayout.JAVA_BYTE, pos++) & 0xFF;
      result |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0) {
        // Zigzag decode to signed.
        final int decoded = (int) ((result >>> 1) ^ -(result & 1));
        return ((long) pos << 32) | (decoded & 0xFFFFFFFFL);
      }
      shift += 7;
      if (shift >= 64) {
        return READ_FAILED;
      }
    }
    return READ_FAILED;
  }

  private static int skipVarint(final MemorySegment slot, int pos, final int limit) {
    while (pos < limit) {
      final byte b = slot.get(ValueLayout.JAVA_BYTE, pos++);
      if ((b & 0x80) == 0) {
        return pos;
      }
    }
    return limit;
  }

  /** Zigzag-encode an int into an unsigned long suitable for LEB128. */
  private static long zigzagEncodeInt(final int value) {
    return ((long) value << 1) ^ ((long) (value >> 31));
  }

  /** Number of LEB128 bytes required to encode {@code zigzag}. */
  private static int varintByteLength(long zigzag) {
    int n = 1;
    while ((zigzag & ~0x7FL) != 0) {
      n++;
      zigzag >>>= 7;
    }
    return n;
  }

  /** Write signed varint at {@code out[offset..]}; returns bytes written. */
  private static int writeSignedVarint(final byte[] out, final int offset, final int value) {
    long v = zigzagEncodeInt(value);
    int pos = offset;
    while ((v & ~0x7FL) != 0) {
      out[pos++] = (byte) ((v & 0x7F) | 0x80);
      v >>>= 7;
    }
    out[pos++] = (byte) v;
    return pos - offset;
  }
}
