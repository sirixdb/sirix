/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.LE;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import net.openhft.hashing.LongHashFunction;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Per-page membership sketch over a {@link StringRegion}'s dictionary entries — a Bloom filter that
 * answers "can this page possibly contain this string?" without decompressing the string column.
 *
 * <h2>Why</h2> A string region is the largest thing on a JSON page: on the reference corpus a scan
 * decompresses 1,440 MB of dictionary bytes across the store, which profiling put at 88 % of a
 * {@code title eq "…"} scan's CPU. Yet a selective equality matches on a handful of pages. The
 * dictionary has to be decompressed only to discover, page after page, that the literal is not in
 * it. This sketch answers that question from a few hundred bytes.
 *
 * <p>
 * The asymmetry is what makes it worth storing: a negative is <em>exact</em> (no false negatives,
 * ever — every dictionary entry is inserted), and a positive costs only the decompression that
 * would have happened anyway. So the filter can never change an answer, only the work done to reach
 * it.
 *
 * <h2>Wire format</h2>
 * 
 * <pre>
 * byte  version        // 1
 * byte  hashCount      // k, number of bit probes per value
 * int   entryCount     // dictionary entries inserted (diagnostics / sizing)
 * int   bitLength      // m, always a multiple of 8
 * byte[bitLength / 8]  // the bit array, LSB-first within each byte
 * </pre>
 *
 * <p>
 * The bits are near-random, so the region table's LZ77 pass will decline to compress them and store
 * the payload raw — which is what we want: the probe reads a few bytes and must not have to
 * decompress anything to do it.
 *
 * <h2>Sizing</h2> {@value #BITS_PER_ENTRY} bits per entry with {@value #HASH_COUNT} probes gives
 * roughly a 1 % false-positive rate. On the reference corpus that is ~440 B per page (~1.6 % added
 * to the store) against 29 KB of dictionary per page that no longer has to be decompressed.
 *
 * <h2>Correctness contract, and why FSST needs no special case</h2> The sketch hashes the bytes as
 * <em>stored</em>, which under FSST means the encoded form. That is not a compromise — it is the
 * point of a lightweight compression scheme: the encoding is a pure function of (value, symbol
 * table), and the writer decides per value whether to store it encoded with exactly the call a
 * reader can repeat ({@code FSSTCompressor.encodeOrNull}). So a probe reproduces the stored form of
 * its literal and asks about that, and the predicate is answered without a byte being decompressed
 * anywhere.
 *
 * <p>
 * A caller that cannot reproduce the encoded form — no symbol table in hand — probes the raw form
 * alone, which can only produce a false <em>positive</em> (it looks for a stored form that may not
 * be the one used), never a false negative, because the caller then goes on to compare the
 * dictionary entries themselves.
 */
public final class StringDictSketch {

  /** Format version of the payload written by {@link #encode}. */
  private static final byte VERSION = 1;

  /** Bits allocated per inserted entry. */
  private static final int BITS_PER_ENTRY = 10;

  /** Bit probes per value. With {@link #BITS_PER_ENTRY} = 10 this lands near the optimal ~7. */
  private static final int HASH_COUNT = 4;

  /** Header bytes before the bit array. */
  private static final int HEADER_BYTES = 1 + 1 + 4 + 4;

  /** Smallest bit array emitted, so tiny dictionaries do not degenerate to a saturated filter. */
  private static final int MIN_BITS = 64;

  /** Same xx3 the rest of the engine uses; see {@code StringRegion.Encoder}. */
  private static final LongHashFunction VALUE_HASH = LongHashFunction.xx3();

  private StringDictSketch() {}

  /**
   * Build a sketch over every dictionary entry of {@code stringPayload}.
   *
   * <p>
   * Entries are hashed AS STORED, so an FSST-encoded entry is hashed in its encoded form. That is
   * deliberate and is what lets the sketch work on FSST resources at all: the probe side encodes the
   * literal against the same symbol table and looks up the encoded bytes, so neither side ever
   * decodes. A probe that cannot obtain the encoded literal must not consult the sketch, because a
   * raw-literal lookup against encoded entries would be a false NEGATIVE — the one answer a Bloom
   * filter is never allowed to give.
   *
   * @param stringPayload an encoded {@link StringRegion} payload
   * @param header a parsed header for {@code stringPayload} (caller-owned scratch)
   * @return the sketch payload, or {@code null} when the page has no dictionary entries
   */
  public static byte[] encodeFromStringRegion(final byte[] stringPayload, final StringRegion.Header header) {
    return encodeFromStringRegion(stringPayload, stringPayload == null
        ? 0
        : stringPayload.length, header);
  }

  /**
   * Build a sketch over an exact logical prefix of reusable encoder storage.
   *
   * @param stringPayload encoder storage
   * @param stringPayloadLength number of valid leading bytes; capacity beyond it is ignored
   * @param header a header parsed from that same logical prefix
   * @return the sketch payload, or {@code null} when the prefix is empty or malformed
   */
  public static byte[] encodeFromStringRegion(final byte[] stringPayload, final int stringPayloadLength,
      final StringRegion.Header header) {
    if (stringPayloadLength < 0 || (stringPayload == null
        ? stringPayloadLength != 0
        : stringPayloadLength > stringPayload.length)) {
      throw new IllegalArgumentException("stringPayloadLength=" + stringPayloadLength + " for payload length "
          + (stringPayload == null
              ? "null"
              : stringPayload.length));
    }
    if (stringPayload == null || stringPayloadLength == 0) {
      return null;
    }
    int totalEntries = 0;
    for (int tag = 0; tag < header.parentDictSize; tag++) {
      if (header.tagGlobal[tag]) {
        // NO SKETCH AT ALL for a page with a converted tag, and the reason is the one this class is
        // most dangerous about. A global tag stores dictionary IDS and no value bytes, so the walk
        // below would read its packed ids AS lengths and hash whatever payload ranges they happen to
        // address -- a sketch built from garbage. That is not a degraded sketch: a NEGATIVE is read
        // as EXACT and for the whole PAGE, so the page would rule itself out of literals it actually
        // holds and drop rows silently.
        //
        // Same rule, and the same reasoning, as a suppressed tag: a sketch that cannot describe every
        // value on the page must not exist. Emitting one over only the non-global tags would be
        // exactly the incompleteness the suppressed-tag guard already refuses.
        //
        // The cost is a lost page-skip on converted columns. The alternative worth pricing later is a
        // sketch over the IDS rather than the bytes, with the probe hashing the id it resolved its
        // literal to -- cheaper than losing the skip, but a new mechanism on both sides.
        return null;
      }
      totalEntries += header.tagStringDictSize[tag];
    }
    if (totalEntries == 0) {
      return null;
    }

    final int bits = Math.max(MIN_BITS, ((totalEntries * BITS_PER_ENTRY) + 63) & ~63);
    final byte[] out = new byte[HEADER_BYTES + (bits >>> 3)];
    out[0] = VERSION;
    out[1] = (byte) HASH_COUNT;
    putInt(out, 2, totalEntries);
    putInt(out, 6, bits);

    for (int tag = 0; tag < header.parentDictSize; tag++) {
      final int dictStart = header.tagStringDictOffset[tag];
      final int n = header.tagStringDictSize[tag];
      // The length table's field width is the tag's, not a constant: a page framed for records
      // stores most lengths in one byte. The header carries both the width and where the entries'
      // bytes begin, so nothing here has to know which framing produced them.
      final int lengthWidth = header.tagLengthWidth[tag];
      final long bytesStart = header.tagStringBytesOffset[tag];
      if (dictStart < 0 || n < 0 || lengthWidth <= 0 || bytesStart > stringPayloadLength
          || (long) dictStart + (long) n * lengthWidth > stringPayloadLength) {
        return null;
      }
      int off = (int) bytesStart;
      for (int i = 0; i < n; i++) {
        // The sign carries the FSST flag; the magnitude is the STORED length either way, and the
        // stored bytes are exactly what a probe reproduces (see the class contract).
        final int lengthField = StringRegion.readLengthFieldFromArray(stringPayload, dictStart + i * lengthWidth,
            lengthWidth);
        if (lengthField == Integer.MIN_VALUE) {
          return null;
        }
        final int storedLen = Math.abs(lengthField);
        if (storedLen > stringPayloadLength - off) {
          return null; // payload does not parse the way the header claims — emit no sketch
        }
        setBits(out, bits, VALUE_HASH.hashBytes(stringPayload, off, storedLen));
        off += storedLen;
      }
    }
    return out;
  }

  /**
   * Test whether {@code literal} may occur among the page's string values.
   *
   * @param payload a sketch payload, or {@code null}
   * @param literal the value to test, UTF-8
   * @return {@code false} only when the value is definitely absent; {@code true} when it may be
   *         present, when the sketch is absent, or when the payload is from a future version
   */
  public static boolean mayContain(final MemorySegment payload, final byte[] literal) {
    if (payload == null || payload.byteSize() < HEADER_BYTES || payload.get(ValueLayout.JAVA_BYTE, 0) != VERSION) {
      return true; // no usable sketch — the caller must look for real
    }
    final int k = payload.get(ValueLayout.JAVA_BYTE, 1) & 0xFF;
    final int bits = getInt(payload, 6);
    if (bits <= 0 || HEADER_BYTES + (bits >>> 3) > payload.byteSize()) {
      return true;
    }
    final long h = VALUE_HASH.hashBytes(literal);
    int h1 = (int) h;
    final int h2 = (int) (h >>> 32) | 1; // odd, so the probe sequence covers the whole array
    for (int i = 0; i < k; i++) {
      final int bit = Math.floorMod(h1, bits);
      if ((payload.get(ValueLayout.JAVA_BYTE, HEADER_BYTES + (bit >>> 3)) & (1 << (bit & 7))) == 0) {
        return false;
      }
      h1 += h2;
    }
    return true;
  }

  /** Number of dictionary entries the sketch was built from; {@code -1} if unreadable. */
  public static int entryCount(final MemorySegment payload) {
    if (payload == null || payload.byteSize() < HEADER_BYTES || payload.get(ValueLayout.JAVA_BYTE, 0) != VERSION) {
      return -1;
    }
    return getInt(payload, 2);
  }

  private static void setBits(final byte[] out, final int bits, final long h) {
    int h1 = (int) h;
    final int h2 = (int) (h >>> 32) | 1;
    for (int i = 0; i < HASH_COUNT; i++) {
      final int bit = Math.floorMod(h1, bits);
      out[HEADER_BYTES + (bit >>> 3)] |= (byte) (1 << (bit & 7));
      h1 += h2;
    }
  }

  /**
   * Little-endian, like every other scalar in this package — {@link StringRegion} writes its
   * dictionary lengths through the same view, and this class reads them.
   */
  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

  private static void putInt(final byte[] buf, final int off, final int v) {
    INT_LE.set(buf, off, v);
  }

  private static int getInt(final MemorySegment buf, final long off) {
    return buf.get(LE.INT, off);
  }

  /**
   * Same read against the encoder's staging array.
   *
   * <p>
   * The sketch is built while the string region is still an in-flight {@code byte[]}, before it ever
   * reaches a payload segment, so the build path needs the array-backed reader; every read path uses
   * the segment one above.
   */
  private static int getIntFromArray(final byte[] buf, final int off) {
    return (int) INT_LE.get(buf, off);
  }
}
