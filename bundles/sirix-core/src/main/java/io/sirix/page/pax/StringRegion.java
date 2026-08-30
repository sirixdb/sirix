/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import io.sirix.node.LE;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Per-page PAX region for {@code OBJECT_STRING_VALUE} slots, dictionary- and bit-pack-encoded in
 * the BtrBlocks/Umbra style.
 *
 * <h2>Motivation</h2>
 *
 * In-record string storage (current Sirix) writes each value's bytes verbatim in the slotted-page
 * heap. For low-cardinality fields (8 departments, 50 countries, ...) this is extremely wasteful —
 * the same 5-byte string repeats hundreds of times per page.
 *
 * <p>
 * A compression study at Sirix's scan workload showed that lightweight column-wise encoding at
 * per-page granularity beats even Zstd-19 in absolute size <b>and</b> enables SIMD scan over the
 * encoded bytes without full decompression. This region implements that idea.
 *
 * <h2>Wire format</h2>
 *
 * <pre>
 *   byte          encodingKind           // 0 = DICT_BITPACKED_ZM (only variant so far)
 *   byte          tagKind                // 0 = nameKey-tagged (compression-only)
 *                                        //   1 = pathNodeKey-tagged (SIMD-safe for path scans)
 *   int           count                  // total OBJECT_STRING_VALUE entries on page
 *   byte          valueBitWidth          // 1..32 — bits per per-record dict index
 *                                        //   (sized to max local dict size)
 *
 *   int           parentDictSize         // number of distinct parent OBJECT_KEY
 *                                        //   nameKeys ("dept", "city", ...) whose values ARE
 *                                        //   in this region; negative when a suppressed-tag
 *                                        //   list follows (see below), low 31 bits are the size
 *   int[ps]       parentDict             // parent nameKeys, ordered by tag id
 *   int[ps]       tagStart               // first value-index for each tag
 *   int[ps]       tagCount               // number of values under each tag
 *   int[ps]       tagStringDictSize      // local string-dictionary size per tag
 *
 *   // Only when parentDictSize carried its sign bit:
 *   int           suppressedTagCount     // tags present on the page but NOT in this region
 *   int[sc]       suppressedTags         // their tag values, in first-seen order
 *
 *   // Per-tag local string dictionary (concatenated):
 *   //   For each tag in order:
 *   //     int[tagStringDictSize[t]] stringLengths
 *   //     byte[]                    stringBytes (concatenated UTF-8 values)
 *
 *   // Dict indices, bit-packed, tag-grouped:
 *   byte[]        valueDictIds           // (count * valueBitWidth + 7) / 8 bytes
 * </pre>
 *
 * <p>
 * For the reference Chicago-like workload (90 records/page, 2 string fields ~8 unique values each)
 * a typical page encodes to ~220 bytes vs ~1440 bytes of raw in-record UTF-8 — a 6.5× reduction at
 * per-page granularity before any outer-block compression.
 *
 * <h2>HFT-grade access</h2>
 *
 * Values are stored as per-record dictionary indices bit-packed at a width chosen globally (across
 * tags) to accommodate the largest local dict on the page. A {@code groupByCount(tag)} scan
 * iterates exactly {@code tagCount[t]} 3-bit lanes with a single SIMD popcount per dict id — no
 * UTF-8 parsing, no byte-by-byte compare. The producer path is offered via
 * {@link Encoder#addValue(int,byte[])} and {@link Encoder#finish()}.
 */
public final class StringRegion {

  /** DICT + bit-packed value IDs + per-tag local string dicts, with zone maps. */
  public static final byte ENC_DICT_BITPACKED_ZM = 0;

  /**
   * Identical layout to {@link #ENC_DICT_BITPACKED_ZM}, plus one guarantee: array-element staging RAN
   * and was published for this page.
   *
   * <p>
   * That guarantee is what makes an ABSENT {@link #TAG_ORPHAN_ELEMENTS} informative. The page holding
   * an array that reaches its own last slot has to know whether the array continues onto the next
   * page, and "the next page published no orphans" answers that — but only if the next page tried.
   * Without the distinction, "no orphans" and "elements were never collected here" look identical,
   * and a reader would have to assume the worse of the two on every page.
   *
   * <p>
   * A byte in the same position with the same layout after it: a reader that does not know the value
   * parses the region exactly as before.
   */
  public static final byte ENC_DICT_BITPACKED_ZM_ELEMENTS = 1;

  /**
   * Tag dictionary classification; see {@link Header#tagKind}. Same semantics as
   * {@link NumberRegion#TAG_KIND_NAME}/{@link NumberRegion#TAG_KIND_PATH_NODE}:
   * {@link #TAG_KIND_NAME} tags are nameKeys (compression-safe only), {@link #TAG_KIND_PATH_NODE}
   * tags are pathNodeKeys truncated to int (SIMD-safe for path-scoped scans).
   */
  public static final byte TAG_KIND_NAME = 0;
  public static final byte TAG_KIND_PATH_NODE = 1;

  /**
   * Reserved tag for array elements whose enclosing array opens on the PREVIOUS page.
   *
   * <p>
   * Such an element carries no path node key of its own and its array is off-page, so this page
   * cannot name the tag it belongs to — which is why they used to be dropped from the column
   * entirely. They do not need naming: slots are in node-key order and an array's elements are
   * contiguous, so only the LAST array of a page can spill, and every orphan at the head of the next
   * page therefore belongs to that one array. The page that owns the array knows which one it is;
   * this page only has to keep the values where that page can find them.
   *
   * <p>
   * Negative, so it cannot collide with a path node key (always positive) or a name key. It is a tag
   * like any other to every existing reader: one it never looks up, and whose count therefore never
   * enters a completeness check for a path.
   */
  public static final int TAG_ORPHAN_ELEMENTS = Integer.MIN_VALUE;

  /**
   * Tags the page holds values for that this region deliberately does not carry.
   *
   * <p>
   * A fused string whose value outgrew the inline record cap becomes an overflow descriptor: the
   * field is on the page, its bytes are not. A column that held the tag's OTHER values would make
   * every count and every exact negative under that tag a lie — {@code tagCount} is read everywhere
   * as the complete number of that tag's values on the page — so the tag's values stay in the record
   * heap and the tag is named here instead. Its ABSENCE from {@link Header#parentDict} is what every
   * existing reader already handles (it declines and walks the records); the list turns that decline
   * from an accident into a statement, and lets a reader tell "no such field here" from "the field
   * is here, ask the records".
   *
   * <p>
   * Costs nothing when empty: the list is written only when {@link Header#parentDictSize} carries
   * its sign bit, so a page with no oversized string encodes byte-for-byte as before.
   *
   * @return whether {@code tag}'s values are on the page but out of this region
   */
  public static boolean isTagSuppressed(final Header h, final int tag) {
    final int n = h.suppressedTagCount;
    for (int i = 0; i < n; i++) {
      if (h.suppressedTags[i] == tag) {
        return true;
      }
    }
    return false;
  }

  private StringRegion() {}

  // ───────────────────────────────────────────────────────────── header

  /** Parsed header, reused across calls to avoid allocation on the scan hot path. */
  public static final class Header {
    public byte encodingKind;
    /** Tag dictionary classification; see {@link #TAG_KIND_NAME}/{@link #TAG_KIND_PATH_NODE}. */
    public byte tagKind;
    public int count;
    public byte valueBitWidth;
    public int parentDictSize;
    public int[] parentDict; // length >= parentDictSize
    public int[] tagStart; // length >= parentDictSize
    public int[] tagCount; // length >= parentDictSize
    public int[] tagStringDictSize; // length >= parentDictSize
    /**
     * Tags whose values this page holds but this region does not carry; see
     * {@link #isTagSuppressed(Header, int)}. Zero on every page without an oversized string.
     */
    public int suppressedTagCount;
    /** Length >= {@link #suppressedTagCount}; only that prefix is current. */
    public int[] suppressedTags;
    /** For each tag: offset (within payload) of the per-tag length table. */
    public int[] tagStringDictOffset;
    /** valueDictIds byte-region offset within the payload. */
    public int valueDictIdsOffset;
    /** valueDictIds bit-width (same as valueBitWidth; duplicated for convenience). */
    public int valueBitWidthEff;

    public Header parseInto(final MemorySegment payload) {
      int pos = 0;
      encodingKind = payload.get(ValueLayout.JAVA_BYTE, pos++);
      tagKind = payload.get(ValueLayout.JAVA_BYTE, pos++);
      count = getInt(payload, pos);
      pos += 4;
      valueBitWidth = payload.get(ValueLayout.JAVA_BYTE, pos++);
      // The sign bit says a suppressed-tag list follows the four per-tag arrays. A region without
      // one writes a plain positive size and is byte-identical to the pre-suppression encoding.
      final int taggedParentDictSize = getInt(payload, pos);
      pos += 4;
      parentDictSize = taggedParentDictSize & Integer.MAX_VALUE;
      final boolean withSuppressedTags = taggedParentDictSize < 0;
      if (parentDict == null || parentDict.length < parentDictSize)
        parentDict = new int[Math.max(4, parentDictSize)];
      if (tagStart == null || tagStart.length < parentDictSize)
        tagStart = new int[Math.max(4, parentDictSize)];
      if (tagCount == null || tagCount.length < parentDictSize)
        tagCount = new int[Math.max(4, parentDictSize)];
      if (tagStringDictSize == null || tagStringDictSize.length < parentDictSize)
        tagStringDictSize = new int[Math.max(4, parentDictSize)];
      if (tagStringDictOffset == null || tagStringDictOffset.length < parentDictSize)
        tagStringDictOffset = new int[Math.max(4, parentDictSize)];
      for (int i = 0; i < parentDictSize; i++) {
        parentDict[i] = getInt(payload, pos);
        pos += 4;
      }
      for (int i = 0; i < parentDictSize; i++) {
        tagStart[i] = getInt(payload, pos);
        pos += 4;
      }
      for (int i = 0; i < parentDictSize; i++) {
        tagCount[i] = getInt(payload, pos);
        pos += 4;
      }
      for (int i = 0; i < parentDictSize; i++) {
        tagStringDictSize[i] = getInt(payload, pos);
        pos += 4;
      }
      if (withSuppressedTags) {
        suppressedTagCount = getInt(payload, pos);
        pos += 4;
        if (suppressedTags == null || suppressedTags.length < suppressedTagCount) {
          suppressedTags = new int[Math.max(4, suppressedTagCount)];
        }
        for (int i = 0; i < suppressedTagCount; i++) {
          suppressedTags[i] = getInt(payload, pos);
          pos += 4;
        }
      } else {
        suppressedTagCount = 0;
      }
      // Per-tag local dicts: lengths[...] + bytes[...]
      for (int t = 0; t < parentDictSize; t++) {
        tagStringDictOffset[t] = pos;
        final int n = tagStringDictSize[t];
        int total = 0;
        for (int i = 0; i < n; i++)
          total += Math.abs(getInt(payload, pos + i * 4));
        pos += n * 4 + total;
      }
      valueDictIdsOffset = pos;
      valueBitWidthEff = valueBitWidth & 0xFF;
      return this;
    }
  }

  // ──────────────────────────────────────────────────────────── decoding

  /**
   * Local tag id for a parent tag value, or {@code -1} when absent. O(dictSize). The tag value is
   * interpreted according to {@link Header#tagKind}: nameKey for {@link #TAG_KIND_NAME}, pathNodeKey
   * (int-truncated) for {@link #TAG_KIND_PATH_NODE}.
   */
  public static int lookupTag(final Header h, final int tag) {
    for (int i = 0; i < h.parentDictSize; i++) {
      if (h.parentDict[i] == tag)
        return i;
    }
    return -1;
  }

  /** Decode the dict-id for the {@code index}-th value (absolute, tag-grouped). */
  public static int decodeDictIdAt(final MemorySegment payload, final Header h, final int index) {
    final int bw = h.valueBitWidthEff;
    if (bw == 0)
      return 0;
    final long mask = bw == 32
        ? 0xFFFFFFFFL
        : ((1L << bw) - 1L);
    final long bitOff = (long) index * bw;
    final int byteOff = h.valueDictIdsOffset + (int) (bitOff >>> 3);
    final int shift = (int) (bitOff & 7L);
    // Read up to 8 bytes; for widths <= 25 one int is enough. Use long for safety.
    final long word = readUpToLongLE(payload, byteOff);
    return (int) ((word >>> shift) & mask);
  }

  /**
   * Bulk-count occurrences of each dict-id over {@code n} consecutive values starting at absolute
   * index {@code start}, accumulating into {@code counts[dictId]}. Amortises the 64-bit payload read
   * across consecutive dict-ids that share an 8-byte window — for typical bit widths (3-8) this cuts
   * the number of {@code readUpToLongLE} calls by {@code 8/bw}, which directly hits the iter-5
   * profile's {@code decodeDictIdAt} ~4% self-time.
   *
   * <p>
   * Caller must ensure {@code counts} is sized to at least {@code 1 << bw} entries; the method never
   * bounds-checks that array.
   */
  public static void countDictIds(final MemorySegment payload, final Header h, final int start, final int n,
      final long[] counts) {
    if (n <= 0)
      return;
    final int bw = h.valueBitWidthEff;
    if (bw == 0) {
      counts[0] += n;
      return;
    }
    if (StringRegionSimd.histogramDictIds(payload, h.valueDictIdsOffset, bw, start, n, counts)) {
      return;
    }
    // Widths the vector unpack declines still decode here, with the 64-bit read cached across the
    // values that share a window.
    final long mask = bw == 32
        ? 0xFFFFFFFFL
        : ((1L << bw) - 1L);
    final int base = h.valueDictIdsOffset;
    long bitOff = (long) start * bw;
    long cachedWord = 0L;
    int cachedByteOff = -1;
    for (int i = 0; i < n; i++) {
      final int byteOff = base + (int) (bitOff >>> 3);
      final int shift = (int) (bitOff & 7L);
      final long word;
      if (byteOff == cachedByteOff) {
        word = cachedWord;
      } else {
        word = readUpToLongLE(payload, byteOff);
        cachedWord = word;
        cachedByteOff = byteOff;
      }
      counts[(int) ((word >>> shift) & mask)]++;
      bitOff += bw;
    }
  }

  /**
   * Count how many of the {@code n} values starting at {@code start} carry a dict id the caller's
   * predicate accepted.
   *
   * <p>
   * The entry point for string predicates that are not equality — {@code IN}, prefix, range,
   * {@code LIKE}. The caller resolves the predicate against this tag's dictionary once, setting bit
   * {@code k} of {@code idSet} for each accepted entry, and the scan then tests set membership per
   * value at the cost of an equality test. Work proportional to the column's cardinality replaces
   * work proportional to its length.
   *
   * @param idSet membership bitmap over dict ids
   * @param dictSize number of entries in this tag's dictionary
   */
  public static int countDictIdSet(final MemorySegment payload, final Header h, final int start, final int n,
      final long[] idSet, final int dictSize) {
    if (n <= 0) {
      return 0;
    }
    final int bw = h.valueBitWidthEff;
    if (bw == 0) {
      return (idSet.length > 0 && (idSet[0] & 1L) != 0L)
          ? n
          : 0;
    }
    final long simd = StringRegionSimd.countDictIdSet(payload, h.valueDictIdsOffset, bw, start, n, idSet, dictSize);
    if (simd >= 0L) {
      return (int) simd;
    }
    int matched = 0;
    for (int i = 0; i < n; i++) {
      final int id = decodeDictIdAt(payload, h, start + i);
      if (id < dictSize && (idSet[id >>> 6] & (1L << (id & 63))) != 0L) {
        matched++;
      }
    }
    return matched;
  }

  /**
   * Decode the string bytes for the given dict-id within a tag. Returns offset and length in the
   * payload's per-tag local dictionary, avoiding a copy on the group-by hot path.
   */
  public static int decodeStringOffset(final MemorySegment payload, final Header h, final int tag, final int dictId) {
    final int dictStart = h.tagStringDictOffset[tag];
    final int n = h.tagStringDictSize[tag];
    // lengths[0..n), then bytes — walk lengths to sum offsets.
    int off = dictStart + n * 4;
    for (int i = 0; i < dictId; i++) {
      off += Math.abs(getInt(payload, dictStart + i * 4));
    }
    return off;
  }

  public static int decodeStringLength(final MemorySegment payload, final Header h, final int tag, final int dictId) {
    return Math.abs(getInt(payload, h.tagStringDictOffset[tag] + dictId * 4));
  }

  /**
   * Whether the dict entry's bytes are FSST-encoded (against the owning page's symbol table) rather
   * than raw UTF-8. Carried as the sign of the entry's length; raw entries — the only kind that
   * existed before per-value encoding — are non-negative, so the flag costs no bytes.
   */
  public static boolean isEntryCompressed(final MemorySegment payload, final Header h, final int tag,
      final int dictId) {
    return getInt(payload, h.tagStringDictOffset[tag] + dictId * 4) < 0;
  }

  /** {@link #findDictId} result: the tag's dictionary holds no entry equal to the literal. */
  public static final int DICT_ID_ABSENT = -1;

  /**
   * {@link #findDictId} result: the tag holds FSST-encoded entries and the caller supplied no encoded
   * form of the literal, so equality cannot be decided from the region alone.
   */
  public static final int DICT_ID_UNDECIDABLE = -2;

  /**
   * Find the dictionary id of {@code literal} within one tag's local string dictionary.
   *
   * <p>
   * One pass over the length table, accumulating the byte offset as it goes — the per-entry
   * {@link #decodeStringOffset} re-walks the lengths from zero, which would make probing the whole
   * dictionary quadratic. Entries are deduplicated by the encoder, so at most one can match.
   *
   * <p>
   * Entries are compared in their STORED form: raw entries against {@code literal}, FSST-encoded
   * entries against {@code encodedLiteral}. Passing {@code null} for the latter is allowed — the
   * search then reports {@link #DICT_ID_UNDECIDABLE} the moment it meets an encoded entry, rather
   * than skipping it and silently missing a match.
   *
   * @param payload the region payload
   * @param h parsed header
   * @param tag local tag id
   * @param literal the value to look for, UTF-8
   * @param encodedLiteral the same value encoded against this page's FSST symbol table, or
   *        {@code null} when no table is in hand
   * @return the dict id, {@link #DICT_ID_ABSENT}, or {@link #DICT_ID_UNDECIDABLE}
   */
  public static int findDictId(final MemorySegment payload, final Header h, final int tag, final byte[] literal,
      final byte @Nullable [] encodedLiteral) {
    final int dictStart = h.tagStringDictOffset[tag];
    final int n = h.tagStringDictSize[tag];
    int off = dictStart + n * 4;
    for (int i = 0; i < n; i++) {
      final int lenField = getInt(payload, dictStart + i * 4);
      final boolean compressed = lenField < 0;
      final int storedLen = compressed
          ? -lenField
          : lenField;
      // An FSST-encoded entry is compared against the FSST-encoded literal: same symbol table,
      // same encoder, so equal values have equal stored bytes. Nothing is decompressed.
      final byte[] want = compressed
          ? encodedLiteral
          : literal;
      if (compressed && encodedLiteral == null) {
        return DICT_ID_UNDECIDABLE;
      }
      if (storedLen == want.length) {
        int k = 0;
        while (k < storedLen && payload.get(ValueLayout.JAVA_BYTE, off + k) == want[k]) {
          k++;
        }
        if (k == storedLen) {
          return i;
        }
      }
      off += storedLen;
    }
    return DICT_ID_ABSENT;
  }

  /**
   * Count how many of the {@code n} values starting at absolute index {@code start} carry
   * {@code dictId}.
   *
   * <p>
   * The equality-count counterpart to {@link #countDictIds}: that one histograms every id into a
   * caller-sized array, which a 32-bit width makes impossible; this one needs no array at all. The
   * 64-bit read is cached across the values that share a window, exactly as there.
   */
  public static int countDictId(final MemorySegment payload, final Header h, final int start, final int n,
      final int dictId) {
    if (n <= 0)
      return 0;
    final int bw = h.valueBitWidthEff;
    if (bw == 0) {
      return dictId == 0
          ? n
          : 0;
    }
    final long simd = StringRegionSimd.countDictId(payload, h.valueDictIdsOffset, bw, start, n, dictId);
    if (simd >= 0L) {
      return (int) simd;
    }
    // Widths the kernel declines still decode here, with the 64-bit read cached across the values
    // that share a window.
    final long mask = bw == 32
        ? 0xFFFFFFFFL
        : ((1L << bw) - 1L);
    final int base = h.valueDictIdsOffset;
    long bitOff = (long) start * bw;
    long cachedWord = 0L;
    int cachedByteOff = -1;
    int matched = 0;
    for (int i = 0; i < n; i++) {
      final int byteOff = base + (int) (bitOff >>> 3);
      final int shift = (int) (bitOff & 7L);
      final long word;
      if (byteOff == cachedByteOff) {
        word = cachedWord;
      } else {
        word = readUpToLongLE(payload, byteOff);
        cachedWord = word;
        cachedByteOff = byteOff;
      }
      if ((int) ((word >>> shift) & mask) == dictId) {
        matched++;
      }
      bitOff += bw;
    }
    return matched;
  }

  /**
   * Selection form of {@link #countDictId}: set bit {@code i} of {@code rowBits} for each of the
   * {@code n} values whose dict id is {@code dictId}, bits indexed relative to {@code start}.
   *
   * <p>
   * Exists for the fused multi-column kernel, which intersects one row bitmap per predicate leaf; a
   * count cannot be AND-ed with the rows another column produced.
   *
   * @return the number of bits set, or {@code -1} when the width has no SIMD plan — the caller then
   *         decides the page through the record path, exactly as for any declined kernel
   */
  public static int selectDictIdInto(final MemorySegment payload, final Header h, final int start, final int n,
      final int dictId, final long[] rowBits) {
    if (n <= 0)
      return 0;
    final int bw = h.valueBitWidthEff;
    if (bw == 0) {
      // A one-entry dictionary packs to zero bits: every value is id 0, so the answer is all rows
      // or none of them without reading a column that does not exist.
      final int words = (n + 63) >>> 6;
      Arrays.fill(rowBits, 0, words, 0L);
      if (dictId != 0) {
        return 0;
      }
      for (int w = 0; w < words; w++) {
        final int width = Math.min(64, n - (w << 6));
        rowBits[w] = width >= 64
            ? ~0L
            : (1L << width) - 1L;
      }
      return n;
    }
    final long simd = StringRegionSimd.selectDictIdInto(payload, h.valueDictIdsOffset, bw, start, n, dictId, rowBits);
    return simd >= 0L
        ? (int) simd
        : -1;
  }

  /**
   * Live-value counterpart of {@link #countDictId}, for a versioned merge in which some values are
   * shadowed by a newer fragment and must not be counted.
   *
   * <p>
   * Same sequential window cache as {@link #countDictId}, which is the whole point: the merge used to
   * decode each surviving value through {@link #decodeDictIdAt}, one independent 64-bit read per
   * value, discarding the window every time. At the usual 3-8 bit widths that is up to {@code 8/bw}
   * times the reads the single-fragment path performs for the same column — so a page touched by more
   * than one commit paid MORE per value than an untouched one, on top of a branch per value. Liveness
   * arrives as a bitmap so the decode stays a straight-line walk.
   *
   * <p>
   * Callers with nothing shadowed should use {@link #countDictId}: it is this loop without the bitmap
   * load, and it covers the newest fragment of every page.
   *
   * @param liveBits bit {@code k} set when value {@code start + k} is not shadowed
   */
  public static int countDictIdMasked(final MemorySegment payload, final Header h, final int start, final int n,
      final int dictId, final long[] liveBits) {
    if (n <= 0)
      return 0;
    final int bw = h.valueBitWidthEff;
    if (bw == 0) {
      if (dictId != 0) {
        return 0;
      }
      int live = 0;
      for (int i = 0; i < n; i++) {
        live += (int) ((liveBits[i >>> 6] >>> (i & 63)) & 1L);
      }
      return live;
    }
    final long simd = StringRegionSimd.countDictIdMasked(payload, h.valueDictIdsOffset, bw, start, n, dictId, liveBits);
    if (simd >= 0L) {
      return (int) simd;
    }
    final long mask = bw == 32
        ? 0xFFFFFFFFL
        : ((1L << bw) - 1L);
    final int base = h.valueDictIdsOffset;
    long bitOff = (long) start * bw;
    long cachedWord = 0L;
    int cachedByteOff = -1;
    int matched = 0;
    for (int i = 0; i < n; i++) {
      final int byteOff = base + (int) (bitOff >>> 3);
      final int shift = (int) (bitOff & 7L);
      final long word;
      if (byteOff == cachedByteOff) {
        word = cachedWord;
      } else {
        word = readUpToLongLE(payload, byteOff);
        cachedWord = word;
        cachedByteOff = byteOff;
      }
      // Decode unconditionally and let the liveness bit gate the increment: the window cache only
      // pays off when the walk is sequential, so skipping shadowed values would cost more than
      // decoding them.
      final int hit = ((int) ((word >>> shift) & mask) == dictId)
          ? 1
          : 0;
      matched += hit & (int) ((liveBits[i >>> 6] >>> (i & 63)) & 1L);
      bitOff += bw;
    }
    return matched;
  }

  // ───────────────────────────────────────────────────────────── encoder

  /**
   * Streaming producer: owner adds (parentNameKey, valueBytes) pairs in any order, then calls
   * {@link #finish()} to obtain the packed payload.
   *
   * <h2>HFT-grade producer</h2> All per-value bookkeeping uses fastutil primitive collections — no
   * {@code Integer}/{@code Long} autoboxing on the encode hot path. String dedup is keyed by a
   * pre-computed 64-bit hash (xxHash-like) stored in a primitive {@code Long2IntOpenHashMap};
   * collisions are resolved by a linear rescan of the candidate dict bucket. For the reference
   * workload (90 records × 2 string fields, 8 unique values each) this eliminates ~200
   * {@code Integer} boxes + ~200 {@code BytesKey} allocations per page that the earlier
   * ArrayList/HashMap/BytesKey implementation paid.
   */
  public static final class Encoder {
    /** Parent nameKeys, in tag-id order. Size = number of distinct parents. */
    private final IntArrayList tagOrder = new IntArrayList(4);
    /** parentNameKey → tag id (negative sentinel for absent). */
    private final Int2IntOpenHashMap tagIndex = new Int2IntOpenHashMap(4);
    /** Per-tag dict-ids in record-insertion order. */
    private final IntArrayList[] tagDictIds0 = new IntArrayList[4];
    private IntArrayList[] tagDictIds = tagDictIds0;
    /** Per-tag dictionary hashes, indexed by local dictionary id. */
    private long[][] tagHashes = new long[4][];
    /** Store holding each dictionary entry's bytes. Usually {@link #ownedValueStore}. */
    private ValueStore[][] tagStores = new ValueStore[4][];
    /** Start offset of each dictionary entry within its {@link #tagStores} value store. */
    private int[][] tagOffsets = new int[4][];
    /** Exact stored-byte length of each dictionary entry. */
    private int[][] tagLengths = new int[4][];
    /**
     * Parallel to {@link #tagStores}: whether each dict entry's bytes are FSST-encoded rather than raw
     * UTF-8. Rides the dedup: two adds only fold into one entry when bytes AND flag agree, because raw
     * bytes that happen to equal some other value's encoded form are still a different value.
     */
    private boolean[][] tagCompressed = new boolean[4][];
    private int[] tagDictSize = new int[4];
    /**
     * Parallel to {@link #tagDictSize}: the tag is named on the page but its values do not enter the
     * region. Set by {@link #suppressTag(int)}; see {@link #isTagSuppressed(Header, int)}.
     */
    private boolean[] tagSuppressed = new boolean[4];
    /** Number of set entries in {@link #tagSuppressed} — the common page keeps this at zero. */
    private int suppressedTagCount;
    /** Reusable retained-tag index buffer for {@link #encodeInto}; never escapes the encoder. */
    private int[] retainedTags = new int[4];
    /**
     * Owner-confined, grow-only backing store for dictionary misses. Its capacity survives reset; only
     * the logical length returns to zero. Entries in the alternative name/path encoder may temporarily
     * borrow an exact range from this store.
     */
    private final ValueStore ownedValueStore = new ValueStore();
    /** Owner-confined reusable wire scratch. Only {@code [0, encodedLength)} is current. */
    private byte[] output = new byte[256];
    /** Exact logical length of the most recent successful {@link #encodeInto} call. */
    private int encodedLength;

    public Encoder() {
      tagIndex.defaultReturnValue(-1);
    }

    /**
     * Reset for reuse across pages. All internal arrays and the owned value-store capacity are
     * retained; live foreign-store references and per-tag counts are cleared, then the owned store's
     * logical length is reset as soon as no alternative encoder still references one of its ranges.
     * Zero allocations.
     */
    public void reset() {
      encodedLength = 0;
      final int prevTags = tagOrder.size();
      for (int t = 0; t < prevTags; t++) {
        final IntArrayList ids = tagDictIds[t];
        if (ids != null)
          ids.clear();
        final ValueStore[] stores = tagStores[t];
        if (stores != null) {
          final int sz = tagDictSize[t];
          for (int i = 0; i < sz; i++) {
            final ValueStore store = stores[i];
            if (store != null && store != ownedValueStore) {
              store.release();
            }
            stores[i] = null;
          }
        }
        tagDictSize[t] = 0;
        tagSuppressed[t] = false;
      }
      suppressedTagCount = 0;
      tagIndex.clear();
      tagIndex.defaultReturnValue(-1);
      tagOrder.clear();
      ownedValueStore.requestReset();
    }

    /**
     * Declare that {@code parentNameKey}'s values live on the page but must not enter this region.
     *
     * <p>
     * Idempotent, and independent of ordering: values already added under the tag are dropped at
     * encode time, and values added afterwards are dropped too. The tag itself is remembered and
     * written to the suppressed-tag list, so a reader can tell it from a tag the page never held.
     * Adding no value at all is a legal use — an oversized string is the tag's only occurrence on
     * many pages.
     */
    public void suppressTag(final int parentNameKey) {
      final int tag = getOrCreateTag(parentNameKey);
      if (!tagSuppressed[tag]) {
        tagSuppressed[tag] = true;
        suppressedTagCount++;
      }
    }

    public void addValue(final int parentNameKey, final byte[] value) {
      addValue(parentNameKey, value, false);
    }

    /**
     * Add a value whose bytes are stored as-is, flagged as raw UTF-8 or FSST-encoded.
     *
     * <p>
     * The region deliberately stores the heap's stored form verbatim — encoded when the heap compressed
     * the slot, raw otherwise — so that value elision remains a pure byte copy in both directions and
     * no decode ever happens at page-deserialize time (where no reader, and therefore no symbol table,
     * is in scope). The flag travels as the sign of the entry's length on the wire.
     *
     * <p>
     * On a dictionary miss the encoder copies the bytes into its reusable grow-only store. The caller
     * may therefore reuse or mutate {@code value} as soon as this method returns.
     */
    public void addValue(final int parentNameKey, final byte[] value, final boolean compressed) {
      if (value == null) {
        throw new NullPointerException("value");
      }
      addValueInternal(parentNameKey, value, 0, value.length, compressed, VALUE_HASH.hashBytes(value, 0, value.length));
    }

    /**
     * Add a slice from reusable caller scratch without retaining that scratch.
     *
     * <p>
     * Hashing and collision confirmation operate directly on
     * {@code value[valueOffset..valueOffset + valueLength)}. A dictionary hit therefore performs no
     * allocation. On a miss, the encoder appends to one owner-confined grow-only store; later changes
     * to the caller's array cannot alter dictionary identity or encoded wire bytes.
     *
     * @param parentNameKey semantic tag value (name key or path node key, as selected at finish)
     * @param value caller-owned source array
     * @param valueOffset first byte of the value
     * @param valueLength number of bytes in the value
     * @param compressed whether the slice is FSST-encoded rather than raw UTF-8
     */
    public void addValue(final int parentNameKey, final byte[] value, final int valueOffset, final int valueLength,
        final boolean compressed) {
      addValueCopiedAndShareWith(parentNameKey, value, valueOffset, valueLength, compressed, null, 0);
    }

    /**
     * Add a reusable-scratch slice and optionally share its private canonical representation with a
     * second tag encoder.
     *
     * <p>
     * This encoder appends to its owner-confined store on a dictionary miss (or reuses its existing
     * entry on a hit). When {@code alternate} is non-null, its dictionary references the exact same
     * private store range through the no-copy internal path. This is how the page writer builds name-
     * and path-tagged candidates without either retaining caller scratch or copying each distinct value
     * twice. No mutable canonical reference escapes either encoder.
     *
     * @param parentNameKey this encoder's semantic tag
     * @param value caller-owned scratch
     * @param valueOffset first byte of the stored representation
     * @param valueLength number of stored bytes
     * @param compressed whether the stored representation is FSST-encoded
     * @param alternate optional alternative-tag encoder sharing this encoder's canonical bytes
     * @param alternateParentNameKey semantic tag to use in {@code alternate}; ignored when null
     */
    public void addValueCopiedAndShareWith(final int parentNameKey, final byte[] value, final int valueOffset,
        final int valueLength, final boolean compressed, final @Nullable Encoder alternate,
        final int alternateParentNameKey) {
      if (value == null) {
        throw new NullPointerException("value");
      }
      if (valueOffset < 0 || valueLength < 0 || valueOffset > value.length - valueLength) {
        throw new IndexOutOfBoundsException(
            "valueOffset=" + valueOffset + ", valueLength=" + valueLength + ", capacity=" + value.length);
      }
      final long hash = VALUE_HASH.hashBytes(value, valueOffset, valueLength);
      final int id = addValueInternal(parentNameKey, value, valueOffset, valueLength, compressed, hash);
      if (alternate != null) {
        final int tag = tagIndex.get(parentNameKey);
        alternate.addStoredValueInternal(alternateParentNameKey, tagStores[tag][id], tagOffsets[tag][id],
            tagLengths[tag][id], compressed, hash);
      }
    }

    private int addValueInternal(final int parentNameKey, final byte[] value, final int valueOffset,
        final int valueLength, final boolean compressed, final long hash) {
      final int tag = getOrCreateTag(parentNameKey);
      // Dedup by 64-bit hash; equality is confirmed byte-by-byte below, so the hash only decides
      // which candidates get compared and never which values dedup. Nothing here is persisted —
      // dictionary ids are assigned in first-seen order and the table is reset per page.
      final long[] hashes = tagHashes[tag];
      final ValueStore[] stores = tagStores[tag];
      final int[] offsets = tagOffsets[tag];
      final int[] lengths = tagLengths[tag];
      final boolean[] flags = tagCompressed[tag];
      final int n = tagDictSize[tag];
      int id = -1;
      for (int i = 0; i < n; i++) {
        if (hashes[i] == hash && flags[i] == compressed && lengths[i] == valueLength
            && stores[i].equalsRange(offsets[i], value, valueOffset, valueLength)) {
          id = i;
          break;
        }
      }
      if (id < 0) {
        ensureDictionarySlot(tag, n);
        final int offset = ownedValueStore.append(value, valueOffset, valueLength);
        id = n;
        tagHashes[tag][n] = hash;
        tagStores[tag][n] = ownedValueStore;
        tagOffsets[tag][n] = offset;
        tagLengths[tag][n] = valueLength;
        tagCompressed[tag][n] = compressed;
        tagDictSize[tag] = n + 1;
      }
      tagDictIds[tag].add(id);
      return id;
    }

    /** Add a dictionary occurrence from an internal immutable-for-the-page store range. */
    private int addStoredValueInternal(final int parentNameKey, final ValueStore sourceStore, final int sourceOffset,
        final int valueLength, final boolean compressed, final long hash) {
      final int tag = getOrCreateTag(parentNameKey);
      final long[] hashes = tagHashes[tag];
      final ValueStore[] stores = tagStores[tag];
      final int[] offsets = tagOffsets[tag];
      final int[] lengths = tagLengths[tag];
      final boolean[] flags = tagCompressed[tag];
      final int n = tagDictSize[tag];
      int id = -1;
      for (int i = 0; i < n; i++) {
        if (hashes[i] == hash && flags[i] == compressed && lengths[i] == valueLength
            && stores[i].equalsRange(offsets[i], sourceStore, sourceOffset, valueLength)) {
          id = i;
          break;
        }
      }
      if (id < 0) {
        ensureDictionarySlot(tag, n);
        id = n;
        tagHashes[tag][n] = hash;
        tagStores[tag][n] = sourceStore;
        tagOffsets[tag][n] = sourceOffset;
        tagLengths[tag][n] = valueLength;
        tagCompressed[tag][n] = compressed;
        tagDictSize[tag] = n + 1;
        if (sourceStore != ownedValueStore) {
          sourceStore.retain();
        }
      }
      tagDictIds[tag].add(id);
      return id;
    }

    private int getOrCreateTag(final int parentNameKey) {
      int tag = tagIndex.get(parentNameKey);
      if (tag >= 0) {
        return tag;
      }
      tag = tagOrder.size();
      tagIndex.put(parentNameKey, tag);
      tagOrder.add(parentNameKey);
      ensureTagSlot(tag);
      if (tagDictIds[tag] == null) {
        tagDictIds[tag] = new IntArrayList(16);
        tagHashes[tag] = new long[8];
        tagStores[tag] = new ValueStore[8];
        tagOffsets[tag] = new int[8];
        tagLengths[tag] = new int[8];
        tagCompressed[tag] = new boolean[8];
      }
      tagDictSize[tag] = 0;
      tagSuppressed[tag] = false;
      return tag;
    }

    /** Grow-only scratch for the retained-tag mapping; contents are rewritten on every call. */
    private int[] retainedTagScratch(final int tags) {
      if (retainedTags.length < tags) {
        retainedTags = new int[Math.max(tags, retainedTags.length << 1)];
      }
      return retainedTags;
    }

    private void ensureDictionarySlot(final int tag, final int dictionarySize) {
      if (dictionarySize < tagHashes[tag].length) {
        return;
      }
      final int grown = Math.max(8, dictionarySize << 1);
      tagHashes[tag] = Arrays.copyOf(tagHashes[tag], grown);
      tagStores[tag] = Arrays.copyOf(tagStores[tag], grown);
      tagOffsets[tag] = Arrays.copyOf(tagOffsets[tag], grown);
      tagLengths[tag] = Arrays.copyOf(tagLengths[tag], grown);
      tagCompressed[tag] = Arrays.copyOf(tagCompressed[tag], grown);
    }

    /** Package-private white-box check for the one-canonical-store-range sharing invariant. */
    boolean sharesCanonicalValueWith(final int parentNameKey, final int dictId, final Encoder alternate,
        final int alternateParentNameKey, final int alternateDictId) {
      if (alternate == null) {
        return false;
      }
      final int tag = tagIndex.get(parentNameKey);
      final int alternateTag = alternate.tagIndex.get(alternateParentNameKey);
      return tag >= 0 && alternateTag >= 0 && dictId >= 0 && dictId < tagDictSize[tag] && alternateDictId >= 0
          && alternateDictId < alternate.tagDictSize[alternateTag]
          && tagStores[tag][dictId] == alternate.tagStores[alternateTag][alternateDictId]
          && tagOffsets[tag][dictId] == alternate.tagOffsets[alternateTag][alternateDictId]
          && tagLengths[tag][dictId] == alternate.tagLengths[alternateTag][alternateDictId];
    }

    private void ensureTagSlot(final int tag) {
      if (tag < tagDictIds.length)
        return;
      final int grown = Math.max(tag + 1, tagDictIds.length * 2);
      tagDictIds = Arrays.copyOf(tagDictIds, grown);
      tagHashes = Arrays.copyOf(tagHashes, grown);
      tagStores = Arrays.copyOf(tagStores, grown);
      tagOffsets = Arrays.copyOf(tagOffsets, grown);
      tagLengths = Arrays.copyOf(tagLengths, grown);
      tagCompressed = Arrays.copyOf(tagCompressed, grown);
      tagDictSize = Arrays.copyOf(tagDictSize, grown);
      tagSuppressed = Arrays.copyOf(tagSuppressed, grown);
    }

    /**
     * Serialize to a detached exact-size array, defaulting tagKind to {@link #TAG_KIND_NAME}.
     *
     * <p>
     * The returned array is caller-owned and is never reused by this encoder.
     */
    public byte[] finish() {
      return finish(TAG_KIND_NAME);
    }

    /**
     * Serialize to wire format with an explicit {@code tagKind} header byte. Tags themselves are not
     * transformed — the caller is responsible for passing the correct semantic values via
     * {@link #addValue(int, byte[])}.
     */
    public byte[] finish(final byte tagKind) {
      return finish(tagKind, false);
    }

    /**
     * Serialize, recording whether array-element staging ran for this page.
     *
     * @param elementsStaged {@code true} to write {@link #ENC_DICT_BITPACKED_ZM_ELEMENTS}, which
     *        promises a reader that an absent {@link #TAG_ORPHAN_ELEMENTS} means "this page has no
     *        spilled elements" rather than "nobody looked"
     */
    public byte[] finish(final byte tagKind, final boolean elementsStaged) {
      final int length = encodeInto(tagKind, elementsStaged);
      // Length zero is a legal outcome (no values, or every value's tag suppressed) and yields an
      // EMPTY array, not null: a caller installing a region must treat it as "no region".
      return Arrays.copyOf(output, length);
    }

    /**
     * Serialize into this encoder's owner-confined reusable output buffer.
     *
     * <p>
     * Only {@code [0, returnedLength)} of {@link #output()} is valid. The next call may overwrite that
     * prefix or replace the backing array. A caller that retains the result must synchronously copy the
     * exact prefix first; {@link RegionTable#set(byte, byte[], int)} is such an ownership boundary.
     *
     * @param tagKind semantic interpretation of the tag dictionary
     * @param elementsStaged whether array-element staging ran for this page
     * @return exact logical length of the encoded payload, or zero when no value survives — nothing
     *         was added, or every added value's tag is suppressed
     */
    public int encodeInto(final byte tagKind, final boolean elementsStaged) {
      encodedLength = 0;
      if (tagKind != TAG_KIND_NAME && tagKind != TAG_KIND_PATH_NODE) {
        throw new IllegalArgumentException("tagKind=" + tagKind);
      }
      final int tagsSeen = tagOrder.size();
      if (tagsSeen == 0) {
        return 0;
      }
      // Suppressed tags leave the value area entirely and are named in their own list, so every
      // loop below walks the RETAINED tags. Materialised once instead of branching in each loop:
      // a page carries a few dozen tags at most, and the common page (nothing suppressed) gets the
      // identity mapping and therefore the exact byte sequence it had before suppression existed.
      final int[] retained = retainedTagScratch(tagsSeen);
      int ps = 0;
      int count = 0;
      int maxLocalDict = 0;
      for (int t = 0; t < tagsSeen; t++) {
        if (tagSuppressed[t]) {
          continue;
        }
        retained[ps++] = t;
        count += tagDictIds[t].size();
        if (tagDictSize[t] > maxLocalDict)
          maxLocalDict = tagDictSize[t];
      }
      if (count == 0) {
        // Every value on the page belongs to a suppressed tag. A header naming only absences would
        // cost bytes on every read and tell no reader anything it does not already conclude from an
        // absent region, so the page keeps its strings in the heap and publishes nothing.
        return 0;
      }
      final int bitWidth = Math.max(1, 32 - Integer.numberOfLeadingZeros(Math.max(1, maxLocalDict - 1)));
      // +1 byte for tagKind prefix.
      final long suppressedSize = suppressedTagCount == 0
          ? 0L
          : (long) Integer.BYTES + (long) suppressedTagCount * Integer.BYTES;
      final long headerSize =
          1L + 1L + Integer.BYTES + 1L + Integer.BYTES + (long) ps * Integer.BYTES * 4L + suppressedSize;
      long dictBytesSize = 0L;
      for (int r = 0; r < ps; r++) {
        final int t = retained[r];
        final int sz = tagDictSize[t];
        dictBytesSize += (long) sz * Integer.BYTES;
        for (int i = 0; i < sz; i++)
          dictBytesSize += tagLengths[t][i];
      }
      final long valueDictIdBytes = ((long) count * bitWidth + 7L) >>> 3;
      final int totalLength = checkedEncodedLength(headerSize + dictBytesSize + valueDictIdBytes);
      ensureOutputCapacity(totalLength);
      int pos = 0;
      output[pos++] = elementsStaged
          ? ENC_DICT_BITPACKED_ZM_ELEMENTS
          : ENC_DICT_BITPACKED_ZM;
      output[pos++] = tagKind;
      putInt(output, pos, count);
      pos += 4;
      output[pos++] = (byte) bitWidth;
      // The sign bit announces the suppressed-tag list; without one the field is the plain size it
      // has always been.
      putInt(output, pos, suppressedTagCount == 0
          ? ps
          : ps | Integer.MIN_VALUE);
      pos += 4;
      for (int r = 0; r < ps; r++) {
        putInt(output, pos, tagOrder.getInt(retained[r]));
        pos += 4;
      }
      int running = 0;
      for (int r = 0; r < ps; r++) {
        putInt(output, pos, running);
        pos += 4;
        running += tagDictIds[retained[r]].size();
      }
      for (int r = 0; r < ps; r++) {
        putInt(output, pos, tagDictIds[retained[r]].size());
        pos += 4;
      }
      for (int r = 0; r < ps; r++) {
        putInt(output, pos, tagDictSize[retained[r]]);
        pos += 4;
      }
      if (suppressedTagCount != 0) {
        putInt(output, pos, suppressedTagCount);
        pos += 4;
        for (int t = 0; t < tagsSeen; t++) {
          if (tagSuppressed[t]) {
            putInt(output, pos, tagOrder.getInt(t));
            pos += 4;
          }
        }
      }
      for (int r = 0; r < ps; r++) {
        final int t = retained[r];
        final int sz = tagDictSize[t];
        for (int i = 0; i < sz; i++) {
          // Sign bit carries the per-entry FSST flag; consumers read Math.abs for the length.
          final int len = tagLengths[t][i];
          putInt(output, pos, tagCompressed[t][i]
              ? -len
              : len);
          pos += 4;
        }
        for (int i = 0; i < sz; i++) {
          final int len = tagLengths[t][i];
          tagStores[t][i].copyTo(tagOffsets[t][i], output, pos, len);
          pos += len;
        }
      }
      int bitPos = 0;
      final int valueDictIdsBase = pos;
      // bitPackAppend ORs lanes into the destination. A reusable buffer can retain high bits from
      // the preceding page, so restore the zero-initialized-array invariant over the exact body.
      Arrays.fill(output, valueDictIdsBase, totalLength, (byte) 0);
      for (int r = 0; r < ps; r++) {
        final IntArrayList ids = tagDictIds[retained[r]];
        final int sz = ids.size();
        final int[] idsArr = ids.elements();
        for (int i = 0; i < sz; i++) {
          bitPackAppend(output, valueDictIdsBase, bitPos, idsArr[i], bitWidth);
          bitPos += bitWidth;
        }
      }
      if (pos + (int) valueDictIdBytes != totalLength) {
        throw new IllegalStateException(
            "string region size mismatch: expected=" + totalLength + " written=" + (pos + valueDictIdBytes));
      }
      encodedLength = totalLength;
      return totalLength;
    }

    /** Mutable scratch buffer. The next call to {@link #encodeInto} may overwrite or replace it. */
    public byte[] output() {
      return output;
    }

    /**
     * Exact length returned by the most recent successful encode, or zero after an empty/failed
     * attempt.
     */
    public int encodedLength() {
      return encodedLength;
    }

    /** Package-private high-water check used by reset/reuse tests. */
    int valueStoreCapacity() {
      return ownedValueStore.capacity();
    }

    /** Package-private live-byte check used by reset/reuse tests. */
    int valueStoreLength() {
      return ownedValueStore.length();
    }

    private void ensureOutputCapacity(final int required) {
      if (required <= output.length) {
        return;
      }
      final long doubled = Math.min((long) Integer.MAX_VALUE, (long) output.length << 1);
      output = new byte[checkedEncodedLength(Math.max((long) required, doubled))];
    }

    /**
     * One encoder-owned append-only byte area. Alternative tag encoders retain ranges rather than
     * arrays; a reset is therefore deferred until the last foreign dictionary entry releases it. The
     * class is deliberately unsynchronised: encoders and their shared candidate are confined to one
     * page-writer thread.
     */
    private static final class ValueStore {
      private byte[] bytes = new byte[1024];
      private int length;
      private int foreignReferences;
      private boolean resetPending;

      private int append(final byte[] source, final int sourceOffset, final int sourceLength) {
        // A writer that starts the next page before an alternative encoder releases the preceding
        // page cannot reuse the borrowed prefix. Keep it intact and append after it; the next clean
        // reset returns the logical length to zero. PageKind normally releases both candidates first.
        resetPending = false;
        final int offset = length;
        final int required = checkedEncodedLength((long) offset + sourceLength);
        ensureCapacity(required);
        System.arraycopy(source, sourceOffset, bytes, offset, sourceLength);
        length = required;
        return offset;
      }

      private boolean equalsRange(final int offset, final byte[] other, final int otherOffset, final int rangeLength) {
        return Arrays.equals(bytes, offset, offset + rangeLength, other, otherOffset, otherOffset + rangeLength);
      }

      private boolean equalsRange(final int offset, final ValueStore other, final int otherOffset,
          final int rangeLength) {
        return (this == other && offset == otherOffset)
            || Arrays.equals(bytes, offset, offset + rangeLength, other.bytes, otherOffset, otherOffset + rangeLength);
      }

      private void copyTo(final int offset, final byte[] destination, final int destinationOffset,
          final int rangeLength) {
        System.arraycopy(bytes, offset, destination, destinationOffset, rangeLength);
      }

      private void retain() {
        if (foreignReferences == Integer.MAX_VALUE) {
          throw new IllegalStateException("too many shared StringRegion store ranges");
        }
        foreignReferences++;
      }

      private void release() {
        if (foreignReferences <= 0) {
          throw new IllegalStateException("unbalanced StringRegion store-range release");
        }
        foreignReferences--;
        if (foreignReferences == 0 && resetPending) {
          length = 0;
          resetPending = false;
        }
      }

      private void requestReset() {
        if (foreignReferences == 0) {
          length = 0;
          resetPending = false;
        } else {
          resetPending = true;
        }
      }

      private void ensureCapacity(final int required) {
        if (required <= bytes.length) {
          return;
        }
        final long doubled = Math.min((long) Integer.MAX_VALUE, (long) bytes.length << 1);
        bytes = Arrays.copyOf(bytes, checkedEncodedLength(Math.max((long) required, doubled)));
      }

      private int capacity() {
        return bytes.length;
      }

      private int length() {
        return length;
      }
    }

    /**
     * Hash used to pre-filter dictionary candidates.
     *
     * <p>
     * XXH3 rather than the FNV-1a this used to compute by hand. FNV-1a is one multiply per byte on a
     * serial dependency chain; XXH3 consumes eight bytes a step with instruction-level parallelism, and
     * that difference only pays once a value is long enough to amortise its fixed setup.
     * {@code StringRegionDictionaryBenchmark} puts the crossover at roughly twelve bytes: XXH3 is about
     * 1.6× faster over 12-32 byte values, 2.6× over 32-96, and 4.4× on free text, but about 1.6×
     * <em>slower</em> on 4-12 byte ids.
     *
     * <p>
     * Values land above that crossover in practice — this dictionary holds JSON string <em>values</em>,
     * not member names, which arrive as an already-interned name key. Profiling a real ingest agrees:
     * the hand-rolled hash was 2.3% of application-thread samples and the swap cost only 0.7% more in
     * XXH3, so the page dictionary's hashing fell by roughly a third. A corpus of very short values
     * would invert that, which is what the benchmark is for.
     *
     * <p>
     * Swapping it cannot change what this class emits: the hash is a pre-filter whose hits are
     * confirmed with {@link Arrays#equals}, ids are handed out in first-seen order, and the table lives
     * only for the page being encoded. The same {@code xx3} the rest of the engine uses.
     */
    private static final LongHashFunction VALUE_HASH = LongHashFunction.xx3();
  }

  // ────────────────────────────────────────────────── internal helpers

  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private static int getInt(final MemorySegment buf, final long off) {
    return buf.get(LE.INT, off);
  }

  private static void putInt(final byte[] buf, final int off, final int v) {
    INT_LE.set(buf, off, v);
  }

  private static int checkedEncodedLength(final long length) {
    if (length < 0L || length > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("encoded string region is too large: " + length + " bytes");
    }
    return (int) length;
  }

  /**
   * Package-private so {@link StringRegionSimd} reads the packed column through the exact same word
   * load. Two copies of this would be two chances for the SIMD and scalar paths to disagree about a
   * tail byte.
   */
  static long readUpToLongLE(final MemorySegment data, final long off) {
    final long avail = data.byteSize() - off;
    if (avail >= Long.BYTES) {
      return data.get(LE.LONG, off);
    }
    long v = 0L;
    for (long i = 0; i < avail; i++) {
      v |= ((long) (data.get(ValueLayout.JAVA_BYTE, off + i) & 0xFF)) << (i << 3);
    }
    return v;
  }

  private static void bitPackAppend(final byte[] out, final int base, final int bitPos, final int value,
      final int bitWidth) {
    final long mask = bitWidth == 32
        ? 0xFFFFFFFFL
        : ((1L << bitWidth) - 1L);
    long v = value & mask;
    int byteOff = base + (bitPos >>> 3);
    int shift = bitPos & 7;
    int remaining = bitWidth;
    while (remaining > 0) {
      final int bitsThisByte = Math.min(8 - shift, remaining);
      out[byteOff] |= (byte) ((v & ((1L << bitsThisByte) - 1L)) << shift);
      v >>>= bitsThisByte;
      remaining -= bitsThisByte;
      byteOff++;
      shift = 0;
    }
  }
}
