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
 *                                        //   nameKeys ("dept", "city", ...)
 *   int[ps]       parentDict             // parent nameKeys, ordered by tag id
 *   int[ps]       tagStart               // first value-index for each tag
 *   int[ps]       tagCount               // number of values under each tag
 *   int[ps]       tagStringDictSize      // local string-dictionary size per tag
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
      parentDictSize = getInt(payload, pos);
      pos += 4;
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
    /** Per-tag dict — parallel arrays: hash[i] → local dict id, plus byte[] for each id. */
    private long[][] tagHashes = new long[4][];
    private byte[][][] tagBytes = new byte[4][][];
    /**
     * Parallel to {@code tagBytes}: whether each dict entry's bytes are FSST-encoded rather than raw
     * UTF-8. Rides the dedup: two adds only fold into one entry when bytes AND flag agree, because raw
     * bytes that happen to equal some other value's encoded form are still a different value.
     */
    private boolean[][] tagCompressed = new boolean[4][];
    private int[] tagDictSize = new int[4];

    public Encoder() {
      tagIndex.defaultReturnValue(-1);
    }

    /**
     * Reset for reuse across pages. All internal arrays are retained at their current capacity; only
     * per-tag counts and dict byte references are cleared so previously-captured value byte arrays
     * become GC-eligible and the next page's adds start from an empty dictionary per tag. Zero
     * allocations.
     */
    public void reset() {
      final int prevTags = tagOrder.size();
      for (int t = 0; t < prevTags; t++) {
        final IntArrayList ids = tagDictIds[t];
        if (ids != null)
          ids.clear();
        final byte[][] bytes = tagBytes[t];
        if (bytes != null) {
          final int sz = tagDictSize[t];
          for (int i = 0; i < sz; i++)
            bytes[i] = null;
        }
        tagDictSize[t] = 0;
      }
      tagIndex.clear();
      tagIndex.defaultReturnValue(-1);
      tagOrder.clear();
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
     */
    public void addValue(final int parentNameKey, final byte[] value, final boolean compressed) {
      int tag = tagIndex.get(parentNameKey);
      if (tag < 0) {
        tag = tagOrder.size();
        tagIndex.put(parentNameKey, tag);
        tagOrder.add(parentNameKey);
        ensureTagSlot(tag);
        if (tagDictIds[tag] == null) {
          tagDictIds[tag] = new IntArrayList(16);
          tagHashes[tag] = new long[8];
          tagBytes[tag] = new byte[8][];
          tagCompressed[tag] = new boolean[8];
        }
        tagDictSize[tag] = 0;
      }
      // Dedup by 64-bit hash; equality is confirmed byte-by-byte below, so the hash only decides
      // which candidates get compared and never which values dedup. Nothing here is persisted —
      // dictionary ids are assigned in first-seen order and the table is reset per page.
      final long hash = VALUE_HASH.hashBytes(value);
      final long[] hashes = tagHashes[tag];
      final byte[][] bytes = tagBytes[tag];
      final boolean[] flags = tagCompressed[tag];
      final int n = tagDictSize[tag];
      int id = -1;
      for (int i = 0; i < n; i++) {
        if (hashes[i] == hash && flags[i] == compressed && Arrays.equals(bytes[i], value)) {
          id = i;
          break;
        }
      }
      if (id < 0) {
        if (n == hashes.length) {
          tagHashes[tag] = Arrays.copyOf(hashes, n * 2);
          tagBytes[tag] = Arrays.copyOf(bytes, n * 2);
          tagCompressed[tag] = Arrays.copyOf(flags, n * 2);
        }
        id = n;
        tagHashes[tag][n] = hash;
        tagBytes[tag][n] = value;
        tagCompressed[tag][n] = compressed;
        tagDictSize[tag] = n + 1;
      }
      tagDictIds[tag].add(id);
    }

    private void ensureTagSlot(final int tag) {
      if (tag < tagDictIds.length)
        return;
      final int grown = Math.max(tag + 1, tagDictIds.length * 2);
      tagDictIds = Arrays.copyOf(tagDictIds, grown);
      tagHashes = Arrays.copyOf(tagHashes, grown);
      tagBytes = Arrays.copyOf(tagBytes, grown);
      tagCompressed = Arrays.copyOf(tagCompressed, grown);
      tagDictSize = Arrays.copyOf(tagDictSize, grown);
    }

    /** Serialize to wire format, defaulting tagKind to {@link #TAG_KIND_NAME}. */
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
      final int ps = tagOrder.size();
      if (ps == 0) {
        return new byte[0];
      }
      int count = 0;
      int maxLocalDict = 0;
      for (int t = 0; t < ps; t++) {
        count += tagDictIds[t].size();
        if (tagDictSize[t] > maxLocalDict)
          maxLocalDict = tagDictSize[t];
      }
      final int bitWidth = Math.max(1, 32 - Integer.numberOfLeadingZeros(Math.max(1, maxLocalDict - 1)));
      // +1 byte for tagKind prefix.
      int headerSize = 1 + 1 + 4 + 1 + 4 + ps * 4 * 4;
      int dictBytesSize = 0;
      for (int t = 0; t < ps; t++) {
        final int sz = tagDictSize[t];
        dictBytesSize += sz * 4;
        for (int i = 0; i < sz; i++)
          dictBytesSize += tagBytes[t][i].length;
      }
      final int valueDictIdBytes = (count * bitWidth + 7) / 8;
      final byte[] out = new byte[headerSize + dictBytesSize + valueDictIdBytes];
      int pos = 0;
      out[pos++] = elementsStaged
          ? ENC_DICT_BITPACKED_ZM_ELEMENTS
          : ENC_DICT_BITPACKED_ZM;
      out[pos++] = tagKind;
      putInt(out, pos, count);
      pos += 4;
      out[pos++] = (byte) bitWidth;
      putInt(out, pos, ps);
      pos += 4;
      for (int t = 0; t < ps; t++) {
        putInt(out, pos, tagOrder.getInt(t));
        pos += 4;
      }
      int running = 0;
      for (int t = 0; t < ps; t++) {
        putInt(out, pos, running);
        pos += 4;
        running += tagDictIds[t].size();
      }
      for (int t = 0; t < ps; t++) {
        putInt(out, pos, tagDictIds[t].size());
        pos += 4;
      }
      for (int t = 0; t < ps; t++) {
        putInt(out, pos, tagDictSize[t]);
        pos += 4;
      }
      for (int t = 0; t < ps; t++) {
        final int sz = tagDictSize[t];
        for (int i = 0; i < sz; i++) {
          // Sign bit carries the per-entry FSST flag; consumers read Math.abs for the length.
          final int len = tagBytes[t][i].length;
          putInt(out, pos, tagCompressed[t][i]
              ? -len
              : len);
          pos += 4;
        }
        for (int i = 0; i < sz; i++) {
          final byte[] s = tagBytes[t][i];
          System.arraycopy(s, 0, out, pos, s.length);
          pos += s.length;
        }
      }
      int bitPos = 0;
      final int valueDictIdsBase = pos;
      for (int t = 0; t < ps; t++) {
        final IntArrayList ids = tagDictIds[t];
        final int sz = ids.size();
        final int[] idsArr = ids.elements();
        for (int i = 0; i < sz; i++) {
          bitPackAppend(out, valueDictIdsBase, bitPos, idsArr[i], bitWidth);
          bitPos += bitWidth;
        }
      }
      return out;
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
