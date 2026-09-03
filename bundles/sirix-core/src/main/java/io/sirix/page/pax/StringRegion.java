/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import io.sirix.node.LE;
import io.sirix.page.PageSectionDiag;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

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
   * Same column, framed for a page that holds a record's worth of each field rather than a column's.
   *
   * <p>
   * The dictionary form was designed for the case it is named after: eight departments repeated three
   * hundred times. A leaf of a wide record-shaped corpus is the opposite — a few rows of thirty-odd
   * fields, where a fat field's handful of values are all distinct. There the framing was the cost:
   * four fixed-width arrays per tag (16 bytes) and a four-byte length per dictionary entry, for
   * entries whose values are usually under 128 bytes and whose dictionary buys no deduplication at
   * all.
   *
   * <p>
   * Three changes, each chosen by measurement rather than by shape:
   * <ul>
   * <li>the per-tag arrays are varints, and {@code tagStart} and {@code count} are running sums of
   * what is already there rather than arrays of their own;</li>
   * <li>a tag's length table has a width of its own — one, two or four bytes, signed, so the sign
   * still carries the FSST flag and a length stays a single O(1) read;</li>
   * <li>a tag whose values are ALL DISTINCT takes the plain lane: its values are stored in slot
   * order and its rank IS its dictionary id, so it writes no dict ids at all. All-distinct is a
   * precondition, not a heuristic — rank and id must stay a bijection or an equality count over a
   * duplicated value would answer for one of its occurrences.</li>
   * </ul>
   *
   * <pre>
   * byte    encodingKind = 2
   * byte    tagKind
   * byte    flags                 // bit0 = array-element staging ran (the ..._ELEMENTS promise)
   * uvarint retainedTagCount
   * uvarint suppressedTagCount    // zero on every page without an oversized string
   * per retained tag:
   *   svarint parentDictDelta     // parentDict[i] - parentDict[i-1], first from 0
   *   uvarint tagCount            // tagStart is the running sum
   *   uvarint tagMeta             // bit0 plain lane, bits1-2 length width (0:1B 1:2B 2:4B),
   *                               // bits3.. dictionary size (absent, i.e. 0, on the plain lane,
   *                               // where it equals tagCount)
   * per suppressed tag:
   *   svarint tagDelta            // first from 0
   * per retained tag:
   *   length[dictSize] at the tag's width, signed, negative = FSST-encoded
   *   byte[] the entries' stored bytes
   * byte[]  valueDictIds          // DICT-lane tags only, packed at the derived width
   * </pre>
   *
   * <p>
   * {@code count}, {@code tagStart} and {@code valueBitWidth} are derived at parse. Everything a
   * reader sees on {@link Header} is what it saw before, so no consumer outside this class learns
   * that the framing changed.
   */
  public static final byte ENC_VARINT_FRAMED = 2;

  /** {@link #ENC_VARINT_FRAMED} flag bit: array-element staging ran for this page. */
  private static final int FLAG_ELEMENTS_STAGED = 1;

  /** Length-table widths a tag may choose, indexed by the two-bit code in {@code tagMeta}. */
  private static final int[] LENGTH_WIDTHS = {1, 2, 4};

  /**
   * The spare length-width code, repurposed to mark a tag whose dictionary holds GLOBAL IDS.
   *
   * <p>
   * {@link #LENGTH_WIDTHS} has three entries and the field is two bits wide, so code 3 was
   * unreachable. Using it costs no format version and no new field: a reader that does not know the
   * trie lane meets an unknown width code and throws, which is the correct behaviour for a page it
   * cannot read, rather than reading an id table as lengths.
   * </p>
   */
  private static final int GLOBAL_WIDTH_CODE = 3;

  /**
   * Arms the TEMPORAL lane: a tag whose whole dictionary is fixed timestamp text is stored as packed
   * numbers instead of text. OFF by default, so a load that does not ask for it writes exactly the
   * bytes it wrote before; it gates BEHAVIOUR on the WRITE side only, and the decoder always
   * understands what is on the page.
   */
  public static final String TEMPORAL_LANE_PROPERTY = "sirix.page.temporalLane";

  private static final boolean TEMPORAL_LANE_DEFAULT = Boolean.getBoolean(TEMPORAL_LANE_PROPERTY);

  private static volatile Boolean TEMPORAL_LANE_OVERRIDE = null;

  /** Test hook: force-enable/disable the temporal lane without restarting the JVM. */
  public static void setTemporalLaneEnabled(final boolean enabled) {
    TEMPORAL_LANE_OVERRIDE = enabled;
  }

  /** Test hook: clear the override and fall back to the system property. */
  public static void clearTemporalLaneOverride() {
    TEMPORAL_LANE_OVERRIDE = null;
  }

  /**
   * Whether the temporal lane is armed for WRITING. Reads NEVER consult it: a page that was written
   * with the lane stays readable when the switch goes off, which is what makes the switch safe to
   * flip on a database that already exists.
   */
  public static boolean temporalLaneEnabled() {
    final Boolean override = TEMPORAL_LANE_OVERRIDE;
    return override != null
        ? override
        : TEMPORAL_LANE_DEFAULT;
  }

  /**
   * Bits per global id, DERIVED from the dictionary's entry count rather than stored.
   *
   * <p>
   * Ids run {@code 1..entryCount}, so the count fixes the width exactly and a stored width could
   * only ever disagree with it. That is the same discipline {@code valueBitWidth} already follows —
   * derived from the dictionaries it addresses — and it is why the anchor pays for itself twice:
   * it makes resolution a function of the page AND it sizes the lane.
   * </p>
   *
   * <p>
   * Worth 20 % of what the trie lane leaves behind. Post-lever the id table is 88 % of the region's
   * remaining bytes, and URL's 18,342,018 distinct values need 25 bits rather than 32 — 111 MB
   * written across the three columns at 100M. The earlier "the FOR re-pack is dead at 2.3 %" reading
   * was true of the OLD lane and does not survive the lever that shrinks everything around it.
   * </p>
   */
  static int globalIdBits(final int dictionaryEntryCount) {
    return dictionaryEntryCount <= 0 ? 1 : 32 - Integer.numberOfLeadingZeros(dictionaryEntryCount);
  }

  /**
   * The global dictionary id stored for {@code dictId} under a tag whose dictionary is global.
   *
   * <p>
   * The supported route for a tag {@link Header#tagGlobal} marks. Callers pair it with a
   * {@link GlobalStringDictionaries} to reach the bytes, and should batch a tag's ids and resolve
   * them ASCENDING -- a dictionary point read is 417 ns at a random id and 75 ns at a sequential
   * one.
   * </p>
   */
  public static int globalIdAt(final MemorySegment payload, final Header h, final int tag, final int dictId) {
    if (!h.tagGlobal[tag]) {
      throw new IllegalStateException("string region tag " + tag + " does not store global ids");
    }
    final int n = h.tagStringDictSize[tag];
    if (dictId < 0 || dictId >= n) {
      throw new IndexOutOfBoundsException("dict id " + dictId + " outside tag " + tag + "'s " + n + " entries");
    }
    final int bits = globalIdBits(h.tagDictionaryEntryCount[tag]);
    final long mask = bits == 32 ? 0xFFFFFFFFL : ((1L << bits) - 1L);
    final long bitOff = (long) dictId * bits;
    final int byteOff = h.tagStringDictOffset[tag] + (int) (bitOff >>> 3);
    return (int) ((readUpToLongLE(payload, byteOff) >>> (int) (bitOff & 7L)) & mask);
  }

  /**
   * Write {@link #ENC_VARINT_FRAMED}. Off pins the encoder to the dictionary layout byte for byte,
   * the suppressed-tag list and its sign-bit marker included.
   */
  private static volatile Boolean VARINT_FRAMING_OVERRIDE = null;

  /** Test hook: force-enable/disable the varint framing without restarting the JVM. */
  public static void setPlainLaneEnabled(final boolean enabled) {
    VARINT_FRAMING_OVERRIDE = enabled;
  }

  /** Test hook: clear the override and fall back to the system property. */
  public static void clearPlainLaneOverride() {
    VARINT_FRAMING_OVERRIDE = null;
  }

  /** Kill switch {@code -Dsirix.page.stringRegion.plainLane=false}; on by default. */
  public static boolean plainLaneEnabled() {
    final Boolean override = VARINT_FRAMING_OVERRIDE;
    if (override != null) {
      return override;
    }
    return !"false".equalsIgnoreCase(System.getProperty("sirix.page.stringRegion.plainLane"));
  }

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
    /**
     * For each tag: offset (within payload) of the entries' bytes, i.e. just past its length table.
     * Derived, so a reader never has to know how wide that table's fields are.
     */
    public int[] tagStringBytesOffset;
    /** For each tag: bytes per length-table field — 1, 2 or 4. Four on the dictionary layout. */
    public byte[] tagLengthWidth;
    /**
     * For each tag: whether the tag took the plain lane, where the value's RANK within the tag is its
     * dictionary id and no id is stored. False for every tag of the dictionary layout.
     */
    public boolean[] tagPlainLane;

    /**
     * For each tag: whether its per-tag dictionary holds global dictionary IDS instead of value
     * bytes — the trie lane.
     *
     * <p>
     * Signalled on the wire by a NEGATIVE {@code tagStringDictSize}, whose magnitude is the entry
     * count as always. The sign bit is free here and the field is already read per tag, so the
     * layout stays self-describing: a reader learns a tag is global at the same moment it learns
     * how many entries it has, and {@code parentDictSize} already carries a sign bit for the
     * suppressed-tag list, so the idiom is the format's own.
     * </p>
     *
     * <p>
     * A global tag's entries are {@code int[|n|]} ids and NO bytes, so
     * {@link #tagStringBytesOffset} is the end of the id table and every byte-reading accessor must
     * check this first. They refuse rather than read: an id table read as a length table produces
     * plausible garbage, and this format has already paid once for a decoder that guessed.
     * </p>
     */
    public boolean[] tagGlobal;

    /**
     * For each tag: its dictionary is stored as PACKED TIMESTAMPS rather than text (the temporal
     * lane). Mutually exclusive with {@link #tagGlobal} — a tag is one lane or the other.
     *
     * <p>
     * Like a global tag, a temporal tag carries NO value bytes; unlike it, resolving one needs no
     * dictionary at all, because the stored number IS the value. Consumers must therefore treat it the
     * way they treat a global tag — never read bytes from the page for it — and obtain its values from
     * {@link StringRegion#temporalValueAt} or from the resolved table.
     * </p>
     */
    public boolean[] tagTemporal;

    /** For each temporal tag: the {@link TemporalTextCodec} form its values render under. */
    public int[] tagTemporalForm;

    /** For each temporal tag: the frame-of-reference base its packed deltas are relative to. */
    public long[] tagTemporalMin;

    /** For each temporal tag: the bit width of one packed delta ({@code 0} when all values agree). */
    public int[] tagTemporalBits;

    /**
     * For each global tag: the node key of the dictionary its ids were encoded against.
     *
     * <p>
     * The page NAMES its dictionary, exactly as an FSST page names its symbol table, and for the
     * same reason: resolution has to be a pure function of the page. It is not otherwise — a
     * dictionary is a function of (resource, generation), a rank rebuild REASSIGNS every id, and a
     * copy-on-write leaf written against one generation stays reachable after the next. Resolving
     * such a leaf against "the current dictionary" returns plausible wrong values for a page nobody
     * touched, which is the same shape as a verdict keyed without its cardinality.
     * </p>
     */
    public long[] tagDictionaryKey;

    /**
     * For each global tag: the dictionary's entry count when the page was encoded.
     *
     * <p>
     * What it proves and what it does not, stated because a resolver has to know the difference. A
     * rank-ordered dictionary only ever APPENDS in collation order, so ids {@code 1..n} keep their
     * values as it grows: a current count at least this one means every id this page stores is
     * still the value it was. A SMALLER count is a different dictionary under a reused key, and the
     * resolver must refuse rather than resolve. It does not prove the absence of a rebuild that
     * happens to land on the same key with at least as many entries — that case is closed by the
     * key changing on rebuild, and the count is the second line, not the first.
     * </p>
     */
    public int[] tagDictionaryEntryCount;

    /**
     * For each global tag: the byte width of one entry in its LENGTH lane (1, 2 or 4).
     *
     * <p>
     * The lane exists for a reason that has nothing to do with reading a value: the derived
     * value-elision plan reconstructs each elided slot's width from the region's stored string
     * LENGTH, so without lengths a converted page cannot be SERIALIZED at all — the plan-and-verify
     * pass refuses it. §6.4 of the design struck a length table as unnecessary and was right about
     * its original motivation and wrong about this one.
     * </p>
     *
     * <p>
     * It is affordable because a leaf is only about ten ClickBench rows: measured over 4,000 leaves,
     * a tag holds 9.7 values and 1.0-5.7 DISTINCT, maximum ever 10. So this is roughly six length
     * fields per tag per leaf — about 1 % of the ~1.1 KB of value bytes the lane removes from the
     * same leaf. A per-VALUE length table would have been a different proposition; this is per
     * DICTIONARY ENTRY.
     * </p>
     */
    public byte[] tagGlobalLengthWidth;

    /** For each global tag: payload offset of its length lane, which follows the packed id table. */
    public int[] tagGlobalLengthOffset;
    /**
     * For each tag: index of its first value within the packed dict-id lane. Equals
     * {@link #tagStart} whenever no tag took the plain lane, which is every page of the dictionary
     * layout; a plain tag contributes nothing to the lane and its entry is not meaningful.
     */
    public int[] tagIdLaneStart;
    /**
     * True when the dict-id lane is indexed by the absolute value index — no tag took the plain lane.
     * The per-value decoders take a branch-free path on it, which is every legacy page.
     */
    public boolean idLaneIsAbsolute;
    /** valueDictIds byte-region offset within the payload. */
    public int valueDictIdsOffset;
    /** valueDictIds bit-width (same as valueBitWidth; duplicated for convenience). */
    public int valueBitWidthEff;

    public Header parseInto(final MemorySegment payload) {
      if (payload.get(ValueLayout.JAVA_BYTE, 0L) == ENC_VARINT_FRAMED) {
        return parseVarintFramed(payload);
      }
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
      if (tagGlobal == null || tagGlobal.length < parentDictSize)
        tagGlobal = new boolean[Math.max(4, parentDictSize)];
      if (tagDictionaryKey == null || tagDictionaryKey.length < parentDictSize)
        tagDictionaryKey = new long[Math.max(4, parentDictSize)];
      if (tagDictionaryEntryCount == null || tagDictionaryEntryCount.length < parentDictSize)
        tagDictionaryEntryCount = new int[Math.max(4, parentDictSize)];
      if (tagGlobalLengthWidth == null || tagGlobalLengthWidth.length < parentDictSize)
        tagGlobalLengthWidth = new byte[Math.max(4, parentDictSize)];
      if (tagGlobalLengthOffset == null || tagGlobalLengthOffset.length < parentDictSize)
        tagGlobalLengthOffset = new int[Math.max(4, parentDictSize)];
      // The four-byte dictionary layout has no temporal lane -- it has nowhere to put the form, the
      // frame base or the width -- so the flags are allocated and CLEARED here rather than left null.
      // A reused Header crosses layouts, and a stale true would send a reader to render a tag that
      // holds bytes.
      if (tagTemporal == null || tagTemporal.length < parentDictSize)
        tagTemporal = new boolean[Math.max(4, parentDictSize)];
      if (tagTemporalForm == null || tagTemporalForm.length < parentDictSize)
        tagTemporalForm = new int[Math.max(4, parentDictSize)];
      if (tagTemporalMin == null || tagTemporalMin.length < parentDictSize)
        tagTemporalMin = new long[Math.max(4, parentDictSize)];
      if (tagTemporalBits == null || tagTemporalBits.length < parentDictSize)
        tagTemporalBits = new int[Math.max(4, parentDictSize)];
      Arrays.fill(tagTemporal, 0, parentDictSize, false);
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
      ensureDerivedTagArrays(parentDictSize);
      // Per-tag local dicts: lengths[...] + bytes[...]
      for (int t = 0; t < parentDictSize; t++) {
        tagStringDictOffset[t] = pos;
        final int signed = tagStringDictSize[t];
        // NEGATIVE size = the trie lane: |n| global dictionary ids, no value bytes at all.
        final boolean global = signed < 0;
        final int n = global ? -signed : signed;
        tagGlobal[t] = global;
        tagStringDictSize[t] = n;
        if (global) {
          // This layout has nowhere to put the anchor or the length lane -- both live in the varint
          // layout's per-tag header, which this one does not have -- so a tag marked global here
          // cannot be resolved by anyone. Zeroing the anchor is what makes that true rather than
          // merely likely: the Header is REUSED scratch, so leaving the fields alone would hand this
          // tag the PREVIOUS page's dictionary key and entry count, and resolution would then answer
          // from a dictionary this page never named. With them zeroed the resolver refuses, loudly.
          tagDictionaryKey[t] = 0L;
          tagDictionaryEntryCount[t] = 0;
          tagGlobalLengthWidth[t] = 0;
          tagGlobalLengthOffset[t] = pos + n * 4;
          tagStringBytesOffset[t] = pos + n * 4;
          tagLengthWidth[t] = 4;
          tagPlainLane[t] = false;
          tagIdLaneStart[t] = tagStart[t];
          pos += n * 4;
          continue;
        }
        int total = 0;
        for (int i = 0; i < n; i++)
          total += Math.abs(getInt(payload, pos + i * 4));
        // The dictionary layout is the varint one's special case: four-byte fields, no plain lane,
        // and a dict-id lane the absolute value index addresses. Normalising it here is what keeps
        // every accessor below layout-free.
        tagStringBytesOffset[t] = pos + n * 4;
        tagLengthWidth[t] = 4;
        tagPlainLane[t] = false;
        tagIdLaneStart[t] = tagStart[t];
        pos += n * 4 + total;
      }
      idLaneIsAbsolute = true;
      valueDictIdsOffset = pos;
      valueBitWidthEff = valueBitWidth & 0xFF;
      return this;
    }

    /**
     * Parse the {@link #ENC_VARINT_FRAMED} layout into exactly the fields the dictionary layout
     * fills, deriving {@code count}, {@code tagStart} and {@code valueBitWidth} rather than reading
     * them.
     */
    private Header parseVarintFramed(final MemorySegment payload) {
      long pos = 1;
      tagKind = payload.get(ValueLayout.JAVA_BYTE, pos++);
      final int flags = payload.get(ValueLayout.JAVA_BYTE, pos++) & 0xFF;
      encodingKind = (flags & FLAG_ELEMENTS_STAGED) != 0
          ? ENC_DICT_BITPACKED_ZM_ELEMENTS
          : ENC_DICT_BITPACKED_ZM;
      final long tags = VarInt.readUnsigned(payload, pos);
      pos += VarInt.sizeOfUnsigned(tags);
      final long suppressed = VarInt.readUnsigned(payload, pos);
      pos += VarInt.sizeOfUnsigned(suppressed);
      if (tags < 0L || tags > Integer.MAX_VALUE || suppressed < 0L || suppressed > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("string region declares " + tags + " tags and " + suppressed
            + " suppressed tags");
      }
      parentDictSize = (int) tags;
      suppressedTagCount = (int) suppressed;
      ensureTagArrays(parentDictSize);
      ensureDerivedTagArrays(parentDictSize);
      if (suppressedTags == null || suppressedTags.length < suppressedTagCount) {
        suppressedTags = new int[Math.max(4, suppressedTagCount)];
      }

      int previousTag = 0;
      int running = 0;
      int laneRunning = 0;
      int maxDictLane = 0;
      boolean anyPlain = false;
      for (int t = 0; t < parentDictSize; t++) {
        final long tagDelta = VarInt.readSigned(payload, pos);
        pos += VarInt.sizeOfSigned(tagDelta);
        previousTag = (int) (previousTag + tagDelta);
        parentDict[t] = previousTag;

        final long values = VarInt.readUnsigned(payload, pos);
        pos += VarInt.sizeOfUnsigned(values);
        if (values < 0L || values > Integer.MAX_VALUE - running) {
          throw new IllegalArgumentException("string region tag " + t + " declares " + values + " values");
        }
        tagCount[t] = (int) values;
        tagStart[t] = running;
        running += (int) values;

        final long meta = VarInt.readUnsigned(payload, pos);
        pos += VarInt.sizeOfUnsigned(meta);
        boolean plain = (meta & 1L) != 0L;
        final int widthCode = (int) ((meta >>> 1) & 3L);
        // Width code 3 is the TRIE LANE: the two-bit field has three widths {1,2,4} and a spare
        // code, so a global tag needs no new field and no format version. Its entries are int[n]
        // global dictionary ids and there are no value bytes; the width is nominally 4 because that
        // is what an id occupies, which keeps every offset computation below unchanged.
        final boolean global = widthCode == GLOBAL_WIDTH_CODE;
        if (!global && widthCode >= LENGTH_WIDTHS.length) {
          throw new IllegalArgumentException("string region tag " + t + " declares length width code " + widthCode);
        }
        // Width code 3 WITH the plain bit is the TEMPORAL lane. The pair was unreachable before it
        // -- "plain" claims a value's rank is its id, which is a statement about bytes a global tag
        // does not carry -- so no honest encoder ever wrote it, and taking it costs no new field.
        final boolean temporal = global && plain;
        tagTemporal[t] = temporal;
        if (temporal) {
          final long formField = VarInt.readUnsigned(payload, pos);
          pos += VarInt.sizeOfUnsigned(formField);
          final int form = (int) (formField & 3L);
          if (form != TemporalTextCodec.FORM_DATE && form != TemporalTextCodec.FORM_DATETIME) {
            throw new IllegalArgumentException("string region tag " + t + " declares temporal form " + form);
          }
          tagTemporalForm[t] = form;
          // The plain flag rides here because the meta word's bit 0 is what marks the tag temporal.
          plain = (formField & 4L) != 0L;
          final long min = VarInt.readSigned(payload, pos);
          pos += VarInt.sizeOfSigned(min);
          // The frame-of-reference base is bounded here for the same reason form and width are: a
          // base outside the range the codec can render reaches decode as a value it must refuse,
          // and a header is cheaper to refuse whole than one value at a time.
          if (!TemporalTextCodec.isRepresentable(min, form)) {
            throw new IllegalArgumentException("string region tag " + t + " declares temporal base " + min);
          }
          tagTemporalMin[t] = min;
          final long bits = VarInt.readUnsigned(payload, pos);
          pos += VarInt.sizeOfUnsigned(bits);
          if (bits < 0L || bits > 64L) {
            throw new IllegalArgumentException("string region tag " + t + " declares temporal width " + bits);
          }
          tagTemporalBits[t] = (int) bits;
        } else {
          tagTemporalForm[t] = 0;
          tagTemporalMin[t] = 0L;
          tagTemporalBits[t] = 0;
        }
        tagPlainLane[t] = plain;
        anyPlain |= plain;
        tagGlobal[t] = global && !temporal;
        tagLengthWidth[t] = (byte) (temporal
            ? 0
            : global
                ? 4
                : LENGTH_WIDTHS[widthCode]);
        final long dictSize = plain
            ? values
            : meta >>> 3;
        if (dictSize < 0L || dictSize > Integer.MAX_VALUE) {
          throw new IllegalArgumentException("string region tag " + t + " declares dictionary size " + dictSize);
        }
        tagStringDictSize[t] = (int) dictSize;
        if (global && !temporal) {
          // The anchor rides immediately after the meta word, varint-encoded: a dictionary's header
          // key and its entry count are both small numbers, so naming the dictionary costs about
          // four bytes per global tag against the hundreds it removes.
          final long dictionaryKey = VarInt.readUnsigned(payload, pos);
          pos += VarInt.sizeOfUnsigned(dictionaryKey);
          final long encodedEntries = VarInt.readUnsigned(payload, pos);
          pos += VarInt.sizeOfUnsigned(encodedEntries);
          if (dictionaryKey <= 0L || encodedEntries < dictSize) {
            throw new IllegalArgumentException("string region tag " + t + " names dictionary " + dictionaryKey
                + " with " + encodedEntries + " entries, which cannot hold its " + dictSize + " ids");
          }
          tagDictionaryKey[t] = dictionaryKey;
          tagDictionaryEntryCount[t] = (int) Math.min(encodedEntries, Integer.MAX_VALUE);
          // The length lane's width, in its own varint rather than in the meta word: the meta's
          // two-bit width field is fully spent -- three real widths plus the spare code that MARKS
          // this tag as global -- so there is nowhere in it left to say how wide a length is.
          final long lengthWidthCode = VarInt.readUnsigned(payload, pos);
          pos += VarInt.sizeOfUnsigned(lengthWidthCode);
          if (lengthWidthCode >= LENGTH_WIDTHS.length) {
            throw new IllegalArgumentException("string region tag " + t + " declares global length width code "
                + lengthWidthCode);
          }
          tagGlobalLengthWidth[t] = (byte) LENGTH_WIDTHS[(int) lengthWidthCode];
        } else {
          // A reused Header is scratch, and a stale anchor is worse than an absent one: it would let
          // a page that names no dictionary resolve against the PREVIOUS page's. Clearing costs two
          // stores on a path that runs once per tag.
          tagDictionaryKey[t] = 0L;
          tagDictionaryEntryCount[t] = 0;
          tagGlobalLengthWidth[t] = 0;
        }
        if (plain) {
          tagIdLaneStart[t] = -1;
        } else {
          tagIdLaneStart[t] = laneRunning;
          laneRunning += (int) values;
          if (dictSize > maxDictLane) {
            maxDictLane = (int) dictSize;
          }
        }
      }
      count = running;
      idLaneIsAbsolute = !anyPlain;

      int previousSuppressed = 0;
      for (int i = 0; i < suppressedTagCount; i++) {
        final long tagDelta = VarInt.readSigned(payload, pos);
        pos += VarInt.sizeOfSigned(tagDelta);
        previousSuppressed = (int) (previousSuppressed + tagDelta);
        suppressedTags[i] = previousSuppressed;
      }

      for (int t = 0; t < parentDictSize; t++) {
        tagStringDictOffset[t] = (int) pos;
        final int n = tagStringDictSize[t];
        final int width = tagLengthWidth[t];
        final long bytesStart = pos + (long) n * width;
        if (tagTemporal[t]) {
          // Packed deltas and nothing else. Sized from the DECLARED width, never by walking the body
          // as lengths -- the comment below records what that mistake once cost a global tag.
          final long packed = ((long) n * tagTemporalBits[t] + 7L) >>> 3;
          tagGlobalLengthOffset[t] = (int) pos;
          tagStringBytesOffset[t] = (int) (pos + packed);
          pos += packed;
          continue;
        }
        if (tagGlobal[t]) {
          // A PACKED id table, then a fixed-width LENGTH lane, and no value bytes at all. The ids
          // must never be walked as if they were lengths -- doing so reads them AS lengths and
          // advances pos by their sum, which is how a global tag once came out claiming a payload
          // larger than the page. The two lanes are sized from their own declared widths instead.
          final long packed = ((long) n * globalIdBits(tagDictionaryEntryCount[t]) + 7L) >>> 3;
          tagGlobalLengthOffset[t] = (int) (pos + packed);
          final long lengths = (long) n * tagGlobalLengthWidth[t];
          tagStringBytesOffset[t] = (int) (pos + packed + lengths);
          pos += packed + lengths;
          continue;
        }
        long total = 0;
        for (int i = 0; i < n; i++) {
          total += Math.abs(readLengthField(payload, (int) pos + i * width, width));
        }
        tagStringBytesOffset[t] = (int) bytesStart;
        pos = bytesStart + total;
      }
      // The width is a function of the dict-lane dictionaries, so it is derived rather than stored.
      // No dict-lane tag at all means no id bytes: every value is addressed by its rank.
      valueBitWidth = (byte) (maxDictLane == 0
          ? 0
          : Math.max(1, 32 - Integer.numberOfLeadingZeros(Math.max(1, maxDictLane - 1))));
      valueBitWidthEff = valueBitWidth & 0xFF;
      valueDictIdsOffset = (int) pos;
      if (pos + (((long) laneRunning * valueBitWidthEff + 7L) >>> 3) > payload.byteSize()) {
        throw new IllegalArgumentException("string region needs more bytes than the payload holds");
      }
      return this;
    }

    private void ensureTagArrays(final int size) {
      final int capacity = Math.max(4, size);
      if (parentDict == null || parentDict.length < size) {
        parentDict = new int[capacity];
      }
      if (tagStart == null || tagStart.length < size) {
        tagStart = new int[capacity];
      }
      if (tagCount == null || tagCount.length < size) {
        tagCount = new int[capacity];
      }
      if (tagGlobal == null || tagGlobal.length < size) {
        tagGlobal = new boolean[capacity];
      }
      if (tagTemporal == null || tagTemporal.length < size) {
        tagTemporal = new boolean[capacity];
      }
      if (tagTemporalForm == null || tagTemporalForm.length < size) {
        tagTemporalForm = new int[capacity];
      }
      if (tagTemporalMin == null || tagTemporalMin.length < size) {
        tagTemporalMin = new long[capacity];
      }
      if (tagTemporalBits == null || tagTemporalBits.length < size) {
        tagTemporalBits = new int[capacity];
      }
      if (tagDictionaryKey == null || tagDictionaryKey.length < size) {
        tagDictionaryKey = new long[capacity];
      }
      if (tagDictionaryEntryCount == null || tagDictionaryEntryCount.length < size) {
        tagDictionaryEntryCount = new int[capacity];
      }
      if (tagGlobalLengthWidth == null || tagGlobalLengthWidth.length < size) {
        tagGlobalLengthWidth = new byte[capacity];
      }
      if (tagGlobalLengthOffset == null || tagGlobalLengthOffset.length < size) {
        tagGlobalLengthOffset = new int[capacity];
      }
      if (tagStringDictSize == null || tagStringDictSize.length < size) {
        tagStringDictSize = new int[capacity];
      }
      if (tagStringDictOffset == null || tagStringDictOffset.length < size) {
        tagStringDictOffset = new int[capacity];
      }
    }

    private void ensureDerivedTagArrays(final int size) {
      final int capacity = Math.max(4, size);
      if (tagStringBytesOffset == null || tagStringBytesOffset.length < size) {
        tagStringBytesOffset = new int[capacity];
      }
      if (tagLengthWidth == null || tagLengthWidth.length < size) {
        tagLengthWidth = new byte[capacity];
      }
      if (tagPlainLane == null || tagPlainLane.length < size) {
        tagPlainLane = new boolean[capacity];
      }
      if (tagIdLaneStart == null || tagIdLaneStart.length < size) {
        tagIdLaneStart = new int[capacity];
      }
    }

    /**
     * The tag whose value range contains {@code index}, or {@code -1} when it is out of range.
     * Binary search over {@link #tagStart}, which is ascending by construction.
     */
    public int tagOfIndex(final int index) {
      if (index < 0 || index >= count || parentDictSize == 0) {
        return -1;
      }
      int lo = 0;
      int hi = parentDictSize - 1;
      while (lo < hi) {
        final int mid = (lo + hi + 1) >>> 1;
        if (tagStart[mid] <= index) {
          lo = mid;
        } else {
          hi = mid - 1;
        }
      }
      return lo;
    }
  }

  /** One signed length field, at whatever width its tag chose. */
  private static int readLengthField(final MemorySegment payload, final int offset, final int width) {
    return switch (width) {
      case 1 -> payload.get(ValueLayout.JAVA_BYTE, offset);
      case 2 -> payload.get(LE.SHORT, offset);
      default -> getInt(payload, offset);
    };
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

  /**
   * The lane index of an absolute value index, i.e. where its dict id sits in the packed lane.
   *
   * <p>
   * The two coincide unless some tag took the plain lane and therefore contributes no ids, which is
   * why {@link Header#idLaneIsAbsolute} short-circuits every legacy page.
   */
  private static int laneIndexOf(final Header h, final int tag, final int absoluteIndex) {
    return h.tagIdLaneStart[tag] + (absoluteIndex - h.tagStart[tag]);
  }

  /**
   * Resolve the tag a dict-id window belongs to, refusing one that crosses a tag boundary.
   *
   * <p>
   * A dictionary id is tag-local — id 3 under one tag and id 3 under another are different strings —
   * so a window spanning tags never had a meaning. Only reached on a page that carries a plain-lane
   * tag; the dictionary layout keeps its branch-free absolute indexing.
   */
  private static int tagOfWindow(final Header h, final int start, final int n) {
    final int tag = h.tagOfIndex(start);
    if (tag < 0 || start + n > h.tagStart[tag] + h.tagCount[tag]) {
      throw new IllegalArgumentException(
          "dict-id window [" + start + ", " + (start + n) + ") does not lie within one tag");
    }
    return tag;
  }

  /** Decode the dict-id for the {@code index}-th value (absolute, tag-grouped). */
  public static int decodeDictIdAt(final MemorySegment payload, final Header h, final int index) {
    if (!h.idLaneIsAbsolute) {
      final int tag = h.tagOfIndex(index);
      if (tag < 0) {
        throw new IndexOutOfBoundsException("value index " + index + " is outside the region's " + h.count + " values");
      }
      if (h.tagPlainLane[tag]) {
        // The plain lane stores no ids: the value's rank within its tag IS its dictionary id.
        return index - h.tagStart[tag];
      }
      return decodeDictIdAtLane(payload, h, laneIndexOf(h, tag, index));
    }
    return decodeDictIdAtLane(payload, h, index);
  }

  /** Decode the dict-id sitting at {@code index} of the packed lane. */
  private static int decodeDictIdAtLane(final MemorySegment payload, final Header h, final int index) {
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
    int laneStart = start;
    if (!h.idLaneIsAbsolute) {
      final int tag = tagOfWindow(h, start, n);
      if (h.tagPlainLane[tag]) {
        // Every value of a plain tag is its own dictionary entry, so each id in the window occurs
        // exactly once and the histogram is known without reading anything.
        final int firstRank = start - h.tagStart[tag];
        for (int i = 0; i < n; i++) {
          counts[firstRank + i]++;
        }
        return;
      }
      laneStart = laneIndexOf(h, tag, start);
    }
    countDictIdsInLane(payload, h, laneStart, n, counts);
  }

  /** {@link #countDictIds} over lane indices, the translation already done. */
  private static void countDictIdsInLane(final MemorySegment payload, final Header h, final int start, final int n,
      final long[] counts) {
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
    int laneStart = start;
    if (!h.idLaneIsAbsolute) {
      final int tag = tagOfWindow(h, start, n);
      if (h.tagPlainLane[tag]) {
        // The window's ids are exactly its ranks, so membership is read straight off the bitmap.
        final int firstRank = start - h.tagStart[tag];
        int matched = 0;
        for (int i = 0; i < n; i++) {
          final int id = firstRank + i;
          if (id < dictSize && (idSet[id >>> 6] & (1L << (id & 63))) != 0L) {
            matched++;
          }
        }
        return matched;
      }
      laneStart = laneIndexOf(h, tag, start);
    }
    return countDictIdSetInLane(payload, h, laneStart, n, idSet, dictSize);
  }

  /** {@link #countDictIdSet} over lane indices, the translation already done. */
  private static int countDictIdSetInLane(final MemorySegment payload, final Header h, final int start, final int n,
      final long[] idSet, final int dictSize) {
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
      final int id = decodeDictIdAtLane(payload, h, start + i);
      if (id < dictSize && (idSet[id >>> 6] & (1L << (id & 63))) != 0L) {
        matched++;
      }
    }
    return matched;
  }

  /**
   * Whether tag {@code tag}'s dictionary entries are PAGE BYTES — that is, whether
   * {@link #decodeStringOffset} can answer for it at all.
   *
   * <p>
   * The trie lane stores global ids and the temporal lane stores packed numbers. Neither has an
   * offset into the payload to hand back, so {@code decodeStringOffset} refuses both rather than
   * returning a plausible one. That refusal is the LAST line of defence, not the first: a caller
   * that walks a tag's dictionary reading bytes has to ask before it walks, and take whichever
   * slower route it already has for a tag it cannot read this way.
   * </p>
   *
   * <p>
   * Callers ask through this predicate rather than testing the lane flags themselves, so that a lane
   * added later closes every such loop by changing one place instead of by being remembered at each
   * of them.
   * </p>
   *
   * @param h a parsed header
   * @param tag index into the header's tag arrays
   * @return {@code true} when the tag's entries are readable as bytes on this page
   */
  public static boolean tagStoresInlineBytes(final Header h, final int tag) {
    return !h.tagGlobal[tag] && !h.tagTemporal[tag];
  }

  /**
   * Decode the string bytes for the given dict-id within a tag. Returns offset and length in the
   * payload's per-tag local dictionary, avoiding a copy on the group-by hot path.
   *
   * <p>
   * Refuses a tag whose entries are not bytes; callers on a dictionary-walking fast path test
   * {@link #tagStoresInlineBytes} first and decline to their slower route instead.
   * </p>
   */
  public static int decodeStringOffset(final MemorySegment payload, final Header h, final int tag, final int dictId) {
    if (h.tagGlobal[tag]) {
      // Ids, not bytes. Throwing beats returning an offset into an id table: the caller would read
      // four bytes of some id as a string and get a plausible answer, which is the one failure this
      // format cannot afford and cannot detect afterwards.
      throw new IllegalStateException("string region tag " + tag + " stores global ids; resolve via the dictionary");
    }
    if (h.tagTemporal[tag]) {
      // Packed numbers, not bytes -- the same refusal for the same reason. There is no offset to
      // return: the value is rendered by TemporalTextCodec, never read from the page.
      throw new IllegalStateException("string region tag " + tag
          + " stores packed timestamps; render via StringRegion.temporalValueAt");
    }
    final int dictStart = h.tagStringDictOffset[tag];
    final int width = h.tagLengthWidth[tag];
    // lengths[0..n), then bytes — walk lengths to sum offsets.
    int off = h.tagStringBytesOffset[tag];
    for (int i = 0; i < dictId; i++) {
      off += Math.abs(readLengthField(payload, dictStart + i * width, width));
    }
    return off;
  }

  public static int decodeStringLength(final MemorySegment payload, final Header h, final int tag, final int dictId) {
    if (h.tagTemporal[tag]) {
      // The length is a CONSTANT of the form, which is why a temporal tag stores no length lane at
      // all -- and the derived value-elision plan, which reconstructs an elided slot's width from
      // exactly this, is served for free.
      return TemporalTextCodec.lengthOf(h.tagTemporalForm[tag]);
    }
    if (h.tagGlobal[tag]) {
      // A LENGTH a global tag can answer, unlike an offset. This is the whole reason the lane exists:
      // the derived value-elision plan reconstructs an elided slot's width from the region's stored
      // length, so a global tag that could not answer this could not be serialized at all.
      //
      // The four-byte layout cannot carry the lane (its per-tag header has nowhere to put a width),
      // so a global tag parsed from it reports width 0 and is refused here rather than reading the
      // id table as lengths.
      final int globalWidth = h.tagGlobalLengthWidth[tag];
      if (globalWidth == 0) {
        throw new IllegalStateException("string region tag " + tag
            + " stores global ids in a layout that carries no length lane; its lengths are unreadable");
      }
      return Math.abs(readLengthField(payload, h.tagGlobalLengthOffset[tag] + dictId * globalWidth, globalWidth));
    }
    final int width = h.tagLengthWidth[tag];
    return Math.abs(readLengthField(payload, h.tagStringDictOffset[tag] + dictId * width, width));
  }

  /**
   * Whether the dict entry's bytes are FSST-encoded (against the owning page's symbol table) rather
   * than raw UTF-8. Carried as the sign of the entry's length; raw entries — the only kind that
   * existed before per-value encoding — are non-negative, so the flag costs no bytes.
   */
  public static boolean isEntryCompressed(final MemorySegment payload, final Header h, final int tag,
      final int dictId) {
    if (h.tagGlobal[tag] || h.tagTemporal[tag]) {
      // Never, and by construction rather than by observation: the encoder refuses to convert a tag
      // with any FSST-encoded entry, because a stored form is not a value and cannot be looked up in
      // a dictionary -- and resolveTemporal refuses one for the same reason. Reading the sign bit of
      // an ID or of a packed delta here would answer at random.
      return false;
    }
    final int width = h.tagLengthWidth[tag];
    return readLengthField(payload, h.tagStringDictOffset[tag] + dictId * width, width) < 0;
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
    if (h.tagGlobal[tag]) {
      // A global tag carries ids, not bytes. Answering UNDECIDABLE routes the caller to the
      // dictionary rather than letting it read an id table as a length table -- which would not
      // fail, it would match plausible-looking garbage, and this format has already paid once for a
      // decoder that guessed instead of declining.
      return DICT_ID_UNDECIDABLE;
    }
    if (h.tagTemporal[tag]) {
      // A temporal tag answers EXACTLY, and does not decline: equality of timestamps is equality of
      // the numbers they encode to, so the literal is encoded once under the tag's form and compared
      // against the packed lane. A literal the codec refuses cannot equal any entry -- every entry
      // encoded, or the tag would not be on this lane -- so ABSENT is not a guess but a proof.
      final int form = h.tagTemporalForm[tag];
      if (literal.length != TemporalTextCodec.lengthOf(form)) {
        return DICT_ID_ABSENT;
      }
      final long wanted = TemporalTextCodec.encode(literal, 0, literal.length, form);
      if (wanted == TemporalTextCodec.REFUSED) {
        return DICT_ID_ABSENT;
      }
      final int bits = h.tagTemporalBits[tag];
      final long base = h.tagTemporalMin[tag];
      final int entries = h.tagStringDictSize[tag];
      for (int i = 0; i < entries; i++) {
        if (base + bitUnpackLong(payload, h.tagStringDictOffset[tag], (long) i * bits, bits) == wanted) {
          return i;
        }
      }
      return DICT_ID_ABSENT;
    }
    final int dictStart = h.tagStringDictOffset[tag];
    final int n = h.tagStringDictSize[tag];
    final int width = h.tagLengthWidth[tag];
    int off = h.tagStringBytesOffset[tag];
    for (int i = 0; i < n; i++) {
      final int lenField = readLengthField(payload, dictStart + i * width, width);
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
    int laneStart = start;
    if (!h.idLaneIsAbsolute) {
      final int tag = tagOfWindow(h, start, n);
      if (h.tagPlainLane[tag]) {
        // Rank IS the id and the tag's values are all distinct, so the id occurs at most once.
        final int rank = dictId - (start - h.tagStart[tag]);
        return rank >= 0 && rank < n ? 1 : 0;
      }
      laneStart = laneIndexOf(h, tag, start);
    }
    return countDictIdInLane(payload, h, laneStart, n, dictId);
  }

  /** {@link #countDictId} over lane indices, the translation already done. */
  private static int countDictIdInLane(final MemorySegment payload, final Header h, final int start, final int n,
      final int dictId) {
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
    int laneStart = start;
    if (!h.idLaneIsAbsolute) {
      final int tag = tagOfWindow(h, start, n);
      if (h.tagPlainLane[tag]) {
        final int words = (n + 63) >>> 6;
        Arrays.fill(rowBits, 0, words, 0L);
        final int rank = dictId - (start - h.tagStart[tag]);
        if (rank < 0 || rank >= n) {
          return 0;
        }
        rowBits[rank >>> 6] |= 1L << (rank & 63);
        return 1;
      }
      laneStart = laneIndexOf(h, tag, start);
    }
    return selectDictIdIntoLane(payload, h, laneStart, n, dictId, rowBits);
  }

  /** {@link #selectDictIdInto} over lane indices, the translation already done. */
  private static int selectDictIdIntoLane(final MemorySegment payload, final Header h, final int start, final int n,
      final int dictId, final long[] rowBits) {
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
    int laneStart = start;
    if (!h.idLaneIsAbsolute) {
      final int tag = tagOfWindow(h, start, n);
      if (h.tagPlainLane[tag]) {
        final int rank = dictId - (start - h.tagStart[tag]);
        if (rank < 0 || rank >= n) {
          return 0;
        }
        return (int) ((liveBits[rank >>> 6] >>> (rank & 63)) & 1L);
      }
      laneStart = laneIndexOf(h, tag, start);
    }
    return countDictIdMaskedInLane(payload, h, laneStart, n, dictId, liveBits);
  }

  /** {@link #countDictIdMasked} over lane indices, the translation already done. */
  private static int countDictIdMaskedInLane(final MemorySegment payload, final Header h, final int start, final int n,
      final int dictId, final long[] liveBits) {
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
    /** Reusable per-tag lane decision for the varint framing; never escapes the encoder. */
    private boolean[] plainLane = new boolean[4];
    /** Reusable per-tag length-table widths for the varint framing; never escapes the encoder. */
    private byte[] lengthWidths = new byte[4];

    /**
     * Resolver for tags whose values live in a resource-wide dictionary, or {@code null}.
     *
     * <p>
     * Supplied by whoever holds the writing context; the encoder itself cannot reach a dictionary
     * for the same reason the decoder cannot. When absent every tag encodes its bytes, which is the
     * behaviour that existed before the lane.
     * </p>
     */
    private @Nullable GlobalStringDictionaries dictionaries;

    /** Reusable per-retained-tag decision: this tag writes ids, not bytes. */
    private boolean[] globalTag = new boolean[4];

    /** Reusable per-retained-tag resolved ids, so a tag is resolved once and written once. */
    private int[][] globalIds = new int[4][];

    /** Per retained tag: the byte width its global LENGTH lane is written at. */
    private byte[] globalLengthWidth = new byte[4];

    /**
     * Per retained tag: the dictionary anchor SNAPSHOTTED once, at resolution.
     *
     * <p>
     * Not a convenience. The entry count fixes three things that must agree exactly — the bit width
     * the ids are packed at, the width the parser DERIVES from the count it reads, and the count
     * written into the header — and they are computed in three different passes over the tag. A live
     * dictionary is a moving target between them: {@code buildRegionTable} runs on the flush lane's
     * parallel threads while the load is still interning, so a count that grew mid-encode would pack
     * ids at one width and declare another. The parser would then read every id of that tag shifted,
     * with no failure anywhere — the exact silent corruption the anchor exists to prevent.
     * </p>
     */
    private int[] globalEntryCount = new int[4];

    /** Per retained tag: the dictionary key snapshotted beside {@link #globalEntryCount}. */
    private long[] globalDictionaryKey = new long[4];

    /** Reusable buffer for one entry's bytes while it is probed; never escapes the encoder. */
    private byte[] globalProbeScratch = new byte[256];
    /** Per-tag: the tag's whole dictionary encodes as a fixed timestamp text (the temporal lane). */
    private boolean[] temporalTag = new boolean[4];
    /** Parallel to {@link #temporalTag}: {@link TemporalTextCodec}'s form for the tag. */
    private int[] temporalForm = new int[4];
    /** Parallel to {@link #temporalTag}: the encoded values, in dictionary-entry order. */
    private long[][] temporalValues = new long[4][];
    /** Parallel to {@link #temporalTag}: the frame-of-reference base (the tag's smallest value). */
    private long[] temporalMin = new long[4];
    /** Parallel to {@link #temporalTag}: bit width of {@code value - min}. */
    private int[] temporalBits = new int[4];
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
     * Install the dictionary resolver, or {@code null} to encode every tag as bytes.
     *
     * <p>
     * A tag converts ALL OF ITS ENTRIES or none. A single value the dictionary does not hold makes
     * the whole tag fall back to bytes rather than mint an id: the dictionary is complete before the
     * load starts, so a miss means the writer and the pre-pass disagree about the value set, and an
     * id no reader can resolve is a worse outcome than the bytes it replaced.
     * </p>
     */
    public void setDictionaries(final @Nullable GlobalStringDictionaries resolver) {
      this.dictionaries = resolver;
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
      if (plainLaneEnabled()) {
        return encodeVarintFramed(tagKind, elementsStaged, retained, ps, count);
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
      final int dictBase = pos;
      for (int r = 0; r < ps; r++) {
        final int t = retained[r];
        final int sz = tagDictSize[t];
        final int tagLengthBase = pos;
        for (int i = 0; i < sz; i++) {
          // Sign bit carries the per-entry FSST flag; consumers read Math.abs for the length.
          final int len = tagLengths[t][i];
          putInt(output, pos, tagCompressed[t][i]
              ? -len
              : len);
          pos += 4;
        }
        final int tagValueBase = pos;
        // The four-byte layout has no trie lane: its sizing pass never consults a resolver, so it
        // must never consult the decision either. Reading globalTag here would read whatever a
        // previous varint encode on this reused Encoder left behind.
        for (int i = 0; i < sz; i++) {
          final int len = tagLengths[t][i];
          tagStores[t][i].copyTo(tagOffsets[t][i], output, pos, len);
          pos += len;
        }
        if (PageSectionDiag.STRING_TAG_DIAG) {
          // [DIAG] Measured write positions, not a formula — an attribution derived from the layout
          // twice can disagree with itself; this one cannot.
          final int values = tagDictIds[t].size();
          PageSectionDiag.recordStringRegionTag(tagKind, tagOrder.getInt(t), values, sz,
              tagValueBase - tagLengthBase, pos - tagValueBase, (long) values * bitWidth,
              (long) values * forWidth(sz));
        }
      }
      if (PageSectionDiag.STRING_TAG_DIAG) {
        PageSectionDiag.recordStringRegionCensus(dictBase, valueDictIdBytes, totalLength);
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

    /**
     * Serialize the {@link #ENC_VARINT_FRAMED} layout.
     *
     * <p>
     * The lane decision is per tag and is not a heuristic: a tag goes plain exactly when its
     * dictionary has one entry per value, i.e. its values are all distinct. Then dropping the ids
     * costs nothing to look up — rank IS the id — and saves every one of them. A tag with even one
     * repeat keeps the dictionary, because rank and id would no longer be a bijection and an
     * equality count would answer for one occurrence of a value instead of all of them.
     *
     * @param retained tag ids that survive suppression, in write order
     * @param ps number of retained tags
     * @param count total values across retained tags
     */
    private int encodeVarintFramed(final byte tagKind, final boolean elementsStaged, final int[] retained,
        final int ps, final int count) {
      // Per-tag decisions first: they size the header, the length tables and the id lane.
      final boolean[] plain = plainScratch(ps);
      final byte[] widths = lengthWidthScratch(ps);
      int laneValues = 0;
      int maxLaneDict = 0;
      long dictBytesSize = 0L;
      long headerSize = 1L + 1L + 1L + VarInt.sizeOfUnsigned(ps) + VarInt.sizeOfUnsigned(suppressedTagCount);
      int previousTag = 0;
      ensureGlobalScratch(ps);
      for (int r = 0; r < ps; r++) {
        final int t = retained[r];
        final int values = tagDictIds[t].size();
        final int sz = tagDictSize[t];
        final boolean tagIsPlain = sz == values;
        plain[r] = tagIsPlain;
        // The trie lane, decided per tag and resolved ONCE: the write pass reuses these ids rather
        // than probing the dictionary a second time. A plain tag is excluded because its lane is its
        // rank, which only means anything about bytes it would no longer store.
        globalTag[r] = !tagIsPlain && resolveGlobalIds(t, r, sz);
        // The temporal lane is considered only for a tag the trie lane did not take, and it is
        // considered for a PLAIN tag too: a timestamp column's values are usually all distinct, which
        // is exactly the plain case, and packing them is worth far more than dropping their ids.
        temporalTag[r] = !globalTag[r] && resolveTemporal(t, r, sz);
        if (temporalTag[r]) {
          widths[r] = 0;
          final int tagValueTemporal = tagOrder.getInt(t);
          headerSize += VarInt.sizeOfSigned((long) tagValueTemporal - previousTag);
          previousTag = tagValueTemporal;
          headerSize += VarInt.sizeOfUnsigned(values);
          headerSize += VarInt.sizeOfUnsigned(temporalTagMeta(sz));
          headerSize += VarInt.sizeOfUnsigned(temporalFormField(temporalForm[r], tagIsPlain));
          headerSize += VarInt.sizeOfSigned(temporalMin[r]);
          headerSize += VarInt.sizeOfUnsigned(temporalBits[r]);
          dictBytesSize += ((long) sz * temporalBits[r] + 7L) >>> 3;
          // A temporal tag keeps its id lane exactly like any other dictionary tag: the lane maps a
          // ROW to an entry, and nothing about how the entry is stored changes that. A plain temporal
          // tag keeps the plain lane's absence of ids for the same unchanged reason.
          if (!tagIsPlain) {
            laneValues += values;
            if (sz > maxLaneDict) {
              maxLaneDict = sz;
            }
          }
          continue;
        }
        if (globalTag[r]) {
          widths[r] = 4;
          dictBytesSize += ((long) sz * globalIdBits(globalEntryCount[r]) + 7L) >>> 3;
          // The LENGTH lane, at the narrowest width this tag's own values need. About six entries per
          // tag per leaf, so a handful of bytes against the ~1.1 KB of value bytes the lane removes
          // from the same leaf -- and without it the page cannot be SERIALIZED at all, because the
          // derived elision plan reconstructs its slot widths from exactly these lengths.
          final int lengthWidthCode = globalLengthWidthCode(t, sz);
          globalLengthWidth[r] = (byte) LENGTH_WIDTHS[lengthWidthCode];
          dictBytesSize += (long) sz * globalLengthWidth[r];
          final int tagValueGlobal = tagOrder.getInt(t);
          headerSize += VarInt.sizeOfSigned((long) tagValueGlobal - previousTag);
          previousTag = tagValueGlobal;
          headerSize += VarInt.sizeOfUnsigned(values);
          headerSize += VarInt.sizeOfUnsigned(globalTagMeta(sz));
          headerSize += VarInt.sizeOfUnsigned(globalDictionaryKey[r]);
          headerSize += VarInt.sizeOfUnsigned(globalEntryCount[r]);
          headerSize += VarInt.sizeOfUnsigned(lengthWidthCode);
          laneValues += values;
          if (sz > maxLaneDict) {
            maxLaneDict = sz;
          }
          continue;
        }
        int width = 1;
        for (int i = 0; i < sz; i++) {
          final int field = tagCompressed[t][i]
              ? -tagLengths[t][i]
              : tagLengths[t][i];
          if (field < Byte.MIN_VALUE || field > Byte.MAX_VALUE) {
            width = Math.max(width, field < Short.MIN_VALUE || field > Short.MAX_VALUE
                ? 4
                : 2);
          }
          dictBytesSize += tagLengths[t][i];
        }
        widths[r] = (byte) width;
        dictBytesSize += (long) sz * width;

        final int tagValue = tagOrder.getInt(t);
        headerSize += VarInt.sizeOfSigned((long) tagValue - previousTag);
        previousTag = tagValue;
        headerSize += VarInt.sizeOfUnsigned(values);
        headerSize += VarInt.sizeOfUnsigned(tagMeta(tagIsPlain, width, sz));
        if (!tagIsPlain) {
          laneValues += values;
          if (sz > maxLaneDict) {
            maxLaneDict = sz;
          }
        }
      }
      int previousSuppressed = 0;
      for (int t = 0; t < tagOrder.size(); t++) {
        if (tagSuppressed[t]) {
          final int tagValue = tagOrder.getInt(t);
          headerSize += VarInt.sizeOfSigned((long) tagValue - previousSuppressed);
          previousSuppressed = tagValue;
        }
      }
      final int bitWidth = maxLaneDict == 0
          ? 0
          : Math.max(1, 32 - Integer.numberOfLeadingZeros(Math.max(1, maxLaneDict - 1)));
      final long laneBytes = ((long) laneValues * bitWidth + 7L) >>> 3;
      final int totalLength = checkedEncodedLength(headerSize + dictBytesSize + laneBytes);
      ensureOutputCapacity(totalLength);

      int pos = 0;
      output[pos++] = ENC_VARINT_FRAMED;
      output[pos++] = tagKind;
      output[pos++] = (byte) (elementsStaged
          ? FLAG_ELEMENTS_STAGED
          : 0);
      pos = VarInt.writeUnsigned(output, pos, ps);
      pos = VarInt.writeUnsigned(output, pos, suppressedTagCount);
      previousTag = 0;
      for (int r = 0; r < ps; r++) {
        final int t = retained[r];
        final int tagValue = tagOrder.getInt(t);
        pos = VarInt.writeSigned(output, pos, (long) tagValue - previousTag);
        previousTag = tagValue;
        pos = VarInt.writeUnsigned(output, pos, tagDictIds[t].size());
        pos = VarInt.writeUnsigned(output, pos, temporalTag[r]
            ? temporalTagMeta(tagDictSize[t])
            : globalTag[r]
                ? globalTagMeta(tagDictSize[t])
                : tagMeta(plain[r], widths[r], tagDictSize[t]));
        if (temporalTag[r]) {
          pos = VarInt.writeUnsigned(output, pos, temporalFormField(temporalForm[r], plain[r]));
          pos = VarInt.writeSigned(output, pos, temporalMin[r]);
          pos = VarInt.writeUnsigned(output, pos, temporalBits[r]);
        }
        if (globalTag[r]) {
          // The page names the dictionary it was encoded against and how large it was, so that
          // resolution is a function of the page rather than of whatever is current.
          pos = VarInt.writeUnsigned(output, pos, globalDictionaryKey[r]);
          pos = VarInt.writeUnsigned(output, pos, globalEntryCount[r]);
          pos = VarInt.writeUnsigned(output, pos, widthCodeOf(globalLengthWidth[r]));
        }
      }
      previousSuppressed = 0;
      for (int t = 0; t < tagOrder.size(); t++) {
        if (tagSuppressed[t]) {
          final int tagValue = tagOrder.getInt(t);
          pos = VarInt.writeSigned(output, pos, (long) tagValue - previousSuppressed);
          previousSuppressed = tagValue;
        }
      }
      final int dictBase = pos;
      for (int r = 0; r < ps; r++) {
        final int t = retained[r];
        final int sz = tagDictSize[t];
        final int width = widths[r];
        final int tagLengthBase = pos;
        if (temporalTag[r]) {
          // Packed (value - min) and NOTHING else: no length table, because the length is a constant
          // of the form, and no value bytes, because the number IS the value. That absence is the
          // lever. bitPackAppend ORs into the destination, so the reused buffer must start zeroed.
          final int bits = temporalBits[r];
          final int packedBytes = (int) (((long) sz * bits + 7L) >>> 3);
          Arrays.fill(output, pos, pos + packedBytes, (byte) 0);
          if (bits > 0) {
            final long[] vals = temporalValues[r];
            final long base = temporalMin[r];
            for (int i = 0; i < sz; i++) {
              bitPackAppendLong(output, pos, (long) i * bits, vals[i] - base, bits);
            }
          }
          pos += packedBytes;
          if (PageSectionDiag.STRING_TAG_DIAG) {
            final int valuesForDiag = tagDictIds[t].size();
            PageSectionDiag.recordStringRegionTag(tagKind, tagOrder.getInt(t), valuesForDiag, sz,
                packedBytes, 0, plain[r]
                    ? 0L
                    : (long) valuesForDiag * bitWidth, plain[r]
                        ? 0L
                        : (long) valuesForDiag * forWidth(sz));
          }
          continue;
        }
        if (globalTag[r]) {
          // Ids only, PACKED at the width the dictionary's size implies. No length table and no
          // bytes: that absence is the lever, and the packing is 20 % of what the lever leaves.
          final int[] ids = globalIds[r];
          final int bits = globalIdBits(globalEntryCount[r]);
          final int packedBytes = (int) (((long) sz * bits + 7L) >>> 3);
          // bitPackAppend ORs into the destination, so the reused buffer must start zeroed here.
          Arrays.fill(output, pos, pos + packedBytes, (byte) 0);
          for (int i = 0; i < sz; i++) {
            bitPackAppend(output, pos, i * bits, ids[i], bits);
          }
          pos += packedBytes;
          // Then the lengths, in the same entry order as the ids. Written POSITIVE: a global tag
          // carries no FSST-encoded entry (resolveGlobalIds refuses one), so the sign bit that
          // carries that flag everywhere else has nothing to say here.
          final int globalLength = globalLengthWidth[r];
          for (int i = 0; i < sz; i++) {
            StringRegion.writeLengthField(output, pos, globalLength, tagLengths[t][i]);
            pos += globalLength;
          }
        } else {
          for (int i = 0; i < sz; i++) {
            // The sign carries the FSST flag at every width, exactly as the four-byte field did.
            StringRegion.writeLengthField(output, pos, width, tagCompressed[t][i]
                ? -tagLengths[t][i]
                : tagLengths[t][i]);
            pos += width;
          }
        }
        final int tagValueBase = pos;
        if (!globalTag[r]) {
          for (int i = 0; i < sz; i++) {
            final int len = tagLengths[t][i];
            tagStores[t][i].copyTo(tagOffsets[t][i], output, pos, len);
            pos += len;
          }
        }
        if (PageSectionDiag.STRING_TAG_DIAG) {
          // A PLAIN tag stores no ids at all — its values ARE its dictionary — so it contributes
          // nothing to either lane figure. Counting it would invent a lane that is not there.
          final int values = tagDictIds[t].size();
          PageSectionDiag.recordStringRegionTag(tagKind, tagOrder.getInt(t), values, sz,
              tagValueBase - tagLengthBase, pos - tagValueBase, plain[r]
                  ? 0L
                  : (long) values * bitWidth, plain[r]
                      ? 0L
                      : (long) values * forWidth(sz));
        }
      }
      if (PageSectionDiag.STRING_TAG_DIAG) {
        PageSectionDiag.recordStringRegionCensus(dictBase, laneBytes, totalLength);
      }
      final int laneBase = pos;
      // bitPackAppend ORs lanes into the destination, so the reused buffer's high bits from the
      // preceding page have to go before the first OR.
      Arrays.fill(output, laneBase, totalLength, (byte) 0);
      int bitPos = 0;
      for (int r = 0; r < ps; r++) {
        if (plain[r]) {
          continue;
        }
        final IntArrayList ids = tagDictIds[retained[r]];
        final int sz = ids.size();
        final int[] idsArr = ids.elements();
        for (int i = 0; i < sz; i++) {
          bitPackAppend(output, laneBase, bitPos, idsArr[i], bitWidth);
          bitPos += bitWidth;
        }
      }
      if (laneBase + (int) laneBytes != totalLength) {
        throw new IllegalStateException(
            "string region size mismatch: expected=" + totalLength + " written=" + (laneBase + laneBytes));
      }
      if (count <= 0) {
        throw new IllegalStateException("string region encoded " + count + " values");
      }
      encodedLength = totalLength;
      return totalLength;
    }

    /**
     * Bits one id needs at a tag's OWN dictionary size — the width a per-tag (FOR-packed) lane would
     * use, against the page-wide width the shared lane is forced to.
     *
     * @param dictSize the tag's local dictionary size
     * @return bits per id, at least one
     */
    private static int forWidth(final int dictSize) {
      return Math.max(1, 32 - Integer.numberOfLeadingZeros(Math.max(1, dictSize - 1)));
    }

    /** {@code tagMeta}: plain flag, length-width code, and the dictionary size of a dict-lane tag. */
    /**
     * Meta word for a tag that stores global ids: width code {@link #GLOBAL_WIDTH_CODE}, never plain.
     */
    private static long globalTagMeta(final int dictSize) {
      return ((long) dictSize << 3) | ((long) GLOBAL_WIDTH_CODE << 1);
    }

    /**
     * Meta word for a TEMPORAL tag: width code {@link #GLOBAL_WIDTH_CODE} AND the plain bit.
     *
     * <p>
     * That combination was unreachable and the parser refused it as malformed, exactly as width code
     * 3 itself was unreachable before the trie lane took it. Taking it costs no new field and no
     * format version: "plain" means a value's RANK is its id, which is a claim about bytes a global
     * tag does not carry, so no honest encoder could ever have written the pair.
     * </p>
     */
    private static long temporalTagMeta(final int dictSize) {
      return ((long) dictSize << 3) | ((long) GLOBAL_WIDTH_CODE << 1) | 1L;
    }

    /**
     * The temporal tag's form field: the {@link TemporalTextCodec} form in the low two bits and the
     * PLAIN-lane flag above it.
     *
     * <p>
     * The plain flag has to live here because the meta word's bit 0 -- where every other tag carries
     * it -- is what MARKS this tag temporal. A temporal tag still needs the flag: its values are
     * usually all distinct, which is exactly the plain case, and a plain tag stores no id lane at all.
     * Losing the distinction would cost an id lane per tag per leaf, or invent one the writer never
     * wrote.
     * </p>
     */
    private static long temporalFormField(final int form, final boolean plain) {
      return (long) form | (plain
          ? 4L
          : 0L);
    }

    /**
     * Resolve a tag's whole dictionary to global ids, or report that it cannot be.
     *
     * <p>
     * ALL OR NOTHING: one absent value, or one FSST-encoded entry whose bytes are not the value,
     * and the tag keeps its bytes. Resolving entry by entry and converting the ones that succeed
     * would put a tag on disk that is half ids and half bytes with nothing on the wire to say which
     * is which.
     * </p>
     */
    /**
     * Whether this tag's WHOLE dictionary is fixed timestamp text, and if so its encoded values.
     *
     * <p>
     * ALL OR NOTHING, for {@link #resolveGlobalIds}'s own reason: a tag half packed and half bytes
     * has nothing on the wire to say which half an entry is in. One entry that is FSST-encoded (whose
     * stored bytes are not its value) or that {@link TemporalTextCodec} refuses, and the tag keeps its
     * bytes exactly as before.
     * </p>
     *
     * <p>
     * The FORM is decided by the FIRST entry and then required of the rest, rather than per entry: a
     * column is dates or datetimes, and a tag holding both would need a per-value form bit that costs
     * more than the mixture can ever save.
     * </p>
     */
    private boolean resolveTemporal(final int t, final int r, final int sz) {
      if (!temporalLaneEnabled() || sz == 0) {
        return false;
      }
      final int form = TemporalTextCodec.formOf(tagLengths[t][0]);
      if (form == TemporalTextCodec.FORM_REFUSED) {
        return false;
      }
      long[] values = temporalValues[r];
      if (values == null || values.length < sz) {
        values = new long[Math.max(sz, 16)];
        temporalValues[r] = values;
      }
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (int i = 0; i < sz; i++) {
        if (tagCompressed[t][i]) {
          return false;
        }
        final int len = tagLengths[t][i];
        if (len != TemporalTextCodec.lengthOf(form)) {
          return false;
        }
        if (globalProbeScratch.length < len) {
          globalProbeScratch = new byte[Math.max(len, globalProbeScratch.length << 1)];
        }
        tagStores[t][i].copyTo(tagOffsets[t][i], globalProbeScratch, 0, len);
        final long value = TemporalTextCodec.encode(globalProbeScratch, 0, len, form);
        if (value == TemporalTextCodec.REFUSED) {
          return false;
        }
        values[i] = value;
        if (value < min) {
          min = value;
        }
        if (value > max) {
          max = value;
        }
      }
      temporalForm[r] = form;
      temporalMin[r] = min;
      // Frame of reference: the span, not the magnitude, sets the width. A leaf's timestamps sit
      // within seconds of each other, so this is usually a handful of bits against the 64 a raw
      // value would take -- and it is the whole reason the lane beats a fixed-width epoch column.
      final long span = max - min;
      temporalBits[r] = span == 0L
          ? 0
          : 64 - Long.numberOfLeadingZeros(span);
      return true;
    }

    private boolean resolveGlobalIds(final int t, final int r, final int sz) {
      final GlobalStringDictionaries resolver = dictionaries;
      if (resolver == null || sz == 0) {
        return false;
      }
      final int tagValue = tagOrder.getInt(t);
      if (!resolver.hasDictionary(tagValue)) {
        return false;
      }
      // ONE read of each anchor field, for the whole encode of this tag. Everything downstream --
      // the packing width, the header's count, the parser's derived width -- must come from this
      // snapshot rather than from the live dictionary, which keeps growing underneath a parallel
      // flush.
      final long dictionaryKey = resolver.dictionaryKey(tagValue);
      if (dictionaryKey <= 0L) {
        return false; // 0 is the "this tag has no dictionary" sentinel, for every lane
      }
      globalDictionaryKey[r] = dictionaryKey;
      int maxId = 0;
      int[] ids = globalIds[r];
      if (ids == null || ids.length < sz) {
        ids = new int[Math.max(sz, 16)];
        globalIds[r] = ids;
      }
      for (int i = 0; i < sz; i++) {
        if (tagCompressed[t][i]) {
          // An FSST-encoded entry's stored bytes are not its value, so it cannot be looked up
          // without decoding, and the encoder holds no symbol table.
          return false;
        }
        final int len = tagLengths[t][i];
        if (globalProbeScratch.length < len) {
          globalProbeScratch = new byte[Math.max(len, globalProbeScratch.length << 1)];
        }
        tagStores[t][i].copyTo(tagOffsets[t][i], globalProbeScratch, 0, len);
        final int id = resolver.idOf(tagValue, globalProbeScratch, 0, len);
        if (id == GlobalStringDictionaries.ID_ABSENT) {
          return false;
        }
        ids[i] = id;
        if (id > maxId) {
          maxId = id;
        }
      }
      // The count is read AFTER the ids, and that ordering is load-bearing. A prebuilt dictionary is
      // fixed, so either order gives the same number; a SEGMENT-scoped dictionary is minted BY THIS
      // LOOP, so a count read first would be the count before this page's own values existed and the
      // derived width would be too narrow for ids the page just created. Read after, it covers them.
      // It is still ONE read used for the packing width, the header's count and the parser's derived
      // width, which is the property the snapshot existed for.
      final int entryCount = resolver.dictionaryEntryCount(tagValue);
      if (maxId > entryCount) {
        // DENSITY, asserted rather than assumed. The lane's width comes from the entry count, which
        // is only a bound on the ids because they run 1..entryCount with no gaps. If a dictionary
        // ever became sparse -- reserved ranges, tombstoned ids, per-column partitioning -- an id
        // could exceed the count, the derived width would be too narrow, and the id would be written
        // TRUNCATED: a silently different value, not a failure.
        throw new IllegalStateException("global dictionary for tag " + tagValue + " issued id " + maxId
            + " above its entry count " + entryCount + "; the trie lane derives its bit width from that count "
            + "and requires ids to be dense in 1..entryCount");
      }
      globalEntryCount[r] = entryCount;
      return true;
    }

    private void ensureGlobalScratch(final int tags) {
      if (globalTag.length < tags) {
        globalTag = new boolean[Math.max(tags, globalTag.length << 1)];
        globalIds = Arrays.copyOf(globalIds, globalTag.length);
        globalLengthWidth = Arrays.copyOf(globalLengthWidth, globalTag.length);
        globalEntryCount = Arrays.copyOf(globalEntryCount, globalTag.length);
        globalDictionaryKey = Arrays.copyOf(globalDictionaryKey, globalTag.length);
        temporalTag = Arrays.copyOf(temporalTag, globalTag.length);
        temporalForm = Arrays.copyOf(temporalForm, globalTag.length);
        temporalValues = Arrays.copyOf(temporalValues, globalTag.length);
        temporalMin = Arrays.copyOf(temporalMin, globalTag.length);
        temporalBits = Arrays.copyOf(temporalBits, globalTag.length);
      }
      Arrays.fill(globalTag, 0, tags, false);
      Arrays.fill(temporalTag, 0, tags, false);
    }

    /**
     * The narrowest of {@link #LENGTH_WIDTHS} that holds every length in this tag's dictionary.
     *
     * <p>
     * Positive lengths only — a global tag has no FSST-encoded entry — but the field is read back
     * SIGNED, so the thresholds are the signed ones. A fused record is capped at 1,023 bytes, so two
     * is the realistic answer and four is there for a cap that moves.
     * </p>
     */
    private int globalLengthWidthCode(final int t, final int sz) {
      int maximum = 0;
      for (int i = 0; i < sz; i++) {
        final int length = tagLengths[t][i];
        if (length > maximum) {
          maximum = length;
        }
      }
      if (maximum <= Byte.MAX_VALUE) {
        return 0;
      }
      return maximum <= Short.MAX_VALUE
          ? 1
          : 2;
    }

    /** Inverse of {@link #LENGTH_WIDTHS}, for a width this encoder chose itself. */
    private static int widthCodeOf(final int width) {
      return switch (width) {
        case 1 -> 0;
        case 2 -> 1;
        case 4 -> 2;
        default -> throw new IllegalStateException("no length width code for " + width);
      };
    }

    private static long tagMeta(final boolean plain, final int width, final int dictSize) {
      final int widthCode = width == 1
          ? 0
          : (width == 2
              ? 1
              : 2);
      return (plain
          ? 0L
          : (long) dictSize << 3) | ((long) widthCode << 1) | (plain
              ? 1L
              : 0L);
    }

    /** Reusable per-tag lane decisions; never escapes the encoder. */
    private boolean[] plainScratch(final int tags) {
      if (plainLane.length < tags) {
        plainLane = new boolean[Math.max(tags, plainLane.length << 1)];
      }
      return plainLane;
    }

    /** Reusable per-tag length widths; never escapes the encoder. */
    private byte[] lengthWidthScratch(final int tags) {
      if (lengthWidths.length < tags) {
        lengthWidths = new byte[Math.max(tags, lengthWidths.length << 1)];
      }
      return lengthWidths;
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

  private static final VarHandle SHORT_LE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private static int getInt(final MemorySegment buf, final long off) {
    return buf.get(LE.INT, off);
  }

  /**
   * One signed length field read out of an encoder-side {@code byte[]} rather than a payload
   * segment, for the sketch, which is built over the encoder's scratch before the region is
   * installed.
   */
  static int readLengthFieldFromArray(final byte[] payload, final int offset, final int width) {
    return switch (width) {
      case 1 -> payload[offset];
      case 2 -> (short) SHORT_LE.get(payload, offset);
      default -> (int) INT_LE.get(payload, offset);
    };
  }

  /** Write one signed length field at the width its tag chose. */
  private static void writeLengthField(final byte[] target, final int offset, final int width, final int value) {
    switch (width) {
      case 1 -> target[offset] = (byte) value;
      case 2 -> SHORT_LE.set(target, offset, (short) value);
      default -> putInt(target, offset, value);
    }
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

  /**
   * The 64-bit sibling of {@link #bitPackAppend}, for the temporal lane's frame-of-reference deltas.
   *
   * <p>
   * A separate method rather than a widened one: the id lane packs at most 32 bits and is the hottest
   * write in the region, so it keeps its {@code int} arithmetic. A width of {@code 0} writes nothing,
   * which is what a tag whose values are all equal needs.
   * </p>
   *
   * @param out destination, which must be ZEROED over the range (this ORs)
   * @param base byte offset of the lane
   * @param bitPos bit offset within the lane
   * @param value the value to write, which must fit in {@code bitWidth} bits
   * @param bitWidth 0..64
   */
  /**
   * The value of entry {@code entry} of a TEMPORAL tag, rendered into {@code dst} at {@code off}.
   *
   * <p>
   * A temporal tag stores numbers, so this is the whole of its read path: no dictionary, no page
   * bytes, one unpack and one render. The caller sizes {@code dst} from
   * {@link TemporalTextCodec#lengthOf} of {@link Header#tagTemporalForm}.
   * </p>
   *
   * @param payload the region payload
   * @param header a header already parsed from {@code payload}
   * @param tagIndex the tag, which must be temporal
   * @param entry the dictionary entry
   * @param dst destination for the rendered text
   * @param off offset within {@code dst}
   * @return bytes written
   * @throws IllegalArgumentException if the tag is not temporal
   */
  public static int temporalValueAt(final MemorySegment payload, final Header header, final int tagIndex,
      final int entry, final byte[] dst, final int off) {
    if (!header.tagTemporal[tagIndex]) {
      throw new IllegalArgumentException("string region tag " + tagIndex + " is not a temporal tag");
    }
    Objects.checkIndex(entry, header.tagStringDictSize[tagIndex]);
    final int bits = header.tagTemporalBits[tagIndex];
    final long delta = bitUnpackLong(payload, header.tagStringDictOffset[tagIndex], (long) entry * bits, bits);
    return TemporalTextCodec.decode(header.tagTemporalMin[tagIndex] + delta, header.tagTemporalForm[tagIndex], dst,
        off);
  }

  /** Byte length every value of a temporal tag renders to. */
  public static int temporalValueLength(final Header header, final int tagIndex) {
    if (!header.tagTemporal[tagIndex]) {
      throw new IllegalArgumentException("string region tag " + tagIndex + " is not a temporal tag");
    }
    return TemporalTextCodec.lengthOf(header.tagTemporalForm[tagIndex]);
  }

  private static void bitPackAppendLong(final byte[] out, final int base, final long bitPos, final long value,
      final int bitWidth) {
    if (bitWidth == 0) {
      return;
    }
    final long mask = bitWidth == 64
        ? -1L
        : ((1L << bitWidth) - 1L);
    long v = value & mask;
    int byteOff = base + (int) (bitPos >>> 3);
    int shift = (int) (bitPos & 7L);
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

  /**
   * Read back one value {@link #bitPackAppendLong} wrote, from a payload segment.
   *
   * @param payload the region payload
   * @param base byte offset of the lane within {@code payload}
   * @param bitPos bit offset within the lane
   * @param bitWidth 0..64; {@code 0} always reads {@code 0}
   * @return the unsigned value
   */
  private static long bitUnpackLong(final MemorySegment payload, final long base, final long bitPos,
      final int bitWidth) {
    if (bitWidth == 0) {
      return 0L;
    }
    long value = 0L;
    int gathered = 0;
    long byteOff = base + (bitPos >>> 3);
    int shift = (int) (bitPos & 7L);
    int remaining = bitWidth;
    while (remaining > 0) {
      final int bitsThisByte = Math.min(8 - shift, remaining);
      final int b = payload.get(ValueLayout.JAVA_BYTE, byteOff) & 0xFF;
      final long chunk = (b >>> shift) & ((1L << bitsThisByte) - 1L);
      value |= chunk << gathered;
      gathered += bitsThisByte;
      remaining -= bitsThisByte;
      byteOff++;
      shift = 0;
    }
    return value;
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
