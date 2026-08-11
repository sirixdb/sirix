/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.utils.FSSTCompressor;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Segmented persistence codec for projection leaves
 * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3): splits one raw leaf into
 * <em>semantic segments</em> — the record-key column, one body per column
 * (presence + encoded values, flags at the head), and one dictionary per
 * string column — each destined for its own CoW-versioned
 * {@link io.sirix.page.OverflowPage} (referenced segments) or the descriptor's own inline region
 * (small segments), addressed from a {@link RowGroupDescriptor} slot
 * value. Per-segment encodings are byte-compatible with
 * {@link ProjectionIndexRowGroupCodec}'s compact streams (same delta/FOR record
 * keys, FOR bit-packed numerics, packed dict-ids, marker-byte presence — the
 * primitives are shared), regrouped so that
 *
 * <ul>
 *   <li>a query reading column {@code c} fetches {@code BODY(c)} (+
 *       {@code DICT(c)} for string predicates) and nothing else;</li>
 *   <li>a single-column in-place update re-encodes one body segment, and the
 *       byte-shift cascade of the monolithic form is contained inside that
 *       column by construction;</li>
 *   <li>{@link #assembleRaw} reconstructs the raw scan form
 *       <b>byte-identically</b> (same guarantee as
 *       {@link ProjectionIndexRowGroupCodec#decode}), presence, unrepresentable
 *       and integrality provenance included.</li>
 * </ul>
 *
 * <h2>Segment id scheme</h2>
 *
 * {@code 0 = KEYS}, {@code 3c+1 = BODY(c)}, {@code 3c+2 = DICT(c)} — capped
 * at {@link RowGroupDescriptor#MAX_COLUMNS} columns by the 16-bit id space of the
 * HOT side-map composite key.
 *
 * <h2>Per-segment wire form</h2>
 *
 * Every segment is self-describing: {@code int "PIXS"; byte version; byte
 * segKind}, then the kind-specific payload:
 *
 * <pre>
 *   KEYS:  long firstRecordKey; long lastRecordKey;
 *          [rowCount &gt; 0] byte mode; long base; byte width; packed keys
 *   BODY:  byte colFlags;                       // provenance TRUTH (5.1-7)
 *          [rowCount &gt; 0] long min; long max;   // zone-map truth
 *          presence marker byte [+ words];
 *          NUMERIC: long base; byte width; packed values
 *          BOOLEAN: words verbatim
 *          STRING:  byte idWidth; packed dict-ids
 *   DICT:  int dictSize; int[dictSize] lens; concatenated UTF-8
 * </pre>
 *
 * <p>An empty leaf ({@code rowCount == 0}) still emits KEYS (fences) and one
 * BODY per column (flag truth) — no DICT segments; the descriptor stays the
 * bounds authority.
 *
 * <p>Integrity: {@link #assembleRaw} verifies every segment's exact
 * {@code byteLen} and XXH3-64 {@code contentHash} against the descriptor
 * before parsing — segment pages persist behind bare offset keys, so the
 * descriptor hash is their only checksum. A mismatch throws (callers fail
 * soft to the generic pipeline and negative-cache, §4). The hash doubles as
 * the maintenance write-path no-op comparator (§3).
 */
public final class ProjectionIndexColumnSegmentCodec {

  /** Leading magic of every segment ("PIXS" little-endian). */
  public static final int SEGMENT_MAGIC = 0x53584950;

  /** Segment layout version; bumped on any wire change. */
  public static final byte SEGMENT_VERSION = 1;

  /** Segment kind tags. */
  public static final byte SEG_KIND_KEYS = 0;
  public static final byte SEG_KIND_BODY = 1;
  public static final byte SEG_KIND_DICT = 2;

  /**
   * Per-value ROW COUNTS for a set column: the values, each with the number of rows on this leaf
   * whose set contains it.
   *
   * <p>Its own segment kind rather than a section of {@link #SEG_KIND_DICT} for one reason: SIZE.
   * The inline policy stores a segment's bytes in the descriptor itself when they are small enough,
   * and the descriptor is metadata the reader already holds — so an inline segment costs NO page
   * fetch. A dictionary of 41 genre strings runs past the 192-byte cap and lands on a page; the
   * counts beside a compact value list fit under it. Splitting them is what turns a membership
   * count from 110 MB of segment reads into zero.
   *
   * <p>Emitted only while the dictionary stays small enough to inline — past that the segment would
   * be referenced, buy nothing, and cost a page of its own, so it is not written at all and the
   * reader falls back to scanning.
   */
  public static final byte SEG_KIND_SET_COUNTS = 3;

  /**
   * Per-leaf BLOOM FINGERPRINT over a string column's distinct values — the equality analogue of
   * the numeric zone map for UNSORTED string data.
   *
   * <p>Why min/max cannot do this job: the descriptor's zone pair prunes only when the column is
   * clustered, and a leaf of ~1024 arbitrary titles spans nearly the whole collation range, so a
   * string-equality literal zone-tests as "possible" on every leaf. The fingerprint answers the
   * question the zone map cannot: "can THIS value be on this leaf at all?" — probabilistically,
   * with no false negatives. A miss skips the leaf's BODY and DICT fetch AND decode entirely; a
   * false positive costs exactly what every leaf cost before this segment existed.
   *
   * <p>Sized at ~10 bits per distinct value (3 probes of one 64-bit hash), clamped to
   * [64 B, 2 KiB] of filter words: ~1 % false-positive rate at full occupancy. On the movies
   * corpus a title-equality one-shot reads ~4 MB of fingerprints instead of ~55 MB of
   * dictionary + id segments.
   *
   * <p>ADDITIVE format: readers discover the segment by its chain being present. An index built
   * before this kind existed simply has no fingerprint chain, and the scan keeps every leaf —
   * the pre-fingerprint behaviour, not an error.
   */
  public static final byte SEG_KIND_STRING_BLOOM = 4;

  /** Fixed per-segment header size: magic + version + segKind. */
  public static final int SEGMENT_HEADER_BYTES = 6;

  /**
   * Hybrid inline policy (docs/PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md §3.3): a segment whose
   * encoded length is {@code <=} {@link #inlineMaxColumnSegmentBytes} is eligible to live inline in
   * the owning {@link RowGroupDescriptor} slot instead of a side-map {@link io.sirix.page.OverflowPage}
   * — the projection analogue of {@link io.sirix.page.KeyValueLeafPage} keeping a small record in
   * the slot heap. {@link #inlineMaxTotalBytes} caps the total inlined per leaf (smallest-first;
   * the rest spill to pages) so the descriptor slot stays small and the HOT trie shallow. Setting
   * {@code inlineMaxColumnSegmentBytes=0} disables inlining → the pre-hybrid all-referenced layout
   * (escape hatch / A-B baseline). Read from the system property once at class load; deterministic
   * for a given value, so identical re-encodes classify identically and the maintenance no-op hash
   * stays stable (5.2-n). Production reads the immutable defaults; the per-thread
   * {@link #INLINE_POLICY} override exists only so tests can exercise both storage classes without
   * a shared-global race (each test thread sees its own value even under parallel execution).
   */
  private static final int DEFAULT_INLINE_MAX_SEGMENT_BYTES =
      Integer.getInteger("sirix.projection.inlineMaxColumnSegmentBytes", 192);

  /** Per-leaf inline budget; see {@link #DEFAULT_INLINE_MAX_SEGMENT_BYTES}. */
  private static final int DEFAULT_INLINE_MAX_TOTAL_BYTES =
      Integer.getInteger("sirix.projection.inlineMaxTotalBytes", 512);

  /**
   * Per-thread inline-policy override ({@code [maxColumnSegmentBytes, maxTotalBytes]}), or {@code null}
   * to use the defaults — a test-only seam ({@link #setInlinePolicyForTesting} /
   * {@link #clearInlinePolicyForTesting}). Thread-local, so a test toggling the policy can never
   * bleed into a concurrently-running encode on another thread. Encoding always runs on the caller
   * thread (single-threaded per write transaction), so the override is seen by the encode it wraps.
   */
  private static final ThreadLocal<int[]> INLINE_POLICY = new ThreadLocal<>();

  /** Test seam: set this thread's inline policy. Prefer a try/finally with {@link #clearInlinePolicyForTesting}. */
  static void setInlinePolicyForTesting(final int maxColumnSegmentBytes, final int maxTotalBytes) {
    INLINE_POLICY.set(new int[] {maxColumnSegmentBytes, maxTotalBytes});
  }

  /** Test seam: restore this thread to the default inline policy. */
  static void clearInlinePolicyForTesting() {
    INLINE_POLICY.remove();
  }

  /** XXH3-64 for descriptor content hashes (zero-allocation, shared instance). */
  private static final LongHashFunction XX3 = LongHashFunction.xx3();

  private ProjectionIndexColumnSegmentCodec() {
  }

  /**
   * Per-column segment slots in the id scheme; {@link RowGroupDescriptor#MAX_COLUMNS} is derived
   * from this and the 16-bit id space so the invariant lives in one place.
   */
  public static final int SEGMENTS_PER_COLUMN = 4;

  /** Segment id of the record-key segment. */
  public static int keysColumnSegmentId() {
    return 0;
  }

  /** Segment id of column {@code c}'s body segment. */
  public static int bodyColumnSegmentId(final int column) {
    return SEGMENTS_PER_COLUMN * checkColumn(column) + 1;
  }

  /** Segment id of column {@code c}'s dictionary segment (STRING_DICT columns only). */
  public static int dictColumnSegmentId(final int column) {
    return SEGMENTS_PER_COLUMN * checkColumn(column) + 2;
  }

  /** Segment id of column {@code c}'s {@link #SEG_KIND_SET_COUNTS} segment. */
  public static int setCountsColumnSegmentId(final int column) {
    return SEGMENTS_PER_COLUMN * checkColumn(column) + 3;
  }

  /**
   * Segment id of column {@code c}'s {@link #SEG_KIND_STRING_BLOOM} segment. Sub-id 4 takes the
   * fourth slot of the per-column stride ({@code SEGMENTS_PER_COLUMN} = 4; body/dict/set-counts
   * take 1..3). Widening the stride from 3 to 4 renumbered every column's segment ids and shrank
   * {@link RowGroupDescriptor#MAX_COLUMNS} by a quarter; that is safe across versions only because
   * the {@link ProjectionIndexMetadata} wire-format version bump (0 → 1) makes any pre-renumbering
   * store parse as "no metadata" and be rebuilt rather than read at shifted ids.
   */
  public static int bloomColumnSegmentId(final int column) {
    return SEGMENTS_PER_COLUMN * checkColumn(column) + 4;
  }

  private static int checkColumn(final int column) {
    if (column < 0 || column >= RowGroupDescriptor.MAX_COLUMNS) {
      // Without this check a column past the cap would produce a segment id past the 16-bit sub-id
      // space, colliding with another column's segment — a hash mismatch found only at later assembly.
      throw new IllegalArgumentException("column out of range [0, " + RowGroupDescriptor.MAX_COLUMNS + "): " + column);
    }
    return column;
  }

  /** Column index owning segment id {@code columnSegmentId} (BODY/DICT), or -1 for KEYS. */
  public static int columnOfColumnSegment(final int columnSegmentId) {
    return columnSegmentId == 0 ? -1 : (columnSegmentId - 1) / SEGMENTS_PER_COLUMN;
  }

  /** XXH3-64 content hash as stored in descriptor entries. */
  public static long contentHash(final byte[] segment) {
    return XX3.hashBytes(segment);
  }

  /**
   * One encoded leaf: the descriptor plus parallel arrays of segment ids and segment bytes
   * (ascending id order — KEYS, then per column BODY [, DICT]).
   *
   * <p><b>Aliasing contract (HFT, no defensive copies):</b> the accessors expose the codec's
   * internal arrays. The descriptor embeds each segment's content hash at encode time, so
   * mutating any returned array de-synchronises bytes from their recorded hash and poisons
   * every later assembly with a spurious corruption error. Treat all three as immutable.
   */
  public record EncodedRowGroup(byte[] descriptor, int[] columnSegmentIds, byte[][] segments) {
  }

  /** Resolves a segment's bytes by id — the storage layer's read hook. */
  @FunctionalInterface
  public interface SegmentResolver {
    byte @Nullable [] segment(int columnSegmentId);
  }

  // ==================== encode ====================

  /**
   * Encode a raw leaf payload into descriptor + segments.
   *
   * @throws IllegalStateException when {@code rawPayload} is not a valid raw leaf
   *         (propagated from {@link ProjectionIndexRowGroupPage#deserialize})
   */
  public static @Nullable EncodedRowGroup encode(final byte @Nullable [] rawPayload) {
    return encode(rawPayload, true);
  }

  /**
   * {@link #encode(byte[])} for the SEGMENT-SLOT layout, where every segment lives in its own slot
   * and the descriptor is stored zone-map-only.
   *
   * <p>Skips the inline classification entirely. The hybrid path classifies segments smallest-first
   * under a byte budget and copies the chosen ones into the descriptor's trailing region — work the
   * segment-slot writer then discards wholesale via
   * {@link RowGroupDescriptor#toZoneMapOnly}. Producing the all-referenced descriptor directly is
   * both cheaper and exactly what that writer stores.</p>
   *
   * @param rawPayload the raw leaf payload, or {@code null}
   * @return the encoded row group whose descriptor marks every segment REFERENCED
   */
  public static @Nullable EncodedRowGroup encodeReferencedOnly(final byte @Nullable [] rawPayload) {
    return encode(rawPayload, false);
  }

  private static @Nullable EncodedRowGroup encode(final byte @Nullable [] rawPayload,
      final boolean classifyInline) {
    if (rawPayload == null) {
      // Null-in/null-out mirrors ProjectionIndexRowGroupCodec.encode — an absent leaf stays absent.
      return null;
    }
    final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(rawPayload);
    final int rowCount = page.getRowCount();
    final int columnCount = page.getColumnCount();
    if (columnCount > RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalStateException("columnCount " + columnCount + " exceeds MAX_COLUMNS="
          + RowGroupDescriptor.MAX_COLUMNS);
    }

    // KEYS once, then per column: BODY, an optional DICT, and an optional SET_COUNTS. Sized for
    // the maximum a column can emit — under-sizing this overflows the parallel arrays below rather
    // than dropping a segment, which is how the SET_COUNTS addition first showed up.
    final int maxColumnSegments = 1 + 4 * columnCount;
    final int[] columnSegmentIds = new int[maxColumnSegments];
    final byte[][] segments = new byte[maxColumnSegments][];
    final byte[] entryFlags = new byte[maxColumnSegments];
    final long[] entryMins = new long[maxColumnSegments];
    final long[] entryMaxs = new long[maxColumnSegments];
    int columnSegmentCount = 0;

    // KEYS segment.
    {
      final ByteArrayOutputStream out = newColumnSegmentStream(SEG_KIND_KEYS);
      ProjectionIndexRowGroupCodec.putLongLE(out, page.firstRecordKey());
      ProjectionIndexRowGroupCodec.putLongLE(out, page.lastRecordKey());
      if (rowCount > 0) {
        ProjectionIndexRowGroupCodec.encodeRecordKeys(out, page.recordKeys(), rowCount);
      }
      columnSegmentIds[columnSegmentCount] = keysColumnSegmentId();
      segments[columnSegmentCount] = out.toByteArray();
      columnSegmentCount++;
    }

    final byte[] kinds = new byte[columnCount];
    for (int c = 0; c < columnCount; c++) {
      kinds[c] = page.columnKind(c);

      byte flags = page.columnUnrepresentable(c) ? ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE : 0;
      if (page.columnNumericNonIntegral(c)) {
        flags |= ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL;
      }
      if (page.columnPureDoubleSource(c)) {
        flags |= ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE;
      }

      // BODY segment.
      final ByteArrayOutputStream body = newColumnSegmentStream(SEG_KIND_BODY);
      body.write(flags);
      if (rowCount > 0) {
        ProjectionIndexRowGroupCodec.putLongLE(body, page.columnMin(c));
        ProjectionIndexRowGroupCodec.putLongLE(body, page.columnMax(c));
        ProjectionIndexRowGroupCodec.encodePresence(body, page.presenceColumnBits(c), rowCount);
        switch (kinds[c]) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG ->
              ProjectionIndexRowGroupCodec.encodeForBitPacked(body, page.numericColumn(c), rowCount);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE ->
              ProjectionIndexRowGroupCodec.encodeForBitPackedDouble(body, page.numericColumn(c), rowCount);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> {
            final long[] bits = page.booleanColumnBits(c);
            final int words = (rowCount + 63) >>> 6;
            for (int w = 0; w < words; w++) {
              ProjectionIndexRowGroupCodec.putLongLE(body, bits[w]);
            }
          }
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT ->
              ProjectionIndexRowGroupCodec.encodeDictIds(body, page.stringDictionary(c),
                  page.stringDictIdColumn(c), rowCount);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
            // Counts are ids in their own right — bit-packed to the widest count on the leaf — and
            // the element run reuses the dictionary width. Two packed runs, no lengths beyond the
            // counts themselves.
            final int[] counts = page.stringSetCountColumn(c);
            int maxCount = 0;
            for (int r = 0; r < rowCount; r++) {
              if (counts[r] > maxCount) {
                maxCount = counts[r];
              }
            }
            ProjectionIndexRowGroupCodec.encodePackedIds(body, counts, rowCount, maxCount);
            ProjectionIndexRowGroupCodec.encodeDictIds(body, page.stringDictionary(c),
                page.stringSetIdColumn(c), page.stringSetLength(c));
          }
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        }
      }
      columnSegmentIds[columnSegmentCount] = bodyColumnSegmentId(c);
      segments[columnSegmentCount] = body.toByteArray();
      entryFlags[columnSegmentCount] = flags;
      // Empty leaf: mirror the zone-map sentinel pair (min > max = "no present value"), the
      // same discipline appendRow initialises — a fabricated [0, 0] would defeat descriptor
      // pruning and read as "possibly contains 0".
      entryMins[columnSegmentCount] = rowCount > 0 ? page.columnMin(c) : Long.MAX_VALUE;
      entryMaxs[columnSegmentCount] = rowCount > 0 ? page.columnMax(c) : Long.MIN_VALUE;
      columnSegmentCount++;

      // DICT segment (string columns with rows only).
      if ((kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT || kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) && rowCount > 0) {
        final ByteArrayOutputStream dict = newColumnSegmentStream(SEG_KIND_DICT);
        encodeDictColumnSegmentPayload(dict, page.stringDictionary(c),
            kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET
                ? valueRowCounts(page.stringDictionary(c), page.stringSetCountColumn(c),
                                 page.stringSetIdColumn(c), rowCount)
                : null);
        columnSegmentIds[columnSegmentCount] = dictColumnSegmentId(c);
        segments[columnSegmentCount] = dict.toByteArray();
        columnSegmentCount++;

        // SET_COUNTS segment: the values with their per-value ROW counts, small enough to ride
        // inline in the descriptor so a membership count reads no page at all.
        if (kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
          final byte[] counts = encodeSetCountsPayload(page.stringDictionary(c),
              valueRowCounts(page.stringDictionary(c), page.stringSetCountColumn(c),
                             page.stringSetIdColumn(c), rowCount));
          if (counts != null) {
            columnSegmentIds[columnSegmentCount] = setCountsColumnSegmentId(c);
            segments[columnSegmentCount] = counts;
            columnSegmentCount++;
          }
        }

        // STRING_BLOOM segment: equality fingerprint over the leaf's distinct values.
        columnSegmentIds[columnSegmentCount] = bloomColumnSegmentId(c);
        segments[columnSegmentCount] = encodeStringBloomPayload(page.stringDictionary(c));
        columnSegmentCount++;
      }
    }

    final int[] byteLens = new int[columnSegmentCount];
    final long[] hashes = new long[columnSegmentCount];
    for (int i = 0; i < columnSegmentCount; i++) {
      byteLens[i] = segments[i].length;
      hashes[i] = contentHash(segments[i]);
    }
    // null inline[] => every segment REFERENCED and no trailing inline region — see
    // encodeReferencedOnly for why the segment-slot writer must not pay for the classification.
    final boolean[] inline =
        classifyInline ? classifyInline(byteLens, columnSegmentCount, columnSegmentIds) : null;
    final byte[] descriptor = RowGroupDescriptor.serialize(rowCount, page.firstRecordKey(), page.lastRecordKey(),
        kinds, columnSegmentCount, columnSegmentIds, byteLens, hashes, entryFlags, entryMins, entryMaxs, inline, segments);

    final int[] idsTrimmed = new int[columnSegmentCount];
    System.arraycopy(columnSegmentIds, 0, idsTrimmed, 0, columnSegmentCount);
    final byte[][] segsTrimmed = new byte[columnSegmentCount][];
    System.arraycopy(segments, 0, segsTrimmed, 0, columnSegmentCount);
    return new EncodedRowGroup(descriptor, idsTrimmed, segsTrimmed);
  }

  /**
   * Classify each segment inline vs referenced by size, smallest-first under the per-leaf budget
   * (docs/PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md §3.3). Deterministic — same byteLens in
   * always produce the same classification — so the maintenance no-op hash is stable across
   * identical re-encodes (5.2-n). {@code columnSegmentCount} is tiny (≤ ~169), so the O(columnSegmentCount²)
   * smallest-first scan is free on the build path and needs no allocation beyond the result.
   */
  static boolean[] classifyInline(final int[] byteLens, final int columnSegmentCount) {
    return classifyInline(byteLens, columnSegmentCount, null);
  }

  /**
   * As above, but a {@link #SEG_KIND_SET_COUNTS} segment is inlined AHEAD of the smallest-first
   * size policy — it claims the inline budget before any other segment competes for it, subject
   * only to the structural caps (per-segment and per-leaf), which bound descriptor size and may
   * never be exceeded.
   *
   * <p>That segment exists solely so a membership count can be answered from the descriptor without
   * touching a page. Letting the ordinary smallest-first budget decide would defeat it exactly when
   * it matters — a dictionary with enough distinct values to be worth summarising is also the one
   * that loses the smallest-first race, so it would be referenced, cost a page, and leave the query
   * no better off than scanning. Its size is bounded at write time
   * ({@link #MAX_SET_COUNTS_BYTES}, tied to the per-segment cap); past that the segment is not
   * written at all.
   *
   * @param columnSegmentIds segment ids parallel to {@code byteLens}, or {@code null} to apply the
   *                         size policy uniformly
   */
  static boolean[] classifyInline(final int[] byteLens, final int columnSegmentCount,
      final int @Nullable [] columnSegmentIds) {
    final boolean[] inline = new boolean[columnSegmentCount];
    final int[] policy = INLINE_POLICY.get();
    final int maxColumnSegment = policy != null ? policy[0] : DEFAULT_INLINE_MAX_SEGMENT_BYTES;
    if (maxColumnSegment <= 0) {
      return inline; // inlining disabled → all referenced (pre-hybrid layout)
    }
    int remaining = policy != null ? policy[1] : DEFAULT_INLINE_MAX_TOTAL_BYTES;
    // SET_COUNTS segments claim the budget first — still within the structural caps: they bound a
    // descriptor's size, which bounds how many descriptors fit in a 64 KB HOT leaf page. Forcing a
    // 451-byte payload past them overflowed HOTLeafPage.rebuildForShorterPrefix during a projection
    // build — the invariant is structural, and a segment that does not fit belongs on a page.
    if (columnSegmentIds != null) {
      for (int i = 0; i < columnSegmentCount; i++) {
        if (columnSegmentIds[i] % SEGMENTS_PER_COLUMN == 3
            && byteLens[i] <= maxColumnSegment && byteLens[i] <= remaining) {
          inline[i] = true;
          remaining -= byteLens[i];
        }
      }
    }
    while (true) {
      int best = -1;
      for (int i = 0; i < columnSegmentCount; i++) {
        if (!inline[i] && byteLens[i] <= maxColumnSegment && byteLens[i] <= remaining
            && (best < 0 || byteLens[i] < byteLens[best])) {
          best = i;
        }
      }
      if (best < 0) {
        return inline; // nothing more fits the budget → the rest spill to pages
      }
      inline[best] = true;
      remaining -= byteLens[best];
    }
  }

  /** Filter-size clamp: [512, 16384] bits = [64 B, 2 KiB] of words. */
  private static final int BLOOM_MIN_BITS = 512;
  private static final int BLOOM_MAX_BITS = 16384;

  /**
   * Build the {@link #SEG_KIND_STRING_BLOOM} payload over the leaf's distinct dictionary values.
   * Layout after the shared segment header: {@code int mBits; long[mBits/64] words}.
   */
  private static byte[] encodeStringBloomPayload(final byte[][] dictionary) {
    int count = 0;
    while (count < dictionary.length && dictionary[count] != null) {
      count++;
    }
    int mBits = Integer.highestOneBit(Math.max(1, count * 10));
    if (mBits < count * 10) {
      mBits <<= 1;                            // next power of two at ~10 bits/value
    }
    mBits = Math.max(BLOOM_MIN_BITS, Math.min(BLOOM_MAX_BITS, mBits));
    final long[] words = new long[mBits >>> 6];
    final int mask = mBits - 1;
    for (int i = 0; i < count; i++) {
      final long h = fnv64(dictionary[i]);
      words[(int) ((h & mask) >>> 6)] |= 1L << (h & 63);
      final long h2 = h >>> 21;
      words[(int) ((h2 & mask) >>> 6)] |= 1L << (h2 & 63);
      final long h3 = h >>> 42;
      words[(int) ((h3 & mask) >>> 6)] |= 1L << (h3 & 63);
    }
    final ByteArrayOutputStream out = newColumnSegmentStream(SEG_KIND_STRING_BLOOM);
    ProjectionIndexRowGroupCodec.putIntLE(out, mBits);
    for (final long w : words) {
      ProjectionIndexRowGroupCodec.putLongLE(out, w);
    }
    return out.toByteArray();
  }

  /**
   * Probe a {@link #SEG_KIND_STRING_BLOOM} payload for {@code literalUtf8}. No false negatives:
   * {@code false} PROVES the value is absent from the leaf; {@code true} means "fetch and check".
   * Defensive on malformed input — anything unexpected answers {@code true}, i.e. no pruning.
   */
  public static boolean bloomMayContain(final byte[] segment, final byte[] literalUtf8) {
    return bloomMayContainHash(segment, bloomHash(literalUtf8));
  }

  /**
   * The literal's fingerprint hash, hoisted so a scan over N leaves hashes ONCE and probes N
   * times — the per-leaf work is three word loads and three bit tests.
   */
  public static long bloomHash(final byte[] literalUtf8) {
    return fnv64(literalUtf8);
  }

  /** Probe with a pre-computed {@link #bloomHash}. Same no-false-negative contract. */
  public static boolean bloomMayContainHash(final byte[] segment, final long h) {
    return segment == null || bloomMayContainHashAt(segment, 0, segment.length, h);
  }

  private static boolean bloomMayContainHashAt(final byte[] a, final int off, final int len,
      final long h) {
    if (len < SEGMENT_HEADER_BYTES + Integer.BYTES) {
      return true;
    }
    final int mBits = ProjectionIndexRowGroupCodec.getIntLE(a, off + SEGMENT_HEADER_BYTES);
    if (mBits < BLOOM_MIN_BITS || mBits > BLOOM_MAX_BITS || Integer.bitCount(mBits) != 1
        || len < SEGMENT_HEADER_BYTES + Integer.BYTES + (mBits >>> 3)) {
      return true;
    }
    final int base = off + SEGMENT_HEADER_BYTES + Integer.BYTES;
    final int mask = mBits - 1;
    return bloomBit(a, base, h & mask)
        && bloomBit(a, base, (h >>> 21) & mask)
        && bloomBit(a, base, (h >>> 42) & mask);
  }

  private static boolean bloomBit(final byte[] segment, final int base, final long bit) {
    final long word = ProjectionIndexRowGroupCodec.getLongLE(segment, base + (int) ((bit >>> 6) << 3));
    return (word & 1L << (bit & 63)) != 0;
  }

  // ==================== fingerprint BLOCK ====================
  // One contiguous blob per string column concatenating every leaf's fingerprint segment.
  // Motivation (measured): the per-leaf fingerprint chain is ~2 KiB pages STRIDED between the fat
  // BODY/DICT pages, so a cold chain fetch degenerates to one scattered pread per leaf and cost
  // MORE than the pruning saved (S6 381 -> ~950 ms). As one blob the same bytes are one
  // sequential read. The per-leaf segments remain the WRITTEN truth; the block is a derived
  // acceleration — deleted on incremental maintenance, rebuilt by the next full build — and
  // readers without it fall back to the chain.

  /** Block header: magic + version + leafCount, then (leafCount+1) int offsets, then payloads. */
  private static final int BLOOM_BLOCK_MAGIC = 0x50424C4D;   // "PBLM"
  private static final byte BLOOM_BLOCK_VERSION = 1;
  private static final int BLOOM_BLOCK_HEADER_BYTES = Integer.BYTES + 1 + Integer.BYTES;

  /**
   * Concatenate per-leaf fingerprint segments (index = rowGroupId - 1; {@code null} = the leaf has
   * none, e.g. rowless) into one block. Returns {@code null} when NO leaf carries a fingerprint —
   * writing an all-empty block would cost a slot and prove nothing.
   */
  public static byte @Nullable [] encodeBloomBlock(final byte @Nullable [] @Nullable [] perLeaf,
      final int leafCount) {
    int total = 0;
    boolean any = false;
    for (int i = 0; i < leafCount; i++) {
      final byte[] seg = perLeaf[i];
      if (seg != null) {
        total += seg.length;
        any = true;
      }
    }
    if (!any) {
      return null;
    }
    final byte[] block =
        new byte[BLOOM_BLOCK_HEADER_BYTES + (leafCount + 1) * Integer.BYTES + total];
    ProjectionIndexRowGroupCodec.putIntLEAt(block, 0, BLOOM_BLOCK_MAGIC);
    block[Integer.BYTES] = BLOOM_BLOCK_VERSION;
    ProjectionIndexRowGroupCodec.putIntLEAt(block, Integer.BYTES + 1, leafCount);
    int payload = 0;
    final int tableBase = BLOOM_BLOCK_HEADER_BYTES;
    final int payloadBase = tableBase + (leafCount + 1) * Integer.BYTES;
    for (int i = 0; i < leafCount; i++) {
      ProjectionIndexRowGroupCodec.putIntLEAt(block, tableBase + i * Integer.BYTES, payload);
      final byte[] seg = perLeaf[i];
      if (seg != null) {
        System.arraycopy(seg, 0, block, payloadBase + payload, seg.length);
        payload += seg.length;
      }
    }
    ProjectionIndexRowGroupCodec.putIntLEAt(block, tableBase + leafCount * Integer.BYTES, payload);
    return block;
  }

  /** Leaf count a block declares, or {@code -1} when it is not a well-formed block. */
  public static int bloomBlockLeafCount(final byte @Nullable [] block) {
    if (block == null || block.length < BLOOM_BLOCK_HEADER_BYTES
        || ProjectionIndexRowGroupCodec.getIntLE(block, 0) != BLOOM_BLOCK_MAGIC
        || block[Integer.BYTES] != BLOOM_BLOCK_VERSION) {
      return -1;
    }
    final int leafCount = ProjectionIndexRowGroupCodec.getIntLE(block, Integer.BYTES + 1);
    if (leafCount < 0
        || block.length < BLOOM_BLOCK_HEADER_BYTES + (leafCount + 1L) * Integer.BYTES) {
      return -1;
    }
    return leafCount;
  }

  /**
   * Probe leaf {@code i} of a validated block ({@link #bloomBlockLeafCount} {@code >= i+1}) with a
   * pre-computed {@link #bloomHash}. A leaf without a fingerprint (empty slice) answers
   * {@code true} — no evidence, no prune.
   */
  public static boolean bloomBlockMayContainHash(final byte[] block, final int i, final long h) {
    final int tableBase = BLOOM_BLOCK_HEADER_BYTES;
    final int leafCount = ProjectionIndexRowGroupCodec.getIntLE(block, Integer.BYTES + 1);
    final int off = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + i * Integer.BYTES);
    final int end = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + (i + 1) * Integer.BYTES);
    final int payloadBase = tableBase + (leafCount + 1) * Integer.BYTES;
    if (off < 0 || end <= off || payloadBase + end > block.length) {
      return true;
    }
    return bloomMayContainHashAt(block, payloadBase + off, end - off, h);
  }

  /** FNV-1a 64 over the value bytes — the fingerprint's one hash; probes derive from it. */
  private static long fnv64(final byte[] bytes) {
    long h = 0xcbf29ce484222325L;
    for (final byte b : bytes) {
      h = (h ^ (b & 0xFF)) * 0x100000001b3L;
    }
    return h;
  }

  private static ByteArrayOutputStream newColumnSegmentStream(final byte segKind) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream(256);
    ProjectionIndexRowGroupCodec.putIntLE(out, SEGMENT_MAGIC);
    out.write(SEGMENT_VERSION);
    out.write(segKind);
    return out;
  }

  // ==================== assemble (decode) ====================

  /**
   * Reassemble the raw scan form from a descriptor and its segments, byte-identically to the
   * original {@link ProjectionIndexRowGroupPage#serialize()} output. Verifies each segment's
   * byteLen + contentHash against the descriptor before parsing.
   *
   * @throws IllegalStateException on any missing segment, length/hash mismatch, or malformed
   *         segment bytes — corruption is caught here, at fill time, never mid-kernel
   */
  public static byte[] assembleRaw(final byte[] descriptor, final SegmentResolver resolver) {
    RowGroupDescriptor.validate(descriptor);
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final int columnCount = RowGroupDescriptor.columnCount(descriptor);
    final byte[] kinds = new byte[columnCount];
    for (int c = 0; c < columnCount; c++) {
      kinds[c] = RowGroupDescriptor.kind(descriptor, c);
    }
    // Resolve every inline segment's data offset once (O(columnSegmentCount)) instead of a per-segment
    // prefix walk, so a whole-leaf assembly stays O(columnSegmentCount) in the inline bookkeeping.
    final int[] inlineOffsets = RowGroupDescriptor.inlineOffsets(descriptor);

    // KEYS.
    final ProjectionIndexRowGroupCodec.Cursor keys =
        openColumnSegment(descriptor, resolver, keysColumnSegmentId(), SEG_KIND_KEYS, inlineOffsets);
    final long firstRecordKey = keys.readLong();
    final long lastRecordKey = keys.readLong();
    final long[] recordKeys =
        rowCount > 0 ? ProjectionIndexRowGroupCodec.decodeRecordKeys(keys, rowCount) : new long[0];

    final long[] columnMin = new long[columnCount];
    final long[] columnMax = new long[columnCount];
    final long[][] numericCols = new long[columnCount][];
    final long[][] booleanCols = new long[columnCount][];
    final int[][] dictIdCols = new int[columnCount][];
    final byte[][][] dicts = new byte[columnCount][][];
    final int[][] setCountCols = new int[columnCount][];
    final int[][] setElemCols = new int[columnCount][];
    final byte[] columnFlags = new byte[columnCount];
    final long[][] presence = new long[columnCount][];
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;

    for (int c = 0; c < columnCount; c++) {
      final ProjectionIndexRowGroupCodec.Cursor body =
          openColumnSegment(descriptor, resolver, bodyColumnSegmentId(c), SEG_KIND_BODY, inlineOffsets);
      columnFlags[c] = body.readByte();

      final long[] bits = new long[Math.max(presWords, (ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6)];
      presence[c] = bits;
      if (rowCount == 0) {
        continue;
      }
      columnMin[c] = body.readLong();
      columnMax[c] = body.readLong();
      ProjectionIndexRowGroupCodec.decodePresenceInto(body, bits, presWords, rowCount);
      switch (kinds[c]) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE ->
            numericCols[c] = ProjectionIndexRowGroupCodec.decodeForBitPackedColumn(body, rowCount);
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
            booleanCols[c] = ProjectionIndexRowGroupCodec.decodeBooleanWords(body, presWords);
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
          final ProjectionIndexRowGroupCodec.Cursor dictCur =
              openColumnSegment(descriptor, resolver, dictColumnSegmentId(c), SEG_KIND_DICT, inlineOffsets);
          dicts[c] = decodeDictColumnSegmentPayload(dictCur);
          dictIdCols[c] = ProjectionIndexRowGroupCodec.decodePackedIds(body, rowCount);
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
          final ProjectionIndexRowGroupCodec.Cursor dictCur =
              openColumnSegment(descriptor, resolver, dictColumnSegmentId(c), SEG_KIND_DICT, inlineOffsets);
          dicts[c] = decodeDictColumnSegmentPayload(dictCur);
          // Counts first, then the flat element run whose length is their sum — the same order the
          // encoder writes, and the reason no separate total is stored.
          final int[] counts = ProjectionIndexRowGroupCodec.decodePackedIds(body, rowCount);
          int total = 0;
          for (int r = 0; r < rowCount; r++) {
            total += counts[r];
          }
          setCountCols[c] = counts;
          setElemCols[c] = ProjectionIndexRowGroupCodec.decodePackedIds(body, total);
        }
        default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
      }
    }

    final byte[] direct = writeRawDirect(rowCount, columnCount, kinds, firstRecordKey,
        lastRecordKey, recordKeys, columnMin, columnMax, numericCols, booleanCols, dictIdCols,
        dicts, setCountCols, setElemCols, columnFlags, presence, presWords);
    if (verifyDirectAssembly) {
      final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.reconstruct(kinds, rowCount,
          firstRecordKey, lastRecordKey, recordKeys, columnMin, columnMax,
          numericCols, booleanCols, dictIdCols, dicts, presence, columnFlags);
      final byte[] viaPage = page.serialize();
      if (!Arrays.equals(direct, viaPage)) {
        throw new IllegalStateException("Direct raw assembly diverged from the page-based path ("
            + direct.length + " vs " + viaPage.length + " bytes) — layout drift");
      }
    }
    return direct;
  }

  /**
   * Cross-check switch for {@link #writeRawDirect}: when set, every assembly ALSO runs the
   * historical reconstruct-then-serialize path and fails loudly on any byte difference.
   * Off in production (the direct writer is the hydrate hot path); package-private and
   * mutable so {@code ProjectionColumnScanParityTest#directAssemblyMatchesPageSerialization}
   * can pin the parity in CI — the system property remains a manual diagnostic override.
   */
  static volatile boolean verifyDirectAssembly =
      Boolean.getBoolean("sirix.projection.verifyDirectAssembly");

  /**
   * Single-buffer raw-form writer — byte-identical to
   * {@code ProjectionIndexRowGroupPage.reconstruct(...).serialize()} but with the exact output
   * size precomputed and every array bulk-copied ({@code LongBuffer.put(long[])} is an
   * intrinsified memcpy), instead of a page object, a growing {@code ByteArrayOutputStream},
   * and per-value {@code putLong} calls. Measured 2-3x on the hydrate assemble phase, which
   * dominates cold-open cost.
   */
  private static byte[] writeRawDirect(final int rowCount, final int columnCount,
      final byte[] kinds, final long firstRecordKey, final long lastRecordKey,
      final long[] recordKeys, final long[] columnMin, final long[] columnMax,
      final long[][] numericCols, final long[][] booleanCols, final int[][] dictIdCols,
      final byte[][][] dicts, final int[][] setCountCols, final int[][] setElemCols,
      final byte[] columnFlags, final long[][] presence,
      final int presWords) {
    // ---- exact size ----
    int size = 8 + 16 + columnCount;                      // header
    if (rowCount > 0) {
      size += rowCount * 8;                               // record keys
      for (int c = 0; c < columnCount; c++) {
        size += 16;                                       // min/max
        switch (kinds[c]) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
               ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE -> size += rowCount * 8;
          case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> size += presWords * 8;
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
            final byte[][] dict = dicts[c];
            int dictSize = 0;
            int dictBytes = 0;
            while (dictSize < dict.length && dict[dictSize] != null) {
              dictBytes += dict[dictSize].length;
              dictSize++;
            }
            size += 4 + dictSize * 4 + dictBytes + rowCount * 4;
          }
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
            final byte[][] dict = dicts[c];
            int dictSize = 0;
            int dictBytes = 0;
            while (dictSize < dict.length && dict[dictSize] != null) {
              dictBytes += dict[dictSize].length;
              dictSize++;
            }
            size += 4 + dictSize * 4 + dictBytes + rowCount * 4 + setElemCols[c].length * 4;
          }
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        }
      }
    }
    size += columnCount + columnCount * presWords * 8 + 9; // presence tail + footer
    final byte[] out = new byte[size];
    final ByteBuffer bb = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
    // ---- header ----
    bb.putInt(rowCount);
    bb.putInt(columnCount);
    bb.putLong(firstRecordKey);
    bb.putLong(lastRecordKey);
    bb.put(kinds, 0, columnCount);
    if (rowCount > 0) {
      putLongsBulk(bb, recordKeys, rowCount);
      for (int c = 0; c < columnCount; c++) {
        bb.putLong(columnMin[c]);
        bb.putLong(columnMax[c]);
        switch (kinds[c]) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
               ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE ->
              putLongsBulk(bb, numericCols[c], rowCount);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
              putLongsBulk(bb, booleanCols[c], presWords);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
            final byte[][] dict = dicts[c];
            int dictSize = 0;
            while (dictSize < dict.length && dict[dictSize] != null) {
              dictSize++;
            }
            bb.putInt(dictSize);
            for (int i = 0; i < dictSize; i++) {
              bb.putInt(dict[i].length);
            }
            for (int i = 0; i < dictSize; i++) {
              bb.put(dict[i], 0, dict[i].length);
            }
            final IntBuffer ib = bb.asIntBuffer();
            ib.put(dictIdCols[c], 0, rowCount);
            bb.position(bb.position() + rowCount * 4);
          }
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
            final byte[][] dict = dicts[c];
            int dictSize = 0;
            while (dictSize < dict.length && dict[dictSize] != null) {
              dictSize++;
            }
            bb.putInt(dictSize);
            for (int i = 0; i < dictSize; i++) {
              bb.putInt(dict[i].length);
            }
            for (int i = 0; i < dictSize; i++) {
              bb.put(dict[i], 0, dict[i].length);
            }
            // Same order the page's own serialize writes: counts, then the flat element run.
            final int[] counts = setCountCols[c];
            final int[] elems = setElemCols[c];
            final IntBuffer cb = bb.asIntBuffer();
            cb.put(counts, 0, rowCount);
            bb.position(bb.position() + rowCount * 4);
            final IntBuffer eb = bb.asIntBuffer();
            eb.put(elems, 0, elems.length);
            bb.position(bb.position() + elems.length * 4);
          }
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        }
      }
    }
    // ---- presence tail ----
    bb.put(columnFlags, 0, columnCount);
    if (presWords > 0) {
      for (int c = 0; c < columnCount; c++) {
        putLongsBulk(bb, presence[c], presWords);
      }
    }
    bb.putInt(columnCount + columnCount * presWords * 8); // tailLen
    bb.put(ProjectionIndexRowGroupPage.PRESENCE_TAIL_VERSION);
    bb.putInt(ProjectionIndexRowGroupPage.PRESENCE_TAIL_MAGIC);
    if (bb.position() != size) {
      throw new IllegalStateException("Direct raw assembly size drift: wrote " + bb.position()
          + " of a computed " + size + " bytes");
    }
    return out;
  }

  /** Bulk little-endian long copy — {@code LongBuffer.put(long[])} intrinsifies to memcpy. */
  private static void putLongsBulk(final ByteBuffer bb, final long[] values, final int count) {
    final LongBuffer lb = bb.asLongBuffer();
    lb.put(values, 0, count);
    bb.position(bb.position() + count * 8);
  }

  /**
   * Decode ONE column's BODY segment into a {@link ProjectionColumnStore.ColumnSlice}
   * (P5b stage 2) — the column-pruned alternative to {@link #assembleRaw}: verifies the
   * segment's byteLen + XXH3-64 against the descriptor entry, then parses flags, zone map,
   * presence, and values with the exact decoders the assembler uses. String columns are
   * rejected HERE — their dict ids are meaningless without the DICT segment, so they go
   * through {@link #decodeStringSlice}, which takes both.
   *
   * @throws IllegalStateException on verification or parse failure — callers decline to the
   *         eager whole-leaf path
   */
  static ProjectionColumnStore.ColumnSlice decodeBodySlice(final byte[] descriptor,
      final byte[] bodyColumnSegment, final int col) {
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final byte kind = RowGroupDescriptor.kind(descriptor, col);
    final int bodyId = bodyColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor body =
        openColumnSegment(descriptor, id -> id == bodyId ? bodyColumnSegment : null, bodyId, SEG_KIND_BODY, null);
    final byte flags = body.readByte();
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;
    final long[] presence = new long[presWords];
    if (rowCount == 0) {
      return new ProjectionColumnStore.ColumnSlice(0, flags, Long.MAX_VALUE, Long.MIN_VALUE,
          presence, null, null, null, null);
    }
    final long min = body.readLong();
    final long max = body.readLong();
    ProjectionIndexRowGroupCodec.decodePresenceInto(body, presence, presWords, rowCount);
    return switch (kind) {
      case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
           ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE ->
          new ProjectionColumnStore.ColumnSlice(rowCount, flags, min, max, presence,
              ProjectionIndexRowGroupCodec.decodeForBitPackedColumn(body, rowCount), null, null, null);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
          new ProjectionColumnStore.ColumnSlice(rowCount, flags, min, max, presence, null,
              ProjectionIndexRowGroupCodec.decodeBooleanWords(body, presWords), null, null);
      default -> throw new IllegalStateException("Column " + col + " (kind " + kind
          + ") is not body-only sliceable");
    };
  }

  /**
   * Decode a STRING_DICT column's BODY + DICT segments into one slice — what lets a string
   * equality run column-sliced instead of hydrating whole leaves. Same verification and
   * header discipline as {@link #decodeBodySlice}; the ids come from the BODY (width byte +
   * packed ids, the raw form's own stream) and the dictionary from the DICT segment
   * ({@code null} only for a rowless leaf, which writes no DICT segment at all).
   *
   * @throws IllegalStateException on verification or parse failure — callers decline to the
   *         eager whole-leaf path
   */
  static ProjectionColumnStore.ColumnSlice decodeStringSlice(final byte[] descriptor,
      final byte[] bodyColumnSegment, final byte @Nullable [] dictColumnSegment, final int col) {
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final byte kind = RowGroupDescriptor.kind(descriptor, col);
    if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      throw new IllegalStateException("Column " + col + " (kind " + kind + ") is not STRING_DICT");
    }
    final int bodyId = bodyColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor body =
        openColumnSegment(descriptor, id -> id == bodyId ? bodyColumnSegment : null, bodyId, SEG_KIND_BODY, null);
    final byte flags = body.readByte();
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;
    final long[] presence = new long[presWords];
    if (rowCount == 0) {
      return new ProjectionColumnStore.ColumnSlice(0, flags, Long.MAX_VALUE, Long.MIN_VALUE,
          presence, null, null, null, null);
    }
    if (dictColumnSegment == null) {
      throw new IllegalStateException("Column " + col + " has rows but no DICT segment bytes");
    }
    final long min = body.readLong();
    final long max = body.readLong();
    ProjectionIndexRowGroupCodec.decodePresenceInto(body, presence, presWords, rowCount);
    final int[] ids = ProjectionIndexRowGroupCodec.decodePackedIds(body, rowCount);
    final int dictId = dictColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor dict =
        openColumnSegment(descriptor, id -> id == dictId ? dictColumnSegment : null, dictId, SEG_KIND_DICT, null);
    return new ProjectionColumnStore.ColumnSlice(rowCount, flags, min, max, presence, null, null,
        ids, decodeDictColumnSegmentPayload(dict));
  }

  /**
   * Decode ONE {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SET} column into a slice.
   *
   * <p>Same discipline as {@link #decodeStringSlice}, with the set's shape on top: per-row element
   * COUNTS, then the flat element run whose length is their sum. The counts are what turn a flat
   * run back into rows, and storing them rather than offsets keeps the packed width down — most
   * rows hold a handful of elements while an offset grows with the leaf.
   *
   * @throws IllegalStateException on verification or parse failure
   */
  static ProjectionColumnStore.ColumnSlice decodeStringSetSlice(final byte[] descriptor,
      final byte[] bodyColumnSegment, final byte @Nullable [] dictColumnSegment, final int col) {
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final byte kind = RowGroupDescriptor.kind(descriptor, col);
    if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
      throw new IllegalStateException("Column " + col + " (kind " + kind + ") is not STRING_SET");
    }
    final int bodyId = bodyColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor body =
        openColumnSegment(descriptor, id -> id == bodyId ? bodyColumnSegment : null, bodyId,
                          SEG_KIND_BODY, null);
    final byte flags = body.readByte();
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;
    final long[] presence = new long[presWords];
    if (rowCount == 0) {
      return new ProjectionColumnStore.ColumnSlice(0, flags, Long.MAX_VALUE, Long.MIN_VALUE,
          presence, null, null, null, null);
    }
    if (dictColumnSegment == null) {
      throw new IllegalStateException("Column " + col + " has rows but no DICT segment bytes");
    }
    final long min = body.readLong();
    final long max = body.readLong();
    ProjectionIndexRowGroupCodec.decodePresenceInto(body, presence, presWords, rowCount);
    final int[] counts = ProjectionIndexRowGroupCodec.decodePackedIds(body, rowCount);
    int total = 0;
    for (int r = 0; r < rowCount; r++) {
      total += counts[r];
    }
    final int[] elems = ProjectionIndexRowGroupCodec.decodePackedIds(body, total);
    final int dictId = dictColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor dict =
        openColumnSegment(descriptor, id -> id == dictId ? dictColumnSegment : null, dictId,
                          SEG_KIND_DICT, null);
    return new ProjectionColumnStore.ColumnSlice(rowCount, flags, min, max, presence, null, null,
        elems, decodeDictColumnSegmentPayload(dict), counts);
  }

  /**
   * Decode one leaf's KEYS segment into its per-row record keys — the sorted collections'
   * substrate, verified against the descriptor like every other segment read. Empty for a
   * rowless leaf (whose KEYS segment carries only the first/last mirror).
   *
   * @throws IllegalStateException on verification or parse failure
   */
  static long[] decodeKeysSlice(final byte[] descriptor, final byte[] keysColumnSegment) {
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final int keysId = keysColumnSegmentId();
    final ProjectionIndexRowGroupCodec.Cursor in =
        openColumnSegment(descriptor, id -> id == keysId ? keysColumnSegment : null, keysId, SEG_KIND_KEYS, null);
    in.readLong();  // firstRecordKey — the descriptor mirrors it; the rows are what matters here
    in.readLong();  // lastRecordKey
    return rowCount > 0 ? ProjectionIndexRowGroupCodec.decodeRecordKeys(in, rowCount) : EMPTY_KEYS;
  }

  private static final long[] EMPTY_KEYS = new long[0];

  /**
   * Column-scoped provenance primitive (5.1-7): the flags byte from a BODY segment's bytes —
   * segment TRUTH, as opposed to the descriptor's mirror. Validates the segment header.
   */
  public static byte bodyColumnSegmentFlags(final byte[] bodyColumnSegment) {
    checkColumnSegmentHeader(bodyColumnSegment, SEG_KIND_BODY);
    if (bodyColumnSegment.length < SEGMENT_HEADER_BYTES + 1) {
      throw new IllegalStateException("Truncated BODY segment: no flags byte after the header");
    }
    return bodyColumnSegment[SEGMENT_HEADER_BYTES];
  }

  // ==================== internals ====================

  /**
   * Resolve, verify (length + hash against the descriptor), and open a segment positioned
   * after its header.
   */
  private static ProjectionIndexRowGroupCodec.Cursor openColumnSegment(final byte[] descriptor,
      final SegmentResolver resolver, final int columnSegmentId, final byte expectedKind,
      final int @Nullable [] inlineOffsets) {
    // Hybrid: an inline segment's bytes live in the descriptor itself — resolve them there and
    // never touch the (page) resolver, so a referenced-segment resolver stays oblivious to inline.
    // The single entryIndexOf here is reused by verifyColumnSegment (no second lookup), and when the
    // caller precomputed inlineOffsets for the whole assembly the inline slice is an O(1) lookup
    // instead of a per-segment prefix walk.
    final int entry = RowGroupDescriptor.entryIndexOf(descriptor, columnSegmentId);
    final byte[] segment;
    if (entry >= 0 && RowGroupDescriptor.entryIsInline(descriptor, entry)) {
      final int off = inlineOffsets != null
          ? inlineOffsets[entry]
          : RowGroupDescriptor.inlineDataOffset(descriptor, entry);
      segment = RowGroupDescriptor.inlineColumnSegmentBytesAt(descriptor, entry, off);
    } else {
      segment = resolver.segment(columnSegmentId);
    }
    verifyColumnSegment(descriptor, segment, columnSegmentId, expectedKind, entry);
    return new ProjectionIndexRowGroupCodec.Cursor(segment, SEGMENT_HEADER_BYTES);
  }

  /**
   * Full segment verification against its descriptor entry — exact byteLen, XXH3-64 content
   * hash, and the PIXS header — without opening a cursor. The byte-level column cache
   * ({@code ProjectionColumnStore#columnBytes}) verifies once at fill; the fused fold
   * kernels then trust the cached bytes.
   *
   * @throws IllegalStateException on any mismatch (callers decline through the established
   *         fail-soft flow)
   */
  static void verifyColumnSegment(final byte[] descriptor, final byte @Nullable [] segment,
      final int columnSegmentId, final byte expectedKind) {
    verifyColumnSegment(descriptor, segment, columnSegmentId, expectedKind,
        RowGroupDescriptor.entryIndexOf(descriptor, columnSegmentId));
  }

  /** {@link #verifyColumnSegment} for a caller that already resolved the descriptor entry index. */
  static void verifyColumnSegment(final byte[] descriptor, final byte @Nullable [] segment,
      final int columnSegmentId, final byte expectedKind, final int entry) {
    if (entry < 0) {
      throw new IllegalStateException("Missing descriptor entry for columnSegmentId=" + columnSegmentId);
    }
    if (segment == null) {
      throw new IllegalStateException("Missing segment bytes for columnSegmentId=" + columnSegmentId
          + " (descriptor lists " + RowGroupDescriptor.entryByteLen(descriptor, entry) + " bytes)");
    }
    if (segment.length != RowGroupDescriptor.entryByteLen(descriptor, entry)) {
      throw new IllegalStateException("Segment length mismatch for columnSegmentId=" + columnSegmentId + ": "
          + segment.length + " != descriptor " + RowGroupDescriptor.entryByteLen(descriptor, entry));
    }
    if (contentHash(segment) != RowGroupDescriptor.entryContentHash(descriptor, entry)) {
      throw new IllegalStateException("Segment content-hash mismatch for columnSegmentId=" + columnSegmentId
          + " — corrupted segment page or dangling side-map reference");
    }
    checkColumnSegmentHeader(segment, expectedKind);
  }

  /** DICT payload modes: raw entry stream vs FSST-compressed entries behind a symbol table. */
  private static final byte DICT_MODE_RAW = 0;
  private static final byte DICT_MODE_FSST = 1;

  /**
   * A DICT segment carrying, beside each value, the number of ROWS on this leaf whose set contains
   * it. Written only for {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SET} columns.
   *
   * <p>This is what lets a bare {@code count(... satisfies $g eq lit)} skip the BODY segment
   * entirely. The BODY holds a per-row cardinality and the flat element run — the bulk of the
   * column — and the current path fetches all of it and visits every element. With a per-value row
   * count the same answer is: find the literal in this leaf's dictionary, read its count, add it to
   * the running total. Measured on the movies corpus the dictionary is 41 entries against 6.2M
   * elements, so the counts cost a few hundred bytes per leaf and replace a scan of the whole
   * column.
   *
   * <p>ROWS, not occurrences. A record listing the same genre twice must count once, and the
   * encoder is the only place that can tell — it sees each row's whole element run, so it counts
   * distinct values per row. Deriving this later from occurrence counts would be wrong for exactly
   * the data that makes the optimisation attractive, and wrong in the direction of over-counting.
   */
  private static final byte DICT_MODE_RAW_ROW_COUNTS = 2;

  /** {@link #DICT_MODE_RAW_ROW_COUNTS}, with the dictionary FSST-compressed. */
  private static final byte DICT_MODE_FSST_ROW_COUNTS = 3;

  /**
   * DICT segment payload (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.7): a mode byte, then
   * either the raw entry stream (count, lengths, concatenated UTF-8 — byte-compatible with
   * the monolithic codec's dictionary half) or, for high-cardinality dictionaries that pass
   * {@code FSSTCompressor}'s existing gates AND actually compress, a per-segment symbol table
   * followed by per-entry FSST streams. FSST lives in the PERSISTED form only — decode
   * restores plain UTF-8 dictionary bytes, so the raw scan form (and every kernel comparing
   * dictionary bytes raw) is untouched. Training input is the dictionary in interning order
   * (deterministic), so identical re-encodes hash identically — the carry-forward no-op
   * contract (5.2-n) holds.
   */
  /**
   * ROWS on this leaf whose set contains each dictionary value, indexed by dict id.
   *
   * <p>Rows, not occurrences: a record listing the same genre twice must count once. The encoder is
   * the only place that can tell, because it is the only place that sees a row's whole element run
   * — which is why this is computed here rather than derived later from occurrence counts, where
   * the duplicate is already indistinguishable from two records.
   *
   * <p>The {@code lastRow} marker is the dedup: a value already credited to this row has the row's
   * index parked in its slot, so a repeat is a comparison rather than a set lookup.
   */
  static long[] valueRowCounts(final byte[][] dict, final int[] setCounts, final int[] setElems,
      final int rowCount) {
    final int dictSize = ProjectionIndexRowGroupCodec.dictSizeOf(dict);
    if (dictSize == 0 || setCounts == null || setElems == null) {
      return null;
    }
    final long[] counts = new long[dictSize];
    final int[] lastRow = new int[dictSize];
    java.util.Arrays.fill(lastRow, -1);
    int cursor = 0;
    for (int row = 0; row < rowCount; row++) {
      final int n = setCounts[row];
      for (int k = 0; k < n; k++) {
        final int id = setElems[cursor + k];
        if (id >= 0 && id < dictSize && lastRow[id] != row) {
          lastRow[id] = row;
          counts[id]++;
        }
      }
      cursor += n;
    }
    return counts;
  }

  private static void encodeDictColumnSegmentPayload(final ByteArrayOutputStream out,
      final byte[][] dict, final long @Nullable [] rowCounts) {
    final int dictSize = ProjectionIndexRowGroupCodec.dictSizeOf(dict);
    int totalBytes = 0;
    for (int i = 0; i < dictSize; i++) {
      totalBytes += dict[i].length;
    }
    if (dictSize >= FSSTCompressor.MIN_SAMPLES_FOR_TABLE
        && totalBytes >= FSSTCompressor.MIN_TOTAL_BYTES_FOR_TABLE) {
      final List<byte[]> entries = new ArrayList<>(dictSize);
      for (int i = 0; i < dictSize; i++) {
        entries.add(dict[i]);
      }
      final byte[] table = FSSTCompressor.buildSymbolTable(entries);
      if (table != null && FSSTCompressor.isCompressionBeneficial(entries, table)) {
        // Parse the symbol table ONCE and encode every entry against the parsed form — the
        // same lazy-parsed-table discipline as KeyValueLeafPage's per-page FSST wiring; the
        // byte[]-table overloads re-parse per call, an O(dictSize × tableLen) waste.
        final byte[][] parsedSymbols = FSSTCompressor.parsedFor(table);
        out.write(rowCounts == null ? DICT_MODE_FSST : DICT_MODE_FSST_ROW_COUNTS);
        ProjectionIndexRowGroupCodec.putIntLE(out, table.length);
        out.write(table, 0, table.length);
        ProjectionIndexRowGroupCodec.putIntLE(out, dictSize);
        for (int i = 0; i < dictSize; i++) {
          final byte[] encoded = FSSTCompressor.encode(dict[i], parsedSymbols);
          ProjectionIndexRowGroupCodec.putIntLE(out, encoded.length);
          out.write(encoded, 0, encoded.length);
        }
        writeRowCounts(out, rowCounts, dictSize);
        return;
      }
    }
    out.write(rowCounts == null ? DICT_MODE_RAW : DICT_MODE_RAW_ROW_COUNTS);
    ProjectionIndexRowGroupCodec.encodeDictEntries(out, dict);
    writeRowCounts(out, rowCounts, dictSize);
  }

  /**
   * Append the per-value row counts, bit-packed to the widest count on the leaf.
   *
   * <p>A leaf holds at most {@code MAX_ROWS} rows, so a count needs at most 10 bits and usually
   * fewer — writing longs here would cost more than the dictionary it annotates.
   */
  private static void writeRowCounts(final ByteArrayOutputStream out, final long @Nullable [] counts,
      final int dictSize) {
    if (counts == null) {
      return;
    }
    long max = 0;
    for (int i = 0; i < dictSize; i++) {
      if (counts[i] > max) {
        max = counts[i];
      }
    }
    final int width = max == 0 ? 0 : 64 - Long.numberOfLeadingZeros(max);
    out.write(width);
    if (width > 0) {
      final ProjectionIndexRowGroupCodec.BitWriter bw = new ProjectionIndexRowGroupCodec.BitWriter(out);
      for (int i = 0; i < dictSize; i++) {
        bw.write(counts[i], width);
      }
      bw.flush();
    }
  }

  /**
   * Largest SET_COUNTS payload worth writing.
   *
   * <p>Tied to the inline cap, not chosen freely: past it the segment is REFERENCED, which means a
   * page fetch — and a page fetch is exactly what this segment exists to avoid. A column whose
   * values do not fit is better served by the scanning path, so nothing is written and the reader
   * falls back.
   */
  private static final int MAX_SET_COUNTS_BYTES =
      Integer.getInteger("sirix.projection.maxSetCountsBytes", DEFAULT_INLINE_MAX_SEGMENT_BYTES);

  /**
   * {@code [short valueCount] ( [short len][len bytes value][short rowCount] )*}
   *
   * <p>Values inline rather than hashed: a hash collision would over-count silently, and the whole
   * point of this segment is to answer without reading anything else that could confirm it. At
   * these sizes the bytes cost less than the doubt.
   *
   * @return the payload, or {@code null} when it would exceed {@link #MAX_SET_COUNTS_BYTES}
   */
  private static byte[] encodeSetCountsPayload(final byte[][] dict, final long @Nullable [] counts) {
    if (dict == null || counts == null) {
      return null;
    }
    final int dictSize = ProjectionIndexRowGroupCodec.dictSizeOf(dict);
    if (dictSize == 0 || dictSize > 0xFFFF) {
      return null;
    }
    int size = 2;
    for (int i = 0; i < dictSize; i++) {
      size += 2 + dict[i].length + 2;
      if (size > MAX_SET_COUNTS_BYTES) {
        return null;
      }
    }
    final ByteArrayOutputStream out = newColumnSegmentStream(SEG_KIND_SET_COUNTS);
    putShortLE(out, dictSize);
    for (int i = 0; i < dictSize; i++) {
      putShortLE(out, dict[i].length);
      out.write(dict[i], 0, dict[i].length);
      // A leaf holds at most MAX_ROWS rows, so a row count always fits a short.
      putShortLE(out, (int) Math.min(counts[i], 0xFFFF));
    }
    return out.toByteArray();
  }

  private static void putShortLE(final ByteArrayOutputStream out, final int v) {
    out.write(v & 0xFF);
    out.write((v >>> 8) & 0xFF);
  }

  /** Read the counts {@link #writeRowCounts} wrote; {@code null} when the mode carries none. */
  private static int[] readRowCounts(final ProjectionIndexRowGroupCodec.Cursor in,
      final int dictSize) {
    final int width = in.readByte() & 0xFF;
    // int, not long: a leaf holds at most MAX_ROWS rows, so a per-value row count cannot exceed it
    // and the existing int unpacker serves.
    final int[] counts = new int[dictSize];
    if (width > 0) {
      ProjectionIndexRowGroupCodec.unpackIntsInto(in, dictSize, width, counts);
    }
    return counts;
  }

  /** Inverse of {@link #encodeDictColumnSegmentPayload}; restores plain UTF-8 dictionary bytes. */
  /**
   * Rows on this leaf whose set contains {@code literal}, from an inline SET_COUNTS payload.
   *
   * @return the count, or {@code -1} when the payload is absent or unreadable
   */
  public static long setCountFor(final byte[] descriptor, final byte @Nullable [] segment,
      final int col, final byte[] literal) {
    if (segment == null) {
      return -1;
    }
    final int segId = setCountsColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor in =
        openColumnSegment(descriptor, id -> id == segId ? segment : null, segId,
                          SEG_KIND_SET_COUNTS, null);
    final int values = readShortU(in);
    for (int i = 0; i < values; i++) {
      final int len = readShortU(in);
      final byte[] value = in.readBytes(len);
      final int count = readShortU(in);
      if (java.util.Arrays.equals(value, literal)) {
        return count;
      }
    }
    return 0;   // the leaf lists its values exhaustively, so absence here is a real zero
  }

  /** Little-endian unsigned short, mirroring {@link #putShortLE}. */
  private static int readShortU(final ProjectionIndexRowGroupCodec.Cursor in) {
    final int lo = in.readByte() & 0xFF;
    final int hi = in.readByte() & 0xFF;
    return lo | (hi << 8);
  }

  /** A leaf's dictionary beside the per-value ROW counts, when the segment carries them. */
  public record DictWithRowCounts(byte[][] dict, int @Nullable [] rowCounts) {
  }

  /**
   * Decode a DICT segment INCLUDING its per-value row counts.
   *
   * <p>This is the whole point of putting the counts in the dictionary segment: a membership count
   * needs the dictionary anyway (to resolve the literal to an id) and nothing else. The BODY
   * segment — a per-row cardinality plus the flat element run, and the bulk of the column — is
   * never fetched.
   *
   * @return dictionary and counts; {@code rowCounts} is {@code null} for a segment written before
   *         the counts existed or for a scalar string column, and the caller falls back
   */
  static DictWithRowCounts decodeDictWithRowCounts(final byte[] descriptor,
      final byte @Nullable [] dictColumnSegment, final int col) {
    if (dictColumnSegment == null) {
      return null;
    }
    final int dictId = dictColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor in =
        openColumnSegment(descriptor, id -> id == dictId ? dictColumnSegment : null, dictId,
                          SEG_KIND_DICT, null);
    final int mode = in.readByte() & 0xFF;
    final byte[][] dict;
    final boolean hasCounts;
    switch (mode) {
      case DICT_MODE_RAW -> {
        dict = ProjectionIndexRowGroupCodec.decodeDictEntries(in);
        hasCounts = false;
      }
      case DICT_MODE_RAW_ROW_COUNTS -> {
        dict = ProjectionIndexRowGroupCodec.decodeDictEntries(in);
        hasCounts = true;
      }
      case DICT_MODE_FSST, DICT_MODE_FSST_ROW_COUNTS -> {
        dict = decodeFsstDictEntries(in);
        hasCounts = mode == DICT_MODE_FSST_ROW_COUNTS;
      }
      default -> throw new IllegalStateException("Unknown DICT segment mode " + mode);
    }
    if (!hasCounts) {
      return new DictWithRowCounts(dict, null);
    }
    return new DictWithRowCounts(dict,
                                 readRowCounts(in, ProjectionIndexRowGroupCodec.dictSizeOf(dict)));
  }

  private static byte[][] decodeDictColumnSegmentPayload(final ProjectionIndexRowGroupCodec.Cursor in) {
    final int mode = in.readByte() & 0xFF;
    // The *_ROW_COUNTS modes are the same dictionary with a counts table appended. Readers that
    // only want the entries accept them and simply stop before the counts — otherwise writing the
    // counts would break every existing consumer of a set column's dictionary.
    return switch (mode) {
      case DICT_MODE_RAW, DICT_MODE_RAW_ROW_COUNTS ->
          ProjectionIndexRowGroupCodec.decodeDictEntries(in);
      case DICT_MODE_FSST, DICT_MODE_FSST_ROW_COUNTS -> decodeFsstDictEntries(in);
      default -> throw new IllegalStateException("Unknown DICT segment mode " + mode
                                                     + " — written by a newer version");
    };
  }

  /** The FSST-compressed dictionary entries, with the symbol table parsed once. */
  private static byte[][] decodeFsstDictEntries(final ProjectionIndexRowGroupCodec.Cursor in) {
    final int tableLen = in.readInt();
    final byte[] table = in.readBytes(tableLen);
    final byte[][] parsedSymbols = FSSTCompressor.parsedFor(table);
    final int dictSize = in.readInt();
    final byte[][] dict = new byte[Math.max(16, dictSize)][];
    for (int i = 0; i < dictSize; i++) {
      final int len = in.readInt();
      final byte[] encoded = in.readBytes(len);
      dict[i] = FSSTCompressor.decode(encoded, parsedSymbols);
    }
    return dict;
  }


  private static void checkColumnSegmentHeader(final byte[] segment, final byte expectedKind) {
    if (segment.length < SEGMENT_HEADER_BYTES
        || ProjectionIndexRowGroupCodec.getIntLE(segment, 0) != SEGMENT_MAGIC) {
      throw new IllegalStateException("Not a projection segment (missing PIXS magic)");
    }
    if (segment[4] != SEGMENT_VERSION) {
      throw new IllegalStateException("Unknown segment version " + segment[4] + " (expected "
          + SEGMENT_VERSION + ")");
    }
    if (segment[5] != expectedKind) {
      throw new IllegalStateException("Segment kind mismatch: " + segment[5] + " != expected " + expectedKind);
    }
  }
}
