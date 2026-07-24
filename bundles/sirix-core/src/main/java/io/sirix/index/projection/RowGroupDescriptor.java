/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

/**
 * The projection leaf <em>descriptor</em> ("PIXD"): the tiny HOT slot value of the
 * segment-directory storage layout (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3). It carries
 * the raw-form header (rowCount, columnCount, kinds, record-key fences) plus one fixed-size
 * entry per segment: {@code columnSegmentId}, exact {@code byteLen}, XXH3-64 {@code contentHash}
 * (the write-path no-op comparator AND the read-path integrity check — segment pages persist
 * behind bare offset keys with no checksum of their own), and the per-column stats mirror
 * (flags, min, max) for descriptor-only pruning decisions.
 *
 * <p><b>Mirror discipline (5.2-k):</b> flags/min/max here are a cache of the segment truth
 * (the BODY segment carries the authoritative copies). Pruning may consult the mirror;
 * provenance gates must read segment bytes.
 *
 * <p>Wire layout (all little-endian, fixed offsets — the readers are positional and
 * allocation-free for the scan hot path):
 *
 * <pre>
 *   int    MAGIC = "PIXD"                        [offset 0]
 *   byte   VERSION = 1                           [offset 4]
 *   int    rowCount                              [offset 5]
 *   short  columnCount                           [offset 9]
 *   long   firstRecordKey                        [offset 11]
 *   long   lastRecordKey                         [offset 19]
 *   byte[columnCount] kinds                      [offset 27]
 *   short  columnSegmentCount                              [offset 27 + columnCount]
 *   per entry (ENTRY_BYTES = 31):
 *     short columnSegmentId; int byteLen; long contentHash; byte colFlags; long min; long max
 *   inline region: for each INLINE entry, its full segment bytes (PIXS header
 *                  included), concatenated in ascending-columnSegmentId (= entry) order
 * </pre>
 *
 * <p><b>Hybrid inline/referenced storage</b> (docs/PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md,
 * mirroring {@link io.sirix.page.KeyValueLeafPage}'s inline-record-or-{@link
 * io.sirix.page.OverflowPage}-spill split): each entry is either REFERENCED (bytes in a
 * side-map {@code OverflowPage}, as before) or INLINE (bytes in the trailing region of this
 * slot value, the HOT analogue of a record living inline in the slot heap). The storage class
 * is the high bit of the entry's {@code byteLen} field ({@link #SEG_INLINE_FLAG}); {@code
 * byteLen} readers mask it off, so a referenced entry serializes byte-identically to the
 * pre-hybrid v1 layout — the format is a compatible superset and needs no version bump. Inline
 * bytes are the <em>same</em> bytes a page would hold (header included), so verification and
 * the maintenance no-op hash stay uniform across both classes.
 *
 * <p>A zero-length slot value is the leaf tombstone; a descriptor with {@code rowCount == 0}
 * is a live empty leaf (deletes can legitimately empty a mid-store leaf) — the two are
 * distinct states by design (5.1-4).
 */
public final class RowGroupDescriptor {

  /** Leading magic ("PIXD" little-endian). */
  public static final int MAGIC = 0x44584950;

  /** Layout version; bumped on any wire change. v2 widened the entry columnSegmentId 1→2 bytes. */
  public static final byte VERSION = 2;

  /**
   * Column cap imposed by the 16-bit columnSegmentId space of the HOT side-map composite key:
   * {@code SEGMENTS_PER_COLUMN · c + 2 ≤ MAX_OVERFLOW_PAGE_REF_SUB_ID}. Derived — not restated — from the
   * id-scheme constants so the invariant has a single authority (a fourth per-column segment
   * kind automatically tightens this cap). With a 16-bit sub-id this is {@code (65535-2)/3 = 21844}
   * columns (was 84 at 8 bits); the on-disk entry columnSegmentId field is 2 bytes to match.
   */
  public static final int MAX_COLUMNS =
      (HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID - 2) / ProjectionIndexColumnSegmentCodec.SEGMENTS_PER_COLUMN;

  /** Fixed size of one segment entry (2-byte columnSegmentId + int byteLen + long hash + byte flags + 2 longs). */
  public static final int ENTRY_BYTES = 2 + 4 + 8 + 1 + 8 + 8;

  /**
   * High bit of an entry's {@code byteLen} int marking the segment as INLINE (bytes in this
   * descriptor's trailing region) rather than REFERENCED (bytes in a side-map page). Safe to
   * overload the sign bit: a segment is capped at {@link #MAX_SEGMENT_BYTES} (16 MB ≪ 2^31), so
   * the true length never touches it. {@link #entryByteLen} masks it off; {@link
   * #entryIsInline} tests it. Chosen over a {@code colFlags} bit so the column-provenance mirror
   * (UNREPRESENTABLE/NON_INTEGRAL/PURE_DOUBLE_SOURCE) stays byte-for-byte untouched.
   */
  public static final int SEG_INLINE_FLAG = 0x8000_0000;

  /**
   * Upper bound on one projection segment or serialized descriptor. This is the PROJECTION's own
   * domain limit, not a page-layer one: {@link OverflowPage} deliberately imposes no ceiling
   * (a node record spilled there is unbounded), so bounding a projection segment is this layer's
   * job. It exists to keep {@link #SEG_INLINE_FLAG}'s overload of the {@code byteLen} sign bit
   * sound — the true length must stay far below 2^31 — and to fail a runaway encode loudly at the
   * producer instead of at some later assembly.
   */
  public static final int MAX_SEGMENT_BYTES = 16 * 1024 * 1024;

  /**
   * Upper bound on a descriptor stored as an <b>inline</b> HOT leaf slot value (the descriptor-directory
   * layout), whose on-disk length prefix is an unsigned short ({@code HOTLeafPage} enforces the same
   * 0xFFFF limit independently). The segment-slot layout stores its zone-map descriptor via
   * {@code putBlob}, which spills past this into an {@link OverflowPage}, so it is NOT bounded here —
   * only by {@link #MAX_SEGMENT_BYTES}. Enforced at the descriptor-directory
   * {@code writeSlotValue} call site, not in {@link #serialize} (which caps at the projection's
   * segment ceiling so a wide segment-slot descriptor can still be produced).
   */
  public static final int MAX_SLOT_VALUE_BYTES = 0xFFFF;

  /**
   * Smallest structurally possible descriptor: fixed head through the kinds offset (zero
   * columns) plus the columnSegmentCount short. Cheap plausibility floor for slice-level readers
   * that only need head fields without a full {@link #validate}.
   */
  public static final int MIN_BYTES = 27 + 2;

  private static final int OFF_ROW_COUNT = 5;
  private static final int OFF_COLUMN_COUNT = 9;
  private static final int OFF_FIRST_KEY = 11;
  private static final int OFF_LAST_KEY = 19;
  private static final int OFF_KINDS = 27;

  private RowGroupDescriptor() {
  }

  // ==================== write ====================

  /**
   * Serialize an all-referenced descriptor (no inline segments) — the pre-hybrid layout. Entry
   * arrays are parallel, {@code columnSegmentCount} entries each; entries must be sorted by ascending
   * {@code columnSegmentId} (binary-searchable, deterministic bytes).
   */
  public static byte[] serialize(final int rowCount, final long firstRecordKey, final long lastRecordKey,
      final byte[] kinds, final int columnSegmentCount, final int[] columnSegmentIds, final int[] byteLens,
      final long[] contentHashes, final byte[] colFlags, final long[] mins, final long[] maxs) {
    return serialize(rowCount, firstRecordKey, lastRecordKey, kinds, columnSegmentCount, columnSegmentIds, byteLens,
        contentHashes, colFlags, mins, maxs, null, null);
  }

  /**
   * Serialize a descriptor with per-segment storage classes (hybrid inline/referenced). When
   * {@code inline != null && inline[i]}, entry {@code i}'s {@code byteLen} field is stored with
   * {@link #SEG_INLINE_FLAG} set and {@code segmentBytes[i]} (the segment's full bytes, PIXS
   * header included) is appended to the trailing inline region in ascending-{@code columnSegmentId}
   * (= entry) order; those are the same bytes a referenced page would hold. Entries with
   * {@code inline[i] == false} (or {@code inline == null}) are referenced as before and their
   * {@code segmentBytes[i]} is ignored. {@code byteLens[i]} always carries the true segment
   * length for both classes.
   */
  public static byte[] serialize(final int rowCount, final long firstRecordKey, final long lastRecordKey,
      final byte[] kinds, final int columnSegmentCount, final int[] columnSegmentIds, final int[] byteLens,
      final long[] contentHashes, final byte[] colFlags, final long[] mins, final long[] maxs,
      final boolean @Nullable [] inline, final byte @Nullable [][] segmentBytes) {
    if (kinds.length > MAX_COLUMNS) {
      throw new IllegalArgumentException("columnCount " + kinds.length + " exceeds MAX_COLUMNS=" + MAX_COLUMNS);
    }
    if (rowCount < 0 || rowCount > ProjectionIndexRowGroupPage.MAX_ROWS) {
      throw new IllegalArgumentException("rowCount out of range: " + rowCount);
    }
    if (columnSegmentCount < 0 || columnSegmentCount > 0xFFFF) {
      throw new IllegalArgumentException("columnSegmentCount out of range: " + columnSegmentCount);
    }
    if (columnSegmentIds.length < columnSegmentCount || byteLens.length < columnSegmentCount || contentHashes.length < columnSegmentCount
        || colFlags.length < columnSegmentCount || mins.length < columnSegmentCount || maxs.length < columnSegmentCount) {
      throw new IllegalArgumentException("entry array shorter than columnSegmentCount=" + columnSegmentCount
          + ": columnSegmentIds=" + columnSegmentIds.length + " byteLens=" + byteLens.length
          + " contentHashes=" + contentHashes.length + " colFlags=" + colFlags.length
          + " mins=" + mins.length + " maxs=" + maxs.length);
    }
    // Inline region size + per-entry consistency: an inline entry must supply bytes matching its
    // recorded byteLen, so a later positional read (offset = Σ prior inline byteLens) never drifts.
    // long accumulator: at the u16 columnSegmentCount ceiling the sum of per-entry byteLens can
    // exceed 2^31 and wrap negative, which would slip past the size guard below and surface as a
    // NegativeArraySizeException from the allocation instead of an attributable error. validate()
    // already accumulates the same quantity as a long; the writer must agree with it.
    long inlineRegion = 0;
    for (int i = 0; i < columnSegmentCount; i++) {
      if (byteLens[i] < 0) {
        throw new IllegalArgumentException("negative byteLen " + byteLens[i] + " at entry " + i
            + " — refusing to persist a descriptor that can never verify");
      }
      if (inline != null && inline[i]) {
        if (segmentBytes == null || segmentBytes[i] == null) {
          throw new IllegalArgumentException("inline entry " + i + " (columnSegmentId " + columnSegmentIds[i]
              + ") has no bytes");
        }
        if (segmentBytes[i].length != byteLens[i]) {
          throw new IllegalArgumentException("inline entry " + i + " byteLen " + byteLens[i]
              + " != bytes " + segmentBytes[i].length);
        }
        inlineRegion += byteLens[i];
      }
    }
    final int entriesEnd = OFF_KINDS + kinds.length + 2 + columnSegmentCount * ENTRY_BYTES;
    // Absolute ceiling: a descriptor is stored either as an inline HOT slot value (descriptor-directory
    // layout, capped at MAX_SLOT_VALUE_BYTES by writeSlotValue) or spilled into ONE OverflowPage
    // (segment-slot layout, via putBlob). Cap here at the projection's own segment ceiling — the
    // layout-specific u16 limit is enforced at the descriptor-directory write site, NOT here, so a
    // wide segment-slot descriptor (thousands of columns) can still be produced. Computed in long
    // so an overflowing sum is REJECTED rather than wrapping past this guard.
    final long totalSizeLong = (long) entriesEnd + inlineRegion;
    if (totalSizeLong > MAX_SEGMENT_BYTES) {
      throw new IllegalArgumentException("descriptor of " + totalSizeLong + " bytes exceeds the projection"
          + " segment ceiling " + MAX_SEGMENT_BYTES);
    }
    final int totalSize = (int) totalSizeLong;
    final byte[] out = new byte[totalSize];
    putIntLE(out, 0, MAGIC);
    out[4] = VERSION;
    putIntLE(out, OFF_ROW_COUNT, rowCount);
    putShortLE(out, OFF_COLUMN_COUNT, (short) kinds.length);
    putLongLE(out, OFF_FIRST_KEY, firstRecordKey);
    putLongLE(out, OFF_LAST_KEY, lastRecordKey);
    System.arraycopy(kinds, 0, out, OFF_KINDS, kinds.length);
    int pos = OFF_KINDS + kinds.length;
    putShortLE(out, pos, (short) columnSegmentCount);
    pos += 2;
    int prevId = -1;
    for (int i = 0; i < columnSegmentCount; i++) {
      final int id = columnSegmentIds[i];
      if (id < 0 || id > HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID) {
        throw new IllegalArgumentException("columnSegmentId out of the 16-bit entry field range [0, "
            + HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID + "]: " + id);
      }
      if (id <= prevId) {
        throw new IllegalArgumentException("segment entries must be sorted by ascending columnSegmentId: "
            + id + " after " + prevId);
      }
      prevId = id;
      final boolean isInline = inline != null && inline[i];
      putShortLE(out, pos, (short) id);
      putIntLE(out, pos + 2, isInline ? (byteLens[i] | SEG_INLINE_FLAG) : byteLens[i]);
      putLongLE(out, pos + 6, contentHashes[i]);
      out[pos + 14] = colFlags[i];
      putLongLE(out, pos + 15, mins[i]);
      putLongLE(out, pos + 23, maxs[i]);
      pos += ENTRY_BYTES;
    }
    // Trailing inline region, entry order (= ascending columnSegmentId, matching the read-side offset walk).
    if (inline != null) {
      int inlinePos = entriesEnd;
      for (int i = 0; i < columnSegmentCount; i++) {
        if (inline[i]) {
          System.arraycopy(segmentBytes[i], 0, out, inlinePos, byteLens[i]);
          inlinePos += byteLens[i];
        }
      }
    }
    return out;
  }

  /**
   * Re-serialize {@code d} with every segment marked REFERENCED (no inline region) — the
   * zone-map-only form the segment-slot layout stores, where each segment's bytes live in its own
   * slot rather than inline in the descriptor. Preserves every entry's columnSegmentId / byteLen / hash /
   * flags / min / max; only the storage-class (inline) bit is cleared, so a later
   * {@code assembleRaw} resolves every segment through the resolver (its slot) instead of the
   * inline region.
   *
   * <p>Returns {@code d} itself when no entry is inline — an already-zone-map-only descriptor is
   * exactly what this produces, so the rebuild below would allocate seven arrays and re-serialize
   * the whole descriptor to reproduce identical bytes. This runs on EVERY segment-slot row-group
   * write, so at wide-column sizes the copy dominated the write.</p>
   */
  public static byte[] toZoneMapOnly(final byte[] d) {
    final int segmentCount = columnSegmentCount(d);
    boolean anyInline = false;
    for (int i = 0; i < segmentCount; i++) {
      if (entryIsInline(d, i)) {
        anyInline = true;
        break;
      }
    }
    if (!anyInline) {
      return d;
    }
    final int cc = columnCount(d);
    final byte[] kinds = new byte[cc];
    for (int i = 0; i < cc; i++) {
      kinds[i] = kind(d, i);
    }
    final int sc = columnSegmentCount(d);
    final int[] columnSegmentIds = new int[sc];
    final int[] byteLens = new int[sc];
    final long[] hashes = new long[sc];
    final byte[] flags = new byte[sc];
    final long[] mins = new long[sc];
    final long[] maxs = new long[sc];
    for (int i = 0; i < sc; i++) {
      columnSegmentIds[i] = entryColumnSegmentId(d, i);
      byteLens[i] = entryByteLen(d, i); // masks SEG_INLINE_FLAG → true length
      hashes[i] = entryContentHash(d, i);
      flags[i] = entryColFlags(d, i);
      mins[i] = entryMin(d, i);
      maxs[i] = entryMax(d, i);
    }
    return serialize(rowCount(d), firstRecordKey(d), lastRecordKey(d), kinds, sc, columnSegmentIds, byteLens,
        hashes, flags, mins, maxs); // all-referenced overload → no inline region
  }

  // ==================== positional readers (allocation-free) ====================

  /** {@code true} iff {@code value} starts with the descriptor magic. */
  public static boolean isDescriptor(final byte[] value) {
    return value != null && value.length >= 4 && ProjectionIndexRowGroupCodec.getIntLE(value, 0) == MAGIC;
  }

  /**
   * Structural validation: magic, version, plausible counts, exact length. Throws
   * {@link IllegalStateException} on corruption; unknown version also throws (the metadata
   * version gate triggers a rebuild before hydration ever reaches an incompatible leaf).
   */
  public static void validate(final byte[] d) {
    if (!isDescriptor(d)) {
      throw new IllegalStateException("Not a leaf descriptor (missing PIXD magic)");
    }
    if (d.length < OFF_KINDS || d[4] != VERSION) {
      throw new IllegalStateException("Unknown leaf-descriptor version "
          + (d.length > 4 ? d[4] : "<missing>") + " (expected " + VERSION + ") or truncated header");
    }
    final int rowCount = ProjectionIndexRowGroupCodec.getIntLE(d, OFF_ROW_COUNT);
    final int columnCount = getShortLE(d, OFF_COLUMN_COUNT) & 0xFFFF;
    if (rowCount < 0 || rowCount > ProjectionIndexRowGroupPage.MAX_ROWS || columnCount > MAX_COLUMNS) {
      throw new IllegalStateException("Corrupt leaf descriptor: rowCount=" + rowCount
          + " columnCount=" + columnCount);
    }
    final int segCountOff = OFF_KINDS + columnCount;
    if (d.length < segCountOff + 2) {
      throw new IllegalStateException("Corrupt leaf descriptor: truncated before columnSegmentCount");
    }
    final int columnSegmentCount = getShortLE(d, segCountOff) & 0xFFFF;
    final int entriesEnd = segCountOff + 2 + columnSegmentCount * ENTRY_BYTES;
    if (d.length < entriesEnd) {
      throw new IllegalStateException("Corrupt leaf descriptor: truncated entry table (length "
          + d.length + " < " + entriesEnd + ", columnSegmentCount=" + columnSegmentCount + ")");
    }
    // Trailing inline region: exact length = entry table end + Σ byteLen of inline entries.
    // A referenced-only descriptor has inlineTotal == 0 → the pre-hybrid length rule.
    long inlineTotal = 0;
    for (int i = 0; i < columnSegmentCount; i++) {
      if (entryIsInline(d, i)) {
        inlineTotal += entryByteLen(d, i);
      }
    }
    final long expected = (long) entriesEnd + inlineTotal;
    if (d.length != expected) {
      throw new IllegalStateException("Corrupt leaf descriptor: length " + d.length
          + " != expected " + expected + " (columnSegmentCount=" + columnSegmentCount + ", inlineBytes=" + inlineTotal + ")");
    }
  }

  public static int rowCount(final byte[] d) {
    return ProjectionIndexRowGroupCodec.getIntLE(d, OFF_ROW_COUNT);
  }

  public static int columnCount(final byte[] d) {
    return getShortLE(d, OFF_COLUMN_COUNT) & 0xFFFF;
  }

  public static long firstRecordKey(final byte[] d) {
    return ProjectionIndexRowGroupCodec.getLongLE(d, OFF_FIRST_KEY);
  }

  public static long lastRecordKey(final byte[] d) {
    return ProjectionIndexRowGroupCodec.getLongLE(d, OFF_LAST_KEY);
  }

  public static byte kind(final byte[] d, final int column) {
    return d[OFF_KINDS + column];
  }

  public static int columnSegmentCount(final byte[] d) {
    return getShortLE(d, OFF_KINDS + columnCount(d)) & 0xFFFF;
  }

  private static int entriesOffset(final byte[] d) {
    return OFF_KINDS + columnCount(d) + 2;
  }

  /**
   * Index of the entry for {@code columnSegmentId}, or {@code -1} when absent. Entries are sorted
   * ascending by columnSegmentId (serialize enforces it), so this binary-searches: the write-side
   * carry-forward loops call this once per encoded segment, and at wide-table segment counts (up to
   * {@code 3·MAX_COLUMNS} ≈ 65k) a linear scan would make those loops quadratic.
   */
  public static int entryIndexOf(final byte[] d, final int columnSegmentId) {
    final int base = entriesOffset(d);
    int lo = 0;
    int hi = columnSegmentCount(d) - 1;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      final int id = getShortLE(d, base + mid * ENTRY_BYTES) & 0xFFFF;
      if (id == columnSegmentId) {
        return mid;
      }
      if (id < columnSegmentId) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return -1;
  }

  public static int entryColumnSegmentId(final byte[] d, final int entryIndex) {
    return getShortLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES) & 0xFFFF;
  }

  /** The segment's true length in bytes, with the {@link #SEG_INLINE_FLAG} storage-class bit masked off. */
  public static int entryByteLen(final byte[] d, final int entryIndex) {
    return ProjectionIndexRowGroupCodec.getIntLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 2) & ~SEG_INLINE_FLAG;
  }

  /** {@code true} iff entry {@code entryIndex}'s bytes are stored inline in this descriptor's trailing region. */
  public static boolean entryIsInline(final byte[] d, final int entryIndex) {
    return (ProjectionIndexRowGroupCodec.getIntLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 2)
        & SEG_INLINE_FLAG) != 0;
  }

  /**
   * Absolute offset in {@code d} where inline entry {@code entryIndex}'s bytes begin: the entry
   * table end plus the summed byteLens of the inline entries preceding it (inline bytes are laid
   * out in ascending entry order). Caller must ensure the entry is inline.
   */
  public static int inlineDataOffset(final byte[] d, final int entryIndex) {
    int off = entriesOffset(d) + columnSegmentCount(d) * ENTRY_BYTES;
    for (int j = 0; j < entryIndex; j++) {
      if (entryIsInline(d, j)) {
        off += entryByteLen(d, j);
      }
    }
    return off;
  }

  /**
   * The full inline segment bytes (PIXS header included — the same bytes a referenced page holds)
   * for inline entry {@code entryIndex}, copied out for standalone verification and cursor use.
   * Caller must ensure the entry is inline.
   */
  public static byte[] inlineColumnSegmentBytes(final byte[] d, final int entryIndex) {
    return inlineColumnSegmentBytesAt(d, entryIndex, inlineDataOffset(d, entryIndex));
  }

  /**
   * {@link #inlineColumnSegmentBytes} for a caller that already knows the entry's inline data offset
   * (e.g. from a single {@link #inlineOffsets} precompute spanning a whole assembly), avoiding the
   * per-segment {@link #inlineDataOffset} prefix walk. {@code off} MUST be this entry's true inline
   * offset. Caller must ensure the entry is inline.
   */
  public static byte[] inlineColumnSegmentBytesAt(final byte[] d, final int entryIndex, final int off) {
    final int len = entryByteLen(d, entryIndex);
    // Validated descriptors satisfy this by construction; guard anyway so an unvalidated corrupt
    // inline flag surfaces as a clean IllegalStateException, not an AIOOBE or zero-padded slice.
    if (off < 0 || (long) off + len > d.length) {
      throw new IllegalStateException("inline segment [" + off + ", " + ((long) off + len)
          + ") out of descriptor bounds " + d.length + " at entry " + entryIndex);
    }
    return Arrays.copyOfRange(d, off, off + len);
  }

  /**
   * Absolute inline-data offset of every entry in one O(columnSegmentCount) pass: {@code result[i]} is the
   * inline byte offset of entry {@code i} when it is inline, or {@code -1} when it is referenced.
   * Lets a full-leaf assembly resolve all inline segments in O(columnSegmentCount) total instead of the
   * O(columnSegmentCount²) that per-segment {@link #inlineDataOffset} prefix walks would cost.
   */
  public static int[] inlineOffsets(final byte[] d) {
    final int columnSegmentCount = columnSegmentCount(d);
    final int[] offs = new int[columnSegmentCount];
    int off = entriesOffset(d) + columnSegmentCount * ENTRY_BYTES;
    for (int i = 0; i < columnSegmentCount; i++) {
      if (entryIsInline(d, i)) {
        offs[i] = off;
        off += entryByteLen(d, i);
      } else {
        offs[i] = -1;
      }
    }
    return offs;
  }

  public static long entryContentHash(final byte[] d, final int entryIndex) {
    return ProjectionIndexRowGroupCodec.getLongLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 6);
  }

  public static byte entryColFlags(final byte[] d, final int entryIndex) {
    return d[entriesOffset(d) + entryIndex * ENTRY_BYTES + 14];
  }

  public static long entryMin(final byte[] d, final int entryIndex) {
    return ProjectionIndexRowGroupCodec.getLongLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 15);
  }

  public static long entryMax(final byte[] d, final int entryIndex) {
    return ProjectionIndexRowGroupCodec.getLongLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 23);
  }

  // ==================== little-endian primitives ====================

  private static void putShortLE(final byte[] b, final int off, final short v) {
    b[off] = (byte) v;
    b[off + 1] = (byte) (v >>> 8);
  }

  private static short getShortLE(final byte[] b, final int off) {
    return (short) ((b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8));
  }

  static void putIntLE(final byte[] b, final int off, final int v) {
    b[off] = (byte) v;
    b[off + 1] = (byte) (v >>> 8);
    b[off + 2] = (byte) (v >>> 16);
    b[off + 3] = (byte) (v >>> 24);
  }


  static void putLongLE(final byte[] b, final int off, final long v) {
    putIntLE(b, off, (int) v);
    putIntLE(b, off + 4, (int) (v >>> 32));
  }

}
