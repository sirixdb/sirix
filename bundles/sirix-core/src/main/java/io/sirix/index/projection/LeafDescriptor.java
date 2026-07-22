/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.HOTLeafPage;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

/**
 * The projection leaf <em>descriptor</em> ("PIXD"): the tiny HOT slot value of the
 * segment-directory storage layout (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3). It carries
 * the raw-form header (rowCount, columnCount, kinds, record-key fences) plus one fixed-size
 * entry per segment: {@code segmentId}, exact {@code byteLen}, XXH3-64 {@code contentHash}
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
 *   short  segCount                              [offset 27 + columnCount]
 *   per entry (ENTRY_BYTES = 30):
 *     byte segmentId; int byteLen; long contentHash; byte colFlags; long min; long max
 *   inline region: for each INLINE entry, its full segment bytes (PIXS header
 *                  included), concatenated in ascending-segmentId (= entry) order
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
public final class LeafDescriptor {

  /** Leading magic ("PIXD" little-endian). */
  public static final int MAGIC = 0x44584950;

  /** Layout version; bumped on any wire change. */
  public static final byte VERSION = 1;

  /**
   * Column cap imposed by the 8-bit segmentId space of the HOT side-map composite key:
   * {@code SEGMENTS_PER_COLUMN · c + 2 ≤ MAX_SEGMENT_ID}. Derived — not restated — from the
   * id-scheme constants so the invariant has a single authority (a fourth per-column segment
   * kind automatically tightens this cap).
   */
  public static final int MAX_COLUMNS =
      (HOTLeafPage.MAX_SEGMENT_ID - 2) / ProjectionIndexSegmentCodec.SEGMENTS_PER_COLUMN;

  /** Fixed size of one segment entry. */
  public static final int ENTRY_BYTES = 1 + 4 + 8 + 1 + 8 + 8;

  /**
   * High bit of an entry's {@code byteLen} int marking the segment as INLINE (bytes in this
   * descriptor's trailing region) rather than REFERENCED (bytes in a side-map page). Safe to
   * overload the sign bit: a segment is capped at {@code OverflowPage.MAX_SEGMENT_BYTES} (16 MB
   * ≪ 2^31), so the true length never touches it. {@link #entryByteLen} masks it off; {@link
   * #entryIsInline} tests it. Chosen over a {@code colFlags} bit so the column-provenance mirror
   * (UNREPRESENTABLE/NON_INTEGRAL/PURE_DOUBLE_SOURCE) stays byte-for-byte untouched.
   */
  public static final int SEG_INLINE_FLAG = 0x8000_0000;

  /**
   * Smallest structurally possible descriptor: fixed head through the kinds offset (zero
   * columns) plus the segCount short. Cheap plausibility floor for slice-level readers
   * that only need head fields without a full {@link #validate}.
   */
  public static final int MIN_BYTES = 27 + 2;

  private static final int OFF_ROW_COUNT = 5;
  private static final int OFF_COLUMN_COUNT = 9;
  private static final int OFF_FIRST_KEY = 11;
  private static final int OFF_LAST_KEY = 19;
  private static final int OFF_KINDS = 27;

  private LeafDescriptor() {
  }

  // ==================== write ====================

  /**
   * Serialize an all-referenced descriptor (no inline segments) — the pre-hybrid layout. Entry
   * arrays are parallel, {@code segCount} entries each; entries must be sorted by ascending
   * {@code segmentId} (binary-searchable, deterministic bytes).
   */
  public static byte[] serialize(final int rowCount, final long firstRecordKey, final long lastRecordKey,
      final byte[] kinds, final int segCount, final byte[] segmentIds, final int[] byteLens,
      final long[] contentHashes, final byte[] colFlags, final long[] mins, final long[] maxs) {
    return serialize(rowCount, firstRecordKey, lastRecordKey, kinds, segCount, segmentIds, byteLens,
        contentHashes, colFlags, mins, maxs, null, null);
  }

  /**
   * Serialize a descriptor with per-segment storage classes (hybrid inline/referenced). When
   * {@code inline != null && inline[i]}, entry {@code i}'s {@code byteLen} field is stored with
   * {@link #SEG_INLINE_FLAG} set and {@code segmentBytes[i]} (the segment's full bytes, PIXS
   * header included) is appended to the trailing inline region in ascending-{@code segmentId}
   * (= entry) order; those are the same bytes a referenced page would hold. Entries with
   * {@code inline[i] == false} (or {@code inline == null}) are referenced as before and their
   * {@code segmentBytes[i]} is ignored. {@code byteLens[i]} always carries the true segment
   * length for both classes.
   */
  public static byte[] serialize(final int rowCount, final long firstRecordKey, final long lastRecordKey,
      final byte[] kinds, final int segCount, final byte[] segmentIds, final int[] byteLens,
      final long[] contentHashes, final byte[] colFlags, final long[] mins, final long[] maxs,
      final boolean @Nullable [] inline, final byte @Nullable [][] segmentBytes) {
    if (kinds.length > MAX_COLUMNS) {
      throw new IllegalArgumentException("columnCount " + kinds.length + " exceeds MAX_COLUMNS=" + MAX_COLUMNS);
    }
    if (rowCount < 0 || rowCount > ProjectionIndexLeafPage.MAX_ROWS) {
      throw new IllegalArgumentException("rowCount out of range: " + rowCount);
    }
    if (segCount < 0 || segCount > 0xFFFF) {
      throw new IllegalArgumentException("segCount out of range: " + segCount);
    }
    if (segmentIds.length < segCount || byteLens.length < segCount || contentHashes.length < segCount
        || colFlags.length < segCount || mins.length < segCount || maxs.length < segCount) {
      throw new IllegalArgumentException("entry array shorter than segCount=" + segCount
          + ": segmentIds=" + segmentIds.length + " byteLens=" + byteLens.length
          + " contentHashes=" + contentHashes.length + " colFlags=" + colFlags.length
          + " mins=" + mins.length + " maxs=" + maxs.length);
    }
    // Inline region size + per-entry consistency: an inline entry must supply bytes matching its
    // recorded byteLen, so a later positional read (offset = Σ prior inline byteLens) never drifts.
    int inlineRegion = 0;
    for (int i = 0; i < segCount; i++) {
      if (byteLens[i] < 0) {
        throw new IllegalArgumentException("negative byteLen " + byteLens[i] + " at entry " + i
            + " — refusing to persist a descriptor that can never verify");
      }
      if (inline != null && inline[i]) {
        if (segmentBytes == null || segmentBytes[i] == null) {
          throw new IllegalArgumentException("inline entry " + i + " (segmentId " + (segmentIds[i] & 0xFF)
              + ") has no bytes");
        }
        if (segmentBytes[i].length != byteLens[i]) {
          throw new IllegalArgumentException("inline entry " + i + " byteLen " + byteLens[i]
              + " != bytes " + segmentBytes[i].length);
        }
        inlineRegion += byteLens[i];
      }
    }
    final int entriesEnd = OFF_KINDS + kinds.length + 2 + segCount * ENTRY_BYTES;
    final byte[] out = new byte[entriesEnd + inlineRegion];
    putIntLE(out, 0, MAGIC);
    out[4] = VERSION;
    putIntLE(out, OFF_ROW_COUNT, rowCount);
    putShortLE(out, OFF_COLUMN_COUNT, (short) kinds.length);
    putLongLE(out, OFF_FIRST_KEY, firstRecordKey);
    putLongLE(out, OFF_LAST_KEY, lastRecordKey);
    System.arraycopy(kinds, 0, out, OFF_KINDS, kinds.length);
    int pos = OFF_KINDS + kinds.length;
    putShortLE(out, pos, (short) segCount);
    pos += 2;
    int prevId = -1;
    for (int i = 0; i < segCount; i++) {
      final int id = segmentIds[i] & 0xFF;
      if (id <= prevId) {
        throw new IllegalArgumentException("segment entries must be sorted by ascending segmentId: "
            + id + " after " + prevId);
      }
      prevId = id;
      final boolean isInline = inline != null && inline[i];
      out[pos] = segmentIds[i];
      putIntLE(out, pos + 1, isInline ? (byteLens[i] | SEG_INLINE_FLAG) : byteLens[i]);
      putLongLE(out, pos + 5, contentHashes[i]);
      out[pos + 13] = colFlags[i];
      putLongLE(out, pos + 14, mins[i]);
      putLongLE(out, pos + 22, maxs[i]);
      pos += ENTRY_BYTES;
    }
    // Trailing inline region, entry order (= ascending segmentId, matching the read-side offset walk).
    if (inline != null) {
      int inlinePos = entriesEnd;
      for (int i = 0; i < segCount; i++) {
        if (inline[i]) {
          System.arraycopy(segmentBytes[i], 0, out, inlinePos, byteLens[i]);
          inlinePos += byteLens[i];
        }
      }
    }
    return out;
  }

  // ==================== positional readers (allocation-free) ====================

  /** {@code true} iff {@code value} starts with the descriptor magic. */
  public static boolean isDescriptor(final byte[] value) {
    return value != null && value.length >= 4 && ProjectionIndexLeafCodec.getIntLE(value, 0) == MAGIC;
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
    final int rowCount = ProjectionIndexLeafCodec.getIntLE(d, OFF_ROW_COUNT);
    final int columnCount = getShortLE(d, OFF_COLUMN_COUNT) & 0xFFFF;
    if (rowCount < 0 || rowCount > ProjectionIndexLeafPage.MAX_ROWS || columnCount > MAX_COLUMNS) {
      throw new IllegalStateException("Corrupt leaf descriptor: rowCount=" + rowCount
          + " columnCount=" + columnCount);
    }
    final int segCountOff = OFF_KINDS + columnCount;
    if (d.length < segCountOff + 2) {
      throw new IllegalStateException("Corrupt leaf descriptor: truncated before segCount");
    }
    final int segCount = getShortLE(d, segCountOff) & 0xFFFF;
    final int entriesEnd = segCountOff + 2 + segCount * ENTRY_BYTES;
    if (d.length < entriesEnd) {
      throw new IllegalStateException("Corrupt leaf descriptor: truncated entry table (length "
          + d.length + " < " + entriesEnd + ", segCount=" + segCount + ")");
    }
    // Trailing inline region: exact length = entry table end + Σ byteLen of inline entries.
    // A referenced-only descriptor has inlineTotal == 0 → the pre-hybrid length rule.
    long inlineTotal = 0;
    for (int i = 0; i < segCount; i++) {
      if (entryIsInline(d, i)) {
        inlineTotal += entryByteLen(d, i);
      }
    }
    final long expected = (long) entriesEnd + inlineTotal;
    if (d.length != expected) {
      throw new IllegalStateException("Corrupt leaf descriptor: length " + d.length
          + " != expected " + expected + " (segCount=" + segCount + ", inlineBytes=" + inlineTotal + ")");
    }
  }

  public static int rowCount(final byte[] d) {
    return ProjectionIndexLeafCodec.getIntLE(d, OFF_ROW_COUNT);
  }

  public static int columnCount(final byte[] d) {
    return getShortLE(d, OFF_COLUMN_COUNT) & 0xFFFF;
  }

  public static long firstRecordKey(final byte[] d) {
    return ProjectionIndexLeafCodec.getLongLE(d, OFF_FIRST_KEY);
  }

  public static long lastRecordKey(final byte[] d) {
    return ProjectionIndexLeafCodec.getLongLE(d, OFF_LAST_KEY);
  }

  public static byte kind(final byte[] d, final int column) {
    return d[OFF_KINDS + column];
  }

  public static int segCount(final byte[] d) {
    return getShortLE(d, OFF_KINDS + columnCount(d)) & 0xFFFF;
  }

  private static int entriesOffset(final byte[] d) {
    return OFF_KINDS + columnCount(d) + 2;
  }

  /**
   * Index of the entry for {@code segmentId}, or {@code -1} when absent. Entries are sorted by
   * segmentId; the count is ≤ 254, so a branch-light linear scan beats binary search on real
   * descriptors (≤ ~13 entries at 4 columns).
   */
  public static int entryIndexOf(final byte[] d, final int segmentId) {
    final int segCount = segCount(d);
    int off = entriesOffset(d);
    for (int i = 0; i < segCount; i++, off += ENTRY_BYTES) {
      final int id = d[off] & 0xFF;
      if (id == segmentId) {
        return i;
      }
      if (id > segmentId) {
        return -1;
      }
    }
    return -1;
  }

  public static int entrySegmentId(final byte[] d, final int entryIndex) {
    return d[entriesOffset(d) + entryIndex * ENTRY_BYTES] & 0xFF;
  }

  /** The segment's true length in bytes, with the {@link #SEG_INLINE_FLAG} storage-class bit masked off. */
  public static int entryByteLen(final byte[] d, final int entryIndex) {
    return ProjectionIndexLeafCodec.getIntLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 1) & ~SEG_INLINE_FLAG;
  }

  /** {@code true} iff entry {@code entryIndex}'s bytes are stored inline in this descriptor's trailing region. */
  public static boolean entryIsInline(final byte[] d, final int entryIndex) {
    return (ProjectionIndexLeafCodec.getIntLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 1)
        & SEG_INLINE_FLAG) != 0;
  }

  /**
   * Absolute offset in {@code d} where inline entry {@code entryIndex}'s bytes begin: the entry
   * table end plus the summed byteLens of the inline entries preceding it (inline bytes are laid
   * out in ascending entry order). Caller must ensure the entry is inline.
   */
  public static int inlineDataOffset(final byte[] d, final int entryIndex) {
    int off = entriesOffset(d) + segCount(d) * ENTRY_BYTES;
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
  public static byte[] inlineSegmentBytes(final byte[] d, final int entryIndex) {
    final int off = inlineDataOffset(d, entryIndex);
    return Arrays.copyOfRange(d, off, off + entryByteLen(d, entryIndex));
  }

  public static long entryContentHash(final byte[] d, final int entryIndex) {
    return ProjectionIndexLeafCodec.getLongLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 5);
  }

  public static byte entryColFlags(final byte[] d, final int entryIndex) {
    return d[entriesOffset(d) + entryIndex * ENTRY_BYTES + 13];
  }

  public static long entryMin(final byte[] d, final int entryIndex) {
    return ProjectionIndexLeafCodec.getLongLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 14);
  }

  public static long entryMax(final byte[] d, final int entryIndex) {
    return ProjectionIndexLeafCodec.getLongLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 22);
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
