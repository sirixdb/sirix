/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Which record each OBJECT_KEY slot belongs to — the one thing the PAX columns could not say.
 *
 * <h2>The problem it solves</h2>
 *
 * <p>Every other region in this package is a single column: {@link NumberRegion} holds a field's
 * numeric values, {@link BooleanRegion} holds a field's bits, {@link ObjectKeyNameKeyRegion} says
 * which field each slot names. Each is complete on its own, and a predicate over one field is
 * answered by one kernel over one column.
 *
 * <p>A predicate over <em>two</em> fields is not, and the reason is structural rather than a missing
 * kernel. {@code $u.year gt 1990 and $u.active} needs the year-column position and the bit-column
 * position that belong to the <em>same record</em>. Position {@code i} of one column and position
 * {@code i} of the other are the i-th record <em>carrying that field</em> — which coincide only if
 * both fields sit on exactly the same records in the same order, and nothing in the layout said
 * whether they do. {@link BooleanRegionSimd#andInto} was written for precisely this fusion and had
 * no sound caller, because there was no way to establish the alignment it assumes.
 *
 * <p>Inferring it does not work. Every certificate derivable from the field-name column alone is
 * defeated by a page whose field-name sequence is periodic for reasons other than record structure:
 * with records {@code (a,c)} and {@code (b,d)} the sequence {@code a,c,b,d,a,c,b,d} is perfectly
 * periodic with four distinct names per period, yet the k-th {@code a} and the k-th {@code b} are in
 * <em>different</em> records. A fusion built on that reads as correct and silently pairs values
 * across record boundaries.
 *
 * <p>So the linkage is stored, not guessed: one dense per-page record ordinal for each OBJECT_KEY
 * slot. This is the {@code parentKey} half of the {@link RegionTable#KIND_STRUCT_POINTERS} column
 * that was reserved and never implemented, narrowed to what a multi-column predicate actually needs.
 *
 * <h2>Wire format</h2>
 *
 * <pre>
 * byte   version      // VERSION_V1
 * short  okCount      // OBJECT_KEY slots covered, in the same bitmap order as
 *                     // ObjectKeyNameKeyRegion's dictIds column
 * short  skipCount    // leading slots whose enclosing object is on the PREVIOUS page
 * short  recordCount  // R: distinct enclosing objects, all of them on this page
 * byte   bitWidth     // bits per ordinal; 0 when R &lt;= 1, else ceil(log2(R))
 * byte[] ordinals     // okCount - skipCount ordinals, bit-packed little-endian at bitWidth
 * </pre>
 *
 * <p>Ordinals are dense and assigned in first-appearance order over ascending slots, so on a
 * preorder shred the column is a monotonic run sequence — 10 bits per entry before compression and
 * near nothing after it. At 465 OBJECT_KEY slots that is 8 bytes of header plus ~582 of payload,
 * against the ~140 KB of record heap a two-field predicate would otherwise have to reconstruct.
 *
 * <h2>The skip prefix, and why anything past it is all-or-nothing</h2>
 *
 * <p>Node keys are assigned in creation order and a slot is the key's offset into the page, so a
 * record that spans a page boundary — its object node at the tail of page {@code k}, some of its
 * field nodes at the head of page {@code k+1} — puts its off-page-parented slots in one place only:
 * a leading run at the head of the page. That shape is not an anomaly; on a corpus of multi-field
 * records it is MOST pages, and refusing it meant refusing the linkage almost everywhere. The
 * leading run is therefore recorded as {@code skipCount} and excluded: those slots have no ordinal
 * ({@link #ordinalAt} answers {@code -1} for them), no record on this page claims them, and a fused
 * kernel simply starts each field's column window past its share of the prefix. The spanning
 * record itself is counted on the page that holds its object node — by the fused kernel there if
 * its predicate fields also sit there, by the record path otherwise — so excluding its tail here
 * double-counts nothing and drops nothing.
 *
 * <p>Past the prefix the rule stays all-or-nothing: an off-page parent anywhere else is a page
 * shape this format does not describe, and a column that is right for most slots is worse than
 * absent because a reader cannot tell which entry is the unusable one. Absence is a state every
 * reader already handles — it declines the fusion and the predicate goes through the records,
 * exactly as before this region existed.
 *
 * <h2>Compatibility</h2>
 *
 * <p>Both directions are safe without a format version bump, on the same argument as
 * {@link NumberZoneMapRegion}: an older reader meets a kind ordinal at or above its own
 * {@code KIND_COUNT} and steps over it by its length prefix, and a newer reader meeting a page
 * written without one finds it absent and falls back.
 */
public final class RecordOrdinalRegion {

  /** Current wire format version. */
  public static final byte VERSION_V1 = 1;

  /** Bytes before the packed ordinals. */
  private static final int FIXED_BYTES = 1 + 2 + 2 + 2 + 1;

  /** Slots per page, and therefore the ceiling on both {@code okCount} and {@code recordCount}. */
  private static final int MAX_SLOTS = 1024;

  private RecordOrdinalRegion() {
    throw new AssertionError("no instances");
  }

  /** Parsed region. Reused across pages to keep a scan allocation-free. */
  public static final class Header {
    /** OBJECT_KEY slots covered, in {@link ObjectKeyNameKeyRegion} bitmap order. */
    public int okCount;
    /** Leading slots owned by a record whose object node is on the previous page; no ordinals. */
    public int skipCount;
    /** Distinct enclosing objects on this page. */
    public int recordCount;
    /** Bits per packed ordinal; {@code 0} when the page holds at most one record. */
    public int bitWidth;
    /** Byte offset of the packed ordinals. */
    public int ordinalsOffset;
    /** {@code bitWidth}'s low-bit mask, so readers do not recompute it per access. */
    public long mask;

    /**
     * Parse {@code payload} into this instance.
     *
     * @return {@code this}, or {@code null} when the payload is absent, truncated, internally
     *         inconsistent, or written by a future version — all of which mean "no linkage
     *         available", never a wrong one
     */
    public Header parseInto(final MemorySegment payload) {
      if (payload == null || payload.byteSize() < FIXED_BYTES) {
        return null;
      }
      // Read into locals and commit only once the payload has fully checked out, so a declined
      // parse cannot leave this reusable scratch holding one page's okCount beside another page's
      // offsets — a mixture that reads as valid and links against the wrong column.
      final RegionReader in = new RegionReader(payload);
      if (in.readByte() != VERSION_V1) {
        return null;
      }
      final int readOkCount = in.readShort() & 0xFFFF;
      final int readSkipCount = in.readShort() & 0xFFFF;
      final int readRecordCount = in.readShort() & 0xFFFF;
      final int readBitWidth = in.readByte() & 0xFF;
      if (readOkCount > MAX_SLOTS || readRecordCount > MAX_SLOTS
          || readSkipCount >= readOkCount
          || readRecordCount > readOkCount - readSkipCount
          || readBitWidth > Integer.SIZE
          || readBitWidth != bitWidthFor(readRecordCount)) {
        return null;
      }
      final int offset = FIXED_BYTES;
      final long need = (long) offset + packedBytes(readOkCount - readSkipCount, readBitWidth);
      if (payload.byteSize() < need) {
        return null;
      }
      okCount = readOkCount;
      skipCount = readSkipCount;
      recordCount = readRecordCount;
      bitWidth = readBitWidth;
      ordinalsOffset = offset;
      mask = readBitWidth == 0 ? 0L : BitUnpackSimd.maskFor(readBitWidth);
      return this;
    }
  }

  /**
   * The record ordinal of the OBJECT_KEY slot at {@code bitmapIndex}, or {@code -1} when the index
   * is outside the region or inside the skip prefix — a slot whose record lives on the previous
   * page and therefore has no ordinal here.
   *
   * <p>{@code bitmapIndex} is a position in {@link ObjectKeyNameKeyRegion}'s dictIds column, which
   * is what {@link ObjectKeyNameKeyRegion#findMatchingBitmapIndices} reports — not a slot number.
   * The two differ on any page that is not wholly OBJECT_KEY slots, and confusing them would link
   * a value to whichever record happened to sit at that ordinal.
   */
  public static int ordinalAt(final MemorySegment payload, final Header h, final int bitmapIndex) {
    if (h == null || bitmapIndex < h.skipCount || bitmapIndex >= h.okCount) {
      return -1;
    }
    if (h.bitWidth == 0) {
      return 0;  // a single record owns every slot
    }
    return (int) BitUnpackSimd.decodeAt(payload, h.ordinalsOffset, h.bitWidth, h.mask,
                                        bitmapIndex - h.skipCount);
  }

  /**
   * Whether {@code bitmapIndices[0..n)} map to record ordinals {@code 0, 1, ..., n-1} in order.
   *
   * <p>Equivalent to {@link #alignedLead} answering exactly {@code 0} for a window of every record
   * on the page: the field enumerates them all once, in record order, with no occurrence inside
   * the skip prefix and none left over.
   */
  public static boolean isRecordAligned(final MemorySegment payload, final Header h,
      final int[] bitmapIndices, final int n) {
    return h != null && alignedLead(payload, h, bitmapIndices, n, h.recordCount) == 0;
  }

  /**
   * The number of leading entries of {@code bitmapIndices[0..n)} that fall inside the skip prefix,
   * when the next {@code m} entries map to record ordinals {@code 0, 1, ..., m-1} in order and
   * every entry after those belongs to a record at or past {@code m} — or {@code -1} otherwise.
   *
   * <p>This is the certificate a two-column fusion needs, and it is checked rather than assumed. A
   * non-negative answer {@code lead} says the field's column splits into three runs: entries
   * {@code [0, lead)} belong to the record spanning in from the previous page; entries
   * {@code [lead, lead + m)} enumerate records {@code 0..m-1} once each, in record order; and
   * entries past that belong to the trailing partial records — object nodes at the tail of this
   * page whose remaining fields sit on the next one. When that holds for two fields with the same
   * {@code m}, position {@code lead + i} of either column is record {@code i}, and the two columns
   * can be intersected positionally over the window — each shifted by its own lead — with no
   * permutation between them.
   *
   * <p>It answers {@code -1}, and the caller must decline, whenever the field is missing from some
   * record inside the window, occurs twice in one record, or is interleaved so that its slots do
   * not follow record order — a trailing entry mapping below {@code m} is precisely a duplicate
   * inside the window. Each of those is a real page shape, and each would pair values across
   * records if assumed away.
   *
   * <p>Cost is one bit-unpack per entry: two word loads and a funnel shift, no branch. That is
   * roughly 5 ns per record against the ~300 ns a record reconstruction costs, so paying it to
   * decide whether the columns can be used at all is worth it even when the answer is no.
   */
  public static int alignedLead(final MemorySegment payload, final Header h,
      final int[] bitmapIndices, final int n, final int m) {
    if (h == null || bitmapIndices == null || n < 0 || n > bitmapIndices.length
        || m < 0 || m > h.recordCount) {
      return -1;
    }
    final int lead = skipPrefixLead(h, bitmapIndices, n);
    if (n - lead < m) {
      return -1;  // the field does not cover every record inside the window
    }
    for (int i = lead; i < lead + m; i++) {
      if (ordinalAt(payload, h, bitmapIndices[i]) != i - lead) {
        return -1;
      }
    }
    for (int i = lead + m; i < n; i++) {
      if (ordinalAt(payload, h, bitmapIndices[i]) < m) {
        return -1;  // an occurrence past the window for a record inside it — a duplicate
      }
    }
    return lead;
  }

  /**
   * How many leading entries of {@code bitmapIndices[0..n)} fall inside the skip prefix.
   *
   * <p>The single definition of prefix membership, shared by {@link #alignedLead} and by callers
   * that need a field's lead before running the full certificate — two hand-rolled copies of this
   * scan could drift apart, and a lead that disagrees with the certificate attributes the pending
   * boundary record to the wrong slot.
   */
  public static int skipPrefixLead(final Header h, final int[] bitmapIndices, final int n) {
    int lead = 0;
    while (lead < n && bitmapIndices[lead] >= 0 && bitmapIndices[lead] < h.skipCount) {
      lead++;
    }
    return lead;
  }

  /**
   * Encode the ordinals for {@code okCount} OBJECT_KEY slots from their enclosing objects' node
   * keys.
   *
   * <p>{@code parentKeys} is in the same bitmap order as {@link ObjectKeyNameKeyRegion}'s dictIds
   * column, and holds each slot's enclosing object node key exactly as the page stores it —
   * classification happens HERE, not at the call sites. A key inside this page's slot range is an
   * on-page parent; a key below {@code pageKeyBase} at the head of the page is the record spanning
   * in from the previous page, admitted as the skip prefix only while every such key is the SAME
   * key. Two distinct off-page parents at the head (a field inserted under an older, earlier-page
   * object) would build a prefix mixing two records' values — the caller-side boundary patch then
   * evaluates the wrong record — so the identity check lives in the one place that owns the
   * contract instead of being re-implemented, and eventually mis-implemented, per caller.
   *
   * @return the payload, or {@code null} when there is nothing to link ({@code okCount == 0}, or
   *         every slot belongs to the record spanning in from the previous page) or the linkage
   *         would be incomplete (an off-page parent past the leading run, a second distinct
   *         off-page parent inside it, a parentless slot, or a parent past the page)
   */
  public static byte[] encode(final long[] parentKeys, final long pageKeyBase, final int okCount) {
    if (parentKeys == null || okCount <= 0 || okCount > MAX_SLOTS
        || parentKeys.length < okCount) {
      return null;
    }
    // A leading run of off-page parents is the tail of the record spanning in from the previous
    // page — slot order is node-key order, so its field nodes sit at the head of the page and
    // nowhere else, and its object key is necessarily BELOW the page base. Recorded as skipCount
    // and excluded from the ordinals; an off-page parent anywhere PAST the run, a second distinct
    // parent inside it, or a parent above the page is a shape this format does not describe.
    int skipCount = 0;
    long spanningParent = Long.MIN_VALUE;
    while (skipCount < okCount) {
      final long parentKey = parentKeys[skipCount];
      final long parentSlot = parentKey - pageKeyBase;
      if (parentSlot >= 0L && parentSlot < MAX_SLOTS) {
        break;  // on-page parent — the leading run ends here
      }
      if (parentKey < 0L || parentKey >= pageKeyBase
          || (spanningParent != Long.MIN_VALUE && parentKey != spanningParent)) {
        return null;
      }
      spanningParent = parentKey;
      skipCount++;
    }
    if (skipCount == okCount) {
      return null;  // nothing but the spanning tail — no record on this page to link
    }
    // Parent slot -> dense ordinal, in first-appearance order. A map rather than "increment on
    // change" because a page whose fields are interleaved would otherwise hand one record two
    // ordinals, and a wrong ordinal is worse than a refused page: isRecordAligned would still pass
    // for both fields while the ordinals no longer name records.
    final int[] ordinalOfSlot = ORDINAL_OF_SLOT_SCRATCH.get();
    Arrays.fill(ordinalOfSlot, -1);
    final int[] ordinals = ORDINALS_SCRATCH.get();
    int recordCount = 0;
    for (int i = skipCount; i < okCount; i++) {
      final long parentSlot = parentKeys[i] - pageKeyBase;
      if (parentSlot < 0L || parentSlot >= MAX_SLOTS) {
        return null;  // enclosing object is not on this page — the column would be incomplete
      }
      final int parent = (int) parentSlot;
      int ordinal = ordinalOfSlot[parent];
      if (ordinal < 0) {
        ordinal = recordCount++;
        ordinalOfSlot[parent] = ordinal;
      }
      ordinals[i - skipCount] = ordinal;
    }

    final int packedCount = okCount - skipCount;
    final int bitWidth = bitWidthFor(recordCount);
    final int packed = packedBytes(packedCount, bitWidth);
    final byte[] out = new byte[FIXED_BYTES + packed];
    final ByteBuffer bb = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
    bb.put(VERSION_V1);
    bb.putShort((short) okCount);
    bb.putShort((short) skipCount);
    bb.putShort((short) recordCount);
    bb.put((byte) bitWidth);
    if (bitWidth > 0) {
      // Little-endian bit packing, matching what BitUnpackSimd.decodeAt reads back: entry i starts
      // at bit i*bitWidth and may straddle a byte boundary, so each entry is OR-ed in over as many
      // bytes as it spans rather than written whole.
      for (int i = 0; i < packedCount; i++) {
        final long bitOffset = (long) i * bitWidth;
        int byteIndex = FIXED_BYTES + (int) (bitOffset >>> 3);
        int shift = (int) (bitOffset & 7L);
        long value = ordinals[i] & 0xFFFFFFFFL;
        int remaining = bitWidth;
        while (remaining > 0) {
          final int room = 8 - shift;
          out[byteIndex] |= (byte) ((value & 0xFFL) << shift);
          final int consumed = Math.min(room, remaining);
          value >>>= consumed;
          remaining -= consumed;
          byteIndex++;
          shift = 0;
        }
      }
    }
    return out;
  }

  /** Bits needed to hold ordinals {@code 0 .. recordCount-1}; {@code 0} for at most one record. */
  public static int bitWidthFor(final int recordCount) {
    if (recordCount <= 1) {
      return 0;
    }
    return Integer.SIZE - Integer.numberOfLeadingZeros(recordCount - 1);
  }

  /** Bytes the packed ordinals occupy. */
  private static int packedBytes(final int okCount, final int bitWidth) {
    return (int) (((long) okCount * bitWidth + 7L) >>> 3);
  }

  /** Exact encoded size for a page with no skip prefix, for sizing assertions and diagnostics. */
  public static int encodedSize(final int okCount, final int recordCount) {
    return encodedSize(okCount, 0, recordCount);
  }

  /** Exact encoded size, for sizing assertions and diagnostics. */
  public static int encodedSize(final int okCount, final int skipCount, final int recordCount) {
    if (okCount < 0 || skipCount < 0 || skipCount > okCount || recordCount < 0) {
      throw new IllegalArgumentException(
          "okCount=" + okCount + " skipCount=" + skipCount + " recordCount=" + recordCount);
    }
    return FIXED_BYTES + packedBytes(okCount - skipCount, bitWidthFor(recordCount));
  }

  /**
   * Slot-to-ordinal map, one entry per page slot. Per-thread and refilled per page: a page's worth
   * of ints is cheaper to clear than to allocate, and the writer runs this once per page during an
   * ingest where every allocation shows up in the profile.
   */
  private static final ThreadLocal<int[]> ORDINAL_OF_SLOT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[MAX_SLOTS]);

  /** Assigned ordinals in bitmap order, staged before packing. */
  private static final ThreadLocal<int[]> ORDINALS_SCRATCH =
      ThreadLocal.withInitial(() -> new int[MAX_SLOTS]);
}
