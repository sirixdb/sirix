/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;

/**
 * The PAX regions of one {@link KeyValueLeafPage}, decoded <em>without</em> its record heap.
 *
 * <p>A record page is written as three separable sections: a fixed header + slot bitmap, one
 * compressed body blob (compact directory, offset-table templates, and the row heap), and the
 * {@link RegionTable}. The body is what {@code PageKind.deserializeSlottedPage} spends its time
 * on — it decompresses the blob and re-expands every record into a row-oriented heap, injecting
 * back the very columns the writer elided. A query that reads one number per record needs none of
 * that: the values are already columnar on disk, and the region table is the column store.
 *
 * <p>This class is what a scan gets when it asks for the columns only. The body blob is skipped by
 * its length prefix — never decompressed — and only the region kinds the query named are
 * materialized (see {@link RegionTable#read(io.sirix.node.BytesIn, int)}). What remains is a plain
 * Java object holding {@code byte[]} payloads: no off-heap allocation, no page guard, no cache
 * entry, nothing to close. It is created by one scan worker, consumed by that worker, and
 * collected.
 *
 * <h2>Thread safety</h2>
 * An instance is normally decoded, folded into an accumulator and dropped by one worker. It is NOT
 * confined to that worker, though: a scan may retain decoded columns in a cache shared by every
 * worker of every later query on the same revision, which is sound because the columns of a
 * committed page cannot change. Consequently any state cached lazily here must be safely published
 * — see {@link #numberSegment}. The header parses take a caller-supplied scratch object and keep
 * no state of their own, so they are safe by construction.
 *
 * @author Johannes Lichtenberger
 */
public final class RegionsOnlyPage {

  /** Key of the record page these regions belong to. */
  private final long pageKey;

  /** Revision the page fragment was written in. */
  private final int revision;

  /** The regions the caller asked for; kinds not requested are absent. */
  private final RegionTable regions;

  /**
   * Populated slots on the page. Read from the page header the column-only decode passes over
   * anyway, so it costs nothing — and it answers "how many records are here?" without a single
   * record being built.
   */
  private final int populatedSlotCount;

  /**
   * Id of the FSST symbol table this page's strings were encoded against, or
   * {@link KeyValueLeafPage#NO_FSST_SYMBOL_TABLE_ID}. A string predicate needs it to encode its
   * literal into the same form the dictionary stores, which is how an equality is answered on
   * compressed data without decompressing any.
   */
  private final long fsstSymbolTableId;

  /**
   * The fragment's populated-slot bitmap, 16 words covering slots 0..1023, or {@code null} when the
   * caller did not need it.
   *
   * <p>Load-bearing for versioned reconstruction: a slot this fragment DEFINES wins over every
   * older fragment, and it wins even when the fragment defines it as a deletion — in which case the
   * slot is in the bitmap but absent from the regions. Merging on the regions alone would mistake
   * that for "not modified here" and resurrect the older value.
   */
  private final long[] slotBitmap;

  /**
   * Cached {@link MemorySegment} view over the NUMBER payload for the SIMD kernels.
   *
   * <p>Volatile because instances ARE shared: the scan's decoded-column cache keeps them in a
   * {@code ConcurrentHashMap} that outlives the query, so any worker of any later query can land
   * on the same page and race this lazy write. The value is derived purely from the payload, so
   * two racing threads compute equal segments and a duplicate is harmless -- what is not harmless
   * is publishing the reference without the object behind it.
   */
  private volatile MemorySegment numberSegment;

  public RegionsOnlyPage(final long pageKey, final int revision, final int populatedSlotCount,
      final long fsstSymbolTableId, final RegionTable regions) {
    this(pageKey, revision, populatedSlotCount, fsstSymbolTableId, regions, null);
  }

  public RegionsOnlyPage(final long pageKey, final int revision, final int populatedSlotCount,
      final long fsstSymbolTableId, final RegionTable regions, final long @Nullable [] slotBitmap) {
    this.slotBitmap = slotBitmap;
    this.pageKey = pageKey;
    this.revision = revision;
    this.populatedSlotCount = populatedSlotCount;
    this.fsstSymbolTableId = fsstSymbolTableId;
    this.regions = regions;
  }

  /** Whether this fragment defines {@code slot} at all — including as a deletion. */
  public boolean definesSlot(final int slot) {
    final long[] bitmap = slotBitmap;
    return bitmap != null && (bitmap[slot >>> 6] & (1L << (slot & 63))) != 0;
  }

  /** @return {@code true} when this page was read with its slot bitmap. */
  public boolean hasSlotBitmap() {
    return slotBitmap != null;
  }

  /**
   * How many slots this fragment defines, i.e. the bitmap's cardinality; {@code -1} when the page
   * was read without a bitmap.
   *
   * <p>Distinguishes a bitmap that was DECODED from one that merely EXISTS, which
   * {@link #hasSlotBitmap()} cannot: a reader that allocates the bitmap array up front and then
   * fails to fill it hands back all zeros, and an all-zero bitmap is not "unknown" but the
   * strictly false claim that the fragment defines nothing — every slot then resolves to an older
   * fragment. Compare against {@link #getPopulatedSlotCount()} to assert the two agree.
   */
  public int definedSlotCount() {
    final long[] bitmap = slotBitmap;
    if (bitmap == null) {
      return -1;
    }
    int count = 0;
    for (final long word : bitmap) {
      count += Long.bitCount(word);
    }
    return count;
  }

  /**
   * @return the FSST symbol-table id, or {@link KeyValueLeafPage#NO_FSST_SYMBOL_TABLE_ID} when the
   *         page's strings are stored raw
   */
  public long getFsstSymbolTableId() {
    return fsstSymbolTableId;
  }

  /** Number of populated slots (records) on the page. */
  public int getPopulatedSlotCount() {
    return populatedSlotCount;
  }

  public long getPageKey() {
    return pageKey;
  }

  public int getRevision() {
    return revision;
  }

  /** The decoded region table. Never {@code null}; may hold no regions at all. */
  public RegionTable getRegionTable() {
    return regions;
  }

  /** Raw payload for {@code kind}, or {@code null} when absent or not requested. */
  public byte[] regionPayload(final byte kind) {
    return regions.payload(kind);
  }

  /**
   * Parse this page's NUMBER-region header into {@code scratch}, or return {@code null} when the
   * page carries no numeric column.
   *
   * <p>The header is parsed into a caller-owned instance rather than allocated per page: a scan
   * decodes one page, folds it, and drops it, so a single header per worker thread serves every
   * page it ever sees. {@link NumberRegion.Header#parseInto} already reuses its own arrays when
   * they are large enough, so steady state is zero allocation per page.
   *
   * <p>Unlike {@link KeyValueLeafPage#getNumberRegionHeader()} there is no slot-walk fallback —
   * without a heap there is nothing to walk, and the caller's contract is to fall back to a full
   * page read when the column it needs is not on the wire.
   */
  public NumberRegion.@Nullable Header numberHeaderInto(final NumberRegion.Header scratch) {
    final byte[] payload = regions.payload(RegionTable.KIND_NUMBER);
    if (payload == null || payload.length == 0) {
      return null;
    }
    return scratch.parseInto(payload);
  }

  /**
   * {@link MemorySegment} view over the NUMBER payload, for
   * {@link io.sirix.page.pax.NumberRegionSimd}; {@code null} when the page has no numeric column.
   *
   * <p>Derived from the payload on demand and cached, rather than as a side effect of parsing the
   * header: making it depend on call order meant a caller that asked in the other order silently
   * got {@code null} and fell back, with nothing to indicate why.
   */
  public @Nullable MemorySegment numberSegment() {
    MemorySegment seg = numberSegment;
    if (seg == null) {
      final byte[] payload = regions.payload(RegionTable.KIND_NUMBER);
      if (payload == null || payload.length == 0) {
        return null;
      }
      seg = MemorySegment.ofArray(payload);
      numberSegment = seg;
    }
    return seg;
  }

  /** Parse the STRING-region header into {@code scratch}; {@code null} when the column is absent. */
  public StringRegion.@Nullable Header stringHeaderInto(final StringRegion.Header scratch) {
    final byte[] payload = regions.payload(RegionTable.KIND_STRING);
    if (payload == null || payload.length == 0) {
      return null;
    }
    return scratch.parseInto(payload);
  }

  /** Raw STRING payload, or {@code null}. */
  public byte[] stringPayload() {
    return regions.payload(RegionTable.KIND_STRING);
  }

  /**
   * Bytes of region payload this page retains — what it costs to keep, for a caller that caches it.
   * Excludes the object headers, which are small and fixed next to the payloads.
   *
   * <p>Delegates rather than looping over {@link RegionTable#payload(byte)}: that accessor
   * decompresses deferred regions, so measuring a page this way used to materialize every
   * dictionary the deferred read path had just gone out of its way NOT to decompress — and it did
   * so before the caller had even decided whether to keep the page.
   */
  public int payloadBytes() {
    return regions.retainedBytes();
  }

  /**
   * OR the slots this fragment defines into {@code acc}, which must be
   * {@link Constants#NDP_NODE_COUNT} bits wide.
   *
   * <p>Word-wise on purpose. The caller is the versioned-merge kernel, which does this once per
   * fragment per page; going bit by bit through {@link #definesSlot} costs 1024 bounds-checked
   * loads, shifts and branches to compute what 16 ORs compute exactly.
   */
  public void orDefinedSlotsInto(final long[] acc) {
    final long[] bitmap = slotBitmap;
    if (bitmap == null) {
      return;
    }
    final int words = Math.min(bitmap.length, acc.length);
    for (int w = 0; w < words; w++) {
      acc[w] |= bitmap[w];
    }
  }

  /** Raw OBJECT_KEY-nameKey payload, or {@code null}. */
  public byte[] nameKeyPayload() {
    return regions.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
  }
}
