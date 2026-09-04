/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.page.pax.DoubleRegion;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.NumberZoneMapRegion;
import io.sirix.page.pax.RecordOrdinalRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import org.jspecify.annotations.Nullable;


/**
 * The PAX regions of one {@link KeyValueLeafPage}, decoded <em>without</em> its record heap.
 *
 * <p>
 * A record page is written as three separable sections: a fixed header + slot bitmap, one
 * compressed body blob (compact directory, offset-table templates, and the row heap), and the
 * {@link RegionTable}. The body is what {@code PageKind.deserializeSlottedPage} spends its time on
 * — it decompresses the blob and re-expands every record into a row-oriented heap, injecting back
 * the very columns the writer elided. A query that reads one number per record needs none of that:
 * the values are already columnar on disk, and the region table is the column store.
 *
 * <p>
 * This class is what a scan gets when it asks for the columns only. The body blob is skipped by its
 * length prefix — never decompressed — and only the region kinds the query named are materialized
 * (see {@link RegionTable#read(io.sirix.node.BytesIn, int)}). What remains is a plain Java object
 * holding native, SIMD-friendly payloads. The wrapper owns one reference to its
 * {@link RegionTable}; callers must close it unless they transfer it into the executor's decoded
 * column cache. Closing is idempotent and releases native storage as soon as the final page/wrapper
 * owner leaves.
 *
 * <h2>Thread safety</h2> An instance is normally decoded, folded into an accumulator and dropped by
 * one worker. It is NOT confined to that worker, though: a scan may retain decoded columns in a
 * cache shared by every worker of every later query on the same revision, which is sound because
 * the columns of a committed page cannot change. The header parses take a caller-supplied scratch
 * object and keep no state of their own, so they are safe by construction.
 *
 * @author Johannes Lichtenberger
 */
public final class RegionsOnlyPage implements AutoCloseable {

  /** Key of the record page these regions belong to. */
  private final long pageKey;

  /** Revision the page fragment was written in. */
  private final int revision;

  /** The regions the caller asked for; kinds not requested are absent. */
  private final RegionTable regions;

  /**
   * Idempotence guard: a cache entry and a transient caller must never release the same ownership
   * twice.
   */
  private boolean closed;

  /**
   * Populated slots on the page. Read from the page header the column-only decode passes over anyway,
   * so it costs nothing — and it answers "how many records are here?" without a single record being
   * built.
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
   * <p>
   * Load-bearing for versioned reconstruction: a slot this fragment DEFINES wins over every older
   * fragment, and it wins even when the fragment defines it as a deletion — in which case the slot is
   * in the bitmap but absent from the regions. Merging on the regions alone would mistake that for
   * "not modified here" and resurrect the older value.
   */
  private final long[] slotBitmap;

  /**
   * Positive certificate that no record is stored through the overflow-reference section.
   *
   * <p>
   * Overflow values are intentionally absent from the PAX regions. A column scan therefore must
   * reconstruct a page unless completeness is known; otherwise it could either resurrect an older
   * inline value or miss a predicate matching the current overflow value. Older images carry no
   * positive bit and are conservatively treated as unknown.
   */
  private final boolean completeColumnCoverage;

  public RegionsOnlyPage(final long pageKey, final int revision, final int populatedSlotCount,
      final long fsstSymbolTableId, final RegionTable regions) {
    this(pageKey, revision, populatedSlotCount, fsstSymbolTableId, regions, null, false);
  }

  public RegionsOnlyPage(final long pageKey, final int revision, final int populatedSlotCount,
      final long fsstSymbolTableId, final RegionTable regions, final long @Nullable [] slotBitmap) {
    this(pageKey, revision, populatedSlotCount, fsstSymbolTableId, regions, slotBitmap, false);
  }

  public RegionsOnlyPage(final long pageKey, final int revision, final int populatedSlotCount,
      final long fsstSymbolTableId, final RegionTable regions, final long @Nullable [] slotBitmap,
      final boolean completeColumnCoverage) {
    if (pageKey < 0L) {
      throw new IllegalArgumentException("pageKey must be non-negative: " + pageKey);
    }
    if (revision < 0) {
      throw new IllegalArgumentException("revision must be non-negative: " + revision);
    }
    if (populatedSlotCount < 0 || populatedSlotCount > Constants.NDP_NODE_COUNT) {
      throw new IllegalArgumentException("populatedSlotCount out of range: " + populatedSlotCount);
    }
    if (slotBitmap != null && slotBitmap.length != PageLayout.BITMAP_WORDS) {
      throw new IllegalArgumentException(
          "slotBitmap must contain " + PageLayout.BITMAP_WORDS + " words, got " + slotBitmap.length);
    }
    this.slotBitmap = slotBitmap;
    this.pageKey = pageKey;
    this.revision = revision;
    this.populatedSlotCount = populatedSlotCount;
    this.fsstSymbolTableId = fsstSymbolTableId;
    this.regions = Objects.requireNonNull(regions, "regions");
    this.completeColumnCoverage = completeColumnCoverage;
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
   * @return whether the page positively certifies that every value is covered by its inline
   *         body/regions; {@code false} includes both known overflow and an older/unknown image
   */
  public boolean hasCompleteColumnCoverage() {
    return completeColumnCoverage;
  }

  /**
   * How many slots this fragment defines in {@code [from, to)}, by popcount rather than by asking bit
   * by bit.
   *
   * <p>
   * A column kernel segmenting a page counts the populated slots between one object key and the next,
   * which over a whole page is one {@link #definesSlot} call per slot — 1,024 shifts, masks and
   * branches to answer what sixteen {@code Long.bitCount}s answer exactly.
   *
   * @param from first slot, inclusive
   * @param to one past the last slot
   * @return the number of defined slots in the range, or {@code 0} when the range is empty or the
   *         page carries no bitmap
   */
  public int populatedInRange(final int from, final int to) {
    final long[] bitmap = slotBitmap;
    if (bitmap == null || from >= to || to <= 0) {
      return 0;
    }
    final int lo = Math.max(from, 0);
    final int hi = Math.min(to, bitmap.length << 6);
    if (lo >= hi) {
      return 0;
    }
    final int firstWord = lo >>> 6;
    final int lastWord = (hi - 1) >>> 6;
    // -1L << (lo & 63) keeps the bits at and above lo; Java's shift is mod 64, which is what makes
    // the aligned case (lo & 63 == 0) come out as the full word rather than empty.
    long mask = -1L << (lo & 63);
    int count = 0;
    for (int w = firstWord; w <= lastWord; w++) {
      long word = bitmap[w] & mask;
      if (w == lastWord) {
        final int lastBit = (hi - 1) & 63;
        word &= lastBit == 63
            ? -1L
            : (1L << (lastBit + 1)) - 1L;
      }
      count += Long.bitCount(word);
      mask = -1L;
    }
    return count;
  }

  /**
   * How many slots this fragment defines, i.e. the bitmap's cardinality; {@code -1} when the page was
   * read without a bitmap.
   *
   * <p>
   * Distinguishes a bitmap that was DECODED from one that merely EXISTS, which
   * {@link #hasSlotBitmap()} cannot: a reader that allocates the bitmap array up front and then fails
   * to fill it hands back all zeros, and an all-zero bitmap is not "unknown" but the strictly false
   * claim that the fragment defines nothing — every slot then resolves to an older fragment. Compare
   * against {@link #getPopulatedSlotCount()} to assert the two agree.
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
  public MemorySegment regionPayload(final byte kind) {
    return regions.payload(kind);
  }

  /**
   * Parse this page's NUMBER-region header into {@code scratch}, or return {@code null} when the page
   * carries no numeric column.
   *
   * <p>
   * The header is parsed into a caller-owned instance rather than allocated per page: a scan decodes
   * one page, folds it, and drops it, so a single header per worker thread serves every page it ever
   * sees. {@link NumberRegion.Header#parseInto} already reuses its own arrays when they are large
   * enough, so steady state is zero allocation per page.
   *
   * <p>
   * Unlike {@link KeyValueLeafPage#getNumberRegionHeader()} there is no slot-walk fallback — without
   * a heap there is nothing to walk, and the caller's contract is to fall back to a full page read
   * when the column it needs is not on the wire.
   */
  public NumberRegion.@Nullable Header numberHeaderInto(final NumberRegion.Header scratch) {
    final MemorySegment payload = regions.payload(RegionTable.KIND_NUMBER);
    if (payload == null || payload.byteSize() == 0) {
      return null;
    }
    // A per-tag column whose directory lives in the zone map is only readable together with it. A
    // read mask that asked for the values and not the summary therefore DECLINES here — the caller
    // falls back to the record path, as it does for any column not on the wire — rather than
    // decoding packed bytes against a directory it does not have.
    final MemorySegment directory = regions.payload(RegionTable.KIND_NUMBER_ZONEMAP);
    if (directory == null && NumberRegion.needsExternalDirectory(payload)) {
      return null;
    }
    return scratch.parseInto(payload, directory);
  }

  /**
   * Parse this page's zone-map region into {@code scratch}, or return {@code null} when the page
   * carries none.
   *
   * <p>
   * The one region accessor a scan can call without committing to anything: the zone map is stored
   * separately from the number column. A narrow map is raw and a wide map may use its own bounded
   * LZ77 frame; either way, reading it does not materialize {@link RegionTable#KIND_NUMBER}. A
   * predicate that the bounds settle therefore never pays for the column at all — which is the whole
   * reason the region exists.
   *
   * <p>
   * {@code null} means "no bounds available", never "no match": pages written before this region
   * existed, and pages whose number region uses a legacy encoding without per-tag zone maps, both
   * land here and must fall back to the number region's own header.
   */
  public NumberZoneMapRegion.@Nullable Header numberZoneMapInto(final NumberZoneMapRegion.Header scratch) {
    return scratch.parseInto(regions.payload(RegionTable.KIND_NUMBER_ZONEMAP));
  }

  /**
   * Parse the record-linkage region into {@code scratch}; {@code null} when it is absent.
   *
   * <p>
   * {@code null} means "which record a slot belongs to is unknown on this page", never "no match". It
   * is the normal state for pages written before the region existed and for pages holding a record
   * that spans pages, and every caller declines its multi-column path rather than guessing an
   * alignment — see {@link RecordOrdinalRegion}.
   */
  public RecordOrdinalRegion.@Nullable Header recordOrdinalInto(final RecordOrdinalRegion.Header scratch) {
    return scratch.parseInto(regions.payload(RegionTable.KIND_RECORD_ORDINAL));
  }

  /**
   * Parse the double column into {@code scratch}; {@code null} when the page carries no fractional
   * values — the common case, and exactly what it means.
   */
  public DoubleRegion.@Nullable Header doubleHeaderInto(final DoubleRegion.Header scratch) {
    return scratch.parseInto(regions.payload(RegionTable.KIND_DOUBLE));
  }

  /** Raw double-column payload, or {@code null}. */
  public MemorySegment doublePayload() {
    return regions.payload(RegionTable.KIND_DOUBLE);
  }

  /** Parse the STRING-region header into {@code scratch}; {@code null} when the column is absent. */
  public StringRegion.@Nullable Header stringHeaderInto(final StringRegion.Header scratch) {
    final MemorySegment payload = regions.payload(RegionTable.KIND_STRING);
    if (payload == null || payload.byteSize() == 0) {
      return null;
    }
    return scratch.parseInto(payload);
  }

  /** Raw STRING payload, or {@code null}. */
  public MemorySegment stringPayload() {
    return regions.payload(RegionTable.KIND_STRING);
  }

  /**
   * Bytes of region payload this page retains — what it costs to keep, for a caller that caches it.
   * Excludes the object headers, which are small and fixed next to the payloads.
   *
   * <p>
   * Delegates rather than looping over {@link RegionTable#payload(byte)}: that accessor decompresses
   * deferred regions, so measuring a page this way used to materialize every dictionary the deferred
   * read path had just gone out of its way NOT to decompress — and it did so before the caller had
   * even decided whether to keep the page.
   */
  public long payloadBytes() {
    return regions.retainedFootprintBytes();
  }

  /**
   * OR the slots this fragment defines into {@code acc}, which must be
   * {@link Constants#NDP_NODE_COUNT} bits wide.
   *
   * <p>
   * Word-wise on purpose. The caller is the versioned-merge kernel, which does this once per fragment
   * per page; going bit by bit through {@link #definesSlot} costs 1024 bounds-checked loads, shifts
   * and branches to compute what 16 ORs compute exactly.
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
  public MemorySegment nameKeyPayload() {
    return regions.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
  }

  /** Release this wrapper's ownership of the region table. */
  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    regions.close();
  }
}
