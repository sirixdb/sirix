/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.page.pax.RegionTable;

import java.util.concurrent.atomic.LongAdder;

/**
 * Diagnostic byte-count aggregator for the per-section serialized size of a
 * {@link KeyValueLeafPage}. Activated via {@code -Dsirix.pageSectionDiag=true} from
 * {@link PageKind}; off by default.
 *
 * <p>
 * Sections tracked:
 * <ul>
 * <li>{@code headerBitmap}: fixed 160-byte page header + bitmap prefix.</li>
 * <li>{@code encodedBody}: the compact-dir + template pool + slotIds + compressed-heap body.</li>
 * <li>{@code regionTable}: PAX region table (number/string/struct/DeweyID payloads). This is where
 * NumberRegion / StringRegion dictionaries and value arrays live, so expect it to dominate for
 * columnar workloads.</li>
 * <li>{@code overlong}: overlong-entries bitmap + references.</li>
 * <li>{@code fsst}: FSST symbol table (small).</li>
 * </ul>
 *
 * <p>
 * Beyond the section split, the report answers the questions a section total cannot:
 * <ul>
 * <li>the staged heap split by RECORD KIND, and the payload bytes that stayed inline because value
 * elision did not reach the slot;</li>
 * <li>the metadata each elision lever STAGES, against the bytes it saves;</li>
 * <li>the body-path outcome per index type — encoded or inline, elision active, refused, or nothing
 * elidable on the page at all — and, for the inline path, the reason;</li>
 * <li>how often one oversized fused string suppresses a page's whole string region, how many values
 * and bytes that strands inline, and the per-page overflow-descriptor histogram;</li>
 * <li>each region kind's bytes AS WRITTEN — after per-region LZ77 and with its framing — beside the
 * raw payload it was encoded from.</li>
 * </ul>
 *
 * <p>
 * A shutdown hook prints a cumulative summary ordered by absolute bytes. The summary is printed to
 * {@code System.out} so it's captured by stdout logging from the bench runner.
 *
 * <p>
 * HFT-grade: per-record path uses only {@link LongAdder} additions and one pageCount increment. No
 * allocation on the hot path.
 */
public final class PageSectionDiag {

  private static final LongAdder PAGE_COUNT = new LongAdder();
  private static final LongAdder HEADER_BITMAP_BYTES = new LongAdder();
  private static final LongAdder ENCODED_BODY_BYTES = new LongAdder();
  private static final LongAdder REGION_TABLE_BYTES = new LongAdder();
  private static final LongAdder OVERLONG_BYTES = new LongAdder();
  private static final LongAdder FSST_BYTES = new LongAdder();
  private static final LongAdder COMPACT_DIR_BYTES = new LongAdder();
  private static final LongAdder TEMPLATE_POOL_BYTES = new LongAdder();
  private static final LongAdder COMPRESSED_HEAP_BYTES = new LongAdder();
  /** Pre-compression bytes of the whole staged body (directory + templates + heap), for the ratio. */
  private static final LongAdder BODY_STAGING_BYTES = new LongAdder();
  /** Populated slots (records) over every serialization, for per-record averages. */
  private static final LongAdder RECORDS = new LongAdder();
  private static final LongAdder HASH_ELISION_PAGES = new LongAdder();
  private static final LongAdder HASH_ELISION_BYTES_SAVED = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_PAGES = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_BYTES_SAVED = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_CANDIDATE_PAGES = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_RAW_BYTES = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_ENCODED_BYTES = new LongAdder();
  private static final LongAdder RIGHT_SIB_KEY_COLUMN_PAGES = new LongAdder();
  private static final LongAdder RIGHT_SIB_KEY_COLUMN_BYTES_SAVED = new LongAdder();
  private static final LongAdder RIGHT_SIB_KEY_COLUMN_CANDIDATE_PAGES = new LongAdder();
  private static final LongAdder RIGHT_SIB_KEY_COLUMN_RAW_BYTES = new LongAdder();
  private static final LongAdder RIGHT_SIB_KEY_COLUMN_ENCODED_BYTES = new LongAdder();

  /**
   * Region-table bytes per region kind, indexed by {@link io.sirix.page.pax.RegionTable}'s kind
   * ordinal. Counted before compression, so this reports what each region actually holds; the on-disk
   * figure is the {@code regionTable} total in the section line above. The region table is the larger
   * half of a page on record-shaped JSON and holds a columnar copy of data the record heap also
   * carries, so knowing which kind spends the bytes is the difference between shrinking the database
   * and guessing at it.
   */
  private static final LongAdder[] REGION_BYTES_BY_KIND = newAdders(RegionTable.KIND_COUNT);

  /** Pages carrying at least one region of the given kind. */
  private static final LongAdder[] REGION_PAGES_BY_KIND = newAdders(RegionTable.KIND_COUNT);

  private static final LongAdder VALUE_ELISION_PAGES = new LongAdder();
  private static final LongAdder VALUE_ELISION_BYTES_SAVED = new LongAdder();
  private static final LongAdder NAME_KEY_ELISION_PAGES = new LongAdder();
  private static final LongAdder NAME_KEY_ELISION_BYTES_SAVED = new LongAdder();

  // ==================================================================================
  // U1 — heap composition per record kind.
  //
  // The wire body is the largest single section of a record-shaped leaf and the plan's first
  // compaction target, but "6.8 B per record" says nothing about WHICH records spend it. These four
  // arrays split the staged heap by the record's node-kind id, and separately count the payload
  // bytes that stayed INLINE in the heap because value elision did not cover that slot — the exact
  // quantity T1-d and T1-a move. A page that lost its string region leaves every fused string's
  // bytes inline, and that shows up here as inline-value bytes under the fused-string kind.
  // ==================================================================================

  /** Node-kind ids are a byte on the wire; index by the unsigned value. */
  private static final int NODE_KIND_SLOTS = 256;

  /** Populated slots per node-kind id. */
  private static final LongAdder[] HEAP_SLOTS_BY_KIND = newAdders(NODE_KIND_SLOTS);

  /** Staged (pre-compression) on-disk heap bytes per node-kind id, after every strip. */
  private static final LongAdder[] HEAP_ON_DISK_BYTES_BY_KIND = newAdders(NODE_KIND_SLOTS);

  /** Fused-primitive payload bytes that stayed inline in the heap, per node-kind id. */
  private static final LongAdder[] INLINE_VALUE_BYTES_BY_KIND = newAdders(NODE_KIND_SLOTS);

  /** Fused-primitive slots whose payload stayed inline, per node-kind id. */
  private static final LongAdder[] INLINE_VALUE_SLOTS_BY_KIND = newAdders(NODE_KIND_SLOTS);

  // Staged structural-metadata bytes, as sized by the writer for the staging buffer. These are the
  // bytes elision COSTS, against the BYTES_SAVED figures above: a lever that saves 5 B/record while
  // spending 4 B/record of metadata is nearly a wash, and only the pair of numbers says so.
  private static final LongAdder VALUE_ELISION_META_BYTES = new LongAdder();
  private static final LongAdder NAME_KEY_ELISION_META_BYTES = new LongAdder();
  private static final LongAdder ZERO_HASH_BITMAP_STAGED_BYTES = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_STAGED_BYTES = new LongAdder();
  private static final LongAdder PATH_NODE_KEY_COLUMN_STAGED_BYTES = new LongAdder();

  // ==================================================================================
  // U3 / inline path — body outcome per page, split by the page's index type.
  //
  // Value elision activates on a fraction of pages and the fraction alone cannot say why: a page
  // may hold no elidable slot at all (structure-only, or a non-document index whose records are not
  // fused primitives), or hold them and have the wire cost of naming them beat the saving. Split by
  // index type AND by "had candidates", the two causes separate.
  // ==================================================================================

  /** The page took the inline body path because it holds no populated slot. */
  public static final int INLINE_REASON_EMPTY_PAGE = 0;

  /** The page took the inline body path because offset-table template dedup aborted (> 255 templates). */
  public static final int INLINE_REASON_TEMPLATE_DEDUP_ABORTED = 1;

  /** The page took the inline body path because a record was shorter than kindId + templateId. */
  public static final int INLINE_REASON_SHORT_RECORD = 2;

  private static final int INLINE_REASON_COUNT = 3;

  private static final int INDEX_TYPE_SLOTS = indexTypeSlots();

  /** Pages that took the encoded (template-deduped) body path, per index-type id. */
  private static final LongAdder[] ENCODED_BODY_PAGES_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Encoded-path pages on which value elision activated, per index-type id. */
  private static final LongAdder[] VALUE_ELISION_PAGES_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Encoded-path pages that held no elidable fused-primitive slot at all, per index-type id. */
  private static final LongAdder[] VALUE_ELISION_NO_CANDIDATE_PAGES_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Pages that fell to the inline body path, per index-type id. */
  private static final LongAdder[] INLINE_BODY_PAGES_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Pages that fell to the inline body path, per {@code INLINE_REASON_*}. */
  private static final LongAdder[] INLINE_BODY_PAGES_BY_REASON = newAdders(INLINE_REASON_COUNT);

  // ==================================================================================
  // U2 — string-region suppression by an overflow descriptor.
  //
  // One fused string too large to stay inline becomes an overflow descriptor, and a single
  // descriptor suppresses the value region for the WHOLE page: no string dictionary, no sketch, no
  // string elision, so every other string on the page stays inline in the heap. These counters say
  // how many pages that is, how many string values and bytes it strands, and — through the
  // descriptor histogram — whether the cap is missed by one record or by many.
  // ==================================================================================

  /** Pages that reached the string-region decision inside the region build (the histogram's total). */
  private static final LongAdder REGION_BUILD_PAGES = new LongAdder();

  /** Pages that staged strings but wrote no string region because of an overflow descriptor. */
  private static final LongAdder STRING_REGION_SUPPRESSED_PAGES = new LongAdder();

  /** String values stranded inline by that suppression. */
  private static final LongAdder STRING_REGION_SUPPRESSED_VALUES = new LongAdder();

  /** Stored bytes of those stranded values — what the heap carries because the region was dropped. */
  private static final LongAdder STRING_REGION_SUPPRESSED_BYTES = new LongAdder();

  /** Pages that did write a string region. */
  private static final LongAdder STRING_REGION_WRITTEN_PAGES = new LongAdder();

  /** Overflow descriptors per page, bucketed 0 / 1 / 2-3 / 4+. Sums to {@link #REGION_BUILD_PAGES}. */
  private static final LongAdder[] OVERFLOW_DESCRIPTOR_HISTOGRAM = newAdders(4);

  // ==================================================================================
  // U4 — post-envelope region bytes.
  //
  // {@link #REGION_BYTES_BY_KIND} counts a region's RAW payload; what the file pays is the payload
  // after per-region LZ77 plus its framing. Only the written figure can say whether the number
  // region's raw bulk survives compression (it does, at 64-bit hash entropy) or evaporates.
  // ==================================================================================

  /** Bytes actually written for each region kind, framing included, per {@link RegionTable}'s ordinal. */
  private static final LongAdder[] REGION_WRITTEN_BYTES_BY_KIND = newAdders(RegionTable.KIND_COUNT);

  /** Regions written of each kind. */
  private static final LongAdder[] REGION_WRITTEN_COUNT_BY_KIND = newAdders(RegionTable.KIND_COUNT);

  /** Regions of each kind whose LZ77 form won the wire-size comparison. */
  private static final LongAdder[] REGION_WRITTEN_LZ77_COUNT_BY_KIND = newAdders(RegionTable.KIND_COUNT);

  private static LongAdder[] newAdders(final int n) {
    final LongAdder[] adders = new LongAdder[n];
    for (int i = 0; i < n; i++) {
      adders[i] = new LongAdder();
    }
    return adders;
  }

  /** Record one region's on-disk payload size. */
  public static void recordRegion(final int kind, final long payloadBytes) {
    if (kind < 0 || kind >= RegionTable.KIND_COUNT) {
      return;
    }
    REGION_BYTES_BY_KIND[kind].add(payloadBytes);
    REGION_PAGES_BY_KIND[kind].increment();
  }

  /**
   * Record activation of value elision — the lever that drops a fused primitive's payload from the
   * heap because the region can reconstruct it. Without it the value is stored twice.
   */
  public static void recordValueElision(final long bytesSaved) {
    VALUE_ELISION_PAGES.increment();
    VALUE_ELISION_BYTES_SAVED.add(bytesSaved);
  }

  /** Record activation of name-key elision, the same idea for the fused nameKey varint. */
  public static void recordNameKeyElision(final long bytesSaved) {
    NAME_KEY_ELISION_PAGES.increment();
    NAME_KEY_ELISION_BYTES_SAVED.add(bytesSaved);
  }

  /**
   * Record one node kind's contribution to a single page's staged heap.
   *
   * <p>
   * Called once per (page, distinct node kind), never per slot: the caller folds its slots into
   * stack locals first, so a 1,024-slot page pays at most one call per kind present.
   *
   * @param nodeKindId the record's node-kind id as it appears in the compact directory
   * @param slots populated slots of that kind on this page
   * @param onDiskHeapBytes their staged heap bytes, after every strip the page's levers applied
   * @param inlineValueBytes payload bytes of those slots that stayed INLINE because value elision did
   *        not cover them (0 for kinds that carry no elidable payload)
   * @param inlineValueSlots how many of those slots kept their payload inline
   */
  public static void recordHeapKind(final int nodeKindId, final long slots, final long onDiskHeapBytes,
      final long inlineValueBytes, final long inlineValueSlots) {
    if (nodeKindId < 0 || nodeKindId >= NODE_KIND_SLOTS) {
      return;
    }
    HEAP_SLOTS_BY_KIND[nodeKindId].add(slots);
    HEAP_ON_DISK_BYTES_BY_KIND[nodeKindId].add(onDiskHeapBytes);
    if (inlineValueBytes != 0L) {
      INLINE_VALUE_BYTES_BY_KIND[nodeKindId].add(inlineValueBytes);
    }
    if (inlineValueSlots != 0L) {
      INLINE_VALUE_SLOTS_BY_KIND[nodeKindId].add(inlineValueSlots);
    }
  }

  /**
   * Record the structural-metadata bytes one page STAGED, exactly as the writer sized them for the
   * staging buffer (each {@code 0} when its lever did not activate). These are the bytes the levers
   * cost, to be read against the {@code bytesSaved} counters.
   *
   * @param valueElisionBytes the value-elision section (count prefix + per-slot gap/type/width/index
   *        varints)
   * @param nameKeyElisionBytes the name-key elision section (count prefix + one width byte per slot)
   * @param zeroHashBitmapBytes the zero-hash bitmap
   * @param parentKeyColumnBytes the parent-key column, length prefix included
   * @param pathNodeKeyColumnBytes the pathNodeKey column, length prefix included
   */
  public static void recordStagedElisionMetadata(final long valueElisionBytes, final long nameKeyElisionBytes,
      final long zeroHashBitmapBytes, final long parentKeyColumnBytes, final long pathNodeKeyColumnBytes) {
    VALUE_ELISION_META_BYTES.add(valueElisionBytes);
    NAME_KEY_ELISION_META_BYTES.add(nameKeyElisionBytes);
    ZERO_HASH_BITMAP_STAGED_BYTES.add(zeroHashBitmapBytes);
    PARENT_KEY_COLUMN_STAGED_BYTES.add(parentKeyColumnBytes);
    PATH_NODE_KEY_COLUMN_STAGED_BYTES.add(pathNodeKeyColumnBytes);
  }

  /**
   * Record one page that took the encoded (template-deduped) body path.
   *
   * @param indexTypeId {@link IndexType#getID()} of the page's index type
   * @param valueElisionActive whether value elision activated on this page
   * @param hasValueElisionCandidates whether the page held any fused-primitive slot at all — the
   *        difference between "nothing on this page could ever be elided" and "elision was possible
   *        and refused"
   */
  public static void recordEncodedBodyOutcome(final int indexTypeId, final boolean valueElisionActive,
      final boolean hasValueElisionCandidates) {
    final int slot = indexTypeSlot(indexTypeId);
    ENCODED_BODY_PAGES_BY_INDEX_TYPE[slot].increment();
    if (valueElisionActive) {
      VALUE_ELISION_PAGES_BY_INDEX_TYPE[slot].increment();
    } else if (!hasValueElisionCandidates) {
      VALUE_ELISION_NO_CANDIDATE_PAGES_BY_INDEX_TYPE[slot].increment();
    }
  }

  /**
   * Record one page that fell to the inline body path — no template dedup, and therefore none of the
   * elisions either.
   *
   * @param indexTypeId {@link IndexType#getID()} of the page's index type
   * @param reason one of {@link #INLINE_REASON_EMPTY_PAGE},
   *        {@link #INLINE_REASON_TEMPLATE_DEDUP_ABORTED}, {@link #INLINE_REASON_SHORT_RECORD}
   */
  public static void recordInlineBodyPage(final int indexTypeId, final int reason) {
    INLINE_BODY_PAGES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].increment();
    if (reason >= 0 && reason < INLINE_REASON_COUNT) {
      INLINE_BODY_PAGES_BY_REASON[reason].increment();
    }
  }

  /**
   * Record one page's string-region outcome, at the point the region build decides whether to publish
   * the string region.
   *
   * @param suppressedByOverflow whether a fused-string overflow descriptor suppressed the region
   * @param overflowDescriptorCount fused-string overflow descriptors seen on the page
   * @param stagedStringValues string values staged for the region
   * @param stagedStringBytes their stored bytes — stranded inline in the heap when suppressed
   */
  public static void recordStringRegionOutcome(final boolean suppressedByOverflow, final int overflowDescriptorCount,
      final long stagedStringValues, final long stagedStringBytes) {
    REGION_BUILD_PAGES.increment();
    OVERFLOW_DESCRIPTOR_HISTOGRAM[overflowBucket(overflowDescriptorCount)].increment();
    if (suppressedByOverflow) {
      STRING_REGION_SUPPRESSED_PAGES.increment();
      STRING_REGION_SUPPRESSED_VALUES.add(stagedStringValues);
      STRING_REGION_SUPPRESSED_BYTES.add(stagedStringBytes);
    } else if (stagedStringValues > 0L) {
      STRING_REGION_WRITTEN_PAGES.increment();
    }
  }

  /**
   * Record one region's bytes AS WRITTEN — kind tag, payload-form byte, length fields and the
   * (possibly LZ77-compressed) payload. The counterpart to {@link #recordRegion}, which counts the
   * raw payload the encoder produced.
   *
   * @param kind {@link RegionTable}'s kind ordinal
   * @param writtenBytes bytes the region occupied on the wire, framing included
   * @param compressed whether the LZ77 form won the wire-size comparison
   */
  public static void recordRegionWritten(final int kind, final long writtenBytes, final boolean compressed) {
    if (kind < 0 || kind >= RegionTable.KIND_COUNT) {
      return;
    }
    REGION_WRITTEN_BYTES_BY_KIND[kind].add(writtenBytes);
    REGION_WRITTEN_COUNT_BY_KIND[kind].increment();
    if (compressed) {
      REGION_WRITTEN_LZ77_COUNT_BY_KIND[kind].increment();
    }
  }

  /** Histogram bucket for a page's overflow-descriptor count: 0, 1, 2-3, 4+. */
  private static int overflowBucket(final int count) {
    if (count <= 0) {
      return 0;
    }
    if (count == 1) {
      return 1;
    }
    return count <= 3
        ? 2
        : 3;
  }

  /** One slot per {@link IndexType} id, plus a trailing catch-all for an id outside the enum. */
  private static int indexTypeSlots() {
    int max = 0;
    for (final IndexType type : IndexType.values()) {
      final int id = type.getID() & 0xFF;
      if (id > max) {
        max = id;
      }
    }
    return max + 2;
  }

  /** The array slot for an index-type id; an id outside the enum lands in the trailing catch-all. */
  private static int indexTypeSlot(final int indexTypeId) {
    return indexTypeId >= 0 && indexTypeId < INDEX_TYPE_SLOTS - 1
        ? indexTypeId
        : INDEX_TYPE_SLOTS - 1;
  }

  /** Display name for an index-type slot; the catch-all and unknown ids print numerically. */
  private static String indexTypeName(final int slot) {
    for (final IndexType type : IndexType.values()) {
      if ((type.getID() & 0xFF) == slot) {
        return type.name();
      }
    }
    return "id#" + slot;
  }

  /** Display name for a node-kind id; an id no {@link NodeKind} claims prints numerically. */
  private static String nodeKindName(final int nodeKindId) {
    try {
      return NodeKind.getKind((byte) nodeKindId).name();
    } catch (final IllegalStateException unknown) {
      return "kind#" + nodeKindId;
    }
  }

  // Per-codec selection counters (pages for which each codec was chosen as
  // smallest). Exercised by the write path's pick-smallest logic between
  // ZeroRunByteCodec (0), ByteRunCodec (2), and SirixLZ77Codec (3).
  private static final LongAdder CODEC_ZERORUN_PAGES = new LongAdder();
  private static final LongAdder CODEC_BYTERUN_PAGES = new LongAdder();
  private static final LongAdder CODEC_LZ77_PAGES = new LongAdder();
  private static final LongAdder CODEC_ZERORUN_BYTES = new LongAdder();
  private static final LongAdder CODEC_BYTERUN_BYTES = new LongAdder();
  private static final LongAdder CODEC_LZ77_BYTES = new LongAdder();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(PageSectionDiag::dumpStats, "page-section-diag-dump"));
  }

  private PageSectionDiag() {
    throw new AssertionError();
  }

  /**
   * Accumulate per-section byte counts for the encodedBody breakdown (compactDir, templatePool +
   * slotIds, compressedHeap incl. length+codec).
   */
  public static void recordEncodedBody(final long compactDir, final long templatePool, final long compressedHeap) {
    recordEncodedBody(compactDir, templatePool, compressedHeap, 0L);
  }

  /**
   * Record one encoded body: {@code compactDir} and {@code templatePool} PRE-compression, {@code bodyOnWire}
   * the whole body blob as written (it contains the directory, the templates and the heap, compressed
   * together), {@code stagingBytes} the whole pre-compression staged body ({@code 0} when unknown). The
   * report derives the raw heap as staging minus directory minus templates and the compression ratio as
   * wire over staging — the earlier report added the pre-compression directory and templates to the
   * on-wire body and called the sum "encoded body", double counting them.
   */
  public static void recordEncodedBody(final long compactDir, final long templatePool, final long bodyOnWire,
      final long stagingBytes) {
    COMPACT_DIR_BYTES.add(compactDir);
    TEMPLATE_POOL_BYTES.add(templatePool);
    COMPRESSED_HEAP_BYTES.add(bodyOnWire);
    BODY_STAGING_BYTES.add(stagingBytes);
  }

  /** Record the populated slots of one serialized page (per-record averages). */
  public static void recordRecords(final int populatedSlots) {
    RECORDS.add(populatedSlots);
  }

  /**
   * Record activation of the hash-elision structural encoder on a single page along with the number
   * of pre-compression bytes stripped.
   */
  public static void recordHashElision(final long bytesSaved) {
    HASH_ELISION_PAGES.increment();
    HASH_ELISION_BYTES_SAVED.add(bytesSaved);
  }

  /**
   * Record activation of the parent-key column extractor on a single page along with the number of
   * pre-compression bytes it displaced from the heap body.
   */
  public static void recordParentKeyColumn(final long bytesSaved) {
    PARENT_KEY_COLUMN_PAGES.increment();
    PARENT_KEY_COLUMN_BYTES_SAVED.add(bytesSaved);
  }

  /**
   * Record a parent-key column candidate (a page with at least one slot whose kind has a parent-key
   * field) regardless of whether the column ultimately paid off. Used to diagnose why the column
   * fails to activate.
   */
  public static void recordParentKeyColumnCandidate(final long rawStrippedBytes, final long encodedColumnBytes) {
    PARENT_KEY_COLUMN_CANDIDATE_PAGES.increment();
    PARENT_KEY_COLUMN_RAW_BYTES.add(rawStrippedBytes);
    PARENT_KEY_COLUMN_ENCODED_BYTES.add(encodedColumnBytes);
  }

  /**
   * Record activation of the right-sibling-key column extractor on a single page along with the
   * number of pre-compression bytes it displaced from the heap body.
   */
  public static void recordRightSibKeyColumn(final long bytesSaved) {
    RIGHT_SIB_KEY_COLUMN_PAGES.increment();
    RIGHT_SIB_KEY_COLUMN_BYTES_SAVED.add(bytesSaved);
  }

  /**
   * Record a right-sibling-key column candidate (a page with at least one slot whose kind has a
   * right-sibling-key field) regardless of whether the column ultimately paid off. Used to diagnose
   * why the column fails to activate.
   */
  public static void recordRightSibKeyColumnCandidate(final long rawStrippedBytes, final long encodedColumnBytes) {
    RIGHT_SIB_KEY_COLUMN_CANDIDATE_PAGES.increment();
    RIGHT_SIB_KEY_COLUMN_RAW_BYTES.add(rawStrippedBytes);
    RIGHT_SIB_KEY_COLUMN_ENCODED_BYTES.add(encodedColumnBytes);
  }

  /** Record that ZeroRunByteCodec (codec=0) was chosen for this page. */
  public static void recordCodecZeroRun(final long encodedBytes) {
    CODEC_ZERORUN_PAGES.increment();
    CODEC_ZERORUN_BYTES.add(encodedBytes);
  }

  /** Record that ByteRunCodec (codec=2) was chosen for this page. */
  public static void recordCodecByteRun(final long encodedBytes) {
    CODEC_BYTERUN_PAGES.increment();
    CODEC_BYTERUN_BYTES.add(encodedBytes);
  }

  /** Record that SirixLZ77Codec (codec=3) was chosen for this page. */
  public static void recordCodecLz77(final long encodedBytes) {
    CODEC_LZ77_PAGES.increment();
    CODEC_LZ77_BYTES.add(encodedBytes);
  }

  /**
   * Accumulate one page's per-section byte counts.
   *
   * @param headerBitmap bytes written for the 160-byte header + bitmap prefix
   * @param encodedBody bytes written for the compact-dir + template pool + compressed heap
   * @param regionTable bytes written for the PAX region table
   * @param overlong bytes written for the overlong-entries bitmap + references
   * @param fsst bytes written for the FSST symbol table
   */
  public static void record(final long headerBitmap, final long encodedBody, final long regionTable,
      final long overlong, final long fsst) {
    PAGE_COUNT.increment();
    HEADER_BITMAP_BYTES.add(headerBitmap);
    ENCODED_BODY_BYTES.add(encodedBody);
    REGION_TABLE_BYTES.add(regionTable);
    OVERLONG_BYTES.add(overlong);
    FSST_BYTES.add(fsst);
  }

  private static void dumpStats() {
    // StorageProfile has a separate shutdown hook. Serialize their complete reports rather than
    // relying on PrintStream's per-call lock, which permits line-level interleaving.
    synchronized (System.out) {
      dumpStatsLocked();
    }
  }

  private static void dumpStatsLocked() {
    final long pages = PAGE_COUNT.sum();
    if (pages == 0)
      return;
    final long hb = HEADER_BITMAP_BYTES.sum();
    final long eb = ENCODED_BODY_BYTES.sum();
    final long rt = REGION_TABLE_BYTES.sum();
    final long ov = OVERLONG_BYTES.sum();
    final long fsst = FSST_BYTES.sum();
    final long total = hb + eb + rt + ov + fsst;
    final String fmt = "[PageSectionDiag] pages=%,d total=%,d B (%.1f MB)  headerBitmap=%,d (%.1f%%)"
        + "  encodedBody=%,d (%.1f%%)  regionTable=%,d (%.1f%%)" + "  overlong=%,d (%.1f%%)  fsst=%,d (%.1f%%)%n";
    System.out.printf(fmt, pages, total, total / (1024.0 * 1024.0), hb, pct(hb, total), eb, pct(eb, total), rt,
        pct(rt, total), ov, pct(ov, total), fsst, pct(fsst, total));
    final long cd = COMPACT_DIR_BYTES.sum();
    final long tp = TEMPLATE_POOL_BYTES.sum();
    final long wire = COMPRESSED_HEAP_BYTES.sum();
    final long staging = BODY_STAGING_BYTES.sum();
    final long rawHeap = Math.max(0L, staging - cd - tp);
    final long records = RECORDS.sum();
    System.out.printf(
        "[PageSectionDiag] encodedBody on wire=%,d B; pre-compression staging=%,d B = compactDir %,d (%.1f%%)"
            + " + templatePool+slotIds %,d (%.1f%%) + heap %,d (%.1f%%); wire/staging=%.3f%n",
        wire, staging, cd, pct(cd, staging), tp, pct(tp, staging), rawHeap, pct(rawHeap, staging),
        staging == 0 ? 0.0 : wire / (double) staging);
    if (records > 0) {
      System.out.printf(
          "[PageSectionDiag] per record (%,d records over %,d serializations; re-serialized pages count again):"
              + " page %.2f B, headerBitmap %.2f, body on wire %.2f (pre-compression: dir %.2f, templates %.2f,"
              + " heap %.2f), regionTable %.2f, overlong %.2f, fsst %.2f%n",
          records, pages, total / (double) records, hb / (double) records, eb / (double) records,
          cd / (double) records, tp / (double) records, rawHeap / (double) records, rt / (double) records,
          ov / (double) records, fsst / (double) records);
    }
    final long hePages = HASH_ELISION_PAGES.sum();
    final long heBytes = HASH_ELISION_BYTES_SAVED.sum();
    final long pkPages = PARENT_KEY_COLUMN_PAGES.sum();
    final long pkBytes = PARENT_KEY_COLUMN_BYTES_SAVED.sum();
    System.out.printf("[PageSectionDiag] encoders: hashElision pages=%,d (%.1f%%)  bytesSaved=%,d (%.1f MB)%n", hePages,
        pct(hePages, pages), heBytes, heBytes / (1024.0 * 1024.0));
    System.out.printf("[PageSectionDiag] encoders: parentKeyColumn pages=%,d (%.1f%%)  bytesSaved=%,d (%.1f MB)%n",
        pkPages, pct(pkPages, pages), pkBytes, pkBytes / (1024.0 * 1024.0));
    final long pkCandidates = PARENT_KEY_COLUMN_CANDIDATE_PAGES.sum();
    final long pkRaw = PARENT_KEY_COLUMN_RAW_BYTES.sum();
    final long pkEncoded = PARENT_KEY_COLUMN_ENCODED_BYTES.sum();
    System.out.printf(
        "[PageSectionDiag] parentKeyColumn candidates=%,d rawBytes=%,d (%.1f MB)"
            + "  encodedBytes=%,d (%.1f MB)  avgRaw/page=%.1f  avgEncoded/page=%.1f%n",
        pkCandidates, pkRaw, pkRaw / (1024.0 * 1024.0), pkEncoded, pkEncoded / (1024.0 * 1024.0), pkCandidates == 0
            ? 0
            : (double) pkRaw / pkCandidates,
        pkCandidates == 0
            ? 0
            : (double) pkEncoded / pkCandidates);
    final long rsPages = RIGHT_SIB_KEY_COLUMN_PAGES.sum();
    final long rsBytes = RIGHT_SIB_KEY_COLUMN_BYTES_SAVED.sum();
    System.out.printf("[PageSectionDiag] encoders: rightSibKeyColumn pages=%,d (%.1f%%)  bytesSaved=%,d (%.1f MB)%n",
        rsPages, pct(rsPages, pages), rsBytes, rsBytes / (1024.0 * 1024.0));
    final long rsCandidates = RIGHT_SIB_KEY_COLUMN_CANDIDATE_PAGES.sum();
    final long rsRaw = RIGHT_SIB_KEY_COLUMN_RAW_BYTES.sum();
    final long rsEncoded = RIGHT_SIB_KEY_COLUMN_ENCODED_BYTES.sum();
    System.out.printf(
        "[PageSectionDiag] rightSibKeyColumn candidates=%,d rawBytes=%,d (%.1f MB)"
            + "  encodedBytes=%,d (%.1f MB)  avgRaw/page=%.1f  avgEncoded/page=%.1f%n",
        rsCandidates, rsRaw, rsRaw / (1024.0 * 1024.0), rsEncoded, rsEncoded / (1024.0 * 1024.0), rsCandidates == 0
            ? 0
            : (double) rsRaw / rsCandidates,
        rsCandidates == 0
            ? 0
            : (double) rsEncoded / rsCandidates);
    final long veP = VALUE_ELISION_PAGES.sum();
    final long veB = VALUE_ELISION_BYTES_SAVED.sum();
    final long nkP = NAME_KEY_ELISION_PAGES.sum();
    final long nkB = NAME_KEY_ELISION_BYTES_SAVED.sum();
    System.out.printf(
        "[PageSectionDiag] encoders: valueElision pages=%,d (%.1f%%) bytesSaved=%,d (%.1f MB)"
            + "   nameKeyElision pages=%,d (%.1f%%) bytesSaved=%,d (%.1f MB)%n",
        veP, pct(veP, pages), veB, veB / (1024.0 * 1024.0), nkP, pct(nkP, pages), nkB, nkB / (1024.0 * 1024.0));
    long regionTotal = 0;
    for (int kind = 0; kind < RegionTable.KIND_COUNT; kind++) {
      regionTotal += REGION_BYTES_BY_KIND[kind].sum();
    }
    for (int kind = 0; kind < RegionTable.KIND_COUNT; kind++) {
      final long bytes = REGION_BYTES_BY_KIND[kind].sum();
      if (bytes == 0) {
        continue;
      }
      final long regionPages = REGION_PAGES_BY_KIND[kind].sum();
      System.out.printf(
          "[PageSectionDiag] region %-14s pages=%,d (%.1f%%)  rawBytes=%,d (%.1f MB)  %.1f%% of raw regions%n",
          RegionTable.kindName(kind), regionPages, pct(regionPages, pages), bytes, bytes / (1024.0 * 1024.0),
          pct(bytes, regionTotal));
    }
    // ---- U1: what the levers cost, against what they save ----
    final long veMeta = VALUE_ELISION_META_BYTES.sum();
    final long nkMeta = NAME_KEY_ELISION_META_BYTES.sum();
    final long hashMeta = ZERO_HASH_BITMAP_STAGED_BYTES.sum();
    final long pkMeta = PARENT_KEY_COLUMN_STAGED_BYTES.sum();
    final long pnkMeta = PATH_NODE_KEY_COLUMN_STAGED_BYTES.sum();
    final long stagedMeta = veMeta + nkMeta + hashMeta + pkMeta + pnkMeta;
    System.out.printf(
        "[PageSectionDiag] staged elision metadata=%,d B (%.2f B/record): valueElision %,d (%.2f)"
            + "  nameKeyElision %,d (%.2f)  zeroHashBitmap %,d (%.2f)  parentKeyColumn %,d (%.2f)"
            + "  pathNodeKeyColumn %,d (%.2f)%n",
        stagedMeta, perRecord(stagedMeta, records), veMeta, perRecord(veMeta, records), nkMeta,
        perRecord(nkMeta, records), hashMeta, perRecord(hashMeta, records), pkMeta, perRecord(pkMeta, records), pnkMeta,
        perRecord(pnkMeta, records));

    // ---- U1: staged heap split by record kind, with the payload that stayed inline ----
    long heapKindTotal = 0;
    long inlineValueTotal = 0;
    for (int kind = 0; kind < NODE_KIND_SLOTS; kind++) {
      heapKindTotal += HEAP_ON_DISK_BYTES_BY_KIND[kind].sum();
      inlineValueTotal += INLINE_VALUE_BYTES_BY_KIND[kind].sum();
    }
    if (heapKindTotal > 0) {
      System.out.printf(
          "[PageSectionDiag] staged heap by record kind: %,d B over %,d records (%.2f B/record);"
              + " payload left INLINE by value elision: %,d B (%.2f B/record)%n",
          heapKindTotal, records, perRecord(heapKindTotal, records), inlineValueTotal,
          perRecord(inlineValueTotal, records));
      for (int kind = 0; kind < NODE_KIND_SLOTS; kind++) {
        final long kindSlots = HEAP_SLOTS_BY_KIND[kind].sum();
        if (kindSlots == 0) {
          continue;
        }
        final long kindBytes = HEAP_ON_DISK_BYTES_BY_KIND[kind].sum();
        final long kindInlineBytes = INLINE_VALUE_BYTES_BY_KIND[kind].sum();
        final long kindInlineSlots = INLINE_VALUE_SLOTS_BY_KIND[kind].sum();
        System.out.printf(
            "[PageSectionDiag]   heap kind %3d %-28s slots=%,d (%.1f%%)  bytes=%,d (%.1f%% of heap, %.2f B/slot)"
                + "  inlineValue=%,d B over %,d slots (%.2f B/slot of that kind)%n",
            kind, nodeKindName(kind), kindSlots, pct(kindSlots, records), kindBytes, pct(kindBytes, heapKindTotal),
            perRecord(kindBytes, kindSlots), kindInlineBytes, kindInlineSlots, perRecord(kindInlineBytes, kindSlots));
      }
    }

    // ---- U3 + inline path: body outcome per index type ----
    long encodedPagesTotal = 0;
    long inlinePagesTotal = 0;
    for (int slot = 0; slot < INDEX_TYPE_SLOTS; slot++) {
      encodedPagesTotal += ENCODED_BODY_PAGES_BY_INDEX_TYPE[slot].sum();
      inlinePagesTotal += INLINE_BODY_PAGES_BY_INDEX_TYPE[slot].sum();
    }
    if (encodedPagesTotal + inlinePagesTotal > 0) {
      System.out.printf("[PageSectionDiag] body path: encoded=%,d pages  inline=%,d pages (%.1f%%)"
          + "  [emptyPage=%,d  templateDedupAborted=%,d  shortRecord=%,d]%n", encodedPagesTotal, inlinePagesTotal,
          pct(inlinePagesTotal, encodedPagesTotal + inlinePagesTotal),
          INLINE_BODY_PAGES_BY_REASON[INLINE_REASON_EMPTY_PAGE].sum(),
          INLINE_BODY_PAGES_BY_REASON[INLINE_REASON_TEMPLATE_DEDUP_ABORTED].sum(),
          INLINE_BODY_PAGES_BY_REASON[INLINE_REASON_SHORT_RECORD].sum());
      for (int slot = 0; slot < INDEX_TYPE_SLOTS; slot++) {
        final long encodedPages = ENCODED_BODY_PAGES_BY_INDEX_TYPE[slot].sum();
        final long inlinePages = INLINE_BODY_PAGES_BY_INDEX_TYPE[slot].sum();
        if (encodedPages == 0 && inlinePages == 0) {
          continue;
        }
        final long elidedPages = VALUE_ELISION_PAGES_BY_INDEX_TYPE[slot].sum();
        final long noCandidatePages = VALUE_ELISION_NO_CANDIDATE_PAGES_BY_INDEX_TYPE[slot].sum();
        final long refusedPages = encodedPages - elidedPages - noCandidatePages;
        System.out.printf(
            "[PageSectionDiag]   indexType %-18s encoded=%,d  inline=%,d  valueElision active=%,d (%.1f%% of encoded)"
                + "  noCandidate=%,d  refused=%,d%n",
            indexTypeName(slot), encodedPages, inlinePages, elidedPages, pct(elidedPages, encodedPages),
            noCandidatePages, refusedPages);
      }
    }

    // ---- U2: string region suppressed by an overflow descriptor ----
    final long regionBuildPages = REGION_BUILD_PAGES.sum();
    if (regionBuildPages > 0) {
      final long suppressedPages = STRING_REGION_SUPPRESSED_PAGES.sum();
      final long suppressedValues = STRING_REGION_SUPPRESSED_VALUES.sum();
      final long suppressedBytes = STRING_REGION_SUPPRESSED_BYTES.sum();
      System.out.printf(
          "[PageSectionDiag] stringRegion: built on %,d pages; written=%,d  suppressedByOverflow=%,d (%.1f%%)"
              + "  strandedValues=%,d  strandedBytes=%,d (%.1f MB, %.2f B/record)%n",
          regionBuildPages, STRING_REGION_WRITTEN_PAGES.sum(), suppressedPages, pct(suppressedPages, regionBuildPages),
          suppressedValues, suppressedBytes, suppressedBytes / (1024.0 * 1024.0), perRecord(suppressedBytes, records));
      System.out.printf(
          "[PageSectionDiag] overflow descriptors per page: 0=%,d (%.1f%%)  1=%,d (%.1f%%)  2-3=%,d (%.1f%%)"
              + "  4+=%,d (%.1f%%)  [sums to %,d region-build pages]%n",
          OVERFLOW_DESCRIPTOR_HISTOGRAM[0].sum(), pct(OVERFLOW_DESCRIPTOR_HISTOGRAM[0].sum(), regionBuildPages),
          OVERFLOW_DESCRIPTOR_HISTOGRAM[1].sum(), pct(OVERFLOW_DESCRIPTOR_HISTOGRAM[1].sum(), regionBuildPages),
          OVERFLOW_DESCRIPTOR_HISTOGRAM[2].sum(), pct(OVERFLOW_DESCRIPTOR_HISTOGRAM[2].sum(), regionBuildPages),
          OVERFLOW_DESCRIPTOR_HISTOGRAM[3].sum(), pct(OVERFLOW_DESCRIPTOR_HISTOGRAM[3].sum(), regionBuildPages),
          regionBuildPages);
    }

    // ---- U4: what each region kind costs AS WRITTEN, against its raw payload ----
    long regionWrittenTotal = 0;
    for (int kind = 0; kind < RegionTable.KIND_COUNT; kind++) {
      regionWrittenTotal += REGION_WRITTEN_BYTES_BY_KIND[kind].sum();
    }
    if (regionWrittenTotal > 0) {
      System.out.printf("[PageSectionDiag] region table as written=%,d B (%.1f MB, %.2f B/record)%n",
          regionWrittenTotal, regionWrittenTotal / (1024.0 * 1024.0), perRecord(regionWrittenTotal, records));
      for (int kind = 0; kind < RegionTable.KIND_COUNT; kind++) {
        final long writtenBytes = REGION_WRITTEN_BYTES_BY_KIND[kind].sum();
        if (writtenBytes == 0) {
          continue;
        }
        final long rawBytes = REGION_BYTES_BY_KIND[kind].sum();
        final long writtenCount = REGION_WRITTEN_COUNT_BY_KIND[kind].sum();
        final long lz77Count = REGION_WRITTEN_LZ77_COUNT_BY_KIND[kind].sum();
        System.out.printf(
            "[PageSectionDiag]   region %-14s written=%,d B (%.1f%% of written, %.2f B/record) raw=%,d B"
                + "  written/raw=%.3f  regions=%,d  lz77 won=%,d (%.1f%%)%n",
            RegionTable.kindName(kind), writtenBytes, pct(writtenBytes, regionWrittenTotal),
            perRecord(writtenBytes, records), rawBytes, rawBytes == 0
                ? 0.0
                : writtenBytes / (double) rawBytes,
            writtenCount, lz77Count, pct(lz77Count, writtenCount));
      }
    }

    final long cZero = CODEC_ZERORUN_PAGES.sum();
    final long cByte = CODEC_BYTERUN_PAGES.sum();
    final long cLz77 = CODEC_LZ77_PAGES.sum();
    final long cZeroB = CODEC_ZERORUN_BYTES.sum();
    final long cByteB = CODEC_BYTERUN_BYTES.sum();
    final long cLz77B = CODEC_LZ77_BYTES.sum();
    final long cTotalPages = cZero + cByte + cLz77;
    final long cTotalBytes = cZeroB + cByteB + cLz77B;
    if (cTotalPages > 0) {
      System.out.printf(
          "[PageSectionDiag] codec wins: zeroRun=%,d (%.1f%%) bytes=%,d (%.1f MB)"
              + "  byteRun=%,d (%.1f%%) bytes=%,d (%.1f MB)" + "  lz77=%,d (%.1f%%) bytes=%,d (%.1f MB)%n",
          cZero, pct(cZero, cTotalPages), cZeroB, cZeroB / (1024.0 * 1024.0), cByte, pct(cByte, cTotalPages), cByteB,
          cByteB / (1024.0 * 1024.0), cLz77, pct(cLz77, cTotalPages), cLz77B, cLz77B / (1024.0 * 1024.0));
      if (cTotalBytes > 0) {
        System.out.printf("[PageSectionDiag] codec total encoded bytes: %,d (%.1f MB) avg=%.1f/page%n", cTotalBytes,
            cTotalBytes / (1024.0 * 1024.0), (double) cTotalBytes / cTotalPages);
      }
    }
  }

  // ==================================================================================
  // Test seam — package-private snapshot accessors.
  //
  // The counters are otherwise write-only until the shutdown hook prints them, and a witness that
  // reads its instrument only after the JVM exits is no witness. These give a same-package test the
  // before/after deltas it needs to prove each counter is wired to the site it names. Nothing on the
  // write path reads them.
  // ==================================================================================

  /** Populated slots counted over every serialization. */
  static long recordsCounted() {
    return RECORDS.sum();
  }

  /** Pages counted by {@link #record}. */
  static long pagesCounted() {
    return PAGE_COUNT.sum();
  }

  /** Bytes attributed to the region-table section by {@link #record}. */
  static long regionTableSectionBytes() {
    return REGION_TABLE_BYTES.sum();
  }

  static long heapSlotsForKind(final int nodeKindId) {
    return HEAP_SLOTS_BY_KIND[nodeKindId].sum();
  }

  static long heapOnDiskBytesForKind(final int nodeKindId) {
    return HEAP_ON_DISK_BYTES_BY_KIND[nodeKindId].sum();
  }

  static long inlineValueBytesForKind(final int nodeKindId) {
    return INLINE_VALUE_BYTES_BY_KIND[nodeKindId].sum();
  }

  static long inlineValueSlotsForKind(final int nodeKindId) {
    return INLINE_VALUE_SLOTS_BY_KIND[nodeKindId].sum();
  }

  static long stagedValueElisionMetaBytes() {
    return VALUE_ELISION_META_BYTES.sum();
  }

  static long stagedNameKeyElisionMetaBytes() {
    return NAME_KEY_ELISION_META_BYTES.sum();
  }

  static long encodedBodyPagesForIndexType(final int indexTypeId) {
    return ENCODED_BODY_PAGES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].sum();
  }

  static long valueElisionPagesForIndexType(final int indexTypeId) {
    return VALUE_ELISION_PAGES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].sum();
  }

  static long valueElisionNoCandidatePagesForIndexType(final int indexTypeId) {
    return VALUE_ELISION_NO_CANDIDATE_PAGES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].sum();
  }

  static long inlineBodyPagesForIndexType(final int indexTypeId) {
    return INLINE_BODY_PAGES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].sum();
  }

  static long inlineBodyPagesForReason(final int reason) {
    return INLINE_BODY_PAGES_BY_REASON[reason].sum();
  }

  static long regionBuildPages() {
    return REGION_BUILD_PAGES.sum();
  }

  static long stringRegionSuppressedPages() {
    return STRING_REGION_SUPPRESSED_PAGES.sum();
  }

  static long stringRegionSuppressedValues() {
    return STRING_REGION_SUPPRESSED_VALUES.sum();
  }

  static long stringRegionSuppressedBytes() {
    return STRING_REGION_SUPPRESSED_BYTES.sum();
  }

  static long overflowDescriptorHistogramBucket(final int bucket) {
    return OVERFLOW_DESCRIPTOR_HISTOGRAM[bucket].sum();
  }

  /** Buckets in the overflow-descriptor histogram: 0, 1, 2-3, 4+. */
  static int overflowDescriptorHistogramBuckets() {
    return OVERFLOW_DESCRIPTOR_HISTOGRAM.length;
  }

  static long regionRawBytesForKind(final int kind) {
    return REGION_BYTES_BY_KIND[kind].sum();
  }

  static long regionWrittenBytesForKind(final int kind) {
    return REGION_WRITTEN_BYTES_BY_KIND[kind].sum();
  }

  static long regionWrittenCountForKind(final int kind) {
    return REGION_WRITTEN_COUNT_BY_KIND[kind].sum();
  }

  /** Renders the full cumulative report on demand — the same text the shutdown hook prints. */
  static void dumpNow() {
    dumpStats();
  }

  /** Per-record average, {@code 0} when nothing was counted. */
  private static double perRecord(final long bytes, final long records) {
    return records == 0
        ? 0.0
        : bytes / (double) records;
  }

  private static double pct(final long part, final long total) {
    return total == 0
        ? 0.0
        : 100.0 * part / total;
  }
}
