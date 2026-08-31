/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.page.pax.RegionTable;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReferenceArray;
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
  private static final LongAdder LEFT_SIB_KEY_COLUMN_PAGES = new LongAdder();
  private static final LongAdder LEFT_SIB_KEY_COLUMN_BYTES_SAVED = new LongAdder();
  private static final LongAdder LEFT_SIB_KEY_COLUMN_CANDIDATE_PAGES = new LongAdder();
  private static final LongAdder LEFT_SIB_KEY_COLUMN_RAW_BYTES = new LongAdder();
  private static final LongAdder LEFT_SIB_KEY_COLUMN_ENCODED_BYTES = new LongAdder();
  private static final LongAdder REVISION_ELISION_PAGES = new LongAdder();
  private static final LongAdder REVISION_ELISION_BYTES_SAVED = new LongAdder();
  private static final LongAdder REVISION_ELISION_META_BYTES = new LongAdder();

  // ─────────────────────────────────── post-codec attribution (U5, diag-gated)

  /**
   * Section ids for the post-codec attribution. The body is staged as one contiguous buffer and
   * compressed as one blob, so what any single section costs on disk is not observable from the wire —
   * only the total is. These lanes are that total taken apart: each staged section compressed ON ITS
   * OWN with the codec the page elected, which is an upper bound for its share (the whole body is
   * always at least as compressible as its parts, because the codec sees cross-section repetition
   * too). The gap between the sum and the real body is that cross-section gain, and it is reported.
   */
  public static final int SECTION_COMPACT_DIR = 0;

  /** Post-codec section: the template pool plus the per-slot template ids. */
  public static final int SECTION_TEMPLATES = 1;

  /** Post-codec section: the zero-hash bitmap. */
  public static final int SECTION_ZERO_HASH_BITMAP = 2;

  /** Post-codec section: the parentKey column. */
  public static final int SECTION_PARENT_KEY_COLUMN = 3;

  /** Post-codec section: the right-sibling column. */
  public static final int SECTION_RIGHT_SIB_COLUMN = 4;

  /** Post-codec section: the left-sibling column. */
  public static final int SECTION_LEFT_SIB_COLUMN = 5;

  /** Post-codec section: the pathNodeKey column. */
  public static final int SECTION_PATH_NODE_KEY_COLUMN = 6;

  /** Post-codec section: the value-elision section. */
  public static final int SECTION_VALUE_ELISION = 7;

  /** Post-codec section: the name-key-elision section. */
  public static final int SECTION_NAME_KEY_ELISION = 8;

  /** Post-codec section: the record heap, whole. */
  public static final int SECTION_HEAP = 9;

  /** Post-codec section: the heap bytes of fused {@code OBJECT_NAMED_*} records alone. */
  public static final int SECTION_HEAP_FUSED = 10;

  /** Post-codec section: the heap bytes of structural {@code OBJECT} / {@code ARRAY} records alone. */
  public static final int SECTION_HEAP_STRUCTURAL = 11;

  /** Post-codec section: the heap bytes of every other record kind. */
  public static final int SECTION_HEAP_OTHER = 12;

  /** Number of post-codec section lanes. */
  public static final int SECTION_COUNT = 13;

  private static final String[] SECTION_NAMES = {"compactDir", "templates+slotIds", "zeroHashBitmap",
      "parentKeyColumn", "rightSibColumn", "leftSibColumn", "pathNodeKeyColumn", "valueElision", "nameKeyElision",
      "heap", "heap:fused", "heap:structural", "heap:other"};

  private static final LongAdder[] SECTION_RAW_BYTES = newAdders(SECTION_COUNT);
  private static final LongAdder[] SECTION_ENCODED_BYTES = newAdders(SECTION_COUNT);
  private static final LongAdder POST_CODEC_PAGES = new LongAdder();
  private static final LongAdder POST_CODEC_SECTION_SUM = new LongAdder();
  private static final LongAdder POST_CODEC_ACTUAL_BODY = new LongAdder();
  private static final LongAdder COMPACT_DIR_ENTRIES = new LongAdder();
  private static final LongAdder COMPACT_DIR_PREDICTABLE_ENTRIES = new LongAdder();

  /**
   * Record one staged section's raw and standalone-compressed sizes.
   *
   * @param section one of the {@code SECTION_*} ids
   * @param rawBytes the section's staged length
   * @param encodedBytes what the page's elected codec makes of those bytes on their own
   */
  public static void recordPostCodecSection(final int section, final long rawBytes, final long encodedBytes) {
    SECTION_RAW_BYTES[section].add(rawBytes);
    SECTION_ENCODED_BYTES[section].add(encodedBytes);
  }

  /**
   * Record one page's post-codec attribution total beside what the body actually took on the wire.
   *
   * @param sectionEncodedSum sum of the sections compressed on their own
   * @param actualBodyBytes what the whole body compressed to, as written
   */
  public static void recordPostCodecBody(final long sectionEncodedSum, final long actualBodyBytes) {
    POST_CODEC_PAGES.increment();
    POST_CODEC_SECTION_SUM.add(sectionEncodedSum);
    POST_CODEC_ACTUAL_BODY.add(actualBodyBytes);
  }

  /**
   * Record how much of the compact directory a reader could have predicted.
   *
   * @param entries directory entries on the page
   * @param predictable those whose kind AND length repeat the previous entry of the same template —
   *        the fraction T1-b would be able to drop
   */
  public static void recordCompactDirPredictability(final long entries, final long predictable) {
    COMPACT_DIR_ENTRIES.add(entries);
    COMPACT_DIR_PREDICTABLE_ENTRIES.add(predictable);
  }

  /** Raw staged bytes attributed to {@code section}, for the counter witness. */
  static long postCodecSectionRawBytes(final int section) {
    return SECTION_RAW_BYTES[section].sum();
  }

  /** Standalone-compressed bytes attributed to {@code section}, for the counter witness. */
  static long postCodecSectionEncodedBytes(final int section) {
    return SECTION_ENCODED_BYTES[section].sum();
  }

  /** Pages the post-codec attribution ran on. */
  static long postCodecPages() {
    return POST_CODEC_PAGES.sum();
  }

  /** Sum over pages of the sections compressed on their own. */
  static long postCodecSectionSum() {
    return POST_CODEC_SECTION_SUM.sum();
  }

  /** Sum over pages of what the bodies actually took on the wire. */
  static long postCodecActualBody() {
    return POST_CODEC_ACTUAL_BODY.sum();
  }

  /** Compact-directory entries seen by the predictability counter. */
  static long compactDirEntries() {
    return COMPACT_DIR_ENTRIES.sum();
  }

  /** Of those, the ones a reader could have predicted from the template. */
  static long compactDirPredictableEntries() {
    return COMPACT_DIR_PREDICTABLE_ENTRIES.sum();
  }

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
  // U6 — serialization-work attribution (the discarded-encode ledger).
  //
  // A page can be FULLY encoded — body staging, template dedup, the region build and its per-region
  // LZ77 — without one byte of that work reaching the file. The async snapshot pre-serializer
  // encodes a disposable copy and may then REFUSE it, and a refused leaf is promoted back into the
  // live TIL, where the next epoch encodes it again. Byte totals cannot see this: they count bytes
  // written, not the work that produced them, and a discarded encode writes nothing.
  //
  // These lanes count the encodes themselves, split by the call path that asked for one and by the
  // page's index type, time them, and reconcile them against the pages the writer actually
  // appended. The repeat ledger answers the question a ratio cannot: whether an excess encode is
  // the SAME page key encoded again, and whether that repeat happened inside one flush epoch (a
  // genuine redundancy) or across epochs (a page that keeps being refused, or keeps changing).
  // ==================================================================================

  /**
   * Whether the section diagnostic is active in this JVM. Public because the ingest and flush paths
   * that feed the U6 lanes live outside this package; every U6 call site is guarded by it, so all of
   * U6 folds away when the property is absent.
   */
  public static final boolean ENABLED = Boolean.getBoolean("sirix.pageSectionDiag");

  /** Call path: the write path — {@code PagePersister.serializePage} on the way to the file. */
  public static final int SER_PATH_WRITE = 0;

  /** Call path: the async snapshot pre-serializer ({@code PageKind.serializeDisposablePage}). */
  public static final int SER_PATH_SNAPSHOT = 1;

  private static final int SER_PATH_COUNT = 2;

  /** Discard: the encoded copy still carried overflow references with unassigned disk keys. */
  public static final int DISCARD_UNRESOLVED_OVERFLOW = 0;

  /** Discard: the encoded copy did not fit the disposable native frame it must be published into. */
  public static final int DISCARD_FRAME_TOO_SMALL = 1;

  /** Discard: serialization threw, so the encode produced nothing publishable. */
  public static final int DISCARD_SERIALIZATION_FAILED = 2;

  private static final int DISCARD_REASON_COUNT = 3;

  /** Carrier state of a refused snapshot copy: every carrier already durable (or none). */
  public static final int REFUSAL_CARRIERS_RESOLVED = 0;

  /** Carrier state of a refused copy: its unresolved carriers are staged side pages. */
  public static final int REFUSAL_CARRIERS_PENDING = 1;

  /** Carrier state of a refused copy: a carrier only the recursive final commit can key. */
  public static final int REFUSAL_CARRIERS_UNRESOLVED = 2;

  private static final int REFUSAL_CARRIER_STATE_COUNT = 3;

  /** Refused snapshot copies by the carrier state observed on the copy, per {@code REFUSAL_CARRIERS_*}. */
  private static final LongAdder[] REFUSAL_CARRIER_STATE = newAdders(REFUSAL_CARRIER_STATE_COUNT);

  /** Snapshot entries that reached the pre-serializer already carrying the refusal mark. */
  private static final LongAdder MARKED_ARRIVALS = new LongAdder();

  /** Full (cache-missing) encodes per index-type id. */
  private static final LongAdder[] FULL_ENCODES_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Wall-clock nanos spent inside those encodes, per index-type id. */
  private static final LongAdder[] FULL_ENCODE_NANOS_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Bytes each of those encodes produced, per index-type id. */
  private static final LongAdder[] FULL_ENCODE_BYTES_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Full encodes per {@code SER_PATH_*}. */
  private static final LongAdder[] FULL_ENCODES_BY_PATH = newAdders(SER_PATH_COUNT);

  /** Wall-clock nanos of those encodes per {@code SER_PATH_*}. */
  private static final LongAdder[] FULL_ENCODE_NANOS_BY_PATH = newAdders(SER_PATH_COUNT);

  /** Serializations served straight from a page's encoded cache — no encode ran. */
  private static final LongAdder[] CACHE_SERVES_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Pages whose bytes the writer actually appended to the file, per index-type id. */
  private static final LongAdder[] PAGE_WRITES_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Encodes whose bytes were thrown away, per {@code DISCARD_*}. */
  private static final LongAdder[] DISCARDED_ENCODES_BY_REASON = newAdders(DISCARD_REASON_COUNT);

  /** Bytes those discarded encodes produced, per {@code DISCARD_*}. */
  private static final LongAdder[] DISCARDED_ENCODE_BYTES_BY_REASON = newAdders(DISCARD_REASON_COUNT);

  /** Snapshot leaves promoted back into the live TIL after a refused encode, per index-type id. */
  private static final LongAdder[] PROMOTED_PAGES_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Snapshot leaves deferred to the next epoch BEFORE any encode ran, per index-type id. */
  private static final LongAdder[] DEFERRED_PAGES_BY_INDEX_TYPE = newAdders(INDEX_TYPE_SLOTS);

  /** Repeat encodes of a page key already encoded inside the CURRENT flush epoch. */
  private static final LongAdder SAME_EPOCH_REPEAT_ENCODES = new LongAdder();

  /** Repeat encodes of a page key last encoded in an EARLIER flush epoch. */
  private static final LongAdder CROSS_EPOCH_REPEAT_ENCODES = new LongAdder();

  /** Encodes the repeat ledger could not track because it hit {@link #REPEAT_LEDGER_CAPACITY}. */
  private static final LongAdder REPEAT_LEDGER_OVERFLOW = new LongAdder();

  /**
   * Hard bound on the repeat ledger. The ledger holds one entry per (index type, page key) pair and
   * exists only under the diagnostic, but a 100M load has enough distinct leaves to matter; past the
   * bound new keys are counted as overflow rather than grown into.
   */
  private static final int REPEAT_LEDGER_CAPACITY = 8_000_000;

  /** (index type, page key) → {@code (epoch << 32) | encodeCount}. Guarded by itself. */
  private static final Long2LongOpenHashMap REPEAT_LEDGER = new Long2LongOpenHashMap(1 << 16);

  /**
   * Flush-epoch counter. Written by the flush driver before each snapshot epoch and read by every
   * encoding thread; a plain int would let a worker attribute a repeat to the previous epoch.
   */
  private static volatile int flushEpoch;

  // ==================================================================================
  // U7 — the string region, split by TAG (i.e. by column).
  //
  // The string region is the single largest thing the trie leaf stores (17.3 GB at 100M), and every
  // byte of it belongs to some column — but the region is written as ONE blob, so no existing
  // counter can say which column owns which bytes. The trie-lane design needs exactly that split:
  // what share belongs to the four fat columns whose resource-wide dictionaries exist or are coming,
  // and what a FOR-packed id lane would cost to replace them with.
  //
  // The region is per LEAF, so a column's real figure is its per-tag bytes summed over every leaf,
  // which is what these lanes accumulate. Three quantities per tag, because they move independently
  // under the lever: the per-tag local dictionary's LENGTH table, its VALUE bytes (the thing a
  // resource-wide dictionary deletes outright), and the id lane (the thing that stays, and shrinks
  // only if it is re-packed per tag).
  //
  // BYTES HERE ARE RAW — the region's pre-codec payload. The region is LZ77'd as a whole, so a
  // per-tag figure AS WRITTEN is not separable; the report prints the region's measured raw→written
  // ratio beside the split so a raw number can be scaled to disk, and any lever priced off these
  // numbers must be scaled that way. Judging a trie lever by staged bytes is precisely the error
  // this campaign has already made once.
  //
  // THE CONTRACT WITH WHOEVER READS THE OUTPUT: the report reconciles itself against the region's
  // own encoded length and prints the difference as `residual`. IF A RUN PRINTS A NON-ZERO
  // RESIDUAL, BELIEVE THE RESIDUAL OVER THE TABLE — it means the encoder's layout moved and this
  // instrument no longer follows it, so the per-tag shares are wrong in some direction the table
  // itself cannot show you. That is not hypothetical: building this, the identity caught two of its
  // own defects before any number was consumed — first rounding the id lane to bytes PER TAG when
  // the encoder rounds the packed lane once (residual −1), then rounding once GLOBALLY when each
  // region rounds its own (residual +3). Both were silent mis-attribution that a plausible-looking
  // share would have hidden. A share that looks reasonable is not evidence; a zero residual is.
  // ==================================================================================

  /** Sub-gate: the per-tag lane adds work per tag per page, so it is opt-in even under the diag. */
  public static final boolean STRING_TAG_DIAG = ENABLED && Boolean.getBoolean("sirix.pageSectionDiag.stringTags");

  /**
   * Tags are nameKeys or pathNodeKeys; both are small dense ints, so a direct array indexes them.
   *
   * <p>
   * ZERO when the sub-gate is off, which is what keeps the table from being ALLOCATED in a
   * production JVM. The recording branch already folds away — both gates are {@code static final} —
   * but a table sized from a constant would still be built at class initialisation, and "off" has to
   * mean no footprint, not merely no per-call work. With no slots every tag routes to the overflow
   * accumulator, which nothing under the gate ever reaches.
   * </p>
   */
  private static final int STRING_TAG_SLOTS = STRING_TAG_DIAG
      ? 1 << 12
      : 0;

  /** One accumulator set per tag kind ({@code 0} = nameKey, {@code 1} = pathNodeKey). */
  private static final int STRING_TAG_KINDS = 2;

  /**
   * Per-tag accumulators, allocated on first sight so a run pays only for the tags it meets. A tag
   * at or beyond {@link #STRING_TAG_SLOTS} lands in the overflow accumulator rather than growing the
   * table — the census stays bounded, and the report says how much it could not attribute.
   */
  private static final AtomicReferenceArray<StringTagStats> STRING_TAG_STATS =
      new AtomicReferenceArray<>(STRING_TAG_KINDS * STRING_TAG_SLOTS);

  /** Bytes belonging to tags the table could not address. */
  private static final StringTagStats STRING_TAG_OVERFLOW = new StringTagStats(-1, -1);

  /** Region bytes that belong to no tag: the encoding kind, the tag directory, the suppressed list. */
  private static final LongAdder STRING_REGION_FRAMING_BYTES = new LongAdder();

  /** Id-lane bytes as the ENCODER rounded them, per region — one rounding each, summed here. */
  private static final LongAdder STRING_REGION_LANE_BYTES = new LongAdder();

  /** Regions the per-tag census covered, so the framing average has a denominator. */
  private static final LongAdder STRING_REGION_CENSUS_PAGES = new LongAdder();

  /** Raw region bytes the census walked; must equal framing + every tag's bytes. */
  private static final LongAdder STRING_REGION_CENSUS_BYTES = new LongAdder();

  /** One column's string-region cost, summed over every leaf that carries it. */
  private static final class StringTagStats {

    private final int tagKind;

    private final int tag;

    /** Leaves whose string region carries this tag. */
    private final LongAdder pages = new LongAdder();

    /** Values stored under this tag, over all those leaves. */
    private final LongAdder values = new LongAdder();

    /** Per-leaf local-dictionary entries, summed — NOT the resource-wide distinct count. */
    private final LongAdder distinct = new LongAdder();

    /** The per-tag length table: 4 B/entry on the bit-packed lane, 1/2/4 on the varint lane. */
    private final LongAdder dictLengthBytes = new LongAdder();

    /** The per-tag value bytes — what a resource-wide dictionary removes outright. */
    private final LongAdder dictValueBytes = new LongAdder();

    /** Id-lane BITS at the page-wide width. Bits, not bytes: the lane is packed across tags. */
    private final LongAdder idLaneBits = new LongAdder();

    /** Id-lane BITS this tag would occupy at its OWN width — the FOR-packed lane's price. */
    private final LongAdder forIdLaneBits = new LongAdder();

    private StringTagStats(final int tagKind, final int tag) {
      this.tagKind = tagKind;
      this.tag = tag;
    }

    /**
     * This tag's bytes expressed in BITS, because the id lane is packed across tags and the region
     * rounds it to bytes exactly ONCE, at the end. Rounding each tag up on its own over-counts by up
     * to seven bits per tag — which is precisely what the census-identity line caught the first time
     * this report ran, so the identity is checked in bits and converted once, like the encoder does.
     */
    private long rawBits() {
      return (dictLengthBytes.sum() + dictValueBytes.sum()) * 8L + idLaneBits.sum();
    }
  }

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

  // ─────────────────────────────────── U7 recording API

  /**
   * Record one tag's share of one leaf's string region.
   *
   * <p>
   * Called once per retained tag per encoded region, from inside the encoder's own per-tag loop, so
   * the dictionary figures are measured write positions rather than a formula that can drift from
   * the layout. The id lane is reported in BITS because it is bit-packed ACROSS tags: a tag's share
   * of the packed array is exact in bits and only becomes approximate if rounded to bytes per tag.
   * </p>
   *
   * @param tagKind {@code 0} for a nameKey tag, {@code 1} for a pathNodeKey tag
   * @param tag the tag value — the column identity within the resource
   * @param values values stored under this tag on this leaf
   * @param distinct this leaf's local dictionary size for this tag
   * @param dictLengthBytes bytes the leaf spent on this tag's length table
   * @param dictValueBytes bytes the leaf spent on this tag's value payload
   * @param idLaneBits bits this tag occupies in the leaf's packed id lane, at the page-wide width
   * @param forIdLaneBits bits it would occupy at its OWN width — the FOR-packed alternative
   */
  public static void recordStringRegionTag(final int tagKind, final int tag, final long values,
      final long distinct, final long dictLengthBytes, final long dictValueBytes, final long idLaneBits,
      final long forIdLaneBits) {
    final StringTagStats stats = stringTagStats(tagKind, tag);
    stats.pages.increment();
    stats.values.add(values);
    stats.distinct.add(distinct);
    stats.dictLengthBytes.add(dictLengthBytes);
    stats.dictValueBytes.add(dictValueBytes);
    stats.idLaneBits.add(idLaneBits);
    stats.forIdLaneBits.add(forIdLaneBits);
  }

  /**
   * Record one encoded string region's tag-independent bytes and its total, closing the census.
   *
   * <p>
   * The report checks framing plus every tag's bytes against {@code regionRawBytes}. That identity is
   * the census's own witness: a layout change that this instrument does not follow shows up as a
   * residual instead of as a quietly wrong share.
   * </p>
   *
   * @param framingBytes encoding kind, tag directory, suppressed-tag list — bytes belonging to no tag
   * @param laneBytes the packed id lane as the encoder rounded it, for THIS region
   * @param regionRawBytes the region's whole pre-codec length
   */
  public static void recordStringRegionCensus(final long framingBytes, final long laneBytes,
      final long regionRawBytes) {
    STRING_REGION_FRAMING_BYTES.add(framingBytes);
    // Taken from the encoder rather than summed from the per-tag bits: the lane is rounded to bytes
    // once PER REGION, so a bit-sum across regions loses up to seven bits each and the census
    // identity drifts by exactly the number of regions. The per-tag bits stay exact and unrounded —
    // they price the FOR lane, they do not reconstruct the file.
    STRING_REGION_LANE_BYTES.add(laneBytes);
    STRING_REGION_CENSUS_BYTES.add(regionRawBytes);
    STRING_REGION_CENSUS_PAGES.increment();
  }

  /** The accumulator for one tag, allocated on first sight; out-of-range tags share the overflow. */
  private static StringTagStats stringTagStats(final int tagKind, final int tag) {
    if (tagKind < 0 || tagKind >= STRING_TAG_KINDS || tag < 0 || tag >= STRING_TAG_SLOTS) {
      return STRING_TAG_OVERFLOW;
    }
    final int index = tagKind * STRING_TAG_SLOTS + tag;
    final StringTagStats resident = STRING_TAG_STATS.getAcquire(index);
    if (resident != null) {
      return resident;
    }
    final StringTagStats minted = new StringTagStats(tagKind, tag);
    final StringTagStats winner = STRING_TAG_STATS.compareAndExchange(index, null, minted);
    return winner == null
        ? minted
        : winner;
  }

  static long stringTagRawBits(final int tagKind, final int tag) {
    return stringTagStats(tagKind, tag).rawBits();
  }

  static long stringTagValues(final int tagKind, final int tag) {
    return stringTagStats(tagKind, tag).values.sum();
  }

  static long stringTagDictValueBytes(final int tagKind, final int tag) {
    return stringTagStats(tagKind, tag).dictValueBytes.sum();
  }

  static long stringTagIdLaneBits(final int tagKind, final int tag) {
    return stringTagStats(tagKind, tag).idLaneBits.sum();
  }

  static long stringTagForIdLaneBits(final int tagKind, final int tag) {
    return stringTagStats(tagKind, tag).forIdLaneBits.sum();
  }

  /** Every tag's bits, for the census identity the report and the witness both check. */
  static long stringTagRawBitsTotal() {
    long total = STRING_TAG_OVERFLOW.rawBits();
    for (int i = 0; i < STRING_TAG_STATS.length(); i++) {
      final StringTagStats stats = STRING_TAG_STATS.getAcquire(i);
      if (stats != null) {
        total += stats.rawBits();
      }
    }
    return total;
  }

  static long stringRegionCensusBytes() {
    return STRING_REGION_CENSUS_BYTES.sum();
  }

  static long stringRegionFramingBytes() {
    return STRING_REGION_FRAMING_BYTES.sum();
  }

  static long stringRegionLaneBytes() {
    return STRING_REGION_LANE_BYTES.sum();
  }

  /** Every tag's DICTIONARY bytes — length tables plus value payload, no lane. */
  static long stringTagDictBytesTotal() {
    long total = STRING_TAG_OVERFLOW.dictLengthBytes.sum() + STRING_TAG_OVERFLOW.dictValueBytes.sum();
    for (int i = 0; i < STRING_TAG_STATS.length(); i++) {
      final StringTagStats stats = STRING_TAG_STATS.getAcquire(i);
      if (stats != null) {
        total += stats.dictLengthBytes.sum() + stats.dictValueBytes.sum();
      }
    }
    return total;
  }

  // ─────────────────────────────────── U6 recording API

  /**
   * Open a new flush epoch. Called once per snapshot flush by the flush driver, so a repeat encode
   * of the same page key can be attributed to redundancy inside one epoch or to a leaf that survives
   * from epoch to epoch.
   */
  public static void noteFlushEpoch() {
    flushEpoch++;
  }

  /**
   * Record one FULL page encode — a serialization that missed the page's encoded cache and therefore
   * ran the whole body staging, template dedup, region build and codec pass.
   *
   * @param indexTypeId {@link IndexType#getID()} of the encoded page
   * @param callPath one of {@link #SER_PATH_WRITE}, {@link #SER_PATH_SNAPSHOT}
   * @param nanos wall-clock nanos the encode took
   * @param encodedBytes bytes the encode produced
   * @param pageKey the page's key, for the repeat ledger
   */
  public static void recordFullEncode(final int indexTypeId, final int callPath, final long nanos,
      final long encodedBytes, final long pageKey) {
    final int slot = indexTypeSlot(indexTypeId);
    FULL_ENCODES_BY_INDEX_TYPE[slot].increment();
    FULL_ENCODE_NANOS_BY_INDEX_TYPE[slot].add(nanos);
    FULL_ENCODE_BYTES_BY_INDEX_TYPE[slot].add(encodedBytes);
    final int pathSlot = callPath >= 0 && callPath < SER_PATH_COUNT
        ? callPath
        : SER_PATH_WRITE;
    FULL_ENCODES_BY_PATH[pathSlot].increment();
    FULL_ENCODE_NANOS_BY_PATH[pathSlot].add(nanos);
    noteRepeatEncode(slot, pageKey);
  }

  /** Ledger one encode of {@code (slot, pageKey)} and classify it as first / same-epoch / cross-epoch. */
  private static void noteRepeatEncode(final int slot, final long pageKey) {
    final int epoch = flushEpoch;
    final long ledgerKey = pageKey * INDEX_TYPE_SLOTS + slot;
    synchronized (REPEAT_LEDGER) {
      final long previous = REPEAT_LEDGER.get(ledgerKey);
      if (previous == 0L) {
        if (REPEAT_LEDGER.size() >= REPEAT_LEDGER_CAPACITY) {
          REPEAT_LEDGER_OVERFLOW.increment();
          return;
        }
        REPEAT_LEDGER.put(ledgerKey, packLedgerEntry(epoch, 1));
        return;
      }
      if ((int) (previous >>> 32) == epoch) {
        SAME_EPOCH_REPEAT_ENCODES.increment();
      } else {
        CROSS_EPOCH_REPEAT_ENCODES.increment();
      }
      REPEAT_LEDGER.put(ledgerKey, packLedgerEntry(epoch, (int) previous + 1));
    }
  }

  /** {@code count} is always {@code >= 1}, so a packed entry is never the map's {@code 0} default. */
  private static long packLedgerEntry(final int epoch, final int count) {
    return ((long) epoch << 32) | (count & 0xFFFF_FFFFL);
  }

  /**
   * Record one serialization served straight from the page's encoded cache: bytes were copied, no
   * encode ran.
   *
   * @param indexTypeId {@link IndexType#getID()} of the page
   */
  public static void recordCacheServe(final int indexTypeId) {
    CACHE_SERVES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].increment();
  }

  /**
   * Record one page whose bytes the writer appended to the file. The denominator every encode count
   * is measured against.
   *
   * @param indexTypeId {@link IndexType#getID()} of the written page
   */
  public static void recordPageWrite(final int indexTypeId) {
    PAGE_WRITES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].increment();
  }

  /**
   * Record one encode whose bytes were thrown away.
   *
   * @param reason one of {@link #DISCARD_UNRESOLVED_OVERFLOW}, {@link #DISCARD_FRAME_TOO_SMALL},
   *        {@link #DISCARD_SERIALIZATION_FAILED}
   * @param encodedBytes bytes the discarded encode produced ({@code 0} when unknown)
   */
  public static void recordDiscardedEncode(final int reason, final long encodedBytes) {
    if (reason < 0 || reason >= DISCARD_REASON_COUNT) {
      return;
    }
    DISCARDED_ENCODES_BY_REASON[reason].increment();
    DISCARDED_ENCODE_BYTES_BY_REASON[reason].add(encodedBytes);
  }

  /**
   * Record one snapshot leaf promoted back into the live TIL after its encode was refused.
   *
   * @param indexTypeId {@link IndexType#getID()} of the promoted page
   */
  public static void recordSnapshotPromotion(final int indexTypeId) {
    PROMOTED_PAGES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].increment();
  }

  /**
   * Record one snapshot leaf deferred to the next epoch before any encode ran — the cheap refusal,
   * counted separately because it wastes no codec work.
   *
   * @param indexTypeId {@link IndexType#getID()} of the deferred page
   */
  public static void recordSnapshotDeferral(final int indexTypeId) {
    DEFERRED_PAGES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].increment();
  }

  static long fullEncodesForIndexType(final int indexTypeId) {
    return FULL_ENCODES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].sum();
  }

  static long cacheServesForIndexType(final int indexTypeId) {
    return CACHE_SERVES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].sum();
  }

  static long pageWritesForIndexType(final int indexTypeId) {
    return PAGE_WRITES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].sum();
  }

  static long promotedPagesForIndexType(final int indexTypeId) {
    return PROMOTED_PAGES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].sum();
  }

  static long deferredPagesForIndexType(final int indexTypeId) {
    return DEFERRED_PAGES_BY_INDEX_TYPE[indexTypeSlot(indexTypeId)].sum();
  }

  static long discardedEncodesForReason(final int reason) {
    return reason >= 0 && reason < DISCARD_REASON_COUNT
        ? DISCARDED_ENCODES_BY_REASON[reason].sum()
        : 0L;
  }

  static long fullEncodesForPath(final int callPath) {
    return callPath >= 0 && callPath < SER_PATH_COUNT
        ? FULL_ENCODES_BY_PATH[callPath].sum()
        : 0L;
  }

  /**
   * Record the carrier state observed on a refused snapshot copy, which is what decides whether the
   * refusal is permanent for this content.
   *
   * @param state one of {@link #REFUSAL_CARRIERS_RESOLVED}, {@link #REFUSAL_CARRIERS_PENDING},
   *        {@link #REFUSAL_CARRIERS_UNRESOLVED}
   */
  public static void recordRefusalCarrierState(final int state) {
    if (state >= 0 && state < REFUSAL_CARRIER_STATE_COUNT) {
      REFUSAL_CARRIER_STATE[state].increment();
    }
  }

  /** Record one snapshot entry that arrived at the pre-serializer already marked as unflushable. */
  public static void recordMarkedArrival() {
    MARKED_ARRIVALS.increment();
  }

  static long refusalCarrierState(final int state) {
    return state >= 0 && state < REFUSAL_CARRIER_STATE_COUNT
        ? REFUSAL_CARRIER_STATE[state].sum()
        : 0L;
  }

  static long markedArrivals() {
    return MARKED_ARRIVALS.sum();
  }

  static long sameEpochRepeatEncodes() {
    return SAME_EPOCH_REPEAT_ENCODES.sum();
  }

  static long crossEpochRepeatEncodes() {
    return CROSS_EPOCH_REPEAT_ENCODES.sum();
  }

  /**
   * Print the string region split by tag — which column owns which bytes, and what a FOR-packed id
   * lane would cost. Raw (pre-codec) bytes throughout, with the measured raw→written ratio printed
   * beside them so any lever priced from this table is scaled to disk rather than to staging.
   *
   * <p>
   * The first line reconciles framing + dictionaries + id lane against the region's own encoded
   * length. <b>A non-zero {@code residual} beats every number below it:</b> it says the encoder's
   * layout moved and this attribution no longer follows it, so the per-tag shares are wrong in a
   * direction the table cannot show. Fix the instrument before quoting the table.
   * </p>
   */
  private static void dumpStringRegionByTag() {
    final long censusBytes = STRING_REGION_CENSUS_BYTES.sum();
    if (censusBytes == 0L) {
      return;
    }

    final ArrayList<StringTagStats> present = new ArrayList<>();
    for (int i = 0; i < STRING_TAG_STATS.length(); i++) {
      final StringTagStats stats = STRING_TAG_STATS.getAcquire(i);
      if (stats != null) {
        present.add(stats);
      }
    }
    if (STRING_TAG_OVERFLOW.pages.sum() != 0L) {
      present.add(STRING_TAG_OVERFLOW);
    }
    present.sort((left, right) -> Long.compare(right.rawBits(), left.rawBits()));

    long taggedBits = 0L;
    long idLaneBits = 0L;
    long forIdLaneBits = 0L;
    for (final StringTagStats stats : present) {
      taggedBits += stats.rawBits();
      idLaneBits += stats.idLaneBits.sum();
      forIdLaneBits += stats.forIdLaneBits.sum();
    }
    final long framing = STRING_REGION_FRAMING_BYTES.sum();
    final long laneBytes = STRING_REGION_LANE_BYTES.sum();
    final long dictBytes = stringTagDictBytesTotal();
    final long taggedBytes = dictBytes + laneBytes;
    final long residual = censusBytes - framing - taggedBytes;

    // The region is one LZ77 blob, so per-tag WRITTEN bytes do not exist. This ratio is how a raw
    // number from the table below is turned into a number about the file.
    final long rawRegion = REGION_BYTES_BY_KIND[RegionTable.KIND_STRING].sum();
    final long writtenRegion = REGION_WRITTEN_BYTES_BY_KIND[RegionTable.KIND_STRING].sum();
    final double writtenRatio = rawRegion == 0L
        ? 0.0d
        : (double) writtenRegion / rawRegion;

    System.out.printf(
        "[PageSectionDiag] stringRegion by TAG over %,d regions: raw=%,d B  framing=%,d B (%.1f%%)"
            + "  dictionaries=%,d B (%.1f%%)  idLane=%,d B (%.1f%%)  residual=%,d B%s%n",
        STRING_REGION_CENSUS_PAGES.sum(), censusBytes, framing, pct(framing, censusBytes), dictBytes,
        pct(dictBytes, censusBytes), laneBytes, pct(laneBytes, censusBytes), residual, residual == 0L
            ? " (census EXACT)"
            : "  <<< CENSUS INCOMPLETE — the encoder layout moved and this table under-attributes");
    System.out.printf(
        "[PageSectionDiag]   region as WRITTEN: raw=%,d B -> written=%,d B (ratio %.3f)."
            + " Every raw figure below scales by that ratio to reach the file.%n", rawRegion, writtenRegion,
        writtenRatio);
    System.out.printf(
        "[PageSectionDiag]   id lane, per-tag basis (unrounded bits; differs from the census lane figure"
            + " by the per-region rounding): pageWideWidth=%,d B  perTagWidth(FOR)=%,d B  saving=%,d B (%.1f%%)%n",
        (idLaneBits + 7L) / 8L, (forIdLaneBits + 7L) / 8L, (idLaneBits - forIdLaneBits) / 8L,
        pct(idLaneBits - forIdLaneBits, idLaneBits));

    for (final StringTagStats stats : present) {
      final long raw = stats.rawBits() / 8L;
      if (stats.rawBits() == 0L) {
        continue;
      }
      final long values = stats.values.sum();
      // Floor-divided for display; only the reconciliation above needs bit exactness.
      final long tagIdLane = stats.idLaneBits.sum() / 8L;
      final long tagForLane = stats.forIdLaneBits.sum() / 8L;
      System.out.printf(
          "[PageSectionDiag]   tag %-5s %-8d raw=%,15d B (%5.1f%%)  leaves=%,9d  values=%,12d"
              + "  localDictEntries=%,11d  dictValues=%,14d B  dictLengths=%,11d B  idLane=%,11d B"
              + "  idLaneFOR=%,11d B  B/value=%.2f%n",
          stats.tagKind == 0
              ? "name"
              : (stats.tagKind == 1
                  ? "path"
                  : "over"), stats.tag, raw, pct(raw, censusBytes), stats.pages.sum(), values,
          stats.distinct.sum(), stats.dictValueBytes.sum(), stats.dictLengthBytes.sum(), tagIdLane, tagForLane,
          perRecord(raw, values));
    }
  }

  /**
   * Print the serialization-work ledger: encodes against writes per index type, the call path that
   * asked for each encode, what the refusals cost, and the repeat histogram.
   */
  private static void dumpSerializationWork() {
    long encodesTotal = 0;
    long writesTotal = 0;
    long cacheServesTotal = 0;
    long encodeNanosTotal = 0;
    long encodeBytesTotal = 0;
    for (int slot = 0; slot < INDEX_TYPE_SLOTS; slot++) {
      encodesTotal += FULL_ENCODES_BY_INDEX_TYPE[slot].sum();
      writesTotal += PAGE_WRITES_BY_INDEX_TYPE[slot].sum();
      cacheServesTotal += CACHE_SERVES_BY_INDEX_TYPE[slot].sum();
      encodeNanosTotal += FULL_ENCODE_NANOS_BY_INDEX_TYPE[slot].sum();
      encodeBytesTotal += FULL_ENCODE_BYTES_BY_INDEX_TYPE[slot].sum();
    }
    if (encodesTotal == 0 && writesTotal == 0) {
      return;
    }

    System.out.printf(
        "[PageSectionDiag] serialization work: fullEncodes=%,d  cacheServes=%,d  pagesWritten=%,d"
            + "  excessEncodes=%,d (%.1f%% of encodes)  encodeWall=%.2f s over %,d B produced%n",
        encodesTotal, cacheServesTotal, writesTotal, encodesTotal - writesTotal,
        pct(encodesTotal - writesTotal, encodesTotal), encodeNanosTotal / 1e9, encodeBytesTotal);

    System.out.printf("[PageSectionDiag]   by call path: write=%,d (%.2f s)  snapshotPreSerialize=%,d (%.2f s)%n",
        FULL_ENCODES_BY_PATH[SER_PATH_WRITE].sum(), FULL_ENCODE_NANOS_BY_PATH[SER_PATH_WRITE].sum() / 1e9,
        FULL_ENCODES_BY_PATH[SER_PATH_SNAPSHOT].sum(), FULL_ENCODE_NANOS_BY_PATH[SER_PATH_SNAPSHOT].sum() / 1e9);

    for (int slot = 0; slot < INDEX_TYPE_SLOTS; slot++) {
      final long encodes = FULL_ENCODES_BY_INDEX_TYPE[slot].sum();
      final long writes = PAGE_WRITES_BY_INDEX_TYPE[slot].sum();
      final long serves = CACHE_SERVES_BY_INDEX_TYPE[slot].sum();
      final long promoted = PROMOTED_PAGES_BY_INDEX_TYPE[slot].sum();
      final long deferred = DEFERRED_PAGES_BY_INDEX_TYPE[slot].sum();
      if (encodes == 0 && writes == 0 && serves == 0) {
        continue;
      }
      System.out.printf(
          "[PageSectionDiag]   indexType %-18s encodes=%,d  writes=%,d  encodesPerWrite=%.2f"
              + "  cacheServes=%,d  wall=%.2f s  bytes=%,d  snapshotPromoted=%,d  snapshotDeferred=%,d%n",
          indexTypeName(slot), encodes, writes, writes == 0
              ? 0.0d
              : (double) encodes / writes, serves, FULL_ENCODE_NANOS_BY_INDEX_TYPE[slot].sum() / 1e9,
          FULL_ENCODE_BYTES_BY_INDEX_TYPE[slot].sum(), promoted, deferred);
    }

    long discardedEncodes = 0;
    long discardedBytes = 0;
    for (int reason = 0; reason < DISCARD_REASON_COUNT; reason++) {
      discardedEncodes += DISCARDED_ENCODES_BY_REASON[reason].sum();
      discardedBytes += DISCARDED_ENCODE_BYTES_BY_REASON[reason].sum();
    }
    System.out.printf(
        "[PageSectionDiag]   discarded encodes=%,d (%,d B produced then dropped)"
            + "  [unresolvedOverflow=%,d  frameTooSmall=%,d  serializationFailed=%,d]%n",
        discardedEncodes, discardedBytes, DISCARDED_ENCODES_BY_REASON[DISCARD_UNRESOLVED_OVERFLOW].sum(),
        DISCARDED_ENCODES_BY_REASON[DISCARD_FRAME_TOO_SMALL].sum(),
        DISCARDED_ENCODES_BY_REASON[DISCARD_SERIALIZATION_FAILED].sum());

    System.out.printf(
        "[PageSectionDiag]   refusal carrier state: resolved=%,d  pendingSideWrites=%,d  unresolved=%,d"
            + "  markedArrivals=%,d%n",
        REFUSAL_CARRIER_STATE[REFUSAL_CARRIERS_RESOLVED].sum(),
        REFUSAL_CARRIER_STATE[REFUSAL_CARRIERS_PENDING].sum(),
        REFUSAL_CARRIER_STATE[REFUSAL_CARRIERS_UNRESOLVED].sum(), MARKED_ARRIVALS.sum());

    // The repeat ledger: distinct page identities against the encodes spent on them.
    long distinctKeys = 0;
    long once = 0;
    long twice = 0;
    long threeToFive = 0;
    long sixPlus = 0;
    int maxCount = 0;
    synchronized (REPEAT_LEDGER) {
      distinctKeys = REPEAT_LEDGER.size();
      for (final long packed : REPEAT_LEDGER.values()) {
        final int count = (int) packed;
        if (count > maxCount) {
          maxCount = count;
        }
        if (count == 1) {
          once++;
        } else if (count == 2) {
          twice++;
        } else if (count <= 5) {
          threeToFive++;
        } else {
          sixPlus++;
        }
      }
    }
    System.out.printf(
        "[PageSectionDiag]   repeat ledger: distinctPageKeys=%,d  encodedOnce=%,d  twice=%,d  3-5x=%,d  6+x=%,d"
            + "  max=%,d  repeats[sameEpoch=%,d crossEpoch=%,d]  ledgerOverflow=%,d%n",
        distinctKeys, once, twice, threeToFive, sixPlus, maxCount, SAME_EPOCH_REPEAT_ENCODES.sum(),
        CROSS_EPOCH_REPEAT_ENCODES.sum(), REPEAT_LEDGER_OVERFLOW.sum());
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

  /**
   * Record activation of the left-sibling-key column extractor on a single page along with the number
   * of pre-compression bytes it displaced from the heap body.
   */
  public static void recordLeftSibKeyColumn(final long bytesSaved) {
    LEFT_SIB_KEY_COLUMN_PAGES.increment();
    LEFT_SIB_KEY_COLUMN_BYTES_SAVED.add(bytesSaved);
  }

  /**
   * Record a left-sibling-key column candidate (a page with at least one slot whose kind has a
   * left-sibling-key field) regardless of whether the column ultimately paid off.
   */
  public static void recordLeftSibKeyColumnCandidate(final long rawStrippedBytes, final long encodedColumnBytes) {
    LEFT_SIB_KEY_COLUMN_CANDIDATE_PAGES.increment();
    LEFT_SIB_KEY_COLUMN_RAW_BYTES.add(rawStrippedBytes);
    LEFT_SIB_KEY_COLUMN_ENCODED_BYTES.add(encodedColumnBytes);
  }

  /**
   * Record revision elision on a single page: the bytes its records' revision varints no longer take,
   * and what the bitmap that names them costs.
   *
   * @param bytesSaved pre-compression heap bytes the elision removed
   * @param metadataBytes bytes the section naming the elided slots takes
   */
  public static void recordRevisionElision(final long bytesSaved, final long metadataBytes) {
    REVISION_ELISION_PAGES.increment();
    REVISION_ELISION_BYTES_SAVED.add(bytesSaved);
    REVISION_ELISION_META_BYTES.add(metadataBytes);
  }

  /** Bytes the revision-elision sections staged, for the counter witness. */
  static long stagedRevisionElisionMetaBytes() {
    return REVISION_ELISION_META_BYTES.sum();
  }

  /** Heap bytes revision elision removed, for the counter witness. */
  static long revisionElisionBytesSaved() {
    return REVISION_ELISION_BYTES_SAVED.sum();
  }

  /** Heap bytes the left-sibling column removed, for the counter witness. */
  static long leftSibKeyColumnBytesSaved() {
    return LEFT_SIB_KEY_COLUMN_BYTES_SAVED.sum();
  }

  /** Heap bytes the right-sibling column removed, for the counter witness. */
  static long rightSibKeyColumnBytesSaved() {
    return RIGHT_SIB_KEY_COLUMN_BYTES_SAVED.sum();
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
      // Outside dumpStatsLocked's `pages == 0` guard on purpose: the per-tag census has its own
      // denominator, and a run that encodes regions without completing a page serialization must
      // still report what it measured rather than silently printing nothing.
      dumpStringRegionByTag();
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
            + " + templatePool+slotIds %,d (%.1f%%) + heap %,d (%.1f%%); BODY wire/staging=%.3f  <<< this ratio is the encoded BODY only. It does NOT apply to any region: each region prints its own measured raw->written ratio below.%n",
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
    final long lsPages = LEFT_SIB_KEY_COLUMN_PAGES.sum();
    final long lsBytes = LEFT_SIB_KEY_COLUMN_BYTES_SAVED.sum();
    System.out.printf("[PageSectionDiag] encoders: leftSibKeyColumn pages=%,d (%.1f%%)  bytesSaved=%,d (%.1f MB)%n",
        lsPages, pct(lsPages, pages), lsBytes, lsBytes / (1024.0 * 1024.0));
    final long lsCandidates = LEFT_SIB_KEY_COLUMN_CANDIDATE_PAGES.sum();
    final long lsRaw = LEFT_SIB_KEY_COLUMN_RAW_BYTES.sum();
    final long lsEncoded = LEFT_SIB_KEY_COLUMN_ENCODED_BYTES.sum();
    System.out.printf(
        "[PageSectionDiag] leftSibKeyColumn candidates=%,d rawBytes=%,d (%.1f MB)"
            + "  encodedBytes=%,d (%.1f MB)  avgRaw/page=%.1f  avgEncoded/page=%.1f%n",
        lsCandidates, lsRaw, lsRaw / (1024.0 * 1024.0), lsEncoded, lsEncoded / (1024.0 * 1024.0), lsCandidates == 0
            ? 0
            : (double) lsRaw / lsCandidates,
        lsCandidates == 0
            ? 0
            : (double) lsEncoded / lsCandidates);
    final long revPages = REVISION_ELISION_PAGES.sum();
    final long revBytes = REVISION_ELISION_BYTES_SAVED.sum();
    final long revMeta = REVISION_ELISION_META_BYTES.sum();
    System.out.printf(
        "[PageSectionDiag] encoders: revisionElision pages=%,d (%.1f%%)  bytesSaved=%,d (%.1f MB)"
            + "  metadata=%,d (%.2f B/record)%n",
        revPages, pct(revPages, pages), revBytes, revBytes / (1024.0 * 1024.0), revMeta, RECORDS.sum() == 0
            ? 0.0
            : (double) revMeta / RECORDS.sum());
    final long postCodecPages = POST_CODEC_PAGES.sum();
    if (postCodecPages > 0) {
      final long recordsSeen = RECORDS.sum();
      final long sectionSum = POST_CODEC_SECTION_SUM.sum();
      final long actualBody = POST_CODEC_ACTUAL_BODY.sum();
      System.out.printf(
          "[PageSectionDiag] post-codec attribution over %,d pages — each staged section compressed ON ITS OWN with"
              + " the page's elected codec, so these are upper bounds; the whole body beats their sum by the"
              + " cross-section redundancy the codec also sees%n",
          postCodecPages);
      for (int section = 0; section < SECTION_COUNT; section++) {
        final long raw = SECTION_RAW_BYTES[section].sum();
        final long encoded = SECTION_ENCODED_BYTES[section].sum();
        if (raw == 0 && encoded == 0) {
          continue;
        }
        System.out.printf(
            "[PageSectionDiag]   %-18s raw=%,d (%.2f B/record)  alone=%,d (%.2f B/record)  ratio=%.3f%s%n",
            SECTION_NAMES[section], raw, perRecord(raw, recordsSeen), encoded, perRecord(encoded, recordsSeen),
            raw == 0
                ? 0.0
                : (double) encoded / raw, section >= SECTION_HEAP_FUSED
                    ? "  (part of heap)"
                    : "");
      }
      System.out.printf(
          "[PageSectionDiag]   sections alone sum=%,d (%.2f B/record)  actual body=%,d (%.2f B/record)"
              + "  cross-section gain=%,d (%.1f%%)%n",
          sectionSum, perRecord(sectionSum, recordsSeen), actualBody, perRecord(actualBody, recordsSeen),
          sectionSum - actualBody, sectionSum == 0
              ? 0.0
              : 100.0 * (sectionSum - actualBody) / sectionSum);
    }
    final long dirEntries = COMPACT_DIR_ENTRIES.sum();
    if (dirEntries > 0) {
      final long predictable = COMPACT_DIR_PREDICTABLE_ENTRIES.sum();
      System.out.printf(
          "[PageSectionDiag] compact directory: %,d entries, %,d (%.1f%%) repeat the kind AND length of the previous"
              + " entry of the same template — what a template-implied directory could drop (T1-b)%n",
          dirEntries, predictable, pct(predictable, dirEntries));
    }
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

    // ---- U6: serialization work — encodes against the pages that reached the file ----
    dumpSerializationWork();

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

  /** Page bodies emitted with the wire codec {@code codec}: 0 zero-run, 2 byte-run, 3 LZ77. */
  static long codecPages(final int codec) {
    return switch (codec) {
      case 0 -> CODEC_ZERORUN_PAGES.sum();
      case 2 -> CODEC_BYTERUN_PAGES.sum();
      case 3 -> CODEC_LZ77_PAGES.sum();
      default -> throw new IllegalArgumentException("codec=" + codec);
    };
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
