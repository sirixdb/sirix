/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.page.pax.RegionTable;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;

import static io.sirix.cache.MemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Witness for the {@link PageSectionDiag} counters the storage plan's B0 measurement reads: the
 * staged-heap split per record kind, the payload bytes value elision left inline, the metadata each
 * elision lever costs, the body-path outcome per index type, the string-region suppression an
 * overflow descriptor causes, and the region table's post-envelope bytes.
 *
 * <p>
 * <b>Assert-and-provide.</b> Every one of these counters sits behind
 * {@code -Dsirix.pageSectionDiag}, a static-final gate read at class initialisation so the branches
 * fold away in production. A test therefore cannot switch it on for itself: with the gate off every
 * counter reads zero, and a zero from a disabled instrument is indistinguishable from a zero from a
 * healthy one. So the suite ASSERTS the gate is on and {@code bundles/sirix-core/build.gradle}
 * provides it — the same contract the HOT merge-diagnostic suites use.
 *
 * <p>
 * The counters are process-cumulative, so every assertion is on a BEFORE/AFTER delta around one
 * serialization, never on an absolute value.
 */
@DisplayName("PageSectionDiag counters")
final class PageSectionDiagCountersTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  /** Records of each fused primitive kind on the mixed fixture page. */
  private static final int RECORDS_PER_KIND = 8;

  private Arena arena;

  @BeforeAll
  static void requireTheInstrumentIsOn() {
    assertTrue(PageKind.sectionDiagEnabled(),
        "the section diagnostic is off: run with -Dsirix.pageSectionDiag=true. Every counter this "
            + "suite asserts on reads zero when the gate is off, so a passing run would prove nothing.");
  }

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
  }

  @AfterEach
  void tearDown() {
    if (arena != null) {
      arena.close();
    }
  }

  @Test
  @DisplayName("a mixed fused-primitive page moves the per-kind heap, elision-metadata and index-type counters")
  void mixedFusedPageMovesTheHeapCounters() {
    final Snapshot before = Snapshot.take();

    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config, 0);
    long nodeKey = 0;
    for (int i = 0; i < RECORDS_PER_KIND; i++) {
      // Wide payloads on purpose: value elision activates only when the elided bytes beat the wire
      // cost of naming each slot, and a fixture that clears that bar by a byte would be a coin flip.
      writeNumber(page, nodeKey++, 100 + i, 4_000_000_000_000_000_000L + i);
      writeString(page, nodeKey++, 200 + i, "value-" + i + "-" + "p".repeat(40));
      writeBoolean(page, nodeKey++, 300 + i, (i & 1) == 0);
    }
    final int expectedRecords = 3 * RECORDS_PER_KIND;

    serialize(config, page);
    final Snapshot after = Snapshot.take();
    page.close();

    // --- records and the per-kind heap split ---
    assertEquals(1, after.pages - before.pages, "exactly one page was serialized");
    assertEquals(expectedRecords, after.records - before.records, "every populated slot is counted once");
    assertEquals(expectedRecords, after.heapSlotsAllKinds - before.heapSlotsAllKinds,
        "the per-kind heap fold must account for every record exactly once");
    assertEquals(RECORDS_PER_KIND,
        after.heapSlotsForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID)
            - before.heapSlotsForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID),
        "fused numbers land under their own kind");
    assertEquals(RECORDS_PER_KIND,
        after.heapSlotsForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID)
            - before.heapSlotsForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID),
        "fused strings land under their own kind");
    assertEquals(RECORDS_PER_KIND,
        after.heapSlotsForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID)
            - before.heapSlotsForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID),
        "fused booleans land under their own kind");
    assertTrue(after.heapBytesAllKinds - before.heapBytesAllKinds >= 2L * expectedRecords,
        "every staged record is at least kindId + templateId, so the heap cannot be smaller than 2 B/record");

    // --- the body path and the index-type split ---
    assertEquals(1, after.encodedBodyPages - before.encodedBodyPages,
        "a page of fused records takes the encoded (template-deduped) body path under DOCUMENT");
    assertEquals(0, after.inlineBodyPages - before.inlineBodyPages, "and not the inline path");
    assertEquals(0, after.valueElisionNoCandidatePages - before.valueElisionNoCandidatePages,
        "the page holds fused primitives, so it is never a no-candidate page");

    // --- elision cost against elision effect ---
    final long elidedPages = after.valueElisionPages - before.valueElisionPages;
    final long elisionMeta = after.valueElisionMetaBytes - before.valueElisionMetaBytes;
    assertEquals(1, elidedPages, "value elision activates on a page of elidable fused primitives");
    assertTrue(elisionMeta > 0, "an active value elision always stages its section: got " + elisionMeta + " B");
    assertEquals(0,
        after.inlineValueSlotsForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID)
            - before.inlineValueSlotsForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID),
        "with elision active every fused number's payload leaves the heap, so none counts as inline");

    // --- the region table, as written ---
    assertRegionTableAccounting(before, after, 1);
  }

  /**
   * Re-recorded for per-tag string-region completeness.
   *
   * <p>
   * This page used to lose its string region outright, and this counter is what measured that. It now
   * keeps it: only the oversized field's tag leaves, so the counter reads zero suppressions and one
   * written region. The old reading is still reachable and still asserted — see
   * {@link #theKillSwitchRestoresTheWholePageSuppression()} — which is what makes this pair the
   * before/after the plan's T1-d lever is judged on.
   */
  @Test
  @DisplayName("an oversized fused string no longer suppresses the whole page's string region")
  void overflowDescriptorNoLongerSuppressesTheStringRegion() {
    final Snapshot before = Snapshot.take();
    final KeyValueLeafPage page = buildOverflowDescriptorFixture();
    final Snapshot after = Snapshot.take();
    page.close();

    assertEquals(1, after.regionBuildPages - before.regionBuildPages, "one page reached the string-region decision");
    assertEquals(0, after.stringRegionSuppressedPages - before.stringRegionSuppressedPages,
        "the descriptor costs its own tag, not the page's region");
    assertEquals(1,
        after.regionWrittenCount[RegionTable.KIND_STRING] - before.regionWrittenCount[RegionTable.KIND_STRING],
        "and the region is written, with every other field's strings in it");
    assertEquals(0, after.stringRegionSuppressedValues - before.stringRegionSuppressedValues,
        "no value is stranded by a suppression that did not happen");
    assertEquals(1, after.overflowHistogram[1] - before.overflowHistogram[1],
        "one descriptor on the page still lands in the '1' bucket");
    assertHistogramSumsToRegionBuildPages(after);

    // The four complete fields' payloads left the heap for the region; only the descriptor slot's
    // own payload — a reference, which no column can hold — stays inline.
    assertEquals(1,
        after.inlineValueSlotsForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID)
            - before.inlineValueSlotsForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID),
        "exactly the overflow descriptor keeps its payload inline");
  }

  @Test
  @DisplayName("the kill switch restores the whole-page suppression the counter used to report")
  void theKillSwitchRestoresTheWholePageSuppression() {
    final boolean before_ = PageKind.STRING_REGION_PER_TAG_COMPLETENESS;
    PageKind.STRING_REGION_PER_TAG_COMPLETENESS = false;
    try {
      final Snapshot before = Snapshot.take();
      final KeyValueLeafPage page = buildOverflowDescriptorFixture();
      final Snapshot after = Snapshot.take();
      page.close();

      assertEquals(1, after.regionBuildPages - before.regionBuildPages, "one page reached the string-region decision");
      assertEquals(1, after.stringRegionSuppressedPages - before.stringRegionSuppressedPages,
          "with the old rule the descriptor suppressed this page's string region");
      assertEquals(4, after.stringRegionSuppressedValues - before.stringRegionSuppressedValues,
          "the four inline strings are the ones stranded by the suppression");
      assertTrue(after.stringRegionSuppressedBytes - before.stringRegionSuppressedBytes > 0,
          "their stored bytes stay in the record heap and must be counted");

      // Nothing was elided out of those strings, because there is no region to put them back from.
      assertTrue(
          after.inlineValueBytesForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID)
              - before.inlineValueBytesForKind(KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID) > 0,
          "a page that lost its string region keeps every string's payload inline in the heap");
    } finally {
      PageKind.STRING_REGION_PER_TAG_COMPLETENESS = before_;
    }
  }

  /**
   * Four ordinary string fields plus one value past the fused record cap. The oversized value does
   * not fit the heap, so it is parked as a record snapshot and only becomes an inline overflow
   * descriptor + same-key {@link OverflowPage} during serialization — which is why the fixture can
   * only be checked afterwards. The returned page is still open; the caller closes it.
   */
  private KeyValueLeafPage buildOverflowDescriptorFixture() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config, 0);
    for (int i = 0; i < 4; i++) {
      // Wide enough that value elision clears its own wire cost; a fixture that misses that bar by
      // a byte would make the elision assertions a coin flip.
      writeString(page, i, 200 + i, "inline-" + i + "-" + "p".repeat(40));
    }
    writeString(page, 4, 204, "x".repeat(PageConstants.MAX_RECORD_SIZE + 4_096));

    serialize(config, page);
    assertTrue(page.isFusedObjectNamedStringOverflowDescriptor(4),
        "the fixture must actually produce an overflow descriptor, otherwise it witnesses nothing");
    assertEquals(0, page.getSideSlotCount(),
        "the descriptor must land in the heap; a side slot would refuse the region build outright");
    return page;
  }

  @Test
  @DisplayName("the post-codec attribution charges every staged section and sums to at least the real body")
  void postCodecAttributionChargesEverySection() {
    final Snapshot before = Snapshot.take();

    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config, 0);
    long nodeKey = 0;
    for (int i = 0; i < RECORDS_PER_KIND; i++) {
      writeNumber(page, nodeKey++, 100 + i, 4_000_000_000_000_000_000L + i);
      writeString(page, nodeKey++, 200 + i, "value-" + i + "-" + "p".repeat(40));
      writeBoolean(page, nodeKey++, 300 + i, (i & 1) == 0);
    }
    final int expectedRecords = 3 * RECORDS_PER_KIND;

    serialize(config, page);
    final Snapshot after = Snapshot.take();
    page.close();

    assertEquals(1, after.postCodecPages - before.postCodecPages, "the attribution ran on exactly one page");

    // Every section the page staged is charged, with a raw size that matches what it holds.
    final long dirRaw = after.postCodecRawFor(PageSectionDiag.SECTION_COMPACT_DIR)
        - before.postCodecRawFor(PageSectionDiag.SECTION_COMPACT_DIR);
    assertEquals((long) expectedRecords * PageLayout.COMPACT_DIR_ENTRY_SIZE, dirRaw,
        "the compact directory's raw size is two bytes per populated slot");
    final long heapRaw =
        after.postCodecRawFor(PageSectionDiag.SECTION_HEAP) - before.postCodecRawFor(PageSectionDiag.SECTION_HEAP);
    assertTrue(heapRaw > 0, "the heap must be charged");
    final long fusedRaw = after.postCodecRawFor(PageSectionDiag.SECTION_HEAP_FUSED)
        - before.postCodecRawFor(PageSectionDiag.SECTION_HEAP_FUSED);
    assertEquals(heapRaw, fusedRaw, "this fixture holds only fused records, so the whole heap is that class");
    assertEquals(0, after.postCodecRawFor(PageSectionDiag.SECTION_HEAP_STRUCTURAL)
        - before.postCodecRawFor(PageSectionDiag.SECTION_HEAP_STRUCTURAL), "and none of it is structural");

    // The load-bearing property: a section compressed ALONE cannot beat the same bytes compressed as
    // part of the whole body, because the codec also sees repetition across sections. If the sum ever
    // came out under the real body the attribution would be reporting a saving that does not exist.
    final long sectionSum = after.postCodecSectionSum - before.postCodecSectionSum;
    final long actualBody = after.postCodecActualBody - before.postCodecActualBody;
    assertTrue(actualBody > 0, "the body must have been written");
    assertTrue(sectionSum >= actualBody, "the sections compressed on their own must sum to at least the real body — "
        + sectionSum + " vs " + actualBody);
    // And the sum is exactly the whole-section lanes, with the per-kind heap lanes left out — they are
    // PARTS of the heap lane, and counting a record's bytes twice would make the attribution lie.
    long wholeSectionEncoded = 0;
    for (int section = 0; section <= PageSectionDiag.SECTION_HEAP; section++) {
      wholeSectionEncoded += after.postCodecEncodedFor(section) - before.postCodecEncodedFor(section);
    }
    assertEquals(wholeSectionEncoded, sectionSum, "the reported sum must be the whole-section lanes and nothing else");
    final long fusedEncoded = after.postCodecEncodedFor(PageSectionDiag.SECTION_HEAP_FUSED)
        - before.postCodecEncodedFor(PageSectionDiag.SECTION_HEAP_FUSED);
    assertTrue(fusedEncoded > 0, "the fused-record lane must be charged, and separately from the sum");

    // The compact directory's predictability, the T1-b question: every entry after the first of each
    // template repeats that template's kind, and this fixture's records are uniform per kind, so most
    // entries are predictable.
    final long dirEntries = after.compactDirEntries - before.compactDirEntries;
    final long predictable = after.compactDirPredictableEntries - before.compactDirPredictableEntries;
    assertEquals(expectedRecords, dirEntries, "every populated slot is one directory entry");
    assertTrue(predictable > 0 && predictable < dirEntries,
        "some but not all entries repeat their template's previous kind and length — " + predictable + " of "
            + dirEntries);
  }

  @Test
  @DisplayName("raw slab slots fall to the inline body path and are counted with their reason")
  void inlineBodyPathIsCountedWithItsReason() {
    final Snapshot before = Snapshot.take();

    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config, 0);
    // Slot bytes without an offset table abort template dedup, which is the same fallback a page
    // with more than 255 distinct templates takes — and on that path none of the elisions run.
    page.setSlot(new byte[] {1, 2, 3}, 1);
    page.setSlot(new byte[] {4, 5, 6}, 10);

    serialize(config, page);
    final Snapshot after = Snapshot.take();
    page.close();

    assertEquals(1, after.inlineBodyPages - before.inlineBodyPages, "the page fell to the inline body path");
    assertEquals(1, after.inlineDedupAborted - before.inlineDedupAborted,
        "and the reason recorded is the aborted template dedup");
    assertEquals(0, after.encodedBodyPages - before.encodedBodyPages, "so it is not an encoded-path page");
    assertEquals(0, after.valueElisionMetaBytes - before.valueElisionMetaBytes,
        "the inline path stages no elision metadata at all");
  }

  @Test
  @DisplayName("an empty page is counted as an inline-path page with the empty reason")
  void emptyPageIsCountedWithItsOwnReason() {
    final Snapshot before = Snapshot.take();

    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config, 0);
    serialize(config, page);
    final Snapshot after = Snapshot.take();
    page.close();

    assertEquals(1, after.inlineEmptyPage - before.inlineEmptyPage, "an empty page has its own inline reason");
    assertEquals(0, after.records - before.records, "and contributes no records");
  }

  // ───────────────────────────────────────────────────────────────── shared assertions

  /**
   * The region-table section the page writer measured must equal the four-byte region count plus the
   * bytes {@link RegionTable#write} reported per kind. That identity is what proves the per-kind
   * written figures name the same bytes the section total does — a per-kind counter that silently
   * missed a region would still look plausible on its own.
   */
  private static void assertRegionTableAccounting(final Snapshot before, final Snapshot after, final int pages) {
    final long sectionDelta = after.regionTableSectionBytes - before.regionTableSectionBytes;
    final long writtenDelta = after.regionWrittenAllKinds - before.regionWrittenAllKinds;
    assertEquals(sectionDelta, 4L * pages + writtenDelta,
        "the region-table section is the region-count prefix plus every region as written");
    for (int kind = 0; kind < RegionTable.KIND_COUNT; kind++) {
      final long written = after.regionWritten[kind] - before.regionWritten[kind];
      if (written == 0) {
        continue;
      }
      final long raw = after.regionRaw[kind] - before.regionRaw[kind];
      final long regions = after.regionWrittenCount[kind] - before.regionWrittenCount[kind];
      // Framing per region: kind byte + payload-form byte + rawLen int, plus an encodedLen int when
      // the LZ77 form won. A written figure outside those bounds is measuring the wrong span.
      assertTrue(written >= 6L * regions,
          RegionTable.kindName(kind) + ": written=" + written + " is below its own framing");
      assertTrue(written <= raw + 10L * regions,
          RegionTable.kindName(kind) + ": written=" + written + " exceeds raw=" + raw + " plus framing");
    }
  }

  private static void assertHistogramSumsToRegionBuildPages(final Snapshot snapshot) {
    long sum = 0;
    for (final long bucket : snapshot.overflowHistogram) {
      sum += bucket;
    }
    assertEquals(snapshot.regionBuildPages, sum,
        "every page that reached the string-region decision lands in exactly one histogram bucket");
  }

  // ───────────────────────────────────────────────────────────────── fixture helpers

  private static ResourceConfiguration newConfig() {
    return new ResourceConfiguration.Builder("pageSectionDiagCounters").build();
  }

  private KeyValueLeafPage newPage(final ResourceConfiguration config, final long recordPageKey) {
    return new KeyValueLeafPage(recordPageKey, IndexType.DOCUMENT, config, 1, arena.allocate(SIXTYFOUR_KB), null);
  }

  /**
   * Serialize the page and leave it open — an oversized record only becomes an inline overflow
   * descriptor during serialization ({@code addReferences} → {@code processEntries} diverts it to an
   * {@link OverflowPage} and writes the descriptor into the heap), so the fixture can only be checked
   * afterwards. Callers close the page themselves.
   */
  private static void serialize(final ResourceConfiguration config, final KeyValueLeafPage page) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
  }

  private static void writeNumber(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final long value) {
    final ObjectNamedNumberNode node = new ObjectNamedNumberNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, -1L, 0, 0, 0L,
        value, HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slotOf(nodeKey));
  }

  private static void writeString(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final String value) {
    final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, -1L, 0, 0, 0L,
        value.getBytes(StandardCharsets.UTF_8), HASH_FN, (byte[]) null, false, null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slotOf(nodeKey));
  }

  private static void writeBoolean(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final boolean value) {
    final ObjectNamedBooleanNode node = new ObjectNamedBooleanNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, -1L, 0, 0, 0L,
        value, HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slotOf(nodeKey));
  }

  private static int slotOf(final long nodeKey) {
    return (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
  }

  // ───────────────────────────────────────────────────────────────── counter snapshot

  /**
   * One cumulative reading of every counter this suite asserts on. The counters never reset, so
   * assertions are always on the difference between two readings.
   */
  private static final class Snapshot {

    /** Node-kind ids are a byte; the fixture only touches a handful, but the fold covers them all. */
    private static final int NODE_KINDS = 256;

    private static final int DOCUMENT_INDEX_TYPE = IndexType.DOCUMENT.getID();

    private final long pages;
    private final long records;
    private final long regionTableSectionBytes;
    private final long heapSlotsAllKinds;
    private final long heapBytesAllKinds;
    private final long[] heapSlots = new long[NODE_KINDS];
    private final long[] inlineValueBytes = new long[NODE_KINDS];
    private final long[] inlineValueSlots = new long[NODE_KINDS];
    private final long valueElisionMetaBytes;
    private final long encodedBodyPages;
    private final long valueElisionPages;
    private final long valueElisionNoCandidatePages;
    private final long inlineBodyPages;
    private final long inlineEmptyPage;
    private final long inlineDedupAborted;
    private final long regionBuildPages;
    private final long stringRegionSuppressedPages;
    private final long stringRegionSuppressedValues;
    private final long stringRegionSuppressedBytes;
    private final long[] overflowHistogram;
    private final long[] regionRaw = new long[RegionTable.KIND_COUNT];
    private final long[] regionWritten = new long[RegionTable.KIND_COUNT];
    private final long[] regionWrittenCount = new long[RegionTable.KIND_COUNT];
    private final long regionWrittenAllKinds;
    private final long[] postCodecRaw = new long[PageSectionDiag.SECTION_COUNT];
    private final long[] postCodecEncoded = new long[PageSectionDiag.SECTION_COUNT];
    private final long postCodecPages;
    private final long postCodecSectionSum;
    private final long postCodecActualBody;
    private final long compactDirEntries;
    private final long compactDirPredictableEntries;

    private Snapshot() {
      pages = PageSectionDiag.pagesCounted();
      records = PageSectionDiag.recordsCounted();
      regionTableSectionBytes = PageSectionDiag.regionTableSectionBytes();
      long slots = 0;
      long bytes = 0;
      for (int kind = 0; kind < NODE_KINDS; kind++) {
        heapSlots[kind] = PageSectionDiag.heapSlotsForKind(kind);
        inlineValueBytes[kind] = PageSectionDiag.inlineValueBytesForKind(kind);
        inlineValueSlots[kind] = PageSectionDiag.inlineValueSlotsForKind(kind);
        slots += heapSlots[kind];
        bytes += PageSectionDiag.heapOnDiskBytesForKind(kind);
      }
      heapSlotsAllKinds = slots;
      heapBytesAllKinds = bytes;
      valueElisionMetaBytes = PageSectionDiag.stagedValueElisionMetaBytes();
      encodedBodyPages = PageSectionDiag.encodedBodyPagesForIndexType(DOCUMENT_INDEX_TYPE);
      valueElisionPages = PageSectionDiag.valueElisionPagesForIndexType(DOCUMENT_INDEX_TYPE);
      valueElisionNoCandidatePages = PageSectionDiag.valueElisionNoCandidatePagesForIndexType(DOCUMENT_INDEX_TYPE);
      inlineBodyPages = PageSectionDiag.inlineBodyPagesForIndexType(DOCUMENT_INDEX_TYPE);
      inlineEmptyPage = PageSectionDiag.inlineBodyPagesForReason(PageSectionDiag.INLINE_REASON_EMPTY_PAGE);
      inlineDedupAborted =
          PageSectionDiag.inlineBodyPagesForReason(PageSectionDiag.INLINE_REASON_TEMPLATE_DEDUP_ABORTED);
      regionBuildPages = PageSectionDiag.regionBuildPages();
      stringRegionSuppressedPages = PageSectionDiag.stringRegionSuppressedPages();
      stringRegionSuppressedValues = PageSectionDiag.stringRegionSuppressedValues();
      stringRegionSuppressedBytes = PageSectionDiag.stringRegionSuppressedBytes();
      overflowHistogram = new long[PageSectionDiag.overflowDescriptorHistogramBuckets()];
      for (int bucket = 0; bucket < overflowHistogram.length; bucket++) {
        overflowHistogram[bucket] = PageSectionDiag.overflowDescriptorHistogramBucket(bucket);
      }
      long written = 0;
      for (int kind = 0; kind < RegionTable.KIND_COUNT; kind++) {
        regionRaw[kind] = PageSectionDiag.regionRawBytesForKind(kind);
        regionWritten[kind] = PageSectionDiag.regionWrittenBytesForKind(kind);
        regionWrittenCount[kind] = PageSectionDiag.regionWrittenCountForKind(kind);
        written += regionWritten[kind];
      }
      regionWrittenAllKinds = written;
      for (int section = 0; section < PageSectionDiag.SECTION_COUNT; section++) {
        postCodecRaw[section] = PageSectionDiag.postCodecSectionRawBytes(section);
        postCodecEncoded[section] = PageSectionDiag.postCodecSectionEncodedBytes(section);
      }
      postCodecPages = PageSectionDiag.postCodecPages();
      postCodecSectionSum = PageSectionDiag.postCodecSectionSum();
      postCodecActualBody = PageSectionDiag.postCodecActualBody();
      compactDirEntries = PageSectionDiag.compactDirEntries();
      compactDirPredictableEntries = PageSectionDiag.compactDirPredictableEntries();
    }

    long postCodecRawFor(final int section) {
      return postCodecRaw[section];
    }

    long postCodecEncodedFor(final int section) {
      return postCodecEncoded[section];
    }

    static Snapshot take() {
      return new Snapshot();
    }

    long heapSlotsForKind(final int nodeKindId) {
      return heapSlots[nodeKindId];
    }

    long inlineValueBytesForKind(final int nodeKindId) {
      return inlineValueBytes[nodeKindId];
    }

    long inlineValueSlotsForKind(final int nodeKindId) {
      return inlineValueSlots[nodeKindId];
    }
  }
}
