/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageKind;
import io.sirix.page.PageLayout;
import io.sirix.page.RegionsOnlyPage;
import io.sirix.page.chunked.ChunkedPageHarness.ChunkedLayout;
import io.sirix.page.pax.RegionTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.LinkedHashMap;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The chunk-framed record-page body, end to end on the wire.
 *
 * <p>
 * A chunked body carries the same section bytes as the monolith body it replaces, framed apart: one
 * META frame for the page-global metadata, then the heap cut into chunks at entry boundaries, each
 * frame compressed and checksummed on its own. The property that matters is that framing changes
 * nothing a reader can observe — so every test here builds the same logical page twice, writes it
 * both ways, and compares what comes back rather than what went out.
 *
 * <p>
 * These pages take the degenerate (un-deduped) body shape: their records carry an unknown kind id,
 * which is what the template pool refuses on, so the framing is exercised over a META section that
 * is the compact dir alone. The deduped shape, with real records and every elision lever, is
 * covered through the shredder in {@code ChunkedBodyShredRoundTripTest} — hand-built records cannot
 * honestly reach the column and elision encoders.
 */
@DisplayName("Chunk-framed record-page body")
final class ChunkedBodyWireFormatTest {

  private static final long PAGE_KEY = 7L;

  /** No kind has this id, so the offset-table pool disables dedup and the body stays degenerate. */
  private static final byte UNKNOWN_KIND_ID = 100;

  private boolean previouslyEnabled;
  private int previousTarget;

  @BeforeEach
  void setUp() {
    Allocators.getInstance().init(256L * 1024 * 1024);
    previouslyEnabled = ChunkedBodyConfig.setEnabledForTesting(false);
    previousTarget = ChunkedBodyConfig.targetChunkBytes();
    PageKind.resetChunkedBodyStats();
  }

  @AfterEach
  void tearDown() {
    ChunkedBodyConfig.setEnabledForTesting(previouslyEnabled);
    ChunkedBodyConfig.setTargetChunkBytesForTesting(previousTarget);
  }

  @Test
  @DisplayName("a chunked page decodes to the same slotted page as the monolith it replaces")
  void chunkedDecodesToTheMonolithPage() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage monolith = deserialize(config, serialize(config, populatedPage(config), false));
    final KeyValueLeafPage chunked = deserialize(config, serialize(config, populatedPage(config), true));
    try {
      assertTrue(PageKind.chunkedBodiesWritten() >= 1, "the chunked writer never ran");
      assertTrue(PageKind.chunkedBodiesRead() >= 1, "the chunked reader never ran");
      assertSameSlottedPage(monolith, chunked);
    } finally {
      monolith.close();
      chunked.close();
    }
  }

  @Test
  @DisplayName("an empty page frames a zero-chunk body and round-trips")
  void emptyPage() {
    final ResourceConfiguration config = newConfig();
    final MemorySegment wire = serialize(config, emptyPage(config), true);
    final ChunkedLayout layout = parseChunkedLayout(wire);

    assertEquals(0, layout.populatedCount, "an empty page has no entries");
    assertEquals(0, layout.chunkCount, "no entries means no chunks");

    final KeyValueLeafPage read = deserialize(config, wire);
    try {
      assertEquals(PAGE_KEY, read.getPageKey());
      assertEquals(0, PageLayout.getPopulatedCount(read.getSlottedPage()));
    } finally {
      read.close();
    }
  }

  @Test
  @DisplayName("a one-slot page frames a single chunk and round-trips")
  void singleSlotPage() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage original = emptyPage(config);
    final byte[] record = record(37, 0x5A);
    original.setSlot(record, 11);

    final MemorySegment wire = serialize(config, original, true);
    final ChunkedLayout layout = parseChunkedLayout(wire);
    assertEquals(1, layout.populatedCount);
    assertEquals(1, layout.chunkCount, "one record fills one chunk");

    final KeyValueLeafPage read = deserialize(config, wire);
    try {
      assertArrayEquals(record, read.getSlotAsByteArray(11));
    } finally {
      original.close();
      read.close();
    }
  }

  @Test
  @DisplayName("a small chunk target cuts the heap into many chunks, and they still decode as one")
  void smallTargetProducesManyChunks() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage monolith = deserialize(config, serialize(config, populatedPage(config), false));
    ChunkedBodyConfig.setTargetChunkBytesForTesting(64);
    final MemorySegment wire = serialize(config, populatedPage(config), true);
    final ChunkedLayout layout = parseChunkedLayout(wire);
    final KeyValueLeafPage chunked = deserialize(config, wire);
    try {
      assertTrue(layout.chunkCount > 4,
          "a 64-byte target should cut this page into many chunks, got " + layout.chunkCount);
      assertSameSlottedPage(monolith, chunked);
    } finally {
      monolith.close();
      chunked.close();
    }
  }

  /**
   * The chunk count is one byte wide, so a page whose records are all smaller than the target must
   * not be able to demand more rows than the table has: the planner doubles the target instead.
   */
  @Test
  @DisplayName("more entries than the chunk table can hold grow the chunks instead of overflowing")
  void chunkCountIsCappedByGrowingTheTarget() {
    final ResourceConfiguration config = newConfig();
    ChunkedBodyConfig.setTargetChunkBytesForTesting(64);
    final KeyValueLeafPage original = emptyPage(config);
    for (int slot = 0; slot < 400; slot++) {
      original.setSlot(record(70, slot), slot);
    }
    final MemorySegment wire = serialize(config, original, true);
    final ChunkedLayout layout = parseChunkedLayout(wire);
    assertEquals(400, layout.populatedCount);
    assertTrue(layout.chunkCount <= ChunkedBodyConfig.MAX_CHUNKS,
        "the planner emitted " + layout.chunkCount + " chunks, above the table's cap");
    assertTrue(layout.chunkCount > 1, "400 records at a 64-byte target should still be several chunks");

    final KeyValueLeafPage read = deserialize(config, wire);
    try {
      for (int slot = 0; slot < 400; slot++) {
        assertArrayEquals(record(70, slot), read.getSlotAsByteArray(slot), "slot " + slot);
      }
    } finally {
      original.close();
      read.close();
    }
  }

  /**
   * Serializing the same content twice must produce the same bytes: the commit path serializes a page
   * a second time whenever its overflow references were still unresolved on the first pass, and a
   * body that framed differently the second time would break the compressed-segment cache's
   * assumption that both passes agree.
   */
  @Test
  @DisplayName("the same content frames to the same bytes twice")
  void framingIsDeterministic() {
    final ResourceConfiguration config = newConfig();
    final MemorySegment first = serialize(config, populatedPage(config), true);
    final MemorySegment second = serialize(config, populatedPage(config), true);
    assertArrayEquals(toArray(first), toArray(second), "two passes over the same content diverged");
  }

  /**
   * All four entry points into a page's prefix have to agree, because they are used interchangeably:
   * the full deserializer, the column-only deserializer, the bounded-read probe, and the region-table
   * decoder the probe hands off to. The dictionary id is the field that makes this more than
   * bookkeeping — a reader that got it wrong would compare string predicates against the wrong
   * dictionary and answer the query with a straight face.
   */
  @Test
  @DisplayName("every prefix parser reads the same page identity, dictionary id and tail offset")
  void everyPrefixParserAgrees() {
    final ResourceConfiguration config = newConfig();
    final long dictId = 4242L;
    final KeyValueLeafPage original = populatedPage(config);
    original.setFsstSymbolTableId(dictId);
    final MemorySegment wire = serialize(config, original, true);

    // 1. the probe: page identity plus, on a chunked page only, the dictionary id.
    final long[] probe = new long[5];
    final long[] bitmap = new long[PageLayout.BITMAP_WORDS];
    final MemorySegmentBytesIn probeIn = new MemorySegmentBytesIn(wire);
    probeIn.readByte(); // page-kind id
    final long regionTableOffset = PageKind.KEYVALUELEAFPAGE.probeRegionTableOffset(probeIn, probe, bitmap);
    assertEquals(PAGE_KEY, probe[0], "probe: page key");
    assertEquals(dictId, probe[3], "probe: FSST dictionary id");

    // 2. the column-only parse: same identity, same id, reached by stepping over the body.
    final MemorySegmentBytesIn regionsIn = new MemorySegmentBytesIn(wire);
    regionsIn.readByte();
    final RegionsOnlyPage regionsOnly =
        PageKind.KEYVALUELEAFPAGE.deserializeRegionsOnlyPage(config, regionsIn, RegionTable.ALL_KINDS, 0);
    assertEquals(probe[0], regionsOnly.getPageKey(), "regions-only: page key");
    assertEquals((int) probe[2], regionsOnly.getPopulatedSlotCount(), "regions-only: populated count");
    assertEquals(dictId, regionsOnly.getFsstSymbolTableId(), "regions-only: FSST dictionary id");
    assertTrue(regionsOnly.hasCompleteColumnCoverage(), "full image tail certifies no overflow values");
    assertEquals(1L, probe[4], "bounded probe did not see the positive column-coverage bit");

    // 3. the region-table decoder, fed the offset and the id the probe reported: the pairing the
    // bounded read actually performs.
    final MemorySegmentBytesIn tailIn = new MemorySegmentBytesIn(wire);
    tailIn.position(regionTableOffset);
    final RegionsOnlyPage fromTail = PageKind.KEYVALUELEAFPAGE.deserializeRegionTableAt(config, tailIn, probe[0],
        (int) probe[1], (int) probe[2], probe[3], RegionTable.ALL_KINDS, 0, bitmap.clone(), probe[4] != 0L);
    assertEquals(dictId, fromTail.getFsstSymbolTableId(), "region-table decoder: FSST dictionary id");
    assertEquals(regionsOnly.getPopulatedSlotCount(), fromTail.getPopulatedSlotCount());
    assertTrue(fromTail.hasCompleteColumnCoverage(), "probe certificate was not propagated");

    // 4. the full deserializer.
    final KeyValueLeafPage full = deserialize(config, wire);
    try {
      assertEquals(probe[0], full.getPageKey(), "full parse: page key");
      assertEquals(dictId, full.getFsstSymbolTableId(), "full parse: FSST dictionary id");
      assertEquals((int) probe[2], PageLayout.getPopulatedCount(full.getSlottedPage()), "full parse: populated count");
    } finally {
      original.close();
      full.close();
      fromTail.close();
      regionsOnly.close();
    }
  }

  @Test
  @DisplayName("an image without the positive coverage bit is bounded-unknown but full-tail safe")
  void absentCoverageCertificateIsConservative() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage original = populatedPage(config);
    final byte[] legacyLike = toArray(serialize(config, original, true));
    // PAGE_KEY is a one-byte varlong in this fixture. The existing inner-header flag byte follows
    // {kind, version, envelope flags, page key, revision, index type}.
    final int innerFlagsOffset = 1 + 2 + 1 + Integer.BYTES + 1 + PageLayout.OFF_FLAGS;
    legacyLike[innerFlagsOffset] &= (byte) ~PageLayout.FLAG_COMPLETE_COLUMN_COVERAGE;
    final MemorySegment wire = MemorySegment.ofArray(legacyLike);

    final long[] probe = new long[5];
    final long[] bitmap = new long[PageLayout.BITMAP_WORDS];
    final MemorySegmentBytesIn probeIn = new MemorySegmentBytesIn(wire);
    probeIn.readByte();
    final long regionTableOffset = PageKind.KEYVALUELEAFPAGE.probeRegionTableOffset(probeIn, probe, bitmap);
    assertEquals(0L, probe[4], "missing positive bit must remain unknown to a bounded read");

    final MemorySegmentBytesIn tailIn = new MemorySegmentBytesIn(wire);
    tailIn.position(regionTableOffset);
    final RegionsOnlyPage bounded = PageKind.KEYVALUELEAFPAGE.deserializeRegionTableAt(config, tailIn, probe[0],
        (int) probe[1], (int) probe[2], probe[3], RegionTable.ALL_KINDS, 0, bitmap, false);
    final MemorySegmentBytesIn fullIn = new MemorySegmentBytesIn(wire);
    fullIn.readByte();
    final RegionsOnlyPage full =
        PageKind.KEYVALUELEAFPAGE.deserializeRegionsOnlyPage(config, fullIn, RegionTable.ALL_KINDS, 0);
    try {
      assertFalse(bounded.hasCompleteColumnCoverage(), "bounded read trusted an uncertified image");
      assertTrue(full.hasCompleteColumnCoverage(), "full read did not recover completeness from the existing tail");
    } finally {
      bounded.close();
      full.close();
      original.close();
    }
  }

  @Test
  @DisplayName("a flipped bit in a chunk payload is named, not decoded")
  void corruptChunkPayloadIsRefused() {
    final ResourceConfiguration config = newConfig();
    ChunkedBodyConfig.setTargetChunkBytesForTesting(64);
    final MemorySegment wire = serialize(config, populatedPage(config), true);
    final ChunkedLayout layout = parseChunkedLayout(wire);
    assertTrue(layout.chunkCount >= 2, "this sabotage wants a page with several chunks");

    final byte[] sabotaged = toArray(wire);
    final int victim = (int) layout.chunkPayloadOffset[0];
    sabotaged[victim] ^= 0x01;

    final SirixIOException thrown =
        assertThrows(SirixIOException.class, () -> deserialize(config, MemorySegment.ofArray(sabotaged)));
    assertTrue(thrown.getMessage().contains("chunk 0"),
        "the failure must name the chunk that failed: " + thrown.getMessage());
    assertTrue(thrown.getMessage().contains("checksum"),
        "the failure must say what went wrong: " + thrown.getMessage());
  }

  @Test
  @DisplayName("a flipped bit in the META frame is named, not decoded")
  void corruptMetaFrameIsRefused() {
    final ResourceConfiguration config = newConfig();
    final MemorySegment wire = serialize(config, populatedPage(config), true);
    final ChunkedLayout layout = parseChunkedLayout(wire);

    final byte[] sabotaged = toArray(wire);
    sabotaged[(int) layout.metaPayloadOffset] ^= 0x01;

    final SirixIOException thrown =
        assertThrows(SirixIOException.class, () -> deserialize(config, MemorySegment.ofArray(sabotaged)));
    assertTrue(thrown.getMessage().contains("META frame"),
        "the failure must name the META frame: " + thrown.getMessage());
  }

  @Test
  @DisplayName("a chunk table that disagrees with the page header is refused")
  void chunkTableMustAgreeWithTheHeader() {
    final ResourceConfiguration config = newConfig();
    final MemorySegment wire = serialize(config, populatedPage(config), true);
    final ChunkedLayout layout = parseChunkedLayout(wire);

    // Claim one entry fewer than the page holds: the sum check has to catch it, because a chunk
    // table that partitions the wrong number of entries would expand records into the wrong slots.
    final byte[] sabotaged = toArray(wire);
    final int entryCountAt = (int) layout.chunkTableOffset + 2;
    final int declared = ((sabotaged[entryCountAt] & 0xFF) << 8) | (sabotaged[entryCountAt + 1] & 0xFF);
    assertNotEquals(0, declared, "the first chunk should cover at least one entry");
    sabotaged[entryCountAt] = (byte) ((declared - 1) >>> 8);
    sabotaged[entryCountAt + 1] = (byte) (declared - 1);

    final SirixIOException thrown =
        assertThrows(SirixIOException.class, () -> deserialize(config, MemorySegment.ofArray(sabotaged)));
    assertTrue(thrown.getMessage().contains("entries") || thrown.getMessage().contains("entry"),
        "the failure must point at the entry accounting: " + thrown.getMessage());
  }

  // ---------------------------------------------------------------- helpers

  private static ChunkedLayout parseChunkedLayout(final MemorySegment wire) {
    return ChunkedPageHarness.parseChunkedLayout(wire);
  }

  private static void assertSameSlottedPage(final KeyValueLeafPage expected, final KeyValueLeafPage actual) {
    ChunkedPageHarness.assertSameSlottedPage(expected, actual, "page");
  }

  private static byte[] toArray(final MemorySegment wire) {
    return ChunkedPageHarness.toArray(wire);
  }

  private static MemorySegment serialize(final ResourceConfiguration config, final KeyValueLeafPage page,
      final boolean chunked) {
    return ChunkedPageHarness.serialize(config, page, chunked);
  }

  private static KeyValueLeafPage deserialize(final ResourceConfiguration config, final MemorySegment wire) {
    return ChunkedPageHarness.deserialize(config, wire);
  }

  private static KeyValueLeafPage emptyPage(final ResourceConfiguration config) {
    return new KeyValueLeafPage(PAGE_KEY, 0, IndexType.DOCUMENT, config, false, null, new LinkedHashMap<>(),
        Allocators.getInstance().allocate(1), null, -1);
  }

  /**
   * A page of records with a mix of compressible and incompressible content, so the codec bake-off
   * has something to choose between and some frames end up stored verbatim.
   */
  private static KeyValueLeafPage populatedPage(final ResourceConfiguration config) {
    final KeyValueLeafPage page = new KeyValueLeafPage(PAGE_KEY, 0, IndexType.DOCUMENT, config, false, null,
        new LinkedHashMap<>(), Allocators.getInstance().allocate(64 * 1024), null, -1);
    final Random random = new Random(20260816L);
    for (int slot = 0; slot < 120; slot++) {
      final byte[] data;
      if (slot % 3 == 0) {
        data = record(48 + (slot % 17), slot);
      } else {
        data = new byte[32 + random.nextInt(48)];
        random.nextBytes(data);
        data[0] = UNKNOWN_KIND_ID;
      }
      page.setSlot(data, slot * 5 + 1);
    }
    return page;
  }

  /** A record of {@code length} bytes whose content repeats, hence compresses. */
  private static byte[] record(final int length, final int seed) {
    final byte[] data = new byte[length];
    data[0] = UNKNOWN_KIND_ID;
    for (int i = 1; i < length; i++) {
      data[i] = (byte) ((seed + (i % 7)) & 0xFF);
    }
    return data;
  }

  private static ResourceConfiguration newConfig() {
    return new ResourceConfiguration.Builder("chunked-body").build();
  }
}
