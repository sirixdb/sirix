/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.page.chunked.ChunkedPageGenerator.Body;
import io.sirix.page.chunked.ChunkedPageGenerator.Hash;
import io.sirix.page.chunked.ChunkedPageGenerator.Names;
import io.sirix.page.chunked.ChunkedPageGenerator.ParentKeys;
import io.sirix.page.chunked.ChunkedPageGenerator.PathKeys;
import io.sirix.page.chunked.ChunkedPageGenerator.Recipe;
import io.sirix.page.chunked.ChunkedPageGenerator.Shape;
import io.sirix.page.chunked.ChunkedPageGenerator.Sizes;
import io.sirix.page.chunked.ChunkedPageGenerator.Values;
import io.sirix.page.chunked.ChunkedPageHarness.ChunkedLayout;
import io.sirix.page.chunked.ChunkedSweepCases.Case;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every page the generator can describe, written both ways and compared.
 *
 * <p>
 * The claim under test is the one the whole format rests on: chunk framing changes how a page's
 * bytes are packaged and nothing about the page they unpack into. So each recipe is built three
 * times — once serialized monolithically, twice chunk-framed — and the sweep asserts that the two
 * decoded pages are byte-identical across header, slot bitmap, directory and heap (DeweyID trailers
 * included), and that the two chunked serializations are byte-identical to each other.
 *
 * <p>
 * <b>What is enumerated and what is not.</b> The structural levers, page shapes, entry counts,
 * record sizes and chunk targets are swept as a full cross-product within each group below; the
 * group split follows the plan's core-plus-overlay pattern, because a single product over every
 * axis would multiply out to hundreds of thousands of pages for coverage the axes do not interact
 * to produce. The <em>codec</em> axis is deliberately not one of them: a codec sees an opaque byte
 * range and hands back the same bytes, so its independence is proven once, per codec, over the byte
 * shapes a body actually contains, in {@link BodyCodecRoundTripTest}. Which codecs this sweep
 * happened to elect is reported, not asserted.
 *
 * <p>
 * <b>Reachability.</b> The five structural levers are content-driven and cannot be forced from
 * outside, so the sweep reports which of the 32 flag combinations its recipes actually provoked —
 * currently 17. The rest are unreached rather than proven impossible: most pair a lever that fires
 * only on large pages of structural records with the absence of one that always pays on such a
 * page, so no single recipe produces them. That is a fact about this writer's activation predicates
 * and this generator's content, and it has to be re-derived when either changes.
 */
@DisplayName("Chunked body conformance sweep")
final class ChunkedBodyConformanceSweepTest {

  private static final String[] FLAG_NAMES = {"hashElision", "parentKeyColumn", "pathNodeKeyColumn", "valueElision",
      "nameKeyElision", "derivedElision", null, "extended"};

  /** Names of the second flags byte's bits, in the same order. */
  private static final String[] EXTENDED_FLAG_NAMES = {"rightSibColumn", "leftSibColumn"};

  private boolean previouslyEnabled;
  private int previousTarget;

  @BeforeEach
  void setUp() {
    Allocators.getInstance().init(2L * 1024 * 1024 * 1024);
    previouslyEnabled = ChunkedBodyConfig.setEnabledForTesting(false);
    previousTarget = ChunkedBodyConfig.targetChunkBytes();
  }

  @AfterEach
  void tearDown() {
    ChunkedBodyConfig.setEnabledForTesting(previouslyEnabled);
    ChunkedBodyConfig.setTargetChunkBytesForTesting(previousTarget);
  }

  @Test
  @DisplayName("every generated page decodes identically chunked and monolithic, and frames deterministically")
  void sweep() {
    final ResourceConfiguration plain = new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
    final ResourceConfiguration dewey =
        new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).useDeweyIDs(true).build();

    final Map<String, Integer> groupSizes = new LinkedHashMap<>();
    ChunkedSweepCases.byGroup().forEach((group, groupCases) -> groupSizes.put(group, groupCases.size()));
    final List<Case> cases = ChunkedSweepCases.all();

    final Stats stats = new Stats();
    final long startedAt = System.nanoTime();
    for (final Case testCase : cases) {
      run(testCase, testCase.dewey()
          ? dewey
          : plain, stats);
    }
    final long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

    report(groupSizes, cases.size(), elapsedMillis, stats);

    // The sweep is worthless if the recipes never provoked the levers it claims to cover.
    assertTrue(stats.flagCombos.size() > 1,
        "every templated page produced the same structural-flags byte: " + stats.flagCombos.keySet());
    assertTrue(stats.chunkCounts.lastKey() > 1, "no recipe produced more than one chunk");
    assertTrue(stats.chunkCounts.firstKey() <= 1, "no recipe produced a single-chunk (or empty) page");
    assertTrue(stats.codecs.size() > 1, "every frame elected the same codec, so the codec axis saw one value");
  }

  /**
   * The defect this sweep found, pinned so it cannot come back.
   *
   * <p>
   * Name-key elision strips a record's name key and leaves the reader to fetch it from the page's
   * name-key region. Whether that region exists is decided by its encoder, which refuses any page
   * holding 255 or more distinct names — counting the fused primitives <em>and</em> the fused
   * structurals (OBJECT- and ARRAY-valued fields). The writer's own ceiling check counted only the
   * primitives, so a page mixing the two could stay under the writer's count, activate elision, and
   * then be written with no region behind it. Nothing failed at write time; the page simply could
   * never be read again — every read threw "ObjectKeyNameKeyRegion missing for elided page".
   *
   * <p>
   * This is a property of the monolith writer, not of chunk framing: both bodies produced the same
   * unreadable page. The writer now activates the elision on the region's presence rather than on a
   * count that stood in for it.
   */
  @Test
  @DisplayName("a page mixing fused primitives and structurals past the name-region ceiling stays readable")
  void nameKeyElisionNeverOutrunsItsRegion() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
    // 256 entries of five interleaved kinds, every one with a distinct name: past the region
    // encoder's ceiling when counted over all fused-named slots, under it when counted over the
    // primitives alone.
    final Recipe recipe = new Recipe(Body.TEMPLATED, Hash.NONE_ZERO, ParentKeys.SEQUENTIAL, PathKeys.SINGLE,
        Values.MIXED_STRUCTURAL, Names.MANY, Shape.DENSE, Sizes.SMALL, 256, false);
    for (final boolean chunked : new boolean[] {false, true}) {
      final MemorySegment wire = serializeFresh(config, recipe, chunked);
      final KeyValueLeafPage page = decodeOrFail(config, wire, recipe + (chunked
          ? " [chunked]"
          : " [monolith]"));
      try {
        assertEquals(256, PageLayout.getPopulatedCount(page.getSlottedPage()));
      } finally {
        page.close();
      }
    }
  }

  private void run(final Case testCase, final ResourceConfiguration config, final Stats stats) {
    ChunkedBodyConfig.setTargetChunkBytesForTesting(testCase.targetChunkBytes());
    final Recipe recipe = testCase.recipe();
    final String what = recipe + " @C=" + testCase.targetChunkBytes();

    final MemorySegment monolithWire = serializeFresh(config, recipe, false);
    final MemorySegment chunkedWire = serializeFresh(config, recipe, true);
    // A page is serialized a second time whenever its overflow references were unresolved on the
    // first pass, and the compressed-segment cache assumes both passes agree byte for byte.
    final MemorySegment chunkedAgain = serializeFresh(config, recipe, true);
    assertArrayEquals(ChunkedPageHarness.toArray(chunkedWire), ChunkedPageHarness.toArray(chunkedAgain),
        what + ": framing is not deterministic");

    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(chunkedWire);
    assertChunkTablePartitionsThePage(layout, what);
    stats.record(layout);

    // A page that refuses to decode throws from deep in the deserializer, where nothing knows which
    // recipe produced it; without the recipe a sweep failure is a needle in 2000 haystacks.
    final KeyValueLeafPage monolith = decodeOrFail(config, monolithWire, what + " [monolith]");
    final KeyValueLeafPage chunked = decodeOrFail(config, chunkedWire, what + " [chunked]");
    try {
      assertEquals(recipe.entryCount(), PageLayout.getPopulatedCount(monolith.getSlottedPage()),
          what + ": the generator did not populate the slots it promised");
      ChunkedPageHarness.assertSameSlottedPage(monolith, chunked, what);
    } finally {
      monolith.close();
      chunked.close();
    }
  }

  private static KeyValueLeafPage decodeOrFail(final ResourceConfiguration config, final MemorySegment wire,
      final String what) {
    try {
      return ChunkedPageHarness.deserialize(config, wire);
    } catch (final RuntimeException e) {
      throw new AssertionError(what + ": " + e, e);
    }
  }

  /**
   * A page is built fresh for each serialization: serializing one twice would hand back the cached
   * compressed bytes of the first pass, which would make the determinism check vacuous and the second
   * format never run at all.
   */
  private static MemorySegment serializeFresh(final ResourceConfiguration config, final Recipe recipe,
      final boolean chunked) {
    final KeyValueLeafPage page = ChunkedPageGenerator.build(recipe, config);
    try {
      return ChunkedPageHarness.serialize(config, page, chunked);
    } finally {
      page.close();
    }
  }

  /**
   * The chunk table has to partition the page's entries: contiguous, ascending, covering every entry
   * exactly once, and accounting for every heap byte. A table that does not is how a record's bytes
   * end up expanded into another record's slot.
   */
  private static void assertChunkTablePartitionsThePage(final ChunkedLayout layout, final String what) {
    int entries = 0;
    long heapBytes = 0;
    for (int c = 0; c < layout.chunkCount; c++) {
      assertEquals(entries, layout.chunkFirstEntry[c],
          what + ": chunk " + c + " does not start where " + (c - 1) + " ended");
      assertTrue(layout.chunkEntryCount[c] > 0, what + ": chunk " + c + " covers no entries");
      entries += layout.chunkEntryCount[c];
      heapBytes += layout.chunkRawLen[c];
    }
    assertEquals(layout.populatedCount, entries, what + ": the chunk table covers the wrong number of entries");
    assertEquals(layout.onDiskHeapSize, heapBytes, what + ": the chunk table covers the wrong number of heap bytes");
  }

  // ---------------------------------------------------------------- reporting

  private static final class Stats {
    private final Map<String, Integer> flagCombos = new TreeMap<>();
    private final TreeMap<Integer, Integer> chunkCounts = new TreeMap<>();
    private final TreeMap<Integer, Integer> codecs = new TreeMap<>();
    private int degeneratePages;
    private long totalChunks;
    private long metaBytes;
    private long heapBytes;

    void record(final ChunkedLayout layout) {
      if (layout.templateCount == 0) {
        degeneratePages++;
      } else {
        flagCombos.merge(flagsToString(layout.structuralFlags, layout.extendedStructuralFlags), 1, Integer::sum);
      }
      chunkCounts.merge(layout.chunkCount, 1, Integer::sum);
      codecs.merge(layout.metaCodec, 1, Integer::sum);
      for (int c = 0; c < layout.chunkCount; c++) {
        codecs.merge(layout.chunkCodec[c], 1, Integer::sum);
      }
      totalChunks += layout.chunkCount;
      metaBytes += layout.metaRawLen;
      heapBytes += layout.onDiskHeapSize;
    }
  }

  private static String flagsToString(final int flags, final int extendedFlags) {
    if (flags == 0 && extendedFlags == 0) {
      return "(none)";
    }
    final StringBuilder text = new StringBuilder();
    appendFlags(text, flags, FLAG_NAMES);
    appendFlags(text, extendedFlags, EXTENDED_FLAG_NAMES);
    return text.toString();
  }

  private static void appendFlags(final StringBuilder text, final int flags, final String[] names) {
    for (int bit = 0; bit < names.length; bit++) {
      if ((flags & (1 << bit)) != 0 && names[bit] != null) {
        if (!text.isEmpty()) {
          text.append('+');
        }
        text.append(names[bit]);
      }
    }
  }

  private static void report(final Map<String, Integer> groupSizes, final int total, final long elapsedMillis,
      final Stats stats) {
    final StringBuilder out = new StringBuilder(1024);
    out.append("[chunked-sweep] pages compared, by group:\n");
    for (final Map.Entry<String, Integer> group : groupSizes.entrySet()) {
      out.append("  ").append(group.getKey()).append(": ").append(group.getValue()).append('\n');
    }
    out.append("  TOTAL: ")
       .append(total)
       .append(" recipes × 3 serializations + 2 deserializations, in ")
       .append(elapsedMillis)
       .append(" ms\n");
    out.append("[chunked-sweep] chunks per page: ")
       .append(stats.chunkCounts)
       .append(" (")
       .append(stats.totalChunks)
       .append(" chunks framed)\n");
    out.append("[chunked-sweep] frame codecs elected (0=ZeroRun 2=ByteRun 3=LZ77 4=STORED): ")
       .append(stats.codecs)
       .append('\n');
    out.append("[chunked-sweep] degenerate-body pages: ")
       .append(stats.degeneratePages)
       .append("; META bytes framed: ")
       .append(stats.metaBytes)
       .append("; heap bytes framed: ")
       .append(stats.heapBytes)
       .append('\n');
    out.append("[chunked-sweep] structural-flag combinations reached by templated pages:\n");
    for (final Map.Entry<String, Integer> combo : stats.flagCombos.entrySet()) {
      out.append("  ").append(combo.getKey()).append(": ").append(combo.getValue()).append(" pages\n");
    }
    out.append("[chunked-sweep] combinations NO recipe in this sweep produced — a fact about this writer's"
        + " activation predicates (PageKind.writeEncodedBody) and this generator's content, not about the"
        + " format; re-derive when either changes:\n");
    int unreached = 0;
    // Six flags in the first byte and two in the second, enumerated as one space so a lever that never
    // activates in this sweep is named rather than silently absent.
    final int combinations = 1 << 8;
    for (int combo = 0; combo < combinations; combo++) {
      final String name = flagsToString(combo & 0x3F, combo >>> 6);
      if (!stats.flagCombos.containsKey(name)) {
        out.append("  ").append(name).append('\n');
        unreached++;
      }
    }
    out.append("  ")
       .append(combinations - unreached)
       .append(" of ")
       .append(combinations)
       .append(" combinations reached\n");
    System.out.println(out);
  }
}
