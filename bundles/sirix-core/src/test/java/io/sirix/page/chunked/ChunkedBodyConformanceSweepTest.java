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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
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

  private static final String[] FLAG_NAMES =
      {"hashElision", "parentKeyColumn", "pathNodeKeyColumn", "valueElision", "nameKeyElision"};

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

    final List<Case> cases = new ArrayList<>();
    final Map<String, Integer> groupSizes = new LinkedHashMap<>();
    addAll(cases, groupSizes, "core lever cross-product", coreCases());
    addAll(cases, groupSizes, "column-activation overlay", columnCases());
    addAll(cases, groupSizes, "page-shape overlay", shapeCases());
    addAll(cases, groupSizes, "record-size overlay", sizeCases());
    addAll(cases, groupSizes, "DeweyID overlay", deweyCases());
    addAll(cases, groupSizes, "degenerate-body arm", degenerateCases());

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

  // ---------------------------------------------------------------- the cross-product

  /**
   * The full product of the five content levers, at a page shape and entry count that straddle chunk
   * boundaries: 65 records is more than one chunk at the small target and exactly one at the default.
   */
  private static List<Case> coreCases() {
    final List<Case> cases = new ArrayList<>();
    for (final Hash hash : Hash.values()) {
      for (final ParentKeys parentKeys : ParentKeys.values()) {
        for (final PathKeys pathKeys : PathKeys.values()) {
          for (final Values values : Values.values()) {
            for (final Names names : Names.values()) {
              for (final int target : new int[] {64, 4096}) {
                cases.add(new Case(new Recipe(Body.TEMPLATED, hash, parentKeys, pathKeys, values, names, Shape.DENSE,
                    Sizes.SMALL, 65, false), target));
              }
            }
          }
        }
      }
    }
    return cases;
  }

  /**
   * Bigger pages, where the dictionary columns start to pay for themselves: a column only activates
   * when its encoding comes out smaller than the varints it displaces, which a 65-record page rarely
   * manages and a 512-record one usually does.
   */
  private static List<Case> columnCases() {
    final List<Case> cases = new ArrayList<>();
    for (final int entries : new int[] {256, 512}) {
      for (final PathKeys pathKeys : PathKeys.values()) {
        for (final Hash hash : Hash.values()) {
          for (final Values values : new Values[] {Values.MIXED, Values.STRUCTURAL, Values.MIXED_STRUCTURAL}) {
            for (final Names names : new Names[] {Names.WIDE, Names.MANY}) {
              cases.add(new Case(new Recipe(Body.TEMPLATED, hash, ParentKeys.SEQUENTIAL, pathKeys, values, names,
                  Shape.DENSE, Sizes.SMALL, entries, false), 4096));
            }
          }
        }
      }
    }
    return cases;
  }

  /**
   * Bitmap shapes and entry counts, including the counts that sit either side of a chunk boundary.
   */
  private static List<Case> shapeCases() {
    final List<Case> cases = new ArrayList<>();
    final Recipe[] profiles = {
        new Recipe(Body.TEMPLATED, Hash.ALL_ZERO, ParentKeys.SEQUENTIAL, PathKeys.SINGLE, Values.MIXED, Names.WIDE,
            Shape.DENSE, Sizes.SMALL, 0, false),
        new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.ALL_NULL, PathKeys.DISTINCT, Values.NUMBERS, Names.FEW,
            Shape.DENSE, Sizes.MIXED, 0, false)};
    for (final Shape shape : Shape.values()) {
      for (final int entries : new int[] {0, 1, 2, 63, 64, 65, 511, 512}) {
        if (entries > ChunkedPageGenerator.capacity(shape)) {
          continue;
        }
        for (final int target : new int[] {64, 4096}) {
          for (final Recipe profile : profiles) {
            cases.add(new Case(withShape(profile, shape, entries), target));
          }
        }
      }
    }
    return cases;
  }

  /**
   * Record sizes, including one record larger than the chunk target and one near the record ceiling.
   */
  private static List<Case> sizeCases() {
    final List<Case> cases = new ArrayList<>();
    for (final Sizes sizes : Sizes.values()) {
      for (final int entries : new int[] {8, 64}) {
        for (final int target : new int[] {64, 4096, 1 << 20}) {
          cases.add(new Case(new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.SEQUENTIAL, PathKeys.FEW,
              Values.STRINGS, Names.WIDE, Shape.DENSE, sizes, entries, false), target));
        }
      }
    }
    return cases;
  }

  /** DeweyID trailers live inside the heap, so they ride the chunks rather than any slot's bytes. */
  private static List<Case> deweyCases() {
    final List<Case> cases = new ArrayList<>();
    for (final Values values : Values.values()) {
      for (final int target : new int[] {64, 4096}) {
        cases.add(new Case(new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.MIXED_NULL, PathKeys.FEW, values,
            Names.WIDE, Shape.DENSE, Sizes.MIXED, 33, true), target));
      }
    }
    return cases;
  }

  /** The degenerate body: META is the compact dir alone and the chunks are verbatim records. */
  private static List<Case> degenerateCases() {
    final List<Case> cases = new ArrayList<>();
    for (final Shape shape : Shape.values()) {
      for (final int entries : new int[] {0, 1, 2, 64, 512}) {
        if (entries > ChunkedPageGenerator.capacity(shape)) {
          continue;
        }
        for (final Sizes sizes : Sizes.values()) {
          for (final int target : new int[] {64, 4096}) {
            cases.add(new Case(new Recipe(Body.DEGENERATE, Hash.NONE_ZERO, ParentKeys.SEQUENTIAL, PathKeys.SINGLE,
                Values.NUMBERS, Names.ONE, shape, sizes, entries, false), target));
          }
        }
      }
    }
    return cases;
  }

  private static Recipe withShape(final Recipe profile, final Shape shape, final int entries) {
    return new Recipe(profile.body(), profile.hash(), profile.parentKeys(), profile.pathKeys(), profile.values(),
        profile.names(), shape, profile.sizes(), entries, profile.deweyIds());
  }

  private static void addAll(final List<Case> all, final Map<String, Integer> groupSizes, final String group,
      final List<Case> cases) {
    groupSizes.put(group, cases.size());
    all.addAll(cases);
  }

  private record Case(Recipe recipe, int targetChunkBytes) {
    boolean dewey() {
      return recipe.deweyIds();
    }
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
        flagCombos.merge(flagsToString(layout.structuralFlags), 1, Integer::sum);
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

  private static String flagsToString(final int flags) {
    if (flags == 0) {
      return "(none)";
    }
    final StringBuilder text = new StringBuilder();
    for (int bit = 0; bit < FLAG_NAMES.length; bit++) {
      if ((flags & (1 << bit)) != 0) {
        if (!text.isEmpty()) {
          text.append('+');
        }
        text.append(FLAG_NAMES[bit]);
      }
    }
    return text.toString();
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
    for (int flags = 0; flags < 32; flags++) {
      if (!stats.flagCombos.containsKey(flagsToString(flags))) {
        out.append("  ").append(flagsToString(flags)).append('\n');
        unreached++;
      }
    }
    out.append("  ").append(32 - unreached).append(" of 32 combinations reached\n");
    System.out.println(out);
  }
}
