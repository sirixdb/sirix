/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.page.pax.GlobalStringDictionaries;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.ResolvedGlobalStrings;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The trie lane's READ SEAM: a page carries resolved BYTES, and expansion reads nothing else.
 *
 * <p>
 * {@link StringRegionGlobalLaneTest} proves the wire format. This proves the seam above it — the
 * one that decides whether a page whose values live in a resource-wide dictionary can be read back
 * at all. The cases are written against the hazards rather than the happy path, because the failures
 * this design exists to prevent are silent: a half-injected heap looks like a whole one, and a page
 * resolved against the wrong dictionary returns bytes of exactly the right shape.
 * </p>
 *
 * <p>
 * Every case first asserts that the lane ENGAGED ({@link KeyValueLeafPage#hasGlobalStringTags()}).
 * Without that a green run would prove only that a page with no global tag behaves like a page with
 * no global tag, which is the shape of hollow witness this campaign has already been caught by.
 * </p>
 */
@DisplayName("Trie lane read seam")
final class TrieLaneReadSeamTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  /** The path class the converted column tags with. */
  private static final int TAG = 4_242;

  /** The dictionary the fixture's pages name. */
  private static final long DICTIONARY_KEY = 168_227L;

  /**
   * Distinct values, in the rank order a resource-wide dictionary would have assigned. Ids are
   * therefore 1..3, and their order in the page's LOCAL dictionary is whatever the encoder chose --
   * which is the reason the resolution walk sorts.
   */
  private static final String[] VALUES = { "alpha", "beta", "gamma" };

  /** Enough slots that a partial injection would leave visible holes. */
  private static final int SLOTS = 24;

  private static MemorySegmentAllocator allocator;

  private Arena arena;
  private boolean chunkedBodyBefore;
  private boolean derivedElisionBefore;

  @BeforeAll
  static void setUpClass() {
    allocator = Allocators.getInstance();
    allocator.init(2L * 1024 * 1024 * 1024);
  }

  /**
   * The lane's two PREREQUISITES, set here because they are findings rather than fixture taste.
   *
   * <p>
   * <b>Chunked bodies.</b> Laziness is decided at WRITE time — {@code ChunkedBodyConfig.enabled()}
   * sets a flag on the serialized page, and {@code deserializePageLazily} reads it. A page written
   * without that flag can only be expanded eagerly, and eager expansion happens inside
   * {@code deserializePage} where the page object does not exist and nothing can hold a resolved
   * table. So the trie lane cannot be READ AT ALL on a page written with the flag off, and the flag
   * is off by default ({@code sirix.chunkedBody.enable}). A converted resource has to be written
   * with it on.
   * </p>
   *
   * <p>
   * <b>Derived value elision.</b> The derived form recovers each elided slot's width from the
   * region's stored string LENGTH — which is exactly what the trie lane removes. The writer's
   * plan-and-verify pass therefore refuses a global tag outright
   * ({@code StringRegion.decodeStringLength} throws for one), and the two levers collide. The tuple
   * form stores each width explicitly and does not collide, which is what this fixture uses. Which
   * way that collision is resolved decides part of the lever's size and is not a decision this test
   * makes.
   * </p>
   */
  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    chunkedBodyBefore = ChunkedBodyConfig.enabled();
    derivedElisionBefore = PageKind.DERIVED_ELISION_SECTIONS;
    ChunkedBodyConfig.setEnabledForTesting(true);
    PageKind.DERIVED_ELISION_SECTIONS = false;
  }

  @AfterEach
  void tearDown() {
    PageKind.DERIVED_ELISION_SECTIONS = derivedElisionBefore;
    ChunkedBodyConfig.setEnabledForTesting(chunkedBodyBefore);
    if (arena != null) {
      arena.close();
    }
  }

  // ------------------------------------------------------------------------------------------
  // The lane engages, and an unresolved page refuses rather than degrades
  // ------------------------------------------------------------------------------------------

  @Test
  @DisplayName("a converted page comes back marked, and owing a resolution")
  void aConvertedPageComesBackMarked() {
    withRoundTrip(new FakeDictionary(), (original, reloaded) -> {
      assertTrue(reloaded.hasGlobalStringTags(),
          "the page must carry a global tag, or every other case here proves nothing");
      assertTrue(reloaded.needsGlobalStringResolution(),
          "a freshly deserialized converted page owes a resolution; nothing has resolved it yet");
      assertNull(reloaded.resolvedGlobalStrings(),
          "null and NONE are different answers: null is 'nobody resolved', NONE is 'nothing to resolve'");
      assertNull(reloaded.globalStringDictionaries(),
          "the READ path must retain no resolver on the page -- a resolver holds a reader, and a page "
              + "outlives transactions in the buffer cache");
    });
  }

  @Test
  @DisplayName("expanding an unresolved page throws, naming the page and the tag")
  void anUnresolvedPageIsRefusedLoudly() {
    withRoundTrip(new FakeDictionary(), (original, reloaded) -> {
      assertTrue(reloaded.hasGlobalStringTags(), "the lane must have engaged");
      final SirixIOException refusal =
          assertThrows(SirixIOException.class, () -> reloaded.getSlotAsByteArray(0),
              "a page whose global tags nobody resolved must refuse, not hand back placeholder bytes");
      assertTrue(refusal.getMessage().contains(String.valueOf(TAG)),
          () -> "the refusal must name the tag that went unresolved: " + refusal.getMessage());
      assertTrue(refusal.getMessage().contains("resolved"),
          () -> "the refusal must say what is missing: " + refusal.getMessage());
    });
  }

  /**
   * A refused expansion is RETRY-SAFE: the page still reads correctly once it is resolved.
   *
   * <p>
   * <b>What this does not prove, established by mutation rather than assumed.</b> Deleting the
   * pre-pass leaves this case green. The reason is in {@code LazyChunkedBody.materialize}: the
   * chunk's materialized bit is set with a release store only AFTER {@code injector.inject}
   * returns, so a throw anywhere inside injection leaves the chunk pending and the next touch
   * re-runs decode and expansion from the wire bytes it still holds. A mid-loop throw is therefore
   * recoverable today, and this case cannot tell one from a pre-pass.
   * </p>
   *
   * <p>
   * What the pre-pass is still for, then, is two things this test does not carry: the refusal names
   * the PAGE and the tag rather than a slot (which {@link #anUnresolvedPageIsRefusedLoudly} does
   * carry, and which deleting the pre-pass does break), and it holds if a future expansion route
   * ever publishes a chunk before injecting into it. That is defence in depth, stated as such
   * instead of claimed as proven.
   * </p>
   */
  @Test
  @DisplayName("a refused expansion is retry-safe: the page reads correctly once resolved")
  void aRefusedExpansionIsRetrySafe() {
    final FakeDictionary dictionary = new FakeDictionary();
    withRoundTrip(dictionary, (original, reloaded) -> {
      assertThrows(SirixIOException.class, () -> reloaded.getSlotAsByteArray(0));
      assertThrows(SirixIOException.class, () -> reloaded.getSlotAsByteArray(SLOTS - 1));

      resolve(reloaded, dictionary);
      assertSlotsMatch(original, reloaded);
    });
  }

  // ------------------------------------------------------------------------------------------
  // A resolved page reads back byte for byte, from the table alone
  // ------------------------------------------------------------------------------------------

  @Test
  @DisplayName("a resolved page round-trips every slot byte for byte")
  void aResolvedPageRoundTrips() {
    final FakeDictionary dictionary = new FakeDictionary();
    withRoundTrip(dictionary, (original, reloaded) -> {
      resolve(reloaded, dictionary);
      assertSlotsMatch(original, reloaded);
      assertFalse(reloaded.needsGlobalStringResolution(), "a resolved page owes nothing further");
    });
  }

  /**
   * Expansion consults the TABLE and never a resolver — the property that makes shape (d) safe.
   *
   * <p>
   * The dictionary is disarmed after the resolution: any further call to it fails the test. If
   * expansion still reached a resolver — through the page, through a captured lambda, through
   * anything — the reads below would trip it. This is the case that would catch a well-meaning
   * change that "just looks the value up when it is missing".
   * </p>
   */
  @Test
  @DisplayName("expansion after resolution never touches a dictionary again")
  void expansionReadsOnlyTheTable() {
    final FakeDictionary dictionary = new FakeDictionary();
    withRoundTrip(dictionary, (original, reloaded) -> {
      resolve(reloaded, dictionary);
      dictionary.poisoned = true;
      assertSlotsMatch(original, reloaded);
    });
  }

  /**
   * Resolution is PAGE-determined: the first transaction to resolve fixes the values.
   *
   * <p>
   * A second transaction whose own anchors would have refused the tag is not a disagreement about
   * the bytes -- it is one party declining to answer. The page NAMES its dictionary and a
   * rank-ordered dictionary only appends, so any reader that can see that dictionary at all computes
   * the same values. First-writer-wins is therefore free of the usual race hazard, and this pins it
   * so a later "keep the newest" refactor has to argue with a test.
   * </p>
   */
  @Test
  @DisplayName("a second resolution does not displace the first")
  void resolutionIsPageDetermined() {
    final FakeDictionary dictionary = new FakeDictionary();
    withRoundTrip(dictionary, (original, reloaded) -> {
      resolve(reloaded, dictionary);
      final ResolvedGlobalStrings first = reloaded.resolvedGlobalStrings();
      assertNotNull(first, "the resolution must have published a table");

      reloaded.setResolvedGlobalStrings(ResolvedGlobalStrings.NONE);
      assertSame(first, reloaded.resolvedGlobalStrings(),
          "the first resolution stands; a second must not replace a table readers may already be walking");
      assertSlotsMatch(original, reloaded);
    });
  }

  // ------------------------------------------------------------------------------------------
  // Refusals: a stale anchor, and the reader-less routes
  // ------------------------------------------------------------------------------------------

  /**
   * The temporal-validity refusal, at the layer that owns it.
   *
   * <p>
   * The dictionary here holds the right values under the right key, and has SHRUNK. A rank-ordered
   * dictionary only ever appends, so a live count below the recorded one is not a stale page — it is
   * a different dictionary under a reused key, whose ids mean something else. Resolving anyway would
   * return plausible bytes, which is why this must be an exception and not a fallback.
   * </p>
   */
  @Test
  @DisplayName("a page naming a shrunk dictionary is refused, naming the dictionary and the id")
  void aStaleAnchorIsRefusedLoudly() {
    final FakeDictionary dictionary = new FakeDictionary();
    withRoundTrip(dictionary, (original, reloaded) -> {
      dictionary.liveEntryCount = 1;
      final SirixIOException refusal =
          assertThrows(SirixIOException.class, () -> resolve(reloaded, dictionary),
              "a dictionary that shrank under a reused key must refuse rather than resolve");
      assertTrue(refusal.getMessage().contains(String.valueOf(DICTIONARY_KEY)),
          () -> "the refusal must name the dictionary the page recorded: " + refusal.getMessage());
      assertTrue(refusal.getMessage().contains("appends"),
          () -> "and say why a smaller live count is not staleness: " + refusal.getMessage());

      assertTrue(reloaded.needsGlobalStringResolution(),
          "a refused resolution must publish nothing; a half-table is worse than none");
      assertThrows(SirixIOException.class, () -> reloaded.getSlotAsByteArray(0),
          "and the page must still refuse to expand");
    });
  }

  /**
   * §5 of the seam design: the copy-on-write flush lane holds no reader.
   *
   * <p>
   * {@code deepCopy()} expands its SOURCE before copying the segment, from a stack with no reader on
   * it. The writer fronts the resolution before a page reaches that lane; this is the assertion that
   * says so when it does not, and it names the route rather than a slot.
   * </p>
   */
  @Test
  @DisplayName("copy-on-write refuses an unresolved page, naming the route")
  void copyOnWriteRefusesAnUnresolvedPage() {
    withRoundTrip(new FakeDictionary(), (original, reloaded) -> {
      final IllegalStateException refusal = assertThrows(IllegalStateException.class, reloaded::deepCopy,
          "deepCopy expands its source and holds no reader; an unresolved page must not reach it");
      assertTrue(refusal.getMessage().contains("copy-on-write"),
          () -> "the refusal must name the route that needs fronting: " + refusal.getMessage());
    });
  }

  @Test
  @DisplayName("a reused frame inherits no resolved values from its previous occupant")
  void resetClearsTheBinding() {
    final FakeDictionary dictionary = new FakeDictionary();
    withRoundTrip(dictionary, (original, reloaded) -> {
      resolve(reloaded, dictionary);
      assertNotNull(reloaded.resolvedGlobalStrings());

      reloaded.reset();
      assertNull(reloaded.resolvedGlobalStrings(),
          "a table indexed by the OLD page's tag positions would answer the new page's lookups with the "
              + "old page's values -- plausible bytes of the right shape");
      assertFalse(reloaded.hasGlobalStringTags(), "and the flag that describes that region must go with it");
      assertNull(reloaded.globalStringDictionaries(), "a pooled frame must not hold a transaction's reader alive");
    });
  }

  // ------------------------------------------------------------------------------------------
  // The resolution walk itself
  // ------------------------------------------------------------------------------------------

  @Test
  @DisplayName("the walk resolves a tag's ids in ascending order")
  void theWalkResolvesAscending() {
    final FakeDictionary dictionary = new FakeDictionary();
    withRoundTrip(dictionary, (original, reloaded) -> {
      resolve(reloaded, dictionary);
      final List<Integer> asked = dictionary.requestedIds;
      assertEquals(VALUES.length, asked.size(), "every distinct value on the page is resolved exactly once");
      for (int i = 1; i < asked.size(); i++) {
        assertTrue(asked.get(i - 1) < asked.get(i),
            () -> "ids must be walked ascending -- a random walk costs 417 ns against 75 ns and no test "
                + "would otherwise notice: " + asked);
      }
    });
  }

  @Test
  @DisplayName("a tag index carrying a different tag value is refused, not read")
  void aShiftedTagIndexIsRefused() {
    final ResolvedGlobalStrings table = ResolvedGlobalStrings.forTags(2)
                                                             .tag(0, TAG, new int[] { 1 },
                                                                 new byte[][] { "alpha".getBytes(StandardCharsets.UTF_8) })
                                                             .build();
    assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), table.value(0, TAG, 0));
    assertThrows(IllegalStateException.class, () -> table.value(0, TAG + 1, 0),
        "the table is indexed by a position, and a position means nothing for a region it was not parsed "
            + "from -- a re-encode must fail loudly rather than read another tag's dictionary");
    assertThrows(IllegalStateException.class, () -> table.value(1, TAG, 0),
        "an index that was never resolved holds no values and must not answer");
  }

  @Test
  @DisplayName("a tag resolved with a hole is refused at build time")
  void aHoleIsRefusedAtBuildTime() {
    assertThrows(IllegalArgumentException.class,
        () -> ResolvedGlobalStrings.forTags(1).tag(0, TAG, new int[] { 1, 2 }, new byte[][] { new byte[1], null }),
        "a half-resolved tag would expand into a record with an absent value, which is not a record with an "
            + "empty value");
  }

  // ------------------------------------------------------------------------------------------
  // Fixture
  // ------------------------------------------------------------------------------------------

  /** What a case does with the page it wrote and the page that came back. */
  private interface RoundTrip {
    void accept(KeyValueLeafPage original, KeyValueLeafPage reloaded);
  }

  /**
   * Write a converted page, serialize it, read it back LAZILY, and hand both to {@code body}.
   *
   * <p>
   * Lazy is not a detail of the fixture: the trie lane requires it. Eager expansion runs inside
   * {@code deserializePage}, where the page object does not exist yet, so nothing can hold a resolved
   * table — which is why the eager path refuses a global tag outright.
   * </p>
   */
  private void withRoundTrip(final FakeDictionary dictionary, final RoundTrip body) {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("trieLaneReadSeam").build();
    final KeyValueLeafPage original = new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1,
        arena.allocate(MemorySegmentAllocator.SIXTYFOUR_KB), null);
    KeyValueLeafPage reloaded = null;
    try {
      original.setGlobalStringDictionaries(dictionary);
      for (int slot = 0; slot < SLOTS; slot++) {
        writeString(original, slot, 100, TAG, VALUES[slot % VALUES.length]);
      }

      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, original, SerializationType.DATA);

      final BytesIn<?> source = sink.bytesForRead();
      source.readByte();
      reloaded = (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE
          .deserializePageLazily(config, source, SerializationType.DATA, null);
      body.accept(original, reloaded);
    } finally {
      if (reloaded != null) {
        reloaded.close();
      }
      original.close();
    }
  }

  /** What the reader's resolution site does, minus the guard discipline that is its own concern. */
  private static void resolve(final KeyValueLeafPage page, final GlobalStringDictionaries dictionary) {
    final RegionTable regions = page.getRegionTable();
    assertNotNull(regions, "a converted page must carry a region table");
    final MemorySegment payload = regions.payload(RegionTable.KIND_STRING);
    assertNotNull(payload, "and a string region");
    final StringRegion.Header header = new StringRegion.Header();
    header.parseInto(payload);
    page.setResolvedGlobalStrings(ResolvedGlobalStrings.resolve(header, payload, dictionary, page.getPageKey()));
  }

  private static void assertSlotsMatch(final KeyValueLeafPage original, final KeyValueLeafPage reloaded) {
    for (int slot = 0; slot < SLOTS; slot++) {
      assertArrayEquals(original.getSlotAsByteArray(slot), reloaded.getSlotAsByteArray(slot),
          "slot " + slot + " did not survive the round trip through the dictionary");
    }
  }

  private static void writeString(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final long pathNodeKey, final String value) {
    final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey,
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, pathNodeKey, 0, 0, 0L,
        value.getBytes(StandardCharsets.UTF_8), HASH_FN, (byte[]) null, false, null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1)));
  }

  /**
   * A resource-wide dictionary, stubbed: rank-ordered ids, a live count that can be made to shrink,
   * a record of the ids it was asked for, and a poison switch.
   *
   * <p>
   * The poison switch is what turns "expansion reads the table" from an assertion about code shape
   * into an assertion a test can fail.
   * </p>
   */
  private static final class FakeDictionary implements GlobalStringDictionaries {

    private final Map<String, Integer> ids = new LinkedHashMap<>();
    private final Map<Integer, byte[]> values = new HashMap<>();

    /** Ids this dictionary was asked to resolve, in the order asked. */
    private final List<Integer> requestedIds = new ArrayList<>();

    /** Live entry count; negative means "as many as it holds", so a shrink is constructible. */
    private int liveEntryCount = -1;

    /** Once true, any further use fails the test. */
    private boolean poisoned;

    FakeDictionary() {
      for (int i = 0; i < VALUES.length; i++) {
        ids.put(VALUES[i], i + 1);
        values.put(i + 1, VALUES[i].getBytes(StandardCharsets.UTF_8));
      }
    }

    private void checkAlive() {
      if (poisoned) {
        throw new AssertionError("the dictionary was consulted after resolution; expansion must read the "
            + "page's resolved table and nothing else");
      }
    }

    @Override
    public boolean hasDictionary(final int tag) {
      checkAlive();
      return tag == TAG;
    }

    @Override
    public int idOf(final int tag, final byte[] value, final int offset, final int length) {
      checkAlive();
      return tag == TAG
          ? ids.getOrDefault(new String(value, offset, length, StandardCharsets.UTF_8), ID_ABSENT)
          : ID_ABSENT;
    }

    @Override
    public byte @Nullable [] valueOf(final int tag, final long dictionaryKey, final int recordedEntryCount,
        final int id) {
      checkAlive();
      requestedIds.add(id);
      return tag == TAG && accepts(tag, dictionaryKey, recordedEntryCount)
          ? values.get(id)
          : null;
    }

    @Override
    public boolean accepts(final int tag, final long dictionaryKey, final int recordedEntryCount) {
      checkAlive();
      if (tag != TAG || dictionaryKey != DICTIONARY_KEY) {
        return false;
      }
      final int live = liveEntryCount < 0
          ? ids.size()
          : liveEntryCount;
      return live >= recordedEntryCount;
    }

    @Override
    public long dictionaryKey(final int tag) {
      checkAlive();
      return DICTIONARY_KEY;
    }

    @Override
    public int dictionaryEntryCount(final int tag) {
      checkAlive();
      return ids.size();
    }
  }
}
