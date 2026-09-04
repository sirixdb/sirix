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
import io.sirix.node.json.ObjectNamedNumberNode;
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
 * at all. The cases are written against the hazards rather than the happy path, because the
 * failures this design exists to prevent are silent: a half-injected heap looks like a whole one,
 * and a page resolved against the wrong dictionary returns bytes of exactly the right shape.
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
  private static final String[] VALUES = {"alpha", "beta", "gamma"};

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
   * is off by default ({@code sirix.chunkedBody.enable}). A converted resource has to be written with
   * it on.
   * </p>
   *
   * <p>
   * <b>Derived value elision stays ON, at its default, deliberately.</b> The derived form recovers
   * each elided slot's width from the region's stored string LENGTH, so a lane that removed the
   * lengths could not be serialized at all — the writer's plan-and-verify pass refused a global tag
   * outright. That is what the per-dictionary-entry length lane exists for, and running this fixture
   * with the default rather than with the lever switched off is the only way the suite can tell
   * whether the collision is actually resolved.
   * </p>
   */
  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    chunkedBodyBefore = ChunkedBodyConfig.enabled();
    derivedElisionBefore = PageKind.DERIVED_ELISION_SECTIONS;
    ChunkedBodyConfig.setEnabledForTesting(true);
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
      final SirixIOException refusal = assertThrows(SirixIOException.class, () -> reloaded.getSlotAsByteArray(0),
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
   * chunk's materialized bit is set with a release store only AFTER {@code injector.inject} returns,
   * so a throw anywhere inside injection leaves the chunk pending and the next touch re-runs decode
   * and expansion from the wire bytes it still holds. A mid-loop throw is therefore recoverable
   * today, and this case cannot tell one from a pre-pass.
   * </p>
   *
   * <p>
   * What the pre-pass is still for, then, is two things this test does not carry: the refusal names
   * the PAGE and the tag rather than a slot (which {@link #anUnresolvedPageIsRefusedLoudly} does
   * carry, and which deleting the pre-pass does break), and it holds if a future expansion route ever
   * publishes a chunk before injecting into it. That is defence in depth, stated as such instead of
   * claimed as proven.
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
   * anything — the reads below would trip it. This is the case that would catch a well-meaning change
   * that "just looks the value up when it is missing".
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
   * A second transaction whose own anchors would have refused the tag is not a disagreement about the
   * bytes -- it is one party declining to answer. The page NAMES its dictionary and a rank-ordered
   * dictionary only appends, so any reader that can see that dictionary at all computes the same
   * values. First-writer-wins is therefore free of the usual race hazard, and this pins it so a later
   * "keep the newest" refactor has to argue with a test.
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
      final SirixIOException refusal = assertThrows(SirixIOException.class, () -> resolve(reloaded, dictionary),
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
   * §5: the copy-on-write refusal, driven by CONSTRUCTING the forbidden state rather than by noting
   * that no current path reaches it.
   *
   * <p>
   * {@code deepCopy()} expands its SOURCE before copying the segment, from a stack with no reader on
   * it. There is deliberately NO writer-side front: the one that existed ran inside
   * {@code serializeSnapshotWindowAsync}'s parallel {@code forEach}, so it would have put many
   * ForkJoinPool threads through a reader declared single-threaded — a front that converts a loud
   * impossible state into silent guard-count corruption is worse than no front. The refusal carries
   * the invariant instead.
   * </p>
   *
   * <p>
   * Today the state is unreachable in production: {@code lazyChunkedBody} is set only by the
   * deserializer, and every disk-to-intent-log path rebuilds pages through {@code newInstance}. That
   * is a POLICY, held up by an {@code assert} that is off in production plus the habit of building
   * intent-log pages fresh — and the trie lane creates a direct incentive to break that habit. So the
   * page here is genuinely lazy ({@code chunkCount() > 0}) and genuinely global-tagged, and the
   * refusal has to fire on it.
   * </p>
   */
  @Test
  @DisplayName("copy-on-write refuses a lazy, global-tagged, unresolved page")
  void copyOnWriteRefusesAnUnresolvedPage() {
    withRoundTrip(new FakeDictionary(), (original, reloaded) -> {
      assertTrue(reloaded.chunkCount() > 0,
          "the forbidden state has to be REAL: a page with no lazy body could never reach deepCopy's "
              + "expansion, so a refusal on it would prove nothing");
      assertTrue(reloaded.hasGlobalStringTags(), "and it has to carry a global tag");
      final IllegalStateException refusal = assertThrows(IllegalStateException.class, reloaded::deepCopy,
          "deepCopy expands its source and holds no reader; an unresolved page must not get through it");
      assertTrue(refusal.getMessage().contains("copy-on-write"),
          () -> "the refusal must name the route, not a slot: " + refusal.getMessage());
      assertTrue(refusal.getMessage().contains(String.valueOf(reloaded.getPageKey())),
          () -> "and the page: " + refusal.getMessage());
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

  /**
   * The length lane answers, and it answers the SAME lengths the values have.
   *
   * <p>
   * This is the property the derived elision plan depends on: it asks the region how long each elided
   * value was, and a global tag now has to be able to say. The check is against the resolved bytes
   * rather than against a constant, so a lane written in the wrong entry order — the easiest way to
   * get this wrong, since the ids are packed and the lengths are not — fails here.
   * </p>
   */
  @Test
  @DisplayName("a converted tag reports each entry's length, matching the resolved value")
  void theLengthLaneAgreesWithTheValues() {
    final FakeDictionary dictionary = new FakeDictionary();
    withRoundTrip(dictionary, (original, reloaded) -> {
      final RegionTable regions = reloaded.getRegionTable();
      final MemorySegment payload = regions.payload(RegionTable.KIND_STRING);
      final StringRegion.Header header = new StringRegion.Header();
      header.parseInto(payload);
      resolve(reloaded, dictionary);
      final ResolvedGlobalStrings table = reloaded.resolvedGlobalStrings();

      int checked = 0;
      for (int tagIndex = 0; tagIndex < header.parentDictSize; tagIndex++) {
        if (!header.tagGlobal[tagIndex]) {
          continue;
        }
        assertTrue(header.tagGlobalLengthWidth[tagIndex] > 0, "a converted tag must declare a length width");
        for (int entry = 0; entry < header.tagStringDictSize[tagIndex]; entry++) {
          final int length = StringRegion.decodeStringLength(payload, header, tagIndex, entry);
          assertEquals(table.value(tagIndex, header.parentDict[tagIndex], entry).length, length,
              "entry " + entry + "'s recorded length must be its value's length");
          checked++;
        }
      }
      assertEquals(VALUES.length, checked, "every distinct value on the page must carry a length");
    });
  }

  /**
   * A global tag still refuses to hand back a byte OFFSET — it has no bytes.
   *
   * <p>
   * The pair matters: adding the length lane made {@code decodeStringLength} answer, and it would be
   * an easy and invisible mistake to let {@code decodeStringOffset} answer too, pointing into the id
   * table. That returns plausible bytes, which is the failure this format cannot detect afterwards.
   * </p>
   */
  @Test
  @DisplayName("a converted tag answers lengths but still refuses offsets")
  void lengthsAnswerButOffsetsDoNot() {
    withRoundTrip(new FakeDictionary(), (original, reloaded) -> {
      final MemorySegment payload = reloaded.getRegionTable().payload(RegionTable.KIND_STRING);
      final StringRegion.Header header = new StringRegion.Header();
      header.parseInto(payload);
      int globalTag = -1;
      for (int t = 0; t < header.parentDictSize; t++) {
        if (header.tagGlobal[t]) {
          globalTag = t;
          break;
        }
      }
      assertTrue(globalTag >= 0, "the lane must have engaged");
      final int tag = globalTag;
      assertTrue(StringRegion.decodeStringLength(payload, header, tag, 0) > 0, "lengths must be readable");
      assertThrows(IllegalStateException.class, () -> StringRegion.decodeStringOffset(payload, header, tag, 0),
          "a tag that stores no bytes must not hand back an offset into its id table");
      assertFalse(StringRegion.isEntryCompressed(payload, header, tag, 0),
          "a converted tag has no FSST entry, and must not read an id's sign bit to answer");
    });
  }

  /**
   * The injector refuses a dictionary whose value disagrees with the length the page recorded.
   *
   * <p>
   * The provenance check, driven from the read side. A resolver that answers from a different
   * generation, a reused key or a rebuilt ranking returns a plausible string — this is the cheap
   * check that catches the whole family, and it fires before a byte reaches the heap.
   * </p>
   */
  @Test
  @DisplayName("a dictionary that returns a differently-sized value is refused, naming both sizes")
  void aValueOfTheWrongLengthIsRefused() {
    final FakeDictionary honest = new FakeDictionary();
    withRoundTrip(honest, (original, reloaded) -> {
      final FakeDictionary lying = new FakeDictionary();
      lying.substitute = true;
      final SirixIOException refusal = assertThrows(SirixIOException.class, () -> {
        resolve(reloaded, lying);
        reloaded.getSlotAsByteArray(0);
      }, "a value whose length disagrees with the one the page elided must be refused");
      assertTrue(refusal.getMessage().contains("disagree") || refusal.getMessage().contains("mismatch"),
          () -> "the refusal must say the page and the dictionary disagree: " + refusal.getMessage());
    });
  }

  /**
   * A converted page carries NO dictionary sketch.
   *
   * <p>
   * The sketch hashes each dictionary entry's stored BYTES by walking the tag's length table at
   * {@code tagLengthWidth}, which the parse pins to 4 for a global tag. A converted tag has no length
   * table and no bytes — that walk reads its packed id table instead.
   * </p>
   *
   * <p>
   * <b>What this test does NOT prove, established by mutation and then by direct measurement.</b>
   * Deleting the guard leaves this case green, and I chased that rather than patching the assertion.
   * The reason is that the corrupted walk is self-limiting: four-byte reads over a packed id table
   * (19 bytes for a 25-bit id space at ~6 entries) yield values in the millions, which trip
   * {@code storedLen > stringPayloadLength - off} and return null — the same outcome the guard
   * produces. Measured at three payload sizes, 19 B, 292 B and 29 KB: null in every one, guard or no
   * guard.
   * </p>
   *
   * <p>
   * So the guard is not repairing a demonstrated row-loss bug; it is replacing an outcome that
   * happens to be right with one that is right by construction. Worth having — "no sketch" should not
   * depend on whether some ids read as implausible lengths — but claimed as that and no more.
   * </p>
   */
  /**
   * A dictionary that GROWS mid-encode must not tear the page.
   *
   * <p>
   * The entry count fixes three things that have to agree exactly — the width the ids are packed at,
   * the count written into the header, and the width the parser DERIVES from that count — and the
   * encoder computes them in three separate passes over the tag. During a load the dictionary is
   * still being interned into on other threads, so a count read three times can be three different
   * numbers, and the page would then declare one width and pack another. Nothing fails: every id of
   * that tag simply reads back shifted.
   * </p>
   *
   * <p>
   * The fixture returns a DIFFERENT count on every read, which is the strongest form of the hazard,
   * and the test pins the fix directly: the encoder must ask exactly ONCE per global tag. A second
   * read is not a slow path, it is the bug — the page would declare the width from one answer and
   * pack the ids at another. Asserting the call COUNT rather than only the round trip is what makes
   * this fail if the snapshot is ever unwound, since with only one tag and small ids several of the
   * old three reads happened to agree often enough to round-trip anyway.
   * </p>
   */
  @Test
  @DisplayName("the encoder reads a dictionary's entry count exactly once per tag")
  void aGrowingDictionaryDoesNotTearThePage() {
    final FakeDictionary dictionary = new FakeDictionary();
    dictionary.growing = true;
    withRoundTrip(dictionary, (original, reloaded) -> {
      assertTrue(reloaded.hasGlobalStringTags(), "the lane must have engaged");
      assertEquals(1, dictionary.growth,
          "the entry count fixes the packing width, the declared count and the parser's derived width; "
              + "reading it more than once lets a growing dictionary make them disagree");
      dictionary.growing = false;
      resolve(reloaded, dictionary);
      assertSlotsMatch(original, reloaded);
    });
  }

  /**
   * A page carrying BOTH converted strings and fused NUMBERS round-trips every slot.
   *
   * <p>
   * The 1M gate found what the string-only fixture could not: a converted database fails a subtree
   * serialization with {@code AssertionError: Type not known} out of {@code deserializeNumber} — a
   * fused NUMBER record's payload type byte is wrong — while four arms without the lane serialize
   * byte-identically. Every other case in this class writes strings and only strings, so the whole
   * value-elision loop was only ever exercised with one kind on the page. A real record page carries
   * numbers, strings and booleans in one elision section, sharing the slot/type/width arrays.
   * </p>
   *
   * <p>
   * <b>It does NOT reproduce the gate's failure, and that is recorded here so nobody reads it as
   * covering it.</b> It was written as the reproduction hypothesis — string-only fixtures never
   * exercised the shared slot/type/width arrays with two kinds — and it passes. So the corruption
   * needs something this page does not have: it appears only a few thousand records into a real load,
   * on pages that also carry name-key elision, overflow carriers, many more tags, and values long
   * enough to matter. The next attempt should start from the loaded database, not from here.
   * </p>
   *
   * <p>
   * The case earns its place anyway: mixed kinds on one converted page were genuinely untested, and
   * that gap was real whether or not it is this bug.
   * </p>
   */
  @Test
  @DisplayName("a page mixing converted strings with fused numbers round-trips every slot")
  void aMixedKindPageRoundTrips() {
    final FakeDictionary dictionary = new FakeDictionary();
    final ResourceConfiguration config = new ResourceConfiguration.Builder("trieLaneMixed").build();
    final KeyValueLeafPage original = new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1,
        arena.allocate(MemorySegmentAllocator.SIXTYFOUR_KB), null);
    KeyValueLeafPage reloaded = null;
    try {
      original.setGlobalStringDictionaries(dictionary);
      // Interleaved on purpose: the elision section walks slots in order, and a bug that shifts the
      // per-slot type or width arrays only shows when the kinds alternate.
      for (int slot = 0; slot < SLOTS; slot++) {
        if ((slot & 1) == 0) {
          writeString(original, slot, 100, TAG, VALUES[(slot / 2) % VALUES.length]);
        } else {
          writeNumber(original, slot, 101, TAG + 1, slot * 7L);
        }
      }

      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, original, SerializationType.DATA);
      final BytesIn<?> source = sink.bytesForRead();
      source.readByte();
      reloaded = (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePageLazily(config, source,
          SerializationType.DATA, null);

      assertTrue(reloaded.hasGlobalStringTags(),
          "the mixed page must still convert its string tag, or this proves nothing about the lane");
      resolve(reloaded, dictionary);
      for (int slot = 0; slot < SLOTS; slot++) {
        assertArrayEquals(original.getSlotAsByteArray(slot), reloaded.getSlotAsByteArray(slot),
            "slot " + slot + " (" + ((slot & 1) == 0
                ? "converted string"
                : "fused number") + ") did not survive the round trip");
      }
    } finally {
      if (reloaded != null) {
        reloaded.close();
      }
      original.close();
    }
  }

  @Test
  @DisplayName("a converted page emits no dictionary sketch")
  void aConvertedPageHasNoSketch() {
    withRoundTrip(new FakeDictionary(), (original, reloaded) -> {
      assertTrue(reloaded.hasGlobalStringTags(), "the lane must have engaged");
      final MemorySegment sketch = reloaded.getRegionTable().payload(RegionTable.KIND_STRING_DICT_SKETCH);
      assertTrue(sketch == null || sketch.byteSize() == 0,
          "a page whose values live in a dictionary cannot describe them in a byte sketch; emitting one "
              + "would let the page rule itself out of literals it holds");
    });
  }

  @Test
  @DisplayName("a tag index carrying a different tag value is refused, not read")
  void aShiftedTagIndexIsRefused() {
    final ResolvedGlobalStrings table =
        ResolvedGlobalStrings.forTags(2)
                             .tag(0, TAG, new int[] {1}, new byte[][] {"alpha".getBytes(StandardCharsets.UTF_8)})
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
        () -> ResolvedGlobalStrings.forTags(1).tag(0, TAG, new int[] {1, 2}, new byte[][] {new byte[1], null}),
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
      reloaded = (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePageLazily(config, source,
          SerializationType.DATA, null);
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

  private static void writeNumber(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final long pathNodeKey, final long value) {
    final ObjectNamedNumberNode node = new ObjectNamedNumberNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, pathNodeKey, 0,
        0, 0L, value, HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1)));
  }

  private static void writeString(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final long pathNodeKey, final String value) {
    final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, pathNodeKey, 0,
        0, 0L, value.getBytes(StandardCharsets.UTF_8), HASH_FN, (byte[]) null, false, null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1)));
  }

  /**
   * A resource-wide dictionary, stubbed: rank-ordered ids, a live count that can be made to shrink, a
   * record of the ids it was asked for, and a poison switch.
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

    /** Once true, every value comes back a different length — a wrong-generation dictionary. */
    private boolean substitute;

    /** Once true, the live entry count grows on every read — a dictionary still being loaded into. */
    private boolean growing;

    private int growth;

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
      if (tag != TAG || !accepts(tag, dictionaryKey, recordedEntryCount)) {
        return null;
      }
      final byte[] value = values.get(id);
      return value == null || !substitute
          ? value
          : (new String(value, StandardCharsets.UTF_8) + "-from-another-generation").getBytes(StandardCharsets.UTF_8);
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
      return growing
          ? ids.size() + growth++
          : ids.size();
    }
  }
}
