/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.StructuralKeyColumnCodec;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Witness for per-tag string-region completeness.
 *
 * <p>
 * A fused string past the inline record cap becomes an overflow descriptor. Completeness used to be
 * decided for the whole PAGE: one such descriptor and the string region was not written at all, so
 * every other field's strings stayed inline in the record heap with no dictionary, no membership
 * sketch and no column to scan. Measured on a 1M-row ClickBench load that cost 11.8 percent of
 * document leaves their string region.
 *
 * <p>
 * Completeness is per TAG now: the descriptor's own tag leaves the region — named in the header's
 * suppressed-tag list, its slots keeping their values inline — and every other tag is published and
 * elided exactly as before. The mutation each test names is the kill switch
 * {@code -Dsirix.page.stringRegion.perTagCompleteness=false}, which restores the old rule; every
 * assertion here fails under it, and the byte pin below proves the restoration is exact.
 */
@DisplayName("String region per-tag completeness")
final class StringRegionPerTagCompletenessTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  /** Short-string fields on the fixture page; each is its own tag. */
  private static final int INLINE_FIELDS = 30;
  /** First nameKey of the short-string fields. */
  private static final int FIRST_INLINE_NAME_KEY = 200;
  /** The field whose value fits the RAISED cap but not the old one. */
  private static final int WIDE_NAME_KEY = 300;
  /** The field whose value overflows either way — the tag that must be suppressed. */
  private static final int OVERSIZED_NAME_KEY = 400;
  /** Value length that fits the raised cap (record is 929 B) but not the old 512-byte one. */
  private static final int WIDE_VALUE_LENGTH = 900;
  /** Value length no cap can hold inline. */
  private static final int OVERSIZED_VALUE_LENGTH = 2_000;
  /** First path node key of the short-string fields on the path-tagged fixture. */
  private static final int FIRST_PATH_NODE_KEY = 900;
  /** Path node key of the oversized field on the path-tagged fixture. */
  private static final int OVERSIZED_PATH_NODE_KEY = 999;

  /**
   * The fixture page's exact wire bytes as an encoder built from {@code HEAD} (before per-tag
   * completeness) produced them, with the all-or-nothing rule in force.
   *
   * <p>
   * The fixture deliberately holds no record in {@code (512, 1023]}, so the fused-cap raise that
   * ships with this change cannot move these bytes and the only thing the comparison can catch is a
   * completeness rule that fails to restore itself. Recorded by compiling {@code git show HEAD:} of
   * every file this change touches into a separate output directory and serializing the same fixture
   * there.
   */
  private static final String GOLDEN_KILL_SWITCH_PAGE =
      "0100000001000000010000000000000000010000001f00ec070000ec07000001000000000000000000ffffff7f000000"
          + "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
          + "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
          + "000000000000000000000000000000000000000000000000001f0000009706000001120b000000db00000003fda90e2e"
          + "0df202002f0e32020013ef047232090001020305060708100001000e9f0301001f0000001f0201000b1b324900af4c66"
          + "69656c642d302d7601000a0f3700041f313700231f323700231f333700231f343700231f353700231f363700231f3737"
          + "00231f383700231f3937001c124e26022f31303800241f317000241f32a800241f33e000241f341801241f355001241f"
          + "368801241f37c001241f38f801240f3002241f323002241f323002241f323002241f323002241f323002241f32300224"
          + "1f323002241f323002241f323002241f3230021c2002000100000004031e010000ac000000fd9e02ff751fc8000000c9"
          + "000000ca000000cb000000cc000000cd000000ce000000cf000000d0000000d1000000d2000000d3000000d4000000d5"
          + "000000d6000000d7000000d8000000d9000000da000000db000000dc000000dd000000de000000df000000e0000000e1"
          + "000000e2000000e3000000e4000000e5000000900100001f00ffffff7f00010069f00f0102030405060708090a0b0c0d"
          + "0e0f101112131415161718191a1b1c1d1e04000000004001000000f1ffffffffffffff00000000";

  private Arena arena;
  private boolean perTagCompletenessBefore;
  private boolean derivedElisionBefore;
  private boolean siblingColumnsBefore;
  private boolean runLengthLaneBefore;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    perTagCompletenessBefore = PageKind.STRING_REGION_PER_TAG_COMPLETENESS;
    derivedElisionBefore = PageKind.DERIVED_ELISION_SECTIONS;
    siblingColumnsBefore = PageKind.SIBLING_KEY_COLUMNS_ENABLED;
    runLengthLaneBefore = StructuralKeyColumnCodec.RUN_LENGTH_LANE_ENABLED;
  }

  @AfterEach
  void tearDown() {
    PageKind.STRING_REGION_PER_TAG_COMPLETENESS = perTagCompletenessBefore;
    NumberRegion.setPerTagWidthEnabled(true);
    StringRegion.setPlainLaneEnabled(true);
    NumberRegion.setExternalHeaderEnabled(true);
    PageKind.DERIVED_ELISION_SECTIONS = derivedElisionBefore;
    PageKind.SIBLING_KEY_COLUMNS_ENABLED = siblingColumnsBefore;
    StructuralKeyColumnCodec.RUN_LENGTH_LANE_ENABLED = runLengthLaneBefore;
    if (arena != null) {
      arena.close();
    }
  }

  @Test
  @DisplayName("one oversized field suppresses its own tag and no other")
  void oversizedFieldSuppressesOnlyItsOwnTag() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage deserialized = null;
    try {
      fillFixture(page);
      deserialized = roundTrip(config, page);
      assertTrue(page.isFusedObjectNamedStringOverflowDescriptor(INLINE_FIELDS + 1),
          "the fixture must actually produce an overflow descriptor, otherwise it witnesses nothing");

      final StringRegion.Header header = deserialized.getStringRegionHeader();
      assertNotNull(header, "the page keeps its string region; under the old rule it had none");
      // Every tag but the oversized one is published, values and all.
      assertEquals(INLINE_FIELDS + 1, header.parentDictSize, "one tag per published field");
      assertEquals(INLINE_FIELDS + 1, header.count, "one value per published field");
      for (int f = 0; f < INLINE_FIELDS; f++) {
        final int nameKey = FIRST_INLINE_NAME_KEY + f;
        assertTrue(StringRegion.lookupTag(header, nameKey) >= 0, "tag " + nameKey + " must be published");
        assertFalse(StringRegion.isTagSuppressed(header, nameKey), "tag " + nameKey + " is complete");
      }
      assertTrue(StringRegion.lookupTag(header, WIDE_NAME_KEY) >= 0,
          "a value that fits the raised cap is an ordinary inline string and joins the region");

      // ...and the oversized one is named as absent rather than silently missing.
      assertEquals(1, header.suppressedTagCount, "exactly the oversized field's tag left the region");
      assertEquals(OVERSIZED_NAME_KEY, header.suppressedTags[0]);
      assertEquals(-1, StringRegion.lookupTag(header, OVERSIZED_NAME_KEY),
          "a suppressed tag carries no values, so it is not in the tag dictionary");
      assertTrue(StringRegion.isTagSuppressed(header, OVERSIZED_NAME_KEY),
          "and a reader can tell that from 'this page has no such field'");
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("region-only reads decode every complete tag's value")
  void regionOnlyReadsServeTheCompleteTags() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage deserialized = null;
    try {
      fillFixture(page);
      deserialized = roundTrip(config, page);

      final StringRegion.Header header = deserialized.getStringRegionHeader();
      assertNotNull(header);
      final MemorySegment payload = deserialized.getStringRegionPayload();
      assertNotNull(payload);
      for (int f = 0; f < INLINE_FIELDS; f++) {
        final int nameKey = FIRST_INLINE_NAME_KEY + f;
        final int tag = StringRegion.lookupTag(header, nameKey);
        assertEquals(1, header.tagCount[tag], "tag " + nameKey + " holds exactly its one value");
        final int dictId = StringRegion.decodeDictIdAt(payload, header, header.tagStart[tag]);
        final int offset = StringRegion.decodeStringOffset(payload, header, tag, dictId);
        final int length = StringRegion.decodeStringLength(payload, header, tag, dictId);
        assertEquals(inlineValue(f),
            new String(payload.asSlice(offset, length).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8),
            "region-only read of tag " + nameKey);
      }
      final int wideTag = StringRegion.lookupTag(header, WIDE_NAME_KEY);
      final int wideDictId = StringRegion.decodeDictIdAt(payload, header, header.tagStart[wideTag]);
      assertEquals(WIDE_VALUE_LENGTH, StringRegion.decodeStringLength(payload, header, wideTag, wideDictId),
          "the value that fits the raised cap is served from the region in full");

      // The membership sketch proves absence for the whole PAGE, so a page holding a value the
      // dictionary cannot contain must not publish one: the suppressed tag's strings — and the
      // out-of-line value itself, which no region could ever hold — are on this page.
      assertNull(deserialized.regionPayload(RegionTable.KIND_STRING_DICT_SKETCH),
          "a page with a suppressed tag forfeits its dictionary sketch");
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("every slot round-trips byte-identically, the descriptor included")
  void everySlotRoundTripsThroughValueElision() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage deserialized = null;
    try {
      fillFixture(page);
      deserialized = roundTrip(config, page);
      // The published tags' values were stripped from the heap and re-injected from the region; the
      // suppressed tag's slot kept its own. Both have to come back as the writer left them.
      for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
        assertArrayEquals(page.getSlotAsByteArray(slot), deserialized.getSlotAsByteArray(slot),
            "slot " + slot + " did not survive the round trip");
      }
      assertNotNull(deserialized.getSlotAsByteArray(INLINE_FIELDS + 1),
          "the overflow descriptor is a record like any other and must be on the page");
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("the derive-on-read path suppresses the tag instead of refusing the page")
  void derivedRegionSuppressesTheTagToo() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage deserialized = null;
    try {
      fillFixture(page);
      // Serialize to turn the oversized record into a descriptor, then read the page back and drop
      // its written region: what is left is the shape a page reconstructed from a versioned fragment
      // has, and the derive-on-read builder is what has to answer for it.
      deserialized = roundTrip(config, page);
      deserialized.invalidateStringRegion();

      final StringRegion.Header header = deserialized.getStringRegionHeader();
      assertNotNull(header, "the derive path used to memoize the whole page as not derivable");
      assertTrue(StringRegion.lookupTag(header, FIRST_INLINE_NAME_KEY) >= 0);
      assertEquals(1, header.suppressedTagCount);
      assertEquals(OVERSIZED_NAME_KEY, header.suppressedTags[0]);
      assertNull(deserialized.regionPayload(RegionTable.KIND_STRING_DICT_SKETCH),
          "the derived region forfeits its sketch for the same reason the written one does");
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("a path-tagged region suppresses the path tag, in its own key space")
  void pathTaggedRegionSuppressesThePathTag() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage deserialized = null;
    try {
      // Real path node keys make the region path-tagged, which is the tagging every path-scoped scan
      // needs. A suppression recorded only in nameKey space would leave that region claiming a
      // completeness it does not have, so it has to be expressed in whichever space wins.
      for (int f = 0; f < INLINE_FIELDS; f++) {
        writeString(page, f, FIRST_INLINE_NAME_KEY + f, FIRST_PATH_NODE_KEY + f, inlineValue(f));
      }
      writeString(page, INLINE_FIELDS, OVERSIZED_NAME_KEY, OVERSIZED_PATH_NODE_KEY, "X".repeat(OVERSIZED_VALUE_LENGTH));
      deserialized = roundTrip(config, page);

      final StringRegion.Header header = deserialized.getStringRegionHeader();
      assertNotNull(header);
      assertEquals(StringRegion.TAG_KIND_PATH_NODE, header.tagKind, "the fixture must produce a path-tagged region");
      assertTrue(StringRegion.lookupTag(header, FIRST_PATH_NODE_KEY) >= 0);
      assertEquals(1, header.suppressedTagCount);
      assertEquals(OVERSIZED_PATH_NODE_KEY, header.suppressedTags[0],
          "the suppressed tag is named as a path node key, not as a name key");
      assertTrue(StringRegion.isTagSuppressed(header, OVERSIZED_PATH_NODE_KEY));
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("a page whose every string tag is suppressed publishes no region at all")
  void allTagsSuppressedPublishesNothing() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage deserialized = null;
    try {
      // One tag, an inline value and an oversized one. The inline value IS staged and then dropped
      // with its tag, so this is the encoder's own "nothing survived" path, not the writer's
      // "nothing was staged" one. A header naming only absences would cost bytes on every read and
      // tell a reader nothing an absent region does not already tell it.
      writeString(page, 0, OVERSIZED_NAME_KEY, inlineValue(0));
      writeString(page, 1, OVERSIZED_NAME_KEY, "X".repeat(OVERSIZED_VALUE_LENGTH));
      deserialized = roundTrip(config, page);

      assertNull(deserialized.getStringRegionPayload(), "nothing survives suppression, so nothing is written");
      assertNull(deserialized.regionPayload(RegionTable.KIND_STRING_DICT_SKETCH));
      for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
        assertArrayEquals(page.getSlotAsByteArray(slot), deserialized.getSlotAsByteArray(slot),
            "slot " + slot + " did not survive the round trip");
      }
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("a page whose only string is oversized stages nothing and writes nothing")
  void nothingStagedPublishesNothing() {
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage deserialized = null;
    try {
      writeString(page, 0, OVERSIZED_NAME_KEY, "X".repeat(OVERSIZED_VALUE_LENGTH));
      deserialized = roundTrip(config, page);
      assertNull(deserialized.getStringRegionPayload(),
          "a suppressed tag alone is not worth a region; the page reads exactly as it did before");
      assertArrayEquals(page.getSlotAsByteArray(0), deserialized.getSlotAsByteArray(0));
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("the kill switch restores the all-or-nothing rule")
  void killSwitchDropsTheWholeRegion() {
    PageKind.STRING_REGION_PER_TAG_COMPLETENESS = false;
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage deserialized = null;
    try {
      fillFixture(page);
      deserialized = roundTrip(config, page);
      assertNull(deserialized.getStringRegionHeader(),
          "with the old rule one descriptor takes the whole page's string region");
      for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
        assertArrayEquals(page.getSlotAsByteArray(slot), deserialized.getSlotAsByteArray(slot),
            "slot " + slot + " did not survive the round trip");
      }
    } finally {
      if (deserialized != null) {
        deserialized.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("the kill switch reproduces the pre-change bytes exactly")
  void killSwitchIsByteIdenticalToHead() {
    PageKind.STRING_REGION_PER_TAG_COMPLETENESS = false;
    // The pin is of HEAD's bytes, so every lever landed since then has to be off for it to mean what
    // its name says — the derived elision sections and the structural columns included.
    PageKind.DERIVED_ELISION_SECTIONS = false;
    PageKind.SIBLING_KEY_COLUMNS_ENABLED = false;
    StructuralKeyColumnCodec.RUN_LENGTH_LANE_ENABLED = false;
    NumberRegion.setPerTagWidthEnabled(false); // B5-c's per-tag number region, landed after the pin
    StringRegion.setPlainLaneEnabled(false); // B5-c's string-region framing, landed after the pin
    NumberRegion.setExternalHeaderEnabled(false); // B5-c's folded per-tag directory, landed after the pin
    final ResourceConfiguration config = newConfig();
    final KeyValueLeafPage page = newPage(config);
    try {
      // Same shape as the fixture above minus the wide field, so no record lands in the window the
      // cap raise opened and the comparison isolates the completeness rule.
      for (int f = 0; f < INLINE_FIELDS; f++) {
        writeString(page, f, FIRST_INLINE_NAME_KEY + f, inlineValue(f));
      }
      writeString(page, INLINE_FIELDS, OVERSIZED_NAME_KEY, "X".repeat(OVERSIZED_VALUE_LENGTH));
      // Golden bytes require the exhaustive pick-smallest codec choice; an earlier test in the same
      // fork may have elected a different sticky winner. Same reset GoldenCompositePageTest uses.
      PageKind.resetStickyCodecElectionForCurrentThread();
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      assertEquals(GOLDEN_KILL_SWITCH_PAGE, hex(sink.toByteArray()),
          "the kill switch must reproduce the pre-change encoding byte for byte");
    } finally {
      page.close();
    }
  }

  // ───────────────────────────────────────────────────────────────── fixture helpers

  /**
   * Thirty ordinary string fields, one field whose value only the raised cap keeps inline, and one
   * that overflows whatever the cap is.
   */
  private void fillFixture(final KeyValueLeafPage page) {
    for (int f = 0; f < INLINE_FIELDS; f++) {
      writeString(page, f, FIRST_INLINE_NAME_KEY + f, inlineValue(f));
    }
    writeString(page, INLINE_FIELDS, WIDE_NAME_KEY, "N".repeat(WIDE_VALUE_LENGTH));
    writeString(page, INLINE_FIELDS + 1, OVERSIZED_NAME_KEY, "X".repeat(OVERSIZED_VALUE_LENGTH));
  }

  private static String inlineValue(final int field) {
    return "field-" + field + "-" + "v".repeat(30);
  }

  private static ResourceConfiguration newConfig() {
    return new ResourceConfiguration.Builder("stringRegionPerTagCompleteness").build();
  }

  private KeyValueLeafPage newPage(final ResourceConfiguration config) {
    return new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, arena.allocate(MemorySegmentAllocator.SIXTYFOUR_KB),
        null);
  }

  /**
   * Serialize and read the page back. An oversized record only becomes an inline overflow descriptor
   * during serialization, so the fixture is only complete afterwards; the caller closes both pages.
   */
  private static KeyValueLeafPage roundTrip(final ResourceConfiguration config, final KeyValueLeafPage page) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
    final BytesIn<?> source = sink.bytesForRead();
    source.readByte();
    return (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
  }

  private static void writeString(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final String value) {
    writeString(page, nodeKey, nameKey, -1L, value);
  }

  private static void writeString(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final long pathNodeKey, final String value) {
    final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, pathNodeKey, 0,
        0, 0L, value.getBytes(StandardCharsets.UTF_8), HASH_FN, (byte[]) null, false, null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1)));
  }

  private static String hex(final byte[] bytes) {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (final byte b : bytes) {
      sb.append(Character.forDigit((b >>> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }
}
