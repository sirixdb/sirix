/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.json.ObjectNamedStringNode;
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
import java.util.ArrayList;
import java.util.List;

import static io.sirix.cache.MemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which side of the TEMPORAL lane's kill switch a READ can reach.
 *
 * <p>
 * {@link StringRegion#temporalLaneEnabled()} documents two halves that pull in opposite directions,
 * and both of them are read-path claims, so both are pinned here rather than in the encoder suite:
 * </p>
 *
 * <ul>
 * <li><b>Decoding is unconditional.</b> A region already on the page is read from the tag flags the
 * region itself carries, so a page written WITH the lane stays readable after the switch goes off.
 * That is what makes the switch safe to flip on a database that already exists.</li>
 * <li><b>A region that has to be REBUILT is an encode.</b>
 * {@link KeyValueLeafPage#getStringRegionHeader()} falls back to deriving the region from the
 * slotted page when none was persisted — the state a versioning-reconstructed or merged page starts
 * in — and that derive consults the switch like any other encode. So the rebuilt region's layout
 * follows the CURRENT setting, not the writer's.</li>
 * </ul>
 *
 * <p>
 * The fixture is a page carrying slots but no persisted region, which is exactly the state the
 * fallback exists for, driven through the public accessors instead of through a versioning
 * reconstruction so the two routes differ in the switch alone.
 * </p>
 *
 * <p>
 * The consequence is latency, never answers: every case below asserts the SAME twelve timestamps
 * come back whichever route ran, and the routes are proven distinct by the region's own tag flag.
 * </p>
 */
@DisplayName("string region derived at read time, temporal lane")
final class StringRegionDerivedTemporalLaneTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  /** Twelve rows of nineteen-byte timestamps, all distinct, all on the codec's canonical shape. */
  private static final int ROWS = 12;

  /** The nameKey the timestamps hang under, i.e. the region tag they group into. */
  private static final int TS_NAME_KEY = 11;

  private Arena arena;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    StringRegion.clearTemporalLaneOverride();
  }

  @AfterEach
  void tearDown() {
    StringRegion.clearTemporalLaneOverride();
    if (arena != null) {
      arena.close();
    }
  }

  /** {@code "2013-07-15 12:00:SS"} — all nineteen bytes always, which is what the codec accepts. */
  private static String timestamp(final int row) {
    return "2013-07-15 12:00:" + (row < 10
        ? "0"
        : "") + row;
  }

  private KeyValueLeafPage pageWithTimestampSlots() {
    final KeyValueLeafPage page = new KeyValueLeafPage(0, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("testResource").build(), 1, arena.allocate(SIXTYFOUR_KB), null);
    for (int row = 0; row < ROWS; row++) {
      writeObjectNamedString(page, row, TS_NAME_KEY, timestamp(row));
    }
    // The state the fallback is for: slots on the page, nothing in the region table.
    assertNull(page.getStringRegionPayload(), "the fixture must start with NO persisted region");
    return page;
  }

  /** Write a fused {@link ObjectNamedStringNode} at the slot derived from {@code nodeKey}. */
  private static void writeObjectNamedString(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final String value) {
    final int slot = (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
    final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey,
        /* pathNodeKey */ -1L, /* previousRevision */ 0, /* lastModifiedRevision */ 0, /* hash */ 0L,
        value.getBytes(StandardCharsets.UTF_8), HASH_FN, (byte[]) null, /* isCompressed */ false,
        /* fsstSymbolTable */ null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);
  }

  /**
   * Every value of the timestamp tag, read through whichever route that tag is actually on.
   *
   * <p>
   * A temporal tag hands back no offset at all — its values are unpacked and rendered — so reading
   * both routes through one helper is what makes "the answers are the same" an assertion about the
   * values rather than about the layout.
   * </p>
   */
  private static List<String> valuesOfTheTimestampTag(final KeyValueLeafPage page, final boolean expectTemporal) {
    final StringRegion.Header header = page.getStringRegionHeader();
    assertNotNull(header, "the page must have a region to read");
    final int tag = StringRegion.lookupTag(header, TS_NAME_KEY);
    assertTrue(tag >= 0, "the timestamp tag must be in the region");
    assertEquals(expectTemporal, header.tagTemporal[tag], expectTemporal
        ? "expected the tag ON the temporal lane, or this case proves nothing"
        : "expected the tag on its BYTES, or this case proves nothing");
    final MemorySegment payload = page.getStringRegionPayload();
    assertNotNull(payload);
    final int entries = header.tagStringDictSize[tag];
    final List<String> values = new ArrayList<>(entries);
    if (expectTemporal) {
      final byte[] scratch = new byte[StringRegion.temporalValueLength(header, tag)];
      for (int entry = 0; entry < entries; entry++) {
        final int n = StringRegion.temporalValueAt(payload, header, tag, entry, scratch, 0);
        values.add(new String(scratch, 0, n, StandardCharsets.UTF_8));
      }
    } else {
      for (int entry = 0; entry < entries; entry++) {
        final int off = StringRegion.decodeStringOffset(payload, header, tag, entry);
        final int len = StringRegion.decodeStringLength(payload, header, tag, entry);
        values.add(new String(payload.asSlice(off, len).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8));
      }
    }
    return values;
  }

  private static List<String> expectedValues() {
    final List<String> expected = new ArrayList<>(ROWS);
    for (int row = 0; row < ROWS; row++) {
      expected.add(timestamp(row));
    }
    return expected;
  }

  @Test
  @DisplayName("a rebuild under a DISARMED switch keeps the tag on its bytes")
  void rebuildUnderTheDisarmedSwitchKeepsTheBytes() {
    final KeyValueLeafPage page = pageWithTimestampSlots();
    try {
      StringRegion.setTemporalLaneEnabled(false);
      assertEquals(expectedValues(), valuesOfTheTimestampTag(page, false));
    } finally {
      page.close();
    }
  }

  @Test
  @DisplayName("THE POINT: a rebuild under an ARMED switch gives the page a tag it never had persisted")
  void rebuildUnderTheArmedSwitchTakesTheLane() {
    final KeyValueLeafPage page = pageWithTimestampSlots();
    try {
      // Nothing on the page says "temporal": the region is being derived from the slots for the
      // first time, and only the CURRENT setting decides which layout that derive writes.
      StringRegion.setTemporalLaneEnabled(true);
      assertEquals(expectedValues(), valuesOfTheTimestampTag(page, true),
          "the lane must change the route, never the values");
    } finally {
      page.close();
    }
  }

  @Test
  @DisplayName("and it is the same page: the switch alone decides which region the rebuild installs")
  void theSwitchAloneDecidesWhatTheRebuildInstalls() {
    final long armed;
    final long disarmed;
    final KeyValueLeafPage onTheLane = pageWithTimestampSlots();
    try {
      StringRegion.setTemporalLaneEnabled(true);
      assertEquals(expectedValues(), valuesOfTheTimestampTag(onTheLane, true));
      armed = onTheLane.getStringRegionPayload().byteSize();
    } finally {
      onTheLane.close();
    }
    final KeyValueLeafPage onItsBytes = pageWithTimestampSlots();
    try {
      StringRegion.setTemporalLaneEnabled(false);
      assertEquals(expectedValues(), valuesOfTheTimestampTag(onItsBytes, false));
      disarmed = onItsBytes.getStringRegionPayload().byteSize();
    } finally {
      onItsBytes.close();
    }
    // Twelve nineteen-byte timestamps is 228 bytes of text the packed lane does not write; asserting
    // a real fraction of it keeps a rebuild that merely reordered something from passing.
    assertTrue(armed < disarmed - 150,
        "the two rebuilds must differ by the text the lane removed, got " + (disarmed - armed) + " bytes");
  }

  @Test
  @DisplayName("DECODING is unconditional: a persisted temporal region reads back with the switch OFF")
  void aPersistedTemporalRegionReadsBackWithTheSwitchOff() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("testResource").build();
    final KeyValueLeafPage page = pageWithTimestampSlots();
    KeyValueLeafPage deserialized = null;
    try {
      StringRegion.setTemporalLaneEnabled(true);
      assertEquals(expectedValues(), valuesOfTheTimestampTag(page, true));

      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      final BytesIn<?> source = sink.bytesForRead();
      source.readByte(); // pageKind id

      // Everything past this line runs on a JVM that would never WRITE a temporal tag.
      StringRegion.setTemporalLaneEnabled(false);
      deserialized =
          (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
      assertEquals(expectedValues(), valuesOfTheTimestampTag(deserialized, true),
          "a page written with the lane must stay readable when the switch goes off");
      for (int row = 0; row < ROWS; row++) {
        final int slot = (int) (row & (Constants.NDP_NODE_COUNT - 1));
        assertFalse(page.getSlotAsByteArray(slot) == null, "the fixture must have written slot " + slot);
        assertEquals(new String(page.getSlotAsByteArray(slot), StandardCharsets.UTF_8),
            new String(deserialized.getSlotAsByteArray(slot), StandardCharsets.UTF_8),
            "record " + row + " must come back byte for byte");
      }
    } finally {
      page.close();
      if (deserialized != null) {
        deserialized.close();
      }
    }
  }
}
