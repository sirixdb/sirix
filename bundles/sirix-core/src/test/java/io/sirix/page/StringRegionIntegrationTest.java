package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.page.pax.StringRegion;
import io.sirix.page.pax.StringRegionEncoderTestAccess;
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
import java.util.Arrays;
import java.util.Objects;

import static io.sirix.cache.MemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test for {@link KeyValueLeafPage}'s lazy StringRegion build path. Builds a page with
 * several fused OBJECT_NAMED_STRING records (the on-disk shape produced by Sirix when shredding
 * {@code {"dept":"Eng","city":"NYC"}}), then calls {@link KeyValueLeafPage#getStringRegionHeader()}
 * and verifies the region is built correctly with parent-grouped tags + dictionaries.
 *
 * <p>
 * Also verifies invalidation: writing a new OBJECT_NAMED_STRING on the page must drop the cached
 * region so the next read rebuilds.
 */
@DisplayName("StringRegion page integration")
final class StringRegionIntegrationTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  private Arena arena;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
  }

  @AfterEach
  void tearDown() {
    if (arena != null)
      arena.close();
  }

  private KeyValueLeafPage createPage(final long recordPageKey) {
    return new KeyValueLeafPage(recordPageKey, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("testResource").build(), 1, arena.allocate(SIXTYFOUR_KB), null);
  }

  /** Write a fused {@link ObjectNamedStringNode} at the slot derived from {@code nodeKey}. */
  private void writeObjectNamedString(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final String value) {
    final int slot = (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(), // parentKey
        Fixed.NULL_NODE_KEY.getStandardProperty(), // rightSiblingKey
        Fixed.NULL_NODE_KEY.getStandardProperty(), // leftSiblingKey
        nameKey, /* pathNodeKey */ -1L, /* previousRevision */ 0, /* lastModifiedRevision */ 0, /* hash */ 0L, bytes,
        HASH_FN, (byte[]) null, /* isCompressed */ false, /* fsstSymbolTable */ null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);
  }

  @Test
  @DisplayName("page with one (dept → 'Eng') pair builds a 1-tag, 1-entry-dict region")
  void buildsRegionFromSingleStringPair() {
    final KeyValueLeafPage page = createPage(0);
    final int deptNameKey = 7;
    writeObjectNamedString(page, /* nodeKey */ 0, deptNameKey, "Eng");

    // Cache should be empty before first call.
    assertNull(page.getStringRegionPayload());

    final StringRegion.Header h = page.getStringRegionHeader();
    assertNotNull(h);
    assertEquals(1, h.count);
    assertEquals(1, h.parentDictSize);
    assertEquals(deptNameKey, h.parentDict[0]);
    assertEquals(1, h.tagCount[0]);
    assertEquals(1, h.tagStringDictSize[0]);
    final MemorySegment payload = page.getStringRegionPayload();
    final int dictId = StringRegion.decodeDictIdAt(payload, h, 0);
    assertEquals(0, dictId); // first (and only) dict entry
    final int off = StringRegion.decodeStringOffset(payload, h, 0, dictId);
    final int len = StringRegion.decodeStringLength(payload, h, 0, dictId);
    assertEquals("Eng", new String(payload.asSlice(off, len).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8));
  }

  @Test
  @DisplayName("multiple values dedupe into a per-tag local dictionary")
  void dedupesPerTagDictionary() {
    final KeyValueLeafPage page = createPage(0);
    final int deptNameKey = 7;
    // Three fused records under the same nameKey: Eng, Sales, Eng — dict should have 2 entries.
    writeObjectNamedString(page, 0, deptNameKey, "Eng");
    writeObjectNamedString(page, 1, deptNameKey, "Sales");
    writeObjectNamedString(page, 2, deptNameKey, "Eng");

    final StringRegion.Header h = page.getStringRegionHeader();
    assertNotNull(h);
    assertEquals(3, h.count);
    assertEquals(1, h.parentDictSize);
    assertEquals(2, h.tagStringDictSize[0]); // Eng + Sales

    final MemorySegment payload = page.getStringRegionPayload();
    final String[] decoded = new String[3];
    for (int i = 0; i < 3; i++) {
      final int dictId = StringRegion.decodeDictIdAt(payload, h, i);
      final int off = StringRegion.decodeStringOffset(payload, h, 0, dictId);
      final int len = StringRegion.decodeStringLength(payload, h, 0, dictId);
      decoded[i] = new String(payload.asSlice(off, len).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }
    assertEquals("Eng", decoded[0]);
    assertEquals("Sales", decoded[1]);
    assertEquals("Eng", decoded[2]);
  }

  @Test
  @DisplayName("two distinct nameKeys (dept + city) → two tags with independent dicts")
  void twoFieldsTwoTags() {
    final KeyValueLeafPage page = createPage(0);
    final int deptKey = 7, cityKey = 9;
    // Object 1: dept=Eng, city=NYC
    writeObjectNamedString(page, 0, deptKey, "Eng");
    writeObjectNamedString(page, 1, cityKey, "NYC");
    // Object 2: dept=Sales, city=NYC
    writeObjectNamedString(page, 2, deptKey, "Sales");
    writeObjectNamedString(page, 3, cityKey, "NYC");

    final StringRegion.Header h = page.getStringRegionHeader();
    assertNotNull(h);
    assertEquals(4, h.count);
    assertEquals(2, h.parentDictSize);

    final int deptTag = StringRegion.lookupTag(h, deptKey);
    final int cityTag = StringRegion.lookupTag(h, cityKey);
    assertTrue(deptTag >= 0);
    assertTrue(cityTag >= 0);
    assertEquals(2, h.tagCount[deptTag]); // Eng + Sales
    assertEquals(2, h.tagCount[cityTag]); // NYC + NYC
    assertEquals(2, h.tagStringDictSize[deptTag]); // distinct dept values
    assertEquals(1, h.tagStringDictSize[cityTag]); // only NYC
  }

  @Test
  @DisplayName("invalidation: writing a new OBJECT_NAMED_STRING drops cached region")
  void invalidationOnNewStringWrite() {
    final KeyValueLeafPage page = createPage(0);
    final int deptKey = 7;
    writeObjectNamedString(page, 0, deptKey, "Eng");

    final StringRegion.Header h1 = page.getStringRegionHeader();
    assertNotNull(h1);
    assertEquals(1, h1.count);
    assertNotNull(page.getStringRegionPayload());

    // Add another string value — should invalidate.
    writeObjectNamedString(page, 1, deptKey, "Sales");
    assertNull(page.getStringRegionPayload(), "writing a new OBJECT_NAMED_STRING should invalidate the cached region");

    final StringRegion.Header h2 = page.getStringRegionHeader();
    assertNotNull(h2);
    assertEquals(2, h2.count, "rebuilt region must include the new value");
  }

  @Test
  @DisplayName("invalidateStringRegion is a no-op fast-path when nothing cached")
  void invalidateNoOpWhenUnbuilt() {
    final KeyValueLeafPage page = createPage(0);
    page.invalidateStringRegion(); // should not throw
    assertNull(page.getStringRegionPayload());
    page.invalidateStringRegion();
    assertNull(page.getStringRegionPayload());
  }

  @Test
  @DisplayName("isStringValueKindId discriminates correctly")
  void kindIdDiscriminator() {
    assertTrue(KeyValueLeafPage.isStringValueKindId(io.sirix.node.NodeKind.OBJECT_NAMED_STRING.getId()));
    assertTrue(KeyValueLeafPage.isStringValueKindId(io.sirix.node.NodeKind.STRING_VALUE.getId()));
    assertEquals(false, KeyValueLeafPage.isStringValueKindId(io.sirix.node.NodeKind.OBJECT_NAMED_NUMBER.getId()));
    assertEquals(false, KeyValueLeafPage.isStringValueKindId(io.sirix.node.NodeKind.OBJECT_NAMED_OBJECT.getId()));
  }

  @Test
  @DisplayName("stored fused strings copy atomically into reusable caller scratch")
  void storedStringScratchCopyContract() {
    final KeyValueLeafPage page = createPage(0);
    final byte[] stored = bytes("stored-fsst-form");
    final ObjectNamedStringNode node = new ObjectNamedStringNode(0, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 7, -1L, 0, 0, 0L, stored,
        HASH_FN, (byte[]) null, true, null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, 0, 0);

    final byte[] tooSmall = new byte[stored.length - 1];
    Arrays.fill(tooSmall, (byte) 0x5a);
    final byte[] untouched = tooSmall.clone();
    assertEquals(stored.length, page.copyFusedObjectNamedStringStoredBytes(0, tooSmall));
    assertArrayEquals(untouched, tooSmall, "a failed capacity probe must not copy a prefix");

    final byte[] exact = new byte[stored.length];
    assertEquals(stored.length, page.copyFusedObjectNamedStringStoredBytes(0, exact));
    assertArrayEquals(stored, exact);
    assertTrue(page.isFusedObjectNamedStringValueCompressed(0));
    assertArrayEquals(stored, page.readFusedObjectNamedStringStoredBytes(0),
        "the public detached byte[] API remains unchanged");

    assertEquals(-1, page.copyFusedObjectNamedStringStoredBytes(1, exact));
    assertThrows(IndexOutOfBoundsException.class,
        () -> page.copyFusedObjectNamedStringStoredBytes(PageLayout.SLOT_COUNT, exact));
    assertThrows(NullPointerException.class, () -> page.copyFusedObjectNamedStringStoredBytes(0, null));
  }

  @Test
  @DisplayName("PageKind releases path candidates across true/false/false/true resource switches")
  void pathCandidateLifecycleAcrossResourceModes() {
    // Start from a known state even when another test ran on this worker thread first.
    assertNull(PageKind.resetStringRegionPathCandidate(false, null));
    final StringRegion.Encoder name = new StringRegion.Encoder();
    final StringRegion.Encoder path = Objects.requireNonNull(PageKind.resetStringRegionPathCandidate(true, null));
    final byte[] scratch = new byte[192];

    try {
      addSharedCandidateValue(name, path, scratch, "same-value", false);
      addSharedCandidateValue(name, path, scratch, "same-value", true);
      addSharedCandidateValue(name, path, scratch, "same-value", false);
      final byte[] nameA = name.finish(StringRegion.TAG_KIND_NAME);
      final byte[] pathA = path.finish(StringRegion.TAG_KIND_PATH_NODE);

      // true -> false: this is the production acquisition order. reset(name) must defer because the
      // old path candidate still borrows its ranges; the actual PageKind helper must then release it
      // even though it returns null for a no-path resource.
      name.reset();
      assertTrue(StringRegionEncoderTestAccess.valueStoreLength(name) > 0);
      assertNull(PageKind.resetStringRegionPathCandidate(false, null));
      assertEquals(0, StringRegionEncoderTestAccess.valueStoreLength(name));

      int bLogicalLength = 0;
      for (int i = 0; i < 48; i++) {
        final String value = "large-page-value-" + i + "-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        bLogicalLength += bytes(value).length;
        addNameCandidateValue(name, scratch, value, (i & 1) != 0);
      }
      final byte[] nameB = name.finish(StringRegion.TAG_KIND_NAME);
      final int highWaterCapacity = StringRegionEncoderTestAccess.valueStoreCapacity(name);
      assertTrue(highWaterCapacity > 1024);
      assertEquals(bLogicalLength, StringRegionEncoderTestAccess.valueStoreLength(name));

      // false -> false: the next name-only page must start at offset zero, not append after B.
      name.reset();
      assertNull(PageKind.resetStringRegionPathCandidate(false, null));
      assertEquals(0, StringRegionEncoderTestAccess.valueStoreLength(name));
      for (int i = 0; i < 48; i++) {
        final String value = "large-page-value-" + i + "-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        addNameCandidateValue(name, scratch, value, (i & 1) != 0);
      }
      assertEquals(bLogicalLength, StringRegionEncoderTestAccess.valueStoreLength(name));
      assertEquals(highWaterCapacity, StringRegionEncoderTestAccess.valueStoreCapacity(name));
      assertArrayEquals(nameB, name.finish(StringRegion.TAG_KIND_NAME));

      // false -> true: PageKind reuses the same per-thread path encoder and both wires reproduce A.
      name.reset();
      final StringRegion.Encoder enabledAgain =
          Objects.requireNonNull(PageKind.resetStringRegionPathCandidate(true, null));
      assertSame(path, enabledAgain);
      addSharedCandidateValue(name, enabledAgain, scratch, "same-value", false);
      addSharedCandidateValue(name, enabledAgain, scratch, "same-value", true);
      addSharedCandidateValue(name, enabledAgain, scratch, "same-value", false);
      assertArrayEquals(nameA, name.finish(StringRegion.TAG_KIND_NAME));
      assertArrayEquals(pathA, enabledAgain.finish(StringRegion.TAG_KIND_PATH_NODE));
      assertEquals(highWaterCapacity, StringRegionEncoderTestAccess.valueStoreCapacity(name));
    } finally {
      name.reset();
      PageKind.resetStringRegionPathCandidate(false, null);
    }
  }

  private static void addNameCandidateValue(final StringRegion.Encoder name, final byte[] scratch, final String value,
      final boolean compressed) {
    final byte[] source = bytes(value);
    final int offset = 7;
    System.arraycopy(source, 0, scratch, offset, source.length);
    name.addValue(23, scratch, offset, source.length, compressed);
    Arrays.fill(scratch, (byte) 0x5a);
  }

  private static void addSharedCandidateValue(final StringRegion.Encoder name, final StringRegion.Encoder path,
      final byte[] scratch, final String value, final boolean compressed) {
    final byte[] source = bytes(value);
    final int offset = 7;
    System.arraycopy(source, 0, scratch, offset, source.length);
    name.addValueCopiedAndShareWith(23, scratch, offset, source.length, compressed, path, 101);
    Arrays.fill(scratch, (byte) 0x5a);
  }

  private static byte[] bytes(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("a page whose column is all distinct takes the plain lane and round-trips its records")
  void plainLaneColumnSurvivesASerializationRoundTrip() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("testResource").build();
    final KeyValueLeafPage page = createPage(0);
    KeyValueLeafPage deserialized = null;
    try {
      final int urlNameKey = 11;
      final int browserNameKey = 12;
      final String[] browsers = {"Firefox", "Chrome", "Safari"};
      final int rows = 12;
      for (int row = 0; row < rows; row++) {
        writeObjectNamedString(page, row, urlNameKey, "https://example.org/item-" + row + "?ref=catalogue");
        writeObjectNamedString(page, rows + row, browserNameKey, browsers[row % browsers.length]);
      }

      final StringRegion.Header built = page.getStringRegionHeader();
      assertNotNull(built);
      final int urlTag = StringRegion.lookupTag(built, urlNameKey);
      final int browserTag = StringRegion.lookupTag(built, browserNameKey);
      assertTrue(urlTag >= 0 && browserTag >= 0);
      assertTrue(built.tagPlainLane[urlTag], "twelve distinct URLs must write no dict ids");
      assertTrue(!built.tagPlainLane[browserTag], "three browsers under twelve records keep their dictionary");

      // The record path is what value elision has to reproduce: the values leave the heap into the
      // region on write and are reinjected on read, so an equal slot means the plain lane survived
      // serialization, deserialization and reinjection.
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      final BytesIn<?> source = sink.bytesForRead();
      source.readByte(); // pageKind id
      deserialized =
          (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
      for (int row = 0; row < 2 * rows; row++) {
        final int slot = (int) (row & (Constants.NDP_NODE_COUNT - 1));
        assertArrayEquals(page.getSlotAsByteArray(slot), deserialized.getSlotAsByteArray(slot),
            "record " + row + " must come back byte for byte");
      }

      final StringRegion.Header readBack = deserialized.getStringRegionHeader();
      assertNotNull(readBack, "the region must survive the round trip");
      final int readUrlTag = StringRegion.lookupTag(readBack, urlNameKey);
      assertTrue(readUrlTag >= 0);
      assertTrue(readBack.tagPlainLane[readUrlTag], "and still be on the plain lane");
      assertEquals(rows, readBack.tagStringDictSize[readUrlTag]);
      for (int row = 0; row < rows; row++) {
        final int id = StringRegion.decodeDictIdAt(deserialized.getStringRegionPayload(), readBack,
            readBack.tagStart[readUrlTag] + row);
        assertEquals(row, id, "on the plain lane a value's rank IS its id");
      }
    } finally {
      page.close();
      if (deserialized != null) {
        deserialized.close();
      }
    }
  }
}
