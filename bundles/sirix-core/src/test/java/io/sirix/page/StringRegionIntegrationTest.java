package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectStringNode;
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
import java.nio.charset.StandardCharsets;

import static io.sirix.cache.MemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test for {@link KeyValueLeafPage}'s lazy StringRegion build path.
 * Builds a page with several OBJECT_KEY → OBJECT_STRING_VALUE pairs (same
 * shape as Sirix shreds JSON like {@code {"dept":"Eng","city":"NYC"}}), then
 * calls {@link KeyValueLeafPage#getStringRegionHeader()} and verifies the
 * region is built correctly with parent-grouped tags + dictionaries.
 *
 * <p>Also verifies invalidation: setting a new OBJECT_STRING_VALUE on the
 * page must drop the cached region so the next read rebuilds.
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
    if (arena != null) arena.close();
  }

  private KeyValueLeafPage createPage(final long recordPageKey) {
    return new KeyValueLeafPage(recordPageKey, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("testResource").build(), 1,
        arena.allocate(SIXTYFOUR_KB), null);
  }

  /** Write an OBJECT_KEY at slot {@code keySlot} with given nameKey + firstChild. */
  private void writeObjectKey(final KeyValueLeafPage page, final long nodeKey,
      final long firstChildKey, final int nameKey) {
    final int slot = (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
    final ObjectKeyNode node = new ObjectKeyNode(nodeKey,
        Fixed.NULL_NODE_KEY.getStandardProperty(), // parentKey (unused here)
        /*pathNodeKey*/ -1,
        /*previousRevision*/ 0,
        /*lastModifiedRevision*/ 0,
        Fixed.NULL_NODE_KEY.getStandardProperty(), // rightSibling
        Fixed.NULL_NODE_KEY.getStandardProperty(), // leftSibling
        firstChildKey,
        nameKey,
        /*descendantCount*/ 0L,
        /*hash*/ 0L,
        HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);
  }

  /** Write an OBJECT_STRING_VALUE at slot derived from {@code nodeKey}. */
  private void writeObjectStringValue(final KeyValueLeafPage page, final long nodeKey,
      final long parentKey, final String value) {
    final int slot = (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    final ObjectStringNode node = new ObjectStringNode(nodeKey, parentKey, 0, 0,
        0L, bytes, HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);
  }

  @Test
  @DisplayName("page with one (dept → 'Eng') pair builds a 1-tag, 1-entry-dict region")
  void buildsRegionFromSingleStringPair() {
    final KeyValueLeafPage page = createPage(0);
    final int deptNameKey = 7;
    writeObjectKey(page, /*okNodeKey*/ 0, /*firstChild*/ 1, deptNameKey);
    writeObjectStringValue(page, /*svNodeKey*/ 1, /*parent*/ 0, "Eng");

    // Cache should be empty before first call.
    assertNull(page.getStringRegionPayload());

    final StringRegion.Header h = page.getStringRegionHeader();
    assertNotNull(h);
    assertEquals(1, h.count);
    assertEquals(1, h.parentDictSize);
    assertEquals(deptNameKey, h.parentDict[0]);
    assertEquals(1, h.tagCount[0]);
    assertEquals(1, h.tagStringDictSize[0]);
    final byte[] payload = page.getStringRegionPayload();
    final int dictId = StringRegion.decodeDictIdAt(payload, h, 0);
    assertEquals(0, dictId); // first (and only) dict entry
    final int off = StringRegion.decodeStringOffset(payload, h, 0, dictId);
    final int len = StringRegion.decodeStringLength(payload, h, 0, dictId);
    assertEquals("Eng", new String(payload, off, len, StandardCharsets.UTF_8));
  }

  @Test
  @DisplayName("multiple values dedupe into a per-tag local dictionary")
  void dedupesPerTagDictionary() {
    final KeyValueLeafPage page = createPage(0);
    final int deptNameKey = 7;
    // Three OBJECT_KEY/value pairs: Eng, Sales, Eng — the dict should have 2 entries.
    writeObjectKey(page, 0, 1, deptNameKey);
    writeObjectStringValue(page, 1, 0, "Eng");
    writeObjectKey(page, 2, 3, deptNameKey);
    writeObjectStringValue(page, 3, 2, "Sales");
    writeObjectKey(page, 4, 5, deptNameKey);
    writeObjectStringValue(page, 5, 4, "Eng");

    final StringRegion.Header h = page.getStringRegionHeader();
    assertNotNull(h);
    assertEquals(3, h.count);
    assertEquals(1, h.parentDictSize);
    assertEquals(2, h.tagStringDictSize[0]); // Eng + Sales

    final byte[] payload = page.getStringRegionPayload();
    final String[] decoded = new String[3];
    for (int i = 0; i < 3; i++) {
      final int dictId = StringRegion.decodeDictIdAt(payload, h, i);
      final int off = StringRegion.decodeStringOffset(payload, h, 0, dictId);
      final int len = StringRegion.decodeStringLength(payload, h, 0, dictId);
      decoded[i] = new String(payload, off, len, StandardCharsets.UTF_8);
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
    writeObjectKey(page, 0, 1, deptKey);
    writeObjectStringValue(page, 1, 0, "Eng");
    writeObjectKey(page, 2, 3, cityKey);
    writeObjectStringValue(page, 3, 2, "NYC");
    // Object 2: dept=Sales, city=NYC
    writeObjectKey(page, 4, 5, deptKey);
    writeObjectStringValue(page, 5, 4, "Sales");
    writeObjectKey(page, 6, 7, cityKey);
    writeObjectStringValue(page, 7, 6, "NYC");

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
  @DisplayName("invalidation: writing a new OBJECT_STRING_VALUE drops cached region")
  void invalidationOnNewStringWrite() {
    final KeyValueLeafPage page = createPage(0);
    final int deptKey = 7;
    writeObjectKey(page, 0, 1, deptKey);
    writeObjectStringValue(page, 1, 0, "Eng");

    final StringRegion.Header h1 = page.getStringRegionHeader();
    assertNotNull(h1);
    assertEquals(1, h1.count);
    assertNotNull(page.getStringRegionPayload());

    // Add another string value — should invalidate.
    writeObjectKey(page, 2, 3, deptKey);
    writeObjectStringValue(page, 3, 2, "Sales");
    assertNull(page.getStringRegionPayload(),
        "writing a new OBJECT_STRING_VALUE should invalidate the cached region");

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
    assertTrue(KeyValueLeafPage.isStringValueKindId(io.sirix.node.NodeKind.OBJECT_STRING_VALUE.getId()));
    assertTrue(KeyValueLeafPage.isStringValueKindId(io.sirix.node.NodeKind.STRING_VALUE.getId()));
    assertEquals(false, KeyValueLeafPage.isStringValueKindId(io.sirix.node.NodeKind.OBJECT_NUMBER_VALUE.getId()));
    assertEquals(false, KeyValueLeafPage.isStringValueKindId(io.sirix.node.NodeKind.OBJECT_KEY.getId()));
  }
}
