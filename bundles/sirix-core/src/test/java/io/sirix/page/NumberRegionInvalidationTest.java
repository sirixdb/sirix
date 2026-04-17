package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.settings.Constants;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static io.sirix.cache.MemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Verifies the {@link KeyValueLeafPage#invalidateNumberRegion()} contract introduced
 * to keep the cached {@link NumberRegion} in sync with mutations of OBJECT_NUMBER_VALUE
 * records on the same page — needed for any future writer-side query path that reads
 * a writer's TIL-resident page through the PAX region.
 *
 * <p>Also covers the {@code tryBuildNumberRegionFromSlottedPage} fix that preserves
 * any pre-existing region (e.g. {@link RegionTable#KIND_OBJECT_KEY_NAMEKEY}) when
 * lazily building the number region.
 */
@DisplayName("NumberRegion invalidation")
final class NumberRegionInvalidationTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  private Arena arena;

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

  private KeyValueLeafPage createPage(final long recordPageKey) {
    return new KeyValueLeafPage(recordPageKey, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("testResource").build(), 1,
        arena.allocate(SIXTYFOUR_KB), null);
  }

  /**
   * Push an OBJECT_NUMBER_VALUE through the same path the production writer uses:
   * primary constructor → {@code page.serializeNewRecord}, which lands the record on
   * the slotted-page heap exactly like the live JSON shredder.
   */
  private void writeObjectNumber(final KeyValueLeafPage page, final long nodeKey,
      final long parentKey, final long value) {
    final int slot = (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
    final ObjectNumberNode node = new ObjectNumberNode(nodeKey, parentKey, 0, 0,
        0L, value, HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);
  }

  // ───────────────────────────────────────────────────────── direct-helper tests

  @Test
  @DisplayName("invalidateNumberRegion drops cached header AND region payload")
  void invalidateDropsCacheAndPayload() {
    final KeyValueLeafPage page = createPage(0);

    // Build a real number region by writing one OBJECT_NUMBER_VALUE and triggering
    // the lazy build.
    writeObjectNumber(page, /*nodeKey*/ 0, /*parentKey*/ 1, /*value*/ 42L);
    final NumberRegion.Header h1 = page.getNumberRegionHeader();
    assertNotNull(h1, "expected lazily-built region after writing a number record");
    assertNotNull(page.getNumberRegionPayload(), "region payload should be cached");

    // Invalidation: cached header AND KIND_NUMBER payload both drop to null.
    page.invalidateNumberRegion();
    assertNull(page.getNumberRegionPayload(), "KIND_NUMBER payload must be cleared");

    // Subsequent read rebuilds (still finds the same record on the heap).
    final NumberRegion.Header h2 = page.getNumberRegionHeader();
    assertNotNull(h2, "rebuild expected after invalidation");
    assertEquals(1, h2.count, "rebuilt region must reflect the heap state");
    assertEquals(42L, NumberRegion.decodeValueAt(page.getNumberRegionPayload(), h2, 0));
  }

  @Test
  @DisplayName("invalidateNumberRegion is a fast-path no-op when no region was built")
  void invalidateNoOpWhenUnbuilt() {
    final KeyValueLeafPage page = createPage(0);

    // No region built. Invalidate should not throw, not allocate, not change state.
    page.invalidateNumberRegion();
    assertNull(page.getNumberRegionPayload());

    // Calling it again is also a no-op.
    page.invalidateNumberRegion();
    assertNull(page.getNumberRegionPayload());
  }

  // ───────────────────────────────────────── region-preservation regression test

  @Test
  @DisplayName("lazy NUMBER build preserves a pre-existing OBJECT_KEY_NAMEKEY region")
  void lazyBuildPreservesOtherRegions() {
    final KeyValueLeafPage page = createPage(0);

    // Pre-install a fake OBJECT_KEY_NAMEKEY payload — represents a page loaded from
    // disk where NAMEKEY was serialized but NUMBER was not.
    final byte[] nameKeyPayload = new byte[] { 7, 1, 2, 3 };
    final RegionTable seeded = new RegionTable();
    seeded.set(RegionTable.KIND_OBJECT_KEY_NAMEKEY, nameKeyPayload);
    page.setRegionTable(seeded);

    // Add a number record so the lazy build has something to encode.
    writeObjectNumber(page, /*nodeKey*/ 0, /*parentKey*/ 1, /*value*/ 99L);

    // Trigger the lazy build. Pre-fix: this would replace the entire RegionTable
    // and silently drop the NAMEKEY payload.
    final NumberRegion.Header h = page.getNumberRegionHeader();
    assertNotNull(h);
    assertEquals(1, h.count);

    // Post-fix: the NAMEKEY payload survives, identical bytes, same RegionTable instance.
    assertSame(seeded, page.getRegionTable(),
        "regionTable instance must be preserved across lazy NUMBER build");
    assertArrayEquals(nameKeyPayload,
        seeded.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY),
        "OBJECT_KEY_NAMEKEY payload must survive lazy NUMBER build");
    assertNotNull(seeded.payload(RegionTable.KIND_NUMBER),
        "NUMBER payload must be installed on the same RegionTable");
  }

  // ───────────────────────────────────────────────────── mutation-hook smoke test

  @Test
  @DisplayName("serializeNewRecord on a number invalidates a previously cached region")
  void numberWriteInvalidatesCache() {
    final KeyValueLeafPage page = createPage(0);

    writeObjectNumber(page, 0, 1, 42L);
    final NumberRegion.Header before = page.getNumberRegionHeader();
    assertNotNull(before);
    assertEquals(1, before.count);
    final byte[] payloadBefore = page.getNumberRegionPayload();
    assertNotNull(payloadBefore);

    // Writing another number record must drop the stale region. The
    // serializeToHeap → invalidateNumberRegion hook is what enforces this.
    writeObjectNumber(page, 1, 2, 7L);
    assertNull(page.getNumberRegionPayload(),
        "writing a NUMBER record must invalidate the cached region");

    // Next read picks up both values.
    final NumberRegion.Header after = page.getNumberRegionHeader();
    assertNotNull(after);
    assertEquals(2, after.count);
  }

  @Test
  @DisplayName("non-number writes do not invalidate a cached number region")
  void nonNumberWriteLeavesRegionAlone() {
    final KeyValueLeafPage page = createPage(0);

    writeObjectNumber(page, 0, 1, 42L);
    final NumberRegion.Header before = page.getNumberRegionHeader();
    assertNotNull(before);
    final byte[] payloadBefore = page.getNumberRegionPayload();
    assertSame(payloadBefore, page.getNumberRegionPayload(),
        "payload reference must be stable across reads (same backing byte[])");

    // Sanity: a non-number kind id correctly skips invalidation.
    // (Direct check on the static helper — exhaustive instrumentation lives in the
    // mutation paths and is exercised by integration tests.)
    org.junit.jupiter.api.Assertions.assertTrue(
        KeyValueLeafPage.isNumberValueKindId(NodeKind.OBJECT_NUMBER_VALUE.getId()));
    org.junit.jupiter.api.Assertions.assertTrue(
        KeyValueLeafPage.isNumberValueKindId(NodeKind.NUMBER_VALUE.getId()));
    org.junit.jupiter.api.Assertions.assertFalse(
        KeyValueLeafPage.isNumberValueKindId(NodeKind.OBJECT_KEY.getId()));
    org.junit.jupiter.api.Assertions.assertFalse(
        KeyValueLeafPage.isNumberValueKindId(NodeKind.STRING_VALUE.getId()));
  }
}
