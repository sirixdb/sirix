package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.page.pax.RegionTable;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static io.sirix.cache.MemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every column region's drop-set must equal its derive-set.
 *
 * <p>{@code invalidateNumberRegion} and {@code invalidateStringRegion} have had that property for a
 * while; the field-name and boolean columns had no invalidation counterpart at all, even though
 * {@code PageKind#serializePage} installs the whole region table — names, record ordinals and
 * booleans included — onto the writer's live in-memory page. A page carrying those columns is then
 * only kept off a subsequent write by the frozen-container copy-on-write guard, which is a check
 * rather than an impossibility.
 *
 * <p>The record-ordinal column is the one where a stale survivor is worst. It numbers records by
 * position in the field-name column collected in the same pass, and a fused cross-column predicate
 * takes that numbering as its alignment certificate — so a linkage that outlives the names it
 * indexes produces a wrong answer rather than a stale bound.
 */
@DisplayName("Column-region invalidation symmetry")
final class ColumnRegionInvalidationSymmetryTest {

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

  /** Writes a fused OBJECT_NAMED_BOOLEAN through the production writer path. */
  private void writeObjectBoolean(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final boolean value) {
    final int slot = (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
    final ObjectNamedBooleanNode node = new ObjectNamedBooleanNode(nodeKey,
        Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(),
        nameKey,
        /*pathNodeKey*/ -1L,
        /*previousRevision*/ 0,
        /*lastModifiedRevision*/ 0,
        /*hash*/ 0L,
        value,
        HASH_FN,
        (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);
  }

  /** Writes a fused OBJECT_NAMED_NUMBER through the production writer path. */
  private void writeObjectNumber(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final long value) {
    final int slot = (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
    final ObjectNamedNumberNode node = new ObjectNamedNumberNode(nodeKey,
        Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(),
        nameKey,
        /*pathNodeKey*/ -1L,
        /*previousRevision*/ 0,
        /*lastModifiedRevision*/ 0,
        /*hash*/ 0L,
        value,
        HASH_FN,
        (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);
  }

  /** Installs the columns a serialized page carries, standing in for the writer's own table. */
  private static RegionTable seedNamesAndBooleanColumns(final KeyValueLeafPage page) {
    final RegionTable seeded = new RegionTable();
    seeded.set(RegionTable.KIND_OBJECT_KEY_NAMEKEY, new byte[] { 7, 1, 2, 3 });
    seeded.set(RegionTable.KIND_RECORD_ORDINAL, new byte[] { 4, 5, 6 });
    seeded.set(RegionTable.KIND_BOOLEAN, new byte[] { 9, 8 });
    page.setRegionTable(seeded);
    return seeded;
  }

  @Test
  @DisplayName("invalidateNamesRegion drops the names column AND the record linkage indexed by it")
  void namesInvalidationDropsTheOrdinalsToo() {
    final KeyValueLeafPage page = createPage(0);
    final RegionTable seeded = seedNamesAndBooleanColumns(page);

    page.invalidateNamesRegion();

    assertNull(seeded.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY), "the field-name column must be dropped");
    assertNull(seeded.payload(RegionTable.KIND_RECORD_ORDINAL),
               "the record linkage is indexed by the names column and must fall with it");
    assertNotNull(seeded.payload(RegionTable.KIND_BOOLEAN),
                  "a drop-set wider than the derive-set throws away a column for nothing");
  }

  @Test
  @DisplayName("invalidateBooleanRegion drops exactly the boolean column")
  void booleanInvalidationDropsOnlyItsOwnColumn() {
    final KeyValueLeafPage page = createPage(0);
    final RegionTable seeded = seedNamesAndBooleanColumns(page);

    page.invalidateBooleanRegion();

    assertNull(seeded.payload(RegionTable.KIND_BOOLEAN));
    assertNotNull(seeded.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY));
    assertNotNull(seeded.payload(RegionTable.KIND_RECORD_ORDINAL));
  }

  @Test
  @DisplayName("both invalidations are fast-path no-ops on a page with no columns")
  void invalidationsAreNoOpsWhenNothingIsInstalled() {
    final KeyValueLeafPage page = createPage(0);

    page.invalidateNamesRegion();
    page.invalidateBooleanRegion();
    page.invalidateNamesRegion();
    page.invalidateBooleanRegion();

    assertNull(page.getRegionTable(), "invalidation must not conjure a region table into existence");
  }

  @Test
  @DisplayName("writing a fused named boolean drops the boolean, names and ordinal columns")
  void booleanWriteInvalidatesEveryColumnItFeeds() {
    final KeyValueLeafPage page = createPage(0);
    // A record already on the heap, so the seeded columns describe a real page state.
    writeObjectBoolean(page, /*nodeKey*/ 0, /*nameKey*/ 1, true);
    final RegionTable seeded = seedNamesAndBooleanColumns(page);

    // A second named boolean: a new row in the boolean column, a new row in the field-name column,
    // and therefore a different ordinal numbering. All three snapshots are now stale.
    writeObjectBoolean(page, /*nodeKey*/ 1, /*nameKey*/ 2, false);

    assertNull(seeded.payload(RegionTable.KIND_BOOLEAN), "the boolean column must be dropped");
    assertNull(seeded.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY),
               "a fused OBJECT_NAMED_* record is a row of the field-name column too");
    assertNull(seeded.payload(RegionTable.KIND_RECORD_ORDINAL),
               "a stale alignment certificate is a wrong answer, not a stale bound");
  }

  @Test
  @DisplayName("writing a fused named number drops the names column as well as the number column")
  void numberWriteAlsoInvalidatesTheNamesColumn() {
    final KeyValueLeafPage page = createPage(0);
    writeObjectNumber(page, /*nodeKey*/ 0, /*nameKey*/ 1, 42L);
    final RegionTable seeded = seedNamesAndBooleanColumns(page);

    writeObjectNumber(page, /*nodeKey*/ 1, /*nameKey*/ 2, 7L);

    assertNull(seeded.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY),
               "the number/string if-else chain used to be exclusive, so the names column survived");
    assertNull(seeded.payload(RegionTable.KIND_RECORD_ORDINAL));
    assertNotNull(seeded.payload(RegionTable.KIND_BOOLEAN),
                  "a number write feeds no boolean column and must leave it alone");
  }

  @Test
  @DisplayName("the boolean drop-set tracks the boolean builder's selection exactly")
  void booleanKindPredicateMatchesTheBuilder() {
    assertTrue(KeyValueLeafPage.isBooleanValueKindId(NodeKind.OBJECT_NAMED_BOOLEAN.getId()));
    // collectAndEncodeBooleanRegion matches the FUSED kind alone, so a standalone BOOLEAN_VALUE
    // contributes no column row and must not trigger a drop.
    assertFalse(KeyValueLeafPage.isBooleanValueKindId(NodeKind.BOOLEAN_VALUE.getId()));
    assertFalse(KeyValueLeafPage.isBooleanValueKindId(NodeKind.OBJECT_NAMED_NUMBER.getId()));
  }
}
