package io.sirix.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringRegion;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.Constants;
import io.sirix.settings.StringCompressionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Array-element strings in the PAX string column.
 *
 * <h2>What was missing</h2>
 *
 * <p>
 * The column held object-named strings and nothing else: {@code collectAndEncodeStringRegion}
 * matched {@code FUSED_OBJECT_NAMED_STRING} and every other kind fell through a {@code continue}.
 * So a predicate over an array of strings — {@code some $g in $m.genres[] satisfies $g eq "Drama"}
 * — had no column to read and went through the records, which on a 3.48M-record corpus meant
 * rebuilding all 51,745 slotted pages: 78 % of a one-shot query, and the one shape left an order of
 * magnitude behind DuckDB.
 *
 * <p>
 * The reason was not a policy about arrays. An array element is a standalone {@code STRING_VALUE}
 * that carries NO path node key of its own — every one reads back as {@code -1} — and the region is
 * path-tagged, so there was nothing to tag it by. Its enclosing array does have one, and that is
 * the tag a query actually names when it writes {@code $m.genres[]}.
 *
 * <h2>What is asserted</h2>
 *
 * <p>
 * That the elements arrive under the ENCLOSING ARRAY's path node key, that their count is the
 * complete set for the page, and — the half that keeps the change safe — that the object-named tags
 * beside them are untouched. Every reader of this region treats {@code tagCount} as the complete
 * number of that path's values on the page, so a tag that gained or lost entries would silently
 * change what those readers count.
 */
final class ArrayElementStringColumnTest {

  private Path dbDir;

  @AfterEach
  void tearDown() throws Exception {
    KeyValueLeafPage.ARRAY_ELEMENT_STRINGS_IN_REGION = false;
    if (dbDir != null && Files.exists(dbDir)) {
      Databases.removeDatabase(dbDir);
    }
  }

  @Test
  @DisplayName("array elements are absent from the column by default and present when enabled")
  void arrayElementsJoinTheColumnOnlyWhenEnabled() throws Exception {
    final int withoutElements = tagKeyCount(false);
    final int withElements = tagKeyCount(true);
    assertTrue(withElements > withoutElements,
        "enabling the flag added no tag: array-element strings still never reach the "
            + "column, so a predicate over an array has nothing columnar to read");
  }

  @Test
  @DisplayName("elements land under the enclosing array's path, and the named tags are untouched")
  void elementsAreTaggedByTheirArrayAndLeaveNamedTagsAlone() throws Exception {
    final StringRegion.Header without = header(false);
    final StringRegion.Header with = header(true);
    assertNotNull(without, "no string region without the flag");
    assertNotNull(with, "no string region with the flag");

    // Every tag that existed before must survive with exactly its old count: those counts are what
    // the existing kernels check completeness against.
    for (int i = 0; i < without.parentDictSize; i++) {
      final int key = without.parentDict[i];
      final int tag = StringRegion.lookupTag(with, key);
      assertTrue(tag >= 0, "tag " + key + " disappeared when array elements were added");
      assertEquals(without.tagCount[i], with.tagCount[tag],
          "tag " + key + " changed count when array elements were added — the existing "
              + "kernels read tagCount as the complete count for that path");
    }

    // And the new tag holds every element the corpus put on the page: 3 records x 2 genres, under
    // the ENCLOSING ARRAY's key. Asserting the key and not just the count is what catches reading
    // a structural record's name or path with the primitive layout's field index — which yields a
    // plausible-looking number rather than a failure.
    int addedTags = 0;
    int addedValues = 0;
    int addedKey = Integer.MIN_VALUE;
    for (int i = 0; i < with.parentDictSize; i++) {
      if (StringRegion.lookupTag(without, with.parentDict[i]) < 0) {
        addedTags++;
        addedValues += with.tagCount[i];
        addedKey = with.parentDict[i];
      }
    }
    assertEquals(1, addedTags, "the elements must land under ONE tag — their enclosing array's");
    assertEquals(6, addedValues, "the added tag does not hold all six genre elements; a tag covering only some of "
        + "a path's values is worse than none, because tagCount is read as complete");
    assertEquals(genresPathNodeKey(), addedKey,
        "the elements are not tagged by their enclosing array's path node key — a query "
            + "naming $m.genres[] would look under that key and find nothing");
  }

  /**
   * The path node key of {@code /[]/genres/[]} — the anonymous ARRAY layer, which is what a fused
   * OBJECT_NAMED_ARRAY slot carries and therefore the tag its elements must land under.
   */
  private int genresPathNodeKey() throws Exception {
    final var database = Databases.openJsonDatabase(dbDir);
    try (final JsonResourceSession session = database.beginResourceSession("records");
        final var rtx = session.beginNodeReadOnlyTrx()) {
      final long pages = (rtx.getMaxNodeKey() >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
      for (long pk = 0; pk < pages; pk++) {
        for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
          final long nodeKey = (pk << Constants.INP_REFERENCE_COUNT_EXPONENT) + slot;
          if (rtx.moveTo(nodeKey) && rtx.getKind() == io.sirix.node.NodeKind.OBJECT_NAMED_ARRAY) {
            return (int) rtx.getPathNodeKey();
          }
        }
      }
      throw new IllegalStateException("no array-valued field in the corpus");
    }
  }

  /** Number of distinct tags in the page's string region. */
  private int tagKeyCount(final boolean elementsEnabled) throws Exception {
    final StringRegion.Header header = header(elementsEnabled);
    return header == null
        ? 0
        : header.parentDictSize;
  }

  private StringRegion.Header header(final boolean elementsEnabled) throws Exception {
    KeyValueLeafPage.ARRAY_ELEMENT_STRINGS_IN_REGION = elementsEnabled;
    if (dbDir != null && Files.exists(dbDir)) {
      Databases.removeDatabase(dbDir);
    }
    dbDir = Files.createTempDirectory("sirix-array-element-strings-");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbDir));
    final var database = Databases.openJsonDatabase(dbDir);
    database.createResource(ResourceConfiguration.newBuilder("records")
                                                 .stringCompressionType(StringCompressionType.NONE)
                                                 .buildPathSummary(true)
                                                 .build());
    try (final JsonResourceSession session = database.beginResourceSession("records")) {
      // Three records, each with a named string and a two-element genre array: the named tag and
      // the array tag must both be identifiable in the result.
      final String json = """
          [{"title":"a","genres":["Drama","Short"]},\
          {"title":"b","genres":["Drama","Comedy"]},\
          {"title":"c","genres":["Silent","Short"]}]""";
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
        wtx.commit();
      }
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        final int rev = rtx.getRevisionNumber();
        final long pages = (rtx.getMaxNodeKey() >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
        final IndexLogKey key = new IndexLogKey(IndexType.DOCUMENT, 0, 0, rev);
        for (long pk = 0; pk < pages; pk++) {
          final RegionsOnlyPage regions =
              reader.getRecordPageRegionsOnly(key.setRecordPageKey(pk), 1 << RegionTable.KIND_STRING, 0);
          if (regions == null) {
            continue;
          }
          try (regions) {
            final StringRegion.Header header = regions.stringHeaderInto(new StringRegion.Header());
            if (header != null && header.parentDictSize > 0) {
              return header;
            }
          }
        }
      }
      return null;
    }
  }
}
