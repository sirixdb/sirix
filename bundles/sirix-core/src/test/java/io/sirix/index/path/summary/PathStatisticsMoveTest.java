package io.sirix.index.path.summary;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A subtree move has to RE-ATTRIBUTE the statistics of the records it moves, not merely give up on
 * them.
 *
 * <p>Moving a record changes which path counts it, and the previous behaviour marked both the
 * source and the destination path subtree stale to cover that. Nothing ever clears
 * {@code countDirty}, and the reader disqualifies every aggregate on a stale count — so a single
 * move anywhere in a resource permanently switched summary-served aggregates off for two whole path
 * subtrees, for the life of that resource. On a workload that moves at all, "path statistics on by
 * default" then bought nothing.
 *
 * <p>The values themselves survive a move untouched, so both ends are exactly repairable for
 * {@code count} and for an integral {@code sum}: subtract the subtree from the paths it sits under
 * before the move, add it back under the paths it has after. That is what these tests pin.
 */
final class PathStatisticsMoveTest {

  /**
   * Two arrays of objects carrying the SAME field name under DIFFERENT paths, so moving an object
   * between them re-points its {@code age} records from one path class to another instead of
   * carrying its path node along with it.
   */
  private static final String JSON =
      "{\"src\":[{\"age\":1},{\"age\":2}],\"dst\":[{\"age\":10}]}";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static ResourceConfiguration statsConfig() {
    return ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                .buildPathSummary(true)
                                .buildPathStatistics(true)
                                .build();
  }

  /**
   * The path node that carries an array's ELEMENT statistics: the {@code __array__} child of the
   * named path node. A bare array element has no name of its own, so its observations are counted
   * one level below the array's own path node rather than on it.
   */
  private static PathNode elementPathOf(final PathSummaryReader summary, final String arrayName) {
    final List<PathNode> named = summary.findPathsByLocalName(arrayName);
    assertEquals(1, named.size(), "expected exactly one path node for '" + arrayName + "'");
    assertTrue(summary.moveTo(named.getFirst().getNodeKey()), "path node for '" + arrayName + "'");
    assertTrue(summary.moveToFirstChild(),
               "'" + arrayName + "' has no element path node below it");
    final PathNode element = summary.getPathNode();
    assertNotNull(element, "no element path node for '" + arrayName + "'");
    return element;
  }

  /** The {@code age} path nodes, keyed by the array they hang under ({@code src} / {@code dst}). */
  private static Map<String, PathNode> agePathsByArray(final PathSummaryReader summary) {
    final List<PathNode> agePaths = summary.findPathsByLocalName("age");
    final Map<String, PathNode> byArray = new HashMap<>(4);
    for (final PathNode pathNode : agePaths) {
      summary.moveTo(pathNode.getNodeKey());
      final String path = String.valueOf(summary.getPath());
      if (path.contains("src")) {
        byArray.put("src", pathNode);
      } else if (path.contains("dst")) {
        byArray.put("dst", pathNode);
      }
    }
    assertEquals(2, byArray.size(),
                 "expected one 'age' path under each array, found paths for " + byArray.keySet()
                     + " among " + agePaths.size() + " 'age' path node(s)");
    return byArray;
  }

  /**
   * Move the first object of {@code src} into {@code dst}, then assert both ends counted it.
   *
   * <p>The counts are what matters: they are exactly maintainable and the reader's gate treats a
   * dirty count as disqualifying for every aggregate, {@code count} included.
   */
  @Test
  @DisplayName("a move re-attributes the moved records' statistics to their new path")
  void moveReAttributesStatistics() throws Exception {
    try (final var database = JsonTestHelper.getDatabaseWithResourceConfig(
        JsonTestHelper.PATHS.PATH1.getFile(), statsConfig());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      try (final var wtx = session.beginNodeTrx();
          final var reader = JsonShredder.createStringReader(JSON)) {
        wtx.insertSubtreeAsFirstChild(reader);
        wtx.commit();
      }

      // Baseline: the shredded state, before anything moves.
      try (final var summary = session.openPathSummary()) {
        final Map<String, PathNode> ages = agePathsByArray(summary);
        assertEquals(2L, ages.get("src").getStatsValueCount(), "src ages before the move");
        assertEquals(1L + 2L, ages.get("src").getStatsSum(), "src age sum before the move");
        assertEquals(1L, ages.get("dst").getStatsValueCount(), "dst ages before the move");
        assertEquals(10L, ages.get("dst").getStatsSum(), "dst age sum before the move");
      }

      try (final var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveToDocumentRoot());
        assertTrue(wtx.moveToFirstChild());        // the top-level object
        assertTrue(wtx.moveToFirstChild());        // "src", fused array
        final long srcArrayKey = wtx.getNodeKey();
        assertTrue(wtx.moveToRightSibling());      // "dst", fused array
        final long dstArrayKey = wtx.getNodeKey();

        assertTrue(wtx.moveTo(srcArrayKey));
        assertTrue(wtx.moveToFirstChild());        // {"age":1}
        final long movedObjectKey = wtx.getNodeKey();

        assertTrue(wtx.moveTo(dstArrayKey));
        wtx.moveSubtreeToFirstChild(movedObjectKey);
        wtx.commit();
      }

      try (final var summary = session.openPathSummary()) {
        final Map<String, PathNode> ages = agePathsByArray(summary);
        final PathNode src = ages.get("src");
        final PathNode dst = ages.get("dst");

        assertFalse(src.isStatsCountDirty(),
                    "the source path's count was marked dirty by the move. Nothing ever clears "
                        + "that flag and the reader disqualifies every aggregate on a stale count, "
                        + "so one move disables summary-served aggregates for good");
        assertFalse(dst.isStatsCountDirty(), "the destination path's count was marked dirty");

        assertEquals(1L, src.getStatsValueCount(),
                     "the source still counts the record that left it");
        assertEquals(2L, src.getStatsSum(), "the source's sum still includes the value that left");
        assertEquals(2L, dst.getStatsValueCount(),
                     "the destination never counted the record that arrived");
        assertEquals(10L + 1L, dst.getStatsSum(),
                     "the destination's sum never gained the value that arrived");

        // Adding is monotone, so the destination's bounds stay exact through a move.
        assertFalse(dst.isStatsMinDirty(), "the destination's min should survive a move exactly");
        assertEquals(1L, dst.getStatsMin(), "the arriving 1 is the destination's new min");
        assertEquals(10L, dst.getStatsMax());
      }
    }
  }

  /**
   * {@code nullCount} has to move with the records too, and it is the one statistic where being
   * wrong costs answers rather than speed.
   *
   * <p>{@code SirixVectorizedExecutor#acceptsPredicate} declines the vectorized path for an
   * ORDERING comparison against any path whose {@code nullCount} is above zero, because JSONiq
   * gives null a total order the kernels do not implement. Over-counting there only costs the fast
   * path; UNDER-counting lets the gate admit a column that does contain nulls, which is a wrong
   * answer. A bare {@code null} array element is a plain {@code NULL_VALUE} record counted under
   * its PARENT's path — the attribution the transfer has to match, and the one it could get wrong.
   */
  @Test
  @DisplayName("a move carries the null count to the destination path")
  void moveCarriesNullCount() throws Exception {
    try (final var database = JsonTestHelper.getDatabaseWithResourceConfig(
        JsonTestHelper.PATHS.PATH1.getFile(), statsConfig());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      // Bare nulls, so they are plain NULL_VALUE records rather than fused OBJECT_NAMED_NULL ones.
      try (final var wtx = session.beginNodeTrx();
          final var reader = JsonShredder.createStringReader("{\"src\":[null,7],\"dst\":[9]}")) {
        wtx.insertSubtreeAsFirstChild(reader);
        wtx.commit();
      }

      // Baseline: the bare null has to be counted before a move can carry it. Array ELEMENTS are
      // counted on the array's "__array__" child path node, not on the named path node itself, so
      // that is where the null lands.
      try (final var summary = session.openPathSummary()) {
        assertEquals(1L, elementPathOf(summary, "src").getStatsNullCount(),
                     "the shredded bare null was never counted under its array's element path");
        assertEquals(0L, elementPathOf(summary, "dst").getStatsNullCount());
      }

      try (final var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveToDocumentRoot());
        assertTrue(wtx.moveToFirstChild());
        assertTrue(wtx.moveToFirstChild());        // "src"
        final long srcArrayKey = wtx.getNodeKey();
        assertTrue(wtx.moveToRightSibling());      // "dst"
        final long dstArrayKey = wtx.getNodeKey();

        assertTrue(wtx.moveTo(srcArrayKey));
        assertTrue(wtx.moveToFirstChild());        // the null element
        final long nullKey = wtx.getNodeKey();

        assertTrue(wtx.moveTo(dstArrayKey));
        wtx.moveSubtreeToFirstChild(nullKey);
        wtx.commit();
      }

      try (final var summary = session.openPathSummary()) {
        assertEquals(0L, elementPathOf(summary, "src").getStatsNullCount(),
                     "the source still counts a null that moved away — an over-count only costs "
                         + "the fast path, but it should be exact");
        assertEquals(1L, elementPathOf(summary, "dst").getStatsNullCount(),
                     "the destination never counted the null that arrived. Under-counting is the "
                         + "unsafe direction: the ordering-comparison gate then admits a column "
                         + "that holds nulls, and the kernels do not implement their total order");
      }
    }
  }

  /**
   * The same for a move that changes nothing about the path. The subtree keeps its path class, so
   * subtracting and re-adding has to land back on the identical numbers — a re-attribution that
   * double-counts or drops is most visible here, where the answer is known exactly.
   */
  @Test
  @DisplayName("a move within one array leaves the statistics identical")
  void moveWithinTheSameArrayIsANoOpForStatistics() throws Exception {
    try (final var database = JsonTestHelper.getDatabaseWithResourceConfig(
        JsonTestHelper.PATHS.PATH1.getFile(), statsConfig());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      try (final var wtx = session.beginNodeTrx();
          final var reader = JsonShredder.createStringReader(JSON)) {
        wtx.insertSubtreeAsFirstChild(reader);
        wtx.commit();
      }

      try (final var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveToDocumentRoot());
        assertTrue(wtx.moveToFirstChild());
        assertTrue(wtx.moveToFirstChild());        // "src"
        final long srcArrayKey = wtx.getNodeKey();
        assertTrue(wtx.moveToFirstChild());        // {"age":1}
        assertTrue(wtx.moveToRightSibling());      // {"age":2}
        final long lastKey = wtx.getNodeKey();

        assertTrue(wtx.moveTo(srcArrayKey));
        wtx.moveSubtreeToFirstChild(lastKey);      // reorder within the same array
        wtx.commit();
      }

      try (final var summary = session.openPathSummary()) {
        final PathNode src = agePathsByArray(summary).get("src");
        assertNotNull(src);
        assertFalse(src.isStatsCountDirty(), "a reorder inside one array must not stale the count");
        assertEquals(2L, src.getStatsValueCount(), "a reorder changes no counts");
        assertEquals(1L + 2L, src.getStatsSum(), "a reorder changes no sums");
      }
    }
  }
}
