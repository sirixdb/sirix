/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.path.summary;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Removing an observation has to cancel it EXACTLY, for every {@code long} without exception.
 *
 * <p>
 * {@code Long.MIN_VALUE} is the exception a negate-and-add implementation has: {@code -Long.MIN_VALUE
 * == Long.MIN_VALUE}, so cancelling it that way adds it a second time. Both supported routes reach
 * that code — a delete and a subtree move, which subtract through the same
 * {@code PathSummaryWriter.removeValue} — and the damage is invisible because the corrupted total can
 * be perfectly representable, so the derived trust verdict approves it and the summary serves a sum
 * the scan disagrees with.
 *
 * <p>
 * Both assertions below read through {@link PathNode#isStatsSumTrustworthy()} and
 * {@link PathNode#getStatsSum()} — the pair the vectorized executor's aggregate short-circuit
 * consults before answering {@code sum()} from the summary.
 */
final class PathStatsRemoveExactnessTest {

  private static final long MIN = Long.MIN_VALUE;
  private static final long M = Long.MAX_VALUE;

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

  /** Delete route: the remaining total is representable, so the exact value must be served. */
  @Test
  void deletingLongMinValueCancelsItExactly() {
    try (var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(),
        statsConfig()); var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      insert(session, "[{\"v\":" + MIN + "},{\"v\":1}]");
      removeFirstMember(session);

      try (var summary = session.openPathSummary()) {
        final PathNode v = onlyPathNamed(summary, "v");
        assertEquals(1L, v.getStatsValueCount(), "one observation remains");
        assertTrue(v.isStatsSumTrustworthy(),
            "the remaining total is exactly 1 — a delete must not leave the accumulator unservable");
        assertEquals(1L, v.getStatsSum(), "removing Long.MIN_VALUE must subtract it, not add it again");
      }
    }
  }

  /**
   * Delete route, the dangerous direction: the corrupted total lands back INSIDE {@code long} range,
   * so nothing marks it and the wrong sum is served.
   */
  @Test
  void deletingLongMinValueDoesNotFabricateAServableTotal() {
    try (var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(),
        statsConfig()); var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      insert(session, "[{\"v\":" + MIN + "},{\"v\":" + M + "},{\"v\":" + M + "}]");
      removeFirstMember(session);

      try (var summary = session.openPathSummary()) {
        final PathNode v = onlyPathNamed(summary, "v");
        assertEquals(2L, v.getStatsValueCount(), "two observations remain");
        assertFalse(v.isStatsSumTrustworthy(),
            "the true remaining total is 2 * Long.MAX_VALUE, which no long can hold — serving anything "
                + "here means the delete corrupted the accumulator into a representable wrong answer");
      }
    }
  }

  /** Move route: {@code transferPathStatForRecord} subtracts from the source through the same code. */
  @Test
  void movingLongMinValueOutOfAPathCancelsItExactly() {
    try (var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(),
        statsConfig()); var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      insert(session, "{\"src\":[{\"v\":" + MIN + "},{\"v\":" + M + "},{\"v\":" + M + "}],\"dst\":[{\"v\":0}]}");

      try (var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveToDocumentRoot());
        assertTrue(wtx.moveToFirstChild());   // the top-level object
        assertTrue(wtx.moveToFirstChild());   // "src"
        final long srcArrayKey = wtx.getNodeKey();
        assertTrue(wtx.moveToRightSibling()); // "dst"
        final long dstArrayKey = wtx.getNodeKey();

        assertTrue(wtx.moveTo(srcArrayKey));
        assertTrue(wtx.moveToFirstChild());   // {"v":MIN}
        final long movedObjectKey = wtx.getNodeKey();

        assertTrue(wtx.moveTo(dstArrayKey));
        wtx.moveSubtreeToFirstChild(movedObjectKey);
        wtx.commit();
      }

      try (var summary = session.openPathSummary()) {
        final Map<String, PathNode> byArray = pathsNamedByEnclosingArray(summary);
        final PathNode src = byArray.get("src");
        final PathNode dst = byArray.get("dst");

        assertEquals(2L, src.getStatsValueCount(), "the source no longer counts the record that left");
        assertFalse(src.isStatsSumTrustworthy(),
            "the source's true remaining total is 2 * Long.MAX_VALUE — a move that added Long.MIN_VALUE "
                + "back instead of subtracting it lands on a representable wrong total");

        assertEquals(2L, dst.getStatsValueCount(), "the destination counted the record that arrived");
        assertTrue(dst.isStatsSumTrustworthy(), "0 + Long.MIN_VALUE fits a long");
        assertEquals(MIN, dst.getStatsSum(), "the destination's sum gained exactly the arriving value");
      }
    }
  }

  private static void insert(final JsonResourceSession session, final String json) {
    try (var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
      wtx.commit();
    }
  }

  private static void removeFirstMember(final JsonResourceSession session) {
    try (var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild()); // the array
      assertTrue(wtx.moveToFirstChild()); // its first member
      wtx.remove();
      wtx.commit();
    }
  }

  private static PathNode onlyPathNamed(final PathSummaryReader summary, final String name) {
    final List<PathNode> paths = summary.findPathsByLocalName(name);
    assertEquals(1, paths.size(), "expected exactly one path class for '" + name + "'");
    return paths.getFirst();
  }

  private static Map<String, PathNode> pathsNamedByEnclosingArray(final PathSummaryReader summary) {
    final Map<String, PathNode> byArray = new HashMap<>(4);
    for (final PathNode pathNode : summary.findPathsByLocalName("v")) {
      summary.moveTo(pathNode.getNodeKey());
      final String path = String.valueOf(summary.getPath());
      if (path.contains("src")) {
        byArray.put("src", pathNode);
      } else if (path.contains("dst")) {
        byArray.put("dst", pathNode);
      }
    }
    assertEquals(2, byArray.size(), "expected one 'v' path under each array, found " + byArray.keySet());
    return byArray;
  }
}
