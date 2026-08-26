/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.path.summary;

import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The persisted per-path statistics must be a function of the OBSERVED VALUES, not of how many
 * flushes or commits the ingestion happened to take.
 *
 * <p>
 * The witness is {@code [M, M, -M]} with {@code M = Long.MAX_VALUE}: the running total leaves
 * {@code long} range after the second value and returns with the third. A 64-bit accumulator that
 * drops the overflowing addend answers this one way when everything lands in one flush and another
 * way when a commit boundary falls in the middle — same document, different persisted sum, different
 * serving decision. Both arms below load exactly the same values under exactly the same path class,
 * and every field a reader can see must agree after a cold reopen.
 *
 * <p>
 * This is the multi-flush half of the guarantee; {@link BulkPathStatsAccumulatorContractTest} pins
 * the within-batch half and {@link BulkPathStatsDifferentialTest} pins cursor-vs-bulk identity.
 */
final class PathStatsPersistedSumOrderIndependenceTest {

  private static final long M = Long.MAX_VALUE;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private record Snapshot(long count, long sum, long min, long max, boolean sumTrustworthy, boolean sumIntegral,
      boolean minDirty, boolean maxDirty, boolean countDirty) {
  }

  @Test
  void aCommitBoundaryInTheMiddleChangesNothing() {
    final Snapshot single = load(session -> {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"v\":" + M + "},{\"v\":" + M + "},{\"v\":-" + M + "}]"));
        wtx.commit();
      }
    });

    final Snapshot perValueCommits = load(session -> {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"v\":" + M + "}]"));
        wtx.commit();
      }
      appendMember(session, "{\"v\":" + M + "}");
      appendMember(session, "{\"v\":-" + M + "}");
    });

    assertEquals(single, perValueCommits, "the persisted statistics depend on the commit cadence");

    // ...and the values themselves are right: M + M - M is exactly M, which DOES fit a long.
    assertEquals(3L, single.count());
    assertEquals(M, single.sum());
    assertEquals(-M, single.min());
    assertEquals(M, single.max());
    assertTrue(single.sumTrustworthy(), "the true total fits a long, so it must stay servable");
    assertTrue(single.sumIntegral());
    assertFalse(single.countDirty());
  }

  /**
   * The companion direction: a total that genuinely leaves {@code long} range must be refused by both
   * cadences, so the widening cannot be mistaken for permission to serve a wrapped value.
   */
  @Test
  void aTrueOverflowIsRefusedUnderEitherCadence() {
    final Snapshot single = load(session -> {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"v\":" + M + "},{\"v\":" + M + "}]"));
        wtx.commit();
      }
    });

    final Snapshot perValueCommits = load(session -> {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"v\":" + M + "}]"));
        wtx.commit();
      }
      appendMember(session, "{\"v\":" + M + "}");
    });

    assertEquals(single, perValueCommits, "the persisted statistics depend on the commit cadence");
    assertFalse(single.sumTrustworthy(), "2 * Long.MAX_VALUE must not be servable as a long sum");
    assertEquals(2L, single.count(), "an unrepresentable sum must not disturb the count");
  }

  private interface Load {
    void run(JsonResourceSession session);
  }

  private static void appendMember(final JsonResourceSession session, final String member) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(member));
      wtx.commit();
    }
  }

  private static Snapshot load(final Load load) {
    JsonTestHelper.deleteEverything();
    final ResourceConfiguration config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                              .hashKind(HashType.NONE)
                                                              .storeNodeHistory(false)
                                                              .buildPathSummary(true)
                                                              .buildPathStatistics(true)
                                                              .build();
    try (Database<JsonResourceSession> database =
        JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config)) {
      try (JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        load.run(session);
      }
    }
    // Cold reopen: the comparison must see what a fresh reader deserializes, not writer state.
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        PathSummaryReader summary = session.openPathSummary()) {
      final List<PathNode> paths = summary.findPathsByLocalName("v");
      assertEquals(1, paths.size(), "expected exactly one path class for 'v'");
      final PathNode node = paths.getFirst();
      return new Snapshot(node.getStatsValueCount(), node.getStatsSum(), node.getStatsMin(), node.getStatsMax(),
          node.isStatsSumTrustworthy(), node.isStatsSumIntegral(), node.isStatsMinDirty(), node.isStatsMaxDirty(),
          node.isStatsCountDirty());
    }
  }
}
