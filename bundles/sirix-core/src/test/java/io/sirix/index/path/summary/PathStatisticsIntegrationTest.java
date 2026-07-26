package io.sirix.index.path.summary;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test for the PathStatistics feature: shreds a small JSON with stats
 * enabled, then verifies the PathSummary's per-path stats reflect the inserted
 * values exactly.
 */
final class PathStatisticsIntegrationTest {

  private static final String JSON =
      "[{\"age\":1,\"dept\":\"eng\"},{\"age\":5,\"dept\":\"eng\"},{\"age\":3,\"dept\":\"sales\"}]";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void shredJson_populatesPerPathStats() throws Exception {
    final var config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
        .buildPathSummary(true)
        .buildPathStatistics(true)
        .build();

    try (final var database = JsonTestHelper.getDatabaseWithResourceConfig(
        JsonTestHelper.PATHS.PATH1.getFile(), config);
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      try (final var wtx = session.beginNodeTrx();
          final var reader = JsonShredder.createStringReader(JSON)) {
        wtx.insertSubtreeAsFirstChild(reader);
        wtx.commit();
      }

      try (final var summary = session.openPathSummary()) {
        final List<PathNode> agePaths = summary.findPathsByLocalName("age");
        assertEquals(1, agePaths.size(), "expected one path node for 'age'");
        final PathNode agePath = agePaths.getFirst();
        assertEquals(3L, agePath.getStatsValueCount(), "count of age values");
        assertEquals(1L + 5L + 3L, agePath.getStatsSum(), "sum of age values");
        assertEquals(1L, agePath.getStatsMin(), "min of age values");
        assertEquals(5L, agePath.getStatsMax(), "max of age values");
        assertFalse(agePath.isStatsMinDirty());
        assertFalse(agePath.isStatsMaxDirty());
        assertNotNull(agePath.getHllSketch(), "HLL sketch should be populated");
        final long ageDistinctEst = agePath.getHllSketch().estimate();
        assertTrue(ageDistinctEst >= 2 && ageDistinctEst <= 4,
            "HLL estimate for 3 distinct ages should be near 3, was " + ageDistinctEst);

        final List<PathNode> deptPaths = summary.findPathsByLocalName("dept");
        assertEquals(1, deptPaths.size(), "expected one path node for 'dept'");
        final PathNode deptPath = deptPaths.getFirst();
        assertEquals(3L, deptPath.getStatsValueCount());
        assertNotNull(deptPath.getStatsMinBytes());
        assertNotNull(deptPath.getStatsMaxBytes());
        assertEquals("eng", new String(deptPath.getStatsMinBytes()));
        assertEquals("sales", new String(deptPath.getStatsMaxBytes()));
        assertNotNull(deptPath.getHllSketch());
        assertEquals(2L, deptPath.getHllSketch().estimate(),
            "HLL should nail 2 distinct values via linear counting");
      }
    }
  }

  @Test
  void shredJson_withStatsDisabled_leavesStatsEmpty() throws Exception {
    final var config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
        .buildPathSummary(true)
        // buildPathStatistics defaults to false
        .build();

    try (final var database = JsonTestHelper.getDatabaseWithResourceConfig(
        JsonTestHelper.PATHS.PATH1.getFile(), config);
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      try (final var wtx = session.beginNodeTrx();
          final var reader = JsonShredder.createStringReader(JSON)) {
        wtx.insertSubtreeAsFirstChild(reader);
        wtx.commit();
      }

      try (final var summary = session.openPathSummary()) {
        final List<PathNode> agePaths = summary.findPathsByLocalName("age");
        assertFalse(agePaths.isEmpty(), "path node exists");
        final PathNode agePath = agePaths.getFirst();
        assertEquals(0L, agePath.getStatsValueCount(),
            "stats disabled → count stays 0 even though path node exists");
      }
    }
  }
}
