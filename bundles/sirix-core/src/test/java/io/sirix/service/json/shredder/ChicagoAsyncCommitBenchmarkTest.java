package io.sirix.service.json.shredder;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.node.NodeKind;
import io.sirix.settings.VersioningType;
import io.sirix.utils.LogWrapper;
import org.checkerframework.org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Benchmark for importing the full City of Chicago crime dataset (~3.6 GB, ~7M lines)
 * using the async auto-commit mechanism.
 *
 * <p>The async commit mechanism pipelines serialization with fsync:
 * when auto-committing, fsync runs asynchronously so that the next
 * commit's page serialization overlaps with the previous commit's
 * disk I/O.</p>
 *
 * <p><b>Note:</b> The full Chicago dataset cannot be imported without
 * auto-commit — the Transaction Intent Log (TIL) grows to exceed available
 * heap memory. Auto-commit periodically flushes the TIL, keeping memory
 * bounded. This is a key benefit of the async auto-commit mechanism.</p>
 *
 * <p>Tests using the full dataset are {@link Disabled} by default because
 * they require the {@code cityofchicago.json} file (~3.6 GB) and take
 * several minutes to run. The subset tests run on every build.</p>
 */
public final class ChicagoAsyncCommitBenchmarkTest {

  private static final LogWrapper logger =
      new LogWrapper(LoggerFactory.getLogger(ChicagoAsyncCommitBenchmarkTest.class));

  private static final Path JSON = Paths.get("src", "test", "resources", "json");
  private static final Path CHICAGO_JSON = JSON.resolve("cityofchicago.json");

  /**
   * Auto-commit threshold: number of node modifications before an intermediate
   * commit is triggered. 262144 << 3 = 2,097,152 — matches the existing
   * Jackson shredder Chicago test.
   */
  private static final int AUTO_COMMIT_NODE_COUNT = 262_144 << 3;

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  // ==================== Full dataset benchmark (manual) ====================

  /**
   * Import the full Chicago dataset (~3.6 GB, ~310M nodes) with async auto-commit.
   *
   * <p>The transaction auto-commits every {@value #AUTO_COMMIT_NODE_COUNT}
   * modifications. Each intermediate commit uses async fsync (pipelined I/O),
   * overlapping the next commit's page serialization with the current
   * commit's disk flush.</p>
   *
   * <p>On a typical development machine (NVMe SSD, 8+ cores, 12 GB heap) this
   * completes in ~3 minutes, producing ~310M nodes across multiple revisions.</p>
   */
  @Disabled("Manual benchmark — requires cityofchicago.json (~3.6 GB)")
  @Test
  public void testImportFullChicagoAsyncAutoCommit() {
    logger.info("=== FULL CHICAGO IMPORT WITH ASYNC AUTO-COMMIT ===");
    final var stopWatch = new StopWatch();
    stopWatch.start();

    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(buildResourceConfig());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx(AUTO_COMMIT_NODE_COUNT, AfterCommitState.KEEP_OPEN_ASYNC);
           final var parser = JacksonJsonShredder.createFileParser(CHICAGO_JSON)) {
        trx.insertSubtreeAsFirstChild(parser, JsonNodeTrx.Commit.NO);
        trx.commit();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // Verify data integrity
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        final long maxNodeKey = rtx.getMaxNodeKey();
        assertTrue(maxNodeKey > 1_000_000,
            "Full Chicago import should produce >1M nodes, got " + maxNodeKey);

        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild(), "Should have root object");

        boolean foundData = false;
        if (rtx.moveToFirstChild()) {
          do {
            if (rtx.getKind() == NodeKind.OBJECT_KEY
                && "data".equals(rtx.getName().getLocalName())) {
              foundData = true;
              assertTrue(rtx.moveToFirstChild(), "data key should have array value");
              assertTrue(rtx.getChildCount() > 100_000,
                  "Full Chicago data array should have >100K records");
              logger.info("Data array child count: " + rtx.getChildCount());
              break;
            }
          } while (rtx.moveToRightSibling());
        }
        assertTrue(foundData, "Should find 'data' key in Chicago dataset");

        stopWatch.stop();
        final long elapsedMs = stopWatch.getTime(TimeUnit.MILLISECONDS);
        final int revisions = manager.getMostRecentRevisionNumber();
        logger.info("=== RESULTS ===");
        logger.info("  Total time:     " + elapsedMs + " ms ("
            + stopWatch.getTime(TimeUnit.SECONDS) + "s)");
        logger.info("  Max node key:   " + maxNodeKey);
        logger.info("  Revisions:      " + revisions
            + " (auto-commit every " + AUTO_COMMIT_NODE_COUNT + " modifications)");
        logger.info("  Throughput:     " + String.format("%.0f", maxNodeKey * 1000.0 / elapsedMs)
            + " nodes/sec");
      }
    }
  }

  // ==================== Subset tests (CI-safe) ====================

  /**
   * Import chicago-subset.json with async auto-commit using the Jackson shredder,
   * then verify data integrity. Runs on every build as a quick sanity check.
   */
  @Test
  public void testImportChicagoSubsetAsyncAutoCommit() throws IOException {
    final var subsetPath = JSON.resolve("chicago-subset.json");

    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(buildResourceConfig());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx(512, AfterCommitState.KEEP_OPEN_ASYNC);
           final var parser = JacksonJsonShredder.createFileParser(subsetPath)) {
        trx.insertSubtreeAsFirstChild(parser, JsonNodeTrx.Commit.NO);
        trx.commit();
      }

      // Verify data integrity
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        final long maxNodeKey = rtx.getMaxNodeKey();
        assertTrue(maxNodeKey > 100,
            "Subset import should produce >100 nodes, got " + maxNodeKey);

        // Navigate to "data" array and verify 101 records
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild(), "Should have root object");

        boolean foundData = false;
        if (rtx.moveToFirstChild()) {
          do {
            if (rtx.getKind() == NodeKind.OBJECT_KEY
                && "data".equals(rtx.getName().getLocalName())) {
              foundData = true;
              assertTrue(rtx.moveToFirstChild(), "data key should have array value");
              assertEquals(101, rtx.getChildCount(),
                  "Chicago subset data array should have 101 children");
              break;
            }
          } while (rtx.moveToRightSibling());
        }
        assertTrue(foundData, "Should find 'data' key in Chicago subset");

        logger.info("Subset async import OK: maxNodeKey=" + maxNodeKey);
      }
    }
  }

  /**
   * Verify that async auto-commit produces the same serialized JSON as
   * a plain synchronous import of the chicago-subset.
   */
  @Test
  public void testSubsetAsyncVsSyncProducesIdenticalOutput() throws IOException {
    final var subsetPath = JSON.resolve("chicago-subset.json");

    // --- Sync import (baseline) ---
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final String syncJson;
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(buildResourceConfig());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx();
           final var parser = JacksonJsonShredder.createFileParser(subsetPath)) {
        trx.insertSubtreeAsFirstChild(parser);
      }

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        syncJson = serializeAll(manager);
      }
    }

    // --- Async auto-commit import ---
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final String asyncJson;
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(buildResourceConfig());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx(512, AfterCommitState.KEEP_OPEN_ASYNC);
           final var parser = JacksonJsonShredder.createFileParser(subsetPath)) {
        trx.insertSubtreeAsFirstChild(parser, JsonNodeTrx.Commit.NO);
        trx.commit();
      }

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        asyncJson = serializeAll(manager);
      }
    }

    assertEquals(syncJson, asyncJson,
        "Async auto-commit must produce identical JSON to synchronous import");
    logger.info("Subset sync vs async: output is identical ("
        + syncJson.length() + " chars)");
  }

  // ==================== Internal helpers ====================

  private String serializeAll(final io.sirix.api.json.JsonResourceSession session) {
    try (final var writer = new java.io.StringWriter()) {
      new io.sirix.service.json.serialize.JsonSerializer.Builder(session, writer)
          .build().call();
      return writer.toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  //@Disabled("Manual benchmark — requires cityofchicago.json (~3.6 GB)")
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  void thresholdSweep(final int shift) {
    final int threshold = 262_144 << shift;
    logger.info("=== THRESHOLD SWEEP: 262144 << " + shift + " = " + threshold + " ===");

    JsonTestHelper.deleteEverything();

    // Drop OS page cache between runs for fair comparison
    try {
      new ProcessBuilder("sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches")
          .inheritIO().start().waitFor();
      logger.info("  OS page cache cleared");
    } catch (Exception e) {
      logger.warn("  Could not clear OS page cache: " + e.getMessage());
    }

    final var stopWatch = new StopWatch();
    stopWatch.start();

    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(buildResourceConfig());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx(threshold, AfterCommitState.KEEP_OPEN_ASYNC);
           final var parser = JacksonJsonShredder.createFileParser(CHICAGO_JSON)) {
        trx.insertSubtreeAsFirstChild(parser, JsonNodeTrx.Commit.NO);
        trx.commit();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        stopWatch.stop();
        final long maxNodeKey = rtx.getMaxNodeKey();
        final long elapsedMs = stopWatch.getTime(TimeUnit.MILLISECONDS);
        final int revisions = manager.getMostRecentRevisionNumber();
        logger.info("  Threshold:  " + threshold + " (262144 << " + shift + ")");
        logger.info("  Time:       " + elapsedMs + " ms (" + stopWatch.getTime(TimeUnit.SECONDS) + "s)");
        logger.info("  Nodes:      " + maxNodeKey);
        logger.info("  Revisions:  " + revisions);
        logger.info("  Throughput: " + String.format("%.0f", maxNodeKey * 1000.0 / elapsedMs) + " nodes/sec");
      }
    }
  }

  private ResourceConfiguration buildResourceConfig() {
    return ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
        .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
        .buildPathSummary(true)
        .storeDiffs(true)
        .storeNodeHistory(false)
        .storeChildCount(true)
        .hashKind(HashType.ROLLING)
        .useTextCompression(false)
        .storageType(StorageType.MEMORY_MAPPED)
        .useDeweyIDs(false)
        .byteHandlerPipeline(new ByteHandlerPipeline(new FFILz4Compressor()))
        .build();
  }
}
