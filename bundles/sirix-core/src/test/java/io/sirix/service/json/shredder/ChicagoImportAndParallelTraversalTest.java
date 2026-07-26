package io.sirix.service.json.shredder;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.PostOrderAxis;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.settings.VersioningType;
import io.sirix.utils.LogWrapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Chicago-corpus scale gate: imports the ~4.4 GiB {@code cityofchicago.json} dataset with the
 * exact configuration of the manual {@link JsonShredderTest} harness (auto-committing epochs of
 * {@code (262_144 << 4) + 262_144} nodes — the same constant the {@code maxNodes} default in
 * {@code JsonNodeTrxImpl} is tuned for) and then verifies CONCURRENT read-only traversals over
 * the imported resource agree with each other and with a post-order reference walk.
 *
 * <p>Opt-in only — multi-minute runtime and the dataset is gitignored:
 *
 * <pre>
 *   ./gradlew :sirix-core:test --tests "*ChicagoImportAndParallelTraversalTest*" -Dsirix.chicago.run=true
 * </pre>
 */
public final class ChicagoImportAndParallelTraversalTest {

  private static final LogWrapper logger =
      new LogWrapper(LoggerFactory.getLogger(ChicagoImportAndParallelTraversalTest.class));

  private static final Path CHICAGO = Paths.get("src", "test", "resources", "json", "cityofchicago.json");

  /** Same auto-commit epoch size as the manual Chicago harness in {@link JsonShredderTest}. */
  private static final int MAX_NODES_PER_EPOCH = (262_144 << 4) + 262_144;

  private static final int PARALLELISM = Math.max(2, Runtime.getRuntime().availableProcessors());

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void importChicagoAndTraverseInParallel() throws Exception {
    assumeTrue("true".equals(System.getProperty("sirix.chicago.run")),
               "opt-in via -Dsirix.chicago.run=true (multi-minute, needs the 4.4 GiB Chicago dataset)");
    assumeTrue(Files.exists(CHICAGO), "cityofchicago.json not present under src/test/resources/json");

    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .buildPathSummary(true)
                                                   .storeDiffs(true)
                                                   .storeNodeHistory(false)
                                                   .storeChildCount(true)
                                                   .hashKind(HashType.ROLLING)
                                                   .useTextCompression(false)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .useDeweyIDs(false)
                                                   .byteHandlerPipeline(new ByteHandlerPipeline(new FFILz4Compressor()))
                                                   .build());

      final long importStart = System.nanoTime();
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx(MAX_NODES_PER_EPOCH)) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(CHICAGO));
      }
      logger.info("Chicago import done [" + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - importStart) + "s].");

      final long maxNodeKey;
      final int revisions;
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = session.beginNodeReadOnlyTrx()) {
        maxNodeKey = rtx.getMaxNodeKey();
        revisions = rtx.getRevisionNumber();
      }
      logger.info("maxNodeKey=" + maxNodeKey + ", revisions=" + revisions);

      // The corpus is far larger than one epoch, so the import MUST have auto-committed
      // intermediate revisions — that's the bulk-insert epoch path under test.
      assertTrue(revisions >= 2, "expected multiple auto-commit epochs, got revision " + revisions);
      assertTrue(maxNodeKey > 1_000_000L, "suspiciously small import: maxNodeKey=" + maxNodeKey);

      // Concurrent full traversals: one read-only trx per thread, all started together.
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final ExecutorService pool = Executors.newFixedThreadPool(PARALLELISM);
        try {
          final var barrier = new CyclicBarrier(PARALLELISM);
          final List<Future<Long>> futures = new ArrayList<>(PARALLELISM);
          final long traverseStart = System.nanoTime();
          for (int i = 0; i < PARALLELISM; i++) {
            futures.add(pool.submit(() -> {
              barrier.await();
              try (final var rtx = session.beginNodeReadOnlyTrx()) {
                final var axis = new DescendantAxis(rtx);
                long count = 0;
                while (axis.hasNext()) {
                  axis.nextLong();
                  count++;
                }
                return count;
              }
            }));
          }

          final long[] counts = new long[PARALLELISM];
          for (int i = 0; i < PARALLELISM; i++) {
            counts[i] = futures.get(i).get(30, TimeUnit.MINUTES);
          }
          logger.info("parallel descendant traversals (" + PARALLELISM + " threads) done [" + TimeUnit.NANOSECONDS.toSeconds(
              System.nanoTime() - traverseStart) + "s], count=" + counts[0]);

          for (int i = 1; i < PARALLELISM; i++) {
            assertEquals(counts[0], counts[i], "thread " + i + " saw a different node count");
          }
          assertTrue(counts[0] > 1_000_000L, "suspiciously small traversal: " + counts[0]);

          // Post-order reference walk must visit exactly the same number of nodes.
          try (final var rtx = session.beginNodeReadOnlyTrx()) {
            final long postOrderStart = System.nanoTime();
            final var axis = new PostOrderAxis(rtx);
            long postOrderCount = 0;
            while (axis.hasNext()) {
              axis.nextLong();
              postOrderCount++;
            }
            logger.info("post-order traversal done [" + TimeUnit.NANOSECONDS.toSeconds(
                System.nanoTime() - postOrderStart) + "s], count=" + postOrderCount);
            assertEquals(counts[0], postOrderCount, "post-order count diverges from descendant-axis count");
          }
        } finally {
          pool.shutdownNow();
        }
      }
    }
  }
}
