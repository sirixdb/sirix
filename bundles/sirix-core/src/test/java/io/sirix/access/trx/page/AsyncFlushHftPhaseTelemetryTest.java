package io.sirix.access.trx.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/** Focused opt-in gate for the primitive async-epoch phase attribution. */
final class AsyncFlushHftPhaseTelemetryTest {

  private static final String RESOURCE = "async-flush-hft-phase-telemetry";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
  }

  @AfterEach
  void tearDown() {
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
    JsonTestHelper.deleteEverything();
  }

  @Test
  @EnabledIfSystemProperty(named = "sirix.hft.telemetry", matches = "true")
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void slowestAndBlockingEpochsRetainACompletePhaseTuple() throws ReflectiveOperationException {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final NodeStorageEngineWriter measuredWriter;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());

      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(256, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        measuredWriter = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        for (int index = 0; index < 5_000; index++) {
          wtx.moveTo(arrayNodeKey);
          wtx.insertStringValueAsFirstChild("value-" + index);
        }
        wtx.commit();
      }
    }

    final long epochCount = longField(measuredWriter, "hftEpochSequence");
    final long maxWorkerEpoch = longField(measuredWriter, "hftMaxWorkerEpochId");
    final long maxBlockedEpoch = longField(measuredWriter, "hftMaxBlockedEpochId");
    assertTrue(epochCount > 1L, "the fixture must rotate more than once");
    assertTrue(maxWorkerEpoch > 0L && maxWorkerEpoch <= epochCount);
    assertTrue(maxBlockedEpoch > 0L && maxBlockedEpoch <= epochCount);
    assertTrue(longField(measuredWriter, "hftMaxWorkerNanos") > 0L);
    assertTrue(longField(measuredWriter, "hftMaxWorkerEpochQueueWaitNanos") >= 0L);
    assertTrue(longField(measuredWriter, "hftMaxWorkerEpochSideNanos") >= 0L);
    assertTrue(longField(measuredWriter, "hftMaxWorkerEpochSerializeJoinWaitNanos") >= 0L);
    assertTrue(longField(measuredWriter, "hftMaxWorkerEpochKvlAppendNanos") > 0L);
    assertTrue(longField(measuredWriter, "hftMaxWorkerEpochFinalFlushNanos") > 0L);
    assertTrue(longField(measuredWriter, "hftMaxBlockedEpochForegroundWaitNanos") > 0L);
    assertTrue(longField(measuredWriter,
        "hftMaxSnapshotKvlAttemptedPages") <= NodeStorageEngineWriter.MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT);
    assertEquals(longField(measuredWriter, "hftCombinedEpochs"), longField(measuredWriter, "hftForegroundFlushCount"));
    assertTrue(longField(measuredWriter, "hftMaxForegroundFlushNanos") > 0L);
    assertTrue(longField(measuredWriter, "hftForegroundFlushNanos") >= longField(measuredWriter,
        "hftMaxForegroundFlushNanos"));
    assertTrue(booleanField(measuredWriter, "hftMaxWorkerEpochDataGrowExact"));
    assertTrue(booleanField(measuredWriter, "hftMaxBlockedEpochDataGrowExact"));

    // The per-epoch tuples above retain the slowest and the most-blocking epoch, which on a bulk
    // import is the JIT-warm-up epoch in both cases; the run totals are what attributes the steady
    // state. Each total sums every worker run INCLUDING the retained maximum, so it can never be
    // smaller than it — an accumulation that is dropped or wired to the wrong phase reads as zero
    // against a positive maximum and fails here rather than silently reporting a flat profile.
    assertTrue(longField(measuredWriter, "hftSerializeJoinWaitNanosTotal") >= longField(measuredWriter,
        "hftMaxWorkerEpochSerializeJoinWaitNanos"), "serialize-join total must cover the slowest epoch's share");
    assertTrue(longField(measuredWriter, "hftKvlAppendNanosTotal") >= longField(measuredWriter,
        "hftMaxWorkerEpochKvlAppendNanos"), "KVL-append total must cover the slowest epoch's share");
    assertTrue(
        longField(measuredWriter, "hftSideNanosTotal") >= longField(measuredWriter, "hftMaxWorkerEpochSideNanos"),
        "side-page total must cover the slowest epoch's share");
    assertTrue(longField(measuredWriter, "hftFinalFlushNanosTotal") >= longField(measuredWriter,
        "hftMaxWorkerEpochFinalFlushNanos"), "final-flush total must cover the slowest epoch's share");
    // Both phases run in every combined epoch, so with more than one epoch the totals must exceed
    // any single epoch's contribution rather than merely equal the retained maximum.
    assertTrue(longField(measuredWriter, "hftKvlAppendNanosTotal") > longField(measuredWriter,
        "hftMaxWorkerEpochKvlAppendNanos"), "several epochs appended, so the total must exceed one epoch");
  }

  @Test
  @EnabledIfSystemProperty(named = "sirix.hft.telemetry", matches = "true")
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void failedEpochIdentityIsConsumedBeforeTeardownReacquiresThePermit() throws ReflectiveOperationException {
    final AtomicReference<NodeStorageEngineWriter> measuredWriter = new AtomicReference<>();
    NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
      if ("write".equals(site)) {
        measuredWriter.compareAndSet(null, writer);
        throw new IllegalStateException("injected HFT epoch failure");
      }
    };

    assertThrows(Throwable.class, () -> runFailingImport(64, 1_000));

    final NodeStorageEngineWriter writer = measuredWriter.get();
    assertNotNull(writer, "the background epoch fault must fire");
    assertTrue(longField(writer, "hftMaxBlockedEpochId") > 0L,
        "the failed worker completion must be attributed by its first permit acquire");
    assertEquals(0L, longField(writer, "hftCompletedEpochId"),
        "rollback/close must not retain and reattribute an already-acquired completion identity");
  }

  private static void runFailingImport(final int flushThreshold, final int insertedRecords) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());

      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(flushThreshold, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        for (int index = 0; index < insertedRecords; index++) {
          wtx.moveTo(arrayNodeKey);
          wtx.insertStringValueAsFirstChild("failed-value-" + index);
        }
        wtx.commit();
      }
    }
  }

  private static long longField(final NodeStorageEngineWriter writer, final String name)
      throws ReflectiveOperationException {
    final Field field = NodeStorageEngineWriter.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.getLong(writer);
  }

  private static boolean booleanField(final NodeStorageEngineWriter writer, final String name)
      throws ReflectiveOperationException {
    final Field field = NodeStorageEngineWriter.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.getBoolean(writer);
  }
}
