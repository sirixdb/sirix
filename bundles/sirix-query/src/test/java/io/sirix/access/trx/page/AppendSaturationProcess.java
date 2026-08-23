/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import com.sun.management.HotSpotDiagnosticMXBean;
import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.Sequence;
import io.sirix.HftBoundaryTelemetry;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.GlobalValueDictionary;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexChangeListener;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.bench.clickbench.HftRuntimeEvidence;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.settings.VersioningType;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class AppendSaturationProcess {

  private AppendSaturationProcess() {
    throw new AssertionError("no instances");
  }

  public static void main(final String[] args) throws Exception {
    if (args.length != 2)
      throw new IllegalArgumentException("root and record count are required");
    final Path root = Path.of(args[0]);
    final int records = Integer.parseInt(args[1]);
    if (records <= 0)
      throw new IllegalArgumentException("record count must be positive");
    final int appendWorkers = Integer.getInteger("sirix.asyncFlush.appendParallelism", -1);
    final int queueCapacity = Integer.getInteger("sirix.asyncFlush.appendQueueCapacity", -1);
    final VersioningType versioningType =
        VersioningType.valueOf(System.getProperty("versioningType", VersioningType.FULL.name()));
    if (appendWorkers != 1 || queueCapacity != 1) {
      throw new IllegalArgumentException("canonical append saturation requires p=1/q=1");
    }
    final HftRuntimeEvidence.Build build = System.getProperty("sirix.hft.gitSha") == null
        ? null
        : HftRuntimeEvidence.capture(AppendSaturationProcess.class);
    final String hftConfiguration = build == null
        ? null
        : String.format(Locale.ROOT,
            "# HFT_SATURATION_CONFIG initialHeapBytes=%d maxHeapBytes=%d "
                + "maxNewSizeBytes=%d g1RegionSizeBytes=%d gcLogging=%s safepointLogging=%s",
            effectiveVmOption("InitialHeapSize"), effectiveVmOption("MaxHeapSize"), effectiveVmOption("MaxNewSize"),
            effectiveVmOption("G1HeapRegionSize"), build.gcLogging(), build.safepointLogging());
    if (build != null) {
      System.out.println("# HFT_BUILD gitSha=" + build.gitSha() + " artifactSha256=" + build.artifactSha256());
    }
    final int resources = 4;
    final String dataset = PinnedTrieProjectionSpillColdReopenTest.dataset(records);
    final Path[] locations = new Path[resources];
    final Path[] databasePaths = new Path[resources];
    for (int resource = 0; resource < resources; resource++) {
      locations[resource] = root.resolve("resource-" + resource);
      databasePaths[resource] = locations[resource].resolve("projection-spill-db");
    }
    final String[] projectionSourcePath = {"[]"};
    final long expectedSum = Math.multiplyExact((long) records, records - 1L) / 2L;
    final CountDownLatch ready = new CountDownLatch(resources);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch workerBlocked = new CountDownLatch(1);
    final CountDownLatch admissionAttempts = new CountDownLatch(resources);
    final CountDownLatch releaseWorker = new CountDownLatch(1);
    final AtomicBoolean blockFirst = new AtomicBoolean(true);
    final AtomicReference<Throwable> ownershipFailure = new AtomicReference<>();
    final ExecutorState saturatedState = new ExecutorState();
    final ExecutorState drainedState = new ExecutorState();
    NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
      if ("prepare".equals(site)) {
        admissionAttempts.countDown();
        return;
      }
      if (!"write".equals(site))
        return;
      if (!NodeStorageEngineWriter.isSnapshotAppendWorkerThread()) {
        ownershipFailure.compareAndSet(null, new AssertionError("append executed outside its worker"));
      }
      if (blockFirst.compareAndSet(true, false)) {
        workerBlocked.countDown();
        try {
          if (!releaseWorker.await(30, TimeUnit.SECONDS)) {
            throw new AssertionError("timed out holding the append worker");
          }
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new AssertionError("append worker interrupted", interrupted);
        }
      }
    };

    final ExecutorService executor = Executors.newFixedThreadPool(resources);
    final List<Future<?>> loads = new ArrayList<>(resources);
    try {
      for (int resource = 0; resource < resources; resource++) {
        final Path location = locations[resource];
        loads.add(executor.submit(() -> {
          ready.countDown();
          start.await();
          PinnedTrieProjectionSpillColdReopenTest.runProjectionLoad(location, versioningType, records, dataset, 64);
          return null;
        }));
      }
      if (!ready.await(30, TimeUnit.SECONDS))
        throw new AssertionError("resource loads did not become ready");
      HftBoundaryTelemetry.reset();
      ProjectionIndexChangeListener.resetMaintenanceTelemetry();
      GlobalValueDictionary.resetProbeTelemetry();
      NodeStorageEngineWriter.resetGlobalAppendTelemetry();
      System.out.println("# HFT_MEASURE_START");
      if (hftConfiguration != null) {
        System.out.println(hftConfiguration);
      }
      start.countDown();
      if (!workerBlocked.await(30, TimeUnit.SECONDS))
        throw new AssertionError("append worker was not occupied");
      if (!admissionAttempts.await(30, TimeUnit.SECONDS)) {
        throw new AssertionError("all resource writers did not reach append admission");
      }
      awaitSaturatedExecutor(resources, appendWorkers, queueCapacity, saturatedState);
      Thread.sleep(150L);
      releaseWorker.countDown();
      for (int loadIndex = 0; loadIndex < loads.size(); loadIndex++) {
        loads.get(loadIndex).get(90, TimeUnit.SECONDS);
      }
    } finally {
      releaseWorker.countDown();
      start.countDown();
      executor.shutdownNow();
      executor.awaitTermination(30, TimeUnit.SECONDS);
      NodeStorageEngineWriter.asyncFlushFaultHook = null;
    }

    if (ownershipFailure.get() != null)
      throw new AssertionError(ownershipFailure.get());
    if (NodeStorageEngineWriter.globalCallerAppendRuns() != 0L) {
      throw new AssertionError("append work ran on a caller thread");
    }
    if (NodeStorageEngineWriter.globalSubmitWaitCount() < resources
        || NodeStorageEngineWriter.globalSubmitWaitNanos() <= 0L
        || NodeStorageEngineWriter.globalSubmitWaitMaxNanos() < TimeUnit.MILLISECONDS.toNanos(100L)) {
      throw new AssertionError("the p=1/q=1 executor was not observably saturated");
    }

    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    int coldReopens = 0;
    for (int resource = 0; resource < resources; resource++) {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePaths[resource]);
          JsonResourceSession session = database.beginResourceSession("projection-spill.jn");
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final SirixVectorizedExecutor vectorized =
            new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
        try {
          final Sequence projected = vectorized.executeAggregate(null, projectionSourcePath, "sum", "id");
          if (!(projected instanceof Int64 sum) || sum.longValue() != expectedSum) {
            throw new AssertionError("cold projection result differs after saturated append admission");
          }
        } finally {
          vectorized.close();
        }
        coldReopens++;
      }
    }
    awaitDrainedExecutor(appendWorkers, queueCapacity, drainedState);

    System.out.printf(
        "# HFT_APPEND_SATURATION versioningType=%s resources=%d records=%d appendWorkers=%d queueCapacity=%d "
            + "callerThreadAppendRuns=%d submitWaitCount=%d submitWaitTotalNs=%d submitWaitMaxNs=%d "
            + "saturatedActiveWorkers=%d saturatedQueuedTasks=%d saturatedAdmissionWaiters=%d "
            + "saturatedAvailableAdmissions=%d drainedActiveWorkers=%d drainedQueuedTasks=%d "
            + "drainedAdmissionWaiters=%d drainedAvailableAdmissions=%d coldReopens=%d%n",
        versioningType, resources, records, appendWorkers, queueCapacity,
        NodeStorageEngineWriter.globalCallerAppendRuns(), NodeStorageEngineWriter.globalSubmitWaitCount(),
        NodeStorageEngineWriter.globalSubmitWaitNanos(), NodeStorageEngineWriter.globalSubmitWaitMaxNanos(),
        saturatedState.activeWorkers(), saturatedState.queuedTasks(), saturatedState.admissionWaiters(),
        saturatedState.availableAdmissions(), drainedState.activeWorkers(), drainedState.queuedTasks(),
        drainedState.admissionWaiters(), drainedState.availableAdmissions(), coldReopens);
    System.out.println("# HFT_MEASURE_END");
  }

  private static long effectiveVmOption(final String name) {
    final HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
    if (bean == null) {
      throw new IllegalStateException("HotSpotDiagnosticMXBean is unavailable");
    }
    return Long.parseLong(bean.getVMOption(name).getValue());
  }

  private static void awaitSaturatedExecutor(final int resources, final int appendWorkers, final int queueCapacity,
      final ExecutorState state) throws InterruptedException {
    final int requiredWaiters = resources - appendWorkers - queueCapacity;
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
    do {
      state.capture();
      if (state.activeWorkers() == appendWorkers && state.queuedTasks() == queueCapacity
          && state.admissionWaiters() >= requiredWaiters && state.availableAdmissions() == 0) {
        return;
      }
      Thread.sleep(1L);
    } while (System.nanoTime() < deadline);
    throw new AssertionError("append executor did not become saturated: " + state);
  }

  private static void awaitDrainedExecutor(final int appendWorkers, final int queueCapacity, final ExecutorState state)
      throws InterruptedException {
    final int admissionCapacity = Math.addExact(appendWorkers, queueCapacity);
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
    do {
      state.capture();
      if (state.activeWorkers() == 0 && state.queuedTasks() == 0 && state.admissionWaiters() == 0
          && state.availableAdmissions() == admissionCapacity) {
        return;
      }
      Thread.sleep(1L);
    } while (System.nanoTime() < deadline);
    throw new AssertionError("append executor did not become drained: " + state);
  }

  /** Reused sampler so the 1 ms admission-state poll creates no per-sample garbage. */
  private static final class ExecutorState {
    private int activeWorkers;
    private int queuedTasks;
    private int admissionWaiters;
    private int availableAdmissions;

    private void capture() {
      activeWorkers = NodeStorageEngineWriter.snapshotAppendActiveWorkers();
      queuedTasks = NodeStorageEngineWriter.snapshotAppendQueuedTasks();
      admissionWaiters = NodeStorageEngineWriter.snapshotAppendAdmissionWaiters();
      availableAdmissions = NodeStorageEngineWriter.snapshotAppendAvailableAdmissions();
    }

    private int activeWorkers() {
      return activeWorkers;
    }

    private int queuedTasks() {
      return queuedTasks;
    }

    private int admissionWaiters() {
      return admissionWaiters;
    }

    private int availableAdmissions() {
      return availableAdmissions;
    }

    @Override
    public String toString() {
      return "ExecutorState[activeWorkers=" + activeWorkers + ", queuedTasks=" + queuedTasks + ", admissionWaiters="
          + admissionWaiters + ", availableAdmissions=" + availableAdmissions + ']';
    }
  }
}
