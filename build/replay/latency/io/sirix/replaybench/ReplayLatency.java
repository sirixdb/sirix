package io.sirix.replaybench;

import com.sun.management.ThreadMXBean;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonResourceCopy;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;

import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Standalone campaign harness; no timing assertions, no changes to production configuration. */
public final class ReplayLatency {
  private static final ThreadMXBean ALLOCATION = (ThreadMXBean) ManagementFactory.getThreadMXBean();
  private static final String[] METRICS = {"bulkAppendCommit", "publicCompactDiff", "publicMaterializedDiff",
      "replayRead", "unchangedPublicDiff", "revisionCopy"};
  private static final String[] COUNTERS = {"recordVisits", "createdIdentities", "stagedRecords",
      "ancestorSteps", "sidecarReads", "fallbackPages", "pathSteps", "bookkeepingOperations"};
  private static volatile int sink;

  private ReplayLatency() {}

  @FunctionalInterface
  private interface Operation { void run() throws Exception; }

  public static void main(final String[] args) throws Exception {
    if (args.length != 4) throw new IllegalArgumentException("label directory warmups samples");
    final String label = args[0];
    final Path directory = Path.of(args[1]);
    final int warmups = Integer.parseInt(args[2]);
    final int samples = Integer.parseInt(args[3]);
    final String[] scenarios = System.getProperty("sirix.replay.bench.scenarios", "append").split(",");
    Files.createDirectories(directory);
    ALLOCATION.setThreadAllocatedMemoryEnabled(true);
    final Method delta = deltaReader();
    final Method[] counters = counters();
    System.out.println("RUN," + label + ",pid=" + ProcessHandle.current().pid() + ",java=" + System.getProperty("java.version")
        + ",identity=" + (delta != null) + ",warmups=" + warmups + ",samples=" + samples);
    System.out.println("JVM," + label + ",maxHeap=" + Runtime.getRuntime().maxMemory() + ",arguments="
        + ManagementFactory.getRuntimeMXBean().getInputArguments());
    System.out.println("COLUMNS,label,scenario,iteration,metric,nanos,threadAllocatedBytes," + String.join(",", COUNTERS));
    for (final String scenario : scenarios) {
      final String initial = initial(scenario);
      final String appended = array(scenario.equals("append") ? 4096 : scenario.equals("unchanged") ? 8 : 3);
      for (int iteration = -warmups; iteration < samples; iteration++) {
        final Path sourcePath = directory.resolve(scenario + "-source");
        final Path targetPath = directory.resolve(scenario + "-target");
        Databases.removeDatabase(sourcePath);
        Databases.removeDatabase(targetPath);
        Databases.clearGlobalCaches();
        final long[][] work = new long[METRICS.length][2 + COUNTERS.length];
        String copyFailure = null;
        try (final var sourceDb = create(sourcePath); final var source = sourceDb.beginResourceSession("resource")) {
          try (final var writer = source.beginNodeTrx()) {
            writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(initial), JsonNodeTrx.Commit.NO);
            writer.commit();
            measure(work[0], counters, () -> {
              mutate(scenario, writer, appended);
              writer.commit();
            });
            if (scenario.equals("restore")) writer.revertTo(1);
            writer.commit();
          }
          final var diff = new BasicJsonDiff(sourceDb.getName());
          measure(work[1], counters, () -> sink ^= diff.generateDiff(source, 1, 2, 0, 0, false).length());
          measure(work[2], counters, () -> sink ^= diff.generateDiff(source, 1, 2, 0, 0, true).length());
          measure(work[3], counters, () -> {
            if (delta == null) {
              sink ^= diff.generateDiffForReplay(source, 1, 2).length();
            } else {
              try (final var before = source.beginNodeReadOnlyTrx(1); final var after = source.beginNodeReadOnlyTrx(2)) {
                sink ^= System.identityHashCode(delta.invoke(null, before, after, 2));
              }
            }
          });
          measure(work[4], counters, () -> sink ^= diff.generateDiff(source, 2, 3, 0, 0, false).length());
          try (final var targetDb = create(targetPath); final var target = targetDb.beginResourceSession("resource");
              final var reader = source.beginNodeReadOnlyTrx(1); final var writer = target.beginNodeTrx()) {
            try {
              measure(work[5], counters, () -> new JsonResourceCopy.Builder(writer, reader, InsertPosition.AS_FIRST_CHILD)
                  .copyAllRevisionsUpToMostRecent().build().call());
              if (target.getMostRecentRevisionNumber() != 3) throw new IllegalStateException("wrong copied revision count");
              for (int revision = 1; revision <= 3; revision++) {
                if (!json(source, revision).equals(json(target, revision))) throw new IllegalStateException("wrong copy JSON at " + revision);
              }
            } catch (final RuntimeException failure) {
              if (delta != null || scenario.equals("append")) throw failure;
              copyFailure = failure.getClass().getSimpleName() + ":" + failure.getMessage();
              Arrays.fill(work[5], -1);
              writer.rollback();
            }
          }
        } catch (final RuntimeException failure) {
          if (delta != null || scenario.equals("append")) throw failure;
          System.out.println("UNSUPPORTED_SCENARIO," + label + ',' + scenario + ',' + iteration + ','
              + failure.getClass().getSimpleName() + ":" + failure.getMessage());
          continue;
        } finally {
          Databases.removeDatabase(sourcePath);
          Databases.removeDatabase(targetPath);
        }
        if (iteration >= 0) {
          for (int metric = 0; metric < METRICS.length; metric++) {
            final var row = new StringBuilder("SAMPLE,").append(label).append(',').append(scenario).append(',')
                .append(iteration).append(',').append(metricName(scenario, metric));
            for (final long value : work[metric]) row.append(',').append(value);
            System.out.println(row);
          }
          if (copyFailure != null) System.out.println("UNSUPPORTED," + label + ',' + scenario + ',' + iteration + ',' + copyFailure);
        }
      }
    }
    System.out.println("END," + label + ",sink=" + sink);
  }

  private static String metricName(final String scenario, final int metric) {
    if (metric == 0 && (scenario.equals("moves") || scenario.equals("restore"))) return "sourceMutationCommit";
    if (metric == 4 && scenario.equals("restore")) return "restorationPublicDiff";
    return METRICS[metric];
  }

  private static void measure(final long[] result, final Method[] counters, final Operation operation) throws Exception {
    final long[] before = read(counters);
    final long bytes = ALLOCATION.getThreadAllocatedBytes(Thread.currentThread().threadId());
    final long start = System.nanoTime();
    operation.run();
    result[0] = System.nanoTime() - start;
    result[1] = ALLOCATION.getThreadAllocatedBytes(Thread.currentThread().threadId()) - bytes;
    final long[] after = read(counters);
    for (int index = 0; index < counters.length; index++) result[index + 2] = before[index] < 0 ? -1 : after[index] - before[index];
  }

  private static void mutate(final String scenario, final JsonNodeTrx writer, final String appended) {
    if (scenario.equals("moves")) {
      require(writer.moveTo(1));
      writer.moveSubtreeToFirstChild(8);
    } else if (scenario.equals("restore")) {
      require(writer.moveTo(3));
      writer.remove();
    } else {
      require(writer.moveTo(scenario.equals("deep") ? 64 : 1));
      if (scenario.equals("sparse")) writer.getStorageEngineReader().getActualRevisionRootPage()
          .setMaxNodeKeyInDocumentIndex(1_000_000_000_000L);
      writer.insertSubtreeAsLastChild(JsonShredder.createStringReader(appended), JsonNodeTrx.Commit.NO,
          JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
    }
  }

  private static String initial(final String scenario) {
    return switch (scenario) {
      case "append" -> array(4096);
      case "unchanged" -> array(32768);
      case "deep" -> "[".repeat(64) + "0" + "]".repeat(64);
      case "sparse" -> array(16);
      case "moves" -> "[[1],[1],[1],[1]]";
      case "restore" -> "[0,1,2]";
      default -> throw new IllegalArgumentException(scenario);
    };
  }

  private static String array(final int size) {
    final var result = new StringBuilder(size * 5).append('[');
    for (int index = 0; index < size; index++) {
      if (index != 0) result.append(',');
      result.append(index);
    }
    return result.append(']').toString();
  }

  private static Database<JsonResourceSession> create(final Path path) {
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openJsonDatabase(path);
    database.createResource(ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL)
        .versioningApproach(VersioningType.SLIDING_SNAPSHOT).hashKind(HashType.ROLLING).useDeweyIDs(false).build());
    return database;
  }

  private static String json(final JsonResourceSession resource, final int revision) throws Exception {
    final var writer = new StringWriter();
    JsonSerializer.newBuilder(resource, writer, revision).build().call();
    return writer.toString();
  }

  private static Method deltaReader() throws ReflectiveOperationException {
    try {
      return Class.forName("io.sirix.service.json.replay.JsonIdentityDeltaReader")
          .getMethod("between", JsonNodeReadOnlyTrx.class, JsonNodeReadOnlyTrx.class, int.class);
    } catch (final ClassNotFoundException missing) {
      return null;
    }
  }

  private static Method[] counters() throws ReflectiveOperationException {
    final var result = new Method[COUNTERS.length];
    if (!Boolean.getBoolean("sirix.replay.workDiag")) return result;
    try {
      final var diagnostics = Class.forName("io.sirix.utils.ReplayWorkDiagnostics");
      for (int index = 0; index < result.length; index++) result[index] = diagnostics.getMethod(COUNTERS[index]);
    } catch (final ClassNotFoundException missing) {
      // The baseline predates these work counters. Never label their absence as zero measured work.
    }
    return result;
  }

  private static long[] read(final Method[] counters) throws ReflectiveOperationException {
    final long[] result = new long[counters.length];
    for (int index = 0; index < result.length; index++) result[index] = counters[index] == null ? -1 : (long) counters[index].invoke(null);
    return result;
  }

  private static void require(final boolean moved) {
    if (!moved) throw new IllegalStateException("missing fixture identity");
  }
}
