/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.query.json.ProjectionSpec;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Three-arm cost measurement for the parallel importer's one-pass projection build:
 *
 * <ol>
 * <li><b>bare</b> — parallel import, no projection;</li>
 * <li><b>onepass</b> — parallel import with the projection armed before the data;</li>
 * <li><b>postpass</b> — parallel import bare, then {@code createIndexes} over the finished
 * resource.</li>
 * </ol>
 *
 * Arms are INTERLEAVED (bare, onepass, postpass, bare, onepass, ...) rather than run in blocks, so
 * a machine whose clock or background load drifts over the session penalises every arm equally. The
 * reported figure per arm is the MINIMUM over repetitions, which is the least noise-contaminated
 * estimator available when the box is shared.
 *
 * <pre>
 * java ... io.sirix.query.bench.clickbench.ClickBenchParallelProjectionCostMain &lt;corpus.json&gt; &lt;workDir&gt; [reps]
 * </pre>
 */
public final class ClickBenchParallelProjectionCostMain {

  private static final String RESOURCE = "res.jn";
  private static final int PROJECTION_INDEX_NUMBER = 0;
  /** The import transaction's node bound; matches the shipping bulk-import guidance. */
  private static final int NODES_BEFORE_EPOCH_ROTATION = 262144;

  private ClickBenchParallelProjectionCostMain() {
    throw new AssertionError("no instances");
  }

  private enum Arm {
    BARE, ONEPASS, POSTPASS
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: <corpus.json> <workDir> [reps]");
      System.exit(2);
    }
    final Path corpus = Path.of(args[0]);
    final Path workDir = Path.of(args[1]);
    final int reps = args.length > 2
        ? Integer.parseInt(args[2])
        : 3;
    if (!Files.isRegularFile(corpus)) {
      throw new IllegalArgumentException("corpus not found: " + corpus);
    }
    final long expectedRows = Long.getLong("cost.expectedRows", -1L);

    final List<double[]> samples = new ArrayList<>();
    for (final Arm ignored : Arm.values()) {
      samples.add(new double[reps]);
    }
    for (int rep = 0; rep < reps; rep++) {
      for (final Arm arm : Arm.values()) {
        final Path dbPath = workDir.resolve("cost-" + arm.name().toLowerCase(Locale.ROOT));
        deleteRecursively(dbPath);
        final long nanos = System.nanoTime();
        final long rows = runArm(arm, corpus, dbPath, expectedRows);
        final double seconds = (System.nanoTime() - nanos) / 1e9;
        samples.get(arm.ordinal())[rep] = seconds;
        System.out.printf(Locale.ROOT, "rep %d  %-8s  %8.3f s   rows=%d%n", rep + 1,
            arm.name().toLowerCase(Locale.ROOT), seconds, rows);
        System.out.flush();
        deleteRecursively(dbPath);
      }
    }

    System.out.println();
    System.out.printf(Locale.ROOT, "%-8s %10s %10s%n", "arm", "min(s)", "median(s)");
    final double[] mins = new double[Arm.values().length];
    for (final Arm arm : Arm.values()) {
      final double[] armSamples = samples.get(arm.ordinal()).clone();
      java.util.Arrays.sort(armSamples);
      mins[arm.ordinal()] = armSamples[0];
      System.out.printf(Locale.ROOT, "%-8s %10.3f %10.3f%n", arm.name().toLowerCase(Locale.ROOT), armSamples[0],
          armSamples[armSamples.length / 2]);
    }
    final double bare = mins[Arm.BARE.ordinal()];
    final double onePass = mins[Arm.ONEPASS.ordinal()];
    final double postPass = mins[Arm.POSTPASS.ordinal()];
    System.out.printf(Locale.ROOT, "%nONE-PASS OVERHEAD over bare: %+.3f s (%+.1f%%)%n", onePass - bare,
        100.0 * (onePass - bare) / bare);
    System.out.printf(Locale.ROOT, "ONE-PASS vs BARE+POST-PASS:  %+.3f s (%.2fx)%n", onePass - postPass,
        postPass / onePass);
  }

  private static long runArm(final Arm arm, final Path corpus, final Path dbPath, final long expectedRows)
      throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (
            JsonNodeTrx wtx = session.beginNodeTrx(NODES_BEFORE_EPOCH_ROTATION, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH);
            InputStream in = openCorpus(corpus)) {
          if (arm == Arm.ONEPASS) {
            final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
            controller.createProjectionIndexAtLoadStart(projectionDef(expectedRows), wtx, expectedRows);
          }
          ParallelBulkJsonImporter.assemble(wtx, in);
          wtx.commit();
        }
        if (arm == Arm.POSTPASS) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            session.getWtxIndexController(wtx.getRevisionNumber())
                   .createIndexes(Set.of(projectionDef(expectedRows)), wtx);
            wtx.commit();
          }
        }
        return verify(session, arm);
      }
    }
  }

  /**
   * Read the result back so an arm that quietly produced nothing cannot post the best time. For the
   * projection arms this additionally asserts the index is LIVE: an abandoned build leaves the stale
   * tombstone behind and would otherwise look like the fastest way to build an index.
   */
  private static long verify(final JsonResourceSession session, final Arm arm) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      rtx.moveToDocumentRoot();
      if (!rtx.moveToFirstChild()) {
        throw new IllegalStateException(arm + " produced no top-level array");
      }
      final long rows = rtx.getChildCount();
      if (rows == 0) {
        throw new IllegalStateException(arm + " produced an empty record set");
      }
      if (arm != Arm.BARE) {
        final byte[] raw =
            ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), PROJECTION_INDEX_NUMBER, 0L);
        if (raw == null) {
          throw new IllegalStateException(arm + " published no projection metadata");
        }
        final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(raw);
        if (metadata.isStale()) {
          throw new IllegalStateException(arm + " ABANDONED its projection — the tombstone is still in slot 0");
        }
        System.out.printf(Locale.ROOT, "        (%s projection: %d row groups)%n", arm.name().toLowerCase(Locale.ROOT),
            metadata.rowGroupCount());
      }
      return rows;
    }
  }

  private static IndexDef projectionDef(final long expectedRows) {
    final ProjectionSpec spec = ClickBenchProjection.spec(expectedRows);
    return spec.toIndexDef();
  }

  private static InputStream openCorpus(final Path corpus) throws IOException {
    return new BufferedInputStream(Files.newInputStream(corpus), 1 << 20);
  }

  private static void deleteRecursively(final Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (var walk = Files.walk(path)) {
      final List<Path> entries = walk.sorted(Comparator.reverseOrder()).toList();
      for (final Path entry : entries) {
        Files.deleteIfExists(entry);
      }
    }
  }
}
