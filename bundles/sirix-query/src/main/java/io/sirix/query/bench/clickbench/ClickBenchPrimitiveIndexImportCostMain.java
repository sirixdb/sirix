/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Two-arm cost measurement for the parallel importer's PATH/CAS/NAME maintenance:
 *
 * <ol>
 * <li><b>bare</b> — parallel import, no indexes;</li>
 * <li><b>indexed</b> — the same import with one PATH, one NAME and two CAS definitions catalogued,
 * maintained by the workers' tuple collection and the coordinator drain.</li>
 * </ol>
 *
 * Arms are interleaved per rep; the reported figure per arm is the minimum over repetitions.
 *
 * <pre>
 * java ... io.sirix.query.bench.clickbench.ClickBenchPrimitiveIndexImportCostMain &lt;corpus.json&gt; &lt;workDir&gt; [reps]
 * </pre>
 */
public final class ClickBenchPrimitiveIndexImportCostMain {

  private static final String RESOURCE = "res.jn";
  private static final int NODES_BEFORE_EPOCH_ROTATION = 262144;

  private ClickBenchPrimitiveIndexImportCostMain() {
    throw new AssertionError("no instances");
  }

  private enum Arm {
    BARE, INDEXED
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: <corpus.json> <workDir> [reps]");
      System.exit(2);
    }
    final java.nio.file.Path corpus = java.nio.file.Path.of(args[0]);
    final java.nio.file.Path workDir = java.nio.file.Path.of(args[1]);
    final int reps = args.length > 2
        ? Integer.parseInt(args[2])
        : 3;
    if (!Files.isRegularFile(corpus)) {
      throw new IllegalArgumentException("corpus not found: " + corpus);
    }

    final double[][] samples = new double[Arm.values().length][reps];
    for (int rep = 0; rep < reps; rep++) {
      for (final Arm arm : Arm.values()) {
        final java.nio.file.Path dbPath = workDir.resolve("idxcost-" + arm.name().toLowerCase(Locale.ROOT));
        deleteRecursively(dbPath);
        final long nanos = System.nanoTime();
        final long rows = runArm(arm, corpus, dbPath);
        final double seconds = (System.nanoTime() - nanos) / 1e9;
        samples[arm.ordinal()][rep] = seconds;
        System.out.printf(Locale.ROOT, "rep %d  %-8s %8.3f s   rows=%d%n", rep + 1,
            arm.name().toLowerCase(Locale.ROOT), seconds, rows);
        System.out.flush();
        deleteRecursively(dbPath);
      }
    }

    System.out.println();
    System.out.printf(Locale.ROOT, "%-8s %10s %10s%n", "arm", "min(s)", "median(s)");
    final double[] mins = new double[Arm.values().length];
    for (final Arm arm : Arm.values()) {
      final double[] armSamples = samples[arm.ordinal()].clone();
      java.util.Arrays.sort(armSamples);
      mins[arm.ordinal()] = armSamples[0];
      System.out.printf(Locale.ROOT, "%-8s %10.3f %10.3f%n", arm.name().toLowerCase(Locale.ROOT), armSamples[0],
          armSamples[armSamples.length / 2]);
    }
    final double bare = mins[Arm.BARE.ordinal()];
    final double indexed = mins[Arm.INDEXED.ordinal()];
    System.out.printf(Locale.ROOT, "%nPATH+CAS+NAME MAINTENANCE over bare: %+.3f s (%+.1f%%)%n", indexed - bare,
        100.0 * (indexed - bare) / bare);
  }

  private static long runArm(final Arm arm, final java.nio.file.Path corpus, final java.nio.file.Path dbPath)
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
        try (JsonNodeTrx wtx = session.beginNodeTrx(NODES_BEFORE_EPOCH_ROTATION,
            AfterCommitState.KEEP_OPEN_ASYNC_FLUSH); InputStream in = openCorpus(corpus)) {
          if (arm == Arm.INDEXED) {
            session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(indexDefs(), wtx);
          }
          ParallelBulkJsonImporter.assemble(wtx, in);
          wtx.commit();
        }
        return verify(session, arm);
      }
    }
  }

  /** Read the result back so an arm that quietly produced nothing cannot post the best time. */
  private static long verify(final JsonResourceSession session, final Arm arm) {
    try (JsonNodeTrx trx = session.beginNodeTrx()) {
      trx.moveToDocumentRoot();
      if (!trx.moveToFirstChild()) {
        throw new IllegalStateException(arm + " produced no top-level array");
      }
      final long rows = trx.getChildCount();
      if (arm == Arm.INDEXED) {
        final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
        final IndexDef pathDef = controller.getIndexes().getIndexDef(0, IndexType.PATH);
        final long pathEntries = countPostings(controller.openPathIndex(trx.getStorageEngineReader(), pathDef,
            controller.createPathFilter(Set.of("/[]/URL"), trx)));
        final IndexDef nameDef =
            controller.getIndexes().getIndexDef(IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON).getID(),
                IndexType.NAME);
        final long nameEntries = countPostings(controller.openNameIndex(trx.getStorageEngineReader(), nameDef,
            controller.createNameFilter(Set.of("URL"))));
        if (pathEntries != rows || nameEntries != rows) {
          throw new IllegalStateException("indexed arm read back path=" + pathEntries + " name=" + nameEntries
              + " postings for " + rows + " rows — the indexes are short");
        }
        System.out.printf(Locale.ROOT, "        (indexed: URL path postings=%d, URL name postings=%d)%n", pathEntries,
            nameEntries);
      }
      return rows;
    }
  }

  private static long countPostings(final Iterator<NodeReferences> hits) {
    long count = 0;
    while (hits.hasNext()) {
      count += hits.next().getNodeKeys().getLongCardinality();
    }
    return count;
  }

  private static Set<IndexDef> indexDefs() {
    return Set.of(
        IndexDefs.createPathIdxDef(Set.of(Path.parse("/[]/URL", PathParser.Type.JSON),
            Path.parse("/[]/EventTime", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON),
        IndexDefs.createSelectiveNameIdxDef(
            Set.of(new QNm("URL"), new QNm("EventTime"), new QNm("CounterID")), 0, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.LON, Set.of(Path.parse("/[]/CounterID", PathParser.Type.JSON)), 0,
            IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(Path.parse("/[]/URL", PathParser.Type.JSON)), 1,
            IndexDef.DbType.JSON));
  }

  private static InputStream openCorpus(final java.nio.file.Path corpus) throws IOException {
    return new BufferedInputStream(Files.newInputStream(corpus), 1 << 20);
  }

  private static void deleteRecursively(final java.nio.file.Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (var walk = Files.walk(path)) {
      final List<java.nio.file.Path> entries = walk.sorted(Comparator.reverseOrder()).toList();
      for (final java.nio.file.Path entry : entries) {
        Files.deleteIfExists(entry);
      }
    }
  }
}
