package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.backup.BackupManager;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.DerivedIndexTestSupport;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.io.StorageType;
import io.sirix.query.compiler.translator.TemporalAggregateRoute;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TXN-613 end to end: a backup carries the hidden derived resource that holds the
 * temporal-aggregate data, so after a restore elsewhere the same aggregate query is still served
 * from the index and still returns what a full scan returns. Before the backup copied derived
 * resources, the restored database had none and the data this index holds was silently lost.
 */
@Isolated // TemporalAggregateRoute.servedCount() is process-wide.
final class BackupRestoreTemporalAggregateTest {

  private static final String RESOURCE = "records";
  private static final String DERIVED = RESOURCE + DerivedIndexMaintainer.SUFFIX;
  private static final int REVISIONS = 3;
  private static final String POINT = "2024-06-01T00:00:00Z";

  @TempDir
  File directory;

  @Test
  void aRestoredBackupStillServesTheTemporalAggregateIndex() throws Exception {
    final Path stores = directory.toPath().resolve("stores");
    Files.createDirectories(stores);
    final Path sourceDatabase = stores.resolve("edges");
    createSourceDatabase(sourceDatabase);

    // The backup lives outside the store location, so only real databases are queried below.
    final Path backupDirectory = directory.toPath().resolve("backup");
    final BackupManager.BackupSummary backup = BackupManager.backupDatabase(sourceDatabase, backupDirectory);
    assertEquals(List.of(DERIVED), names(backup), "the backup must carry the hidden derived resource");

    final Path restoredDatabase = stores.resolve("restored");
    final BackupManager.BackupSummary restore = BackupManager.restoreDatabase(backupDirectory, restoredDatabase);
    assertEquals(List.of(DERIVED), names(restore), "the restore must verify and report the derived resource");

    try (var store = BasicJsonDBStore.newBuilder().location(stores).storageType(StorageType.FILE_CHANNEL).build();
        var scanContext = SirixQueryContext.createWithJsonStore(store);
        var scan = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var indexedContext = SirixQueryContext.createWithJsonStore(store);
        var indexed = SirixCompileChain.createWithJsonStore(store)) {
      for (int revision = 1; revision <= REVISIONS; revision++) {
        final String query = groupedAggregateQuery("restored", revision);
        final String scanned = run(scan, scanContext, query);
        assertTrue(scanned.contains("\"g\":"), () -> "test setup: the scan must return groups, was " + scanned);
        final long before = TemporalAggregateRoute.servedCount();
        assertEquals(scanned, run(indexed, indexedContext, query), "revision " + revision);
        assertEquals(before + 1, TemporalAggregateRoute.servedCount(),
            "revision " + revision + " must be served from the restored derived index");
        assertEquals(scanned, run(indexed, indexedContext, groupedAggregateQuery("edges", revision)),
            "the restored answer must equal the original's at revision " + revision);
      }
    }
  }

  private static void createSourceDatabase(final Path databasePath) {
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .customCommitTimestamps(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"g":1,"n":10,"lo":"2024-01-01T00:00:00Z","hi":"2025-01-01T00:00:00Z"},
             {"g":1,"n":30,"lo":"2024-01-01T00:00:00Z","hi":"2025-01-01T00:00:00Z"},
             {"g":2,"n":-7,"lo":"2024-01-01T00:00:00Z","hi":"2025-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "edges");
        final IndexDef projection = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
            List.of(parse("/[]/g", PathParser.Type.JSON), parse("/[]/n", PathParser.Type.JSON),
                parse("/[]/lo", PathParser.Type.JSON), parse("/[]/hi", PathParser.Type.JSON)),
            List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
        TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
            List.of("g"), List.of("count(*)", "sum(n)", "min(n)", "max(n)"));
        writer.commit("initial", Instant.parse("2024-01-15T00:00:00Z"));
        DerivedIndexTestSupport.await(session);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToRightSibling();
        writer.moveToFirstChild();
        for (int revision = 2; revision <= REVISIONS; revision++) {
          writer.setNumberValue(revision * 100L);
          writer.commit("revision " + revision, Instant.parse("2024-0" + revision + "-15T00:00:00Z"));
          DerivedIndexTestSupport.await(session);
        }
        assertEquals(REVISIONS, session.getMostRecentRevisionNumber(), "test setup: source revisions");
      }
    }
  }

  private static String groupedAggregateQuery(final String databaseName, final int revision) {
    return "for $r in jn:doc('" + databaseName + "','" + RESOURCE + "'," + revision + ")[] "
        + "where xs:dateTime($r.lo) le xs:dateTime('" + POINT + "') " + "and xs:dateTime('" + POINT
        + "') lt xs:dateTime($r.hi) "
        + "let $g := $r.g group by $g let $c := count($r), $s := sum($r.n), $l := min($r.n), $h := max($r.n) "
        + "order by $g return {\"g\":$g,\"c\":$c,\"s\":$s,\"l\":$l,\"h\":$h}";
  }

  private static List<String> names(final BackupManager.BackupSummary summary) {
    return summary.derivedResources().stream().map(BackupManager.ResourceSummary::resourceName).toList();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String text)
      throws Exception {
    TemporalAggregateTestSupport.await(context);
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(output, false, StandardCharsets.UTF_8)) {
      new Query(chain, text).serialize(context, writer);
      writer.flush();
      return output.toString(StandardCharsets.UTF_8).trim();
    }
  }
}
