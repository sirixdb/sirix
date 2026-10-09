package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Differential boundary and historical-revision gates for the compressed single-stab path. */
final class ValidTimeRunStabTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void everyBoundaryAndInexactPointMatchesTheScanInOldAndNewRevisions(final VersioningType versioning) {
    final String json = """
        [{"vf":"2024-01-01T00:00:00Z","vt":"2024-01-02T00:00:00Z"},
         {"vf":"2024-01-01T00:00:00Z","vt":"2024-01-02T00:00:00.000500Z"},
         {"vf":"2024-01-01T00:00:00.000500Z","vt":"2024-01-02T00:00:00Z"},
         {"vf":"2024-01-02T00:00:00Z","vt":"2024-01-02T00:00:00Z"},
         {"vf":"1969-12-31T23:59:59Z","vt":"1970-01-01T00:00:00.000500Z"},
         {"vf":"2024-01-01T00:00:00Z"},
         {"vt":"2024-01-02T00:00:00.000500Z"},
         {"vf":"2024-01-01T00:00:00Z","vt":"2024-01-02T00:00:00Z",
          "nested":[{"vf":"2020-01-01T00:00:00Z","vt":"2030-01-01T00:00:00Z"}]}]
        """;
    final Path databasePath = directory.resolve("runs");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .validTimePaths("vf", "vt")
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        writer.commit();
      }
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "let $d := jn:doc('runs','rows') let $i := jn:create-valid-time-index($d) return sdb:commit($d)").evaluate(
              context);
      final var collection = store.lookup("runs");
      final JsonDBItem original = collection.getDocument("rows");
      final var session = original.getResourceSession();
      final int before = original.getTrx().getRevisionNumber();
      final var writer = session.getNodeTrx().orElseGet(session::beginNodeTrx);
      writer.moveTo(original.getNodeKey());
      writer.moveToFirstChild();
      writer.moveToLastChild();
      writer.setStringValue("2024-01-01T00:00:00Z");
      writer.commit();
      final int after = session.getMostRecentRevisionNumber();
      for (final int revision : new int[] {before, after, before}) {
        final JsonDBItem document = collection.getDocument("rows", revision);
        final ValidTimeConfig config = Objects.requireNonNull(session.getResourceConfig().getValidTimeConfig());
        for (final String timestamp : new String[] {"1969-12-31T23:59:59Z", "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.000250Z", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00.000250Z",
            "2024-01-01T00:00:00.000500Z", "2024-01-02T00:00:00Z", "2024-01-02T00:00:00.000250Z",
            "2024-01-02T00:00:00.000500Z", "2024-01-02T00:00:00.001Z", "+11000-01-01T00:00:00Z"}) {
          final Instant point = Instant.parse(timestamp);
          for (final boolean strictStart : new boolean[] {false, true}) {
            for (final boolean strictEnd : new boolean[] {false, true}) {
              final Sequence indexed = ValidTimeIntervalIndex.sequence(document, point, config, strictStart, strictEnd);
              assertNotNull(indexed);
              final Sequence scan = ValidTimeFilter.linearScanSequence(document, point, config, strictStart, strictEnd);
              assertArrayEquals(keys(scan), keys(indexed), revision + " " + timestamp + " " + strictStart + strictEnd);
            }
          }
        }
      }
    }
  }

  private static long[] keys(final Sequence sequence) {
    final LongArrayList keys = new LongArrayList();
    try (var iterator = sequence.iterate()) {
      for (var item = iterator.next(); item != null; item = iterator.next()) {
        keys.add(((JsonDBItem) item).getNodeKey());
      }
    }
    final long[] sorted = keys.toLongArray();
    Arrays.sort(sorted);
    return sorted;
  }
}
