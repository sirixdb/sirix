package io.sirix.budget;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.InternalJsonNodeTrx;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.diff.JsonDiffSidecar;
import io.sirix.io.StorageType;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import io.sirix.service.json.replay.JsonReplayGraphValidator;
import io.sirix.service.json.replay.JsonReplaySnapshotOracle;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.sirix.budget.EngineWorkCounters.REPLAY;
import static io.sirix.budget.EngineWorkCounters.REPLAY_CREATED_IDENTITIES;
import static io.sirix.budget.EngineWorkCounters.REPLAY_STAGED_RECORDS;
import static io.sirix.budget.EngineWorkCounters.REPLAY_RECORD_VISITS;
import static io.sirix.budget.EngineWorkCounters.REPLAY_PATH_STEPS;
import static io.sirix.budget.EngineWorkCounters.REPLAY_ANCESTOR_STEPS;
import static io.sirix.budget.EngineWorkCounters.REPLAY_SIDECAR_READS;
import static io.sirix.budget.EngineWorkCounters.REPLAY_FALLBACK_PAGES;

/**
 * Authoritative replay must scale with changed pages/records, never the unchanged tree or frontier.
 */
@Isolated
final class JsonIdentityReplayWorkBudgetTest {
  @TempDir
  Path directory;

  static Stream<Arguments> configurations() {
    return Stream.of(VersioningType.values())
                 .flatMap(version -> Stream.of(16, 4096, 16384).map(prefix -> Arguments.of(version, prefix)));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void appendNoopAndSparseEpochs(final VersioningType version, final int prefix) throws Exception {
    final Path sourcePath = directory.resolve("source");
    try (final var database = create(sourcePath, version);
        final var source = database.beginResourceSession("resource");
        final var writer = source.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + "0,".repeat(prefix - 1) + "0]"),
          JsonNodeTrx.Commit.NO);
      writer.commit();
      append(writer);
      writer.commit();
      writer.commit();
      writer.getStorageEngineReader().getActualRevisionRootPage().setMaxNodeKeyInDocumentIndex(1_000_000_000_000L);
      writer.commit();
      append(writer);
      writer.commit();
    }
    Databases.clearGlobalCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = create(directory.resolve("target"), version);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var writer = target.beginNodeTrx()) {
      final var importer = (InternalJsonNodeTrx) writer;
      // Positive control for the zero-sidecar-read replay budget: exercise the real reader seam.
      final var sidecar = source.getResourceConfig()
                                .getResource()
                                .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                .resolve("diffFromRev1toRev2.json");
      WorkCapture.of(REPLAY)
                 .run(() -> JsonDiffSidecar.read(sidecar, "resource", 1, 2, false))
                 .assertExactly(REPLAY_SIDECAR_READS, 1, "the sidecar counter must observe an actual read");
      try (final var first = source.beginNodeReadOnlyTrx(1)) {
        final var initial =
            WorkCapture.of(REPLAY)
                       .run(() -> importer.importRevision(JsonIdentityDeltaReader.snapshot(first, 1), first));
        initial.assertExactly(REPLAY_CREATED_IDENTITIES, prefix + 1,
            "initial replay must create each live identity once")
               .assertExactly(REPLAY_STAGED_RECORDS, prefix + 2,
                   "initial replay stages the document and live identities")
               .assertAtLeast(REPLAY_RECORD_VISITS, prefix, "record lookup diagnostics must be live")
               .assertAtLeast(REPLAY_PATH_STEPS, 1, "path cache lifecycle diagnostics must be live");
      }
      for (int revision = 2; revision <= 5; revision++) {
        final int current = revision;
        final boolean append = revision == 2 || revision == 5;
        final var capture = WorkCapture.of(REPLAY).call(() -> {
          try (final var before = source.beginNodeReadOnlyTrx(current - 1);
              final var after = source.beginNodeReadOnlyTrx(current)) {
            final var delta = JsonIdentityDeltaReader.between(before, after, current);
            importer.importRevision(delta, after);
            return delta;
          }
        });
        capture.work()
               .assertExactly(REPLAY_CREATED_IDENTITIES, append
                   ? 3
                   : 0, "no hidden replacement identities")
               .assertExactly(REPLAY_STAGED_RECORDS, append
                   ? 6
                   : 0, "only three new leaves and three boundaries are staged")
               .assertZero(REPLAY_SIDECAR_READS, "identity replay must be independent of presentation sidecars");
        if (append) {
          capture.work()
                 .assertBetween(REPLAY_FALLBACK_PAGES, 1, 20, "append must skip unchanged durable regions")
                 .assertBetween(REPLAY_RECORD_VISITS, 50, 2300,
                     "append must not scan the unchanged prefix or numeric gap")
                 .assertBetween(REPLAY_ANCESTOR_STEPS, 3, 6, "ancestor proofs must share their already rooted prefix");
        } else {
          capture.work()
                 .assertZero(REPLAY_FALLBACK_PAGES, "identical durable roots prove a no-op without page visits")
                 .assertBetween(REPLAY_RECORD_VISITS, 1, 40, "no-op import must not traverse the unchanged tree")
                 .assertZero(REPLAY_ANCESTOR_STEPS, "no changed identities need an ancestry proof");
        }
        capture.work()
               .assertBetween(REPLAY_PATH_STEPS, 1, 8, "path cache maintenance must remain bounded for one array PCR");
        try (final var original = source.beginNodeReadOnlyTrx(revision);
            final var copied = target.beginNodeReadOnlyTrx(revision)) {
          assertEquals(original.getMaxNodeKey(), copied.getMaxNodeKey());
          JsonReplayGraphValidator.validate(copied);
          final var expected = JsonReplaySnapshotOracle.snapshot(original);
          final var actual = JsonReplaySnapshotOracle.snapshot(copied);
          expected.remove(0);
          actual.remove(0);
          assertEquals(expected, actual, "identity graph in revision " + revision);
          assertTrue(original.moveTo(0));
          assertTrue(copied.moveTo(0));
          assertEquals(original.getHash(), copied.getHash());
          assertEquals(original.getDescendantCount(), copied.getDescendantCount());
        }
      }
    }
  }

  static Stream<Arguments> depths() {
    return Stream.of(VersioningType.values())
                 .flatMap(version -> Stream.of(32, 96).map(depth -> Arguments.of(version, depth)));
  }

  @ParameterizedTest
  @MethodSource("depths")
  void appendedSiblingsShareTheirAncestorProof(final VersioningType version, final int depth) throws Exception {
    final Path sourcePath = directory.resolve("source");
    try (final var database = create(sourcePath, version);
        final var source = database.beginResourceSession("resource");
        final var writer = source.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[".repeat(depth) + "0" + "]".repeat(depth)),
          JsonNodeTrx.Commit.NO);
      writer.commit();
      assertTrue(writer.moveTo(depth));
      writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("[" + "1,".repeat(63) + "1]"),
          JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
      writer.commit();
    }
    Databases.clearGlobalCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = create(directory.resolve("target"), version);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var writer = target.beginNodeTrx();
        final var before = source.beginNodeReadOnlyTrx(1);
        final var after = source.beginNodeReadOnlyTrx(2)) {
      final var importer = (InternalJsonNodeTrx) writer;
      importer.importRevision(JsonIdentityDeltaReader.snapshot(before, 1), before);
      final var work =
          WorkCapture.of(REPLAY)
                     .run(() -> importer.importRevision(JsonIdentityDeltaReader.between(before, after, 2), after));
      work.assertBetween(REPLAY_ANCESTOR_STEPS, depth + 64, depth + 65,
          "each changed identity shares the proven ancestry instead of walking to the root again")
          .assertExactly(REPLAY_CREATED_IDENTITIES, 64, "the append must create exactly its new leaves")
          .assertExactly(REPLAY_STAGED_RECORDS, depth + 66,
              "stage new leaves, their boundary, ancestors and document once")
          .assertZero(REPLAY_SIDECAR_READS, "deep replay must not consult the presentation sidecar");
      try (final var copied = target.beginNodeReadOnlyTrx(2)) {
        JsonReplayGraphValidator.validate(copied);
        final var expected = JsonReplaySnapshotOracle.snapshot(after);
        final var actual = JsonReplaySnapshotOracle.snapshot(copied);
        expected.remove(0);
        actual.remove(0);
        assertEquals(expected, actual);
      }
    }
  }

  private static void append(final JsonNodeTrx writer) {
    assertTrue(writer.moveTo(1));
    writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("[1,2,3]"), JsonNodeTrx.Commit.NO,
        JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
  }

  private static Database<JsonResourceSession> create(final Path path, final VersioningType version) {
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openJsonDatabase(path);
    database.createResource(ResourceConfiguration.newBuilder("resource")
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .versioningApproach(version)
                                                 .hashKind(HashType.ROLLING)
                                                 .useDeweyIDs(false)
                                                 .buildPathStatistics(true)
                                                 .build());
    return database;
  }
}
