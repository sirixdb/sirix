package io.sirix.access.trx.node.json;

import io.sirix.access.Databases;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import io.sirix.service.json.replay.JsonIdentityDelta;
import io.sirix.service.json.replay.JsonReplayGraphValidator;
import io.sirix.service.json.replay.JsonReplayManifest;
import io.sirix.service.json.replay.JsonReplaySnapshotOracle;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertPaths;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertSnapshot;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class JsonIdentityEpochTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearHook() {
    JsonNodeTrxImpl.replayTestHook = null;
  }

  static Stream<Arguments> configurations() {
    return JsonIdentityImportTest.configurations();
  }

  static Stream<Arguments> commitModes() {
    return configurations().flatMap(configuration -> Stream.of(AfterCommitState.KEEP_OPEN,
        AfterCommitState.KEEP_OPEN_ASYNC_FLUSH, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT).map(mode -> {
          final Object[] values = configuration.get();
          return Arguments.of(values[0], values[1], values[2], mode);
        }));
  }

  @ParameterizedTest
  @MethodSource("commitModes")
  void failedLaterEpochPreservesHistoryAndCanRetry(final VersioningType versioning, final HashType hash,
      final boolean dewey, final AfterCommitState mode) {
    final Path sourcePath = directory.resolve("source");
    final Path targetPath = directory.resolve("target");
    try (final var sourceDb = JsonIdentityImportTest.create(sourcePath, versioning, hash, dewey);
        final var targetDb = JsonIdentityImportTest.create(targetPath, versioning, hash, dewey);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      seed(source);
      try (final var first = source.beginNodeReadOnlyTrx(1);
          final var second = source.beginNodeReadOnlyTrx(2);
          final var writer = target.beginNodeTrx(1, mode)) {
        final var importer = (InternalJsonNodeTrx) writer;
        importer.importRevision(JsonIdentityDeltaReader.snapshot(first, 1), first);
        final var delta = JsonIdentityDeltaReader.between(first, second, 2);
        for (final String failurePhase : List.of("identities-staged", "links-installed", "derived-state-finalized",
            "before-publish")) {
          JsonNodeTrxImpl.replayTestHook = (phase, transaction) -> {
            assertEquals(1, target.getMostRecentRevisionNumber(), "staging must not publish");
            if (phase.equals(failurePhase)) {
              throw new IllegalStateException("injected " + phase);
            }
          };
          final var failure = assertThrows(IllegalStateException.class, () -> importer.importRevision(delta, second));
          assertEquals("injected " + failurePhase, failure.getMessage());
          assertEquals(1, target.getMostRecentRevisionNumber());
          assertEquals(2, writer.getRevisionNumber());
          assertSnapshot(first, writer, 0);
          try (final var committed = target.beginNodeReadOnlyTrx(1)) {
            assertSnapshot(first, committed, 0);
          }
          assertPaths(source, 1, target, 1);
        }
        JsonNodeTrxImpl.replayTestHook = null;
        importer.importRevision(delta, second);
        assertEquals(2, target.getMostRecentRevisionNumber());
        try (final var committed = target.beginNodeReadOnlyTrx(2)) {
          assertSnapshot(second, committed, 0);
        }
      }
    }
    Databases.clearGlobalCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = Databases.openJsonDatabase(targetPath);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (final var expected = source.beginNodeReadOnlyTrx(revision);
            final var actual = target.beginNodeReadOnlyTrx(revision)) {
          assertSnapshot(expected, actual, 0);
        }
        assertPaths(source, revision, target, revision);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void rejectsWrongEpochWithoutChangingTheCleanDestination(final VersioningType versioning, final HashType hash,
      final boolean dewey) {
    try (final var sourceDb = JsonIdentityImportTest.create(directory.resolve("source"), versioning, hash, dewey);
        final var targetDb = JsonIdentityImportTest.create(directory.resolve("target"), versioning, hash, dewey);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      seed(source);
      try (final var first = source.beginNodeReadOnlyTrx(1);
          final var second = source.beginNodeReadOnlyTrx(2);
          final var writer = target.beginNodeTrx()) {
        final var importer = (InternalJsonNodeTrx) writer;
        final var initial = JsonIdentityDeltaReader.snapshot(first, 1);
        importer.importRevision(initial, first);
        final var delta = JsonIdentityDeltaReader.between(first, second, 2);
        final var manifest = delta.manifest();
        final var badManifests = List.of(
            new JsonReplayManifest(1, manifest.sourceResource().resolveSibling("foreign"), manifest.sourceIdentity(), 1,
                2, 2, manifest.baseFrontier(), manifest.targetFrontier(), dewey, hash),
            new JsonReplayManifest(1, manifest.sourceResource(), UUID.randomUUID(), 1, 2, 2, manifest.baseFrontier(),
                manifest.targetFrontier(), dewey, hash),
            new JsonReplayManifest(1, manifest.sourceResource(), manifest.sourceIdentity(), 1, 2, 2,
                manifest.baseFrontier() + 100, manifest.targetFrontier(), dewey, hash),
            new JsonReplayManifest(1, manifest.sourceResource(), manifest.sourceIdentity(), 1, 2, 2,
                manifest.baseFrontier(), manifest.targetFrontier() + 100, dewey, hash),
            new JsonReplayManifest(1, manifest.sourceResource(), manifest.sourceIdentity(), 1, 2, 2,
                manifest.baseFrontier(), manifest.targetFrontier(), !dewey, hash),
            new JsonReplayManifest(1, manifest.sourceResource(), manifest.sourceIdentity(), 1, 2, 2,
                manifest.baseFrontier(), manifest.targetFrontier(), dewey, hash == HashType.NONE
                    ? HashType.ROLLING
                    : HashType.NONE));
        for (final var bad : badManifests) {
          assertThrows(IllegalArgumentException.class,
              () -> importer.importRevision(JsonReplaySnapshotOracle.withManifest(delta, bad), second));
          assertEquals(1, target.getMostRecentRevisionNumber());
          assertSnapshot(first, writer, 0);
        }
        assertThrows(IllegalArgumentException.class, () -> importer.importRevision(delta, first));
        assertThrows(IllegalArgumentException.class, () -> importer.importRevision(initial, first));
        // Ordinary changes followed by rollback must retain the last successful replay binding.
        assertTrue(writer.moveTo(1));
        writer.insertNullValueAsLastChild();
        writer.rollback();
        importer.importRevision(delta, second);
        assertThrows(IllegalArgumentException.class, () -> importer.importRevision(delta, second));
        assertEquals(2, target.getMostRecentRevisionNumber());
        assertSnapshot(second, writer, 0);
        // An independently committed epoch invalidates the sequential replay contract.
        writer.commit();
        assertThrows(IllegalArgumentException.class, () -> importer.importRevision(delta, second));
        assertEquals(3, target.getMostRecentRevisionNumber());
        assertSnapshot(second, writer, 0);
        writer.revertTo(1);
        writer.rollback();
        assertThrows(IllegalArgumentException.class, () -> importer.importRevision(delta, second));
      }
    }
  }

  @Test
  void readerRejectsUncommittedCrossResourceAndNonconsecutiveEpochs() {
    try (
        final var sourceDb =
            JsonIdentityImportTest.create(directory.resolve("source"), VersioningType.FULL, HashType.ROLLING, true);
        final var otherDb =
            JsonIdentityImportTest.create(directory.resolve("other"), VersioningType.FULL, HashType.ROLLING, true);
        final var source = sourceDb.beginResourceSession("resource");
        final var other = otherDb.beginResourceSession("resource")) {
      seed(source);
      seed(other);
      try (final var writer = source.beginNodeTrx();
          final var first = source.beginNodeReadOnlyTrx(1);
          final var second = source.beginNodeReadOnlyTrx(2);
          final var foreign = other.beginNodeReadOnlyTrx(2);
          final var empty = source.beginNodeReadOnlyTrx(0)) {
        assertThrows(IllegalArgumentException.class, () -> JsonIdentityDeltaReader.snapshot(writer, 1));
        assertThrows(IllegalArgumentException.class, () -> JsonIdentityDeltaReader.snapshot(empty, 1));
        assertThrows(IllegalArgumentException.class, () -> JsonIdentityDeltaReader.between(first, foreign, 2));
        assertThrows(IllegalArgumentException.class, () -> JsonIdentityDeltaReader.between(second, first, 2));
        assertThrows(IllegalArgumentException.class, () -> JsonIdentityDeltaReader.between(first, first, 2));
        assertThrows(IllegalArgumentException.class, () -> JsonIdentityDeltaReader.between(first, writer, 2));
        assertThrows(IllegalArgumentException.class, () -> JsonIdentityDeltaReader.snapshot(second, 2));
        assertThrows(IllegalArgumentException.class, () -> JsonIdentityDeltaReader.between(first, second, 1));
        assertThrows(IllegalArgumentException.class, () -> JsonIdentityDeltaReader.between(first, second, 3));
      }
    }
  }

  @Test
  void commitWaitsForCompleteImportRatherThanPublishingStaging() throws Exception {
    try (
        final var sourceDb = JsonIdentityImportTest.create(directory.resolve("source"), VersioningType.SLIDING_SNAPSHOT,
            HashType.ROLLING, true);
        final var targetDb = JsonIdentityImportTest.create(directory.resolve("target"), VersioningType.SLIDING_SNAPSHOT,
            HashType.ROLLING, true);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      seed(source);
      // A positive time threshold enables the same lock used by the scheduler; the test triggers
      // its commit path deterministically rather than depending on a wall-clock scheduling delay.
      try (final var reader = source.beginNodeReadOnlyTrx(1);
          final var writer = target.beginNodeTrx(1, 1, TimeUnit.DAYS, AfterCommitState.KEEP_OPEN);
          final var executor = Executors.newSingleThreadExecutor()) {
        final var started = new CountDownLatch(1);
        final var pending = new AtomicReference<Future<?>>();
        JsonNodeTrxImpl.replayTestHook = (phase, transaction) -> {
          assertEquals(0, target.getMostRecentRevisionNumber());
          if (phase.equals("identities-staged")) {
            pending.set(executor.submit(() -> {
              started.countDown();
              writer.commit("autoCommit", null);
            }));
            try {
              assertTrue(started.await(30, TimeUnit.SECONDS));
            } catch (final InterruptedException failure) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(failure);
            }
            assertFalse(requireNonNull(pending.get()).isDone());
          }
        };
        ((InternalJsonNodeTrx) writer).importRevision(JsonIdentityDeltaReader.snapshot(reader, 1), reader);
        JsonNodeTrxImpl.replayTestHook = null;
        requireNonNull(pending.get()).get(30, TimeUnit.SECONDS);
        assertEquals(2, target.getMostRecentRevisionNumber());
        for (int revision = 1; revision <= 2; revision++) {
          try (final var copy = target.beginNodeReadOnlyTrx(revision)) {
            assertSnapshot(reader, copy, 0);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("commitModes")
  void authoritativeEpochsFollowBulkAsyncRollbackAndRevert(final VersioningType versioning, final HashType hash,
      final boolean dewey, final AfterCommitState mode) {
    final Path sourcePath = directory.resolve("source-epochs");
    final Path targetPath = directory.resolve("target-epochs");
    try (final var sourceDb = JsonIdentityImportTest.create(sourcePath, versioning, hash, dewey);
        final var source = sourceDb.beginResourceSession("resource")) {
      try (final var writer = source.beginNodeTrx(3, mode)) {
        writer.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("[{\"a\":[0,1],\"b\":true},{\"c\":\"v\"},null,0,1,2,3,4,5,6]"),
            JsonNodeTrx.Commit.NO);
        writer.commit();
        final int completedBulk = source.getMostRecentRevisionNumber();
        if (mode != AfterCommitState.KEEP_OPEN_ASYNC_FLUSH) {
          assertTrue(completedBulk > 1, "fixture must include actual intermediate durable epochs");
        }
        assertTrue(writer.moveTo(1));
        writer.insertStringValueAsLastChild("rolled back");
        writer.rollback();
        writer.commit();
        writer.revertTo(1);
        writer.commit();
        writer.revertTo(completedBulk);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = JsonIdentityImportTest.create(targetPath, versioning, hash, dewey);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var writer = target.beginNodeTrx(1, mode)) {
      for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
        try (final var reader = source.beginNodeReadOnlyTrx(revision)) {
          JsonReplayGraphValidator.validate(reader);
          final JsonIdentityDelta delta;
          if (revision == 1) {
            delta = JsonIdentityDeltaReader.snapshot(reader, 1);
          } else {
            try (final var base = source.beginNodeReadOnlyTrx(revision - 1)) {
              delta = JsonIdentityDeltaReader.between(base, reader, revision);
              final var reference = JsonReplaySnapshotOracle.between(base, reader, revision);
              assertEquals(reference.manifest(), delta.manifest());
              assertEquals(reference.puts(), delta.puts(), "source epoch " + revision);
              assertEquals(reference.deletes(), delta.deletes(), "source epoch " + revision);
            }
          }
          ((InternalJsonNodeTrx) writer).importRevision(delta, reader);
          assertEquals(revision, target.getMostRecentRevisionNumber());
          try (final var copied = target.beginNodeReadOnlyTrx(revision)) {
            assertSnapshot(reader, copied, 0);
          }
          assertPaths(source, revision, target, revision);
        }
      }
    }
    Databases.clearGlobalCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = Databases.openJsonDatabase(targetPath);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      assertEquals(source.getMostRecentRevisionNumber(), target.getMostRecentRevisionNumber());
      for (int revision = 1; revision <= source.getMostRecentRevisionNumber(); revision++) {
        try (final var expected = source.beginNodeReadOnlyTrx(revision);
            final var actual = target.beginNodeReadOnlyTrx(revision)) {
          assertSnapshot(expected, actual, 0);
        }
        assertPaths(source, revision, target, revision);
      }
    }
  }

  private static void seed(final JsonResourceSession source) {
    try (final var writer = source.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0]"), JsonNodeTrx.Commit.NO);
      writer.commit();
      assertTrue(writer.moveTo(1));
      writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("[{\"Aa\":1,\"BB\":2},{}]"),
          JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
      assertTrue(writer.moveTo(6));
      writer.moveSubtreeToFirstChild(4);
      assertTrue(writer.moveTo(3));
      writer.remove();
      writer.commit();
    }
  }
}
