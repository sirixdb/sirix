package io.sirix.diff;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pipelined epochs must serialize their frozen state without exposing an unpublished revision. */
final class JsonAsyncCommitSidecarTest {
  @TempDir
  Path directory;

  static Stream<Arguments> configurations() {
    return Stream.of(VersioningType.values())
                 .flatMap(version -> Stream.of(false, true).map(dewey -> Arguments.of(version, dewey)));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void intermediateScalarUpdatesKeepExactSidecars(final VersioningType version, final boolean dewey) throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(version)
                                                   .useDeweyIDs(dewey)
                                                   .build());
      try (final var session = database.beginResourceSession("resource")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"values\":[0]}"), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        try (final var writer = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
          for (int value = 1; value <= 4; value++) {
            assertTrue(writer.moveTo(3));
            writer.setNumberValue(value);
            assertEquals(3, writer.getNodeKey(), "commit serialization must restore the caller's cursor");
          }
          writer.commit();
        }
        final var updates = session.getResourceConfig()
                                   .getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath());
        for (int revision = 2; revision <= 5; revision++) {
          final Path sidecar = updates.resolve("diffFromRev" + (revision - 1) + "toRev" + revision + ".json");
          assertTrue(Files.exists(sidecar), "missing pipelined sidecar for revision " + revision);
          final var diff = JsonDiffSidecar.read(sidecar, "resource", revision - 1, revision, dewey);
          assertEquals(1, diff.getAsJsonArray("diffs").size());
          final var update = diff.getAsJsonArray("diffs").get(0).getAsJsonObject().getAsJsonObject("update");
          assertEquals(3, update.get("nodeKey").getAsLong());
          assertEquals(revision - 1, update.get("value").getAsInt());
          assertEquals("/values/[0]", update.get("path").getAsString());
          try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
            assertTrue(reader.moveTo(3));
            assertEquals(revision - 1, reader.getNumberValue().intValue());
          }
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void intermediateArrayInsertsKeepFinalOrdinals(final VersioningType version, final boolean dewey) throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(version)
                                                   .useDeweyIDs(dewey)
                                                   .build());
      try (final var session = database.beginResourceSession("resource")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"values\":[]}"), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        try (final var writer = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
          for (int value = 1; value <= 4; value++) {
            assertTrue(writer.moveTo(2));
            writer.insertNumberValueAsLastChild(value);
          }
          writer.commit();
        }
        final var updates = session.getResourceConfig()
                                   .getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath());
        for (int revision = 2; revision <= 5; revision++) {
          final var diff =
              JsonDiffSidecar.read(updates.resolve("diffFromRev" + (revision - 1) + "toRev" + revision + ".json"),
                  "resource", revision - 1, revision, dewey);
          assertEquals(1, diff.getAsJsonArray("diffs").size());
          final var insert = diff.getAsJsonArray("diffs").get(0).getAsJsonObject().getAsJsonObject("insert");
          assertEquals(revision + 1, insert.get("nodeKey").getAsLong());
          assertEquals(revision - 1, insert.get("data").getAsInt());
          assertEquals("/values/[" + (revision - 2) + "]", insert.get("path").getAsString());
        }
      }
    }
  }

}
