package io.sirix.diff;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.settings.StringCompressionType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Committed overflow updates must retain their Dewey metadata and durable diff sidecars. */
final class JsonDiffOverflowVersioningTest {
  private static final String RESOURCE = "overflow-diffs";
  private static final int RECORDS = 512;
  private static final int LAST_REVISION = 8;
  private static final String PAYLOAD = "overflow-payload-".repeat(160);

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void overflowUpdatesKeepEveryCommittedSidecar(final VersioningType versioning) throws Exception {
    final Path databasePath = directory.resolve(versioning.name());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final long[] keys = new long[RECORDS];
    final String[] deweyIds = new String[RECORDS];
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .storeDiffs(true)
                                                              .useDeweyIDs(true)
                                                              .storeNodeHistory(false)
                                                              .hashKind(HashType.NONE)
                                                              .stringCompressionType(StringCompressionType.NONE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .build()));
      try (final var session = database.beginResourceSession(RESOURCE); final var writer = session.beginNodeTrx()) {
        final long root = writer.insertObjectAsFirstChild().getNodeKey();
        for (int i = 0; i < RECORDS; i++) {
          assertTrue(writer.moveTo(root));
          keys[i] = writer.insertObjectRecordAsLastChild("field" + i, new StringValue(value(i, 1))).getNodeKey();
          deweyIds[i] = writer.getDeweyID().toString();
        }
        writer.commit();
        for (int revision = 2; revision <= LAST_REVISION; revision++) {
          for (int i = 0; i < RECORDS; i++) {
            assertTrue(writer.moveTo(keys[i]));
            writer.setStringValue(value(i, revision));
          }
          writer.commit();
          assertEquals(revision, session.getMostRecentRevisionNumber());
        }
      }
    }

    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession(RESOURCE)) {
      assertAll(IntStream.rangeClosed(2, LAST_REVISION).mapToObj(revision -> () -> {
        assertSidecar(session, revision, keys, deweyIds);
      }));
      for (int revision = 1; revision <= LAST_REVISION; revision++) {
        try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
          for (int i = 0; i < RECORDS; i++) {
            assertTrue(reader.moveTo(keys[i]));
            assertEquals(value(i, revision), reader.getValue());
            assertEquals(deweyIds[i], reader.getDeweyID().toString());
          }
        }
      }
    }
  }

  private static String value(final int record, final int revision) {
    return PAYLOAD + record + "-revision-" + revision;
  }

  private static void assertSidecar(final JsonResourceSession session, final int revision, final long[] keys,
      final String[] deweyIds) throws Exception {
    final Path file = session.getResourceConfig()
                             .getResource()
                             .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                             .resolve("diffFromRev" + (revision - 1) + "toRev" + revision + ".json");
    assertTrue(Files.exists(file), "missing durable sidecar for revision " + revision);
    final JsonObject sidecar = JsonParser.parseString(Files.readString(file)).getAsJsonObject();
    JsonDiffIntegrity.validate(sidecar);
    final var diffs = sidecar.getAsJsonArray("diffs");
    assertEquals(RECORDS, diffs.size());
    for (final var diff : diffs) {
      final var update = diff.getAsJsonObject().getAsJsonObject("update");
      final String value = update.get("value").getAsString();
      final int record = Integer.parseInt(value.substring(PAYLOAD.length(), value.indexOf("-revision-")));
      assertEquals(keys[record], update.get("nodeKey").getAsLong());
      assertEquals(value(record, revision), value);
      assertEquals(deweyIds[record], update.get("deweyID").getAsString());
    }
    try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
      assertEquals(RECORDS, reader.getUpdateOperations().size());
    }
  }
}
