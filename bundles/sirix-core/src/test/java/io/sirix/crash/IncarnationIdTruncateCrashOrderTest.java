package io.sirix.crash;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FMT-209 / TXN-404 ordering: {@code truncateTo(r)} MUST durably publish the new incarnation id
 * <em>before removing any data</em>. A crash between the two must leave the new id with the old
 * data, which only rejects tokens that would otherwise have been accepted; the opposite order could
 * accept a token from a destroyed timeline.
 *
 * <p>
 * The test records a real truncate through the same power-loss recording storage as
 * {@link ExplicitRollbackCrashSimulationTest} and observes the persisted {@code incarnationId}
 * member of {@code ressetting.obj} (CONF-102) synchronously at every recorded channel operation. It
 * asserts that
 *
 * <ol>
 * <li>by the time the first removal ({@code TRUNCATE} of a resource file) has happened, the
 * persisted id is already the new id;</li>
 * <li>a copy of the whole database taken at the first instant the new id is visible, and another
 * taken at the first removal, each open cold, still carry the new id, and sit at the old head or at
 * the truncate target, never between.</li>
 * </ol>
 *
 * <p>
 * Durability of the settings write (an {@code fsync} of the file and its directory) is not
 * observable through the recorded channels, which cover only the data and revisions files; that
 * part of "durably publish" stays an open question recorded in the pull request, not an assertion
 * here.
 *
 * <p>
 * Written before the implementation and enabled unchanged by the change that implemented FMT-209;
 * no assertion may be weakened.
 */
final class IncarnationIdTruncateCrashOrderTest {

  private static final String RESOURCE = "incarnation-crash-resource";

  private static final int ORIGINAL_REVISION = 3;

  private static final int TARGET_REVISION = 1;

  private static final Pattern INCARNATION_ID = Pattern.compile("[0-9a-f]{16}");

  @TempDir
  Path workRoot;

  @Test
  @DisplayName("FMT-209, TXN-404: the new incarnation id is published before any data is removed")
  void txn404TruncatePublishesTheIncarnationIdBeforeRemovingData() throws Exception {
    final PowerLossRecorder recorder = new PowerLossRecorder();
    // Database opens canonicalize paths; the recording provider must see the same root on macOS.
    final Path recordDb = workRoot.toRealPath().resolve("record-db");
    final Map<String, Path> snapshots = new LinkedHashMap<>();
    final String[] observed = new String[3]; // [0] old id, [1] id at first removal, [2] id at first change

    PowerLossRecordingStorage.install(recordDb, recorder);
    try {
      Databases.createJsonDatabase(new DatabaseConfiguration(recordDb));
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(recordDb)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .versioningApproach(VersioningType.FULL)
                                                     .hashKind(HashType.NONE)
                                                     .buildPathSummary(false)
                                                     .storeDiffs(false)
                                                     .build());
        try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"r1\"]"), JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
          for (final String value : new String[] {"{\"r\":2}", "{\"r\":3}"}) {
            try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
              wtx.moveToDocumentRoot();
              wtx.moveToFirstChild();
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(value), JsonNodeTrx.Commit.NO);
              wtx.commit();
            }
          }
          assertEquals(ORIGINAL_REVISION, session.getMostRecentRevisionNumber(), "setup");

          observed[0] = persistedId(recordDb);
          recorder.setOpListener(op -> observe(op, recordDb, observed, snapshots));
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.truncateTo(TARGET_REVISION);
          }
          recorder.setOpListener(null);
        }
      }
    } finally {
      PowerLossRecordingStorage.uninstall();
    }

    assertTrue(snapshots.containsKey("firstRemoval"),
        "the recorded truncate performed no removal; the ordering window was not exercised");
    assertNotEquals(observed[0], observed[1],
        "the persisted incarnation id MUST already be the new id when the first data is removed");
    assertNotEquals(observed[0], persistedId(recordDb), "the id after the truncate MUST be the new id");
    assertEquals(observed[1], persistedId(recordDb),
        "the id published before the removal is the id that stays; the truncate draws exactly one");

    // Crash states: the copies carry the published id and open at the old head or the target, never
    // between.
    for (final Map.Entry<String, Path> snapshot : snapshots.entrySet()) {
      final Path state = snapshot.getValue();
      assertTrue(Files.notExists(state.resolve(DatabaseConfiguration.DatabasePaths.LOCK.getFile())),
          "crash snapshots contain persisted content, not the live process lock");
      Files.deleteIfExists(commitMarker(state));
      assertEquals(observed[1], persistedId(state), "crash state '" + snapshot.getKey() + "' lost the published id");
      Databases.clearGlobalCaches();
      final int head;
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(state);
          final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        head = session.getMostRecentRevisionNumber();
      }
      assertTrue(head == ORIGINAL_REVISION || head == TARGET_REVISION,
          "crash state '" + snapshot.getKey() + "' opened at revision " + head);
      assertEquals(observed[1], persistedId(state),
          "opening crash state '" + snapshot.getKey() + "' MUST NOT change the published id");
      dropRegistryEntries(state);
    }
  }

  /** Runs on the recording thread right after each recorded channel operation. */
  private static void observe(final PowerLossRecorder.Op op, final Path database, final String[] observed,
      final Map<String, Path> snapshots) {
    final String id = persistedId(database);
    if (observed[2] == null && !id.equals(observed[0])) {
      observed[2] = id;
      snapshots.put("idPublished", snapshot(database, "id-published"));
    }
    if (op.kind == PowerLossRecorder.OpKind.TRUNCATE && !snapshots.containsKey("firstRemoval")) {
      observed[1] = id;
      snapshots.put("firstRemoval", snapshot(database, "first-removal"));
    }
  }

  private static Path snapshot(final Path database, final String name) {
    final Path target = database.resolveSibling(name);
    try (var paths = Files.walk(database)) {
      for (final Path path : (Iterable<Path>) paths::iterator) {
        final Path destination = target.resolve(database.relativize(path));
        if (Files.isDirectory(path)) {
          Files.createDirectories(destination);
        } else if (!path.equals(database.resolve(DatabaseConfiguration.DatabasePaths.LOCK.getFile()))) {
          // The live process holds this file locked; Windows forbids reading it even for a copy.
          Files.createDirectories(destination.getParent());
          Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return target;
  }

  /** The persisted {@code incarnationId} member of the resource settings (CONF-102), well formed. */
  private static String persistedId(final Path database) {
    try {
      final JsonObject settings = JsonParser.parseString(
          Files.readString(database.resolve("resources").resolve(RESOURCE).resolve("ressetting.obj")))
                                            .getAsJsonObject();
      assertTrue(settings.has("incarnationId"), "the resource settings lack the incarnationId member");
      final String id = settings.get("incarnationId").getAsString();
      assertTrue(INCARNATION_ID.matcher(id).matches(), "malformed incarnationId '" + id + "'");
      return id;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Path commitMarker(final Path database) {
    return database.resolve("resources")
                   .resolve(RESOURCE)
                   .resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
                   .resolve(".commit");
  }

  private static void dropRegistryEntries(final Path database) {
    final Path key = database.resolve("resources")
                             .resolve(RESOURCE)
                             .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                             .resolve("sirix.data");
    StorageType.CACHE_REPOSITORY.remove(key);
    StorageType.REVISION_INDEX_REPOSITORY.remove(key);
  }
}
