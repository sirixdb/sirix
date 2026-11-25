package io.sirix.access;

import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.page.StoreFeatures;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FMT-209 last-revert revision through the public transaction surface: the revision number of the
 * most recent commit that published a revert, 0 when none; published atomically with that commit,
 * carried forward by every later revision root, surviving cold reopen on both persistent backends
 * and every versioning strategy; a store carrying it sets readOnlyCompat bit 0 and stays writable
 * by this engine.
 */
final class LastRevertRevisionConformanceTest {

  private static final String RESOURCE = "resource";

  @TempDir
  Path temporary;

  static Stream<Arguments> backendsAndVersioning() {
    final Stream.Builder<Arguments> arguments = Stream.builder();
    for (final StorageType storage : new StorageType[] {StorageType.FILE_CHANNEL, StorageType.MEMORY_MAPPED}) {
      for (final VersioningType versioning : VersioningType.values()) {
        arguments.add(Arguments.of(storage, versioning));
      }
    }
    return arguments.build();
  }

  @ParameterizedTest
  @MethodSource("backendsAndVersioning")
  @DisplayName("FMT-209: the last-revert revision is published by the revert commit and carried forward")
  void lastRevertRevisionLifecycle(final StorageType storage, final VersioningType versioning) {
    final Path database = temporary.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(database));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database)) {
      assertTrue(db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).storageType(storage).versioningApproach(versioning).build()));
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        commit(session, 3);
        for (int revision = 1; revision <= 3; revision++) {
          assertEquals(0, lastRevert(session, revision), "a never-reverted resource reads 0 at " + revision);
        }
        assertTrue(features(session).isEmpty(), "no feature bit before the first revert");

        // A revert that is rolled back publishes nothing.
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.revertTo(1);
          wtx.rollback();
        }
        assertEquals(3, session.getMostRecentRevisionNumber());
        assertEquals(0, lastRevert(session, 3));

        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.revertTo(1);
          wtx.commit();
        }
        assertEquals(4, session.getMostRecentRevisionNumber(), "TXN-403: a revert appends head+1");
        assertEquals(4, lastRevert(session, 4), "published atomically with the revert commit");
        assertEquals(0, lastRevert(session, 3), "earlier revision roots are unchanged");
        assertEquals(StoreFeatures.CONDITIONAL_WRITE_METADATA,
            features(session).readOnlyCompat() & StoreFeatures.CONDITIONAL_WRITE_METADATA,
            "a store carrying the entry sets readOnlyCompat bit 0");

        commit(session, 2);
        assertEquals(4, lastRevert(session, 5), "carried forward unchanged");
        assertEquals(4, lastRevert(session, 6), "carried forward unchanged");
      }
    }

    // Cold reopen: the entry and the feature bit are read back, and the store stays writable.
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      assertEquals(4, lastRevert(session, 6));
      assertEquals(4, lastRevert(session, 4));
      assertEquals(0, lastRevert(session, 3));
      assertFalse(features(session).isEmpty());
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.revertTo(2);
        wtx.commit();
      }
      assertEquals(7, lastRevert(session, 7), "the most recent revert wins");
      commit(session, 1);
      assertEquals(7, lastRevert(session, 8));
    }
  }

  private static void commit(final JsonResourceSession session, final int count) {
    for (int i = 0; i < count; i++) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        if (session.getMostRecentRevisionNumber() == 0) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"r1\"]"), JsonNodeTrx.Commit.NO);
        } else {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"r\":" + i + "}"), JsonNodeTrx.Commit.NO);
        }
        wtx.commit();
      }
    }
  }

  private static int lastRevert(final JsonResourceSession session, final int revision) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      return rtx.getStorageEngineReader().getActualRevisionRootPage().getLastRevertRevision();
    }
  }

  private static StoreFeatures features(final JsonResourceSession session) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      return rtx.getStorageEngineReader().getUberPage().getFeatures();
    }
  }
}
