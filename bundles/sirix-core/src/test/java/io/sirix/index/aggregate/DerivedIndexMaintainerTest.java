package io.sirix.index.aggregate;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.changelog.ChangeLogSlab;
import io.sirix.index.changelog.ChangeLogStorage;
import io.sirix.io.StorageType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import io.sirix.utils.SirixFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DerivedIndexMaintainerTest {
  @TempDir
  File directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void durableSnapshotsLagRevertAndReopen(final VersioningType versioning) {
    final var path = directory.toPath().resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .validTimePaths("lo", "hi")
                                                   .customCommitTimestamps(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"g":1,"n":10,"lo":"2024-01-01T00:00:00Z","hi":"2024-12-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
            List.of(field("g"), field("n"), field("lo"), field("hi")), List.of(Type.LON, Type.LON, Type.STR, Type.STR),
            0, IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
        TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
            List.of("g"), List.of("sum(n)", "min(n)", "max(n)"));
        final DerivedIndexMaintainer.Resource state = state(session);
        state.pause();
        writer.commit("first", Instant.parse("2024-01-01T00:00:00Z"));
        try (StorageEngineReader source = session.createStorageEngineReader(1)) {
          assertNull(session.openDerivedIndexReader(source), "Lag must decline, never return an old aggregate");
        }
        assertEquals(1, database.listResources().size());
        assertThrows(RuntimeException.class, () -> database.beginResourceSession("records$derived"));
        state.resume();
        await(state, 1);
        assertSum(session, 1, 10);
        state.pause();
        setNumber(writer, 20);
        session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
        assertTrue(writer.getStorageEngineWriter()
                         .getLog()
                         .getList()
                         .stream()
                         .noneMatch(container -> container.getModified() instanceof HOTLeafPage page
                             && page.getIndexType() == IndexType.TEMPORAL_AGGREGATE));
        writer.commit("second", Instant.parse("2024-02-01T00:00:00Z"));
        assertSum(session, 1, 10);
        try (StorageEngineReader source = session.createStorageEngineReader(2)) {
          assertNull(session.openDerivedIndexReader(source));
        }
        state.resume();
        await(state, 2);
        assertSum(session, 2, 20);
        writer.revertTo(1);
        setNumber(writer, 7);
        writer.commit("revert", Instant.parse("2024-03-01T00:00:00Z"));
        await(state, 3);
        assertSum(session, 1, 10);
        assertSum(session, 2, 20);
        assertSum(session, 3, 7);
        writer.truncateTo(1);
        writer.close();
        state.pause();
        try (JsonNodeTrx replacement = session.beginNodeTrx()) {
          setNumber(replacement, 30);
          replacement.commit("replacement", Instant.parse("2024-02-01T00:00:00Z"));
        }
        try (StorageEngineReader source = session.createStorageEngineReader(2)) {
          assertNull(session.openDerivedIndexReader(source),
              "Reused revision and timestamp must invalidate old mapping");
        }
        state.resume();
        await(state, 2);
        assertSum(session, 2, 30);
      }
      // Closing the last user session must not close the source underneath the worker.
      try (JsonResourceSession session = database.beginResourceSession("records")) {
        assertSum(session, 2, 30);
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("records")) {
      await(state(session), 2);
      assertSum(session, 1, 10);
      assertSum(session, 2, 30);
    }
  }

  @Test
  void noDeclarationsDoNotStartMaintainer() {
    final var path = directory.toPath().resolve("plain");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(
          ResourceConfiguration.newBuilder("records").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("records")) {
        assertNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
        assertFalse(database.existsResource("records$derived"));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 5})
  void malformedLogPoisonsMaintenanceWithoutFailingTheSource(final int corruptRevision) {
    final var path = directory.toPath().resolve("poison");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        declareSimple(session, writer);
        final DerivedIndexMaintainer.Resource resource = state(session);
        resource.pause();
        writer.commit();
        resource.drain();
        for (int revision = 2; revision < corruptRevision; revision++) {
          setNumber(writer, revision);
          writer.commit();
        }
        setNumber(writer, 20);
        session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
        // The log is a persisted public storage contract: write a syntactically valid slab with a
        // schema that cannot describe this declaration, after the listener's final capture.
        try (ChangeLogStorage log = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
          log.write(
              new ChangeLogSlab(0, corruptRevision, corruptRevision - 1, new byte[] {ChangeLogSlab.LONG}, List.of()));
        }
        writer.commit();
        assertFalse(resource.maintainNextBatch());
        assertEquals(corruptRevision, resource.poisonedRevision());
        assertEquals(1, resource.maintainedRevision(), "Failed batches must not advance the durable frontier");
        assertNotNull(resource.failure());
        assertEquals(corruptRevision, session.getMostRecentRevisionNumber());
        try (StorageEngineReader source = session.createStorageEngineReader(corruptRevision)) {
          assertNull(session.openDerivedIndexReader(source));
        }
        setNumber(writer, 30);
        writer.commit();
        assertEquals(corruptRevision + 1, session.getMostRecentRevisionNumber(),
            "Poison must not stop source publication");
      }
    }
  }

  @Test
  void dropRecreateAndDatabaseReopenRetainHistoricalDefinitions() {
    final var path = directory.toPath().resolve("declarations");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        declareSimple(session, writer);
        final DerivedIndexMaintainer.Resource resource = state(session);
        resource.pause();
        writer.commit();
        resource.drain();
        final IndexDef first = session.getWtxIndexController(writer.getRevisionNumber())
                                      .getIndexes()
                                      .getIndexDef(0, IndexType.TEMPORAL_AGGREGATE);
        session.getWtxIndexController(writer.getRevisionNumber()).dropIndexes(Set.of(first), writer);
        setNumber(writer, 20);
        writer.commit();
        resource.drain();
        try (StorageEngineReader source = session.createStorageEngineReader(2)) {
          assertNull(session.openDerivedIndexReader(source));
        }
        final IndexDef second = TemporalAggregateDeclarations.declare(
            session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]", List.of("g"), List.of("sum(n)"));
        assertEquals(1, second.getID(), "Dropped ids must stay reserved without source aggregate pages");
        writer.commit();
        resource.drain();
        assertSum(session, 1, 10);
        try (StorageEngineReader source = session.createStorageEngineReader(3);
            StorageEngineReader derived = session.openDerivedIndexReader(source)) {
          assertNotNull(derived);
          assertNull(derived.getResourceSession()
                            .getRtxIndexController(derived.getRevisionNumber())
                            .getIndexes()
                            .getIndexDef(0, IndexType.TEMPORAL_AGGREGATE));
          assertNotNull(derived.getResourceSession()
                               .getRtxIndexController(derived.getRevisionNumber())
                               .getIndexes()
                               .getIndexDef(1, IndexType.TEMPORAL_AGGREGATE));
        }
      }
      database.removeResource("records");
      assertTrue(database.listResources().isEmpty());
      assertFalse(database.existsResource("records$derived"));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void batchesReplayInteriorRevertBasesAndTruncateToInterior(final VersioningType versioning) {
    final var path = directory.toPath().resolve("batch");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .versioningApproach(versioning)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          declareSimple(session, writer);
          final DerivedIndexMaintainer.Resource resource = state(session);
          resource.pause();
          writer.commit();
          resource.drain();
          for (int revision = 2; revision <= 9; revision++) {
            setNumber(writer, revision * 10);
            writer.commit();
          }
          assertTrue(resource.maintainNextBatch());
          assertEquals(9, resource.maintainedRevision());
          assertSum(session, 1, 10);
          assertSum(session, 9, 90);
          try (StorageEngineReader source = session.createStorageEngineReader(5)) {
            assertNull(session.openDerivedIndexReader(source),
                "Interior revisions must decline, never serve the batch end");
          }
          try (StorageEngineReader source = session.createStorageEngineReader(9);
              StorageEngineReader derived = session.openDerivedIndexReader(source)) {
            assertEquals(2, Objects.requireNonNull(derived).getRevisionNumber(),
                "Eight revisions share one durable commit");
          }
          writer.revertTo(5);
          setNumber(writer, 777);
          writer.commit();
          resource.drain();
          assertSum(session, 10, 777);
          assertSum(session, 9, 90);
          writer.truncateTo(5);
        }
        final DerivedIndexMaintainer.Resource resource = state(session);
        resource.drain();
        assertEquals(5, resource.maintainedRevision());
        assertSum(session, 5, 50);
        try (JsonNodeTrx replacement = session.beginNodeTrx()) {
          setNumber(replacement, 888);
          replacement.commit();
        }
        resource.drain();
        assertSum(session, 6, 888);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unavailableDerivedStoreRebuildsWithoutLosingSourceHistory(final boolean corrupt) throws Exception {
    final var path = directory.toPath().resolve("rebuild");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        declareSimple(session, writer);
        final DerivedIndexMaintainer.Resource resource = state(session);
        resource.pause();
        writer.commit();
        resource.drain();
        setNumber(writer, 20);
        writer.commit();
        resource.drain();
      }
    }
    final var derivedPath = path.resolve("resources").resolve("records$derived");
    assertTrue(Files.isDirectory(derivedPath));
    if (corrupt) {
      Files.writeString(derivedPath.resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath()), "broken");
    } else {
      SirixFiles.recursiveRemove(derivedPath);
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("records")) {
      final DerivedIndexMaintainer.Resource resource = state(session);
      resource.pause();
      resource.drain();
      assertSum(session, 1, 10);
      assertSum(session, 2, 20);
      assertEquals(1, database.listResources().size());
    }
  }

  @Test
  void sourceReaderOwnsAnExactSnapshotAndRetriesLag() {
    final var path = directory.toPath().resolve("reader-lifetime");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        declareSimple(session, writer);
        final DerivedIndexMaintainer.Resource resource = state(session);
        resource.pause();
        writer.commit();
        final StorageEngineReader snapshot;
        try (StorageEngineReader source = session.createStorageEngineReader(1)) {
          assertNull(source.getDerivedIndexReader());
          resource.drain();
          snapshot = source.getDerivedIndexReader();
          assertNotNull(snapshot);
          assertSame(snapshot, source.getDerivedIndexReader());
          assertNull(writer.getStorageEngineWriter().getDerivedIndexReader(), "Writers never borrow committed indexes");
          setNumber(writer, 20);
          writer.commit();
          resource.drain();
          assertSame(snapshot, source.getDerivedIndexReader(), "New publications do not replace an immutable view");
          assertFalse(snapshot.isClosed());
        }
        assertTrue(snapshot.isClosed(), "Closing the source releases its derived snapshot");
        assertSum(session, 2, 20);
      }
    }
  }

  @Test
  void pipelinedPublicationsAndUserSessionsShareOneMaintainer() {
    final var path = directory.toPath().resolve("pipelined");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession first = database.beginResourceSession("records");
          JsonResourceSession second = database.beginResourceSession("records")) {
        try (JsonNodeTrx writer = first.beginNodeTrx()) {
          declareSimple(first, writer);
          writer.commit();
        }
        final DerivedIndexMaintainer.Resource resource = state(first);
        assertSame(resource, state(second));
        try (JsonNodeTrx writer = second.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
          for (int value = 100; value <= 110; value++) {
            setNumber(writer, value);
          }
          writer.commit();
        }
        final int revision = first.getMostRecentRevisionNumber();
        assertTrue(revision > 2, "Intermediate commits must exercise pipelined publication");
        await(resource, revision);
        assertSum(first, revision, 110);
        assertNull(resource.failure());
      }
    }
  }

  @Test
  void sustainedBacklogAndRepeatedRevertsStayWithinThePartDirectory() {
    final var path = directory.toPath().resolve("folds");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        declareSimple(session, writer);
        final DerivedIndexMaintainer.Resource resource = state(session);
        resource.pause();
        writer.commit();
        resource.drain();
        for (int revision = 2; revision <= 180; revision++) {
          if (revision > 60 && revision % 17 == 0) {
            writer.revertTo(revision - 15);
          }
          setNumber(writer, revision);
          writer.commit();
        }
        while (resource.maintainedRevision() < 180) {
          assertTrue(resource.maintainNextBatch());
          assertNull(resource.failure());
          assertSum(session, resource.maintainedRevision(), resource.maintainedRevision());
        }
        resource.drain();
        assertSum(session, 180, 180);
      }
    }
  }

  private static void declareSimple(final JsonResourceSession session, final JsonNodeTrx writer) {
    writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
        [{"g":1,"n":10,"lo":"2024-01-01T00:00:00Z","hi":"2024-12-01T00:00:00Z"}]
        """), JsonNodeTrx.Commit.NO);
    final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
        List.of(field("g"), field("n"), field("lo"), field("hi")), List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0,
        IndexDef.DbType.JSON);
    session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
    TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
        List.of("g"), List.of("sum(n)", "min(n)", "max(n)"));
  }

  static DerivedIndexMaintainer.Resource state(final JsonResourceSession session) {
    final DerivedIndexMaintainer.Resource resource = ((AbstractResourceSession<?, ?>) session).getDerivedResource();
    assertNotNull(resource);
    return resource;
  }

  static void await(final DerivedIndexMaintainer.Resource state, final int revision) {
    final long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (state.maintainedRevision() < revision) {
      if (state.failure() != null) {
        throw new AssertionError("Derived maintenance failed", state.failure());
      }
      if (System.nanoTime() >= deadline) {
        throw new AssertionError("Timed out waiting for derived maintenance");
      }
      LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
    }
  }

  private static Path<QNm> field(final String name) {
    return Path.parse("/[]/" + name, PathParser.Type.JSON);
  }

  private static void setNumber(final JsonNodeTrx writer, final long value) {
    writer.moveToDocumentRoot();
    writer.moveToFirstChild();
    writer.moveToFirstChild();
    writer.moveToFirstChild();
    while (!"n".equals(writer.getName().getLocalName())) {
      writer.moveToRightSibling();
    }
    writer.moveToFirstChild();
    writer.setNumberValue(value);
  }

  private static void assertSum(final JsonResourceSession session, final int revision, final long sum) {
    try (StorageEngineReader source = session.createStorageEngineReader(revision)) {
      final StorageEngineReader derived = source.getDerivedIndexReader();
      assertNotNull(derived, "Expected an exact derived snapshot for revision " + revision);
      final IndexDef definition =
          session.getRtxIndexController(revision).getIndexes().getIndexDef(0, IndexType.TEMPORAL_AGGREGATE);
      final TemporalAggregateDomain domain = new TemporalAggregateDomain();
      final TemporalAggregateParts parts =
          new TemporalAggregateParts(definition.getTemporalAggregateSpec(), domain.height());
      try (TemporalAggregateStorage.Reader storage = new TemporalAggregateStorage.Reader(derived, 0)) {
        final int[] groups = {0};
        assertTrue(storage.groups(1, (key, group) -> {
          groups[0]++;
          final AggregateSummary summary =
              parts.point(storage.parts(), group.parts(), domain.point(Instant.parse("2024-06-01T00:00:00Z")));
          assertEquals(1, summary.count());
          assertEquals(BigInteger.valueOf(sum), summary.sum(0));
        }));
        assertEquals(1, groups[0]);
      }
    }
  }
}
