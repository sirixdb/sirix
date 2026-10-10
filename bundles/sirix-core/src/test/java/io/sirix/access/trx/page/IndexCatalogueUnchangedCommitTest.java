package io.sirix.access.trx.page;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/**
 * A commit serializes its index catalogue ({@code indexes/<revision>.xml}) only when the
 * definitions changed. The catalogue of a revision is the newest file at or below it, so the
 * unchanged commits in between need no file of their own, and what every revision resolves to must
 * survive a reopen: the catalogue created, the drop of the last index (an empty file, or a reopen
 * would resurrect the older catalogue), the represented catalogue of a revert (re-published even
 * when it is the same as the one it replaced, so a later open cannot inherit the newer one), and a
 * leftover file of a commit that was never acknowledged, which must neither be read by the writer
 * of that revision number nor survive its commit.
 */
final class IndexCatalogueUnchangedCommitTest {

  private static final String RESOURCE = "catalogue";

  private static final String CATEGORY_PATH = "/[]/category";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.closeEverything();
    JsonTestHelper.deleteEverything();
  }

  @Test
  void unchangedCommitsWriteNoFileAndEveryRevisionResolvesAfterReopen() throws Exception {
    final var databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final Path indexes;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        indexes = indexesDirectory(session);
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"), JsonNodeTrx.Commit.NO);
        trx.commit(); // 1: no catalogue
        createCasIndex(session, trx, 0);
        trx.commit(); // 2: the catalogue
        for (int i = 0; i < 3; i++) {
          insertObject(trx, "b" + i);
          trx.commit(); // 3, 4, 5: unchanged
        }
        assertEquals(6, trx.getRevisionNumber(), "the fixture's revision bookkeeping");
        assertFalse(Files.exists(indexes.resolve("1.xml")), "a revision without definitions has no file");
        assertTrue(Files.exists(indexes.resolve("2.xml")), "the catalogue's own revision has its file");
        for (int revision = 3; revision <= 5; revision++) {
          assertFalse(Files.exists(indexes.resolve(revision + ".xml")),
              "an unchanged commit wrote a catalogue file for revision " + revision);
        }
        for (int revision = 2; revision <= 5; revision++) {
          assertEquals(1, casDefinitions(session.getRtxIndexController(revision)),
              "CAS definitions at revision " + revision);
        }
      }
    }

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      for (int revision = 2; revision <= 5; revision++) {
        assertEquals(1, casDefinitions(session.getRtxIndexController(revision)),
            "CAS definitions at revision " + revision + " after a reopen");
      }
      assertEquals(0, casDefinitions(session.getRtxIndexController(1)), "no definitions before the catalogue");
      try (final JsonNodeTrx trx = session.beginNodeTrx()) {
        assertEquals(1, casDefinitions(session.getWtxIndexController(trx.getRevisionNumber())),
            "the writer after a reopen carries the catalogue of a revision without its own file");
        insertObject(trx, "c");
        trx.commit(); // 6: unchanged
        assertFalse(Files.exists(indexes.resolve("6.xml")), "an unchanged commit after a reopen wrote a file");
        dropCasIndex(session, trx, 0);
        trx.commit(); // 7: the empty catalogue of the drop
        assertTrue(Files.exists(indexes.resolve("7.xml")),
            "the drop of the last index must publish an empty catalogue");
        insertObject(trx, "d");
        trx.commit(); // 8: unchanged (empty)
        assertFalse(Files.exists(indexes.resolve("8.xml")), "a commit without definitions wrote a file");
        assertEquals(0, casDefinitions(session.getWtxIndexController(trx.getRevisionNumber())));
      }
    }

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(1, casDefinitions(session.getRtxIndexController(6)), "CAS definitions at revision 6");
      assertEquals(0, casDefinitions(session.getRtxIndexController(7)), "the drop must stick across a reopen");
      assertEquals(0, casDefinitions(session.getRtxIndexController(8)), "CAS definitions at revision 8");
      assertEquals(1, casDefinitions(session.getRtxIndexController(5)), "time travel keeps the older catalogue");
    }
  }

  @Test
  void revertRepublishesTheRepresentedCatalogue() throws Exception {
    final var databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final Path indexes;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        indexes = indexesDirectory(session);
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"), JsonNodeTrx.Commit.NO);
        trx.commit(); // 1: no catalogue
        createCasIndex(session, trx, 0);
        trx.commit(); // 2: the catalogue
        insertObject(trx, "b");
        trx.commit(); // 3: unchanged
        trx.revertTo(1);
        trx.commit(); // 4: represents revision 1, which has no definitions
        assertEquals(5, trx.getRevisionNumber(), "the fixture's revision bookkeeping");
        assertFalse(Files.exists(indexes.resolve("3.xml")), "an unchanged commit wrote a catalogue file");
        assertTrue(Files.exists(indexes.resolve("4.xml")),
            "a revert must re-publish its represented catalogue, or revision 4 would inherit revision 2's");
        assertEquals(0, casDefinitions(session.getRtxIndexController(4)), "the reverted revision has no definitions");
        assertEquals(1, casDefinitions(session.getRtxIndexController(3)), "the revision before the revert keeps its");
        insertObject(trx, "c");
        trx.commit(); // 5: unchanged (empty)
        assertFalse(Files.exists(indexes.resolve("5.xml")), "an unchanged commit after a revert wrote a file");
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(1, casDefinitions(session.getRtxIndexController(2)), "CAS definitions at revision 2");
      assertEquals(1, casDefinitions(session.getRtxIndexController(3)), "CAS definitions at revision 3");
      assertEquals(0, casDefinitions(session.getRtxIndexController(4)), "CAS definitions at revision 4");
      assertEquals(0, casDefinitions(session.getRtxIndexController(5)), "CAS definitions at revision 5");
    }
  }

  @Test
  void leftoverFileOfAnUnacknowledgedCommitIsNeitherReadNorKept() throws Exception {
    final var databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final Path indexes = indexesDirectory(session);
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"),
              JsonNodeTrx.Commit.NO);
          trx.commit(); // 1: no catalogue
          createCasIndex(session, trx, 0);
          trx.commit(); // 2: the catalogue
          assertEquals(3, trx.getRevisionNumber(), "the fixture's revision bookkeeping");
        }
        // A commit of revision 3 that serialized an (empty) catalogue and crashed before its beacon
        // leaves this file behind: revision 3 was never committed, so the file describes nothing.
        final Path leftover = indexes.resolve("3.xml");
        final String orphanCatalogue = "<indexes>" + " ".repeat(16_384) + "</indexes>";
        Files.writeString(leftover, orphanCatalogue, StandardCharsets.UTF_8);
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          assertEquals(3, trx.getRevisionNumber());
          assertEquals(1, casDefinitions(session.getWtxIndexController(trx.getRevisionNumber())),
              "the writer of revision 3 read the leftover file of an unacknowledged commit of that number");
          insertObject(trx, "b");
          trx.commit(); // 3: unchanged definitions, but the leftover must not survive as revision 3's catalogue
          assertTrue(Files.exists(leftover), "the committed revision's catalogue file");
          assertTrue(Files.size(leftover) < orphanCatalogue.length(), "the shorter replacement kept orphan bytes");
          assertEquals(1, casDefinitions(session.getRtxIndexController(3)), "CAS definitions at revision 3");
        }
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(1, casDefinitions(session.getRtxIndexController(3)), "CAS definitions at revision 3 after a reopen");
    }
  }

  @Test
  void numericCoverageChangesSurviveCommitAndReopen() throws Exception {
    final Path databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"value\":1}]"), JsonNodeTrx.Commit.NO);
        createNumericCasIndex(session, trx, 0, "/[]/value");
        trx.commit();
        assertTrue(
            session.getRtxIndexController(1).getIndexes().getIndexDef(0, IndexType.CAS).hasCompleteNumericCoverage());
        moveToNumericValue(trx);
        trx.setNumberValue(1.5);
        trx.commit();
        final IndexDef definition = session.getRtxIndexController(2).getIndexes().getIndexDef(0, IndexType.CAS);
        assertTrue(definition.hasNumericValuesOnly());
        assertFalse(definition.hasCompleteNumericCoverage());
        assertTrue(Files.exists(indexesDirectory(session).resolve("2.xml")));
        trx.commit();
        assertFalse(Files.exists(indexesDirectory(session).resolve("3.xml")));
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertTrue(
          session.getRtxIndexController(1).getIndexes().getIndexDef(0, IndexType.CAS).hasCompleteNumericCoverage());
      for (int revision = 2; revision <= 3; revision++) {
        final IndexDef definition = session.getRtxIndexController(revision).getIndexes().getIndexDef(0, IndexType.CAS);
        assertTrue(definition.hasNumericValuesOnly());
        assertFalse(definition.hasCompleteNumericCoverage());
        try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
          moveToNumericValue(reader);
          assertEquals(1.5, reader.getNumberValue().doubleValue());
        }
      }
    }
  }

  @Test
  void rollbackDoesNotMutateCachedCommittedCoverage() throws Exception {
    final Path databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"value\":1}]"), JsonNodeTrx.Commit.NO);
        createNumericCasIndex(session, trx, 0, "/[]/value");
        trx.commit();
        moveToNumericValue(trx);
        trx.setNumberValue(1.5);
        assertFalse(session.getWtxIndexController(trx.getRevisionNumber())
                           .getIndexes()
                           .getIndexDef(0, IndexType.CAS)
                           .hasCompleteNumericCoverage());
        trx.rollback();
        assertTrue(session.getWtxIndexController(trx.getRevisionNumber())
                          .getIndexes()
                          .getIndexDef(0, IndexType.CAS)
                          .hasCompleteNumericCoverage());
        assertTrue(
            session.getRtxIndexController(1).getIndexes().getIndexDef(0, IndexType.CAS).hasCompleteNumericCoverage());
        moveToNumericValue(trx);
        assertEquals(1, trx.getNumberValue().intValue());
        trx.commit();
        assertFalse(Files.exists(indexesDirectory(session).resolve("2.xml")));
      }
    }
  }

  @Test
  void initialListingRetainsEveryCataloguePublishedBeforeItsSnapshot() throws Exception {
    final Path databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"), JsonNodeTrx.Commit.NO);
        createCasIndex(session, trx, 0);
        trx.commit();
        trx.commit();
        createCasIndex(session, trx, 1);
        trx.commit();
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {
      final Path indexes = indexesDirectory(session);
      try (final MockedStatic<Files> files = mockStatic(Files.class, CALLS_REAL_METHODS)) {
        files.when(() -> {
          try (final Stream<Path> ignored = Files.list(indexes)) {
          }
        }).thenAnswer(invocation -> {
          @SuppressWarnings("unchecked")
          final Stream<Path> listed = (Stream<Path>) invocation.callRealMethod();
          return listed.onClose(() -> {
            createCasIndex(session, trx, 2);
            trx.commit();
            createCasIndex(session, trx, 3);
            trx.commit();
          });
        });
        assertEquals(0, casDefinitions(session.getRtxIndexController(0)));
      }
      assertEquals(3, casDefinitions(session.getRtxIndexController(4)));
      assertEquals(4, casDefinitions(session.getRtxIndexController(5)));
    }
  }

  @Test
  void reusedRevisionReloadsItsReplacementCatalogue() throws Exception {
    final Path databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"a\":1,\"b\":2,\"c\":3}]"),
              JsonNodeTrx.Commit.NO);
          createNumericCasIndex(session, trx, 0, "/[]/a");
          trx.commit();
          createNumericCasIndex(session, trx, 1, "/[]/b");
          trx.commit();
          assertCasPath(session, 2, 1, "/[]/b");
          trx.truncateTo(1);
        }
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          assertEquals(2, trx.getRevisionNumber());
          assertEquals(1, casDefinitions(session.getWtxIndexController(trx.getRevisionNumber())));
          createNumericCasIndex(session, trx, 1, "/[]/c");
          trx.commit();
          assertCasPath(session, 2, 1, "/[]/c");
          assertEquals(1, casDefinitions(session.getRtxIndexController(1)));
        }
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertCasPath(session, 2, 1, "/[]/c");
      assertEquals(1, casDefinitions(session.getRtxIndexController(1)));
    }
  }

  private static void moveToNumericValue(final JsonNodeReadOnlyTrx trx) {
    assertTrue(trx.moveToDocumentRoot() && trx.moveToFirstChild() && trx.moveToFirstChild() && trx.moveToFirstChild());
    if (!trx.isNumberValue()) {
      assertTrue(trx.moveToFirstChild());
    }
    assertTrue(trx.isNumberValue());
  }

  private static void createNumericCasIndex(final JsonResourceSession session, final JsonNodeTrx trx, final int number,
      final String path) {
    session.getWtxIndexController(trx.getRevisionNumber())
           .createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.INT, Set.of(parse(path, PathParser.Type.JSON)),
               number, IndexDef.DbType.JSON)), trx);
  }

  private static void assertCasPath(final JsonResourceSession session, final int revision, final int number,
      final String path) {
    assertEquals(Set.of(parse(path, PathParser.Type.JSON)),
        session.getRtxIndexController(revision).getIndexes().getIndexDef(number, IndexType.CAS).getPaths());
  }

  private static Path indexesDirectory(final JsonResourceSession session) throws IOException {
    final Path indexes =
        session.getResourceConfig().getResource().resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
    Files.createDirectories(indexes);
    return indexes;
  }

  private static void createCasIndex(final JsonResourceSession session, final JsonNodeTrx trx, final int number) {
    final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
    controller.createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.STR,
        Set.of(parse(CATEGORY_PATH, PathParser.Type.JSON)), number, IndexDef.DbType.JSON)), trx);
  }

  private static void dropCasIndex(final JsonResourceSession session, final JsonNodeTrx trx, final int number) {
    final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
    controller.dropIndexes(Set.of(controller.getIndexes().getIndexDef(number, IndexType.CAS)), trx);
  }

  private static void insertObject(final JsonNodeTrx trx, final String category) {
    trx.moveToDocumentRoot();
    assertTrue(trx.moveToFirstChild(), "the array root");
    trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"category\":\"" + category + "\"}"),
        JsonNodeTrx.Commit.NO);
  }

  private static int casDefinitions(final JsonIndexController controller) {
    return controller.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS);
  }
}
