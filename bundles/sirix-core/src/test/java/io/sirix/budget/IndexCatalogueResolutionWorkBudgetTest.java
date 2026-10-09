/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.XmlTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Work budget for resolving a transaction's index catalogue: a commit never lists the catalogue
 * directory.
 *
 * <p>
 * The catalogue of a revision is the newest {@code indexes/<revision>.xml} at or below it. A commit
 * writes one only when it changed the definitions (an index created or dropped, or the represented
 * catalogue re-published after a revert): every other commit saves the file creation, the XML
 * materialization and the metadata fsync, so the directory holds one file per catalogue change and
 * most revisions have none of their own. A writer asks for the revision it is about to create once
 * per commit. Answering that from a directory listing is work proportional to the number of
 * catalogue files on every commit; when every commit still wrote one, a one-operation-per-commit
 * load spent 78 % of its commit CPU there after 21,000 revisions. The definitions come out the same
 * either way, so only the listing and file counts tell the routes apart.
 *
 * <p>
 * Measured on these fixtures: a session lists at most once, when it cannot know better (at its
 * first transaction on a resource without a catalogue, or at the first lookup whose own and
 * previous revision both have no file), remembers every file it saw and every file written since,
 * and never lists during a commit: 0 in the 12 commits of the first session, 0 in the 6 commits
 * of the reopened one, 0 in the auto-committing load, 0 in the XML fixture. Resolving every writer from the
 * directory lists 12, 6, 13 and 4 times there. The files written are 3 in the first session
 * (revisions 3, 8 and 11), 1 in the reopened one (the drop at 15) and 0 afterwards; writing one per
 * commit with definitions, as before, writes 9, 3 and 0.
 *
 * <p>
 * A session that answers from what it remembers can also answer <em>wrongly</em>, which a listing
 * cannot: a catalogue its writer serialized but never reported, or a listing that remembered the
 * revision it answered for instead of the newest one it saw, makes a later revision read no
 * definitions. So every fixture is its own oracle: each revision's definitions are read back in the
 * session that wrote them, in a session that committed on top of them, and in a fresh one whose
 * first lookup is its oldest revision. Without the writer's report, revision 3 reads none. And a
 * catalogue committed through another handle on the same database is read back in the first handle.
 * The handles must also share catalogue knowledge: a second handle must not list again after the
 * first established that no catalogue exists, or lose definitions when resolving the next revision
 * after a catalogue committed through the other handle.
 */
@Isolated
final class IndexCatalogueResolutionWorkBudgetTest {

  private static final String RESOURCE = "catalogue";

  private static final String CATEGORY_PATH = "/[]/category";

  private static final WorkCounter LISTINGS = EngineWorkCounters.INDEX_CATALOGUE_LISTINGS;

  private static final WorkCounter FILES = EngineWorkCounters.INDEX_CATALOGUE_FILES_WRITTEN;

  private static final WorkCapture CAPTURE = WorkCapture.of(LISTINGS).and(FILES);

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    XmlTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
  }

  @Test
  void commitsNeverListTheCatalogueDirectory() throws Exception {
    final var databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    // CAS definitions in effect at each committed revision; revision 0 is the empty bootstrap.
    final int[] expected = new int[21];

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        // Nothing but the directory can tell a fresh session that its resource has no catalogue.
        final WorkCapture.Captured<JsonNodeTrx> firstWriter = CAPTURE.call(() -> session.beginNodeTrx());
        try (final JsonNodeTrx trx = firstWriter.result()) {
          firstWriter.work()
                     .assertExactly(LISTINGS, 1,
                         "the first writer of a resource without a catalogue did not establish that by one listing");

          final WorkReport commits = CAPTURE.run(() -> {
            trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"),
                JsonNodeTrx.Commit.NO);
            trx.commit(); // 1: data, the resource has no catalogue
            insertObject(trx, "b");
            trx.commit(); // 2
            createCasIndex(session, trx, 0);
            trx.commit(); // 3: the first catalogue
            expected[3] = 1;
            for (int i = 0; i < 4; i++) { // 4..7: data commits, no catalogue file
              insertObject(trx, "c" + i);
              trx.commit();
              expected[4 + i] = 1;
            }
            dropCasIndex(session, trx, 0);
            trx.commit(); // 8: the empty catalogue of the drop
            insertObject(trx, "d");
            trx.commit(); // 9: no catalogue file
            insertObject(trx, "e");
            trx.commit(); // 10: no catalogue file
            createCasIndex(session, trx, 1);
            trx.commit(); // 11
            expected[11] = 1;
            insertObject(trx, "rolled back");
            trx.rollback();
            assertEquals(1, casDefinitions(session.getWtxIndexController(trx.getRevisionNumber())),
                "the writer after a rollback keeps the committed catalogue");
            insertObject(trx, "f");
            trx.commit(); // 12
            expected[12] = 1;
            assertEquals(13, trx.getRevisionNumber(), "the fixture's revision bookkeeping");
          });
          commits.assertZero(LISTINGS,
              "a commit listed the index-catalogue directory to resolve its writer's catalogue");
          // The creation (3), the drop (8) and the re-creation (11) each write a file; the nine
          // commits that left the definitions as they were write none.
          commits.assertExactly(FILES, 3, "a commit that did not change the definitions wrote a catalogue file");
        }

        // The first writer's listing told the session every catalogue file there is, and every file
        // written since was reported to it: readers never touch the directory.
        CAPTURE.run(() -> assertDefinitions(session, expected, 12))
               .assertZero(LISTINGS, "readers listed the catalogue directory although the session knows every file");
      }
    }

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final WorkCapture.Captured<JsonNodeTrx> writerAfterReopen = CAPTURE.call(() -> session.beginNodeTrx());
      try (final JsonNodeTrx trx = writerAfterReopen.result()) {
        assertEquals(1, casDefinitions(session.getWtxIndexController(trx.getRevisionNumber())),
            "the writer after a reopen carries the committed catalogue");
        writerAfterReopen.work()
                         .assertZero(LISTINGS,
                             "the first writer of a session listed the directory although the previous revision has a catalogue");
        final WorkReport firstCommit = CAPTURE.run(() -> {
          insertObject(trx, "g");
          trx.commit(); // 13: no catalogue file
          expected[13] = 1;
        });
        firstCommit.assertZero(LISTINGS,
            "the first commit after reopen forgot the catalogue resolved at writer creation");
        firstCommit.assertZero(FILES, "a commit that did not change the definitions wrote a catalogue file");
        final WorkReport secondCommit = CAPTURE.run(() -> {
          insertObject(trx, "h");
          trx.commit(); // 14: no catalogue file
          expected[14] = 1;
        });
        secondCommit.assertZero(LISTINGS, "a later commit forgot the catalogue resolved at writer creation");
        secondCommit.assertZero(FILES, "a commit that did not change the definitions wrote a catalogue file");
        final WorkReport laterCommits = CAPTURE.run(() -> {
          dropCasIndex(session, trx, 1);
          trx.commit(); // 15: the empty catalogue of the drop
          insertObject(trx, "i");
          trx.commit(); // 16: no catalogue file
          insertObject(trx, "j");
          trx.commit(); // 17: no catalogue file
          insertObject(trx, "k");
          trx.commit(); // 18: no catalogue file
        });
        laterCommits.assertZero(LISTINGS,
            "a resource whose catalogue was emptied listed its directory on every commit");
        laterCommits.assertExactly(FILES, 1,
            "the drop of the last index did not write its empty catalogue, or an unchanged commit wrote one");
      }
      assertDefinitions(session, expected, 18);
    }

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      // Neither revision 18 nor 19 has a catalogue file: a fresh session has to list, once.
      final WorkCapture.Captured<JsonNodeTrx> writerAfterDrop = CAPTURE.call(() -> session.beginNodeTrx());
      try (final JsonNodeTrx trx = writerAfterDrop.result()) {
        assertEquals(0, casDefinitions(session.getWtxIndexController(trx.getRevisionNumber())),
            "the writer after a reopen sees the dropped catalogue");
        writerAfterDrop.work()
                       .assertExactly(LISTINGS, 1,
                           "the first writer of a session after the catalogue was emptied did not find it by one listing");
        final WorkReport commits = CAPTURE.run(() -> {
          insertObject(trx, "l");
          trx.commit(); // 19
          insertObject(trx, "m");
          trx.commit(); // 20
        });
        commits.assertZero(LISTINGS, "a resource whose catalogue was emptied listed its directory on every commit");
        commits.assertZero(FILES, "a commit of a resource without definitions wrote a catalogue file");
      }
      assertDefinitions(session, expected, 20);
    }

    // A session whose first lookup is its oldest revision: what that listing learns about the newest
    // catalogue must not be confused with what it answers for the revision that asked.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertDefinitions(session, expected, 20);
    }
  }

  @ParameterizedTest
  @CsvSource({"11, false", "12, false", "13, false", "11, true", "12, true", "13, true"})
  void reopenedJsonWriterRetainsResolvedCatalogueForSuccessors(final int committedRevision,
      final boolean historicalLookupFirst) throws Exception {
    final var databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"),
            JsonNodeTrx.Commit.NO);
        for (int revision = 1; revision <= committedRevision; revision++) {
          if (revision == 3 || revision == 11) {
            createCasIndex(session, trx, revision == 3 ? 0 : 1);
          }
          trx.commit();
        }
        final var indexes = session.getResourceConfig().getResource()
            .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
        assertTrue(Files.exists(indexes.resolve("11.xml")));
        assertFalse(Files.exists(indexes.resolve("12.xml")));
        assertFalse(Files.exists(indexes.resolve("13.xml")));
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(committedRevision, session.getMostRecentRevisionNumber());
      if (historicalLookupFirst) {
        assertEquals(1, casDefinitions(session.getRtxIndexController(3)));
      }
      final WorkCapture.Captured<JsonNodeTrx> writer = CAPTURE.call(() -> session.beginNodeTrx());
      writer.work().assertExactly(LISTINGS, committedRevision == 13 ? 1 : 0,
          "the reopened writer did not resolve the newest committed catalogue by the expected route");
      try (final JsonNodeTrx trx = writer.result()) {
        assertEquals(2, casDefinitions(session.getWtxIndexController(trx.getRevisionNumber())));
        final WorkReport commits = CAPTURE.run(() -> {
          trx.commit();
          trx.commit();
        });
        commits.assertZero(LISTINGS, "successor writers forgot the committed catalogue resolved after reopen");
        commits.assertZero(FILES, "unchanged commits wrote catalogue files after reopen");
        assertEquals(2, casDefinitions(session.getRtxIndexController(committedRevision + 2)));
        assertEquals(1, casDefinitions(session.getRtxIndexController(3)));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"11, false", "12, false", "13, false", "11, true", "12, true", "13, true"})
  void reopenedXmlWriterRetainsResolvedCatalogueForSuccessors(final int committedRevision,
      final boolean historicalLookupFirst) throws Exception {
    final var databasePath = XmlTestHelper.PATHS.PATH1.getFile();
    Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final XmlResourceSession session = database.beginResourceSession(RESOURCE);
          final XmlNodeTrx trx = session.beginNodeTrx()) {
        trx.insertElementAsFirstChild(new QNm("root"));
        for (int revision = 1; revision <= committedRevision; revision++) {
          if (revision == 3 || revision == 11) {
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(
                Set.of(IndexDefs.createNameIdxDef(revision == 3 ? 0 : 1, IndexDef.DbType.XML)), trx);
          }
          trx.commit();
        }
      }
    }
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath);
        final XmlResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(committedRevision, session.getMostRecentRevisionNumber());
      if (historicalLookupFirst) {
        assertEquals(1, session.getRtxIndexController(3).getIndexes().getNrOfIndexDefsWithType(IndexType.NAME));
      }
      final WorkCapture.Captured<XmlNodeTrx> writer = CAPTURE.call(() -> session.beginNodeTrx());
      writer.work().assertExactly(LISTINGS, committedRevision == 13 ? 1 : 0,
          "the reopened XML writer did not resolve the newest committed catalogue by the expected route");
      try (final XmlNodeTrx trx = writer.result()) {
        assertEquals(2, session.getWtxIndexController(trx.getRevisionNumber()).getIndexes()
            .getNrOfIndexDefsWithType(IndexType.NAME));
        final WorkReport commits = CAPTURE.run(() -> {
          trx.commit();
          trx.commit();
        });
        commits.assertZero(LISTINGS, "XML successor writers forgot the catalogue resolved after reopen");
        commits.assertZero(FILES, "unchanged XML commits wrote catalogue files after reopen");
        assertEquals(2, session.getRtxIndexController(committedRevision + 2).getIndexes()
            .getNrOfIndexDefsWithType(IndexType.NAME));
        assertEquals(1, session.getRtxIndexController(3).getIndexes().getNrOfIndexDefsWithType(IndexType.NAME));
      }
    }
  }

  @Test
  void netUnchangedCatalogueWritesNoFileAndPaysNoExtraBarrier() throws Exception {
    final var databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"),
            JsonNodeTrx.Commit.NO);
        createCasIndex(session, trx, 0);
        trx.commit();
        dropCasIndex(session, trx, 0);
        trx.commit();
        createCasIndex(session, trx, 1);
        dropCasIndex(session, trx, 1);
        final WorkReport commit = CAPTURE.and(EngineWorkCounters.DATA_FILE_FORCES).run(trx::commit);
        commit.assertZero(FILES, "an unchanged empty catalogue paid for a file and its metadata fsync");
        commit.assertZero(LISTINGS, "an unchanged empty catalogue listed the directory");
        commit.assertBetween(EngineWorkCounters.DATA_FILE_FORCES, 1, 2,
            "an unchanged catalogue added a barrier to the commit's data durability protocol");
        final var indexes = session.getResourceConfig().getResource()
            .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
        assertFalse(Files.exists(indexes.resolve("3.xml")));
        assertEquals(0, casDefinitions(session.getRtxIndexController(3)));
        CAPTURE.and(EngineWorkCounters.DATA_FILE_FORCES).run(trx::close)
            .assertZero(EngineWorkCounters.DATA_FILE_FORCES, "closing the committed writer added a force");
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(0, casDefinitions(session.getRtxIndexController(3)));
    }
  }

  @Test
  void intermediateCommitsWithoutACatalogueFileStillResolveWithoutListing() throws Exception {
    final var databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      final int indexedFrom;
      final int lastRevision;
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx(64)) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"), JsonNodeTrx.Commit.NO);
        createCasIndex(session, trx, 0);
        trx.commit();
        indexedFrom = trx.getRevisionNumber() - 1;

        // An auto-committing bulk insert: its intermediate commits skip an unchanged catalogue.
        final StringBuilder objects = new StringBuilder("[");
        for (int i = 0; i < 400; i++) {
          objects.append(i == 0
              ? ""
              : ",").append("{\"category\":\"bulk").append(i).append("\"}");
        }
        objects.append(']');
        final WorkReport load = CAPTURE.run(() -> {
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild(), "the array root");
          trx.insertSubtreeAsLastChild(JsonShredder.createStringReader(objects.toString()), JsonNodeTrx.Commit.NO);
          trx.commit();
        });
        lastRevision = trx.getRevisionNumber() - 1;
        load.assertZero(LISTINGS, "an auto-committing load listed the index-catalogue directory");
        load.assertZero(FILES, "an auto-committing load wrote catalogue files for unchanged definitions");

        final var indexes =
            session.getResourceConfig().getResource().resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
        int revisionsWithoutCatalogueFile = 0;
        for (int revision = indexedFrom; revision <= lastRevision; revision++) {
          if (!Files.exists(indexes.resolve(revision + ".xml"))) {
            revisionsWithoutCatalogueFile++;
          }
        }
        assertTrue(revisionsWithoutCatalogueFile > 0,
            "the load no longer leaves a revision without its own catalogue file, so this fixture does not"
                + " exercise that shape: " + (lastRevision - indexedFrom + 1) + " revisions, all with a file");
      }
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        for (int revision = indexedFrom; revision <= lastRevision; revision++) {
          assertEquals(1, casDefinitions(session.getRtxIndexController(revision)),
              "CAS definitions at revision " + revision);
        }
      }
    }
  }

  /**
   * A catalogue committed through another handle is visible at its exact revision without listing.
   */
  @Test
  void aCatalogueCommittedThroughAnotherSessionIsFoundAtItsRevision() throws Exception {
    final var databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"),
              JsonNodeTrx.Commit.NO);
          trx.commit(); // 1: no catalogue; the session has listed and remembers that
          insertObject(trx, "b");
          trx.commit(); // 2
          assertEquals(3, trx.getRevisionNumber(), "the fixture's revision bookkeeping");
        }
        assertEquals(0, casDefinitions(session.getRtxIndexController(2)), "no catalogue before the other commit");

        try (final Database<JsonResourceSession> other = Databases.openJsonDatabase(databasePath);
            final JsonResourceSession otherSession = other.beginResourceSession(RESOURCE);
            final JsonNodeTrx trx = otherSession.beginNodeTrx()) {
          createCasIndex(otherSession, trx, 0);
          trx.commit(); // 3: a catalogue published through the other handle
        }

        final WorkReport lookup = CAPTURE.run(() -> assertEquals(1, casDefinitions(session.getRtxIndexController(3)),
            "CAS definitions at the revision another session committed"));
        lookup.assertZero(LISTINGS, "a catalogue found through the revision's own file needed a listing");
      }
    }
  }

  /**
   * With per-session listing state the second handle lists once instead of zero times. With
   * per-session serialized state the first handle resolves zero CAS definitions instead of one. Both
   * defects were restored independently and this fixture failed at the corresponding assertion.
   */
  @Test
  void handlesShareListedAndSerializedCatalogueKnowledge() throws Exception {
    final var databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> first = Databases.openJsonDatabase(databasePath);
        final Database<JsonResourceSession> second = Databases.openJsonDatabase(databasePath)) {
      first.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = first.beginResourceSession(RESOURCE);
          final JsonResourceSession other = second.beginResourceSession(RESOURCE)) {
        final WorkCapture.Captured<JsonNodeTrx> firstWriter = CAPTURE.call(session::beginNodeTrx);
        try (final JsonNodeTrx trx = firstWriter.result()) {
          firstWriter.work().assertExactly(LISTINGS, 1, "the first writer establishes that no catalogue exists");
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"),
              JsonNodeTrx.Commit.NO);
          trx.commit();
        }
        CAPTURE.run(() -> assertEquals(0, casDefinitions(other.getRtxIndexController(1))))
               .assertZero(LISTINGS, "a second handle forgot the first handle's directory listing");

        try (final JsonNodeTrx trx = other.beginNodeTrx()) {
          createCasIndex(other, trx, 0);
          trx.commit(); // 2: the catalogue serialized through the second handle
        }
        final int revision = other.getMostRecentRevisionNumber();
        assertEquals(revision, session.getMostRecentRevisionNumber());
        final var indexes =
            other.getResourceConfig().getResource().resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
        assertTrue(Files.exists(indexes.resolve(revision + ".xml")), "the committed catalogue exists");
        assertFalse(Files.exists(indexes.resolve((revision + 1) + ".xml")), "the next revision has no own file");
        // Use a fresh reader controller to exercise resolution; the writer's next controller is cached.
        CAPTURE.run(() -> assertEquals(1, casDefinitions(session.getRtxIndexController(revision + 1)),
            "the first handle sees the catalogue serialized through the second"))
               .assertZero(LISTINGS, "a second handle forgot the other handle's serialized catalogue");
      }
    }
  }

  @Test
  void xmlCommitsNeverListTheCatalogueDirectory() throws Exception {
    final Database<XmlResourceSession> database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile());
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
    try (final XmlResourceSession session = database.beginResourceSession(RESOURCE);
        final XmlNodeTrx trx = session.beginNodeTrx()) {
      final XmlIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
      controller.createIndexes(Set.of(IndexDefs.createNameIdxDef(0, IndexDef.DbType.XML)), trx);
      trx.insertElementAsFirstChild(new QNm("root"));
      trx.commit(); // 1: the first catalogue

      final WorkReport commits = CAPTURE.run(() -> {
        for (int i = 0; i < 4; i++) { // 2..5
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild(), "the root element");
          trx.insertElementAsFirstChild(new QNm("child" + i));
          trx.commit();
        }
      });
      commits.assertZero(LISTINGS, "an XML commit listed the index-catalogue directory");
      commits.assertZero(FILES, "an XML commit that did not change the definitions wrote a catalogue file");

      for (int revision = 1; revision <= 5; revision++) {
        final XmlIndexController reader = session.getRtxIndexController(revision);
        assertEquals(1, reader.getIndexes().getNrOfIndexDefsWithType(IndexType.NAME),
            "NAME definitions at revision " + revision);
      }
    }
  }

  private static void createCasIndex(final JsonResourceSession session, final JsonNodeTrx trx, final int number) {
    final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
    controller.createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.STR,
        Set.of(Path.parse(CATEGORY_PATH, PathParser.Type.JSON)), number, IndexDef.DbType.JSON)), trx);
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

  private static void assertDefinitions(final JsonResourceSession session, final int[] expected,
      final int lastRevision) {
    for (int revision = 1; revision <= lastRevision; revision++) {
      assertEquals(expected[revision], casDefinitions(session.getRtxIndexController(revision)),
          "CAS definitions at revision " + revision);
    }
  }
}
