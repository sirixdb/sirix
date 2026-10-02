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
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Files;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Work budget for resolving a transaction's index catalogue: a commit never lists the catalogue
 * directory.
 *
 * <p>
 * The catalogue of a revision is the newest {@code indexes/<revision>.xml} at or below it, and a
 * commit with definitions writes one, so the directory holds about one file per revision. A writer
 * asks for the revision it is about to create, whose file cannot exist, once per commit. Answering
 * that from a directory listing is work proportional to the number of revisions on every commit; a
 * one-operation-per-commit load spent 78 % of its commit CPU there after 21,000 revisions. The
 * definitions come out the same either way, so only the listing count tells the routes apart.
 *
 * <p>
 * Measured on these fixtures: a session lists once when it cannot know better, at its first
 * transaction on a resource without a catalogue or at the first revision whose own and previous
 * catalogue are both missing, and its commits never list: 0 in the 12 commits of the first session,
 * 0 in the auto-committing load, 0 in the XML fixture. Resolving every writer from the directory,
 * as before, lists 12, 13 and 4 times there. Without the session's own knowledge the commits of a
 * resource that has no catalogue, or emptied it, list: 4 and 6. Without the previous-revision probe
 * the first writer of a reopened session lists, and so does a reader of a revision whose
 * predecessor's catalogue exists: the readers' capture reads 4 instead of at most 3.
 *
 * <p>
 * A session that answers from what it remembers can also answer <em>wrongly</em>, which a listing
 * cannot: a catalogue its writer serialized but never reported, or a listing that remembered the
 * revision it answered for instead of the newest one it saw, makes a later revision read no
 * definitions. So every fixture is its own oracle: each revision's definitions are read back in the
 * session that wrote them, in a session that committed on top of them, and in a fresh one whose
 * first lookup is its oldest revision. Without the writer's report, revision 3 reads none.
 */
@Isolated
final class IndexCatalogueResolutionWorkBudgetTest {

  private static final String RESOURCE = "catalogue";

  private static final String CATEGORY_PATH = "/[]/category";

  private static final WorkCounter LISTINGS = EngineWorkCounters.INDEX_CATALOGUE_LISTINGS;

  private static final WorkCapture CAPTURE = WorkCapture.of(LISTINGS);

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
            for (int i = 0; i < 4; i++) { // 4..7: data commits, one catalogue each
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
        }

        // Revisions 1, 2 and 10 have neither their own catalogue nor their predecessor's: reading
        // them is the listing's remaining job, and it is what proves the counter counts.
        CAPTURE.run(() -> assertDefinitions(session, expected, 12))
               .assertBetween(LISTINGS, 1, 3,
                   "readers listed the catalogue directory for a revision whose own or previous catalogue exists");
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
        final WorkReport commits = CAPTURE.run(() -> {
          insertObject(trx, "g");
          trx.commit(); // 13
          expected[13] = 1;
          insertObject(trx, "h");
          trx.commit(); // 14
          expected[14] = 1;
          dropCasIndex(session, trx, 1);
          trx.commit(); // 15: the empty catalogue of the drop
        });
        commits.assertZero(LISTINGS, "a commit after a reopen listed the index-catalogue directory");

        // This session has never listed the directory, so the first revision with neither its own
        // catalogue nor its predecessor's makes it list, once; from then on it knows.
        final WorkReport firstGap = CAPTURE.run(() -> {
          insertObject(trx, "i");
          trx.commit(); // 16: no catalogue file
        });
        firstGap.assertExactly(LISTINGS, 1,
            "the writer after the first revision without a catalogue did not establish the newest one by one listing");
        final WorkReport laterGaps = CAPTURE.run(() -> {
          insertObject(trx, "j");
          trx.commit(); // 17: no catalogue file
          insertObject(trx, "k");
          trx.commit(); // 18: no catalogue file
        });
        laterGaps.assertZero(LISTINGS, "a resource whose catalogue was emptied listed its directory on every commit");
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
