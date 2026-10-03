package io.sirix.index.hot;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
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
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.roaringbitmap.longlong.LongIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Append-only posting deltas ({@link PostingDeltas}): a hot CAS posting list changed one node key
 * per commit must read exactly right at every revision — before, at and after every fold, after
 * removals, after a rollback and after a cold reopen — under all four versioning types, and the
 * delta path must actually be taken (source-level counters).
 */
final class PostingDeltaTest {

  private static final String RESOURCE = "posting-deltas";
  private static final String CATEGORY_PATH = "/[]/category";
  private static final String HOT = "hot";
  private static final int BASE_ROWS = 400;
  private static final int SINGLE_INSERTS = 150;
  private static final int SINGLE_REMOVALS = 20;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static IndexDef casDef() {
    return IndexDefs.createCASIdxDef(false, Type.STR, Set.of(Path.parse(CATEGORY_PATH, PathParser.Type.JSON)), 0,
        IndexDef.DbType.JSON);
  }

  private static TreeSet<Long> postings(final JsonIndexController controller, final JsonNodeReadOnlyTrx trx) {
    final IndexDef def = controller.getIndexes().getIndexDef(0, IndexType.CAS);
    final Iterator<NodeReferences> hits = controller.openCASIndex(trx.getStorageEngineReader(), def,
        controller.createCASFilter(Set.of(CATEGORY_PATH), new Str(HOT), SearchMode.EQUAL, new JsonPCRCollector(trx)));
    final TreeSet<Long> keys = new TreeSet<>();
    while (hits.hasNext()) {
      final LongIterator it = hits.next().getNodeKeys().getLongIterator();
      while (it.hasNext()) {
        keys.add(it.next());
      }
    }
    return keys;
  }

  /** Insert one hot object as the array's last child; returns the object's node key. */
  private static long insertHot(final JsonNodeTrx trx) {
    trx.moveToDocumentRoot();
    assertTrue(trx.moveToFirstChild(), "array root");
    trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"category\":\"" + HOT + "\"}"),
        JsonNodeTrx.Commit.NO);
    return trx.getNodeKey();
  }

  /** The postings gained exactly one key belonging to the inserted object; returns that key. */
  private static long gainedKey(final TreeSet<Long> before, final TreeSet<Long> after, final JsonNodeTrx trx,
      final long objectKey) {
    final TreeSet<Long> gained = new TreeSet<>(after);
    gained.removeAll(before);
    assertEquals(1, gained.size(), "exactly one posting gained");
    assertEquals(before.size() + 1, after.size(), "no posting lost");
    final long key = gained.first();
    assertTrue(trx.moveTo(key), "gained posting addresses a node");
    assertEquals(objectKey, trx.getParentKey(), "gained posting is the inserted object's value node");
    return key;
  }

  @ParameterizedTest(name = "{0}: hot postings exact across deltas, folds, removals, rollback and reopen")
  @EnumSource(VersioningType.class)
  void hotPostingsStayExact(final VersioningType versioningType) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final List<TreeSet<Long>> snapshots = new ArrayList<>(); // index = revision
    snapshots.add(new TreeSet<>()); // revision 0
    final List<Long> insertedObjects = new ArrayList<>();
    final List<Long> insertedKeys = new ArrayList<>();
    final long writesBefore = HOTIndexWriter.postingDeltaWrites();
    final long foldsBefore = HOTIndexWriter.postingDeltaFolds();
    final long referencedBefore = AbstractHOTIndexWriter.REFERENCED_CHUNK_WRITES.get();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(BASE_ROWS * 20).append('[');
        for (int i = 0; i < BASE_ROWS; i++) {
          json.append(i > 0
              ? ","
              : "")
              .append("{\"category\":\"")
              .append(i % 5 == 0
                  ? "cold"
                  : HOT)
              .append("\"}");
        }
        json.append(']');
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
        controller.createIndexes(Set.of(casDef()), trx);
        trx.commit(); // revision 1: the base chunk, hot from the start
        TreeSet<Long> expected = postings(session.getWtxIndexController(trx.getRevisionNumber()), trx);
        assertEquals(BASE_ROWS - BASE_ROWS / 5, expected.size(), "base postings");
        snapshots.add(new TreeSet<>(expected));
        for (int i = 0; i < SINGLE_INSERTS; i++) {
          final long objectKey = insertHot(trx);
          trx.commit();
          final TreeSet<Long> now = postings(session.getWtxIndexController(trx.getRevisionNumber()), trx);
          final long key = gainedKey(expected, now, trx, objectKey);
          insertedObjects.add(objectKey);
          insertedKeys.add(key);
          expected = now;
          snapshots.add(new TreeSet<>(expected));
        }
        assertTrue(HOTIndexWriter.postingDeltaWrites() - writesBefore >= SINGLE_INSERTS - 3,
            "the single inserts must take the delta path");
        assertTrue(HOTIndexWriter.postingDeltaFolds() - foldsBefore >= 2, "at least two folds happened");
        assertTrue(AbstractHOTIndexWriter.REFERENCED_CHUNK_WRITES.get() - referencedBefore >= 1,
            "hot folds must store referenced payloads");
        for (int i = 0; i < SINGLE_REMOVALS; i++) {
          final long objectKey = insertedObjects.get(i);
          final long key = insertedKeys.get(i);
          assertTrue(trx.moveTo(objectKey));
          trx.remove();
          trx.commit();
          final TreeSet<Long> now = postings(session.getWtxIndexController(trx.getRevisionNumber()), trx);
          final TreeSet<Long> want = new TreeSet<>(expected);
          assertTrue(want.remove(key), "removed key was expected");
          assertEquals(want, now, "postings after removing object " + i);
          expected = now;
          snapshots.add(new TreeSet<>(expected));
        }
        // rollback: three inserts discarded, postings unchanged
        for (int i = 0; i < 3; i++) {
          insertHot(trx);
        }
        trx.rollback();
        assertEquals(expected, postings(session.getWtxIndexController(trx.getRevisionNumber()), trx),
            "postings after rollback");
        final long objectKey = insertHot(trx);
        trx.commit();
        final TreeSet<Long> now = postings(session.getWtxIndexController(trx.getRevisionNumber()), trx);
        gainedKey(expected, now, trx, objectKey);
        expected = now;
        snapshots.add(new TreeSet<>(expected));
        assertEquals(snapshots.size() - 1, trx.getRevisionNumber() - 1, "snapshot per committed revision");
      }
      // reads at every revision, including those immediately before and after folds
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        assertRevisions(session, snapshots);
      }
    }
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertRevisions(session, snapshots);
    }
  }

  private static void assertRevisions(final JsonResourceSession session, final List<TreeSet<Long>> snapshots) {
    final int last = snapshots.size() - 1;
    for (int revision = 1; revision <= last; revision++) {
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        assertEquals(snapshots.get(revision), postings(session.getRtxIndexController(revision), rtx),
            "postings at revision " + revision);
      }
    }
  }
}
