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
 * Batched commits: thousands of posting changes on hot values inside ONE transaction (many deltas
 * per chunk, several folds per transaction, leaf splits while delta slots are live), across several
 * popular values and several chunks, then removals in bulk — the shape of the SH1 per-publication
 * load that failed with the first delta layouts. Postings must be exact after every commit, at
 * every recorded revision and after a cold reopen, under all four versioning types.
 */
final class PostingDeltaBatchTest {

  private static final String RESOURCE = "posting-delta-batch";
  private static final String CATEGORY_PATH = "/[]/category";
  private static final String[] VALUES = {"hot-a", "hot-b", "hot-c"};
  private static final int BASE_PER_VALUE = 600;
  private static final int BATCH_PER_VALUE = 1_500; // > 20 folds per value inside one transaction
  private static final int BATCHES = 3;
  private static final int REMOVE_PER_VALUE = 700;

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

  private static Set<Long> postings(final JsonIndexController controller, final JsonNodeReadOnlyTrx trx,
      final String value) {
    final IndexDef def = controller.getIndexes().getIndexDef(0, IndexType.CAS);
    final Iterator<NodeReferences> hits = controller.openCASIndex(trx.getStorageEngineReader(), def,
        controller.createCASFilter(Set.of(CATEGORY_PATH), new Str(value), SearchMode.EQUAL, new JsonPCRCollector(trx)));
    final TreeSet<Long> keys = new TreeSet<>();
    while (hits.hasNext()) {
      final LongIterator it = hits.next().getNodeKeys().getLongIterator();
      while (it.hasNext()) {
        keys.add(it.next());
      }
    }
    return keys;
  }

  /**
   * Insert one object as the array's last child; returns its category node key (the indexed node).
   */
  private static long insert(final JsonNodeTrx trx, final String value) {
    trx.moveToDocumentRoot();
    assertTrue(trx.moveToFirstChild(), "array root");
    trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"category\":\"" + value + "\"}"),
        JsonNodeTrx.Commit.NO);
    final long object = trx.getNodeKey();
    assertTrue(trx.moveToFirstChild(), "category node");
    final long key = trx.getNodeKey();
    trx.moveTo(object);
    return key;
  }

  @ParameterizedTest(name = "{0}: thousands of deltas and folds per transaction stay exact")
  @EnumSource(VersioningType.class)
  void batchedDeltasStayExact(final VersioningType versioningType) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final List<Set<Long>[]> snapshots = new ArrayList<>(); // per revision, per value
    final long writesBefore = HOTIndexWriter.postingDeltaWrites();
    final long foldsBefore = HOTIndexWriter.postingDeltaFolds();
    final long referencedBefore = AbstractHOTIndexWriter.REFERENCED_CHUNK_WRITES.get();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      @SuppressWarnings("unchecked")
      final Set<Long>[] expected = new Set[VALUES.length];
      @SuppressWarnings("unchecked")
      final List<Long>[] objects = new List[VALUES.length];
      for (int v = 0; v < VALUES.length; v++) {
        expected[v] = new TreeSet<>();
        objects[v] = new ArrayList<>();
      }
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(BASE_PER_VALUE * VALUES.length * 24).append('[');
        for (int i = 0; i < BASE_PER_VALUE * VALUES.length; i++) {
          json.append(i > 0
              ? ","
              : "").append("{\"category\":\"").append(VALUES[i % VALUES.length]).append("\"}");
        }
        json.append(']');
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
        controller.createIndexes(Set.of(casDef()), trx);
        trx.commit(); // revision 1: three hot base chunks
        for (int v = 0; v < VALUES.length; v++) {
          expected[v] = postings(session.getWtxIndexController(trx.getRevisionNumber()), trx, VALUES[v]);
          assertEquals(BASE_PER_VALUE, expected[v].size(), "base postings of " + VALUES[v]);
        }
        snapshots.add(copy(expected));
        // batched inserts: thousands of deltas per value inside one transaction, several transactions
        for (int batch = 0; batch < BATCHES; batch++) {
          for (int i = 0; i < BATCH_PER_VALUE; i++) {
            for (int v = 0; v < VALUES.length; v++) {
              final long key = insert(trx, VALUES[v]);
              expected[v].add(key);
              objects[v].add(trx.getNodeKey());
            }
          }
          trx.commit();
          for (int v = 0; v < VALUES.length; v++) {
            assertEquals(expected[v], postings(session.getWtxIndexController(trx.getRevisionNumber()), trx, VALUES[v]),
                "postings of " + VALUES[v] + " after batch " + batch);
          }
          snapshots.add(copy(expected));
        }
        assertTrue(HOTIndexWriter.postingDeltaWrites() - writesBefore > 1_000, "deltas were written");
        assertTrue(HOTIndexWriter.postingDeltaFolds() - foldsBefore > 10, "folds happened inside batches");
        assertTrue(AbstractHOTIndexWriter.REFERENCED_CHUNK_WRITES.get() - referencedBefore >= 10,
            "hot folds must store referenced payloads");
        // batched removals: remove hundreds of objects per value in one transaction
        for (int v = 0; v < VALUES.length; v++) {
          for (int i = 0; i < REMOVE_PER_VALUE; i++) {
            final long object = objects[v].get(i);
            assertTrue(trx.moveTo(object));
            assertTrue(trx.moveToFirstChild());
            final long key = trx.getNodeKey();
            trx.moveTo(object);
            trx.remove();
            assertTrue(expected[v].remove(key), "removed key was indexed");
          }
        }
        trx.commit();
        for (int v = 0; v < VALUES.length; v++) {
          assertEquals(expected[v], postings(session.getWtxIndexController(trx.getRevisionNumber()), trx, VALUES[v]),
              "postings of " + VALUES[v] + " after the batched removal");
        }
        snapshots.add(copy(expected));
      }
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

  @SuppressWarnings("unchecked")
  private static Set<Long>[] copy(final Set<Long>[] sets) {
    final Set<Long>[] out = new Set[sets.length];
    for (int i = 0; i < sets.length; i++) {
      out[i] = new TreeSet<>(sets[i]);
    }
    return out;
  }

  private static void assertRevisions(final JsonResourceSession session, final List<Set<Long>[]> snapshots) {
    for (int i = 0; i < snapshots.size(); i++) {
      final int revision = i + 1;
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        for (int v = 0; v < VALUES.length; v++) {
          assertEquals(snapshots.get(i)[v], postings(session.getRtxIndexController(revision), rtx, VALUES[v]),
              "postings of " + VALUES[v] + " at revision " + revision);
        }
      }
    }
  }
}
