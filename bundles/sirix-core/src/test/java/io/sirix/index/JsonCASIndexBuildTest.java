package io.sirix.index;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.longlong.LongIterator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * CAS index construction over the default (HOT) index backend.
 *
 * <p>
 * Covers both ways an index is populated, because they run through different machinery: an index
 * created <em>before</em> the data is inserted is filled entry-by-entry by the change listener,
 * while an index created <em>over an existing revision</em> is bulk-built from the traversal. Both
 * must produce the same postings, including for values that repeat across many nodes — the shape
 * the bulk-comparison corpus has, and the one that used to make index building quadratic.
 * </p>
 */
public final class JsonCASIndexBuildTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  /** Field values in the synthetic document; each repeats {@link #RECORDS} / its arity times. */
  private static final String[] CATEGORIES = {"alpha", "beta", "gamma", "delta", "epsilon"};

  private static final int RECORDS = 2_000;

  private static final String CATEGORY_PATH = "/[]/category";

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /**
   * A document of {@link #RECORDS} objects whose {@code category} cycles over {@link #CATEGORIES}.
   */
  private static String duplicateHeavyDocument() {
    final StringBuilder json = new StringBuilder(RECORDS * 48);
    json.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"id\":")
          .append(i)
          .append(",\"category\":\"")
          .append(CATEGORIES[i % CATEGORIES.length])
          .append("\"}");
    }
    return json.append(']').toString();
  }

  /**
   * The node keys the CAS index holds for every value in {@link #CATEGORIES}, as a value → sorted
   * node keys map.
   */
  private static Map<String, TreeSet<Long>> readPostings(final JsonIndexController indexController,
      final JsonNodeTrx trx, final IndexDef indexDef) {
    final Map<String, TreeSet<Long>> postings = new LinkedHashMap<>(CATEGORIES.length);
    for (final String category : CATEGORIES) {
      final Iterator<NodeReferences> hits =
          indexController.openCASIndex(trx.getStorageEngineReader(), indexDef, indexController.createCASFilter(
              Set.of(CATEGORY_PATH), new Str(category), SearchMode.EQUAL, new JsonPCRCollector(trx)));
      final TreeSet<Long> nodeKeys = new TreeSet<>();
      while (hits.hasNext()) {
        final LongIterator it = hits.next().getNodeKeys().getLongIterator();
        while (it.hasNext()) {
          nodeKeys.add(it.next());
        }
      }
      postings.put(category, nodeKeys);
    }
    return postings;
  }

  private static IndexDef categoryIndexDef(final int id) {
    return IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(parse(CATEGORY_PATH, PathParser.Type.JSON)),
        id, IndexDef.DbType.JSON);
  }

  private static void shred(final JsonNodeTrx trx, final String json) {
    new JsonShredder.Builder(trx, JsonShredder.createStringReader(json),
        InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
  }

  /** Postings of an index created over an already-shredded revision (the bulk-built path). */
  private Map<String, TreeSet<Long>> buildOverExistingRevision(final Path databasePath) {
    final var database = JsonTestHelper.getDatabase(databasePath);
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      shred(trx, duplicateHeavyDocument());
      final JsonIndexController indexController = manager.getWtxIndexController(trx.getRevisionNumber());
      indexController.createIndexes(Set.of(categoryIndexDef(0)), trx);
      trx.commit();
      return readPostings(indexController, trx, indexController.getIndexes().getIndexDef(0, IndexType.CAS));
    }
  }

  /** Postings of an index created before the data existed (the change-listener path). */
  private Map<String, TreeSet<Long>> buildWhileInserting(final Path databasePath) {
    final var database = JsonTestHelper.getDatabase(databasePath);
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      final JsonIndexController indexController = manager.getWtxIndexController(trx.getRevisionNumber());
      indexController.createIndexes(Set.of(categoryIndexDef(0)), trx);
      shred(trx, duplicateHeavyDocument());
      return readPostings(indexController, trx, indexController.getIndexes().getIndexDef(0, IndexType.CAS));
    }
  }

  @Test
  public void buildingOverAnExistingRevisionIndexesEveryOccurrenceOfARepeatedValue() {
    final Map<String, TreeSet<Long>> postings = buildOverExistingRevision(JsonTestHelper.PATHS.PATH1.getFile());

    long total = 0;
    for (final String category : CATEGORIES) {
      final TreeSet<Long> nodeKeys = postings.get(category);
      assertEquals("postings for " + category, RECORDS / CATEGORIES.length, nodeKeys.size());
      total += nodeKeys.size();
    }
    assertEquals("every record must be indexed exactly once", RECORDS, total);
  }

  /**
   * Building over an existing revision must materialise the trie in one canonical pass. The
   * incremental path it replaced reached the same result by inserting entry-by-entry and healing the
   * malformed nodes its folds produced — thousands of {@code O(subtree)} rebuilds for a document this
   * size, which is what made building an index over a large corpus impractical. Counting rebuilds
   * rather than milliseconds keeps the guard deterministic.
   */
  @Test
  public void buildingOverAnExistingRevisionRebuildsNoSubtree() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      shred(trx, duplicateHeavyDocument());
      final JsonIndexController indexController = manager.getWtxIndexController(trx.getRevisionNumber());

      final long validationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();
      final long propagationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get();

      indexController.createIndexes(Set.of(categoryIndexDef(0)), trx);

      assertEquals("structural validation failures during a bulk build", validationFailuresBefore,
          AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get());
      assertEquals("propagation preflight failures during a bulk build", propagationFailuresBefore,
          AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get());
    }
  }

  @Test
  public void bulkBuiltAndListenerBuiltIndexesAgree() {
    final Map<String, TreeSet<Long>> bulk = buildOverExistingRevision(JsonTestHelper.PATHS.PATH1.getFile());
    final Map<String, TreeSet<Long>> incremental = buildWhileInserting(JsonTestHelper.PATHS.PATH2.getFile());

    assertEquals("both builds must cover the same values", bulk.keySet(), incremental.keySet());
    for (final String category : CATEGORIES) {
      assertEquals("postings for " + category + " must not depend on how the index was built", bulk.get(category),
          incremental.get(category));
    }
  }

  /**
   * Every index now starts life as a {@link io.sirix.index.hot.HOTBulkBuilder}-produced trie rather
   * than as the empty leaf the incremental path used to grow from, so the first update a resource
   * takes after a build lands on a shape the incremental machinery had never been fed. This walks
   * that transition: bulk-build, commit, then insert and delete through the change listener — merging
   * into a key the bulk build already wrote, adding a key it never saw, and removing nodes of both —
   * and checks the postings still describe the document.
   */
  @Test
  public void incrementalInsertsAndDeletesAfterABulkBuild() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final Map<String, Integer> expected = new LinkedHashMap<>();
    for (final String category : CATEGORIES) {
      expected.put(category, RECORDS / CATEGORIES.length);
    }
    final String freshCategory = "zeta";
    expected.put(freshCategory, 0);

    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      shred(trx, duplicateHeavyDocument());
      final JsonIndexController indexController = manager.getWtxIndexController(trx.getRevisionNumber());
      indexController.createIndexes(Set.of(categoryIndexDef(0)), trx);
      trx.commit();

      // Insert as first child, so the most recently inserted object is the array's first child —
      // which is what the delete loop below then walks off the front.
      final int inserts = 20;
      trx.moveToDocumentRoot();
      trx.moveToFirstChild();
      final long arrayNodeKey = trx.getNodeKey();
      for (int i = 0; i < inserts; i++) {
        final String category = i % 2 == 0
            ? CATEGORIES[0]
            : freshCategory;
        trx.moveTo(arrayNodeKey);
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"category\":\"" + category + "\"}"),
            JsonNodeTrx.Commit.NO);
        expected.merge(category, 1, Integer::sum);
      }

      final int deletes = 6;
      for (int i = 0; i < deletes; i++) {
        final String category = (inserts - 1 - i) % 2 == 0
            ? CATEGORIES[0]
            : freshCategory;
        trx.moveTo(arrayNodeKey);
        trx.moveToFirstChild();
        trx.remove();
        expected.merge(category, -1, Integer::sum);
      }
      trx.commit();

      final IndexDef indexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);
      long total = 0;
      for (final Map.Entry<String, Integer> entry : expected.entrySet()) {
        final TreeSet<Long> nodeKeys = new TreeSet<>();
        final Iterator<NodeReferences> hits =
            indexController.openCASIndex(trx.getStorageEngineReader(), indexDef, indexController.createCASFilter(
                Set.of(CATEGORY_PATH), new Str(entry.getKey()), SearchMode.EQUAL, new JsonPCRCollector(trx)));
        while (hits.hasNext()) {
          final LongIterator it = hits.next().getNodeKeys().getLongIterator();
          while (it.hasNext()) {
            nodeKeys.add(it.next());
          }
        }
        assertEquals("postings for " + entry.getKey(), entry.getValue().intValue(), nodeKeys.size());
        for (final long nodeKey : nodeKeys) {
          assertTrue("index points at a live node", trx.moveTo(nodeKey));
          assertEquals("indexed node still holds the value it was indexed under", entry.getKey(), trx.getValue());
        }
        total += nodeKeys.size();
      }
      assertEquals("every remaining record must be indexed exactly once", RECORDS + 20 - 6, total);
    }
  }

  /**
   * The loader keeps composite keys in fixed 1 MiB blocks, so sorting, grouping and the final key
   * copy all have to address across block boundaries. Long values reach that in a few thousand
   * records: 10 header bytes + a 200-byte value + the 4-byte chunkIdx trailer is 214 bytes a key, so
   * this build spans two blocks. Values are near {@code MAX_STRING_VALUE_BYTES} but under it, so
   * nothing is truncated and every value stays distinguishable.
   */
  @Test
  public void buildsAnIndexWhoseKeysSpanSeveralBlocks() {
    final int records = 6_000;
    final int valueLength = 200;
    final StringBuilder json = new StringBuilder(records * (valueLength + 24));
    json.append('[');
    for (int i = 0; i < records; i++) {
      if (i > 0) {
        json.append(',');
      }
      final String suffix = Integer.toString(i);
      json.append("{\"category\":\"").append("v".repeat(valueLength - suffix.length())).append(suffix).append("\"}");
    }
    json.append(']');

    final String probe = "v".repeat(valueLength - 4) + "4242";

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      shred(trx, json.toString());
      final JsonIndexController indexController = manager.getWtxIndexController(trx.getRevisionNumber());
      indexController.createIndexes(Set.of(categoryIndexDef(0)), trx);
      trx.commit();

      final IndexDef indexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);
      assertEquals("every long value must be indexed", records,
          count(indexController.openCASIndex(trx.getStorageEngineReader(), indexDef,
              indexController.createCASFilter(Set.of(), null, SearchMode.EQUAL, new JsonPCRCollector(trx)))));

      final Iterator<NodeReferences> hits =
          indexController.openCASIndex(trx.getStorageEngineReader(), indexDef, indexController.createCASFilter(
              Set.of(CATEGORY_PATH), new Str(probe), SearchMode.EQUAL, new JsonPCRCollector(trx)));
      final TreeSet<Long> nodeKeys = new TreeSet<>();
      while (hits.hasNext()) {
        final LongIterator it = hits.next().getNodeKeys().getLongIterator();
        while (it.hasNext()) {
          nodeKeys.add(it.next());
        }
      }
      assertEquals("exactly one node holds the probed value", 1, nodeKeys.size());
      assertTrue(trx.moveTo(nodeKeys.first()));
      assertEquals("the indexed node holds the probed value", probe, trx.getValue());
    }
  }

  @Test
  public void buildsStringAndNumericIndexesOverARealCorpus() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();

      final JsonIndexController indexController = manager.getWtxIndexController(trx.getRevisionNumber());
      final var typePath = parse("/features/[]/type", PathParser.Type.JSON);
      final var namePath = parse("/features/[]/properties/name", PathParser.Type.JSON);
      indexController.createIndexes(
          Set.of(IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(typePath), 0, IndexDef.DbType.JSON),
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(namePath), 1, IndexDef.DbType.JSON)),
          trx);
      trx.commit();

      // "Feature" is the value of every /features/[]/type — the single-value extreme.
      final var typeDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);
      assertEquals(53,
          count(indexController.openCASIndex(trx.getStorageEngineReader(), typeDef, indexController.createCASFilter(
              Set.of("/features/[]/type"), new Str("Feature"), SearchMode.EQUAL, new JsonPCRCollector(trx)))));

      // Names are distinct — the all-singleton extreme.
      final var nameDef = indexController.getIndexes().getIndexDef(1, IndexType.CAS);
      assertEquals(1,
          count(indexController.openCASIndex(trx.getStorageEngineReader(), nameDef,
              indexController.createCASFilter(Set.of("/features/[]/properties/name"), new Str("ABC Radio Adelaide"),
                  SearchMode.EQUAL, new JsonPCRCollector(trx)))));
      assertEquals(53, count(indexController.openCASIndex(trx.getStorageEngineReader(), nameDef,
          indexController.createCASFilter(Set.of(), null, SearchMode.EQUAL, new JsonPCRCollector(trx)))));

      // A value that is not in the corpus must not be reported.
      assertTrue(count(indexController.openCASIndex(trx.getStorageEngineReader(), nameDef,
          indexController.createCASFilter(Set.of("/features/[]/properties/name"), new Str("no such station"),
              SearchMode.EQUAL, new JsonPCRCollector(trx)))) == 0);
    }
  }

  private static long count(final Iterator<NodeReferences> hits) {
    long total = 0;
    while (hits.hasNext()) {
      total += hits.next().getNodeKeys().getLongCardinality();
    }
    return total;
  }
}
