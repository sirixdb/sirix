package io.sirix.index;

import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.longlong.LongIterator;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static io.brackit.query.util.path.Path.parse;
import static io.brackit.query.util.path.PathParser.Type.JSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * PATH and NAME index construction over the default (HOT) index backend.
 *
 * <p>
 * Both families used to add a node to a key's posting list by reading the whole list back and
 * re-inserting it, which is quadratic in how many nodes share the key — and sharing keys is the
 * entire point of these two indexes: every node under an indexed path shares one PATH key, and
 * every element with the same name shares one NAME key. Building over an existing revision now
 * bulk-loads instead, so the tests below pin both the postings and the absence of the trie rebuilds
 * the incremental path used to trigger.
 * </p>
 */
public final class JsonPathAndNameIndexBuildTest {

  private static final String[] CATEGORIES = {"alpha", "beta", "gamma", "delta"};

  private static final int RECORDS = 1_200;

  private static final String TITLE_PATH = "/[]/title";

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /** {@code [{"title": "...", "category": "..."}, ...]} — every field name repeats per record. */
  private static String document() {
    final StringBuilder json = new StringBuilder(RECORDS * 56);
    json.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"title\":\"t")
          .append(i)
          .append("\",\"category\":\"")
          .append(CATEGORIES[i % CATEGORIES.length])
          .append("\"}");
    }
    return json.append(']').toString();
  }

  private static void shred(final JsonNodeTrx trx, final String json) {
    new JsonShredder.Builder(trx, JsonShredder.createStringReader(json),
        InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
  }

  private static TreeSet<Long> collect(final Iterator<NodeReferences> hits) {
    final TreeSet<Long> nodeKeys = new TreeSet<>();
    while (hits.hasNext()) {
      final LongIterator it = hits.next().getNodeKeys().getLongIterator();
      while (it.hasNext()) {
        nodeKeys.add(it.next());
      }
    }
    return nodeKeys;
  }

  private static IndexDef pathIndexDef() {
    return IndexDefs.createPathIdxDef(Collections.singleton(parse(TITLE_PATH, JSON)), 0, IndexDef.DbType.JSON);
  }

  private static IndexDef nameIndexDef() {
    return IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
  }

  private TreeSet<Long> pathPostings(final Path databasePath, final boolean indexFirst) {
    final var database = JsonTestHelper.getDatabase(databasePath);
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      final JsonIndexController indexController;
      if (indexFirst) {
        indexController = manager.getWtxIndexController(trx.getRevisionNumber());
        indexController.createIndexes(Set.of(pathIndexDef()), trx);
        shred(trx, document());
      } else {
        shred(trx, document());
        indexController = manager.getWtxIndexController(trx.getRevisionNumber());
        indexController.createIndexes(Set.of(pathIndexDef()), trx);
        trx.commit();
      }
      final IndexDef def = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
      return collect(indexController.openPathIndex(trx.getStorageEngineReader(), def,
          indexController.createPathFilter(Set.of(TITLE_PATH), trx)));
    }
  }

  private TreeSet<Long> namePostings(final Path databasePath, final boolean indexFirst) {
    final var database = JsonTestHelper.getDatabase(databasePath);
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      // A NAME definition's ID is offset by the name-index page slot, so ask for the ID the
      // definition itself carries rather than assuming 0.
      final IndexDef requested = nameIndexDef();
      final JsonIndexController indexController;
      if (indexFirst) {
        indexController = manager.getWtxIndexController(trx.getRevisionNumber());
        indexController.createIndexes(Set.of(requested), trx);
        shred(trx, document());
      } else {
        shred(trx, document());
        indexController = manager.getWtxIndexController(trx.getRevisionNumber());
        indexController.createIndexes(Set.of(requested), trx);
        trx.commit();
      }
      final IndexDef def = indexController.getIndexes().getIndexDef(requested.getID(), IndexType.NAME);
      return collect(indexController.openNameIndex(trx.getStorageEngineReader(), def,
          indexController.createNameFilter(Set.of("category"))));
    }
  }

  @Test
  public void bulkBuiltAndListenerBuiltPathIndexesAgree() {
    final TreeSet<Long> bulk = pathPostings(JsonTestHelper.PATHS.PATH1.getFile(), false);
    final TreeSet<Long> incremental = pathPostings(JsonTestHelper.PATHS.PATH2.getFile(), true);

    assertFalse("the path index must not be empty", bulk.isEmpty());
    assertEquals("one posting per title", RECORDS, bulk.size());
    assertEquals("postings must not depend on how the index was built", incremental, bulk);
  }

  @Test
  public void bulkBuiltAndListenerBuiltNameIndexesAgree() {
    final TreeSet<Long> bulk = namePostings(JsonTestHelper.PATHS.PATH1.getFile(), false);
    final TreeSet<Long> incremental = namePostings(JsonTestHelper.PATHS.PATH2.getFile(), true);

    assertFalse("the name index must not be empty", bulk.isEmpty());
    assertEquals("one posting per \"category\" field", RECORDS, bulk.size());
    assertEquals("postings must not depend on how the index was built", incremental, bulk);
  }

  /**
   * A NAME key is the field name as raw UTF-8, so nothing bounds it but the document. Both write
   * paths used to size their key buffer from the length the serializer <em>returned</em> — after it
   * had already written that many bytes into a 512-byte buffer — so a field name past roughly 500
   * characters wrote off the end. Both now size from an upper bound taken before the write, so the
   * only thing a long name costs is a bigger buffer.
   */
  @Test
  public void indexesAFieldNameLongerThanTheKeyBuffer() {
    final String longName = "n".repeat(2_000);
    final String json = "[{\"" + longName + "\":1},{\"" + longName + "\":2},{\"other\":3}]";

    final TreeSet<Long> bulk = longNamePostings(JsonTestHelper.PATHS.PATH1.getFile(), json, longName, false);
    final TreeSet<Long> incremental = longNamePostings(JsonTestHelper.PATHS.PATH2.getFile(), json, longName, true);

    assertEquals("both fields with the long name must be indexed", 2, bulk.size());
    assertEquals("postings must not depend on how the index was built", incremental, bulk);
  }

  private TreeSet<Long> longNamePostings(final Path databasePath, final String json, final String name,
      final boolean indexFirst) {
    final var database = JsonTestHelper.getDatabase(databasePath);
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      final IndexDef requested = nameIndexDef();
      final JsonIndexController indexController;
      if (indexFirst) {
        indexController = manager.getWtxIndexController(trx.getRevisionNumber());
        indexController.createIndexes(Set.of(requested), trx);
        shred(trx, json);
      } else {
        shred(trx, json);
        indexController = manager.getWtxIndexController(trx.getRevisionNumber());
        indexController.createIndexes(Set.of(requested), trx);
        trx.commit();
      }
      final IndexDef def = indexController.getIndexes().getIndexDef(requested.getID(), IndexType.NAME);
      return collect(indexController.openNameIndex(trx.getStorageEngineReader(), def,
          indexController.createNameFilter(Set.of(name))));
    }
  }

  @Test
  public void buildingOverAnExistingRevisionRebuildsNoSubtree() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      shred(trx, document());
      final JsonIndexController indexController = manager.getWtxIndexController(trx.getRevisionNumber());

      final long validationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();
      final long propagationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get();

      indexController.createIndexes(Set.of(pathIndexDef(), IndexDefs.createNameIdxDef(1, IndexDef.DbType.JSON)), trx);

      assertEquals("structural validation failures during a bulk build", validationFailuresBefore,
          AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get());
      assertEquals("propagation preflight failures during a bulk build", propagationFailuresBefore,
          AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get());
    }
  }
}
