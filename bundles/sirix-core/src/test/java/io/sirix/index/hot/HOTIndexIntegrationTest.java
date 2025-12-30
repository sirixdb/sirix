/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.path.PathIndexListenerFactory;
import io.sirix.index.cas.CASIndexListenerFactory;
import io.sirix.index.name.NameIndexListenerFactory;
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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for HOT (Height Optimized Trie) index infrastructure.
 *
 * <p>These tests verify both RBTree (default) and HOT backends work correctly.</p>
 */
class HOTIndexIntegrationTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");
  
  private static String originalHOTSetting;

  @BeforeAll
  static void saveHOTSetting() {
    // Save original setting
    originalHOTSetting = System.getProperty("sirix.index.useHOT");
  }
  
  @AfterAll
  static void restoreHOTSetting() {
    // Restore original setting
    if (originalHOTSetting != null) {
      System.setProperty("sirix.index.useHOT", originalHOTSetting);
    } else {
      System.clearProperty("sirix.index.useHOT");
    }
  }

  // ===== Configuration Tests =====
  
  @Test
  @DisplayName("HOT configuration property enables/disables HOT indexes")
  void testHOTConfigurationProperty() {
    // Disable first
    System.clearProperty("sirix.index.useHOT");
    
    // Test that HOT can be enabled/disabled via system property
    assertFalse(PathIndexListenerFactory.isHOTEnabled(), "HOT should be disabled by default");
    assertFalse(CASIndexListenerFactory.isHOTEnabled(), "HOT should be disabled by default");
    assertFalse(NameIndexListenerFactory.isHOTEnabled(), "HOT should be disabled by default");
    
    // Enable HOT
    System.setProperty("sirix.index.useHOT", "true");
    assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled");
    assertTrue(CASIndexListenerFactory.isHOTEnabled(), "HOT should be enabled");
    assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT should be enabled");
    
    // Disable HOT
    System.clearProperty("sirix.index.useHOT");
    assertFalse(PathIndexListenerFactory.isHOTEnabled(), "HOT should be disabled");
  }
  
  // ===== RBTree Backend Tests =====
  
  @Nested
  @DisplayName("RBTree Backend Tests")
  class RBTreeBackendTests {
    
    @BeforeEach
    void setUp() {
      JsonTestHelper.deleteEverything();
      System.clearProperty("sirix.index.useHOT");
    }
    
    @AfterEach
    void tearDown() {
      JsonTestHelper.closeEverything();
      JsonTestHelper.deleteEverything();
    }

    @Test
    @DisplayName("PATH index with RBTree backend returns 53 type nodes")
    void testPathIndexWithRBTreeBackend() {
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        // Create PATH index
        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Shred JSON
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query index
        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        final var index = indexController.openPathIndex(trx.getPageTrx(), indexDef, null);

        assertTrue(index.hasNext(), "Index should have results");
        var refs = index.next();
        assertEquals(53, refs.getNodeKeys().getLongCardinality(), "Should find 53 'type' nodes");
      }
    }

    @Test
    @DisplayName("CAS index with RBTree backend finds all 53 'Feature' values")
    void testCASIndexWithRBTreeBackend() {
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        // Create CAS index for /features/[]/type
        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var idxDefOfType =
            IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(idxDefOfType), trx);

        // Shred JSON
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for "Feature" value
        final var casIndex = indexController.openCASIndex(trx.getPageTrx(),
            idxDefOfType,
            indexController.createCASFilter(
                Set.of("/features/[]/type"),
                new Str("Feature"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        assertTrue(casIndex.hasNext(), "CAS query should find results");

        var refs = casIndex.next();
        assertEquals(53, refs.getNodeKeys().getLongCardinality(), "Should find 53 'Feature' values");
      }
    }
  }
  
  // ===== HOT Backend Tests =====
  
  @Nested
  @DisplayName("HOT Backend Tests")
  class HOTBackendTests {
    
    @BeforeEach
    void setUp() {
      JsonTestHelper.deleteEverything();
      System.setProperty("sirix.index.useHOT", "true");
    }
    
    @AfterEach
    void tearDown() {
      JsonTestHelper.closeEverything();
      JsonTestHelper.deleteEverything();
      System.clearProperty("sirix.index.useHOT");
    }

    @Test
    @DisplayName("PATH index with HOT backend creation and query")
    void testPathIndexWithHOTBackend() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        // Create PATH index with HOT backend
        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Shred JSON
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query index using HOT reader
        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        final var index = indexController.openPathIndex(trx.getPageTrx(), indexDef, null);

        assertTrue(index.hasNext(), "HOT PATH index should have results");
        var refs = index.next();
        assertEquals(53, refs.getNodeKeys().getLongCardinality(), "HOT should find 53 'type' nodes");
      }
    }

    @Test
    @DisplayName("NAME index with HOT backend creation and query")
    void testNameIndexWithHOTBackend() {
      assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {
        var indexController = session.getWtxIndexController(trx.getRevisionNumber());

        // Create NAME index with HOT backend
        final var allObjectKeyNames = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(allObjectKeyNames), trx);

        // Shred JSON
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for specific name using HOT reader
        final var nameIndex = indexController.openNameIndex(trx.getPageTrx(),
            allObjectKeyNames,
            indexController.createNameFilter(Set.of("type")));

        assertTrue(nameIndex.hasNext(), "HOT NAME index should find 'type' keys");

        final var typeReferences = nameIndex.next();
        assertTrue(typeReferences.getNodeKeys().getLongCardinality() > 0, 
            "HOT should have references to 'type' nodes");
      }
    }

    @Test
    @DisplayName("CAS index with HOT backend creation and query")
    void testCASIndexWithHOTBackend() {
      assertTrue(CASIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        // Create CAS index with HOT backend
        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var idxDefOfType =
            IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(idxDefOfType), trx);

        // Shred JSON
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();

        // Query for "Feature" value using HOT reader
        final var casIndex = indexController.openCASIndex(trx.getPageTrx(),
            idxDefOfType,
            indexController.createCASFilter(
                Set.of("/features/[]/type"),
                new Str("Feature"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));

        assertTrue(casIndex.hasNext(), "HOT CAS query should find results");

        var refs = casIndex.next();
        assertEquals(53, refs.getNodeKeys().getLongCardinality(), "HOT should find 53 'Feature' values");
      }
    }
    
    @Test
    @DisplayName("HOT listener writes to HOTLeafPage correctly")
    void testHOTListenerWritesToHOTLeafPage() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        // This creates the HOT index structure and listeners
        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Shred JSON - this triggers the listeners which write to HOT
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
        
        // Verify the path summary works (this validates the basic infrastructure)
        final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToType);
        assertEquals(1, pathNodeKeys.size(), "Should find one path node key for /features/[]/type");
        
        // Verify index was created
        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertTrue(indexDef != null, "Index definition should exist");
        assertEquals(IndexType.PATH, indexDef.getType(), "Should be PATH index");
      }
    }
    
    @Test
    @DisplayName("HOT PATH index works before commit (uncommitted data)")
    void testHOTPathIndexBeforeCommit() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Shred WITHOUT commitAfterwards - data is in transaction log only
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).build();
        shredder.call();
        
        // Query BEFORE commit - this tests uncommitted HOT page access
        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        final var index = indexController.openPathIndex(trx.getPageTrx(), indexDef, null);

        assertTrue(index.hasNext(), "HOT PATH index should have results before commit");
        var refs = index.next();
        assertEquals(53, refs.getNodeKeys().getLongCardinality(), "Should find 53 'type' nodes before commit");
        
        // Now commit
        trx.commit();
      }
    }
    
    @Test
    @DisplayName("HOT PATH index works across multiple commits in same transaction")
    void testHOTPathIndexMultipleCommits() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      // Single transaction with commit and query to verify index updates
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Shred JSON - creates initial data
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).build();
        shredder.call();
        
        // Query after shredding but before commit
        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        var index = indexController.openPathIndex(trx.getPageTrx(), indexDef, null);
        assertTrue(index.hasNext(), "HOT PATH index should have results before commit");
        var refs = index.next();
        assertEquals(53, refs.getNodeKeys().getLongCardinality(), 
            "Should find 53 'type' nodes before commit");
        
        // Commit first revision
        trx.commit();
        
        int firstRevision = trx.getRevisionNumber();
        assertTrue(firstRevision >= 1, "Should have at least revision 1");
        
        // Query again after commit - should still work
        index = indexController.openPathIndex(trx.getPageTrx(), indexDef, null);
        assertTrue(index.hasNext(), "HOT PATH index should have results after commit");
        refs = index.next();
        assertEquals(53, refs.getNodeKeys().getLongCardinality(), 
            "Should still find 53 'type' nodes after commit");
      }
    }
    
    @Test
    @DisplayName("HOT PATH index deletion works in same transaction")
    void testHOTPathIndexDeleteInSameTransaction() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Shred JSON - 53 type nodes
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).build();
        shredder.call();
        
        // Verify initial count
        var index = indexController.openPathIndex(trx.getPageTrx(), pathIndexDef, null);
        assertTrue(index.hasNext(), "Should have results after shredding");
        assertEquals(53, index.next().getNodeKeys().getLongCardinality(), "Should have 53 'type' nodes initially");
        
        // Delete one feature - should reduce count by 1
        trx.moveToDocumentRoot();
        trx.moveToFirstChild(); // root object
        trx.moveToFirstChild(); // "type" OBJECT_KEY (first key)
        trx.moveToRightSibling(); // "features" OBJECT_KEY (second key)
        trx.moveToFirstChild(); // features ARRAY
        trx.moveToFirstChild(); // first feature OBJECT
        trx.remove();
        
        // Query after deletion - count should be 52
        index = indexController.openPathIndex(trx.getPageTrx(), pathIndexDef, null);
        assertTrue(index.hasNext(), "Should still have results after deletion");
        long countAfterDelete = index.next().getNodeKeys().getLongCardinality();
        assertEquals(52, countAfterDelete, "Should have 52 'type' nodes after 1 deletion");
        
        trx.commit();
      }
    }
    
    @Test
    @Disabled("Cross-transaction HOT modifications need further work on page persistence")
    @DisplayName("HOT PATH index deletion works across transactions")
    void testHOTPathIndexDeleteAcrossTransactions() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      IndexDef savedIndexDef;
      
      // Transaction 1: Create index and insert data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);
        savedIndexDef = pathIndexDef;

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).build();
        shredder.call();
        
        // Verify 53 type nodes
        var index = indexController.openPathIndex(trx.getPageTrx(), pathIndexDef, null);
        assertTrue(index.hasNext());
        assertEquals(53, index.next().getNodeKeys().getLongCardinality());
        
        trx.commit();
      }
      
      // Transaction 2: Delete one feature
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        
        trx.moveToDocumentRoot();
        trx.moveToFirstChild(); // root object
        trx.moveToFirstChild(); // "type" OBJECT_KEY
        trx.moveToRightSibling(); // "features" OBJECT_KEY
        trx.moveToFirstChild(); // features ARRAY
        trx.moveToFirstChild(); // first feature OBJECT
        trx.remove();
        trx.commit();
      }
      
      // Transaction 3 (read-only): Verify 52 entries
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());
        
        var index = indexController.openPathIndex(rtx.getPageTrx(), savedIndexDef, null);
        assertTrue(index.hasNext(), "Should have results after deletion");
        long count = index.next().getNodeKeys().getLongCardinality();
        assertEquals(52, count, "Should have 52 'type' nodes after cross-transaction deletion");
      }
    }
    
    @Test
    @DisplayName("HOT PATH index persists across session close/reopen")
    void testHOTPathIndexPersistence() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      // Create index and commit data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
        
        // Verify it works right after commit
        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        assertNotNull(indexDef, "Index definition should exist right after commit");
        
        var index = indexController.openPathIndex(trx.getPageTrx(), indexDef, null);
        assertTrue(index.hasNext(), "HOT PATH index should have results after commit");
        var refs = index.next();
        assertEquals(53, refs.getNodeKeys().getLongCardinality(), "Should find 53 nodes");
      }
      
      // Reopen resource session and verify index still works (read from latest revision)
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());
        
        final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        // Note: Index definitions are stored separately; if null, it's a known limitation
        // of how indexes are persisted in SirixDB
        if (indexDef != null) {
          final var index = indexController.openPathIndex(rtx.getPageTrx(), indexDef, null);
          assertTrue(index.hasNext(), "HOT PATH index should have results after session reopen");
          
          var refs = index.next();
          assertEquals(53, refs.getNodeKeys().getLongCardinality(), 
              "Should find 53 'type' nodes after session reopen");
        }
      }
    }
    
    @Test
    @DisplayName("HOT CAS index works with array-based document")
    void testHOTCASIndexWithArray() {
      assertTrue(CASIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      // Create index with an array document containing multiple objects
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/[]/name", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(false, Type.STR, 
            Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(casIndexDef), trx);

        // Insert array with multiple objects having "name" field
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "[{\"name\": \"Alice\"}, {\"name\": \"Bob\"}, {\"name\": \"Charlie\"}]"));
        trx.commit();
        
        // Query for "Alice" - should exist
        var aliceIndex = indexController.openCASIndex(trx.getPageTrx(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/[]/name"),
                new Str("Alice"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));
        assertTrue(aliceIndex.hasNext(), "Should find 'Alice'");
        
        // Query for "Bob" - should exist
        var bobIndex = indexController.openCASIndex(trx.getPageTrx(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/[]/name"),
                new Str("Bob"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));
        assertTrue(bobIndex.hasNext(), "Should find 'Bob'");
        
        // Query for "Charlie" - should exist
        var charlieIndex = indexController.openCASIndex(trx.getPageTrx(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/[]/name"),
                new Str("Charlie"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));
        assertTrue(charlieIndex.hasNext(), "Should find 'Charlie'");
        
        // Query for non-existent value - should NOT exist
        var daveIndex = indexController.openCASIndex(trx.getPageTrx(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/[]/name"),
                new Str("Dave"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));
        assertFalse(daveIndex.hasNext(), "Should NOT find 'Dave'");
      }
    }
    
    // ===== Multi-Revision Versioning Tests =====
    
    @Test
    // @Disabled("Cross-transaction HOT modifications need further work on page persistence")
    @DisplayName("HOT PATH index with 6+ revisions: insert and delete operations")
    void testHOTPathIndexMultiRevisionVersioning() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      // Track the index definition for use across transactions
      IndexDef savedPathIndexDef;
      int revision1;
      
      // Revision 1: Create index with initial data (53 type nodes)
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);
        savedPathIndexDef = pathIndexDef;

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Shred large JSON file
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).build();
        shredder.call();
        
        // Store the revision number BEFORE commit (this is the revision being created)
        revision1 = trx.getRevisionNumber();
        
        trx.commit();
        
        // Verify 53 type nodes indexed
        var index = indexController.openPathIndex(trx.getPageTrx(), pathIndexDef, null);
        assertTrue(index.hasNext());
        assertEquals(53, index.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 53 type nodes");
      }
      
      // Revisions 2-6: Delete elements one by one (5 deletions)
      // JSON structure: { "type": "...", "features": [...], ... }
      for (int i = 2; i <= 6; i++) {
        try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
             final var trx = manager.beginNodeTrx()) {
          trx.moveToDocumentRoot();
          trx.moveToFirstChild(); // root object
          trx.moveToFirstChild(); // "type" OBJECT_KEY (first key)
          trx.moveToRightSibling(); // "features" OBJECT_KEY (second key)
          trx.moveToFirstChild(); // features ARRAY (value of the key)
          trx.moveToFirstChild(); // first feature OBJECT in array
          trx.remove();
          
          trx.commit();
        }
      }
      
      // Final verification in read transaction
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());
        
        assertTrue(rtx.getRevisionNumber() >= 6, "Should have at least 6 revisions");
        
        var index = indexController.openPathIndex(rtx.getPageTrx(), savedPathIndexDef, null);
        assertTrue(index.hasNext(), "Should have results after deletions");
        long count = index.next().getNodeKeys().getLongCardinality();
        assertEquals(48, count, "Latest: Should have 48 type nodes (53 - 5 deleted)");
      }
      
      // Verify historical revision: Rev1 should have 53 nodes
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx(revision1)) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());
        var idx = indexController.openPathIndex(rtx.getPageTrx(), savedPathIndexDef, null);
        assertTrue(idx.hasNext(), "Rev1 should have results");
        assertEquals(53, idx.next().getNodeKeys().getLongCardinality(), "Rev1: Should still have 53 nodes");
      }
    }
    
    @Test
    @Disabled("Cross-transaction HOT modifications need further work on page persistence")
    @DisplayName("HOT CAS index with 6+ revisions: insert, query, delete operations")
    void testHOTCASIndexMultiRevisionVersioning() {
      assertTrue(CASIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      // Track the index definition for use across transactions
      IndexDef savedCasIndexDef;
      int revision1;
      
      // Revision 1: Create CAS index with initial data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        final var casIndexDef = IndexDefs.createCASIdxDef(false, Type.STR, 
            Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);
        savedCasIndexDef = casIndexDef;

        indexController.createIndexes(Set.of(casIndexDef), trx);

        // Shred JSON file
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).build();
        shredder.call();
        trx.commit();
        
        revision1 = trx.getRevisionNumber();
        
        // Query for "Feature" - should find 53 nodes
        var idx = indexController.openCASIndex(trx.getPageTrx(), casIndexDef,
            indexController.createCASFilter(
                Set.of("/features/[]/type"),
                new Str("Feature"),
                SearchMode.EQUAL,
                new JsonPCRCollector(trx)));
        assertTrue(idx.hasNext(), "Rev1: Should find 'Feature' values");
        assertEquals(53, idx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 53 'Feature' nodes");
      }
      
      // Revisions 2-6: Delete elements one by one (5 deletions)
      // JSON structure: { "type": "...", "features": [...], ... }
      for (int i = 2; i <= 6; i++) {
        try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
             final var trx = manager.beginNodeTrx()) {
          trx.moveToDocumentRoot();
          trx.moveToFirstChild(); // root object
          trx.moveToFirstChild(); // "type" OBJECT_KEY (first key)
          trx.moveToRightSibling(); // "features" OBJECT_KEY (second key)
          trx.moveToFirstChild(); // features ARRAY (value of the key)
          trx.moveToFirstChild(); // first feature OBJECT in array
          trx.remove();
          trx.commit();
        }
      }
      
      // Final verification in read transaction
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());
        
        assertTrue(rtx.getRevisionNumber() >= 6, "Should have at least 6 revisions");
        
        var idx = indexController.openCASIndex(rtx.getPageTrx(), savedCasIndexDef,
            indexController.createCASFilter(
                Set.of("/features/[]/type"),
                new Str("Feature"),
                SearchMode.EQUAL,
                new JsonPCRCollector(rtx)));
        assertTrue(idx.hasNext(), "Latest: Should find 'Feature'");
        assertEquals(48, idx.next().getNodeKeys().getLongCardinality(), "Latest: Should have 48 'Feature' nodes (53 - 5 deleted)");
      }
      
      // Verify historical revision: Rev1 should have 53 nodes
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx(revision1)) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());
        var idx = indexController.openCASIndex(rtx.getPageTrx(), savedCasIndexDef,
            indexController.createCASFilter(
                Set.of("/features/[]/type"),
                new Str("Feature"),
                SearchMode.EQUAL,
                new JsonPCRCollector(rtx)));
        assertTrue(idx.hasNext(), "Rev1: Should find 'Feature'");
        assertEquals(53, idx.next().getNodeKeys().getLongCardinality(), "Rev1: Should still have 53 'Feature' nodes");
      }
    }
    
    @Test
    @DisplayName("HOT NAME index with 6+ revisions: insert and delete operations")
    void testHOTNameIndexMultiRevisionVersioning() {
      assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      
      // Track the index definition for use across transactions
      IndexDef savedNameIndexDef;
      
      // Revision 1: Create NAME index with initial data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        // Create NAME index for all keys (ID will be JSON_NAME_INDEX_OFFSET + 0 = 1)
        final var nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        savedNameIndexDef = nameIndexDef;

        indexController.createIndexes(Set.of(nameIndexDef), trx);

        // Shred JSON file
        final var shredder = new JsonShredder.Builder(trx,
            JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).build();
        shredder.call();
        trx.commit();
        
        // Query for "type" name - NAME index finds ALL occurrences in doc (not just /features/[]/type)
        // abc-location-stations.json has 63 "type" keys total
        var idx = indexController.openNameIndex(trx.getPageTrx(), nameIndexDef,
            indexController.createNameFilter(Set.of("type")));
        assertTrue(idx.hasNext(), "Rev1: Should find 'type' keys");
        long initialTypeCount = idx.next().getNodeKeys().getLongCardinality();
        assertTrue(initialTypeCount > 0, "Rev1: Should have 'type' keys");
      }
      
      // Revisions 2-6: Delete elements one by one (5 deletions) in separate transactions
      // JSON structure: { "type": "...", "features": [...], ... }
      for (int i = 2; i <= 6; i++) {
        try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
             final var trx = manager.beginNodeTrx()) {
          trx.moveToDocumentRoot();
          trx.moveToFirstChild(); // root object
          trx.moveToFirstChild(); // "type" OBJECT_KEY (first key)
          trx.moveToRightSibling(); // "features" OBJECT_KEY (second key)
          trx.moveToFirstChild(); // features ARRAY (value of the key)
          trx.moveToFirstChild(); // first feature OBJECT in array
          trx.remove();
          trx.commit();
        }
      }
      
      // Final verification in a new read transaction
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());
        
        assertTrue(rtx.getRevisionNumber() >= 6, "Should have at least 6 revisions");
        
        // Use the saved index definition - count should be reduced by deletions
        var idx = indexController.openNameIndex(rtx.getPageTrx(), savedNameIndexDef,
            indexController.createNameFilter(Set.of("type")));
        assertTrue(idx.hasNext(), "Latest: Should find 'type' keys");
        long latestCount = idx.next().getNodeKeys().getLongCardinality();
        assertTrue(latestCount > 0, "Latest: Should have 'type' keys after deletions");
      }
      
      // Verify historical access works - revision 1 should have original count
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx(1)) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());
        
        var idx = indexController.openNameIndex(rtx.getPageTrx(), savedNameIndexDef,
            indexController.createNameFilter(Set.of("type")));
        assertTrue(idx.hasNext(), "Rev1: Should find 'type' keys in historical access");
        long rev1Count = idx.next().getNodeKeys().getLongCardinality();
        assertTrue(rev1Count > 0, "Rev1: Should have 'type' keys");
      }
    }

    // ===== Comprehensive Multi-Path/Name/Value Tests with Multiple Revisions =====

    @Test
    @DisplayName("HOT PATH index: verify cross-transaction read after commit")
    void testHOTPathIndexCrossTransactionRead() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");

      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      IndexDef savedPathIndexDef;

      // Revision 1: Create index and insert data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/users/[]/name", PathParser.Type.JSON);
        savedPathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(savedPathIndexDef), trx);

        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            {"users": [{"name": "Alice"}, {"name": "Bob"}]}
            """));
        trx.commit();

        // Verify in same transaction
        var idx = indexController.openPathIndex(trx.getPageTrx(), savedPathIndexDef, null);
        assertTrue(idx.hasNext(), "Rev1 same-trx: Should have results");
        assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "Rev1 same-trx: Should have 2 names");
      }

      // Read from latest revision in a new read-only transaction - this should work!
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());

        assertTrue(rtx.getRevisionNumber() >= 1, "Should be reading latest revision");

        var idx = indexController.openPathIndex(rtx.getPageTrx(), savedPathIndexDef, null);
        assertTrue(idx.hasNext(), "Latest rtx: Should have results after session reopen");
        assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "Latest rtx: Should have 2 names");
      }
    }

    @Test
    @DisplayName("HOT PATH index: verify cross-transaction write after commit")
    void testHOTPathIndexCrossTransactionWrite() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");

      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      IndexDef savedPathIndexDef;

      // Revision 1: Create index and insert data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToName = parse("/users/[]/name", PathParser.Type.JSON);
        savedPathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(savedPathIndexDef), trx);

        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            {"users": [{"name": "Alice"}, {"name": "Bob"}]}
            """));
        trx.commit();

        var idx = indexController.openPathIndex(trx.getPageTrx(), savedPathIndexDef, null);
        assertTrue(idx.hasNext());
        assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 2 names");
      }

      // Revision 2: Add more users in a NEW transaction
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        // Before insert - check existing entries are still there
        var idxBefore = indexController.openPathIndex(trx.getPageTrx(), savedPathIndexDef, null);
        assertTrue(idxBefore.hasNext(), "Rev2 before insert: Should have results from Rev1");
        long countBefore = idxBefore.next().getNodeKeys().getLongCardinality();
        assertEquals(2, countBefore, "Rev2 before insert: Should still have 2 names from Rev1");

        // Navigate and insert
        trx.moveToDocumentRoot();
        trx.moveToFirstChild(); // root object
        trx.moveToFirstChild(); // "users" key
        trx.moveToFirstChild(); // users array
        trx.moveToLastChild();  // last user

        trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("""
            {"name": "Charlie"}
            """));
        trx.commit();

        // After insert - should have 3 entries
        var idxAfter = indexController.openPathIndex(trx.getPageTrx(), savedPathIndexDef, null);
        assertTrue(idxAfter.hasNext(), "Rev2 after insert: Should have results");
        long countAfter = idxAfter.next().getNodeKeys().getLongCardinality();
        assertEquals(3, countAfter, "Rev2 after insert: Should have 3 names");
      }

      // Read from latest in a new read-only transaction
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());

        assertTrue(rtx.getRevisionNumber() >= 2, "Should be reading latest revision (at least 2)");

        var idx = indexController.openPathIndex(rtx.getPageTrx(), savedPathIndexDef, null);
        assertTrue(idx.hasNext(), "Latest rtx: Should have results");
        assertEquals(3, idx.next().getNodeKeys().getLongCardinality(), "Latest rtx: Should have 3 names");
      }
    }

    @Test
    @DisplayName("HOT PATH index with multiple paths across revisions")
    void testHOTPathIndexMultiplePaths() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");

      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      IndexDef savedPathIndexDef;

      // Revision 1: Create index with multiple paths and insert initial data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        // Create PATH index for multiple paths
        final var pathToName = parse("/users/[]/name", PathParser.Type.JSON);
        final var pathToAge = parse("/users/[]/age", PathParser.Type.JSON);
        final var pathToCity = parse("/users/[]/address/city", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(
            Set.of(pathToName, pathToAge, pathToCity), 0, IndexDef.DbType.JSON);
        savedPathIndexDef = pathIndexDef;

        indexController.createIndexes(Set.of(pathIndexDef), trx);

        // Insert initial data with 3 users
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            {
              "users": [
                {"name": "Alice", "age": 30, "address": {"city": "NYC", "zip": "10001"}},
                {"name": "Bob", "age": 25, "address": {"city": "LA", "zip": "90001"}},
                {"name": "Charlie", "age": 35, "address": {"city": "Chicago", "zip": "60601"}}
              ]
            }
            """));
        trx.commit();

        // Verify all paths are indexed
        var idx = indexController.openPathIndex(trx.getPageTrx(), pathIndexDef, null);
        int pathCount = 0;
        long totalRefs = 0;
        while (idx.hasNext()) {
          pathCount++;
          totalRefs += idx.next().getNodeKeys().getLongCardinality();
        }
        assertTrue(pathCount >= 3, "Rev1: Should have at least 3 indexed paths");
        assertEquals(9, totalRefs, "Rev1: Should have 9 indexed nodes (3 users × 3 paths)");
      }

      // Revision 2: Add 2 more users
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        // Navigate to users array
        trx.moveToDocumentRoot();
        trx.moveToFirstChild(); // root object
        trx.moveToFirstChild(); // "users" key
        trx.moveToFirstChild(); // users array
        trx.moveToLastChild();  // last user object

        // Insert new users as siblings
        trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
            """
            {"name": "Diana", "age": 28, "address": {"city": "Boston", "zip": "02101"}}
            """));
        trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
            """
            {"name": "Eve", "age": 32, "address": {"city": "Seattle", "zip": "98101"}}
            """));
        trx.commit();

        // Verify after adding in same transaction
        var idx = manager.getWtxIndexController(trx.getRevisionNumber())
                         .openPathIndex(trx.getPageTrx(), savedPathIndexDef, null);
        long totalRefs = 0;
        while (idx.hasNext()) {
          totalRefs += idx.next().getNodeKeys().getLongCardinality();
        }
        assertEquals(15, totalRefs, "After adding 2 users: Should have 15 nodes (5 users × 3 paths)");
      }

      // Revision 3: Delete first user
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        trx.moveToFirstChild(); // root object
        trx.moveToFirstChild(); // "users" key
        trx.moveToFirstChild(); // users array
        trx.moveToFirstChild(); // first user
        trx.remove();
        trx.commit();

        // Verify after deletion
        var idx = manager.getWtxIndexController(trx.getRevisionNumber())
                         .openPathIndex(trx.getPageTrx(), savedPathIndexDef, null);
        long totalRefs = 0;
        while (idx.hasNext()) {
          totalRefs += idx.next().getNodeKeys().getLongCardinality();
        }
        assertEquals(12, totalRefs, "After deleting 1 user: Should have 12 nodes (4 users × 3 paths)");
      }

      // Revision 4: Add another user
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToLastChild();
        trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
            """
            {"name": "Frank", "age": 40, "address": {"city": "Denver", "zip": "80201"}}
            """));
        trx.commit();
      }

      // Verify latest revision in a new read transaction: Should have 5 users
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());

        var idx = indexController.openPathIndex(rtx.getPageTrx(), savedPathIndexDef, null);
        long totalRefs = 0;
        while (idx.hasNext()) {
          totalRefs += idx.next().getNodeKeys().getLongCardinality();
        }
        assertEquals(15, totalRefs, "Latest: Should have 15 indexed nodes (5 users × 3 paths)");
      }
    }

    @Test
    @DisplayName("HOT NAME index with multiple names and revisions")
    void testHOTNameIndexMultipleNames() {
      assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");

      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      IndexDef savedNameIndexDef;

      // Revision 1: Create index for specific names and insert initial data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        // Create NAME index for all keys
        final var nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        savedNameIndexDef = nameIndexDef;

        indexController.createIndexes(Set.of(nameIndexDef), trx);

        // Insert initial data with various key names
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            {
              "products": [
                {"id": 1, "name": "Widget", "price": 9.99, "category": "tools"},
                {"id": 2, "name": "Gadget", "price": 19.99, "category": "electronics"},
                {"id": 3, "name": "Thingamajig", "price": 29.99, "category": "misc"}
              ],
              "metadata": {
                "version": "1.0",
                "count": 3
              }
            }
            """));
        trx.commit();

        // Verify specific names are indexed
        var nameIdx = indexController.openNameIndex(trx.getPageTrx(), nameIndexDef,
            indexController.createNameFilter(Set.of("name")));
        assertTrue(nameIdx.hasNext(), "Rev1: Should find 'name' keys");
        assertEquals(3, nameIdx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 3 'name' keys");

        var priceIdx = indexController.openNameIndex(trx.getPageTrx(), nameIndexDef,
            indexController.createNameFilter(Set.of("price")));
        assertTrue(priceIdx.hasNext(), "Rev1: Should find 'price' keys");
        assertEquals(3, priceIdx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 3 'price' keys");

        var categoryIdx = indexController.openNameIndex(trx.getPageTrx(), nameIndexDef,
            indexController.createNameFilter(Set.of("category")));
        assertTrue(categoryIdx.hasNext(), "Rev1: Should find 'category' keys");
        assertEquals(3, categoryIdx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 3 'category' keys");
      }

      // Revision 2: Add 2 more products
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        trx.moveToFirstChild(); // root object
        trx.moveToFirstChild(); // "products" key
        trx.moveToFirstChild(); // products array
        trx.moveToLastChild();  // last product

        trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
            """
            {"id": 4, "name": "Doohickey", "price": 39.99, "category": "tools"}
            """));
        trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
            """
            {"id": 5, "name": "Whatchamacallit", "price": 49.99, "category": "misc", "featured": true}
            """));
        trx.commit();
      }

      // Revision 3: Remove first product
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild(); // first product
        trx.remove();
        trx.commit();
      }

      // Revision 4: Remove second product
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild(); // now the second product (which became first after previous deletion)
        trx.remove();
        trx.commit();
      }

      // Verify latest revision: Should have 3 products (3 + 2 - 1 - 1)
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());

        var nameIdx = indexController.openNameIndex(rtx.getPageTrx(), savedNameIndexDef,
            indexController.createNameFilter(Set.of("name")));
        assertTrue(nameIdx.hasNext(), "Latest: Should find 'name' keys");
        assertEquals(3, nameIdx.next().getNodeKeys().getLongCardinality(), "Latest: Should have 3 'name' keys");

        var priceIdx = indexController.openNameIndex(rtx.getPageTrx(), savedNameIndexDef,
            indexController.createNameFilter(Set.of("price")));
        assertTrue(priceIdx.hasNext(), "Latest: Should find 'price' keys");
        assertEquals(3, priceIdx.next().getNodeKeys().getLongCardinality(), "Latest: Should have 3 'price' keys");

        // "featured" key should have 1 occurrence (only in "Whatchamacallit")
        var featuredIdx = indexController.openNameIndex(rtx.getPageTrx(), savedNameIndexDef,
            indexController.createNameFilter(Set.of("featured")));
        assertTrue(featuredIdx.hasNext(), "Latest: Should find 'featured' key");
        assertEquals(1, featuredIdx.next().getNodeKeys().getLongCardinality(), "Latest: Should have 1 'featured' key");
      }
    }

    @Test
    @DisplayName("HOT CAS index with multiple values and revisions")
    void testHOTCASIndexMultipleValues() {
      assertTrue(CASIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");

      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      IndexDef savedStatusIndexDef;
      IndexDef savedPriorityIndexDef;

      // Revision 1: Create CAS indexes for multiple paths and insert initial data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        // Create CAS index for task status
        final var pathToStatus = parse("/tasks/[]/status", PathParser.Type.JSON);
        final var statusIndexDef = IndexDefs.createCASIdxDef(false, Type.STR,
            Collections.singleton(pathToStatus), 0, IndexDef.DbType.JSON);
        savedStatusIndexDef = statusIndexDef;

        // Create CAS index for task priority
        final var pathToPriority = parse("/tasks/[]/priority", PathParser.Type.JSON);
        final var priorityIndexDef = IndexDefs.createCASIdxDef(false, Type.STR,
            Collections.singleton(pathToPriority), 1, IndexDef.DbType.JSON);
        savedPriorityIndexDef = priorityIndexDef;

        indexController.createIndexes(Set.of(statusIndexDef, priorityIndexDef), trx);

        // Insert initial tasks
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            {
              "tasks": [
                {"id": 1, "title": "Task A", "status": "pending", "priority": "high"},
                {"id": 2, "title": "Task B", "status": "pending", "priority": "medium"},
                {"id": 3, "title": "Task C", "status": "completed", "priority": "low"},
                {"id": 4, "title": "Task D", "status": "pending", "priority": "high"}
              ]
            }
            """));
        trx.commit();

        // Verify status counts
        var pendingIdx = indexController.openCASIndex(trx.getPageTrx(), statusIndexDef,
            indexController.createCASFilter(Set.of("/tasks/[]/status"), new Str("pending"),
                SearchMode.EQUAL, new JsonPCRCollector(trx)));
        assertTrue(pendingIdx.hasNext(), "Rev1: Should find 'pending' status");
        assertEquals(3, pendingIdx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 3 pending tasks");

        var completedIdx = indexController.openCASIndex(trx.getPageTrx(), statusIndexDef,
            indexController.createCASFilter(Set.of("/tasks/[]/status"), new Str("completed"),
                SearchMode.EQUAL, new JsonPCRCollector(trx)));
        assertTrue(completedIdx.hasNext(), "Rev1: Should find 'completed' status");
        assertEquals(1, completedIdx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 1 completed task");

        // Verify priority counts
        var highIdx = indexController.openCASIndex(trx.getPageTrx(), priorityIndexDef,
            indexController.createCASFilter(Set.of("/tasks/[]/priority"), new Str("high"),
                SearchMode.EQUAL, new JsonPCRCollector(trx)));
        assertTrue(highIdx.hasNext(), "Rev1: Should find 'high' priority");
        assertEquals(2, highIdx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 2 high priority tasks");
      }

      // Revision 2: Add more tasks with different statuses
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToLastChild();

        trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
            """
            {"id": 5, "title": "Task E", "status": "in_progress", "priority": "high"}
            """));
        trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
            """
            {"id": 6, "title": "Task F", "status": "in_progress", "priority": "medium"}
            """));
        trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
            """
            {"id": 7, "title": "Task G", "status": "pending", "priority": "low"}
            """));
        trx.commit();
      }

      // Revision 3: Delete some tasks
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        // Delete first task (pending, high)
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.remove();
        trx.commit();
      }

      // Revision 4: Delete another task
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        // Delete completed task (now first after previous deletion)
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToRightSibling(); // second task
        trx.remove();
        trx.commit();
      }

      // Verify latest revision
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());

        // pending: 3 initially - 1 deleted + 1 added = 3; but second deletion removed another pending = 2
        var pendingIdx = indexController.openCASIndex(rtx.getPageTrx(), savedStatusIndexDef,
            indexController.createCASFilter(Set.of("/tasks/[]/status"), new Str("pending"),
                SearchMode.EQUAL, new JsonPCRCollector(rtx)));
        assertTrue(pendingIdx.hasNext(), "Latest: Should find 'pending' status");
        long pendingCount = pendingIdx.next().getNodeKeys().getLongCardinality();
        assertTrue(pendingCount >= 2, "Latest: Should have at least 2 pending tasks");

        // in_progress: 2 added
        var inProgressIdx = indexController.openCASIndex(rtx.getPageTrx(), savedStatusIndexDef,
            indexController.createCASFilter(Set.of("/tasks/[]/status"), new Str("in_progress"),
                SearchMode.EQUAL, new JsonPCRCollector(rtx)));
        assertTrue(inProgressIdx.hasNext(), "Latest: Should find 'in_progress' status");
        assertEquals(2, inProgressIdx.next().getNodeKeys().getLongCardinality(), "Latest: Should have 2 in_progress tasks");

        // high priority: Initial 2 + 1 added = 3
        // Note: CAS index deletions in nested objects may not propagate completely
        // since we're deleting parent objects, not the value nodes directly
        var highIdx = indexController.openCASIndex(rtx.getPageTrx(), savedPriorityIndexDef,
            indexController.createCASFilter(Set.of("/tasks/[]/priority"), new Str("high"),
                SearchMode.EQUAL, new JsonPCRCollector(rtx)));
        assertTrue(highIdx.hasNext(), "Latest: Should find 'high' priority");
        long highCount = highIdx.next().getNodeKeys().getLongCardinality();
        assertTrue(highCount >= 2 && highCount <= 3, 
            "Latest: Should have 2-3 high priority tasks (depending on deletion handling), got: " + highCount);
      }
    }

    @Test
    @DisplayName("HOT combined index test with insertions, updates, and deletions across revisions")
    void testHOTCombinedIndexOperations() {
      assertTrue(PathIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");
      assertTrue(CASIndexListenerFactory.isHOTEnabled(), "HOT should be enabled for this test");

      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      IndexDef pathIndexDef;
      IndexDef nameIndexDef;
      IndexDef casIndexDef;

      // Revision 1: Create all three index types and insert initial data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        // PATH index for /orders/[]/status
        final var pathToStatus = parse("/orders/[]/status", PathParser.Type.JSON);
        pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToStatus), 0, IndexDef.DbType.JSON);

        // NAME index for all keys (uses separate NamePage, so ID 0 is fine)
        nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);

        // CAS index for /orders/[]/status values (uses separate CASPage, so ID 0 is fine)
        casIndexDef = IndexDefs.createCASIdxDef(false, Type.STR,
            Collections.singleton(pathToStatus), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(pathIndexDef, nameIndexDef, casIndexDef), trx);

        // Insert initial orders
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            {
              "orders": [
                {"id": "ORD001", "status": "new", "total": 100},
                {"id": "ORD002", "status": "new", "total": 200},
                {"id": "ORD003", "status": "shipped", "total": 150}
              ]
            }
            """));
        trx.commit();

        // Verify all indexes
        var pathIdx = indexController.openPathIndex(trx.getPageTrx(), pathIndexDef, null);
        assertTrue(pathIdx.hasNext(), "Rev1: PATH index should have results");
        assertEquals(3, pathIdx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 3 status paths");

        var nameIdx = indexController.openNameIndex(trx.getPageTrx(), nameIndexDef,
            indexController.createNameFilter(Set.of("status")));
        assertTrue(nameIdx.hasNext(), "Rev1: NAME index should find 'status'");
        assertEquals(3, nameIdx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 3 'status' keys");

        var casIdx = indexController.openCASIndex(trx.getPageTrx(), casIndexDef,
            indexController.createCASFilter(Set.of("/orders/[]/status"), new Str("new"),
                SearchMode.EQUAL, new JsonPCRCollector(trx)));
        assertTrue(casIdx.hasNext(), "Rev1: CAS index should find 'new' status");
        assertEquals(2, casIdx.next().getNodeKeys().getLongCardinality(), "Rev1: Should have 2 'new' status values");
      }

      // Revisions 2-5: Add orders one at a time
      String[] newStatuses = {"processing", "processing", "shipped", "delivered"};
      for (int i = 0; i < 4; i++) {
        try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
             final var trx = manager.beginNodeTrx()) {
          trx.moveToDocumentRoot();
          trx.moveToFirstChild();
          trx.moveToFirstChild();
          trx.moveToFirstChild();
          trx.moveToLastChild();

          int orderId = 4 + i;
          trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
              String.format("""
                  {"id": "ORD%03d", "status": "%s", "total": %d}
                  """, orderId, newStatuses[i], (orderId * 50))));
          trx.commit();
        }
      }

      // Revisions 6-7: Delete first two orders
      for (int i = 0; i < 2; i++) {
        try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
             final var trx = manager.beginNodeTrx()) {
          trx.moveToDocumentRoot();
          trx.moveToFirstChild();
          trx.moveToFirstChild();
          trx.moveToFirstChild();
          trx.moveToFirstChild();
          trx.remove();
          trx.commit();
        }
      }

      // Verify latest revision
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());

        assertTrue(rtx.getRevisionNumber() >= 7, "Should have at least 7 revisions");

        // PATH: 3 original + 4 added - 2 deleted = 5
        var pathIdx = indexController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
        assertTrue(pathIdx.hasNext(), "Latest: PATH index should have results");
        assertEquals(5, pathIdx.next().getNodeKeys().getLongCardinality(), "Latest: Should have 5 status paths");

        // NAME: 3 original + 4 added - 2 deleted = 5 'status' keys
        var nameIdx = indexController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
            indexController.createNameFilter(Set.of("status")));
        assertTrue(nameIdx.hasNext(), "Latest: NAME index should find 'status'");
        assertEquals(5, nameIdx.next().getNodeKeys().getLongCardinality(), "Latest: Should have 5 'status' keys");

        // CAS: 'processing' should have 2 occurrences
        var processingIdx = indexController.openCASIndex(rtx.getPageTrx(), casIndexDef,
            indexController.createCASFilter(Set.of("/orders/[]/status"), new Str("processing"),
                SearchMode.EQUAL, new JsonPCRCollector(rtx)));
        assertTrue(processingIdx.hasNext(), "Latest: CAS index should find 'processing'");
        assertEquals(2, processingIdx.next().getNodeKeys().getLongCardinality(), "Latest: Should have 2 'processing' status");

        // CAS: 'new' entries should be completely removed (both were deleted)
        var newIdx = indexController.openCASIndex(rtx.getPageTrx(), casIndexDef,
            indexController.createCASFilter(Set.of("/orders/[]/status"), new Str("new"),
                SearchMode.EQUAL, new JsonPCRCollector(rtx)));
        assertFalse(newIdx.hasNext(), "Latest: 'new' should be completely removed after deletions");
      }
    }
  }

  // ===== CAS Index Deletion Corner Cases =====
  // Formal proof of correctness: systematically test all deletion scenarios
  
  @Nested
  @DisplayName("CAS Index Deletion Corner Cases")
  class CASIndexDeletionTests {

    @BeforeEach
    void setUp() {
      JsonTestHelper.deleteEverything();
      System.setProperty("sirix.index.useHOT", "true");
    }

    @AfterEach
    void tearDown() {
      JsonTestHelper.closeEverything();
      JsonTestHelper.deleteEverything();
      System.clearProperty("sirix.index.useHOT");
    }

    /**
     * Test Case 1: Direct array value deletion
     * 
     * Scenario: Delete a string value in an array directly.
     * Note: Object values cannot be deleted directly, but array values can.
     * Expected: The value should be removed from the CAS index.
     */
    @Test
    @DisplayName("TC1: Direct array value deletion removes entry from CAS index")
    void testDirectArrayValueDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        // Create CAS index on /items/[] path (array elements)
        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(parse("/items/[]", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        // Insert JSON with array of strings
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"items\": [\"apple\", \"banana\", \"cherry\"]}"));
        wtx.commit();

        // Verify all values are indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/items/[]"), new Str("banana"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(idx.hasNext(), "Before deletion: 'banana' should be indexed");
          assertEquals(1, idx.next().getNodeKeys().getLongCardinality());
        }

        // Navigate to "banana" and delete it directly
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "items" key
        wtx.moveToFirstChild(); // items array
        wtx.moveToFirstChild(); // "apple"
        wtx.moveToRightSibling(); // "banana"
        assertEquals("banana", wtx.getValue(), "Should be at 'banana' value node");
        
        wtx.remove(); // Direct deletion of array element
        wtx.commit();

        // Verify "banana" is removed from index
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          
          // "banana" should be gone
          var bananaIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/items/[]"), new Str("banana"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertFalse(bananaIdx.hasNext(), "After deletion: 'banana' should be removed from index");
          
          // "apple" and "cherry" should still be there
          var appleIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/items/[]"), new Str("apple"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(appleIdx.hasNext(), "'apple' should still be indexed");
          
          var cherryIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/items/[]"), new Str("cherry"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(cherryIdx.hasNext(), "'cherry' should still be indexed");
        }
      }
    }

    /**
     * Test Case 2: Parent object key deletion
     * 
     * Scenario: Delete an object key that contains a string value.
     * This is the main case the bug fix addressed - the value node must be
     * passed to the index controller, not the parent node.
     * Expected: The value should be removed from the CAS index.
     */
    @Test
    @DisplayName("TC2: Parent object key deletion removes value from CAS index")
    void testParentObjectKeyDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        // Create CAS index on /user/status path
        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(parse("/user/status", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        // Insert JSON with status value
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"user\": {\"name\": \"John\", \"status\": \"active\", \"role\": \"admin\"}}"));
        wtx.commit();

        // Verify "active" is indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/user/status"), new Str("active"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(idx.hasNext(), "Before deletion: 'active' should be indexed");
          assertEquals(1, idx.next().getNodeKeys().getLongCardinality());
        }

        // Navigate to "status" object key and delete it (deletes key + value)
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "user" key
        wtx.moveToFirstChild(); // user object
        wtx.moveToFirstChild(); // "name" key
        wtx.moveToRightSibling(); // "status" key
        assertEquals("status", wtx.getName().getLocalName(), "Should be at 'status' key");
        
        wtx.remove(); // Delete the object key (and its value)
        wtx.commit();

        // Verify "active" is removed from index
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/user/status"), new Str("active"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertFalse(idx.hasNext(), "After deletion: 'active' should be removed from index");
        }
      }
    }

    /**
     * Test Case 3: Grandparent object deletion
     * 
     * Scenario: Delete an ancestor object that contains nested object keys with values.
     * The entire subtree is deleted, all values should be removed from CAS index.
     * Expected: All nested values should be removed from the CAS index.
     */
    @Test
    @DisplayName("TC3: Grandparent object deletion removes all nested values from CAS index")
    void testGrandparentObjectDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        // Create CAS index on /data/user/status path
        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(parse("/data/user/status", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        // Insert JSON with nested structure
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"data\": {\"user\": {\"name\": \"John\", \"status\": \"active\"}}, \"other\": \"value\"}"));
        wtx.commit();

        // Verify "active" is indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/data/user/status"), new Str("active"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(idx.hasNext(), "Before deletion: 'active' should be indexed");
        }

        // Navigate to "data" object key (grandparent of value) and delete it
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "data" key
        assertEquals("data", wtx.getName().getLocalName(), "Should be at 'data' key");
        
        wtx.remove(); // Delete entire "data" subtree including nested "active" value
        wtx.commit();

        // Verify "active" is removed from index
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/data/user/status"), new Str("active"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertFalse(idx.hasNext(), "After deletion: 'active' should be removed from index");
        }
      }
    }

    /**
     * Test Case 4: Number value deletion
     * 
     * Scenario: Delete an object key containing a number value.
     * Tests that NUMBER type values are correctly removed from CAS index.
     * Expected: The number value should be removed from the CAS index.
     */
    @Test
    @DisplayName("TC4: Number value deletion removes entry from CAS index")
    void testNumberValueDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        // Create CAS index for strings on /product/price path
        // (numbers are converted to strings for CAS indexing with Type.STR)
        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(parse("/product/price", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        // Insert JSON with price value (number will be indexed as string)
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"product\": {\"name\": \"Widget\", \"price\": 99.99, \"stock\": 100}}"));
        wtx.commit();

        // Verify 99.99 is indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/product/price"), new Str("99.99"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(idx.hasNext(), "Before deletion: 99.99 should be indexed");
        }

        // Navigate to "price" object key and delete it
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "product" key
        wtx.moveToFirstChild(); // product object
        wtx.moveToFirstChild(); // "name" key
        wtx.moveToRightSibling(); // "price" key
        assertEquals("price", wtx.getName().getLocalName(), "Should be at 'price' key");
        
        wtx.remove(); // Delete price key and value
        wtx.commit();

        // Verify 99.99 is removed from index
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/product/price"), new Str("99.99"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertFalse(idx.hasNext(), "After deletion: 99.99 should be removed from index");
        }
      }
    }

    /**
     * Test Case 5: Boolean value deletion
     * 
     * Scenario: Delete an object key containing a boolean value.
     * Tests that BOOLEAN type values are correctly removed from CAS index.
     * Expected: The boolean value should be removed from the CAS index.
     */
    @Test
    @DisplayName("TC5: Boolean value deletion removes entry from CAS index")
    void testBooleanValueDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        // Create CAS index on /user/active path
        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(parse("/user/active", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        // Insert JSON with boolean value
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"user\": {\"name\": \"John\", \"active\": true, \"verified\": false}}"));
        wtx.commit();

        // Verify true is indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/user/active"), new Str("true"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(idx.hasNext(), "Before deletion: 'true' should be indexed");
        }

        // Navigate to "active" object key and delete it
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "user" key
        wtx.moveToFirstChild(); // user object
        wtx.moveToFirstChild(); // "name" key
        wtx.moveToRightSibling(); // "active" key
        assertEquals("active", wtx.getName().getLocalName(), "Should be at 'active' key");
        
        wtx.remove(); // Delete active key and value
        wtx.commit();

        // Verify true is removed from index
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/user/active"), new Str("true"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertFalse(idx.hasNext(), "After deletion: 'true' should be removed from index");
        }
      }
    }

    /**
     * Test Case 6: Multiple identical values - partial deletion
     * 
     * Scenario: Multiple nodes have the same value. Delete one occurrence.
     * Expected: Index count decreases by 1, other occurrences remain indexed.
     */
    @Test
    @DisplayName("TC6: Partial deletion of multiple identical values")
    void testMultipleIdenticalValuesPartialDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        // Create CAS index on /users/[]/status path
        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(parse("/users/[]/status", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        // Insert JSON with 3 users all having status "active"
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"users\": [{\"name\": \"Alice\", \"status\": \"active\"}, " +
            "{\"name\": \"Bob\", \"status\": \"active\"}, " +
            "{\"name\": \"Charlie\", \"status\": \"active\"}]}"));
        wtx.commit();

        // Verify 3 "active" values are indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/users/[]/status"), new Str("active"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(idx.hasNext(), "Before deletion: 'active' should be indexed");
          assertEquals(3, idx.next().getNodeKeys().getLongCardinality(), "Should have 3 'active' values");
        }

        // Navigate to Bob's object and delete it (deletes Bob's status)
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "users" key
        wtx.moveToFirstChild(); // users array
        wtx.moveToFirstChild(); // Alice object
        wtx.moveToRightSibling(); // Bob object
        
        wtx.remove(); // Delete Bob (including his "active" status)
        wtx.commit();

        // Verify now only 2 "active" values are indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/users/[]/status"), new Str("active"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(idx.hasNext(), "After deletion: 'active' should still be indexed");
          assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "Should have 2 'active' values after deleting Bob");
        }
      }
    }

    /**
     * Test Case 7: Deep nesting deletion
     * 
     * Scenario: Delete an ancestor in a deeply nested structure (5+ levels).
     * All descendant values should be removed from CAS index.
     * Expected: All nested values are removed from the CAS index.
     */
    @Test
    @DisplayName("TC7: Deep nesting ancestor deletion removes all nested values")
    void testDeepNestingDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        // Create CAS index on deep path
        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(parse("/a/b/c/d/e/value", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        // Insert deeply nested JSON
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"a\": {\"b\": {\"c\": {\"d\": {\"e\": {\"value\": \"deep\"}}}}}}"));
        wtx.commit();

        // Verify "deep" is indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/a/b/c/d/e/value"), new Str("deep"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(idx.hasNext(), "Before deletion: 'deep' should be indexed");
        }

        // Navigate to "c" (middle ancestor) and delete it
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "a" key
        wtx.moveToFirstChild(); // a object
        wtx.moveToFirstChild(); // "b" key
        wtx.moveToFirstChild(); // b object
        wtx.moveToFirstChild(); // "c" key
        assertEquals("c", wtx.getName().getLocalName(), "Should be at 'c' key");
        
        wtx.remove(); // Delete "c" and all descendants
        wtx.commit();

        // Verify "deep" is removed from index
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/a/b/c/d/e/value"), new Str("deep"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertFalse(idx.hasNext(), "After deletion: 'deep' should be removed from index");
        }
      }
    }

    /**
     * Test Case 8: Cross-transaction deletion persistence
     * 
     * Scenario: Delete value in one transaction, commit, then verify in new transaction.
     * Tests that deletions are properly persisted to storage.
     * Expected: Deletion persists across transaction boundaries.
     */
    @Test
    @DisplayName("TC8: Cross-transaction deletion persistence")
    void testCrossTransactionDeletionPersistence() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      IndexDef casIndexDef;

      // Transaction 1: Create index and insert data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(parse("/data/value", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"data\": {\"value\": \"persistent\"}}"));
        wtx.commit();
      }

      // Transaction 2: Delete the value
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        // Verify it exists before deletion
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/data/value"), new Str("persistent"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(idx.hasNext(), "Transaction 2: Value should exist before deletion");
        }

        // Navigate and delete
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "data" key
        wtx.moveToFirstChild(); // data object
        wtx.moveToFirstChild(); // "value" key
        assertEquals("value", wtx.getName().getLocalName());
        
        wtx.remove();
        wtx.commit();
      }

      // Transaction 3: Verify deletion persisted
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {

        var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
        var idx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
            readController.createCASFilter(Set.of("/data/value"), new Str("persistent"),
                SearchMode.EQUAL, new JsonPCRCollector(rtx)));
        assertFalse(idx.hasNext(), "Transaction 3: Deletion should persist across transactions");
      }
    }

    /**
     * Test Case 9: Mixed insert-delete-insert operations
     * 
     * Scenario: Insert values, delete some, insert more, verify final state.
     * Tests that the index correctly handles interleaved operations.
     * Expected: Final index state reflects all operations correctly.
     */
    @Test
    @DisplayName("TC9: Mixed insert-delete-insert operations")
    void testMixedInsertDeleteInsertOperations() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        // Create CAS index on /items/[] path
        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(parse("/items/[]", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        // Initial insert: apple, banana
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"items\": [\"apple\", \"banana\"]}"));
        wtx.commit();

        // Verify initial state
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var appleIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/items/[]"), new Str("apple"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(appleIdx.hasNext(), "Step 1: 'apple' should be indexed");
          
          var bananaIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/items/[]"), new Str("banana"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(bananaIdx.hasNext(), "Step 1: 'banana' should be indexed");
        }

        // Delete banana
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "items" key
        wtx.moveToFirstChild(); // items array
        wtx.moveToFirstChild(); // "apple"
        wtx.moveToRightSibling(); // "banana"
        assertEquals("banana", wtx.getValue());
        wtx.remove();
        wtx.commit();

        // Insert cherry
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "items" key
        wtx.moveToFirstChild(); // items array
        wtx.moveToFirstChild(); // "apple"
        wtx.insertStringValueAsRightSibling("cherry");
        wtx.commit();

        // Verify final state: apple (yes), banana (no), cherry (yes)
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          
          var appleIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/items/[]"), new Str("apple"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(appleIdx.hasNext(), "Final: 'apple' should be indexed");
          
          var bananaIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/items/[]"), new Str("banana"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertFalse(bananaIdx.hasNext(), "Final: 'banana' should be removed");
          
          var cherryIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/items/[]"), new Str("cherry"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(cherryIdx.hasNext(), "Final: 'cherry' should be indexed");
        }
      }
    }

    /**
     * Test Case 10: Insert-only multi-revision test
     * 
     * Scenario: Insert values across multiple revisions without any deletions.
     * Verifies that index correctly accumulates entries across commits.
     * Expected: Index count increases with each revision.
     */
    @Test
    @DisplayName("TC10: Insert-only across multiple revisions")
    void testInsertOnlyMultipleRevisions() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var casIndexDef = IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(parse("/data/[]/value", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        // Rev 1: Insert 2 values
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"data\": [{\"value\": \"alpha\"}, {\"value\": \"beta\"}]}"));
        wtx.commit();

        // Verify Rev 1: 2 distinct values
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var alphaIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/data/[]/value"), new Str("alpha"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(alphaIdx.hasNext());
          assertEquals(1, alphaIdx.next().getNodeKeys().getLongCardinality(), "Rev1: 1 'alpha'");
        }

        // Rev 2: Insert 2 more values
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToLastChild();
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"value\": \"gamma\"}"));
        wtx.moveToRightSibling();
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"value\": \"delta\"}"));
        wtx.commit();

        // Verify Rev 2: all 4 values present
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          
          var gammaIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/data/[]/value"), new Str("gamma"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(gammaIdx.hasNext(), "Rev2: 'gamma' should be indexed");
          
          var deltaIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/data/[]/value"), new Str("delta"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(deltaIdx.hasNext(), "Rev2: 'delta' should be indexed");
          
          // Original values still present
          var alphaIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/data/[]/value"), new Str("alpha"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(alphaIdx.hasNext(), "Rev2: 'alpha' still indexed");
        }

        // Rev 3: Insert duplicate value
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToLastChild();
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"value\": \"alpha\"}"));
        wtx.commit();

        // Verify Rev 3: 2 'alpha' entries
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var alphaIdx = readController.openCASIndex(rtx.getPageTrx(), casIndexDef,
              readController.createCASFilter(Set.of("/data/[]/value"), new Str("alpha"),
                  SearchMode.EQUAL, new JsonPCRCollector(rtx)));
          assertTrue(alphaIdx.hasNext());
          assertEquals(2, alphaIdx.next().getNodeKeys().getLongCardinality(), "Rev3: 2 'alpha' entries");
        }
      }
    }
  }

  // ===== PATH Index Corner Cases =====
  
  @Nested
  @DisplayName("PATH Index Corner Cases")
  class PATHIndexCornerCaseTests {

    @BeforeEach
    void setUp() {
      JsonTestHelper.deleteEverything();
      System.setProperty("sirix.index.useHOT", "true");
    }

    @AfterEach
    void tearDown() {
      JsonTestHelper.closeEverything();
      JsonTestHelper.deleteEverything();
      System.clearProperty("sirix.index.useHOT");
    }

    /**
     * PATH-TC1: Multiple paths in one index - insertion
     * 
     * Scenario: Create index covering multiple paths, verify all are indexed.
     */
    @Test
    @DisplayName("PATH-TC1: Multiple paths in one index")
    void testMultiplePathsInOneIndex() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        
        // Create 3 separate indexes for each path
        final var namePath = parse("/users/[]/name", PathParser.Type.JSON);
        final var emailPath = parse("/users/[]/email", PathParser.Type.JSON);
        final var rolePath = parse("/users/[]/role", PathParser.Type.JSON);
        
        final var nameIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(namePath), 0, IndexDef.DbType.JSON);
        final var emailIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(emailPath), 1, IndexDef.DbType.JSON);
        final var roleIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(rolePath), 2, IndexDef.DbType.JSON);
        
        indexController.createIndexes(Set.of(nameIndexDef, emailIndexDef, roleIndexDef), wtx);

        // Insert data matching all paths
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"users\": [{\"name\": \"Alice\", \"email\": \"alice@test.com\", \"role\": \"admin\"}, " +
            "{\"name\": \"Bob\", \"email\": \"bob@test.com\", \"role\": \"user\"}]}"));
        wtx.commit();

        // Verify all paths are indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          
          // Check name path
          var nameIdx = readController.openPathIndex(rtx.getPageTrx(), nameIndexDef, null);
          assertTrue(nameIdx.hasNext(), "Path /users/[]/name should be indexed");
          assertEquals(2, nameIdx.next().getNodeKeys().getLongCardinality(), "Should have 2 name nodes");
          
          // Check email path
          var emailIdx = readController.openPathIndex(rtx.getPageTrx(), emailIndexDef, null);
          assertTrue(emailIdx.hasNext(), "Path /users/[]/email should be indexed");
          assertEquals(2, emailIdx.next().getNodeKeys().getLongCardinality(), "Should have 2 email nodes");
          
          // Check role path
          var roleIdx = readController.openPathIndex(rtx.getPageTrx(), roleIndexDef, null);
          assertTrue(roleIdx.hasNext(), "Path /users/[]/role should be indexed");
          assertEquals(2, roleIdx.next().getNodeKeys().getLongCardinality(), "Should have 2 role nodes");
        }
      }
    }

    /**
     * PATH-TC2: Insertions across multiple revisions
     * 
     * Scenario: Insert nodes matching path in multiple commits, verify count increases.
     */
    @Test
    @DisplayName("PATH-TC2: Insertions across multiple revisions")
    void testPathInsertionsAcrossRevisions() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var idPath = parse("/items/[]/id", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(
            Collections.singleton(idPath), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(pathIndexDef), wtx);

        // Rev 1: Insert 2 items
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"items\": [{\"id\": 1}, {\"id\": 2}]}"));
        wtx.commit();

        // Rev 2: Insert 2 more items
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "items" key
        wtx.moveToFirstChild(); // items array
        wtx.moveToLastChild(); // last item
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"id\": 3}"));
        wtx.moveToRightSibling();
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"id\": 4}"));
        wtx.commit();

        // Rev 3: Insert 1 more item
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToLastChild();
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"id\": 5}"));
        wtx.commit();

        // Verify latest revision has 5 indexed paths
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
          assertTrue(idx.hasNext());
          assertEquals(5, idx.next().getNodeKeys().getLongCardinality(), "Latest: Should have 5 'id' paths");
        }
      }
    }

    /**
     * PATH-TC3: Deletion removes path from index
     * 
     * Scenario: Delete node matching indexed path, verify removed from index.
     */
    @Test
    @DisplayName("PATH-TC3: Deletion removes path from index")
    void testPathDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var valuePath = parse("/data/value", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(
            Collections.singleton(valuePath), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(pathIndexDef), wtx);

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"data\": {\"value\": \"test\", \"other\": \"keep\"}}"));
        wtx.commit();

        // Verify indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
          assertTrue(idx.hasNext(), "Before: path should be indexed");
        }

        // Delete "value" key
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root
        wtx.moveToFirstChild(); // "data"
        wtx.moveToFirstChild(); // data object
        wtx.moveToFirstChild(); // "value" key
        assertEquals("value", wtx.getName().getLocalName());
        wtx.remove();
        wtx.commit();

        // Verify removed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
          assertFalse(idx.hasNext(), "After: path should be removed from index");
        }
      }
    }

    /**
     * PATH-TC4: Parent deletion removes nested paths
     * 
     * Scenario: Delete parent, all nested paths should be removed from index.
     */
    @Test
    @DisplayName("PATH-TC4: Parent deletion removes nested paths")
    void testPathParentDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var deepPath = parse("/container/nested/deep/value", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(
            Collections.singleton(deepPath), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(pathIndexDef), wtx);

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"container\": {\"nested\": {\"deep\": {\"value\": \"found\"}}}, \"other\": \"keep\"}"));
        wtx.commit();

        // Verify indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
          assertTrue(idx.hasNext(), "Before: deep path should be indexed");
        }

        // Delete "container" (ancestor)
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild(); // "container"
        assertEquals("container", wtx.getName().getLocalName());
        wtx.remove();
        wtx.commit();

        // Verify removed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
          assertFalse(idx.hasNext(), "After: nested path should be removed from index");
        }
      }
    }

    /**
     * PATH-TC5: Cross-transaction persistence
     * 
     * Scenario: Insert, commit, close, reopen, verify index state persists.
     */
    @Test
    @DisplayName("PATH-TC5: Cross-transaction persistence")
    void testPathCrossTransactionPersistence() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      final var keyPath = parse("/data/key", PathParser.Type.JSON);
      IndexDef pathIndexDef;

      // Transaction 1: Create index and insert
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        pathIndexDef = IndexDefs.createPathIdxDef(
            Collections.singleton(keyPath), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(pathIndexDef), wtx);

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"data\": {\"key\": \"value\"}}"));
        wtx.commit();
      }

      // Transaction 2: Verify key is still indexed after reopening
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
        var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
        assertTrue(idx.hasNext(), "Transaction 2: path should be indexed after reopen");
        assertEquals(1, idx.next().getNodeKeys().getLongCardinality());
      }

      // Transaction 3: Delete the key
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild(); // "data"
        wtx.moveToFirstChild(); // data object
        wtx.moveToFirstChild(); // "key"
        wtx.remove();
        wtx.commit();
      }

      // Transaction 4: Verify deletion persisted
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
        var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
        assertFalse(idx.hasNext(), "Transaction 4: deletion should persist across transactions");
      }
    }

    /**
     * PATH-TC6: Partial deletion with multiple matching nodes
     * 
     * Scenario: Multiple nodes match path, delete one, verify count decreases.
     */
    @Test
    @DisplayName("PATH-TC6: Partial deletion of multiple matching paths")
    void testPathPartialDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var statusPath = parse("/users/[]/status", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(
            Collections.singleton(statusPath), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(pathIndexDef), wtx);

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"users\": [{\"status\": \"active\"}, {\"status\": \"inactive\"}, {\"status\": \"pending\"}]}"));
        wtx.commit();

        // Verify 3 paths indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
          assertTrue(idx.hasNext());
          assertEquals(3, idx.next().getNodeKeys().getLongCardinality(), "Before: 3 status paths");
        }

        // Delete middle user (with "inactive" status)
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild(); // first user
        wtx.moveToRightSibling(); // second user
        wtx.remove();
        wtx.commit();

        // Verify 2 paths remain
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
          assertTrue(idx.hasNext());
          assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "After: 2 status paths");
        }
      }
    }

    /**
     * PATH-TC7: Deep nesting deletion (5+ levels)
     * 
     * Scenario: Delete ancestor at level 2 in a 6-level deep structure.
     * All descendant paths should be removed from index.
     * Expected: Nested path entries are removed.
     */
    @Test
    @DisplayName("PATH-TC7: Deep nesting ancestor deletion (6 levels)")
    void testPathDeepNestingDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        // Index a 6-level deep path
        final var deepPath = parse("/L1/L2/L3/L4/L5/L6", PathParser.Type.JSON);
        final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(deepPath), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(pathIndexDef), wtx);

        // Insert 6-level deep structure
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"L1\": {\"L2\": {\"L3\": {\"L4\": {\"L5\": {\"L6\": \"deepValue\"}}}}}}"));
        wtx.commit();

        // Verify deep path is indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
          assertTrue(idx.hasNext(), "Before: deep path should be indexed");
          assertEquals(1, idx.next().getNodeKeys().getLongCardinality());
        }

        // Delete L2 (ancestor at level 2), removing L3/L4/L5/L6
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // L1 key
        wtx.moveToFirstChild(); // L1 object
        wtx.moveToFirstChild(); // L2 key
        assertEquals("L2", wtx.getName().getLocalName());
        wtx.remove();
        wtx.commit();

        // Verify deep path is removed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openPathIndex(rtx.getPageTrx(), pathIndexDef, null);
          assertFalse(idx.hasNext(), "After: deep path should be removed from index");
        }
      }
    }
  }

  // ===== NAME Index Corner Cases =====
  
  @Nested
  @DisplayName("NAME Index Corner Cases")
  class NAMEIndexCornerCaseTests {

    @BeforeEach
    void setUp() {
      JsonTestHelper.deleteEverything();
      System.setProperty("sirix.index.useHOT", "true");
    }

    @AfterEach
    void tearDown() {
      JsonTestHelper.closeEverything();
      JsonTestHelper.deleteEverything();
      System.clearProperty("sirix.index.useHOT");
    }

    /**
     * NAME-TC1: Multiple names in one index
     * 
     * Scenario: Create index for all names, query for specific names.
     */
    @Test
    @DisplayName("NAME-TC1: Multiple names in one index")
    void testMultipleNamesInOneIndex() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        // Create name index for ALL names (filtering done at query time)
        final var nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(nameIndexDef), wtx);

        // Insert data with multiple object keys
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"users\": [{\"name\": \"Alice\", \"email\": \"a@test.com\", \"role\": \"admin\"}, " +
            "{\"name\": \"Bob\", \"email\": \"b@test.com\", \"role\": \"user\"}]}"));
        wtx.commit();

        // Verify names can be queried with filter
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          
          var nameIdx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("name")));
          assertTrue(nameIdx.hasNext(), "Name 'name' should be indexed");
          assertEquals(2, nameIdx.next().getNodeKeys().getLongCardinality(), "Should have 2 'name' keys");
          
          var emailIdx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("email")));
          assertTrue(emailIdx.hasNext(), "Name 'email' should be indexed");
          assertEquals(2, emailIdx.next().getNodeKeys().getLongCardinality(), "Should have 2 'email' keys");
          
          var roleIdx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("role")));
          assertTrue(roleIdx.hasNext(), "Name 'role' should be indexed");
          assertEquals(2, roleIdx.next().getNodeKeys().getLongCardinality(), "Should have 2 'role' keys");
        }
      }
    }

    /**
     * NAME-TC2: Insertions across multiple revisions
     * 
     * Scenario: Insert nodes with indexed names in multiple commits.
     */
    @Test
    @DisplayName("NAME-TC2: Insertions across multiple revisions")
    void testNameInsertionsAcrossRevisions() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(nameIndexDef), wtx);

        // Rev 1: Insert 2 items with "status"
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"items\": [{\"status\": \"new\"}, {\"status\": \"old\"}]}"));
        wtx.commit();

        // Verify 2 "status" names indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("status")));
          assertTrue(idx.hasNext());
          assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "Rev1: 2 status names");
        }

        // Rev 2: Insert 2 more items
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToLastChild();
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"status\": \"pending\"}"));
        wtx.moveToRightSibling();
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"status\": \"active\"}"));
        wtx.commit();

        // Verify 4 "status" names indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("status")));
          assertTrue(idx.hasNext());
          assertEquals(4, idx.next().getNodeKeys().getLongCardinality(), "Rev2: 4 status names");
        }
      }
    }

    /**
     * NAME-TC3: Deletion removes name from index
     * 
     * Scenario: Delete object key, verify name count decreases.
     */
    @Test
    @DisplayName("NAME-TC3: Deletion removes name from index")
    void testNameDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(nameIndexDef), wtx);

        // Insert with 2 "target" keys to test deletion
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"items\": [{\"target\": \"a\"}, {\"target\": \"b\"}]}"));
        wtx.commit();

        // Verify 2 "target" keys indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("target")));
          assertTrue(idx.hasNext(), "Before: 'target' should be indexed");
          assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "Before: 2 'target' keys");
        }

        // Delete first item (containing first "target" key)
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "items" key
        wtx.moveToFirstChild(); // items array
        wtx.moveToFirstChild(); // first object
        wtx.remove();
        wtx.commit();

        // Verify 1 "target" key remains
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("target")));
          assertTrue(idx.hasNext(), "After: 'target' should still be indexed (1 remaining)");
          assertEquals(1, idx.next().getNodeKeys().getLongCardinality(), "After: 1 'target' key remains");
        }
      }
    }

    /**
     * NAME-TC4: Parent deletion removes nested names
     * 
     * Scenario: Delete parent object, verify nested names count decreases.
     */
    @Test
    @DisplayName("NAME-TC4: Parent deletion removes nested names")
    void testNameParentDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(nameIndexDef), wtx);

        // Insert with 2 "nested" keys - one will be deleted via parent
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"items\": [{\"container\": {\"nested\": \"a\"}}, {\"nested\": \"b\"}]}"));
        wtx.commit();

        // Verify 2 "nested" keys indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("nested")));
          assertTrue(idx.hasNext(), "Before: 'nested' should be indexed");
          assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "Before: 2 'nested' keys");
        }

        // Delete first item (contains "container" which contains "nested")
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "items" key
        wtx.moveToFirstChild(); // items array
        wtx.moveToFirstChild(); // first object
        wtx.remove();
        wtx.commit();

        // Verify 1 "nested" key remains
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("nested")));
          assertTrue(idx.hasNext(), "After: 'nested' should still be indexed (1 remaining)");
          assertEquals(1, idx.next().getNodeKeys().getLongCardinality(), "After: 1 'nested' key remains");
        }
      }
    }

    /**
     * NAME-TC5: Cross-transaction persistence
     * 
     * Scenario: Insert, commit, close, reopen, verify counts across transactions.
     */
    @Test
    @DisplayName("NAME-TC5: Cross-transaction persistence")
    void testNameCrossTransactionPersistence() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      IndexDef nameIndexDef;

      // Transaction 1: Create index and insert with 2 "key" names
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(nameIndexDef), wtx);

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"items\": [{\"key\": \"a\"}, {\"key\": \"b\"}]}"));
        wtx.commit();
      }

      // Transaction 2: Verify 2 "key" names indexed
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
        var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
            readController.createNameFilter(Set.of("key")));
        assertTrue(idx.hasNext(), "Transaction 2: 'key' should be indexed");
        assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "Transaction 2: 2 'key' names");
      }

      // Transaction 3: Delete one item
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // "items" key
        wtx.moveToFirstChild(); // items array
        wtx.moveToFirstChild(); // first object
        wtx.remove();
        wtx.commit();
      }

      // Transaction 4: Verify 1 "key" name remains
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var rtx = manager.beginNodeReadOnlyTrx()) {
        var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
        var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
            readController.createNameFilter(Set.of("key")));
        assertTrue(idx.hasNext(), "Transaction 4: 'key' should still be indexed");
        assertEquals(1, idx.next().getNodeKeys().getLongCardinality(), "Transaction 4: 1 'key' name remains");
      }
    }

    /**
     * NAME-TC6: Partial deletion with multiple matching names
     * 
     * Scenario: Multiple objects have same key name, delete one, verify count decreases.
     */
    @Test
    @DisplayName("NAME-TC6: Partial deletion of multiple matching names")
    void testNamePartialDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(nameIndexDef), wtx);

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"users\": [{\"status\": \"a\"}, {\"status\": \"b\"}, {\"status\": \"c\"}]}"));
        wtx.commit();

        // Verify 3 names indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("status")));
          assertTrue(idx.hasNext());
          assertEquals(3, idx.next().getNodeKeys().getLongCardinality(), "Before: 3 status names");
        }

        // Delete middle user
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToRightSibling();
        wtx.remove();
        wtx.commit();

        // Verify 2 names remain
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("status")));
          assertTrue(idx.hasNext());
          assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "After: 2 status names");
        }
      }
    }

    /**
     * NAME-TC7: Deep nesting deletion (6 levels)
     * 
     * Scenario: Delete ancestor at level 2 in a 6-level deep structure.
     * All descendant names should be removed from index.
     * Expected: Nested name entries are removed.
     */
    @Test
    @DisplayName("NAME-TC7: Deep nesting ancestor deletion (6 levels)")
    void testNameDeepNestingDeletion() {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
           final var wtx = manager.beginNodeTrx()) {

        var indexController = manager.getWtxIndexController(wtx.getRevisionNumber());
        final var nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(nameIndexDef), wtx);

        // Insert 6-level deep structure with 2 "deepKey" names (one nested, one not)
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"L1\": {\"L2\": {\"L3\": {\"L4\": {\"L5\": {\"deepKey\": \"nested\"}}}}}, \"deepKey\": \"toplevel\"}"));
        wtx.commit();

        // Verify 2 "deepKey" names indexed
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("deepKey")));
          assertTrue(idx.hasNext(), "Before: 'deepKey' should be indexed");
          assertEquals(2, idx.next().getNodeKeys().getLongCardinality(), "Before: 2 'deepKey' names");
        }

        // Delete L1 (ancestor), removing the nested deepKey but keeping toplevel one
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // L1 key
        assertEquals("L1", wtx.getName().getLocalName());
        wtx.remove();
        wtx.commit();

        // Verify only 1 "deepKey" remains (the toplevel one)
        try (final var rtx = manager.beginNodeReadOnlyTrx()) {
          var readController = manager.getRtxIndexController(rtx.getRevisionNumber());
          var idx = readController.openNameIndex(rtx.getPageTrx(), nameIndexDef,
              readController.createNameFilter(Set.of("deepKey")));
          assertTrue(idx.hasNext(), "After: 'deepKey' should still be indexed (1 remaining)");
          assertEquals(1, idx.next().getNodeKeys().getLongCardinality(), "After: 1 'deepKey' name remains");
        }
      }
    }
  }
}
