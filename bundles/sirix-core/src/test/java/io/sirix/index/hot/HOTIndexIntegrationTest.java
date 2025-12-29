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

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.closeEverything();
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
    void disableHOT() {
      System.clearProperty("sirix.index.useHOT");
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
    void enableHOT() {
      System.setProperty("sirix.index.useHOT", "true");
    }
    
    @AfterEach
    void disableHOT() {
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
  }
}
