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
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for HOT (Height Optimized Trie) index listener infrastructure.
 *
 * <p>These tests verify that the index listeners can be configured for both 
 * RBTree (default) and HOT backends, and that the HOT configuration property works.</p>
 * 
 * <p>Note: These tests run with RBTree backend by default. HOT backend tests will be
 * enabled once the HOT trie navigation is fully implemented in the storage engine.</p>
 */
class HOTIndexIntegrationTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");
  
  private static String originalHOTSetting;

  @BeforeAll
  static void saveHOTSetting() {
    // Save original setting
    originalHOTSetting = System.getProperty("sirix.index.useHOT");
    // Disable HOT for now - the index query path (PathIndex.openIndex) still uses RBTreeReader
    // which expects KeyValueLeafPage. Full HOT activation requires implementing HOT-based
    // index query reading in PathIndex, CASIndex, and NameIndex.
    System.clearProperty("sirix.index.useHOT");
  }
  
  @AfterAll
  static void restoreHOTSetting() {
    // Restore original setting
    if (originalHOTSetting != null) {
      System.setProperty("sirix.index.useHOT", originalHOTSetting);
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
  
  // ===== RBTree-based Integration Tests (verify listener infrastructure works) =====

  @Test
  @DisplayName("PATH index creation and query works with listener infrastructure")
  void testHOTPathIndexCreationAndQuery() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx()) {
      var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

      // Create PATH index for /features/[]/type
      final var pathToFeatureType = parse("/features/[]/type", PathParser.Type.JSON);
      final var idxDefOfFeatureType =
          IndexDefs.createPathIdxDef(Collections.singleton(pathToFeatureType), 0, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(idxDefOfFeatureType), trx);

      // Shred JSON
      final var shredder = new JsonShredder.Builder(trx,
          JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      // Query the index
      final var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
      final var pathNodeKeys = trx.getPathSummary().getPCRsForPath(pathToFeatureType);

      assertEquals(1, pathNodeKeys.size(), "Should find one path node key");

      // Open path index
      final var index = indexController.openPathIndex(trx.getPageTrx(), indexDef, null);

      assertTrue(index.hasNext(), "Index should have results");

      // Verify we can iterate through results
      var count = 0;
      while (index.hasNext()) {
        var nodeReferences = index.next();
        assertTrue(nodeReferences.getNodeKeys().getLongCardinality() > 0, "Should have node keys");
        count++;
      }
      assertTrue(count >= 1, "Should have at least one result");
    }
  }

  @Test
  @DisplayName("NAME index creation and query works with listener infrastructure")
  void testHOTNameIndexCreationAndQuery() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var trx = session.beginNodeTrx()) {
      var indexController = session.getWtxIndexController(trx.getRevisionNumber());

      // Create NAME index for all object keys
      final var allObjectKeyNames = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      indexController.createIndexes(Set.of(allObjectKeyNames), trx);

      // Shred JSON
      final var shredder = new JsonShredder.Builder(trx,
          JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      // Query for specific name
      final var nameIndex = indexController.openNameIndex(trx.getPageTrx(),
          allObjectKeyNames,
          indexController.createNameFilter(Set.of("type")));

      assertTrue(nameIndex.hasNext(), "Should find 'type' keys");

      final var typeReferences = nameIndex.next();
      assertTrue(typeReferences.getNodeKeys().getLongCardinality() > 0, 
          "Should have references to 'type' nodes");
    }
  }

  @Test
  @DisplayName("CAS index creation and query works with listener infrastructure")
  void testHOTCASIndexCreationAndQuery() {
    final var jsonPath = JSON.resolve("abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var trx = manager.beginNodeTrx()) {
      var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

      // Create CAS index for /features/[]/type with string values
      final var pathToFeatureType = parse("/features/[]/type", PathParser.Type.JSON);
      final var idxDefOfFeatureType =
          IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToFeatureType), 0, IndexDef.DbType.JSON);

      indexController.createIndexes(Set.of(idxDefOfFeatureType), trx);

      // Shred JSON
      final var shredder = new JsonShredder.Builder(trx,
          JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();

      // Query the CAS index for "Feature" value
      final var casIndex = indexController.openCASIndex(trx.getPageTrx(),
          idxDefOfFeatureType,
          indexController.createCASFilter(
              Set.of("/features/[]/type"),
              new Str("Feature"),
              SearchMode.EQUAL,
              new JsonPCRCollector(trx)));

      assertTrue(casIndex.hasNext(), "CAS index should find 'Feature' values");

      final var nodeReferences = casIndex.next();
      assertTrue(nodeReferences.getNodeKeys().getLongCardinality() > 0, 
          "Should reference multiple nodes");

      // Verify we can navigate to the referenced nodes
      final var iter = nodeReferences.getNodeKeys().getLongIterator();
      while (iter.hasNext()) {
        long nodeKey = iter.next();
        trx.moveTo(nodeKey);
        assertEquals("Feature", trx.getValue());
      }
    }
  }

  @Test
  @DisplayName("PATH index returns correct number of results (53 type nodes)")
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
  @DisplayName("CAS index finds all 53 'Feature' values")
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
