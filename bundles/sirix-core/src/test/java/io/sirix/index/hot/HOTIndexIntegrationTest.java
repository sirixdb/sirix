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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for HOT (Height Optimized Trie) index listener infrastructure.
 *
 * <p>These tests verify that the index listeners can be configured for both 
 * RBTree (default) and HOT backends, and that the HOT configuration property works.</p>
 * 
 * <p>Note: These tests run with RBTree backend by default. HOT backend tests will be
 * enabled once the HOT trie navigation is fully implemented in the storage engine.</p>
 */
public final class HOTIndexIntegrationTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");
  
  private static String originalHOTSetting;

  @BeforeClass
  public static void saveHOTSetting() {
    // Save original setting
    originalHOTSetting = System.getProperty("sirix.index.useHOT");
    // Disable HOT for now (RBTree tests) until HOT storage engine integration is complete
    System.clearProperty("sirix.index.useHOT");
  }
  
  @AfterClass
  public static void restoreHOTSetting() {
    // Restore original setting
    if (originalHOTSetting != null) {
      System.setProperty("sirix.index.useHOT", originalHOTSetting);
    }
  }

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }
  
  // ===== Configuration Tests =====
  
  @Test
  public void testHOTConfigurationProperty() {
    // Test that HOT can be enabled/disabled via system property
    assertFalse("HOT should be disabled by default", PathIndexListenerFactory.isHOTEnabled());
    assertFalse("HOT should be disabled by default", CASIndexListenerFactory.isHOTEnabled());
    assertFalse("HOT should be disabled by default", NameIndexListenerFactory.isHOTEnabled());
    
    // Enable HOT
    System.setProperty("sirix.index.useHOT", "true");
    assertTrue("HOT should be enabled", PathIndexListenerFactory.isHOTEnabled());
    assertTrue("HOT should be enabled", CASIndexListenerFactory.isHOTEnabled());
    assertTrue("HOT should be enabled", NameIndexListenerFactory.isHOTEnabled());
    
    // Disable HOT
    System.clearProperty("sirix.index.useHOT");
    assertFalse("HOT should be disabled", PathIndexListenerFactory.isHOTEnabled());
  }
  
  // ===== RBTree-based Integration Tests (verify listener infrastructure works) =====

  @Test
  public void testHOTPathIndexCreationAndQuery() {
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

      assertEquals("Should find one path node key", 1, pathNodeKeys.size());

      // Open path index
      final var index = indexController.openPathIndex(trx.getPageTrx(), indexDef, null);

      assertTrue("Index should have results", index.hasNext());

      // Verify we can iterate through results
      var count = 0;
      while (index.hasNext()) {
        var nodeReferences = index.next();
        assertTrue("Should have node keys", nodeReferences.getNodeKeys().getLongCardinality() > 0);
        count++;
      }
      assertTrue("Should have at least one result", count >= 1);
    }
  }

  @Test
  public void testHOTNameIndexCreationAndQuery() {
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

      assertTrue("Should find 'type' keys", nameIndex.hasNext());

      final var typeReferences = nameIndex.next();
      assertTrue("Should have references to 'type' nodes", 
          typeReferences.getNodeKeys().getLongCardinality() > 0);
    }
  }

  @Test
  public void testHOTCASIndexCreationAndQuery() {
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

      assertTrue("CAS index should find 'Feature' values", casIndex.hasNext());

      final var nodeReferences = casIndex.next();
      assertTrue("Should reference multiple nodes", 
          nodeReferences.getNodeKeys().getLongCardinality() > 0);

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
  public void testPathIndexWithRBTreeBackend() {
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

      assertTrue("Index should have results", index.hasNext());
      var refs = index.next();
      assertEquals("Should find 53 'type' nodes", 53, refs.getNodeKeys().getLongCardinality());
    }
  }

  @Test
  public void testCASIndexWithRBTreeBackend() {
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

      assertTrue("CAS query should find results", casIndex.hasNext());

      var refs = casIndex.next();
      assertEquals("Should find 53 'Feature' values", 53, refs.getNodeKeys().getLongCardinality());
    }
  }
}

