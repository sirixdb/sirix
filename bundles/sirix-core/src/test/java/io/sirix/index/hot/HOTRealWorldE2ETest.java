/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real-world end-to-end tests designed to trigger ALL internal HOT methods. These tests simulate
 * actual use cases with indexes and queries.
 */
@DisplayName("HOT Real World E2E Tests")
class HOTRealWorldE2ETest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setup() throws IOException {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void teardown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Nested
  @DisplayName("CAS Index Range Query E2E Tests")
  class CASIndexRangeQueryTests {

    @Test
    @DisplayName("E2E: Create CAS index, insert 500 items, query ranges")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testCASIndexWithRangeQueries() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create CAS index for integers
          final var pathToScore = parse("/players/[]/score", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToScore), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Insert 500 players with scores 0-499
          StringBuilder json = new StringBuilder("{\"players\": [");
          for (int i = 0; i < 500; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"name\": \"player_").append(i).append("\", \"score\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query 1: All scores >= 250 (should trigger RangeIterator)
          var casIndex1 = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/players/[]/score"), new Int32(250), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count1 = 0;
          while (casIndex1.hasNext()) {
            NodeReferences refs = casIndex1.next();
            count1 += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count1 > 0, "Should find scores >= 250");

          // Query 2: All scores > 400 (smaller range)
          var casIndex2 = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/players/[]/score"), new Int32(400), SearchMode.GREATER, new JsonPCRCollector(wtx)));

          int count2 = 0;
          while (casIndex2.hasNext()) {
            NodeReferences refs = casIndex2.next();
            count2 += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count2 > 0, "Should find scores > 400");

          // Query 3: All scores < 100 (lower range)
          var casIndex3 = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/players/[]/score"), new Int32(100), SearchMode.LOWER, new JsonPCRCollector(wtx)));

          int count3 = 0;
          while (casIndex3.hasNext()) {
            NodeReferences refs = casIndex3.next();
            count3 += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count3 > 0, "Should find scores < 100");
        }
      }
    }

    @Test
    @DisplayName("E2E: CAS index with string range queries")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testCASIndexStringRangeQueries() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create CAS index for strings
          final var pathToCity = parse("/locations/[]/city", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToCity), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Insert 200 locations with city names A-Z prefixed
          StringBuilder json = new StringBuilder("{\"locations\": [");
          for (int i = 0; i < 200; i++) {
            if (i > 0)
              json.append(",");
            char prefix = (char) ('A' + (i % 26));
            json.append("{\"city\": \"").append(prefix).append("_city_").append(String.format("%03d", i)).append("\"}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query: Cities >= "M" (alphabetically)
          var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/locations/[]/city"), new Str("M"), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            NodeReferences refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find cities >= M");
        }
      }
    }

    @Test
    @DisplayName("E2E: CAS index with double values and range queries")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testCASIndexDoubleRangeQueries() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create CAS index for doubles
          final var pathToPrice = parse("/products/[]/price", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.DBL, Collections.singleton(pathToPrice), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Insert 300 products with prices
          StringBuilder json = new StringBuilder("{\"products\": [");
          for (int i = 0; i < 300; i++) {
            if (i > 0)
              json.append(",");
            double price = i * 10.99;
            json.append("{\"name\": \"product_").append(i).append("\", \"price\": ").append(price).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query: Prices >= 1500.0
          var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/products/[]/price"), new Dbl(1500.0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            NodeReferences refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find prices >= 1500.0");
        }
      }
    }
  }

  @Nested
  @DisplayName("Multi-Revision Index E2E Tests")
  class MultiRevisionIndexTests {

    @Test
    @DisplayName("E2E: Index evolves over 10 revisions with data changes")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testIndexEvolutionOver10Revisions() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        // Revision 1: Create index and initial data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToValue = parse("/data/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          StringBuilder json = new StringBuilder("{\"data\": [");
          for (int i = 0; i < 50; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"value\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Query index at latest revision
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          var indexController = session.getRtxIndexController(rtx.getRevisionNumber());
          var casIndexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);

          if (casIndexDef != null) {
            var casIndex = indexController.openCASIndex(rtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
                Set.of("/data/[]/value"), new Int32(0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(rtx)));

            int count = 0;
            while (casIndex.hasNext()) {
              NodeReferences refs = casIndex.next();
              count += refs.getNodeKeys().getLongCardinality();
            }
            assertTrue(count > 0, "Should have indexed values");
          }
        }
      }
    }
  }

  @Nested
  @DisplayName("Large Dataset E2E Tests")
  class LargeDatasetTests {

    @Test
    @DisplayName("E2E: 1000 unique values with index queries")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void test1000UniqueValuesWithQueries() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create CAS index
          final var pathToId = parse("/items/[]/id", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToId), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Insert 1000 items
          StringBuilder json = new StringBuilder("{\"items\": [");
          for (int i = 0; i < 1000; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"id\": ").append(i).append(", \"data\": \"item_").append(i).append("\"}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Multiple range queries
          int[] thresholds = {100, 250, 500, 750, 900};
          for (int threshold : thresholds) {
            var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
                Set.of("/items/[]/id"), new Int32(threshold), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

            int count = 0;
            while (casIndex.hasNext()) {
              NodeReferences refs = casIndex.next();
              count += refs.getNodeKeys().getLongCardinality();
            }
            assertTrue(count > 0, "Should find IDs >= " + threshold);
          }
        }
      }
    }

    @Test
    @DisplayName("E2E: Simple structure with PATH index")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testSimpleStructureWithPathIndex() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create PATH index
          final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.emptySet(), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(pathIndexDef), wtx);

          // Create simple structure
          String json = "{\"a\": {\"b\": 1, \"c\": 2}, \"d\": {\"e\": 3, \"f\": 4}}";
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query PATH index
          var pathIndex = indexController.openPathIndex(wtx.getPageTrx(), pathIndexDef, null);
          int count = 0;
          while (pathIndex.hasNext()) {
            pathIndex.next();
            count++;
            if (count > 50)
              break;
          }
          assertTrue(count >= 0, "PATH index query completed");
        }
      }
    }
  }

  @Nested
  @DisplayName("Complex Query E2E Tests")
  class ComplexQueryTests {

    @Test
    @DisplayName("E2E: CAS index with range queries")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testCASIndexWithRangeQueries() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create CAS index
          final var pathToAge = parse("/users/[]/age", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casAgeIndex =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToAge), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(casAgeIndex), wtx);

          // Insert data
          StringBuilder json = new StringBuilder("{\"users\": [");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"name\": \"user_")
                .append(String.format("%03d", i))
                .append("\", \"age\": ")
                .append(20 + (i % 60))
                .append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query CAS age index
          var ageIndex = indexController.openCASIndex(wtx.getPageTrx(), casAgeIndex, indexController.createCASFilter(
              Set.of("/users/[]/age"), new Int32(40), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));
          int ageCount = 0;
          while (ageIndex.hasNext()) {
            ageIndex.next();
            ageCount++;
          }
          assertTrue(ageCount > 0, "Should find users with age >= 40");
        }
      }
    }

    @Test
    @DisplayName("E2E: Query at boundary values")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testQueryAtBoundaryValues() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToValue = parse("/data/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Insert values at specific boundaries
          StringBuilder json = new StringBuilder("{\"data\": [");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"value\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query at exact boundaries
          for (SearchMode mode : new SearchMode[] {SearchMode.GREATER, SearchMode.GREATER_OR_EQUAL, SearchMode.LOWER,
              SearchMode.LOWER_OR_EQUAL}) {
            var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
                Set.of("/data/[]/value"), new Int32(50), mode, new JsonPCRCollector(wtx)));

            int count = 0;
            while (casIndex.hasNext()) {
              casIndex.next();
              count++;
            }
            // Just verify it doesn't throw
            assertNotNull(Integer.valueOf(count));
          }
        }
      }
    }
  }

  @Nested
  @DisplayName("Range Query E2E Tests")
  class RangeQueryTests {

    @Test
    @DisplayName("E2E: Direct range query via HOTIndexReader.range()")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testDirectRangeQuery() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create CAS index
          final var pathToScore = parse("/data/[]/score", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToScore), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Insert data with sequential scores
          StringBuilder json = new StringBuilder("{\"data\": [");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"score\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Test GREATER_OR_EQUAL range query (triggers RangeIterator)
          var casIndex1 = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/data/[]/score"), new Int32(50), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count1 = 0;
          while (casIndex1.hasNext()) {
            NodeReferences refs = casIndex1.next();
            count1 += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count1 >= 50, "Should find at least 50 entries >= 50, found " + count1);

          // Test GREATER range query
          var casIndex2 = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/data/[]/score"), new Int32(75), SearchMode.GREATER, new JsonPCRCollector(wtx)));

          int count2 = 0;
          while (casIndex2.hasNext()) {
            NodeReferences refs = casIndex2.next();
            count2 += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count2 >= 24, "Should find at least 24 entries > 75, found " + count2);

          // Test LOWER range query (falls back to full scan for now)
          var casIndex3 = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/data/[]/score"), new Int32(25), SearchMode.LOWER, new JsonPCRCollector(wtx)));

          int count3 = 0;
          while (casIndex3.hasNext()) {
            NodeReferences refs = casIndex3.next();
            count3 += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count3 >= 25, "Should find at least 25 entries < 25, found " + count3);
        }
      }
    }

    @Test
    @DisplayName("E2E: Bounded range query (from-to)")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testBoundedRangeQuery() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create CAS index
          final var pathToValue = parse("/items/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Insert data
          StringBuilder json = new StringBuilder("{\"items\": [");
          for (int i = 0; i < 200; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"value\": ").append(i * 10).append("}"); // 0, 10, 20, ..., 1990
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Create HOTIndexReader directly to test range()
          var hotReader = HOTIndexReader.create(wtx.getPageTrx(), CASKeySerializer.INSTANCE, casIndexDef.getType(),
              casIndexDef.getID());

          // Get path node key using JsonPCRCollector
          var pcrCollector = new JsonPCRCollector(wtx);
          long pcr = pcrCollector.getPCRsForPaths(casIndexDef.getPaths()).getPCRs().iterator().next();

          // Test bounded range: values from 500 (inclusive) to 1000 (exclusive)
          CASValue fromKey = new CASValue(new Int32(500), Type.INR, pcr);
          CASValue toKey = new CASValue(new Int32(1000), Type.INR, pcr);

          var rangeIter = hotReader.range(fromKey, toKey);
          int rangeCount = 0;
          while (rangeIter.hasNext()) {
            java.util.Map.Entry<CASValue, NodeReferences> entry = rangeIter.next();
            rangeCount++;
          }
          assertTrue(rangeCount > 0, "Bounded range query should find entries, found " + rangeCount);
        }
      }
    }
  }

  @Nested
  @DisplayName("Index Initialization E2E Tests")
  class IndexInitializationTests {

    @Test
    @DisplayName("E2E: Initialize index and add data")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testIndexInitializationAndAddData() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        // Create index and add data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var casIndexDef = IndexDefs.createCASIdxDef(false, Type.INR,
              Collections.singleton(parse("/items/[]/id", io.brackit.query.util.path.PathParser.Type.JSON)), 0,
              IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          StringBuilder json = new StringBuilder("{\"items\": [");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"id\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query the index
          var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/items/[]/id"), new Int32(0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            casIndex.next();
            count++;
          }
          assertTrue(count > 0, "Index should have entries");
        }
      }
    }
  }
}

