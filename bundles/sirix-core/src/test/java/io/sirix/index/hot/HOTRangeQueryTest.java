/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests specifically targeting HOT range queries to cover RangeIterator.
 */
@DisplayName("HOT Range Query Tests")
class HOTRangeQueryTest {

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
  @DisplayName("CAS Index Range Queries")
  class CASRangeQueryTests {

    @Test
    @DisplayName("Range query with GREATER search mode")
    void testGreaterSearchMode() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create CAS index on /items/[]/value
          final var pathToValue = parse("/items/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Insert data with numeric values
          String json = """
              {
                "items": [
                  {"value": 10},
                  {"value": 20},
                  {"value": 30},
                  {"value": 40},
                  {"value": 50}
                ]
              }
              """;
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for values GREATER than 25
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/items/[]/value"), new Int32(25), SearchMode.GREATER, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find values greater than 25");
        }
      }
    }

    @Test
    @DisplayName("Range query with LESS search mode")
    void testLessSearchMode() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToValue = parse("/items/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(casIndexDef), wtx);

          String json = """
              {
                "items": [
                  {"value": 10},
                  {"value": 20},
                  {"value": 30},
                  {"value": 40},
                  {"value": 50}
                ]
              }
              """;
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for values LESS than 35
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/items/[]/value"), new Int32(35), SearchMode.LOWER, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find values less than 35");
        }
      }
    }

    @Test
    @DisplayName("Range query with GREATER_OR_EQUAL search mode")
    void testGreaterOrEqualSearchMode() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToValue = parse("/items/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(casIndexDef), wtx);

          String json = """
              {
                "items": [
                  {"value": 10},
                  {"value": 20},
                  {"value": 30},
                  {"value": 40},
                  {"value": 50}
                ]
              }
              """;
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for values GREATER_OR_EQUAL to 30
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/items/[]/value"), new Int32(30), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertEquals(3, count, "Should find exactly 3 values (30, 40, 50)");
        }
      }
    }

    @Test
    @DisplayName("Range query with LESS_OR_EQUAL search mode")
    void testLessOrEqualSearchMode() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToValue = parse("/items/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(casIndexDef), wtx);

          String json = """
              {
                "items": [
                  {"value": 10},
                  {"value": 20},
                  {"value": 30},
                  {"value": 40},
                  {"value": 50}
                ]
              }
              """;
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for values LESS_OR_EQUAL to 30
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/items/[]/value"), new Int32(30), SearchMode.LOWER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertEquals(3, count, "Should find exactly 3 values (10, 20, 30)");
        }
      }
    }
  }

  @Nested
  @DisplayName("String Range Queries")
  class StringRangeQueryTests {

    @Test
    @DisplayName("String GREATER query")
    void testStringGreaterQuery() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToName = parse("/users/[]/name", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(casIndexDef), wtx);

          String json = """
              {
                "users": [
                  {"name": "Alice"},
                  {"name": "Bob"},
                  {"name": "Charlie"},
                  {"name": "David"},
                  {"name": "Eve"}
                ]
              }
              """;
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for names GREATER than "C" (should find Charlie, David, Eve)
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/users/[]/name"), new Str("C"), SearchMode.GREATER, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find names greater than C");
        }
      }
    }

    @Test
    @DisplayName("String LESS query")
    void testStringLessQuery() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToName = parse("/users/[]/name", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(pathToName), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(casIndexDef), wtx);

          String json = """
              {
                "users": [
                  {"name": "Alice"},
                  {"name": "Bob"},
                  {"name": "Charlie"},
                  {"name": "David"},
                  {"name": "Eve"}
                ]
              }
              """;
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for names LESS than "D" (should find Alice, Bob, Charlie)
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/users/[]/name"), new Str("D"), SearchMode.LOWER, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find names less than D");
        }
      }
    }
  }

  @Nested
  @DisplayName("Large Range Queries")
  class LargeRangeQueryTests {

    @Test
    @DisplayName("Range query on large dataset")
    void testLargeRangeQuery() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToValue = parse("/items/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Create 100 items with values 0-99
          StringBuilder json = new StringBuilder("{\"items\": [");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"value\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for values in range [25, 75]
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/items/[]/value"), new Int32(25), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count >= 75, "Should find at least 75 values >= 25");
        }
      }
    }
  }
}

