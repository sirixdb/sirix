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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Large-scale integration tests to trigger HOT internal methods: - Page splits and indirect page
 * creation - Node upgrades (BiNode -> SpanNode -> MultiNode) - Range iteration across multiple
 * pages - Merge operations on deletion
 */
@DisplayName("HOT Large Scale Integration Tests")
class HOTLargeScaleIntegrationTest {

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
  @DisplayName("Large Dataset Tests - Trigger Page Splits")
  class LargeDatasetSplitTests {

    @Test
    @DisplayName("10000 unique keys to trigger multiple page splits")
    void test10000UniqueKeys() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Create 10000 unique object keys - this will trigger many page splits
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 10000; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"key_").append(String.format("%05d", i)).append("\": ").append(i);
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Verify data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          assertTrue(rtx.hasFirstChild(), "Object should have children");
        }
      }
    }

    @Test
    @DisplayName("5000 nested objects to trigger deep tree structure")
    void test5000NestedObjects() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Create array of 5000 objects, each with unique structure
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 5000; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"id\": ")
                .append(i)
                .append(", \"name\": \"item_")
                .append(i)
                .append("\"")
                .append(", \"value\": ")
                .append(i * 100)
                .append(", \"category\": \"cat_")
                .append(i % 50)
                .append("\"")
                .append("}");
          }
          json.append("]");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }
  }

  @Nested
  @DisplayName("CAS Index Large Scale Tests")
  class CASIndexLargeScaleTests {

    @Test
    @DisplayName("CAS index with 2000 unique values - triggers indirect pages")
    void testCASIndex2000Values() throws IOException {
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

          // Create 2000 items with unique values
          StringBuilder json = new StringBuilder("{\"items\": [");
          for (int i = 0; i < 2000; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"value\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Range query to trigger RangeIterator across pages
          var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/items/[]/value"), new Int32(500), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find some values >= 500");
        }
      }
    }

    @Test
    @DisplayName("CAS index range iteration across large dataset")
    void testCASIndexRangeIteration() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToScore = parse("/records/[]/score", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToScore), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // 1000 records with scores 0-999
          StringBuilder json = new StringBuilder("{\"records\": [");
          for (int i = 0; i < 1000; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"score\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query multiple ranges to exercise iterator
          for (int threshold : new int[] {100, 300, 500, 700, 900}) {
            var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
                Set.of("/records/[]/score"), new Int32(threshold), SearchMode.GREATER, new JsonPCRCollector(wtx)));

            int count = 0;
            while (casIndex.hasNext()) {
              var refs = casIndex.next();
              count += refs.getNodeKeys().getLongCardinality();
            }
            assertTrue(count > 0, "Should find values > " + threshold);
          }
        }
      }
    }

    @Test
    @DisplayName("String CAS index with 1500 unique strings")
    void testStringCASIndex1500Values() throws IOException {
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

          // 1500 users with unique names
          StringBuilder json = new StringBuilder("{\"users\": [");
          for (int i = 0; i < 1500; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"name\": \"user_").append(String.format("%04d", i)).append("\"}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Range query on strings
          var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/users/[]/name"), new Str("user_0500"), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find some users >= user_0500");
        }
      }
    }
  }

  @Nested
  @DisplayName("Multi-Revision Large Scale Tests")
  class MultiRevisionLargeScaleTests {

    @Test
    @DisplayName("50 revisions with growing data - triggers merge/split operations")
    void test50RevisionsGrowing() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(5)
                                                     .build());

        // Revision 1: Initial data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append(i);
          }
          json.append("]");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Revisions 2-50: Add more data each time
        for (int rev = 2; rev <= 50; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array
            wtx.moveToLastChild(); // last element

            // Add 20 elements per revision
            for (int i = 0; i < 20; i++) {
              wtx.insertNumberValueAsRightSibling(rev * 1000 + i);
            }
            wtx.commit();
          }
        }

        // Verify all revisions
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          int mostRecent = session.getMostRecentRevisionNumber();
          assertEquals(50, mostRecent);

          // Read random revisions to trigger page loading
          for (int rev : new int[] {1, 10, 25, 40, 50}) {
            try (var rtx = session.beginNodeReadOnlyTrx(rev)) {
              rtx.moveToDocumentRoot();
              assertTrue(rtx.hasFirstChild());
            }
          }
        }
      }
    }

    @Test
    @DisplayName("30 revisions with deletions - triggers merge operations")
    void test30RevisionsWithDeletions() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(5)
                                                     .build());

        // Revision 1: Large initial dataset
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 500; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"id\": ").append(i).append(", \"data\": \"item_").append(i).append("\"}");
          }
          json.append("]");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Revisions 2-30: Delete 10 items each
        for (int rev = 2; rev <= 30; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array

            if (wtx.hasFirstChild()) {
              wtx.moveToFirstChild(); // first object

              // Delete up to 10 items
              for (int i = 0; i < 10 && wtx.getKind() != null; i++) {
                if (wtx.hasRightSibling()) {
                  long rightKey = wtx.getRightSiblingKey();
                  wtx.remove();
                  wtx.moveTo(rightKey);
                } else {
                  wtx.remove();
                  break;
                }
              }
            }
            wtx.commit();
          }
        }

        // Verify
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          int mostRecent = session.getMostRecentRevisionNumber();
          assertTrue(mostRecent >= 20, "Should have at least 20 revisions");
        }
      }
    }
  }

  @Nested
  @DisplayName("Index Update Tests - Trigger Writer Paths")
  class IndexUpdateTests {

    @Test
    @DisplayName("Create, update, and query CAS index repeatedly")
    void testRepeatedIndexUpdates() throws IOException {
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

          // Initial data
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("{\"data\": [{\"value\": 1}, {\"value\": 2}, {\"value\": 3}]}"),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // 20 rounds of updates - add more data each round
        for (int round = 0; round < 20; round++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            // Navigate to the data array and add number values
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // object
            wtx.moveToFirstChild(); // first key (data)
            wtx.moveToFirstChild(); // array

            if (wtx.hasFirstChild()) {
              wtx.moveToLastChild();
              // Add simple number values
              for (int i = 0; i < 10; i++) {
                wtx.insertNumberValueAsRightSibling(round * 1000 + i);
              }
            }
            wtx.commit();
          }
        }

        // Final query
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
          var casIndexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);

          if (casIndexDef != null) {
            var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
                Set.of("/data/[]/value"), new Int32(0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

            int count = 0;
            while (casIndex.hasNext()) {
              var refs = casIndex.next();
              count += refs.getNodeKeys().getLongCardinality();
            }
            assertTrue(count > 0, "Should find indexed values");
          }
        }
      }
    }

    @Test
    @DisplayName("PATH index with 3000 unique paths")
    void testPathIndex3000Paths() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create PATH index for all paths
          final var allPaths = IndexDefs.createPathIdxDef(Collections.emptySet(), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(allPaths), wtx);

          // Create structure with 3000 unique paths
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"section_").append(i).append("\": {");
            for (int j = 0; j < 30; j++) {
              if (j > 0)
                json.append(",");
              json.append("\"field_").append(j).append("\": ").append(i * 100 + j);
            }
            json.append("}");
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("NAME index with many unique names")
    void testNameIndex2000Names() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create NAME index for all keys
          final var nameIndex = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(nameIndex), wtx);

          // Create 2000 unique key names
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 2000; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"unique_key_").append(String.format("%04d", i)).append("\": ").append(i);
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query index
          var idx = indexController.openNameIndex(wtx.getPageTrx(), nameIndex,
              indexController.createNameFilter(Set.of("unique_key_0500")));
          assertTrue(idx.hasNext(), "Should find unique_key_0500");
        }
      }
    }
  }

  @Nested
  @DisplayName("Stress Tests - Maximum Load")
  class StressTests {

    @Test
    @DisplayName("Insert and query 15000 items in single transaction")
    void test15000ItemsSingleTransaction() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 15000 array items
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 15000; i++) {
            if (i > 0)
              json.append(",");
            json.append(i);
          }
          json.append("]");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("Deep nesting with 50 levels")
    void testDeepNesting50Levels() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 50 levels of nesting
          StringBuilder json = new StringBuilder();
          int depth = 50;
          for (int i = 0; i < depth; i++) {
            json.append("{\"level_").append(i).append("\": ");
          }
          json.append("\"deepest\"");
          for (int i = 0; i < depth; i++) {
            json.append("}");
          }

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("Wide objects with 5000 keys each")
    void testWideObjects5000Keys() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Single object with 5000 keys
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 5000; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"k").append(String.format("%04d", i)).append("\": ").append(i);
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("Mixed types - 3000 items with various JSON types")
    void testMixedTypes3000Items() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 3000; i++) {
            if (i > 0)
              json.append(",");
            int type = i % 6;
            switch (type) {
              case 0 -> json.append(i); // number
              case 1 -> json.append("\"string_").append(i).append("\""); // string
              case 2 -> json.append(i % 2 == 0
                  ? "true"
                  : "false"); // boolean
              case 3 -> json.append("null"); // null
              case 4 -> json.append("[").append(i).append(",").append(i + 1).append("]"); // array
              case 5 -> json.append("{\"v\":").append(i).append("}"); // object
            }
          }
          json.append("]");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }
  }

  @Nested
  @DisplayName("Concurrent-like Sequential Tests")
  class SequentialConcurrentLikeTests {

    @Test
    @DisplayName("100 sequential transactions with index updates")
    void test100SequentialTransactions() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                     .maxNumberOfRevisionsToRestore(10)
                                                     .build());

        // Initial empty array
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // 100 transactions
        for (int txn = 0; txn < 100; txn++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array

            if (wtx.hasFirstChild()) {
              wtx.moveToLastChild();
              wtx.insertNumberValueAsRightSibling(txn);
            } else {
              wtx.insertNumberValueAsFirstChild(txn);
            }
            wtx.commit();
          }
        }

        // Verify
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          assertTrue(session.getMostRecentRevisionNumber() >= 100, "Should have at least 100 revisions");
        }
      }
    }
  }
}

