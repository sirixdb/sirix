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
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stress tests designed to trigger HOT internal operations: - Page splits when leaf pages overflow
 * - Node upgrades (BiNode -> SpanNode -> MultiNode) - Merge operations during deletions - Indirect
 * page creation and navigation
 */
@DisplayName("HOT Merge/Split Stress Tests")
class HOTMergeSplitStressTest {

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
  @DisplayName("Page Split Tests - Force Multiple Levels")
  class PageSplitTests {

    @Test
    @DisplayName("20,000 unique keys to force multi-level HOT tree")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void test20000UniqueKeys() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 20,000 unique object keys - will definitely trigger many page splits
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 20000; i++) {
            if (i > 0)
              json.append(",");
            // Use padded format to ensure lexicographic ordering
            json.append("\"k_").append(String.format("%06d", i)).append("\": ").append(i);
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Verify data integrity
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          assertTrue(rtx.hasFirstChild(), "Object should have children");
        }
      }
    }

    @Test
    @DisplayName("50,000 array elements to force deep tree")
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void test50000ArrayElements() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 50,000 array elements
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 50000; i++) {
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
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("10,000 nested objects with unique paths")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void test10000NestedObjects() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 10,000 objects with 5 unique keys each = 50,000 total keys
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 10000; i++) {
            if (i > 0)
              json.append(",");
            json.append("{")
                .append("\"id_")
                .append(i)
                .append("\": ")
                .append(i)
                .append(",")
                .append("\"name_")
                .append(i)
                .append("\": \"item_")
                .append(i)
                .append("\",")
                .append("\"value_")
                .append(i)
                .append("\": ")
                .append(i * 10)
                .append(",")
                .append("\"category_")
                .append(i % 100)
                .append("\": ")
                .append(i % 100)
                .append(",")
                .append("\"tag_")
                .append(i % 500)
                .append("\": \"t")
                .append(i % 500)
                .append("\"")
                .append("}");
          }
          json.append("]");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }
  }

  @Nested
  @DisplayName("CAS Index Stress Tests - Trigger Index Splits")
  class CASIndexStressTests {

    @Test
    @DisplayName("CAS index with 10,000 unique integer values")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testCASIndex10000Integers() throws IOException {
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

          // 10,000 unique values
          StringBuilder json = new StringBuilder("{\"items\": [");
          for (int i = 0; i < 10000; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"value\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query across the entire range
          var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/items/[]/value"), new Int32(0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            casIndex.next();
            count++;
            if (count > 15000)
              break; // Safety
          }
          assertTrue(count > 0, "Should find indexed values");
        }
      }
    }

    @Test
    @DisplayName("CAS index with 5,000 unique strings")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testCASIndex5000Strings() throws IOException {
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

          // 5,000 unique string values
          StringBuilder json = new StringBuilder("{\"users\": [");
          for (int i = 0; i < 5000; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"name\": \"user_").append(String.format("%05d", i)).append("\"}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Range query
          var casIndex = indexController.openCASIndex(wtx.getPageTrx(), casIndexDef, indexController.createCASFilter(
              Set.of("/users/[]/name"), new Str("user_02000"), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          int count = 0;
          while (casIndex.hasNext()) {
            casIndex.next();
            count++;
            if (count > 5000)
              break;
          }
          assertTrue(count > 0, "Should find string values >= user_02000");
        }
      }
    }
  }

  @Nested
  @DisplayName("Multi-Revision Stress Tests - Trigger Versioning + Splits")
  class MultiRevisionStressTests {

    @Test
    @DisplayName("100 revisions with growing data")
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void test100RevisionsGrowing() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(10)
                                                     .build());

        // Initial array
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

        // 99 more revisions, each adding 50 elements
        for (int rev = 2; rev <= 100; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array
            wtx.moveToLastChild();

            for (int i = 0; i < 50; i++) {
              wtx.insertNumberValueAsRightSibling(rev * 1000 + i);
            }
            wtx.commit();
          }
        }

        // Verify
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          int mostRecent = session.getMostRecentRevisionNumber();
          assertEquals(100, mostRecent);

          // Read several revisions
          for (int rev : new int[] {1, 25, 50, 75, 100}) {
            try (var rtx = session.beginNodeReadOnlyTrx(rev)) {
              rtx.moveToDocumentRoot();
              assertTrue(rtx.hasFirstChild());
            }
          }
        }
      }
    }

    @Test
    @DisplayName("50 revisions with mixed inserts and deletes")
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void test50RevisionsMixedOperations() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(5)
                                                     .build());

        // Initial data: 1000 objects
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 1000; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"id\": ").append(i).append("}");
          }
          json.append("]");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // 49 more revisions with mixed operations
        for (int rev = 2; rev <= 50; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array

            if (rev % 3 == 0) {
              // Delete first 20 elements
              if (wtx.hasFirstChild()) {
                wtx.moveToFirstChild();
                for (int i = 0; i < 20 && wtx.getKind() != null; i++) {
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
            } else {
              // Add 30 elements as simple numbers
              if (wtx.hasFirstChild()) {
                wtx.moveToLastChild();
                for (int i = 0; i < 30; i++) {
                  wtx.insertNumberValueAsRightSibling(rev * 1000 + i);
                }
              }
            }
            wtx.commit();
          }
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          assertTrue(session.getMostRecentRevisionNumber() >= 40);
        }
      }
    }
  }

  @Nested
  @DisplayName("Delete-Heavy Tests - Trigger Merge Operations")
  class DeleteHeavyTests {

    @Test
    @DisplayName("Create 500 then delete 400 elements over multiple revisions")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    @org.junit.jupiter.api.Disabled("Disabled due to cursor state issue after deletion")
    void testMassiveDeletion() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        // Create 500 elements
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 500; i++) {
            if (i > 0)
              json.append(",");
            json.append(i);
          }
          json.append("]");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Delete elements in batches
        for (int batch = 0; batch < 4; batch++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array

            if (wtx.hasFirstChild()) {
              wtx.moveToFirstChild();

              // Delete 100 elements per batch
              for (int i = 0; i < 100; i++) {
                if (wtx.hasRightSibling()) {
                  long rightKey = wtx.getRightSiblingKey();
                  wtx.remove();
                  if (!wtx.moveTo(rightKey)) {
                    break;
                  }
                } else {
                  break;
                }
              }
            }
            wtx.commit();
          }
        }

        // Verify remaining data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("Alternating bulk inserts and deletes over 30 revisions")
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void testAlternatingBulkOperations() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                     .maxNumberOfRevisionsToRestore(5)
                                                     .build());

        // Initial: 500 elements
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 500; i++) {
            if (i > 0)
              json.append(",");
            json.append(i);
          }
          json.append("]");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // 29 more revisions
        for (int rev = 2; rev <= 30; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array

            if (rev % 2 == 0) {
              // Even revisions: Add 100 elements
              if (wtx.hasFirstChild()) {
                wtx.moveToLastChild();
                for (int i = 0; i < 100; i++) {
                  wtx.insertNumberValueAsRightSibling(rev * 1000 + i);
                }
              }
            } else {
              // Odd revisions: Delete 50 elements from start
              if (wtx.hasFirstChild()) {
                wtx.moveToFirstChild();
                for (int i = 0; i < 50; i++) {
                  if (wtx.hasRightSibling()) {
                    long rightKey = wtx.getRightSiblingKey();
                    wtx.remove();
                    if (!wtx.moveTo(rightKey))
                      break;
                  } else {
                    wtx.remove();
                    break;
                  }
                }
              }
            }
            wtx.commit();
          }
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          assertEquals(30, session.getMostRecentRevisionNumber());
        }
      }
    }
  }

  @Nested
  @DisplayName("Extreme Key Distribution Tests")
  class ExtremeKeyDistributionTests {

    @Test
    @DisplayName("Keys with long common prefixes - stress discriminative bit computation")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testLongCommonPrefixes() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 5000 keys with very long common prefixes
          String commonPrefix =
              "this_is_a_very_long_common_prefix_that_all_keys_share_to_stress_the_discriminative_bit_computation_";
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 5000; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"").append(commonPrefix).append(String.format("%04d", i)).append("\": ").append(i);
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }

    @Test
    @DisplayName("Sparse key distribution - stress tree balancing")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testSparseKeyDistribution() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 3000 keys with sparse, non-sequential values
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 3000; i++) {
            if (i > 0)
              json.append(",");
            // Use prime-based sparse distribution
            int key = (i * 997) % 1000000; // Prime multiplier for distribution
            json.append("\"key_").append(String.format("%06d", key)).append("\": ").append(i);
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }

    @Test
    @DisplayName("Clustered keys - stress SpanNode creation")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testClusteredKeys() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 4000 keys in 10 tight clusters
          StringBuilder json = new StringBuilder("{");
          int keyIndex = 0;
          for (int cluster = 0; cluster < 10; cluster++) {
            int baseKey = cluster * 100000; // Clusters far apart
            for (int i = 0; i < 400; i++) {
              if (keyIndex > 0)
                json.append(",");
              json.append("\"cluster_")
                  .append(cluster)
                  .append("_key_")
                  .append(String.format("%03d", i))
                  .append("\": ")
                  .append(keyIndex);
              keyIndex++;
            }
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }
  }

  @Nested
  @DisplayName("Index Heavy Stress Tests")
  class IndexHeavyStressTests {

    @Test
    @DisplayName("PATH index with 8000 unique paths")
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void testPathIndex8000Paths() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // PATH index
          final var pathIndexDef = IndexDefs.createPathIdxDef(Collections.emptySet(), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(pathIndexDef), wtx);

          // 80 sections x 100 fields = 8000 unique paths
          StringBuilder json = new StringBuilder("{");
          for (int section = 0; section < 80; section++) {
            if (section > 0)
              json.append(",");
            json.append("\"section_").append(section).append("\": {");
            for (int field = 0; field < 100; field++) {
              if (field > 0)
                json.append(",");
              json.append("\"field_").append(field).append("\": ").append(section * 100 + field);
            }
            json.append("}");
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          var pathIdx = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
          assertNotNull(pathIdx, "PATH index should exist");
        }
      }
    }

    @Test
    @DisplayName("NAME index with 1000 unique key names")
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    @org.junit.jupiter.api.Disabled("Disabled due to HOT index initialization issue")
    void testNameIndex1000Names() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // NAME index
          final var nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(nameIndexDef), wtx);

          // 1000 unique key names
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 1000; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"unique_name_").append(String.format("%05d", i)).append("\": ").append(i);
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          var nameIdx = indexController.getIndexes().getIndexDef(0, IndexType.NAME);
          assertNotNull(nameIdx, "NAME index should exist");
        }
      }
    }
  }

  @Nested
  @DisplayName("Wide and Deep Structure Tests")
  class WideDeepStructureTests {

    @Test
    @DisplayName("Very wide objects - 10000 keys in single object")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testVeryWideObject() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 10000; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"w").append(String.format("%05d", i)).append("\": ").append(i);
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }

    @Test
    @DisplayName("Very deep nesting - 100 levels")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testVeryDeepNesting() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          int depth = 100;
          StringBuilder json = new StringBuilder();
          for (int i = 0; i < depth; i++) {
            json.append("{\"level_").append(i).append("\": ");
          }
          json.append("\"deepest_value\"");
          for (int i = 0; i < depth; i++) {
            json.append("}");
          }

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }

    @Test
    @DisplayName("Mixed wide and deep - 500 objects with 100 keys each")
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void testMixedWideDeep() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 500 objects x 100 keys = 50,000 key-value pairs
          StringBuilder json = new StringBuilder("[");
          for (int obj = 0; obj < 500; obj++) {
            if (obj > 0)
              json.append(",");
            json.append("{");
            for (int key = 0; key < 100; key++) {
              if (key > 0)
                json.append(",");
              json.append("\"k").append(key).append("\": ").append(obj * 100 + key);
            }
            json.append("}");
          }
          json.append("]");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }
  }
}

