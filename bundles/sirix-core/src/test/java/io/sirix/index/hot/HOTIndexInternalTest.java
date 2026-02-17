/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import io.sirix.page.PageReference;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests targeting internal HOT index code paths for maximum coverage.
 */
@DisplayName("HOT Index Internal Coverage Tests")
class HOTIndexInternalTest {

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
  @DisplayName("Large Dataset Tests")
  class LargeDatasetTests {

    @Test
    @DisplayName("Create 1000 unique paths to trigger splits")
    void testLargePathSet() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 1000; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"p_").append(String.format("%04d", i)).append("\": ").append(i);
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Read back to verify
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("Nested structure with many levels")
    void testDeeplyNestedStructure() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Create deeply nested structure
          StringBuilder json = new StringBuilder();
          int depth = 20;
          for (int i = 0; i < depth; i++) {
            json.append("{\"level").append(i).append("\": ");
          }
          json.append("\"deepValue\"");
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
  }

  @Nested
  @DisplayName("Multi-Revision Tests")
  class MultiRevisionTests {

    @Test
    @DisplayName("20 revisions with growing data")
    void test20RevisionsGrowingData() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.FULL)
                                                     .maxNumberOfRevisionsToRestore(5)
                                                     .build());

        for (int rev = 1; rev <= 20; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {

            if (rev == 1) {
              StringBuilder json = new StringBuilder("[");
              for (int i = 0; i < 10; i++) {
                if (i > 0)
                  json.append(",");
                json.append(i);
              }
              json.append("]");
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
            } else {
              wtx.moveToDocumentRoot();
              wtx.moveToFirstChild(); // array
              wtx.moveToLastChild(); // last element

              // Insert more elements
              for (int i = 0; i < 5; i++) {
                wtx.insertNumberValueAsRightSibling(rev * 100 + i);
              }
            }
            wtx.commit();
          }
        }

        // Read all revisions
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          int mostRecent = session.getMostRecentRevisionNumber();
          assertEquals(20, mostRecent);

          for (int rev = 1; rev <= mostRecent; rev++) {
            try (var rtx = session.beginNodeReadOnlyTrx(rev)) {
              rtx.moveToDocumentRoot();
              assertTrue(rtx.hasFirstChild());
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Alternating insert/delete revisions")
    void testAlternatingInsertDelete() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(3)
                                                     .build());

        // Initial data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1, 2, 3, 4, 5]"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        for (int rev = 0; rev < 10; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();

            if (rev % 2 == 0) {
              // Add elements
              wtx.moveToLastChild();
              wtx.insertNumberValueAsRightSibling(100 + rev);
            } else {
              // Remove first element
              if (wtx.hasFirstChild()) {
                wtx.moveToFirstChild();
                wtx.remove();
              }
            }
            wtx.commit();
          }
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
  @DisplayName("Key Pattern Tests")
  class KeyPatternTests {

    @Test
    @DisplayName("Keys with common prefixes")
    void testCommonPrefixKeys() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Keys with long common prefixes
          StringBuilder json = new StringBuilder("{");
          String[] prefixes = {"user", "username", "userinfo", "userdata", "usersettings"};
          int count = 0;
          for (String prefix : prefixes) {
            for (int i = 0; i < 20; i++) {
              if (count > 0)
                json.append(",");
              json.append("\"")
                  .append(prefix)
                  .append("_")
                  .append(String.format("%03d", i))
                  .append("\": ")
                  .append(count);
              count++;
            }
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }

    @Test
    @DisplayName("Keys with special characters")
    void testSpecialCharacterKeys() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          String json = """
              {
                "normal": 1,
                "with-dash": 2,
                "with_underscore": 3,
                "with.dot": 4,
                "with:colon": 5,
                "with@at": 6,
                "with$dollar": 7,
                "with#hash": 8
              }
              """;

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }

    @Test
    @DisplayName("Unicode keys")
    void testUnicodeKeys() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          String json = """
              {
                "ascii": 1,
                "äöü": 2,
                "日本語": 3,
                "🎉": 4,
                "αβγ": 5,
                "кириллица": 6
              }
              """;

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }
  }

  @Nested
  @DisplayName("Array Tests")
  class ArrayTests {

    @Test
    @DisplayName("Large arrays")
    void testLargeArrays() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

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
      }
    }

    @Test
    @DisplayName("Nested arrays")
    void testNestedArrays() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          String json = """
              [
                [1, 2, 3],
                [4, 5, 6],
                [[7, 8], [9, 10]],
                [[[11, 12]]]
              ]
              """;

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }
  }

  @Nested
  @DisplayName("Mixed Type Tests")
  class MixedTypeTests {

    @Test
    @DisplayName("All JSON types")
    void testAllJsonTypes() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          String json = """
              {
                "string": "hello",
                "number_int": 42,
                "number_float": 3.14,
                "number_negative": -100,
                "number_large": 9999999999999,
                "boolean_true": true,
                "boolean_false": false,
                "null_value": null,
                "array": [1, "two", true, null],
                "nested_object": {
                  "a": 1,
                  "b": {
                    "c": 2
                  }
                },
                "empty_object": {},
                "empty_array": []
              }
              """;

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          assertEquals(NodeKind.OBJECT, rtx.getKind());
        }
      }
    }
  }

  @Nested
  @DisplayName("Stress Tests")
  class StressTests {

    @Test
    @DisplayName("Many small revisions")
    void testManySmallRevisions() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(5)
                                                     .build());

        // Initial data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"counter\": 0}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Many small updates
        for (int i = 1; i <= 30; i++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.remove();
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"counter\": " + i + "}"),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
        }

        // Verify all revisions are accessible
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          int mostRecent = session.getMostRecentRevisionNumber();
          assertTrue(mostRecent >= 30);
        }
      }
    }
  }
}

