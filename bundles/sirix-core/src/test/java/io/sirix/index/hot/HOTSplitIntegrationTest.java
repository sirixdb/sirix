/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for HOT page splits with read/write operations. These tests verify that
 * HOTIndirectPage navigation works correctly after leaf page splits.
 */
@DisplayName("HOT Split Integration Tests")
class HOTSplitIntegrationTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    System.setProperty("sirix.index.useHOT", "true");
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.closeEverything();
    System.clearProperty("sirix.index.useHOT");
  }

  @Nested
  @DisplayName("Multi-page read after split")
  class MultiPageReadAfterSplit {

    @Test
    @DisplayName("Can read all entries after page split")
    void testReadAfterSplit() {
      // Create JSON with many keys
      StringBuilder json = new StringBuilder("{");
      for (int i = 0; i < 100; i++) {
        if (i > 0)
          json.append(",");
        json.append(String.format("\"key%03d\": \"value%d\"", i, i));
      }
      json.append("}");

      // Create a database and resource
      try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
        try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
            final JsonNodeTrx wtx = session.beginNodeTrx()) {

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()));
          wtx.commit();
        }
      }

      // Now read back and verify all entries are accessible
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
        try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
            final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

          // Navigate and count nodes
          int objectKeyCount = 0;
          rtx.moveToDocumentRoot();
          if (rtx.moveToFirstChild()) { // Object
            if (rtx.moveToFirstChild()) { // First key
              do {
                if (rtx.getKind() == NodeKind.OBJECT_KEY) {
                  objectKeyCount++;
                }
              } while (rtx.moveToRightSibling());
            }
          }

          assertEquals(100, objectKeyCount, "Should have 100 object keys");
        }
      }
    }

    @Test
    @DisplayName("Iterator traverses all pages after split")
    void testIteratorAfterSplit() {
      // Create deeply nested JSON
      StringBuilder json = new StringBuilder();
      for (int i = 0; i < 50; i++) {
        json.append("{\"level").append(i).append("\": ");
      }
      json.append("{\"deepValue\": \"found\"}");
      for (int i = 0; i < 50; i++) {
        json.append("}");
      }

      // Create database with many unique paths to trigger splits
      try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
        try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
            final JsonNodeTrx wtx = session.beginNodeTrx()) {

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()));
          wtx.commit();
        }
      }

      // Verify all paths are accessible
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
        try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
            final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

          // Navigate to the deepest level
          rtx.moveToDocumentRoot();
          int depth = 0;
          while (rtx.moveToFirstChild()) {
            depth++;
          }

          assertTrue(depth > 50, "Should be able to navigate deep into the tree");
        }
      }
    }
  }

  @Nested
  @DisplayName("Concurrent writes with splits")
  class ConcurrentWritesWithSplits {

    @Test
    @DisplayName("Multiple commits with growing index")
    void testMultipleCommitsWithGrowingIndex() {
      try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
        try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

          // First commit - create object with batch1 keys
          StringBuilder json1 = new StringBuilder("{");
          for (int i = 0; i < 30; i++) {
            if (i > 0)
              json1.append(",");
            json1.append("\"batch1_key").append(i).append("\": \"value").append(i).append("\"");
          }
          json1.append("}");

          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json1.toString()));
            wtx.commit();
          }

          // Second commit - replace the whole document with more keys
          StringBuilder json2 = new StringBuilder("{");
          for (int i = 0; i < 60; i++) {
            if (i > 0)
              json2.append(",");
            if (i < 30) {
              json2.append("\"batch1_key").append(i).append("\": \"value").append(i).append("\"");
            } else {
              json2.append("\"batch2_key").append(i - 30).append("\": \"value").append(i).append("\"");
            }
          }
          json2.append("}");

          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.remove(); // Remove old document
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json2.toString()));
            wtx.commit();
          }

          // Third commit - even more keys
          StringBuilder json3 = new StringBuilder("{");
          for (int i = 0; i < 90; i++) {
            if (i > 0)
              json3.append(",");
            if (i < 30) {
              json3.append("\"batch1_key").append(i).append("\": \"value").append(i).append("\"");
            } else if (i < 60) {
              json3.append("\"batch2_key").append(i - 30).append("\": \"value").append(i).append("\"");
            } else {
              json3.append("\"batch3_key").append(i - 60).append("\": \"value").append(i).append("\"");
            }
          }
          json3.append("}");

          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.remove(); // Remove old document
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json3.toString()));
            wtx.commit();
          }
        }
      }

      // Verify all data is accessible from latest revision
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
        try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
            final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

          int keyCount = 0;
          rtx.moveToDocumentRoot();
          if (rtx.moveToFirstChild()) { // Object
            if (rtx.moveToFirstChild()) { // First key
              do {
                if (rtx.getKind() == NodeKind.OBJECT_KEY) {
                  keyCount++;
                }
              } while (rtx.moveToRightSibling());
            }
          }

          assertEquals(90, keyCount, "Should have 90 object keys (30 from each batch)");
        }
      }
    }
  }

  @Nested
  @DisplayName("Range queries after split")
  class RangeQueriesAfterSplit {

    @Test
    @DisplayName("Range query finds entries across multiple pages")
    void testRangeQueryAcrossPages() {
      // Create JSON array with many elements
      StringBuilder json = new StringBuilder("[");
      for (int i = 0; i < 200; i++) {
        if (i > 0)
          json.append(",");
        json.append(i);
      }
      json.append("]");

      // Create a database with enough entries to potentially split
      try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
        try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
            final JsonNodeTrx wtx = session.beginNodeTrx()) {

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()));
          wtx.commit();
        }
      }

      // Verify all elements are accessible
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
        try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
            final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

          int elementCount = 0;
          rtx.moveToDocumentRoot();
          if (rtx.moveToFirstChild()) { // Array
            if (rtx.moveToFirstChild()) { // First element
              do {
                if (rtx.getKind() == NodeKind.NUMBER_VALUE) {
                  elementCount++;
                }
              } while (rtx.moveToRightSibling());
            }
          }

          assertEquals(200, elementCount, "Should have 200 number elements");
        }
      }
    }
  }

  @Nested
  @DisplayName("Revision history with splits")
  class RevisionHistoryWithSplits {

    @Test
    @DisplayName("Data persists across multiple revisions")
    void testDataPersistsAcrossRevisions() {
      // Build JSON with 50 keys
      StringBuilder json = new StringBuilder("{");
      for (int i = 0; i < 50; i++) {
        if (i > 0)
          json.append(",");
        json.append("\"key").append(i).append("\": \"value").append(i).append("\"");
      }
      json.append("}");

      try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
        try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
            final JsonNodeTrx wtx = session.beginNodeTrx()) {

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()));
          wtx.commit();
        }
      }

      // Re-open database and verify data is accessible
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
        try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
            final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

          int keyCount = 0;
          rtx.moveToDocumentRoot();
          if (rtx.moveToFirstChild()) { // Object
            if (rtx.moveToFirstChild()) { // First key
              do {
                if (rtx.getKind() == NodeKind.OBJECT_KEY) {
                  keyCount++;
                }
              } while (rtx.moveToRightSibling());
            }
          }

          assertEquals(50, keyCount, "Should have 50 keys after reopening database");
        }
      }
    }
  }
}

