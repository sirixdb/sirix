/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.query.function.jn.index;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.path.PathIndexListenerFactory;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Stress tests for HOT (Height Optimized Trie) indexes with large numbers of unique paths and
 * names.
 * 
 * <p>
 * These tests verify that HOT indexes can handle at least 50,000 unique names and paths
 * efficiently, with proper verification of:
 * </p>
 * <ul>
 * <li>Index iteration returns all entries</li>
 * <li>NodeKey correctness</li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 */
@Tag("stress")
@DisplayName("HOT Index Stress Tests")
public class HOTIndexStressTest {

  private static final String TMPDIR = System.getProperty("java.io.tmpdir");

  /** Use unique path per test to avoid interference. */
  private Path testDbPath;

  @BeforeEach
  void setUp(TestInfo testInfo) throws IOException {
    // Use unique database path per test
    String testName = testInfo.getDisplayName().replaceAll("[^a-zA-Z0-9]", "_");
    testDbPath = Path.of(TMPDIR, "sirix", "hot-stress-" + testName + "-" + System.nanoTime());

    // Clean up any previous test data
    Databases.removeDatabase(testDbPath);
    Files.createDirectories(testDbPath.getParent());

    // Enable HOT indexes for testing
    System.setProperty(PathIndexListenerFactory.USE_HOT_PROPERTY, "true");
  }

  @AfterEach
  void tearDown() {
    try {
      Databases.removeDatabase(testDbPath);
    } catch (Exception e) {
      // Ignore cleanup errors
    }
    System.clearProperty(PathIndexListenerFactory.USE_HOT_PROPERTY);
  }

  // =========================================================================
  // QUICK VALIDATION TESTS (1,000 entries)
  // =========================================================================

  @Test
  @DisplayName("Quick validation: 1,000 names with iteration verification")
  void testQuickNameIndex() throws IOException {
    final int count = 1_000;
    final Roaring64Bitmap expectedNodeKeys = new Roaring64Bitmap();

    final var dbConfig = new DatabaseConfiguration(testDbPath);
    Databases.createJsonDatabase(dbConfig);

    try (final var database = Databases.openJsonDatabase(testDbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("test").build());

      // Insert data and track node keys
      try (final var session = database.beginResourceSession("test"); final var wtx = session.beginNodeTrx()) {

        wtx.insertObjectAsFirstChild();

        for (int i = 0; i < count; i++) {
          String fieldName = "field_" + i;
          wtx.insertObjectRecordAsFirstChild(fieldName, new NumberValue(i));
          // After insertObjectRecordAsFirstChild, cursor is on the VALUE node
          // The name index indexes the KEY node (parent), so we need its node key
          wtx.moveToParent(); // Move to object key node
          expectedNodeKeys.add(wtx.getNodeKey());
          wtx.moveToParent(); // Move to parent object
        }

        wtx.commit();

        // Create name index
        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
        final var indexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(indexDef), wtx);
        wtx.commit();
      }

      // Verify with full iteration through indexController
      try (final var session = database.beginResourceSession("test"); final var rtx = session.beginNodeReadOnlyTrx()) {

        final var indexController = session.getRtxIndexController(rtx.getRevisionNumber());
        var nameIndexOpt = indexController.getIndexes().findNameIndex();
        assertTrue(nameIndexOpt.isPresent(), "Name index should exist");
        IndexDef indexDef = nameIndexOpt.get();

        // Use proper indexController API (empty filter = get all)
        Iterator<NodeReferences> iter =
            indexController.openNameIndex(rtx.getPageTrx(), indexDef, indexController.createNameFilter(Set.of()));

        final Roaring64Bitmap foundNodeKeys = new Roaring64Bitmap();
        int iterCount = 0;
        while (iter.hasNext()) {
          NodeReferences refs = iter.next();
          foundNodeKeys.or(refs.getNodeKeys());
          iterCount++;
        }

        assertEquals(count, iterCount, "Iterator should return all " + count + " names");
        assertEquals(count, foundNodeKeys.getLongCardinality(), "Should have " + count + " unique node keys");

        // Verify all expected node keys are present
        final Roaring64Bitmap missingKeys = expectedNodeKeys.clone();
        missingKeys.andNot(foundNodeKeys);
        if (!missingKeys.isEmpty()) {
          LongIterator missingIter = missingKeys.getLongIterator();
          StringBuilder sb = new StringBuilder("Missing node keys: ");
          int shown = 0;
          while (missingIter.hasNext() && shown < 10) {
            sb.append(missingIter.next()).append(" ");
            shown++;
          }
          fail(sb.toString());
        }
      }
    }
  }

  @Test
  @DisplayName("Quick validation: 1,000 paths with iteration verification")
  void testQuickPathIndex() throws IOException {
    final int count = 1_000;
    // Path index indexes by PCR, all entries at same level share PCR
    // So we just verify the total node key count matches
    long totalExpectedNodeKeys = 0;

    final var dbConfig = new DatabaseConfiguration(testDbPath);
    Databases.createJsonDatabase(dbConfig);

    try (final var database = Databases.openJsonDatabase(testDbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("test").build());

      // Insert data first, then create index (like the name index test)
      try (final var session = database.beginResourceSession("test"); final var wtx = session.beginNodeTrx()) {

        wtx.insertObjectAsFirstChild();

        for (int i = 0; i < count; i++) {
          String fieldName = "path_" + i;
          wtx.insertObjectRecordAsFirstChild(fieldName, new NumberValue(i));
          // After insertObjectRecordAsFirstChild, cursor is on the VALUE node
          // The path index indexes the KEY node (parent)
          wtx.moveToParent(); // Move to object key node
          totalExpectedNodeKeys++; // Just count how many we expect
          wtx.moveToParent(); // Move to parent object
        }

        wtx.commit();

        // Create path index AFTER data insertion
        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
        final var indexDef = IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(indexDef), wtx);
        wtx.commit();
      }

      // Verify with full iteration
      try (final var session = database.beginResourceSession("test"); final var rtx = session.beginNodeReadOnlyTrx()) {

        final var indexController = session.getRtxIndexController(rtx.getRevisionNumber());
        var pathIndexOpt = indexController.getIndexes()
                                          .getIndexDefs()
                                          .stream()
                                          .filter(idx -> idx.getType() == IndexType.PATH)
                                          .findFirst();
        assertTrue(pathIndexOpt.isPresent(), "Path index should exist");
        IndexDef indexDef = pathIndexOpt.get();

        // Use proper indexController API
        Iterator<NodeReferences> iter = indexController.openPathIndex(rtx.getPageTrx(), indexDef, null);

        final Roaring64Bitmap foundNodeKeys = new Roaring64Bitmap();
        int entryCount = 0;
        while (iter.hasNext()) {
          NodeReferences refs = iter.next();
          foundNodeKeys.or(refs.getNodeKeys());
          entryCount++;
        }

        assertTrue(entryCount > 0, "Should have indexed PCR entries, got: " + entryCount);

        // Path index should have indexed all ObjectKeyNodes
        // Each unique path creates its own PCR entry, so entryCount should equal count
        assertEquals(count, entryCount, "Should have " + count + " unique PCR entries (one per unique path)");
        assertEquals(count, foundNodeKeys.getLongCardinality(),
            "Should have " + count + " unique node keys in path index");
      }
    }
  }

  // =========================================================================
  // LARGE SCALE TESTS (50,000 entries)
  // =========================================================================

  @Test
  @DisplayName("50,000 unique names with verification")
  void testLargeNameIndex() throws IOException {
    final int count = 50_000;
    final Roaring64Bitmap expectedNodeKeys = new Roaring64Bitmap();

    final var dbConfig = new DatabaseConfiguration(testDbPath);
    Databases.createJsonDatabase(dbConfig);

    try (final var database = Databases.openJsonDatabase(testDbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("test").build());

      long startTime = System.nanoTime();

      // Insert data
      try (final var session = database.beginResourceSession("test"); final var wtx = session.beginNodeTrx()) {

        wtx.insertObjectAsFirstChild();

        for (int i = 0; i < count; i++) {
          String fieldName = generateUniqueName(i);
          wtx.insertObjectRecordAsFirstChild(fieldName, new NumberValue(i));
          // After insertObjectRecordAsFirstChild, cursor is on the VALUE node
          // The name index indexes the KEY node (parent)
          wtx.moveToParent(); // Move to object key node
          expectedNodeKeys.add(wtx.getNodeKey());
          wtx.moveToParent(); // Move to parent object

          if (i % 10_000 == 0 && i > 0) {
            System.out.printf("  Inserted %d names...%n", i);
          }
        }

        long insertTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        System.out.printf("Inserted %d names in %d ms%n", count, insertTime);

        wtx.commit();

        // Create name index
        startTime = System.nanoTime();
        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
        final var indexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(indexDef), wtx);

        long indexTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        System.out.printf("Created name index in %d ms%n", indexTime);

        wtx.commit();
      }

      // Verify
      try (final var session = database.beginResourceSession("test"); final var rtx = session.beginNodeReadOnlyTrx()) {

        final var indexController = session.getRtxIndexController(rtx.getRevisionNumber());
        var nameIndexOpt = indexController.getIndexes().findNameIndex();
        assertTrue(nameIndexOpt.isPresent(), "Name index should exist");
        IndexDef indexDef = nameIndexOpt.get();

        // Full iteration (empty filter = get all)
        long startTime2 = System.nanoTime();
        Iterator<NodeReferences> iter =
            indexController.openNameIndex(rtx.getPageTrx(), indexDef, indexController.createNameFilter(Set.of()));

        int iterCount = 0;
        final Roaring64Bitmap allNodeKeys = new Roaring64Bitmap();
        while (iter.hasNext()) {
          NodeReferences refs = iter.next();
          allNodeKeys.or(refs.getNodeKeys());
          iterCount++;
        }

        long iterTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime2);
        System.out.printf("Full iteration: %d entries, %d node keys in %d ms%n", iterCount,
            allNodeKeys.getLongCardinality(), iterTime);

        assertEquals(count, iterCount, "Iterator should return all " + count + " names");
        assertEquals(count, allNodeKeys.getLongCardinality(), "Should have " + count + " unique node keys");

        // Verify all expected node keys are present
        final Roaring64Bitmap missingKeys = expectedNodeKeys.clone();
        missingKeys.andNot(allNodeKeys);
        if (!missingKeys.isEmpty()) {
          LongIterator missingIter = missingKeys.getLongIterator();
          StringBuilder sb =
              new StringBuilder("Missing " + missingKeys.getLongCardinality() + " node keys. First 10: ");
          int shown = 0;
          while (missingIter.hasNext() && shown < 10) {
            sb.append(missingIter.next()).append(" ");
            shown++;
          }
          fail(sb.toString());
        }
      }
    }
  }

  @Test
  @DisplayName("50,000 unique paths with verification")
  void testLargePathIndex() throws IOException {
    final int count = 50_000;
    final Roaring64Bitmap expectedNodeKeys = new Roaring64Bitmap();

    final var dbConfig = new DatabaseConfiguration(testDbPath);
    Databases.createJsonDatabase(dbConfig);

    try (final var database = Databases.openJsonDatabase(testDbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("test").build());

      long startTime = System.nanoTime();

      // Insert data first
      try (final var session = database.beginResourceSession("test"); final var wtx = session.beginNodeTrx()) {

        wtx.insertObjectAsFirstChild();

        for (int i = 0; i < count; i++) {
          String fieldName = "path_" + i + "_" + Integer.toHexString(i);
          wtx.insertObjectRecordAsFirstChild(fieldName, new NumberValue(i));
          // After insertObjectRecordAsFirstChild, cursor is on the VALUE node
          // The path index indexes the KEY node (parent)
          wtx.moveToParent(); // Move to object key node
          expectedNodeKeys.add(wtx.getNodeKey());
          wtx.moveToParent(); // Move to parent object

          if (i % 10_000 == 0 && i > 0) {
            System.out.printf("  Inserted %d paths...%n", i);
          }
        }

        long insertTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        System.out.printf("Inserted %d paths in %d ms%n", count, insertTime);

        wtx.commit();

        // Create path index AFTER data insertion
        startTime = System.nanoTime();
        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
        final var indexDef = IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(indexDef), wtx);

        long indexTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        System.out.printf("Created path index in %d ms%n", indexTime);

        wtx.commit();
      }

      // Verify
      try (final var session = database.beginResourceSession("test"); final var rtx = session.beginNodeReadOnlyTrx()) {

        final var indexController = session.getRtxIndexController(rtx.getRevisionNumber());
        var pathIndexOpt = indexController.getIndexes()
                                          .getIndexDefs()
                                          .stream()
                                          .filter(idx -> idx.getType() == IndexType.PATH)
                                          .findFirst();
        assertTrue(pathIndexOpt.isPresent(), "Path index should exist");
        IndexDef indexDef = pathIndexOpt.get();

        // Full iteration
        long startTime2 = System.nanoTime();
        final Roaring64Bitmap foundNodeKeys = new Roaring64Bitmap();
        Iterator<NodeReferences> iter = indexController.openPathIndex(rtx.getPageTrx(), indexDef, null);
        int entryCount = 0;
        while (iter.hasNext()) {
          NodeReferences refs = iter.next();
          foundNodeKeys.or(refs.getNodeKeys());
          entryCount++;
        }

        long iterTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime2);
        System.out.printf("Full iteration: %d PCR entries, %d node keys in %d ms%n", entryCount,
            foundNodeKeys.getLongCardinality(), iterTime);

        assertTrue(entryCount > 0, "Should have indexed entries");

        // Verify all expected node keys are present
        final Roaring64Bitmap missingKeys = expectedNodeKeys.clone();
        missingKeys.andNot(foundNodeKeys);
        if (!missingKeys.isEmpty()) {
          LongIterator missingIter = missingKeys.getLongIterator();
          StringBuilder sb =
              new StringBuilder("Missing " + missingKeys.getLongCardinality() + " node keys. First 10: ");
          int shown = 0;
          while (missingIter.hasNext() && shown < 10) {
            sb.append(missingIter.next()).append(" ");
            shown++;
          }
          fail(sb.toString());
        }

        assertEquals(count, foundNodeKeys.getLongCardinality(),
            "Should have " + count + " unique node keys in path index");
      }
    }
  }

  // =========================================================================
  // HELPER METHODS
  // =========================================================================

  /**
   * Generate a unique field name for testing.
   */
  private String generateUniqueName(int index) {
    String[] prefixes = {"user", "item", "data", "config", "meta", "info", "attr", "prop", "val", "elem"};
    String prefix = prefixes[index % prefixes.length];
    return prefix + "_" + index + "_" + Integer.toHexString(index);
  }
}
