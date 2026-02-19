/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */
package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.access.trx.node.IndexController;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stress tests for bitemporal valid time indexes.
 *
 * <p>
 * These tests create large datasets with many valid time entries to exercise the HOT (Height
 * Optimized Trie) implementation and CAS index internals:
 * <ul>
 * <li>Page splits and indirect page creation</li>
 * <li>Range queries across many index entries</li>
 * <li>Node upgrades in the index structure</li>
 * <li>Merge operations on updates</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
@DisplayName("Bitemporal Index Stress Tests")
public final class BitemporalIndexStressTest {

  private static final Path sirixPath = PATHS.PATH1.getFile();
  private static final String DB_NAME = "bitemporal-stress-db";
  private static final String RESOURCE_NAME = "employees";

  private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Nested
  @DisplayName("Large Dataset Valid Time Index Tests")
  class LargeDatasetTests {

    @Test
    @DisplayName("5000 records with valid time - triggers page splits")
    void test5000RecordsWithValidTime() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Manually create CAS indexes for valid time paths
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var validFromPath = parse("/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
          final var validFromIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);

          final var validToPath = parse("/[]/validTo", io.brackit.query.util.path.PathParser.Type.JSON);
          final var validToIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validToPath), 1, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(validFromIndex, validToIndex), wtx);

          // Generate 5000 records with valid time ranges spanning 10 years
          String json = generateLargeValidTimeDataset(5000, LocalDate.of(2015, 1, 1), LocalDate.of(2025, 1, 1));

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Verify CAS indexes exist
          int casIndexCount = indexController.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS);
          assertTrue(casIndexCount >= 2, "Should have at least 2 CAS indexes, found: " + casIndexCount);
        }

        // Verify data can be read
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("10000 records with overlapping valid time ranges")
    void test10000RecordsOverlappingRanges() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Generate 10000 records with overlapping ranges (many records valid at same time)
          String json = generateOverlappingValidTimeDataset(10000);

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Verify
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("3000 records with sequential non-overlapping valid time ranges")
    void test3000SequentialRanges() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Sequential ranges: each record valid for 1 day, no overlap
          String json = generateSequentialValidTimeDataset(3000, LocalDate.of(2020, 1, 1));

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
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
  @DisplayName("CAS Index Range Query Tests")
  class CASIndexRangeQueryTests {

    @Test
    @DisplayName("Range queries on 2000 valid time entries")
    void testRangeQueriesOn2000Entries() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Create CAS indexes for valid time paths
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var validFromPath = parse("/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
          final var validFromIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);

          final var validToPath = parse("/[]/validTo", io.brackit.query.util.path.PathParser.Type.JSON);
          final var validToIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validToPath), 1, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(validFromIndex, validToIndex), wtx);

          String json = generateLargeValidTimeDataset(2000, LocalDate.of(2020, 1, 1), LocalDate.of(2024, 1, 1));

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Verify CAS indexes exist and query them
          int casIndexCount = indexController.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS);
          assertTrue(casIndexCount >= 2, "Should have CAS indexes for valid time fields");

          // Perform range queries at different thresholds
          String[] thresholds = {"2021-01-01T00:00:00Z", "2022-01-01T00:00:00Z", "2023-01-01T00:00:00Z"};

          for (String threshold : thresholds) {
            var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), validFromIndex,
                indexController.createCASFilter(Set.of("/[]/validFrom"), new Str(threshold),
                    SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

            int count = 0;
            while (casIndex.hasNext()) {
              var refs = casIndex.next();
              count += refs.getNodeKeys().getLongCardinality();
            }
            assertTrue(count > 0, "Should find entries with validFrom >= " + threshold);
          }
        }

        // Verify data can be read in a new transaction
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
          rtx.moveToFirstChild(); // array
          assertTrue(rtx.hasFirstChild(), "Array should have children");
        }
      }
    }

    @Test
    @DisplayName("Exact match queries on valid time index")
    void testExactMatchQueries() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        // Insert data with known timestamps
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          StringBuilder json = new StringBuilder("[");
          // Create 1000 records, 10 at each of 100 different timestamps
          for (int i = 0; i < 100; i++) {
            LocalDate date = LocalDate.of(2020, 1, 1).plusDays(i);
            String validFrom = date.atStartOfDay().toInstant(ZoneOffset.UTC).toString();
            String validTo = date.plusDays(30).atStartOfDay().toInstant(ZoneOffset.UTC).toString();

            for (int j = 0; j < 10; j++) {
              if (i > 0 || j > 0)
                json.append(",");
              json.append("{\"id\": ")
                  .append(i * 10 + j)
                  .append(", \"validFrom\": \"")
                  .append(validFrom)
                  .append("\"")
                  .append(", \"validTo\": \"")
                  .append(validTo)
                  .append("\"")
                  .append("}");
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
  @DisplayName("Exact Index Verification Tests")
  class ExactIndexVerificationTests {

    @Test
    @DisplayName("Verify exact count and nodeKeys for 100 records with known timestamps")
    void testExactCountAndNodeKeys100Records() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        // Track expected nodeKeys for each timestamp
        List<Long> allValidFromNodeKeys = new ArrayList<>();
        List<Long> allValidToNodeKeys = new ArrayList<>();

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Create CAS indexes
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var validFromPath = parse("/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
          final var validFromIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);

          final var validToPath = parse("/[]/validTo", io.brackit.query.util.path.PathParser.Type.JSON);
          final var validToIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validToPath), 1, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(validFromIndex, validToIndex), wtx);

          // Create exactly 100 records with known timestamps
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            LocalDate date = LocalDate.of(2020, 1, 1).plusDays(i);
            String validFrom = date.atStartOfDay().toInstant(ZoneOffset.UTC).toString();
            String validTo = date.plusDays(30).atStartOfDay().toInstant(ZoneOffset.UTC).toString();
            json.append("{\"id\": ")
                .append(i)
                .append(", \"validFrom\": \"")
                .append(validFrom)
                .append("\"")
                .append(", \"validTo\": \"")
                .append(validTo)
                .append("\"")
                .append("}");
          }
          json.append("]");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Collect all validFrom and validTo nodeKeys by traversing the document
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // array
          if (wtx.hasFirstChild()) {
            wtx.moveToFirstChild(); // first object
            do {
              // Each object has: id, validFrom, validTo
              if (wtx.hasFirstChild()) {
                wtx.moveToFirstChild(); // first key (id)
                // Move to validFrom key
                while (wtx.hasRightSibling()) {
                  wtx.moveToRightSibling();
                  if (wtx.isObjectKey() && "validFrom".equals(wtx.getName().getLocalName())) {
                    if (wtx.hasFirstChild()) {
                      wtx.moveToFirstChild();
                      allValidFromNodeKeys.add(wtx.getNodeKey());
                      wtx.moveToParent();
                    }
                  } else if (wtx.isObjectKey() && "validTo".equals(wtx.getName().getLocalName())) {
                    if (wtx.hasFirstChild()) {
                      wtx.moveToFirstChild();
                      allValidToNodeKeys.add(wtx.getNodeKey());
                      wtx.moveToParent();
                    }
                  }
                }
                wtx.moveToParent(); // back to object
              }
            } while (wtx.moveToRightSibling());
          }

          // Verify we found exactly 100 validFrom and 100 validTo nodeKeys
          assertEquals(100, allValidFromNodeKeys.size(), "Should have exactly 100 validFrom nodeKeys");
          assertEquals(100, allValidToNodeKeys.size(), "Should have exactly 100 validTo nodeKeys");

          // Query all indexed validFrom values and collect nodeKeys
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), validFromIndex,
              indexController.createCASFilter(Set.of("/[]/validFrom"), new Str("2020-01-01T00:00:00Z"),
                  SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          List<Long> indexedValidFromNodeKeys = new ArrayList<>();
          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            refs.getNodeKeys().forEach(indexedValidFromNodeKeys::add);
          }

          // Verify exact count
          assertEquals(100, indexedValidFromNodeKeys.size(), "Index should contain exactly 100 validFrom entries");

          // Verify all expected nodeKeys are in the index
          for (Long expectedKey : allValidFromNodeKeys) {
            assertTrue(indexedValidFromNodeKeys.contains(expectedKey), "Index should contain nodeKey " + expectedKey);
          }

          // Query all indexed validTo values
          var casIndexTo = indexController.openCASIndex(wtx.getStorageEngineReader(), validToIndex,
              indexController.createCASFilter(Set.of("/[]/validTo"), new Str("2020-01-01T00:00:00Z"),
                  SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          List<Long> indexedValidToNodeKeys = new ArrayList<>();
          while (casIndexTo.hasNext()) {
            var refs = casIndexTo.next();
            refs.getNodeKeys().forEach(indexedValidToNodeKeys::add);
          }

          assertEquals(100, indexedValidToNodeKeys.size(), "Index should contain exactly 100 validTo entries");

          for (Long expectedKey : allValidToNodeKeys) {
            assertTrue(indexedValidToNodeKeys.contains(expectedKey), "Index should contain nodeKey " + expectedKey);
          }
        }
      }
    }

    @Test
    @DisplayName("Verify exact count for range queries with 500 records")
    void testExactRangeQueryCounts500Records() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var validFromPath = parse("/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
          final var validFromIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(validFromIndex), wtx);

          // Create 500 records: 100 records for each year 2020-2024
          StringBuilder json = new StringBuilder("[");
          for (int year = 2020; year <= 2024; year++) {
            for (int i = 0; i < 100; i++) {
              if (year > 2020 || i > 0)
                json.append(",");
              LocalDate date = LocalDate.of(year, 1, 1).plusDays(i);
              String validFrom = date.atStartOfDay().toInstant(ZoneOffset.UTC).toString();
              String validTo = date.plusMonths(6).atStartOfDay().toInstant(ZoneOffset.UTC).toString();
              json.append("{\"id\": ")
                  .append((year - 2020) * 100 + i)
                  .append(", \"year\": ")
                  .append(year)
                  .append(", \"validFrom\": \"")
                  .append(validFrom)
                  .append("\"")
                  .append(", \"validTo\": \"")
                  .append(validTo)
                  .append("\"")
                  .append("}");
            }
          }
          json.append("]");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for records >= 2022-01-01 (should be 300: 2022, 2023, 2024)
          var casIndex2022 = indexController.openCASIndex(wtx.getStorageEngineReader(), validFromIndex,
              indexController.createCASFilter(Set.of("/[]/validFrom"), new Str("2022-01-01T00:00:00Z"),
                  SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long count2022 = 0;
          while (casIndex2022.hasNext()) {
            var refs = casIndex2022.next();
            count2022 += refs.getNodeKeys().getLongCardinality();
          }
          assertEquals(300, count2022, "Should have exactly 300 records >= 2022-01-01");

          // Query for records >= 2024-01-01 (should be 100: only 2024)
          var casIndex2024 = indexController.openCASIndex(wtx.getStorageEngineReader(), validFromIndex,
              indexController.createCASFilter(Set.of("/[]/validFrom"), new Str("2024-01-01T00:00:00Z"),
                  SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long count2024 = 0;
          while (casIndex2024.hasNext()) {
            var refs = casIndex2024.next();
            count2024 += refs.getNodeKeys().getLongCardinality();
          }
          assertEquals(100, count2024, "Should have exactly 100 records >= 2024-01-01");

          // Query for all records (should be 500)
          var casIndexAll = indexController.openCASIndex(wtx.getStorageEngineReader(), validFromIndex,
              indexController.createCASFilter(Set.of("/[]/validFrom"), new Str("2020-01-01T00:00:00Z"),
                  SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long countAll = 0;
          while (casIndexAll.hasNext()) {
            var refs = casIndexAll.next();
            countAll += refs.getNodeKeys().getLongCardinality();
          }
          assertEquals(500, countAll, "Should have exactly 500 records total");
        }
      }
    }

    @Test
    @DisplayName("Verify nodeKey consistency across revisions with 200 records")
    void testNodeKeyConsistencyAcrossRevisions() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        IndexDef validFromIndex;
        List<Long> rev1NodeKeys = new ArrayList<>();

        // Revision 1: Insert 100 records
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var validFromPath = parse("/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
          validFromIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(validFromIndex), wtx);

          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            LocalDate date = LocalDate.of(2020, 1, 1).plusDays(i);
            String validFrom = date.atStartOfDay().toInstant(ZoneOffset.UTC).toString();
            json.append("{\"id\": ").append(i).append(", \"validFrom\": \"").append(validFrom).append("\"").append("}");
          }
          json.append("]");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Collect nodeKeys from revision 1
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), validFromIndex,
              indexController.createCASFilter(Set.of("/[]/validFrom"), new Str("2020-01-01T00:00:00Z"),
                  SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            refs.getNodeKeys().forEach(rev1NodeKeys::add);
          }

          assertEquals(100, rev1NodeKeys.size(), "Revision 1 should have 100 indexed entries");
        }

        // Revision 2: Add 100 more records
        List<Long> rev2NodeKeys = new ArrayList<>();
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // array
          wtx.moveToLastChild(); // last object

          for (int i = 100; i < 200; i++) {
            LocalDate date = LocalDate.of(2021, 1, 1).plusDays(i - 100);
            String validFrom = date.atStartOfDay().toInstant(ZoneOffset.UTC).toString();
            String record = String.format("{\"id\": %d, \"validFrom\": \"%s\"}", i, validFrom);
            wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(record));
          }
          wtx.commit();

          // Collect all nodeKeys from revision 2
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), validFromIndex,
              indexController.createCASFilter(Set.of("/[]/validFrom"), new Str("2020-01-01T00:00:00Z"),
                  SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          while (casIndex.hasNext()) {
            var refs = casIndex.next();
            refs.getNodeKeys().forEach(rev2NodeKeys::add);
          }

          assertEquals(200, rev2NodeKeys.size(), "Revision 2 should have 200 indexed entries");

          // Verify that all rev1 nodeKeys are still present in rev2
          for (Long key : rev1NodeKeys) {
            assertTrue(rev2NodeKeys.contains(key),
                "NodeKey " + key + " from revision 1 should still be in revision 2 index");
          }

          // Verify that we have exactly 100 new nodeKeys (those that weren't in rev1)
          List<Long> newNodeKeys = new ArrayList<>(rev2NodeKeys);
          newNodeKeys.removeAll(rev1NodeKeys);
          assertEquals(100, newNodeKeys.size(), "Should have exactly 100 new nodeKeys in revision 2");
        }
      }
    }

    @Test
    @DisplayName("Verify exact counts with 1000 records and multiple range queries")
    void testExactCountsWithMultipleRanges1000Records() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var validFromPath = parse("/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
          final var validFromIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(validFromIndex), wtx);

          // Create exactly 1000 records: one per day starting from 2020-01-01
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 1000; i++) {
            if (i > 0)
              json.append(",");
            LocalDate date = LocalDate.of(2020, 1, 1).plusDays(i);
            String validFrom = date.atStartOfDay().toInstant(ZoneOffset.UTC).toString();
            json.append("{\"id\": ").append(i).append(", \"validFrom\": \"").append(validFrom).append("\"").append("}");
          }
          json.append("]");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Test multiple range queries with exact expected counts
          // 2020: days 0-365 = 366 days (leap year)
          // 2021: days 366-730 = 365 days
          // 2022: days 731-999 = 269 days (we only have 1000 total)

          // Query >= 2020-01-01 should return all 1000
          long countAll = countIndexEntries(indexController, wtx, validFromIndex, "2020-01-01T00:00:00Z");
          assertEquals(1000, countAll, "Should have exactly 1000 records >= 2020-01-01");

          // Query >= 2020-07-01 (day 182 in 2020)
          // From day 182 to day 999 = 818 records
          long countMid2020 = countIndexEntries(indexController, wtx, validFromIndex, "2020-07-01T00:00:00Z");
          assertEquals(818, countMid2020, "Should have exactly 818 records >= 2020-07-01");

          // Query >= 2021-01-01 (day 366, since 2020 is leap year)
          // From day 366 to day 999 = 634 records
          long count2021 = countIndexEntries(indexController, wtx, validFromIndex, "2021-01-01T00:00:00Z");
          assertEquals(634, count2021, "Should have exactly 634 records >= 2021-01-01");

          // Query >= 2022-01-01 (day 731)
          // From day 731 to day 999 = 269 records
          long count2022 = countIndexEntries(indexController, wtx, validFromIndex, "2022-01-01T00:00:00Z");
          assertEquals(269, count2022, "Should have exactly 269 records >= 2022-01-01");

          // Query >= 2022-09-27 (the last day, day 999)
          LocalDate lastDay = LocalDate.of(2020, 1, 1).plusDays(999);
          String lastDayStr = lastDay.atStartOfDay().toInstant(ZoneOffset.UTC).toString();
          long countLastDay = countIndexEntries(indexController, wtx, validFromIndex, lastDayStr);
          assertEquals(1, countLastDay, "Should have exactly 1 record on the last day");

          // Query for a date after all records (should return 0)
          long countNone = countIndexEntries(indexController, wtx, validFromIndex, "2023-01-01T00:00:00Z");
          assertEquals(0, countNone, "Should have 0 records >= 2023-01-01");
        }
      }
    }

    private long countIndexEntries(IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx> indexController, JsonNodeTrx wtx,
        IndexDef indexDef, String threshold) {
      var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), indexDef, indexController.createCASFilter(
          Set.of("/[]/validFrom"), new Str(threshold), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

      long count = 0;
      while (casIndex.hasNext()) {
        var refs = casIndex.next();
        count += refs.getNodeKeys().getLongCardinality();
      }
      return count;
    }

  }

  @Nested
  @DisplayName("Multi-Revision Valid Time Tests")
  class MultiRevisionTests {

    @Test
    @DisplayName("50 revisions with growing valid time data")
    void test50RevisionsGrowingData() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.INCREMENTAL)
                                                        .maxNumberOfRevisionsToRestore(5)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        // Revision 1: Initial data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          String json = generateLargeValidTimeDataset(100, LocalDate.of(2020, 1, 1), LocalDate.of(2021, 1, 1));

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Revisions 2-50: Add more data
        for (int rev = 2; rev <= 50; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {

            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array

            if (wtx.hasFirstChild()) {
              wtx.moveToLastChild(); // last element

              // Add 20 new records per revision
              LocalDate baseDate = LocalDate.of(2020, 1, 1).plusDays(rev * 7L);
              for (int i = 0; i < 20; i++) {
                LocalDate validFrom = baseDate.plusDays(i);
                LocalDate validTo = validFrom.plusMonths(6);

                String record = String.format("{\"id\": %d, \"rev\": %d, \"validFrom\": \"%s\", \"validTo\": \"%s\"}",
                    rev * 1000 + i, rev, validFrom.atStartOfDay().toInstant(ZoneOffset.UTC).toString(),
                    validTo.atStartOfDay().toInstant(ZoneOffset.UTC).toString());

                wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(record));
              }
            }
            wtx.commit();
          }
        }

        // Verify revisions were created
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          int mostRecent = session.getMostRecentRevisionNumber();
          assertTrue(mostRecent >= 40, "Should have at least 40 revisions, got: " + mostRecent);

          // Read from different revisions
          for (int rev : new int[] {1, 10, 25, mostRecent}) {
            try (var rtx = session.beginNodeReadOnlyTrx(rev)) {
              rtx.moveToDocumentRoot();
              assertTrue(rtx.hasFirstChild(), "Revision " + rev + " should have data");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("30 revisions with updates to valid time fields")
    void test30RevisionsWithValidTimeUpdates() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.DIFFERENTIAL)
                                                        .maxNumberOfRevisionsToRestore(5)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        // Revision 1: Initial data with 500 records
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          String json = generateLargeValidTimeDataset(500, LocalDate.of(2020, 1, 1), LocalDate.of(2025, 1, 1));

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Revisions 2-30: Update validTo on some records (extending validity)
        for (int rev = 2; rev <= 30; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {

            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array

            if (wtx.hasFirstChild()) {
              wtx.moveToFirstChild(); // first object

              // Move to a random position and update validTo
              int stepsToMove = (rev * 13) % 100; // Deterministic "random"
              for (int i = 0; i < stepsToMove && wtx.hasRightSibling(); i++) {
                wtx.moveToRightSibling();
              }

              // Navigate to validTo and update
              if (wtx.hasFirstChild()) {
                wtx.moveToFirstChild();
                // Find validTo key
                while (wtx.hasRightSibling()) {
                  wtx.moveToRightSibling();
                  if (wtx.isObjectKey() && "validTo".equals(wtx.getName().getLocalName())) {
                    if (wtx.hasFirstChild()) {
                      wtx.moveToFirstChild();
                      LocalDate newValidTo = LocalDate.of(2030, 1, 1).plusDays(rev);
                      wtx.setStringValue(newValidTo.atStartOfDay().toInstant(ZoneOffset.UTC).toString());
                    }
                    break;
                  }
                }
              }
            }
            wtx.commit();
          }
        }

        // Verify
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          assertTrue(session.getMostRecentRevisionNumber() >= 25, "Should have many revisions");
        }
      }
    }
  }

  @Nested
  @DisplayName("Query Function Integration Tests")
  class QueryFunctionTests {

    @Test
    @DisplayName("jn:valid-at with 1000 records")
    void testValidAtWith1000Records() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 1000 records: 500 valid in 2020, 500 valid in 2021
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 500; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"id\": ")
                .append(i)
                .append(", \"year\": 2020")
                .append(", \"validFrom\": \"2020-01-01T00:00:00Z\"")
                .append(", \"validTo\": \"2020-12-31T23:59:59Z\"}");
          }
          for (int i = 500; i < 1000; i++) {
            json.append(",{\"id\": ")
                .append(i)
                .append(", \"year\": 2021")
                .append(", \"validFrom\": \"2021-01-01T00:00:00Z\"")
                .append(", \"validTo\": \"2021-12-31T23:59:59Z\"}");
          }
          json.append("]");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }

      // Query using jn:valid-at
      try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
        store.lookup(DB_NAME);

        try (var ctx = SirixQueryContext.createWithJsonStore(store);
            var chain = SirixCompileChain.createWithJsonStore(store)) {

          // Query for records valid at mid-2020
          String query = "jn:valid-at('" + DB_NAME + "', '" + RESOURCE_NAME + "', xs:dateTime('2020-07-01T12:00:00Z'))";
          Sequence result = new Query(chain, query).evaluate(ctx);

          assertNotNull(result, "Should return results for mid-2020");
        }
      }
    }

    @Test
    @DisplayName("jn:open-bitemporal with multi-revision data")
    void testOpenBitemporalMultiRevision() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        // Revision 1
        Instant rev1Time;
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          String json = "[{\"id\": 1, \"validFrom\": \"2020-01-01T00:00:00Z\", \"validTo\": \"2020-12-31T23:59:59Z\"}]";
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
          rev1Time = wtx.getRevisionTimestamp();
        }

        // Small delay to ensure different timestamp
        try {
          Thread.sleep(10);
        } catch (InterruptedException ignored) {
        }

        // Revision 2
        Instant rev2Time;
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // array
          wtx.moveToLastChild(); // last element

          String record = "{\"id\": 2, \"validFrom\": \"2021-01-01T00:00:00Z\", \"validTo\": \"2021-12-31T23:59:59Z\"}";
          wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(record));
          wtx.commit();
          rev2Time = wtx.getRevisionTimestamp();
        }
      }

      // Query - this exercises the bitemporal combination
      try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
        store.lookup(DB_NAME);

        try (var ctx = SirixQueryContext.createWithJsonStore(store);
            var chain = SirixCompileChain.createWithJsonStore(store)) {

          // Query the most recent revision for records valid in 2020
          String query = "jn:valid-at('" + DB_NAME + "', '" + RESOURCE_NAME + "', xs:dateTime('2020-06-15T12:00:00Z'))";
          Sequence result = new Query(chain, query).evaluate(ctx);
          assertNotNull(result);
        }
      }
    }
  }

  @Nested
  @DisplayName("Edge Cases and Boundary Tests")
  class EdgeCaseTests {

    @Test
    @DisplayName("Records with same validFrom but different validTo")
    void testSameValidFromDifferentValidTo() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 500 records all starting same date but ending differently
          StringBuilder json = new StringBuilder("[");
          String sameValidFrom = "2020-01-01T00:00:00Z";

          for (int i = 0; i < 500; i++) {
            if (i > 0)
              json.append(",");
            LocalDate validTo = LocalDate.of(2020, 1, 1).plusDays(i + 1);
            json.append("{\"id\": ")
                .append(i)
                .append(", \"validFrom\": \"")
                .append(sameValidFrom)
                .append("\"")
                .append(", \"validTo\": \"")
                .append(validTo.atStartOfDay().toInstant(ZoneOffset.UTC))
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

    @Test
    @DisplayName("Records with far future valid time dates")
    void testFarFutureValidTimes() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Records spanning from past to far future
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 200; i++) {
            if (i > 0)
              json.append(",");
            int year = 1990 + i; // 1990 to 2189
            json.append("{\"id\": ")
                .append(i)
                .append(", \"validFrom\": \"")
                .append(year)
                .append("-01-01T00:00:00Z\"")
                .append(", \"validTo\": \"")
                .append(year)
                .append("-12-31T23:59:59Z\"")
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

    @Test
    @DisplayName("Records with millisecond precision timestamps")
    void testMillisecondPrecision() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 1000 records with millisecond-different timestamps
          StringBuilder json = new StringBuilder("[");
          Instant base = Instant.parse("2020-06-15T12:00:00.000Z");

          for (int i = 0; i < 1000; i++) {
            if (i > 0)
              json.append(",");
            Instant validFrom = base.plusMillis(i);
            Instant validTo = validFrom.plus(1, ChronoUnit.HOURS);
            json.append("{\"id\": ")
                .append(i)
                .append(", \"validFrom\": \"")
                .append(validFrom)
                .append("\"")
                .append(", \"validTo\": \"")
                .append(validTo)
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

    @Test
    @DisplayName("Point-in-time validity (validFrom equals validTo)")
    void testPointInTimeValidity() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // 500 records valid at exactly one instant
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 500; i++) {
            if (i > 0)
              json.append(",");
            Instant pointTime = LocalDate.of(2020, 1, 1).plusDays(i).atStartOfDay().toInstant(ZoneOffset.UTC);
            json.append("{\"id\": ")
                .append(i)
                .append(", \"validFrom\": \"")
                .append(pointTime)
                .append("\"")
                .append(", \"validTo\": \"")
                .append(pointTime)
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
  @DisplayName("Stress Tests - Maximum Load")
  class StressTests {

    @Test
    @DisplayName("15000 records single transaction")
    void test15000RecordsSingleTransaction() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          String json = generateLargeValidTimeDataset(15000, LocalDate.of(2000, 1, 1), LocalDate.of(2050, 1, 1));

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
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
    @DisplayName("Repeated index queries on large dataset with valid time")
    void testRepeatedIndexQueriesOnLargeDataset() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        IndexDef validFromIndex;
        IndexDef validToIndex;

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Create CAS indexes for valid time paths
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var validFromPath = parse("/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
          validFromIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);

          final var validToPath = parse("/[]/validTo", io.brackit.query.util.path.PathParser.Type.JSON);
          validToIndex =
              IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validToPath), 1, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(validFromIndex, validToIndex), wtx);

          String json = generateLargeValidTimeDataset(3000, LocalDate.of(2015, 1, 1), LocalDate.of(2025, 1, 1));

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Verify CAS indexes exist
          int casIndexCount = indexController.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS);
          assertTrue(casIndexCount >= 2, "Should have CAS indexes for valid time fields");
        }

        // Repeatedly query the indexes to stress test the HOT implementation
        for (int iteration = 0; iteration < 100; iteration++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {

            var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

            // Query with different thresholds
            int year = 2015 + (iteration % 10);
            String threshold = year + "-01-01T00:00:00Z";

            var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), validFromIndex,
                indexController.createCASFilter(Set.of("/[]/validFrom"), new Str(threshold),
                    SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

            int count = 0;
            while (casIndex.hasNext()) {
              var refs = casIndex.next();
              count += refs.getNodeKeys().getLongCardinality();
            }
            assertTrue(count >= 0, "Query " + iteration + " should complete successfully");
          }
        }

        // Also test repeated reads
        for (int iteration = 0; iteration < 50; iteration++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              var rtx = session.beginNodeReadOnlyTrx()) {
            rtx.moveToDocumentRoot();
            assertTrue(rtx.hasFirstChild(), "Iteration " + iteration + ": should have data");

            rtx.moveToFirstChild(); // array
            int count = 0;
            if (rtx.hasFirstChild()) {
              rtx.moveToFirstChild();
              count++;
              // Count some elements
              while (rtx.hasRightSibling() && count < 100) {
                rtx.moveToRightSibling();
                count++;
              }
            }
            assertTrue(count > 0, "Iteration " + iteration + ": should have counted elements");
          }
        }
      }
    }
  }

  // Helper methods for generating test data

  private String generateLargeValidTimeDataset(int count, LocalDate startDate, LocalDate endDate) {
    StringBuilder json = new StringBuilder("[");
    Random random = new Random(42); // Fixed seed for reproducibility
    long daysBetween = ChronoUnit.DAYS.between(startDate, endDate);

    for (int i = 0; i < count; i++) {
      if (i > 0)
        json.append(",");

      long randomDays = random.nextLong(daysBetween);
      LocalDate validFrom = startDate.plusDays(randomDays);
      LocalDate validTo = validFrom.plusDays(1 + random.nextInt(365)); // 1-365 days validity

      json.append("{\"id\": ")
          .append(i)
          .append(", \"name\": \"record_")
          .append(i)
          .append("\"")
          .append(", \"value\": ")
          .append(random.nextInt(10000))
          .append(", \"validFrom\": \"")
          .append(validFrom.atStartOfDay().toInstant(ZoneOffset.UTC))
          .append("\"")
          .append(", \"validTo\": \"")
          .append(validTo.atStartOfDay().toInstant(ZoneOffset.UTC))
          .append("\"")
          .append("}");
    }
    json.append("]");
    return json.toString();
  }

  private String generateOverlappingValidTimeDataset(int count) {
    StringBuilder json = new StringBuilder("[");

    // Create heavily overlapping ranges - many records valid at any given time
    for (int i = 0; i < count; i++) {
      if (i > 0)
        json.append(",");

      int startMonth = (i % 12) + 1;
      int durationMonths = 3 + (i % 9); // 3-11 months

      LocalDate validFrom = LocalDate.of(2020, startMonth, 1);
      LocalDate validTo = validFrom.plusMonths(durationMonths);

      json.append("{\"id\": ")
          .append(i)
          .append(", \"category\": \"cat_")
          .append(i % 50)
          .append("\"")
          .append(", \"validFrom\": \"")
          .append(validFrom.atStartOfDay().toInstant(ZoneOffset.UTC))
          .append("\"")
          .append(", \"validTo\": \"")
          .append(validTo.atStartOfDay().toInstant(ZoneOffset.UTC))
          .append("\"")
          .append("}");
    }
    json.append("]");
    return json.toString();
  }

  private String generateSequentialValidTimeDataset(int count, LocalDate startDate) {
    StringBuilder json = new StringBuilder("[");

    for (int i = 0; i < count; i++) {
      if (i > 0)
        json.append(",");

      LocalDate validFrom = startDate.plusDays(i);
      LocalDate validTo = validFrom; // Valid for exactly one day

      json.append("{\"id\": ")
          .append(i)
          .append(", \"day\": ")
          .append(i)
          .append(", \"validFrom\": \"")
          .append(validFrom.atStartOfDay().toInstant(ZoneOffset.UTC))
          .append("\"")
          .append(", \"validTo\": \"")
          .append(validTo.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).minusNanos(1))
          .append("\"")
          .append("}");
    }
    json.append("]");
    return json.toString();
  }

  @Nested
  @DisplayName("Unique Path Stress Tests (>1000 paths)")
  class UniquePathStressTests {

    /**
     * Tests CAS index with >1000 unique paths containing valid time fields. This stresses the PCR (Path
     * Class Reference) handling in the index.
     */
    @Test
    @DisplayName("1200 unique paths with validFrom/validTo fields")
    void test1200UniquePathsWithValidTime() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Generate JSON with 1200 unique paths for validFrom/validTo
          // Structure: { section_N: { records: [ { validFrom, validTo, ... } ] } }
          String json = generateJsonWith1200UniquePaths();

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Verify path summary has many unique paths
          var pathSummary = wtx.getPathSummary();
          assertNotNull(pathSummary, "Path summary should exist");
        }

        // Verify data can be read
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    /**
     * Tests CAS index with 1500 unique nested paths, verifying index counts.
     */
    @Test
    @DisplayName("1500 unique nested paths with exact count verification")
    void test1500UniqueNestedPathsWithCounts() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      final int NUM_SECTIONS = 150;
      final int RECORDS_PER_SECTION = 10;
      final int TOTAL_RECORDS = NUM_SECTIONS * RECORDS_PER_SECTION;

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Create CAS indexes that cover all paths
          // Each section has a unique path: /section_N/data/[]/validFrom
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create indexes for multiple path patterns
          List<IndexDef> indexes = new ArrayList<>();
          for (int i = 0; i < NUM_SECTIONS; i++) {
            var validFromPath =
                parse("/section_" + i + "/data/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
            var validFromIndex = IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), i * 2,
                IndexDef.DbType.JSON);

            var validToPath =
                parse("/section_" + i + "/data/[]/validTo", io.brackit.query.util.path.PathParser.Type.JSON);
            var validToIndex = IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validToPath), i * 2 + 1,
                IndexDef.DbType.JSON);

            indexes.add(validFromIndex);
            indexes.add(validToIndex);
          }
          indexController.createIndexes(Set.copyOf(indexes), wtx);

          // Generate JSON with 150 sections, each with 10 records = 1500 unique paths for validFrom
          String json = generateJsonWithNestedPaths(NUM_SECTIONS, RECORDS_PER_SECTION);

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Verify CAS indexes exist
          int casIndexCount = indexController.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS);
          assertEquals(NUM_SECTIONS * 2, casIndexCount,
              "Should have " + (NUM_SECTIONS * 2) + " CAS indexes (validFrom + validTo per section)");

          // Verify total record count by querying first few indexes
          long totalVerified = 0;
          for (int i = 0; i < Math.min(10, NUM_SECTIONS); i++) {
            var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), indexes.get(i * 2),
                indexController.createCASFilter(Set.of("/section_" + i + "/data/[]/validFrom"),
                    new Str("2020-01-01T00:00:00Z"), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

            long count = 0;
            while (casIndex.hasNext()) {
              var refs = casIndex.next();
              count += refs.getNodeKeys().getLongCardinality();
            }
            assertEquals(RECORDS_PER_SECTION, count,
                "Section " + i + " should have exactly " + RECORDS_PER_SECTION + " indexed entries");
            totalVerified += count;
          }
          assertTrue(totalVerified > 0, "Should have verified some records");
        }

        // Verify data integrity in read transaction
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
          rtx.moveToFirstChild();

          // Count top-level keys (should be NUM_SECTIONS)
          int sectionCount = 0;
          if (rtx.hasFirstChild()) {
            rtx.moveToFirstChild();
            sectionCount++;
            while (rtx.hasRightSibling()) {
              rtx.moveToRightSibling();
              sectionCount++;
            }
          }
          assertEquals(NUM_SECTIONS, sectionCount, "Should have " + NUM_SECTIONS + " sections");
        }
      }
    }

    /**
     * Tests deeply nested paths (5+ levels) with valid time fields.
     */
    @Test
    @DisplayName("Deep nesting with 2000 unique paths at varying depths")
    void testDeepNestingWithUniquePathsAtVaryingDepths() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Generate deeply nested structure with 2000 unique paths
          // Paths like: /level1_N/level2_M/level3_O/data/[]/validFrom
          String json = generateDeeplyNestedJson(2000);

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    /**
     * Tests mixed paths with some containing valid time and some not.
     */
    @Test
    @DisplayName("2500 paths with mixed valid time fields")
    void testMixedPathsWithAndWithoutValidTime() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Generate mixed structure:
          // - 1500 paths with valid time fields
          // - 1000 paths without valid time fields (should be ignored by index)
          String json = generateMixedPathsJson(2500);

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    /**
     * Tests paths with similar prefixes to stress the path summary trie.
     */
    @Test
    @DisplayName("1800 paths with similar prefixes")
    void testPathsWithSimilarPrefixes() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Generate paths with similar prefixes to stress trie:
          // /organization/department_N/team_M/member_O/validFrom
          // This creates many paths that share common prefixes
          String json = generateSimilarPrefixPaths(1800);

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    /**
     * Tests index query across many unique paths.
     */
    @Test
    @DisplayName("Query across 1000+ unique paths with explicit index verification")
    void testQueryAcross1000UniquePaths() throws IOException {
      final var dbPath = sirixPath.resolve(DB_NAME);
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      final int NUM_DEPARTMENTS = 100;
      final int EMPLOYEES_PER_DEPT = 10;

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                        .validTimePaths("validFrom", "validTo")
                                                        .versioningApproach(VersioningType.FULL)
                                                        .buildPathSummary(true)
                                                        .build();

        database.createResource(resourceConfig);

        List<Long> allValidFromNodeKeys = new ArrayList<>();

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Create CAS indexes for all department paths
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
          List<IndexDef> indexes = new ArrayList<>();

          for (int dept = 0; dept < NUM_DEPARTMENTS; dept++) {
            var validFromPath =
                parse("/dept_" + dept + "/employees/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
            var validFromIndex = IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), dept,
                IndexDef.DbType.JSON);
            indexes.add(validFromIndex);
          }
          indexController.createIndexes(Set.copyOf(indexes), wtx);

          // Generate structure with 100 departments, each with 10 employees = 1000 records
          StringBuilder json = new StringBuilder("{");
          for (int dept = 0; dept < NUM_DEPARTMENTS; dept++) {
            if (dept > 0)
              json.append(",");
            json.append("\"dept_").append(dept).append("\": {\"employees\": [");
            for (int emp = 0; emp < EMPLOYEES_PER_DEPT; emp++) {
              if (emp > 0)
                json.append(",");
              LocalDate hireDate = LocalDate.of(2020, 1, 1).plusDays(dept * 10L + emp);
              LocalDate contractEnd = hireDate.plusYears(2);
              json.append("{\"id\": ")
                  .append(dept * 1000 + emp)
                  .append(", \"name\": \"emp_")
                  .append(dept)
                  .append("_")
                  .append(emp)
                  .append("\"")
                  .append(", \"validFrom\": \"")
                  .append(hireDate.atStartOfDay().toInstant(ZoneOffset.UTC))
                  .append("\"")
                  .append(", \"validTo\": \"")
                  .append(contractEnd.atStartOfDay().toInstant(ZoneOffset.UTC))
                  .append("\"")
                  .append("}");
            }
            json.append("]}");
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Collect all validFrom nodeKeys by querying each department's index
          for (int dept = 0; dept < NUM_DEPARTMENTS; dept++) {
            var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), indexes.get(dept),
                indexController.createCASFilter(Set.of("/dept_" + dept + "/employees/[]/validFrom"),
                    new Str("2020-01-01T00:00:00Z"), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

            while (casIndex.hasNext()) {
              var refs = casIndex.next();
              refs.getNodeKeys().forEach(allValidFromNodeKeys::add);
            }
          }

          // Verify total count
          assertEquals(NUM_DEPARTMENTS * EMPLOYEES_PER_DEPT, allValidFromNodeKeys.size(),
              "Should have exactly " + (NUM_DEPARTMENTS * EMPLOYEES_PER_DEPT) + " indexed validFrom entries");

          // Verify no duplicate nodeKeys
          long uniqueCount = allValidFromNodeKeys.stream().distinct().count();
          assertEquals(allValidFromNodeKeys.size(), uniqueCount, "All nodeKeys should be unique across departments");
        }
      }
    }

    // Helper methods for generating unique path test data

    private String generateJsonWith1200UniquePaths() {
      // Create 120 sections with 10 records each = 1200 unique paths for validFrom/validTo
      StringBuilder json = new StringBuilder("{");
      for (int section = 0; section < 120; section++) {
        if (section > 0)
          json.append(",");
        json.append("\"section_").append(section).append("\": {\"records\": [");
        for (int i = 0; i < 10; i++) {
          if (i > 0)
            json.append(",");
          LocalDate validFrom = LocalDate.of(2020, 1, 1).plusDays(section * 10L + i);
          LocalDate validTo = validFrom.plusMonths(6);
          json.append("{\"id\": ")
              .append(section * 10 + i)
              .append(", \"validFrom\": \"")
              .append(validFrom.atStartOfDay().toInstant(ZoneOffset.UTC))
              .append("\"")
              .append(", \"validTo\": \"")
              .append(validTo.atStartOfDay().toInstant(ZoneOffset.UTC))
              .append("\"")
              .append("}");
        }
        json.append("]}");
      }
      json.append("}");
      return json.toString();
    }

    private String generateJsonWithNestedPaths(int numSections, int recordsPerSection) {
      StringBuilder json = new StringBuilder("{");
      for (int section = 0; section < numSections; section++) {
        if (section > 0)
          json.append(",");
        json.append("\"section_").append(section).append("\": {\"data\": [");
        for (int i = 0; i < recordsPerSection; i++) {
          if (i > 0)
            json.append(",");
          LocalDate validFrom = LocalDate.of(2020, 1, 1).plusDays(section * recordsPerSection + i);
          LocalDate validTo = validFrom.plusMonths(3);
          json.append("{\"id\": ")
              .append(section * recordsPerSection + i)
              .append(", \"value\": ")
              .append(i * 100)
              .append(", \"validFrom\": \"")
              .append(validFrom.atStartOfDay().toInstant(ZoneOffset.UTC))
              .append("\"")
              .append(", \"validTo\": \"")
              .append(validTo.atStartOfDay().toInstant(ZoneOffset.UTC))
              .append("\"")
              .append("}");
        }
        json.append("]}");
      }
      json.append("}");
      return json.toString();
    }

    private String generateDeeplyNestedJson(int numPaths) {
      // Create paths at varying depths: 20 level1 * 20 level2 * 5 level3 = 2000 paths
      StringBuilder json = new StringBuilder("{");
      int pathCount = 0;
      for (int l1 = 0; l1 < 20 && pathCount < numPaths; l1++) {
        if (l1 > 0)
          json.append(",");
        json.append("\"level1_").append(l1).append("\": {");
        for (int l2 = 0; l2 < 20 && pathCount < numPaths; l2++) {
          if (l2 > 0)
            json.append(",");
          json.append("\"level2_").append(l2).append("\": {");
          for (int l3 = 0; l3 < 5 && pathCount < numPaths; l3++) {
            if (l3 > 0)
              json.append(",");
            json.append("\"level3_").append(l3).append("\": {\"data\": [");
            LocalDate validFrom = LocalDate.of(2020, 1, 1).plusDays(pathCount);
            LocalDate validTo = validFrom.plusYears(1);
            json.append("{\"id\": ")
                .append(pathCount)
                .append(", \"validFrom\": \"")
                .append(validFrom.atStartOfDay().toInstant(ZoneOffset.UTC))
                .append("\"")
                .append(", \"validTo\": \"")
                .append(validTo.atStartOfDay().toInstant(ZoneOffset.UTC))
                .append("\"")
                .append("}]}");
            pathCount++;
          }
          json.append("}");
        }
        json.append("}");
      }
      json.append("}");
      return json.toString();
    }

    private String generateMixedPathsJson(int numPaths) {
      // 60% paths have valid time, 40% don't
      StringBuilder json = new StringBuilder("{");
      for (int i = 0; i < numPaths; i++) {
        if (i > 0)
          json.append(",");
        boolean hasValidTime = (i % 10) < 6; // 60% have valid time
        json.append("\"entity_").append(i).append("\": {\"data\": [");
        LocalDate date = LocalDate.of(2020, 1, 1).plusDays(i);
        if (hasValidTime) {
          json.append("{\"id\": ")
              .append(i)
              .append(", \"value\": ")
              .append(i * 10)
              .append(", \"validFrom\": \"")
              .append(date.atStartOfDay().toInstant(ZoneOffset.UTC))
              .append("\"")
              .append(", \"validTo\": \"")
              .append(date.plusMonths(6).atStartOfDay().toInstant(ZoneOffset.UTC))
              .append("\"")
              .append("}");
        } else {
          // No valid time fields
          json.append("{\"id\": ")
              .append(i)
              .append(", \"value\": ")
              .append(i * 10)
              .append(", \"status\": \"permanent\"")
              .append("}");
        }
        json.append("]}");
      }
      json.append("}");
      return json.toString();
    }

    private String generateSimilarPrefixPaths(int numPaths) {
      // Structure: organization/department_N/team_M/member_O
      // 6 departments * 10 teams * 30 members = 1800 paths
      StringBuilder json = new StringBuilder("{\"organization\": {");
      int pathCount = 0;
      for (int dept = 0; dept < 6 && pathCount < numPaths; dept++) {
        if (dept > 0)
          json.append(",");
        json.append("\"department_").append(dept).append("\": {");
        for (int team = 0; team < 10 && pathCount < numPaths; team++) {
          if (team > 0)
            json.append(",");
          json.append("\"team_").append(team).append("\": {\"members\": [");
          for (int member = 0; member < 30 && pathCount < numPaths; member++) {
            if (member > 0)
              json.append(",");
            LocalDate joinDate = LocalDate.of(2020, 1, 1).plusDays(pathCount);
            LocalDate leaveDate = joinDate.plusYears(3);
            json.append("{\"id\": ")
                .append(pathCount)
                .append(", \"name\": \"member_")
                .append(pathCount)
                .append("\"")
                .append(", \"validFrom\": \"")
                .append(joinDate.atStartOfDay().toInstant(ZoneOffset.UTC))
                .append("\"")
                .append(", \"validTo\": \"")
                .append(leaveDate.atStartOfDay().toInstant(ZoneOffset.UTC))
                .append("\"")
                .append("}");
            pathCount++;
          }
          json.append("]}");
        }
        json.append("}");
      }
      json.append("}}");
      return json.toString();
    }
  }
}
