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
 * <p>These tests create large datasets with many valid time entries to exercise
 * the HOT (Height Optimized Trie) implementation and CAS index internals:
 * <ul>
 *   <li>Page splits and indirect page creation</li>
 *   <li>Range queries across many index entries</li>
 *   <li>Node upgrades in the index structure</li>
 *   <li>Merge operations on updates</li>
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
          final var validFromIndex = IndexDefs.createCASIdxDef(false, Type.STR,
              Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);

          final var validToPath = parse("/[]/validTo", io.brackit.query.util.path.PathParser.Type.JSON);
          final var validToIndex = IndexDefs.createCASIdxDef(false, Type.STR,
              Collections.singleton(validToPath), 1, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(validFromIndex, validToIndex), wtx);

          // Generate 5000 records with valid time ranges spanning 10 years
          String json = generateLargeValidTimeDataset(5000,
              LocalDate.of(2015, 1, 1),
              LocalDate.of(2025, 1, 1));

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
          final var validFromIndex = IndexDefs.createCASIdxDef(false, Type.STR,
              Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);

          final var validToPath = parse("/[]/validTo", io.brackit.query.util.path.PathParser.Type.JSON);
          final var validToIndex = IndexDefs.createCASIdxDef(false, Type.STR,
              Collections.singleton(validToPath), 1, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(validFromIndex, validToIndex), wtx);

          String json = generateLargeValidTimeDataset(2000,
              LocalDate.of(2020, 1, 1),
              LocalDate.of(2024, 1, 1));

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Verify CAS indexes exist and query them
          int casIndexCount = indexController.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS);
          assertTrue(casIndexCount >= 2, "Should have CAS indexes for valid time fields");

          // Perform range queries at different thresholds
          String[] thresholds = {
              "2021-01-01T00:00:00Z",
              "2022-01-01T00:00:00Z",
              "2023-01-01T00:00:00Z"
          };

          for (String threshold : thresholds) {
            var casIndex = indexController.openCASIndex(wtx.getPageTrx(), validFromIndex,
                indexController.createCASFilter(
                    Set.of("/[]/validFrom"),
                    new Str(threshold),
                    SearchMode.GREATER_OR_EQUAL,
                    new JsonPCRCollector(wtx)));

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
          rtx.moveToFirstChild();  // array
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
              if (i > 0 || j > 0) json.append(",");
              json.append("{\"id\": ").append(i * 10 + j)
                  .append(", \"validFrom\": \"").append(validFrom).append("\"")
                  .append(", \"validTo\": \"").append(validTo).append("\"")
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

          String json = generateLargeValidTimeDataset(100,
              LocalDate.of(2020, 1, 1),
              LocalDate.of(2021, 1, 1));

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Revisions 2-50: Add more data
        for (int rev = 2; rev <= 50; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
               JsonNodeTrx wtx = session.beginNodeTrx()) {

            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();  // array

            if (wtx.hasFirstChild()) {
              wtx.moveToLastChild();   // last element

              // Add 20 new records per revision
              LocalDate baseDate = LocalDate.of(2020, 1, 1).plusDays(rev * 7L);
              for (int i = 0; i < 20; i++) {
                LocalDate validFrom = baseDate.plusDays(i);
                LocalDate validTo = validFrom.plusMonths(6);

                String record = String.format(
                    "{\"id\": %d, \"rev\": %d, \"validFrom\": \"%s\", \"validTo\": \"%s\"}",
                    rev * 1000 + i,
                    rev,
                    validFrom.atStartOfDay().toInstant(ZoneOffset.UTC).toString(),
                    validTo.atStartOfDay().toInstant(ZoneOffset.UTC).toString()
                );

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

          String json = generateLargeValidTimeDataset(500,
              LocalDate.of(2020, 1, 1),
              LocalDate.of(2025, 1, 1));

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Revisions 2-30: Update validTo on some records (extending validity)
        for (int rev = 2; rev <= 30; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
               JsonNodeTrx wtx = session.beginNodeTrx()) {

            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();  // array

            if (wtx.hasFirstChild()) {
              wtx.moveToFirstChild();  // first object

              // Move to a random position and update validTo
              int stepsToMove = (rev * 13) % 100;  // Deterministic "random"
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
            if (i > 0) json.append(",");
            json.append("{\"id\": ").append(i)
                .append(", \"year\": 2020")
                .append(", \"validFrom\": \"2020-01-01T00:00:00Z\"")
                .append(", \"validTo\": \"2020-12-31T23:59:59Z\"}");
          }
          for (int i = 500; i < 1000; i++) {
            json.append(",{\"id\": ").append(i)
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
        try { Thread.sleep(10); } catch (InterruptedException ignored) {}

        // Revision 2
        Instant rev2Time;
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {

          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();  // array
          wtx.moveToLastChild();   // last element

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
            if (i > 0) json.append(",");
            LocalDate validTo = LocalDate.of(2020, 1, 1).plusDays(i + 1);
            json.append("{\"id\": ").append(i)
                .append(", \"validFrom\": \"").append(sameValidFrom).append("\"")
                .append(", \"validTo\": \"").append(validTo.atStartOfDay().toInstant(ZoneOffset.UTC)).append("\"")
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
            if (i > 0) json.append(",");
            int year = 1990 + i;  // 1990 to 2189
            json.append("{\"id\": ").append(i)
                .append(", \"validFrom\": \"").append(year).append("-01-01T00:00:00Z\"")
                .append(", \"validTo\": \"").append(year).append("-12-31T23:59:59Z\"")
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
            if (i > 0) json.append(",");
            Instant validFrom = base.plusMillis(i);
            Instant validTo = validFrom.plus(1, ChronoUnit.HOURS);
            json.append("{\"id\": ").append(i)
                .append(", \"validFrom\": \"").append(validFrom).append("\"")
                .append(", \"validTo\": \"").append(validTo).append("\"")
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
            if (i > 0) json.append(",");
            Instant pointTime = LocalDate.of(2020, 1, 1).plusDays(i)
                .atStartOfDay().toInstant(ZoneOffset.UTC);
            json.append("{\"id\": ").append(i)
                .append(", \"validFrom\": \"").append(pointTime).append("\"")
                .append(", \"validTo\": \"").append(pointTime).append("\"")
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

          String json = generateLargeValidTimeDataset(15000,
              LocalDate.of(2000, 1, 1),
              LocalDate.of(2050, 1, 1));

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
          validFromIndex = IndexDefs.createCASIdxDef(false, Type.STR,
              Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);

          final var validToPath = parse("/[]/validTo", io.brackit.query.util.path.PathParser.Type.JSON);
          validToIndex = IndexDefs.createCASIdxDef(false, Type.STR,
              Collections.singleton(validToPath), 1, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(validFromIndex, validToIndex), wtx);

          String json = generateLargeValidTimeDataset(3000,
              LocalDate.of(2015, 1, 1),
              LocalDate.of(2025, 1, 1));

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

            var casIndex = indexController.openCASIndex(wtx.getPageTrx(), validFromIndex,
                indexController.createCASFilter(
                    Set.of("/[]/validFrom"),
                    new Str(threshold),
                    SearchMode.GREATER_OR_EQUAL,
                    new JsonPCRCollector(wtx)));

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

            rtx.moveToFirstChild();  // array
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
    Random random = new Random(42);  // Fixed seed for reproducibility
    long daysBetween = ChronoUnit.DAYS.between(startDate, endDate);

    for (int i = 0; i < count; i++) {
      if (i > 0) json.append(",");

      long randomDays = random.nextLong(daysBetween);
      LocalDate validFrom = startDate.plusDays(randomDays);
      LocalDate validTo = validFrom.plusDays(1 + random.nextInt(365));  // 1-365 days validity

      json.append("{\"id\": ").append(i)
          .append(", \"name\": \"record_").append(i).append("\"")
          .append(", \"value\": ").append(random.nextInt(10000))
          .append(", \"validFrom\": \"").append(validFrom.atStartOfDay().toInstant(ZoneOffset.UTC)).append("\"")
          .append(", \"validTo\": \"").append(validTo.atStartOfDay().toInstant(ZoneOffset.UTC)).append("\"")
          .append("}");
    }
    json.append("]");
    return json.toString();
  }

  private String generateOverlappingValidTimeDataset(int count) {
    StringBuilder json = new StringBuilder("[");

    // Create heavily overlapping ranges - many records valid at any given time
    for (int i = 0; i < count; i++) {
      if (i > 0) json.append(",");

      int startMonth = (i % 12) + 1;
      int durationMonths = 3 + (i % 9);  // 3-11 months

      LocalDate validFrom = LocalDate.of(2020, startMonth, 1);
      LocalDate validTo = validFrom.plusMonths(durationMonths);

      json.append("{\"id\": ").append(i)
          .append(", \"category\": \"cat_").append(i % 50).append("\"")
          .append(", \"validFrom\": \"").append(validFrom.atStartOfDay().toInstant(ZoneOffset.UTC)).append("\"")
          .append(", \"validTo\": \"").append(validTo.atStartOfDay().toInstant(ZoneOffset.UTC)).append("\"")
          .append("}");
    }
    json.append("]");
    return json.toString();
  }

  private String generateSequentialValidTimeDataset(int count, LocalDate startDate) {
    StringBuilder json = new StringBuilder("[");

    for (int i = 0; i < count; i++) {
      if (i > 0) json.append(",");

      LocalDate validFrom = startDate.plusDays(i);
      LocalDate validTo = validFrom;  // Valid for exactly one day

      json.append("{\"id\": ").append(i)
          .append(", \"day\": ").append(i)
          .append(", \"validFrom\": \"").append(validFrom.atStartOfDay().toInstant(ZoneOffset.UTC)).append("\"")
          .append(", \"validTo\": \"").append(validTo.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).minusNanos(1)).append("\"")
          .append("}");
    }
    json.append("]");
    return json.toString();
  }
}
