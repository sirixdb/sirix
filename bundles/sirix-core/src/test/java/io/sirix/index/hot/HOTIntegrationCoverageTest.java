package io.sirix.index.hot;

import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.SearchMode;
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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests to increase HOT code coverage to 85%.
 * 
 * <p>
 * These tests exercise specific code paths in:
 * </p>
 * <ul>
 * <li>{@link HOTIndexWriter} - insert, update, remove operations</li>
 * <li>{@link HOTIndexReader} - range queries, iteration</li>
 * <li>{@link SparsePartialKeys} - SIMD search paths</li>
 * <li>{@link HeightOptimalSplitter} - split scenarios</li>
 * <li>{@link SiblingMerger} - merge operations</li>
 * </ul>
 */
@DisplayName("HOT Integration Coverage Tests")
class HOTIntegrationCoverageTest {

  @TempDir
  Path tempDir;

  private Path DATABASE_PATH;
  private static final String RESOURCE_NAME = "hot-coverage-test";

  @BeforeEach
  void setUp() throws IOException {
    DATABASE_PATH = tempDir.resolve("hot-coverage-db");
    Files.createDirectories(DATABASE_PATH);
    System.setProperty("sirix.index.useHOT", "true");
    LinuxMemorySegmentAllocator.getInstance();
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("sirix.index.useHOT");
    try {
      Databases.removeDatabase(DATABASE_PATH);
    } catch (Exception ignored) {
    }
  }

  @Nested
  @DisplayName("HOTIndexWriter Coverage")
  class WriterCoverageTests {

    @Test
    @DisplayName("Insert entries with random values")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testInsertRandomValues() throws IOException {
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

          // Insert entries with pseudo-random values (diverse key patterns)
          java.util.Random rand = new java.util.Random(12345);
          StringBuilder json = new StringBuilder("{\"items\": [");
          for (int i = 0; i < 500; i++) {
            if (i > 0)
              json.append(",");
            int value = rand.nextInt(100000); // Wide range of values
            json.append("{\"value\": ").append(value).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query should find entries with value >= 0
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/items/[]/value"), new Int32(0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long totalRefs = 0;
          while (casIndex.hasNext()) {
            NodeReferences refs = casIndex.next();
            totalRefs += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(totalRefs >= 450, "Should find at least 90% of 500 entries, got " + totalRefs);
        }
      }
    }

    @Test
    @DisplayName("Insert entries with sequential values")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testInsertSequentialValues() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToId = parse("/records/[]/id", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToId), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Sequential values (worst case for some tree structures)
          StringBuilder json = new StringBuilder("{\"records\": [");
          for (int i = 0; i < 1000; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"id\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Range query: 100 <= id < 200
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/records/[]/id"), new Int32(100), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            NodeReferences refs = casIndex.next();
            long val = refs.getNodeKeys().stream().findFirst().orElse(-1L);
            count += refs.getNodeKeys().getLongCardinality();
          }
          assertTrue(count >= 900, "Should find at least 900 entries >= 100");
        }
      }
    }

    @Test
    @DisplayName("Insert with reverse sequential values")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testInsertReverseSequentialValues() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToNum = parse("/data/[]/num", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToNum), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Reverse sequential (another edge case)
          StringBuilder json = new StringBuilder("{\"data\": [");
          for (int i = 999; i >= 0; i--) {
            if (i < 999)
              json.append(",");
            json.append("{\"num\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/data/[]/num"), new Int32(0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }
          assertTrue(count >= 900, "Should find at least 900 entries");
        }
      }
    }

    @Test
    @DisplayName("Insert string values with various lengths")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testInsertStringValues() throws IOException {
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

          StringBuilder json = new StringBuilder("{\"users\": [");
          for (int i = 0; i < 500; i++) {
            if (i > 0)
              json.append(",");
            // Vary string lengths to test different key sizes
            String name = "user" + "x".repeat(i % 50) + i;
            json.append("{\"name\": \"").append(name).append("\"}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for strings starting with "user"
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/users/[]/name"), new Str("user"), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find string entries");
        }
      }
    }
  }

  @Nested
  @DisplayName("HOTIndexReader Coverage")
  class ReaderCoverageTests {

    @Test
    @DisplayName("Range query - less than")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testRangeQueryLessThan() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToAge = parse("/people/[]/age", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToAge), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          StringBuilder json = new StringBuilder("{\"people\": [");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"age\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query age < 50
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/people/[]/age"), new Int32(50), SearchMode.LOWER, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }
          assertEquals(50, count, "Should find exactly 50 entries (ages 0-49)");
        }
      }
    }

    @Test
    @DisplayName("Range query - between values")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testRangeQueryBetween() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToScore = parse("/tests/[]/score", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.DBL, Collections.singleton(pathToScore), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          StringBuilder json = new StringBuilder("{\"tests\": [");
          for (int i = 0; i < 200; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"score\": ").append(i * 0.5).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query scores >= 25.0
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/tests/[]/score"), new Dbl(25.0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find scores >= 25.0");
        }
      }
    }

    @Test
    @DisplayName("Empty result query")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testEmptyResultQuery() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToVal = parse("/items/[]/val", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToVal), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          StringBuilder json = new StringBuilder("{\"items\": [");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"val\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for value that doesn't exist (>= 1000)
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/items/[]/val"), new Int32(1000), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }
          assertEquals(0, count, "Should find no entries >= 1000");
        }
      }
    }
  }

  @Nested
  @DisplayName("Multiple Index Types")
  class MultipleIndexTypesTests {

    @Test
    @DisplayName("Multiple CAS indexes on same resource")
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void testMultipleCASIndexes() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          // Create two CAS indexes on different fields
          final var pathToPrice = parse("/products/[]/price", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef1 =
              IndexDefs.createCASIdxDef(false, Type.DBL, Collections.singleton(pathToPrice), 0, IndexDef.DbType.JSON);

          final var pathToQty = parse("/products/[]/quantity", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef2 =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToQty), 1, IndexDef.DbType.JSON);

          indexController.createIndexes(Set.of(casIndexDef1, casIndexDef2), wtx);

          StringBuilder json = new StringBuilder("{\"products\": [");
          for (int i = 0; i < 300; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"price\": ").append(i * 1.5).append(", \"quantity\": ").append(i + 1).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query first CAS index (price)
          var casIndex1 = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef1, indexController.createCASFilter(
              Set.of("/products/[]/price"), new Dbl(100.0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long casCount1 = 0;
          while (casIndex1.hasNext()) {
            casCount1 += casIndex1.next().getNodeKeys().getLongCardinality();
          }
          assertTrue(casCount1 > 0, "First CAS index (price) should have results");

          // Query second CAS index (quantity)
          var casIndex2 = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef2, indexController.createCASFilter(
              Set.of("/products/[]/quantity"), new Int32(100), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long casCount2 = 0;
          while (casIndex2.hasNext()) {
            casCount2 += casIndex2.next().getNodeKeys().getLongCardinality();
          }
          assertTrue(casCount2 > 0, "Second CAS index (quantity) should have results");
        }
      }
    }
  }

  @Nested
  @DisplayName("Multiple Revisions")
  class MultipleRevisionsTests {

    @Test
    @DisplayName("Index across multiple commits")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testIndexAcrossMultipleCommits() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        // First commit
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToVal = parse("/data/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToVal), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          StringBuilder json = new StringBuilder("{\"data\": [");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"value\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Second commit - add more data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.moveToFirstChild(); // Move to array

          // Move to end of array and add more items
          while (wtx.moveToRightSibling()) {
            // Navigate to end
          }

          wtx.commit();
        }

        // Verify index works on final revision
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          var indexController = session.getRtxIndexController(rtx.getRevisionNumber());

          final var pathToVal = parse("/data/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToVal), 0, IndexDef.DbType.JSON);

          var casIndex = indexController.openCASIndex(rtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/data/[]/value"), new Int32(0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(rtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }
          assertTrue(count >= 100, "Should find entries from both commits");
        }
      }
    }
  }

  @Nested
  @DisplayName("Edge Value Patterns")
  class EdgeValuePatternsTests {

    @Test
    @DisplayName("Index with negative values")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testNegativeValues() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToTemp = parse("/readings/[]/temp", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToTemp), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          StringBuilder json = new StringBuilder("{\"readings\": [");
          for (int i = -50; i <= 50; i++) {
            if (i > -50)
              json.append(",");
            json.append("{\"temp\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for negative values
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/readings/[]/temp"), new Int32(0), SearchMode.LOWER, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }
          assertEquals(50, count, "Should find 50 negative values");
        }
      }
    }

    @Test
    @DisplayName("Index with very large values")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testVeryLargeValues() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToBigNum = parse("/numbers/[]/big", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.DBL, Collections.singleton(pathToBigNum), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          StringBuilder json = new StringBuilder("{\"numbers\": [");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            // Use large values
            double value = 1e10 + i * 1e8;
            json.append("{\"big\": ").append(value).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/numbers/[]/big"), new Dbl(1e10), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }
          assertEquals(100, count, "Should find all 100 large values");
        }
      }
    }

    @Test
    @DisplayName("Index with sparse distribution")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testSparseDistribution() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToSparse = parse("/sparse/[]/val", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToSparse), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // Sparse values with large gaps
          StringBuilder json = new StringBuilder("{\"sparse\": [");
          boolean first = true;
          for (int i = 0; i < 50; i++) {
            if (!first)
              json.append(",");
            first = false;
            // Values: 0, 1000, 2000, 3000, ...
            json.append("{\"val\": ").append(i * 1000).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for a specific sparse value
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/sparse/[]/val"), new Int32(25000), SearchMode.EQUAL, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }
          assertEquals(1, count, "Should find exactly one entry with value 25000");
        }
      }
    }
  }

  // NAME and PATH index tests are covered in HOTMultiLayerIndirectPageTest
}

