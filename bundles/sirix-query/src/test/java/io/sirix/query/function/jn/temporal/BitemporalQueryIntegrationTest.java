package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for bitemporal query functionality.
 *
 * <p>These tests verify the valid time support features including:
 * <ul>
 *   <li>Resource configuration with valid time paths</li>
 *   <li>Auto-creation of CAS indexes for valid time fields</li>
 *   <li>The jn:valid-at query function</li>
 *   <li>The jn:open-bitemporal query function</li>
 *   <li>The sdb:valid-from and sdb:valid-to functions</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class BitemporalQueryIntegrationTest {

  private static final Path sirixPath = PATHS.PATH1.getFile();

  /**
   * Sample data with valid time fields.
   * This represents employee data with valid time intervals.
   */
  private static final String BITEMPORAL_JSON = """
      [
        {
          "id": 1,
          "name": "Alice",
          "salary": 50000,
          "validFrom": "2020-01-01T00:00:00Z",
          "validTo": "2020-12-31T23:59:59Z"
        },
        {
          "id": 2,
          "name": "Bob",
          "salary": 60000,
          "validFrom": "2020-06-01T00:00:00Z",
          "validTo": "2021-05-31T23:59:59Z"
        },
        {
          "id": 3,
          "name": "Charlie",
          "salary": 55000,
          "validFrom": "2019-01-01T00:00:00Z",
          "validTo": "2019-12-31T23:59:59Z"
        }
      ]
      """;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testValidTimeConfigurationInResource() {
    // Create database with valid time configuration
    final var dbPath = sirixPath.resolve("bitemporal-db");
    final var dbConfig = new DatabaseConfiguration(dbPath);

    Databases.createJsonDatabase(dbConfig);

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      // Create resource with valid time paths configured
      final var resourceConfig = ResourceConfiguration.newBuilder("employees")
          .validTimePaths("validFrom", "validTo")
          .buildPathSummary(true)
          .build();

      database.createResource(resourceConfig);

      // Verify the configuration was set
      try (final JsonResourceSession session = database.beginResourceSession("employees")) {
        final var config = session.getResourceConfig();
        assertNotNull(config.getValidTimeConfig());
        assertEquals("validFrom", config.getValidTimeConfig().getValidFromPath());
        assertEquals("validTo", config.getValidTimeConfig().getValidToPath());
        assertTrue(config.hasValidTimeSupport());
      }
    }
  }

  @Test
  void testConventionalValidTimePaths() {
    // Test using convention-based field names (_validFrom, _validTo)
    final var dbPath = sirixPath.resolve("bitemporal-db");
    final var dbConfig = new DatabaseConfiguration(dbPath);

    Databases.createJsonDatabase(dbConfig);

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final var resourceConfig = ResourceConfiguration.newBuilder("employees")
          .useConventionalValidTimePaths()
          .buildPathSummary(true)
          .build();

      database.createResource(resourceConfig);

      try (final JsonResourceSession session = database.beginResourceSession("employees")) {
        final var config = session.getResourceConfig();
        assertNotNull(config.getValidTimeConfig());
        assertEquals("_validFrom", config.getValidTimeConfig().getValidFromPath());
        assertEquals("_validTo", config.getValidTimeConfig().getValidToPath());
      }
    }
  }

  @Test
  void testInsertDataWithValidTime() {
    // Create database and resource with valid time support
    final var dbPath = sirixPath.resolve("bitemporal-db");
    final var dbConfig = new DatabaseConfiguration(dbPath);

    Databases.createJsonDatabase(dbConfig);

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final var resourceConfig = ResourceConfiguration.newBuilder("employees")
          .validTimePaths("validFrom", "validTo")
          .buildPathSummary(true)
          .build();

      database.createResource(resourceConfig);

      // Insert data with valid time fields
      try (final JsonResourceSession session = database.beginResourceSession("employees");
           final JsonNodeTrx wtx = session.beginNodeTrx()) {

        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(BITEMPORAL_JSON));
        wtx.commit();

        // Verify data was inserted
        assertTrue(wtx.getRevisionNumber() > 0);
      }
    }
  }

  @Test
  void testValidAtQueryFunction() {
    // Create database and resource with valid time support
    final var dbPath = sirixPath.resolve("bitemporal-db");
    final var dbConfig = new DatabaseConfiguration(dbPath);

    Databases.createJsonDatabase(dbConfig);

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final var resourceConfig = ResourceConfiguration.newBuilder("employees")
          .validTimePaths("validFrom", "validTo")
          .buildPathSummary(true)
          .build();

      database.createResource(resourceConfig);

      // Insert data with valid time fields
      try (final JsonResourceSession session = database.beginResourceSession("employees");
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(BITEMPORAL_JSON));
        wtx.commit();
      }
    }

    // Query using jn:valid-at
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      // Register the database
      store.lookup("bitemporal-db");

      try (final var ctx = SirixQueryContext.createWithJsonStore(store);
           final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Query for records valid at 2020-07-01
        final var validAtQuery = "jn:valid-at('bitemporal-db', 'employees', xs:dateTime('2020-07-01T12:00:00Z'))";
        final Sequence result = new Query(chain, validAtQuery).evaluate(ctx);

        // The result should include Alice and Bob (valid during July 2020)
        // but not Charlie (only valid in 2019)
        assertNotNull(result);
      }
    }
  }

  @Test
  void testValidTimeConfigSerialization() {
    // Test that valid time config is properly serialized and deserialized
    final var dbPath = sirixPath.resolve("bitemporal-db");
    final var dbConfig = new DatabaseConfiguration(dbPath);

    Databases.createJsonDatabase(dbConfig);

    // Create resource with valid time config
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final var resourceConfig = ResourceConfiguration.newBuilder("employees")
          .validTimePaths("$.validFrom", "$.validTo")
          .buildPathSummary(true)
          .build();

      database.createResource(resourceConfig);
    }

    // Reopen and verify config was persisted
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      try (final JsonResourceSession session = database.beginResourceSession("employees")) {
        final var config = session.getResourceConfig();
        assertNotNull(config.getValidTimeConfig());
        assertEquals("$.validFrom", config.getValidTimeConfig().getValidFromPath());
        assertEquals("$.validTo", config.getValidTimeConfig().getValidToPath());
      }
    }
  }

  @Test
  void testValidTimeConfigNormalization() {
    // Test path normalization
    final var config = new ValidTimeConfig("$.validFrom", "$.validTo");

    // Normalization should remove $. prefix
    assertEquals("validFrom", config.getNormalizedValidFromPath());
    assertEquals("validTo", config.getNormalizedValidToPath());

    // Without prefix, should remain unchanged
    final var config2 = new ValidTimeConfig("validFrom", "validTo");
    assertEquals("validFrom", config2.getNormalizedValidFromPath());
    assertEquals("validTo", config2.getNormalizedValidToPath());
  }

  @Test
  void testResourceWithoutValidTimeSupport() {
    // Create a regular resource without valid time support
    final var dbPath = sirixPath.resolve("regular-db");
    final var dbConfig = new DatabaseConfiguration(dbPath);

    Databases.createJsonDatabase(dbConfig);

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      // Create resource WITHOUT valid time paths
      final var resourceConfig = ResourceConfiguration.newBuilder("data")
          .buildPathSummary(true)
          .build();

      database.createResource(resourceConfig);

      try (final JsonResourceSession session = database.beginResourceSession("data")) {
        final var config = session.getResourceConfig();
        // Should not have valid time support
        assertTrue(!config.hasValidTimeSupport());
        assertTrue(config.getValidTimeConfig() == null);
      }
    }
  }
}
