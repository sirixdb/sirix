package io.sirix.query.function.jn.docs;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests validating the JSONiq queries shown on the
 * <a href="https://sirix.io/docs/use-cases.html">sirix.io use-cases page</a>.
 *
 * <p>These tests ensure that every code block on the page compiles, executes, and returns
 * meaningful results against real bitemporal data.</p>
 */
public final class UseCasesDocQueryTest {

  private static final Path SIRIX_DB_PATH = PATHS.PATH1.getFile();

  /**
   * Risk exposure data with valid time intervals covering different periods.
   * <ul>
   * <li>ACME: valid Feb 2025 only</li>
   * <li>Globex: valid March 2025 only</li>
   * <li>Initech: valid all of 2025</li>
   * </ul>
   */
  private static final String EXPOSURE_DATA = """
      [
        {
          "id": 1,
          "entity": "ACME Corp",
          "exposure": 5000000,
          "validFrom": "2025-02-01T00:00:00Z",
          "validTo": "2025-02-28T23:59:59Z"
        },
        {
          "id": 2,
          "entity": "Globex Inc",
          "exposure": 3000000,
          "validFrom": "2025-03-01T00:00:00Z",
          "validTo": "2025-03-31T23:59:59Z"
        },
        {
          "id": 3,
          "entity": "Initech LLC",
          "exposure": 8000000,
          "validFrom": "2025-01-01T00:00:00Z",
          "validTo": "2025-12-31T23:59:59Z"
        }
      ]
      """;

  /**
   * Transaction data for fraud detection. TX-001 and TX-002 have validFrom far in the past
   * (clearly backdated relative to any realistic commit time). TX-003 has a validFrom within
   * 1 day of the current date so the gap is always &lt; 7 days.
   */
  private static final String TRANSACTION_DATA = """
      [
        {
          "txId": "TX-001",
          "amount": 50000,
          "account": "ACC-123",
          "validFrom": "2024-01-15T00:00:00Z",
          "validTo": "2099-12-31T23:59:59Z"
        },
        {
          "txId": "TX-002",
          "amount": 75000,
          "account": "ACC-456",
          "validFrom": "2024-06-01T00:00:00Z",
          "validTo": "2099-12-31T23:59:59Z"
        },
        {
          "txId": "TX-003",
          "amount": 10000,
          "account": "ACC-789",
          "validFrom": "%s",
          "validTo": "2099-12-31T23:59:59Z"
        }
      ]
      """.formatted(java.time.Instant.now().toString());

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  // ========================= Query 1: jn:open-bitemporal =========================

  @Test
  @DisplayName("Use-case query: jn:open-bitemporal for regulatory audit trails")
  void testOpenBitemporalQuery() {
    final var dbPath = SIRIX_DB_PATH.resolve("risk-db");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      final var resourceConfig = ResourceConfiguration.newBuilder("exposures")
          .validTimePaths("validFrom", "validTo")
          .buildPathSummary(true)
          .build();
      db.createResource(resourceConfig);

      try (final JsonResourceSession session = db.beginResourceSession("exposures");
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(EXPOSURE_DATA));
        wtx.commit();
      }
    }

    // Execute the exact query pattern from use-cases.md:
    //   jn:open-bitemporal($coll, $res, $transactionTime, $validTime)
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH).build()) {
      store.lookup("risk-db");

      try (final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Far-future transaction time → picks the latest revision.
        // Valid time = March 15 → filters to records valid on that date.
        final var query = """
            jn:open-bitemporal('risk-db', 'exposures',
              xs:dateTime('2099-12-31T23:59:59Z'),
              xs:dateTime('2025-03-15T00:00:00Z'))""";

        final var result = serializeQuery(chain, ctx, query);
        assertNotNull(result, "Query result should not be null");
        assertFalse(result.isEmpty(), "Query result should not be empty");

        // Globex (valid March 1–31) → valid on March 15 ✓
        assertTrue(result.contains("Globex"), "Should contain Globex (valid March 1-31): " + result);
        // Initech (valid Jan 1 – Dec 31) → valid on March 15 ✓
        assertTrue(result.contains("Initech"), "Should contain Initech (valid all 2025): " + result);
        // ACME (valid Feb 1–28) → NOT valid on March 15 ✗
        assertFalse(result.contains("ACME"), "Should not contain ACME (expired Feb 28): " + result);

        System.out.println("open-bitemporal result: " + result);
      }
    }
  }

  // ========================= Query 2: Fraud Detection =========================

  @Test
  @DisplayName("Use-case query: fraud detection via datetime subtraction")
  void testFraudDetectionQuery() {
    final var dbPath = SIRIX_DB_PATH.resolve("ledger");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      final var resourceConfig = ResourceConfiguration.newBuilder("transactions")
          .validTimePaths("validFrom", "validTo")
          .buildPathSummary(true)
          .build();
      db.createResource(resourceConfig);

      try (final JsonResourceSession session = db.beginResourceSession("transactions");
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(TRANSACTION_DATA));
        wtx.commit();
      }
    }

    // Execute the exact query pattern from use-cases.md
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH).build()) {
      store.lookup("ledger");

      try (final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Query from use-cases.md: find records backdated > 7 days
        // Includes the sdb:timestamp gt sdb:valid-from guard to exclude future-dated records
        final var query = """
            for $rev in jn:all-times(jn:doc('ledger', 'transactions'))
            for $r in $rev[]
            where sdb:timestamp($rev) gt sdb:valid-from($r)
              and sdb:timestamp($rev) - sdb:valid-from($r)
                gt xs:dayTimeDuration('P7D')
            return $r""";

        final var result = serializeQuery(chain, ctx, query);
        assertNotNull(result, "Query result should not be null");
        assertFalse(result.isEmpty(), "Query result should not be empty");

        // TX-001 (validFrom = Jan 2024): commit time - Jan 2024 ≫ 7 days → flagged
        assertTrue(result.contains("TX-001"), "Should detect TX-001 as backdated: " + result);
        // TX-002 (validFrom = Jun 2024): commit time - Jun 2024 ≫ 7 days → flagged
        assertTrue(result.contains("TX-002"), "Should detect TX-002 as backdated: " + result);
        // TX-003 (validFrom ≈ now): gap is < 7 days → not flagged
        assertFalse(result.contains("TX-003"),
            "Should not flag TX-003 (validFrom is recent): " + result);

        System.out.println("fraud detection result: " + result);
      }
    }
  }

  // ========================= DateTime Arithmetic =========================

  @Test
  @DisplayName("xs:dateTime subtraction produces xs:dayTimeDuration comparable with gt")
  void testDatetimeSubtractionArithmetic() {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      // 14 days > 7 days → true
      final var queryTrue = """
          xs:dateTime('2025-03-15T00:00:00Z') - xs:dateTime('2025-03-01T00:00:00Z')
            gt xs:dayTimeDuration('P7D')""";
      assertEquals("true", serializeQuery(chain, ctx, queryTrue).trim());

      // 3 days > 7 days → false
      final var queryFalse = """
          xs:dateTime('2025-03-04T00:00:00Z') - xs:dateTime('2025-03-01T00:00:00Z')
            gt xs:dayTimeDuration('P7D')""";
      assertEquals("false", serializeQuery(chain, ctx, queryFalse).trim());

      // Exactly 7 days > 7 days → false (not strictly greater)
      final var queryExact = """
          xs:dateTime('2025-03-08T00:00:00Z') - xs:dateTime('2025-03-01T00:00:00Z')
            gt xs:dayTimeDuration('P7D')""";
      assertEquals("false", serializeQuery(chain, ctx, queryExact).trim());
    }
  }

  private String serializeQuery(SirixCompileChain chain, SirixQueryContext ctx, String query) {
    try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, printWriter);
      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("Query serialization failed: " + query, e);
    }
  }
}
