/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */
package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential correctness gate for the CAS-index-accelerated valid-time functions
 * ({@code jn:valid-at} / {@code jn:open-bitemporal}).
 *
 * <p>
 * For a dataset of records with {@code validFrom}/{@code validTo} dateTime fields and CAS indexes on
 * those paths, for many {@code validTime} values (including every boundary case), this asserts that:
 * </p>
 * <ol>
 * <li>the index-accelerated path ({@link ValidTimeIndexScan#tryIndexScan}, which we assert is
 * actually taken) returns exactly the brute-force reference set;</li>
 * <li>the {@code jn:valid-at} query over an <em>indexed</em> resource returns exactly that set;</li>
 * <li>the {@code jn:valid-at} query over a <em>plain</em> resource (no index → linear-scan
 * fallback) returns exactly that set.</li>
 * </ol>
 * <p>
 * All three must agree, for every {@code t}, by id-set equality.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
@DisplayName("Valid-Time Index Scan Differential Test")
public final class ValidTimeIndexScanDifferentialTest {

  private static final Path sirixPath = PATHS.PATH1.getFile();
  private static final String DB_NAME = "valid-time-diff-db";
  private static final String INDEXED_RESOURCE = "indexed";
  private static final String PLAIN_RESOURCE = "plain";

  private static final String VALID_FROM = "validFrom";
  private static final String VALID_TO = "validTo";

  /** A record in the dataset (the brute-force oracle's source of truth). */
  private record Record(int id, Instant validFrom, Instant validTo) {
    boolean validAt(final Instant t) {
      return !t.isBefore(validFrom) && !t.isAfter(validTo);
    }
  }

  private List<Record> records;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  @DisplayName("index path == scan == brute force for all t (incl. boundaries)")
  void indexEqualsScanEqualsBruteForceForAllT() throws IOException {
    records = buildDataset();
    final String json = toJson(records);

    final var dbPath = sirixPath.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      // Resource WITH CAS indexes on the valid-time paths.
      createResourceWithValidTime(database, INDEXED_RESOURCE);
      try (JsonResourceSession session = database.beginResourceSession(INDEXED_RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

        final var validFromPath = parse("/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
        final var validFromIndex =
            IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);
        final var validToPath = parse("/[]/validTo", io.brackit.query.util.path.PathParser.Type.JSON);
        final var validToIndex =
            IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validToPath), 1, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(validFromIndex, validToIndex), wtx);
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();

        assertTrue(indexController.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS) >= 2,
            "Should have 2 CAS indexes for the valid-time fields");
      }

      // Resource WITHOUT any index (forces the linear-scan fallback).
      createResourceWithValidTime(database, PLAIN_RESOURCE);
      try (JsonResourceSession session = database.beginResourceSession(PLAIN_RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    final List<Instant> testTimes = buildTestTimes(records);

    int boundaryEqualsFrom = 0;
    int boundaryEqualsTo = 0;
    int zeroMatchTimes = 0;
    int allMatchTimes = 0;
    int indexPathTakenCount = 0;

    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB_NAME);

      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {

        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB_NAME);
        final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);

        for (final Instant t : testTimes) {
          // (0) brute-force oracle
          final Set<Integer> brute = new TreeSet<>();
          for (final Record r : records) {
            if (r.validAt(t)) {
              brute.add(r.id());
            }
          }
          if (brute.isEmpty()) {
            zeroMatchTimes++;
          }
          if (brute.size() == records.size()) {
            allMatchTimes++;
          }
          for (final Record r : records) {
            if (t.equals(r.validFrom())) {
              boundaryEqualsFrom++;
              break;
            }
          }
          for (final Record r : records) {
            if (t.equals(r.validTo())) {
              boundaryEqualsTo++;
              break;
            }
          }

          // (1) DIRECT index path — assert it is actually taken, then compare its result set.
          final JsonDBItem indexedDoc = collection.getDocument(INDEXED_RESOURCE);
          final ValidTimeIndexScan.Result indexScan =
              ValidTimeIndexScan.tryIndexScan(indexedDoc, t, validTimeConfig);
          assertNotNull(indexScan,
              "Index path must be taken on the indexed resource (a CAS index exists) at t=" + t);
          indexPathTakenCount++;
          final Set<Integer> directIndexIds = idsOfItems(indexScan.items());
          assertEquals(brute, directIndexIds,
              "Direct index-scan result must equal brute force at t=" + t + " (field used: "
                  + indexScan.indexedField() + ", candidates examined: " + indexScan.candidatesExamined() + ")");

          // (2) jn:valid-at over the INDEXED resource (fast path through execute()).
          final Set<Integer> indexedQueryIds = idsFromValidAtQuery(chain, ctx, INDEXED_RESOURCE, t);
          assertEquals(brute, indexedQueryIds, "jn:valid-at over indexed resource must equal brute force at t=" + t);

          // (3) jn:valid-at over the PLAIN resource (linear-scan fallback).
          final Set<Integer> plainQueryIds = idsFromValidAtQuery(chain, ctx, PLAIN_RESOURCE, t);
          assertEquals(brute, plainQueryIds, "jn:valid-at over plain resource (scan) must equal brute force at t=" + t);
        }

        // Sanity: the plain resource really has NO usable index (the fallback path is exercised).
        final JsonDBItem plainDoc = collection.getDocument(PLAIN_RESOURCE);
        assertNull(ValidTimeIndexScan.tryIndexScan(plainDoc, testTimes.get(0), validTimeConfig),
            "Plain resource must have no usable CAS index (linear-scan fallback)");
      }
    }

    // Make sure the curated boundary cases were actually present in the run (901 times over 178
    // records as constructed: ~172 exactly equal a validFrom, ~136 exactly equal a validTo).
    assertTrue(boundaryEqualsFrom > 0, "Expected at least one t exactly equal to a validFrom");
    assertTrue(boundaryEqualsTo > 0, "Expected at least one t exactly equal to a validTo");
    assertTrue(zeroMatchTimes > 0, "Expected at least one t where zero records match");
    assertTrue(allMatchTimes > 0, "Expected at least one t where all records match");
    assertEquals(testTimes.size(), indexPathTakenCount, "Index path must have been taken for every t");
  }

  @Test
  @DisplayName("validFrom-only index path (safeUpperSecondBound) == brute force for all t")
  void validFromOnlyIndexEqualsBruteForce() throws IOException {
    records = buildDataset();
    final String json = toJson(records);

    final var dbPath = sirixPath.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      createResourceWithValidTime(database, INDEXED_RESOURCE);
      try (JsonResourceSession session = database.beginResourceSession(INDEXED_RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
        // ONLY a validFrom CAS index — forces the validFrom branch (range validFrom <= t).
        final var validFromPath = parse("/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
        final var validFromIndex =
            IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(validFromIndex), wtx);
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    final List<Instant> testTimes = buildTestTimes(records);

    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB_NAME);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {

        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB_NAME);
        final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);

        for (final Instant t : testTimes) {
          final Set<Integer> brute = new TreeSet<>();
          for (final Record r : records) {
            if (r.validAt(t)) {
              brute.add(r.id());
            }
          }

          final JsonDBItem doc = collection.getDocument(INDEXED_RESOURCE);
          final ValidTimeIndexScan.Result indexScan = ValidTimeIndexScan.tryIndexScan(doc, t, validTimeConfig);
          assertNotNull(indexScan, "Index path must be taken (validFrom index exists) at t=" + t);
          assertEquals(ValidTimeIndexScan.ValidField.VALID_FROM, indexScan.indexedField(),
              "Only the validFrom index exists, so it must be the one used at t=" + t);
          assertEquals(brute, idsOfItems(indexScan.items()),
              "validFrom-only index path must equal brute force at t=" + t);

          // And the function as a whole (through execute()).
          assertEquals(brute, idsFromValidAtQuery(chain, ctx, INDEXED_RESOURCE, t),
              "jn:valid-at (validFrom-only index) must equal brute force at t=" + t);
        }
      }
    }
  }

  @Test
  @DisplayName("jn:open-bitemporal index path == brute force for all t (incl. boundaries)")
  void openBitemporalIndexEqualsBruteForce() throws IOException {
    records = buildDataset();
    final String json = toJson(records);

    final var dbPath = sirixPath.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    Instant txTime;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      createResourceWithValidTime(database, INDEXED_RESOURCE);
      try (JsonResourceSession session = database.beginResourceSession(INDEXED_RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
        final var validFromPath = parse("/[]/validFrom", io.brackit.query.util.path.PathParser.Type.JSON);
        final var validFromIndex =
            IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validFromPath), 0, IndexDef.DbType.JSON);
        final var validToPath = parse("/[]/validTo", io.brackit.query.util.path.PathParser.Type.JSON);
        final var validToIndex =
            IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(validToPath), 1, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(validFromIndex, validToIndex), wtx);
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
        txTime = wtx.getRevisionTimestamp();
      }
    }

    final List<Instant> testTimes = buildTestTimes(records);
    final String txTimeStr = txTime.plus(1, ChronoUnit.SECONDS).toString();

    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB_NAME);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {

        for (final Instant t : testTimes) {
          final Set<Integer> brute = new TreeSet<>();
          for (final Record r : records) {
            if (r.validAt(t)) {
              brute.add(r.id());
            }
          }

          final String query = "jn:open-bitemporal('" + DB_NAME + "', '" + INDEXED_RESOURCE + "', xs:dateTime('"
              + txTimeStr + "'), xs:dateTime('" + t + "'))";
          final Set<Integer> got = idsFromObjectQuery(chain, ctx, query);
          assertEquals(brute, got, "jn:open-bitemporal (indexed) must equal brute force at t=" + t);
        }
      }
    }
  }

  // ---- helpers -------------------------------------------------------------------------------

  private static void createResourceWithValidTime(final Database<JsonResourceSession> database, final String name) {
    final var resourceConfig = ResourceConfiguration.newBuilder(name)
                                                    .validTimePaths(VALID_FROM, VALID_TO)
                                                    .buildPathSummary(true)
                                                    .build();
    database.createResource(resourceConfig);
  }

  /** An instant every record's interval contains, so {@code t == UNIVERSAL} matches them all. */
  private static final Instant UNIVERSAL = Instant.parse("2020-06-01T12:00:00Z");

  /**
   * Builds a varied dataset: random overlapping intervals, some open-ended (far-future validTo),
   * some with millisecond fractions, plus point-in-time records — every interval is constructed to
   * contain {@link #UNIVERSAL} so there is a guaranteed "all match" time, while {@code validFrom}
   * still spans ~2.4 years and {@code validTo} varies widely (incl. open-ended).
   */
  private static List<Record> buildDataset() {
    final List<Record> recs = new ArrayList<>();
    final Random rnd = new Random(20240607L);
    final Instant base = Instant.parse("2018-01-01T00:00:00Z");

    int id = 0;

    // 160 intervals: validFrom in [2018-01-01, UNIVERSAL); ~1 in 6 open-ended, the rest end at a
    // varied point that is always >= UNIVERSAL (so they all contain UNIVERSAL but still vary).
    final long maxFromOffsetDays = ChronoUnit.DAYS.between(base, UNIVERSAL); // ~883 days
    for (int i = 0; i < 160; i++) {
      final Instant from = base.plus(rnd.nextInt((int) maxFromOffsetDays), ChronoUnit.DAYS)
                               .plusSeconds(rnd.nextInt(86_400));
      final Instant to;
      if (i % 6 == 0) {
        to = Instant.parse("2999-12-31T23:59:59Z"); // open-ended
      } else {
        // End between 1 and ~900 days AFTER UNIVERSAL -> always contains UNIVERSAL.
        to = UNIVERSAL.plus(1 + rnd.nextInt(900), ChronoUnit.DAYS).plusSeconds(rnd.nextInt(86_400));
      }
      recs.add(new Record(id++, from, to));
    }

    // Sub-second (millisecond) precision spanning UNIVERSAL, to exercise the whole-second bound +
    // read-verification.
    for (int i = 0; i < 12; i++) {
      final Instant from = UNIVERSAL.minusMillis(i); // i ms before .. at UNIVERSAL
      final Instant to = UNIVERSAL.plus(2, ChronoUnit.HOURS).plusMillis(500);
      recs.add(new Record(id++, from, to));
    }

    // Point-in-time validity (validFrom == validTo == UNIVERSAL): matches only exactly at UNIVERSAL
    // (boundary equality on BOTH ends), and keeps the "all match" guarantee intact.
    for (int i = 0; i < 6; i++) {
      recs.add(new Record(id++, UNIVERSAL, UNIVERSAL));
    }

    return recs;
  }

  private static String toJson(final List<Record> recs) {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < recs.size(); i++) {
      final Record r = recs.get(i);
      if (i > 0) {
        sb.append(",");
      }
      sb.append("{\"id\": ")
        .append(r.id())
        .append(", \"validFrom\": \"")
        .append(r.validFrom())
        .append("\", \"validTo\": \"")
        .append(r.validTo())
        .append("\"}");
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Curated test times: before all, after all, every record's exact validFrom and validTo (boundary
   * equality on both ends), points just inside/outside those boundaries, fractional-second points,
   * a guaranteed zero-match time, and a guaranteed all-match time.
   */
  private static List<Instant> buildTestTimes(final List<Record> recs) {
    final Set<Instant> times = new LinkedHashSet<>();

    times.add(Instant.parse("1900-01-01T00:00:00Z")); // before all -> zero match
    times.add(Instant.parse("2998-01-01T00:00:00Z")); // after all closed intervals
    times.add(UNIVERSAL);                              // inside every interval -> all match

    for (final Record r : recs) {
      times.add(r.validFrom());                                  // == validFrom (boundary)
      times.add(r.validTo());                                    // == validTo (boundary)
      times.add(r.validFrom().minusMillis(1));                   // just before from
      times.add(r.validFrom().plusMillis(1));                    // just after from
      times.add(r.validTo().minusMillis(1));                     // just before to
      times.add(r.validTo().plusMillis(1));                      // just after to
    }

    return new ArrayList<>(times);
  }

  private static Set<Integer> idsFromValidAtQuery(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String resource, final Instant t) {
    final String query = "jn:valid-at('" + DB_NAME + "', '" + resource + "', xs:dateTime('" + t + "'))";
    return idsFromObjectQuery(chain, ctx, query);
  }

  /** Evaluate a query that yields a sequence of objects and collect each object's {@code id}. */
  private static Set<Integer> idsFromObjectQuery(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String query) {
    final Sequence result = new Query(chain, query).evaluate(ctx);
    final Set<Integer> ids = new TreeSet<>();
    if (result == null) {
      return ids;
    }
    final Iter iter = result.iterate();
    try {
      Item item;
      while ((item = iter.next()) != null) {
        final io.brackit.query.jdm.json.Object obj = (io.brackit.query.jdm.json.Object) item;
        ids.add(((Numeric) obj.get(new io.brackit.query.atomic.QNm("id"))).intValue());
      }
    } finally {
      iter.close();
    }
    return ids;
  }

  private static Set<Integer> idsOfItems(final List<JsonDBItem> items) {
    final Set<Integer> ids = new TreeSet<>();
    for (final JsonDBItem item : items) {
      final io.brackit.query.jdm.json.Object obj = (io.brackit.query.jdm.json.Object) item;
      final Sequence idSeq = obj.get(new io.brackit.query.atomic.QNm("id"));
      ids.add(((Numeric) idSeq).intValue());
    }
    return ids;
  }
}
