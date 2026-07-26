/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * END-TO-END test for the valid-time interval index using the user-facing JSONiq function
 * {@code jn:create-valid-time-index}, under DEFAULT JVM settings (NO {@code -Dsirix.index.useHOT}).
 *
 * <p>The flow mirrors a real user: create a resource with valid-time config, shred data, create the
 * interval index from a query ({@code jn:create-valid-time-index($doc)} + {@code sdb:commit}), then
 * run {@code jn:valid-at} for many instants (including all boundary cases) and assert the results
 * equal a brute-force Java oracle — and that the interval-index fast path is actually taken.</p>
 *
 * @author Johannes Lichtenberger
 */
@DisplayName("Valid-Time Interval Index End-to-End Test (jn:create-valid-time-index, no -D flags)")
public final class ValidTimeIndexEndToEndTest {

  private static final Path sirixPath = PATHS.PATH1.getFile();
  private static final String DB_NAME = "valid-time-e2e-db";
  private static final String RESOURCE = "vt-resource";

  private static final String VALID_FROM = "validFrom";
  private static final String VALID_TO = "validTo";

  /**
   * Test times per store instance. Each query/getDocument pins a read-only trx (and its file
   * descriptors) until the store closes, so chunking bounds concurrent open trxes.
   */
  private static final int QUERY_CHUNK_SIZE = 100;

  private record Record(int id, Instant validFrom, Instant validTo) {
    boolean validAt(final Instant t) {
      return !t.isBefore(validFrom) && !t.isAfter(validTo);
    }
  }

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
  @DisplayName("create-valid-time-index from a query, then jn:valid-at == brute force for all t")
  void createIndexViaQueryThenValidAt() throws IOException {
    // Assert NO -Dsirix.index.useHOT is in effect — this test must pass with default settings.
    assertTrue(System.getProperty("sirix.index.useHOT") == null
            || "false".equalsIgnoreCase(System.getProperty("sirix.index.useHOT")),
        "This end-to-end test must run WITHOUT -Dsirix.index.useHOT (got: "
            + System.getProperty("sirix.index.useHOT") + ")");

    final List<Record> records = buildDataset();
    final String json = toJson(records);

    final var dbPath = sirixPath.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    // Create the resource WITH valid-time config + shred the data + commit (no index yet).
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE)
                                                      .validTimePaths(VALID_FROM, VALID_TO)
                                                      .buildPathSummary(true)
                                                      .build();
      database.createResource(resourceConfig);
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);
    final List<Instant> testTimes = buildTestTimes(records);

    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB_NAME);

      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {

        // 1) CREATE THE INDEX VIA A QUERY — exactly how a user would, then commit.
        final String createQuery =
            "let $doc := jn:doc('" + DB_NAME + "', '" + RESOURCE + "') "
                + "let $idx := jn:create-valid-time-index($doc) "
                + "return sdb:commit($doc)";
        final Sequence createResult = new Query(chain, createQuery).evaluate(ctx);
        assertNotNull(createResult, "jn:create-valid-time-index query must return a result");
      }
    }

    // 2) QUERY via jn:valid-at and assert == brute force, with a fresh store (cold caches) so the
    //    index is read from persisted pages.
    Databases.getGlobalBufferManager().clearAllCaches();

    int boundaryFrom = 0;
    int boundaryTo = 0;
    int zeroMatch = 0;
    int allMatch = 0;
    int fastPathTaken = 0;

    // Every jn:valid-at evaluation and every getDocument(...) opens a read-only trx (holding file
    // descriptors) that lives until the store closes; chunking the times over fresh stores bounds
    // the number of concurrently open trxes so the test also passes under low open-file limits.
    for (int chunkStart = 0; chunkStart < testTimes.size(); chunkStart += QUERY_CHUNK_SIZE) {
      final List<Instant> chunk =
          testTimes.subList(chunkStart, Math.min(chunkStart + QUERY_CHUNK_SIZE, testTimes.size()));

      try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
        store.lookup(DB_NAME);
        try (var ctx = SirixQueryContext.createWithJsonStore(store);
            var chain = SirixCompileChain.createWithJsonStore(store)) {

          final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB_NAME);

          for (final Instant t : chunk) {
            final Set<Integer> brute = bruteForce(records, t);
            if (brute.isEmpty()) {
              zeroMatch++;
            }
            if (brute.size() == records.size()) {
              allMatch++;
            }
            for (final Record r : records) {
              if (t.equals(r.validFrom())) {
                boundaryFrom++;
                break;
              }
            }
            for (final Record r : records) {
              if (t.equals(r.validTo())) {
                boundaryTo++;
                break;
              }
            }

            // (a) The interval-index FAST PATH must be taken (a VALIDTIME index exists + is usable).
            final JsonDBItem doc = collection.getDocument(RESOURCE);
            final ValidTimeIntervalIndex.Result fast =
                ValidTimeIntervalIndex.tryIndexScan(doc, t, validTimeConfig);
            assertNotNull(fast, "Interval-index fast path must be taken at t=" + t
                + " (the index was created via jn:create-valid-time-index)");
            fastPathTaken++;
            assertEquals(brute, idsOfItems(fast.items()),
                "Direct interval-index result must equal brute force at t=" + t);
            // All result items wrap the document's trx; ids are materialized, so release it now.
            doc.getTrx().close();

            // (b) The jn:valid-at function (which prefers the interval index) must agree.
            assertEquals(brute, idsFromValidAt(chain, ctx, t),
                "jn:valid-at must equal brute force at t=" + t);
          }
        }
      }
    }

    assertEquals(testTimes.size(), fastPathTaken, "Interval-index fast path must be taken for EVERY t");
    assertTrue(boundaryFrom > 0, "Expected a t == some validFrom (boundary)");
    assertTrue(boundaryTo > 0, "Expected a t == some validTo (boundary)");
    assertTrue(zeroMatch > 0, "Expected a zero-match t");
    assertTrue(allMatch > 0, "Expected an all-match t");
  }

  @Test
  @DisplayName("RBTree CAS index + HOT VALIDTIME index coexist on one resource (no -D flags)")
  void rbtreeCasAndHotValidTimeCoexist() throws IOException {
    assertTrue(System.getProperty("sirix.index.useHOT") == null
            || "false".equalsIgnoreCase(System.getProperty("sirix.index.useHOT")),
        "This coexistence test must run WITHOUT -Dsirix.index.useHOT");

    // A small, deterministic dataset so we can assert an exact CAS hit.
    final List<Record> records = new ArrayList<>();
    final Instant uFrom = Instant.parse("2021-01-15T00:00:00Z");
    records.add(new Record(0, uFrom, Instant.parse("2021-12-31T23:59:59Z")));
    records.add(new Record(1, Instant.parse("2020-06-01T00:00:00Z"), Instant.parse("2030-01-01T00:00:00Z")));
    records.add(new Record(2, Instant.parse("2022-03-03T03:03:03Z"), Instant.parse("2999-12-31T23:59:59Z")));
    final String json = toJson(records);

    final var dbPath = sirixPath.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    // Resource with the RBTree backend selected for secondary indexes (CAS/PATH/NAME) AND valid-time
    // config. The VALIDTIME index will still force HOT internally.
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE)
                                                      .validTimePaths(VALID_FROM, VALID_TO)
                                                      .indexBackendType(io.sirix.access.IndexBackendType.RBTREE)
                                                      .buildPathSummary(true)
                                                      .build();
      database.createResource(resourceConfig);
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);

    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB_NAME);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Create an RBTree CAS index on /[]/validFrom, then the HOT VALIDTIME index, then commit.
        final String createBoth =
            "let $doc := jn:doc('" + DB_NAME + "', '" + RESOURCE + "') "
                + "let $cas := jn:create-cas-index($doc, 'xs:string', '/[]/" + VALID_FROM + "') "
                + "let $vt := jn:create-valid-time-index($doc) "
                + "return sdb:commit($doc)";
        assertNotNull(new Query(chain, createBoth).evaluate(ctx),
            "creating both a CAS and a VALIDTIME index in one query must succeed");
      }
    }

    Databases.getGlobalBufferManager().clearAllCaches();

    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB_NAME);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {

        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB_NAME);

        // (1) Both index types must be registered on the resource.
        final JsonDBItem doc = collection.getDocument(RESOURCE);
        final var controller = doc.getTrx().getResourceSession()
                                  .getRtxIndexController(doc.getTrx().getRevisionNumber());
        assertTrue(controller.getIndexes().getNrOfIndexDefsWithType(io.sirix.index.IndexType.CAS) >= 1,
            "resource must have the RBTree CAS index");
        assertTrue(controller.getIndexes().getNrOfIndexDefsWithType(io.sirix.index.IndexType.VALIDTIME) >= 1,
            "resource must have the HOT VALIDTIME index");

        // (2) The HOT VALIDTIME index must answer jn:valid-at correctly via its fast path.
        for (final Instant t : List.of(uFrom, Instant.parse("2021-06-01T12:00:00Z"),
            Instant.parse("2019-01-01T00:00:00Z"))) {
          final Set<Integer> brute = bruteForce(records, t);
          final ValidTimeIntervalIndex.Result fast =
              ValidTimeIntervalIndex.tryIndexScan(doc, t, validTimeConfig);
          assertNotNull(fast, "interval-index fast path must be taken alongside an RBTree CAS index at t=" + t);
          assertEquals(brute, idsOfItems(fast.items()), "interval index must be correct at t=" + t);
          assertEquals(brute, idsFromValidAt(chain, ctx, t), "jn:valid-at must be correct at t=" + t);
        }

        // (3) The RBTree CAS index must independently still work (scan it for a known validFrom).
        final String casScan =
            "let $doc := jn:doc('" + DB_NAME + "', '" + RESOURCE + "') "
                + "let $n := jn:find-cas-index($doc, 'xs:string', '/[]/" + VALID_FROM + "') "
                + "for $node in jn:scan-cas-index($doc, $n, '" + uFrom + "', '==', '/[]/" + VALID_FROM + "') "
                + "return sdb:nodekey($node)";
        final Sequence casResult = new Query(chain, casScan).evaluate(ctx);
        int casHits = 0;
        if (casResult != null) {
          final Iter it = casResult.iterate();
          try {
            while (it.next() != null) {
              casHits++;
            }
          } finally {
            it.close();
          }
        }
        assertTrue(casHits >= 1,
            "the RBTree CAS index must return the validFrom='" + uFrom + "' node (record 0)");
      }
    }
  }

  // ---- helpers -------------------------------------------------------------------------------

  private static Set<Integer> bruteForce(final List<Record> records, final Instant t) {
    final Set<Integer> brute = new TreeSet<>();
    for (final Record r : records) {
      if (r.validAt(t)) {
        brute.add(r.id());
      }
    }
    return brute;
  }

  private static final Instant UNIVERSAL = Instant.parse("2021-06-01T12:00:00Z");

  private static List<Record> buildDataset() {
    final List<Record> recs = new ArrayList<>();
    final Random rnd = new Random(20260619L);
    final Instant base = Instant.parse("2019-01-01T00:00:00Z");
    int id = 0;
    final long maxFromOffsetDays = ChronoUnit.DAYS.between(base, UNIVERSAL);
    for (int i = 0; i < 120; i++) {
      final Instant from = base.plus(rnd.nextInt((int) maxFromOffsetDays), ChronoUnit.DAYS)
                               .plusSeconds(rnd.nextInt(86_400));
      final Instant to = (i % 6 == 0)
          ? Instant.parse("2999-12-31T23:59:59Z") // open-ended
          : UNIVERSAL.plus(1 + rnd.nextInt(700), ChronoUnit.DAYS).plusSeconds(rnd.nextInt(86_400));
      recs.add(new Record(id++, from, to));
    }
    // Millisecond precision spanning UNIVERSAL.
    for (int i = 0; i < 10; i++) {
      recs.add(new Record(id++, UNIVERSAL.minusMillis(i), UNIVERSAL.plus(3, ChronoUnit.HOURS).plusMillis(250)));
    }
    // Point-in-time validity at UNIVERSAL.
    for (int i = 0; i < 5; i++) {
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
      sb.append("{\"id\": ").append(r.id())
        .append(", \"").append(VALID_FROM).append("\": \"").append(r.validFrom())
        .append("\", \"").append(VALID_TO).append("\": \"").append(r.validTo()).append("\"}");
    }
    sb.append("]");
    return sb.toString();
  }

  private static List<Instant> buildTestTimes(final List<Record> recs) {
    final Set<Instant> times = new LinkedHashSet<>();
    times.add(Instant.parse("1900-01-01T00:00:00Z")); // before all -> zero match
    times.add(Instant.parse("2998-01-01T00:00:00Z")); // after all closed intervals
    times.add(UNIVERSAL);                              // all match
    for (final Record r : recs) {
      times.add(r.validFrom());
      times.add(r.validTo());
      times.add(r.validFrom().minusMillis(1));
      times.add(r.validFrom().plusMillis(1));
      times.add(r.validTo().minusMillis(1));
      times.add(r.validTo().plusMillis(1));
    }
    return new ArrayList<>(times);
  }

  private static Set<Integer> idsFromValidAt(final SirixCompileChain chain, final SirixQueryContext ctx,
      final Instant t) {
    final String query = "jn:valid-at('" + DB_NAME + "', '" + RESOURCE + "', xs:dateTime('" + t + "'))";
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
      ids.add(((Numeric) obj.get(new io.brackit.query.atomic.QNm("id"))).intValue());
    }
    return ids;
  }
}
