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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hard correctness gate (the oracle) for the persistent valid-time interval index
 * ({@link io.sirix.index.IndexType#VALIDTIME}, a HOT-backed Relational-Interval-Tree) wired into
 * {@code jn:valid-at} / {@code jn:open-bitemporal}.
 *
 * <p>For a dataset of records with {@code validFrom}/{@code validTo} dateTime fields and a VALIDTIME
 * interval index, for MANY query instants (including every boundary case), this asserts that:</p>
 * <ol>
 * <li>the interval-index path ({@link ValidTimeIntervalIndex#tryIndexScan}, which we assert is
 * actually taken) returns exactly the brute-force Java reference set;</li>
 * <li>the {@code jn:valid-at} query over the indexed resource (interval-index fast path) returns
 * exactly that set;</li>
 * <li>the {@code jn:valid-at} query over a plain resource (no index → linear-scan fallback) returns
 * exactly that set.</li>
 * </ol>
 * <p>All must agree, for every instant, by id-set equality.</p>
 *
 * <p>Plus: INCREMENTAL maintenance (insert a record + delete a record, commit, re-query) exercises
 * the listener, and PERSISTENCE (reopen the resource at the committed revision and query) exercises
 * CoW page persistence — both re-asserted against brute force.</p>
 *
 * @author Johannes Lichtenberger
 */
@DisplayName("Valid-Time Interval Index Differential Test")
public final class ValidTimeIntervalIndexDifferentialTest {

  private static final Path sirixPath = PATHS.PATH1.getFile();
  private static final String DB_NAME = "valid-time-interval-diff-db";
  private static final String INDEXED_RESOURCE = "indexed";
  private static final String PLAIN_RESOURCE = "plain";

  private static final String VALID_FROM = "validFrom";
  private static final String VALID_TO = "validTo";

  /**
   * Test times per store instance. Each query/getDocument pins a read-only trx (and its file
   * descriptors) until the store closes, so chunking bounds concurrent open trxes.
   */
  private static final int QUERY_CHUNK_SIZE = 100;

  /** A record in the dataset (the brute-force oracle's source of truth). */
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
  @DisplayName("interval index == scan == brute force for all t (incl. boundaries) + incremental + persistence")
  void intervalIndexEqualsScanEqualsBruteForce() throws IOException {
    // The VALIDTIME index forces the HOT backend INTERNALLY (ValidTimeIntervalIndexFactory always
    // builds HOT readers/writers), so NO -Dsirix.index.useHOT flag is required. Explicitly clear it
    // for the duration of this test to prove the index works under default settings.
    final String prevUseHot = System.getProperty("sirix.index.useHOT");
    System.clearProperty("sirix.index.useHOT");
    try {
      runGate();
    } finally {
      if (prevUseHot == null) {
        System.clearProperty("sirix.index.useHOT");
      } else {
        System.setProperty("sirix.index.useHOT", prevUseHot);
      }
    }
  }

  private void runGate() throws IOException {
    final List<Record> records = new ArrayList<>(buildDataset());
    final String json = toJson(records);

    final var dbPath = sirixPath.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    final int committedRevisionAfterBuild;

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      // Resource WITH a VALIDTIME interval index. Shred FIRST, then create the index on the
      // populated resource — this exercises the full-scan BUILDER path.
      createResourceWithValidTime(database, INDEXED_RESOURCE);
      try (JsonResourceSession session = database.beginResourceSession(INDEXED_RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();

        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
        final var validFromPath = parse("/[]/" + VALID_FROM, io.brackit.query.util.path.PathParser.Type.JSON);
        final var validToPath = parse("/[]/" + VALID_TO, io.brackit.query.util.path.PathParser.Type.JSON);
        final Set<io.brackit.query.util.path.Path<io.brackit.query.atomic.QNm>> paths = new LinkedHashSet<>();
        paths.add(validFromPath);
        paths.add(validToPath);
        final IndexDef intervalIndex = IndexDefs.createValidTimeIdxDef(paths, 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(intervalIndex), wtx);
        wtx.commit();

        committedRevisionAfterBuild = session.getMostRecentRevisionNumber();
        assertTrue(indexController.getIndexes().getNrOfIndexDefsWithType(IndexType.VALIDTIME) >= 1,
            "Should have a VALIDTIME interval index");
      }

      // Resource WITHOUT any index (forces the linear-scan fallback — the independent oracle).
      createResourceWithValidTime(database, PLAIN_RESOURCE);
      try (JsonResourceSession session = database.beginResourceSession(PLAIN_RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    // ---- Phase A: the BUILD-path gate over many instants (incl. all boundary cases). ----
    final List<Instant> testTimes = buildTestTimes(records);
    assertTrue(testTimes.size() >= 300, "Expected >= 300 distinct test instants, was " + testTimes.size());

    int boundaryEqualsFrom = 0;
    int boundaryEqualsTo = 0;
    int zeroMatchTimes = 0;
    int allMatchTimes = 0;
    int subSecondTimes = 0;
    int indexPathTakenCount = 0;

    // Every jn:valid-at evaluation and every getDocument(...) opens a read-only trx (holding file
    // descriptors) that lives until the store closes; chunking the times over fresh stores bounds
    // the number of concurrently open trxes so the test also passes under low open-file limits.
    final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);
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
              zeroMatchTimes++;
            }
            if (brute.size() == records.size()) {
              allMatchTimes++;
            }
            if (t.getNano() != 0) { // a sub-second (fractional, e.g. millisecond) instant
              subSecondTimes++;
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

            // (1) DIRECT interval-index path — assert it is taken, then compare its result set.
            final JsonDBItem indexedDoc = collection.getDocument(INDEXED_RESOURCE);
            final ValidTimeIntervalIndex.Result indexScan =
                ValidTimeIntervalIndex.tryIndexScan(indexedDoc, t, validTimeConfig);
            assertNotNull(indexScan,
                "Interval-index path must be taken on the indexed resource (a VALIDTIME index exists) at t=" + t);
            indexPathTakenCount++;
            assertEquals(brute, idsOfItems(indexScan.items()),
                "Direct interval-index result must equal brute force at t=" + t
                    + " (candidates examined: " + indexScan.candidatesExamined() + ")");
            // All result items wrap the document's trx; ids are materialized, so release it now.
            indexedDoc.getTrx().close();

            // (2) jn:valid-at over the INDEXED resource (interval-index fast path through execute()).
            assertEquals(brute, idsFromValidAtQuery(chain, ctx, INDEXED_RESOURCE, t),
                "jn:valid-at over indexed resource must equal brute force at t=" + t);

            // (3) jn:valid-at over the PLAIN resource (linear-scan fallback — the second oracle).
            assertEquals(brute, idsFromValidAtQuery(chain, ctx, PLAIN_RESOURCE, t),
                "jn:valid-at over plain resource (scan) must equal brute force at t=" + t);
          }
        }
      }
    }

    assertTrue(boundaryEqualsFrom > 0, "Expected at least one t exactly equal to a validFrom");
    assertTrue(boundaryEqualsTo > 0, "Expected at least one t exactly equal to a validTo");
    assertTrue(zeroMatchTimes > 0, "Expected at least one t where zero records match");
    assertTrue(allMatchTimes > 0, "Expected at least one t where all records match");
    assertTrue(subSecondTimes > 0, "Expected at least one sub-second (millis) t");
    assertEquals(testTimes.size(), indexPathTakenCount, "Interval-index path must have been taken for every t");

    // ---- Phase B: PERSISTENCE — reopen the resource at the committed revision and re-query. ----
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        JsonResourceSession session = database.beginResourceSession(INDEXED_RESOURCE)) {
      assertEquals(committedRevisionAfterBuild, session.getMostRecentRevisionNumber(),
          "The build commit should be the most recent revision");
    }
    // A fresh store (cold caches) re-reads the index purely from persisted CoW pages.
    Databases.getGlobalBufferManager().clearAllCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB_NAME);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB_NAME);
        for (final Instant t : reopenSampleTimes(records)) {
          final Set<Integer> brute = bruteForce(records, t);
          final JsonDBItem indexedDoc = collection.getDocument(INDEXED_RESOURCE);
          final ValidTimeIntervalIndex.Result indexScan =
              ValidTimeIntervalIndex.tryIndexScan(indexedDoc, t, validTimeConfig);
          assertNotNull(indexScan, "Persisted interval index must be usable after reopen at t=" + t);
          assertEquals(brute, idsOfItems(indexScan.items()),
              "Persisted interval-index result must equal brute force after reopen at t=" + t);
          assertEquals(brute, idsFromValidAtQuery(chain, ctx, INDEXED_RESOURCE, t),
              "jn:valid-at over reopened indexed resource must equal brute force at t=" + t);
        }
      }
    }

    // ---- Phase C: INCREMENTAL maintenance — insert a new record + delete an existing one. ----
    final int newId = 100_000;
    final Instant newFrom = Instant.parse("2019-03-15T08:30:00Z");
    final Instant newTo = Instant.parse("2025-09-01T00:00:00Z");
    final Record newRecord = new Record(newId, newFrom, newTo);

    // Pick an existing record to delete (a closed, mid-range one) and remove it from the oracle too.
    final Record deleted = records.get(7);

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        JsonResourceSession session = database.beginResourceSession(INDEXED_RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      // INSERT: append the new object as the first child of the top-level array.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      final String newObjJson = toJsonObject(newRecord);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(newObjJson), JsonNodeTrx.Commit.NO);

      // DELETE: locate the object whose id == deleted.id() among the array elements and remove it.
      final long deletedKey = findObjectKeyById(wtx, deleted.id());
      assertTrue(deletedKey != Long.MIN_VALUE, "Should locate the record to delete (id=" + deleted.id() + ")");
      wtx.moveTo(deletedKey);
      wtx.remove();

      wtx.commit();
    }

    // Update the oracle to match: + newRecord, - deleted.
    final List<Record> mutated = new ArrayList<>(records);
    mutated.add(newRecord);
    mutated.removeIf(r -> r.id() == deleted.id());

    Databases.getGlobalBufferManager().clearAllCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB_NAME);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB_NAME);

        // Sample instants that specifically exercise the inserted + deleted records' boundaries,
        // plus a broad sweep.
        final Set<Instant> incTimes = new LinkedHashSet<>(reopenSampleTimes(mutated));
        incTimes.add(newFrom);
        incTimes.add(newTo);
        incTimes.add(newFrom.minusMillis(1));
        incTimes.add(newTo.plusMillis(1));
        incTimes.add(newFrom.plus(100, ChronoUnit.DAYS));
        incTimes.add(deleted.validFrom());
        incTimes.add(deleted.validTo());
        incTimes.add(deleted.validFrom().plusMillis(1));

        for (final Instant t : incTimes) {
          final Set<Integer> brute = bruteForce(mutated, t);
          final JsonDBItem indexedDoc = collection.getDocument(INDEXED_RESOURCE);
          final ValidTimeIntervalIndex.Result indexScan =
              ValidTimeIntervalIndex.tryIndexScan(indexedDoc, t, validTimeConfig);
          assertNotNull(indexScan, "Interval index must be usable after incremental maintenance at t=" + t);
          assertEquals(brute, idsOfItems(indexScan.items()),
              "Interval-index result must equal brute force AFTER incremental insert+delete at t=" + t);
          assertEquals(brute, idsFromValidAtQuery(chain, ctx, INDEXED_RESOURCE, t),
              "jn:valid-at must equal brute force AFTER incremental insert+delete at t=" + t);
        }

        // Cross-check: the deleted record's id must NOT appear at its own validFrom; the new one MUST.
        final Set<Integer> atNewFrom = idsFromValidAtQuery(chain, ctx, INDEXED_RESOURCE, newFrom);
        assertTrue(atNewFrom.contains(newId), "Newly inserted record must be found at its validFrom");
        final Set<Integer> atDeletedFrom = idsFromValidAtQuery(chain, ctx, INDEXED_RESOURCE, deleted.validFrom());
        assertTrue(!atDeletedFrom.contains(deleted.id()),
            "Deleted record must NOT be found at its validFrom after deletion");
      }
    }
  }

  @Test
  @DisplayName("open-ended intervals (absent validFrom / validTo) agree across interval index, indexed query, and linear scan")
  void openEndedIntervalsAgreeAcrossPaths() throws IOException {
    // id 1: closed   [2020-01-01 .. 2020-12-31].
    // id 2: open END — validFrom present, NO validTo  => "valid from 2021-01-01 onward" [2021, +inf).
    // id 3: open START — NO validFrom, validTo present => "valid up to 2019-12-31"      (-inf, 2019].
    final String json = """
        [
          {"id": 1, "validFrom": "2020-01-01T00:00:00Z", "validTo": "2020-12-31T23:59:59Z"},
          {"id": 2, "validFrom": "2021-01-01T00:00:00Z"},
          {"id": 3, "validTo": "2019-12-31T23:59:59Z"}
        ]
        """;

    final var dbPath = sirixPath.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      // Indexed resource: shred, then build the VALIDTIME interval index on the populated resource.
      createResourceWithValidTime(database, INDEXED_RESOURCE);
      try (JsonResourceSession session = database.beginResourceSession(INDEXED_RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
        final Set<io.brackit.query.util.path.Path<io.brackit.query.atomic.QNm>> paths = new LinkedHashSet<>();
        paths.add(parse("/[]/" + VALID_FROM, io.brackit.query.util.path.PathParser.Type.JSON));
        paths.add(parse("/[]/" + VALID_TO, io.brackit.query.util.path.PathParser.Type.JSON));
        indexController.createIndexes(Set.of(IndexDefs.createValidTimeIdxDef(paths, 0, IndexDef.DbType.JSON)), wtx);
        wtx.commit();
      }
      // Plain resource: no index, forces the linear-scan fallback (the independent oracle).
      createResourceWithValidTime(database, PLAIN_RESOURCE);
      try (JsonResourceSession session = database.beginResourceSession(PLAIN_RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    // (instant, expected id-set) — the +inf / -inf semantics the new predicate and index must share.
    final List<Instant> times = List.of(
        Instant.parse("2018-06-01T00:00:00Z"),  // only id 3 (valid up to 2019)
        Instant.parse("2019-12-31T23:59:59Z"),  // boundary: only id 3
        Instant.parse("2020-06-01T00:00:00Z"),  // only id 1 (the closed record)
        Instant.parse("2021-01-01T00:00:00Z"),  // boundary: only id 2 (open-ended start instant)
        Instant.parse("2021-06-01T00:00:00Z"),  // only id 2
        Instant.parse("2500-01-01T00:00:00Z")); // far future: only id 2 stays valid forever
    final List<Set<Integer>> expected =
        List.of(Set.of(3), Set.of(3), Set.of(1), Set.of(2), Set.of(2), Set.of(2));

    Databases.getGlobalBufferManager().clearAllCaches();
    final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB_NAME);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB_NAME);

        for (int i = 0; i < times.size(); i++) {
          final Instant t = times.get(i);
          final Set<Integer> want = expected.get(i);

          // Path 1: the interval index directly (assert it is actually taken).
          final JsonDBItem indexedDoc = collection.getDocument(INDEXED_RESOURCE);
          final ValidTimeIntervalIndex.Result idx =
              ValidTimeIntervalIndex.tryIndexScan(indexedDoc, t, validTimeConfig);
          assertNotNull(idx, "interval index must be usable at t=" + t);
          assertEquals(want, idsOfItems(idx.items()), "interval-index path at t=" + t);

          // Path 2: jn:valid-at over the indexed resource (interval-index fast path).
          assertEquals(want, idsFromValidAtQuery(chain, ctx, INDEXED_RESOURCE, t), "indexed jn:valid-at at t=" + t);

          // Path 3: jn:valid-at over the plain resource (linear-scan fallback).
          assertEquals(want, idsFromValidAtQuery(chain, ctx, PLAIN_RESOURCE, t), "plain jn:valid-at at t=" + t);
        }
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
   * Builds a varied dataset (&gt;= 150 records): random overlapping intervals, some open-ended
   * (far-future validTo), some with millisecond fractions, plus point-in-time records — every
   * interval contains {@link #UNIVERSAL} so there is a guaranteed all-match time.
   */
  private static List<Record> buildDataset() {
    final List<Record> recs = new ArrayList<>();
    final Random rnd = new Random(20260619L);
    final Instant base = Instant.parse("2018-01-01T00:00:00Z");

    int id = 0;

    final long maxFromOffsetDays = ChronoUnit.DAYS.between(base, UNIVERSAL); // ~883 days
    for (int i = 0; i < 160; i++) {
      final Instant from = base.plus(rnd.nextInt((int) maxFromOffsetDays), ChronoUnit.DAYS)
                               .plusSeconds(rnd.nextInt(86_400));
      final Instant to;
      if (i % 6 == 0) {
        to = Instant.parse("2999-12-31T23:59:59Z"); // open-ended
      } else {
        to = UNIVERSAL.plus(1 + rnd.nextInt(900), ChronoUnit.DAYS).plusSeconds(rnd.nextInt(86_400));
      }
      recs.add(new Record(id++, from, to));
    }

    // Sub-second (millisecond) precision spanning UNIVERSAL.
    for (int i = 0; i < 12; i++) {
      final Instant from = UNIVERSAL.minusMillis(i);
      final Instant to = UNIVERSAL.plus(2, ChronoUnit.HOURS).plusMillis(500);
      recs.add(new Record(id++, from, to));
    }

    // Point-in-time validity (validFrom == validTo == UNIVERSAL): boundary equality on BOTH ends.
    for (int i = 0; i < 6; i++) {
      recs.add(new Record(id++, UNIVERSAL, UNIVERSAL));
    }

    return recs;
  }

  private static String toJson(final List<Record> recs) {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < recs.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(toJsonObject(recs.get(i)));
    }
    sb.append("]");
    return sb.toString();
  }

  private static String toJsonObject(final Record r) {
    return "{\"id\": " + r.id() + ", \"" + VALID_FROM + "\": \"" + r.validFrom()
        + "\", \"" + VALID_TO + "\": \"" + r.validTo() + "\"}";
  }

  /**
   * Curated test times: before all, after all, every record's exact validFrom and validTo (boundary
   * equality on both ends), points just inside/outside those boundaries, fractional-second points,
   * a guaranteed zero-match time, and a guaranteed all-match time. Well over 300 distinct instants.
   */
  private static List<Instant> buildTestTimes(final List<Record> recs) {
    final Set<Instant> times = new LinkedHashSet<>();

    times.add(Instant.parse("1900-01-01T00:00:00Z")); // before all -> zero match
    times.add(Instant.parse("2998-01-01T00:00:00Z")); // after all closed intervals
    times.add(UNIVERSAL);                              // inside every interval -> all match

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

  /** A representative subset of instants for the reopen / incremental re-query (keeps them fast). */
  private static List<Instant> reopenSampleTimes(final List<Record> recs) {
    final Set<Instant> times = new LinkedHashSet<>();
    times.add(Instant.parse("1900-01-01T00:00:00Z"));
    times.add(Instant.parse("2998-01-01T00:00:00Z"));
    times.add(UNIVERSAL);
    times.add(UNIVERSAL.plusMillis(1));
    times.add(UNIVERSAL.minusMillis(1));
    int i = 0;
    for (final Record r : recs) {
      if (i++ % 11 == 0) { // sample every 11th record's boundaries
        times.add(r.validFrom());
        times.add(r.validTo());
        times.add(r.validFrom().minusMillis(1));
        times.add(r.validTo().plusMillis(1));
      }
    }
    return new ArrayList<>(times);
  }

  /** Locate the array-element OBJECT whose {@code id} field equals {@code id}; -inf if not found. */
  private static long findObjectKeyById(final JsonNodeTrx wtx, final int id) {
    wtx.moveToDocumentRoot();
    if (!wtx.moveToFirstChild() || !wtx.hasFirstChild()) {
      return Long.MIN_VALUE;
    }
    wtx.moveToFirstChild(); // first object element
    do {
      final long objectKey = wtx.getNodeKey();
      if (wtx.isObject() && wtx.moveToFirstChild()) {
        do {
          if (wtx.getName() != null && "id".equals(wtx.getName().getLocalName())) {
            final Number n = wtx.isNumberValue() ? wtx.getNumberValue() : null;
            if (n != null && n.intValue() == id) {
              wtx.moveTo(objectKey);
              return objectKey;
            }
          }
        } while (wtx.moveToRightSibling());
        wtx.moveTo(objectKey);
      }
    } while (wtx.moveToRightSibling());
    return Long.MIN_VALUE;
  }

  private static Set<Integer> idsFromValidAtQuery(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String resource, final Instant t) {
    final String query = "jn:valid-at('" + DB_NAME + "', '" + resource + "', xs:dateTime('" + t + "'))";
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
