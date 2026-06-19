/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.compiler.AST;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HARD GATE for {@code jn:drop-valid-time-index}: the drop must persist across a commit, after which
 * the index is no longer maintained/used at the new revision (jn:valid-at falls back, the optimizer
 * stops rewriting), while time-travel at the pre-drop revision still uses the index. No {@code -D}
 * flags.
 *
 * @author Johannes Lichtenberger
 */
@DisplayName("Valid-Time Interval Index — drop (jn:drop-valid-time-index) gate")
public final class ValidTimeIndexDropTest {

  private static final Path sirixPath = PATHS.PATH1.getFile();
  private static final String DB = "vt-drop-db";
  private static final String RES = "r";

  private static final String VALID_FROM = "validFrom";
  private static final String VALID_TO = "validTo";

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
  @DisplayName("drop persists; fallback correct; no rewrite; insert after drop ok; time-travel keeps index")
  void dropGate() throws IOException {
    assertTrue(System.getProperty("sirix.index.useHOT") == null
            || "false".equalsIgnoreCase(System.getProperty("sirix.index.useHOT")),
        "must run WITHOUT -Dsirix.index.useHOT");

    records = new ArrayList<>(buildDataset());
    final String json = toJson(records);

    final var dbPath = sirixPath.resolve(DB);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    // rev1: data. rev2: create VALIDTIME index (the PRE-DROP revision we time-travel back to).
    final int preDropRevision;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RES)
          .validTimePaths(VALID_FROM, VALID_TO).buildPathSummary(true).build());
      try (JsonResourceSession session = database.beginResourceSession(RES);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }
    createValidTimeIndexViaQuery();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        JsonResourceSession session = database.beginResourceSession(RES)) {
      preDropRevision = session.getMostRecentRevisionNumber();
      assertEquals(1, session.getRtxIndexController(preDropRevision).getIndexes()
              .getNrOfIndexDefsWithType(IndexType.VALIDTIME),
          "the pre-drop revision must have exactly one VALIDTIME index");
    }

    final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);
    final List<Instant> sampleTimes = sampleTimes(records);

    // Sanity: before the drop, jn:valid-at + interval-index fast path are correct and the optimizer
    // rewrites the FLWOR.
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB);
        final Instant t = UNIVERSAL;
        assertNotNull(ValidTimeIntervalIndex.tryIndexScan(collection.getDocument(RES), t, validTimeConfig),
            "interval index must be usable before the drop");
        assertTrue(optimizedContainsScanFunction(store, flwor(t)),
            "optimizer must rewrite before the drop");
      }
    }

    // ---- DROP the index + commit. ----
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final Sequence r = new Query(chain,
            "let $doc := jn:doc('" + DB + "','" + RES + "') "
                + "let $d := jn:drop-valid-time-index($doc) return sdb:commit($doc)").evaluate(ctx);
        assertNotNull(r, "drop query must return a result");
      }
    }
    Databases.getGlobalBufferManager().clearAllCaches();

    // ---- Assert the drop PERSISTED at the new revision (cold reopen). ----
    final int postDropRevision;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        JsonResourceSession session = database.beginResourceSession(RES)) {
      postDropRevision = session.getMostRecentRevisionNumber();
      assertTrue(postDropRevision > preDropRevision, "drop+commit must create a new revision");
      assertEquals(0, session.getRtxIndexController(postDropRevision).getIndexes()
              .getNrOfIndexDefsWithType(IndexType.VALIDTIME),
          "the catalog at the new revision must have 0 VALIDTIME index defs");
    }

    // ---- After drop: jn:valid-at still correct (fallback), optimizer no longer rewrites. ----
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {

        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB);

        // The interval-index fast path must NO LONGER be taken (index gone).
        assertNull(ValidTimeIntervalIndex.tryIndexScan(collection.getDocument(RES), UNIVERSAL, validTimeConfig),
            "interval-index fast path must NOT apply after the drop");

        for (final Instant t : sampleTimes) {
          final Set<Integer> brute = bruteForce(records, t);

          // jn:valid-at falls back to CAS-narrowing / linear scan and is still correct.
          assertEquals(brute, idsFromValidAt(chain, ctx, t),
              "jn:valid-at must still equal brute force AFTER the drop (fallback) at t=" + t);

          // The optimizer must NOT rewrite the FLWOR predicate anymore.
          assertFalse(optimizedContainsScanFunction(store, flwor(t)),
              "optimizer must NOT rewrite after the drop at t=" + t);

          // The FLWOR (cast form) itself, evaluated via normal eval, must still be correct.
          assertEquals(brute, idsFromObjectQuery(chain, ctx, flwor(t)),
              "FLWOR (normal eval, post-drop) must equal brute force at t=" + t);
        }
      }
    }

    // ---- Insert + commit AFTER the drop: no error, results still correct. ----
    final int newId = 999_000;
    final Instant newFrom = Instant.parse("2020-02-02T00:00:00Z");
    final Instant newTo = Instant.parse("2026-02-02T00:00:00Z");
    records.add(new Record(newId, newFrom, newTo));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        JsonResourceSession session = database.beginResourceSession(RES);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"id\": " + newId + ", \"" + VALID_FROM + "\": \"" + newFrom + "\", \"" + VALID_TO + "\": \"" + newTo + "\"}"),
          JsonNodeTrx.Commit.NO);
      wtx.commit(); // must not try to maintain the dropped index -> no error
    }
    Databases.getGlobalBufferManager().clearAllCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        for (final Instant t : List.of(newFrom, newTo, UNIVERSAL, newFrom.plus(10, ChronoUnit.DAYS))) {
          final Set<Integer> brute = bruteForce(records, t);
          assertEquals(brute, idsFromValidAt(chain, ctx, t),
              "jn:valid-at must equal brute force after a post-drop insert at t=" + t);
        }
        final Set<Integer> atNewFrom = idsFromValidAt(chain, ctx, newFrom);
        assertTrue(atNewFrom.contains(newId), "the post-drop inserted record must be found at its validFrom");
      }
    }

    // ---- TIME-TRAVEL: at the PRE-DROP revision the VALIDTIME index still exists and works. ----
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        JsonResourceSession session = database.beginResourceSession(RES)) {
      assertEquals(1, session.getRtxIndexController(preDropRevision).getIndexes()
              .getNrOfIndexDefsWithType(IndexType.VALIDTIME),
          "time-travel: the pre-drop revision must STILL have the VALIDTIME index");
    }
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB);
        // open the document at the pre-drop revision and verify the interval index is usable there.
        final JsonDBItem preDropDoc = collection.getDocument(RES, preDropRevision);
        for (final Instant t : List.of(UNIVERSAL, records.get(0).validFrom(), records.get(0).validTo())) {
          final ValidTimeIntervalIndex.Result fast =
              ValidTimeIntervalIndex.tryIndexScan(preDropDoc, t, validTimeConfig);
          assertNotNull(fast, "time-travel: interval index must still be usable at the pre-drop revision at t=" + t);
          // The pre-drop revision's data does NOT include the post-drop-inserted record.
          final Set<Integer> brutePreDrop = bruteForceExcluding(records, t, newId);
          assertEquals(brutePreDrop, idsOfItems(fast.items()),
              "time-travel: interval index at pre-drop revision must equal brute force (pre-drop data) at t=" + t);
        }
      }
    }
  }

  @Test
  @DisplayName("drop VALIDTIME while keeping a CAS index: catalog keeps CAS, drops VALIDTIME, both correct")
  void dropValidTimeKeepingCasIndex() throws IOException {
    assertTrue(System.getProperty("sirix.index.useHOT") == null
            || "false".equalsIgnoreCase(System.getProperty("sirix.index.useHOT")),
        "must run WITHOUT -Dsirix.index.useHOT");

    records = new ArrayList<>(buildDataset());
    final String json = toJson(records);
    final var dbPath = sirixPath.resolve(DB);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RES)
          .validTimePaths(VALID_FROM, VALID_TO).buildPathSummary(true).build());
      try (JsonResourceSession session = database.beginResourceSession(RES);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    // Create a CAS index AND a VALIDTIME index.
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        new Query(chain, "let $doc := jn:doc('" + DB + "','" + RES + "') "
            + "let $cas := jn:create-cas-index($doc, 'xs:string', '/[]/" + VALID_FROM + "') "
            + "let $vt := jn:create-valid-time-index($doc) return sdb:commit($doc)").evaluate(ctx);
      }
    }
    Databases.getGlobalBufferManager().clearAllCaches();

    // Drop ONLY the VALIDTIME index + commit.
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        new Query(chain, "let $doc := jn:doc('" + DB + "','" + RES + "') "
            + "let $d := jn:drop-valid-time-index($doc) return sdb:commit($doc)").evaluate(ctx);
      }
    }
    Databases.getGlobalBufferManager().clearAllCaches();

    // Catalog now has the CAS index but NOT the VALIDTIME index (non-empty branch of the persist fix).
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        JsonResourceSession session = database.beginResourceSession(RES)) {
      final var controller = session.getRtxIndexController(session.getMostRecentRevisionNumber());
      assertEquals(0, controller.getIndexes().getNrOfIndexDefsWithType(IndexType.VALIDTIME),
          "VALIDTIME index must be gone after drop");
      assertTrue(controller.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS) >= 1,
          "the CAS index must SURVIVE the VALIDTIME drop");
    }

    // jn:valid-at still correct (fallback); the surviving CAS index still scannable.
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final ValidTimeConfig vtc = new ValidTimeConfig(VALID_FROM, VALID_TO);
        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB);
        assertNull(ValidTimeIntervalIndex.tryIndexScan(collection.getDocument(RES), UNIVERSAL, vtc),
            "interval-index fast path must be gone after the drop");
        for (final Instant t : List.of(UNIVERSAL, records.get(0).validFrom(), records.get(0).validTo())) {
          assertEquals(bruteForce(records, t), idsFromValidAt(chain, ctx, t),
              "jn:valid-at must still equal brute force after dropping VALIDTIME (CAS-narrowing fallback) at t=" + t);
        }
        // The surviving CAS index returns the record whose validFrom == records.get(0).validFrom().
        final String casScan = "let $doc := jn:doc('" + DB + "','" + RES + "') "
            + "let $n := jn:find-cas-index($doc, 'xs:string', '/[]/" + VALID_FROM + "') "
            + "for $node in jn:scan-cas-index($doc, $n, '" + records.get(0).validFrom() + "', '==', '/[]/" + VALID_FROM + "') "
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
        assertTrue(casHits >= 1, "the surviving CAS index must still return matching nodes");
      }
    }
  }

  // ---- helpers -------------------------------------------------------------------------------

  private String flwor(final Instant t) {
    final String lit = "xs:dateTime('" + t + "')";
    return "for $x in jn:doc('" + DB + "','" + RES + "')[] "
        + "where xs:dateTime($x.validFrom) <= " + lit + " and " + lit + " <= xs:dateTime($x.validTo) return $x";
  }

  private static boolean optimizedContainsScanFunction(final BasicJsonDBStore store, final String query) {
    final SirixCompileChain chain = new SirixCompileChain(null, store);
    chain.compile(query);
    return astContainsValue(chain.getOptimizedAST(), "scan-valid-time-index");
  }

  private static boolean astContainsValue(final AST node, final String sub) {
    if (node == null) {
      return false;
    }
    final Object v = node.getValue();
    if (v != null && v.toString().contains(sub)) {
      return true;
    }
    for (int i = 0, n = node.getChildCount(); i < n; i++) {
      if (astContainsValue(node.getChild(i), sub)) {
        return true;
      }
    }
    return false;
  }

  private static Set<Integer> bruteForce(final List<Record> records, final Instant t) {
    final Set<Integer> brute = new TreeSet<>();
    for (final Record r : records) {
      if (r.validAt(t)) {
        brute.add(r.id());
      }
    }
    return brute;
  }

  private static Set<Integer> bruteForceExcluding(final List<Record> records, final Instant t, final int excludeId) {
    final Set<Integer> brute = new TreeSet<>();
    for (final Record r : records) {
      if (r.id() != excludeId && r.validAt(t)) {
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
    for (int i = 0; i < 130; i++) {
      final Instant from = base.plus(rnd.nextInt((int) maxFromOffsetDays), ChronoUnit.DAYS)
                               .plusSeconds(rnd.nextInt(86_400));
      final Instant to = (i % 6 == 0)
          ? Instant.parse("2999-12-31T23:59:59Z")
          : UNIVERSAL.plus(1 + rnd.nextInt(800), ChronoUnit.DAYS).plusSeconds(rnd.nextInt(86_400));
      recs.add(new Record(id++, from, to));
    }
    for (int i = 0; i < 10; i++) {
      recs.add(new Record(id++, UNIVERSAL.minusMillis(i), UNIVERSAL.plus(3, ChronoUnit.HOURS).plusMillis(250)));
    }
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

  private static List<Instant> sampleTimes(final List<Record> recs) {
    final Set<Instant> times = new LinkedHashSet<>();
    times.add(Instant.parse("1900-01-01T00:00:00Z"));
    times.add(Instant.parse("2998-01-01T00:00:00Z"));
    times.add(UNIVERSAL);
    times.add(UNIVERSAL.plusMillis(1));
    times.add(UNIVERSAL.minusMillis(1));
    int i = 0;
    for (final Record r : recs) {
      if (i++ % 9 == 0) {
        times.add(r.validFrom());
        times.add(r.validTo());
        times.add(r.validFrom().minusMillis(1));
        times.add(r.validTo().plusMillis(1));
      }
    }
    return new ArrayList<>(times);
  }

  private static void createValidTimeIndexViaQuery() {
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        new Query(chain, "let $doc := jn:doc('" + DB + "','" + RES + "') "
            + "let $i := jn:create-valid-time-index($doc) return sdb:commit($doc)").evaluate(ctx);
      }
    }
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  private static Set<Integer> idsFromValidAt(final SirixCompileChain chain, final SirixQueryContext ctx,
      final Instant t) {
    return idsFromObjectQuery(chain, ctx,
        "jn:valid-at('" + DB + "', '" + RES + "', xs:dateTime('" + t + "'))");
  }

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
      ids.add(((Numeric) obj.get(new io.brackit.query.atomic.QNm("id"))).intValue());
    }
    return ids;
  }
}
