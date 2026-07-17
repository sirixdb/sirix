/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Object;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.Databases;
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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * END-TO-END test for creating a resource with valid-time support and an AUTO-CREATED valid-time
 * interval index purely via JSONiq: {@code jn:store($coll, $res, $data, true(), $options)} with the
 * options {@code validFromPath}/{@code validToPath} (or {@code useConventionalValidTime}). No Java
 * API calls are needed to configure the resource — the options object drives the
 * {@link ValidTimeConfig} and the interval index is created in the same transaction/revision as the
 * initial shred.
 *
 * @author Johannes Lichtenberger
 */
@DisplayName("jn:store with valid-time options — auto-created interval index end-to-end")
public final class StoreValidTimeAutoIndexTest {

  private static final Path sirixPath = PATHS.PATH1.getFile();
  private static final String DB = "vt-store-options-db";
  private static final String RES = "vt-resource";

  private static final String VALID_FROM = "validFrom";
  private static final String VALID_TO = "validTo";

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
  @DisplayName("store with validFromPath/validToPath: config set, interval index auto-created, valid-at correct")
  void storeWithValidTimePathsAutoCreatesIntervalIndex() throws IOException {
    final List<Record> records = buildDataset();
    final String json = toJson(records, VALID_FROM, VALID_TO);

    storeViaQuery("jn:store('" + DB + "','" + RES + "','" + json + "', true(), "
        + "{\"validFromPath\": \"" + VALID_FROM + "\", \"validToPath\": \"" + VALID_TO + "\"})");

    // COLD reopen: the valid-time config and the interval index must be persisted, and data + index
    // must have landed in ONE revision.
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(sirixPath.resolve(DB));
        JsonResourceSession session = database.beginResourceSession(RES)) {
      final ValidTimeConfig config = session.getResourceConfig().getValidTimeConfig();
      assertNotNull(config, "resource must have a valid-time config");
      assertEquals(VALID_FROM, config.getNormalizedValidFromPath());
      assertEquals(VALID_TO, config.getNormalizedValidToPath());

      final int mostRecentRevision = session.getMostRecentRevisionNumber();
      assertEquals(1, mostRecentRevision, "data + auto-created index must land in a single revision");
      assertEquals(1,
          session.getRtxIndexController(mostRecentRevision).getIndexes().getNrOfIndexDefsWithType(IndexType.VALIDTIME),
          "exactly one VALIDTIME interval index must have been auto-created");
    }

    // Query correctness + interval-index fast path against a brute-force oracle.
    final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB);
        for (final Instant t : sampleTimes(records)) {
          final Set<Integer> brute = bruteForce(records, t);
          assertEquals(brute, idsFromValidAt(chain, ctx, t), "jn:valid-at must equal brute force at t=" + t);

          final ValidTimeIntervalIndex.Result fast =
              ValidTimeIntervalIndex.tryIndexScan(collection.getDocument(RES), t, validTimeConfig);
          assertNotNull(fast, "the auto-created interval index must be usable at t=" + t);
          assertEquals(brute, idsOfItems(fast.items()), "interval-index scan must equal brute force at t=" + t);
        }
      }
    }
  }

  @Test
  @DisplayName("store with useConventionalValidTime: _validFrom/_validTo config + auto-created index")
  void storeWithConventionalValidTimeOption() throws IOException {
    final List<Record> records = buildDataset();
    final String json = toJson(records, "_validFrom", "_validTo");

    storeViaQuery("jn:store('" + DB + "','" + RES + "','" + json + "', true(), "
        + "{\"useConventionalValidTime\": true()})");

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(sirixPath.resolve(DB));
        JsonResourceSession session = database.beginResourceSession(RES)) {
      final ValidTimeConfig config = session.getResourceConfig().getValidTimeConfig();
      assertNotNull(config, "resource must have a valid-time config");
      assertEquals("_validFrom", config.getNormalizedValidFromPath());
      assertEquals("_validTo", config.getNormalizedValidToPath());
      assertEquals(1,
          session.getRtxIndexController(session.getMostRecentRevisionNumber())
                 .getIndexes()
                 .getNrOfIndexDefsWithType(IndexType.VALIDTIME),
          "exactly one VALIDTIME interval index must have been auto-created");
    }

    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        for (final Instant t : sampleTimes(records)) {
          assertEquals(bruteForce(records, t), idsFromValidAt(chain, ctx, t),
              "jn:valid-at must equal brute force at t=" + t);
        }
      }
    }
  }

  @Test
  @DisplayName("autoCreateValidTimeIndex: false() — config set but NO interval index; valid-at falls back")
  void autoCreateValidTimeIndexOptOut() throws IOException {
    final List<Record> records = buildDataset();
    final String json = toJson(records, VALID_FROM, VALID_TO);

    storeViaQuery("jn:store('" + DB + "','" + RES + "','" + json + "', true(), "
        + "{\"validFromPath\": \"" + VALID_FROM + "\", \"validToPath\": \"" + VALID_TO + "\", "
        + "\"autoCreateValidTimeIndex\": false()})");

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(sirixPath.resolve(DB));
        JsonResourceSession session = database.beginResourceSession(RES)) {
      assertNotNull(session.getResourceConfig().getValidTimeConfig(), "resource must have a valid-time config");
      assertEquals(0,
          session.getRtxIndexController(session.getMostRecentRevisionNumber())
                 .getIndexes()
                 .getNrOfIndexDefsWithType(IndexType.VALIDTIME),
          "no VALIDTIME interval index must exist after opting out");
    }

    final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB);
        final Instant t = Instant.parse("2021-01-15T00:00:00Z");
        assertNull(ValidTimeIntervalIndex.tryIndexScan(collection.getDocument(RES), t, validTimeConfig),
            "the interval-index fast path must NOT apply after opting out");
        assertEquals(bruteForce(records, t), idsFromValidAt(chain, ctx, t),
            "jn:valid-at must still be correct via fallback at t=" + t);
      }
    }
  }

  @Test
  @DisplayName("validFromPath without validToPath is rejected")
  void validTimePathOptionsMustBePaired() {
    assertThrows(QueryException.class,
        () -> storeViaQuery("jn:store('" + DB + "','" + RES + "','[]', true(), "
            + "{\"validFromPath\": \"" + VALID_FROM + "\"})"),
        "specifying only one of validFromPath/validToPath must fail");
  }

  @Test
  @DisplayName("insert after store: the auto-created index is maintained by the change listener")
  void insertAfterStoreMaintainsAutoCreatedIndex() throws IOException {
    final List<Record> records = new ArrayList<>(buildDataset());
    final String json = toJson(records, VALID_FROM, VALID_TO);

    storeViaQuery("jn:store('" + DB + "','" + RES + "','" + json + "', true(), "
        + "{\"validFromPath\": \"" + VALID_FROM + "\", \"validToPath\": \"" + VALID_TO + "\"})");

    // Insert a record in a LATER revision — the listener registered from the persisted index
    // definition must maintain the interval index.
    final int newId = 999;
    final Instant newFrom = Instant.parse("2030-01-01T00:00:00Z");
    final Instant newTo = Instant.parse("2030-12-31T00:00:00Z");
    records.add(new Record(newId, newFrom, newTo));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(sirixPath.resolve(DB));
        JsonResourceSession session = database.beginResourceSession(RES);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // the top-level array
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"id\": " + newId + ", \"" + VALID_FROM + "\": \"" + newFrom + "\", \"" + VALID_TO + "\": \"" + newTo
              + "\"}"), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }
    Databases.getGlobalBufferManager().clearAllCaches();

    final ValidTimeConfig validTimeConfig = new ValidTimeConfig(VALID_FROM, VALID_TO);
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final JsonDBCollection collection = (JsonDBCollection) store.lookup(DB);
        for (final Instant t : List.of(newFrom, newTo, newFrom.minusMillis(1), Instant.parse("2021-01-15T00:00:00Z"))) {
          final Set<Integer> brute = bruteForce(records, t);
          assertEquals(brute, idsFromValidAt(chain, ctx, t),
              "jn:valid-at must equal brute force after a post-store insert at t=" + t);

          final ValidTimeIntervalIndex.Result fast =
              ValidTimeIntervalIndex.tryIndexScan(collection.getDocument(RES), t, validTimeConfig);
          assertNotNull(fast, "the interval index must still be usable after a post-store insert at t=" + t);
          assertEquals(brute, idsOfItems(fast.items()),
              "interval-index scan must include listener-maintained entries at t=" + t);
        }
        assertTrue(idsFromValidAt(chain, ctx, newFrom).contains(newId),
            "the inserted record must be found at its validFrom");
      }
    }
  }

  @Test
  @DisplayName("store an empty resource with valid-time options: index definition persisted for future data")
  void storeEmptyResourcePersistsIndexDefinition() {
    storeViaQuery("jn:store('" + DB + "','" + RES + "','', true(), "
        + "{\"validFromPath\": \"" + VALID_FROM + "\", \"validToPath\": \"" + VALID_TO + "\"})");

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(sirixPath.resolve(DB));
        JsonResourceSession session = database.beginResourceSession(RES)) {
      assertNotNull(session.getResourceConfig().getValidTimeConfig(), "resource must have a valid-time config");
      assertEquals(1,
          session.getRtxIndexController(session.getMostRecentRevisionNumber())
                 .getIndexes()
                 .getNrOfIndexDefsWithType(IndexType.VALIDTIME),
          "the VALIDTIME index definition must be persisted even without initial data");
    }
  }

  // ---- helpers -------------------------------------------------------------------------------

  private static void storeViaQuery(final String storeQuery) {
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, storeQuery).evaluate(ctx);
    }
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  private static List<Record> buildDataset() {
    final List<Record> recs = new ArrayList<>(8);
    recs.add(new Record(1, Instant.parse("2020-01-01T00:00:00Z"), Instant.parse("2020-12-31T23:59:59Z")));
    recs.add(new Record(2, Instant.parse("2020-06-01T00:00:00Z"), Instant.parse("2021-06-01T00:00:00Z")));
    recs.add(new Record(3, Instant.parse("2021-01-01T00:00:00Z"), Instant.parse("2021-12-31T23:59:59Z")));
    recs.add(new Record(4, Instant.parse("2019-01-01T00:00:00Z"), Instant.parse("2022-01-01T00:00:00Z")));
    recs.add(new Record(5, Instant.parse("2021-01-15T00:00:00Z"), Instant.parse("2021-01-15T00:00:00Z")));
    recs.add(new Record(6, Instant.parse("2018-01-01T00:00:00Z"), Instant.parse("2018-06-01T00:00:00Z")));
    recs.add(new Record(7, Instant.parse("2023-01-01T00:00:00Z"), Instant.parse("2999-12-31T23:59:59Z")));
    return recs;
  }

  private static List<Instant> sampleTimes(final List<Record> recs) {
    final Set<Instant> times = new LinkedHashSet<>();
    times.add(Instant.parse("1900-01-01T00:00:00Z"));
    times.add(Instant.parse("2998-01-01T00:00:00Z"));
    times.add(Instant.parse("2021-01-15T00:00:00Z"));
    for (final Record r : recs) {
      times.add(r.validFrom());
      times.add(r.validTo());
      times.add(r.validFrom().minusMillis(1));
      times.add(r.validTo().plusMillis(1));
    }
    return new ArrayList<>(times);
  }

  private static String toJson(final List<Record> recs, final String validFromField, final String validToField) {
    final StringBuilder sb = new StringBuilder(recs.size() * 96);
    sb.append("[");
    for (int i = 0; i < recs.size(); i++) {
      final Record r = recs.get(i);
      if (i > 0) {
        sb.append(",");
      }
      sb.append("{\"id\": ").append(r.id())
        .append(", \"").append(validFromField).append("\": \"").append(r.validFrom())
        .append("\", \"").append(validToField).append("\": \"").append(r.validTo()).append("\"}");
    }
    sb.append("]");
    return sb.toString();
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

  private static Set<Integer> idsFromValidAt(final SirixCompileChain chain, final SirixQueryContext ctx,
      final Instant t) {
    final Sequence result =
        new Query(chain, "jn:valid-at('" + DB + "', '" + RES + "', xs:dateTime('" + t + "'))").evaluate(ctx);
    final Set<Integer> ids = new TreeSet<>();
    if (result == null) {
      return ids;
    }
    final Iter iter = result.iterate();
    try {
      Item item;
      while ((item = iter.next()) != null) {
        ids.add(((Numeric) ((Object) item).get(new QNm("id"))).intValue());
      }
    } finally {
      iter.close();
    }
    return ids;
  }

  private static Set<Integer> idsOfItems(final List<JsonDBItem> items) {
    final Set<Integer> ids = new TreeSet<>();
    for (final JsonDBItem item : items) {
      ids.add(((Numeric) ((Object) item).get(new QNm("id"))).intValue());
    }
    return ids;
  }
}
