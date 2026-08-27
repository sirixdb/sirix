/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * GATE 2 + the lifted-path smoke: a resource loaded by the PARALLEL BULK importer with
 * {@code buildPathStatistics} enabled must (a) load at all (the refusal is lifted), (b) answer
 * aggregate queries with the values a scan computes, and (c) genuinely answer them FROM THE PATH
 * SUMMARY — witnessed by {@link SirixVectorizedExecutor#pathSummaryStatsServed()}, the counter
 * added because a served answer and a declined fallback are otherwise indistinguishable.
 *
 * <p>
 * The query satisfies the full serve-precondition conjunction: statistics on, function in {count,
 * sum, min, max} (never avg), a numeric NON-array field, a single ancestor-chain match.
 */
public final class BulkPathStatsSummaryServedTest {

  private static final String DB_NAME = "json-path1";
  private static final String RESOURCE = "a.jn";
  private static final int RECORDS = 500;

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void bulkLoadedStatsServeAggregatesFromTheSummary() throws Exception {
    // Corpus: age = i, so count/sum/min/max have closed forms the query answers must match.
    final StringBuilder json = new StringBuilder(RECORDS * 24);
    json.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"age\":").append(i).append('}');
    }
    json.append(']');

    // Bulk-load via the PARALLEL importer into the store's expected database layout, with the
    // statistics flag ON — the load succeeding at all is the lifted-refusal smoke.
    final Path location = JsonTestHelper.PATHS.PATH1.getFile().getParent();
    final Path dbPath = location.resolve(DB_NAME);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .hashKind(HashType.NONE)
                                                   .storeNodeHistory(false)
                                                   .buildPathSummary(true)
                                                   .buildPathStatistics(true)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        ParallelBulkJsonImporter.assemble(wtx, new StringReader(json.toString()), 2048, 3);
        wtx.commit();
      }
    }

    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
        SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      // The VALUE aggregates — the feature's point — must each be answered FROM the summary:
      // per-function witness deltas, because equality alone cannot distinguish a served answer
      // from a scan fallback.
      assertServed(chain, ctx, "sum", String.valueOf((long) RECORDS * (RECORDS - 1) / 2));
      assertServed(chain, ctx, "min", "0");
      assertServed(chain, ctx, "max", String.valueOf(RECORDS - 1));
      // count is allowed to take an even cheaper route than the path-summary stats (a structural
      // count needs no statistics at all) — assert only the value, and record which route ran.
      final long before = SirixVectorizedExecutor.pathSummaryStatsServed();
      assertEquals(String.valueOf(RECORDS), evaluate(chain, ctx, "count"), "count value");
      System.out.println("[served-witness] count route: " + (SirixVectorizedExecutor.pathSummaryStatsServed() > before
          ? "path-summary stats"
          : "structural/scan (allowed)"));
    }
  }

  private static void assertServed(final SirixCompileChain chain, final SirixQueryContext ctx, final String func,
      final String expected) throws Exception {
    final long before = SirixVectorizedExecutor.pathSummaryStatsServed();
    assertEquals(expected, evaluate(chain, ctx, func), func + " value");
    assertTrue(SirixVectorizedExecutor.pathSummaryStatsServed() > before,
        func + " was answered by a fallback route, not the path-summary statistics");
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext ctx, final String func)
      throws Exception {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream(); PrintWriter pw = new PrintWriter(out)) {
      new Query(chain, func + "(for $r in jn:doc('" + DB_NAME + "','" + RESOURCE + "')[] return $r.age)").serialize(ctx,
          pw);
      pw.flush();
      return out.toString();
    }
  }
}
