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
 * Bulk-imported value aggregates retain exact results through the typed route, while structural
 * field counts retain positively witnessed summary serving and cache reuse.
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
  public void bulkLoadedStatsRetainExactAggregatesAndSummaryCounts() throws Exception {
    final StringBuilder json = new StringBuilder(RECORDS * 24);
    json.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"age\":").append(i + 2).append('}');
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
      assertNotSummaryServed(chain, ctx, "sum", String.valueOf((long) RECORDS * (RECORDS + 3) / 2));
      assertNotSummaryServed(chain, ctx, "min", "2");
      assertNotSummaryServed(chain, ctx, "max", String.valueOf(RECORDS + 1));
      assertNotSummaryServed(chain, ctx, "avg", "251.5");
      assertEquals(String.valueOf(RECORDS), evaluate(chain, ctx, "count"), "count value");
      try (final JsonResourceSession session = store.lookup(DB_NAME).getDatabase().beginResourceSession(RESOURCE)) {
        final SirixVectorizedExecutor executor =
            new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
        try {
          final long before = SirixVectorizedExecutor.pathSummaryStatsServed();
          assertEquals(String.valueOf(RECORDS),
              executor.executeAggregate(ctx, new String[] {"[]"}, "count", "age").toString());
          assertTrue(SirixVectorizedExecutor.pathSummaryStatsServed() > before,
              "field count must be answered from structural summary references");
          final long after = SirixVectorizedExecutor.pathSummaryStatsServed();
          assertEquals(String.valueOf(RECORDS),
              executor.executeAggregate(ctx, new String[] {"[]"}, "count", "age").toString());
          assertEquals(after, SirixVectorizedExecutor.pathSummaryStatsServed(), "repeat count must use its cache");
          assertEquals("0", executor.executeAggregate(ctx, new String[] {"[]"}, "count", "absent").toString());
          assertTrue(SirixVectorizedExecutor.pathSummaryStatsServed() > after,
              "the exact summary must prove an absent field contributes no values");
        } finally {
          executor.close();
        }
      }
    }
  }

  private static void assertNotSummaryServed(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String func, final String expected) throws Exception {
    final long before = SirixVectorizedExecutor.pathSummaryStatsServed();
    assertEquals(expected, evaluate(chain, ctx, func), func + " value");
    assertEquals(before, SirixVectorizedExecutor.pathSummaryStatsServed(),
        func + " must not use value statistics without numeric purity evidence");
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
