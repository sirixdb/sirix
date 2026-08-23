/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Pins the TRIGGER POINT of the sliced-to-whole-leaf promotion, which no other test measures.
 *
 * <p>
 * The group arms serve from column slices while the projection's leaves are unmaterialized, and
 * promote a handle that keeps arriving at that route to the contiguous byte kernels — slices exist
 * to skip the cold-start assembly, and once the leaves are in memory the byte scan wins. The policy
 * is {@code handle.slicedRouteTick() >= SLICED_PROMOTE_AFTER}, where the tick returns the count
 * BEFORE its increment, so the arrival that kicks the promotion is number
 * {@code SLICED_PROMOTE_AFTER + 1}.
 *
 * <p>
 * That number is a hot-path performance decision and nothing else asserted it. The two column-slice
 * tests in {@link ProjectionIndexCatalogServingTest} pin two consecutive sliced serves, which holds
 * for every threshold at or above two; {@code StringDistinctGroupServingTest} loops up to forty
 * times until promotion lands, so it tolerates any trigger point at all. Either the policy's
 * conditions or its counter's semantics could therefore be changed — for instance ticking only on
 * serves that actually took the sliced route, which is a strict subset of arrivals — and the
 * trigger would move with no test going red. This test is the witness for that number.
 *
 * <p>
 * What it does NOT witness, stated so nobody reads more into a green run than is there: ticking on
 * serves rather than on arrivals measures the SAME trigger point here, and the two can only diverge
 * on a query that reaches the route and then declines to slice — which in both arms falls straight
 * into {@code leafPayloadsOrNull}, materializing the leaves and, since that flag is write-once,
 * retiring the promotion policy for that handle for good.
 *
 * <p>
 * The observable is the route itself: {@link SirixVectorizedExecutor#groupAggSlicedServedCount()}
 * is ticked inside the sliced kernels, so a query that promoted reports zero even though it is
 * still served from the projection and still answers identically — which is exactly why byte
 * equality cannot see this and a counter must.
 */
public final class SlicedPromotionTriggerTest extends AbstractJsonTest {

  /** Read from the same property the executor reads, so a threshold override cannot desync them. */
  private static final int SLICED_PROMOTE_AFTER = Integer.getInteger("sirix.projection.slicedPromoteAfter", 2);

  /**
   * Promotion is asynchronous; poll the handle for its landing rather than sleeping blind. The runs
   * BEFORE the trigger burn the whole budget by construction (nothing was kicked, so nothing will
   * land), which is why it is a second and not a minute: assembling nine rows takes microseconds, so
   * this is four orders of magnitude of margin over the thing being waited for.
   */
  private static final int MATERIALIZE_POLL_MILLIS = 10;
  private static final int MATERIALIZE_POLL_ATTEMPTS = 100;

  @BeforeEach
  public void clearProjectionStateBefore() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearProjectionStateAfter() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @Test
  public void theHandlePromotesOffTheSlicesOnTheArrivalAfterTheThreshold() throws IOException {
    query("""
          jn:store('json-path1','promo.jn','[
            {"grp": 7,  "age": 30, "tag": "aa"},
            {"grp": 7,  "age": 45, "tag": "bbbb"},
            {"grp": 0,  "age": 20, "tag": "c"},
            {"grp": 3,  "age": 8},
            {"grp": 7,  "age": 12, "tag": "dd"},
            {"grp": 3,  "age": 15, "tag": "ee"},
            {"age": 99, "tag": "fff"},
            {"grp": -5, "age": 60, "tag": "g"},
            {"grp": 4,  "age": 2,  "tag": "hh"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','promo.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/grp', '/[]/age', '/[]/tag'),
              ('long', 'long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String topKQuery = """
          subsequence(
            for $r in jn:doc('json-path1','promo.jn')[]
            where $r.age gt 5
            let $g := $r.grp
            group by $g
            let $c := count($r)
            order by $c descending
            return {"g": $g, "c": $c, "total": sum($r.age), "hi": max($r.age)}, 1, 3)
        """;

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("promo.jn");
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final int revision = session.getMostRecentRevisionNumber();
      // The oracle MUST run on a chain that cannot auto-wire an executor: createWithJsonStore wires
      // one per query, so the oracle leg would be a route ARRIVAL and would spend the very promotion
      // budget this test measures. See the twin note in ProjectionIndexCatalogServingTest.
      final SirixCompileChain genericChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
      final String generic = evaluateQuery(genericChain, ctx, topKQuery);

      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        // Runs are numbered from zero, the tick returns its pre-increment value, so run number
        // SLICED_PROMOTE_AFTER is the arrival that kicks the promotion — and the route latch means
        // it still serves sliced. Every run after it must take the whole-leaf byte kernel.
        final int probes = SLICED_PROMOTE_AFTER + 3;
        for (int run = 0; run < probes; run++) {
          final long aggBefore = SirixVectorizedExecutor.groupAggServedCount();
          final long slicedBefore = SirixVectorizedExecutor.groupAggSlicedServedCount();
          Assertions.assertEquals(generic, evaluateQuery(chain, ctx, topKQuery),
              "run " + run + " must answer exactly like the generic pipeline, on either route");
          Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - aggBefore,
              "run " + run + " must stay SERVED from the projection — promotion changes the route, "
                  + "never whether the projection answers");
          final long slicedThisRun = SirixVectorizedExecutor.groupAggSlicedServedCount() - slicedBefore;
          Assertions.assertEquals(run <= SLICED_PROMOTE_AFTER
              ? 1L
              : 0L, slicedThisRun,
              run <= SLICED_PROMOTE_AFTER
                  ? "run " + run + " is arrival " + (run + 1) + " of at most " + (SLICED_PROMOTE_AFTER + 1)
                      + " before the promotion lands, so it must still be served from column slices"
                  : "run " + run + " comes after the promotion, so it must take the whole-leaf byte "
                      + "kernel — a sliced serve here means the trigger moved LATER");
          // Promotion is kicked onto the warm-up lane, so wait for it to LAND before the next run;
          // otherwise the run at which the byte kernel takes over is a race with the background
          // assembly rather than a property of the trigger point.
          awaitMaterialized(session, resourceKey, revision);
        }
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  /**
   * Block until the handle's whole-leaf payloads are in memory, or the poll budget runs out — a
   * budget exhaustion is not an error here, it just means no promotion has been kicked yet.
   */
  private static void awaitMaterialized(final JsonResourceSession session, final String resourceKey,
      final int revision) {
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session, resourceKey, revision,
        new String[] {"[]"}, new String[] {"grp", "age"});
    if (handle == null) {
      return;
    }
    for (int attempt = 0; attempt < MATERIALIZE_POLL_ATTEMPTS && !handle.payloadsMaterialized(); attempt++) {
      try {
        Thread.sleep(MATERIALIZE_POLL_MILLIS);
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private static String evaluateQuery(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr)
      throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final PrintWriter printWriter = new PrintWriter(out)) {
      new Query(chain, queryStr).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }
}
