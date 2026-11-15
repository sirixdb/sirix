package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.HashType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The planner's document-size signal must not depend on the resource's hash setting.
 *
 * <p>
 * A container record only carries a descendant count when the resource maintains node hashes, which
 * is off by default since 1.0. Four planner heuristics — the JSON path walker's minimum node count,
 * the XML path-summary filter threshold, the parallel-deref decision and the cost-based stage's
 * total node count — used to read {@code getDescendantCount()} directly and would therefore see
 * {@code 0} for every default resource, declining the large-resource route in each case. They now
 * go through {@link DocumentNodeCount}.
 *
 * <p>
 * This is a latency guard, not a results guard: the queries answer the same either way, which is
 * why nothing else in the suite notices.
 */
final class DocumentNodeCountTest {

  private static final String DATABASE = "json-path1";

  private static final String RESOURCE = "mydoc.jn";

  /** Enough records that a planner would have to call this a large document. */
  private static final int ELEMENTS = 2048;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void aHashlessResourceStillReportsItsSize() {
    final long hashless = estimateFor(HashType.NONE);
    final long hashed = estimateFor(HashType.ROLLING);

    assertTrue(hashless >= ELEMENTS,
        "a hashless resource must still report its size, got " + hashless + " for " + ELEMENTS + " elements");
    assertTrue(hashed >= ELEMENTS, "the hashed arm must report its size too, got " + hashed);
  }

  /** The stored descendant count is used where there is one. */
  @Test
  void aHashedResourceUsesItsStoredDescendantCount() {
    store(HashType.ROLLING);
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      assertSame(HashType.ROLLING, session.getResourceConfig().hashType);
      rtx.moveToDocumentRoot();
      assertTrue(rtx.getDescendantCount() > 0, "a hashed resource must store descendant counts");
      assertEquals(rtx.getDescendantCount(), DocumentNodeCount.estimate(rtx));
    }
  }

  /** The hashless fallback is the node-key frontier, which is maintained unconditionally. */
  @Test
  void theHashlessFallbackIsTheNodeKeyFrontier() {
    store(HashType.NONE);
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      assertSame(HashType.NONE, session.getResourceConfig().hashType);
      rtx.moveToDocumentRoot();
      assertEquals(0L, rtx.getDescendantCount(), "a hashless resource stores no descendant count");
      assertEquals(rtx.getMaxNodeKey(), DocumentNodeCount.estimate(rtx));
      assertTrue(rtx.getMaxNodeKey() > 0);
    }
  }

  /**
   * The regression this guards: the cost-based stage drops every annotation when the total node count
   * is not positive, so a hashless resource used to get no statistics at all.
   */
  @Test
  void theCostBasedStageGetsAPositiveTotalNodeCount() {
    for (final HashType hashType : new HashType[] {HashType.NONE, HashType.ROLLING}) {
      JsonTestHelper.deleteEverything();
      try (final var store = storeFor(hashType);
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {
        new Query(chain, storeQuery()).evaluate(ctx);
        try (final var provider = new SirixStatisticsProvider(store)) {
          final long total = provider.getTotalNodeCount(DATABASE, RESOURCE, -1);
          assertTrue(total > 0,
              "the cost-based stage would drop its annotations for hashType=" + hashType + ", got " + total);
        }
      }
    }
  }

  private static long estimateFor(final HashType hashType) {
    store(hashType);
    try (final var database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      return DocumentNodeCount.estimate(rtx);
    } finally {
      JsonTestHelper.deleteEverything();
    }
  }

  private static void store(final HashType hashType) {
    try (final var store = storeFor(hashType);
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, storeQuery()).evaluate(ctx);
    }
  }

  private static BasicJsonDBStore storeFor(final HashType hashType) {
    return BasicJsonDBStore.newBuilder()
                           .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                           .hashType(hashType)
                           .build();
  }

  private static String storeQuery() {
    final StringBuilder json = new StringBuilder("[");
    for (int index = 0; index < ELEMENTS; index++) {
      if (index != 0) {
        json.append(',');
      }
      json.append("{\"n\":").append(index).append('}');
    }
    json.append(']');
    return "jn:store('" + DATABASE + "','" + RESOURCE + "','" + json + "')";
  }
}
