package io.sirix.query;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.brackit.query.Query;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.parser.JsoniqParser;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.pageskip.PageSkipRegistry;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A compile chain built from a store alone auto-wires the vectorized executor by reading each
 * query's own {@code jn:doc}, so the analytical fast paths are on without the caller naming a
 * resource up front.
 *
 * <p>
 * What makes that safe is that the resolution is only a <em>hint</em> about which executor to
 * build; whether it may serve a scan stays the decision of
 * {@link SirixVectorizedExecutor#acceptsSource}, which runs later against the analyzed AST. Every
 * test here therefore asserts on an ANSWER, not on whether a fast path was taken — an executor
 * wired to the wrong resource would surface as a wrong number, which is the only failure mode worth
 * writing a test for.
 *
 * <p>
 * The two resources hold deliberately different sums, in two databases because {@code jn:store}
 * recreates the database it targets. An answer served from the wrong one cannot agree by accident.
 *
 * @see StoreBoundExecutorCache
 */
public final class AutoWiredExecutorTest {

  private static final String DB_A = "json-path1";
  private static final String DB_B = "json-path2";
  private static final String RES_A = "a.jn";
  private static final String RES_B = "b.jn";

  /** a.jn: ages 10,20,30,40 — count(age gt 15) = 3, sum = 100. */
  private static final String SUM_A = "100";
  private static final String COUNT_ABOVE_A = "3";
  /** b.jn: ages 1,2,3 — count(age gt 15) = 0, sum = 6. */
  private static final String SUM_B = "6";
  private static final String COUNT_ABOVE_B = "0";

  private static final int AGE_THRESHOLD = 15;

  /** Enough records to span several pages, so a scan has something to publish a bitmap about. */
  private static final int SCAN_RECORDS = 999;

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
    // Another test in this JVM may have left a process-wide executor registered, which would
    // legitimately suppress auto-wiring — see explicitRegistrationWinsOverAutoWiring.
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    query("jn:store('" + DB_A + "','" + RES_A + "','[{\"age\":10},{\"age\":20},{\"age\":30},{\"age\":40}]')");
    query("jn:store('" + DB_B + "','" + RES_B + "','[{\"age\":1},{\"age\":2},{\"age\":3}]')");
  }

  @AfterEach
  public void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    JsonTestHelper.deleteEverything();
  }

  /** The plain store factory — no session, no explicit executor — still answers correctly. */
  @Test
  public void storeOnlyChainAnswersAggregatesCorrectly() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      assertNotNull(chain.storeBoundExecutors(), "a store-only chain must auto-wire");

      assertEquals("4", evaluate(chain, ctx, "count(jn:doc('" + DB_A + "','" + RES_A + "')[])"));
      assertEquals(SUM_A, evaluate(chain, ctx, sum(DB_A, RES_A)));
      assertEquals(COUNT_ABOVE_A, evaluate(chain, ctx, countAbove(DB_A, RES_A)));
      assertTrue(chain.storeBoundExecutors().cachedExecutorCount() >= 1,
          "the resolved executor should have been cached");
    }
  }

  /**
   * The executor is resolved per query, not per chain: the same chain must answer over a second
   * resource with that resource's data. Getting this wrong is the failure the whole design guards
   * against, and it is invisible unless the two resources hold different numbers.
   */
  @Test
  public void sameChainServesEachResourceFromItsOwnData() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      assertEquals(SUM_A, evaluate(chain, ctx, sum(DB_A, RES_A)));
      assertEquals(SUM_B, evaluate(chain, ctx, sum(DB_B, RES_B)));
      // ... and back, so a cached executor for A cannot have been left bound to B.
      assertEquals(COUNT_ABOVE_A, evaluate(chain, ctx, countAbove(DB_A, RES_A)));
      assertEquals(COUNT_ABOVE_B, evaluate(chain, ctx, countAbove(DB_B, RES_B)));

      assertEquals(2, chain.storeBoundExecutors().cachedExecutorCount(),
          "one executor per resource, reused across queries");
    }
  }

  /**
   * A query naming two resources is accelerated on BOTH. The chain binds to the first document, and
   * every further scan resolves its own executor through the per-source hook — so the answer must be
   * right (each half read from its own resource) AND an executor must exist for each.
   */
  @Test
  public void multiResourceQueryIsServedOnEveryResource() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      assertEquals("106", evaluate(chain, ctx, "(" + sum(DB_A, RES_A) + ") + (" + sum(DB_B, RES_B) + ")"));
      assertEquals(2, chain.storeBoundExecutors().cachedExecutorCount(),
          "each scanned document must have resolved its own executor");
      // The two resources hold different sums, so a scan served from the wrong one is a wrong
      // number rather than a slow query. Order reversed to catch a binding that only ever works
      // for whichever document comes first.
      assertEquals("106", evaluate(chain, ctx, "(" + sum(DB_B, RES_B) + ") + (" + sum(DB_A, RES_A) + ")"));
      assertEquals("3", evaluate(chain, ctx, "(" + countAbove(DB_B, RES_B) + ") + (" + countAbove(DB_A, RES_A) + ")"));
    }
  }

  /**
   * The regression that made auto-wiring risky in the first place: an executor memoises answers for
   * the revision it is pinned to, so a chain that keeps one across a commit answers from before the
   * write. Resolution re-reads the most recent revision on every compile.
   */
  @Test
  public void aCommitOnTheSameChainIsVisibleToTheNextQuery() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      assertEquals(SUM_A, evaluate(chain, ctx, sum(DB_A, RES_A)));

      new Query(chain,
          "let $doc := jn:doc('" + DB_A + "','" + RES_A + "') return insert json {\"age\":50} into $doc").evaluate(ctx);

      assertEquals("150", evaluate(chain, ctx, sum(DB_A, RES_A)),
          "the inserted record must be visible — a stale executor would still answer " + SUM_A);
      assertEquals("4", evaluate(chain, ctx, countAbove(DB_A, RES_A)));
    }
  }

  /**
   * A read that follows an UNCOMMITTED write. The auto-wired executor is pinned to the last committed
   * revision and memoises answers for it, which is the shape of the staleness bug this design has to
   * keep avoiding — so the fast path and the generic pipeline are made to answer the same
   * update-then-read sequence and compared.
   *
   * <p>
   * (Read-your-own-writes <em>within</em> one query is not expressible: XQuery Update rejects a query
   * that both updates and returns a value, {@code err:XUST0001}.)
   */
  @Test
  public void anUncommittedWriteIsReadTheSameWayWithAndWithoutAutoWiring() throws IOException {
    final String write = "let $doc := jn:doc('" + DB_A + "','" + RES_A + "') return insert json {\"age\":50} into $doc";

    final String generic;
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      new Query(chain, write).evaluate(ctx);
      generic = evaluate(chain, ctx, sum(DB_A, RES_A));
    }

    // The write above is part of the measurement, so put the resource back before repeating it.
    query("jn:store('" + DB_A + "','" + RES_A + "','[{\"age\":10},{\"age\":20},{\"age\":30},{\"age\":40}]')");

    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, write).evaluate(ctx);
      assertEquals(generic, evaluate(chain, ctx, sum(DB_A, RES_A)),
          "the auto-wired chain must read an uncommitted write exactly as the generic one does");
    }
  }

  /**
   * A query compiled ONCE and executed again after a commit must see the commit — the way anyone
   * using a prepared query would expect, and what the generic pipeline does, because its
   * {@code jn:doc} opens the most recent revision at EXECUTE time.
   *
   * <p>
   * This is the second half of the staleness bug. Re-resolving the revision per COMPILE fixed a chain
   * that compiles every query; it does nothing for an already-compiled query, because the translator
   * captured the executor object and an executor is pinned to one revision. The compiled expression
   * therefore captures a revision-tracking indirection instead.
   */
  @Test
  public void aQueryCompiledOnceSeesCommitsMadeAfterItWasCompiled() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final Query compiledOnce = new Query(chain, sum(DB_A, RES_A));
      assertEquals(SUM_A, serialize(compiledOnce, ctx));

      new Query(chain,
          "let $doc := jn:doc('" + DB_A + "','" + RES_A + "') return insert json {\"age\":50} into $doc").evaluate(ctx);

      assertEquals("150", serialize(compiledOnce, ctx),
          "the SAME compiled query must see the insert — pinning it to its compile-time revision " + "would answer "
              + SUM_A);
    }
  }

  /**
   * Most vectorized entry points are substituted at TRANSLATE time and brackit turns a {@code null}
   * result into a failed query rather than a fallback, so any shape the kernels decline at run time
   * would surface as an error rather than a slow answer. Field-free predicates are the family that
   * reaches those declines, so they are checked against the generic pipeline — equal answers, and in
   * particular no exception on one side only.
   */
  @Test
  public void constantPredicateShapesAnswerAsTheGenericPipelineDoes() throws IOException {
    final String source = "jn:doc('" + DB_A + "','" + RES_A + "')[]";
    final String[] queries = {"count(for $u in " + source + " where true() return $u)",
        "count(for $u in " + source + " where 1 eq 1 return $u)",
        "count(for $u in " + source + " where false() return $u)",
        "sum(for $u in " + source + " where true() return $u.age)",
        "avg(for $u in " + source + " where true() return $u.age)",
        "min(for $u in " + source + " where 1 eq 1 return $u.age)",
        "sum(for $u in " + source + " where not(false()) return $u.age)",
        "count(for $u in " + source + " where $u.age gt 0 and true() return $u)",
        "sum(for $u in " + source + " where $u.age gt 0 or true() return $u.age)",};
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        final SirixCompileChain autoWired = SirixCompileChain.createWithJsonStore(store)) {
      for (final String query : queries) {
        assertEquals(evaluate(generic, ctx, query), evaluate(autoWired, ctx, query), query);
      }
    }
  }

  /**
   * An executor a caller registered explicitly stays in charge. It is bound to A, so a query over B
   * must be declined by its own source gate and answered generically — the auto-wiring must not step
   * in behind the caller's back, because a caller who registers an executor is usually A/B-testing
   * that exact one.
   */
  @Test
  public void explicitRegistrationWinsOverAutoWiring() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonResourceSession sessionA = openResource(store, DB_A, RES_A);
      final SirixVectorizedExecutor explicit =
          new SirixVectorizedExecutor(sessionA, sessionA.getMostRecentRevisionNumber());
      SequentialPipelineStrategy.setVectorizedExecutor(explicit);
      try {
        assertEquals(SUM_A, evaluate(chain, ctx, sum(DB_A, RES_A)));
        assertEquals(SUM_B, evaluate(chain, ctx, sum(DB_B, RES_B)));
        assertEquals(0, chain.storeBoundExecutors().cachedExecutorCount(),
            "auto-wiring must not build an executor while one is registered");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        explicit.close();
      }
    }
  }

  /**
   * The cache is bounded, so an executor can be closed while a query compiled against it is still
   * reachable. That has to degrade, not fail: the worker pool is gone, so the scan runs inline, and
   * lazy record cursors remain owned by the resource session. Committing repeatedly is the cheapest
   * way to overflow a cache keyed by {@code (database, resource, revision)}.
   */
  @Test
  public void anEvictedExecutorStillAnswersTheQueryThatHoldsIt() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final Query first = new Query(chain, sum(DB_A, RES_A));
      final SirixVectorizedExecutor firstExecutor = chain.storeBoundExecutors().anyCachedExecutor();
      assertNotNull(firstExecutor, "the first compile must have cached an executor");

      // Each insert commits a revision, and each new revision is a new cache key. The inserted
      // ages are all 0, so the expected sum never moves.
      final List<Query> pinned = new ArrayList<>();
      for (int i = 0; i < 12; i++) {
        new Query(chain,
            "let $doc := jn:doc('" + DB_A + "','" + RES_A + "') return insert json {\"age\":0} into $doc").evaluate(
                ctx);
        pinned.add(new Query(chain, sum(DB_A, RES_A)));
      }
      assertTrue(chain.storeBoundExecutors().cachedExecutorCount() <= 8, "the cache must stay bounded");
      assertTrue(firstExecutor.isClosed(), "the first executor should have been evicted and closed");

      assertEquals(SUM_A, serialize(first, ctx),
          "a query holding an evicted executor must still answer from its own revision");
      assertEquals(SUM_A, serialize(pinned.get(pinned.size() - 1), ctx));
    }
  }

  /** Closing the chain releases every executor it built. */
  @Test
  public void closingTheChainClosesTheExecutorsItBuilt() throws IOException {
    final SirixVectorizedExecutor executor;
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store)) {
      try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
        evaluate(chain, ctx, sum(DB_A, RES_A));
        executor = chain.storeBoundExecutors().anyCachedExecutor();
        assertNotNull(executor);
      }
      assertTrue(executor.isClosed(), "the chain must close the executors it built");
    }
  }

  /** A session-bound chain keeps its binding and never builds a store-resolved cache. */
  @Test
  public void sessionBoundChainDoesNotAutoWireFromTheStore() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store)) {
      final JsonResourceSession sessionA = openResource(store, DB_A, RES_A);
      try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store, sessionA)) {
        assertNull(chain.storeBoundExecutors(), "an explicitly-bound chain must not auto-resolve");
        assertEquals(SUM_A, evaluate(chain, ctx, sum(DB_A, RES_A)));
        // Its executor is bound to A, so B is declined and answered generically — still correct.
        assertEquals(SUM_B, evaluate(chain, ctx, sum(DB_B, RES_B)));
      }
    }
  }

  /** A query naming no literal document resolves to nothing rather than to an arbitrary resource. */
  @Test
  public void aQueryWithoutALiteralDocumentResolvesNoExecutor() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      assertEquals("3", evaluate(chain, ctx, "1 + 2"));
      assertEquals(0, chain.storeBoundExecutors().cachedExecutorCount());

      // A computed resource name is not a literal, so there is nothing to bind at compile time —
      // and the query still has to produce the right answer through the generic pipeline.
      assertEquals(SUM_A,
          evaluate(chain, ctx, "sum(for $r in jn:doc('" + DB_A + "', concat('a', '.jn'))[] return $r.age)"));
      assertEquals(0, chain.storeBoundExecutors().cachedExecutorCount());
    }
  }

  /** A document that cannot be opened must decline rather than turn into a failure of its own. */
  @Test
  public void anUnresolvableDocumentDeclinesInsteadOfThrowing() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final StoreBoundExecutorCache cache = chain.storeBoundExecutors();
      assertNull(cache.resolve(parse("count(jn:doc('no-such-db','no-such-resource')[])")));
      assertNull(cache.resolve(parse("count(jn:doc('" + DB_A + "','no-such-resource')[])")));
      // An out-of-range revision names a snapshot that does not exist; falling back to the latest
      // one would answer a different question.
      assertNull(cache.resolve(parse("count(jn:doc('" + DB_A + "','" + RES_A + "',9999)[])")));
      assertEquals(0, cache.cachedExecutorCount());
    }
  }

  /**
   * The cache is keyed by the RESOLVED revision, so two queries agree on an executor exactly when
   * they read the same snapshot — including a bare {@code jn:doc} and one that names the latest
   * revision explicitly, which are the same read. An older revision is a different snapshot and gets
   * its own executor, because a memoised aggregate is only valid for the one it was computed from.
   */
  @Test
  public void resolutionIsMemoisedPerResourceAndRevision() throws IOException {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final StoreBoundExecutorCache cache = chain.storeBoundExecutors();
      final SirixVectorizedExecutor first = cache.resolve(parse(countAbove(DB_A, RES_A)));
      final SirixVectorizedExecutor second = cache.resolve(parse(sum(DB_A, RES_A)));
      assertNotNull(first);
      assertSame(first, second, "the same resource and revision must share one executor");
      assertEquals(1, cache.cachedExecutorCount());

      final int latest = openResource(store, DB_A, RES_A).getMostRecentRevisionNumber();
      assertSame(first, cache.resolve(parse(count(DB_A, RES_A, latest))),
          "naming the latest revision explicitly is the same read as a bare jn:doc");
      assertEquals(1, cache.cachedExecutorCount());

      // A commit makes the previous revision an older snapshot, which cannot share an executor
      // with the current one.
      new Query(chain,
          "let $doc := jn:doc('" + DB_A + "','" + RES_A + "') return insert json {\"age\":0} into $doc").evaluate(ctx);
      final SirixVectorizedExecutor pinnedToOlder = cache.resolve(parse(count(DB_A, RES_A, latest)));
      assertNotNull(pinnedToOlder);
      assertSame(first, pinnedToOlder, "the older revision keeps the executor already built for it");
      final SirixVectorizedExecutor current = cache.resolve(parse(sum(DB_A, RES_A)));
      assertNotNull(current);
      assertEquals(latest + 1, current.getRevision(), "a bare jn:doc must resolve to the new revision");
      assertEquals(2, cache.cachedExecutorCount(), "two snapshots, two executors");
    }
  }

  /**
   * The point of the whole change: the auto-wired executor must actually run the vectorized scan, not
   * merely be constructed. A feature that is on by default and inert would satisfy every other test
   * here, because a correct answer is exactly what the generic pipeline also produces.
   *
   * <p>
   * The observable is the page-skip registry: a completed vectorized predicate scan publishes a
   * per-resource bitmap for the anchor field, and nothing else in the system does. The resource is
   * built without a path summary so the PathSummary-persisted bitmap cannot be the one that shows up,
   * and it holds enough records to span several pages.
   */
  @Test
  public void theAutoWiredExecutorActuallyRunsTheVectorizedScan() throws Exception {
    final Path scanDir = Files.createTempDirectory("sirix-auto-wired-scan-");
    try {
      final String database = "scan-db";
      final String resource = "records.jn";
      final String resourceKey = buildUnsummarizedResource(scanDir, database, resource);
      final String query =
          "count(for $u in jn:doc('" + database + "','" + resource + "')[] where $u.amount gt 500 return $u)";

      PageSkipRegistry.clear();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(scanDir).build();
          final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
          final SirixCompileChain generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
        final String interpreted = evaluate(generic, ctx, query);
        assertNull(PageSkipRegistry.lookup(resourceKey), "the generic pipeline must not publish a page-skip bitmap");

        try (final SirixCompileChain autoWired = SirixCompileChain.createWithJsonStore(store)) {
          assertEquals(interpreted, evaluate(autoWired, ctx, query),
              "the auto-wired answer must equal the interpreted one");
        }
        final PageSkipRegistry.Handle handle = PageSkipRegistry.lookup(resourceKey);
        assertNotNull(handle, "the auto-wired chain must have run a vectorized scan");
        assertNotNull(handle.pagesForOrNull("amount".hashCode()),
            "a completed vectorized scan publishes the anchor field's page bitmap");
      } finally {
        PageSkipRegistry.clear();
      }
    } finally {
      Databases.removeDatabase(scanDir.resolve("scan-db"));
    }
  }

  @Test
  public void autoWiringServesSirixSpecificPredicateAndConstantGroupRoutes() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .storeDeweyIds(true)
                                                        .build()) {
      store.create(DB_A, RES_A, "[{\"age\":10},{\"age\":20},{\"age\":30},{\"age\":40}]");
    }
    query("""
        let $doc := jn:doc('json-path1','a.jn')
        let $stats := jn:create-projection-index($doc, '/[]', ('/[]/age'), ('long'))
        return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String predicate = "for $r in jn:doc('json-path1','a.jn')[] where $r.age gt 15 return $r";
    final String constantGroup = """
        for $r in jn:doc('json-path1','a.jn')[]
        let $g := 1
        group by $g
        return {"total": sum($r.age)}
        """;

    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        final SirixCompileChain autoWired = SirixCompileChain.createWithJsonStore(store)) {
      final String expectedPredicate = evaluate(generic, ctx, predicate);
      final String expectedConstantGroup = evaluate(generic, ctx, constantGroup);
      final long predicateBefore = SirixVectorizedExecutor.predicateScanServedCount();
      final long constantBefore = SirixVectorizedExecutor.constGroupAggServedCount();
      final Query compiledConstantGroup = new Query(autoWired, constantGroup);

      assertEquals(expectedPredicate, evaluate(autoWired, ctx, predicate));
      assertEquals(expectedConstantGroup, serialize(compiledConstantGroup, ctx));
      new Query(autoWired,
          "let $doc := jn:doc('json-path1','a.jn') return insert json {\"age\":50} into $doc").evaluate(ctx);
      assertEquals(evaluate(generic, ctx, constantGroup), serialize(compiledConstantGroup, ctx));
      assertEquals(1L, SirixVectorizedExecutor.predicateScanServedCount() - predicateBefore);
      assertEquals(2L, SirixVectorizedExecutor.constGroupAggServedCount() - constantBefore);
    }
  }

  /**
   * The const-group SEGMENT-FOLD arm: shifted sums, min, max, avg and count over one long column fold
   * straight from the packed segment bytes, with and without a predicate, and every function derives
   * from its lane's requested slots only (an unrequested extremum stays at its fold identity — the
   * MAX_VALUE + k overflow the exact add would otherwise refuse). Pinned against the generic
   * pipeline; the served counter proves the arm — not the resident-slice fold — answered.
   */
  @Test
  public void constantGroupShiftedAggregatesFoldFromSegmentBytes() throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .storeDeweyIds(true)
                                                        .build()) {
      final StringBuilder json = new StringBuilder(4096).append('[');
      for (int i = 0; i < 300; i++) {
        if (i > 0) {
          json.append(',');
        }
        json.append("{\"age\":").append((i * 7919L) % 101 - 50).append('}');
      }
      store.create(DB_A, RES_A, json.append(']').toString());
    }
    query("""
        let $doc := jn:doc('json-path1','a.jn')
        let $stats := jn:create-projection-index($doc, '/[]', ('/[]/age'), ('long'))
        return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] shapes = {"""
        for $r in jn:doc('json-path1','a.jn')[]
        let $g := 1, $a := $r.age, $b := $r.age + 7, $c := $r.age - 3, $d := $r.age + 1000
        group by $g
        return {"s": sum($a), "s7": sum($b), "mn": min($c), "mx": max($d), "avg": avg($b), "n": count($r),
                "mn0": min($a), "mx7": max($b)}
        """, """
        for $r in jn:doc('json-path1','a.jn')[]
        where $r.age gt -20
        let $g := 1, $a := $r.age, $b := $r.age + 7, $c := $r.age - 3
        group by $g
        return {"s": sum($a), "s7": sum($b), "mn": min($c), "n": count($r), "mx": max($c)}
        """};
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        final SirixCompileChain autoWired = SirixCompileChain.createWithJsonStore(store)) {
      for (final String shape : shapes) {
        final long foldBefore = SirixVectorizedExecutor.constGroupSegmentFoldServedCount();
        final long constantBefore = SirixVectorizedExecutor.constGroupAggServedCount();
        assertEquals(evaluate(generic, ctx, shape), evaluate(autoWired, ctx, shape), shape);
        assertEquals(1L, SirixVectorizedExecutor.constGroupAggServedCount() - constantBefore, "const-group arm");
        assertEquals(1L, SirixVectorizedExecutor.constGroupSegmentFoldServedCount() - foldBefore,
            "segment-fold arm: " + shape);
      }
    }
  }

  /**
   * A resource of {@link #SCAN_RECORDS} records built through the core API without a path summary, so
   * the page-skip registry is the only skip index in play. Returns its registry key.
   */
  private static String buildUnsummarizedResource(final Path location, final String database, final String resource) {
    final Random rng = new Random(13);
    final StringBuilder sb = new StringBuilder(SCAN_RECORDS * 32);
    sb.append('[');
    for (int i = 0; i < SCAN_RECORDS; i++) {
      if (i > 0)
        sb.append(',');
      sb.append("{\"amount\":").append(rng.nextInt(1000)).append('}');
    }
    sb.append(']');

    Databases.createJsonDatabase(new DatabaseConfiguration(location.resolve(database)));
    try (final var db = Databases.openJsonDatabase(location.resolve(database))) {
      db.createResource(ResourceConfiguration.newBuilder(resource).buildPathSummary(false).build());
      try (final var session = db.beginResourceSession(resource); final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(sb.toString()));
        wtx.commit();
        return session.getResourceConfig().getResource().toString();
      }
    }
  }

  /** The document lifted from a parsed query, independent of any store. */
  @Test
  public void theParsedQueryIsWhereTheDocumentComesFrom() {
    final StoreBoundExecutorCache.DocumentSource bare =
        StoreBoundExecutorCache.firstDocumentSource(parse("count(jn:doc('db','res')[])"));
    assertNotNull(bare);
    assertEquals("db", bare.database());
    assertEquals("res", bare.resource());
    assertEquals(-1, bare.revision(), "a bare jn:doc opens the most recent revision");

    final StoreBoundExecutorCache.DocumentSource pinned =
        StoreBoundExecutorCache.firstDocumentSource(parse("count(jn:doc('db','res',7)[])"));
    assertNotNull(pinned);
    assertEquals(7, pinned.revision());

    // jn:open names a document too; jn:collection spans several and must not resolve.
    assertNotNull(StoreBoundExecutorCache.firstDocumentSource(parse("jn:open('db','res')")));
    assertNull(StoreBoundExecutorCache.firstDocumentSource(parse("jn:collection('db')")));
    assertNull(StoreBoundExecutorCache.firstDocumentSource(parse("count(jn:doc('db', $r)[])")));
    assertNull(StoreBoundExecutorCache.firstDocumentSource(parse("1 + 2")));

    // The first in document order wins; the gate declines scans over the others.
    final StoreBoundExecutorCache.DocumentSource ofTwo = StoreBoundExecutorCache.firstDocumentSource(
        parse("count(jn:doc('first','x')[]) + count(jn:doc('second','y')[])"));
    assertNotNull(ofTwo);
    assertEquals("first", ofTwo.database());
  }

  // ---------------------------------------------------------------------------------------------

  private static String sum(final String database, final String resource) {
    return "sum(for $r in jn:doc('" + database + "','" + resource + "')[] return $r.age)";
  }

  private static String count(final String database, final String resource, final int revision) {
    return "count(jn:doc('" + database + "','" + resource + "'," + revision + ")[])";
  }

  private static String countAbove(final String database, final String resource) {
    return "count(for $r in jn:doc('" + database + "','" + resource + "')[] where $r.age gt " + AGE_THRESHOLD
        + " return $r)";
  }

  private static AST parse(final String query) {
    return new JsoniqParser(query).parse();
  }

  private static JsonResourceSession openResource(final BasicJsonDBStore store, final String database,
      final String resource) {
    final JsonDBCollection collection = store.lookup(database);
    return collection.getDatabase().beginResourceSession(resource);
  }

  private static BasicJsonDBStore newStore() {
    return BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
  }

  private static void query(final String query) {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, query).evaluate(ctx);
    }
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext ctx, final String query)
      throws IOException {
    return serialize(new Query(chain, query), ctx);
  }

  private static String serialize(final Query query, final SirixQueryContext ctx) throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final PrintWriter printWriter = new PrintWriter(out)) {
      query.serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }
}
