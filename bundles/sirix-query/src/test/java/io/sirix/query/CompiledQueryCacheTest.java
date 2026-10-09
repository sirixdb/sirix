package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.AnyURI;
import io.brackit.query.function.FunctionExpr;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.api.json.JsonNodeTrx.Commit;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.CompileChain.PlanStage;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Type;
import io.brackit.query.module.Module;
import io.brackit.query.module.MainModule;
import io.brackit.query.util.path.PathParser;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.query.compiler.optimizer.PlanCache;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class CompiledQueryCacheTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearBindings() {
    BoundDocumentHint.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    SequentialPipelineStrategy.clearThreadVectorizedExecutor();
    SequentialPipelineStrategy.setMorselEnabled(false);
  }

  @Test
  void sharedTargetsBindFunctionExpressionsToTheNewAnalysisContext() throws Exception {
    try (final var store = newStore();
         final var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
         final var context = SirixQueryContext.createWithJsonStore(store)) {
      final String text = "declare base-uri 'https://sirix.test/context/'; resolve-uri('value')";
      final Module fresh = chain.compile(text);
      final long hits = chain.planCacheHits();
      final String variant = text + " (: shared optimized target :)";
      final Module shared = chain.compile(variant);
      assertTrue(chain.planCacheHits() > hits, "the variant must reuse the optimized target");
      assertNotSame(fresh.getStaticContext(), shared.getStaticContext());

      final Field staticContext = FunctionExpr.class.getDeclaredField("sctx");
      staticContext.setAccessible(true);
      assertSame(fresh.getStaticContext(), staticContext.get(assertInstanceOf(FunctionExpr.class, fresh.getBody())));
      assertSame(shared.getStaticContext(), staticContext.get(assertInstanceOf(FunctionExpr.class, shared.getBody())),
          "a detached target must inherit this analysis's context before translation");
      for (final Module module : List.of(fresh, shared, chain.compile(variant))) {
        assertEquals("https://sirix.test/context/value",
            assertInstanceOf(AnyURI.class, new Query(module).execute(context)).stringValue());
      }
      assertSame(shared, chain.compile(variant));
    }
  }

  @Test
  void schemaFacadeReturnsTheOriginalBodyAndRevalidatesBeforeEvaluation() {
    try (final var store = newStore();
         final var chain = SirixCompileChain.createWithJsonStore(store);
         final var context = SirixQueryContext.createWithJsonStore(store)) {
      final String text = "for $n in 1 to 3 return $n + $n";
      final Module cached = chain.compile(text);
      final var raw = new MainModule();
      raw.setExpr(cached.getBody());
      final var facade = new SchemaFencedModule(chain, text, PlanCache.indexSchemaVersion(), raw);
      assertSame(raw.getBody(), facade.getBody(), "schema revalidation must not wrap expression evaluation");
      assertEquals("2 4 6", answer(new Query(facade), context));
      PlanCache.signalIndexSchemaChange();
      assertNotSame(raw.getBody(), facade.getBody(), "a retained module must select a new body after schema change");
      assertEquals("2 4 6", answer(new Query(facade), context));
      assertSame(chain.compile(text).getBody(), facade.getBody());
    }
  }

  @Test
  void fullTreeAndPrologPreventPlanCollisions() {
    try (final var store = newStore();
         final var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
         final var context = SirixQueryContext.createWithJsonStore(store)) {
      assertEquals("2 3", answer(chain, context, "for $x in 1 to 2 return $x + 1"));
      assertEquals("11 12", answer(chain, context, "for $x in 1 to 2 return $x + 10"));
      assertEquals("2 3", answer(chain, context, "for $x in 1 to 2 return $x + 1 (: same plan :)"));
      assertTrue(chain.planCacheHits() > 0, "optimizer instances must share their cache");
      assertEquals("8", answer(chain, context, "declare variable $x := 7; $x + 1"));
      assertEquals("12", answer(chain, context, "declare variable $x := 11; $x + 1"));
      assertEquals("12", answer(chain, context, "declare variable $x := 11; $x + 1 (: same plan :)"));
    }
  }

  @Test
  void externalAndInitializedVariablesBelongToEachExecution() {
    try (final var store = newStore();
         final var chain = SirixCompileChain.createWithJsonStore(store);
         final var context = SirixQueryContext.createWithJsonStore(store)) {
      final String text = "declare variable $input external; declare variable $derived := $input + 1;"
          + " for $n in 1 to 3 return $derived + $n";
      final Module first = chain.compile(text);
      context.bind(new QNm("input"), new Int64(10));
      assertEquals("12 13 14", answer(new Query(first), context));
      context.bind(new QNm("input"), new Int64(20));
      final Module again = chain.compile(text);
      assertSame(first, again);
      assertEquals("22 23 24", answer(new Query(again), context));
      assertEquals("22 23 24", answer(new Query(first), context));
    }
  }

  @Test
  void concurrentMissAndLazyExecutionUseIndependentBindings() throws Exception {
    try (final var store = newStore();
         final var chain = SirixCompileChain.createWithJsonStore(store);
         final var workers = Executors.newFixedThreadPool(8)) {
      final String text = "declare variable $input external; declare variable $derived := $input + 1;"
          + " for $n in 1 to 20 return $derived + $n";
      final var barrier = new CyclicBarrier(8);
      final List<Callable<Module>> tasks = new ArrayList<>(8);
      for (int i = 0; i < 8; i++) {
        final int input = i * 100;
        tasks.add(() -> {
          {
            final var context = SirixQueryContext.createWithJsonStore(store);
            context.bind(new QNm("input"), new Int64(input));
            barrier.await();
            final Module module = chain.compile(text);
            final StringBuilder expected = new StringBuilder();
            for (int n = 1; n <= 20; n++) {
              if (n > 1) expected.append(' ');
              expected.append(input + n + 1);
            }
            for (int repetition = 0; repetition < 10; repetition++) {
              assertEquals(expected.toString(), answer(new Query(module), context));
            }
            return module;
          }
        });
      }
      final var results = workers.invokeAll(tasks);
      final Module first = results.getFirst().get();
      for (final var result : results) assertSame(first, result.get());
      assertEquals(1, chain.compiledQueryCount());
    }
  }

  @Test
  void documentHintsAndStoreIdentityFenceCompiledModules() {
    try (final var store = newStore();
         final var chain = SirixCompileChain.createWithJsonStore(store);
         final var other = SirixCompileChain.createWithJsonStore(store);
         final var context = SirixQueryContext.createWithJsonStore(store)) {
      store.create("a", "r", "[10,20]");
      store.create("b", "r", "[1,2,3]");
      final String text = "declare variable $doc external; sum($doc[])";
      context.bind(new QNm("doc"), store.lookup("a").getDocument("r"));
      final Module a = chain.compile(text);
      assertEquals("30", answer(new Query(a), context));
      context.bind(new QNm("doc"), store.lookup("b").getDocument("r"));
      final Module b = chain.compile(text);
      assertNotSame(a, b);
      assertEquals("6", answer(new Query(b), context));
      assertNotSame(b, other.compile(text));
      context.bind(new QNm("doc"), store.lookup("a").getDocument("r"));
      assertSame(a, chain.compile(text));
      assertEquals("30", answer(new Query(a), context));
    }
  }

  @Test
  void coreIndexCreateDropAndCommitInvalidateBothCaches() {
    try (final var store = newStore();
         final var chain = SirixCompileChain.createWithJsonStore(store);
         final var context = SirixQueryContext.createWithJsonStore(store)) {
      final var collection = store.create("db", "r", "[{\"v\":1},{\"v\":2},{\"v\":3}]");
      final var session = collection.getDatabase().beginResourceSession("r");
      final String text = "for $x in jn:doc('db','r')[] where $x.v eq 2 return $x.v";
      final Module scan = chain.compile(text);
      assertEquals("2", answer(new Query(scan), context));
      final IndexDef definition = IndexDefs.createCASIdxDef(false, Type.INR,
          Set.of(new PathParser("/[]/v", PathParser.Type.JSON).parse()), 0, IndexDef.DbType.JSON);
      final long before = PlanCache.indexSchemaVersion();
      try (final var writer = session.beginNodeTrx()) {
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(definition), writer);
        writer.commit();
      }
      assertTrue(PlanCache.indexSchemaVersion() > before);
      final Module indexed = chain.compile(text);
      assertNotSame(scan, indexed);
      assertEquals("2", answer(new Query(indexed), context));
      final long beforeDrop = PlanCache.indexSchemaVersion();
      try (final var writer = session.beginNodeTrx()) {
        session.getWtxIndexController(writer.getRevisionNumber()).dropIndexes(Set.of(definition), writer);
        writer.commit();
      }
      assertTrue(PlanCache.indexSchemaVersion() > beforeDrop);
      final Module dropped = chain.compile(text);
      assertNotSame(indexed, dropped);
      assertEquals("2", answer(new Query(dropped), context));
      assertEquals("2", answer(new Query(indexed), context), "retained prepared queries must also honour index drops");
      try (final var writer = session.beginNodeTrx()) {
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"v\":2}"), Commit.NO);
        writer.commit();
      }
      final Module changed = chain.compile(text);
      assertNotSame(dropped, changed, "commits can change path-summary routing even without index DDL");
      assertEquals("2 2", answer(new Query(changed), context));
      assertSame(changed, chain.compile(text));
    }
  }

  @Test
  void schemaChangeDuringCompilationCannotPublishTheOldGeneration() throws Exception {
    try (final var store = newStore(); final var worker = Executors.newSingleThreadExecutor()) {
      store.create("db", "r", "[{\"v\":1},{\"v\":2}]");
      final var lookupEntered = new CountDownLatch(1);
      final var continueLookup = new CountDownLatch(1);
      final var firstLookup = new AtomicBoolean(true);
      final JsonDBStore observed = (JsonDBStore) Proxy.newProxyInstance(JsonDBStore.class.getClassLoader(),
          new Class<?>[] {JsonDBStore.class}, (proxy, method, arguments) -> {
            if (method.getName().equals("lookup") && firstLookup.compareAndSet(true, false)) {
              lookupEntered.countDown();
              continueLookup.await();
            }
            try {
              return method.invoke(store, arguments);
            } catch (final InvocationTargetException exception) {
              throw exception.getCause();
            }
          });
      try (final var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(observed);
           final var context = SirixQueryContext.createWithJsonStore(store)) {
        final String text = "for $r in jn:doc('db','r')[] where $r.v eq 2 return $r.v";
        final var pending = worker.submit(() -> chain.compile(text));
        lookupEntered.await();
        PlanCache.signalIndexSchemaChange();
        continueLookup.countDown();
        final Module raced = pending.get();
        assertEquals(0, chain.compiledQueryCount(), "compilation straddling a schema change must not be published");
        final Module stable = chain.compile(text);
        assertNotSame(raced, stable);
        assertEquals("2", answer(new Query(stable), context));
        assertSame(stable, chain.compile(text));
      }
    }
  }

  @Test
  void explicitExecutorAndMorselBindingsCannotReuseTheWrongPlan() {
    try (final var store = newStore();
         final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.create("db", "r", "[1,2,3]");
      final Module normal = chain.compile("1 + 1");
      final var executor = new SirixVectorizedExecutor(collection.getDatabase().beginResourceSession("r"), 1);
      try {
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        assertNotSame(normal, chain.compile("1 + 1"));
        assertNotSame(chain.compile("1 + 1"), chain.compile("1 + 1"));
      } finally {
        executor.close();
      }
      SequentialPipelineStrategy.setVectorizedExecutor(null);
      SequentialPipelineStrategy.setMorselEnabled(true);
      final Module morsel = chain.compile("1 + 1");
      assertNotSame(normal, morsel);
      SequentialPipelineStrategy.setMorselEnabled(false);
      assertSame(normal, chain.compile("1 + 1"));
    }
  }

  @Test
  void boundedLruCloseAndOversizedQueriesReleaseEntries() {
    try (final var store = newStore(); final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final Module eldest = chain.compile("1 + 1");
      final Module fourth = chain.compile("4 + 1");
      for (int i = 2; i <= CompiledQueryCache.MAX_ENTRIES; i++) chain.compile(i + " + 1");
      final Module second = chain.compile("2 + 1");
      assertSame(eldest, chain.compile("1 + 1"), "a hit must refresh LRU order");
      final Module third = chain.compile("3 + 1");
      chain.compile("129 + 1");
      assertEquals(CompiledQueryCache.MAX_ENTRIES, chain.compiledQueryCount());
      assertSame(eldest, chain.compile("1 + 1"));
      assertSame(second, chain.compile("2 + 1"));
      assertSame(third, chain.compile("3 + 1"));
      assertNotSame(fourth, chain.compile("4 + 1"), "least recently used entry must be evicted");
      chain.compile("130 + 1");
      assertEquals(CompiledQueryCache.MAX_ENTRIES, chain.compiledQueryCount());
      final String oversized = "1 (:" + "x".repeat(CompiledQueryCache.MAX_QUERY_LENGTH) + ":)";
      assertNotSame(chain.compile(oversized), chain.compile(oversized));
      chain.close();
      assertEquals(0, chain.compiledQueryCount());
      assertThrows(IllegalStateException.class, () -> chain.compile("1"));
      assertThrows(NullPointerException.class, () -> chain.compile(null));
    }
  }

  @Test
  void descendantExpressionsDoNotShareMutablePathMatchesAcrossResourcesOrWorkers() throws Exception {
    try (final var store = newStore();
         final var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
         final var workers = Executors.newFixedThreadPool(4)) {
      store.create("a", "r", "{\"a\":{\"needle\":7},\"b\":0}");
      store.create("b", "r", "{\"a\":{\"other\":4},\"z\":{\"needle\":13}}");
      final Query query = new Query(chain, "declare variable $doc external; $doc=>>needle");
      final List<Callable<String>> tasks = new ArrayList<>(4);
      for (int i = 0; i < 4; i++) {
        tasks.add(() -> {
          // Context.close closes the shared store; the outer owner closes it after all workers join.
          final var context = SirixQueryContext.createWithJsonStore(store);
          for (int repetition = 0; repetition < 20; repetition++) {
            for (final String database : List.of("a", "b")) {
              final var document = store.lookup(database).getDocument("r");
              context.bind(new QNm("doc"), document);
              assertEquals(database.equals("a") ? "7" : "13", answer(query, context));
              document.getTrx().close();
            }
          }
          BoundDocumentHint.clear();
          return "done";
        });
      }
      for (final var result : workers.invokeAll(tasks)) assertEquals("done", result.get());
    }
  }

  @Test
  void importedModulesBypassTextCachingAndLocalFunctionsKeepTheirOwnBindings() {
    try (final var store = newStore();
         final var chain = SirixCompileChain.createWithJsonStore(store);
         final var context = SirixQueryContext.createWithJsonStore(store)) {
      chain.compile("module namespace m = 'urn:cache-test'; declare function m:f() { 7 }; ");
      final String imported = "import module namespace m = 'urn:cache-test'; m:f()";
      final Module first = chain.compile(imported);
      assertNotSame(first, chain.compile(imported));
      assertEquals("7", answer(new Query(first), context));
      final String local = "declare variable $input external; declare function local:f($n) {$n + 1}; local:f($input)";
      final Module function = chain.compile(local);
      context.bind(new QNm("input"), new Int64(3));
      assertEquals("4", answer(new Query(function), context));
      context.bind(new QNm("input"), new Int64(9));
      assertSame(function, chain.compile(local));
      assertEquals("10", answer(new Query(function), context));
    }
  }

  @Test
  void diagnosticsDescribeTheLastQueryEvenOnACacheHit() {
    try (final var store = newStore(); final var chain = SirixCompileChain.createWithJsonStore(store)) {
      chain.compile("1 + 1");
      final String first = chain.getPlanAsJSON(PlanStage.BOTH);
      chain.compile("2 + 2");
      chain.compile("1 + 1");
      assertEquals(first, chain.getPlanAsJSON(PlanStage.BOTH));
      chain.getParsedAST().setValue("mutated by diagnostics consumer");
      assertEquals(first, chain.getPlanAsJSON(PlanStage.BOTH));
    }
  }

  private BasicJsonDBStore newStore() {
    return BasicJsonDBStore.newBuilder().location(directory).build();
  }

  private static String answer(final SirixCompileChain chain, final SirixQueryContext context, final String text) {
    return answer(new Query(chain, text), context);
  }

  private static String answer(final Query query, final SirixQueryContext context) {
    final StringWriter output = new StringWriter();
    query.serialize(context, new PrintWriter(output));
    return output.toString();
  }
}
