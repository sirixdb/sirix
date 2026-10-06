package io.sirix.query.function.jn.index;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexDef;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.function.sdb.explain.QueryPlan;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBObject;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

final class RuntimeRevisionCASTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void historicalFilterKeepsTimestamp(final VersioningType versioning) {
    create(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String suffix : List.of("root')[]", "nested').rows[]")) {
        final String source = "jn:open('cas','" + suffix.replace("')", "',xs:dateTime('2020-01-01T00:00:00Z'))");
        final Query query = new Query(chain, source + "[?$$.id eq 1].value");
        assertEquals(List.of(10L), values(query.execute(context)));
        assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr), "CAS filter must route through the index");
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void flworKeepsResidualAndEvaluatesRevisionPerTuple(final VersioningType versioning) {
    create(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String text = "for $e in [{\"ts\":\"2020-01-01T00:00:00Z\"},{\"ts\":\"2021-01-01T00:00:00Z\"}][] "
          + "for $c in jn:open('cas','root',xs:dateTime($e.ts))[] "
          + "where $c.id eq 1 and $c.value lt 15 return $c.value";
      final Query query = new Query(chain, text);
      assertEquals(List.of(10L), values(query.execute(context)));
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr), "FLWOR must route through CAS");
      assertTrue(contains(chain.getOptimizedAST(), XQ.Selection), "residual must survive");
      final Query both = new Query(chain, text.replace(" and $c.value lt 15", ""));
      assertEquals(List.of(10L, 20L), values(both.execute(context)));
    }
  }

  @Test
  void prologAndIntegerRevisionOperandsStayRuntimeBound() {
    create(VersioningType.FULL);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final Query prolog = new Query(chain, "declare variable $T := xs:dateTime('2020-06-01T00:00:00Z'); "
          + "for $c in jn:open('cas','root',$T)[] where $c.id eq 1 return $c.value");
      assertEquals(List.of(10L), values(prolog.execute(context)));
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
      final Query integer =
          new Query(chain, "for $r in (1,2,1) for $c in jn:doc('cas','root',$r)[] where $c.id eq 1 return $c.value");
      assertEquals(List.of(10L, 20L, 10L), values(integer.execute(context)));
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
      final Query before = new Query(chain,
          "for $c in jn:open('cas','root',xs:dateTime('2019-01-01T00:00:00Z'))[] where $c.id eq 1 return $c.value");
      assertEquals(List.of(), values(before.execute(context)));
    }
  }

  @Test
  void revisionBeforeIndexCreationFallsBackToItsOwnData() {
    final Path databasePath = directory.resolve("cas");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("root").customCommitTimestamps(true).build());
      try (var session = database.beginResourceSession("root"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json(false, 10)), JsonNodeTrx.Commit.NO);
        writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.INR,
                   Set.of(parse("/[]/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)), writer);
        writer.commit(null, Instant.parse("2021-01-01T00:00:00Z"));
      }
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final Query query = new Query(chain, "for $c in jn:open('cas','root',xs:dateTime('2020-01-01T00:00:00Z'))[] "
          + "where $c.id eq 1 and $c.value gt 5 return $c.value");
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
      assertEquals(List.of(10L), values(query.execute(context)));
    }
  }

  @Test
  void revisionOperandsRetainFunctionArgumentConversion() {
    create(VersioningType.FULL);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String source : List.of("jn:open('cas','root',xs:untypedAtomic('2020-01-01T00:00:00Z'))",
          "jn:doc('cas','root',xs:untypedAtomic('1'))", "jn:doc('cas','root',xs:decimal('1.9'))")) {
        final Query query = new Query(chain, "for $c in " + source + "[] where $c.id eq 1 return $c.value");
        assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
        assertEquals(List.of(10L), values(query.execute(context)));
      }
      for (final String source : List.of("jn:open('cas','root','2020-01-01T00:00:00Z')",
          "jn:doc('cas','root',xs:integer('4294967297'))")) {
        assertThrows(QueryException.class, () -> values(
            new Query(chain, "for $c in " + source + "[] where $c.id eq 1 return $c.value").execute(context)));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void insertedCandidatesRetainArrayOrder(final boolean deweyIDs) {
    create(VersioningType.FULL, deweyIDs);
    try (var database = Databases.openJsonDatabase(directory.resolve("cas"));
        var session = database.beginResourceSession("root");
        var writer = session.beginNodeTrx()) {
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":1,\"value\":30}"),
          JsonNodeTrx.Commit.NO);
      writer.commit(null, Instant.parse("2022-01-01T00:00:00Z"));
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final Query query = new Query(chain, "for $c in jn:doc('cas','root')[] where $c.id eq 1 return $c.value");
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
      assertEquals(List.of(30L, 20L), values(query.execute(context)));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"root", "nested"})
  void nestedProjectionsRemainRelativeToCASRows(final String resource) {
    create(VersioningType.FULL);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String source = "jn:doc('cas','" + resource + "')" + (resource.equals("root") ? "[]" : ".rows[]");
      for (final String projection : List.of("$c.details.value", "$c.details.values[0]",
          "let $d := $c.details return $d.value",
          "for $d in $c.details.values[] where $d eq 20 return $d")) {
        final String text = "for $c in " + source
            + " where $c.id eq 1 and $c.details.value eq 20 return " + projection;
        final Query query = new Query(chain, text);
        assertEquals(List.of(20L), values(query.execute(context)), projection);
        final QueryPlan plan = QueryPlan.explain(text, store, null);
        assertTrue(plan.usesIndex());
        assertEquals("CAS", plan.indexType());
      }
      final String filter = source + "[?$$.id eq 1].details.value";
      assertEquals(List.of(20L), values(new Query(chain, filter).execute(context)));
      final QueryPlan plan = QueryPlan.explain(filter, store, null);
      assertTrue(plan.usesIndex());
      assertEquals("CAS", plan.indexType());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"flwor", "filter", "legacy"})
  void parallelCandidatesKeepIndependentWorkerCursors(final String shape) throws Exception {
    final int count = 2_048;
    createCandidates(count);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var sequential = SirixCompileChain.createWithJsonStore(store);
        var parallel = SirixCompileChain.createParallel(null, store);
        var workers = Executors.newFixedThreadPool(2)) {
      final String text = switch (shape) {
        case "flwor" -> "for $c in jn:doc('cas','root')[] where $c.id eq 1 return $c";
        case "filter" -> "jn:doc('cas','root')[][?$$.id eq 1]";
        case "legacy" -> "jn:doc('cas','nested')[].item[?$$.id ge 1]";
        default -> throw new AssertionError(shape);
      };
      final QueryPlan plan = QueryPlan.explain(text, store, null);
      assertTrue(plan.usesIndex(), plan::toJSON);
      assertEquals("CAS", plan.indexType());
      final List<JsonDBObject> rows = new ArrayList<>(count);
      try (var iter = new Query(parallel, text).execute(context).iterate()) {
        Item item;
        while ((item = iter.next()) != null) {
          rows.add((JsonDBObject) item);
        }
      }
      assertEquals(count, rows.size());
      final JsonDBObject first = rows.getFirst();
      final JsonDBObject last = rows.getLast();
      final long firstKey = first.getNodeKey();
      final long lastKey = last.getNodeKey();
      final CountDownLatch firstPositioned = new CountDownLatch(1);
      final CountDownLatch lastPositioned = new CountDownLatch(1);
      final CountDownLatch firstRead = new CountDownLatch(1);
      final Future<?> firstResult = workers.submit(() -> {
        try {
          assertTrue(first.getTrx().moveTo(firstKey));
          firstPositioned.countDown();
          lastPositioned.await();
          assertEquals(firstKey, first.getTrx().getNodeKey());
          assertEquals(0, ((Numeric) first.get(new QNm("value"))).intValue());
        } finally {
          firstPositioned.countDown();
          firstRead.countDown();
        }
        return null;
      });
      final Future<?> lastResult = workers.submit(() -> {
        firstPositioned.await();
        try {
          assertTrue(last.getTrx().moveTo(lastKey));
        } finally {
          lastPositioned.countDown();
        }
        firstRead.await();
        assertEquals(lastKey, last.getTrx().getNodeKey());
        assertEquals(count - 1, ((Numeric) last.get(new QNm("value"))).intValue());
        return null;
      });
      firstResult.get();
      lastResult.get();
      final String projection = "for $c in (" + text + ") return $c.value";
      final List<Long> expected = new ArrayList<>(count);
      for (long i = 0; i < count; i++) {
        expected.add(i);
      }
      assertEquals(expected, values(new Query(sequential, projection).execute(context)));
      assertEquals(expected, values(new Query(parallel, projection).execute(context)));
    }
  }

  @Test
  void parallelCorrelatedTimestampsKeepTheirHistoricalValues() {
    create(VersioningType.FULL);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createParallel(null, store)) {
      final String text = "sum(for $i in 1 to 4096 "
          + "let $t := if ($i mod 2 eq 0) then xs:dateTime('2020-01-01T00:00:00Z') "
          + "else xs:dateTime('2021-01-01T00:00:00Z') "
          + "for $c in jn:open('cas','root',$t)[] where $c.id eq 1 return $c.value)";
      assertEquals(61_440, ((Numeric) new Query(chain, text).evaluate(context)).intValue());
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
    }
  }

  @Test
  void concurrentRevisionMissIsResolvedOnceBeforePublicationInvalidation() throws Exception {
    final JsonResourceSession session = mock(JsonResourceSession.class);
    final AtomicInteger head = new AtomicInteger(1);
    final AtomicInteger resolutions = new AtomicInteger();
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Instant instant = Instant.parse("2030-01-01T00:00:00Z");
    doAnswer(call -> head.get()).when(session).getMostRecentRevisionNumber();
    doAnswer(call -> {
      if (resolutions.getAndIncrement() == 0) {
        entered.countDown();
        release.await();
      }
      return head.get();
    }).when(session).getRevisionNumber(instant);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store)) {
      final FutureTask<Integer> first = new FutureTask<>(() -> context.resolveRevision(session, instant));
      final FutureTask<Integer> second = new FutureTask<>(() -> context.resolveRevision(session, instant));
      final Thread firstThread = Thread.ofPlatform().unstarted(first);
      final Thread secondThread = Thread.ofPlatform().unstarted(second);
      final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
      firstThread.start();
      try {
        entered.await();
        secondThread.start();
        while (!second.isDone()) {
          final ThreadInfo info = threads.getThreadInfo(secondThread.threadId());
          if (info != null && info.getLockOwnerId() == firstThread.threadId() && info.getLockInfo() != null
              && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(context)) {
            break;
          }
          Thread.onSpinWait();
        }
      } finally {
        release.countDown();
        firstThread.join();
        secondThread.join();
      }
      assertEquals(1, first.get().intValue());
      assertEquals(1, second.get().intValue());
      assertEquals(1, resolutions.get());
      head.set(2);
      assertEquals(2, context.resolveRevision(session, instant));
      assertEquals(2, resolutions.get());
    }
  }

  @Test
  void sharedRevisionMemoKeepsConcurrentInstantsAndPublicationInvalidation() throws Exception {
    final JsonResourceSession session = mock(JsonResourceSession.class);
    final AtomicInteger head = new AtomicInteger(512);
    doAnswer(call -> head.get()).when(session).getMostRecentRevisionNumber();
    doAnswer(call -> (int) call.getArgument(0, Instant.class).getEpochSecond() + head.get() - 511)
        .when(session).getRevisionNumber(any(Instant.class));
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var workers = Executors.newFixedThreadPool(8)) {
      for (int publication = 0; publication < 2; publication++) {
        head.set((publication + 1) * 512);
        final int offset = publication * 512;
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<?>> results = new ArrayList<>(8);
        for (int worker = 0; worker < 8; worker++) {
          final int lane = worker;
          results.add(workers.submit(() -> {
            start.await();
            for (int i = 0; i < 2_048; i++) {
              final int key = (i * 31 + lane) & 511;
              assertEquals(key + offset + 1, context.resolveRevision(session, Instant.ofEpochSecond(key)));
            }
            return null;
          }));
        }
        start.countDown();
        for (final Future<?> result : results) {
          result.get();
        }
      }
      final Instant instant = Instant.ofEpochSecond(1_000);
      doReturn(1_024).when(session).getRevisionNumber(instant);
      assertEquals(1_024, context.resolveRevision(session, instant));
      assertEquals(1_024, context.resolveRevision(session, instant));
      verify(session, times(1)).getRevisionNumber(instant);
      head.set(1_025);
      doReturn(1_025).when(session).getRevisionNumber(instant);
      assertEquals(1_025, context.resolveRevision(session, instant));
      verify(session, times(2)).getRevisionNumber(instant);
    }
  }

  private void createCandidates(final int count) {
    final StringBuilder rows = new StringBuilder(count * 24).append('[');
    final int nestedCount = count * 5;
    final StringBuilder nestedRows = new StringBuilder(nestedCount * 34).append('[');
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        rows.append(',');
        nestedRows.append(',');
      }
      rows.append("{\"id\":1,\"value\":").append(i).append('}');
      nestedRows.append("{\"item\":{\"id\":1,\"value\":").append(i).append("}}");
    }
    rows.append(']');
    for (int i = count; i < nestedCount; i++) {
      nestedRows.append(",{\"item\":{\"id\":0,\"value\":").append(i).append("}}");
    }
    nestedRows.append(']');
    Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("cas")));
    try (var database = Databases.openJsonDatabase(directory.resolve("cas"))) {
      for (final String resource : List.of("root", "nested")) {
        database.createResource(ResourceConfiguration.newBuilder(resource).useDeweyIDs(true).storeDiffs(false).build());
        try (var session = database.beginResourceSession(resource); var writer = session.beginNodeTrx()) {
          final boolean nested = resource.equals("nested");
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(nested
              ? nestedRows.toString()
              : rows.toString()), JsonNodeTrx.Commit.NO);
          final IndexDef index = IndexDefs.createCASIdxDef(false, Type.INR,
              Set.of(parse(nested ? "/[]/item/id" : "/[]/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
          session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(index), writer);
          writer.commit();
        }
      }
    }
  }

  private void create(final VersioningType versioning) {
    create(versioning, false);
  }

  private void create(final VersioningType versioning, final boolean deweyIDs) {
    final Path databasePath = directory.resolve("cas");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      for (final String resource : List.of("root", "nested")) {
        database.createResource(ResourceConfiguration.newBuilder(resource)
                                                     .versioningApproach(versioning)
                                                     .customCommitTimestamps(true)
                                                     .storeDiffs(false)
                                                     .useDeweyIDs(deweyIDs)
                                                     .build());
        try (var session = database.beginResourceSession(resource); var writer = session.beginNodeTrx()) {
          final boolean nested = resource.equals("nested");
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json(nested, 10)), JsonNodeTrx.Commit.NO);
          final var index = IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse(nested
              ? "/rows/[]/id"
              : "/[]/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
          session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(index), writer);
          writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
          writer.moveToDocumentRoot();
          writer.moveToFirstChild();
          writer.remove();
          writer.moveToDocumentRoot();
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json(nested, 20)), JsonNodeTrx.Commit.NO);
          writer.commit(null, Instant.parse("2021-01-01T00:00:00Z"));
        }
      }
    }
  }

  private static String json(final boolean nested, final int value) {
    final String rows = "[{\"id\":1,\"value\":" + value + ",\"details\":{\"value\":" + value
        + ",\"values\":[" + value + "," + (value + 1) + "]}},{\"id\":2,\"value\":5}]";
    return nested
        ? "{\"rows\":" + rows + "}"
        : rows;
  }

  private static List<Long> values(final Sequence sequence) {
    final List<Long> result = new ArrayList<>();
    if (sequence != null) {
      try (var iter = sequence.iterate()) {
        Item item;
        while ((item = iter.next()) != null) {
          result.add(((Numeric) item).longValue());
        }
      }
    }
    return result;
  }

  private static boolean contains(final AST node, final int type) {
    if (node.getType() == type) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (contains(node.getChild(i), type)) {
        return true;
      }
    }
    return false;
  }
}
