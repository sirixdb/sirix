package io.sirix.query.function.jn.io;

import io.brackit.query.Query;
import io.sirix.access.MutationGate;
import io.brackit.query.QueryException;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.MutationAuthorizer;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.SirixQueryContext.CommitStrategy;
import io.sirix.query.compiler.optimizer.PlanCache;
import io.sirix.query.compiler.optimizer.stats.Histogram;
import io.sirix.query.compiler.optimizer.stats.HistogramCollector;
import io.sirix.query.compiler.optimizer.stats.StatisticsCatalog;
import io.sirix.query.function.jn.JNFun;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.XmlDBStore;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

/**
 * {@code jn:store} and {@code jn:load} into an existing database (#1384): a new resource name adds
 * the resource; an existing name is replaced alone when {@code $create-new} is true and refused
 * when it is false. No call drops the database, and every other resource keeps its content,
 * revisions and indexes.
 */
final class StoreIntoExistingDatabaseTest {
  private static final String OTHER = "jn:doc('db','other')";

  @TempDir
  Path directory;

  private Path content;

  @BeforeEach
  void writeInputFiles() throws IOException {
    content = Files.createDirectories(directory.resolve("input"));
    Files.writeString(content.resolve("new.json"), "{\"v\":\"new\"}");
    Files.writeString(content.resolve("second.json"), "{\"v\":\"second\"}");
    Files.writeString(content.resolve("third.json"), "{\"v\":\"third\"}");
  }

  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void aNewResourceNameAddsTheResourceAndKeepsTheRest(final String function) {
    for (final String flag : new String[] {"", ",true()", ",false()"}) {
      withDatabase(function + flag, (chain, context, store) -> {
        final Witness before = witness(chain, context, store);
        run(chain, context, call(function, "'added'", "new", flag));
        assertEquals(List.of("added", "first", "other"), resources(store));
        assertEquals("{\"v\":\"new\"}", run(chain, context, "jn:doc('db','added')"));
        assertEquals(before, witness(chain, context, store), function + flag);
      });
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void anExistingNameIsReplacedAloneWithANewHistoryWhenCreateNewIsTrue(final String function) {
    for (final String flag : new String[] {"", ",true()"}) {
      withDatabase(function + flag, (chain, context, store) -> {
        // The query context already holds a session on the resource it replaces.
        assertEquals("{\"v\":\"first-2\"}", run(chain, context, "jn:doc('db','first')"));
        final Witness before = witness(chain, context, store);
        run(chain, context, call(function, "'first'", "new", flag));
        assertEquals(List.of("first", "other"), resources(store));
        assertEquals("{\"v\":\"new\"}", run(chain, context, "jn:doc('db','first')"));
        assertEquals(1, revisions(store, "first"), "a replaced resource starts a new history");
        assertEquals(before, witness(chain, context, store), function + flag);
      });
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void anExistingNameIsRefusedUnchangedWhenCreateNewIsFalse(final String function) {
    withDatabase(function, (chain, context, store) -> {
      final Witness before = witness(chain, context, store);
      final QueryException refused =
          assertThrows(QueryException.class, () -> run(chain, context, call(function, "'first'", "new", ",false()")));
      assertEquals(JNFun.ERR_RESOURCE_EXISTS, refused.getCode(), refused.getMessage());
      assertEquals(List.of("first", "other"), resources(store));
      assertEquals("{\"v\":\"first-2\"}", run(chain, context, "jn:doc('db','first')"));
      assertEquals(2, revisions(store, "first"));
      assertEquals(before, witness(chain, context, store));
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void severalItemsAreAddedUnderNewNamesWithoutReplacingAnything(final String function) {
    withDatabase(function, (chain, context, store) -> {
      final Witness before = witness(chain, context, store);
      final String items = "store".equals(function)
          ? "('{\"v\":\"second\"}','{\"v\":\"third\"}')"
          : "('" + content.resolve("second.json").toUri() + "','" + content.resolve("third.json").toUri() + "')";
      run(chain, context, "jn:" + function + "('db',()," + items + ")");
      final List<String> names = resources(store);
      assertEquals(4, names.size(), names.toString());
      assertEquals(2, revisions(store, "first"));
      assertEquals(before, witness(chain, context, store));
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void aMissingDatabaseIsCreated(final String function) {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory.resolve("fresh")).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      run(chain, context, call(function, "'first'", "new", ",false()").replace("'db'", "'created'"));
      assertEquals("{\"v\":\"new\"}", run(chain, context, "jn:doc('created','first')"));
    }
  }

  /**
   * Replacing discards the resource's history, so it needs write and delete permission besides the
   * create permission; a refusal leaves the resource as it was.
   */
  @ParameterizedTest
  @CsvSource({"store,write", "store,delete", "load,write", "load,delete"})
  void replacingNeedsWriteAndDeletePermission(final String function, final String refusedPermission) {
    withDatabase(function + refusedPermission, (chain, context, store) -> {
      final XmlDBStore refusing = authorizer();
      final SecurityException refusal = new SecurityException(refusedPermission + " refused");
      if ("write".equals(refusedPermission)) {
        doThrow(refusal).when((MutationAuthorizer) refusing).authorizeWrite(any(), any());
      } else {
        doThrow(refusal).when((MutationAuthorizer) refusing).authorizeDelete(any());
      }
      final QueryException refused = assertThrows(QueryException.class,
          () -> runAuthorized(store, refusing, call(function, "'first'", "new", "")));
      assertSame(refusal, refused.getCause(), refused.getMessage());
      assertEquals("{\"v\":\"first-2\"}", run(chain, context, "jn:doc('db','first')"));
      assertEquals(2, revisions(store, "first"));

      final XmlDBStore granting = authorizer();
      runAuthorized(store, granting, call(function, "'first'", "new", ""));
      verify((MutationAuthorizer) granting).authorizeWrite("db", "first");
      verify((MutationAuthorizer) granting).authorizeDelete("db");
      assertEquals(1, revisions(store, "first"));
    });
  }

  /** A context store that authorizes mutations, as the REST stores are; the JSON store is real. */
  private static XmlDBStore authorizer() {
    return mock(XmlDBStore.class, withSettings().extraInterfaces(MutationAuthorizer.class));
  }

  private static void runAuthorized(final BasicJsonDBStore store, final XmlDBStore authorizer, final String query) {
    try (final SirixCompileChain chain = SirixCompileChain.createWithNodeAndJsonStore(authorizer, store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(authorizer,
            store, CommitStrategy.AUTO)) {
      new Query(chain, query).execute(context);
    }
  }

  /** jn:load returns the collection it loaded into, whether it created it or added to it. */
  @Test
  void loadReturnsTheCollectionItLoadedInto() {
    withDatabase("load-result", (chain, context, store) -> {
      assertEquals("true", run(chain, context, "exists(" + call("load", "'added'", "new", "") + ")"));
      assertEquals("true",
          run(chain, context, "exists(" + call("load", "'first'", "new", "").replace("'db'", "'created'") + ")"));
    });
  }

  /**
   * A caller that finds the database missing creates it under the exclusive database hold and looks
   * again once it holds it: a database another caller created meanwhile is written into, not dropped
   * and recreated.
   */
  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void aDatabaseCreatedByARacingCallerIsKept(final String function) throws Exception {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory.resolve(function)).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final Path database = store.databaseDirectory("db");
      final Thread caller;
      try (var gate = MutationGate.acquireDatabase(database)) {
        caller = new Thread(() -> run(chain, context, call(function, "'added'", "new", "")));
        caller.start();
        // Without the hold the caller would create the database now; with it, it parks.
        while (caller.getState() != Thread.State.WAITING && caller.isAlive()) {
          Thread.onSpinWait();
        }
        store.create("db", "racer", "{\"v\":\"racer\"}");
      }
      caller.join();
      assertEquals(List.of("added", "racer"), resources(store));
      assertEquals("{\"v\":\"racer\"}", run(chain, context, "jn:doc('db','racer')"));
    }
  }

  /**
   * A replacement starts a new history, so statistics of the replaced resource are dropped and cached
   * plans are invalidated (its indexes are gone); another resource's statistics are kept.
   */
  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void replacingDropsTheReplacedResourcesStatisticsAndInvalidatesPlans(final String function) throws IOException {
    Files.writeString(content.resolve("numbers.json"), "{\"n\":[7,8]}");
    final StatisticsCatalog catalog = StatisticsCatalog.getInstance();
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(directory.resolve(function)).buildPathSummary(true).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      run(chain, context, "jn:store('stats','first','{\"n\":[1,2]}')");
      run(chain, context, "jn:store('stats','other','{\"n\":[5,6]}',false())");
      final HistogramCollector collector = new HistogramCollector(store);
      assertTrue(collector.collectAndRegister("stats", "first", "n"));
      assertTrue(collector.collectAndRegister("stats", "other", "n"));
      final Histogram kept = catalog.get("stats", "other", "n");
      final long version = PlanCache.indexSchemaVersion();
      final String input = "store".equals(function)
          ? "'{\"n\":[7,8]}'"
          : "'" + content.resolve("numbers.json").toUri() + "'";
      run(chain, context, "jn:" + function + "('stats','first'," + input + ")");
      assertNull(catalog.get("stats", "first", "n"), "statistics of the replaced history must be dropped");
      assertSame(kept, catalog.get("stats", "other", "n"));
      assertTrue(PlanCache.indexSchemaVersion() > version, "plans using the replaced resource's indexes are stale");
    } finally {
      catalog.invalidateDatabase("stats");
    }
  }

  /** Everything the call must leave alone: the other resource's content, history and index. */
  private record Witness(String content, int revisions, String index) {
  }

  private Witness witness(final SirixCompileChain chain, final SirixQueryContext context,
      final BasicJsonDBStore store) {
    return new Witness(run(chain, context, OTHER), revisions(store, "other"),
        run(chain, context, "jn:find-path-index(" + OTHER + ",'/v')"));
  }

  private String call(final String function, final String resource, final String file, final String flag) {
    final String input = "store".equals(function)
        ? "'{\"v\":\"" + file + "\"}'"
        : "'" + content.resolve(file + ".json").toUri() + "'";
    return "jn:" + function + "('db'," + resource + "," + input + flag + ")";
  }

  private interface Body {
    void run(SirixCompileChain chain, SirixQueryContext context, BasicJsonDBStore store);
  }

  /**
   * Database {@code db}: {@code first} with two revisions, {@code other} with two and a path index.
   */
  private void withDatabase(final String name, final Body body) {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory.resolve(name)).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      run(chain, context, "jn:store('db','first','{\"v\":\"first-1\"}')");
      run(chain, context, "jn:store('db','other','{\"v\":\"other-1\"}',false())");
      run(chain, context, "replace json value of jn:doc('db','first').v with 'first-2'");
      run(chain, context, "replace json value of " + OTHER + ".v with 'other-2'");
      run(chain, context,
          "let $doc := " + OTHER + " let $i := jn:create-path-index($doc,'/v') return sdb:commit($doc)");
      body.run(chain, context, store);
    }
  }

  private static List<String> resources(final BasicJsonDBStore store) {
    final JsonDBCollection collection = store.lookup("db");
    return collection.getDatabase()
                     .listResources()
                     .stream()
                     .map(path -> path.getFileName().toString())
                     .sorted()
                     .toList();
  }

  private static int revisions(final BasicJsonDBStore store, final String resource) {
    final JsonResourceSession session = store.lookup("db").getDatabase().beginResourceSession(resource);
    return session.getMostRecentRevisionNumber();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String query) {
    final StringWriter out = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(out)) {
      new Query(chain, query).serialize(context, writer);
    }
    return out.toString();
  }
}
