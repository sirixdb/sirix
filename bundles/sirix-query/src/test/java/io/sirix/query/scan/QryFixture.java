/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.DblNumeric;
import io.brackit.query.atomic.DecNumeric;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Object;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.settings.VersioningType;

import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One database of QRY fixtures and the two ways of evaluating a query against it: the
 * <em>interpreter</em> (brackit's generic pipeline, no executor installed, auto-wiring off; the
 * only oracle of the chapter) and the <em>route</em> (a {@link SirixVectorizedExecutor} bound to
 * one revision with a fixed thread count, installed for the query).
 *
 * <p>
 * Queries name their source as {@code SRC}; {@link #src} and {@link #srcAt} expand it to the
 * {@code jn:doc} of a resource.
 */
public final class QryFixture {

  /** The placeholder a query uses for its source. */
  public static final String SRC = "SRC";

  private final Path dir;
  private final String db;
  private final VersioningType versioning;
  private final StorageType storage;
  private final boolean pathSummary;

  public QryFixture(final Path dir, final String db) {
    this(dir, db, VersioningType.FULL, null, true);
  }

  /**
   * @param storage the storage backend, or {@code null} for the platform default
   * @param pathSummary whether resources are stored with a path summary
   */
  public QryFixture(final Path dir, final String db, final VersioningType versioning, final StorageType storage,
      final boolean pathSummary) {
    this.dir = Objects.requireNonNull(dir);
    this.db = Objects.requireNonNull(db);
    this.versioning = Objects.requireNonNull(versioning);
    this.storage = storage;
    this.pathSummary = pathSummary;
  }

  public String db() {
    return db;
  }

  /** {@code jn:doc(db, resource)[]}. */
  public String src(final String resource) {
    return "jn:doc('" + db + "','" + resource + "')[]";
  }

  /** {@code jn:doc(db, resource, revision)[]}. */
  public String srcAt(final String resource, final int revision) {
    return "jn:doc('" + db + "','" + resource + "'," + revision + ")[]";
  }

  /** {@code jn:open(db, resource, xs:dateTime(instant))[]}. */
  public String srcAtTime(final String resource, final Instant instant) {
    return "jn:open('" + db + "','" + resource + "',xs:dateTime('" + instant + "'))[]";
  }

  BasicJsonDBStore newStore() {
    final BasicJsonDBStore.Builder builder =
        BasicJsonDBStore.newBuilder().location(dir).buildPathSummary(pathSummary).versioningType(versioning);
    if (storage != null) {
      builder.storageType(storage);
    }
    return builder.build();
  }

  /** Stores {@code json} as {@code resource} (revision 1). */
  public void store(final String resource, final String json) throws Exception {
    try (var store = newStore();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + db + "','" + resource + "','" + json.replace("'", "''") + "')").evaluate(ctx);
    }
  }

  /** Runs one updating query and commits it as one new revision. */
  public void update(final String query) throws Exception {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    try (var store = newStore();
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      new Query(chain, query).evaluate(ctx);
      ctx.applyUpdates();
    }
  }

  /** The interpreter's outcome for {@code query} (its source already expanded). */
  public Outcome interpreter(final String query) throws Exception {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    try (var store = newStore();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      return evaluate(chain, ctx, query);
    }
  }

  /** The route's outcome for {@code query} on the most recent revision of {@code resource}. */
  public Outcome route(final String resource, final String query, final int threads) throws Exception {
    return route(resource, -1, query, threads);
  }

  /**
   * The route's outcome for {@code query} with an executor bound to {@code revision} of
   * {@code resource} ({@code -1}: the most recent).
   */
  public Outcome route(final String resource, final int revision, final String query, final int threads)
      throws Exception {
    return route(resource, revision, query, threads, true);
  }

  /**
   * {@link #route(String, int, String, int)} with the route's own switch (QRY-601) set to
   * {@code routeEnabled} for this query and restored afterwards. The route is switched on explicitly
   * by default because it lands default-off (QRY-601: on by default only once its evidence exists).
   */
  public Outcome route(final String resource, final int revision, final String query, final int threads,
      final boolean routeEnabled) throws Exception {
    final boolean previous = QrySeam.setRouteEnabled(routeEnabled);
    try {
      return routeWithStartupSwitch(resource, revision, query, threads);
    } finally {
      QrySeam.setRouteEnabled(previous);
    }
  }

  public Outcome routeWithStartupSwitch(final String resource, final int revision, final String query, final int threads)
      throws Exception {
    try (var store = newStore();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.lookup(db);
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession(resource)) {
        final int bound = revision < 0
            ? session.getMostRecentRevisionNumber()
            : revision;
        final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, bound, threads);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          return evaluate(chain, ctx, query);
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  /** The route's outcome with an executor over an open write transaction on {@code resource}. */
  public Outcome routeOverWriteTransaction(final String resource, final String query, final int threads)
      throws Exception {
    final boolean previous = QrySeam.setRouteEnabled(true);
    try {
      return routeOverWriteTransactionAsSwitched(resource, query, threads);
    } finally {
      QrySeam.setRouteEnabled(previous);
    }
  }

  private Outcome routeOverWriteTransactionAsSwitched(final String resource, final String query, final int threads)
      throws Exception {
    try (var store = newStore();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.lookup(db);
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession(resource);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(wtx, threads);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          return evaluate(chain, ctx, query);
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
          wtx.rollback();
        }
      }
    }
  }

  /**
   * One store, query context, compile chain and resource session kept open across queries, so the
   * executor-side result and plan caches of QRY-403 see every query of a sequence. The route is
   * switched on for the session's lifetime and restored on close.
   */
  public OpenSession open(final String resource) {
    return new OpenSession(resource);
  }

  /** See {@link #open}. */
  public final class OpenSession implements AutoCloseable {
    private final BasicJsonDBStore store;
    private final SirixQueryContext ctx;
    private final SirixCompileChain chain;
    private final JsonResourceSession session;
    private final boolean previousSwitch;
    private final Map<ExecutorKey, SirixVectorizedExecutor> executors = new HashMap<>();

    private record ExecutorKey(int revision, int threads) {
    }

    private OpenSession(final String resource) {
      previousSwitch = QrySeam.setRouteEnabled(true);
      store = newStore();
      ctx = SirixQueryContext.createWithJsonStore(store);
      chain = SirixCompileChain.createWithJsonStore(store);
      session = store.lookup(db).getDatabase().beginResourceSession(resource);
    }

    /** The route's outcome with an executor of this session bound to {@code revision}. */
    public Outcome route(final int revision, final String query, final int threads) {
      final SirixVectorizedExecutor executor = executor(revision, threads);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        return evaluate(chain, ctx, query);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
      }
    }

    private SirixVectorizedExecutor executor(final int revision, final int threads) {
      final int bound = revision < 0 ? session.getMostRecentRevisionNumber() : revision;
      return executors.computeIfAbsent(new ExecutorKey(bound, threads),
          key -> new SirixVectorizedExecutor(session, key.revision(), key.threads()));
    }

    public void clearResultCaches(final int revision, final int threads) {
      executor(revision, threads).clearAggregateResultCachesForBenchmarks();
    }

    /** The outcome through the chain's own auto-wiring, which resolves the executor from the source. */
    public Outcome autoWired(final String query) {
      SequentialPipelineStrategy.setVectorizedExecutor(null);
      return evaluate(chain, ctx, query);
    }

    @Override
    public void close() throws Exception {
      try {
        for (final SirixVectorizedExecutor executor : executors.values()) {
          executor.close();
        }
        executors.clear();
        session.close();
        chain.close();
        ctx.close();
        store.close();
      } finally {
        QrySeam.setRouteEnabled(previousSwitch);
      }
    }
  }

  /** The commit instant of {@code revision} of {@code resource}. */
  public Instant revisionTimestamp(final String resource, final int revision) {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dir.resolve(db));
        JsonResourceSession session = database.beginResourceSession(resource);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      return rtx.getRevisionTimestamp();
    }
  }

  /** The most recent revision of {@code resource}. */
  public int mostRecentRevision(final String resource) {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dir.resolve(db));
        JsonResourceSession session = database.beginResourceSession(resource)) {
      return session.getMostRecentRevisionNumber();
    }
  }

  /** The largest node key of the most recent revision of {@code resource}. */
  public long maxNodeKey(final String resource) {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dir.resolve(db));
        JsonResourceSession session = database.beginResourceSession(resource);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      return rtx.getMaxNodeKey();
    }
  }

  /**
   * Asserts the node at {@code key} has {@code kind} and, for a named node, {@code name}; for a
   * string element, {@code name} is its value. A fixture whose geometry depends on node-key
   * arithmetic calls this before it trusts the arithmetic.
   */
  public void assertNodeAt(final String resource, final long key, final NodeKind kind, final String name) {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dir.resolve(db));
        JsonResourceSession session = database.beginResourceSession(resource);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(key), "fixture geometry: no node at key " + key);
      assertEquals(kind, rtx.getKind(), "fixture geometry: kind of node " + key);
      if (name != null) {
        final String actual = kind == NodeKind.STRING_VALUE
            ? rtx.getValue()
            : rtx.getName().getLocalName();
        assertEquals(name, actual, "fixture geometry: name of node " + key);
      }
    }
  }

  /** Removes the database. */
  public void drop() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    Databases.removeDatabase(dir.resolve(db));
  }

  private static Outcome evaluate(final SirixCompileChain chain, final SirixQueryContext ctx, final String query) {
    try {
      return Outcome.of(new Query(chain, query).evaluate(ctx));
    } catch (final QueryException e) {
      return Outcome.error(e.getCode().toString());
    }
  }

  /**
   * An observable query result: the item type class ({@code xs:integer}, {@code xs:decimal},
   * {@code xs:double}, …) and value, or the error code. Two outcomes are equal exactly when QRY-201
   * calls them equal.
   */
  public record Outcome(String type, String value, String error) {

    static Outcome of(final Sequence result) {
      if (result == null) {
        return new Outcome("empty-sequence()", "", null);
      }
      if (result instanceof Atomic atomic) {
        return new Outcome(typeOf(atomic), atomic.stringValue(), null);
      }
      if (result instanceof Object object) {
        final Map<String, Outcome> members = new TreeMap<>();
        for (int i = 0; i < object.len(); i++) {
          members.put(object.name(i).getLocalName(), of(object.value(i)));
        }
        return object(members);
      }
      return new Outcome(result.getClass().getSimpleName(), String.valueOf(result), null);
    }

    static Outcome error(final String code) {
      return new Outcome(null, null, code);
    }

    /** An {@code xs:integer} outcome. */
    public static Outcome integer(final long value) {
      return new Outcome("xs:integer", Long.toString(value), null);
    }

    public static Outcome object(final Map<String, Outcome> members) {
      return new Outcome("object()", new TreeMap<>(members).toString(), null);
    }

    public boolean isError() {
      return error != null;
    }

    private static String typeOf(final Atomic atomic) {
      if (atomic instanceof IntNumeric) {
        return "xs:integer";
      }
      if (atomic instanceof DecNumeric) {
        return "xs:decimal";
      }
      if (atomic instanceof DblNumeric) {
        return "xs:double";
      }
      return String.valueOf(atomic.type());
    }

    @Override
    public String toString() {
      return isError()
          ? "error " + error
          : type + " " + value;
    }
  }
}
