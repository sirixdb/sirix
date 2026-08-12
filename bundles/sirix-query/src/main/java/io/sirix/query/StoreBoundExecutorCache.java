package io.sirix.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.settings.Fixed;

/**
 * Resolves the {@link SirixVectorizedExecutor} a query needs from the query itself, so a compile
 * chain that was handed only a {@link JsonDBStore} still gets the analytical fast paths.
 *
 * <p>
 * {@link SirixCompileChain#createWithJsonStore(JsonDBStore, JsonResourceSession)} solves this by
 * making the caller name the resource up front, which only works when the caller has one. A chain
 * built from a store alone serves every resource in it, so the resource has to come from the query:
 * this class lifts the first literal {@code jn:doc}/{@code jn:open} out of the PARSED AST, opens
 * that resource through the store and hands back an executor bound to it.
 *
 * <h2>Why reading the parsed AST is safe</h2>
 *
 * <p>
 * The parsed AST is pre-analysis, so a prefixed name like {@code jn:doc} still carries its prefix
 * rather than the resolved namespace URI, and a query is free to bind {@code jn} elsewhere. That
 * imprecision is deliberate and harmless: what this class returns only decides <em>which executor
 * gets built</em>. Whether that executor may actually serve a given scan is decided later, at
 * translate time, by {@link SirixVectorizedExecutor#acceptsSource(SourceRef)} against the
 * {@link SourceRef} brackit's optimizer lifts from the ANALYZED AST — where namespaces are resolved
 * and variable bindings followed. A wrong guess here therefore costs the fast path and nothing
 * else: the gate declines and the generic pipeline answers.
 *
 * <p>
 * A query naming several resources is served on all of them. The first literal document binds the
 * chain, and every further scan resolves its OWN executor through {@link #resolve(SourceRef)} —
 * brackit asks per admitted scan, so a two-document join is accelerated on both sides rather than
 * only on whichever document happened to be named first.
 *
 * <h2>Executor lifetime</h2>
 *
 * <p>
 * Executors are cached per {@code (database, resource, revision)} because building one per compile
 * would build a worker pool per compile. The cache is bounded and access-ordered: the
 * least-recently-used entry is closed on overflow. Two things make eviction (and the revision
 * advance that causes most of it) safe rather than merely likely-safe — a
 * {@link SirixVectorizedExecutor} whose pool has been shut down runs its chunks on the calling
 * thread, and its record transaction is reopened when it is found closed. A compiled query holding
 * an evicted executor therefore keeps answering, single-threaded, from the revision it was compiled
 * against.
 *
 * <p>
 * Not thread-confined: one chain may compile on many threads, so every mutation of the cache takes
 * its monitor. Compilation is not a hot path — the scan it enables is — so a lock here costs
 * nothing measurable.
 */
final class StoreBoundExecutorCache implements AutoCloseable {

  /**
   * Revision sentinel for a {@code jn:doc} that names none, mirroring
   * {@link SourceRef#LATEST_REVISION} so a literal {@code jn:doc('db','r',-1)} is classified here
   * exactly as brackit's optimizer classifies it.
   */
  private static final int LATEST_REVISION = SourceRef.LATEST_REVISION;

  /**
   * Upper bound on cached executors. Each holds a lazily-populated worker pool and the resource
   * session's shared read-only transactions for its revision, so an unbounded cache would retain a
   * pool per revision on a chain that outlives many commits. Eight covers a chain serving a handful
   * of resources across a few revisions; beyond that the least-recently-used one is closed.
   */
  private static final int MAX_CACHED_EXECUTORS = 8;

  /** Guards {@link #executors} — compile-time only, never on a scan path. */
  private final Object lock = new Object();

  /** The store every resource is resolved through; never {@code null}. */
  private final JsonDBStore store;

  /** Access-ordered LRU; the evicted entry is closed by {@link #removeEldestEntry}. */
  private final LinkedHashMap<ExecutorKey, SirixVectorizedExecutor> executors;

  /** Set by {@link #close()}; a resolve after close hands back nothing rather than a live pool. */
  private boolean closed;

  /** Identity of a cached executor: the resource it reads and the revision it is pinned to. */
  private record ExecutorKey(String database, String resource, int revision) {
  }

  /** A literal {@code jn:doc}/{@code jn:open} lifted from the parsed AST. */
  record DocumentSource(String database, String resource, int revision) {
  }

  StoreBoundExecutorCache(final JsonDBStore store) {
    if (store == null) {
      throw new IllegalArgumentException("store must not be null");
    }
    this.store = store;
    this.executors = new LinkedHashMap<>(MAX_CACHED_EXECUTORS * 2, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(final Map.Entry<ExecutorKey, SirixVectorizedExecutor> eldest) {
        if (size() <= MAX_CACHED_EXECUTORS) {
          return false;
        }
        closeQuietly(eldest.getValue());
        return true;
      }
    };
  }

  /**
   * The executor for the document this query reads, or {@code null} when the query names none, the
   * resource cannot be opened, or the cache is closed. Never throws: a chain that cannot resolve an
   * executor simply compiles the generic pipeline, which is what it did before auto-wiring existed.
   *
   * @param parsedAST the AST as produced by the parser, before analysis and optimization
   */
  SirixVectorizedExecutor resolve(final AST parsedAST) {
    final DocumentSource source = firstDocumentSource(parsedAST);
    return source == null
        ? null
        : resolve(source);
  }

  /**
   * The executor for {@code source} at its CURRENT revision. Called once per compile to bind a query,
   * and again on every execution of that query through
   * {@link io.sirix.query.scan.RevisionTrackingExecutor} — which is what makes a bare {@code jn:doc}
   * mean "most recent when the query runs", the same thing it means to the generic pipeline. Never
   * throws.
   */
  SirixVectorizedExecutor resolve(final DocumentSource source) {
    if (source == null) {
      return null;
    }
    try {
      return resolveDocument(source);
    } catch (final RuntimeException e) {
      // A missing database, a resource that does not exist yet, a store already closed: the query
      // itself will fail (or not) on its own terms. Auto-wiring never turns a resolution problem
      // into a query failure.
      return null;
    }
  }

  /**
   * The executor for the document a {@link SourceRef} names, or {@code null} when the ref names no
   * concrete document or the store cannot reach it. This is what lets ONE chain serve a query that
   * reads several resources: brackit asks per scan, and each gets an executor bound to its own
   * document instead of the whole query being pinned to whichever one came first.
   */
  SirixVectorizedExecutor resolve(final SourceRef ref) {
    if (ref == null || ref.kind() != SourceRef.Kind.DOCUMENT) {
      return null;
    }
    return resolve(new DocumentSource(ref.databaseName(), ref.resourceName(), ref.revision()));
  }

  /** Open {@code source}'s resource through the store and bind (or reuse) an executor for it. */
  private SirixVectorizedExecutor resolveDocument(final DocumentSource source) {
    final JsonDBCollection collection = store.lookup(source.database());
    if (collection == null) {
      return null;
    }
    final JsonResourceSession session = collection.getDatabase().beginResourceSession(source.resource());
    if (session == null || session.isClosed()) {
      return null;
    }
    final int mostRecent = session.getMostRecentRevisionNumber();
    // A bare jn:doc opens the most recent revision, and "most recent" has to be re-read on every
    // compile: an executor built before a commit memoises aggregates, path statistics and decoded
    // columns for the revision it was pinned to, so reusing it afterwards answers from before the
    // write. Re-resolving here is what keeps an auto-wired chain honest across commits.
    final int revision = source.revision() == LATEST_REVISION
        ? mostRecent
        : source.revision();
    if (revision < 0 || revision > mostRecent) {
      return null;
    }
    final ExecutorKey key = new ExecutorKey(source.database(), source.resource(), revision);
    synchronized (lock) {
      if (closed) {
        return null;
      }
      final SirixVectorizedExecutor cached = executors.get(key);
      if (cached != null) {
        return cached;
      }
      final SirixVectorizedExecutor built = new SirixVectorizedExecutor(session, revision);
      executors.put(key, built);
      return built;
    }
  }

  /** Close every cached executor. Idempotent; the chain's stores are closed by the chain. */
  @Override
  public void close() {
    final List<SirixVectorizedExecutor> toClose;
    synchronized (lock) {
      if (closed) {
        return;
      }
      closed = true;
      toClose = new ArrayList<>(executors.values());
      executors.clear();
    }
    for (final SirixVectorizedExecutor executor : toClose) {
      closeQuietly(executor);
    }
  }

  private static void closeQuietly(final SirixVectorizedExecutor executor) {
    try {
      executor.close();
    } catch (final Exception ignored) {
      // Best-effort: a failing executor close must not mask the caller's own teardown.
    }
  }

  // ==================== Parsed-AST document lifting ====================

  /**
   * The first literal {@code jn:doc}/{@code jn:open} in document order, or {@code null} if the query
   * contains none that can be read off the AST.
   */
  static DocumentSource firstDocumentSource(final AST node) {
    if (node == null) {
      return null;
    }
    if (node.getType() == XQ.FunctionCall) {
      final DocumentSource source = documentSourceOf(node);
      if (source != null) {
        return source;
      }
    }
    for (int i = 0, childCount = node.getChildCount(); i < childCount; i++) {
      final DocumentSource source = firstDocumentSource(node.getChild(i));
      if (source != null) {
        return source;
      }
    }
    return null;
  }

  /**
   * The document a {@link Sequence} IS, when it is a whole Sirix JSON document — the other way a
   * query can name its input, and the one no AST walk can see.
   *
   * <p>
   * {@code declare variable $doc external} with the document bound through the
   * {@link io.brackit.query.QueryContext} is the ordinary embedding shape: bind once, run many
   * queries. It puts no {@code jn:doc} in the tree, so {@link #firstDocumentSource} finds nothing and
   * the whole auto-wiring declines — measured at 705 ms against 1.1 ms for the same query written
   * with a literal {@code jn:doc}, same answer.
   *
   * <p>
   * Only the document's TOP-LEVEL node qualifies, for the reason
   * {@code SirixVectorizedExecutor.servesWholeDocument} spells out: a scan's source path is written
   * relative to the binding and resolved absolutely, so a nested binding would aggregate rows outside
   * it. The runtime source gate refuses one anyway; recognising it here would only spend a compile
   * discovering that.
   *
   * <p>
   * A binding at the resource's most recent revision is recorded as {@link #LATEST_REVISION} rather
   * than as that number, so the executor tracks commits exactly as a bare {@code jn:doc} does — a
   * caller that rebinds a fresh document after a commit without recompiling keeps the fast path. An
   * explicitly older revision is recorded as itself: it names one immutable snapshot.
   *
   * @return the document source, or {@code null} for anything that is not a whole Sirix JSON document
   */
  static DocumentSource boundDocumentSource(final Sequence sequence) {
    if (!(sequence instanceof JsonDBItem item)) {
      return null;
    }
    try {
      final JsonNodeReadOnlyTrx trx = item.getTrx();
      if (trx == null || trx.getParentKey() != Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
        return null;
      }
      final JsonDBCollection collection = item.getCollection();
      final JsonResourceSession session = item.getResourceSession();
      if (collection == null || session == null || collection.getName() == null) {
        return null;
      }
      final int revision = trx.getRevisionNumber();
      return new DocumentSource(collection.getName(),
          session.getResourceConfig().getResource().getFileName().toString(),
          revision == session.getMostRecentRevisionNumber()
              ? LATEST_REVISION
              : revision);
    } catch (final RuntimeException e) {
      // A closed transaction, a store shutting down: the binding simply does not name a document
      // this cache can reach, and the query compiles the generic pipeline as it always did.
      return null;
    }
  }

  /**
   * Classify a function call as a concrete document opener. Mirrors the classification brackit's
   * {@code VectorizedGroupByDetection} performs on the analyzed AST, minus the namespace resolution
   * that has not happened yet — see the class comment for why the looser match is safe.
   */
  private static DocumentSource documentSourceOf(final AST call) {
    if (!(call.getValue() instanceof QNm name) || !isJsonFunction(name)) {
      return null;
    }
    final String localName = name.getLocalName();
    // Only the single-document openers. jn:collection and the multi-revision openers span more
    // than one resource or revision, which no single bound executor can serve.
    if (!"doc".equals(localName) && !"open".equals(localName)) {
      return null;
    }
    if (call.getChildCount() < 2) {
      return null;
    }
    final String database = stringLiteralValue(call.getChild(0));
    final String resource = stringLiteralValue(call.getChild(1));
    if (database == null || resource == null) {
      // A computed database or resource name — nothing to bind to at compile time.
      return null;
    }
    if (call.getChildCount() == 2) {
      return new DocumentSource(database, resource, LATEST_REVISION);
    }
    final Integer revision = literalRevision(call.getChild(2));
    return revision == null
        ? null
        : new DocumentSource(database, resource, revision);
  }

  /**
   * Whether {@code name} denotes a function in the JSONiq namespace. The parser leaves a prefixed
   * name's URI empty (prefixes are bound during analysis), so the prefix is accepted as evidence when
   * no URI is present.
   */
  private static boolean isJsonFunction(final QNm name) {
    final String namespaceURI = name.getNamespaceURI();
    if (namespaceURI != null && !namespaceURI.isEmpty()) {
      return JSONFun.JSON_NSURI.equals(namespaceURI);
    }
    return JSONFun.JSON_PREFIX.equals(name.getPrefix());
  }

  /** The value of a string literal argument, or {@code null} for anything computed. */
  private static String stringLiteralValue(final AST node) {
    if (node == null || node.getType() != XQ.Str) {
      return null;
    }
    final Object value = node.getValue();
    if (value instanceof String string) {
      return string;
    }
    if (value instanceof Str string) {
      return string.stringValue();
    }
    return value != null
        ? value.toString()
        : null;
  }

  /**
   * The exact int of a literal integer revision argument; {@code null} for anything non-literal or
   * out of range. A negated literal parses as a unary-minus expression rather than an {@link XQ#Int},
   * so it declines here exactly as it does in brackit's own classification.
   */
  private static Integer literalRevision(final AST node) {
    if (node == null || node.getType() != XQ.Int) {
      return null;
    }
    final Object value = node.getValue();
    final long revision;
    if (value instanceof Numeric numeric) {
      try {
        revision = numeric.decimalValue().longValueExact();
      } catch (final ArithmeticException e) {
        return null;
      }
    } else if (value instanceof Number number) {
      revision = number.longValue();
    } else {
      return null;
    }
    if (revision < Integer.MIN_VALUE || revision > Integer.MAX_VALUE) {
      return null;
    }
    return (int) revision;
  }

  /** Number of live cached executors — for tests and diagnostics. */
  int cachedExecutorCount() {
    synchronized (lock) {
      return executors.size();
    }
  }

  /** Any currently cached executor, or {@code null} when none is — for tests. */
  SirixVectorizedExecutor anyCachedExecutor() {
    synchronized (lock) {
      final Iterator<SirixVectorizedExecutor> values = executors.values().iterator();
      return values.hasNext()
          ? values.next()
          : null;
    }
  }

  /**
   * Whether an executor for the given resource and revision is currently cached — for tests.
   * {@code containsKey} does not count as an access, so asking leaves the LRU order untouched.
   */
  boolean isCached(final String database, final String resource, final int revision) {
    synchronized (lock) {
      return executors.containsKey(new ExecutorKey(database, resource, revision));
    }
  }
}
