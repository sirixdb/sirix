package io.sirix.access.trx.node;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.utils.ObjectPool;
import io.brackit.query.jdm.DocumentException;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ResourceStore;
import io.sirix.access.User;
import io.sirix.access.trx.node.xml.XmlResourceSessionImpl;
import io.sirix.access.trx.page.NodeStorageEngineReader;
import io.sirix.access.trx.page.StorageEngineWriterFactory;
import io.sirix.access.trx.page.StorageEngineReaderFactory;
import io.sirix.access.trx.page.RevisionRootPageReader;
import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.RecordHistoryVisitor;
import io.sirix.api.RecordRunVisitor;
import io.sirix.api.ResourceSession;
import io.sirix.api.RevisionInfo;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.cache.BufferManager;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixThreadedException;
import io.sirix.exception.SirixUsageException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionIndex;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.StorageType;
import io.sirix.io.Writer;
import io.sirix.metrics.TransactionMetrics;
import io.sirix.node.RevisionReferencesNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.Node;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.UberPage;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.sirix.utils.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("ConstantValue")
public abstract class AbstractResourceSession<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    implements ResourceSession<R, W>, InternalResourceSession<R, W> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractResourceSession.class);

  /**
   * Feature flag for optimized revision search using SIMD/Eytzinger layout. Set to false to use
   * legacy binary search (for rollback if needed).
   */
  private static final boolean USE_OPTIMIZED_REVISION_SEARCH =
      Boolean.parseBoolean(System.getProperty("sirix.optimizedRevisionSearch", "true"));

  /**
   * Write lock to assure only one exclusive write transaction exists.
   */
  final Semaphore writeLock;

  /**
   * Strong reference to uber page before the begin of a write transaction.
   */
  final AtomicReference<UberPage> lastCommittedUberPage;

  /**
   * Remember all running node transactions (both read and write).
   */
  final ConcurrentMap<Integer, R> nodeTrxMap;

  /**
   * Remember all running storage engine instances (both readers and writers).
   */
  final ConcurrentMap<Integer, StorageEngineReader> storageEngineReaderMap;

  /**
   * Remember the write separately because of the concurrent writes.
   */
  final ConcurrentMap<Integer, StorageEngineWriter> storageEngineWriterMap;

  /**
   * Cache key for {@link #REVISION_INFO_CACHE}: database ids are random positive longs persisted per
   * database and claimed per directory in-JVM, resource ids are persisted per resource — the pair is
   * stable and unique, so (databaseId, resourceId, revision) can never be misattributed across
   * resources.
   */
  private record RevisionInfoKey(long databaseId, long resourceId, int revision) {
  }

  /**
   * GLOBAL cache of (databaseId, resourceId, revision) → {@link RevisionInfo} (author, timestamp,
   * commit message). A committed revision is immutable, so an entry never has to be invalidated by
   * new commits (they add new keys). The cache is static because REST closes the session per request
   * — a per-session cache re-read one {@code RevisionRootPage} per revision on EVERY {@code /history}
   * call; the global cache only pays I/O for revisions not yet seen by this JVM. Invalidation:
   * resource removal drops the (databaseId, resourceId) slice, database removal and crash-recovery
   * truncation drop the databaseId slice, {@code Databases.clearGlobalCaches()} (cold-process
   * simulation in tests) drops everything.
   */
  private static final Cache<RevisionInfoKey, RevisionInfo> REVISION_INFO_CACHE =
      Caffeine.newBuilder().maximumSize(100_000).build();

  /** Shared empty result for history-timestamp queries on resources without user revisions. */
  private static final long[] EMPTY_LONG_ARRAY = new long[0];

  /** Shared empty result for record-change-revision queries. */
  private static final int[] EMPTY_INT_ARRAY = new int[0];

  /** Shared zero-length array for {@link java.util.Collection#toArray(Object[])} of futures. */
  private static final CompletableFuture<?>[] EMPTY_FUTURES = new CompletableFuture[0];

  /** The catalogue revision of a revision with no index catalogue at or below it. */
  private static final int NO_INDEX_CATALOGUE = -1;

  private static final int PARSED_INDEX_CATALOGUE_CACHE_SIZE = 64;

  /** Listings of {@code indexes/} taken by {@link #listIndexCatalogueRevisions}, process-wide. */
  private static final LongAdder INDEX_CATALOGUE_DIRECTORY_LISTINGS = new LongAdder();

  /**
   * Catalogue files written and reported through {@link #recordSerializedIndexCatalogueRevision},
   * process-wide.
   */
  private static final LongAdder INDEX_CATALOGUE_FILES_WRITTEN = new LongAdder();

  /** Drops one database's entries from the global revision-info cache. */
  public static void invalidateRevisionInfoCache(final long databaseId) {
    REVISION_INFO_CACHE.asMap().keySet().removeIf(key -> key.databaseId() == databaseId);
  }

  /** Drops one resource's entries from the global revision-info cache. */
  public static void invalidateRevisionInfoCache(final long databaseId, final long resourceId) {
    REVISION_INFO_CACHE.asMap()
                       .keySet()
                       .removeIf(key -> key.databaseId() == databaseId && key.resourceId() == resourceId);
  }

  /** Drops every entry from the global revision-info cache — cold-process simulation for tests. */
  public static void clearRevisionInfoCache() {
    REVISION_INFO_CACHE.invalidateAll();
  }

  /**
   * Lock for blocking the commit.
   */
  private final Lock commitLock;

  /**
   * Resource configuration.
   */
  final ResourceConfiguration resourceConfig;

  /**
   * Factory for all interactions with the storage.
   */
  final IOStorage storage;

  /**
   * Atomic counter for concurrent generation of transaction ids, shared by node transactions and
   * storage engine readers/writers alike.
   *
   * <p>
   * ONE counter, deliberately: node trx ids key {@link #nodeTrxMap} and
   * {@link #storageEngineWriterMap}, storage engine ids key {@link #storageEngineReaderMap}, and
   * close paths cross between them (a node trx closes its storage engine reader; a storage engine
   * reader consults the node trx bookkeeping). Two independent counters made those two id spaces
   * collide numerically while meaning different things, so a close could skip its own entry (the map
   * grew for the session's lifetime) or evict a live, unrelated one. A single monotonic counter makes
   * an id unique across BOTH spaces, which is what every cross-map lookup here already assumed. Ids
   * also stay unique in pin diagnostics and logs, where they identify a transaction, not a slot in
   * some space.
   *
   * <p>
   * A read-write node transaction still shares one id with its bound storage engine writer — that is
   * a single {@code incrementAndGet} used for both, not a collision.
   */
  private final AtomicInteger trxIDCounter;

  /**
   * Per-thread shared read-only transactions for parallel query execution. Key: (threadId, revision)
   * → one read-only trx per worker thread per revision.
   */
  private final ConcurrentHashMap<SharedTrxKey, R> sharedTrxMap;

  private final AtomicReference<ObjectPool<StorageEngineReader>> pool;

  /**
   * The sorted revisions of every catalogue file of this resource, once any of its sessions has
   * listed {@code indexes/}, extended by every file their writers serialize since. See
   * {@link #resolveIndexCatalogueRevision}.
   */
  @SuppressWarnings("ArrayRecordComponent") // Primitive revisions are searched directly; record equality is not used.
  private record CatalogueRevisions(int[] revisions, boolean listed) {
  }

  private final AtomicReference<CatalogueRevisions> knownIndexCatalogueRevisions;

  /**
   * The parsed definitions of recent catalogue files this resource's sessions have read, by the
   * file's revision, shared between handles for the lifetime of their shared resource session. See
   * {@link #parsedIndexCatalogue} for isolation and invalidation requirements.
   */
  private final Int2ObjectLinkedOpenHashMap<List<IndexDef>> parsedIndexCatalogues;

  /**
   * Determines if session was closed.
   */
  volatile boolean isClosed;

  /**
   * The cache of in-memory pages shared amongst all manager / resource transactions.
   */
  final BufferManager bufferManager;

  /**
   * Tracks the minimum active revision for MVCC-aware page eviction. NOTE: This is now the GLOBAL
   * epoch tracker shared across all databases/resources. Each session registers its active revisions
   * with the global tracker.
   */
  final RevisionEpochTracker revisionEpochTracker;

  /**
   * The resource store with which this manager has been created.
   */
  final ResourceStore<? extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> resourceStore;

  /**
   * The user interacting with SirixDB.
   */
  final User user;

  /** The database-owned session that owns storage and the shared committed-revision state. */
  private final @Nullable AbstractResourceSession<R, W> sharedSession;

  /** Number of handle sessions using this database-owned session; guarded by its monitor. */
  private int userSessionCount;

  /**
   * A factory that creates new {@link StorageEngineWriter} instances.
   */
  private final StorageEngineWriterFactory storageEngineWriterFactory;

  /**
   * ID Generation exception message for duplicate ID.
   */
  private final String ID_GENERATION_EXCEPTION = "ID generation is bogus because of duplicate ID.";

  /**
   * Creates a new instance of this class.
   *
   * @param resourceStore the resource store with which this session has been created
   * @param resourceConf {@link DatabaseConfiguration} for general setting about the storage
   * @param bufferManager the cache of in-memory pages shared amongst all resource sessions and
   *        transactions
   * @param storage the I/O backed storage backend
   * @param uberPage holds a reference to the revision root page tree
   * @param writeLock allow for concurrent writes
   * @param user the user tied to the resource session
   * @param storageEngineWriterFactory A factory that creates new {@link StorageEngineWriter}
   *        instances.
   * @throws SirixException if Sirix encounters an exception
   */
  protected AbstractResourceSession(final ResourceStore<? extends ResourceSession<R, W>> resourceStore,
      final ResourceConfiguration resourceConf, final BufferManager bufferManager, final IOStorage storage,
      final UberPage uberPage, final Semaphore writeLock, final @Nullable User user,
      final StorageEngineWriterFactory storageEngineWriterFactory) {
    this.resourceStore = requireNonNull(resourceStore);
    resourceConfig = requireNonNull(resourceConf);
    this.bufferManager = requireNonNull(bufferManager);
    this.storage = requireNonNull(storage);
    this.storageEngineWriterFactory = storageEngineWriterFactory;

    nodeTrxMap = new ConcurrentHashMap<>();
    storageEngineReaderMap = new ConcurrentHashMap<>();
    storageEngineWriterMap = new ConcurrentHashMap<>();

    trxIDCounter = new AtomicInteger();
    commitLock = new ReentrantLock(false);
    sharedTrxMap = new ConcurrentHashMap<>();

    this.writeLock = requireNonNull(writeLock);

    lastCommittedUberPage = new AtomicReference<>(uberPage);
    this.user = user;
    sharedSession = null;
    pendingRevisionRoot = new AtomicReference<>();
    knownIndexCatalogueRevisions = new AtomicReference<>(new CatalogueRevisions(EMPTY_INT_ARRAY, false));
    parsedIndexCatalogues = new Int2ObjectLinkedOpenHashMap<>(PARSED_INDEX_CATALOGUE_CACHE_SIZE);
    pool = new AtomicReference<>();

    // Use GLOBAL epoch tracker (shared across all databases/resources)
    // This follows PostgreSQL pattern where all sessions register with a global tracker
    this.revisionEpochTracker = Databases.getGlobalEpochTracker();

    // Register this resource's current revision with the global tracker
    // This allows MVCC-aware eviction across all resources
    this.revisionEpochTracker.setLastCommittedRevision(uberPage.getRevisionNumber());

    // NOTE: ClockSweepers are now GLOBAL (started with BufferManager, not per-session)
    // This follows PostgreSQL bgwriter pattern - background threads run continuously

    isClosed = false;
  }

  /**
   * Create a user session with its own transactions and reader pool over the database's shared
   * storage, write/commit locks and published revision. No storage is reopened or revision read.
   */
  protected AbstractResourceSession(final AbstractResourceSession<R, W> sharedSession,
      final ResourceStore<? extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> resourceStore,
      final User user) {
    sharedSession.assertNotClosed();
    this.sharedSession = sharedSession;
    this.resourceStore = requireNonNull(resourceStore);
    this.user = requireNonNull(user);
    resourceConfig = sharedSession.resourceConfig;
    bufferManager = sharedSession.bufferManager;
    storage = sharedSession.storage;
    storageEngineWriterFactory = sharedSession.storageEngineWriterFactory;
    writeLock = sharedSession.writeLock;
    commitLock = sharedSession.commitLock;
    lastCommittedUberPage = sharedSession.lastCommittedUberPage;
    pendingRevisionRoot = sharedSession.pendingRevisionRoot;
    knownIndexCatalogueRevisions = sharedSession.knownIndexCatalogueRevisions;
    parsedIndexCatalogues = sharedSession.parsedIndexCatalogues;
    revisionEpochTracker = sharedSession.revisionEpochTracker;
    trxIDCounter = sharedSession.trxIDCounter;
    nodeTrxMap = new ConcurrentHashMap<>();
    storageEngineReaderMap = new ConcurrentHashMap<>();
    storageEngineWriterMap = new ConcurrentHashMap<>();
    sharedTrxMap = new ConcurrentHashMap<>();
    pool = new AtomicReference<>();
    if (sharedSession.pool.get() != null) {
      createStorageEnginePool();
    }
  }

  public final synchronized ResourceSession<R, W> openUserSession(
      final ResourceStore<? extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> resourceStore,
      final User user) {
    assertNotClosed();
    requireNonNull(resourceStore);
    requireNonNull(user);
    userSessionCount++;
    try {
      return createUserSession(resourceStore, user);
    } catch (final RuntimeException | Error e) {
      userSessionCount--;
      throw e;
    }
  }

  protected ResourceSession<R, W> createUserSession(
      final ResourceStore<? extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> resourceStore,
      final User user) {
    throw new UnsupportedOperationException("This resource session does not support user sessions");
  }

  public final void releaseUserSession() {
    if (sharedSession == null) {
      throw new IllegalStateException("Only a user session can release its shared session");
    }
    synchronized (sharedSession) {
      if (sharedSession.userSessionCount <= 0) {
        throw new IllegalStateException("No user session is registered");
      }
      if (sharedSession.userSessionCount == 1) {
        sharedSession.close();
      }
      sharedSession.userSessionCount--;
    }
  }

  // REMOVED: ClockSweeper management moved to BufferManager (global lifecycle)
  // This follows PostgreSQL bgwriter pattern - background threads run continuously,
  // not tied to individual session lifecycle

  public void createStorageEnginePool() {
    if (pool.get() == null) {
      final StorageEngineReaderFactory factory = new StorageEngineReaderFactory(this);
      pool.set(new ObjectPool<>(factory, factory));
    }
  }

  protected void initializeIndexController(final int revision, final AbstractIndexController<?, ?> controller) {
    loadIndexCatalogue(revision, controller.getIndexes());
    controller.refreshIndexCapabilities();
  }

  @Override
  public void restoreIndexCatalogue(final int revision, final Indexes indexes) {
    checkArgument(revision >= 0, "revision must be >= 0!");
    // loadIndexCatalogue replaces the definitions with the resolved file's, or resets them when there
    // is none; a controller that already holds exactly that file's definitions is left alone, which
    // is the common case: the writer's controller was created for its prospective revision from the
    // same file a moment earlier.
    loadIndexCatalogue(revision, requireNonNull(indexes));
  }

  private void loadIndexCatalogue(final int revision, final Indexes indexes) {
    // Writer rebinding restores the represented revision without truncating later catalogues.
    final Path indexesDir =
        getResourceConfig().getResource().resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
    // A writer asks for the revision it is about to create. No committed catalogue can be newer than
    // the most recent revision, so the lookup is clamped to it: the prospective revision's own file
    // is never consulted (it can only be the leftover of a commit of that number that was never
    // acknowledged), and the writer's definitions are restored from its represented revision by
    // StorageEngineWriterFactory anyway.
    final int mostRecentRevision = getMostRecentRevisionNumber();
    final int committedRevision = Math.max(0, Math.min(revision, mostRecentRevision));
    final int catalogueRevision = resolveIndexCatalogueRevision(indexesDir, committedRevision);
    if (catalogueRevision == NO_INDEX_CATALOGUE) {
      indexes.reset();
      return; // no definitions were serialized at or below the requested revision
    }
    if (committedRevision == mostRecentRevision) {
      rememberIndexCatalogueRevision(catalogueRevision);
    }
    if (indexes.catalogueRevision() == catalogueRevision && !indexes.differsFromPersisted()) {
      return; // already holds exactly that file's definitions
    }
    indexes.initFrom(catalogueRevision, parsedIndexCatalogue(indexesDir, catalogueRevision));
  }

  /**
   * Cached definitions from a committed catalogue file. {@link Indexes#initFrom} copies them before a
   * controller can mutate numeric coverage, so cached instances remain unchanged. Concurrent cache
   * misses may parse independently; subsequent lookups reuse the installed parse. Truncation and
   * revision-number reuse invalidate affected entries through {@link #invalidateIndexCataloguesAfter}
   * and {@link #recordSerializedIndexCatalogueRevision}.
   */
  private List<IndexDef> parsedIndexCatalogue(final Path indexesDir, final int catalogueRevision) {
    synchronized (parsedIndexCatalogues) {
      final List<IndexDef> cached = parsedIndexCatalogues.getAndMoveToLast(catalogueRevision);
      if (cached != null) {
        return cached;
      }
    }
    final Indexes parsed = new Indexes();
    try (final InputStream in = new FileInputStream(indexesDir.resolve(catalogueRevision + ".xml").toFile())) {
      parsed.init(IndexController.deserialize(in).getFirstChild());
    } catch (IOException | DocumentException | SirixException e) {
      throw new SirixIOException("Index definitions couldn't be deserialized!", e);
    }
    final List<IndexDef> definitions = List.copyOf(parsed.getIndexDefsInOrder());
    synchronized (parsedIndexCatalogues) {
      final List<IndexDef> raced = parsedIndexCatalogues.getAndMoveToLast(catalogueRevision);
      if (raced != null) {
        return raced;
      }
      if (parsedIndexCatalogues.size() == PARSED_INDEX_CATALOGUE_CACHE_SIZE) {
        final int latestCommittedCatalogue = greatestAtOrBelow(
            requireNonNull(knownIndexCatalogueRevisions.get()).revisions(), getMostRecentRevisionNumber());
        if (parsedIndexCatalogues.firstIntKey() == latestCommittedCatalogue) {
          parsedIndexCatalogues.getAndMoveToLast(latestCommittedCatalogue);
        }
        parsedIndexCatalogues.removeFirst();
      }
      parsedIndexCatalogues.putAndMoveToLast(catalogueRevision, definitions);
    }
    return definitions;
  }

  /**
   * The revision whose catalogue file ({@code indexes/<revision>.xml}) holds the index definitions in
   * effect at {@code revision}: the greatest catalogue revision at or below it, or
   * {@link #NO_INDEX_CATALOGUE}.
   *
   * <p>
   * Most revisions inherit an earlier catalogue. Directory knowledge is shared for the resource
   * session's lifetime; after a listing, subsequent lookups need no directory access:
   * <ol>
   * <li>what this resource's sessions know. Once one has listed the directory, the sorted revisions
   * of every catalogue file are shared by all of them and extended by every file their writers
   * serialize since, and every lookup is answered from that array without file-system access;</li>
   * <li>the requested revision's own file, one {@code stat}: an exact answer for a revision that
   * committed definitions;</li>
   * <li>the newest catalogue resolved at the latest committed revision or serialized by a writer,
   * when it is at or below the requested revision;</li>
   * <li>the previous revision's file: the first writer of a session, one more {@code stat};</li>
   * <li>one directory listing, which establishes (1) for all sessions sharing this resource.</li>
   * </ol>
   * Each step returns what the listing would return. (1) and (3) hold because every catalogue file
   * this resource's writers create is reported through
   * {@link #recordSerializedIndexCatalogueRevision(int)} as soon as it is durable. Truncation and
   * crash recovery invalidate discarded revisions through {@link #invalidateIndexCataloguesAfter};
   * unrelated removal is excluded ({@code Database.removeResource} refuses while a session is
   * registered; a restore only fills an empty directory).
   *
   * <p>
   * {@code Databases} holds an operating-system lock on the database's persistent {@code .lock} file
   * until the last handle closes, refusing another process's open. Handles at the same canonical path
   * share one database backend. Their user sessions share storage, the committed uber-page, catalogue
   * knowledge and the resource's {@code WriteLocksRegistry} semaphore. Every writer therefore
   * publishes into the same view and records into the same catalogue state. Only committed revisions
   * are looked up ({@link #loadIndexCatalogue} clamps a writer's prospective revision), so a file
   * left behind by a commit that was never acknowledged is never consulted and is overwritten by the
   * next commit of that revision number.
   */
  @SuppressWarnings("NullAway") // Catalogue snapshots are initialized and never set to null.
  private int resolveIndexCatalogueRevision(final Path indexesDir, final int revision) {
    final CatalogueRevisions known = knownIndexCatalogueRevisions.get();
    if (known.listed()) {
      return greatestAtOrBelow(known.revisions(), revision);
    }
    if (Files.exists(indexesDir.resolve(revision + ".xml"))) {
      return revision;
    }
    final int[] remembered = known.revisions();
    final int latest = remembered.length == 0
        ? NO_INDEX_CATALOGUE
        : remembered[remembered.length - 1];
    if (latest != NO_INDEX_CATALOGUE && latest <= revision) {
      return latest;
    }
    if (revision > 0 && Files.exists(indexesDir.resolve((revision - 1) + ".xml"))) {
      return revision - 1;
    }
    return greatestAtOrBelow(listIndexCatalogueRevisions(indexesDir), revision);
  }

  /**
   * The greatest revision of a sorted array at or below {@code revision}, or
   * {@link #NO_INDEX_CATALOGUE}.
   */
  private static int greatestAtOrBelow(final int[] sortedRevisions, final int revision) {
    int low = 0;
    int high = sortedRevisions.length - 1;
    int found = NO_INDEX_CATALOGUE;
    while (low <= high) {
      final int mid = (low + high) >>> 1;
      if (sortedRevisions[mid] <= revision) {
        found = sortedRevisions[mid];
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return found;
  }

  /**
   * ONE directory listing, remembering every catalogue file's revision for all sessions of this
   * resource, so that no later lookup touches the file system. The code before it probed revision,
   * revision-1, ..., 0 with one {@code Files.exists} each: O(revision) {@code access()} syscalls PER
   * CONTROLLER CREATION (measured: 50 MILLION calls building a 10k-revision resource with no
   * indexes).
   *
   * @return the sorted revisions of all catalogue files
   */
  private int[] listIndexCatalogueRevisions(final Path indexesDir) {
    final List<Integer> revisions = new ArrayList<>();
    if (Files.isDirectory(indexesDir)) {
      INDEX_CATALOGUE_DIRECTORY_LISTINGS.increment();
      try (final var children = Files.list(indexesDir)) {
        for (final var it = children.iterator(); it.hasNext();) {
          final String name = it.next().getFileName().toString();
          if (name.endsWith(".xml")) {
            try {
              revisions.add(Integer.parseInt(name.substring(0, name.length() - 4)));
            } catch (final NumberFormatException ignored) {
              // foreign file in the indexes directory — not ours to interpret
            }
          }
        }
      } catch (final IOException e) {
        throw new SirixIOException("Index definitions couldn't be listed!", e);
      }
    }
    final int[] sorted = revisions.stream().mapToInt(Integer::intValue).distinct().sorted().toArray();
    return knownIndexCatalogueRevisions.updateAndGet(
        current -> new CatalogueRevisions(mergeSorted(current.revisions(), sorted), true)).revisions();
  }

  /** Sorted union of two sorted arrays without duplicates. */
  private static int[] mergeSorted(final int[] left, final int[] right) {
    final int[] merged = new int[left.length + right.length];
    int l = 0;
    int r = 0;
    int n = 0;
    while (l < left.length || r < right.length) {
      final int next;
      if (r >= right.length || (l < left.length && left[l] <= right[r])) {
        next = left[l++];
      } else {
        next = right[r++];
      }
      if (n == 0 || merged[n - 1] != next) {
        merged[n++] = next;
      }
    }
    return n == merged.length
        ? merged
        : Arrays.copyOf(merged, n);
  }

  @Override
  public void recordSerializedIndexCatalogueRevision(final int revision) {
    checkArgument(revision >= 0, "revision must be >= 0!");
    INDEX_CATALOGUE_FILES_WRITTEN.increment();
    final boolean invalidated;
    synchronized (parsedIndexCatalogues) {
      invalidated = parsedIndexCatalogues.remove(revision) != null;
    }
    if (invalidated) {
      invalidateIndexControllers(revision);
    }
    rememberIndexCatalogueRevision(revision);
  }

  @SuppressWarnings("NullAway") // Catalogue snapshots are initialized and never set to null.
  private void rememberIndexCatalogueRevision(final int revision) {
    if (greatestAtOrBelow(knownIndexCatalogueRevisions.get().revisions(), revision) == revision) {
      return;
    }
    knownIndexCatalogueRevisions.updateAndGet(current -> greatestAtOrBelow(current.revisions(), revision) == revision
        ? current
        : new CatalogueRevisions(mergeSorted(current.revisions(), new int[] {revision}), current.listed()));
  }

  @Override
  public void invalidateIndexCataloguesAfter(final int revision) {
    checkArgument(revision >= 0, "revision must be >= 0!");
    synchronized (parsedIndexCatalogues) {
      parsedIndexCatalogues.keySet().removeIf((int catalogueRevision) -> catalogueRevision > revision);
    }
    knownIndexCatalogueRevisions.updateAndGet(current -> {
      final int[] revisions = current.revisions();
      int retained = Arrays.binarySearch(revisions, revision);
      retained = retained >= 0
          ? retained + 1
          : -retained - 1;
      return new CatalogueRevisions(Arrays.copyOf(revisions, retained), current.listed());
    });
    invalidateIndexControllers(revision + 1);
  }

  protected abstract void invalidateIndexControllers(int firstRevision);

  /**
   * Number of index-catalogue directory listings since the JVM started.
   *
   * <p>
   * Unconditional rather than gated behind a diagnostic flag: a listing is an {@code opendir}, a
   * {@code getdents} walk over every catalogue file and a {@code close}, so a striped counter
   * increment is free at this granularity. A writer that falls back to the listing on every commit
   * returns the same definitions and is invisible except as a commit that slows down as the resource
   * accumulates revisions; this counter is what makes that visible to a test.
   */
  public static long indexCatalogueDirectoryListings() {
    return INDEX_CATALOGUE_DIRECTORY_LISTINGS.sum();
  }

  /**
   * Number of index-catalogue files written since the JVM started, every one reported by the writer
   * that created it. Unconditional for the same reason as the listings: each event is a file
   * creation, an XML materialization and a metadata fsync on the commit path, and the counter is what
   * lets a test see that a commit which did not change its definitions wrote no catalogue.
   */
  public static long indexCatalogueFilesWritten() {
    return INDEX_CATALOGUE_FILES_WRITTEN.sum();
  }

  public Reader createReader() {
    return storage.createReader();
  }

  /**
   * Create a new {@link StorageEngineWriter}.
   *
   * @param id the transaction ID
   * @param representRevision the revision which is represented
   * @param storedRevision the revision which is stored
   * @param abort determines if a transaction must be aborted (rollback) or not
   * @return a new {@link StorageEngineWriter} instance
   */
  @Override
  public StorageEngineWriter createPageTransaction(final int id, final int representRevision, final int storedRevision,
      final Abort abort, final boolean isBoundToNodeTrx) {
    return createPageTransaction(id, representRevision, storedRevision, abort, isBoundToNodeTrx, null);
  }

  @Override
  public StorageEngineWriter createPageTransaction(final int id, final int representRevision, final int storedRevision,
      final Abort abort, final boolean isBoundToNodeTrx, final @Nullable UberPage pendingBaseUberPage) {
    checkArgument(id >= 0, "id must be >= 0!");
    checkArgument(representRevision >= 0, "representRevision must be >= 0!");
    checkArgument(storedRevision >= 0, "storedRevision must be >= 0!");

    final Writer writer = storage.createWriter();

    // Pipelined async commits pass the pending (phase-1-complete, canonical in-memory) uber page
    // of the still-hardening revision as the base for the successor epoch; readers keep resolving
    // "latest" through lastCommittedUberPage until the background hardening publishes it.
    final UberPage lastCommittedUberPage = pendingBaseUberPage != null
        ? pendingBaseUberPage
        : this.lastCommittedUberPage.get();
    final int lastCommittedRev = lastCommittedUberPage.getRevisionNumber();

    // Crash recovery runs BEFORE the writer is constructed, and the ordering is load-bearing.
    // createStorageEngineWriter eagerly reads pages (NamePage's dictionaries, for one) through the
    // shared caches. Doing that first meant the new transaction read the file while it still carried
    // the aborted commit's bytes and then kept those pages alive in swizzled PageReferences and its
    // own page guard — references the cache invalidation cannot reach — so the very content this
    // recovery exists to discard stayed visible to the transaction that triggered the recovery.
    // It also forced the invalidation to run against a transaction holding live guards, which is
    // where BufferManagerImpl's guard-draining came from. Truncating first leaves nothing loaded and
    // nothing guarded: the writer below reads the recovered file through clean caches.
    //
    // The commit marker legitimately exists while an in-process async commit hardens — the
    // truncate check exists for CRASH recovery and must not fire for pipelined successor epochs.
    if (pendingBaseUberPage == null) {
      truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists(writer, lastCommittedRev);
    }

    return this.storageEngineWriterFactory.createStorageEngineWriter(this,
        abort == Abort.YES && lastCommittedUberPage.isBootstrap()
            ? new UberPage()
            : new UberPage(lastCommittedUberPage),
        writer, id, representRevision, storedRevision, lastCommittedRev, isBoundToNodeTrx, bufferManager);
  }

  /** Depth-1 pipelined async commit: the pending (phase-1-complete, unhardened) revision root. */
  private final AtomicReference<PendingRevisionRoot> pendingRevisionRoot;

  private record PendingRevisionRoot(int revision, RevisionRootPage rootPage) {
  }

  @Override
  public void putPendingRevisionRoot(final int revision, final RevisionRootPage rootPage) {
    pendingRevisionRoot.set(new PendingRevisionRoot(revision, rootPage));
  }

  @Override
  public RevisionRootPage getPendingRevisionRoot(final int revision) {
    final PendingRevisionRoot pending = pendingRevisionRoot.get();
    return pending != null && pending.revision() == revision
        ? pending.rootPage()
        : null;
  }

  @Override
  public void clearPendingRevisionRoot(final int revision) {
    final PendingRevisionRoot pending = pendingRevisionRoot.get();
    if (pending != null && pending.revision() == revision) {
      pendingRevisionRoot.compareAndSet(pending, null);
    }
  }

  @Override
  public void detachNodePageWriteTransaction(final int transactionID) {
    // Pipelined async commit: the superseded page transaction is handed to the background
    // hardening thread, which closes it after the beacon write — remove it from the map WITHOUT
    // closing so the successor can register itself.
    storageEngineWriterMap.remove(transactionID);
  }

  private void truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists(final Writer writer,
      final int lastCommittedRev) {
    if (!Files.exists(getCommitFile())) {
      return;
    }
    // A backend that cannot truncate must not be ASKED to. This runs on the way into every
    // beginNodeTrx, and a commit marker only disappears on a successful commit or rollback — so a
    // throw here is not one failed transaction, it is a resource that can never open a write
    // transaction again. In-memory storage cannot identify the pages to discard (no per-revision
    // tracking, no retained previous uber page), and its data does not outlive the process, so the
    // marker describes a commit whose pages are gone with it: log and carry on.
    if (!writer.supportsTruncateTo()) {
      LOGGER.warn(
          "Resource {} has a commit marker from an aborted commit, but {} cannot truncate —"
              + " skipping crash recovery. Use a persistent StorageType where rollback matters.",
          resourceConfig.getResource(), writer.getClass().getSimpleName());
      return;
    }
    invalidateIndexCataloguesAfter(lastCommittedRev);
    writer.truncateTo(lastCommittedRev);
    // The truncated range's offsets are reused by subsequent commits, but pages of the aborted
    // commit may already sit in the warm global caches under those offsets (caches survive
    // close now) — drop THIS RESOURCE's entries so post-recovery reads can never observe
    // pre-truncation bytes. Resource-scoped: a sibling resource's file was not truncated, so its
    // cached pages are still valid and may be in active use.
    Databases.clearCachesForResource(resourceConfig.getDatabaseId(), resourceConfig.getID());
  }

  @Override
  public List<RevisionInfo> getHistory() {
    return getHistoryInformations(Integer.MAX_VALUE);
  }

  @Override
  public List<RevisionInfo> getHistory(int revisions) {
    return getHistoryInformations(revisions);
  }

  @Override
  public List<RevisionInfo> getHistory(int fromRevision, int toRevision) {
    assertAccess(fromRevision);
    assertAccess(toRevision);

    checkArgument(fromRevision > 0 && toRevision > 0, "Revision numbers must be positive, but got %s and %s.",
        fromRevision, toRevision);

    // Accept both argument orders (callers like the REST history endpoint naturally pass an
    // ascending [start, end]) and from == to; results are returned newest-first like the other
    // history overloads.
    final int newestRevision = Math.max(fromRevision, toRevision);
    final int oldestRevision = Math.min(fromRevision, toRevision);

    return buildHistory(newestRevision, oldestRevision);
  }

  @Override
  public long[] getHistoryTimestamps() {
    assertNotClosed();
    final int newest = getMostRecentRevisionNumber();
    if (newest < 1) {
      return EMPTY_LONG_ARRAY;
    }
    return historyTimestampsNewestFirst(newest, 1);
  }

  @Override
  public long[] getHistoryTimestamps(final int fromRevision, final int toRevision) {
    assertAccess(fromRevision);
    assertAccess(toRevision);

    checkArgument(fromRevision > 0 && toRevision > 0, "Revision numbers must be positive, but got %s and %s.",
        fromRevision, toRevision);

    final int newest = Math.max(fromRevision, toRevision);
    final int oldest = Math.min(fromRevision, toRevision);

    return historyTimestampsNewestFirst(newest, oldest);
  }

  /**
   * Read the commit timestamps (epoch millis) for the inclusive revision range
   * {@code [oldest, newest]} from the in-memory {@link RevisionIndex} and return them newest-first.
   * No {@link StorageEngineReader} is opened and no {@code RevisionRootPage} is read — the timestamps
   * are already resident.
   *
   * <p>
   * If the in-memory index lags the requested range (a fresh process, or out-of-band growth by
   * another writer), it is resynced once from disk via {@link IOStorage#loadRevisionIndex}, mirroring
   * the storage-open path.
   */
  private long[] historyTimestampsNewestFirst(final int newest, final int oldest) {
    final RevisionIndexHolder holder = storage.getRevisionIndexHolder();
    RevisionIndex index = holder.get();
    if (index.size() <= newest) {
      storage.loadRevisionIndex(holder);
      index = holder.get();
    }
    if (index.size() <= newest) {
      throw new IllegalStateException(
          "Revision index holds " + index.size() + " entries but revision " + newest + " was requested.");
    }

    // RevisionIndex stores timestamps in ascending revision order; reverse in place to honour
    // the newest-first contract shared by all history queries.
    final long[] timestamps = index.timestampsMillis(oldest, newest);
    for (int lo = 0, hi = timestamps.length - 1; lo < hi; lo++, hi--) {
      final long tmp = timestamps[lo];
      timestamps[lo] = timestamps[hi];
      timestamps[hi] = tmp;
    }
    return timestamps;
  }

  private List<RevisionInfo> getHistoryInformations(final int revisions) {
    checkArgument(revisions > 0);

    final int newest = getMostRecentRevisionNumber();
    if (newest < 1) {
      return List.of();
    }
    // The most recent `revisions` revisions, clamped to the first user revision (1); revision 0
    // is the empty bootstrap and is never reported.
    final int oldest = revisions >= newest
        ? 1
        : newest - revisions + 1;
    return buildHistory(newest, oldest);
  }

  /**
   * Build the history (newest revision first) for the inclusive revision range
   * {@code [oldest, newest]}.
   *
   * <p>
   * Already-seen, immutable revisions are served synchronously from {@link #REVISION_INFO_CACHE}
   * straight into a pre-sized result array — no per-revision {@link CompletableFuture}, no stream
   * pipeline. Only cache misses (cold revisions) open a {@link StorageEngineReader}, and those run in
   * parallel so a cold first call still overlaps its I/O across revisions.
   */
  private List<RevisionInfo> buildHistory(final int newest, final int oldest) {
    final int count = newest - oldest + 1;
    final RevisionInfo[] result = new RevisionInfo[count];

    List<CompletableFuture<Void>> misses = null;
    for (int i = 0; i < count; i++) {
      final int revision = newest - i;
      final var cacheKey = new RevisionInfoKey(resourceConfig.getDatabaseId(), resourceConfig.getID(), revision);
      final RevisionInfo cached = REVISION_INFO_CACHE.getIfPresent(cacheKey);
      if (cached != null) {
        result[i] = cached;
      } else {
        final int slot = i;
        if (misses == null) {
          misses = new ArrayList<>();
        }
        misses.add(
            CompletableFuture.runAsync(() -> result[slot] = REVISION_INFO_CACHE.get(cacheKey, this::loadRevisionInfo)));
      }
    }

    if (misses != null) {
      CompletableFuture.allOf(misses.toArray(EMPTY_FUTURES)).join();
    }

    return List.of(result);
  }

  /**
   * Cold-path loader for one revision's {@link RevisionInfo}: reads the commit credentials and
   * timestamp directly through a {@link StorageEngineReader}, bypassing the full node-transaction
   * machinery (node cursor, item list, per-trx wiring) that {@code beginNodeReadOnlyTrx} sets up.
   */
  private RevisionInfo loadRevisionInfo(final RevisionInfoKey key) {
    final int revision = key.revision();
    try (final StorageEngineReader reader = createStorageEngineReader(revision)) {
      final CommitCredentials commitCredentials = reader.getCommitCredentials();
      return new RevisionInfo(commitCredentials.getUser(), revision,
          Instant.ofEpochMilli(reader.getActualRevisionRootPage().getRevisionTimestamp()),
          commitCredentials.getMessage());
    }
  }

  @Override
  public int[] getRecordChangeRevisions(final long nodeKey) {
    assertNotClosed();

    // The RECORD_TO_REVISIONS index only exists when the resource was created with
    // storeNodeHistory; otherwise the trie infrastructure is shared across index types and a
    // lookup could return an unrelated record, so guard up-front (mirrors RecordRevisionsLookup).
    if (!resourceConfig.storeNodeHistory()) {
      return EMPTY_INT_ARRAY;
    }

    final int newest = getMostRecentRevisionNumber();
    if (newest < 1) {
      return EMPTY_INT_ARRAY;
    }

    // RECORD_TO_REVISIONS is itself versioned; reading it at the most recent revision yields the
    // complete set of revisions in which the record was ever created or modified.
    try (final StorageEngineReader reader = createStorageEngineReader(newest)) {
      final DataRecord record = reader.getRecord(nodeKey, IndexType.RECORD_TO_REVISIONS, 0);
      if (record instanceof RevisionReferencesNode revisionReferences) {
        final int[] revisions = revisionReferences.getRevisions();
        if (revisions != null && revisions.length > 0) {
          // Defensive copy — the array on the node is the live, read-only index entry.
          return revisions.clone();
        }
      }
      return EMPTY_INT_ARRAY;
    }
  }

  @Override
  public void scanRecordHistory(final long nodeKey, final RecordHistoryVisitor visitor) {
    requireNonNull(visitor);
    assertNotClosed();

    if (resourceConfig.storeNodeHistory()) {
      // Fast path: only the revisions in which the record actually changed need to be read; its
      // value is unchanged in between.
      for (final int revision : getRecordChangeRevisions(nodeKey)) {
        visitRecord(nodeKey, revision, visitor);
      }
    } else {
      // No node-history index — scan every user revision, still via the lightweight reader path.
      final int newest = getMostRecentRevisionNumber();
      for (int revision = 1; revision <= newest; revision++) {
        visitRecord(nodeKey, revision, visitor);
      }
    }
  }

  /**
   * Read {@code nodeKey} at {@code revision} through a lightweight {@link StorageEngineReader} (no
   * {@link NodeReadOnlyTrx} wrapper, document-node fetch, or trx bookkeeping) and hand it to
   * {@code visitor} if the record exists in that revision. The reader stays open for the duration of
   * the callback so the record remains valid.
   */
  private void visitRecord(final long nodeKey, final int revision, final RecordHistoryVisitor visitor) {
    try (final StorageEngineReader reader = createStorageEngineReader(revision)) {
      final DataRecord record = reader.getRecord(nodeKey, IndexType.DOCUMENT, -1);
      if (record != null) {
        visitor.visit(revision, record);
      }
    }
  }

  @Override
  public void scanValueRuns(final long nodeKey, final RecordRunVisitor visitor) {
    requireNonNull(visitor);
    assertNotClosed();

    final int maxRevision = getMostRecentRevisionNumber();
    if (maxRevision < 1) {
      return;
    }

    if (resourceConfig.storeNodeHistory()) {
      // Each entry in the change set starts a run that holds until the revision before the next
      // change (or the most recent revision for the final entry). The record is read once per run.
      final int[] changeRevisions = getRecordChangeRevisions(nodeKey);
      for (int i = 0; i < changeRevisions.length; i++) {
        final int fromRevision = changeRevisions[i];
        final int toRevision = (i + 1 < changeRevisions.length)
            ? changeRevisions[i + 1] - 1
            : maxRevision;
        visitRun(nodeKey, fromRevision, toRevision, visitor);
      }
    } else {
      // No node-history index — value-change boundaries are unknown, so report each existing
      // revision as its own single-revision run (still via the lightweight reader path).
      for (int revision = 1; revision <= maxRevision; revision++) {
        visitRun(nodeKey, revision, revision, visitor);
      }
    }
  }

  /**
   * Read {@code nodeKey} at {@code fromRevision} through a lightweight {@link StorageEngineReader}
   * and report the run {@code [fromRevision, toRevision]} to {@code visitor} when the record exists.
   * The reader stays open for the duration of the callback so the record remains valid.
   */
  private void visitRun(final long nodeKey, final int fromRevision, final int toRevision,
      final RecordRunVisitor visitor) {
    try (final StorageEngineReader reader = createStorageEngineReader(fromRevision)) {
      final DataRecord record = reader.getRecord(nodeKey, IndexType.DOCUMENT, -1);
      if (record != null) {
        visitor.visit(fromRevision, toRevision, record);
      }
    }
  }

  @Override
  public Path getResourcePath() {
    assertNotClosed();

    return resourceConfig.resourcePath;
  }

  @Override
  public Lock getCommitLock() {
    assertNotClosed();

    return commitLock;
  }

  @Override
  public R beginNodeReadOnlyTrx(final int revision) {
    // Read-only opens have no shared-state concurrency concerns:
    // * assertAccess() reads a volatile and a volatile-published epoch — thread-safe.
    // * createStorageEngineReader() uses AtomicInteger for IDs and ConcurrentMap for the
    // bookkeeping — already not synchronized.
    // * getDocumentNode() operates on the just-constructed reader (per-thread ownership).
    // * trxIDCounter is an AtomicInteger; nodeTrxMap is a ConcurrentMap.
    // Removing the per-session monitor allows N concurrent reader-opens to run in parallel,
    // unblocking the depth-N pipeline in the prefetched temporal axes (and any other caller
    // that opens multiple rtxs back-to-back from concurrent threads).
    assertAccess(revision);

    final StorageEngineReader storageEngineReader = createStorageEngineReader(revision);

    // Every failure after the reader exists must close it: it holds an epoch ticket, a page
    // reader and an entry in storageEngineReaderMap, none of which anything else would reclaim
    // (getDocumentNode already closes on its own failure path — closing twice is idempotent).
    boolean success = false;
    try {
      final Node documentNode = getDocumentNode(storageEngineReader);

      // Create new reader.
      final R reader = createNodeReadOnlyTrx(trxIDCounter.incrementAndGet(), storageEngineReader, documentNode);

      // Remember reader for debugging and safe close.
      if (nodeTrxMap.put(reader.getId(), reader) != null) {
        throw new SirixUsageException(ID_GENERATION_EXCEPTION);
      }
      TransactionMetrics.onReadOnlyTrxOpened();

      success = true;
      return reader;
    } finally {
      if (!success) {
        storageEngineReader.close();
      }
    }
  }

  public abstract R createNodeReadOnlyTrx(int nodeTrxId, StorageEngineReader storageEngineReader, Node documentNode);

  public abstract W createNodeReadWriteTrx(int nodeTrxId, StorageEngineWriter storageEngineWriter, int maxNodeCount,
      Duration autoCommitDelay, Node documentNode, AfterCommitState afterCommitState);

  static Node getDocumentNode(final StorageEngineReader storageEngineReader) {
    final Node node =
        storageEngineReader.getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), IndexType.DOCUMENT, -1);
    if (node == null) {
      storageEngineReader.close();
      throw new IllegalStateException("Node couldn't be fetched from persistent storage!");
    }

    return node;
  }

  /**
   * A commit file which is used by a {@link XmlNodeTrx} to denote if it's currently commiting or not.
   */
  @Override
  public Path getCommitFile() {
    return resourceConfig.resourcePath.resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
                                      .resolve(".commit");
  }

  @Override
  public W beginNodeTrx() {
    return beginNodeTrx(0, 0, TimeUnit.MILLISECONDS, AfterCommitState.KEEP_OPEN);
  }

  @Override
  public W beginNodeTrx(final int maxNodeCount) {
    return beginNodeTrx(maxNodeCount, 0, TimeUnit.MILLISECONDS, AfterCommitState.KEEP_OPEN);
  }

  @Override
  public W beginNodeTrx(final int maxTime, final TimeUnit timeUnit) {
    return beginNodeTrx(0, maxTime, timeUnit, AfterCommitState.KEEP_OPEN);
  }

  @Override
  public W beginNodeTrx(final int maxNodeCount, final int maxTime, final TimeUnit timeUnit) {
    return beginNodeTrx(maxNodeCount, maxTime, timeUnit, AfterCommitState.KEEP_OPEN);
  }

  @Override
  public W beginNodeTrx(final AfterCommitState afterCommitState) {
    return beginNodeTrx(0, 0, TimeUnit.MILLISECONDS, afterCommitState);
  }

  @Override
  public W beginNodeTrx(final int maxNodeCount, final AfterCommitState afterCommitState) {
    return beginNodeTrx(maxNodeCount, 0, TimeUnit.MILLISECONDS, afterCommitState);
  }

  @Override
  public W beginNodeTrx(final int maxTime, final TimeUnit timeUnit, final AfterCommitState afterCommitState) {
    return beginNodeTrx(0, maxTime, timeUnit, afterCommitState);
  }

  @Override
  public synchronized W beginNodeTrx(final int maxNodeCount, final int maxTime, final TimeUnit timeUnit,
      final AfterCommitState afterCommitState) {
    // Checks.
    assertAccess(getMostRecentRevisionNumber());
    if (maxNodeCount < 0 || maxTime < 0) {
      throw new SirixUsageException("maxNodeCount may not be < 0!");
    }
    requireNonNull(timeUnit);

    // KEEP_OPEN_ASYNC_FLUSH / KEEP_OPEN_ASYNC_COMMIT runtime guards.
    //
    // Both async modes append through FileChannelWriter regardless of backend (the
    // MEMORY_MAPPED storage constructs the identical writer — see MMStorage.createWriter),
    // so KEEP_OPEN_ASYNC_FLUSH is supported on FILE_CHANNEL and MEMORY_MAPPED alike. The
    // memory-mapped read side stays correct by ORDERING, not isolation: an async-flushed
    // offset becomes reachable (via cleanupSnapshot applying it to the page reference)
    // only after the background thread has fully appended and flushed the buffered tail
    // and released the flush permit — so any subsequent read of that offset, whether
    // through the write transaction's FileChannelReader or through an MMFileReader that a
    // fragment re-read obtains from resourceSession.createReader() (which remaps to the
    // grown file size under MMStorage's remapLock), observes fully written bytes via the
    // POSIX-unified page cache. Read-only sessions additionally never reach unpublished
    // offsets at all before the final synchronous commit publishes the revision.
    // KEEP_OPEN_ASYNC_COMMIT additionally publishes durable revisions from the background
    // thread and stays FILE_CHANNEL-only until that mid-transaction publication is
    // validated against concurrently remapping memory-mapped readers.
    if (afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH
        || afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_COMMIT) {
      final StorageType storageType = getResourceConfig().getStorageType();
      final boolean supportedBackend = storageType == StorageType.FILE_CHANNEL
          || (afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH && storageType == StorageType.MEMORY_MAPPED);
      if (!supportedBackend) {
        throw new IllegalArgumentException(afterCommitState + " requires the FILE_CHANNEL"
            + (afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH
                ? " or MEMORY_MAPPED"
                : "")
            + " storage backend; got " + storageType);
      }
      if (maxTime > 0) {
        throw new IllegalArgumentException(
            afterCommitState + " does not support timed auto-commit; use count-based only");
      }
    }

    // Make sure not to exceed available number of write transactions.
    // The writeLock is shared across ALL ResourceSession instances for the same resource
    // (via WriteLocksRegistry), so we cannot detect orphaned locks by checking only this
    // session's transaction maps - another session may legitimately hold the lock.
    try {
      if (!writeLock.tryAcquire(5, TimeUnit.SECONDS)) {
        throw new SirixUsageException(
            "No read-write transaction available, please close the running read-write transaction first.");
      }
    } catch (final InterruptedException e) {
      throw new SirixThreadedException(e);
    }

    LOGGER.trace("Lock: lock acquired (beginNodeTrx)");

    boolean success = false;
    try {
      // Create new storage engine writer (shares the same ID with the node write trx).
      final int nodeTrxId = trxIDCounter.incrementAndGet();
      final int lastRev = getMostRecentRevisionNumber();
      final StorageEngineWriter storageEngineWriter =
          createPageTransaction(nodeTrxId, lastRev, lastRev, Abort.NO, true);

      final W wtx;
      try {
        final Node documentNode = getDocumentNode(storageEngineWriter);

        // Create new node write transaction. Listener binding happens inside this construction and
        // can deliberately fail closed (for example, an inconsistent persisted projection without
        // its required path summary). Until construction returns, the writer is not registered in
        // either transaction map, so no ordinary close path can find it.
        final var autoCommitDelay = Duration.of(maxTime, timeUnit.toChronoUnit());
        wtx = createNodeReadWriteTrx(nodeTrxId, storageEngineWriter, maxNodeCount, autoCommitDelay, documentNode,
            afterCommitState);
      } catch (final RuntimeException | Error constructionFailure) {
        closeUnregisteredWriter(storageEngineWriter, constructionFailure);
        throw constructionFailure;
      }

      // Remember node transaction for debugging and safe close.
      // noinspection unchecked
      if (nodeTrxMap.put(nodeTrxId, (R) wtx) != null
          || storageEngineWriterMap.put(nodeTrxId, storageEngineWriter) != null) {
        // Clean up: remove any entries we just inserted, then close resources.
        nodeTrxMap.remove(nodeTrxId);
        storageEngineWriterMap.remove(nodeTrxId);
        try {
          wtx.close();
        } finally {
          storageEngineWriter.close();
        }
        throw new SirixThreadedException(ID_GENERATION_EXCEPTION);
      }
      TransactionMetrics.onReadWriteTrxOpened();

      success = true;
      return wtx;
    } finally {
      if (!success) {
        writeLock.release();
        LOGGER.trace("Lock: lock released (beginNodeTrx failed)");
      }
    }
  }

  /**
   * Close a writer whose node transaction constructor failed, without masking that primary failure.
   */
  private static void closeUnregisteredWriter(final StorageEngineWriter storageEngineWriter,
      final Throwable primaryFailure) {
    try {
      storageEngineWriter.close();
    } catch (final RuntimeException | Error cleanupFailure) {
      if (cleanupFailure != primaryFailure) {
        try {
          primaryFailure.addSuppressed(cleanupFailure);
        } catch (final RuntimeException | Error ignored) {
          // The constructor failure remains authoritative even if diagnostics cannot be attached.
        }
      }
    }
  }

  @Override
  public synchronized void close() {
    if (!isClosed) {
      // NOTE: ClockSweepers are GLOBAL now - don't stop them per-session
      // They continue running, managed by BufferManager lifecycle

      // Close all shared per-thread read-only transactions.
      for (final NodeReadOnlyTrx rtx : sharedTrxMap.values()) {
        rtx.close();
      }
      sharedTrxMap.clear();

      // Close all open node transactions.
      for (NodeReadOnlyTrx rtx : nodeTrxMap.values()) {
        if (rtx instanceof XmlNodeTrx xmlNodeTrx) {
          xmlNodeTrx.rollback();
        } else if (rtx instanceof JsonNodeTrx jsonNodeTrx) {
          jsonNodeTrx.rollback();
        }
        rtx.close();
      }
      // Close all open storage engine writers.
      for (StorageEngineReader rtx : storageEngineWriterMap.values()) {
        rtx.close();
      }
      // Close all open storage engine readers.
      for (StorageEngineReader rtx : storageEngineReaderMap.values()) {
        rtx.close();
      }

      // NOTE: Don't clear BufferManager caches here - other sessions might be using same resource!
      // Pages will be evicted by normal cache LRU policy or cleaned up at database close
      // PostgreSQL-style: buffers released when ALL sessions release them, not when one closes

      // Immediately release all resources.
      nodeTrxMap.clear();
      storageEngineReaderMap.clear();
      storageEngineWriterMap.clear();
      if (pool.get() != null) {
        pool.get().close();
      }
      if (sharedSession == null) {
        storage.close();
      }
      resourceStore.closeResourceSession(resourceConfig.getResource());
      isClosed = true;
    }
  }

  /**
   * Checks for valid revision.
   *
   * @param revision revision number to check
   * @throws IllegalStateException if {@link XmlResourceSessionImpl} is already closed
   * @throws IllegalArgumentException if revision isn't valid
   */
  @Override
  public void assertAccess(final int revision) {
    assertNotClosed();
    if (revision > getMostRecentRevisionNumber()) {
      throw new IllegalArgumentException(
          "Revision must not be bigger than " + Long.toString(getMostRecentRevisionNumber()) + "!");
    }
  }

  @Override
  public String toString() {
    return "ResourceSession{" + "resourceConfig=" + resourceConfig + ", isClosed=" + isClosed + '}';
  }

  private void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Resource session is already closed!");
    }
  }

  @Override
  public boolean hasRunningNodeWriteTrx() {
    assertNotClosed();
    if (writeLock.tryAcquire()) {
      writeLock.release();
      return false;
    }

    return true;
  }

  /**
   * Set a new storage engine writer for a node transaction.
   *
   * @param transactionID storage engine writer transaction ID
   * @param storageEngineWriter storage engine writer
   */
  @Override
  public void setNodePageWriteTransaction(final int transactionID, final StorageEngineWriter storageEngineWriter) {
    assertNotClosed();
    storageEngineWriterMap.put(transactionID, storageEngineWriter);
  }

  /**
   * Close a storage engine writer for a node transaction.
   *
   * @param transactionID storage engine writer transaction ID
   * @throws SirixIOException if an I/O error occurs
   */
  @Override
  public void closeNodePageWriteTransaction(final int transactionID) {
    assertNotClosed();
    final StorageEngineReader storageEngineReader = storageEngineWriterMap.remove(transactionID);
    if (storageEngineReader != null) {
      storageEngineReader.close();
    }
  }

  /**
   * Close a write transaction.
   *
   * @param transactionID write transaction ID
   */
  @Override
  public void closeWriteTransaction(final int transactionID) {
    assertNotClosed();

    // Remove from internal map.
    removeFromPageMapping(transactionID);

    // Make new transactions available.
    LOGGER.trace("Lock unlock (closeWriteTransaction).");
    writeLock.release();
  }

  /**
   * Close a read transaction.
   *
   * @param transactionID read transaction ID
   */
  @Override
  public void closeReadTransaction(final int transactionID) {
    assertNotClosed();

    // Remove from internal map.
    removeFromPageMapping(transactionID);
  }

  /**
   * Close a storage engine writer that is NOT bound to a node transaction.
   *
   * @param transactionID storage engine writer ID
   * @param storageEngineWriter the writer being closed; the bookkeeping entry is dropped only if the
   *        map still maps {@code transactionID} to exactly this instance
   */
  @Override
  public void closePageWriteTransaction(final int transactionID, final StorageEngineWriter storageEngineWriter) {
    assertNotClosed();

    // Remove from internal map. Identity-scoped: see closePageReadTransaction.
    storageEngineReaderMap.remove(transactionID, requireNonNull(storageEngineWriter));

    // Make new transactions available. Unconditional — this writer took a permit in
    // createStorageEngineWriter whether or not its bookkeeping entry is still present.
    LOGGER.trace("Lock unlock (closePageWriteTransaction).");
    writeLock.release();
  }

  /**
   * Close a storage engine reader: drop it from {@link #storageEngineReaderMap}.
   *
   * <p>
   * Removal is by (key, value) rather than by key alone. A storage engine reader bound to a node
   * transaction carries that transaction's id, and while ids are unique session-wide (see
   * {@link #trxIDCounter}) the two maps are keyed by different populations — so a bare
   * {@code remove(id)} from a bound reader could only ever be a no-op or, if the id spaces were ever
   * allowed to overlap again, evict a live foreign reader. Identity removal makes that structurally
   * impossible and is idempotent, so a double close cannot drop a successor that reused the id.
   *
   * @param transactionID storage engine reader ID
   * @param storageEngineReader the reader being closed
   */
  @Override
  public void closePageReadTransaction(final int transactionID, final StorageEngineReader storageEngineReader) {
    assertNotClosed();

    // Remove from internal map.
    storageEngineReaderMap.remove(transactionID, requireNonNull(storageEngineReader));
  }

  /**
   * Remove from internal maps.
   *
   * @param transactionID transaction ID to remove
   */
  private void removeFromPageMapping(final Integer transactionID) {
    assertNotClosed();

    // Capture removed entries so we can decrement the right activity counter.
    // storageEngineWriterMap is populated only for read-write trx; nodeTrxMap is
    // populated for both. The presence of a writer entry is the discriminator.
    final R removedTrx = nodeTrxMap.remove(transactionID);
    final StorageEngineWriter removedWriter = storageEngineWriterMap.remove(transactionID);

    if (removedTrx != null) {
      if (removedWriter != null) {
        TransactionMetrics.onReadWriteTrxClosed();
      } else {
        TransactionMetrics.onReadOnlyTrxClosed();
      }
    }
  }

  @Override
  public synchronized boolean isClosed() {
    return isClosed;
  }

  /**
   * Set last commited {@link UberPage}.
   *
   * @param page the new {@link UberPage}
   */
  @Override
  public void setLastCommittedUberPage(final UberPage page) {
    assertNotClosed();

    lastCommittedUberPage.set(requireNonNull(page));
  }

  @Override
  public ResourceConfiguration getResourceConfig() {
    assertNotClosed();

    return resourceConfig;
  }

  /**
   * Get the revision epoch tracker for this resource session.
   *
   * @return the revision epoch tracker
   */
  public RevisionEpochTracker getRevisionEpochTracker() {
    return revisionEpochTracker;
  }

  @Override
  public int getMostRecentRevisionNumber() {
    assertNotClosed();

    return lastCommittedUberPage.get().getRevisionNumber();
  }

  @Override
  public synchronized PathSummaryReader openPathSummary(final int revision) {
    assertAccess(revision);

    StorageEngineReader storageEngineReader;

    final ObjectPool<StorageEngineReader> currentPool = this.pool.get();

    if (currentPool != null) {
      final StorageEngineReader borrowed = currentPool.borrowObject();

      if (borrowed.isClosed() || borrowed.getRevisionNumber() != revision) {
        currentPool.returnObject(borrowed);
        storageEngineReader = createStorageEngineReader(revision);
      } else {
        storageEngineReader = borrowed;
      }
    } else {
      storageEngineReader = createStorageEngineReader(revision);
    }

    return PathSummaryReader.getInstance(storageEngineReader, this);
  }

  @Override
  public StorageEngineReader createStorageEngineReader(final int revision) {
    assertAccess(revision);

    final int currentStorageEngineID = trxIDCounter.incrementAndGet();
    final NodeStorageEngineReader storageEngineReader =
        new NodeStorageEngineReader(currentStorageEngineID, this, lastCommittedUberPage.get(), revision,
            storage.createReader(), bufferManager, new RevisionRootPageReader(), null);
    // Remember storage engine reader for debugging and safe close.
    if (storageEngineReaderMap.put(currentStorageEngineID, storageEngineReader) != null) {
      throw new SirixThreadedException(ID_GENERATION_EXCEPTION);
    }

    return storageEngineReader;
  }

  @Override
  public synchronized StorageEngineWriter createStorageEngineWriter(final int revision) {
    assertAccess(revision);

    // Make sure not to exceed available number of write transactions.
    try {
      if (!writeLock.tryAcquire(20, TimeUnit.SECONDS)) {
        throw new SirixUsageException("No write transaction available, please close the write transaction first.");
      }
    } catch (final InterruptedException e) {
      throw new SirixThreadedException(e);
    }

    LOGGER.debug("Lock: lock acquired (createStorageEngineWriter)");

    boolean success = false;
    try {
      final int currentStorageEngineID = trxIDCounter.incrementAndGet();
      final int lastRev = getMostRecentRevisionNumber();
      final StorageEngineWriter storageEngineWriter =
          createPageTransaction(currentStorageEngineID, lastRev, lastRev, Abort.NO, false);

      // Remember storage engine writer for debugging and safe close.
      if (storageEngineReaderMap.put(currentStorageEngineID, storageEngineWriter) != null) {
        throw new SirixThreadedException(ID_GENERATION_EXCEPTION);
      }

      success = true;
      return storageEngineWriter;
    } finally {
      if (!success) {
        writeLock.release();
        LOGGER.debug("Lock: lock released (createStorageEngineWriter failed)");
      }
    }
  }

  @Override
  public Optional<R> getNodeReadTrxByTrxId(final Integer ID) {
    assertNotClosed();

    return Optional.ofNullable(nodeTrxMap.get(ID));
  }

  @Override
  public synchronized Optional<W> getNodeTrx() {
    assertNotClosed();

    // noinspection unchecked
    return nodeTrxMap.values().stream().filter(NodeTrx.class::isInstance).map(rtx -> (W) rtx).findAny();
  }

  /**
   * Number of currently-open read-only or read-write node transactions on this session. Useful for
   * diagnostics and for asserting the absence of resource leaks in tests.
   */
  public int activeTrxCount() {
    return nodeTrxMap.size();
  }

  /**
   * Number of storage engine readers/writers this session still tracks as open. Every entry is
   * dropped when its reader closes, so a workload that opens and closes transactions in a loop must
   * leave this bounded — it is the direct leak signal for {@link #storageEngineReaderMap}.
   */
  public int activeStorageEngineReaderCount() {
    return storageEngineReaderMap.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public R beginNodeReadOnlyTrx(final Instant pointInTime) {
    requireNonNull(pointInTime);
    assertNotClosed();

    final int revision = getRevisionNumber(pointInTime);
    return beginNodeReadOnlyTrx(revision);
  }

  private int binarySearch(final long timestamp) {
    if (USE_OPTIMIZED_REVISION_SEARCH) {
      return binarySearchOptimized(timestamp);
    }
    return binarySearchLegacy(timestamp);
  }

  /**
   * Optimized revision search using SIMD/Eytzinger layout. Uses cache-friendly data structures for
   * better performance.
   * 
   * @param timestamp the timestamp to search for (epoch millis)
   * @return revision index if exact match, or -(insertionPoint + 1) if not found
   */
  private int binarySearchOptimized(final long timestamp) {
    final RevisionIndexHolder holder = storage.getRevisionIndexHolder();
    final RevisionIndex index = holder.get();
    return index.findRevision(timestamp);
  }

  /**
   * Legacy binary search implementation. Kept for rollback purposes if optimized search has issues.
   * 
   * @param timestamp the timestamp to search for (epoch millis)
   * @return revision index if exact match, or -(insertionPoint + 1) if not found
   */
  private int binarySearchLegacy(final long timestamp) {
    int low = 0;
    int high = getMostRecentRevisionNumber();

    try (final Reader reader = storage.createReader()) {
      while (low <= high) {
        final int mid = (low + high) >>> 1;

        final Instant midVal = reader.readRevisionRootPageCommitTimestamp(mid);
        final int cmp = midVal.compareTo(Instant.ofEpochMilli(timestamp));

        if (cmp < 0)
          low = mid + 1;
        else if (cmp > 0)
          high = mid - 1;
        else
          return mid; // key found
      }
    }

    return -(low + 1); // key not found
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getRevisionNumber(final Instant pointInTime) {
    requireNonNull(pointInTime);
    assertNotClosed();

    final long timestamp = pointInTime.toEpochMilli();
    final int mostRecentRevision = getMostRecentRevisionNumber();

    int revision = binarySearch(timestamp);

    if (revision >= 0) {
      // Exact match found
      return revision;
    }

    // revision < 0 means not found, convert to insertion point
    final int insertionPoint = -revision - 1;

    if (insertionPoint == 0) {
      // Timestamp is before all revisions - return earliest (revision 0)
      return 0;
    } else if (insertionPoint > mostRecentRevision) {
      // Timestamp is after all revisions - return most recent
      return mostRecentRevision;
    } else {
      // Timestamp is between revisions - return the floor (previous revision)
      return insertionPoint - 1;
    }
  }

  @Override
  public int getRevisionNumber(final Instant pointInTime, final int revisionCeiling) {
    requireNonNull(pointInTime);
    checkArgument(revisionCeiling >= 0, "Revision ceiling must not be negative.");
    assertAccess(revisionCeiling);

    final RevisionIndex index = storage.getRevisionIndexHolder().get();
    final long timestamp = pointInTime.toEpochMilli();
    if (timestamp >= index.getTimestampMillis(revisionCeiling)) {
      return revisionCeiling;
    }

    int low = 0;
    int high = revisionCeiling - 1;
    while (low <= high) {
      final int mid = (low + high) >>> 1;
      if (index.getTimestampMillis(mid) <= timestamp) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return Math.max(0, high);
  }

  @Override
  public Optional<User> getUser() {
    assertNotClosed();

    return Optional.ofNullable(user);
  }

  @Override
  public R getOrCreateSharedReadOnlyTrx(final int revision) {
    final var key = new SharedTrxKey(Thread.currentThread().threadId(), revision);
    return sharedTrxMap.computeIfAbsent(key, k -> beginNodeReadOnlyTrx(revision));
  }

  @Override
  public void closeSharedReadOnlyTrxs(final int revision) {
    final var iterator = sharedTrxMap.entrySet().iterator();
    while (iterator.hasNext()) {
      final var entry = iterator.next();
      if (entry.getKey().revision() == revision) {
        entry.getValue().close();
        iterator.remove();
      }
    }
  }
}
