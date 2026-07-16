package io.sirix.access.trx.node;

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
import io.sirix.cache.Cache;
import io.sirix.cache.RBIndexKey;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixThreadedException;
import io.sirix.exception.SirixUsageException;
import io.sirix.index.IndexType;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
   * Cache key for {@link #REVISION_INFO_CACHE}: database ids are random positive longs persisted
   * per database and claimed per directory in-JVM, resource ids are persisted per resource — the
   * pair is stable and unique, so (databaseId, resourceId, revision) can never be misattributed
   * across resources.
   */
  private record RevisionInfoKey(long databaseId, long resourceId, int revision) {
  }

  /**
   * GLOBAL cache of (databaseId, resourceId, revision) → {@link RevisionInfo} (author, timestamp,
   * commit message). A committed revision is immutable, so an entry never has to be invalidated by
   * new commits (they add new keys). The cache is static because REST closes the session per
   * request — a per-session cache re-read one {@code RevisionRootPage} per revision on EVERY
   * {@code /history} call; the global cache only pays I/O for revisions not yet seen by this JVM.
   * Invalidation: resource removal drops the (databaseId, resourceId) slice, database removal and
   * crash-recovery truncation drop the databaseId slice, {@code Databases.clearGlobalCaches()}
   * (cold-process simulation in tests) drops everything.
   */
  private static final com.github.benmanes.caffeine.cache.Cache<RevisionInfoKey, RevisionInfo> REVISION_INFO_CACHE =
      com.github.benmanes.caffeine.cache.Caffeine.newBuilder().maximumSize(100_000).build();

  /** Shared empty result for history-timestamp queries on resources without user revisions. */
  private static final long[] EMPTY_LONG_ARRAY = new long[0];

  /** Shared empty result for record-change-revision queries. */
  private static final int[] EMPTY_INT_ARRAY = new int[0];

  /** Shared zero-length array for {@link java.util.Collection#toArray(Object[])} of futures. */
  private static final CompletableFuture<?>[] EMPTY_FUTURES = new CompletableFuture[0];

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
   * Atomic counter for concurrent generation of node transaction id.
   */
  private final AtomicInteger nodeTrxIDCounter;

  /**
   * Atomic counter for concurrent generation of storage engine id.
   */
  final AtomicInteger storageEngineIDCounter;

  /**
   * Per-thread shared read-only transactions for parallel query execution.
   * Key: (threadId, revision) → one read-only trx per worker thread per revision.
   */
  private final ConcurrentHashMap<SharedTrxKey, R> sharedTrxMap;

  private final AtomicReference<ObjectPool<StorageEngineReader>> pool;

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
   * @param storageEngineWriterFactory A factory that creates new {@link StorageEngineWriter} instances.
   * @throws SirixException if Sirix encounters an exception
   */
  protected AbstractResourceSession(final ResourceStore<? extends ResourceSession<R, W>> resourceStore,
      final ResourceConfiguration resourceConf, final BufferManager bufferManager,
      final IOStorage storage, final UberPage uberPage, final Semaphore writeLock,
      final @Nullable User user, final StorageEngineWriterFactory storageEngineWriterFactory) {
    this.resourceStore = requireNonNull(resourceStore);
    resourceConfig = requireNonNull(resourceConf);
    this.bufferManager = requireNonNull(bufferManager);
    this.storage = requireNonNull(storage);
    this.storageEngineWriterFactory = storageEngineWriterFactory;

    nodeTrxMap = new ConcurrentHashMap<>();
    storageEngineReaderMap = new ConcurrentHashMap<>();
    storageEngineWriterMap = new ConcurrentHashMap<>();

    nodeTrxIDCounter = new AtomicInteger();
    storageEngineIDCounter = new AtomicInteger();
    commitLock = new ReentrantLock(false);
    sharedTrxMap = new ConcurrentHashMap<>();

    this.writeLock = requireNonNull(writeLock);

    lastCommittedUberPage = new AtomicReference<>(uberPage);
    this.user = user;
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

  // REMOVED: ClockSweeper management moved to BufferManager (global lifecycle)
  // This follows PostgreSQL bgwriter pattern - background threads run continuously,
  // not tied to individual session lifecycle

  public void createStorageEnginePool() {
    if (pool.get() == null) {
      final StorageEngineReaderFactory factory = new StorageEngineReaderFactory(this);
      pool.set(new ObjectPool<>(factory, factory));
    }
  }

  protected void initializeIndexController(final int revision, IndexController<?, ?> controller) {
    // Deserialize index definitions.
    // For write transactions, the revision number is the NEW revision being created,
    // but index definitions are stored at the LAST COMMITTED revision (and only for
    // revisions where definitions exist — resources without secondary indexes have NO
    // files here at all).
    final Path indexesDir =
        getResourceConfig().getResource().resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
    Path indexes = indexesDir.resolve(revision + ".xml");

    if (!Files.exists(indexes)) {
      // ONE directory listing picking the most recent definitions at or below the requested
      // revision. The previous code probed revision, revision-1, ..., 0 with one
      // Files.exists each — O(revision) access() syscalls PER CONTROLLER CREATION, i.e.
      // O(R²) over a commit-heavy run (measured: 50 MILLION access() calls building a
      // 10k-revision resource with no indexes; the dominant cause of the long-build
      // commit-rate decline).
      int bestRevision = -1;
      if (Files.isDirectory(indexesDir)) {
        try (final var children = Files.list(indexesDir)) {
          for (final var it = children.iterator(); it.hasNext(); ) {
            final String name = it.next().getFileName().toString();
            if (name.endsWith(".xml")) {
              try {
                final int fileRevision = Integer.parseInt(name.substring(0, name.length() - 4));
                if (fileRevision <= revision && fileRevision > bestRevision) {
                  bestRevision = fileRevision;
                }
              } catch (final NumberFormatException ignored) {
                // foreign file in the indexes directory — not ours to interpret
              }
            }
          }
        } catch (final IOException e) {
          throw new SirixIOException("Index definitions couldn't be listed!", e);
        }
      }
      if (bestRevision < 0) {
        return; // no definitions were ever serialized for this resource
      }
      indexes = indexesDir.resolve(bestRevision + ".xml");
    }

    try (final InputStream in = new FileInputStream(indexes.toFile())) {
      controller.getIndexes().init(IndexController.deserialize(in).getFirstChild());
    } catch (IOException | DocumentException | SirixException e) {
      throw new SirixIOException("Index definitions couldn't be deserialized!", e);
    }
  }

  public Reader createReader() {
    return storage.createReader();
  }

  @Override
  public Cache<RBIndexKey, Node> getIndexCache() {
    return bufferManager.getIndexCache();
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
  public StorageEngineWriter createPageTransaction(final int id, final int representRevision,
      final int storedRevision, final Abort abort, boolean isBoundToNodeTrx) {
    return createPageTransaction(id, representRevision, storedRevision, abort, isBoundToNodeTrx, null);
  }

  @Override
  public StorageEngineWriter createPageTransaction(final int id, final int representRevision,
      final int storedRevision, final Abort abort, boolean isBoundToNodeTrx,
      final @Nullable UberPage pendingBaseUberPage) {
    checkArgument(id >= 0, "id must be >= 0!");
    checkArgument(representRevision >= 0, "representRevision must be >= 0!");
    checkArgument(storedRevision >= 0, "storedRevision must be >= 0!");

    final Writer writer = storage.createWriter();

    // Pipelined async commits pass the pending (phase-1-complete, canonical in-memory) uber page
    // of the still-hardening revision as the base for the successor epoch; readers keep resolving
    // "latest" through lastCommittedUberPage until the background hardening publishes it.
    final UberPage lastCommittedUberPage =
        pendingBaseUberPage != null ? pendingBaseUberPage : this.lastCommittedUberPage.get();
    final int lastCommittedRev = lastCommittedUberPage.getRevisionNumber();
    final var storageEngineWriter = this.storageEngineWriterFactory.createStorageEngineWriter(this,
        abort == Abort.YES && lastCommittedUberPage.isBootstrap()
            ? new UberPage()
            : new UberPage(lastCommittedUberPage),
        writer, id, representRevision, storedRevision, lastCommittedRev, isBoundToNodeTrx, bufferManager);

    // The commit marker legitimately exists while an in-process async commit hardens — the
    // truncate check exists for CRASH recovery and must not fire for pipelined successor epochs.
    if (pendingBaseUberPage == null) {
      truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists(writer, lastCommittedRev, storageEngineWriter);
    }

    return storageEngineWriter;
  }

  /** Depth-1 pipelined async commit: the pending (phase-1-complete, unhardened) revision root. */
  private volatile PendingRevisionRoot pendingRevisionRoot;

  private record PendingRevisionRoot(int revision, RevisionRootPage rootPage) {
  }

  @Override
  public void putPendingRevisionRoot(final int revision, final RevisionRootPage rootPage) {
    pendingRevisionRoot = new PendingRevisionRoot(revision, rootPage);
  }

  @Override
  public RevisionRootPage getPendingRevisionRoot(final int revision) {
    final PendingRevisionRoot pending = pendingRevisionRoot;
    return pending != null && pending.revision() == revision ? pending.rootPage() : null;
  }

  @Override
  public void clearPendingRevisionRoot(final int revision) {
    final PendingRevisionRoot pending = pendingRevisionRoot;
    if (pending != null && pending.revision() == revision) {
      pendingRevisionRoot = null;
    }
  }

  @Override
  public void detachNodePageWriteTransaction(final int transactionID) {
    // Pipelined async commit: the superseded page transaction is handed to the background
    // hardening thread, which closes it after the beacon write — remove it from the map WITHOUT
    // closing so the successor can register itself.
    storageEngineWriterMap.remove(transactionID);
  }

  private void truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists(Writer writer, int lastCommittedRev,
      StorageEngineWriter storageEngineWriter) {
    if (Files.exists(getCommitFile())) {
      writer.truncateTo(storageEngineWriter, lastCommittedRev);
      // The truncated range's offsets are reused by subsequent commits, but pages of the aborted
      // commit may already sit in the warm global caches under those offsets (caches survive
      // close now) — drop this database's entries so post-recovery reads can never observe
      // pre-truncation bytes.
      io.sirix.access.Databases.clearCachesForDatabase(resourceConfig.getDatabaseId());
    }
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

    checkArgument(fromRevision > 0 && toRevision > 0,
                  "Revision numbers must be positive, but got %s and %s.",
                  fromRevision,
                  toRevision);

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

    checkArgument(fromRevision > 0 && toRevision > 0,
                  "Revision numbers must be positive, but got %s and %s.",
                  fromRevision,
                  toRevision);

    final int newest = Math.max(fromRevision, toRevision);
    final int oldest = Math.min(fromRevision, toRevision);

    return historyTimestampsNewestFirst(newest, oldest);
  }

  /**
   * Read the commit timestamps (epoch millis) for the inclusive revision range
   * {@code [oldest, newest]} from the in-memory {@link RevisionIndex} and return them
   * newest-first. No {@link StorageEngineReader} is opened and no {@code RevisionRootPage} is
   * read — the timestamps are already resident.
   *
   * <p>If the in-memory index lags the requested range (a fresh process, or out-of-band growth
   * by another writer), it is resynced once from disk via {@link IOStorage#loadRevisionIndex},
   * mirroring the storage-open path.
   */
  private long[] historyTimestampsNewestFirst(final int newest, final int oldest) {
    final RevisionIndexHolder holder = storage.getRevisionIndexHolder();
    RevisionIndex index = holder.get();
    if (index.size() <= newest) {
      storage.loadRevisionIndex(holder);
      index = holder.get();
    }
    if (index.size() <= newest) {
      throw new IllegalStateException("Revision index holds " + index.size()
          + " entries but revision " + newest + " was requested.");
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
    final int oldest = revisions >= newest ? 1 : newest - revisions + 1;
    return buildHistory(newest, oldest);
  }

  /**
   * Build the history (newest revision first) for the inclusive revision range
   * {@code [oldest, newest]}.
   *
   * <p>Already-seen, immutable revisions are served synchronously from
   * {@link #REVISION_INFO_CACHE} straight into a pre-sized result array — no per-revision
   * {@link CompletableFuture}, no stream pipeline. Only cache misses (cold revisions) open a
   * {@link StorageEngineReader}, and those run in parallel so a cold first call still overlaps
   * its I/O across revisions.
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
        misses.add(CompletableFuture.runAsync(
            () -> result[slot] = REVISION_INFO_CACHE.get(cacheKey, this::loadRevisionInfo)));
      }
    }

    if (misses != null) {
      CompletableFuture.allOf(misses.toArray(EMPTY_FUTURES)).join();
    }

    return List.of(result);
  }

  /**
   * Cold-path loader for one revision's {@link RevisionInfo}: reads the commit credentials and
   * timestamp directly through a {@link StorageEngineReader}, bypassing the full
   * node-transaction machinery (node cursor, item list, per-trx wiring) that
   * {@code beginNodeReadOnlyTrx} sets up.
   */
  private RevisionInfo loadRevisionInfo(final RevisionInfoKey key) {
    final int revision = key.revision();
    try (final StorageEngineReader reader = createStorageEngineReader(revision)) {
      final CommitCredentials commitCredentials = reader.getCommitCredentials();
      return new RevisionInfo(commitCredentials.getUser(),
                              revision,
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
   * Read {@code nodeKey} at {@code revision} through a lightweight {@link StorageEngineReader}
   * (no {@link io.sirix.api.NodeReadOnlyTrx} wrapper, document-node fetch, or trx bookkeeping) and
   * hand it to {@code visitor} if the record exists in that revision. The reader stays open for
   * the duration of the callback so the record remains valid.
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
        final int toRevision = (i + 1 < changeRevisions.length) ? changeRevisions[i + 1] - 1 : maxRevision;
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
    //  * assertAccess() reads a volatile and a volatile-published epoch — thread-safe.
    //  * createStorageEngineReader() uses AtomicInteger for IDs and ConcurrentMap for the
    //    bookkeeping — already not synchronized.
    //  * getDocumentNode() operates on the just-constructed reader (per-thread ownership).
    //  * nodeTrxIDCounter is an AtomicInteger; nodeTrxMap is a ConcurrentMap.
    // Removing the per-session monitor allows N concurrent reader-opens to run in parallel,
    // unblocking the depth-N pipeline in the prefetched temporal axes (and any other caller
    // that opens multiple rtxs back-to-back from concurrent threads).
    assertAccess(revision);

    final StorageEngineReader storageEngineReader = createStorageEngineReader(revision);

    final Node documentNode = getDocumentNode(storageEngineReader);

    // Create new reader.
    final R reader = createNodeReadOnlyTrx(nodeTrxIDCounter.incrementAndGet(), storageEngineReader, documentNode);

    // Remember reader for debugging and safe close.
    if (nodeTrxMap.put(reader.getId(), reader) != null) {
      throw new SirixUsageException(ID_GENERATION_EXCEPTION);
    }
    TransactionMetrics.onReadOnlyTrxOpened();

    return reader;
  }

  public abstract R createNodeReadOnlyTrx(int nodeTrxId, StorageEngineReader storageEngineReader, Node documentNode);

  public abstract W createNodeReadWriteTrx(int nodeTrxId, StorageEngineWriter storageEngineWriter, int maxNodeCount,
      Duration autoCommitDelay, Node documentNode, AfterCommitState afterCommitState);

  static Node getDocumentNode(final StorageEngineReader storageEngineReader) {
    final Node node = storageEngineReader.getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), IndexType.DOCUMENT, -1);
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
  public W beginNodeTrx(final int maxNodeCount, final int maxTime,
      final TimeUnit timeUnit) {
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
  public W beginNodeTrx(final int maxTime, final TimeUnit timeUnit,
      final AfterCommitState afterCommitState) {
    return beginNodeTrx(0, maxTime, timeUnit, afterCommitState);
  }

  @Override
  public synchronized W beginNodeTrx(final int maxNodeCount, final int maxTime,
      final TimeUnit timeUnit, final AfterCommitState afterCommitState) {
    // Checks.
    assertAccess(getMostRecentRevisionNumber());
    if (maxNodeCount < 0 || maxTime < 0) {
      throw new SirixUsageException("maxNodeCount may not be < 0!");
    }
    requireNonNull(timeUnit);

    // KEEP_OPEN_ASYNC_FLUSH / KEEP_OPEN_ASYNC_COMMIT runtime guards
    if (afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH
        || afterCommitState == AfterCommitState.KEEP_OPEN_ASYNC_COMMIT) {
      if (getResourceConfig().getStorageType() != StorageType.FILE_CHANNEL) {
        throw new IllegalArgumentException(
            afterCommitState + " requires FILE_CHANNEL storage backend; got "
                + getResourceConfig().getStorageType());
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
      final int nodeTrxId = nodeTrxIDCounter.incrementAndGet();
      final int lastRev = getMostRecentRevisionNumber();
      final StorageEngineWriter storageEngineWriter = createPageTransaction(nodeTrxId, lastRev, lastRev, Abort.NO, true);

      final Node documentNode = getDocumentNode(storageEngineWriter);

      // Create new node write transaction.
      final var autoCommitDelay = Duration.of(maxTime, timeUnit.toChronoUnit());
      final W wtx =
          createNodeReadWriteTrx(nodeTrxId, storageEngineWriter, maxNodeCount, autoCommitDelay, documentNode, afterCommitState);

      // Remember node transaction for debugging and safe close.
      // noinspection unchecked
      if (nodeTrxMap.put(nodeTrxId, (R) wtx) != null || storageEngineWriterMap.put(nodeTrxId, storageEngineWriter) != null) {
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
      resourceStore.closeResourceSession(resourceConfig.getResource());

      storage.close();

      if (pool.get() != null) {
        pool.get().close();
      }
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
  public void setNodePageWriteTransaction(final int transactionID,
      final StorageEngineWriter storageEngineWriter) {
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
   * Close a write transaction.
   *
   * @param transactionID write transaction ID
   */
  @Override
  public void closePageWriteTransaction(final Integer transactionID) {
    assertNotClosed();

    // Remove from internal map.
    storageEngineReaderMap.remove(transactionID);

    // Make new transactions available.
    LOGGER.trace("Lock unlock (closePageWriteTransaction).");
    writeLock.release();
  }

  /**
   * Close a read transaction.
   *
   * @param transactionID read transaction ID
   */
  @Override
  public void closePageReadTransaction(final Integer transactionID) {
    assertNotClosed();

    // Remove from internal map.
    storageEngineReaderMap.remove(transactionID);
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

    final int currentStorageEngineID = storageEngineIDCounter.incrementAndGet();
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
      final int currentStorageEngineID = storageEngineIDCounter.incrementAndGet();
      final int lastRev = getMostRecentRevisionNumber();
      final StorageEngineWriter storageEngineWriter = createPageTransaction(currentStorageEngineID, lastRev, lastRev, Abort.NO, false);

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
   * Number of currently-open read-only or read-write node transactions on this session.
   * Useful for diagnostics and for asserting the absence of resource leaks in tests.
   */
  public int activeTrxCount() {
    return nodeTrxMap.size();
  }

  /**
   * Begin a read-only transaction at the revision that was valid at the given point in time.
   * 
   * <p>
   * This uses <b>floor semantics</b>: returns the last revision that was committed at or before the
   * given timestamp. This reflects the actual state of the database as it was at that moment in time.
   * 
   * <p>
   * Special cases:
   * <ul>
   * <li>If timestamp is before all revisions → returns revision 0</li>
   * <li>If timestamp is after all revisions → returns the most recent revision</li>
   * <li>If timestamp exactly matches a revision → returns that revision</li>
   * <li>If timestamp is between revisions → returns the earlier revision</li>
   * </ul>
   *
   * @param pointInTime the point in time to query
   * @return a read-only transaction at the revision valid at that time
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
   * Get the revision number that was valid at the given point in time.
   * 
   * <p>
   * This uses <b>floor semantics</b>: returns the last revision that was committed at or before the
   * given timestamp. This reflects the actual state of the database as it was at that moment in time.
   * 
   * <p>
   * Special cases:
   * <ul>
   * <li>If timestamp is before all revisions → returns 0</li>
   * <li>If timestamp is after all revisions → returns the most recent revision number</li>
   * <li>If timestamp exactly matches a revision → returns that revision number</li>
   * <li>If timestamp is between revisions → returns the earlier revision number</li>
   * </ul>
   *
   * @param pointInTime the point in time to query
   * @return the revision number valid at that time
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
