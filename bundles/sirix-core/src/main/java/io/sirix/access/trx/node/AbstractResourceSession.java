package io.sirix.access.trx.node;

import cn.danielw.fop.*;
import io.brackit.query.jdm.DocumentException;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ResourceStore;
import io.sirix.access.User;
import io.sirix.access.trx.node.xml.XmlResourceSessionImpl;
import io.sirix.access.trx.page.NodePageReadOnlyTrx;
import io.sirix.access.trx.page.PageTrxFactory;
import io.sirix.access.trx.page.PageTrxReadOnlyFactory;
import io.sirix.access.trx.page.RevisionRootPageReader;
import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.api.*;
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
import io.sirix.io.Writer;
import io.sirix.node.interfaces.Node;
import io.sirix.page.UberPage;
import io.sirix.settings.Fixed;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("ConstantValue")
public abstract class AbstractResourceSession<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    implements ResourceSession<R, W>, InternalResourceSession<R, W> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractResourceSession.class);

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
   * Remember all running page transactions (both read and write).
   */
  final ConcurrentMap<Integer, PageReadOnlyTrx> pageTrxMap;

  /**
   * Remember the write seperately because of the concurrent writes.
   */
  final ConcurrentMap<Integer, PageTrx> nodePageTrxMap;

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
   * Atomic counter for concurrent generation of page transaction id.
   */
  final AtomicInteger pageTrxIDCounter;

  private final AtomicReference<ObjectPool<PageReadOnlyTrx>> pool;

  /**
   * Determines if session was closed.
   */
  volatile boolean isClosed;

  /**
   * The cache of in-memory pages shared amongst all manager / resource transactions.
   */
  final BufferManager bufferManager;

  /**
   * Tracks the minimum active revision for MVCC-aware page eviction.
   * Pages can only be evicted when their revision < minActiveRevision.
   */
  final RevisionEpochTracker revisionEpochTracker;

  /**
   * Clock sweeper threads for background page eviction.
   * One thread per cache shard for multi-core scalability.
   */
  private final List<Thread> clockSweeperThreads;
  private final List<io.sirix.cache.ClockSweeper> clockSweepers;

  /**
   * The resource store with which this manager has been created.
   */
  final ResourceStore<? extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> resourceStore;

  /**
   * The user interacting with SirixDB.
   */
  final User user;

  /**
   * A factory that creates new {@link PageTrx} instances.
   */
  private final PageTrxFactory pageTrxFactory;

  /**
   * ID Generation exception message for duplicate ID.
   */
  private final String ID_GENERATION_EXCEPTION = "ID generation is bogus because of duplicate ID.";

  /**
   * Creates a new instance of this class.
   *
   * @param resourceStore  the resource store with which this manager has been created
   * @param resourceConf   {@link DatabaseConfiguration} for general setting about the storage
   * @param bufferManager  the cache of in-memory pages shared amongst all resource managers and transactions
   * @param storage        the I/O backed storage backend
   * @param uberPage       holds a reference to the revision root page tree
   * @param writeLock      allow for concurrent writes
   * @param user           the user tied to the resource manager
   * @param pageTrxFactory A factory that creates new {@link PageTrx} instances.
   * @throws SirixException if Sirix encounters an exception
   */
  protected AbstractResourceSession(final @NonNull ResourceStore<? extends ResourceSession<R, W>> resourceStore,
      final @NonNull ResourceConfiguration resourceConf, final @NonNull BufferManager bufferManager,
      final @NonNull IOStorage storage, final @NonNull UberPage uberPage, final @NonNull Semaphore writeLock,
      final @Nullable User user, final PageTrxFactory pageTrxFactory) {
    this.resourceStore = requireNonNull(resourceStore);
    resourceConfig = requireNonNull(resourceConf);
    this.bufferManager = requireNonNull(bufferManager);
    this.storage = requireNonNull(storage);
    this.pageTrxFactory = pageTrxFactory;

    nodeTrxMap = new ConcurrentHashMap<>();
    pageTrxMap = new ConcurrentHashMap<>();
    nodePageTrxMap = new ConcurrentHashMap<>();

    nodeTrxIDCounter = new AtomicInteger();
    pageTrxIDCounter = new AtomicInteger();
    commitLock = new ReentrantLock(false);

    this.writeLock = requireNonNull(writeLock);

    lastCommittedUberPage = new AtomicReference<>(uberPage);
    this.user = user;
    pool = new AtomicReference<>();
    
    // Initialize revision epoch tracker (128 slots = supports up to 128 concurrent transactions)
    this.revisionEpochTracker = new RevisionEpochTracker(128);
    this.revisionEpochTracker.setLastCommittedRevision(uberPage.getRevisionNumber());

    // Start ClockSweeper threads for each ShardedPageCache
    this.clockSweeperThreads = new ArrayList<>();
    this.clockSweepers = new ArrayList<>();
    startClockSweepers();

    isClosed = false;
  }

  /**
   * Start ClockSweeper background threads for page eviction.
   * Creates one sweeper thread per shard in each ShardedPageCache.
   */
  private void startClockSweepers() {
    long databaseId = resourceConfig.getDatabaseId();
    long resourceId = resourceConfig.getID();
    int sweepIntervalMs = 100; // Sweep every 100ms

    // Start ClockSweeper for RecordPageCache (simplified: single thread)
    if (bufferManager.getRecordPageCache() instanceof io.sirix.cache.ShardedPageCache recordCache) {
      io.sirix.cache.ShardedPageCache.Shard shard = recordCache.getShard(
          new io.sirix.page.PageReference().setDatabaseId(databaseId).setResourceId(resourceId));
      
      io.sirix.cache.ClockSweeper sweeper = new io.sirix.cache.ClockSweeper(
          shard, revisionEpochTracker, sweepIntervalMs, 0, databaseId, resourceId);
      
      Thread thread = new Thread(sweeper, "ClockSweeper-RecordPage-" + databaseId + "-" + resourceId);
      thread.setDaemon(true);
      thread.start();
      
      clockSweepers.add(sweeper);
      clockSweeperThreads.add(thread);
      
      LOGGER.info("Started 1 ClockSweeper thread for RecordPageCache (db={}, res={})", databaseId, resourceId);
    }

    // Start ClockSweeper for RecordPageFragmentCache (simplified: single thread)
    if (bufferManager.getRecordPageFragmentCache() instanceof io.sirix.cache.ShardedPageCache fragmentCache) {
      io.sirix.cache.ShardedPageCache.Shard shard = fragmentCache.getShard(
          new io.sirix.page.PageReference().setDatabaseId(databaseId).setResourceId(resourceId));
      
      io.sirix.cache.ClockSweeper sweeper = new io.sirix.cache.ClockSweeper(
          shard, revisionEpochTracker, sweepIntervalMs, 0, databaseId, resourceId);
      
      Thread thread = new Thread(sweeper, "ClockSweeper-FragmentPage-" + databaseId + "-" + resourceId);
      thread.setDaemon(true);
      thread.start();
      
      clockSweepers.add(sweeper);
      clockSweeperThreads.add(thread);
      
      LOGGER.info("Started 1 ClockSweeper thread for RecordPageFragmentCache (db={}, res={})", databaseId, resourceId);
    }
  }

  /**
   * Stop all ClockSweeper threads.
   * Called when resource session is closed.
   */
  private void stopClockSweepers() {
    for (io.sirix.cache.ClockSweeper sweeper : clockSweepers) {
      sweeper.stop();
    }
    
    for (Thread thread : clockSweeperThreads) {
      thread.interrupt();
    }
    
    // Wait for threads to finish (with timeout)
    for (Thread thread : clockSweeperThreads) {
      try {
        thread.join(1000); // Wait max 1 second
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    
    LOGGER.info("Stopped {} ClockSweeper threads", clockSweeperThreads.size());
  }

  public void createPageTrxPool() {
    if (pool.get() == null) {
      final PoolConfig config = new PoolConfig();
      config.setPartitionSize(3);
      config.setMaxSize(10);
      config.setMinSize(5);
      config.setScavengeIntervalMilliseconds(60_000);
      config.setMaxIdleMilliseconds(5);
      config.setMaxWaitMilliseconds(5);
      config.setShutdownWaitMilliseconds(1);

      pool.set(new ObjectPool<>(config, new PageTrxReadOnlyFactory(this)));
    }
  }

  private static long timeDiff(final long lhs, final long rhs) {
    return Math.abs(lhs - rhs);
  }

  protected void initializeIndexController(final int revision, IndexController<?, ?> controller) {
    // Deserialize index definitions.
    final Path indexes = getResourceConfig().getResource()
                                            .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
                                            .resolve(revision + ".xml");
    if (Files.exists(indexes)) {
      try (final InputStream in = new FileInputStream(indexes.toFile())) {
        controller.getIndexes().init(IndexController.deserialize(in).getFirstChild());
      } catch (IOException | DocumentException | SirixException e) {
        throw new SirixIOException("Index definitions couldn't be deserialized!", e);
      }
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
   * Create a new {@link PageTrx}.
   *
   * @param id                the transaction ID
   * @param representRevision the revision which is represented
   * @param storedRevision    the revision which is stored
   * @param abort             determines if a transaction must be aborted (rollback) or not
   * @return a new {@link PageTrx} instance
   */
  @Override
  public PageTrx createPageTransaction(final @NonNegative int id, final @NonNegative int representRevision,
      final @NonNegative int storedRevision, final Abort abort, boolean isBoundToNodeTrx) {
    checkArgument(id >= 0, "id must be >= 0!");
    checkArgument(representRevision >= 0, "representRevision must be >= 0!");
    checkArgument(storedRevision >= 0, "storedRevision must be >= 0!");

    final Writer writer = storage.createWriter();

    final UberPage lastCommittedUberPage = this.lastCommittedUberPage.get();
    final int lastCommittedRev = lastCommittedUberPage.getRevisionNumber();
    final var pageTrx = this.pageTrxFactory.createPageTrx(this,
                                                          abort == Abort.YES && lastCommittedUberPage.isBootstrap()
                                                              ? new UberPage()
                                                              : new UberPage(lastCommittedUberPage),
                                                          writer,
                                                          id,
                                                          representRevision,
                                                          storedRevision,
                                                          lastCommittedRev,
                                                          isBoundToNodeTrx,
                                                          bufferManager);

    truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists(writer, lastCommittedRev, pageTrx);

    return pageTrx;
  }

  private void truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists(Writer writer, int lastCommittedRev,
      PageTrx pageTrx) {
    if (Files.exists(getCommitFile())) {
      writer.truncateTo(pageTrx, lastCommittedRev);
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

    checkArgument(fromRevision > toRevision);

    final var revisionInfos = new ArrayList<CompletableFuture<RevisionInfo>>();

    for (int revision = fromRevision; revision > 0 && revision >= toRevision; revision--) {
      int finalRevision = revision;
      revisionInfos.add(CompletableFuture.supplyAsync(() -> {
        try (final NodeReadOnlyTrx rtx = beginNodeReadOnlyTrx(finalRevision)) {
          final CommitCredentials commitCredentials = rtx.getCommitCredentials();
          return new RevisionInfo(commitCredentials.getUser(),
                                  rtx.getRevisionNumber(),
                                  rtx.getRevisionTimestamp(),
                                  commitCredentials.getMessage());
        }
      }));
    }

    return getResult(revisionInfos);
  }

  private List<RevisionInfo> getHistoryInformations(int revisions) {
    checkArgument(revisions > 0);

    final int lastCommittedRevision = getMostRecentRevisionNumber();
    final var revisionInfos = new ArrayList<CompletableFuture<RevisionInfo>>();

    for (int revision = lastCommittedRevision;
         revision > 0 && revision > lastCommittedRevision - revisions; revision--) {
      int finalRevision = revision;
      revisionInfos.add(CompletableFuture.supplyAsync(() -> {
        try (final NodeReadOnlyTrx rtx = beginNodeReadOnlyTrx(finalRevision)) {
          final CommitCredentials commitCredentials = rtx.getCommitCredentials();
          return new RevisionInfo(commitCredentials.getUser(),
                                  rtx.getRevisionNumber(),
                                  rtx.getRevisionTimestamp(),
                                  commitCredentials.getMessage());
        }
      }));
    }

    return getResult(revisionInfos);
  }

  private List<RevisionInfo> getResult(final List<CompletableFuture<RevisionInfo>> revisionInfos) {
    return revisionInfos.stream().map(CompletableFuture::join).toList();
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
  public synchronized R beginNodeReadOnlyTrx(@NonNegative final int revision) {
    assertAccess(revision);

    final PageReadOnlyTrx pageReadTrx = beginPageReadOnlyTrx(revision);

    final Node documentNode = getDocumentNode(pageReadTrx);

    // Create new reader.
    final R reader = createNodeReadOnlyTrx(nodeTrxIDCounter.incrementAndGet(), pageReadTrx, documentNode);

    // Remember reader for debugging and safe close.
    if (nodeTrxMap.put(reader.getId(), reader) != null) {
      throw new SirixUsageException(ID_GENERATION_EXCEPTION);
    }

    return reader;
  }

  public abstract R createNodeReadOnlyTrx(int nodeTrxId, PageReadOnlyTrx pageReadTrx, Node documentNode);

  public abstract W createNodeReadWriteTrx(int nodeTrxId, PageTrx pageTrx, int maxNodeCount, Duration autoCommitDelay,
      Node documentNode, AfterCommitState afterCommitState);

  static Node getDocumentNode(final PageReadOnlyTrx pageReadTrx) {
    final Node node = pageReadTrx.getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), IndexType.DOCUMENT, -1);
    if (node == null) {
      pageReadTrx.close();
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
  public W beginNodeTrx(final @NonNegative int maxNodeCount) {
    return beginNodeTrx(maxNodeCount, 0, TimeUnit.MILLISECONDS, AfterCommitState.KEEP_OPEN);
  }

  @Override
  public W beginNodeTrx(final @NonNegative int maxTime, final @NonNull TimeUnit timeUnit) {
    return beginNodeTrx(0, maxTime, timeUnit, AfterCommitState.KEEP_OPEN);
  }

  @Override
  public W beginNodeTrx(final @NonNegative int maxNodeCount, final @NonNegative int maxTime,
      final @NonNull TimeUnit timeUnit) {
    return beginNodeTrx(maxNodeCount, maxTime, timeUnit, AfterCommitState.KEEP_OPEN);
  }

  @Override
  public W beginNodeTrx(final @NonNull AfterCommitState afterCommitState) {
    return beginNodeTrx(0, 0, TimeUnit.MILLISECONDS, afterCommitState);
  }

  @Override
  public W beginNodeTrx(final @NonNegative int maxNodeCount, final @NonNull AfterCommitState afterCommitState) {
    return beginNodeTrx(maxNodeCount, 0, TimeUnit.MILLISECONDS);
  }

  @Override
  public W beginNodeTrx(final @NonNegative int maxTime, final @NonNull TimeUnit timeUnit,
      final @NonNull AfterCommitState afterCommitState) {
    return beginNodeTrx(0, maxTime, timeUnit, afterCommitState);
  }

  @Override
  public synchronized W beginNodeTrx(final @NonNegative int maxNodeCount, final @NonNegative int maxTime,
      final @NonNull TimeUnit timeUnit, final @NonNull AfterCommitState afterCommitState) {
    // Checks.
    assertAccess(getMostRecentRevisionNumber());
    if (maxNodeCount < 0 || maxTime < 0) {
      throw new SirixUsageException("maxNodeCount may not be < 0!");
    }
    requireNonNull(timeUnit);

    // Make sure not to exceed available number of write transactions.
    try {
      if (!writeLock.tryAcquire(5, TimeUnit.SECONDS)) {
        throw new SirixUsageException(
            "No read-write transaction available, please close the running read-write transaction first.");
      }
    } catch (final InterruptedException e) {
      throw new SirixThreadedException(e);
    }

    LOGGER.trace("Lock: lock acquired (beginNodeTrx)");

    // Create new page write transaction (shares the same ID with the node write trx).
    final int nodeTrxId = nodeTrxIDCounter.incrementAndGet();
    final int lastRev = getMostRecentRevisionNumber();
    final PageTrx pageWtx = createPageTransaction(nodeTrxId, lastRev, lastRev, Abort.NO, true);

    final Node documentNode = getDocumentNode(pageWtx);

    // Create new node write transaction.
    final var autoCommitDelay = Duration.of(maxTime, timeUnit.toChronoUnit());
    final W wtx =
        createNodeReadWriteTrx(nodeTrxId, pageWtx, maxNodeCount, autoCommitDelay, documentNode, afterCommitState);

    // Remember node transaction for debugging and safe close.
    //noinspection unchecked
    if (nodeTrxMap.put(nodeTrxId, (R) wtx) != null || nodePageTrxMap.put(nodeTrxId, pageWtx) != null) {
      throw new SirixThreadedException(ID_GENERATION_EXCEPTION);
    }

    return wtx;
  }

  @Override
  public synchronized void close() {
    if (!isClosed) {
      // Stop ClockSweeper threads first
      stopClockSweepers();
      
      // Close all open node transactions.
      for (NodeReadOnlyTrx rtx : nodeTrxMap.values()) {
        if (rtx instanceof XmlNodeTrx xmlNodeTrx) {
          xmlNodeTrx.rollback();
        } else if (rtx instanceof JsonNodeTrx jsonNodeTrx) {
          jsonNodeTrx.rollback();
        }
        rtx.close();
      }
      // Close all open node page transactions.
      for (PageReadOnlyTrx rtx : nodePageTrxMap.values()) {
        rtx.close();
      }
      // Close all open page transactions.
      for (PageReadOnlyTrx rtx : pageTrxMap.values()) {
        rtx.close();
      }

      // NOTE: Don't clear BufferManager caches here - other sessions might be using same resource!
      // Pages will be evicted by normal cache LRU policy or cleaned up at database close
      // PostgreSQL-style: buffers released when ALL sessions release them, not when one closes
      
      // Immediately release all ressources.
      nodeTrxMap.clear();
      pageTrxMap.clear();
      nodePageTrxMap.clear();
      resourceStore.closeResourceSession(resourceConfig.getResource());

      storage.close();

      if (pool.get() != null) {
        try {
          pool.get().shutdown();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      isClosed = true;
    }
  }

  /**
   * Checks for valid revision.
   *
   * @param revision revision number to check
   * @throws IllegalStateException    if {@link XmlResourceSessionImpl} is already closed
   * @throws IllegalArgumentException if revision isn't valid
   */
  @Override
  public void assertAccess(final @NonNegative int revision) {
    assertNotClosed();
    if (revision > getMostRecentRevisionNumber()) {
      throw new IllegalArgumentException(
          "Revision must not be bigger than " + Long.toString(getMostRecentRevisionNumber()) + "!");
    }
  }

  @Override
  public String toString() {
    return "ResourceManager{" + "resourceConfig=" + resourceConfig + ", isClosed=" + isClosed + '}';
  }

  private void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Resource manager is already closed!");
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
   * Set a new node page write trx.
   *
   * @param transactionID page write transaction ID
   * @param pageTrx       page write trx
   */
  @Override
  public void setNodePageWriteTransaction(final @NonNegative int transactionID, @NonNull final PageTrx pageTrx) {
    assertNotClosed();
    nodePageTrxMap.put(transactionID, pageTrx);
  }

  /**
   * Close a node page transaction.
   *
   * @param transactionID page write transaction ID
   * @throws SirixIOException if an I/O error occurs
   */
  @Override
  public void closeNodePageWriteTransaction(final @NonNegative int transactionID) {
    assertNotClosed();
    final PageReadOnlyTrx pageRtx = nodePageTrxMap.remove(transactionID);
    if (pageRtx != null) {
      pageRtx.close();
    }
  }

  /**
   * Close a write transaction.
   *
   * @param transactionID write transaction ID
   */
  @Override
  public void closeWriteTransaction(final @NonNegative int transactionID) {
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
  public void closeReadTransaction(final @NonNegative int transactionID) {
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
  public void closePageWriteTransaction(final @NonNegative Integer transactionID) {
    assertNotClosed();

    // Remove from internal map.
    pageTrxMap.remove(transactionID);

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
  public void closePageReadTransaction(final @NonNegative Integer transactionID) {
    assertNotClosed();

    // Remove from internal map.
    pageTrxMap.remove(transactionID);
  }

  /**
   * Remove from internal maps.
   *
   * @param transactionID transaction ID to remove
   */
  private void removeFromPageMapping(final @NonNegative Integer transactionID) {
    assertNotClosed();

    // Purge transaction from internal state.
    nodeTrxMap.remove(transactionID);

    // Removing the write from the own internal mapping
    nodePageTrxMap.remove(transactionID);
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
  public synchronized PathSummaryReader openPathSummary(final @NonNegative int revision) {
    assertAccess(revision);

    PageReadOnlyTrx pageReadOnlyTrx;

    var pool = this.pool.get();

    if (pool != null) {
      Poolable<PageReadOnlyTrx> poolable = null;
      boolean invalidObject = true;
      while (invalidObject) {
        try {
          poolable = pool.borrowObject(false);
          invalidObject = false;
        } catch (PoolInvalidObjectException ignored) {
        } catch (PoolExhaustedException exception) {
          invalidObject = false;
        }
      }

      if (poolable == null) {
        pageReadOnlyTrx = beginPageReadOnlyTrx(revision);
      } else {
        pageReadOnlyTrx = poolable.getObject();

        if (pageReadOnlyTrx.getRevisionNumber() != revision) {
          pool.returnObject(poolable);
          pageReadOnlyTrx = beginPageReadOnlyTrx(revision);
        }
      }
    } else {
      pageReadOnlyTrx = beginPageReadOnlyTrx(revision);
    }

    return PathSummaryReader.getInstance(pageReadOnlyTrx, this);
  }

  @Override
  public PageReadOnlyTrx beginPageReadOnlyTrx(final @NonNegative int revision) {
    assertAccess(revision);

    final int currentPageTrxID = pageTrxIDCounter.incrementAndGet();
    final NodePageReadOnlyTrx pageReadTrx = new NodePageReadOnlyTrx(currentPageTrxID,
                                                                    this,
                                                                    lastCommittedUberPage.get(),
                                                                    revision,
                                                                    storage.createReader(),
                                                                    bufferManager,
                                                                    new RevisionRootPageReader(),
                                                                    null);
    // Remember page transaction for debugging and safe close.
    if (pageTrxMap.put(currentPageTrxID, pageReadTrx) != null) {
      throw new SirixThreadedException(ID_GENERATION_EXCEPTION);
    }
    
    return pageReadTrx;
  }

  @Override
  public synchronized PageTrx beginPageTrx(final @NonNegative int revision) {
    assertAccess(revision);

    // Make sure not to exceed available number of write transactions.
    try {
      if (!writeLock.tryAcquire(20, TimeUnit.SECONDS)) {
        throw new SirixUsageException("No write transaction available, please close the write transaction first.");
      }
    } catch (final InterruptedException e) {
      throw new SirixThreadedException(e);
    }

    LOGGER.debug("Lock: lock acquired (beginPageTrx)");

    final int currentPageTrxID = pageTrxIDCounter.incrementAndGet();
    final int lastRev = getMostRecentRevisionNumber();
    final PageTrx pageTrx = createPageTransaction(currentPageTrxID, lastRev, lastRev, Abort.NO, false);

    // Remember page transaction for debugging and safe close.
    if (pageTrxMap.put(currentPageTrxID, pageTrx) != null) {
      throw new SirixThreadedException(ID_GENERATION_EXCEPTION);
    }

    return pageTrx;
  }

  @Override
  public Optional<R> getNodeReadTrxByTrxId(final Integer ID) {
    assertNotClosed();

    return Optional.ofNullable(nodeTrxMap.get(ID));
  }

  @Override
  public synchronized Optional<W> getNodeTrx() {
    assertNotClosed();

    //noinspection unchecked
    return nodeTrxMap.values().stream().filter(NodeTrx.class::isInstance).map(rtx -> (W) rtx).findAny();
  }

  @Override
  public R beginNodeReadOnlyTrx(final @NonNull Instant pointInTime) {
    requireNonNull(pointInTime);
    assertNotClosed();

    final long timestamp = pointInTime.toEpochMilli();

    int revision = binarySearch(timestamp);

    if (revision < 0) {
      revision = -revision - 1;
    }

    if (revision == 0)
      return beginNodeReadOnlyTrx(0);
    else if (revision == getMostRecentRevisionNumber() + 1)
      return beginNodeReadOnlyTrx();

    final R rtxRevisionMinus1 = beginNodeReadOnlyTrx(revision - 1);
    final R rtxRevision = beginNodeReadOnlyTrx(revision);

    if (timeDiff(timestamp, rtxRevisionMinus1.getRevisionTimestamp().toEpochMilli()) < timeDiff(timestamp,
                                                                                                rtxRevision.getRevisionTimestamp()
                                                                                                           .toEpochMilli())) {
      rtxRevision.close();
      return rtxRevisionMinus1;
    } else {
      rtxRevisionMinus1.close();
      return rtxRevision;
    }
  }

  private int binarySearch(final long timestamp) {
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

  @Override
  public int getRevisionNumber(final @NonNull Instant pointInTime) {
    requireNonNull(pointInTime);
    assertNotClosed();

    final long timestamp = pointInTime.toEpochMilli();

    int revision = binarySearch(timestamp);

    if (revision < 0) {
      revision = -revision - 1;
    }

    if (revision == 0)
      return 0;
    else if (revision == getMostRecentRevisionNumber() + 1)
      return getMostRecentRevisionNumber();

    try (final R rtxRevisionMinus1 = beginNodeReadOnlyTrx(revision - 1);
         final R rtxRevision = beginNodeReadOnlyTrx(revision)) {
      final int revisionNumber;

      if (timeDiff(timestamp, rtxRevisionMinus1.getRevisionTimestamp().toEpochMilli()) < timeDiff(timestamp,
                                                                                                  rtxRevision.getRevisionTimestamp()
                                                                                                             .toEpochMilli())) {
        revisionNumber = rtxRevisionMinus1.getRevisionNumber();
      } else {
        revisionNumber = rtxRevision.getRevisionNumber();
      }

      return revisionNumber;
    }
  }

  @Override
  public Optional<User> getUser() {
    assertNotClosed();

    return Optional.ofNullable(user);
  }
}
