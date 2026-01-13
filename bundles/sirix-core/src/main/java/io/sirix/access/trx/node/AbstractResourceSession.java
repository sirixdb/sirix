package io.sirix.access.trx.node;

import cn.danielw.fop.*;
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
import io.sirix.io.RevisionIndex;
import io.sirix.io.RevisionIndexHolder;
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
   * Feature flag for optimized revision search using SIMD/Eytzinger layout.
   * Set to false to use legacy binary search (for rollback if needed).
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
   * Remember all running page transactions (both read and write).
   */
  final ConcurrentMap<Integer, StorageEngineReader> pageTrxMap;

  /**
   * Remember the write seperately because of the concurrent writes.
   */
  final ConcurrentMap<Integer, StorageEngineWriter> nodePageTrxMap;

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
   * Tracks the minimum active revision for MVCC-aware page eviction.
   * NOTE: This is now the GLOBAL epoch tracker shared across all databases/resources.
   * Each session registers its active revisions with the global tracker.
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
  private final StorageEngineWriterFactory pageTrxFactory;

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
   * @param pageTrxFactory A factory that creates new {@link StorageEngineWriter} instances.
   * @throws SirixException if Sirix encounters an exception
   */
  protected AbstractResourceSession(final @NonNull ResourceStore<? extends ResourceSession<R, W>> resourceStore,
      final @NonNull ResourceConfiguration resourceConf, final @NonNull BufferManager bufferManager,
      final @NonNull IOStorage storage, final @NonNull UberPage uberPage, final @NonNull Semaphore writeLock,
      final @Nullable User user, final StorageEngineWriterFactory pageTrxFactory) {
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

      pool.set(new ObjectPool<>(config, new StorageEngineReaderFactory(this)));
    }
  }

  protected void initializeIndexController(final int revision, IndexController<?, ?> controller) {
    // Deserialize index definitions.
    // For write transactions, the revision number is the NEW revision being created,
    // but index definitions are stored at the LAST COMMITTED revision.
    // Try the requested revision first, then fallback to previous revisions.
    final Path indexesDir = getResourceConfig().getResource()
                                               .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
    Path indexes = indexesDir.resolve(revision + ".xml");
    
    // Search backward through revisions to find the most recent index definitions
    int searchRevision = revision;
    while (!Files.exists(indexes) && searchRevision > 0) {
      searchRevision--;
      indexes = indexesDir.resolve(searchRevision + ".xml");
    }
    
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
   * Create a new {@link StorageEngineWriter}.
   *
   * @param id                the transaction ID
   * @param representRevision the revision which is represented
   * @param storedRevision    the revision which is stored
   * @param abort             determines if a transaction must be aborted (rollback) or not
   * @return a new {@link StorageEngineWriter} instance
   */
  @Override
  public StorageEngineWriter createPageTransaction(final @NonNegative int id, final @NonNegative int representRevision,
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
      StorageEngineWriter pageTrx) {
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

    final StorageEngineReader pageReadTrx = beginPageReadOnlyTrx(revision);

    final Node documentNode = getDocumentNode(pageReadTrx);

    // Create new reader.
    final R reader = createNodeReadOnlyTrx(nodeTrxIDCounter.incrementAndGet(), pageReadTrx, documentNode);

    // Remember reader for debugging and safe close.
    if (nodeTrxMap.put(reader.getId(), reader) != null) {
      throw new SirixUsageException(ID_GENERATION_EXCEPTION);
    }

    return reader;
  }

  public abstract R createNodeReadOnlyTrx(int nodeTrxId, StorageEngineReader pageReadTrx, Node documentNode);

  public abstract W createNodeReadWriteTrx(int nodeTrxId, StorageEngineWriter pageTrx, int maxNodeCount, Duration autoCommitDelay,
      Node documentNode, AfterCommitState afterCommitState);

  static Node getDocumentNode(final StorageEngineReader pageReadTrx) {
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
        // Check if this is an orphaned lock situation (lock held but no transaction tracked)
        final boolean hasTrackedTransaction = nodeTrxMap.values().stream()
            .anyMatch(NodeTrx.class::isInstance);
        
        if (!hasTrackedTransaction) {
          // Orphaned lock detected - the lock is held but no transaction exists
          // This can happen if a transaction was closed improperly (e.g., exception during close)
          LOGGER.warn("Orphaned write lock detected - releasing lock and retrying. " +
              "This may indicate a previous transaction was not properly closed.");
          
          // Force release the orphaned lock
          writeLock.release();
          
          // Try to acquire again
          if (!writeLock.tryAcquire(5, TimeUnit.SECONDS)) {
            throw new SirixUsageException(
                "No read-write transaction available, please close the running read-write transaction first.");
          }
        } else {
          throw new SirixUsageException(
              "No read-write transaction available, please close the running read-write transaction first.");
        }
      }
    } catch (final InterruptedException e) {
      throw new SirixThreadedException(e);
    }

    LOGGER.trace("Lock: lock acquired (beginNodeTrx)");

    // Create new page write transaction (shares the same ID with the node write trx).
    final int nodeTrxId = nodeTrxIDCounter.incrementAndGet();
    final int lastRev = getMostRecentRevisionNumber();
    final StorageEngineWriter pageWtx = createPageTransaction(nodeTrxId, lastRev, lastRev, Abort.NO, true);

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
      // NOTE: ClockSweepers are GLOBAL now - don't stop them per-session
      // They continue running, managed by BufferManager lifecycle
      
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
      for (StorageEngineReader rtx : nodePageTrxMap.values()) {
        rtx.close();
      }
      // Close all open page transactions.
      for (StorageEngineReader rtx : pageTrxMap.values()) {
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
  public void setNodePageWriteTransaction(final @NonNegative int transactionID, @NonNull final StorageEngineWriter pageTrx) {
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
    final StorageEngineReader pageRtx = nodePageTrxMap.remove(transactionID);
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

    StorageEngineReader pageReadOnlyTrx;

    var pool = this.pool.get();

    if (pool != null) {
      Poolable<StorageEngineReader> poolable = null;
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
  public StorageEngineReader beginPageReadOnlyTrx(final @NonNegative int revision) {
    assertAccess(revision);

    final int currentPageTrxID = pageTrxIDCounter.incrementAndGet();
    final NodeStorageEngineReader pageReadTrx = new NodeStorageEngineReader(currentPageTrxID,
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
  public synchronized StorageEngineWriter beginPageTrx(final @NonNegative int revision) {
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
    final StorageEngineWriter pageTrx = createPageTransaction(currentPageTrxID, lastRev, lastRev, Abort.NO, false);

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

  /**
   * Begin a read-only transaction at the revision that was valid at the given point in time.
   * 
   * <p>This uses <b>floor semantics</b>: returns the last revision that was committed
   * at or before the given timestamp. This reflects the actual state of the database
   * as it was at that moment in time.
   * 
   * <p>Special cases:
   * <ul>
   *   <li>If timestamp is before all revisions → returns revision 0</li>
   *   <li>If timestamp is after all revisions → returns the most recent revision</li>
   *   <li>If timestamp exactly matches a revision → returns that revision</li>
   *   <li>If timestamp is between revisions → returns the earlier revision</li>
   * </ul>
   *
   * @param pointInTime the point in time to query
   * @return a read-only transaction at the revision valid at that time
   */
  @Override
  public R beginNodeReadOnlyTrx(final @NonNull Instant pointInTime) {
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
   * Optimized revision search using SIMD/Eytzinger layout.
   * Uses cache-friendly data structures for better performance.
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
   * Legacy binary search implementation.
   * Kept for rollback purposes if optimized search has issues.
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
   * <p>This uses <b>floor semantics</b>: returns the last revision that was committed
   * at or before the given timestamp. This reflects the actual state of the database
   * as it was at that moment in time.
   * 
   * <p>Special cases:
   * <ul>
   *   <li>If timestamp is before all revisions → returns 0</li>
   *   <li>If timestamp is after all revisions → returns the most recent revision number</li>
   *   <li>If timestamp exactly matches a revision → returns that revision number</li>
   *   <li>If timestamp is between revisions → returns the earlier revision number</li>
   * </ul>
   *
   * @param pointInTime the point in time to query
   * @return the revision number valid at that time
   */
  @Override
  public int getRevisionNumber(final @NonNull Instant pointInTime) {
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
}
