package org.sirix.access.trx.node;



import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.sirix.access.LocalXdmDatabase;
import org.sirix.access.ResourceStore;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.trx.node.xdm.XdmResourceManagerImpl;
import org.sirix.access.trx.page.PageReadTrxImpl;
import org.sirix.access.trx.page.PageWriteTrxFactory;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.xdm.XdmNodeWriteTrx;
import org.sirix.cache.BufferManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.io.Storage;
import org.sirix.io.Writer;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;

public abstract class AbstractResourceManager<R extends NodeReadTrx, W extends NodeWriteTrx>
    implements ResourceManager<R, W>, InternalResourceManager<R, W> {

  /** The database. */
  final Database<? extends ResourceManager<R, W>> mDatabase;

  /** Write lock to assure only one exclusive write transaction exists. */
  final Lock mWriteLock;

  /** Read semaphore to control running read transactions. */
  final Semaphore mReadSemaphore;

  /** Strong reference to uber page before the begin of a write transaction. */
  final AtomicReference<UberPage> mLastCommittedUberPage;

  /** Remember all running node transactions (both read and write). */
  final ConcurrentMap<Long, R> mNodeReaderMap;

  /** Remember all running page transactions (both read and write). */
  final ConcurrentMap<Long, PageReadTrx> mPageTrxMap;

  /** Remember the write seperately because of the concurrent writes. */
  final ConcurrentMap<Long, PageWriteTrx<Long, Record, UnorderedKeyValuePage>> mNodePageTrxMap;

  /** Lock for blocking the commit. */
  private final Lock mCommitLock;

  /** Resource configuration. */
  final ResourceConfiguration mResourceConfig;

  /** Factory for all interactions with the storage. */
  final Storage mFac;

  /** Atomic counter for concurrent generation of node transaction id. */
  private final AtomicLong mNodeTrxIDCounter;

  /** Atomic counter for concurrent generation of page transaction id. */
  final AtomicLong mPageTrxIDCounter;

  /** {@link IndexController}s used for this session. */
  final ConcurrentMap<Integer, IndexController> mRtxIndexControllers;

  /** {@link IndexController}s used for this session. */
  final ConcurrentMap<Integer, IndexController> mWtxIndexControllers;

  /** Determines if session was closed. */
  volatile boolean mClosed;

  /** The cache of in-memory pages shared amongst all manager / resource transactions. */
  final BufferManager mBufferManager;

  /** The resource store with which this manager has been created. */
  final ResourceStore<? extends ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx>> mResourceStore;

  /**
   * Package private constructor.
   *
   * @param database {@link LocalXdmDatabase} for centralized operations on related sessions
   * @param resourceStore the resource store with which this manager has been created
   * @param resourceConf {@link DatabaseConfiguration} for general setting about the storage
   * @param pageCache the cache of in-memory pages shared amongst all sessions / resource transactions
   * @throws SirixException if Sirix encounters an exception
   */
  public AbstractResourceManager(final Database<? extends ResourceManager<R, W>> database,
      final @Nonnull ResourceStore<? extends ResourceManager<R, W>> resourceStore,
      final @Nonnull ResourceConfiguration resourceConf, final @Nonnull BufferManager bufferManager,
      final @Nonnull Storage storage, final @Nonnull UberPage uberPage, final @Nonnull Semaphore readSemaphore,
      final @Nonnull Lock writeLock) {
    mDatabase = checkNotNull(database);
    mResourceStore = checkNotNull(resourceStore);
    mResourceConfig = checkNotNull(resourceConf);
    mBufferManager = checkNotNull(bufferManager);
    mFac = checkNotNull(storage);

    mNodeReaderMap = new ConcurrentHashMap<>();
    mPageTrxMap = new ConcurrentHashMap<>();
    mNodePageTrxMap = new ConcurrentHashMap<>();
    mRtxIndexControllers = new ConcurrentHashMap<>();
    mWtxIndexControllers = new ConcurrentHashMap<>();

    mNodeTrxIDCounter = new AtomicLong();
    mPageTrxIDCounter = new AtomicLong();
    mCommitLock = new ReentrantLock(false);

    mReadSemaphore = checkNotNull(readSemaphore);
    mWriteLock = checkNotNull(writeLock);

    mLastCommittedUberPage = new AtomicReference<>(uberPage);

    mClosed = false;
  }

  private static long timeDiff(final long lhs, final long rhs) {
    return Math.abs(lhs - rhs);
  }

  /**
   * Create a new {@link PageWriteTrx}.
   *
   * @param id the transaction ID
   * @param representRevision the revision which is represented
   * @param storeRevision the revision which is stored
   * @param abort determines if a transaction must be aborted (rollback) or not
   * @return a new {@link PageWriteTrx} instance
   */
  @Override
  public PageWriteTrx<Long, Record, UnorderedKeyValuePage> createPageWriteTransaction(final @Nonnegative long id,
      final @Nonnegative int representRevision, final @Nonnegative int storeRevision, final Abort abort,
      boolean isBoundToNodeTrx) {
    checkArgument(id >= 0, "id must be >= 0!");
    checkArgument(representRevision >= 0, "representRevision must be >= 0!");
    checkArgument(storeRevision >= 0, "storeRevision must be >= 0!");
    final Writer writer = mFac.createWriter();
    final int lastCommitedRev = mLastCommittedUberPage.get().getRevisionNumber();
    final UberPage lastCommitedUberPage = mLastCommittedUberPage.get();
    return new PageWriteTrxFactory().createPageWriteTrx(this, abort == Abort.YES && lastCommitedUberPage.isBootstrap()
        ? new UberPage()
        : new UberPage(lastCommitedUberPage, representRevision > 0
            ? writer.readUberPageReference().getKey()
            : -1),
        writer, id, representRevision, storeRevision, lastCommitedRev, mBufferManager, isBoundToNodeTrx);
  }

  @Override
  public Path getResourcePath() {
    return mResourceConfig.resourcePath;
  }

  @Override
  public Lock getCommitLock() {
    return mCommitLock;
  }

  @Override
  public R beginNodeReadTrx() {
    return beginNodeReadTrx(mLastCommittedUberPage.get().getRevisionNumber());
  }

  @Override
  public synchronized R beginNodeReadTrx(@Nonnegative final int revisionKey) {
    assertAccess(revisionKey);

    // Make sure not to exceed available number of read transactions.
    try {
      if (!mReadSemaphore.tryAcquire(20, TimeUnit.SECONDS)) {
        throw new SirixUsageException(
            "No read transactions available, please close at least one read transaction at first!");
      }
    } catch (final InterruptedException e) {
      throw new SirixThreadedException(e);
    }

    final PageReadTrx pageReadTrx = beginPageReadTrx(revisionKey);

    final Node documentNode = getDocumentNode(pageReadTrx);

    // Create new reader.
    final R reader = createNodeReadOnlyTrx(mNodeTrxIDCounter.incrementAndGet(), pageReadTrx, documentNode);

    // Remember reader for debugging and safe close.
    if (mNodeReaderMap.put(reader.getId(), reader) != null) {
      throw new SirixUsageException("ID generation is bogus because of duplicate ID.");
    }

    return reader;
  }

  public abstract R createNodeReadOnlyTrx(long nodeTrxId, PageReadTrx pageReadTrx, Node documentNode);

  public abstract W createNodeReadWriteTrx(long nodeTrxId,
      PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageReadTrx, int maxNodeCount, TimeUnit timeUnit, int maxTime,
      Node documentNode);

  static Node getDocumentNode(final PageReadTrx pageReadTrx) {
    final Node documentNode;

    @SuppressWarnings("unchecked")
    final Optional<? extends Node> node =
        (Optional<? extends Node>) pageReadTrx.getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
            PageKind.RECORDPAGE, -1);
    if (node.isPresent()) {
      documentNode = node.get();
    } else {
      pageReadTrx.close();
      throw new IllegalStateException("Node couldn't be fetched from persistent storage!");
    }

    return documentNode;
  }

  /**
   * A commit file which is used by a {@link XdmNodeWriteTrx} to denote if it's currently commiting or
   * not.
   */
  @Override
  public Path getCommitFile() {
    return mResourceConfig.resourcePath.resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
                                       .resolve(".commit");
  }

  @Override
  public W beginNodeWriteTrx() {
    return beginNodeWriteTrx(0, TimeUnit.MINUTES, 0);
  }

  @Override
  public W beginNodeWriteTrx(final @Nonnegative int maxNodeCount) {
    return beginNodeWriteTrx(maxNodeCount, TimeUnit.MINUTES, 0);
  }

  @Override
  public W beginNodeWriteTrx(final @Nonnull TimeUnit timeUnit, final @Nonnegative int maxTime) {
    return beginNodeWriteTrx(0, timeUnit, maxTime);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized W beginNodeWriteTrx(final @Nonnegative int maxNodeCount, final @Nonnull TimeUnit timeUnit,
      final @Nonnegative int maxTime) {
    // Checks.
    assertAccess(mLastCommittedUberPage.get().getRevision());
    if (maxNodeCount < 0 || maxTime < 0) {
      throw new SirixUsageException("maxNodeCount may not be < 0!");
    }
    checkNotNull(timeUnit);

    // Make sure not to exceed available number of write transactions.
    try {
      if (!mWriteLock.tryLock(20, TimeUnit.SECONDS)) {
        throw new SirixUsageException("No write transaction available, please close the write transaction first.");
      }
    } catch (final InterruptedException e) {
      throw new SirixThreadedException(e);
    }

    // Create new page write transaction (shares the same ID with the node write trx).
    final long nodeTrxId = mNodeTrxIDCounter.incrementAndGet();
    final int lastRev = mLastCommittedUberPage.get().getRevisionNumber();
    final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWtx =
        createPageWriteTransaction(nodeTrxId, lastRev, lastRev, Abort.NO, true);

    final Node documentNode = getDocumentNode(pageWtx);

    // Create new node write transaction.
    final W wtx = createNodeReadWriteTrx(nodeTrxId, pageWtx, maxNodeCount, timeUnit, maxTime, documentNode);

    // Remember node transaction for debugging and safe close.
    if (mNodeReaderMap.put(nodeTrxId, (R) wtx) != null || mNodePageTrxMap.put(nodeTrxId, pageWtx) != null) {
      throw new SirixThreadedException("ID generation is bogus because of duplicate ID.");
    }

    return wtx;
  }

  @Override
  public synchronized void close() {
    if (!mClosed) {
      // Close all open node transactions.
      for (NodeReadTrx rtx : mNodeReaderMap.values()) {
        if (rtx instanceof XdmNodeWriteTrx) {
          ((XdmNodeWriteTrx) rtx).rollback();
        }
        rtx.close();
        rtx = null;
      }
      // Close all open node page transactions.
      for (PageReadTrx rtx : mNodePageTrxMap.values()) {
        rtx.close();
        rtx = null;
      }
      // Close all open page transactions.
      for (PageReadTrx rtx : mPageTrxMap.values()) {
        rtx.close();
        rtx = null;
      }

      // Immediately release all ressources.
      mNodeReaderMap.clear();
      mPageTrxMap.clear();
      mNodePageTrxMap.clear();
      mResourceStore.closeResource(mResourceConfig.getResource());

      mFac.close();
      mClosed = true;
    }
  }

  /**
   * Checks for valid revision.
   *
   * @param revision revision number to check
   * @throws IllegalStateException if {@link XdmResourceManagerImpl} is already closed
   * @throws IllegalArgumentException if revision isn't valid
   */
  @Override
  public void assertAccess(final @Nonnegative int revision) {
    if (mClosed) {
      throw new IllegalStateException("Resource manager is already closed!");
    }
    if (revision < 0) {
      throw new IllegalArgumentException("Revision must be at least 0!");
    } else if (revision > mLastCommittedUberPage.get().getRevision()) {
      throw new IllegalArgumentException(
          new StringBuilder("Revision must not be bigger than ")
                                                                .append(Long.toString(
                                                                    mLastCommittedUberPage.get().getRevision()))
                                                                .append("!")
                                                                .toString());
    }
  }

  @Override
  public int getAvailableNodeReadTrx() {
    return mReadSemaphore.availablePermits();
  }

  @Override
  public boolean hasRunningNodeWriteTrx() {
    if (mWriteLock.tryLock()) {
      mWriteLock.unlock();
      return true;
    }

    return false;
  }

  /**
   * Set a new node page write trx.
   *
   * @param transactionID page write transaction ID
   * @param pageWriteTrx page write trx
   */
  @Override
  public void setNodePageWriteTransaction(final @Nonnegative long transactionID,
      @Nonnull final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx) {
    mNodePageTrxMap.put(transactionID, pageWriteTrx);
  }

  /**
   * Close a node page transaction.
   *
   * @param transactionID page write transaction ID
   * @throws SirixIOException if an I/O error occurs
   */
  @Override
  public void closeNodePageWriteTransaction(final @Nonnegative long transactionID) throws SirixIOException {
    final PageReadTrx pageRtx = mNodePageTrxMap.remove(transactionID);
    if (pageRtx != null)
      // assert pageRtx != null : "Must be in the page trx map!";
      pageRtx.close();
  }

  /**
   * Close a write transaction.
   *
   * @param transactionID write transaction ID
   */
  @Override
  public void closeWriteTransaction(final @Nonnegative long transactionID) {
    // Remove from internal map.
    removeFromPageMapping(transactionID);

    // Make new transactions available.
    mWriteLock.unlock();
  }

  /**
   * Close a read transaction.
   *
   * @param transactionID read transaction ID
   */
  @Override
  public void closeReadTransaction(final @Nonnegative long transactionID) {
    // Remove from internal map.
    removeFromPageMapping(transactionID);

    // Make new transactions available.
    mReadSemaphore.release();
  }

  /**
   * Remove from internal maps.
   *
   * @param transactionID transaction ID to remove
   */
  private void removeFromPageMapping(final @Nonnegative long transactionID) {
    // Purge transaction from internal state.
    mNodeReaderMap.remove(transactionID);

    // Removing the write from the own internal mapping
    mNodePageTrxMap.remove(transactionID);
  }

  @Override
  public synchronized boolean isClosed() {
    return mClosed;
  }

  /**
   * Set last commited {@link UberPage}.
   *
   * @param page the new {@link UberPage}
   */
  @Override
  public void setLastCommittedUberPage(final UberPage page) {
    mLastCommittedUberPage.set(checkNotNull(page));
  }

  @Override
  public ResourceConfiguration getResourceConfig() {
    return mResourceConfig;
  }

  @Override
  public int getMostRecentRevisionNumber() {
    return mLastCommittedUberPage.get().getRevisionNumber();
  }

  @Override
  public synchronized PathSummaryReader openPathSummary(final @Nonnegative int revision) {
    assertAccess(revision);

    final PageReadTrx pageReadTrx = beginPageReadTrx(revision);
    return PathSummaryReader.getInstance(pageReadTrx, this);
  }

  @Override
  public PathSummaryReader openPathSummary() {
    return openPathSummary(mLastCommittedUberPage.get().getRevisionNumber());
  }

  @Override
  public PageReadTrx beginPageReadTrx() {
    return beginPageReadTrx(mLastCommittedUberPage.get().getRevisionNumber());
  }

  @Override
  public synchronized PageReadTrx beginPageReadTrx(final @Nonnegative int revision) {
    assertAccess(revision);

    final long currentPageTrxID = mPageTrxIDCounter.incrementAndGet();
    final PageReadTrx pageReadTrx = new PageReadTrxImpl(currentPageTrxID, this, mLastCommittedUberPage.get(), revision,
        mFac.createReader(), null, null, mBufferManager);

    // Remember page transaction for debugging and safe close.
    if (mPageTrxMap.put(currentPageTrxID, pageReadTrx) != null) {
      throw new SirixThreadedException("ID generation is bogus because of duplicate ID.");
    }

    return pageReadTrx;
  }

  @Override
  public PageWriteTrx<Long, Record, UnorderedKeyValuePage> beginPageWriteTrx() throws SirixException {
    return beginPageWriteTrx(mLastCommittedUberPage.get().getRevisionNumber());
  }

  @Override
  public synchronized PageWriteTrx<Long, Record, UnorderedKeyValuePage> beginPageWriteTrx(
      final @Nonnegative int revision) throws SirixException {
    assertAccess(revision);

    // Make sure not to exceed available number of write transactions.
    try {
      if (!mWriteLock.tryLock(20, TimeUnit.SECONDS)) {
        throw new SirixUsageException("No write transaction available, please close the write transaction first.");
      }
    } catch (final InterruptedException e) {
      throw new SirixThreadedException(e);
    }

    final long currentPageTrxID = mPageTrxIDCounter.incrementAndGet();
    final int lastRev = mLastCommittedUberPage.get().getRevisionNumber();
    final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWtx =
        createPageWriteTransaction(currentPageTrxID, lastRev, lastRev, Abort.NO, false);

    // Remember page transaction for debugging and safe close.
    if (mPageTrxMap.put(currentPageTrxID, pageWtx) != null) {
      throw new SirixThreadedException("ID generation is bogus because of duplicate ID.");
    }

    return pageWtx;
  }

  @Override
  public synchronized Database<?> getDatabase() {
    return mDatabase;
  }

  @Override
  public synchronized IndexController getRtxIndexController(final int revision) {
    IndexController controller = mRtxIndexControllers.get(revision);
    if (controller == null) {
      controller = new IndexController();
      mRtxIndexControllers.put(revision, controller);
    }
    return controller;
  }

  @Override
  public synchronized IndexController getWtxIndexController(final int revision) {
    IndexController controller = mWtxIndexControllers.get(revision);
    if (controller == null) {
      controller = new IndexController();
      mWtxIndexControllers.put(revision, controller);
    }
    return controller;
  }

  @Override
  public Optional<R> getNodeReadTrx(final long ID) {
    return Optional.ofNullable(mNodeReaderMap.get(ID));
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized Optional<W> getNodeWriteTrx() {
    return mNodeReaderMap.values().stream().filter(rtx -> rtx instanceof NodeWriteTrx).map(rtx -> (W) rtx).findAny();
  }

  @Override
  public R beginNodeReadTrx(final Instant pointInTime) {
    checkNotNull(pointInTime);

    final long timestamp = pointInTime.toEpochMilli();

    int revision = binarySearch(timestamp);

    if (revision < 0) {
      revision = -revision - 1;
    }

    if (revision == 0)
      return beginNodeReadTrx(0);
    else if (revision == getMostRecentRevisionNumber() + 1)
      return beginNodeReadTrx();

    final R rtxRevisionMinus1 = beginNodeReadTrx(revision - 1);
    final R rtxRevision = beginNodeReadTrx(revision);

    if (timeDiff(timestamp, rtxRevisionMinus1.getRevisionTimestamp()) < timeDiff(timestamp,
        rtxRevision.getRevisionTimestamp())) {
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

    while (low <= high) {
      final int mid = (low + high) >>> 1;

      try (final PageReadTrx trx = beginPageReadTrx(mid)) {
        final long midVal = trx.getActualRevisionRootPage().getRevisionTimestamp();
        final int cmp = Instant.ofEpochMilli(midVal).compareTo(Instant.ofEpochMilli(timestamp));

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
  public int getRevisionNumber(Instant pointInTime) {
    checkNotNull(pointInTime);

    final long timestamp = pointInTime.toEpochMilli();

    int revision = binarySearch(timestamp);

    if (revision < 0) {
      revision = -revision - 1;
    }

    if (revision == 0)
      return 0;
    else if (revision == getMostRecentRevisionNumber() + 1)
      return getMostRecentRevisionNumber();

    try (final R rtxRevisionMinus1 = beginNodeReadTrx(revision - 1); final R rtxRevision = beginNodeReadTrx(revision)) {
      final int revisionNumber;

      if (timeDiff(timestamp, rtxRevisionMinus1.getRevisionTimestamp()) < timeDiff(timestamp,
          rtxRevision.getRevisionTimestamp())) {
        revisionNumber = rtxRevisionMinus1.getRevisionNumber();
      } else {
        revisionNumber = rtxRevision.getRevisionNumber();
      }

      return revisionNumber;
    }
  }

}
