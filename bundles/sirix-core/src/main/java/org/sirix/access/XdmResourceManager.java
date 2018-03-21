/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.access;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.file.Path;
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
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.ResourceManagerConfiguration;
import org.sirix.api.Database;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.XdmNodeWriteTrx;
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
import com.google.common.base.MoreObjects;

/**
 * <h1>XdmResourceManager</h1>
 *
 * <p>
 * Makes sure that there only is a single resource manager instance per thread bound to a resource.
 * </p>
 */
public final class XdmResourceManager implements ResourceManager {

  /** Database for centralized closure of related Sessions. */
  private final DatabaseImpl mDatabase;

  /** The resource manager configuration. */
  private final ResourceManagerConfiguration mResourceManagerConfig;

  /** Write semaphore to assure only one exclusive write transaction exists. */
  private final Semaphore mWriteSemaphore;

  /** Read semaphore to control running read transactions. */
  private final Semaphore mReadSemaphore;

  /** Strong reference to uber page before the begin of a write transaction. */
  private AtomicReference<UberPage> mLastCommittedUberPage;

  /** Remember all running node transactions (both read and write). */
  private final ConcurrentMap<Long, XdmNodeReadTrx> mNodeReaderMap;

  /** Remember all running page transactions (both read and write). */
  private final ConcurrentMap<Long, PageReadTrx> mPageTrxMap;

  /** Lock for blocking the commit. */
  private final Lock mCommitLock;

  /** Resource configuration. */
  private final ResourceConfiguration mResourceConfig;

  /** Remember the write seperately because of the concurrent writes. */
  private final ConcurrentMap<Long, PageWriteTrx<Long, Record, UnorderedKeyValuePage>> mNodePageTrxMap;

  /** Factory for all interactions with the storage. */
  private final Storage mFac;

  /** Atomic counter for concurrent generation of node transaction id. */
  private final AtomicLong mNodeTrxIDCounter;

  /** Atomic counter for concurrent generation of page transaction id. */
  private final AtomicLong mPageTrxIDCounter;

  /** {@link IndexController}s used for this session. */
  private final ConcurrentMap<Integer, IndexController> mRtxIndexControllers;

  /** {@link IndexController}s used for this session. */
  private final ConcurrentMap<Integer, IndexController> mWtxIndexControllers;

  /** Determines if session was closed. */
  private volatile boolean mClosed;

  /** The cache of in-memory pages shared amongst all manager / resource transactions. */
  private final BufferManager mBufferManager;

  /** The resource store with which this manager has been created. */
  private final ResourceStore mResourceStore;

  /** Abort a write transaction. */
  enum Abort {
    /** Yes, abort. */
    YES,

    /** No, don't abort. */
    NO
  }

  /**
   * Package private constructor.
   *
   * @param database {@link DatabaseImpl} for centralized operations on related sessions
   * @param resourceStore the resource store with which this manager has been created
   * @param resourceConf {@link DatabaseConfiguration} for general setting about the storage
   * @param resourceManagerConfig {@link ResourceManagerConfiguration} for handling this specific
   *        resource transactions
   * @param pageCache the cache of in-memory pages shared amongst all sessions / resource
   *        transactions
   * @throws SirixException if Sirix encounters an exception
   */
  XdmResourceManager(final DatabaseImpl database, final @Nonnull ResourceStore resourceStore,
      final @Nonnull ResourceConfiguration resourceConf,
      final @Nonnull ResourceManagerConfiguration resourceManagerConfig,
      final @Nonnull BufferManager bufferManager, final @Nonnull Storage storage,
      final @Nonnull UberPage uberPage, final @Nonnull Semaphore readSemaphore,
      final @Nonnull Semaphore writeSemaphore) {
    mDatabase = checkNotNull(database);
    mResourceStore = checkNotNull(resourceStore);
    mResourceConfig = checkNotNull(resourceConf);
    mResourceManagerConfig = checkNotNull(resourceManagerConfig);
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
    mWriteSemaphore = checkNotNull(writeSemaphore);

    mLastCommittedUberPage = new AtomicReference<>(uberPage);

    mClosed = false;
  }

  ResourceManagerConfiguration getResourceManagerConfig() {
    return mResourceManagerConfig;
  }

  Lock getCommitLock() {
    return mCommitLock;
  }

  @Override
  public XdmNodeReadTrx beginNodeReadTrx() {
    return beginNodeReadTrx(mLastCommittedUberPage.get().getRevisionNumber());
  }

  @Override
  public synchronized XdmNodeReadTrx beginNodeReadTrx(@Nonnegative final int revisionKey) {
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

    final PageReadTrx pageReadTrx = new PageReadTrxImpl(this, mLastCommittedUberPage.get(),
        revisionKey, mFac.createReader(), null, null, mBufferManager);

    final Node documentNode = getDocumentNode(pageReadTrx);

    // Create new reader.
    final XdmNodeReadTrx reader = new XdmNodeReadTrxImpl(this, mNodeTrxIDCounter.incrementAndGet(),
        pageReadTrx, documentNode);

    // Remember reader for debugging and safe close.
    if (mNodeReaderMap.put(reader.getId(), reader) != null) {
      throw new SirixUsageException("ID generation is bogus because of duplicate ID.");
    }

    return reader;
  }

  Node getDocumentNode(final PageReadTrx pageReadTrx) {
    final Node documentNode;

    @SuppressWarnings("unchecked")
    final Optional<? extends Node> node = (Optional<? extends Node>) pageReadTrx
        .getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), PageKind.RECORDPAGE, -1);
    if (node.isPresent()) {
      documentNode = node.get();
    } else {
      pageReadTrx.close();
      throw new IllegalStateException("Node couldn't be fetched from persistent storage!");
    }

    return documentNode;
  }

  /**
   * A commit file which is used by a {@link XdmNodeWriteTrx} to denote if it's currently commiting
   * or not.
   */
  Path commitFile() {
    return mResourceConfig.mPath
        .resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_LOG.getFile()).resolve(".commit");
  }

  @Override
  public XdmNodeWriteTrx beginNodeWriteTrx() {
    return beginNodeReadTrx(0, TimeUnit.MINUTES, 0);
  }

  @Override
  public synchronized XdmNodeWriteTrx beginNodeReadTrx(final @Nonnegative int maxNodeCount,
      final @Nonnull TimeUnit timeUnit, final @Nonnegative int maxTime) {
    // Checks.
    assertAccess(mLastCommittedUberPage.get().getRevision());
    if (maxNodeCount < 0 || maxTime < 0) {
      throw new SirixUsageException("maxNodeCount may not be < 0!");
    }
    checkNotNull(timeUnit);

    // Make sure not to exceed available number of write transactions.
    try {
      if (!mWriteSemaphore.tryAcquire(20, TimeUnit.SECONDS)) {
        throw new SirixUsageException(
            "No write transaction available, please close the write transaction first.");
      }
    } catch (final InterruptedException e) {
      throw new SirixThreadedException(e);
    }

    // Create new page write transaction (shares the same ID with the node write
    // trx).
    final long currentTrxID = mNodeTrxIDCounter.incrementAndGet();
    final int lastRev = mLastCommittedUberPage.get().getRevisionNumber();
    final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWtx =
        createPageWriteTransaction(currentTrxID, lastRev, lastRev, Abort.NO);

    final Node documentNode = getDocumentNode(pageWtx);

    // Create new node write transaction.
    final XdmNodeWriteTrx wtx = new XdmNodeWriterTrxImpl(currentTrxID, this, pageWtx, maxNodeCount,
        timeUnit, maxTime, documentNode);

    // Remember node transaction for debugging and safe close.
    if (mNodeReaderMap.put(currentTrxID, wtx) != null
        || mNodePageTrxMap.put(currentTrxID, pageWtx) != null) {
      throw new SirixThreadedException("ID generation is bogus because of duplicate ID.");
    }

    return wtx;
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
  PageWriteTrx<Long, Record, UnorderedKeyValuePage> createPageWriteTransaction(
      final @Nonnegative long id, final @Nonnegative int representRevision,
      final @Nonnegative int storeRevision, final Abort abort) {
    checkArgument(id >= 0, "id must be >= 0!");
    checkArgument(representRevision >= 0, "representRevision must be >= 0!");
    checkArgument(storeRevision >= 0, "storeRevision must be >= 0!");
    final Writer writer = mFac.createWriter();
    final int lastCommitedRev = mLastCommittedUberPage.get().getRevisionNumber();
    final UberPage lastCommitedUberPage = mLastCommittedUberPage.get();
    return new PageWriteTrxImpl(this,
        abort == Abort.YES && lastCommitedUberPage.isBootstrap() ? new UberPage()
            : new UberPage(lastCommitedUberPage,
                representRevision > 0 ? writer.readUberPageReference().getKey() : -1),
        writer, id, representRevision, storeRevision, lastCommitedRev, mBufferManager);
  }

  @Override
  public synchronized void close() {
    if (!mClosed) {
      // Close all open node transactions.
      for (XdmNodeReadTrx rtx : mNodeReaderMap.values()) {
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
      mLastCommittedUberPage = null;
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
   * @throws IllegalStateException if {@link XdmResourceManager} is already closed
   * @throws IllegalArgumentException if revision isn't valid
   */
  void assertAccess(final @Nonnegative long revision) {
    if (mClosed) {
      throw new IllegalStateException("Session is already closed!");
    }
    if (revision < 0) {
      throw new IllegalArgumentException("Revision must be at least 0!");
    } else if (revision > mLastCommittedUberPage.get().getRevision()) {
      throw new IllegalArgumentException(new StringBuilder("Revision must not be bigger than ")
          .append(Long.toString(mLastCommittedUberPage.get().getRevision())).append("!")
          .toString());
    }
  }

  @Override
  public int getAvailableNodeReadTrx() {
    return mReadSemaphore.availablePermits();
  }

  @Override
  public int getAvailableNodeWriteTrx() {
    return mWriteSemaphore.availablePermits();
  }

  /**
   * Set a new node page write trx.
   *
   * @param transactionID page write transaction ID
   * @param pageWriteTrx page write trx
   */
  void setNodePageWriteTransaction(final @Nonnegative long transactionID,
      @Nonnull final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx) {
    mNodePageTrxMap.put(transactionID, pageWriteTrx);
  }

  /**
   * Close a node page transaction.
   *
   * @param transactionID page write transaction ID
   * @throws SirixIOException if an I/O error occurs
   */
  void closeNodePageWriteTransaction(final @Nonnegative long transactionID)
      throws SirixIOException {
    final PageReadTrx pageRtx = mNodePageTrxMap.remove(transactionID);
    assert pageRtx != null : "Must be in the page trx map!";
    pageRtx.close();
  }

  /**
   * Close a write transaction.
   *
   * @param transactionID write transaction ID
   */
  void closeWriteTransaction(final @Nonnegative long transactionID) {
    // Remove from internal map.
    removeFromPageMapping(transactionID);

    // Make new transactions available.
    mWriteSemaphore.release();
  }

  /**
   * Close a read transaction.
   *
   * @param transactionID read transaction ID
   */
  void closeReadTransaction(final @Nonnegative long transactionID) {
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("sessionConf", mResourceManagerConfig)
        .add("resourceConf", mResourceConfig).toString();
  }

  /**
   * Set last commited {@link UberPage}.
   *
   * @param page the new {@link UberPage}
   */
  protected void setLastCommittedUberPage(final UberPage page) {
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

    return PathSummaryReader.getInstance(new PageReadTrxImpl(this, mLastCommittedUberPage.get(),
        revision, mFac.createReader(), null, null, mBufferManager), this);
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
    return new PageReadTrxImpl(this, mLastCommittedUberPage.get(), revision, mFac.createReader(),
        null, null, mBufferManager);
  }

  @Override
  public PageWriteTrx<Long, Record, UnorderedKeyValuePage> beginPageWriteTrx()
      throws SirixException {
    return beginPageWriteTrx(mLastCommittedUberPage.get().getRevisionNumber());
  }

  @Override
  public synchronized PageWriteTrx<Long, Record, UnorderedKeyValuePage> beginPageWriteTrx(
      final @Nonnegative int revision) throws SirixException {
    final long currentPageTrxID = mPageTrxIDCounter.incrementAndGet();
    final int lastRev = mLastCommittedUberPage.get().getRevisionNumber();
    final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWtx =
        createPageWriteTransaction(currentPageTrxID, lastRev, lastRev, Abort.NO);

    // Remember page transaction for debugging and safe close.
    if (mPageTrxMap.put(currentPageTrxID, pageWtx) != null) {
      throw new SirixThreadedException("ID generation is bogus because of duplicate ID.");
    }

    return pageWtx;
  }

  @Override
  public synchronized Database getDatabase() {
    return mDatabase;
  }

  @Override
  public synchronized IndexController getRtxIndexController(int revision) {
    IndexController controller = mRtxIndexControllers.get(revision);
    if (controller == null) {
      controller = new IndexController();
      mRtxIndexControllers.put(revision, controller);
    }
    return controller;
  }

  @Override
  public synchronized IndexController getWtxIndexController(int revision) {
    IndexController controller = mWtxIndexControllers.get(revision);
    if (controller == null) {
      controller = new IndexController();
      mWtxIndexControllers.put(revision, controller);
    }
    return controller;
  }

  @Override
  public synchronized ResourceManagerConfiguration getResourceManagerCfg() {
    return mResourceManagerConfig;
  }

  @Override
  public Optional<XdmNodeReadTrx> getNodeReader(final long ID) {
    return Optional.of(mNodeReaderMap.get(ID));
  }

  @Override
  public synchronized Optional<XdmNodeWriteTrx> getNodeWriteTrx() {
    for (final XdmNodeReadTrx rtx : mNodeReaderMap.values()) {
      if (rtx instanceof XdmNodeWriteTrx) {
        return Optional.of((XdmNodeWriteTrx) rtx);
      }
    }
    return Optional.empty();
  }
}
