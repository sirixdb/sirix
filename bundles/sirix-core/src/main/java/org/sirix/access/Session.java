/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.access;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.IPageReadTrx;
import org.sirix.api.IPageWriteTrx;
import org.sirix.api.ISession;
import org.sirix.cache.PageContainer;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.exception.TTThreadedException;
import org.sirix.exception.TTUsageException;
import org.sirix.index.path.PathSummary;
import org.sirix.io.EStorage;
import org.sirix.io.IReader;
import org.sirix.io.IStorage;
import org.sirix.io.IWriter;
import org.sirix.page.EPage;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

/**
 * <h1>Session</h1>
 * 
 * <p>
 * Makes sure that there only is a single session instance bound to a Sirix resource.
 * </p>
 */
public final class Session implements ISession {

  /** Session configuration. */
  protected final ResourceConfiguration mResourceConfig;

  /** Session configuration. */
  protected final SessionConfiguration mSessionConfig;

  /** Database for centralized closure of related Sessions. */
  private final Database mDatabase;

  /** Write semaphore to assure only one exclusive write transaction exists. */
  private final Semaphore mWriteSemaphore;

  /** Read semaphore to control running read transactions. */
  private final Semaphore mReadSemaphore;

  /** Strong reference to uber page before the begin of a write transaction. */
  private volatile UberPage mLastCommittedUberPage;

  /** Remember all running node transactions (both read and write). */
  private final Map<Long, INodeReadTrx> mNodeTrxMap;

  /** Remember all running page transactions (both read and write). */
  private final Map<Long, IPageReadTrx> mPageTrxMap;

  /** Lock for blocking the commit. */
  protected final Lock mCommitLock;

  /** Remember the write seperately because of the concurrent writes. */
  private final Map<Long, IPageWriteTrx> mNodePageTrxMap;

  /** Storing all return futures from the sync process. */
  private final Map<Long, Map<Long, Collection<Future<Void>>>> mSyncTransactionsReturns;

  /** abstract factory for all interaction to the storage. */
  private final IStorage mFac;

  /** Atomic counter for concurrent generation of node transaction id. */
  private final AtomicLong mNodeTrxIDCounter;

  /** Atomic counter for concurrent generation of page transaction id. */
  private final AtomicLong mPageTrxIDCounter;

  /** Determines if session was closed. */
  private volatile boolean mClosed;

  /**
   * Package private constructor.
   * 
   * @param pDatabase
   *          {@link Database} for centralized operations on related sessions
   * @param pDatabaseConf
   *          {@link DatabaseConfiguration} for general setting about the storage
   * @param pSessionConf
   *          {@link SessionConfiguration} for handling this specific session
   * @throws AbsTTException
   *           if sirix encounters an error
   */
  Session(@Nonnull final Database pDatabase,
    @Nonnull final ResourceConfiguration pResourceConf,
    @Nonnull final SessionConfiguration pSessionConf) throws AbsTTException {
    mDatabase = checkNotNull(pDatabase);
    mResourceConfig = checkNotNull(pResourceConf);
    mSessionConfig = checkNotNull(pSessionConf);
    mNodeTrxMap = new ConcurrentHashMap<>();
    mPageTrxMap = new ConcurrentHashMap<>();
    mNodePageTrxMap = new ConcurrentHashMap<>();
    mSyncTransactionsReturns = new ConcurrentHashMap<>();

    mNodeTrxIDCounter = new AtomicLong();
    mPageTrxIDCounter = new AtomicLong();
    mCommitLock = new ReentrantLock(false);

    // Init session members.
    mWriteSemaphore = new Semaphore(pSessionConf.mWtxAllowed);
    mReadSemaphore = new Semaphore(pSessionConf.mRtxAllowed);

    mFac = EStorage.getStorage(mResourceConfig);
    if (mFac.exists()) {
      final IReader reader = mFac.getReader();
      final PageReference firstRef = reader.readFirstReference();
      if (firstRef.getPage() == null) {
        mLastCommittedUberPage = (UberPage)reader.read(firstRef.getKey());
      } else {
        mLastCommittedUberPage = (UberPage)firstRef.getPage();
      }
      reader.close();
    } else {
      // Bootstrap uber page and make sure there already is a root node.
      mLastCommittedUberPage = new UberPage();
    }
    mClosed = false;
  }

  @Override
  public INodeReadTrx beginNodeReadTrx() throws AbsTTException {
    return beginNodeReadTrx(mLastCommittedUberPage.getRevisionNumber());
  }

  @Override
  public synchronized INodeReadTrx beginNodeReadTrx(
    @Nonnegative final long pRevisionKey) throws AbsTTException {
    assertAccess(pRevisionKey);
    // Make sure not to exceed available number of read transactions.
    try {
      mReadSemaphore.acquire();
    } catch (final InterruptedException exc) {
      throw new TTThreadedException(exc);
    }

    // Create new read transaction.
    final INodeReadTrx rtx =
      new NodeReadTrx(this, mNodeTrxIDCounter.incrementAndGet(),
        new PageReadTrx(this, mLastCommittedUberPage, pRevisionKey, mFac
          .getReader()));

    // Remember transaction for debugging and safe close.
    if (mNodeTrxMap.put(rtx.getTransactionID(), rtx) != null) {
      throw new TTUsageException(
        "ID generation is bogus because of duplicate ID.");
    }
    return rtx;
  }

  @Override
  public INodeWriteTrx beginNodeWriteTrx() throws AbsTTException {
    return beginNodeWriteTrx(0, TimeUnit.MINUTES, 0);
  }

  @Override
  public synchronized INodeWriteTrx beginNodeWriteTrx(
    @Nonnegative final int pMaxNodeCount, @Nonnull final TimeUnit pTimeUnit,
    @Nonnegative final int pMaxTime) throws AbsTTException {
    // Checks.
    assertAccess(mLastCommittedUberPage.getRevision());
    if (pMaxNodeCount < 0 || pMaxTime < 0) {
      throw new TTUsageException("pMaxNodeCount may not be < 0!");
    }
    checkNotNull(pTimeUnit);

    // Make sure not to exceed available number of write transactions.
    if (mWriteSemaphore.availablePermits() == 0) {
      throw new IllegalStateException(
        "There already is a running exclusive write transaction.");
    }
    try {
      mWriteSemaphore.acquire();
    } catch (final InterruptedException exc) {
      throw new TTThreadedException(exc);
    }

    // Create new page write transaction (shares the same ID with the node write trx).
    final long currentTrxID = mNodeTrxIDCounter.incrementAndGet();
    final long lastRev = mLastCommittedUberPage.getRevisionNumber();
    final IPageWriteTrx pageWtx =
      createPageWriteTransaction(currentTrxID, lastRev, lastRev);

    // Create new node write transaction.
    final INodeWriteTrx wtx =
      new NodeWriteTrx(currentTrxID, this, pageWtx, pMaxNodeCount, pTimeUnit,
        pMaxTime);

    // Remember node transaction for debugging and safe close.
    if (mNodeTrxMap.put(currentTrxID, wtx) != null
      || mNodePageTrxMap.put(currentTrxID, pageWtx) != null) {
      throw new TTThreadedException(
        "ID generation is bogus because of duplicate ID.");
    }

    return wtx;
  }

  /**
   * Create a new {@link IPageWriteTrx}.
   * 
   * @param pId
   *          the transaction ID
   * @param pRepresentRevision
   *          the revision which is represented
   * @param pStoreRevision
   *          revisions
   * @return a new {@link IPageWriteTrx} instance
   * @throws TTIOException
   *           if an I/O error occurs
   */
  IPageWriteTrx createPageWriteTransaction(@Nonnegative final long pId,
    @Nonnegative final long pRepresentRevision,
    @Nonnegative final long pStoreRevision) throws TTIOException {
    checkArgument(pId >= 0, "pId must be >= 0!");
    checkArgument(pRepresentRevision >= 0, "pRepresentRevision must be >= 0!");
    checkArgument(pStoreRevision >= 0, "pStoreRevision must be >= 0!");
    final IWriter writer = mFac.getWriter();
    final long lastCommitedRev =
      mLastCommittedUberPage.getLastCommitedRevisionNumber() > 0
        ? mLastCommittedUberPage.getLastCommitedRevisionNumber() : 0;
    return new PageWriteTrx(this, new UberPage(mLastCommittedUberPage,
      pStoreRevision + 1), writer, pId, pRepresentRevision, pStoreRevision,
      lastCommitedRev);
  }

  @Override
  public synchronized void close() throws AbsTTException {
    if (!mClosed) {
      // Forcibly close all open node transactions.
      for (INodeReadTrx rtx : mNodeTrxMap.values()) {
        if (rtx instanceof INodeWriteTrx) {
          ((INodeWriteTrx)rtx).abort();
        }
        rtx.close();
        rtx = null;
      }
      // Forcibly close all open page transactions.
      for (IPageReadTrx rtx : mPageTrxMap.values()) {
        rtx.close();
        rtx = null;
      }

      // Immediately release all ressources.
      mLastCommittedUberPage = null;
      mNodeTrxMap.clear();
      mPageTrxMap.clear();
      mNodePageTrxMap.clear();

      mFac.close();
      mDatabase.removeSession(mResourceConfig.mPath);
      mClosed = true;
    }
  }

  /**
   * Checks for valid revision.
   * 
   * @param pRevision
   *          revision number to check
   * @throws IllegalStateException
   *           if {@link Session} is already closed
   * @throws IllegalArgumentException
   *           if revision isn't valid
   */
  protected void assertAccess(final long pRevision) {
    if (mClosed) {
      throw new IllegalStateException("Session is already closed!");
    }
    if (pRevision < 0) {
      throw new IllegalArgumentException("Revision must be at least 0!");
    } else if (pRevision > mLastCommittedUberPage.getRevision()) {
      throw new IllegalArgumentException(new StringBuilder(
        "Revision must not be bigger than").append(
        Long.toString(mLastCommittedUberPage.getRevision())).append("!")
        .toString());
    }
  }

  /**
   * Close a write transaction.
   * 
   * @param pTransactionID
   *          write transaction ID
   */
  void closeWriteTransaction(final long pTransactionID) {
    // Purge transaction from internal state.
    mNodeTrxMap.remove(pTransactionID);

    // Removing the write from the own internal mapping
    mNodePageTrxMap.remove(pTransactionID);

    // Make new transactions available.
    mWriteSemaphore.release();
  }

  /**
   * Close a read transaction.
   * 
   * @param pTransactionID
   *          read transaction ID
   */
  void closeReadTransaction(@Nonnegative final long pTransactionID) {
    // Purge transaction from internal state.
    mNodeTrxMap.remove(pTransactionID);
    // Make new transactions available.
    mReadSemaphore.release();
  }

  @Override
  public synchronized boolean isClosed() {
    return mClosed;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("sessionConf", mSessionConfig).add(
      "resourceConf", mResourceConfig).toString();
  }

  @Override
  public String getUser() {
    return mSessionConfig.mUser;
  }

  /**
   * Synchronize logs.
   * 
   * @param pContToSync
   *          {@link PageContainer} to synchronize
   * @param pTransactionId
   *          transaction ID
   * @throws TTThreadedException
   * 
   */
  protected synchronized void syncLogs(
    final @Nonnull PageContainer pContToSync,
    final @Nonnegative long pTransactionID, final @Nonnull EPage pPage)
    throws TTThreadedException {
    final ExecutorService pool = Executors.newCachedThreadPool();
    final Collection<Future<Void>> returnVals = new ArrayList<>();
    for (final Long key : mNodePageTrxMap.keySet()) {
      if (key != pTransactionID) {
        returnVals.add(pool.submit(new LogSyncer(mNodePageTrxMap.get(key),
          pContToSync, pPage)));
      }
    }
    pool.shutdown();
    if (!mSyncTransactionsReturns.containsKey(pTransactionID)) {
      mSyncTransactionsReturns.put(pTransactionID,
        new ConcurrentHashMap<Long, Collection<Future<Void>>>());
    }
    // if (mSyncTransactionsReturns.get(pTransactionId).put(
    // ((NodePage)pContToSync.getComplete()).getNodePageKey(), returnVals) != null) {
    // throw new TTThreadedException(
    // "only one commit and therefore sync per id and nodepage is allowed!");
    // }
  }

  /**
   * Wait until synchronization is finished.
   * 
   * @param pTransactionID
   *          transaction ID for which to wait (all others)
   * @throws TTThreadedException
   *           if an exception occurs
   */
  protected synchronized void waitForFinishedSync(
    final @Nonnegative long pTransactionID) throws TTThreadedException {
    final Map<Long, Collection<Future<Void>>> completeVals =
      mSyncTransactionsReturns.remove(pTransactionID);
    if (completeVals != null) {
      for (final Collection<Future<Void>> singleVals : completeVals.values()) {
        for (final Future<Void> returnVal : singleVals) {
          try {
            returnVal.get();
          } catch (final InterruptedException exc) {
            throw new TTThreadedException(exc);
          } catch (final ExecutionException exc) {
            throw new TTThreadedException(exc);
          }
        }
      }
    }
  }

  /**
   * Synchronize the log.
   */
  class LogSyncer implements Callable<Void> {

    /** {@link IPageWriteTrx} to interact with the page layer. */
    private final IPageWriteTrx mPageWriteTrx;

    /** {@link PageContainer} reference. */
    private final PageContainer mCont;

    /** Type of page. */
    private final EPage mPage;

    /**
     * Log synchronizer.
     * 
     * @param pPageWriteTransaction
     *          Sirix {@link IPageWriteTrx}
     * @param pNodePageCont
     *          {@link PageContainer} to update
     * @param pPage
     *          page type
     */
    LogSyncer(final @Nonnull IPageWriteTrx pPageWriteTransaction,
      final @Nonnull PageContainer pNodePageCont, final @Nonnull EPage pPage) {
      mPageWriteTrx = checkNotNull(pPageWriteTransaction);
      mCont = checkNotNull(pNodePageCont);
      mPage = checkNotNull(pPage);
    }

    @Override
    public Void call() throws Exception {
      mPageWriteTrx.updateDateContainer(mCont, mPage);
      return null;
    }
  }

  /**
   * Set last commited UberPage.
   * 
   * @param pPage
   *          the new {@link UberPage}
   */
  protected synchronized void setLastCommittedUberPage(
    @Nonnull final UberPage pPage) {
    mLastCommittedUberPage = checkNotNull(pPage);
  }

  @Override
  public ResourceConfiguration getResourceConfig() {
    return mResourceConfig;
  }

  @Override
  public long getLastRevisionNumber() {
    return mLastCommittedUberPage.getRevisionNumber();
  }

  @Override
  public PathSummary openPathSummary(@Nonnegative long pRev)
    throws AbsTTException {
    assertAccess(pRev);

    return PathSummary.getInstance(new PageReadTrx(this,
      mLastCommittedUberPage, pRev, mFac.getReader()), this);
  }

  @Override
  public PathSummary openPathSummary() throws AbsTTException {
    return openPathSummary(mLastCommittedUberPage.getRevisionNumber());
  }

  @Override
  public IPageReadTrx beginPageReadTrx() throws AbsTTException {
    return beginPageReadTrx(mLastCommittedUberPage.getRevisionNumber());
  }

  @Override
  public synchronized IPageReadTrx beginPageReadTrx(@Nonnegative long pRev)
    throws AbsTTException {
    return new PageReadTrx(this, mLastCommittedUberPage, pRev, mFac.getReader());
  }

  @Override
  public IPageWriteTrx beginPageWriteTrx() throws AbsTTException {
    return beginPageWriteTrx(mLastCommittedUberPage.getRevisionNumber());
  }

  @Override
  public synchronized IPageWriteTrx beginPageWriteTrx(@Nonnegative long pRev)
    throws AbsTTException {
    final long currentPageTrxID = mPageTrxIDCounter.incrementAndGet();
    final long lastRev = mLastCommittedUberPage.getRevisionNumber();
    final IPageWriteTrx pageWtx =
      createPageWriteTransaction(currentPageTrxID, lastRev, lastRev);

    // Remember page transaction for debugging and safe close.
    if (mPageTrxMap.put(currentPageTrxID, pageWtx) != null) {
      throw new TTThreadedException(
        "ID generation is bogus because of duplicate ID.");
    }

    return pageWtx;
  }
}
