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

import java.io.File;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.Session;
import org.sirix.cache.NodePageContainer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.PathSummary;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.StorageType;
import org.sirix.io.Writer;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

/**
 * <h1>Session</h1>
 * 
 * <p>
 * Makes sure that there only is a single session instance bound to a Sirix
 * resource.
 * </p>
 */
public final class SessionImpl implements Session {

	/** Database for centralized closure of related Sessions. */
	private final DatabaseImpl mDatabase;

	/** Write semaphore to assure only one exclusive write transaction exists. */
	private final Semaphore mWriteSemaphore;

	/** Read semaphore to control running read transactions. */
	private final Semaphore mReadSemaphore;

	/** Strong reference to uber page before the begin of a write transaction. */
	private AtomicReference<UberPage> mLastCommittedUberPage;

	/** Remember all running node transactions (both read and write). */
	private final Map<Long, NodeReadTrx> mNodeTrxMap;

	/** Remember all running page transactions (both read and write). */
	private final Map<Long, PageReadTrx> mPageTrxMap;

	/** Lock for blocking the commit. */
	final Lock mCommitLock;

	/** Session configuration. */
	final ResourceConfiguration mResourceConfig;

	/** Session configuration. */
	final SessionConfiguration mSessionConfig;

	/** Remember the write seperately because of the concurrent writes. */
	private final Map<Long, PageWriteTrx> mNodePageTrxMap;

	/** Storing all return futures from the sync process. */
	private final Map<Long, Map<Long, Collection<Future<Void>>>> mSyncTransactionsReturns;

	/** abstract factory for all interaction to the storage. */
	private final Storage mFac;

	/** Atomic counter for concurrent generation of node transaction id. */
	private final AtomicLong mNodeTrxIDCounter;

	/** Atomic counter for concurrent generation of page transaction id. */
	private final AtomicLong mPageTrxIDCounter;

	/** Determines if session was closed. */
	private volatile boolean mClosed;

	/** File denoting that currently a version is commited. */
	File mCommitFile;

	/**
	 * Package private constructor.
	 * 
	 * @param pDatabase
	 *          {@link DatabaseImpl} for centralized operations on related
	 *          sessions
	 * @param pDatabaseConf
	 *          {@link DatabaseConfiguration} for general setting about the
	 *          storage
	 * @param pSessionConf
	 *          {@link SessionConfiguration} for handling this specific session
	 * @throws SirixException
	 *           if sirix encounters an error
	 */
	SessionImpl(@Nonnull final DatabaseImpl pDatabase,
			@Nonnull final ResourceConfiguration pResourceConf,
			@Nonnull final SessionConfiguration pSessionConf) throws SirixException {
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

		mFac = StorageType.getStorage(mResourceConfig);
		if (mFac.exists()) {
			final Reader reader = mFac.getReader();
			final PageReference firstRef = reader.readFirstReference();
			if (firstRef.getPage() == null) {
				mLastCommittedUberPage = new AtomicReference<>(
						(UberPage) reader.read(firstRef.getKey(), null));
			} else {
				mLastCommittedUberPage = new AtomicReference<>(
						(UberPage) firstRef.getPage());
			}
			reader.close();
		} else {
			// Bootstrap uber page and make sure there already is a root node.
			mLastCommittedUberPage = new AtomicReference<>(new UberPage());
		}
		mClosed = false;
	}

	@Override
	public NodeReadTrx beginNodeReadTrx() throws SirixException {
		return beginNodeReadTrx(mLastCommittedUberPage.get().getRevisionNumber());
	}

	@Override
	public synchronized NodeReadTrx beginNodeReadTrx(
			@Nonnegative final int revisionKey) throws SirixException {
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

		// Create new read transaction.
		final NodeReadTrx rtx = new NodeReadTrxImpl(this,
				mNodeTrxIDCounter.incrementAndGet(), new PageReadTrxImpl(this,
						mLastCommittedUberPage.get(), revisionKey, mFac.getReader()));

		// Remember transaction for debugging and safe close.
		if (mNodeTrxMap.put(rtx.getTransactionID(), rtx) != null) {
			throw new SirixUsageException(
					"ID generation is bogus because of duplicate ID.");
		}
		return rtx;
	}

	/**
	 * A commit file which is used by a {@link NodeWriteTrx} to denote if it's
	 * currently commiting or not.
	 * 
	 * @param revision
	 *          revision number
	 */
	private void commitFile(final int revision) {
		final int rev = mLastCommittedUberPage.get().isBootstrap() ? 0
				: revision + 1;
		mCommitFile = new File(mResourceConfig.mPath, new File(
				ResourceConfiguration.Paths.TRANSACTION_LOG.getFile(), new File(
						new File(String.valueOf(rev)), ".commit").getPath()).getPath());
	}

	@Override
	public NodeWriteTrx beginNodeWriteTrx() throws SirixException {
		return beginNodeWriteTrx(0, TimeUnit.MINUTES, 0);
	}

	@Override
	public synchronized NodeWriteTrx beginNodeWriteTrx(
			@Nonnegative final int maxNodeCount, @Nonnull final TimeUnit timeUnit,
			@Nonnegative final int maxTime) throws SirixException {
		// Checks.
		assertAccess(mLastCommittedUberPage.get().getRevision());
		if (maxNodeCount < 0 || maxTime < 0) {
			throw new SirixUsageException("pMaxNodeCount may not be < 0!");
		}
		checkNotNull(timeUnit);

		// Make sure not to exceed available number of write transactions.
		if (mWriteSemaphore.availablePermits() == 0) {
			throw new IllegalStateException(
					"There already is a running exclusive write transaction.");
		}
		try {
			mWriteSemaphore.acquire();
		} catch (final InterruptedException e) {
			throw new SirixThreadedException(e);
		}

		// Create new page write transaction (shares the same ID with the node write
		// trx).
		final long currentTrxID = mNodeTrxIDCounter.incrementAndGet();
		final int lastRev = mLastCommittedUberPage.get().getRevisionNumber();
		commitFile(lastRev);
		final PageWriteTrx pageWtx = createPageWriteTransaction(currentTrxID,
				lastRev, lastRev);

		// Create new node write transaction.
		final NodeWriteTrx wtx = new NodeWriteTrxImpl(currentTrxID, this, pageWtx,
				maxNodeCount, timeUnit, maxTime);

		// Remember node transaction for debugging and safe close.
		if (mNodeTrxMap.put(currentTrxID, wtx) != null
				|| mNodePageTrxMap.put(currentTrxID, pageWtx) != null) {
			throw new SirixThreadedException(
					"ID generation is bogus because of duplicate ID.");
		}

		return wtx;
	}

	/**
	 * Create a new {@link PageWriteTrx}.
	 * 
	 * @param id
	 *          the transaction ID
	 * @param representRevision
	 *          the revision which is represented
	 * @param storeRevision
	 *          revisions
	 * @return a new {@link PageWriteTrx} instance
	 * @throws SirixException
	 *           if an error occurs
	 */
	PageWriteTrx createPageWriteTransaction(@Nonnegative final long id,
			@Nonnegative final int representRevision,
			@Nonnegative final int storeRevision) throws SirixException {
		checkArgument(id >= 0, "pId must be >= 0!");
		checkArgument(representRevision >= 0, "pRepresentRevision must be >= 0!");
		checkArgument(storeRevision >= 0, "pStoreRevision must be >= 0!");
		final Writer writer = mFac.getWriter();
		final int lastCommitedRev = mLastCommittedUberPage.get()
				.getLastCommitedRevisionNumber() > 0 ? mLastCommittedUberPage.get()
				.getLastCommitedRevisionNumber() : 0;
		return new PageWriteTrxImpl(this, new UberPage(
				mLastCommittedUberPage.get(), storeRevision + 1), writer, id,
				representRevision, storeRevision, lastCommitedRev);
	}

	@Override
	public synchronized void close() throws SirixException {
		if (!mClosed) {
			// Close all open node transactions.
			for (NodeReadTrx rtx : mNodeTrxMap.values()) {
				if (rtx instanceof NodeWriteTrx) {
					((NodeWriteTrx) rtx).abort();
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
			mNodeTrxMap.clear();
			mPageTrxMap.clear();
			mNodePageTrxMap.clear();

			mDatabase.removeSession(mResourceConfig.mPath);

			mFac.close();
			mClosed = true;
		}
	}

	/**
	 * Checks for valid revision.
	 * 
	 * @param pRevision
	 *          revision number to check
	 * @throws IllegalStateException
	 *           if {@link SessionImpl} is already closed
	 * @throws IllegalArgumentException
	 *           if revision isn't valid
	 */
	protected void assertAccess(final @Nonnegative long pRevision) {
		if (mClosed) {
			throw new IllegalStateException("Session is already closed!");
		}
		if (pRevision < 0) {
			throw new IllegalArgumentException("Revision must be at least 0!");
		} else if (pRevision > mLastCommittedUberPage.get().getRevision()) {
			throw new IllegalArgumentException(new StringBuilder(
					"Revision must not be bigger than")
					.append(Long.toString(mLastCommittedUberPage.get().getRevision()))
					.append("!").toString());
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
	 * @param pTransactionID
	 *          page write transaction ID
	 * @param pPageWriteTrx
	 *          page write trx
	 */
	public void setNodePageWriteTransaction(
			final @Nonnegative long pTransactionID,
			@Nonnull final PageWriteTrx pPageWriteTrx) {
		mNodePageTrxMap.put(pTransactionID, pPageWriteTrx);
	}

	/**
	 * Close a node page transaction.
	 * 
	 * @param pTransactionID
	 *          page write transaction ID
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	void closeNodePageWriteTransaction(final @Nonnegative long pTransactionID)
			throws SirixIOException {
		final PageReadTrx pageRtx = mNodePageTrxMap.remove(pTransactionID);
		assert pageRtx != null : "Must be in the page trx map!";
		pageRtx.close();
	}

	/**
	 * Close a write transaction.
	 * 
	 * @param pTransactionID
	 *          write transaction ID
	 */
	void closeWriteTransaction(final @Nonnegative long pTransactionID) {
		// Remove from internal map.
		removeFromPageMapping(pTransactionID);

		// Make new transactions available.
		mWriteSemaphore.release();
	}

	/**
	 * Close a read transaction.
	 * 
	 * @param pTransactionID
	 *          read transaction ID
	 */
	void closeReadTransaction(final @Nonnegative long pTransactionID) {
		// Remove from internal map.
		removeFromPageMapping(pTransactionID);

		// Make new transactions available.
		mReadSemaphore.release();
	}

	/**
	 * Remove from internal maps.
	 * 
	 * @param pTransactionID
	 *          transaction ID to remove
	 */
	private void removeFromPageMapping(final @Nonnegative long pTransactionID) {
		// Purge transaction from internal state.
		mNodeTrxMap.remove(pTransactionID);

		// Removing the write from the own internal mapping
		mNodePageTrxMap.remove(pTransactionID);
	}

	@Override
	public synchronized boolean isClosed() {
		return mClosed;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("sessionConf", mSessionConfig)
				.add("resourceConf", mResourceConfig).toString();
	}

	@Override
	public String getUser() {
		return mSessionConfig.mUser;
	}

	/**
	 * Synchronize logs.
	 * 
	 * @param pContToSync
	 *          {@link NodePageContainer} to synchronize
	 * @param pTransactionId
	 *          transaction ID
	 * @throws SirixThreadedException
	 * 
	 */
	protected synchronized void syncLogs(
			final @Nonnull NodePageContainer pContToSync,
			final @Nonnegative long pTransactionID, final @Nonnull PageKind pPage)
			throws SirixThreadedException {
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
		// ((NodePage)pContToSync.getComplete()).getNodePageKey(), returnVals) !=
		// null) {
		// throw new TTThreadedException(
		// "only one commit and therefore sync per id and nodepage is allowed!");
		// }
	}

	/**
	 * Wait until synchronization is finished.
	 * 
	 * @param pTransactionID
	 *          transaction ID for which to wait (all others)
	 * @throws SirixThreadedException
	 *           if an exception occurs
	 */
	protected synchronized void waitForFinishedSync(
			final @Nonnegative long pTransactionID) throws SirixThreadedException {
		final Map<Long, Collection<Future<Void>>> completeVals = mSyncTransactionsReturns
				.remove(pTransactionID);
		if (completeVals != null) {
			for (final Collection<Future<Void>> singleVals : completeVals.values()) {
				for (final Future<Void> returnVal : singleVals) {
					try {
						returnVal.get();
					} catch (final InterruptedException exc) {
						throw new SirixThreadedException(exc);
					} catch (final ExecutionException exc) {
						throw new SirixThreadedException(exc);
					}
				}
			}
		}
	}

	/**
	 * Synchronize the log.
	 */
	class LogSyncer implements Callable<Void> {

		/** {@link PageWriteTrx} to interact with the page layer. */
		private final PageWriteTrx mPageWriteTrx;

		/** {@link NodePageContainer} reference. */
		private final NodePageContainer mCont;

		/** Type of page. */
		private final PageKind mPage;

		/**
		 * Log synchronizer.
		 * 
		 * @param pPageWriteTransaction
		 *          Sirix {@link PageWriteTrx}
		 * @param pNodePageCont
		 *          {@link NodePageContainer} to update
		 * @param pPage
		 *          page type
		 */
		LogSyncer(final @Nonnull PageWriteTrx pPageWriteTransaction,
				final @Nonnull NodePageContainer pNodePageCont,
				final @Nonnull PageKind pPage) {
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
	 * Set last commited {@link UberPage}.
	 * 
	 * @param pPage
	 *          the new {@link UberPage}
	 */
	protected void setLastCommittedUberPage(@Nonnull final UberPage pPage) {
		mLastCommittedUberPage.set(checkNotNull(pPage));
	}

	@Override
	public ResourceConfiguration getResourceConfig() {
		return mResourceConfig;
	}

	@Override
	public int getLastRevisionNumber() {
		return mLastCommittedUberPage.get().getRevisionNumber();
	}

	@Override
	public synchronized PathSummary openPathSummary(@Nonnegative int pRev)
			throws SirixException {
		assertAccess(pRev);

		return PathSummary.getInstance(
				new PageReadTrxImpl(this, mLastCommittedUberPage.get(), pRev, mFac
						.getReader()), this);
	}

	@Override
	public PathSummary openPathSummary() throws SirixException {
		return openPathSummary(mLastCommittedUberPage.get().getRevisionNumber());
	}

	@Override
	public PageReadTrx beginPageReadTrx() throws SirixException {
		return beginPageReadTrx(mLastCommittedUberPage.get().getRevisionNumber());
	}

	@Override
	public synchronized PageReadTrx beginPageReadTrx(@Nonnegative int pRev)
			throws SirixException {
		return new PageReadTrxImpl(this, mLastCommittedUberPage.get(), pRev,
				mFac.getReader());
	}

	@Override
	public PageWriteTrx beginPageWriteTrx() throws SirixException {
		return beginPageWriteTrx(mLastCommittedUberPage.get().getRevisionNumber());
	}

	@Override
	public synchronized PageWriteTrx beginPageWriteTrx(@Nonnegative int pRev)
			throws SirixException {
		final long currentPageTrxID = mPageTrxIDCounter.incrementAndGet();
		final int lastRev = mLastCommittedUberPage.get().getRevisionNumber();
		final PageWriteTrx pageWtx = createPageWriteTransaction(currentPageTrxID,
				lastRev, lastRev);

		// Remember page transaction for debugging and safe close.
		if (mPageTrxMap.put(currentPageTrxID, pageWtx) != null) {
			throw new SirixThreadedException(
					"ID generation is bogus because of duplicate ID.");
		}

		return pageWtx;
	}

	@Override
	public synchronized Database getDatabase() {
		return mDatabase;
	}

	@Override
	public Optional<NodeWriteTrx> getNodeWriteTrx() {
		// TODO
		return null;
	}

	@Override
	public Session commitAll() throws SirixException {
		if (!mClosed) {
			for (NodeReadTrx rtx : mNodeTrxMap.values()) {
				if (rtx instanceof NodeWriteTrx) {
					((NodeWriteTrx) rtx).commit();
				}
			}
		}
		return this;
	}
}
