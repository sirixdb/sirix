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
import java.util.HashMap;
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
import org.sirix.cache.RecordPageContainer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.StorageType;
import org.sirix.io.Writer;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;

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
	private final Map<Long, PageWriteTrx<Long, Record, UnorderedKeyValuePage>> mNodePageTrxMap;

	/** Storing all return futures from the sync process. */
	private final Map<Long, Map<Long, Collection<Future<Void>>>> mSyncTransactionsReturns;

	/** abstract factory for all interaction to the storage. */
	private final Storage mFac;

	/** Atomic counter for concurrent generation of node transaction id. */
	private final AtomicLong mNodeTrxIDCounter;

	/** Atomic counter for concurrent generation of page transaction id. */
	private final AtomicLong mPageTrxIDCounter;

	/** {@link IndexController}s used for this session. */
	private final Map<Integer, IndexController> mRtxIndexControllers;
	
	/** {@link IndexController}s used for this session. */
	private final Map<Integer, IndexController> mWtxIndexControllers;

	/** Determines if session was closed. */
	private volatile boolean mClosed;

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
	 * @param database
	 *          {@link DatabaseImpl} for centralized operations on related
	 *          sessions
	 * @param resourceConf
	 *          {@link DatabaseConfiguration} for general setting about the
	 *          storage
	 * @param sessionConf
	 *          {@link SessionConfiguration} for handling this specific session
	 * @throws SirixException
	 *           if Sirix encounters an exception
	 */
	SessionImpl(final DatabaseImpl database,
			@Nonnull final ResourceConfiguration resourceConf,
			@Nonnull final SessionConfiguration sessionConf) throws SirixException {
		mDatabase = checkNotNull(database);
		mResourceConfig = checkNotNull(resourceConf);
		mSessionConfig = checkNotNull(sessionConf);
		mNodeTrxMap = new ConcurrentHashMap<>();
		mPageTrxMap = new ConcurrentHashMap<>();
		mNodePageTrxMap = new ConcurrentHashMap<>();
		mSyncTransactionsReturns = new ConcurrentHashMap<>();
		mRtxIndexControllers = new HashMap<>();
		mWtxIndexControllers = new HashMap<>();

		mNodeTrxIDCounter = new AtomicLong();
		mPageTrxIDCounter = new AtomicLong();
		mCommitLock = new ReentrantLock(false);

		// Init session members.
		mWriteSemaphore = new Semaphore(sessionConf.mWtxAllowed);
		mReadSemaphore = new Semaphore(sessionConf.mRtxAllowed);

		mFac = StorageType.getStorage(mResourceConfig);
		if (mFac.exists()) {
			final Reader reader = mFac.getReader();
			final PageReference firstRef = reader.readUberPageReference();
			if (firstRef.getPage() == null) {
				mLastCommittedUberPage = new AtomicReference<>((UberPage) reader.read(
						firstRef.getKey(), null));
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
						mLastCommittedUberPage.get(), revisionKey, mFac.getReader(),
						Optional.<PageWriteTrxImpl> absent(),
						Optional.<IndexController> absent()));

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
	File commitFile(final int revision) {
		return new File(mResourceConfig.mPath, new File(
				ResourceConfiguration.Paths.TRANSACTION_LOG.getFile(), new File(
						new File(String.valueOf(revision)), ".commit").getPath()).getPath());
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
			throw new SirixUsageException("maxNodeCount may not be < 0!");
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
		final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWtx = createPageWriteTransaction(
				currentTrxID, lastRev, lastRev, Abort.NO);

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
	PageWriteTrx<Long, Record, UnorderedKeyValuePage> createPageWriteTransaction(
			final @Nonnegative long id, final @Nonnegative int representRevision,
			final @Nonnegative int storeRevision, final Abort abort)
			throws SirixException {
		checkArgument(id >= 0, "id must be >= 0!");
		checkArgument(representRevision >= 0, "representRevision must be >= 0!");
		checkArgument(storeRevision >= 0, "storeRevision must be >= 0!");
		final Writer writer = mFac.getWriter();
		final int lastCommitedRev = mLastCommittedUberPage.get()
				.getRevisionNumber();
		final UberPage lastCommitedUberPage = mLastCommittedUberPage.get();
		return new PageWriteTrxImpl(this, abort == Abort.YES
				&& lastCommitedUberPage.isBootstrap() ? new UberPage() : new UberPage(
				lastCommitedUberPage), writer, id, representRevision, storeRevision,
				lastCommitedRev);
	}

	@Override
	public synchronized void close() throws SirixException {
		if (!mClosed) {
			// Close all open node transactions.
			for (NodeReadTrx rtx : mNodeTrxMap.values()) {
				if (rtx instanceof NodeWriteTrx) {
					((NodeWriteTrx) rtx).rollback();
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
	 * @param revision
	 *          revision number to check
	 * @throws IllegalStateException
	 *           if {@link SessionImpl} is already closed
	 * @throws IllegalArgumentException
	 *           if revision isn't valid
	 */
	void assertAccess(final @Nonnegative long revision) {
		if (mClosed) {
			throw new IllegalStateException("Session is already closed!");
		}
		if (revision < 0) {
			throw new IllegalArgumentException("Revision must be at least 0!");
		} else if (revision > mLastCommittedUberPage.get().getRevision()) {
			throw new IllegalArgumentException(new StringBuilder(
					"Revision must not be bigger than ")
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
	 * @param transactionID
	 *          page write transaction ID
	 * @param pageWriteTrx
	 *          page write trx
	 */
	void setNodePageWriteTransaction(
			final @Nonnegative long transactionID,
			@Nonnull final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx) {
		mNodePageTrxMap.put(transactionID, pageWriteTrx);
	}

	/**
	 * Close a node page transaction.
	 * 
	 * @param transactionID
	 *          page write transaction ID
	 * @throws SirixIOException
	 *           if an I/O error occurs
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
	 * @param transactionID
	 *          write transaction ID
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
	 * @param transactionID
	 *          read transaction ID
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
	 * @param transactionID
	 *          transaction ID to remove
	 */
	private void removeFromPageMapping(final @Nonnegative long transactionID) {
		// Purge transaction from internal state.
		mNodeTrxMap.remove(transactionID);

		// Removing the write from the own internal mapping
		mNodePageTrxMap.remove(transactionID);
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
	 * @param contToSync
	 *          {@link RecordPageContainer} to synchronize
	 * @param pTransactionId
	 *          transaction ID
	 * @throws SirixThreadedException
	 * 
	 */
	protected synchronized void syncLogs(
			final RecordPageContainer<UnorderedKeyValuePage> contToSync,
			final @Nonnegative long transactionID, final PageKind pageKind)
			throws SirixThreadedException {
		final ExecutorService pool = Executors.newCachedThreadPool();
		final Collection<Future<Void>> returnVals = new ArrayList<>();
		for (final Long key : mNodePageTrxMap.keySet()) {
			if (key != transactionID) {
				returnVals.add(pool.submit(new LogSyncer(mNodePageTrxMap.get(key),
						contToSync, pageKind)));
			}
		}
		pool.shutdown();
		if (!mSyncTransactionsReturns.containsKey(transactionID)) {
			mSyncTransactionsReturns.put(transactionID,
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
		private final PageWriteTrx<Long, Record, UnorderedKeyValuePage> mPageWriteTrx;

		/** {@link RecordPageContainer} reference. */
		private final RecordPageContainer<UnorderedKeyValuePage> mCont;

		/** Type of page. */
		private final PageKind mPage;

		/**
		 * Log synchronizer.
		 * 
		 * @param pPageWriteTransaction
		 *          Sirix {@link PageWriteTrx}
		 * @param pNodePageCont
		 *          {@link RecordPageContainer} to update
		 * @param pPage
		 *          page type
		 */
		LogSyncer(
				final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pPageWriteTransaction,
				final RecordPageContainer<UnorderedKeyValuePage> pNodePageCont,
				final PageKind pPage) {
			mPageWriteTrx = checkNotNull(pPageWriteTransaction);
			mCont = checkNotNull(pNodePageCont);
			mPage = checkNotNull(pPage);
		}

		@Override
		public Void call() throws Exception {
			mPageWriteTrx.updateDataContainer(mCont, mPage);
			return null;
		}
	}

	/**
	 * Set last commited {@link UberPage}.
	 * 
	 * @param page
	 *          the new {@link UberPage}
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
	public synchronized PathSummaryReader openPathSummary(
			final @Nonnegative int revision) throws SirixException {
		assertAccess(revision);

		return PathSummaryReader.getInstance(
				new PageReadTrxImpl(this, mLastCommittedUberPage.get(), revision, mFac
						.getReader(), Optional.<PageWriteTrxImpl> absent(), Optional
						.<IndexController> absent()), this);
	}

	@Override
	public PathSummaryReader openPathSummary() throws SirixException {
		return openPathSummary(mLastCommittedUberPage.get().getRevisionNumber());
	}

	@Override
	public PageReadTrx beginPageReadTrx() throws SirixException {
		return beginPageReadTrx(mLastCommittedUberPage.get().getRevisionNumber());
	}

	@Override
	public synchronized PageReadTrx beginPageReadTrx(
			final @Nonnegative int revision) throws SirixException {
		return new PageReadTrxImpl(this, mLastCommittedUberPage.get(), revision,
				mFac.getReader(), Optional.<PageWriteTrxImpl> absent(),
				Optional.<IndexController> absent());
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
		final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWtx = createPageWriteTransaction(
				currentPageTrxID, lastRev, lastRev, Abort.NO);

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
	public synchronized Session commitAll() throws SirixException {
		if (!mClosed) {
			for (NodeReadTrx rtx : mNodeTrxMap.values()) {
				if (rtx instanceof NodeWriteTrx) {
					((NodeWriteTrx) rtx).commit();
				}
			}
		}
		return this;
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
}
