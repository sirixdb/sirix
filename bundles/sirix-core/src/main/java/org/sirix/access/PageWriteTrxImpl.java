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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.brackit.xquery.xdm.DocumentException;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.cache.Cache;
import org.sirix.cache.IndexLogKey;
import org.sirix.cache.IndirectPageLogKey;
import org.sirix.cache.RecordPageContainer;
import org.sirix.cache.SynchronizedIndexTransactionLogCache;
import org.sirix.cache.SynchronizedTransactionLogCache;
import org.sirix.cache.SynchronizedTransactionLogPageCache;
import org.sirix.cache.TransactionIndexLogCache;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.io.Writer;
import org.sirix.node.DeletedNode;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.page.CASPage;
import org.sirix.page.IndirectPage;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.PathPage;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.settings.Versioning;
import org.sirix.utils.NamePageHash;

/**
 * <h1>PageWriteTrx</h1>
 * 
 * <p>
 * Implements the {@link PageWriteTrx} interface to provide write capabilities
 * to the persistent storage layer.
 * </p>
 * 
 * @author Marc Kramis, Seabix AG
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
final class PageWriteTrxImpl extends AbstractForwardingPageReadTrx implements
		PageWriteTrx<Long, Record, UnorderedKeyValuePage> {

	/** Page writer to serialize. */
	private final Writer mPageWriter;

	/**
	 * Persistent BerkeleyDB page log for all page types != UnorderedKeyValuePage.
	 */
	final Cache<IndirectPageLogKey, Page> mPageLog;

	/** Cache to store the data changes in this transaction log. */
	final Cache<Long, RecordPageContainer<UnorderedKeyValuePage>> mNodeLog;

	/** Cache to store path summary changes in this transaction log. */
	final Cache<IndexLogKey, RecordPageContainer<UnorderedKeyValuePage>> mPathSummaryLog;

	/** Cache to store path index changes in this transaction log. */
	Cache<IndexLogKey, RecordPageContainer<UnorderedKeyValuePage>> mPathLog;

	/** Cache to store CAS index changes in this transaction log. */
	Cache<IndexLogKey, RecordPageContainer<UnorderedKeyValuePage>> mCASLog;

	/** Cache to store name index changes in this transaction log. */
	Cache<IndexLogKey, RecordPageContainer<UnorderedKeyValuePage>> mNameLog;

	/** Last reference to the actual revRoot. */
	private final RevisionRootPage mNewRoot;

	/** ID for current transaction. */
	private final long mTransactionID;

	/** {@link PageReadTrxImpl} instance. */
	private final PageReadTrxImpl mPageRtx;

	/**
	 * Determines if multiple {@link SynchNodeWriteTrx} are working or just a
	 * single {@link NodeWriteTrxImpl}.
	 */
	private MultipleWriteTrx mMultipleWriteTrx;

	/** Determines if a log must be replayed or not. */
	private Restore mRestore = Restore.NO;

	/** Determines if transaction is closed. */
	private boolean mIsClosed;

	/** Determines if a path summary should be used or not. */
	private final boolean mUsePathSummary;

	/** {@link IndexController} instance. */
	private final IndexController mIndexController;

	/**
	 * Standard constructor.
	 * 
	 * @param session
	 *          {@link ISessionConfiguration} this page write trx is bound to
	 * @param uberPage
	 *          root of revision
	 * @param writer
	 *          writer where this transaction should write to
	 * @param trxId
	 *          the transaction ID
	 * @param representRev
	 *          revision represent
	 * @param lastStoredRev
	 *          last store revision
	 * @throws AbsTTException
	 *           if an error occurs
	 */
	PageWriteTrxImpl(final SessionImpl session, final UberPage uberPage,
			final Writer writer, final @Nonnegative long trxId,
			final @Nonnegative int representRev,
			final @Nonnegative int lastStoredRev,
			final @Nonnegative int lastCommitedRev) {
		final int revision = uberPage.isBootstrap() ? 0 : lastStoredRev + 1;
		mUsePathSummary = session.mResourceConfig.mPathSummary;
		mIndexController = session.getWtxIndexController(representRev);

		// Deserialize index definitions.
		final File indexes = new File(session.mResourceConfig.mPath,
				ResourceConfiguration.Paths.INDEXES.getFile().getPath() + lastStoredRev
						+ ".xml");
		if (indexes.exists()) {
			try (final InputStream in = new FileInputStream(indexes)) {
				mIndexController.getIndexes().init(
						mIndexController.deserialize(in).getFirstChild());
			} catch (IOException | DocumentException | SirixException e) {
				throw new SirixIOException(
						"Index definitions couldn't be deserialized!", e);
			}
		}

		mPageLog = new SynchronizedTransactionLogPageCache(
				session.mResourceConfig.mPath, revision, "page", this);
		mNodeLog = new SynchronizedTransactionLogCache<>(
				session.mResourceConfig.mPath, revision, "node", this);
		if (mUsePathSummary) {
			mPathSummaryLog = new TransactionIndexLogCache<>(
					session.mResourceConfig.mPath, revision, "pathSummary", this);
		} else {
			mPathSummaryLog = null;
		}
		if (mIndexController.containsIndex(IndexType.PATH)) {
			mPathLog = new TransactionIndexLogCache<>(session.mResourceConfig.mPath,
					revision, "path", this);
		}
		if (mIndexController.containsIndex(IndexType.CAS)) {
			mCASLog = new TransactionIndexLogCache<>(session.mResourceConfig.mPath,
					revision, "cas", this);
		}
		if (mIndexController.containsIndex(IndexType.NAME)) {
			mNameLog = new TransactionIndexLogCache<>(session.mResourceConfig.mPath,
					revision, "name", this);
		}

		// Create revision tree if needed.
		if (uberPage.isBootstrap()) {
			uberPage.createRevisionTree(this);
		}

		// Page read trx.
		mPageRtx = new PageReadTrxImpl(session, uberPage, representRev, writer,
				Optional.of(this),
				// uberPage.isBootstrap() ? Optional.of(this) : Optional
				// .<PageWriteTrxImpl> absent(),
				Optional.of(mIndexController));

		mPageWriter = writer;
		mTransactionID = trxId;

		final RevisionRootPage lastCommitedRoot = mPageRtx
				.loadRevRoot(lastCommitedRev);
		mNewRoot = preparePreviousRevisionRootPage(representRev, lastStoredRev);
		mNewRoot.setMaxNodeKey(lastCommitedRoot.getMaxNodeKey());

		// First create revision tree if needed.
		final RevisionRootPage revisionRoot = mPageRtx.getActualRevisionRootPage();
		revisionRoot.createNodeTree(this);

		if (mUsePathSummary) {
			// Create path summary tree if needed.
			final PathSummaryPage page = mPageRtx.getPathSummaryPage(revisionRoot);
			mPageLog.put(new IndirectPageLogKey(PageKind.PATHSUMMARYPAGE, -1, -1, 0),
					page);
			page.createPathSummaryTree(this, 0);
		}

		mPageLog.put(new IndirectPageLogKey(PageKind.NAMEPAGE, -1, -1, 0),
				mPageRtx.getNamePage(revisionRoot));
		mPageLog.put(new IndirectPageLogKey(PageKind.CASPAGE, -1, -1, 0),
				mPageRtx.getCASPage(revisionRoot));
		mPageLog.put(new IndirectPageLogKey(PageKind.PATHPAGE, -1, -1, 0),
				mPageRtx.getPathPage(revisionRoot));
	}

	@Override
	public void restore(final Restore restore) {
		mRestore = checkNotNull(restore);
	}

	@Override
	public Record prepareEntryForModification(final @Nonnegative Long recordKey,
			final PageKind pageKind, final int index,
			final Optional<UnorderedKeyValuePage> keyValuePage)
			throws SirixIOException {
		mPageRtx.assertNotClosed();
		checkNotNull(recordKey);
		checkArgument(recordKey >= 0, "recordKey must be >= 0!");
		checkNotNull(pageKind);
		checkNotNull(keyValuePage);

		final long recordPageKey = mPageRtx.pageKey(recordKey);
		final RecordPageContainer<UnorderedKeyValuePage> cont = prepareRecordPage(
				recordPageKey, index, pageKind);

		Record record = cont.getModified().getValue(recordKey);
		if (record == null) {
			final Record oldRecord = cont.getComplete().getValue(recordKey);
			if (oldRecord == null) {
				throw new SirixIOException("Cannot retrieve record from cache!");
			}
			record = oldRecord;
			cont.getModified().setEntry(record.getNodeKey(), record);
		}
		return record;
	}

	@Override
	public Record createEntry(final Long key, final Record record,
			final PageKind pageKind, final int index,
			final Optional<UnorderedKeyValuePage> keyValuePage)
			throws SirixIOException {
		mPageRtx.assertNotClosed();
		// Allocate record key and increment record count.
		long recordKey;
		switch (pageKind) {
		case RECORDPAGE:
			recordKey = mNewRoot.incrementAndGetMaxNodeKey();
			break;
		case PATHSUMMARYPAGE:
			final PathSummaryPage pathSummaryPage = ((PathSummaryPage) mNewRoot
					.getPathSummaryPageReference().getPage());
			recordKey = pathSummaryPage.incrementAndGetMaxNodeKey(index);
			break;
		case CASPAGE:
			final CASPage casPage = ((CASPage) mNewRoot.getCASPageReference()
					.getPage());
			recordKey = casPage.incrementAndGetMaxNodeKey(index);
			break;
		case PATHPAGE:
			final PathPage pathPage = ((PathPage) mNewRoot.getPathPageReference()
					.getPage());
			recordKey = pathPage.incrementAndGetMaxNodeKey(index);
			break;
		case NAMEPAGE:
			final NamePage namePage = ((NamePage) mNewRoot.getNamePageReference()
					.getPage());
			recordKey = namePage.incrementAndGetMaxNodeKey(index);
			break;
		default:
			throw new IllegalStateException();
		}

		final long recordPageKey = mPageRtx.pageKey(recordKey);
		final RecordPageContainer<UnorderedKeyValuePage> cont = prepareRecordPage(
				recordPageKey, index, pageKind);
		final KeyValuePage<Long, Record> modified = cont.getModified();
		modified.setEntry(record.getNodeKey(), record);
		return record;
	}

	@Override
	public void removeEntry(final Long recordKey,
			@Nonnull final PageKind pageKind, final int index,
			final Optional<UnorderedKeyValuePage> keyValuePage)
			throws SirixIOException {
		mPageRtx.assertNotClosed();
		final long nodePageKey = mPageRtx.pageKey(recordKey);
		final RecordPageContainer<UnorderedKeyValuePage> cont = prepareRecordPage(
				nodePageKey, index, pageKind);
		final Optional<Record> node = getRecord(recordKey, pageKind, index);
		if (node.isPresent()) {
			final Record nodeToDel = node.get();
			final Node delNode = new DeletedNode(new NodeDelegate(
					nodeToDel.getNodeKey(), -1, -1, -1, Optional.<SirixDeweyID> empty()));
			cont.getModified().setEntry(delNode.getNodeKey(), delNode);
			cont.getComplete().setEntry(delNode.getNodeKey(), delNode);
		} else {
			throw new IllegalStateException("Node not found!");
		}
	}

	@Override
	public Optional<Record> getRecord(final @Nonnegative long recordKey,
			final PageKind pageKind, final @Nonnegative int index)
			throws SirixIOException {
		mPageRtx.assertNotClosed();
		checkArgument(recordKey >= Fixed.NULL_NODE_KEY.getStandardProperty());
		checkNotNull(pageKind);
		// Calculate page.
		final long recordPageKey = mPageRtx.pageKey(recordKey);

		final RecordPageContainer<UnorderedKeyValuePage> pageCont = getUnorderedRecordPageContainer(
				pageKind, index, recordPageKey);
		if (pageCont.equals(RecordPageContainer.EMPTY_INSTANCE)) {
			return mPageRtx.getRecord(recordKey, pageKind, index);
		} else {
			Record node = pageCont.getModified().getValue(recordKey);
			if (node == null) {
				node = pageCont.getComplete().getValue(recordKey);
			}
			return mPageRtx.checkItemIfDeleted(node);
		}
	}

	/**
	 * Get the page container.
	 * 
	 * @param pageKind
	 *          the kind of page
	 * @param index
	 *          the index to open or {@code -1} if a regular record page container
	 *          has to be retrieved
	 * @param recordPageKey
	 *          the record page key
	 * @return the {@link RecordPageContainer} instance from the write ahead log
	 */
	private RecordPageContainer<UnorderedKeyValuePage> getUnorderedRecordPageContainer(
			final @Nullable PageKind pageKind, final @Nonnegative int index,
			final @Nonnegative long recordPageKey) {
		if (pageKind != null) {
			switch (pageKind) {
			case RECORDPAGE:
				return mNodeLog.get(recordPageKey);
			case PATHSUMMARYPAGE:
				return mPathSummaryLog.get(new IndexLogKey(recordPageKey, index));
			case PATHPAGE:
				return mPathLog.get(new IndexLogKey(recordPageKey, index));
			case CASPAGE:
				return mCASLog.get(new IndexLogKey(recordPageKey, index));
			case NAMEPAGE:
				return mNameLog.get(new IndexLogKey(recordPageKey, index));
			default:
				throw new IllegalStateException();
			}
		}
		return RecordPageContainer.<UnorderedKeyValuePage> emptyInstance();
	}

	@Override
	public boolean setupIndexTransactionLog(final IndexType indexType)
			throws SirixIOException {
		if (mCASLog != null && mNameLog != null && mPathLog != null) {
			return true;
		}
		switch (indexType) {
		case CAS:
			if (mCASLog == null) {
				mCASLog = new SynchronizedIndexTransactionLogCache<>(
						mPageRtx.mSession.mResourceConfig.mPath,
						mPageRtx.getRevisionNumber(), "cas", this);
			}
			break;
		case NAME:
			if (mNameLog == null) {
				mNameLog = new SynchronizedIndexTransactionLogCache<>(
						mPageRtx.mSession.mResourceConfig.mPath,
						mPageRtx.getRevisionNumber(), "name", this);
			}
			break;
		case PATH:
			if (mPathLog == null) {
				mPathLog = new SynchronizedIndexTransactionLogCache<>(
						mPageRtx.mSession.mResourceConfig.mPath,
						mPageRtx.getRevisionNumber(), "path", this);
			}
			break;
		default:
			throw new IllegalStateException("Index type not known!");
		}
		return false;
	}

	// /**
	// * Remove the page container.
	// *
	// * @param page
	// * the kind of page
	// * @param nodePageKey
	// * the node page key
	// */
	// private void removePageContainer(final PageKind page,
	// final @Nonnegative long nodePageKey) {
	// switch (page) {
	// case NODEPAGE:
	// mNodeLog.remove(nodePageKey);
	// break;
	// case PATHSUMMARYPAGE:
	// mPathLog.remove(nodePageKey);
	// break;
	// case VALUEPAGE:
	// mValueLog.remove(nodePageKey);
	// default:
	// mPageLog.remove(nodePageKey);
	// }
	// }

	@Override
	public String getName(final int nameKey, final Kind nodeKind) {
		mPageRtx.assertNotClosed();
		final NamePage currentNamePage = (NamePage) mNewRoot.getNamePageReference()
				.getPage();
		// if currentNamePage == null -> state was commited and no prepareNodepage
		// was invoked yet
		return (currentNamePage == null || currentNamePage.getName(nameKey,
				nodeKind) == null) ? mPageRtx.getName(nameKey, nodeKind)
				: currentNamePage.getName(nameKey, nodeKind);
	}

	@Override
	public int createNameKey(final @Nullable String name, final Kind nodeKind)
			throws SirixIOException {
		mPageRtx.assertNotClosed();
		checkNotNull(nodeKind);
		final String string = (name == null ? "" : name);
		final int nameKey = NamePageHash.generateHashForString(string);
		final NamePage namePage = (NamePage) mNewRoot.getNamePageReference()
				.getPage();
		namePage.setName(nameKey, string, nodeKind);
		return nameKey;
	}

	@Override
	public void commit(final @Nullable PageReference reference) {
		Page page = null;

		// First, try to get one from the transaction log.
		final long recordPageKey = reference.getKeyValuePageKey();
		final IndirectPageLogKey logKey = reference.getLogKey();
		final PageKind pageKind = logKey == null ? null : logKey.getPageKind();
		final int index = logKey == null ? -1 : logKey.getIndex();
		RecordPageContainer<? extends KeyValuePage<?, ?>> cont = null;
		if (recordPageKey == -1) {
			if (logKey != null) {
				page = mPageLog.get(logKey);
			}
		} else if (pageKind != null) {
			switch (pageKind) {
			case CASPAGE:
			case PATHPAGE:
			case NAMEPAGE:
			case RECORDPAGE:
			case PATHSUMMARYPAGE:
				cont = getUnorderedRecordPageContainer(pageKind, index, recordPageKey);
				break;
			default:
				// throw new IllegalStateException("Page kind not known!");
			}

			if (cont != null) {
				page = cont.getModified();
			}
		}
		// If none is in the log.
		// if (page == null) {
		// // Test if one is instantiated, if so, get
		// // the one from the reference.
		// page = reference.getPage();
		//
		// if (page == null) {
		// // Then try to get one from the page transaction log (indirect pages
		// // forming a tree below revision root pages).
		// final long key = reference.getKey();
		// if (key != Constants.NULL_ID
		// && (getUberPage().getRevision()
		// % mPageRtx.mSession.mResourceConfig.mRevisionsToRestore == 0)
		// && (mPageRtx.mSession.mResourceConfig.mRevisionKind ==
		// Versioning.INCREMENTAL
		// || mPageRtx.mSession.mResourceConfig.mRevisionKind ==
		// Versioning.DIFFERENTIAL)) {
		// /*
		// * Write the whole indirect page tree if it's a full dump, otherwise
		// * record pages which have to be emitted might not be addressable (the
		// * pages from earlier versions would still be reachable).
		// */
		// page = mPageRtx.getFromPageCache(reference);
		// page.setDirty(true);
		//
		// if (page instanceof KeyValuePage) {
		// // If it's a record page, reconstruct it first!
		// @SuppressWarnings("unchecked")
		// final KeyValuePage<Long, Record> recordPage = ((KeyValuePage<Long,
		// Record>) page);
		// final PageKind recordPageKind = recordPage.getPageKind();
		// final long pageKey = recordPage.getPageKey();
		// switch (recordPageKind) {
		// case NAMEPAGE:
		// case CASPAGE:
		// case RECORDPAGE:
		// case PATHSUMMARYPAGE:
		// case PATHPAGE:
		// // Revision to commit is a full dump => get the full page and
		// // dump it (if it is dirty -- checked later).
		// page = mPageRtx
		// .<Long, Record, UnorderedKeyValuePage> getRecordPageContainer(
		// pageKey, index, recordPageKind).getComplete();
		// break;
		// default:
		// throw new IllegalStateException("Page kind not known!");
		// }
		//
		// if (page != null && !page.isDirty()) {
		// // Only write dirty pages which have been modified since the
		// // latest full dump (checked in the versioning algorithms
		// // through setting the dirty flag).
		// return;
		// }
		// }
		// }
		// }
		// }

		if (page == null) {
			return;
		}

		// // Revision to commit is not a full dump => return immediately.
		// if (!page.isDirty()
		// && ((getUberPage().getRevision()
		// % mPageRtx.mSession.mResourceConfig.mRevisionsToRestore != 0) ||
		// mPageRtx.mSession.mResourceConfig.mRevisionKind == Versioning.FULL)) {
		// return;
		// }

		reference.setPage(page);

		// Recursively commit indirectly referenced pages and then
		// write self.
		page.commit(this);
		mPageWriter.write(reference);

		// Remove from transaction log.
		// if (pReference.getPageKind() != null) {
		// removePageContainer(pReference.getPageKind(), nodePageKey);
		// }

		// Remove page reference.
		reference.setPage(null);

		// Afterwards synchronize all logs since the changes must be
		// written to the transaction log as well.
		// if (mMultipleWriteTrx == MultipleWriteTrx.YES && cont != null) {
		// mPageRtx.mSession.syncLogs(cont, mTransactionID,
		// reference.getPageKind());
	}

	@Override
	public UberPage commit(final MultipleWriteTrx multipleWriteTrx) {
		mPageRtx.assertNotClosed();
		mPageRtx.mSession.mCommitLock.lock();
		mMultipleWriteTrx = checkNotNull(multipleWriteTrx);

		final File commitFile = mPageRtx.mSession.commitFile(getRevisionNumber());
		commitFile.deleteOnExit();
		// Issues with windows that it's not created in the first
		// time?
		while (!commitFile.exists()) {
			try {
				commitFile.createNewFile();
			} catch (final IOException e) {
				throw new SirixIOException(e);
			}
		}

		// Forcefully flush write-ahead transaction logs to persistent storage.
		if (mPageRtx.mSession.mSessionConfig.dumpLogs()) {
			mPageLog.toSecondCache();
			mNodeLog.toSecondCache();

			if (mPathSummaryLog != null) {
				mPathSummaryLog.toSecondCache();
			}
			if (mPathLog != null) {
				mPathLog.toSecondCache();
			}
			if (mCASLog != null) {
				mCASLog.toSecondCache();
			}
			if (mNameLog != null) {
				mNameLog.toSecondCache();
			}
		}

		final PageReference uberPageReference = new PageReference();
		final UberPage uberPage = getUberPage();
		uberPageReference.setPage(uberPage);
		final int revision = uberPage.getRevisionNumber();

		// Recursively write indirectly referenced pages.
		uberPage.commit(this);

		uberPageReference.setPage(uberPage);
		mPageWriter.writeUberPageReference(uberPageReference);
		uberPageReference.setPage(null);

		mPageRtx.mSession.waitForFinishedSync(mTransactionID);

		final File indexes = new File(mPageRtx.mResourceConfig.mPath,
				ResourceConfiguration.Paths.INDEXES.getFile().getPath() + revision
						+ ".xml");
		try (final OutputStream out = new FileOutputStream(indexes)) {
			mIndexController.serialize(out);
		} catch (final IOException e) {
			throw new SirixIOException("Index definitions couldn't be serialized!", e);
		}

		// Delete commit file which denotes that a commit must write the log in
		// the data file.
		final boolean deleted = commitFile.delete();
		if (!deleted) {
			throw new SirixIOException("Commit file couldn't be deleted!");
		}

		mPageRtx.mSession.mCommitLock.unlock();
		return uberPage;
	}

	@Override
	public void close() {
		if (!mIsClosed) {
			mPageRtx.assertNotClosed();
			mPageRtx.clearCaches();
			mPageRtx.closeCaches();
			closeCaches();
			mPageWriter.close();
			mIsClosed = true;
		}
	}

	@Override
	public void clearCaches() {
		mPageRtx.assertNotClosed();
		mPageRtx.clearCaches();
		mPageLog.clear();
		mNodeLog.clear();

		if (mPathSummaryLog != null) {
			mPathSummaryLog.clear();
		}
		if (mPathLog != null) {
			mPathLog.clear();
		}
		if (mCASLog != null) {
			mCASLog.clear();
		}
		if (mNameLog != null) {
			mNameLog.clear();
		}
	}

	@Override
	public void closeCaches() {
		mPageRtx.assertNotClosed();
		mPageRtx.closeCaches();
		mNodeLog.close();
		mPageLog.close();

		if (mPathSummaryLog != null) {
			mPathSummaryLog.close();
		}
		if (mPathLog != null) {
			mPathLog.close();
		}
		if (mCASLog != null) {
			mCASLog.close();
		}
		if (mNameLog != null) {
			mNameLog.close();
		}
	}

	/**
	 * Prepare indirect page, that is getting the referenced indirect page or a
	 * new page.
	 * 
	 * @param reference
	 *          {@link PageReference} to get the indirect page from or to create a
	 *          new one
	 * @return {@link IndirectPage} reference
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private IndirectPage prepareIndirectPage(final PageReference reference)
			throws SirixIOException {
		final IndirectPageLogKey logKey = reference.getLogKey();
		IndirectPage page = (IndirectPage) mPageLog.get(logKey);
		if (page == null) {
			if (reference.getKey() == Constants.NULL_ID) {
				page = new IndirectPage();
			} else {
				final IndirectPage indirectPage = mPageRtx
						.dereferenceIndirectPage(reference);
				page = new IndirectPage(indirectPage);
			}
			mPageLog.put(logKey, page);
		}
		return page;
	}

	/**
	 * Prepare record page.
	 * 
	 * @param recordPageKey
	 *          the key of the record page
	 * @param pageKind
	 *          the kind of page (used to determine the right subtree)
	 * @return {@link RecordPageContainer} instance
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private RecordPageContainer<UnorderedKeyValuePage> prepareRecordPage(
			final @Nonnegative long recordPageKey, final int index,
			final PageKind pageKind) throws SirixIOException {
		assert recordPageKey >= 0;
		assert pageKind != null;
		RecordPageContainer<UnorderedKeyValuePage> cont = getUnorderedRecordPageContainer(
				pageKind, index, recordPageKey);
		if (cont.equals(RecordPageContainer.EMPTY_INSTANCE)) {
			// Reference to record page.
			final PageReference reference = prepareLeafOfTree(
					mPageRtx.getPageReference(mNewRoot, pageKind, index), recordPageKey,
					index, pageKind);
			if (reference.getKey() == Constants.NULL_ID) {
				cont = new RecordPageContainer<>(new UnorderedKeyValuePage(
						recordPageKey, pageKind, Optional.<PageReference> empty(),
						mPageRtx));
			} else {
				cont = dereferenceRecordPageForModification(recordPageKey, index,
						pageKind, reference);
			}

			assert cont != null;
			reference.setKeyValuePageKey(recordPageKey);

			switch (pageKind) {
			case RECORDPAGE:
				mNodeLog.put(recordPageKey, cont);
				break;
			case PATHSUMMARYPAGE:
				mPathSummaryLog.put(new IndexLogKey(recordPageKey, index), cont);
				break;
			case PATHPAGE:
				mPathLog.put(new IndexLogKey(recordPageKey, index), cont);
				break;
			case CASPAGE:
				mCASLog.put(new IndexLogKey(recordPageKey, index), cont);
				break;
			case NAMEPAGE:
				mNameLog.put(new IndexLogKey(recordPageKey, index), cont);
				break;
			default:
				throw new IllegalStateException("Page kind not known!");
			}
		}
		return cont;
	}

	/**
	 * Prepare the previous revision root page and retrieve the next
	 * {@link RevisionRootPage}.
	 * 
	 * @param baseRevision
	 *          base revision
	 * @param representRevision
	 *          the revision to represent
	 * @return new {@link RevisionRootPage} instance
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private RevisionRootPage preparePreviousRevisionRootPage(
			final @Nonnegative int baseRevision,
			final @Nonnegative int representRevision) throws SirixIOException {
		if (getUberPage().isBootstrap()) {
			final RevisionRootPage revisionRootPage = mPageRtx
					.loadRevRoot(baseRevision);
			mPageLog.put(new IndirectPageLogKey(PageKind.UBERPAGE, -1,
					Constants.UBPINP_LEVEL_PAGE_COUNT_EXPONENT.length, 0),
					revisionRootPage);
			return revisionRootPage;
		} else {
			// Prepare revision root nodePageReference.
			final RevisionRootPage revisionRootPage = new RevisionRootPage(
					mPageRtx.loadRevRoot(baseRevision), representRevision + 1);

			// Prepare indirect tree to hold reference to prepared revision root
			// nodePageReference.
			final PageReference revisionRootPageReference = prepareLeafOfTree(
					getUberPage().getIndirectPageReference(), getUberPage()
							.getRevisionNumber(), -1, PageKind.UBERPAGE);

			// Link the prepared revision root nodePageReference with the
			// prepared indirect tree.
			mPageLog.put(revisionRootPageReference.getLogKey(), revisionRootPage);

			// Return prepared revision root nodePageReference.
			return revisionRootPage;
		}
	}

	/**
	 * Prepare the leaf of a tree, namely the reference to a
	 * {@link UnorderedKeyValuePage}.
	 * 
	 * @param startReference
	 *          start reference
	 * @param key
	 *          page key to lookup
	 * @param index
	 *          the index number or {@code -1} if a regular record page should be
	 *          prepared
	 * @return {@link PageReference} instance pointing to the right
	 *         {@link UnorderedKeyValuePage} with the {@code pKey}
	 * @throws SirixIOException
	 *           if an I/O error occured
	 */
	private PageReference prepareLeafOfTree(final PageReference startReference,
			final @Nonnegative long key, final int index, final PageKind pageKind)
			throws SirixIOException {
		// Initial state pointing to the indirect nodePageReference of level 0.
		PageReference reference = startReference;
		int offset = 0;
		int parentOffset = 0;
		long levelKey = key;
		final int[] inpLevelPageCountExp = mPageRtx.getUberPage().getPageCountExp(
				pageKind);
		if (reference.getLogKey() == null) {
			reference.setLogKey(new IndirectPageLogKey(pageKind, index, 0,
					(int) levelKey >> inpLevelPageCountExp[0]));
		}

		// Iterate through all levels.
		for (int level = 0, height = inpLevelPageCountExp.length; level < height; level++) {
			offset = (int) (levelKey >> inpLevelPageCountExp[level]);
			levelKey -= offset << inpLevelPageCountExp[level];
			final IndirectPage page = prepareIndirectPage(reference);
			page.setDirty(true);
			reference = page.getReference(offset);
			if (reference.getLogKey() == null) {
				reference.setLogKey(new IndirectPageLogKey(pageKind, index, level + 1,
						parentOffset * Constants.INP_REFERENCE_COUNT + offset));
			}
			parentOffset = offset;
		}

		// Return reference to leaf of indirect tree.
		return reference;
	}

	/**
	 * Dereference record page reference.
	 * 
	 * @param recordPageKey
	 *          key of record page
	 * @param pageKind
	 *          the kind of subtree, the page is in
	 * @return dereferenced page
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private RecordPageContainer<UnorderedKeyValuePage> dereferenceRecordPageForModification(
			final @Nonnegative long recordPageKey, final int index,
			final PageKind pageKind, final PageReference reference)
			throws SirixIOException {
		try {
			final List<UnorderedKeyValuePage> revs = mPageRtx
					.<Long, Record, UnorderedKeyValuePage> getSnapshotPages(
							recordPageKey, index, pageKind, Optional.of(reference));
			final Versioning revisioning = mPageRtx.mSession.mResourceConfig.mRevisionKind;
			final int mileStoneRevision = mPageRtx.mSession.mResourceConfig.mRevisionsToRestore;
			return revisioning.combineRecordPagesForModification(revs,
					mileStoneRevision, mPageRtx, reference);
		} catch (final ExecutionException e) {
			throw new SirixIOException(e.getCause());
		}
	}

	@Override
	public RevisionRootPage getActualRevisionRootPage() {
		return mNewRoot;
	}

	/**
	 * Updating a container in this {@link PageWriteTrxImpl}.
	 * 
	 * @param pCont
	 *          {@link RecordPageContainer} reference to be updated
	 * @param page
	 *          page for which the
	 */
	@Override
	public void updateDataContainer(
			@Nonnull RecordPageContainer<UnorderedKeyValuePage> container,
			final PageKind page) {
		// final long nodePageKey = container.getComplete().getPageKey();
		// switch (page) {
		// case PATHSUMMARYPAGE:
		// container = mPathLog.get(nodePageKey);
		// break;
		// case TEXTVALUEPAGE:
		// container = mTextValueLog.get(nodePageKey);
		// break;
		// case ATTRIBUTEVALUEPAGE:
		// container = mAttributeValueLog.get(nodePageKey);
		// break;
		// case NODEPAGE:
		// container = mNodeLog.get(nodePageKey);
		// break;
		// default:
		// throw new IllegalStateException("page kind not known!");
		// }

		// // Merge containers.
		// final RecordPageImpl modified = container.getModified();
		// final RecordPageImpl otherModified = container.getModified();
		// synchronized (modified) {
		// for (final Entry<Long, Record> entry : otherModified.entrySet()) {
		// if (modified.getRecord(entry.getKey()) == null) {
		// modified.setRecord(entry.getValue());
		// }
		// }
		// }
	}

	@Override
	protected PageReadTrx delegate() {
		return mPageRtx;
	}

	@Override
	public PageReadTrx getPageReadTrx() {
		return mPageRtx;
	}

	@Override
	public void putPageIntoCache(final IndirectPageLogKey key, final Page page) {
		mPageLog.put(checkNotNull(key), checkNotNull(page));
	}

	@Override
	public void putPageIntoKeyValueCache(final PageKind pageKind,
			final @Nonnegative long recordPageKey, final int index,
			final RecordPageContainer<UnorderedKeyValuePage> pageContainer) {
		checkNotNull(pageKind);
		checkArgument(recordPageKey >= 0, "key must be >= 0!");
		checkNotNull(pageContainer);
		switch (pageKind) {
		case RECORDPAGE:
			mNodeLog.put(recordPageKey, pageContainer);
			break;
		case PATHSUMMARYPAGE:
			mPathSummaryLog.put(new IndexLogKey(recordPageKey, index), pageContainer);
			break;
		case PATHPAGE:
			mPathLog.put(new IndexLogKey(recordPageKey, index), pageContainer);
			break;
		case CASPAGE:
			mCASLog.put(new IndexLogKey(recordPageKey, index), pageContainer);
			break;
		case NAMEPAGE:
			mNameLog.put(new IndexLogKey(recordPageKey, index), pageContainer);
			break;
		default:
			throw new IllegalStateException("page kind not known!");
		}
	}

	public Page getFromPageLog(final IndirectPageLogKey key) {
		checkNotNull(key);
		return mPageLog.get(key);
	}
}