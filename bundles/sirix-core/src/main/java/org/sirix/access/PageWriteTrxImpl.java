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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.brackit.xquery.xdm.DocumentException;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.cache.BufferManager;
import org.sirix.cache.Cache;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionLogCache;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
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
 * Implements the {@link PageWriteTrx} interface to provide write capabilities to the persistent
 * storage layer.
 * </p>
 *
 * @author Marc Kramis, Seabix AG
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
final class PageWriteTrxImpl extends AbstractForwardingPageReadTrx
		implements PageWriteTrx<Long, Record, UnorderedKeyValuePage> {

	/** Page writer to serialize. */
	private final Writer mPageWriter;

	/**
	 * Persistent BerkeleyDB page log for all page types != UnorderedKeyValuePage.
	 */
	final Cache<Long, PageContainer> mLog;

	/** Last reference to the actual revRoot. */
	private final RevisionRootPage mNewRoot;

	/** {@link PageReadTrxImpl} instance. */
	private final PageReadTrxImpl mPageRtx;

	/** Determines if a log must be replayed or not. */
	private Restore mRestore = Restore.NO;

	/** Determines if transaction is closed. */
	private boolean mIsClosed;

	/** Determines if a path summary should be used or not. */
	private final boolean mUsePathSummary;

	/** {@link IndexController} instance. */
	private final IndexController mIndexController;

	/** The log key. */
	private long mLogKey;

	/**
	 * Constructor.
	 *
	 * @param resourceManager {@link ISessionConfiguration} this page write trx is bound to
	 * @param uberPage root of revision
	 * @param writer writer where this transaction should write to
	 * @param trxId the transaction ID
	 * @param representRev revision represent
	 * @param lastStoredRev last stored revision
	 * @param bufferManager the page cache buffer
	 */
	PageWriteTrxImpl(final XdmResourceManager resourceManager, final UberPage uberPage,
			final Writer writer, final @Nonnegative long trxId, final @Nonnegative int representRev,
			final @Nonnegative int lastStoredRev, final @Nonnegative int lastCommitedRev,
			final @Nonnull BufferManager bufferManager) {
		mUsePathSummary = resourceManager.getResourceConfig().mPathSummary;
		mIndexController = resourceManager.getWtxIndexController(representRev);

		// Deserialize index definitions.
		final File indexes = new File(resourceManager.getResourceConfig().mPath,
				ResourceConfiguration.Paths.INDEXES.getFile().getPath() + lastStoredRev + ".xml");
		if (indexes.exists()) {
			try (final InputStream in = new FileInputStream(indexes)) {
				mIndexController.getIndexes().init(mIndexController.deserialize(in).getFirstChild());
			} catch (IOException | DocumentException | SirixException e) {
				throw new SirixIOException("Index definitions couldn't be deserialized!", e);
			}
		}

		mLog = new TransactionLogCache(resourceManager.getResourceConfig().mPath, "log", this);

		// Create revision tree if needed.
		if (uberPage.isBootstrap()) {
			uberPage.createRevisionTree(this);
		}

		// Page read trx.
		mPageRtx = new PageReadTrxImpl(resourceManager, uberPage, representRev, writer,
				Optional.of(this), Optional.of(mIndexController), bufferManager);

		mPageWriter = writer;
		final RevisionRootPage lastCommitedRoot = mPageRtx.loadRevRoot(lastCommitedRev);
		mNewRoot = preparePreviousRevisionRootPage(representRev, lastStoredRev);
		mNewRoot.setMaxNodeKey(lastCommitedRoot.getMaxNodeKey());

		// First create revision tree if needed.
		final RevisionRootPage revisionRoot = mPageRtx.getActualRevisionRootPage();
		revisionRoot.createNodeTree(this);

		if (mUsePathSummary) {
			// Create path summary tree if needed.
			final PathSummaryPage page = mPageRtx.getPathSummaryPage(revisionRoot);
			mNewRoot.getPathSummaryPageReference()
					.setLogKey(appendLogRecord(new PageContainer(page, page)));
			page.createPathSummaryTree(this, 0);
		}

		final Page namePage = mPageRtx.getNamePage(revisionRoot);
		final Page casPage = mPageRtx.getCASPage(revisionRoot);
		final Page pathPage = mPageRtx.getPathPage(revisionRoot);

		revisionRoot.getNamePageReference()
				.setLogKey(appendLogRecord(new PageContainer(namePage, namePage)));
		revisionRoot.getCASPageReference()
				.setLogKey(appendLogRecord(new PageContainer(casPage, casPage)));
		revisionRoot.getPathPageReference()
				.setLogKey(appendLogRecord(new PageContainer(pathPage, pathPage)));
	}

	@Override
	public void restore(final Restore restore) {
		mRestore = checkNotNull(restore);
	}

	@Override
	public Record prepareEntryForModification(final @Nonnegative Long recordKey,
			final PageKind pageKind, final int index, final Optional<UnorderedKeyValuePage> keyValuePage)
			throws SirixIOException {
		mPageRtx.assertNotClosed();
		checkNotNull(recordKey);
		checkArgument(recordKey >= 0, "recordKey must be >= 0!");
		checkNotNull(pageKind);
		checkNotNull(keyValuePage);

		final long recordPageKey = mPageRtx.pageKey(recordKey);
		final PageContainer cont = prepareRecordPage(recordPageKey, index, pageKind);

		Record record = ((UnorderedKeyValuePage) cont.getModified()).getValue(recordKey);
		if (record == null) {
			final Record oldRecord = ((UnorderedKeyValuePage) cont.getComplete()).getValue(recordKey);
			if (oldRecord == null) {
				throw new SirixIOException("Cannot retrieve record from cache!");
			}
			record = oldRecord;
			((UnorderedKeyValuePage) cont.getModified()).setEntry(record.getNodeKey(), record);
		}
		return record;
	}

	@Override
	public Record createEntry(final Long key, final Record record, final PageKind pageKind,
			final int index, final Optional<UnorderedKeyValuePage> keyValuePage) throws SirixIOException {
		mPageRtx.assertNotClosed();
		// Allocate record key and increment record count.
		long recordKey;
		switch (pageKind) {
			case RECORDPAGE:
				recordKey = mNewRoot.incrementAndGetMaxNodeKey();
				break;
			case PATHSUMMARYPAGE:
				final PathSummaryPage pathSummaryPage =
						((PathSummaryPage) mNewRoot.getPathSummaryPageReference().getPage());
				recordKey = pathSummaryPage.incrementAndGetMaxNodeKey(index);
				break;
			case CASPAGE:
				final CASPage casPage = ((CASPage) mNewRoot.getCASPageReference().getPage());
				recordKey = casPage.incrementAndGetMaxNodeKey(index);
				break;
			case PATHPAGE:
				final PathPage pathPage = ((PathPage) mNewRoot.getPathPageReference().getPage());
				recordKey = pathPage.incrementAndGetMaxNodeKey(index);
				break;
			case NAMEPAGE:
				final NamePage namePage = ((NamePage) mNewRoot.getNamePageReference().getPage());
				recordKey = namePage.incrementAndGetMaxNodeKey(index);
				break;
			default:
				throw new IllegalStateException();
		}

		final long recordPageKey = mPageRtx.pageKey(recordKey);
		final PageContainer cont = prepareRecordPage(recordPageKey, index, pageKind);
		@SuppressWarnings("unchecked")
		final KeyValuePage<Long, Record> modified = (KeyValuePage<Long, Record>) cont.getModified();
		modified.setEntry(record.getNodeKey(), record);
		return record;
	}

	@Override
	public void removeEntry(final Long recordKey, @Nonnull final PageKind pageKind, final int index,
			final Optional<UnorderedKeyValuePage> keyValuePage) throws SirixIOException {
		mPageRtx.assertNotClosed();
		final long nodePageKey = mPageRtx.pageKey(recordKey);
		final PageContainer cont = prepareRecordPage(nodePageKey, index, pageKind);
		final Optional<Record> node = getRecord(recordKey, pageKind, index);
		if (node.isPresent()) {
			final Record nodeToDel = node.get();
			final Node delNode = new DeletedNode(
					new NodeDelegate(nodeToDel.getNodeKey(), -1, -1, -1, Optional.<SirixDeweyID>empty()));
			((UnorderedKeyValuePage) cont.getModified()).setEntry(delNode.getNodeKey(), delNode);
			((UnorderedKeyValuePage) cont.getComplete()).setEntry(delNode.getNodeKey(), delNode);
		} else {
			throw new IllegalStateException("Node not found!");
		}
	}

	@Override
	public Optional<Record> getRecord(final @Nonnegative long recordKey, final PageKind pageKind,
			final @Nonnegative int index) throws SirixIOException {
		mPageRtx.assertNotClosed();
		checkArgument(recordKey >= Fixed.NULL_NODE_KEY.getStandardProperty());
		checkNotNull(pageKind);
		// Calculate page.
		final long recordPageKey = mPageRtx.pageKey(recordKey);

		final PageContainer pageCont = prepareRecordPage(recordPageKey, index, pageKind);
		if (pageCont.equals(PageContainer.EMPTY_INSTANCE)) {
			return mPageRtx.getRecord(recordKey, pageKind, index);
		} else {
			Record node = ((UnorderedKeyValuePage) pageCont.getModified()).getValue(recordKey);
			if (node == null) {
				node = ((UnorderedKeyValuePage) pageCont.getComplete()).getValue(recordKey);
			}
			return mPageRtx.checkItemIfDeleted(node);
		}
	}

	// /**
	// * Get the page container.
	// *
	// * @param pageKind the kind of page
	// * @param index the index to open or {@code -1} if a regular record page container has to be
	// * retrieved
	// * @param recordPageKey the record page key
	// * @return the {@link PageContainer} instance from the write ahead log
	// */
	// private PageContainer getUnorderedRecordPageContainer(final @Nullable PageKind pageKind,
	// final @Nonnegative int index, final @Nonnegative long recordPageKey) {
	// if (pageKind != null) {
	// switch (pageKind) {
	// case RECORDPAGE:
	// case PATHSUMMARYPAGE:
	// case PATHPAGE:
	// case CASPAGE:
	// case NAMEPAGE:
	// final int[] levelPageCountExp = getUberPage().getPageCountExp(pageKind);
	// final PageContainer pageContainer = mLog.get(
	// new IndirectPageLogKey(pageKind, index, levelPageCountExp.length, 0, recordPageKey));
	// return pageContainer == null ? PageContainer.emptyInstance() : pageContainer;
	// default:
	// throw new IllegalStateException();
	// }
	// }
	// return PageContainer.emptyInstance();
	// }

	@Override
	public String getName(final int nameKey, final Kind nodeKind) {
		mPageRtx.assertNotClosed();
		final NamePage currentNamePage = getNamePage(mNewRoot);
		// If currentNamePage == null -> state was commited and no prepareNodepage was invoked yet.
		return (currentNamePage == null || currentNamePage.getName(nameKey, nodeKind) == null)
				? mPageRtx.getName(nameKey, nodeKind)
				: currentNamePage.getName(nameKey, nodeKind);
	}

	@Override
	public int createNameKey(final @Nullable String name, final Kind nodeKind)
			throws SirixIOException {
		mPageRtx.assertNotClosed();
		checkNotNull(nodeKind);
		final String string = (name == null ? "" : name);
		final int nameKey = NamePageHash.generateHashForString(string);
		final NamePage namePage = getNamePage(mNewRoot);
		namePage.setName(nameKey, string, nodeKind);
		return nameKey;
	}

	@Override
	public void commit(final @Nullable PageReference reference) {
		if (reference == null)
			return;

		final PageContainer container = mLog.get(reference.getLogKey());

		Page page = null;

		if (container != null) {
			page = container.getModified();
		}

		if (page == null) {
			return;
		}

		reference.setPage(page);

		// Recursively commit indirectly referenced pages and then write self.
		page.commit(this);
		mPageWriter.write(reference);

		// Remove page reference.
		reference.setPage(null);
	}

	@Override
	public UberPage commit() {
		mPageRtx.assertNotClosed();

		mPageRtx.mResourceManager.getCommitLock().lock();

		final File commitFile = mPageRtx.mResourceManager.commitFile();
		commitFile.deleteOnExit();
		// Issues with windows that it's not created in the first time?
		while (!commitFile.exists()) {
			try {
				commitFile.createNewFile();
			} catch (final IOException e) {
				throw new SirixIOException(e);
			}
		}

		// Forcefully flush write-ahead transaction logs to persistent storage.
		if (mPageRtx.mResourceManager.getResourceManagerConfig().dumpLogs()) {
			mLog.toSecondCache();
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

		final File indexes = new File(mPageRtx.mResourceConfig.mPath,
				ResourceConfiguration.Paths.INDEXES.getFile().getPath() + revision + ".xml");
		try (final OutputStream out = new FileOutputStream(indexes)) {
			mIndexController.serialize(out);
		} catch (final IOException e) {
			throw new SirixIOException("Index definitions couldn't be serialized!", e);
		}

		mLog.clear();
		mLogKey = 0;

		// Delete commit file which denotes that a commit must write the log in the data file.
		final boolean deleted = commitFile.delete();
		if (!deleted) {
			throw new SirixIOException("Commit file couldn't be deleted!");
		}

		final UberPage commitedUberPage =
				(UberPage) mPageWriter.read(mPageWriter.readUberPageReference().getKey(), mPageRtx);

		mPageRtx.mResourceManager.getCommitLock().unlock();

		return commitedUberPage;
	}

	@Override
	public UberPage rollback() {
		mPageRtx.assertNotClosed();

		final UberPage lastUberPage =
				(UberPage) mPageWriter.read(mPageWriter.readUberPageReference().getKey(), mPageRtx);

		return lastUberPage;
	}

	@Override
	public void close() {
		if (!mIsClosed) {
			mPageRtx.assertNotClosed();

			final UberPage lastUberPage =
					(UberPage) mPageWriter.read(mPageWriter.readUberPageReference().getKey(), mPageRtx);

			mPageRtx.mResourceManager.setLastCommittedUberPage(lastUberPage);

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
		mLog.clear();
		mLogKey = 0;
	}

	@Override
	public void closeCaches() {
		mPageRtx.assertNotClosed();
		mPageRtx.closeCaches();
		mLog.close();
	}

	/**
	 * Prepare indirect page, that is getting the referenced indirect page or a new page.
	 *
	 * @param reference {@link PageReference} to get the indirect page from or to create a new one
	 * @return {@link IndirectPage} reference
	 * @throws SirixIOException if an I/O error occurs
	 */
	private IndirectPage prepareIndirectPage(final PageReference reference) throws SirixIOException {
		final PageContainer cont = mLog.get(reference.getLogKey());
		IndirectPage page = cont == null ? null : (IndirectPage) cont.getComplete();
		if (page == null) {
			if (reference.getKey() == Constants.NULL_ID) {
				page = new IndirectPage();
			} else {
				final IndirectPage indirectPage = mPageRtx.dereferenceIndirectPage(reference);
				page = new IndirectPage(indirectPage);
			}
			reference.setLogKey(appendLogRecord(new PageContainer(page, page)));
		}
		return page;
	}

	/**
	 * Prepare record page.
	 *
	 * @param recordPageKey the key of the record page
	 * @param pageKind the kind of page (used to determine the right subtree)
	 * @return {@link PageContainer} instance
	 * @throws SirixIOException if an I/O error occurs
	 */
	private PageContainer prepareRecordPage(final @Nonnegative long recordPageKey, final int index,
			final PageKind pageKind) throws SirixIOException {
		assert recordPageKey >= 0;
		assert pageKind != null;
		// Get the reference to the unordered key/value page storing the records.
		final PageReference reference = prepareLeafOfTree(
				mPageRtx.getPageReference(mNewRoot, pageKind, index), recordPageKey, index, pageKind);

		PageContainer cont = mLog.get(reference.getLogKey());

		if (cont.equals(PageContainer.EMPTY_INSTANCE)) {
			if (reference.getKey() == Constants.NULL_ID) {
				final UnorderedKeyValuePage completePage = new UnorderedKeyValuePage(recordPageKey,
						pageKind, Optional.<PageReference>empty(), mPageRtx);
				final UnorderedKeyValuePage modifyPage = mPageRtx.clone(completePage);
				cont = new PageContainer(completePage, modifyPage);
			} else {
				cont = dereferenceRecordPageForModification(reference);
			}

			assert cont != null;

			switch (pageKind) {
				case RECORDPAGE:
				case PATHSUMMARYPAGE:
				case PATHPAGE:
				case CASPAGE:
				case NAMEPAGE:
					reference.setLogKey(appendLogRecord(cont));
					break;
				default:
					throw new IllegalStateException("Page kind not known!");
			}
		}

		return cont;
	}

	/**
	 * Prepare the previous revision root page and retrieve the next {@link RevisionRootPage}.
	 *
	 * @param baseRevision base revision
	 * @param representRevision the revision to represent
	 * @return new {@link RevisionRootPage} instance
	 * @throws SirixIOException if an I/O error occurs
	 */
	private RevisionRootPage preparePreviousRevisionRootPage(final @Nonnegative int baseRevision,
			final @Nonnegative int representRevision) throws SirixIOException {
		if (getUberPage().isBootstrap()) {
			final RevisionRootPage revisionRootPage = mPageRtx.loadRevRoot(baseRevision);
			// appendLogRecord(new PageContainer(revisionRootPage, revisionRootPage));
			return revisionRootPage;
		} else {
			// Prepare revision root nodePageReference.
			final RevisionRootPage revisionRootPage =
					new RevisionRootPage(mPageRtx.loadRevRoot(baseRevision), representRevision + 1);

			// Prepare indirect tree to hold reference to prepared revision root nodePageReference.
			final PageReference revisionRootPageReference =
					prepareLeafOfTree(getUberPage().getIndirectPageReference(),
							getUberPage().getRevisionNumber(), -1, PageKind.UBERPAGE);

			// Link the prepared revision root nodePageReference with the prepared indirect tree.
			revisionRootPageReference
					.setLogKey(appendLogRecord(new PageContainer(revisionRootPage, revisionRootPage)));

			// Return prepared revision root nodePageReference.
			return revisionRootPage;
		}
	}

	/**
	 * Prepare the leaf of a tree, namely the reference to a {@link UnorderedKeyValuePage}.
	 *
	 * @param startReference start reference
	 * @param key page key to lookup
	 * @param index the index number or {@code -1} if a regular record page should be prepared
	 * @return {@link PageReference} instance pointing to the right {@link UnorderedKeyValuePage} with
	 *         the {@code pKey}
	 * @throws SirixIOException if an I/O error occured
	 */
	private PageReference prepareLeafOfTree(final PageReference startReference,
			final @Nonnegative long key, final int index, final PageKind pageKind)
			throws SirixIOException {
		// Initial state pointing to the indirect nodePageReference of level 0.
		PageReference reference = startReference;
		int offset = 0;
		long levelKey = key;
		final int[] inpLevelPageCountExp = mPageRtx.getUberPage().getPageCountExp(pageKind);

		// Iterate through all levels.
		for (int level = 0, height = inpLevelPageCountExp.length; level < height; level++) {
			offset = (int) (levelKey >> inpLevelPageCountExp[level]);
			levelKey -= offset << inpLevelPageCountExp[level];
			final IndirectPage page = prepareIndirectPage(reference);
			page.setDirty(true);
			reference = page.getReference(offset);
		}

		// Return reference to leaf of indirect tree.
		return reference;
	}

	/**
	 * Dereference record page reference.
	 *
	 * @param reference reference to leaf, that is the record page
	 * @return dereferenced page
	 */
	private PageContainer dereferenceRecordPageForModification(final PageReference reference) {
		final List<UnorderedKeyValuePage> revs = mPageRtx.getSnapshotPages(reference);
		final Versioning revisioning = mPageRtx.mResourceManager.getResourceConfig().mRevisionKind;
		final int mileStoneRevision = mPageRtx.mResourceManager.getResourceConfig().mRevisionsToRestore;
		return revisioning.combineRecordPagesForModification(revs, mileStoneRevision, mPageRtx,
				reference);
	}

	@Override
	public RevisionRootPage getActualRevisionRootPage() {
		return mNewRoot;
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
	public long appendLogRecord(final PageContainer pageContainer) {
		checkNotNull(pageContainer);
		mLog.put(mLogKey, pageContainer);
		return mLogKey++;
	}

	@Override
	public PageWriteTrx<Long, Record, UnorderedKeyValuePage> truncateTo(int revision) {
		mPageWriter.truncateTo(revision);
		return this;
	}
}
