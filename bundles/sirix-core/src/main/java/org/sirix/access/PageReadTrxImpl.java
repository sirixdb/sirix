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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.ResourceConfiguration.Indexes;
import org.sirix.api.PageReadTrx;
import org.sirix.api.Session;
import org.sirix.cache.RecordPageContainer;
import org.sirix.cache.TransactionLogCache;
import org.sirix.cache.TransactionLogPageCache;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.node.DeletedNode;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.AttributeValuePage;
import org.sirix.page.IndirectPage;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.TextValuePage;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import org.sirix.settings.Revisioning;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * <h1>PageReadTransaction</h1>
 * 
 * <p>
 * Page reading transaction. The only thing shared amongst transactions is the
 * session. Everything else is exclusive to this transaction. It is required
 * that only a single thread has access to this transaction.
 * </p>
 */
final class PageReadTrxImpl implements PageReadTrx {

	/** Page reader exclusively assigned to this transaction. */
	private final Reader mPageReader;

	/** Uber page this transaction is bound to. */
	private final UberPage mUberPage;

	/** Cached name page of this revision. */
	private final RevisionRootPage mRootPage;

	/** Internal reference to node cache. */
	private final LoadingCache<Long, RecordPageContainer<UnorderedKeyValuePage>> mNodeCache;

	/** Internal reference to path cache. */
	private final LoadingCache<Long, RecordPageContainer<UnorderedKeyValuePage>> mPathCache;

	/** Internal reference to text value cache. */
	private final LoadingCache<Long, RecordPageContainer<UnorderedKeyValuePage>> mTextValueCache;

	/** Internal reference to attribute value cache. */
	private final LoadingCache<Long, RecordPageContainer<UnorderedKeyValuePage>> mAttributeValueCache;

	/** Internal reference to page cache. */
	private final LoadingCache<Long, Page> mPageCache;

	/** {@link SessionImpl} reference. */
	protected final SessionImpl mSession;

	/** {@link NamePage} reference. */
	private final NamePage mNamePage;

	/** Determines if page reading transaction is closed or not. */
	private boolean mClosed;

	/** Indexes to read. */
	private final Set<Indexes> mIndexes;

	/**
	 * Optional page transaction log, dependent on the fact, if the log hasn't
	 * been completely transferred into the data file.
	 */
	private final Optional<TransactionLogPageCache> mPageLog;

	/**
	 * Optional path transaction log, dependent on the fact, if the log hasn't
	 * been completely transferred into the data file.
	 */
	private final Optional<TransactionLogCache<UnorderedKeyValuePage>> mPathLog;

	/**
	 * Optional text value transaction log, dependent on the fact, if the log
	 * hasn't been completely transferred into the data file.
	 */
	private final Optional<TransactionLogCache<UnorderedKeyValuePage>> mTextValueLog;

	/**
	 * Optional attribute value transaction log, dependent on the fact, if the log
	 * hasn't been completely transferred into the data file.
	 */
	private final Optional<TransactionLogCache<UnorderedKeyValuePage>> mAttributeValueLog;

	/**
	 * Optional node transaction log, dependent on the fact, if the log hasn't
	 * been completely transferred into the data file.
	 */
	private final Optional<TransactionLogCache<UnorderedKeyValuePage>> mNodeLog;

	/**
	 * {@link ResourceConfiguration} instance.
	 */
	final ResourceConfiguration mResourceConfig;

	/**
	 * Standard constructor.
	 * 
	 * @param session
	 *          current {@link SessionImpl} instance
	 * @param uberPage
	 *          {@link UberPage} to start reading from
	 * @param revision
	 *          key of revision to read from uber page
	 * @param reader
	 *          reader to read stored pages for this transaction
	 * @param persistentCache
	 *          optional persistent cache
	 * @throws SirixIOException
	 *           if reading of the persistent storage fails
	 */
	PageReadTrxImpl(final @Nonnull SessionImpl session,
			final @Nonnull UberPage uberPage, final @Nonnegative int revision,
			final @Nonnull Reader reader) throws SirixIOException {
		checkArgument(revision >= 0, "Revision must be >= 0!");
		mIndexes = session.mResourceConfig.mIndexes;
		mResourceConfig = session.mResourceConfig;

		final File commitFile = session.commitFile(revision);
		final boolean doesExist = commitFile.exists();

		mSession = checkNotNull(session);
		mPageReader = checkNotNull(reader);
		mUberPage = checkNotNull(uberPage);

		// Transaction logs which might have to be read because the data hasn't been
		// commited to the data-file.
		// =======================================================
		mPageLog = doesExist ? Optional.of(new TransactionLogPageCache(
				session.mResourceConfig.mPath, revision, "page", this)) : Optional
				.<TransactionLogPageCache> absent();
		mNodeLog = doesExist ? Optional
				.of(new TransactionLogCache<UnorderedKeyValuePage>(
						session.mResourceConfig.mPath, revision, "node", this)) : Optional
				.<TransactionLogCache<UnorderedKeyValuePage>> absent();
		if (mIndexes.contains(Indexes.PATH)) {
			mPathLog = doesExist ? Optional
					.of(new TransactionLogCache<UnorderedKeyValuePage>(
							session.mResourceConfig.mPath, revision, "path", this))
					: Optional.<TransactionLogCache<UnorderedKeyValuePage>> absent();
		} else {
			mPathLog = Optional.<TransactionLogCache<UnorderedKeyValuePage>> absent();
		}
		if (mIndexes.contains(Indexes.TEXT_VALUE)) {
			mTextValueLog = doesExist ? Optional
					.of(new TransactionLogCache<UnorderedKeyValuePage>(
							session.mResourceConfig.mPath, revision, "value", this))
					: Optional.<TransactionLogCache<UnorderedKeyValuePage>> absent();
		} else {
			mTextValueLog = Optional
					.<TransactionLogCache<UnorderedKeyValuePage>> absent();
		}
		if (mIndexes.contains(Indexes.ATTRIBUTE_VALUE)) {
			mAttributeValueLog = doesExist ? Optional
					.of(new TransactionLogCache<UnorderedKeyValuePage>(
							session.mResourceConfig.mPath, revision, "value", this))
					: Optional.<TransactionLogCache<UnorderedKeyValuePage>> absent();
		} else {
			mAttributeValueLog = Optional
					.<TransactionLogCache<UnorderedKeyValuePage>> absent();
		}

		// In memory caches from data directory.
		// =========================================================
		@SuppressWarnings("resource")
		final PageReadTrxImpl pageReadTrx = this;
		mNodeCache = CacheBuilder
				.newBuilder()
				.maximumSize(1000)
				.expireAfterWrite(5000, TimeUnit.SECONDS)
				.expireAfterAccess(5000, TimeUnit.SECONDS)
				.concurrencyLevel(1)
				.build(
						new CacheLoader<Long, RecordPageContainer<UnorderedKeyValuePage>>() {
							public RecordPageContainer<UnorderedKeyValuePage> load(
									final Long key) throws SirixException {
								final RecordPageContainer<UnorderedKeyValuePage> container = mNodeLog
										.isPresent() ? mNodeLog.get().get(key)
										: RecordPageContainer
												.<UnorderedKeyValuePage> emptyInstance();
								return (RecordPageContainer<UnorderedKeyValuePage>) (container
										.equals(RecordPageContainer.EMPTY_INSTANCE) == true ? pageReadTrx
										.<Long, Record, UnorderedKeyValuePage> getRecordPageContainer(
												key, PageKind.NODEPAGE)
										: container);
							}
						});
		final CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder()
				.concurrencyLevel(1).maximumSize(20);
		if (mIndexes.contains(Indexes.PATH)) {
			mPathCache = builder
					.build(new CacheLoader<Long, RecordPageContainer<UnorderedKeyValuePage>>() {
						public RecordPageContainer<UnorderedKeyValuePage> load(
								final Long key) throws SirixException {
							final RecordPageContainer<UnorderedKeyValuePage> container = mPathLog
									.isPresent() ? mPathLog.get().get(key) : RecordPageContainer
									.<UnorderedKeyValuePage> emptyInstance();
							return (container.equals(RecordPageContainer
									.<UnorderedKeyValuePage> emptyInstance()) == true ? pageReadTrx
									.<Long, Record, UnorderedKeyValuePage> getRecordPageContainer(
											key, PageKind.PATHSUMMARYPAGE)
									: container);
						}
					});
		} else {
			mPathCache = null;
		}
		if (mIndexes.contains(Indexes.TEXT_VALUE)) {
			mTextValueCache = builder
					.build(new CacheLoader<Long, RecordPageContainer<UnorderedKeyValuePage>>() {
						public RecordPageContainer<UnorderedKeyValuePage> load(
								final Long key) throws SirixException {
							final RecordPageContainer<UnorderedKeyValuePage> container = mTextValueLog
									.isPresent() ? mTextValueLog.get().get(key)
									: RecordPageContainer.<UnorderedKeyValuePage> emptyInstance();
							return container.equals(RecordPageContainer
									.<UnorderedKeyValuePage> emptyInstance()) == true ? pageReadTrx
									.<Long, Record, UnorderedKeyValuePage> getRecordPageContainer(
											key, PageKind.TEXTVALUEPAGE)
									: container;
						}
					});
		} else {
			mTextValueCache = null;
		}
		if (mIndexes.contains(Indexes.ATTRIBUTE_VALUE)) {
			mAttributeValueCache = builder
					.build(new CacheLoader<Long, RecordPageContainer<UnorderedKeyValuePage>>() {
						public RecordPageContainer<UnorderedKeyValuePage> load(
								final Long key) throws SirixException {
							final RecordPageContainer<UnorderedKeyValuePage> container = mAttributeValueLog
									.isPresent() ? mAttributeValueLog.get().get(key)
									: RecordPageContainer.<UnorderedKeyValuePage> emptyInstance();
							return container.equals(RecordPageContainer.EMPTY_INSTANCE) == true ? pageReadTrx
									.<Long, Record, UnorderedKeyValuePage> getRecordPageContainer(
									key, PageKind.ATTRIBUTEVALUEPAGE) : container;
						}
					});
		} else {
			mAttributeValueCache = null;
		}

		final CacheBuilder<Object, Object> pageCacheBuilder = CacheBuilder
				.newBuilder();
		final PageReadTrxImpl impl = this;
		mPageCache = pageCacheBuilder.build(new CacheLoader<Long, Page>() {
			public Page load(final Long key) throws SirixException {
				final Page page = mPageLog.isPresent() ? mPageLog.get().get(key) : null;
				if (page == null) {
					return mPageReader.read(key, impl).setDirty(true);
				} else {
					return page;
				}
			}
		});

		// if (mPageLog.isPresent()) {
		// pageCacheBuilder.removalListener(new RemovalListener<Long, Page>() {
		// @Override
		// public void onRemoval(final RemovalNotification<Long, Page> pRemoval) {
		// final Page page = pRemoval.getValue();
		// if (page.isDirty()) {
		// mPageLog.get().put(pRemoval.getKey(), page);
		// }
		// }
		// });
		// }

		// Load revision root.
		mRootPage = loadRevRoot(revision);
		assert mRootPage != null : "root page must not be null!";
		// First create revision tree if needed.
		mRootPage.createNodeTree(this);
		if (mIndexes.contains(Indexes.PATH)) {
			// Create path summary tree if needed.
			getPathSummaryPage(mRootPage);
			mRootPage.createPathSummaryTree(this);
		}
		if (mIndexes.contains(Indexes.TEXT_VALUE)) {
			// Create text value tree if needed.
			getTextValuePage(mRootPage);
			mRootPage.createTextValueTree(this);
		}
		if (mIndexes.contains(Indexes.ATTRIBUTE_VALUE)) {
			// Create attribute value tree if needed.
			getAttributeValuePage(mRootPage);
			mRootPage.createAttributeValueTree(this);
		}
		mNamePage = getNamePage(mRootPage);
		mClosed = false;
	}

	@Override
	public Session getSession() {
		assertNotClosed();
		return mSession;
	}

	/**
	 * Make sure that the transaction is not yet closed when calling this method.
	 */
	final void assertNotClosed() {
		if (mClosed) {
			throw new IllegalStateException("Transaction is already closed.");
		}
	}

	@Override
	public Optional<Record> getRecord(final @Nonnegative long nodeKey,
			final @Nonnull PageKind pageKind) throws SirixIOException {
		checkArgument(nodeKey >= 0);
		checkNotNull(pageKind);
		assertNotClosed();

		final long nodePageKey = pageKey(nodeKey);

		RecordPageContainer<UnorderedKeyValuePage> cont;
		try {
			switch (pageKind) {
			case NODEPAGE:
				cont = mNodeCache.get(nodePageKey);
				break;
			case PATHSUMMARYPAGE:
				cont = mPathCache.get(nodePageKey);
				break;
			case TEXTVALUEPAGE:
				cont = mTextValueCache.get(nodePageKey);
				break;
			case ATTRIBUTEVALUEPAGE:
				cont = mAttributeValueCache.get(nodePageKey);
				break;
			default:
				throw new IllegalStateException();
			}
		} catch (final ExecutionException e) {
			throw new SirixIOException(e);
		}

		if (cont.equals(RecordPageContainer.EMPTY_INSTANCE)) {
			return Optional.<Record> absent();
		}

		final Record retVal = cont.getComplete().getValue(nodeKey);
		return checkItemIfDeleted(retVal);
	}

	/**
	 * Method to check if an {@link Record} is deleted.
	 * 
	 * @param toCheck
	 *          node to check
	 * @return the {@code node} if it is valid, {@code null} otherwise
	 */
	final Optional<Record> checkItemIfDeleted(final @Nullable Record toCheck) {
		if (toCheck instanceof DeletedNode) {
			return Optional.absent();
		} else {
			return Optional.fromNullable(toCheck);
		}
	}

	@Override
	public String getName(final int nameKey, final @Nonnull Kind nodeKind) {
		assertNotClosed();
		return mNamePage.getName(nameKey, nodeKind);
	}

	@Override
	public final byte[] getRawName(final int pNameKey,
			final @Nonnull Kind pNodeKind) {
		assertNotClosed();
		return mNamePage.getRawName(pNameKey, pNodeKind);
	}

	@Override
	public void clearCaches() {
		assertNotClosed();
		if (mIndexes.contains(Indexes.PATH)) {
			mPathCache.invalidateAll();
		}
		if (mIndexes.contains(Indexes.TEXT_VALUE)) {
			mTextValueCache.invalidateAll();
		}
		if (mIndexes.contains(Indexes.ATTRIBUTE_VALUE)) {
			mAttributeValueCache.invalidateAll();
		}
		mNodeCache.invalidateAll();
		mPageCache.invalidateAll();

		if (mPathLog.isPresent()) {
			mPathLog.get().clear();
		}
		if (mTextValueLog.isPresent()) {
			mTextValueLog.get().clear();
		}
		if (mAttributeValueLog.isPresent()) {
			mAttributeValueLog.get().clear();
		}
		if (mNodeLog.isPresent()) {
			mNodeLog.get().clear();
		}
		if (mPageLog.isPresent()) {
			mPageLog.get().clear();
		}
	}

	@Override
	public void closeCaches() {
		assertNotClosed();
		if (mPathLog.isPresent()) {
			mPathLog.get().close();
		}
		if (mTextValueLog.isPresent()) {
			mTextValueLog.get().close();
		}
		if (mAttributeValueLog.isPresent()) {
			mAttributeValueLog.get().close();
		}
		if (mNodeLog.isPresent()) {
			mNodeLog.get().close();
		}
		if (mPageLog.isPresent()) {
			mPageLog.get().close();
		}
	}

	/**
	 * Get revision root page belonging to revision key.
	 * 
	 * @param revisionKey
	 *          key of revision to find revision root page for
	 * @return revision root page of this revision key
	 * 
	 * @throws SirixIOException
	 *           if something odd happens within the creation process
	 */
	final RevisionRootPage loadRevRoot(final @Nonnegative int revisionKey)
			throws SirixIOException {
		checkArgument(
				revisionKey >= 0 && revisionKey <= mSession.getLastRevisionNumber(),
				"%s must be >= 0 and <= last stored revision (%s)!", revisionKey,
				mSession.getLastRevisionNumber());

		// The indirect page reference either fails horribly or returns a non null
		// instance.
		final PageReference ref = getPageReferenceForPage(
				mUberPage.getIndirectPageReference(), revisionKey, PageKind.UBERPAGE);
		RevisionRootPage page = (RevisionRootPage) ref.getPage();

		// If there is no page, get it from the storage and cache it.
		if (page == null) {
			try {
				page = (RevisionRootPage) mPageCache.get(ref.getKey());
			} catch (final ExecutionException e) {
				throw new SirixIOException(e.getCause());
			}
		}

		// Get revision root page which is the leaf of the indirect tree.
		return page;
	}

	@Override
	public final NamePage getNamePage(final @Nonnull RevisionRootPage revisionRoot) throws SirixIOException {
		assertNotClosed();
		final PageReference ref = revisionRoot.getNamePageReference();
		setPage(ref);
		ref.setPageKind(PageKind.NAMEPAGE);
		return (NamePage) ref.getPage();
	}

	@Override
	public final PathSummaryPage getPathSummaryPage(final @Nonnull RevisionRootPage revisionRoot) throws SirixIOException {
		assertNotClosed();
		final PageReference ref = revisionRoot.getPathSummaryPageReference();
		setPage(ref);
		ref.setPageKind(PageKind.PATHSUMMARYPAGE);
		return (PathSummaryPage) ref.getPage();
	}

	@Override
	public final TextValuePage getTextValuePage(final @Nonnull RevisionRootPage revisionRoot) throws SirixIOException {
		assertNotClosed();
		checkNotNull(revisionRoot);
		final PageReference ref = revisionRoot.getTextValuePageReference();
		setPage(ref);
		ref.setPageKind(PageKind.TEXTVALUEPAGE);
		return (TextValuePage) ref.getPage();
	}

	@Override
	public final AttributeValuePage getAttributeValuePage(final @Nonnull RevisionRootPage revisionRoot) throws SirixIOException {
		assertNotClosed();
		final PageReference ref = revisionRoot.getAttributeValuePageReference();
		setPage(ref);
		ref.setPageKind(PageKind.ATTRIBUTEVALUEPAGE);
		return (AttributeValuePage) ref.getPage();
	}

	/**
	 * Set the page if it is not set already.
	 * 
	 * @param ref
	 *          page reference
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void setPage(final @Nonnull PageReference ref)
			throws SirixIOException {
		if (ref.getPage() == null) {
			try {
				ref.setPage(mPageCache.get(ref.getKey()));
			} catch (final ExecutionException e) {
				throw new SirixIOException(e);
			}
		}
	}

	@Override
	public final UberPage getUberPage() {
		assertNotClosed();
		return mUberPage;
	}
	
	// TODO: Write another interface for this internal kind of stuff.
	@Override
	public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> RecordPageContainer<S> getRecordPageContainer(
			final @Nonnull @Nonnegative Long recordPageKey,
			final @Nonnull PageKind pageKind) throws SirixIOException {
		assertNotClosed();
		checkArgument(recordPageKey >= 0, "recordPageKey must not be negative!");
		final List<S> revs = (List<S>) this.<K, V, S>getSnapshotPages(
				checkNotNull(recordPageKey), checkNotNull(pageKind));
		if (revs.size() == 0) {
			return RecordPageContainer.<S>emptyInstance();
		}

		final int mileStoneRevision = mResourceConfig.mRevisionsToRestore;
		final Revisioning revisioning = mResourceConfig.mRevisionKind;
		final S completePage = revisioning.combineRecordPages(revs,
				mileStoneRevision, this);
		return new RecordPageContainer<S>(completePage);
	}

	/**
	 * Dereference key/value page reference and get all leaves, the
	 * {@link KeyValuePage}s from the revision-trees.
	 * 
	 * @param recordPageKey
	 *          key of node page
	 * @param pageKind
	 *          kind of page, that is the type of tree to dereference
	 * @return dereferenced pages
	 * 
	 * @throws SirixIOException
	 *           if an I/O-error occurs within the creation process
	 */
	final <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> List<S> getSnapshotPages(
			final @Nonnegative long recordPageKey, final @Nonnull PageKind pageKind)
			throws SirixIOException {
		assert recordPageKey >= 0;
		assert pageKind != null;
		final ResourceConfiguration config = mSession.getResourceConfig();
		final int revsToRestore = config.mRevisionsToRestore;
		final List<S> pages = new ArrayList<>(revsToRestore);
		final Set<Long> keys = new HashSet<>(revsToRestore);
		for (int i = mRootPage.getRevision(); i >= 0; i--) {
			final PageReference tmpRef = getPageReference(loadRevRoot(i), pageKind);
			final PageReference ref = getPageReferenceForPage(tmpRef, recordPageKey,
					pageKind);
			if (ref != null
					&& (ref.getPage() != null || ref.getKey() != Constants.NULL_ID)) {
				// Probably save page.
				if (ref.getKey() == Constants.NULL_ID || (!keys.contains(ref.getKey()))) {
					@SuppressWarnings("unchecked")
					final S page = (S) (ref.getPage() == null ? mPageReader.read(
							ref.getKey(), this) : ref.getPage());
					ref.setPageKind(pageKind);
					pages.add(page);
					if (ref.getKey() != Constants.NULL_ID) {
						keys.add(ref.getKey());
					}
					if (page.entrySet().size() == Constants.NDP_NODE_COUNT) {
						// Page is full, thus we can skip reconstructing pages with elder
						// versions.
						break;
					}
				}
				if (pages.size() == revsToRestore
						|| config.mRevisionKind == Revisioning.FULL
						|| (config.mRevisionKind == Revisioning.DIFFERENTIAL && pages
								.size() == 2)) {
					break;
				}
				if (config.mRevisionKind == Revisioning.DIFFERENTIAL) {
					if (i - revsToRestore >= 0) {
						i = i - revsToRestore + 1;
					} else if (i == 0) {
						break;
					} else {
						i = 1;
					}
				}
			} else {
				break;
			}
		}

		return pages;
	}

	/**
	 * Get the page reference which points to the right subtree (usual nodes, path
	 * summary nodes, value index nodes).
	 * 
	 * @param revisionRoot
	 *          {@link RevisionRootPage} instance
	 * @param pPage
	 *          the page type to determine the right subtree
	 */
	PageReference getPageReference(final @Nonnull RevisionRootPage revisionRoot,
			final @Nonnull PageKind pPage) throws SirixIOException {
		assert revisionRoot != null;
		PageReference ref = null;
		switch (pPage) {
		case NODEPAGE:
			ref = revisionRoot.getIndirectPageReference();
			break;
		case TEXTVALUEPAGE:
			ref = getTextValuePage(revisionRoot).getIndirectPageReference();
			break;
		case ATTRIBUTEVALUEPAGE:
			ref = getAttributeValuePage(revisionRoot).getIndirectPageReference();
			break;
		case PATHSUMMARYPAGE:
			ref = getPathSummaryPage(revisionRoot).getIndirectPageReference();
			break;
		default:
			throw new IllegalStateException(
					"Only defined for node, path summary, text value and attribute value pages!");
		}
		return ref;
	}

	/**
	 * Dereference indirect page reference.
	 * 
	 * @param reference
	 *          reference to dereference
	 * @return dereferenced page
	 * 
	 * @throws SirixIOException
	 *           if something odd happens within the creation process.
	 */
	final IndirectPage dereferenceIndirectPage(
			final @Nonnull PageReference reference) throws SirixIOException {
		final Page tmpPage = reference.getPage();
		if (tmpPage == null || tmpPage instanceof IndirectPage) {
			IndirectPage page = (IndirectPage) tmpPage;

			// If there is no page, get it from the storage and cache it.
			if (page == null && reference.getKey() != Constants.NULL_ID) {
				try {
					page = (IndirectPage) mPageCache.get(reference.getKey());
				} catch (final ExecutionException e) {
					throw new SirixIOException(e);
				}
			}

			reference.setPage(page);
			return page;
		} else {
			throw new IllegalArgumentException(
					"Must be a reference to an indirect page!");
		}
	}

	/**
	 * Find reference pointing to leaf page of an indirect tree.
	 * 
	 * @param startReference
	 *          start reference pointing to the indirect tree
	 * @param key
	 *          key to look up in the indirect tree
	 * @return reference denoted by key pointing to the leaf page
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	@Nullable
	@Override
	public final PageReference getPageReferenceForPage(
			final @Nonnull PageReference startReference, final @Nonnegative long key,
			final @Nonnull PageKind pageKind) throws SirixIOException {

		// Initial state pointing to the indirect page of level 0.
		PageReference reference = checkNotNull(startReference);
		int offset = 0;
		long levelKey = key;
		final int[] inpLevelPageCountExp = mUberPage.getPageCountExp(pageKind);

		// Iterate through all levels.
		for (int level = 0, height = inpLevelPageCountExp.length; level < height; level++) {
			offset = (int) (levelKey >> inpLevelPageCountExp[level]);
			levelKey -= offset << inpLevelPageCountExp[level];
			final Page derefPage = dereferenceIndirectPage(reference);
			if (derefPage == null) {
				reference = null;
				break;
			} else {
				try {
					reference = derefPage.getReference(offset);
				} catch (final IndexOutOfBoundsException e) {
					throw new SirixIOException("Node key isn't supported, it's too big!");
				}
			}
		}

		// Return reference to leaf of indirect tree.
		if (reference != null) {
			reference.setPageKind(pageKind);
		}
		return reference;
	}

	@Override
	public long pageKey(final @Nonnegative long recordKey) {
		assertNotClosed();
		checkArgument(recordKey >= 0, "recordKey must not be negative!");
		return recordKey >> Constants.NDP_NODE_COUNT_EXPONENT;
	}

	@Override
	public RevisionRootPage getActualRevisionRootPage() throws SirixIOException {
		assertNotClosed();
		return mRootPage;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("Session", mSession)
				.add("PageReader", mPageReader).add("UberPage", mUberPage)
				.add("RevRootPage", mRootPage).toString();
	}

	@Override
	public void close() throws SirixIOException {
		if (!mClosed) {
			closeCaches();
			mPageReader.close();

			mClosed = true;
		}
	}

	@Override
	public int getNameCount(int key, @Nonnull Kind kind) {
		assertNotClosed();
		return mNamePage.getCount(key, kind);
	}

	@Override
	public boolean isClosed() {
		return mClosed;
	}

	@Override
	public int getRevisionNumber() {
		assertNotClosed();
		return mRootPage.getRevision();
	}

	@Override
	public Page getFromPageCache(final @Nonnegative long key)
			throws SirixIOException {
		assertNotClosed();
		return mPageCache.getIfPresent(key);
	}

	@Override
	public void putPageCache(final @Nonnull TransactionLogPageCache pageLog) {
		assertNotClosed();
		pageLog.putAll(mPageCache.asMap());
	}
}