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
import org.sirix.access.conf.ResourceConfiguration.EIndexes;
import org.sirix.api.IPageReadTrx;
import org.sirix.api.ISession;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionLogCache;
import org.sirix.cache.TransactionLogPageCache;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.io.IReader;
import org.sirix.node.DeletedNode;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.page.EPage;
import org.sirix.page.IndirectPage;
import org.sirix.page.NamePage;
import org.sirix.page.NodePage;
import org.sirix.page.PageReference;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.ValuePage;
import org.sirix.page.interfaces.IPage;
import org.sirix.settings.ERevisioning;
import org.sirix.settings.IConstants;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * <h1>PageReadTransaction</h1>
 * 
 * <p>
 * Page reading transaction. The only thing shared amongst transactions is the
 * page cache. Everything else is exclusive to this transaction. It is required
 * that only a single thread has access to this transaction.
 * </p>
 */
final class PageReadTrx implements IPageReadTrx {

	/** Page reader exclusively assigned to this transaction. */
	private final IReader mPageReader;

	/** Uber page this transaction is bound to. */
	private final UberPage mUberPage;

	/** Cached name page of this revision. */
	private final RevisionRootPage mRootPage;

	/** Internal reference to node cache. */
	private final LoadingCache<Long, PageContainer> mNodeCache;

	/** Internal reference to path cache. */
	private final LoadingCache<Long, PageContainer> mPathCache;

	/** Internal reference to value cache. */
	private final LoadingCache<Long, PageContainer> mValueCache;

	/** Internal reference to page cache. */
	private final LoadingCache<Long, IPage> mPageCache;

	/** {@link Session} reference. */
	protected final Session mSession;

	/** {@link NamePage} reference. */
	private final NamePage mNamePage;

	/** Determines if page reading transaction is closed or not. */
	private boolean mClosed;

	/** Indexes to read. */
	private final Set<EIndexes> mIndexes;

	/**
	 * Optional page transaction log, dependent on the fact, if the log hasn't
	 * been completely transferred into the data file.
	 */
	private final Optional<TransactionLogPageCache> mPageLog;

	/**
	 * Optional path transaction log, dependent on the fact, if the log hasn't
	 * been completely transferred into the data file.
	 */
	private final Optional<TransactionLogCache> mPathLog;

	/**
	 * Optional value transaction log, dependent on the fact, if the log hasn't
	 * been completely transferred into the data file.
	 */
	private final Optional<TransactionLogCache> mValueLog;

	/**
	 * Optional node transaction log, dependent on the fact, if the log hasn't
	 * been completely transferred into the data file.
	 */
	private final Optional<TransactionLogCache> mNodeLog;

	/**
	 * Standard constructor.
	 * 
	 * @param pSession
	 *          current {@link Session} instance
	 * @param pUberPage
	 *          {@link UberPage} to start reading from
	 * @param pRevision
	 *          key of revision to read from uber page
	 * @param pReader
	 *          reader to read stored pages for this transaction
	 * @param pPersistentCache
	 *          optional persistent cache
	 * @throws SirixIOException
	 *           if reading of the persistent storage fails
	 */
	PageReadTrx(final @Nonnull Session pSession,
			final @Nonnull UberPage pUberPage, @Nonnegative final int pRevision,
			final @Nonnull IReader pReader,
			final @Nonnull Optional<TransactionLogPageCache> pPersistentCache)
			throws SirixIOException {
		checkArgument(pRevision >= 0, "Revision must be >= 0!");
		mIndexes = pSession.mResourceConfig.mIndexes;

		// Transaction logs which might have to be read because the data hasn't been
		// commited to the data-file.
		// =======================================================
		// final boolean isCreated = pPersistentCache.isPresent()
		// && !pPersistentCache.get().isCreated();
		// mPageLog = !isCreated ? pPersistentCache : Optional
		// .<TransactionLogPageCache> absent();
		// mNodeLog = !isCreated ? Optional.of(new TransactionLogCache(
		// pSession.mResourceConfig.mPath, pRevision, "node")) : Optional
		// .<TransactionLogCache> absent();
		// if (mIndexes.contains(EIndexes.PATH)) {
		// mPathLog = !isCreated ? Optional.of(new TransactionLogCache(
		// pSession.mResourceConfig.mPath, pRevision, "path")) : Optional
		// .<TransactionLogCache> absent();
		// } else {
		// mPathLog = Optional.<TransactionLogCache> absent();
		// }
		// if (mIndexes.contains(EIndexes.VALUE)) {
		// mValueLog = !isCreated ? Optional.of(new TransactionLogCache(
		// pSession.mResourceConfig.mPath, pRevision, "value")) : Optional
		// .<TransactionLogCache> absent();
		// } else {
		// mValueLog = Optional.<TransactionLogCache> absent();
		// }
		mPageLog = Optional.<TransactionLogPageCache> absent();
		mNodeLog = Optional.<TransactionLogCache> absent();
		mPathLog = Optional.<TransactionLogCache> absent();
		mValueLog = Optional.<TransactionLogCache> absent();

		// In memory caches from data directory.
		// =========================================================
		mNodeCache = CacheBuilder.newBuilder().maximumSize(1000)
				.expireAfterWrite(5, TimeUnit.SECONDS)
				.expireAfterAccess(5, TimeUnit.SECONDS).concurrencyLevel(1)
				.build(new CacheLoader<Long, PageContainer>() {
					public PageContainer load(final Long pKey) throws SirixException {
						final PageContainer container = mNodeLog.isPresent() ? mNodeLog
								.get().get(pKey) : PageContainer.EMPTY_INSTANCE;
						if (container.equals(PageContainer.EMPTY_INSTANCE)) {
							return getNodeFromPage(pKey, EPage.NODEPAGE);
						} else {
							return container;
						}
					}
				});
		final CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder()
				.concurrencyLevel(1).maximumSize(20);
		if (mIndexes.contains(EIndexes.PATH)) {
			mPathCache = builder.build(new CacheLoader<Long, PageContainer>() {
				public PageContainer load(final Long pKey) throws SirixException {
					final PageContainer container = mPathLog.isPresent() ? mPathLog.get()
							.get(pKey) : PageContainer.EMPTY_INSTANCE;
					if (container.equals(PageContainer.EMPTY_INSTANCE)) {
						return getNodeFromPage(pKey, EPage.PATHSUMMARYPAGE);
					} else {
						return container;
					}
				}
			});
		} else {
			mPathCache = null;
		}
		if (mIndexes.contains(EIndexes.VALUE)) {
			mValueCache = builder.build(new CacheLoader<Long, PageContainer>() {
				public PageContainer load(final Long pKey) throws SirixException {
					final PageContainer container = mValueLog.isPresent() ? mValueLog
							.get().get(pKey) : null;
					if (container.equals(PageContainer.EMPTY_INSTANCE)) {
						return getNodeFromPage(pKey, EPage.VALUEPAGE);
					} else {
						return container;
					}
				}
			});
		} else {
			mValueCache = null;
		}

		final CacheBuilder<Object, Object> pageCacheBuilder = CacheBuilder
				.newBuilder();
		if (pPersistentCache.isPresent()) {
			pageCacheBuilder.removalListener(new RemovalListener<Long, IPage>() {
				@Override
				public void onRemoval(final RemovalNotification<Long, IPage> pRemoval) {
					// final IPage page = pRemoval.getValue();
					// if (page.isDirty()) {
					// pPersistentCache.get().put(pRemoval.getKey(), page);
					// }
				}
			});
		}

		mPageCache = pageCacheBuilder.build(new CacheLoader<Long, IPage>() {
			public IPage load(final Long pKey) throws SirixException {
				final IPage page = mPageLog.isPresent() ? mPageLog.get().get(pKey)
						: null;
				if (page == null) {
					return mPageReader.read(pKey).setDirty(true);
				} else {
					return page;
				}
			}
		});
		mSession = checkNotNull(pSession);
		mPageReader = checkNotNull(pReader);
		mUberPage = checkNotNull(pUberPage);
		mRootPage = loadRevRoot(pRevision);
		assert mRootPage != null : "root page must not be null!";
		mNamePage = getNamePage();
		final PageReference ref = mRootPage.getPathSummaryPageReference();
		if (ref.getPage() == null) {
			try {
				ref.setPage(mPageCache.get(ref.getKey()));
			} catch (final ExecutionException e) {
				throw new SirixIOException(e);
			}
		}
		mClosed = false;
	}

	@Override
	public ISession getSession() {
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
	public Optional<INodeBase> getNode(@Nonnegative final long pNodeKey,
			final @Nonnull EPage pPage) throws SirixIOException {
		checkArgument(pNodeKey >= 0);
		checkNotNull(pPage);
		assertNotClosed();

		final long nodePageKey = nodePageKey(pNodeKey);
		// final int nodePageOffset = nodePageOffset(pNodeKey);

		PageContainer cont;
		try {
			switch (pPage) {
			case NODEPAGE:
				cont = mNodeCache.get(nodePageKey);
				break;
			case PATHSUMMARYPAGE:
				cont = mPathCache.get(nodePageKey);
				break;
			case VALUEPAGE:
				cont = mValueCache.get(nodePageKey);
				break;
			default:
				throw new IllegalStateException();
			}
		} catch (final ExecutionException e) {
			throw new SirixIOException(e);
		}

		if (cont.equals(PageContainer.EMPTY_INSTANCE)) {
			return Optional.<INodeBase> absent();
		}

		final INodeBase retVal = cont.getComplete().getNode(pNodeKey);
		return Optional.fromNullable(checkItemIfDeleted(retVal));
	}

	/**
	 * Method to check if an {@link INodeBase} is deleted.
	 * 
	 * @param pToCheck
	 *          node to check
	 * @return the {@code node} if it is valid, {@code null} otherwise
	 */
	final INodeBase checkItemIfDeleted(final @Nullable INodeBase pToCheck) {
		if (pToCheck instanceof DeletedNode) {
			return null;
		} else {
			return pToCheck;
		}
	}

	@Override
	public String getName(final int pNameKey, final @Nonnull EKind pNodeKind) {
		assertNotClosed();
		return mNamePage.getName(pNameKey, pNodeKind);
	}

	@Override
	public final byte[] getRawName(final int pNameKey,
			final @Nonnull EKind pNodeKind) {
		assertNotClosed();
		return mNamePage.getRawName(pNameKey, pNodeKind);
	}

	/**
	 * Clear the caches.
	 */
	void clearCaches() {
		assertNotClosed();

		if (mIndexes.contains(EIndexes.PATH)) {
			mPathCache.invalidateAll();
		}
		if (mIndexes.contains(EIndexes.VALUE)) {
			mValueCache.invalidateAll();
		}
		mNodeCache.invalidateAll();
		mPageCache.invalidateAll();
	}

	/**
	 * Close caches.
	 */
	void closeCaches() {
		if (mPathLog.isPresent()) {
			mPathLog.get().close();
		}
		if (mValueLog.isPresent()) {
			mValueLog.get().close();
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
	 * @param pRevisionKey
	 *          key of revision to find revision root page for
	 * @return revision root page of this revision key
	 * 
	 * @throws SirixIOException
	 *           if something odd happens within the creation process
	 */
	final RevisionRootPage loadRevRoot(final @Nonnegative int pRevisionKey)
			throws SirixIOException {
		checkArgument(
				pRevisionKey >= 0 && pRevisionKey <= mSession.getLastRevisionNumber(),
				"%s must be >= 0 and <= last stored revision (%s)!", pRevisionKey,
				mSession.getLastRevisionNumber());
		assertNotClosed();

		// The indirect page reference either fails horribly or returns a non null
		// instance.
		final PageReference ref = dereferenceLeafOfTree(
				mUberPage.getIndirectPageReference(), pRevisionKey, EPage.UBERPAGE);
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

	/**
	 * Initialize NamePage.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private final NamePage getNamePage() throws SirixIOException {
		assertNotClosed();
		final PageReference ref = mRootPage.getNamePageReference();
		if (ref.getPage() == null) {
			try {
				ref.setPage(mPageCache.get(ref.getKey()));
			} catch (final ExecutionException e) {
				throw new SirixIOException(e);
			}
		}
		ref.setPageKind(EPage.NAMEPAGE);
		return (NamePage) ref.getPage();
	}

	/**
	 * Initialize PathSummaryPage.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private final PathSummaryPage getPathSummaryPage(
			final @Nonnull RevisionRootPage pPage) throws SirixIOException {
		assertNotClosed();
		final PageReference ref = pPage.getPathSummaryPageReference();
		if (ref.getPage() == null) {
			try {
				ref.setPage(mPageCache.get(ref.getKey()));
			} catch (final ExecutionException e) {
				throw new SirixIOException(e);
			}
		}
		ref.setPageKind(EPage.PATHSUMMARYPAGE);
		return (PathSummaryPage) ref.getPage();
	}

	/**
	 * Initialize ValuePage.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private final ValuePage getValuePage(final @Nonnull RevisionRootPage pPage)
			throws SirixIOException {
		assertNotClosed();
		final PageReference ref = pPage.getValuePageReference();
		if (ref.getPage() == null) {
			try {
				ref.setPage(mPageCache.get(ref.getKey()));
			} catch (final ExecutionException e) {
				throw new SirixIOException(e);
			}
		}
		ref.setPageKind(EPage.VALUEPAGE);
		return (ValuePage) ref.getPage();
	}

	@Override
	public final UberPage getUberPage() {
		return mUberPage;
	}

	/**
	 * Dereference node page reference and get all leaves, the {@link NodePage}s
	 * from the revision-trees.
	 * 
	 * @param pNodePageKey
	 *          key of node page
	 * @return dereferenced pages
	 * 
	 * @throws SirixIOException
	 *           if an I/O-error occurs within the creation process
	 */
	final NodePage[] getSnapshotPages(final @Nonnegative long pNodePageKey,
			final @Nonnull EPage pPage) throws SirixIOException {
		checkNotNull(pPage);
		assertNotClosed();
		final List<PageReference> refs = new ArrayList<>();
		final Set<Long> keys = new HashSet<>();
		final ResourceConfiguration config = mSession.getResourceConfig();
		final int revsToRestore = config.mRevisionsToRestore;
		for (int i = mRootPage.getRevision(); i >= 0; i--) {
			final PageReference tmpRef = getPageReference(loadRevRoot(i), pPage);
			final PageReference ref = dereferenceLeafOfTree(tmpRef, pNodePageKey,
					pPage);
			if (ref != null
					&& (ref.getPage() != null || ref.getKey() != IConstants.NULL_ID)) {
				if (ref.getKey() == IConstants.NULL_ID
						|| (!keys.contains(ref.getKey()))) {
					refs.add(ref);
					if (ref.getKey() != IConstants.NULL_ID) {
						keys.add(ref.getKey());
					}
				}
				if (refs.size() == revsToRestore
						|| config.mRevisionKind == ERevisioning.FULL
						|| (config.mRevisionKind == ERevisioning.DIFFERENTIAL && refs
								.size() == 2)) {
					break;
				}
				if (config.mRevisionKind == ERevisioning.DIFFERENTIAL) {
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

		// Afterwards read the NodePages if they are not dereferences...
		final NodePage[] pages = new NodePage[refs.size()];
		for (int i = 0; i < pages.length; i++) {
			final PageReference ref = refs.get(i);
			pages[i] = (NodePage) ref.getPage();
			if (pages[i] == null) {
				pages[i] = (NodePage) mPageReader.read(ref.getKey());
			}
			ref.setPageKind(pPage);
		}
		return pages;
	}

	/**
	 * Get the page reference which points to the right subtree (usual nodes, path
	 * summary nodes, value index nodes).
	 * 
	 * @param pRef
	 *          {@link RevisionRootPage} instance
	 * @param pPage
	 *          the page type to determine the right subtree
	 */
	PageReference getPageReference(final @Nonnull RevisionRootPage pRef,
			final @Nonnull EPage pPage) throws SirixIOException {
		assert pRef != null;
		PageReference ref = null;
		switch (pPage) {
		case NODEPAGE:
			ref = pRef.getIndirectPageReference();
			break;
		case VALUEPAGE:
			ref = getValuePage(pRef).getIndirectPageReference();
			break;
		case PATHSUMMARYPAGE:
			ref = getPathSummaryPage(pRef).getIndirectPageReference();
			break;
		default:
			new IllegalStateException(
					"Only defined for node pages and path summary pages!");
		}
		return ref;
	}

	/**
	 * Dereference indirect page reference.
	 * 
	 * @param pReference
	 *          reference to dereference
	 * @return dereferenced page
	 * 
	 * @throws SirixIOException
	 *           if something odd happens within the creation process.
	 */
	final IndirectPage dereferenceIndirectPage(
			final @Nonnull PageReference pReference) throws SirixIOException {
		final IPage tmpPage = pReference.getPage();
		if (tmpPage == null || tmpPage instanceof IndirectPage) {
			IndirectPage page = (IndirectPage) tmpPage;

			// If there is no page, get it from the storage and cache it.
			if (page == null && pReference.getKey() != IConstants.NULL_ID) {
				try {
					page = (IndirectPage) mPageCache.get(pReference.getKey());
				} catch (final ExecutionException e) {
					throw new SirixIOException(e);
				}
			}

			pReference.setPage(page);
			return page;
		} else {
			throw new IllegalArgumentException(
					"Must be a reference to an indirect page!");
		}
	}

	/**
	 * Find reference pointing to leaf page of an indirect tree.
	 * 
	 * @param pStartReference
	 *          start reference pointing to the indirect tree
	 * @param pKey
	 *          key to look up in the indirect tree
	 * @return reference denoted by key pointing to the leaf page
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	@Nullable
	final PageReference dereferenceLeafOfTree(
			final @Nonnull PageReference pStartReference,
			final @Nonnegative long pKey, final @Nonnull EPage pPage)
			throws SirixIOException {

		// Initial state pointing to the indirect page of level 0.
		PageReference reference = checkNotNull(pStartReference);
		int offset = 0;
		long levelKey = pKey;
		final int[] inpLevelPageCountExp = mUberPage.getPageCountExp(pPage);

		// Iterate through all levels.
		for (int level = 0, height = inpLevelPageCountExp.length; level < height; level++) {
			offset = (int) (levelKey >> inpLevelPageCountExp[level]);
			levelKey -= offset << inpLevelPageCountExp[level];
			final IPage page = dereferenceIndirectPage(reference);
			if (page == null) {
				reference = null;
				break;
			} else {
				try {
					reference = page.getReference(offset);
				} catch (final IndexOutOfBoundsException e) {
					throw new SirixIOException("Node key isn't supported, it's too big!");
				}
			}
		}

		// Return reference to leaf of indirect tree.
		if (reference != null) {
			reference.setPageKind(pPage);
		}
		return reference;
	}

	/**
	 * Calculate node page key from a given node key.
	 * 
	 * @param pNodeKey
	 *          node key to find node page key for
	 * @return node page key
	 */
	final long nodePageKey(@Nonnegative final long pNodeKey) {
		checkArgument(pNodeKey >= 0, "pNodeKey must not be negative!");
		return pNodeKey >> IConstants.NDP_NODE_COUNT_EXPONENT;
	}

	// /**
	// * Calculate node page offset for a given node key.
	// *
	// * @param pNodeKey
	// * node key to find offset for
	// * @return offset into node page
	// */
	// final int nodePageOffset(@Nonnegative final long pNodeKey) {
	// checkArgument(pNodeKey >= 0, "pNodeKey must not be negative!");
	// final long shift =
	// ((pNodeKey >> IConstants.NDP_NODE_COUNT_EXPONENT) <<
	// IConstants.NDP_NODE_COUNT_EXPONENT);
	// return (int)(pNodeKey - shift);
	// }

	@Override
	public RevisionRootPage getActualRevisionRootPage() throws SirixIOException {
		return mRootPage;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("Session: ", mSession)
				.add("PageReader: ", mPageReader).add("UberPage: ", mUberPage)
				.add("RevRootPage: ", mRootPage).toString();
	}

	@Override
	public PageContainer getNodeFromPage(final @Nonnegative long pNodePageKey,
			final @Nonnull EPage pPage) throws SirixIOException {
		final NodePage[] revs = getSnapshotPages(pNodePageKey, pPage);
		if (revs.length == 0) {
			return PageContainer.EMPTY_INSTANCE;
		}

		final int mileStoneRevision = mSession.getResourceConfig().mRevisionsToRestore;
		final ERevisioning revisioning = mSession.getResourceConfig().mRevisionKind;
		final NodePage completePage = revisioning.combineNodePages(revs,
				mileStoneRevision);
		return new PageContainer(completePage);
	}

	@Override
	public void close() throws SirixIOException {
		if (!mClosed) {
			closeCaches();
			mClosed = true;
			mPageReader.close();
		}
	}

	@Override
	public int getNameCount(int pKey, @Nonnull EKind pKind) {
		return mNamePage.getCount(pKey, pKind);
	}

	@Override
	public boolean isClosed() {
		return mClosed;
	}

	@Override
	public int getRevisionNumber() {
		return mRootPage.getRevision();
	}

	@Override
	public IPage getFromPageCache(final @Nonnegative long pKey)
			throws SirixIOException {
		IPage retVal = null;
		try {
			retVal = mPageCache.get(pKey);
		} catch (ExecutionException e) {
			throw new SirixIOException(e.getCause());
		}
		return retVal;
	}

	@Override
	public void putPageCache(final @Nonnull TransactionLogPageCache pPageLog) {
		pPageLog.putAll(mPageCache.asMap());
	}
}