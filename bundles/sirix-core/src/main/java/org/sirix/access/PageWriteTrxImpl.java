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

import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.access.conf.ResourceConfiguration.EIndexes;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.cache.Cache;
import org.sirix.cache.NodePageContainer;
import org.sirix.cache.TransactionLogCache;
import org.sirix.cache.TransactionLogPageCache;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Writer;
import org.sirix.node.DeletedNode;
import org.sirix.node.Kind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.NodeBase;
import org.sirix.page.IndirectPage;
import org.sirix.page.NamePage;
import org.sirix.page.NodePage;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.settings.Revisioning;
import org.sirix.utils.NamePageHash;

import com.google.common.base.Optional;

/**
 * <h1>PageWriteTrx</h1>
 * 
 * <p>
 * Implements the {@link PageWriteTrx} interface to provide write capabilities
 * to the persistent storage layer.
 * </p>
 */
final class PageWriteTrxImpl extends AbstractForwardingPageReadTrx implements
		PageWriteTrx {

	/** Page writer to serialize. */
	private final Writer mPageWriter;

	/** Persistent BerkeleyDB page log for all page types != NodePage. */
	private final Cache<Long, Page> mPageLog;

	/** Cache to store the changes in this transaction log. */
	private final Cache<Long, NodePageContainer> mNodeLog;

	/** Cache to store path changes in this transaction log. */
	private final Cache<Long, NodePageContainer> mPathLog;

	/** Cache to store value changes in this transaction log. */
	private final Cache<Long, NodePageContainer> mValueLog;

	/** Last references to the Nodepage, needed for pre/postcondition check. */
	private NodePageContainer mNodePageCon;

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

	private Set<EIndexes> mIndexes;

	/**
	 * Standard constructor.
	 * 
	 * @param session
	 *          {@link ISessionConfiguration} this page write trx is bound to
	 * @param uberPage
	 *          root of revision
	 * @param writer
	 *          writer where this transaction should write to
	 * @param id
	 *          ID
	 * @param representRev
	 *          revision represent
	 * @param lastStoredRev
	 *          last store revision
	 * @throws AbsTTException
	 *           if an error occurs
	 */
	PageWriteTrxImpl(final @Nonnull SessionImpl session,
			final @Nonnull UberPage uberPage, final @Nonnull Writer writer,
			final @Nonnegative long id, final @Nonnegative int representRev,
			final @Nonnegative int lastStoredRev,
			final @Nonnegative int pLastCommitedRev) throws SirixException {
		// Page read trx.
		mPageRtx = new PageReadTrxImpl(session, uberPage, representRev, writer);

		final int revision = uberPage.isBootstrap() ? 0 : representRev + 1;
		mIndexes = session.mResourceConfig.mIndexes;
		mPageLog = new TransactionLogPageCache(session.mResourceConfig.mPath,
				revision, "page");
		mNodeLog = new TransactionLogCache(session.mResourceConfig.mPath, revision,
				"node");
		if (mIndexes.contains(EIndexes.PATH)) {
			mPathLog = new TransactionLogCache(session.mResourceConfig.mPath,
					revision, "path");
		} else {
			mPathLog = null;
		}
		if (mIndexes.contains(EIndexes.VALUE)) {
			mValueLog = new TransactionLogCache(session.mResourceConfig.mPath,
					revision, "value");
		} else {
			mValueLog = null;
		}
		mPageWriter = writer;
		mTransactionID = id;

		final RevisionRootPage lastCommitedRoot = preparePreviousRevisionRootPage(
				representRev, pLastCommitedRev);
		mNewRoot = preparePreviousRevisionRootPage(representRev, lastStoredRev);
		mNewRoot.setMaxNodeKey(lastCommitedRoot.getMaxNodeKey());

		final Set<EIndexes> indexes = session.getResourceConfig().mIndexes;
		if (indexes.contains(EIndexes.PATH)) {
			mNewRoot.setMaxPathNodeKey(lastCommitedRoot.getMaxPathNodeKey());
		}
		if (indexes.contains(EIndexes.VALUE)) {
			mNewRoot.setMaxValueNodeKey(lastCommitedRoot.getMaxValueNodeKey());
		}
	}

	@Override
	public void restore(final @Nonnull Restore pRestore) {
		mRestore = checkNotNull(pRestore);
	}

	@Override
	public NodeBase prepareNodeForModification(final @Nonnegative long pNodeKey,
			final @Nonnull PageKind pPage) throws SirixIOException {
		if (pNodeKey < 0) {
			throw new IllegalArgumentException("pNodeKey must be >= 0!");
		}
		if (mNodePageCon != null) {
			throw new IllegalStateException(
					"Another node page container is currently in the cache for updates!");
		}

		final long nodePageKey = mPageRtx.nodePageKey(pNodeKey);
		// final int nodePageOffset = mPageRtx.nodePageOffset(pNodeKey);
		prepareNodePage(nodePageKey, pPage);

		NodeBase node = mNodePageCon.getModified().getNode(pNodeKey);
		if (node == null) {
			final NodeBase oldNode = mNodePageCon.getComplete().getNode(pNodeKey);
			if (oldNode == null) {
				throw new SirixIOException("Cannot retrieve node from cache!");
			}
			node = oldNode;
			mNodePageCon.getModified().setNode(node);
		}

		return node;
	}

	@Override
	public void finishNodeModification(final @Nonnull long pNodeKey,
			final @Nonnull PageKind pPage) {
		final long nodePageKey = mPageRtx.nodePageKey(pNodeKey);
		if (mNodePageCon == null
				|| (mNodeLog.get(nodePageKey).equals(NodePageContainer.EMPTY_INSTANCE)
						&& mPathLog.get(nodePageKey).equals(
								NodePageContainer.EMPTY_INSTANCE) && mValueLog.get(nodePageKey)
						.equals(NodePageContainer.EMPTY_INSTANCE))) {
			throw new IllegalStateException();
		}

		switch (pPage) {
		case NODEPAGE:
			mNodeLog.put(nodePageKey, mNodePageCon);
			break;
		case PATHSUMMARYPAGE:
			mPathLog.put(nodePageKey, mNodePageCon);
			break;
		case VALUEPAGE:
			mValueLog.put(nodePageKey, mNodePageCon);
			break;
		default:
			throw new IllegalStateException();
		}

		mNodePageCon = null;
	}

	@Override
	public NodeBase createNode(final @Nonnull NodeBase pNode,
			final @Nonnull PageKind pPage) throws SirixIOException {
		// Allocate node key and increment node count.
		long nodeKey;
		switch (pPage) {
		case NODEPAGE:
			mNewRoot.incrementMaxNodeKey();
			nodeKey = mNewRoot.getMaxNodeKey();
			break;
		case PATHSUMMARYPAGE:
			mNewRoot.incrementMaxPathNodeKey();
			nodeKey = mNewRoot.getMaxPathNodeKey();
			break;
		case VALUEPAGE:
			mNewRoot.incrementMaxValueNodeKey();
			nodeKey = mNewRoot.getMaxValueNodeKey();
			break;
		default:
			throw new IllegalStateException();
		}

		final long nodePageKey = mPageRtx.nodePageKey(nodeKey);
		// final int nodePageOffset = mPageRtx.nodePageOffset(nodeKey);
		prepareNodePage(nodePageKey, pPage);
		final NodePage page = mNodePageCon.getModified();
		page.setNode(pNode);
		finishNodeModification(pNode.getNodeKey(), pPage);
		return pNode;
	}

	@Override
	public void removeNode(@Nonnull final long pNodeKey,
			@Nonnull final PageKind pPage) throws SirixIOException {
		final long nodePageKey = mPageRtx.nodePageKey(pNodeKey);
		prepareNodePage(nodePageKey, pPage);
		final Optional<NodeBase> node = getNode(pNodeKey, pPage);
		if (node.isPresent()) {
			final NodeBase nodeToDel = node.get();
			final Node delNode = new DeletedNode(new NodeDelegate(
					nodeToDel.getNodeKey(), -1, -1, -1));
			mNodePageCon.getModified().setNode(delNode);
			mNodePageCon.getComplete().setNode(delNode);
			finishNodeModification(pNodeKey, pPage);
		} else {
			throw new IllegalStateException("Node not found!");
		}

	}

	@Override
	public Optional<NodeBase> getNode(final @Nonnegative long pNodeKey,
			final @Nonnull PageKind pPage) throws SirixIOException {
		checkArgument(pNodeKey >= Fixed.NULL_NODE_KEY.getStandardProperty());
		checkNotNull(pPage);
		// Calculate page.
		final long nodePageKey = mPageRtx.nodePageKey(pNodeKey);
		// final int nodePageOffset = mPageRtx.nodePageOffset(pNodeKey);

		final NodePageContainer pageCont = getPageContainer(pPage, nodePageKey);
		if (pageCont.equals(NodePageContainer.EMPTY_INSTANCE)) {
			return mPageRtx.getNode(pNodeKey, pPage);
		} else {
			NodeBase node = pageCont.getModified().getNode(pNodeKey);
			if (node == null) {
				node = pageCont.getComplete().getNode(pNodeKey);
			}
			return Optional.fromNullable(mPageRtx.checkItemIfDeleted(node));
		}
	}

	/**
	 * Get the page container.
	 * 
	 * @param pPage
	 *          the kind of page
	 * @param pNodePageKey
	 *          the node page key
	 * @return the {@link NodePageContainer} instance from the write ahead log
	 */
	private NodePageContainer getPageContainer(final @Nullable PageKind pPage,
			final @Nonnegative long pNodePageKey) {
		if (pPage != null) {
			switch (pPage) {
			case NODEPAGE:
				return mNodeLog.get(pNodePageKey);
			case PATHSUMMARYPAGE:
				return mPathLog.get(pNodePageKey);
			case VALUEPAGE:
				return mValueLog.get(pNodePageKey);
			default:
				throw new IllegalStateException();
			}
		}
		return NodePageContainer.EMPTY_INSTANCE;
	}

	/**
	 * Remove the page container.
	 * 
	 * @param pPage
	 *          the kind of page
	 * @param pNodePageKey
	 *          the node page key
	 */
	private void removePageContainer(final @Nonnull PageKind pPage,
			final @Nonnegative long pNodePageKey) {
		switch (pPage) {
		case NODEPAGE:
			mNodeLog.remove(pNodePageKey);
			break;
		case PATHSUMMARYPAGE:
			mPathLog.remove(pNodePageKey);
			break;
		case VALUEPAGE:
			mValueLog.remove(pNodePageKey);
		default:
			mPageLog.remove(pNodePageKey);
		}
	}

	@Override
	public String getName(final int pNameKey, final @Nonnull Kind pNodeKind) {
		final NamePage currentNamePage = (NamePage) mNewRoot.getNamePageReference()
				.getPage();
		// if currentNamePage == null -> state was commited and no prepareNodepage
		// was invoked yet
		return (currentNamePage == null || currentNamePage.getName(pNameKey,
				pNodeKind) == null) ? mPageRtx.getName(pNameKey, pNodeKind)
				: currentNamePage.getName(pNameKey, pNodeKind);
	}

	@Override
	public int createNameKey(final @Nullable String pName,
			final @Nonnull Kind pNodeKind) throws SirixIOException {
		checkNotNull(pNodeKind);
		final String string = (pName == null ? "" : pName);
		final int nameKey = NamePageHash.generateHashForString(string);
		final NamePage namePage = (NamePage) mNewRoot.getNamePageReference()
				.getPage();
		namePage.setName(nameKey, string, pNodeKind);
		return nameKey;
	}

	@Override
	public void commit(final @Nullable PageReference pReference)
			throws SirixException {
		Page page = null;

		// If reference is not null, get one from the persistent storage.
		if (pReference != null) {
			// First, try to get one from the transaction log.
			final long nodePageKey = pReference.getNodePageKey();
			final NodePageContainer cont = nodePageKey == -1 ? null
					: getPageContainer(pReference.getPageKind(), nodePageKey);
			if (cont != null) {
				page = cont.getModified();
			}

			// If none is in the log.
			// if (page == null) {
			// // Then try to get one from the page cache.
			// if (nodePageKey == -1 && pReference.getKey() != IConstants.NULL_ID) {
			// page = mPageLog.get(pReference.getKey());
			// }
			if (page == null) {
				// Test if one is instantiated, if so, get
				// the one from the reference.
				page = pReference.getPage();
				if (page == null) {
					return;
				}
			}
			// }

			pReference.setPage(page);
			if (pReference.getPageKind() == null) {
				// Page kind isn't interesting, just that it's not
				// a node page.
				if (page instanceof NodePage) {
					throw new IllegalStateException();
				} else {
					pReference.setPageKind(PageKind.INDIRECTPAGE);
				}
			}

			// Recursively commit indirectely referenced pages and then
			// write self.
			page.commit(this);
			mPageWriter.write(pReference);

			// Remove from transaction log.
			// if (pReference.getPageKind() != null) {
			// removePageContainer(pReference.getPageKind(), nodePageKey);
			// }

			// Remove page reference.
			pReference.setPage(null);

			// Afterwards synchronize all logs since the changes must be
			// written to the transaction log as well.
			if (mMultipleWriteTrx == MultipleWriteTrx.YES && cont != null) {
				mPageRtx.mSession.syncLogs(cont, mTransactionID,
						pReference.getPageKind());
			}
		}
	}

	@Override
	public UberPage commit(final @Nonnull MultipleWriteTrx pMultipleWriteTrx)
			throws SirixException {
		mPageRtx.assertNotClosed();
		mPageRtx.mSession.mCommitLock.lock();
		mMultipleWriteTrx = checkNotNull(pMultipleWriteTrx);

		// Forcefully flush write-ahead transaction logs to persistent storage.
		// Make
		// this optional!
		// mNodeLog.toSecondCache();
		// mPathLog.toSecondCache();
		// mValueLog.toSecondCache();
		// mPageLog.toSecondCache();

		final PageReference uberPageReference = new PageReference();
		final UberPage uberPage = getUberPage();
		uberPageReference.setPage(uberPage);
		uberPageReference.setPageKind(PageKind.UBERPAGE);

		// Recursively write indirectely referenced pages.
		uberPage.commit(this);

		uberPageReference.setPage(uberPage);
		mPageWriter.writeFirstReference(uberPageReference);
		uberPageReference.setPage(null);

		mPageRtx.mSession.waitForFinishedSync(mTransactionID);
		mPageRtx.mSession.mCommitLock.unlock();
		return uberPage;
	}

	@Override
	public void close() throws SirixIOException {
		if (!mIsClosed) {
			mPageRtx.assertNotClosed();
			mPageRtx.clearCaches();
			mPageRtx.closeCaches();
			mNodeLog.close();
			mPageLog.close();
			if (mPathLog != null) {
				mPathLog.close();
			}
			if (mValueLog != null) {
				mValueLog.close();
			}
			mPageWriter.close();
			mIsClosed = true;
		}
	}

	/**
	 * Prepare indirect page, that is getting the referenced indirect page or a
	 * new page.
	 * 
	 * @param pReference
	 *          {@link PageReference} to get the indirect page from or to create a
	 *          new one
	 * @return {@link IndirectPage} reference
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private IndirectPage prepareIndirectPage(
			final @Nonnull PageReference pReference) throws SirixIOException {
		IndirectPage page = (IndirectPage) pReference.getPage();
		if (page == null) {
			if (pReference.getKey() == Constants.NULL_ID) {
				page = new IndirectPage(getUberPage().getRevision());
			} else {
				// Should never be null, otherwise
				// dereferenceIndirectPage(PageReference) fails.
				final IndirectPage indirectPage = mPageRtx
						.dereferenceIndirectPage(pReference);
				page = new IndirectPage(indirectPage, mNewRoot.getRevision() + 1);
			}
			pReference.setPage(page);
		}
		pReference.setPageKind(PageKind.INDIRECTPAGE);
		return page;
	}

	/**
	 * Prepare node page.
	 * 
	 * @param pNodePageKey
	 *          the key of the node page
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void prepareNodePage(final @Nonnegative long pNodePageKey,
			final @Nonnull PageKind pPage) throws SirixIOException {
		// Last level points to node nodePageReference.
		NodePageContainer cont = getPageContainer(pPage, pNodePageKey);
		if (cont.equals(NodePageContainer.EMPTY_INSTANCE)) {
			// Indirect reference.
			final PageReference reference = prepareLeafOfTree(
					mPageRtx.getPageReference(mNewRoot, pPage), pNodePageKey, pPage);
			final NodePage page = (NodePage) reference.getPage();
			if (page == null) {
				if (reference.getKey() == Constants.NULL_ID) {
					cont = new NodePageContainer(new NodePage(pNodePageKey,
							Constants.UBP_ROOT_REVISION_NUMBER));
				} else {
					cont = dereferenceNodePageForModification(pNodePageKey, pPage);
				}
			} else {
				cont = new NodePageContainer(page);
			}

			assert cont != null;
			reference.setNodePageKey(pNodePageKey);
			reference.setPageKind(pPage);

			switch (pPage) {
			case NODEPAGE:
				mNodeLog.put(pNodePageKey, cont);
				break;
			case PATHSUMMARYPAGE:
				mPathLog.put(pNodePageKey, cont);
				break;
			case VALUEPAGE:
				mValueLog.put(pNodePageKey, cont);
				break;
			default:
				throw new IllegalStateException("Page kind not known!");
			}
		}
		mNodePageCon = cont;
	}

	/**
	 * Prepare the previous revision root page and retrieve the next
	 * {@link RevisionRootPage}.
	 * 
	 * @param pBaseRevision
	 *          base revision
	 * @param pRepresentRevision
	 *          the revision to represent
	 * @return new {@link RevisionRootPage} instance
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private RevisionRootPage preparePreviousRevisionRootPage(
			final @Nonnegative int pBaseRevision,
			final @Nonnegative int pRepresentRevision) throws SirixIOException {
		if (getUberPage().isBootstrap()) {
			return mPageRtx.loadRevRoot(pBaseRevision);
		} else {
			// Prepare revision root nodePageReference.
			final RevisionRootPage revisionRootPage = new RevisionRootPage(
					mPageRtx.loadRevRoot(pBaseRevision), pRepresentRevision + 1);

			// Prepare indirect tree to hold reference to prepared revision root
			// nodePageReference.
			final PageReference revisionRootPageReference = prepareLeafOfTree(
					getUberPage().getIndirectPageReference(), getUberPage()
							.getRevisionNumber(), PageKind.UBERPAGE);

			// Link the prepared revision root nodePageReference with the
			// prepared indirect tree.
			revisionRootPageReference.setPage(revisionRootPage);
			revisionRootPageReference.setPageKind(PageKind.REVISIONROOTPAGE);

			// Return prepared revision root nodePageReference.
			return revisionRootPage;
		}
	}

	/**
	 * Prepare the leaf of a tree, namely the reference to a {@link NodePage}.
	 * 
	 * @param pStartReference
	 *          start reference
	 * @param pKey
	 *          page key to lookup
	 * @return {@link PageReference} instance pointing to the right
	 *         {@link NodePage} with the {@code pKey}
	 * @throws SirixIOException
	 *           if an I/O error occured
	 */
	private PageReference prepareLeafOfTree(
			final @Nonnull PageReference pStartReference,
			final @Nonnegative long pKey, final @Nonnull PageKind pPage)
			throws SirixIOException {
		// Initial state pointing to the indirect nodePageReference of level 0.
		PageReference reference = pStartReference;
		int offset = 0;
		long levelKey = pKey;
		final int[] inpLevelPageCountExp = mPageRtx.getUberPage().getPageCountExp(
				pPage);

		// Iterate through all levels.
		for (int level = 0, height = inpLevelPageCountExp.length; level < height; level++) {
			offset = (int) (levelKey >> inpLevelPageCountExp[level]);
			levelKey -= offset << inpLevelPageCountExp[level];
			final IndirectPage page = prepareIndirectPage(reference);
			reference = page.getReference(offset);
		}

		// Return reference to leaf of indirect tree.
		return reference;
	}

	/**
	 * Dereference node page reference.
	 * 
	 * @param pNodePageKey
	 *          key of node page
	 * @return dereferenced page
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private NodePageContainer dereferenceNodePageForModification(
			final @Nonnegative long pNodePageKey, final @Nonnull PageKind pPage)
			throws SirixIOException {
		final NodePage[] revs = mPageRtx.getSnapshotPages(pNodePageKey, pPage);
		final Revisioning revisioning = mPageRtx.mSession.mResourceConfig.mRevisionKind;
		final int mileStoneRevision = mPageRtx.mSession.mResourceConfig.mRevisionsToRestore;
		return revisioning.combineNodePagesForModification(revs, mileStoneRevision);
	}

	@Override
	public RevisionRootPage getActualRevisionRootPage() {
		return mNewRoot;
	}

	/**
	 * Updating a container in this {@link PageWriteTrxImpl}.
	 * 
	 * @param pCont
	 *          {@link NodePageContainer} reference to be updated
	 * @param pPage
	 *          page for which the
	 */
	public void updateDateContainer(final @Nonnull NodePageContainer pContainer,
			final @Nonnull PageKind pPage) {
		final long nodePageKey = pContainer.getComplete().getNodePageKey();
		NodePageContainer container;
		switch (pPage) {
		case PATHSUMMARYPAGE:
			container = mPathLog.get(nodePageKey);
			break;
		case VALUEPAGE:
			container = mValueLog.get(nodePageKey);
			break;
		case NODEPAGE:
			container = mNodeLog.get(nodePageKey);
			break;
		default:
			throw new IllegalStateException("page kind not known!");
		}

		// Merge containers.
		final NodePage modified = container.getModified();
		final NodePage otherModified = pContainer.getModified();
		synchronized (modified) {
			for (final Entry<Long, NodeBase> entry : otherModified.entrySet()) {
				if (modified.getNode(entry.getKey()) == null) {
					modified.setNode(entry.getValue());
				}
			}
		}
	}

	@Override
	protected PageReadTrx delegate() {
		return mPageRtx;
	}

	@Override
	public void clearCaches() {
		mPageRtx.assertNotClosed();
		mPageLog.clear();
		mNodeLog.clear();

		if (mPathLog != null) {
			mPathLog.clear();
		}
		if (mValueLog != null) {
			mValueLog.clear();
		}
	}
}