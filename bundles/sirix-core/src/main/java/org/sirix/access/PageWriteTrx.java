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
import com.google.common.base.Optional;

import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.namespace.QName;

import org.sirix.api.IPageWriteTrx;
import org.sirix.api.ISession;
import org.sirix.cache.BerkeleyPersistencePageCache;
import org.sirix.cache.ICache;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionLogCache;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.io.IWriter;
import org.sirix.node.DeletedNode;
import org.sirix.node.EKind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.page.EPage;
import org.sirix.page.IndirectPage;
import org.sirix.page.NamePage;
import org.sirix.page.NodePage;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.IPage;
import org.sirix.settings.EFixed;
import org.sirix.settings.ERevisioning;
import org.sirix.utils.IConstants;
import org.sirix.utils.NamePageHash;

/**
 * <h1>PageWriteTrx</h1>
 * 
 * <p>
 * Implements the {@link IPageWriteTrx} interface to provide write capabilities to the persistent storage
 * layer.
 * </p>
 */
public final class PageWriteTrx implements IPageWriteTrx {

  /** Page writer to serialize. */
  private final IWriter mPageWriter;

  /** Cache to store the changes in this writetransaction. */
  private final ICache<Long, PageContainer> mNodeLog;

  /** Cache to store path changes in this writetransaction. */
  private final ICache<Long, PageContainer> mPathLog;

  /** Cache to store path changes in this writetransaction. */
  private final ICache<Long, PageContainer> mValueLog;

  /** Last references to the Nodepage, needed for pre/postcondition check. */
  private PageContainer mNodePageCon;

  /** Last reference to the actual revRoot. */
  private final RevisionRootPage mNewRoot;

  /** ID for current transaction. */
  private final long mTransactionID;

  /** {@link PageReadTrx} instance. */
  private final PageReadTrx mPageRtx;

  /** Determines if multiple {@link SynchNodeWriteTrx} are working or just a single {@link NodeWriteTrx}. */
  private EMultipleWriteTrx mMultipleWriteTrx;

  /** Determines if a log must be replayed or not. */
  public enum ERestore {
    /** Yes, it must be replayed. */
    YES,

    /** No, it must not be replayed. */
    NO
  }

  /** Determines if a log must be replayed or not. */
  private ERestore mRestore = ERestore.NO;

  /** Persistent BerkeleyDB page log for all page types != NodePage. */
  private final BerkeleyPersistencePageCache mPageLog;

  /** Pool to flush pages to persistent page log. */
  private final ScheduledExecutorService mPool = Executors
    .newScheduledThreadPool(1);

  /**
   * Standard constructor.
   * 
   * @param pSession
   *          {@link ISessionConfiguration} this page write trx is bound to
   * @param pUberPage
   *          root of revision
   * @param pWriter
   *          writer where this transaction should write to
   * @param pId
   *          ID
   * @param pRepresentRev
   *          revision represent
   * @param pStoreRev
   *          revision store
   * @throws AbsTTException
   *           if an error occurs
   */
  PageWriteTrx(final @Nonnull Session pSession,
    final @Nonnull UberPage pUberPage, final @Nonnull IWriter pWriter,
    final @Nonnegative long pId, final @Nonnegative long pRepresentRev,
    final @Nonnegative long pStoreRev, final @Nonnegative long pLastCommitedRev)
    throws AbsTTException {
    mPathLog =
      new TransactionLogCache(this, pSession.mResourceConfig.mPath, pStoreRev,
        "path");
    mNodeLog =
      new TransactionLogCache(this, pSession.mResourceConfig.mPath, pStoreRev,
        "node");
    mValueLog =
      new TransactionLogCache(this, pSession.mResourceConfig.mPath, pStoreRev,
        "value");
    mPageLog =
      new BerkeleyPersistencePageCache(this, pSession.mResourceConfig.mPath,
        pStoreRev, "page");
    mPageWriter = pWriter;
    mTransactionID = pId;
    mPageRtx =
      new PageReadTrx(pSession, pUberPage, pRepresentRev, pWriter, Optional
        .of(mPageLog));
    mPool.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        mPageRtx.putPageCache(mPageLog);
      }
    }, 20, 20, TimeUnit.SECONDS);

    final RevisionRootPage lastCommitedRoot =
      preparePreviousRevisionRootPage(pRepresentRev, pLastCommitedRev);
    mNewRoot = preparePreviousRevisionRootPage(pRepresentRev, pStoreRev);
    mNewRoot.setMaxNodeKey(lastCommitedRoot.getMaxNodeKey());
    mNewRoot.setMaxPathNodeKey(lastCommitedRoot.getMaxPathNodeKey());
    mNewRoot.setMaxValueNodeKey(lastCommitedRoot.getMaxValueNodeKey());
  }

  @Override
  public boolean isCreated() {
    return mPageLog.isCreated();
  }

  @Override
  public void flushToPersistentCache() {
    mPageRtx.putPageCache(mPageLog);
  }

  @Override
  public void restore(final @Nonnull ERestore pRestore) {
    mRestore = checkNotNull(pRestore);
  }

  @Override
  public INodeBase prepareNodeForModification(final @Nonnegative long pNodeKey,
    final @Nonnull EPage pPage) throws TTIOException {
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

    INodeBase node = mNodePageCon.getModified().getNode(pNodeKey);
    if (node == null) {
      final INodeBase oldNode = mNodePageCon.getComplete().getNode(pNodeKey);
      if (oldNode == null) {
        throw new TTIOException("Cannot retrieve node from cache!");
      }
      node = oldNode;
      mNodePageCon.getModified().setNode(node);
    }

    return node;
  }

  @Override
  public void finishNodeModification(final @Nonnull INodeBase pNode,
    final @Nonnull EPage pPage) {
    final long nodePageKey = mPageRtx.nodePageKey(pNode.getNodeKey());
    if (mNodePageCon == null
      || pNode == null
      || (mNodeLog.get(nodePageKey) == null
        && mPathLog.get(nodePageKey) == null && mValueLog.get(nodePageKey) == null)) {
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
  public INodeBase createNode(final @Nonnull INodeBase pNode,
    final @Nonnull EPage pPage) throws TTIOException {
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
    finishNodeModification(pNode, pPage);
    return pNode;
  }

  @Override
  public void
    removeNode(@Nonnull final INode pNode, @Nonnull final EPage pPage)
      throws TTIOException {
    final long nodePageKey = mPageRtx.nodePageKey(pNode.getNodeKey());
    prepareNodePage(nodePageKey, pPage);
    final INode delNode =
      new DeletedNode(new NodeDelegate(pNode.getNodeKey(),
        pNode.getParentKey(), pNode.getHash()));
    mNodePageCon.getModified().setNode(delNode);
    mNodePageCon.getComplete().setNode(delNode);
    finishNodeModification(pNode, pPage);
  }

  @Override
  public Optional<INodeBase> getNode(final @Nonnegative long pNodeKey,
    final @Nonnull EPage pPage) throws TTIOException {
    checkArgument(pNodeKey >= EFixed.NULL_NODE_KEY.getStandardProperty());
    checkNotNull(pPage);
    // Calculate page and node part for given nodeKey.
    final long nodePageKey = mPageRtx.nodePageKey(pNodeKey);
    // final int nodePageOffset = mPageRtx.nodePageOffset(pNodeKey);

    final PageContainer pageCont = getPageContainer(pPage, nodePageKey);

    if (pageCont == null) {
      return mPageRtx.getNode(pNodeKey, pPage);
    } else {
      INodeBase node = pageCont.getModified().getNode(pNodeKey);
      if (node == null) {
        node = pageCont.getComplete().getNode(pNodeKey);
        return Optional.fromNullable(mPageRtx.checkItemIfDeleted(node));
      } else {
        return Optional.fromNullable(mPageRtx.checkItemIfDeleted(node));
      }
    }
  }

  /**
   * Get the page container.
   * 
   * @param pPage
   *          the kind of page
   * @param pNodePageKey
   *          the node page key
   * @return the {@link PageContainer} instance from the write ahead log
   */
  private PageContainer getPageContainer(final @Nullable EPage pPage,
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
    return null;
  }

  private void removePageContainer(final @Nullable EPage pPage,
    final @Nonnegative long pNodePageKey) {
    if (pPage != null) {
      switch (pPage) {
      case NODEPAGE:
        mNodeLog.remove(pNodePageKey);
        break;
      case PATHSUMMARYPAGE:
        break;
      case VALUEPAGE:
        break;
      default:
        throw new IllegalStateException();
      }
    }
  }

  @Override
  public String getName(final int pNameKey, final @Nonnull EKind pNodeKind) {
    final NamePage currentNamePage =
      (NamePage)mNewRoot.getNamePageReference().getPage();
    // if currentNamePage == null -> state was commited and no prepareNodepage was invoked yet
    return (currentNamePage == null || currentNamePage.getName(pNameKey,
      pNodeKind) == null) ? mPageRtx.getName(pNameKey, pNodeKind)
      : currentNamePage.getName(pNameKey, pNodeKind);
  }

  @Override
  public int createNameKey(final @Nullable String pName,
    final @Nonnull EKind pNodeKind) throws TTIOException {
    checkNotNull(pNodeKind);
    final String string = (pName == null ? "" : pName);
    final int nameKey = NamePageHash.generateHashForString(string);
    final NamePage namePage =
      (NamePage)mNewRoot.getNamePageReference().getPage();
    namePage.setName(nameKey, string, pNodeKind);
    return nameKey;
  }

  @Override
  public void commit(final @Nullable PageReference pReference)
    throws AbsTTException {
    IPage page = null;

    // If reference is not null, get one from the persistent storage.
    if (pReference != null) {
      // First, try to get one from the log.
      final long nodePageKey = pReference.getNodePageKey();
      final PageContainer cont =
        nodePageKey == -1 ? null : getPageContainer(pReference.getPageKind(),
          nodePageKey);
      if (cont != null) {
        page = cont.getModified();
      }

      // If none is in the log.
      if (page == null) {
//        // // Then try to get one from the page cache.
//        if (nodePageKey == -1 && pReference.getKey() != IConstants.NULL_ID) {
//          page = mPageRtx.getFromPageCache(pReference.getKey());
//        }
//        if (page instanceof NodePage) {
//          page = null;
//        }
//        if (page == null) {
          // Test if one is instantiated, if so, get
          // the one from the reference.
          page = pReference.getPage();
          if (page == null) {
            return;
          }
//        } else {
//          assert !(page instanceof NodePage);
//        }
      }

      pReference.setPage(page);
      // Recursively commit indirectely referenced pages and then
      // write self.
      page.commit(this);
      mPageWriter.write(pReference);

//      // Remove from transaction log.
//      removePageContainer(pReference.getPageKind(), nodePageKey);

      pReference.setPage(null);

      // Afterwards synchronize all logs since the changes must be
      // written to the transaction log as well.
      if (mMultipleWriteTrx == EMultipleWriteTrx.YES && cont != null) {
        mPageRtx.mSession.syncLogs(cont, mTransactionID, pReference
          .getPageKind());
      }
    }
  }

  @Override
  public UberPage commit(final @Nonnull EMultipleWriteTrx pMultipleWriteTrx)
    throws AbsTTException {
    mPageRtx.mSession.mCommitLock.lock();
    mMultipleWriteTrx = checkNotNull(pMultipleWriteTrx);

    // Forcefully flush write-ahead transaction logs to persistent storage.
//    mNodeLog.toSecondCache();
//    mPathLog.toSecondCache();
//    mValueLog.toSecondCache();
//    mPageRtx.putPageCache(mPageLog);

    final PageReference uberPageReference = new PageReference();
    final UberPage uberPage = getUberPage();
    uberPageReference.setPage(uberPage);

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
  public void close() throws TTIOException {
    mPageRtx.assertNotClosed();
    mPageRtx.clearCache();
    mNodeLog.clear();
    mPathLog.clear();
    mValueLog.clear();
    mPageLog.clear();
    mPageWriter.close();
    mPool.shutdownNow();
  }

  /**
   * Prepare indirect page, that is getting the referenced indirect page or a new page.
   * 
   * @param pReference
   *          {@link PageReference} to get the indirect page from or to create a new one
   * @return {@link IndirectPage} reference
   * @throws TTIOException
   *           if an I/O error occurs
   */
  @SuppressWarnings("null")
  private IndirectPage prepareIndirectPage(
    final @Nonnull PageReference pReference) throws TTIOException {
    IndirectPage page = (IndirectPage)pReference.getPage();
    if (page == null) {
      if (pReference.getKey() == IConstants.NULL_ID) {
        page = new IndirectPage(getUberPage().getRevision());
      } else {
        // Should never be null, otherwise dereferenceIndirectPage(PageReference) fails.
        final IndirectPage indirectPage =
          mPageRtx.dereferenceIndirectPage(pReference);
        page = new IndirectPage(indirectPage, mNewRoot.getRevision() + 1);
      }
      pReference.setPage(page);
    }
    return page;
  }

  /**
   * Prepare node page.
   * 
   * @param pNodePageKey
   *          the key of the node page
   * @throws TTIOException
   *           if an I/O error occurs
   */
  private void prepareNodePage(final @Nonnegative long pNodePageKey,
    final @Nonnull EPage pPage) throws TTIOException {
    // Last level points to node nodePageReference.
    PageContainer cont = getPageContainer(pPage, pNodePageKey);
    if (cont == null) {
      // Indirect reference.
      final PageReference reference =
        prepareLeafOfTree(mPageRtx.getPageReference(mNewRoot, pPage),
          pNodePageKey);
      final NodePage page = (NodePage)reference.getPage();
      if (page == null) {
        if (reference.getKey() == IConstants.NULL_ID) {
          cont =
            new PageContainer(new NodePage(pNodePageKey,
              IConstants.UBP_ROOT_REVISION_NUMBER));
        } else {
          cont = dereferenceNodePageForModification(pNodePageKey, pPage);
        }
      } else {
        cont = new PageContainer(page);
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
   * Prepare the previous revision root page and retrieve the next {@link RevisionRootPage}.
   * 
   * @param pBaseRevision
   *          base revision
   * @param pRepresentRevision
   *          the revision to represent
   * @return new {@link RevisionRootPage} instance
   * @throws TTIOException
   *           if an I/O error occurs
   */
  private RevisionRootPage preparePreviousRevisionRootPage(
    final @Nonnegative long pBaseRevision,
    final @Nonnegative long pRepresentRevision) throws TTIOException {
    if (getUberPage().isBootstrap()) {
      return mPageRtx.loadRevRoot(pBaseRevision);
    } else {
      // Prepare revision root nodePageReference.
      @SuppressWarnings("null")
      final RevisionRootPage revisionRootPage =
        new RevisionRootPage(mPageRtx.loadRevRoot(pBaseRevision),
          pRepresentRevision + 1);

      // Prepare indirect tree to hold reference to prepared revision root
      // nodePageReference.
      final PageReference revisionRootPageReference =
        prepareLeafOfTree(getUberPage().getIndirectPageReference(),
          getUberPage().getRevisionNumber());

      // Link the prepared revision root nodePageReference with the
      // prepared indirect tree.
      revisionRootPageReference.setPage(revisionRootPage);

      // revisionRootPage.getNamePageReference().setPage(
      // mPageRtx.getActualRevisionRootPage().getNamePageReference().getPage());
      // revisionRootPage.getPathSummaryPageReference().setPage(
      // mPageRtx.getActualRevisionRootPage().getPathSummaryPageReference()
      // .getPage());

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
   * @return {@link PageReference} instance pointing to the right {@link NodePage} with the {@code pKey}
   * @throws TTIOException
   *           if an I/O error occured
   */
  private PageReference prepareLeafOfTree(
    final @Nonnull PageReference pStartReference, final @Nonnegative long pKey)
    throws TTIOException {
    // Initial state pointing to the indirect nodePageReference of level 0.
    PageReference reference = pStartReference;
    int offset = 0;
    long levelKey = pKey;

    // Iterate through all levels.
    for (int level = 0, height =
      IConstants.INP_LEVEL_PAGE_COUNT_EXPONENT.length; level < height; level++) {
      offset =
        (int)(levelKey >> IConstants.INP_LEVEL_PAGE_COUNT_EXPONENT[level]);
      levelKey -= offset << IConstants.INP_LEVEL_PAGE_COUNT_EXPONENT[level];
      final IndirectPage page = prepareIndirectPage(reference);
      reference = page.getReferences()[offset];
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
   * @throws TTIOException
   *           if an I/O error occurs
   */
  private PageContainer dereferenceNodePageForModification(
    final @Nonnegative long pNodePageKey, final @Nonnull EPage pPage)
    throws TTIOException {
    final NodePage[] revs = mPageRtx.getSnapshotPages(pNodePageKey, pPage);
    final ERevisioning revisioning =
      mPageRtx.mSession.mResourceConfig.mRevisionKind;
    final int mileStoneRevision =
      mPageRtx.mSession.mResourceConfig.mRevisionsToRestore;
    return revisioning.combineNodePagesForModification(revs, mileStoneRevision);
  }

  @Override
  public RevisionRootPage getActualRevisionRootPage() {
    return mNewRoot;
  }

  /**
   * Updating a container in this {@link PageWriteTrx}.
   * 
   * @param pCont
   *          {@link PageContainer} reference to be updated
   * @param pPage
   *          page for which the
   */
  public void updateDateContainer(final @Nonnull PageContainer pContainer,
    final @Nonnull EPage pPage) {
    final long nodePageKey = pContainer.getComplete().getNodePageKey();
    PageContainer container;
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
      for (final Entry<Long, INodeBase> entry : otherModified.entrySet()) {
        if (modified.getNode(entry.getKey()) == null) {
          modified.setNode(entry.getValue());
        }
      }
    }
  }

  /**
   * Building name consisting out of prefix and name. NamespaceUri is not used
   * over here.
   * 
   * @param pQName
   *          the {@link QName} of an element
   * @return a string with [prefix:]localname
   */
  public static String buildName(final @Nonnull QName pQName) {
    String name;
    if (pQName.getPrefix().isEmpty()) {
      name = pQName.getLocalPart();
    } else {
      name =
        new StringBuilder(pQName.getPrefix()).append(":").append(
          pQName.getLocalPart()).toString();
    }
    return name;
  }

  @Override
  public byte[] getRawName(final int pNameKey, final @Nonnull EKind pKind) {
    return mPageRtx.getRawName(pNameKey, pKind);
  }

  @Override
  public PageContainer getNodeFromPage(final long pKey,
    final @Nonnull EPage pPage) throws TTIOException {
    return mPageRtx.getNodeFromPage(pKey, pPage);
  }

  @Override
  public UberPage getUberPage() {
    return mPageRtx.getUberPage();
  }

  @Override
  public int getNameCount(final int pKey, final @Nonnull EKind pKind) {
    return mPageRtx.getNameCount(pKey, pKind);
  }

  @Override
  public boolean isClosed() {
    return mPageRtx.isClosed();
  }

  @Override
  public long getRevisionNumber() {
    return mPageRtx.getRevisionNumber();
  }

  @Override
  public ISession getSession() {
    return mPageRtx.getSession();
  }

  @Override
  public IPage getFromPageCache(final @Nonnegative long pKey)
    throws TTIOException {
    return mPageRtx.getFromPageCache(pKey);
  }

  @Override
  public void
    putPageCache(final @Nonnull BerkeleyPersistencePageCache pPageLog) {
    mPageRtx.putPageCache(pPageLog);
  }
}
