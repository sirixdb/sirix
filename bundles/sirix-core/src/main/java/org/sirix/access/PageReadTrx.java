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
import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.IPageReadTrx;
import org.sirix.cache.GuavaCache;
import org.sirix.cache.ICache;
import org.sirix.cache.PageContainer;
import org.sirix.cache.Tuple;
import org.sirix.exception.TTIOException;
import org.sirix.io.IReader;
import org.sirix.node.DeletedNode;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INode;
import org.sirix.page.EPage;
import org.sirix.page.IndirectPage;
import org.sirix.page.NamePage;
import org.sirix.page.NodePage;
import org.sirix.page.PageReference;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.IPage;
import org.sirix.settings.ERevisioning;
import org.sirix.utils.IConstants;

/**
 * <h1>PageReadTransaction</h1>
 * 
 * <p>
 * Page reading transaction. The only thing shared amongst transactions is the page cache. Everything else is
 * exclusive to this transaction. It is required that only a single thread has access to this transaction.
 * </p>
 */
final class PageReadTrx implements IPageReadTrx {

  /** Page reader exclusively assigned to this transaction. */
  private final IReader mPageReader;

  /** Uber page this transaction is bound to. */
  private final UberPage mUberPage;

  /** Cached name page of this revision. */
  private final RevisionRootPage mRootPage;

  /** Internal reference to cache. */
  private final ICache<Tuple, PageContainer> mCache;

  /** Internal reference to cache. */
  private final ICache<Tuple, PageContainer> mPathCache;

  /** {@link Session} reference. */
  protected final Session mSession;

  /** {@link NamePage} reference. */
  private final NamePage mNamePage;

  /** Determines if page reading transaction is closed or not. */
  private boolean mClosed;

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
   * @throws TTIOException
   *           if reading of the persistent storage fails
   */
  PageReadTrx(@Nonnull final Session pSession,
    @Nonnull final UberPage pUberPage, @Nonnegative final long pRevision,
    @Nonnull final IReader pReader) throws TTIOException {
    checkArgument(pRevision >= 0, "Revision must be >= 0!");
    mSession = checkNotNull(pSession);
    mPageReader = checkNotNull(pReader);
    mUberPage = checkNotNull(pUberPage);
    mRootPage = loadRevRoot(pRevision);
    assert mRootPage != null : "root page must not be null!";
    mNamePage = getNamePage();
    final PageReference ref = mRootPage.getPathSummaryPageReference();
    if (ref.getPage() == null) {
      ref.setPage(mPageReader.read(ref.getKey()));
    }
    mClosed = false;
    mCache = new GuavaCache(this);
    mPathCache = new GuavaCache(this);
  }

  /**
   * Make sure that the transaction is not yet closed when calling this method.
   */
  final void assertNotClosed() {
    if (mClosed) {
      throw new IllegalStateException("Transaction is already closed.");
    }
  }

  /**
   * Initialize NamePage.
   * 
   * @throws TTIOException
   *           if an I/O error occurs
   */
  private final NamePage getNamePage() throws TTIOException {
    assertNotClosed();
    final PageReference ref = mRootPage.getNamePageReference();
    if (ref.getPage() == null) {
      ref.setPage(mPageReader.read(ref.getKey()));
    }
    return (NamePage)ref.getPage();
  }

  @Override
  public Optional<INode> getNode(@Nonnegative final long pNodeKey,
    @Nonnull final EPage pPage) throws TTIOException {
    checkArgument(pNodeKey >= 0);
    checkNotNull(pPage);
    assertNotClosed();
    assert pPage == EPage.NODEPAGE || pPage == EPage.PATHSUMMARY;

    final long nodePageKey = nodePageKey(pNodeKey);
    final int nodePageOffset = nodePageOffset(pNodeKey);

    final PageContainer cont =
      pPage == EPage.NODEPAGE ? mCache.get(new Tuple(nodePageKey, pPage))
        : mPathCache.get(new Tuple(nodePageKey, pPage));
    if (cont.equals(PageContainer.EMPTY_INSTANCE)) {
      return Optional.<INode> absent();
    }

    // If nodePage is a weak one, moveto is not cached.
    final INode retVal = ((NodePage)cont.getComplete()).getNode(nodePageOffset);
    return Optional.fromNullable(checkItemIfDeleted(retVal));
  }

  /**
   * Method to check if an {@link INode} is a deleted one.
   * 
   * @param pToCheck
   *          node to check
   * @return the {@code node} if it is valid, {@code null} otherwise
   */
  final INode checkItemIfDeleted(@Nullable final INode pToCheck) {
    if (pToCheck instanceof DeletedNode) {
      return null;
    } else {
      return pToCheck;
    }
  }

  @Override
  public String getName(final int pNameKey, @Nonnull final EKind pNodeKind) {
    assertNotClosed();
    return mNamePage.getName(pNameKey, pNodeKind);
  }

  @Override
  public final byte[] getRawName(final int pNameKey,
    @Nonnull final EKind pNodeKind) {
    assertNotClosed();
    return mNamePage.getRawName(pNameKey, pNodeKind);
  }

  /**
   * Clear the cache.
   */
  void clearCache() {
    assertNotClosed();
    mCache.clear();
  }

  /**
   * Get revision root page belonging to revision key.
   * 
   * @param pRevisionKey
   *          key of revision to find revision root page for
   * @return revision root page of this revision key
   * 
   * @throws TTIOException
   *           if something odd happens within the creation process
   */
  final RevisionRootPage loadRevRoot(@Nonnegative final long pRevisionKey)
    throws TTIOException {
    checkArgument(pRevisionKey >= 0
      && pRevisionKey <= mSession.getLastRevisionNumber(),
      "pRevisionKey must be >= 0 and <= last stored revision!");
    assertNotClosed();

    // The indirect page reference either fails horribly or returns a non null instance.
    @SuppressWarnings("null")
    final PageReference ref =
      dereferenceLeafOfTree(mUberPage.getIndirectPageReference(), pRevisionKey);
    RevisionRootPage page = (RevisionRootPage)ref.getPage();

    // If there is no page, get it from the storage and cache it.
    if (page == null) {
      page = (RevisionRootPage)mPageReader.read(ref.getKey());
    }

    // Get revision root page which is the leaf of the indirect tree.
    // ref.setPage(page);
    return page;
  }

  /**
   * Initialize PathSummaryPage.
   * 
   * @throws TTIOException
   *           if an I/O error occurs
   */
  private final PathSummaryPage getPathSummaryPage(
    @Nonnull final RevisionRootPage pPage) throws TTIOException {
    assertNotClosed();
    final PageReference ref = pPage.getPathSummaryPageReference();
    if (ref.getPage() == null) {
      ref.setPage(mPageReader.read(ref.getKey()));
    }
    return (PathSummaryPage)ref.getPage();
  }

  @Override
  public final UberPage getUberPage() {
    return mUberPage;
  }

  /**
   * Dereference node page reference and get all leaves, the {@link NodePage}s from the revision-trees.
   * 
   * @param pNodePageKey
   *          key of node page
   * @return dereferenced pages
   * 
   * @throws TTIOException
   *           if an I/O-error occurs within the creation process
   */
  final NodePage[] getSnapshotPages(@Nonnegative final long pNodePageKey,
    @Nonnull final EPage pPage) throws TTIOException {
    checkNotNull(pPage);
    assertNotClosed();
    final List<PageReference> refs = new ArrayList<>();
    final Set<Long> keys = new HashSet<>();
    final ResourceConfiguration config = mSession.getResourceConfig();
    final int revsToRestore = config.mRevisionsToRestore;
    for (long i = mRootPage.getRevision(); i >= 0; i--) {
      final PageReference tmpRef = getPageReference(loadRevRoot(i), pPage);
      final PageReference ref = dereferenceLeafOfTree(tmpRef, pNodePageKey);
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
          || (config.mRevisionKind == ERevisioning.DIFFERENTIAL && refs.size() == 2)) {
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
      pages[i] = (NodePage)ref.getPage();
      if (pages[i] == null) {
        pages[i] = (NodePage)mPageReader.read(ref.getKey());
      }
    }
    return pages;
  }

  PageReference getPageReference(@Nonnull final RevisionRootPage pRef,
    @Nonnull final EPage pPage) throws TTIOException {
    assert pRef != null;
    PageReference ref = null;
    switch (pPage) {
    case NODEPAGE:
      ref = pRef.getIndirectPageReference();
      break;
    case PATHSUMMARY:
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
   * @throws TTIOException
   *           if something odd happens within the creation process.
   */
  final IndirectPage dereferenceIndirectPage(
    @Nonnull final PageReference pReference) throws TTIOException {
    final IPage tmpPage = pReference.getPage();
    if (tmpPage == null || tmpPage instanceof IndirectPage) {
      IndirectPage page = (IndirectPage)tmpPage;

      // If there is no page, get it from the storage and cache it.
      if (page == null && pReference.getKey() != IConstants.NULL_ID) {
        page = (IndirectPage)mPageReader.read(pReference.getKey());
        pReference.setPage(page);
      }

      assert page != null;
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
   * @throws TTIOException
   *           if an I/O error occurs
   */
  final PageReference dereferenceLeafOfTree(
    @Nonnull final PageReference pStartReference, @Nonnegative final long pKey)
    throws TTIOException {

    // Initial state pointing to the indirect page of level 0.
    PageReference reference = checkNotNull(pStartReference);
    int offset = 0;
    long levelKey = pKey;

    // Iterate through all levels.
    for (int level = 0, height =
      IConstants.INP_LEVEL_PAGE_COUNT_EXPONENT.length; level < height; level++) {
      offset =
        (int)(levelKey >> IConstants.INP_LEVEL_PAGE_COUNT_EXPONENT[level]);
      levelKey -= offset << IConstants.INP_LEVEL_PAGE_COUNT_EXPONENT[level];
      final IPage page = dereferenceIndirectPage(reference);
      if (page == null) {
        reference = null;
        break;
      } else {
        reference = page.getReferences()[offset];
      }
    }

    // Return reference to leaf of indirect tree.
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

  /**
   * Calculate node page offset for a given node key.
   * 
   * @param pNodeKey
   *          node key to find offset for
   * @return offset into node page
   */
  final int nodePageOffset(@Nonnegative final long pNodeKey) {
    checkArgument(pNodeKey >= 0, "pNodeKey must not be negative!");
    final long shift =
      ((pNodeKey >> IConstants.NDP_NODE_COUNT_EXPONENT) << IConstants.NDP_NODE_COUNT_EXPONENT);
    return (int)(pNodeKey - shift);
  }

  @Override
  public RevisionRootPage getActualRevisionRootPage() throws TTIOException {
    return mRootPage;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("Session: ", mSession).add(
      "PageReader: ", mPageReader).add("UberPage: ", mUberPage).add(
      "RevRootPage: ", mRootPage).toString();
  }

  @Override
  public PageContainer getNodeFromPage(final long pNodePageKey,
    @Nonnull final EPage pPage) throws TTIOException {
    final NodePage[] revs = getSnapshotPages(pNodePageKey, pPage);
    if (revs.length == 0) {
      return PageContainer.EMPTY_INSTANCE;
    }

    final int mileStoneRevision =
      mSession.getResourceConfig().mRevisionsToRestore;
    final ERevisioning revisioning = mSession.getResourceConfig().mRevisionKind;
    final NodePage completePage =
      revisioning.combineNodePages(revs, mileStoneRevision);
    return new PageContainer(completePage);
  }

  @Override
  public void close() throws TTIOException {
    mClosed = true;
    mCache.clear();
    mPageReader.close();
  }

  @Override
  public int getNameCount(int pKey, @Nonnull EKind pKind) {
    return mNamePage.getCount(pKey, pKind);
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }
}
