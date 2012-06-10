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

package org.treetank.access;

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

import org.treetank.api.IPageReadTrx;
import org.treetank.cache.GuavaCache;
import org.treetank.cache.ICache;
import org.treetank.cache.NodePageContainer;
import org.treetank.exception.TTIOException;
import org.treetank.io.IReader;
import org.treetank.node.DeletedNode;
import org.treetank.node.ENode;
import org.treetank.node.interfaces.INode;
import org.treetank.page.IndirectPage;
import org.treetank.page.NamePage;
import org.treetank.page.NodePage;
import org.treetank.page.PageReference;
import org.treetank.page.RevisionRootPage;
import org.treetank.page.UberPage;
import org.treetank.page.interfaces.IPage;
import org.treetank.settings.ERevisioning;
import org.treetank.utils.IConstants;

/**
 * <h1>PageReadTransaction</h1>
 * 
 * <p>
 * State of a reading transaction. The only thing shared amongst transactions is the page cache. Everything
 * else is exclusive to this transaction. It is required that only a single thread has access to this
 * transaction.
 * </p>
 * 
 * <p>
 * A path-like cache boosts sequential operations.
 * </p>
 */
class PageReadTrx implements IPageReadTrx {

  /** Page reader exclusively assigned to this transaction. */
  private final IReader mPageReader;

  /** Uber page this transaction is bound to. */
  private final UberPage mUberPage;

  /** Cached name page of this revision. */
  private final RevisionRootPage mRootPage;

  /** Internal reference to cache. */
  private final ICache<Long, NodePageContainer> mCache;

  /** {@link Session} reference. */
  protected final Session mSession;

  /** {@link NamePage} reference. */
  private final NamePage mNamePage;

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
  PageReadTrx(final Session pSession, final UberPage pUberPage, final long pRevision, final IReader pReader)
    throws TTIOException {
    checkArgument(pRevision >= 0, "Revision must be >= 0!");
    mSession = checkNotNull(pSession);
    mPageReader = checkNotNull(pReader);
    mUberPage = checkNotNull(pUberPage);
    mRootPage = loadRevRoot(pRevision);
    mNamePage = initializeNamePage();
    mCache = new GuavaCache(this);
  }

  @Override
  public Optional<INode> getNode(@Nonnegative final long pNodeKey) throws TTIOException {
    checkArgument(pNodeKey >= 0);

    final long nodePageKey = nodePageKey(pNodeKey);
    final int nodePageOffset = nodePageOffset(pNodeKey);

    final NodePageContainer cont = mCache.get(nodePageKey);
    if (cont.equals(NodePageContainer.EMPTY_INSTANCE)) {
      return Optional.<INode> absent();
    }

    // If nodePage is a weak one, moveto is not cached.
    final INode retVal = cont.getComplete().getNode(nodePageOffset);
    return Optional.fromNullable(checkItemIfDeleted(retVal));
  }

  /**
   * Method to check if an {@link INode} is a deleted one.
   * 
   * @param pToCheck
   *          node to check
   * @return the {@code node} if it is valid, {@code null} otherwise
   */
  final <T extends INode> T checkItemIfDeleted(@Nonnull final T pToCheck) {
    if (pToCheck instanceof DeletedNode) {
      return null;
    } else {
      return pToCheck;
    }
  }

  @Override
  public String getName(final int pNameKey, @Nonnull final ENode pNodeKind) {
    return mNamePage.getName(pNameKey, checkNotNull(pNodeKind));
  }

  @Override
  public final byte[] getRawName(final int pNameKey, @Nonnull final ENode pNodeKind) {
    return mNamePage.getRawName(pNameKey, checkNotNull(pNodeKind));
  }

  /**
   * Clear the cache.
   */
  void clearCache() {
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
  final RevisionRootPage loadRevRoot(final long pRevisionKey) throws TTIOException {
    checkArgument(pRevisionKey >= 0, "pRevisionKey must be >= 0!");
    final PageReference ref = dereferenceLeafOfTree(mUberPage.getIndirectPageReference(), pRevisionKey);
    RevisionRootPage page = (RevisionRootPage)ref.getPage();

    // If there is no page, get it from the storage and cache it.
    if (page == null) {
      page = (RevisionRootPage)mPageReader.read(ref.getKey());
    }

    // Get revision root page which is the leaf of the indirect tree.
    return page;
  }

  /**
   * Initialize NamePage.
   * 
   * @throws TTIOException
   *           if something odd happens during initialization
   */
  final NamePage initializeNamePage() throws TTIOException {
    final PageReference ref = mRootPage.getNamePageReference();
    if (ref.getPage() == null) {
      ref.setPage(mPageReader.read(ref.getKey()));
    }
    return (NamePage)ref.getPage();
  }

  @Override
  public final UberPage getUberPage() {
    return mUberPage;
  }

  /**
   * Dereference node page reference and get all leaves, the nodepages from the revision-trees..
   * 
   * @param pNodePageKey
   *          key of node page
   * @return dereferenced pages
   * 
   * @throws TTIOException
   *           if an I/O-error occurs within the creation process
   */
  final NodePage[] getSnapshotPages(@Nonnegative final long pNodePageKey) throws TTIOException {
    final List<PageReference> revs = new ArrayList<>();
    final Set<Long> keys = new HashSet<>();

    for (long i = mRootPage.getRevision(); i >= 0; i--) {
      final PageReference ref =
        dereferenceLeafOfTree(loadRevRoot(i).getIndirectPageReference(), pNodePageKey);
      if (ref != null && (ref.getPage() != null || ref.getKey() != null)) {
        if (ref.getKey() == null || (!keys.contains(ref.getKey().getIdentifier()))) {
          revs.add(ref);
          if (ref.getKey() != null) {
            keys.add(ref.getKey().getIdentifier());
          }
        }
        if (revs.size() == mSession.getResourceConfig().mRevisionsToRestore) {
          break;
        }
      } else {
        break;
      }
    }

    // Afterwards read the nodepages if they are not dereferences...
    final NodePage[] pages = new NodePage[revs.size()];
    for (int i = 0; i < pages.length; i++) {
      final PageReference rev = revs.get(i);
      pages[i] = (NodePage)rev.getPage();
      if (pages[i] == null) {
        pages[i] = (NodePage)mPageReader.read(rev.getKey());
      }
    }
    return pages;
  }

  /**
   * Dereference indirect page reference.
   * 
   * @param reference
   *          Reference to dereference.
   * @return Dereferenced page.
   * 
   * @throws TTIOException
   *           if something odd happens within the creation process.
   */
  final IndirectPage dereferenceIndirectPage(@Nonnull final PageReference reference) throws TTIOException {
    IndirectPage page = (IndirectPage)reference.getPage();

    // If there is no page, get it from the storage and cache it.
    if (page == null) {
      page = (IndirectPage)mPageReader.read(reference.getKey());
      reference.setPage(page);
    }

    return page;
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
  final PageReference dereferenceLeafOfTree(@Nonnull final PageReference pStartReference, final long pKey)
    throws TTIOException {

    // Initial state pointing to the indirect page of level 0.
    PageReference reference = checkNotNull(pStartReference);
    int offset = 0;
    long levelKey = pKey;

    // Iterate through all levels.
    for (int level = 0, height = IConstants.INP_LEVEL_PAGE_COUNT_EXPONENT.length; level < height; level++) {
      offset = (int)(levelKey >> IConstants.INP_LEVEL_PAGE_COUNT_EXPONENT[level]);
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
    return Objects.toStringHelper(this).add("Session: ", mSession).add("PageReader: ", mPageReader).add(
      "UberPage: ", mUberPage).add("RevRootPage: ", mRootPage).toString();
  }

  @Override
  public NodePageContainer getNodeFromPage(final long pNodePageKey) throws TTIOException {
    final NodePage[] revs = getSnapshotPages(pNodePageKey);
    if (revs.length == 0) {
      return NodePageContainer.EMPTY_INSTANCE;
    }

    final int mileStoneRevision = mSession.getResourceConfig().mRevisionsToRestore;
    final ERevisioning revision = mSession.getResourceConfig().mRevisionKind;
    final NodePage completePage = revision.combinePages(revs, mileStoneRevision);
    return new NodePageContainer(completePage);
  }

  @Override
  public void close() throws TTIOException {
    mCache.clear();
    mPageReader.close();
  }
}
