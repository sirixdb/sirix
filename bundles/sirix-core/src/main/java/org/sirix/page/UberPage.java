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

package org.sirix.page;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.node.interfaces.Record;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;

/**
 * <h1>UberPage</h1>
 *
 * <p>
 * Uber page holds a reference to the static revision root page tree.
 * </p>
 */
public final class UberPage extends AbstractForwardingPage {

  /** Offset of indirect page reference. */
  private static final int INDIRECT_REFERENCE_OFFSET = 0;

  /** Number of revisions. */
  private final int mRevisionCount;

  /**
   * {@code true} if this uber page is the uber page of a fresh sirix file, {@code false} otherwise.
   */
  private boolean mIsBootstrap;

  /** {@link PageDelegate} instance. */
  private final PageDelegate mDelegate;

  /** {@link RevisionRootPage} instance. */
  private RevisionRootPage mRootPage;

  /** The current most recent revision */
  private final int mRevision;

  /** Key to previous uberpage in persistent storage. */
  private long mPreviousUberPageKey;

  /** Current maximum level of indirect pages in the tree. */
  private int mCurrentMaxLevelOfIndirectPages;

  /**
   * Create uber page.
   *
   * @param resourceConfig {@link ResourceConfiguration} reference
   */
  public UberPage() {
    mDelegate = new PageDelegate(1);
    mRevision = Constants.UBP_ROOT_REVISION_NUMBER;
    mRevisionCount = Constants.UBP_ROOT_REVISION_COUNT;
    mIsBootstrap = true;
    mPreviousUberPageKey = -1;
    mRootPage = null;
    mCurrentMaxLevelOfIndirectPages = 1;
  }

  /**
   * Read uber page.
   *
   * @param in input bytes
   * @param resourceConfig {@link ResourceConfiguration} reference
   */
  protected UberPage(final DataInput in, final SerializationType type) throws IOException {
    mDelegate = new PageDelegate(1, in, type);
    mRevisionCount = in.readInt();
    if (in.readBoolean())
      mPreviousUberPageKey = in.readLong();
    mRevision = mRevisionCount == 0
        ? 0
        : mRevisionCount - 1;
    mIsBootstrap = false;
    mRootPage = null;
    mCurrentMaxLevelOfIndirectPages = in.readByte() & 0xFF;
  }

  /**
   * Clone constructor.
   *
   * @param commitedUberPage page to clone
   * @param resourceConfig {@link ResourceConfiguration} reference
   */
  public UberPage(final UberPage committedUberPage, final long previousUberPageKey) {
    mDelegate =
        new PageDelegate(checkNotNull(committedUberPage), committedUberPage.mDelegate.getBitmap());
    mPreviousUberPageKey = previousUberPageKey;
    if (committedUberPage.isBootstrap()) {
      mRevision = committedUberPage.mRevision;
      mRevisionCount = committedUberPage.mRevisionCount;
      mIsBootstrap = committedUberPage.mIsBootstrap;
      mRootPage = committedUberPage.mRootPage;
    } else {
      mRevision = committedUberPage.mRevision + 1;
      mRevisionCount = committedUberPage.mRevisionCount + 1;
      mIsBootstrap = false;
      mRootPage = null;
    }
    mCurrentMaxLevelOfIndirectPages = committedUberPage.mCurrentMaxLevelOfIndirectPages;
  }

  public long getPreviousUberPageKey() {
    return mPreviousUberPageKey;
  }

  /**
   * Get indirect page reference.
   *
   * @return indirect page reference
   */
  public PageReference getIndirectPageReference() {
    return getReference(INDIRECT_REFERENCE_OFFSET);
  }

  /**
   * Get number of revisions.
   *
   * @return number of revisions
   */
  public int getRevisionCount() {
    return mRevisionCount;
  }

  /**
   * Get revision key of current in-memory state.
   *
   * @return revision key
   */
  public int getRevisionNumber() {
    return mRevisionCount - 1;
  }

  /**
   * Flag to indicate whether this uber page is the first ever.
   *
   * @return {@code true} if this uber page is the first one of sirix, {@code false} otherwise
   */
  public boolean isBootstrap() {
    return mIsBootstrap;
  }

  @Override
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    mDelegate.serialize(checkNotNull(out), checkNotNull(type));
    out.writeInt(mRevisionCount);
    out.writeBoolean(!mIsBootstrap);
    if (!mIsBootstrap) {
      out.writeLong(mPreviousUberPageKey);
    }
    out.writeByte(mCurrentMaxLevelOfIndirectPages);
    mIsBootstrap = false;
  }

  public int getCurrentMaxLevelOfIndirectPages() {
    return mCurrentMaxLevelOfIndirectPages;
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages() {
    return ++mCurrentMaxLevelOfIndirectPages;
  }

  @Override
  public void setReference(int offset, PageReference pageReference) {
    delegate().setReference(0, pageReference);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("forwarding page", super.toString())
                      .add("revisionCount", mRevisionCount)
                      .add("indirectPage", getReference(INDIRECT_REFERENCE_OFFSET))
                      .add("isBootstrap", mIsBootstrap)
                      .toString();
  }

  @Override
  protected Page delegate() {
    return mDelegate;
  }

  /**
   * Create revision tree.
   *
   * @param pageReadTrx {@link PageReadOnlyTrx} instance
   * @param revisionRoot {@link RevisionRootPage} instance
   */
  public void createRevisionTree(final TransactionIntentLog log) {
    // Initialize revision tree to guarantee that there is a revision root page.
    Page page = null;
    PageReference reference = getIndirectPageReference();

    page = new IndirectPage();
    log.put(reference, PageContainer.getInstance(page, page));
    reference = page.getReference(0);

    mRootPage = new RevisionRootPage();

    final Page namePage = mRootPage.getNamePageReference().getPage();
    log.put(mRootPage.getNamePageReference(), PageContainer.getInstance(namePage, namePage));

    final Page casPage = mRootPage.getCASPageReference().getPage();
    log.put(mRootPage.getCASPageReference(), PageContainer.getInstance(casPage, casPage));

    final Page pathPage = mRootPage.getPathPageReference().getPage();
    log.put(mRootPage.getPathPageReference(), PageContainer.getInstance(pathPage, pathPage));

    final Page pathSummaryPage = mRootPage.getPathSummaryPageReference().getPage();
    log.put(
        mRootPage.getPathSummaryPageReference(),
        PageContainer.getInstance(pathSummaryPage, pathSummaryPage));

    log.put(reference, PageContainer.getInstance(mRootPage, mRootPage));
  }

  /**
   * Get the page count exponent for the given page.
   *
   * @param pageKind page to lookup the exponent in the constant definition
   * @return page count exponent
   */
  public int[] getPageCountExp(final PageKind pageKind) {
    int[] inpLevelPageCountExp = new int[0];
    switch (pageKind) {
      case PATHSUMMARYPAGE:
        inpLevelPageCountExp = Constants.PATHINP_LEVEL_PAGE_COUNT_EXPONENT;
        break;
      case PATHPAGE:
      case CASPAGE:
      case NAMEPAGE:
      case RECORDPAGE:
        inpLevelPageCountExp = Constants.INP_LEVEL_PAGE_COUNT_EXPONENT;
        break;
      case UBERPAGE:
        inpLevelPageCountExp = Constants.UBPINP_LEVEL_PAGE_COUNT_EXPONENT;
        break;
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("page kind not known!");
    }
    return inpLevelPageCountExp;
  }

  public int getPageReferenceCount(final PageKind pageKind) {
    int referenceCount;
    switch (pageKind) {
      case PATHSUMMARYPAGE:
        referenceCount = Constants.PATHINP_REFERENCE_COUNT;
        break;
      case PATHPAGE:
      case CASPAGE:
      case NAMEPAGE:
      case RECORDPAGE:
        referenceCount = Constants.INP_REFERENCE_COUNT;
        break;
      case UBERPAGE:
        referenceCount = Constants.UBPINP_REFERENCE_COUNT;
        break;
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("page kind not known!");
    }
    return referenceCount;
  }

  public int getPageReferenceCountExp(final PageKind pageKind) {
    int referenceCount;
    switch (pageKind) {
      case PATHSUMMARYPAGE:
        referenceCount = Constants.PATHINP_REFERENCE_COUNT_EXPONENT;
        break;
      case PATHPAGE:
      case CASPAGE:
      case NAMEPAGE:
      case RECORDPAGE:
        referenceCount = Constants.INP_REFERENCE_COUNT_EXPONENT;
        break;
      case UBERPAGE:
        referenceCount = Constants.UBPINP_REFERENCE_COUNT_EXPONENT;
        break;
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("page kind not known!");
    }
    return referenceCount;
  }

  /**
   * Get the revision number.
   *
   * @return revision number
   */
  public int getRevision() {
    return mRevision;
  }

  @Override
  public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
      final PageTrx<K, V, S> pageWriteTrx) {
    mDelegate.commit(pageWriteTrx);
  }

  public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
      final String commitMessage, final PageTrx<K, V, S> pageWriteTrx) {
    pageWriteTrx.getActualRevisionRootPage().setCommitMessage(commitMessage);
    mDelegate.commit(pageWriteTrx);
  }
}
