/*
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

import com.google.common.base.MoreObjects;
import org.sirix.api.PageTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.delegates.BitmapReferencesPage;
import org.sirix.page.delegates.ReferencesPage4;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The Uber page holds a reference to the revision root page tree.
 */
public final class UberPage extends AbstractForwardingPage {

  /**
   * Offset of indirect page reference.
   */
  private static final int INDIRECT_REFERENCE_OFFSET = 0;

  /**
   * Number of revisions.
   */
  private final int revisionCount;

  /**
   * {@code true} if this uber page is the uber page of a fresh sirix file, {@code false} otherwise.
   */
  private boolean isBootstrap;

  /**
   * The references page instance.
   */
  private Page delegate;

  /**
   * {@link RevisionRootPage} instance.
   */
  private RevisionRootPage rootPage;

  /**
   * The current most recent revision
   */
  private final int revision;

  /**
   * Key to previous uberpage in persistent storage.
   */
  private long mPreviousUberPageKey;

  /**
   * Current maximum level of indirect pages in the tree.
   */
  private int currentMaxLevelOfIndirectPages;

  /**
   * Create uber page.
   */
  public UberPage() {
    delegate = new ReferencesPage4();
    revision = Constants.UBP_ROOT_REVISION_NUMBER;
    revisionCount = Constants.UBP_ROOT_REVISION_COUNT;
    isBootstrap = true;
    mPreviousUberPageKey = -1;
    rootPage = null;
    currentMaxLevelOfIndirectPages = 1;
  }

  /**
   * Read uber page.
   *
   * @param in   input bytes
   * @param type the serialization type
   */
  protected UberPage(final DataInput in, final SerializationType type) throws IOException {
    delegate = new ReferencesPage4(in, type);
    revisionCount = in.readInt();
    if (in.readBoolean())
      mPreviousUberPageKey = in.readLong();
    revision = revisionCount == 0 ? 0 : revisionCount - 1;
    isBootstrap = false;
    rootPage = null;
    currentMaxLevelOfIndirectPages = in.readByte() & 0xFF;
  }

  /**
   * Clone constructor.
   *
   * @param committedUberPage   page to clone
   * @param previousUberPageKey the previous uber page key
   */
  public UberPage(final UberPage committedUberPage, final long previousUberPageKey) {
    final Page pageDelegate = committedUberPage.delegate();

    if (pageDelegate instanceof ReferencesPage4) {
      delegate = new ReferencesPage4((ReferencesPage4) pageDelegate);
    } else if (pageDelegate instanceof BitmapReferencesPage) {
      delegate = new BitmapReferencesPage(pageDelegate, ((BitmapReferencesPage) pageDelegate).getBitmap());
    }
    mPreviousUberPageKey = previousUberPageKey;
    if (committedUberPage.isBootstrap()) {
      revision = committedUberPage.revision;
      revisionCount = committedUberPage.revisionCount;
      isBootstrap = committedUberPage.isBootstrap;
      rootPage = committedUberPage.rootPage;
    } else {
      revision = committedUberPage.revision + 1;
      revisionCount = committedUberPage.revisionCount + 1;
      isBootstrap = false;
      rootPage = null;
    }
    currentMaxLevelOfIndirectPages = committedUberPage.currentMaxLevelOfIndirectPages;
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
    return getOrCreateReference(INDIRECT_REFERENCE_OFFSET);
  }

  /**
   * Get number of revisions.
   *
   * @return number of revisions
   */
  public int getRevisionCount() {
    return revisionCount;
  }

  /**
   * Get revision key of current in-memory state.
   *
   * @return revision key
   */
  public int getRevisionNumber() {
    return revisionCount - 1;
  }

  /**
   * Flag to indicate whether this uber page is the first ever.
   *
   * @return {@code true} if this uber page is the first one of sirix, {@code false} otherwise
   */
  public boolean isBootstrap() {
    return isBootstrap;
  }

  @Override
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    delegate.serialize(checkNotNull(out), checkNotNull(type));
    out.writeInt(revisionCount);
    out.writeBoolean(!isBootstrap);
    if (!isBootstrap) {
      out.writeLong(mPreviousUberPageKey);
    }
    out.writeByte(currentMaxLevelOfIndirectPages);
    isBootstrap = false;
  }

  public int getCurrentMaxLevelOfIndirectPages() {
    return currentMaxLevelOfIndirectPages;
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages() {
    return ++currentMaxLevelOfIndirectPages;
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    return delegate().setOrCreateReference(0, pageReference);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("forwarding page", super.toString())
                      .add("revisionCount", revisionCount)
                      .add("indirectPage", getOrCreateReference(INDIRECT_REFERENCE_OFFSET))
                      .add("isBootstrap", isBootstrap)
                      .toString();
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Create revision tree.
   *
   * @param log the transaction intent log
   */
  public void createRevisionTree(final TransactionIntentLog log) {
    // Initialize revision tree to guarantee that there is a revision root page.
    var reference = getIndirectPageReference();

    final var page = new IndirectPage();
    log.put(reference, PageContainer.getInstance(page, page));
    reference = page.getOrCreateReference(0);

    rootPage = new RevisionRootPage();

    final var namePage = rootPage.getNamePageReference().getPage();
    log.put(rootPage.getNamePageReference(), PageContainer.getInstance(namePage, namePage));

    final var casPage = rootPage.getCASPageReference().getPage();
    log.put(rootPage.getCASPageReference(), PageContainer.getInstance(casPage, casPage));

    final var pathPage = rootPage.getPathPageReference().getPage();
    log.put(rootPage.getPathPageReference(), PageContainer.getInstance(pathPage, pathPage));

    final var pathSummaryPage = rootPage.getPathSummaryPageReference().getPage();
    log.put(rootPage.getPathSummaryPageReference(), PageContainer.getInstance(pathSummaryPage, pathSummaryPage));

    log.put(reference, PageContainer.getInstance(rootPage, rootPage));
  }

  /**
   * Get the page count exponent for the given page.
   *
   * @param pageKind page to lookup the exponent in the constant definition
   * @return page count exponent
   */
  public int[] getPageCountExp(final PageKind pageKind) {
    int[] inpLevelPageCountExp;
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

  /**
   * Get the revision number.
   *
   * @return revision number
   */
  public int getRevision() {
    return revision;
  }

  @Override
  public void commit(final PageTrx pageWriteTrx) {
    delegate.commit(pageWriteTrx);
  }
}
