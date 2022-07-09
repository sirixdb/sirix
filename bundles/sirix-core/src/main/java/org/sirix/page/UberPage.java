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
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jetbrains.annotations.NotNull;
import org.sirix.api.PageTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.index.IndexType;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * The Uber page, the main entry into the resource storage.
 */
public final class UberPage implements Page {

  /**
   * Number of revisions.
   */
  private final int revisionCount;

  /**
   * {@code true} if this uber page is the uber page of a fresh sirix file, {@code false} otherwise.
   */
  private boolean isBootstrap;

  /**
   * {@link RevisionRootPage} instance.
   */
  private RevisionRootPage rootPage;
  private PageReference rootPageReference;

  /**
   * Create uber page.
   */
  public UberPage() {
    revisionCount = Constants.UBP_ROOT_REVISION_COUNT;
    isBootstrap = true;
  }

  /**
   * Read uber page.
   *
   * @param in   input bytes
   */
  protected UberPage(final Bytes<ByteBuffer> in) {
    revisionCount = in.readInt();
    isBootstrap = false;
    rootPage = null;
  }

  /**
   * Clone constructor.
   *
   * @param committedUberPage   page to clone
   */
  public UberPage(final UberPage committedUberPage) {
    if (committedUberPage.isBootstrap()) {
      revisionCount = committedUberPage.revisionCount;
      isBootstrap = committedUberPage.isBootstrap;
      rootPage = committedUberPage.rootPage;
    } else {
      revisionCount = committedUberPage.revisionCount + 1;
      isBootstrap = false;
      rootPage = null;
    }
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
  public void serialize(final Bytes<ByteBuffer> out, final SerializationType type) {
    out.writeInt(revisionCount);
    isBootstrap = false;
  }

  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("forwarding page", super.toString())
                      .add("revisionCount", revisionCount)
                      .add("isBootstrap", isBootstrap)
                      .toString();
  }

  /**
   * Create revision tree.
   *
   * @param log the transaction intent log
   */
  public void createRevisionTree(final TransactionIntentLog log) {
    rootPage = new RevisionRootPage();

    final var namePage = rootPage.getNamePageReference().getPage();
    log.put(rootPage.getNamePageReference(), PageContainer.getInstance(namePage, namePage));

    final var casPage = rootPage.getCASPageReference().getPage();
    log.put(rootPage.getCASPageReference(), PageContainer.getInstance(casPage, casPage));

    final var pathPage = rootPage.getPathPageReference().getPage();
    log.put(rootPage.getPathPageReference(), PageContainer.getInstance(pathPage, pathPage));

    final var pathSummaryPage = rootPage.getPathSummaryPageReference().getPage();
    log.put(rootPage.getPathSummaryPageReference(), PageContainer.getInstance(pathSummaryPage, pathSummaryPage));

    final var deweyIDPage = rootPage.getDeweyIdPageReference().getPage();
    log.put(rootPage.getDeweyIdPageReference(), PageContainer.getInstance(deweyIDPage, deweyIDPage));

    rootPageReference = new PageReference();
    log.put(rootPageReference, PageContainer.getInstance(rootPage, rootPage));
  }

  /**
   * Get the page count exponent for the given page.
   *
   * @param indexType page to lookup the exponent in the constant definition
   * @return page count exponent
   */
  public int[] getPageCountExp(final IndexType indexType) {
    return switch (indexType) {
      case PATH_SUMMARY -> Constants.PATHINP_LEVEL_PAGE_COUNT_EXPONENT;
      case DOCUMENT, CHANGED_NODES, RECORD_TO_REVISIONS, DEWEYID_TO_RECORDID, PATH, CAS, NAME ->
          Constants.INP_LEVEL_PAGE_COUNT_EXPONENT;
      case REVISIONS -> Constants.UBPINP_LEVEL_PAGE_COUNT_EXPONENT;
      // $CASES-OMITTED$
    };
  }

  @Override
  public void commit(final @NotNull PageTrx pageWriteTrx) {
    pageWriteTrx.commit(rootPageReference);
  }

  public UberPage setRevisionRootPage(final RevisionRootPage rootPage) {
    this.rootPage = rootPage;
    return this;
  }

  public UberPage setRevisionRootPageReference(final PageReference pageReference) {
    this.rootPageReference = pageReference;
    return this;
  }

  @Override
  public PageReference getOrCreateReference(@NonNegative int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  public PageReference getRevisionRootReference() {
    return rootPageReference;
  }
}
