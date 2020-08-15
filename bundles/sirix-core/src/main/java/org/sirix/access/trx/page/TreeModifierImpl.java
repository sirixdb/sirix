/*
 * Copyright (c) 2020, SirixDB
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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
package org.sirix.access.trx.page;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.page.*;
import org.sirix.settings.Constants;

import javax.annotation.Nonnegative;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class TreeModifierImpl implements TreeModifier {

  /**
   * Package private constructor.
   */
  TreeModifierImpl() {
  }

  @Override
  public RevisionRootPage preparePreviousRevisionRootPage(final UberPage uberPage, final NodePageReadOnlyTrx pageRtx,
      final TransactionIntentLog log, final @Nonnegative int baseRevision, final @Nonnegative int representRevision) {
    final RevisionRootPage revisionRootPage;
    if (uberPage.isBootstrap()) {
      revisionRootPage = pageRtx.loadRevRoot(baseRevision);
    } else {
      // Prepare revision root nodePageReference.
      revisionRootPage = new RevisionRootPage(pageRtx.loadRevRoot(baseRevision), representRevision + 1);

      // Prepare indirect tree to hold reference to prepared revision root nodePageReference.
      final PageReference revisionRootPageReference = prepareLeafOfTree(pageRtx, log,
          uberPage.getPageCountExp(PageKind.UBERPAGE), uberPage.getIndirectPageReference(),
          uberPage.getRevisionNumber(), -1, PageKind.UBERPAGE, revisionRootPage);

      // Link the prepared revision root nodePageReference with the prepared
      // indirect tree.
      log.put(revisionRootPageReference, PageContainer.getInstance(revisionRootPage, revisionRootPage));

      // Return prepared revision root nodePageReference.
    }
    return revisionRootPage;
  }

  @Override
  public PageReference prepareLeafOfTree(final PageReadOnlyTrx pageRtx, final TransactionIntentLog log,
      final int[] inpLevelPageCountExp, final PageReference startReference, @Nonnegative final long pageKey,
      final int index, final PageKind pageKind, final RevisionRootPage revisionRootPage) {
    // Initial state pointing to the indirect nodePageReference of level 0.
    PageReference reference = startReference;

    int offset;
    long levelKey = pageKey;

    int maxHeight = pageRtx.getCurrentMaxIndirectPageTreeLevel(pageKind, index, revisionRootPage);

    // Check if we need an additional level of indirect pages.
    if (pageKey == (1L << inpLevelPageCountExp[inpLevelPageCountExp.length - maxHeight - 1])) {
      maxHeight = incrementCurrentMaxIndirectPageTreeLevel(pageRtx, revisionRootPage, pageKind, index);

      // First, get the old referenced page.
      final IndirectPage oldPage = dereferenceOldIndirectPage(pageRtx, log, reference);

      // Add a new indirect page to the top of the tree and to the transaction-log.
      final IndirectPage page = new IndirectPage();

      // Get the first reference.
      final PageReference newReference = page.getOrCreateReference(0);

      // Set new reference in log with the old referenced page.
      log.put(newReference, PageContainer.getInstance(oldPage, oldPage));

      // Create new page reference, add it to the transaction-log and reassign it in the root pages
      // of the tree.
      final PageReference newPageReference = new PageReference();
      log.put(newPageReference, PageContainer.getInstance(page, page));
      setNewIndirectPage(pageRtx, revisionRootPage, pageKind, index, newPageReference);

      reference = newPageReference;
    }

    // Iterate through all levels.
    for (int level = inpLevelPageCountExp.length - maxHeight, height = inpLevelPageCountExp.length; level < height;
        level++) {
      offset = (int) (levelKey >> inpLevelPageCountExp[level]);
      levelKey -= offset << inpLevelPageCountExp[level];
      final IndirectPage page = prepareIndirectPage(pageRtx, log, reference);
      reference = page.getOrCreateReference(offset);
    }

    // Return reference to leaf of indirect tree.
    return reference;
  }

  private IndirectPage dereferenceOldIndirectPage(final PageReadOnlyTrx pageRtx, final TransactionIntentLog log,
      PageReference reference) throws AssertionError {
    final PageContainer cont = log.get(reference, pageRtx);
    IndirectPage oldPage = cont == null ? null : (IndirectPage) cont.getComplete();
    if (oldPage == null) {
      if (reference.getKey() == Constants.NULL_ID_LONG) {
        throw new AssertionError("The referenced page on top must of our tree must exist (first IndirectPage).");
      } else {
        final IndirectPage indirectPage = pageRtx.dereferenceIndirectPageReference(reference);
        oldPage = new IndirectPage(indirectPage);
      }
    }
    return oldPage;
  }

  private void setNewIndirectPage(final PageReadOnlyTrx pageRtx, final RevisionRootPage revisionRoot,
      final PageKind pageKind, final int index, final PageReference pageReference) {
    // $CASES-OMITTED$
    switch (pageKind) {
      case RECORDPAGE -> revisionRoot.setOrCreateReference(0, pageReference);
      case UBERPAGE -> pageRtx.getUberPage().setOrCreateReference(0, pageReference);
      case CASPAGE -> pageRtx.getCASPage(revisionRoot).setOrCreateReference(index, pageReference);
      case PATHPAGE -> pageRtx.getPathPage(revisionRoot).setOrCreateReference(index, pageReference);
      case NAMEPAGE -> pageRtx.getNamePage(revisionRoot).setOrCreateReference(index, pageReference);
      case PATHSUMMARYPAGE -> pageRtx.getPathSummaryPage(revisionRoot).setOrCreateReference(index, pageReference);
      default -> throw new IllegalStateException(
          "Only defined for node, path summary, text value and attribute value pages!");
    }
  }

  private int incrementCurrentMaxIndirectPageTreeLevel(final PageReadOnlyTrx pageRtx,
      final RevisionRootPage revisionRoot, final PageKind pageKind, final int index) {
    // $CASES-OMITTED$
    return switch (pageKind) {
      case UBERPAGE -> pageRtx.getUberPage().incrementAndGetCurrentMaxLevelOfIndirectPages();
      case RECORDPAGE -> revisionRoot.incrementAndGetCurrentMaxLevelOfIndirectPages();
      case CASPAGE -> pageRtx.getCASPage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
      case PATHPAGE -> pageRtx.getPathPage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
      case NAMEPAGE -> pageRtx.getNamePage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
      case PATHSUMMARYPAGE -> pageRtx.getPathSummaryPage(revisionRoot)
                                     .incrementAndGetCurrentMaxLevelOfIndirectPages(index);
      default -> throw new IllegalStateException(
          "Only defined for node, path summary, text value and attribute value pages!");
    };
  }

  @Override
  public IndirectPage prepareIndirectPage(final PageReadOnlyTrx pageRtx, final TransactionIntentLog log,
      final PageReference reference) {
    final PageContainer cont = log.get(reference, pageRtx);
    IndirectPage page = cont == null ? null : (IndirectPage) cont.getComplete();
    if (page == null) {
      if (reference.getKey() == Constants.NULL_ID_LONG) {
        page = new IndirectPage();
      } else {
        final IndirectPage indirectPage = pageRtx.dereferenceIndirectPageReference(reference);
        page = new IndirectPage(indirectPage);
      }
      log.put(reference, PageContainer.getInstance(page, page));
    }
    return page;
  }
}
