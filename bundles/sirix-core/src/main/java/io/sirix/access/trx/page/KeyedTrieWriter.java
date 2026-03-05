/*
 * Copyright (c) 2023, Sirix
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 * and the following disclaimer in the documentation and/or other materials provided with the
 * distribution.
 *
 * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 */

package io.sirix.access.trx.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.IndirectPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.UberPage;
import io.sirix.settings.Constants;
import org.checkerframework.checker.index.qual.NonNegative;

/**
 * Writer for the keyed indirect-page trie structure.
 *
 * <p>
 * Manages write-side navigation and mutation of the trie of {@link IndirectPage}s that maps
 * integer page keys to leaf {@link PageReference}s using bit-decomposition. Handles:
 * </p>
 * <ul>
 *   <li>Preparing a new revision root page from the previous revision (CoW)</li>
 *   <li>Navigating / growing the trie height when keys exceed the current capacity</li>
 *   <li>Copy-on-Write of frozen {@link IndirectPage}s in the transaction intent log</li>
 * </ul>
 *
 * <p>
 * This class is intentionally allocation-free on the traversal hot path (no varargs, no
 * boxing). It is <em>not</em> thread-safe — each write transaction owns its own instance.
 * </p>
 *
 * <p>
 * Note: the factory ({@link StorageEngineWriterFactory}) creates a temporary instance via
 * {@link #preparePreviousRevisionRootPage} before the full {@link NodeStorageEngineWriter} is
 * constructed.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
final class KeyedTrieWriter {

  /**
   * Prepare the previous revision root page and retrieve the next {@link RevisionRootPage}.
   *
   * @param uberPage the uber page
   * @param storageEngineReader the storage engine reader
   * @param log the transaction intent log
   * @param baseRevision base revision
   * @param representRevision the revision to represent
   * @return new {@link RevisionRootPage} instance
   */
  RevisionRootPage preparePreviousRevisionRootPage(final UberPage uberPage,
      final NodeStorageEngineReader storageEngineReader, final TransactionIntentLog log,
      final @NonNegative int baseRevision, final @NonNegative int representRevision) {
    final RevisionRootPage revisionRootPage;

    if (uberPage.isBootstrap()) {
      revisionRootPage = storageEngineReader.loadRevRoot(baseRevision);
    } else {
      // Prepare revision root nodePageReference.
      revisionRootPage =
          new RevisionRootPage(storageEngineReader.loadRevRoot(baseRevision), representRevision + 1);

      // Link the prepared revision root nodePageReference with the prepared indirect tree.
      final var revRootRef = new PageReference().setDatabaseId(storageEngineReader.getDatabaseId())
                                                .setResourceId(storageEngineReader.getResourceId());
      log.put(revRootRef, PageContainer.getInstance(revisionRootPage, revisionRootPage));
    }

    // Return prepared revision root nodePageReference.
    return revisionRootPage;
  }

  /**
   * Prepare the leaf of the trie, navigating through IndirectPages using bit-decomposition.
   *
   * @param storageEngineWriter the storage engine writer (used for trie metadata queries)
   * @param log the transaction intent log
   * @param inpLevelPageCountExp array which holds the maximum number of indirect page references per
   *        trie level
   * @param startReference the reference to start the trie traversal from
   * @param pageKey page key to lookup (decomposed bit-by-bit)
   * @param index the index number or {@code -1} if a regular record page should be prepared
   * @param indexType the index type
   * @param revisionRootPage the revision root page
   * @return {@link PageReference} instance pointing to the leaf page
   */
  PageReference prepareLeafOfTree(final StorageEngineWriter storageEngineWriter,
      final TransactionIntentLog log, final int[] inpLevelPageCountExp,
      final PageReference startReference, @NonNegative final long pageKey, final int index,
      final IndexType indexType, final RevisionRootPage revisionRootPage) {
    // Initial state pointing to the indirect nodePageReference of level 0.
    PageReference reference = startReference;

    int offset;
    long levelKey = pageKey;

    int maxHeight =
        storageEngineWriter.getCurrentMaxIndirectPageTreeLevel(indexType, index, revisionRootPage);

    // Check if we need an additional level of indirect pages.
    if (pageKey == (1L << inpLevelPageCountExp[inpLevelPageCountExp.length - maxHeight - 1])) {
      maxHeight = incrementCurrentMaxIndirectPageTreeLevel(storageEngineWriter, revisionRootPage,
          indexType, index);

      // Add a new indirect page to the top of the trie and to the transaction-log.
      final IndirectPage page = new IndirectPage();

      // Get the first reference.
      final PageReference newReference = page.getOrCreateReference(0);

      newReference.setKey(reference.getKey());
      newReference.setLogKey(reference.getLogKey());
      newReference.setActiveTilGeneration(reference.getActiveTilGeneration());
      newReference.setPage(reference.getPage());
      newReference.setPageFragments(reference.getPageFragments());

      // Create new page reference, add it to the transaction-log and reassign it in the root pages
      // of the trie.
      final PageReference newPageReference =
          new PageReference().setDatabaseId(storageEngineWriter.getDatabaseId())
                             .setResourceId(storageEngineWriter.getResourceId());
      log.put(newPageReference, PageContainer.getInstance(page, page));
      setNewIndirectPage(storageEngineWriter, revisionRootPage, indexType, index, newPageReference);

      reference = newPageReference;
    }

    // Iterate through all levels using bit-decomposition.
    for (int level = inpLevelPageCountExp.length - maxHeight,
        height = inpLevelPageCountExp.length; level < height; level++) {
      offset = (int) (levelKey >> inpLevelPageCountExp[level]);
      levelKey -= (long) offset << inpLevelPageCountExp[level];

      final IndirectPage page = prepareIndirectPage(storageEngineWriter, log, reference);
      reference = page.getOrCreateReference(offset);
    }

    // Return reference to leaf of indirect trie.
    return reference;
  }

  /**
   * Prepare indirect page, that is getting the referenced indirect page or creating a new page and
   * putting the whole path into the log.
   *
   * @param storageEngineReader the storage engine reader
   * @param log the transaction intent log
   * @param reference {@link PageReference} to get the indirect page from or to create a new one
   * @return {@link IndirectPage} reference
   */
  IndirectPage prepareIndirectPage(final StorageEngineReader storageEngineReader,
      final TransactionIntentLog log, final PageReference reference) {
    final PageContainer cont = log.get(reference);
    IndirectPage page = cont == null ? null : (IndirectPage) cont.getComplete();
    if (page == null) {
      if (reference.getKey() == Constants.NULL_ID_LONG) {
        page = new IndirectPage();
      } else {
        final IndirectPage indirectPage =
            storageEngineReader.dereferenceIndirectPageReference(reference);
        page = new IndirectPage(indirectPage);
      }
      log.put(reference, PageContainer.getInstance(page, page));
    } else if (log.isFrozen(reference)) {
      // CoW: frozen IndirectPage — copy to active TIL before modification.
      // IndirectPage copy constructor deep-copies its references array.
      page = new IndirectPage(page);
      log.put(reference, PageContainer.getInstance(page, page));
    }
    return page;
  }

  /**
   * Set a new indirect page in the appropriate index structure.
   */
  private void setNewIndirectPage(final StorageEngineReader storageEngineReader,
      final RevisionRootPage revisionRoot, final IndexType indexType, final int index,
      final PageReference pageReference) {
    // $CASES-OMITTED$
    switch (indexType) {
      case DOCUMENT -> revisionRoot.setOrCreateReference(0, pageReference);
      case CHANGED_NODES -> revisionRoot.setOrCreateReference(1, pageReference);
      case RECORD_TO_REVISIONS -> revisionRoot.setOrCreateReference(2, pageReference);
      case CAS ->
        storageEngineReader.getCASPage(revisionRoot).setOrCreateReference(index, pageReference);
      case PATH ->
        storageEngineReader.getPathPage(revisionRoot).setOrCreateReference(index, pageReference);
      case NAME ->
        storageEngineReader.getNamePage(revisionRoot).setOrCreateReference(index, pageReference);
      case PATH_SUMMARY -> storageEngineReader.getPathSummaryPage(revisionRoot)
                                              .setOrCreateReference(index, pageReference);
      default -> throw new IllegalStateException(
          "Only defined for node, path summary, text value and attribute value pages!");
    }
  }

  /**
   * Increment the current maximum indirect page trie level for the given index type.
   */
  private int incrementCurrentMaxIndirectPageTreeLevel(
      final StorageEngineReader storageEngineReader, final RevisionRootPage revisionRoot,
      final IndexType indexType, final int index) {
    // $CASES-OMITTED$
    return switch (indexType) {
      case DOCUMENT -> revisionRoot.incrementAndGetCurrentMaxLevelOfDocumentIndexIndirectPages();
      case CHANGED_NODES ->
        revisionRoot.incrementAndGetCurrentMaxLevelOfChangedNodesIndexIndirectPages();
      case RECORD_TO_REVISIONS ->
        revisionRoot.incrementAndGetCurrentMaxLevelOfRecordToRevisionsIndexIndirectPages();
      case CAS -> storageEngineReader.getCASPage(revisionRoot)
                                     .incrementAndGetCurrentMaxLevelOfIndirectPages(index);
      case PATH -> storageEngineReader.getPathPage(revisionRoot)
                                      .incrementAndGetCurrentMaxLevelOfIndirectPages(index);
      case NAME -> storageEngineReader.getNamePage(revisionRoot)
                                      .incrementAndGetCurrentMaxLevelOfIndirectPages(index);
      case PATH_SUMMARY -> storageEngineReader.getPathSummaryPage(revisionRoot)
                                              .incrementAndGetCurrentMaxLevelOfIndirectPages(index);
      default -> throw new IllegalStateException(
          "Only defined for node, path summary, text value and attribute value pages!");
    };
  }
}
