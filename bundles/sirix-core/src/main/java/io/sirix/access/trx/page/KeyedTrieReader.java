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
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.page.IndirectPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Reader for the keyed indirect-page trie structure.
 *
 * <p>
 * Provides read-only navigation of the trie of {@link IndirectPage}s that maps integer page keys
 * to leaf {@link PageReference}s using bit-decomposition. This is the read-side counterpart of
 * {@link KeyedTrieWriter}.
 * </p>
 *
 * <p>
 * The trie is a fixed-fanout tree of {@link IndirectPage}s whose height grows as record keys
 * increase. Each level decomposes a portion of the page key's bits to select the child slot.
 * </p>
 *
 * <p>
 * This class is allocation-free on the traversal hot path. It is <em>not</em> thread-safe — each
 * read transaction owns its own instance.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
final class KeyedTrieReader {

  /**
   * Find the reference pointing to the leaf page of the indirect trie.
   *
   * @param storageEngineReader the storage engine reader (for page dereferencing and trie metadata)
   * @param uberPage the uber page (provides level-count exponents)
   * @param startReference start reference pointing to the indirect tree root
   * @param pageKey key to look up in the indirect tree (decomposed bit-by-bit)
   * @param indexNumber the index number or {@code -1}
   * @param indexType the index type
   * @param revisionRootPage the revision root page
   * @return reference denoted by key pointing to the leaf page, or {@code null} if not found
   * @throws SirixIOException if an I/O error occurs
   */
  @Nullable
  PageReference getReferenceToLeafOfSubtree(final StorageEngineReader storageEngineReader,
      final UberPage uberPage, final PageReference startReference,
      @NonNegative final long pageKey, final int indexNumber,
      final @NonNull IndexType indexType, final RevisionRootPage revisionRootPage) {
    // Initial state pointing to the indirect page of level 0.
    PageReference reference = requireNonNull(startReference);
    int offset;
    long levelKey = pageKey;
    final int[] inpLevelPageCountExp = uberPage.getPageCountExp(indexType);
    final int maxHeight =
        storageEngineReader.getCurrentMaxIndirectPageTreeLevel(indexType, indexNumber,
            revisionRootPage);

    // Iterate through all levels.
    for (int level = inpLevelPageCountExp.length - maxHeight,
        height = inpLevelPageCountExp.length; level < height; level++) {
      final Page derefPage = storageEngineReader.dereferenceIndirectPageReference(reference);
      if (derefPage == null) {
        reference = null;
        break;
      } else {
        offset = (int) (levelKey >> inpLevelPageCountExp[level]);
        levelKey -= (long) offset << inpLevelPageCountExp[level];

        try {
          reference = derefPage.getOrCreateReference(offset);
        } catch (final IndexOutOfBoundsException e) {
          throw new SirixIOException("Node key isn't supported, it's too big!");
        }
      }
    }

    // Return reference to leaf of indirect tree.
    return reference;
  }
}
