/*
 * Copyright (c) 2023, Sirix
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
package io.sirix.access.trx.page;

import io.sirix.api.PageReadOnlyTrx;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.page.IndirectPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.UberPage;
import org.checkerframework.checker.index.qual.NonNegative;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 *
 */
public interface TreeModifier {
  /**
   * Prepare the previous revision root page and retrieve the next {@link RevisionRootPage}.
   *
   * @param uberPage the uber page
   * @param pageRtx the page reading transaction
   * @param baseRevision base revision
   * @param representRevision the revision to represent
   * @return new {@link RevisionRootPage} instance
   * @throws SirixIOException if an I/O error occurs
   */
  RevisionRootPage preparePreviousRevisionRootPage(UberPage uberPage, NodePageReadOnlyTrx pageRtx,
      TransactionIntentLog log, @NonNegative int baseRevision, @NonNegative int representRevision);

  /**
   * Prepare the leaf of a tree, namely the reference to a {@link KeyValueLeafPage} and put the
   * whole path into the log.
   *
   * @param pageRtx the page reading transaction
   * @param log the transaction intent log
   * @param inpLevelPageCountExp array which holds the maximum number of indirect page references per
   *        tree-level
   * @param startReference the reference to start the tree traversal from
   * @param pageKey page key to lookup
   * @param indexNumber the index number or {@code -1} if a regular record page should be prepared
   * @param indexType the index type
   * @param revisionRootPage the revision root page
   * @return {@link PageReference} instance pointing to the right {@link KeyValueLeafPage} with
   *         the {@code key}
   * @throws SirixIOException if an I/O error occured
   */
  PageReference prepareLeafOfTree(PageReadOnlyTrx pageRtx, TransactionIntentLog log, int[] inpLevelPageCountExp,
      PageReference startReference, @NonNegative long pageKey, int indexNumber, IndexType indexType,
      RevisionRootPage revisionRootPage);

  /**
   * Prepare indirect page, that is getting the referenced indirect page or a new page and put the
   * whole path into the log.
   *
   * @param pageRtx the page reading transaction
   * @param log the transaction intent log
   * @param reference {@link PageReference} to get the indirect page from or to create a new one
   * @return {@link IndirectPage} reference
   * @throws SirixIOException if an I/O error occurs
   */
  IndirectPage prepareIndirectPage(PageReadOnlyTrx pageRtx, TransactionIntentLog log, PageReference reference);
}
