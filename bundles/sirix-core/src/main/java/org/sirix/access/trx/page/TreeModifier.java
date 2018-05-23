/**
 * Copyright (c) 2018, Sirix
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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.Nonnegative;
import org.sirix.access.trx.node.XdmResourceManager;
import org.sirix.api.PageReadTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.PersistentFileCache;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.exception.SirixIOException;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.file.FileWriter;
import org.sirix.page.IndirectPage;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.SerializationType;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Constants;

/**
 * @author Johannes Lichtenberger <lichtenberger.johannes@gmail.com>
 *
 */
public final class TreeModifier {

  /**
   * Package private constructor.
   */
  TreeModifier() {}

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
  RevisionRootPage preparePreviousRevisionRootPage(final UberPage uberPage,
      final PageReadTrxImpl pageRtx, final TransactionIntentLog log,
      final @Nonnegative int baseRevision, final @Nonnegative int representRevision) {
    if (uberPage.isBootstrap()) {
      final RevisionRootPage revisionRootPage = pageRtx.loadRevRoot(baseRevision);
      return revisionRootPage;
    } else {
      // Prepare revision root nodePageReference.
      final RevisionRootPage revisionRootPage =
          new RevisionRootPage(pageRtx.loadRevRoot(baseRevision), representRevision + 1);

      // Prepare indirect tree to hold reference to prepared revision root nodePageReference.
      final PageReference revisionRootPageReference = prepareLeafOfTree(
          pageRtx, log, uberPage.getPageCountExp(PageKind.UBERPAGE),
          uberPage.getIndirectPageReference(), uberPage.getRevisionNumber(), -1, PageKind.UBERPAGE);

      // Link the prepared revision root nodePageReference with the prepared indirect tree.
      log.put(revisionRootPageReference, new PageContainer(revisionRootPage, revisionRootPage));

      // Return prepared revision root nodePageReference.
      return revisionRootPage;
    }
  }

  /**
   * Prepare the leaf of a tree, namely the reference to a {@link UnorderedKeyValuePage} and put the
   * whole path into the log.
   *
   * @param pageRtx the page reading transaction
   * @param log the transaction intent log
   * @param uberPage the uber page
   * @param startReference start reference
   * @param key page key to lookup
   * @param index the index number or {@code -1} if a regular record page should be prepared
   * @return {@link PageReference} instance pointing to the right {@link UnorderedKeyValuePage} with
   *         the {@code key}
   * @throws SirixIOException if an I/O error occured
   */
  PageReference prepareLeafOfTree(final PageReadTrx pageRtx, final TransactionIntentLog log,
      final int[] inpLevelPageCountExp, final PageReference startReference,
      final @Nonnegative long key, final int index, final PageKind pageKind) {
    // Initial state pointing to the indirect nodePageReference of level 0.
    PageReference reference = startReference;
    int offset = 0;
    long levelKey = key;

    // Iterate through all levels.
    for (int level = 0, height = inpLevelPageCountExp.length; level < height; level++) {
      offset = (int) (levelKey >> inpLevelPageCountExp[level]);
      levelKey -= offset << inpLevelPageCountExp[level];
      final IndirectPage page = prepareIndirectPage(pageRtx, log, reference);
      reference = page.getReference(offset);
    }

    // Return reference to leaf of indirect tree.
    return reference;
  }

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
  IndirectPage prepareIndirectPage(final PageReadTrx pageRtx, final TransactionIntentLog log,
      final PageReference reference) {
    final PageContainer cont = log.get(reference, pageRtx);
    IndirectPage page = cont == null
        ? null
        : (IndirectPage) cont.getComplete();
    if (page == null) {
      if (reference.getKey() == Constants.NULL_ID_LONG) {
        page = new IndirectPage();
      } else {
        final IndirectPage indirectPage = pageRtx.dereferenceIndirectPageReference(reference);
        page = new IndirectPage(indirectPage);
      }
      log.put(reference, new PageContainer(page, page));
    }
    return page;
  }

  TransactionIntentLog createTrxIntentLog(final XdmResourceManager resourceManager) {
    final Path logFile =
        resourceManager.getResourceConfig().mPath.resolve("log").resolve("intent-log");

    try {
      if (Files.exists(logFile)) {
        Files.delete(logFile);
        Files.createFile(logFile);
      }

      final RandomAccessFile file = new RandomAccessFile(logFile.toFile(), "rw");

      final FileWriter fileWriter = new FileWriter(file, null,
          new ByteHandlePipeline(resourceManager.getResourceConfig().mByteHandler),
          SerializationType.TRANSACTION_INTENT_LOG);

      final PersistentFileCache persistentFileCache = new PersistentFileCache(fileWriter);

      return new TransactionIntentLog(persistentFileCache);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
