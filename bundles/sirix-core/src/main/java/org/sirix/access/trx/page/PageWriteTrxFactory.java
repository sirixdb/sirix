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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.brackit.xquery.xdm.DocumentException;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.trx.node.IndexController;
import org.sirix.access.trx.node.XdmResourceManager;
import org.sirix.api.PageWriteTrx;
import org.sirix.cache.BufferManager;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Writer;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.page.interfaces.Page;

/**
 * Page Write transaction factory.
 *
 * @author Johannes Lichtenberger <lichtenberger.johannes@gmail.com>
 */
public final class PageWriteTrxFactory {

  /**
   * Create a page write trx.
   *
   * @param resourceManager {@link XdmResourceManager} this page write trx is bound to
   * @param uberPage root of revision
   * @param writer writer where this transaction should write to
   * @param trxId the transaction ID
   * @param representRev revision represent
   * @param lastStoredRev last stored revision
   * @param bufferManager the page cache buffer
   */
  public PageWriteTrx<Long, Record, UnorderedKeyValuePage> createPageWriteTrx(
      final XdmResourceManager resourceManager, final UberPage uberPage, final Writer writer,
      final @Nonnegative long trxId, final @Nonnegative int representRev,
      final @Nonnegative int lastStoredRev, final @Nonnegative int lastCommitedRev,
      final @Nonnull BufferManager bufferManager) {
    final boolean usePathSummary = resourceManager.getResourceConfig().mPathSummary;
    final IndexController indexController = resourceManager.getWtxIndexController(representRev);

    // Deserialize index definitions.
    final Path indexes = resourceManager.getResourceConfig().mPath.resolve(
        ResourceConfiguration.ResourcePaths.INDEXES.getPath()).resolve(
            String.valueOf(lastStoredRev) + ".xml");
    if (Files.exists(indexes)) {
      try (final InputStream in = new FileInputStream(indexes.toFile())) {
        indexController.getIndexes().init(IndexController.deserialize(in).getFirstChild());
      } catch (IOException | DocumentException | SirixException e) {
        throw new SirixIOException("Index definitions couldn't be deserialized!", e);
      }
    }

    final TreeModifierImpl treeModifier = new TreeModifierImpl();

    final TransactionIntentLogFactory logFactory = new TransactionIntentLogFactoryImpl();

    final TransactionIntentLog log =
        logFactory.createTrxIntentLog(resourceManager.getResourceConfig());

    // Create revision tree if needed.
    if (uberPage.isBootstrap()) {
      uberPage.createRevisionTree(log);
    }

    // Page read trx.
    final PageReadTrxImpl pageRtx = new PageReadTrxImpl(trxId, resourceManager, uberPage,
        representRev, writer, log, indexController, bufferManager);

    // Create new revision root page.
    final RevisionRootPage lastCommitedRoot = pageRtx.loadRevRoot(lastCommitedRev);
    final RevisionRootPage newRevisionRootPage = treeModifier.preparePreviousRevisionRootPage(
        uberPage, pageRtx, log, representRev, lastStoredRev);
    newRevisionRootPage.setMaxNodeKey(lastCommitedRoot.getMaxNodeKey());

    // First create revision tree if needed.
    newRevisionRootPage.createNodeTree(pageRtx, log);

    if (usePathSummary) {
      // Create path summary tree if needed.
      final PathSummaryPage page = pageRtx.getPathSummaryPage(newRevisionRootPage);

      page.createPathSummaryTree(pageRtx, 0, log);

      if (PageContainer.emptyInstance()
                       .equals(log.get(newRevisionRootPage.getPathSummaryPageReference(), pageRtx)))
        log.put(newRevisionRootPage.getPathSummaryPageReference(), new PageContainer(page, page));
    }

    if (!uberPage.isBootstrap()) {
      if (PageContainer.emptyInstance()
                       .equals(log.get(newRevisionRootPage.getNamePageReference(), pageRtx))) {
        final Page namePage = pageRtx.getNamePage(newRevisionRootPage);
        log.put(newRevisionRootPage.getNamePageReference(), new PageContainer(namePage, namePage));
      }

      if (PageContainer.emptyInstance()
                       .equals(log.get(newRevisionRootPage.getCASPageReference(), pageRtx))) {
        final Page casPage = pageRtx.getCASPage(newRevisionRootPage);
        log.put(newRevisionRootPage.getCASPageReference(), new PageContainer(casPage, casPage));
      }

      if (PageContainer.emptyInstance()
                       .equals(log.get(newRevisionRootPage.getPathPageReference(), pageRtx))) {
        final Page pathPage = pageRtx.getPathPage(newRevisionRootPage);
        log.put(newRevisionRootPage.getPathPageReference(), new PageContainer(pathPage, pathPage));
      }

      final Page indirectPage =
          pageRtx.dereferenceIndirectPageReference(newRevisionRootPage.getIndirectPageReference());
      log.put(
          newRevisionRootPage.getIndirectPageReference(),
          new PageContainer(indirectPage, indirectPage));

      final PageReference revisionRootPageReference = treeModifier.prepareLeafOfTree(
          pageRtx, log, uberPage.getPageCountExp(PageKind.UBERPAGE),
          uberPage.getIndirectPageReference(), uberPage.getRevisionNumber(), -1, PageKind.UBERPAGE);

      log.put(
          revisionRootPageReference, new PageContainer(newRevisionRootPage, newRevisionRootPage));
    }

    return new PageWriteTrxImpl(treeModifier, writer, log, newRevisionRootPage, pageRtx,
        indexController);
  }
}
