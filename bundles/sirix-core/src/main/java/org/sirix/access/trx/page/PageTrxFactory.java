/*
 * Copyright (c) 2018, Sirix
 * <p>
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the <organization> nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * <p>
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

import org.brackit.xquery.xdm.DocumentException;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.IndexController;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.access.trx.node.xml.XmlResourceManagerImpl;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.PageTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.io.Writer;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

import javax.annotation.Nonnegative;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Page transaction factory.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class PageTrxFactory {

  /**
   * Create a page write trx.
   *
   * @param resourceManager    {@link XmlResourceManagerImpl} this page write trx is bound to
   * @param uberPage           root of revision
   * @param writer             writer where this transaction should write to
   * @param trxId              the transaction ID
   * @param representRevision  revision represent
   * @param lastStoredRevision last stored revision
   * @param isBoundToNodeTrx   {@code true} if this page write trx will be bound to a node trx,
   *                           {@code false} otherwise
   */
  public PageTrx createPageTrx(
      final InternalResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx> resourceManager,
      final UberPage uberPage, final Writer writer, final @Nonnegative long trxId,
      final @Nonnegative int representRevision, final @Nonnegative int lastStoredRevision,
      final @Nonnegative int lastCommitedRevision, final boolean isBoundToNodeTrx) {
    final boolean usePathSummary = resourceManager.getResourceConfig().withPathSummary;
    final IndexController<?, ?> indexController = resourceManager.getWtxIndexController(representRevision);

    // Deserialize index definitions.
    final Path indexes =
        resourceManager.getResourceConfig().resourcePath.resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
                                                        .resolve(lastStoredRevision + ".xml");
    if (Files.exists(indexes)) {
      try (final InputStream in = new FileInputStream(indexes.toFile())) {
        indexController.getIndexes().init(IndexController.deserialize(in).getFirstChild());
      } catch (IOException | DocumentException | SirixException e) {
        throw new SirixIOException("Index definitions couldn't be deserialized!", e);
      }
    }

    final TreeModifierImpl treeModifier = new TreeModifierImpl();
    final TransactionIntentLogFactory logFactory = new TransactionIntentLogFactoryImpl();
    final TransactionIntentLog log = logFactory.createTrxIntentLog(resourceManager.getResourceConfig());

    // Create revision tree if needed.
    if (uberPage.isBootstrap()) {
      uberPage.createRevisionTree(log);
    }

    // Page read trx.
    final NodePageReadOnlyTrx pageRtx = new NodePageReadOnlyTrx(trxId,
                                                                resourceManager,
                                                                uberPage,
                                                                representRevision,
                                                                writer,
                                                                log,
                                                                null,
                                                                new RevisionRootPageReader());

    // Create new revision root page.
    final RevisionRootPage lastCommitedRoot = pageRtx.loadRevRoot(lastCommitedRevision);
    final RevisionRootPage newRevisionRootPage =
        treeModifier.preparePreviousRevisionRootPage(uberPage, pageRtx, log, representRevision, lastStoredRevision);
    newRevisionRootPage.setMaxNodeKeyInDocumentIndex(lastCommitedRoot.getMaxNodeKeyInDocumentIndex());
    newRevisionRootPage.setMaxNodeKeyInInChangedNodesIndex(lastCommitedRoot.getMaxNodeKeyInChangedNodesIndex());
    newRevisionRootPage.setMaxNodeKeyInRecordToRevisionsIndex(lastCommitedRoot.getMaxNodeKeyInRecordToRevisionsIndex());

    // First create revision tree if needed.
    newRevisionRootPage.createDocumentIndexTree(pageRtx, log);

    if (usePathSummary) {
      // Create path summary tree if needed.
      final PathSummaryPage page = pageRtx.getPathSummaryPage(newRevisionRootPage);

      page.createPathSummaryTree(pageRtx, 0, log);

      if (PageContainer.emptyInstance().equals(log.get(newRevisionRootPage.getPathSummaryPageReference(), pageRtx))) {
        log.put(newRevisionRootPage.getPathSummaryPageReference(), PageContainer.getInstance(page, page));
      }
    }

    if (uberPage.isBootstrap()) {
      final NamePage namePage = pageRtx.getNamePage(newRevisionRootPage);
      final DeweyIDPage deweyIDPage = pageRtx.getDeweyIDPage(newRevisionRootPage);

      if (resourceManager instanceof JsonResourceManager) {
        namePage.createNameIndexTree(pageRtx, NamePage.JSON_OBJECT_KEY_REFERENCE_OFFSET, log);
        deweyIDPage.createIndexTree(pageRtx, log);
      } else if (resourceManager instanceof XmlResourceManager) {
        namePage.createNameIndexTree(pageRtx, NamePage.ATTRIBUTES_REFERENCE_OFFSET, log);
        namePage.createNameIndexTree(pageRtx, NamePage.ELEMENTS_REFERENCE_OFFSET, log);
        namePage.createNameIndexTree(pageRtx, NamePage.NAMESPACE_REFERENCE_OFFSET, log);
        namePage.createNameIndexTree(pageRtx, NamePage.PROCESSING_INSTRUCTION_REFERENCE_OFFSET, log);
        deweyIDPage.createIndexTree(pageRtx, log);
      } else {
        throw new IllegalStateException("Resource manager type not known.");
      }
    } else {
      if (PageContainer.emptyInstance().equals(log.get(newRevisionRootPage.getNamePageReference(), pageRtx))) {
        final Page namePage = pageRtx.getNamePage(newRevisionRootPage);
        log.put(newRevisionRootPage.getNamePageReference(), PageContainer.getInstance(namePage, namePage));
      }

      if (PageContainer.emptyInstance().equals(log.get(newRevisionRootPage.getCASPageReference(), pageRtx))) {
        final Page casPage = pageRtx.getCASPage(newRevisionRootPage);
        log.put(newRevisionRootPage.getCASPageReference(), PageContainer.getInstance(casPage, casPage));
      }

      if (PageContainer.emptyInstance().equals(log.get(newRevisionRootPage.getPathPageReference(), pageRtx))) {
        final Page pathPage = pageRtx.getPathPage(newRevisionRootPage);
        log.put(newRevisionRootPage.getPathPageReference(), PageContainer.getInstance(pathPage, pathPage));
      }

      if (PageContainer.emptyInstance().equals(log.get(newRevisionRootPage.getDeweyIdPageReference(), pageRtx))) {
        final Page deweyIDPage = pageRtx.getDeweyIDPage(newRevisionRootPage);
        log.put(newRevisionRootPage.getDeweyIdPageReference(), PageContainer.getInstance(deweyIDPage, deweyIDPage));
      }

      final Page indirectPage =
          pageRtx.dereferenceIndirectPageReference(newRevisionRootPage.getIndirectDocumentIndexPageReference());
      log.put(newRevisionRootPage.getIndirectDocumentIndexPageReference(), PageContainer.getInstance(indirectPage, indirectPage));

      final PageReference revisionRootPageReference = treeModifier.prepareLeafOfTree(pageRtx,
                                                                                     log,
                                                                                     uberPage.getPageCountExp(IndexType.REVISIONS),
                                                                                     uberPage.getIndirectPageReference(),
                                                                                     uberPage.getRevisionNumber(),
                                                                                     -1,
                                                                                     IndexType.REVISIONS,
                                                                                     newRevisionRootPage);

      log.put(revisionRootPageReference, PageContainer.getInstance(newRevisionRootPage, newRevisionRootPage));
    }

    return new NodePageTrx(treeModifier,
                           writer,
                           log,
                           newRevisionRootPage,
                           pageRtx,
                           indexController,
                           representRevision,
                           isBoundToNodeTrx);
  }
}
