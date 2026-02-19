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
package io.sirix.access.trx.page;

import io.sirix.access.DatabaseType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.page.*;
import io.sirix.cache.BufferManager;
import io.brackit.query.jdm.DocumentException;
import org.checkerframework.checker.index.qual.NonNegative;
import io.sirix.access.trx.node.xml.XmlResourceSessionImpl;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.io.Writer;
import io.sirix.page.interfaces.Page;

import javax.inject.Inject;
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
public final class StorageEngineWriterFactory {

  private final DatabaseType databaseType;

  @Inject
  public StorageEngineWriterFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  /**
   * Create a page write trx.
   *
   * @param resourceManager {@link XmlResourceSessionImpl} this page write trx is bound to
   * @param uberPage root of revision
   * @param writer writer where this transaction should write to
   * @param trxId the transaction ID
   * @param representRevision revision represent
   * @param lastStoredRevision last stored revision
   * @param isBoundToNodeTrx {@code true} if this page write trx will be bound to a node trx,
   *        {@code false} otherwise
   */
  public StorageEngineWriter createPageTrx(
      final InternalResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> resourceManager,
      final UberPage uberPage, final Writer writer, final @NonNegative int trxId,
      final @NonNegative int representRevision, final @NonNegative int lastStoredRevision,
      final @NonNegative int lastCommitedRevision, final boolean isBoundToNodeTrx, final BufferManager bufferManager) {
    final ResourceConfiguration resourceConfig = resourceManager.getResourceConfig();
    final boolean usePathSummary = resourceConfig.withPathSummary;
    // Use representRevision + 1 because that's the NEW revision being created.
    // The node transaction will use trx.getRevisionNumber() which returns the new revision,
    // so we need to use the same revision for the index controller to ensure they share state.
    final int newRevisionNumber = representRevision + 1;
    final IndexController<?, ?> indexController = resourceManager.getWtxIndexController(newRevisionNumber);

    // Deserialize index definitions.
    final Path indexes = resourceConfig.resourcePath.resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
                                                    .resolve(lastStoredRevision + ".xml");
    if (Files.exists(indexes)) {
      try (final InputStream in = new FileInputStream(indexes.toFile())) {
        indexController.getIndexes().init(IndexController.deserialize(in).getFirstChild());
      } catch (IOException | DocumentException | SirixException e) {
        throw new SirixIOException("Index definitions couldn't be deserialized!", e);
      }
    }

    final TransactionIntentLogFactory logFactory = new TransactionIntentLogFactoryImpl();
    final TransactionIntentLog log = logFactory.createTrxIntentLog(bufferManager, resourceConfig);

    // Create revision tree if needed. Note: This must happen before the page read trx is created.
    if (uberPage.isBootstrap()) {
      uberPage.createRevisionTree(log);
    }

    // Page read trx.
    final NodeStorageEngineReader storageEngineReader = new NodeStorageEngineReader(trxId, resourceManager, uberPage,
        representRevision, writer, bufferManager, new RevisionRootPageReader(), log);

    // Create new revision root page.
    final RevisionRootPage lastCommitedRoot = storageEngineReader.loadRevRoot(lastCommitedRevision);
    // Use temporary IndirectPageTrieWriter to prepare revision root (moved into NodeStorageEngineWriter
    // in Phase 1)
    final var tempTrieWriter = new NodeStorageEngineWriter.TrieWriter();
    final RevisionRootPage newRevisionRootPage =
        tempTrieWriter.preparePreviousRevisionRootPage(uberPage, storageEngineReader, log, representRevision, lastStoredRevision);
    newRevisionRootPage.setMaxNodeKeyInDocumentIndex(lastCommitedRoot.getMaxNodeKeyInDocumentIndex());
    newRevisionRootPage.setMaxNodeKeyInInChangedNodesIndex(lastCommitedRoot.getMaxNodeKeyInChangedNodesIndex());
    if (resourceConfig.storeNodeHistory()) {
      newRevisionRootPage.setMaxNodeKeyInRecordToRevisionsIndex(
          lastCommitedRoot.getMaxNodeKeyInRecordToRevisionsIndex());
    }

    // First create revision tree if needed.
    newRevisionRootPage.createDocumentIndexTree(this.databaseType, storageEngineReader, log);
    newRevisionRootPage.createChangedNodesIndexTree(this.databaseType, storageEngineReader, log);

    if (resourceConfig.storeNodeHistory()) {
      newRevisionRootPage.createRecordToRevisionsIndexTree(this.databaseType, storageEngineReader, log);
    }

    if (usePathSummary) {
      // Create path summary tree if needed.
      final PathSummaryPage page = storageEngineReader.getPathSummaryPage(newRevisionRootPage);

      page.createPathSummaryTree(this.databaseType, storageEngineReader, 0, log);

      if (log.get(newRevisionRootPage.getPathSummaryPageReference()) == null) {
        log.put(newRevisionRootPage.getPathSummaryPageReference(), PageContainer.getInstance(page, page));
      }
    }

    if (uberPage.isBootstrap()) {
      final NamePage namePage = storageEngineReader.getNamePage(newRevisionRootPage);
      final DeweyIDPage deweyIDPage = storageEngineReader.getDeweyIDPage(newRevisionRootPage);

      if (resourceManager instanceof JsonResourceSession) {
        namePage.createNameIndexTree(this.databaseType, storageEngineReader, NamePage.JSON_OBJECT_KEY_REFERENCE_OFFSET, log);
        deweyIDPage.createIndexTree(this.databaseType, storageEngineReader, log);
      } else if (resourceManager instanceof XmlResourceSession) {
        namePage.createNameIndexTree(this.databaseType, storageEngineReader, NamePage.ATTRIBUTES_REFERENCE_OFFSET, log);
        namePage.createNameIndexTree(this.databaseType, storageEngineReader, NamePage.ELEMENTS_REFERENCE_OFFSET, log);
        namePage.createNameIndexTree(this.databaseType, storageEngineReader, NamePage.NAMESPACE_REFERENCE_OFFSET, log);
        namePage.createNameIndexTree(this.databaseType, storageEngineReader, NamePage.PROCESSING_INSTRUCTION_REFERENCE_OFFSET, log);
        deweyIDPage.createIndexTree(this.databaseType, storageEngineReader, log);
      } else {
        throw new IllegalStateException("Resource manager type not known.");
      }
    } else {
      if (log.get(newRevisionRootPage.getNamePageReference()) == null) {
        final Page namePage = storageEngineReader.getNamePage(newRevisionRootPage);
        log.put(newRevisionRootPage.getNamePageReference(), PageContainer.getInstance(namePage, namePage));
      }

      if (log.get(newRevisionRootPage.getCASPageReference()) == null) {
        final Page casPage = storageEngineReader.getCASPage(newRevisionRootPage);
        log.put(newRevisionRootPage.getCASPageReference(), PageContainer.getInstance(casPage, casPage));
      }

      if (log.get(newRevisionRootPage.getPathPageReference()) == null) {
        final Page pathPage = storageEngineReader.getPathPage(newRevisionRootPage);
        log.put(newRevisionRootPage.getPathPageReference(), PageContainer.getInstance(pathPage, pathPage));
      }

      if (log.get(newRevisionRootPage.getDeweyIdPageReference()) == null) {
        final Page deweyIDPage = storageEngineReader.getDeweyIDPage(newRevisionRootPage);
        log.put(newRevisionRootPage.getDeweyIdPageReference(), PageContainer.getInstance(deweyIDPage, deweyIDPage));
      }

      final var revisionRootPageReference =
          new PageReference().setDatabaseId(storageEngineReader.getDatabaseId()).setResourceId(storageEngineReader.getResourceId());
      log.put(revisionRootPageReference, PageContainer.getInstance(newRevisionRootPage, newRevisionRootPage));
      uberPage.setRevisionRootPageReference(revisionRootPageReference);
      uberPage.setRevisionRootPage(newRevisionRootPage);
    }

    return new NodeStorageEngineWriter(writer, log, newRevisionRootPage, storageEngineReader, indexController, representRevision,
        isBoundToNodeTrx);
  }
}
