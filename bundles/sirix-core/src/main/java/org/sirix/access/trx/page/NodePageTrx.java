/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.access.trx.page;

import org.sirix.access.ResourceConfiguration;
import org.sirix.access.User;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.access.trx.node.IndexController;
import org.sirix.access.trx.node.Restore;
import org.sirix.access.trx.node.xml.XmlIndexController;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.io.Writer;
import org.sirix.node.DeletedNode;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.Node;
import org.sirix.page.CASPage;
import org.sirix.page.DeweyIDPage;
import org.sirix.page.NamePage;
import org.sirix.page.PageReference;
import org.sirix.page.PathPage;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.settings.VersioningType;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardOpenOption.CREATE;

/**
 * <p>
 * Implements the {@link PageTrx} interface to provide write capabilities to the persistent storage
 * layer.
 * </p>
 *
 * @author Marc Kramis, Seabix AG
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
final class NodePageTrx extends AbstractForwardingPageReadOnlyTrx implements PageTrx {

  /**
   * Page writer to serialize.
   */
  private final Writer storagePageReaderWriter;

  /**
   * Transaction intent log.
   */
  TransactionIntentLog log;

  /**
   * Last reference to the actual revRoot.
   */
  private final RevisionRootPage newRevisionRootPage;

  /**
   * {@link NodePageReadOnlyTrx} instance.
   */
  private final NodePageReadOnlyTrx pageRtx;

  /**
   * Determines if a log must be replayed or not.
   */
  private Restore restore = Restore.NO;

  /**
   * Determines if transaction is closed.
   */
  private volatile boolean isClosed;

  /**
   * {@link XmlIndexController} instance.
   */
  private final IndexController<?, ?> indexController;

  /**
   * The tree modifier.
   */
  private final TreeModifier treeModifier;

  /**
   * The revision to represent.
   */
  private final int representRevision;

  /**
   * {@code true} if this page write trx will be bound to a node trx, {@code false} otherwise
   */
  private final boolean isBoundToNodeTrx;

  private MostRecentPageContainer mostRecentPageContainer;

  /**
   * Constructor.
   *
   * @param writer            the page writer
   * @param log               the transaction intent log
   * @param revisionRootPage  the revision root page
   * @param pageRtx           the page reading transaction used as a delegate
   * @param indexController   the index controller, which is used to update indexes
   * @param representRevision the revision to represent
   * @param isBoundToNodeTrx  {@code true} if this page write trx will be bound to a node trx,
   *                          {@code false} otherwise
   */
  NodePageTrx(final TreeModifier treeModifier, final Writer writer, final TransactionIntentLog log,
      final RevisionRootPage revisionRootPage, final NodePageReadOnlyTrx pageRtx,
      final IndexController<?, ?> indexController, final int representRevision, final boolean isBoundToNodeTrx) {
    this.treeModifier = checkNotNull(treeModifier);
    storagePageReaderWriter = checkNotNull(writer);
    this.log = checkNotNull(log);
    newRevisionRootPage = checkNotNull(revisionRootPage);
    this.pageRtx = checkNotNull(pageRtx);
    this.indexController = checkNotNull(indexController);
    checkArgument(representRevision >= 0, "The represented revision must be >= 0.");
    this.representRevision = representRevision;
    this.isBoundToNodeTrx = isBoundToNodeTrx;
  }

  @Override
  public int getRevisionToRepresent() {
    return representRevision;
  }

  @Override
  public TransactionIntentLog getLog() {
    return log;
  }

  @Override
  public void restore(final Restore restore) {
    this.restore = checkNotNull(restore);
  }

  @Override
  public int getRevisionNumber() {
    pageRtx.assertNotClosed();
    return newRevisionRootPage.getRevision();
  }

  @Override
  public <K, V> V prepareRecordForModification(@Nonnull final K recordKey, @Nonnull final IndexType indexType,
      final int index) {
    pageRtx.assertNotClosed();
    checkNotNull(recordKey);

    if (recordKey instanceof Long nodeKey) {
      checkArgument(nodeKey >= 0, "recordKey must be >= 0!");
      checkNotNull(indexType);

      final long recordPageKey = pageRtx.pageKey(nodeKey, indexType);
      final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);

      DataRecord record = ((UnorderedKeyValuePage) cont.getModified()).getValue(nodeKey);
      if (record == null) {
        final DataRecord oldRecord = ((UnorderedKeyValuePage) cont.getComplete()).getValue(nodeKey);
        if (oldRecord == null) {
          throw new SirixIOException("Cannot retrieve record from cache!");
        }
        record = oldRecord;
        ((UnorderedKeyValuePage) cont.getModified()).setRecord(nodeKey, record);
      }

      return (V) record;
    }

    // TODO
    return null;
  }

  @Override
  public <K, V> V createRecord(@Nonnull final K recordKey, @Nonnull final V record, @Nonnull final IndexType indexType,
      @Nonnegative final int index) {
    pageRtx.assertNotClosed();

    if (recordKey instanceof Long) {
      // Allocate record key and increment record count.
      // $CASES-OMITTED$
      final long createdRecordKey = switch (indexType) {
        case DOCUMENT -> newRevisionRootPage.incrementAndGetMaxNodeKeyInDocumentIndex();
        case CHANGED_NODES -> newRevisionRootPage.incrementAndGetMaxNodeKeyInRecordToRevisionsIndex() ;
        case RECORD_TO_REVISIONS -> newRevisionRootPage.incrementAndGetMaxNodeKeyInRecordToRevisionsIndex();
        case PATH_SUMMARY -> {
          final PathSummaryPage pathSummaryPage =
              ((PathSummaryPage) newRevisionRootPage.getPathSummaryPageReference().getPage());
          yield pathSummaryPage.incrementAndGetMaxNodeKey(index);
        }
        case CAS -> {
          final CASPage casPage = ((CASPage) newRevisionRootPage.getCASPageReference().getPage());
          yield casPage.incrementAndGetMaxNodeKey(index);
        }
        case PATH -> {
          final PathPage pathPage = ((PathPage) newRevisionRootPage.getPathPageReference().getPage());
          yield pathPage.incrementAndGetMaxNodeKey(index);
        }
        case NAME -> {
          final NamePage namePage = ((NamePage) newRevisionRootPage.getNamePageReference().getPage());
          yield namePage.incrementAndGetMaxNodeKey(index);
        }
        default -> throw new IllegalStateException();
      };

      final long recordPageKey = pageRtx.pageKey(createdRecordKey, indexType);
      final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
      @SuppressWarnings("unchecked")
      final KeyValuePage<Long, DataRecord> modified = (KeyValuePage<Long, DataRecord>) cont.getModified();
      modified.setRecord(createdRecordKey, (DataRecord) record);
      return record;
    }

    // TODO

    return null;
  }

  @Override
  public <K> void removeRecord(final K recordKey, @Nonnull final IndexType indexType, final int index) {
    pageRtx.assertNotClosed();
    checkNotNull(recordKey);

    if (recordKey instanceof Long nodeKey) {
      final long recordPageKey = pageRtx.pageKey(nodeKey, indexType);
      final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
      final Optional<DataRecord> node = getRecord(nodeKey, indexType, index);
      if (node.isPresent()) {
        final DataRecord nodeToDel = node.get();
        final Node delNode = new DeletedNode(new NodeDelegate(nodeToDel.getNodeKey(),
                                                              -1,
                                                              null,
                                                              null,
                                                              pageRtx.getRevisionNumber(),
                                                              null));
        ((UnorderedKeyValuePage) cont.getModified()).setRecord(delNode.getNodeKey(), delNode);
        ((UnorderedKeyValuePage) cont.getComplete()).setRecord(delNode.getNodeKey(), delNode);
      } else {
        throw new IllegalStateException("Node not found!");
      }
    }

    // TODO
  }

  @Override
  public <K, V> Optional<V> getRecord(@Nonnull final K recordKey, @Nonnull final IndexType indexType,
      @Nonnegative final int index) {
    pageRtx.assertNotClosed();

    if (recordKey instanceof Long nodeKey) {
      checkArgument(nodeKey >= Fixed.NULL_NODE_KEY.getStandardProperty());
      checkNotNull(indexType);

      // Calculate page.
      final long recordPageKey = pageRtx.pageKey(nodeKey, indexType);

      final PageContainer pageCont = prepareRecordPage(recordPageKey, index, indexType);
      if (pageCont.equals(PageContainer.emptyInstance())) {
        return pageRtx.getRecord(recordKey, indexType, index);
      } else {
        DataRecord node = ((UnorderedKeyValuePage) pageCont.getModified()).getValue(nodeKey);
        if (node == null) {
          node = ((UnorderedKeyValuePage) pageCont.getComplete()).getValue(nodeKey);
        }
        return (Optional<V>) pageRtx.checkItemIfDeleted(node);
      }
    }

    // TODO
    return null;
  }

  @Override
  public String getName(final int nameKey, @Nonnull final NodeKind nodeKind) {
    pageRtx.assertNotClosed();
    final NamePage currentNamePage = getNamePage(newRevisionRootPage);
    return (currentNamePage == null || currentNamePage.getName(nameKey, nodeKind, pageRtx) == null) ? pageRtx.getName(
        nameKey,
        nodeKind) : currentNamePage.getName(nameKey, nodeKind, pageRtx);
  }

  @Override
  public int createNameKey(final @Nullable String name, @Nonnull final NodeKind nodeKind) {
    pageRtx.assertNotClosed();
    checkNotNull(nodeKind);
    final String string = (name == null ? "" : name);
    final NamePage namePage = getNamePage(newRevisionRootPage);
    return namePage.setName(string, nodeKind, this);
  }

  @Override
  public void commit(final @Nullable PageReference reference) {
    if (reference == null)
      return;

    final PageContainer container = log.get(reference, this);

    log.remove(reference);

    Page page = null;

    if (container != null) {
      page = container.getModified();
    }

    if (page == null) {
      return;
    }

    reference.setPage(page);

    // Recursively commit indirectly referenced pages and then write self.f
    page.commit(this);
    storagePageReaderWriter.write(reference);

    // Remove page reference.
    reference.setPage(null);
  }

  @Override
  public UberPage commit(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp) {
    pageRtx.assertNotClosed();

    pageRtx.resourceManager.getCommitLock().lock();

    try {
      final Path commitFile = pageRtx.resourceManager.getCommitFile();
      commitFile.toFile().deleteOnExit();
      // Issues with windows that it's not created in the first time?
      createIfAbsent(commitFile);

      final PageReference uberPageReference = new PageReference();
      final UberPage uberPage = getUberPage();
      uberPageReference.setPage(uberPage);
      final int revision = uberPage.getRevisionNumber();

      setUserIfPresent();

      if (commitMessage != null) {
        newRevisionRootPage.setCommitMessage(commitMessage);
      }

      if (commitTimestamp != null) {
        newRevisionRootPage.setCommitTimestamp(commitTimestamp);
      }

      // Recursively write indirectly referenced pages.
      uberPage.commit(this);

      uberPageReference.setPage(uberPage);
      storagePageReaderWriter.writeUberPageReference(uberPageReference);
      uberPageReference.setPage(null);

      final Path indexes = pageRtx.getResourceManager()
              .getResourceConfig()
              .resourcePath
              .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
              .resolve(revision + ".xml");

      try (final OutputStream out = newOutputStream(indexes, CREATE)) {
        indexController.serialize(out);
      } catch (final IOException e) {
        throw new SirixIOException("Index definitions couldn't be serialized!", e);
      }

      log.truncate();

      // Delete commit file which denotes that a commit must write the log in the data file.
      try {
        deleteIfExists(commitFile);
      } catch (final IOException e) {
        throw new SirixIOException("Commit file couldn't be deleted!");
      }

    } finally {
      pageRtx.resourceManager.getCommitLock().unlock();
    }

    return (UberPage) storagePageReaderWriter.read(storagePageReaderWriter.readUberPageReference(), pageRtx);
  }

  private void createIfAbsent(final Path file) {
    while (!Files.exists(file)) {
      try {
        Files.createFile(file);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }
  }

  private void setUserIfPresent() {
    final Optional<User> optionalUser = pageRtx.resourceManager.getUser();
    optionalUser.ifPresent(user -> getActualRevisionRootPage().setUser(user));
  }

  @Override
  public UberPage rollback() {
    pageRtx.assertNotClosed();
    log.truncate();
    return (UberPage) storagePageReaderWriter.read(storagePageReaderWriter.readUberPageReference(), pageRtx);
  }

  @Override
  public synchronized void close() {
    if (!isClosed) {
      pageRtx.assertNotClosed();

      final UberPage lastUberPage = (UberPage) storagePageReaderWriter.read(storagePageReaderWriter.readUberPageReference(), pageRtx);

      pageRtx.resourceManager.setLastCommittedUberPage(lastUberPage);

      if (!isBoundToNodeTrx) {
        pageRtx.resourceManager.closePageWriteTransaction(pageRtx.getTrxId());
      }

      log.close();
      pageRtx.close();
      storagePageReaderWriter.close();
      isClosed = true;
    }
  }

  @Override
  public DeweyIDPage getDeweyIDPage(@Nonnull RevisionRootPage revisionRoot) {
    // TODO
    return null;
  }

  /**
   * Prepare record page.
   *
   * @param recordPageKey the key of the record page
   * @param indexNumber   the index number if it's a record-page of an index, {@code -1}, else
   * @param indexType     the index type
   * @return {@link PageContainer} instance
   * @throws SirixIOException if an I/O error occurs
   */
  private PageContainer prepareRecordPage(final @Nonnegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    assert recordPageKey >= 0;
    assert indexType != null;

    if (hasMatchingMostRecentPageContainer(recordPageKey, indexNumber, indexType)) {
      return mostRecentPageContainer.pageContainer();
    }

    final PageReference pageReference = pageRtx.getPageReference(newRevisionRootPage, indexType, indexNumber);

    // Get the reference to the unordered key/value page storing the records.
    final PageReference reference = treeModifier.prepareLeafOfTree(pageRtx,
                                                                   log,
                                                                   getUberPage().getPageCountExp(indexType),
                                                                   pageReference,
                                                                   recordPageKey,
                                                                   indexNumber,
                                                                   indexType,
                                                                   newRevisionRootPage);

    PageContainer pageContainer = log.get(reference, this);

    if (pageContainer.equals(PageContainer.emptyInstance())) {
      if (reference.getKey() == Constants.NULL_ID_LONG) {
        final UnorderedKeyValuePage completePage = new UnorderedKeyValuePage(recordPageKey, indexType, pageRtx);
        final UnorderedKeyValuePage modifyPage = new UnorderedKeyValuePage(pageRtx, completePage);
        pageContainer = PageContainer.getInstance(completePage, modifyPage);
      } else {
        pageContainer = dereferenceRecordPageForModification(reference);
      }

      assert pageContainer != null;

      // $CASES-OMITTED$
      switch (indexType) {
        case DOCUMENT, CHANGED_NODES, RECORD_TO_REVISIONS, DEWEYID_TO_RECORDID, PATH_SUMMARY, PATH, CAS, NAME -> appendLogRecord(
            reference,
            pageContainer);
        default -> throw new IllegalStateException("Page kind not known!");
      }
    }

    if (indexType != IndexType.RECORD_TO_REVISIONS) {
      mostRecentPageContainer = new MostRecentPageContainer(recordPageKey, indexNumber, indexType, pageContainer);
    }

    return pageContainer;
  }

  private boolean hasMatchingMostRecentPageContainer(long recordPageKey, int indexNumber, IndexType indexType) {
    return mostRecentPageContainer != null && mostRecentPageContainer.pageKey() == recordPageKey
        && mostRecentPageContainer.indexNumber() == indexNumber && mostRecentPageContainer.indexType() == indexType;
  }

  /**
   * Dereference record page reference.
   *
   * @param reference reference to leaf, that is the record page
   * @return dereferenced page
   */
  private PageContainer dereferenceRecordPageForModification(final PageReference reference) {
    final List<UnorderedKeyValuePage> pageFragments = pageRtx.getPageFragments(reference);
    final VersioningType revisioning = pageRtx.resourceManager.getResourceConfig().versioningType;
    final int mileStoneRevision = pageRtx.resourceManager.getResourceConfig().maxNumberOfRevisionsToRestore;
    return revisioning.combineRecordPagesForModification(pageFragments, mileStoneRevision, pageRtx, reference, log);
  }

  @Override
  public RevisionRootPage getActualRevisionRootPage() {
    return newRevisionRootPage;
  }

  @Override
  protected PageReadOnlyTrx delegate() {
    return pageRtx;
  }

  @Override
  public PageReadOnlyTrx getPageReadOnlyTrx() {
    return pageRtx;
  }

  @Override
  public PageTrx appendLogRecord(@Nonnull final PageReference reference, @Nonnull final PageContainer pageContainer) {
    checkNotNull(pageContainer);
    log.put(reference, pageContainer);
    return this;
  }

  @Override
  public PageContainer getLogRecord(final PageReference reference) {
    checkNotNull(reference);
    return log.get(reference, this);
  }

  @Override
  public PageTrx truncateTo(final int revision) {
    storagePageReaderWriter.truncateTo(revision);
    return this;
  }

  @Override
  public long getTrxId() {
    return pageRtx.getTrxId();
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    return pageRtx.getCommitCredentials();
  }

  private record MostRecentPageContainer(long pageKey, int indexNumber, IndexType indexType,
                                         PageContainer pageContainer) {
  }
}
