/**
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

package org.sirix.access.trx.page;

import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.access.trx.node.IndexController;
import org.sirix.access.trx.node.Restore;
import org.sirix.access.trx.node.xml.XmlIndexController;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Writer;
import org.sirix.node.DeletedNode;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.page.*;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.settings.VersioningType;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <h1>PageWriteTrx</h1>
 *
 * <p>
 * Implements the {@link PageTrx} interface to provide write capabilities to the persistent storage
 * layer.
 * </p>
 *
 * @author Marc Kramis, Seabix AG
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
final class PageTrxImpl extends AbstractForwardingPageReadOnlyTrx
    implements PageTrx<Long, Record, UnorderedKeyValuePage> {

  /** Page writer to serialize. */
  private final Writer mPageWriter;

  /** Transaction intent log. */
  TransactionIntentLog mLog;

  /** Last reference to the actual revRoot. */
  private final RevisionRootPage mNewRoot;

  /** {@link PageReadOnlyTrxImpl} instance. */
  private final PageReadOnlyTrxImpl mPageRtx;

  /** Determines if a log must be replayed or not. */
  private Restore mRestore = Restore.NO;

  /** Determines if transaction is closed. */
  private boolean mIsClosed;

  /** {@link XmlIndexController} instance. */
  private final IndexController<?, ?> mIndexController;

  /** The tree modifier. */
  private final TreeModifier mTreeModifier;

  /** The revision to represent. */
  private final int mRepresentRevision;

  /** {@code true} if this page write trx will be bound to a node trx, {@code false} otherwise */
  private final boolean mIsBoundToNodeTrx;

  /**
   * Constructor.
   *
   * @param writer the page writer
   * @param log the transaction intent log
   * @param revisionRootPage the revision root page
   * @param pageRtx the page reading transaction used as a delegate
   * @param indexController the index controller, which is used to update indexes
   * @param representRevision the revision to represent
   * @param isBoundToNodeTrx {@code true} if this page write trx will be bound to a node trx,
   *        {@code false} otherwise
   */
  PageTrxImpl(final TreeModifier treeModifier, final Writer writer, final TransactionIntentLog log,
      final RevisionRootPage revisionRootPage, final PageReadOnlyTrxImpl pageRtx,
      final IndexController<?, ?> indexController, final int representRevision, final boolean isBoundToNodeTrx) {
    mTreeModifier = checkNotNull(treeModifier);
    mPageWriter = checkNotNull(writer);
    mLog = checkNotNull(log);
    mNewRoot = checkNotNull(revisionRootPage);
    mPageRtx = checkNotNull(pageRtx);
    mIndexController = checkNotNull(indexController);
    checkArgument(representRevision >= 0, "The represented revision must be >= 0.");
    mRepresentRevision = representRevision;
    mIsBoundToNodeTrx = isBoundToNodeTrx;
  }

  @Override
  public int getRevisionToRepresent() {
    return mRepresentRevision;
  }

  @Override
  public TransactionIntentLog getLog() {
    return mLog;
  }

  @Override
  public void restore(final Restore restore) {
    mRestore = checkNotNull(restore);
  }

  @Override
  public Record prepareEntryForModification(final @Nonnegative Long recordKey, final PageKind pageKind,
      final int index) {
    mPageRtx.assertNotClosed();
    checkNotNull(recordKey);
    checkArgument(recordKey >= 0, "recordKey must be >= 0!");
    checkNotNull(pageKind);

    final long recordPageKey = mPageRtx.pageKey(recordKey);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, pageKind);

    Record record = ((UnorderedKeyValuePage) cont.getModified()).getValue(recordKey);
    if (record == null) {
      final Record oldRecord = ((UnorderedKeyValuePage) cont.getComplete()).getValue(recordKey);
      if (oldRecord == null) {
        throw new SirixIOException("Cannot retrieve record from cache!");
      }
      record = oldRecord;
      ((UnorderedKeyValuePage) cont.getModified()).setEntry(record.getNodeKey(), record);
    }
    return record;
  }

  @Override
  public Record createEntry(final Long key, final Record record, final PageKind pageKind, final int index) {
    mPageRtx.assertNotClosed();
    // Allocate record key and increment record count.
    long recordKey;
    switch (pageKind) {
      case RECORDPAGE:
        recordKey = mNewRoot.incrementAndGetMaxNodeKey();
        break;
      case PATHSUMMARYPAGE:
        final PathSummaryPage pathSummaryPage = ((PathSummaryPage) mNewRoot.getPathSummaryPageReference().getPage());
        recordKey = pathSummaryPage.incrementAndGetMaxNodeKey(index);
        break;
      case CASPAGE:
        final CASPage casPage = ((CASPage) mNewRoot.getCASPageReference().getPage());
        recordKey = casPage.incrementAndGetMaxNodeKey(index);
        break;
      case PATHPAGE:
        final PathPage pathPage = ((PathPage) mNewRoot.getPathPageReference().getPage());
        recordKey = pathPage.incrementAndGetMaxNodeKey(index);
        break;
      case NAMEPAGE:
        final NamePage namePage = ((NamePage) mNewRoot.getNamePageReference().getPage());
        recordKey = namePage.incrementAndGetMaxNodeKey(index);
        break;
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException();
    }

    final long recordPageKey = mPageRtx.pageKey(recordKey);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, pageKind);
    @SuppressWarnings("unchecked")
    final KeyValuePage<Long, Record> modified = (KeyValuePage<Long, Record>) cont.getModified();
    modified.setEntry(record.getNodeKey(), record);
    return record;
  }

  @Override
  public void removeEntry(final Long recordKey, @Nonnull final PageKind pageKind, final int index) {
    mPageRtx.assertNotClosed();
    final long nodePageKey = mPageRtx.pageKey(recordKey);
    final PageContainer cont = prepareRecordPage(nodePageKey, index, pageKind);
    final Optional<Record> node = getRecord(recordKey, pageKind, index);
    if (node.isPresent()) {
      final Record nodeToDel = node.get();
      final Node delNode =
          new DeletedNode(new NodeDelegate(nodeToDel.getNodeKey(), -1, null, null, mPageRtx.getRevisionNumber(), null));
      ((UnorderedKeyValuePage) cont.getModified()).setEntry(delNode.getNodeKey(), delNode);
      ((UnorderedKeyValuePage) cont.getComplete()).setEntry(delNode.getNodeKey(), delNode);
    } else {
      throw new IllegalStateException("Node not found!");
    }
  }

  @Override
  public Optional<Record> getRecord(final @Nonnegative long recordKey, final PageKind pageKind,
      final @Nonnegative int index) {
    mPageRtx.assertNotClosed();
    checkArgument(recordKey >= Fixed.NULL_NODE_KEY.getStandardProperty());
    checkNotNull(pageKind);
    // Calculate page.
    final long recordPageKey = mPageRtx.pageKey(recordKey);

    final PageContainer pageCont = prepareRecordPage(recordPageKey, index, pageKind);
    if (pageCont.equals(PageContainer.emptyInstance())) {
      return mPageRtx.getRecord(recordKey, pageKind, index);
    } else {
      Record node = ((UnorderedKeyValuePage) pageCont.getModified()).getValue(recordKey);
      if (node == null) {
        node = ((UnorderedKeyValuePage) pageCont.getComplete()).getValue(recordKey);
      }
      return mPageRtx.checkItemIfDeleted(node);
    }
  }

  @Override
  public String getName(final int nameKey, final NodeKind nodeKind) {
    mPageRtx.assertNotClosed();
    final NamePage currentNamePage = getNamePage(mNewRoot);
    return (currentNamePage == null || currentNamePage.getName(nameKey, nodeKind, mPageRtx) == null)
        ? mPageRtx.getName(nameKey, nodeKind)
        : currentNamePage.getName(nameKey, nodeKind, mPageRtx);
  }

  @Override
  public int createNameKey(final @Nullable String name, final NodeKind nodeKind) {
    mPageRtx.assertNotClosed();
    checkNotNull(nodeKind);
    final String string = (name == null
        ? ""
        : name);
    final NamePage namePage = getNamePage(mNewRoot);
    return namePage.setName(string, nodeKind, this);
  }

  @Override
  public void commit(final @Nullable PageReference reference) {
    if (reference == null)
      return;

    final PageContainer container = mLog.get(reference, mPageRtx);

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
    mPageWriter.write(reference);

    // Remove page reference.
    reference.setPage(null);
  }

  @Override
  public UberPage commit(final String commitMessage) {
    mPageRtx.assertNotClosed();

    mPageRtx.mResourceManager.getCommitLock().lock();

    final Path commitFile = mPageRtx.mResourceManager.getCommitFile();
    commitFile.toFile().deleteOnExit();
    // Issues with windows that it's not created in the first time?
    while (!Files.exists(commitFile)) {
      try {
        Files.createFile(commitFile);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    final PageReference uberPageReference = new PageReference();
    final UberPage uberPage = getUberPage();
    uberPageReference.setPage(uberPage);
    final int revision = uberPage.getRevisionNumber();

    mPageRtx.mResourceManager.getUser().ifPresent(user -> getActualRevisionRootPage().setUser(user));

    if (commitMessage != null)
      getActualRevisionRootPage().setCommitMessage(commitMessage);

    // Recursively write indirectly referenced pages.
    uberPage.commit(this);

    uberPageReference.setPage(uberPage);
    mPageWriter.writeUberPageReference(uberPageReference);
    uberPageReference.setPage(null);

    final Path indexes =
        mPageRtx.mResourceConfig.resourcePath.resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
                                             .resolve(String.valueOf(revision) + ".xml");

    if (!Files.exists(indexes)) {
      try {
        Files.createFile(indexes);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    try (final OutputStream out = new FileOutputStream(indexes.toFile())) {
      mIndexController.serialize(out);
    } catch (final IOException e) {
      throw new SirixIOException("Index definitions couldn't be serialized!", e);
    }

    mLog.truncate();

    // Delete commit file which denotes that a commit must write the log in the data file.
    try {
      Files.delete(commitFile);
    } catch (final IOException e) {
      throw new SirixIOException("Commit file couldn't be deleted!");
    }

    final UberPage commitedUberPage = (UberPage) mPageWriter.read(mPageWriter.readUberPageReference(), mPageRtx);
    mPageRtx.mResourceManager.getCommitLock().unlock();
    return commitedUberPage;
  }

  @Override
  public UberPage commit() {
    return commit((String) null);
  }

  @Override
  public UberPage rollback() {
    mPageRtx.assertNotClosed();
    mLog.truncate();
    return (UberPage) mPageWriter.read(mPageWriter.readUberPageReference(), mPageRtx);
  }

  @Override
  public void close() {
    if (!mIsClosed) {
      mPageRtx.assertNotClosed();

      final UberPage lastUberPage = (UberPage) mPageWriter.read(mPageWriter.readUberPageReference(), mPageRtx);

      mPageRtx.mResourceManager.setLastCommittedUberPage(lastUberPage);

      if (!mIsBoundToNodeTrx)
        mPageRtx.mResourceManager.closePageWriteTransaction(mPageRtx.getTrxId());

      mLog.close();
      mPageRtx.close();
      mPageWriter.close();
      mIsClosed = true;
    }
  }

  /**
   * Prepare record page.
   *
   * @param recordPageKey the key of the record page
   * @param indexNumber the index number if it's a record-page of an index, {@code -1}, else
   * @param pageKind the kind of page (used to determine the right subtree)
   * @return {@link PageContainer} instance
   * @throws SirixIOException if an I/O error occurs
   */
  private PageContainer prepareRecordPage(final @Nonnegative long recordPageKey, final int indexNumber,
      final PageKind pageKind) {
    assert recordPageKey >= 0;
    assert pageKind != null;

    final PageReference pageReference = mPageRtx.getPageReference(mNewRoot, pageKind, indexNumber);

    // Get the reference to the unordered key/value page storing the records.
    final PageReference reference = mTreeModifier.prepareLeafOfTree(mPageRtx, mLog,
        getUberPage().getPageCountExp(pageKind), pageReference, recordPageKey, indexNumber, pageKind, mNewRoot);

    PageContainer pageContainer = mLog.get(reference, mPageRtx);

    if (pageContainer.equals(PageContainer.emptyInstance())) {
      if (reference.getKey() == Constants.NULL_ID_LONG) {
        final UnorderedKeyValuePage completePage =
            new UnorderedKeyValuePage(recordPageKey, pageKind, Constants.NULL_ID_LONG, mPageRtx);
        final UnorderedKeyValuePage modifyPage = mPageRtx.clone(completePage);
        pageContainer = PageContainer.getInstance(completePage, modifyPage);
      } else {
        pageContainer = dereferenceRecordPageForModification(reference);
      }

      assert pageContainer != null;

      switch (pageKind) {
        case RECORDPAGE:
        case PATHSUMMARYPAGE:
        case PATHPAGE:
        case CASPAGE:
        case NAMEPAGE:
          appendLogRecord(reference, pageContainer);
          break;
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Page kind not known!");
      }
    }

    return pageContainer;
  }

  /**
   * Dereference record page reference.
   *
   * @param reference reference to leaf, that is the record page
   * @return dereferenced page
   */
  private PageContainer dereferenceRecordPageForModification(final PageReference reference) {
    final List<UnorderedKeyValuePage> revs = mPageRtx.getSnapshotPages(reference);
    final VersioningType revisioning = mPageRtx.mResourceManager.getResourceConfig().revisioningType;
    final int mileStoneRevision = mPageRtx.mResourceManager.getResourceConfig().numberOfRevisionsToRestore;
    return revisioning.combineRecordPagesForModification(revs, mileStoneRevision, mPageRtx, reference);
  }

  @Override
  public RevisionRootPage getActualRevisionRootPage() {
    return mNewRoot;
  }

  @Override
  protected PageReadOnlyTrx delegate() {
    return mPageRtx;
  }

  @Override
  public PageReadOnlyTrx getPageReadTrx() {
    return mPageRtx;
  }

  @Override
  public PageTrx<Long, Record, UnorderedKeyValuePage> appendLogRecord(final PageReference reference,
      final PageContainer pageContainer) {
    checkNotNull(pageContainer);
    mLog.put(reference, pageContainer);
    return this;
  }

  @Override
  public PageContainer getLogRecord(final PageReference reference) {
    checkNotNull(reference);
    return mLog.get(reference, mPageRtx);
  }

  @Override
  public PageTrx<Long, Record, UnorderedKeyValuePage> truncateTo(final int revision) {
    mPageWriter.truncateTo(revision);
    return this;
  }

  @Override
  public long getTrxId() {
    return mPageRtx.getTrxId();
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    return mPageRtx.getCommitCredentials();
  }
}
