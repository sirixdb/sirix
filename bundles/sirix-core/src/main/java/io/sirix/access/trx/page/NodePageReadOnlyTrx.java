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

package io.sirix.access.trx.page;

import com.google.common.base.MoreObjects;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.CommitCredentials;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.api.ResourceSession;
import io.sirix.cache.*;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.BytesUtils;
import io.sirix.io.Reader;
import io.sirix.node.DeletedNode;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.*;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.settings.VersioningType;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Page read-only transaction. The only thing shared amongst transactions is the resource manager.
 * Everything else is exclusive to this transaction. It is required that only a single thread has
 * access to this transaction.
 */
public final class NodePageReadOnlyTrx implements PageReadOnlyTrx {

  private record RecordPage(int index, IndexType indexType, long recordPageKey, int revision,
                            PageReference pageReference, KeyValueLeafPage page) {
  }

  /**
   * Page reader exclusively assigned to this transaction.
   */
  private final Reader pageReader;

  /**
   * Uber page this transaction is bound to.
   */
  private final UberPage uberPage;

  /**
   * {@link InternalResourceSession} reference.
   */
  final InternalResourceSession<?, ?> resourceSession;

  /**
   * The revision number, this page trx is bound to.
   */
  private final int revisionNumber;

  /**
   * Determines if page reading transaction is closed or not.
   */
  private volatile boolean isClosed;

  /**
   * {@link ResourceConfiguration} instance.
   */
  private final ResourceConfiguration resourceConfig;

  /**
   * Caches in-memory reconstructed pages of a specific resource.
   */
  private final BufferManager resourceBufferManager;

  /**
   * Transaction intent log.
   */
  private final TransactionIntentLog trxIntentLog;

  /**
   * The transaction-ID.
   */
  private final int trxId;

  /**
   * Cached name page of this revision.
   */
  private final RevisionRootPage rootPage;

  /**
   * {@link NamePage} reference.
   */
  private final NamePage namePage;

  /**
   * Caches the most recently read record page.
   */
  private RecordPage mostRecentlyReadRecordPage;

  /**
   * Caches the second most recently read record page.
   */
  private RecordPage secondMostRecentlyReadRecordPage;

  private RecordPage pathSummaryRecordPage;

  private final Bytes<ByteBuffer> byteBufferForRecords = Bytes.elasticHeapByteBuffer(40);

  /**
   * Standard constructor.
   *
   * @param trxId                 the transaction-ID.
   * @param resourceSession       the resource manager
   * @param uberPage              {@link UberPage} to start reading from
   * @param revision              key of revision to read from uber page
   * @param reader                to read stored pages for this transaction
   * @param resourceBufferManager caches in-memory reconstructed pages
   * @param trxIntentLog          the transaction intent log (can be {@code null})
   * @throws SirixIOException if reading of the persistent storage fails
   */
  public NodePageReadOnlyTrx(final int trxId,
      final InternalResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> resourceSession,
      final UberPage uberPage, final @NonNegative int revision, final Reader reader,
      final BufferManager resourceBufferManager, final @NonNull RevisionRootPageReader revisionRootPageReader,
      final @Nullable TransactionIntentLog trxIntentLog) {
    checkArgument(trxId > 0, "Transaction-ID must be >= 0.");
    this.trxId = trxId;
    this.resourceBufferManager = resourceBufferManager;
    this.isClosed = false;
    this.resourceSession = requireNonNull(resourceSession);
    this.resourceConfig = resourceSession.getResourceConfig();
    this.pageReader = requireNonNull(reader);
    this.uberPage = requireNonNull(uberPage);
    this.trxIntentLog = trxIntentLog;

    revisionNumber = revision;
    rootPage = revisionRootPageReader.loadRevisionRootPage(this, revision);
    namePage = revisionRootPageReader.getNamePage(this, rootPage);
  }

  private Page loadPage(final PageReference reference) {
    Page page = reference.getPage();
    if (page != null) {
      return page;
    }

    if (trxIntentLog != null && reference.getLogKey() != Constants.NULL_ID_INT) {
      page = getFromTrxIntentLog(reference);
      if (page != null) {
        return page;
      }
    }

    if (trxIntentLog == null) {
      assert reference.getLogKey() == Constants.NULL_ID_INT;
      page = resourceBufferManager.getPageCache().get(reference, (pageReference, _) -> {
        try {
          return pageReader.read(pageReference, resourceSession.getResourceConfig());
        } catch (final SirixIOException e) {
          throw new IllegalStateException(e);
        }
      });
      if (page != null) {
        reference.setPage(page);
      }
      return page;
    }

    if (reference.getKey() != Constants.NULL_ID_LONG || reference.getLogKey() != Constants.NULL_ID_INT) {
      page = pageReader.read(reference, resourceSession.getResourceConfig());
    }

    if (page != null) {
      putIntoPageCache(reference, page);
      reference.setPage(page);
    }
    return page;
  }

  private void putIntoPageCache(PageReference reference, Page page) {
    if (!(page instanceof UberPage)) {
      // Put page into buffer manager.
      resourceBufferManager.getPageCache().putIfAbsent(reference, page);
    }
  }

  @Override
  public boolean hasTrxIntentLog() {
    return trxIntentLog != null;
  }

  @Nullable
  private Page getFromTrxIntentLog(PageReference reference) {
    // Try to get it from the transaction log if it's present.
    final PageContainer cont = trxIntentLog.get(reference);
    return cont == null ? null : cont.getComplete();
  }

  @Override
  public int getTrxId() {
    assertNotClosed();
    return trxId;
  }

  @Override
  public ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceSession() {
    assertNotClosed();
    return resourceSession;
  }

  /**
   * Make sure that the transaction is not yet closed when calling this method.
   */
  void assertNotClosed() {
    assert !isClosed : "Transaction is already closed!";
  }

  @Override
  public <V extends DataRecord> V getRecord(final long recordKey, @NonNull final IndexType indexType,
      @NonNegative final int index) {
    requireNonNull(indexType);
    assertNotClosed();

    if (recordKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return null;
    }

    final long recordPageKey = pageKey(recordKey, indexType);

    var indexLogKey = new IndexLogKey(indexType, recordPageKey, index, revisionNumber);

    // $CASES-OMITTED$
    final PageReferenceToPage pageReferenceToPage = switch (indexType) {
      case DOCUMENT, CHANGED_NODES, RECORD_TO_REVISIONS, PATH_SUMMARY, PATH, CAS, NAME -> getRecordPage(indexLogKey);
      default -> throw new IllegalStateException();
    };

    if (pageReferenceToPage == null || pageReferenceToPage.page == null) {
      return null;
    }

    final var dataRecord = getValue(((KeyValueLeafPage) pageReferenceToPage.page), recordKey);

    //noinspection unchecked
    return (V) checkItemIfDeleted(dataRecord);
  }

  @Override
  public DataRecord getValue(final KeyValueLeafPage page, final long nodeKey) {
    final var offset = PageReadOnlyTrx.recordPageOffset(nodeKey);
    DataRecord record = page.getRecord(offset);
    if (record == null) {
      byte[] data = page.getSlotAsByteArray(offset);
      if (data != null) {
        record = getDataRecord(nodeKey, offset, data, page);
      }
      if (record != null) {
        return record;
      }
      try {
        final PageReference reference = page.getPageReference(nodeKey);
        if (reference != null && reference.getKey() != Constants.NULL_ID_LONG) {
          data = ((OverflowPage) pageReader.read(reference, resourceSession.getResourceConfig())).getData();
        } else {
          return null;
        }
      } catch (final SirixIOException e) {
        return null;
      }
      record = getDataRecord(nodeKey, offset, data, page);
    }
    return record;
  }

  private DataRecord getDataRecord(long key, int offset, byte[] data, KeyValueLeafPage page) {
    byteBufferForRecords.clear();
    BytesUtils.doWrite(byteBufferForRecords, data);
    var record = resourceConfig.recordPersister.deserialize(byteBufferForRecords,
                                                            key,
                                                            page.getDeweyIdAsByteArray(offset),
                                                            resourceConfig);
    byteBufferForRecords.clear();
    page.setRecord(record);
    return record;
  }

  /**
   * Method to check if an {@link DataRecord} is deleted.
   *
   * @param toCheck node to check
   * @return the {@code node} if it is valid, {@code null} otherwise
   */
  <V extends DataRecord> V checkItemIfDeleted(final @Nullable V toCheck) {
    if (toCheck instanceof DeletedNode) {
      return null;
    }

    return toCheck;
  }

  @Override
  public String getName(final int nameKey, @NonNull final NodeKind nodeKind) {
    assertNotClosed();
    return namePage.getName(nameKey, nodeKind, this);
  }

  @Override
  public byte[] getRawName(final int nameKey, @NonNull final NodeKind nodeKind) {
    assertNotClosed();
    return namePage.getRawName(nameKey, nodeKind, this);
  }

  /**
   * Get revision root page belonging to revision key.
   *
   * @param revisionKey key of revision to find revision root page for
   * @return revision root page of this revision key
   * @throws SirixIOException if something odd happens within the creation process
   */
  @Override
  public RevisionRootPage loadRevRoot(@NonNegative final int revisionKey) {
    assert revisionKey <= resourceSession.getMostRecentRevisionNumber();
    if (trxIntentLog == null) {
      final Cache<Integer, RevisionRootPage> cache = resourceBufferManager.getRevisionRootPageCache();
      return cache.get(revisionKey, (_, _) -> pageReader.readRevisionRootPage(revisionKey, resourceConfig));
    } else {
      if (revisionKey == 0 && uberPage.getRevisionRootReference() != null) {
        final var revisionRootPageReference = uberPage.getRevisionRootReference();
        final var pageContainer = trxIntentLog.get(revisionRootPageReference);
        assert pageContainer != null;
        return (RevisionRootPage) pageContainer.getModified();
      }
      return pageReader.readRevisionRootPage(revisionKey, resourceConfig);
    }
  }

  @Override
  public NamePage getNamePage(@NonNull final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (NamePage) getPage(revisionRoot.getNamePageReference());
  }

  @Override
  public PathSummaryPage getPathSummaryPage(@NonNull final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (PathSummaryPage) getPage(revisionRoot.getPathSummaryPageReference());
  }

  @Override
  public PathPage getPathPage(@NonNull final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (PathPage) getPage(revisionRoot.getPathPageReference());
  }

  @Override
  public CASPage getCASPage(@NonNull final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (CASPage) getPage(revisionRoot.getCASPageReference());
  }

  @Override
  public DeweyIDPage getDeweyIDPage(@NonNull final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (DeweyIDPage) getPage(revisionRoot.getDeweyIdPageReference());
  }

  @Override
  public BufferManager getBufferManager() {
    return resourceBufferManager;
  }

  /**
   * Set the page if it is not set already.
   *
   * @param reference page reference
   * @throws SirixIOException if an I/O error occurs
   */
  private Page getPage(final PageReference reference) {
    var page = loadPage(reference);
    reference.setPage(page);
    return page;
  }

  @Override
  public UberPage getUberPage() {
    assertNotClosed();
    return uberPage;
  }

  public record PageReferenceToPage(PageReference reference, Page page) {
  }

  @Override
  public PageReferenceToPage getRecordPage(@NonNull IndexLogKey indexLogKey) {
    assertNotClosed();
    checkArgument(indexLogKey.getRecordPageKey() >= 0, "recordPageKey must not be negative!");

    // First: Check most recent pages.
    if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY && isMostRecentlyReadPathSummaryPage(indexLogKey)) {
      var page = pathSummaryRecordPage.page();
      assert !page.isClosed();
      return new PageReferenceToPage(pathSummaryRecordPage.pageReference, page);
    }
    if (isMostRecentlyReadPage(indexLogKey)) {
      var page = mostRecentlyReadRecordPage.page();
      assert !page.isClosed();
      return new PageReferenceToPage(mostRecentlyReadRecordPage.pageReference, page);
    }
    if (isSecondMostRecentlyReadPage(indexLogKey)) {
      var page = secondMostRecentlyReadRecordPage.page();
      assert !page.isClosed();
      return new PageReferenceToPage(secondMostRecentlyReadRecordPage.pageReference, page);
    }

    // Second: Traverse trie.
    final var pageReferenceToRecordPage = getLeafPageReference(indexLogKey.getRecordPageKey(),
                                                               indexLogKey.getIndexNumber(),
                                                               requireNonNull(indexLogKey.getIndexType()));

    if (pageReferenceToRecordPage == null) {
      return null;
    }

    // Third: Try to get in-memory instance.
    var page = getInMemoryPageInstance(indexLogKey, pageReferenceToRecordPage);
    if (page != null && !page.isClosed()) {
      return new PageReferenceToPage(pageReferenceToRecordPage, page);
    }

    // Fourth: Try to get from resource buffer manager.
    if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
      return new PageReferenceToPage(pageReferenceToRecordPage,
                                     getFromBufferManager(indexLogKey, pageReferenceToRecordPage));
    }

    // Read-write-trx and path summary page.
    var loadedPage =
        (KeyValueLeafPage) loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
    assert loadedPage != null;
    assert loadedPage.getIndexType() == IndexType.PATH_SUMMARY;
    return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
  }

  private boolean isMostRecentlyReadPathSummaryPage(IndexLogKey indexLogKey) {
    return pathSummaryRecordPage != null && pathSummaryRecordPage.recordPageKey == indexLogKey.getRecordPageKey()
        && pathSummaryRecordPage.index == indexLogKey.getIndexNumber()
        && pathSummaryRecordPage.revision == indexLogKey.getRevisionNumber();
  }

  private boolean isMostRecentlyReadPage(IndexLogKey indexLogKey) {
    return mostRecentlyReadRecordPage != null
        && mostRecentlyReadRecordPage.recordPageKey == indexLogKey.getRecordPageKey()
        && mostRecentlyReadRecordPage.index == indexLogKey.getIndexNumber()
        && mostRecentlyReadRecordPage.indexType == indexLogKey.getIndexType()
        && mostRecentlyReadRecordPage.revision == indexLogKey.getRevisionNumber();
  }

  private boolean isSecondMostRecentlyReadPage(IndexLogKey indexLogKey) {
    return secondMostRecentlyReadRecordPage != null
        && secondMostRecentlyReadRecordPage.recordPageKey == indexLogKey.getRecordPageKey()
        && secondMostRecentlyReadRecordPage.index == indexLogKey.getIndexNumber()
        && secondMostRecentlyReadRecordPage.indexType == indexLogKey.getIndexType()
        && secondMostRecentlyReadRecordPage.revision == indexLogKey.getRevisionNumber();
  }

  @Nullable
  private Page getFromBufferManager(@NonNull IndexLogKey indexLogKey, PageReference pageReferenceToRecordPage) {
    final KeyValueLeafPage recordPageFromBuffer =
        resourceBufferManager.getRecordPageCache().get(pageReferenceToRecordPage, (_, _) -> {
          var kvPage =
              (KeyValueLeafPage) loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
          if (kvPage != null) {  // assert?
            kvPage.incrementPinCount();
          }
          return kvPage;
        });

    if (recordPageFromBuffer != null) {
      setMostRecentlyReadRecordPage(indexLogKey, pageReferenceToRecordPage, recordPageFromBuffer);
      pageReferenceToRecordPage.setPage(recordPageFromBuffer);
      assert !recordPageFromBuffer.isClosed();
      return recordPageFromBuffer;
    }

    return null;
  }

  private void setMostRecentlyReadRecordPage(@NonNull IndexLogKey indexLogKey, @NonNull PageReference pageReference,
      KeyValueLeafPage recordPage) {
    if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
      if (pathSummaryRecordPage != null) {
        if (trxIntentLog == null) {
          if (resourceBufferManager.getRecordPageCache().get(pathSummaryRecordPage.pageReference) != null) {
            assert !pathSummaryRecordPage.page.isClosed();
            resourceBufferManager.getRecordPageCache().get(pathSummaryRecordPage.pageReference, (_, _) -> {
              var kvPage = pathSummaryRecordPage.page;
              kvPage.decrementPinCount();
              return kvPage;
            });
//            pathSummaryRecordPage.page.decrementPinCount();
//            resourceBufferManager.getRecordPageCache()
//                                 .put(pathSummaryRecordPage.pageReference, pathSummaryRecordPage.page);
          }
        } else {
          pathSummaryRecordPage.pageReference.setPage(null);
          pathSummaryRecordPage.page.clear();
        }
      }

      pathSummaryRecordPage = new RecordPage(indexLogKey.getIndexNumber(),
                                             indexLogKey.getIndexType(),
                                             indexLogKey.getRecordPageKey(),
                                             indexLogKey.getRevisionNumber(),
                                             pageReference,
                                             recordPage);
    } else {
      // May not be in the record page cache if it's a read-write trx as the page is removed from the cache once it's
      // put into the trx intent log.
      if (secondMostRecentlyReadRecordPage != null
          && resourceBufferManager.getRecordPageCache().get(secondMostRecentlyReadRecordPage.pageReference) != null) {
        assert !secondMostRecentlyReadRecordPage.page.isClosed();
        resourceBufferManager.getRecordPageCache().get(secondMostRecentlyReadRecordPage.pageReference, (_, _) -> {
          var kvPage = secondMostRecentlyReadRecordPage.page;
          kvPage.decrementPinCount();
          return kvPage;
        });
//        secondMostRecentlyReadRecordPage.page.decrementPinCount();
//        resourceBufferManager.getRecordPageCache()
//                             .put(secondMostRecentlyReadRecordPage.pageReference,
//                                  secondMostRecentlyReadRecordPage.page);
      }
      assert mostRecentlyReadRecordPage == null || !mostRecentlyReadRecordPage.page.isClosed();
      secondMostRecentlyReadRecordPage = mostRecentlyReadRecordPage;
      mostRecentlyReadRecordPage = new RecordPage(indexLogKey.getIndexNumber(),
                                                  indexLogKey.getIndexType(),
                                                  indexLogKey.getRecordPageKey(),
                                                  indexLogKey.getRevisionNumber(),
                                                  pageReference,
                                                  recordPage);
    }
  }

  @Nullable
  private Page loadDataPageFromDurableStorageAndCombinePageFragments(PageReference pageReferenceToRecordPage) {
    if (pageReferenceToRecordPage.getKey() == Constants.NULL_ID_LONG) {
      // No persistent key set to load page from durable storage.
      return null;
    }

    // Load list of page "fragments" from persistent storage.
    final List<KeyValuePage<DataRecord>> pages = getPageFragments(pageReferenceToRecordPage);

    if (pages.isEmpty()) {
      return null;
    }

    final int maxRevisionsToRestore = resourceConfig.maxNumberOfRevisionsToRestore;
    final VersioningType versioningApproach = resourceConfig.versioningType;

    for (var page : pages) {
      assert page.getPinCount() > 0;
      assert !page.isClosed();
    }

    final Page completePage = versioningApproach.combineRecordPages(pages, maxRevisionsToRestore, this);

    unpinPageFragments(pageReferenceToRecordPage, pages);

    pageReferenceToRecordPage.setPage(completePage);

    assert !completePage.isClosed();
    return completePage;
  }

  private void unpinPageFragments(PageReference pageReference, List<KeyValuePage<DataRecord>> pages) {
    if (pages.isEmpty()) {
      return;
    }

    var mostRecentPageFragment = pages.getFirst();
    resourceBufferManager.getPageCache().get(pageReference, (_, _) -> {
      mostRecentPageFragment.decrementPinCount();
      return mostRecentPageFragment;
    });

    var pageFragments = pageReference.getPageFragments();
    for (int i = 1; i < pages.size(); i++) {
      var pageFragment = pages.get(i);
      pageFragment.decrementPinCount();
      var pageFragmentKey = pageFragments.get(i - 1);
      resourceBufferManager.getPageCache().get(new PageReference().setKey(pageFragmentKey.key()), (_, _) -> pageFragment);
    }
  }

  @Nullable
  private Page getInMemoryPageInstance(@NonNull IndexLogKey indexLogKey,
      @NonNull PageReference pageReferenceToRecordPage) {
    Page page = pageReferenceToRecordPage.getPage();

    if (page != null) {
      if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
        resourceBufferManager.getPageCache().get(pageReferenceToRecordPage, (_, _) -> {
          var kvLeafPage = ((KeyValueLeafPage) page);
          kvLeafPage.incrementPinCount();
          return kvLeafPage;
        });
      }
      setMostRecentlyReadRecordPage(indexLogKey, pageReferenceToRecordPage, (KeyValueLeafPage) page);
      return page;
    }

    return null;
  }

  PageReference getLeafPageReference(final @NonNegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    final PageReference pageReferenceToSubtree = getPageReference(rootPage, indexType, indexNumber);
    return getReferenceToLeafOfSubtree(pageReferenceToSubtree, recordPageKey, indexNumber, indexType, rootPage);
  }

  PageReference getLeafPageReference(final PageReference pageReferenceToSubtree, final @NonNegative long recordPageKey,
      final int indexNumber, final IndexType indexType, final RevisionRootPage revisionRootPage) {
    return getReferenceToLeafOfSubtree(pageReferenceToSubtree, recordPageKey, indexNumber, indexType, revisionRootPage);
  }

  /**
   * Dereference key/value page reference and get all leaves, the {@link KeyValuePage}s from the
   * revision-trees.
   *
   * @param pageReference optional page reference pointing to the first page
   * @return dereferenced pages
   * @throws SirixIOException if an I/O-error occurs within the creation process
   */
  @SuppressWarnings("unchecked")
  List<KeyValuePage<DataRecord>> getPageFragments(final PageReference pageReference) {
    assert pageReference != null;
    final ResourceConfiguration config = resourceSession.getResourceConfig();
    final int revsToRestore = config.maxNumberOfRevisionsToRestore;
    final int[] revisionsToRead = config.versioningType.getRevisionRoots(rootPage.getRevision(), revsToRestore);
    final List<KeyValuePage<DataRecord>> pages = new ArrayList<>(revisionsToRead.length);

    final var pageFragments = pageReference.getPageFragments();
    final var pageReferenceWithKey = new PageReference().setKey(pageReference.getKey());

    KeyValuePage<DataRecord> page;

    if (trxIntentLog == null) {
      page = (KeyValuePage<DataRecord>) resourceBufferManager.getPageCache().get(pageReferenceWithKey, (_, value) -> {
        var kvPage = (KeyValueLeafPage) value;
        if (value == null) {
          kvPage = (KeyValueLeafPage) pageReader.read(pageReferenceWithKey, resourceSession.getResourceConfig());
        }
        kvPage.incrementPinCount();
        return kvPage;
      });
      assert !page.isClosed();
    } else {
      page = (KeyValuePage<DataRecord>) pageReader.read(pageReferenceWithKey, resourceSession.getResourceConfig());
      page.incrementPinCount();
    }
    pages.add(page);

    if (pageFragments.isEmpty() || page.size() == Constants.NDP_NODE_COUNT) {
      return pages;
    }

    final List<PageFragmentKey> pageFragmentKeys = new ArrayList<>(pageFragments.size() + 1);
    pageFragmentKeys.addAll(pageFragments);
    pages.addAll(getPreviousPageFragments(pageFragmentKeys));

    return pages;
  }

  private List<KeyValuePage<DataRecord>> getPreviousPageFragments(final List<PageFragmentKey> pageFragments) {
    final var pages = pageFragments.stream().map(this::readPage).collect(Collectors.toList());
    return sequence(pages).join()
                          .stream()
                          .sorted(Comparator.<KeyValuePage<DataRecord>, Integer>comparing(KeyValuePage::getRevision)
                                            .reversed())
                          .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private CompletableFuture<KeyValuePage<DataRecord>> readPage(final PageFragmentKey pageFragmentKey) {
    final var pageReference = new PageReference().setKey(pageFragmentKey.key());
    if (trxIntentLog == null) {
      final var pageFromBufferManager = resourceBufferManager.getPageCache().get(pageReference);
      if (pageFromBufferManager != null) {
        assert !pageFromBufferManager.isClosed();
        assert pageFragmentKey.revision() == ((KeyValuePage<DataRecord>) pageFromBufferManager).getRevision();
        ((KeyValueLeafPage) pageFromBufferManager).incrementPinCount();
        return CompletableFuture.completedFuture((KeyValuePage<DataRecord>) pageFromBufferManager);
      }
    }
    final var reader = resourceSession.createReader();
    return (CompletableFuture<KeyValuePage<DataRecord>>) reader.readAsync(pageReference,
                                                                          resourceSession.getResourceConfig())
                                                               .whenComplete((page, _) -> {
                                                                 reader.close();
                                                                 if (trxIntentLog == null) {
                                                                   assert pageFragmentKey.revision()
                                                                       == ((KeyValuePage<DataRecord>) page).getRevision();
                                                                   synchronized (resourceBufferManager.getPageCache()) {
                                                                     ((KeyValueLeafPage) page).incrementPinCount();
                                                                     resourceBufferManager.getPageCache()
                                                                                          .put(pageReference, page);
                                                                   }
                                                                 } else {
                                                                   ((KeyValuePage<DataRecord>) page).incrementPinCount();
                                                                 }
                                                               });
  }

  static CompletableFuture<List<KeyValuePage<DataRecord>>> sequence(
      List<CompletableFuture<KeyValuePage<DataRecord>>> listOfCompletableFutures) {
    return CompletableFuture.allOf(listOfCompletableFutures.toArray(new CompletableFuture[0]))
                            .thenApply(_ -> listOfCompletableFutures.stream()
                                                                    .map(CompletableFuture::join)
                                                                    .collect(Collectors.toList()));
  }

  /**
   * Get the page reference which points to the right subtree (nodes, path summary nodes, CAS index
   * nodes, Path index nodes or Name index nodes).
   *
   * @param revisionRoot {@link RevisionRootPage} instance
   * @param indexType    the index type
   * @param index        the index to use
   */
  PageReference getPageReference(final RevisionRootPage revisionRoot, final IndexType indexType, final int index) {
    assert revisionRoot != null;
    // $CASES-OMITTED$
    return switch (indexType) {
      case DOCUMENT -> revisionRoot.getIndirectDocumentIndexPageReference();
      case CHANGED_NODES -> revisionRoot.getIndirectChangedNodesIndexPageReference();
      case RECORD_TO_REVISIONS -> revisionRoot.getIndirectRecordToRevisionsIndexPageReference();
      case DEWEYID_TO_RECORDID -> getDeweyIDPage(revisionRoot).getIndirectPageReference();
      case CAS -> getCASPage(revisionRoot).getIndirectPageReference(index);
      case PATH -> getPathPage(revisionRoot).getIndirectPageReference(index);
      case NAME -> getNamePage(revisionRoot).getIndirectPageReference(index);
      case PATH_SUMMARY -> getPathSummaryPage(revisionRoot).getIndirectPageReference(index);
      default ->
          throw new IllegalStateException("Only defined for node, path summary, text value and attribute value pages!");
    };
  }

  /**
   * Dereference indirect page reference.
   *
   * @param reference reference to dereference
   * @return dereferenced page
   * @throws SirixIOException     if something odd happens within the creation process
   * @throws NullPointerException if {@code reference} is {@code null}
   */
  @Override
  public IndirectPage dereferenceIndirectPageReference(final PageReference reference) {
    return (IndirectPage) loadPage(reference);
  }

  /**
   * Find reference pointing to leaf page of an indirect tree.
   *
   * @param startReference start reference pointing to the indirect tree
   * @param pageKey        key to look up in the indirect tree
   * @return reference denoted by key pointing to the leaf page
   * @throws SirixIOException if an I/O error occurs
   */
  @Nullable
  @Override
  public PageReference getReferenceToLeafOfSubtree(final PageReference startReference, final @NonNegative long pageKey,
      final int indexNumber, final @NonNull IndexType indexType, final RevisionRootPage revisionRootPage) {
    assertNotClosed();

    // Initial state pointing to the indirect page of level 0.
    PageReference reference = requireNonNull(startReference);
    int offset;
    long levelKey = pageKey;
    final int[] inpLevelPageCountExp = uberPage.getPageCountExp(indexType);
    final int maxHeight = getCurrentMaxIndirectPageTreeLevel(indexType, indexNumber, revisionRootPage);

    // Iterate through all levels.
    for (int level = inpLevelPageCountExp.length - maxHeight, height = inpLevelPageCountExp.length;
         level < height; level++) {
      final Page derefPage = dereferenceIndirectPageReference(reference);
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

  @Override
  public long pageKey(@NonNegative final long recordKey, @NonNull final IndexType indexType) {
    assertNotClosed();

    return switch (indexType) {
      case PATH_SUMMARY -> recordKey >> Constants.PATHINP_REFERENCE_COUNT_EXPONENT;
      case REVISIONS -> recordKey >> Constants.UBPINP_REFERENCE_COUNT_EXPONENT;
      case PATH, DOCUMENT, CAS, NAME -> recordKey >> Constants.INP_REFERENCE_COUNT_EXPONENT;
      default -> recordKey >> Constants.NDP_NODE_COUNT_EXPONENT;
    };
  }

  @Override
  public int getCurrentMaxIndirectPageTreeLevel(final IndexType indexType, final int index,
      final RevisionRootPage revisionRootPage) {
    final int maxLevel;
    final RevisionRootPage currentRevisionRootPage = revisionRootPage == null ? rootPage : revisionRootPage;

    // $CASES-OMITTED$
    maxLevel = switch (indexType) {
      case REVISIONS -> throw new IllegalStateException();
      case DOCUMENT -> currentRevisionRootPage.getCurrentMaxLevelOfDocumentIndexIndirectPages();
      case CHANGED_NODES -> currentRevisionRootPage.getCurrentMaxLevelOfChangedNodesIndexIndirectPages();
      case RECORD_TO_REVISIONS -> currentRevisionRootPage.getCurrentMaxLevelOfRecordToRevisionsIndexIndirectPages();
      case CAS -> getCASPage(currentRevisionRootPage).getCurrentMaxLevelOfIndirectPages(index);
      case PATH -> getPathPage(currentRevisionRootPage).getCurrentMaxLevelOfIndirectPages(index);
      case NAME -> getNamePage(currentRevisionRootPage).getCurrentMaxLevelOfIndirectPages(index);
      case PATH_SUMMARY -> getPathSummaryPage(currentRevisionRootPage).getCurrentMaxLevelOfIndirectPages(index);
      case DEWEYID_TO_RECORDID -> getDeweyIDPage(currentRevisionRootPage).getCurrentMaxLevelOfIndirectPages();
    };

    return maxLevel;
  }

  @Override
  public RevisionRootPage getActualRevisionRootPage() {
    assertNotClosed();
    return rootPage;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("Session", resourceSession)
                      .add("PageReader", pageReader)
                      .add("UberPage", uberPage)
                      .add("RevRootPage", rootPage)
                      .toString();
  }

  @Override
  public void close() {
    if (!isClosed) {
      if (trxIntentLog == null) {
        pageReader.close();
      }

      if (resourceSession.getNodeReadTrxByTrxId(trxId).isEmpty()) {
        resourceSession.closePageReadTransaction(trxId);
      }

      if (mostRecentlyReadRecordPage != null) {
        if (mostRecentlyReadRecordPage.page.getPinCount() > 0) {
          mostRecentlyReadRecordPage.page.decrementPinCount();
        }
      }

      if (secondMostRecentlyReadRecordPage != null) {
        if (secondMostRecentlyReadRecordPage.page.getPinCount() > 0) {
          secondMostRecentlyReadRecordPage.page.decrementPinCount();
        }
      }

      if (pathSummaryRecordPage != null) {
        if (pathSummaryRecordPage.page.getPinCount() > 0) {
          pathSummaryRecordPage.page.decrementPinCount();
        }
        if (trxIntentLog != null) {
          pathSummaryRecordPage.page.clear();
        }
      }

      isClosed = true;
    }
  }

  @Override
  public int getNameCount(final int key, @NonNull final NodeKind kind) {
    assertNotClosed();
    return namePage.getCount(key, kind, this);
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public int getRevisionNumber() {
    assertNotClosed();
    return rootPage.getRevision();
  }

  @Override
  public Reader getReader() {
    assertNotClosed();
    return pageReader;
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    assertNotClosed();
    return rootPage.getCommitCredentials();
  }
}
