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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.MoreObjects;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.access.trx.node.InternalResourceSession;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.ResourceSession;
import org.sirix.cache.*;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.io.BytesUtils;
import org.sirix.io.Reader;
import org.sirix.node.DeletedNode;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.*;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.page.interfaces.PageFragmentKey;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.settings.VersioningType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Page read-only transaction. The only thing shared amongst transactions is the resource manager.
 * Everything else is exclusive to this transaction. It is required that only a single thread has
 * access to this transaction.
 */
public final class NodePageReadOnlyTrx implements PageReadOnlyTrx {

  private record RecordPage(int index, IndexType indexType, long recordPageKey, int revision, Page page) {
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
  private final long trxId;

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

  private final Bytes<ByteBuffer> byteBufferForRecords = Bytes.elasticByteBuffer(40);

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
  public NodePageReadOnlyTrx(final long trxId,
      final InternalResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> resourceSession,
      final UberPage uberPage, final @NonNegative int revision, final Reader reader,
      final BufferManager resourceBufferManager, final @NonNull RevisionRootPageReader revisionRootPageReader,
      final @Nullable TransactionIntentLog trxIntentLog) {
    checkArgument(trxId > 0, "Transaction-ID must be >= 0.");
    this.trxId = trxId;
    this.resourceBufferManager = resourceBufferManager;
    this.isClosed = false;
    this.resourceSession = checkNotNull(resourceSession);
    this.resourceConfig = resourceSession.getResourceConfig();
    this.pageReader = checkNotNull(reader);
    this.uberPage = checkNotNull(uberPage);
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
      page = resourceBufferManager.getPageCache().get(reference);
      if (page != null) {
        reference.setPage(page);
        return page;
      }
    }

    if (reference.getKey() != Constants.NULL_ID_LONG || reference.getLogKey() != Constants.NULL_ID_INT) {
      page = pageReader.read(reference, this);
    }

    if (page != null) {
      reference.setPage(page);
      putIntoPageCacheIfItIsNotAWriteTrx(reference, page);
    }
    return page;
  }

  private void putIntoPageCacheIfItIsNotAWriteTrx(PageReference reference, Page page) {
    assert reference.getLogKey() == Constants.NULL_ID_INT;
    if (trxIntentLog == null && !(page instanceof UberPage)) {
      // Put page into buffer manager.
      resourceBufferManager.getPageCache().put(reference, page);
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
  public long getTrxId() {
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
    if (isClosed) {
      throw new IllegalStateException("Transaction is already closed.");
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends DataRecord> V getRecord(final long recordKey, @NonNull final IndexType indexType,
      @NonNegative final int index) {
    checkNotNull(indexType);
    assertNotClosed();

    if (recordKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return null;
    }

    final long recordPageKey = pageKey(recordKey, indexType);

    var indexLogKey = new IndexLogKey(indexType, recordPageKey, index, revisionNumber);

    // $CASES-OMITTED$
    final Page page = switch (indexType) {
      case DOCUMENT, CHANGED_NODES, RECORD_TO_REVISIONS, PATH_SUMMARY, PATH, CAS, NAME -> getRecordPage(indexLogKey);
      default -> throw new IllegalStateException();
    };

    if (page == null) {
      return null;
    }

    final var dataRecord = getValue(((KeyValueLeafPage) page), recordKey);

    return (V) checkItemIfDeleted(dataRecord);
  }

  @Override
  public DataRecord getValue(final KeyValueLeafPage page, final long nodeKey) {
    final var offset = PageReadOnlyTrx.recordPageOffset(nodeKey);
    DataRecord record = page.getRecord(offset);
    if (record == null) {
      byte[] data = page.getSlot(offset);
      if (data != null) {
        record = getDataRecord(nodeKey, offset, data, page);
      }
      if (record != null) {
        return record;
      }
      try {
        final PageReference reference = page.getPageReference(nodeKey);
        if (reference != null && reference.getKey() != Constants.NULL_ID_LONG) {
          data = ((OverflowPage) pageReader.read(reference, this)).getData();
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
    var record = resourceConfig.recordPersister.deserialize(byteBufferForRecords, key, page.getDeweyId(offset), this);
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
      RevisionRootPage revisionRootPage = cache.get(revisionKey);
      if (revisionRootPage == null) {
        revisionRootPage = pageReader.readRevisionRootPage(revisionKey, this);
        cache.put(revisionKey, revisionRootPage);
      }
      return revisionRootPage;
    } else {
      if (revisionKey == 0 && uberPage.getRevisionRootReference() != null) {
        final var revisionRootPageReference = uberPage.getRevisionRootReference();
        final var pageContainer = trxIntentLog.get(revisionRootPageReference);
        return (RevisionRootPage) pageContainer.getModified();
      }
      return pageReader.readRevisionRootPage(revisionKey, this);
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

  @Override
  public Page getRecordPage(@NonNull IndexLogKey indexLogKey) {
    assertNotClosed();
    checkArgument(indexLogKey.getRecordPageKey() >= 0, "recordPageKey must not be negative!");

    // First: Check most recent pages.
    if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY && isMostRecentlyReadPathSummaryPage(indexLogKey)) {
      return pathSummaryRecordPage.page();
    }
    if (isMostRecentlyReadPage(indexLogKey)) {
      return mostRecentlyReadRecordPage.page();
    }
    if (isSecondMostRecentlyReadPage(indexLogKey)) {
      return secondMostRecentlyReadRecordPage.page();
    }

    // Second: Traverse trie.
    final var pageReferenceToRecordPage = getLeafPageReference(indexLogKey.getRecordPageKey(),
                                                               indexLogKey.getIndexNumber(),
                                                               checkNotNull(indexLogKey.getIndexType()));

    if (pageReferenceToRecordPage == null) {
      return null;
    }

    // Third: Try to get in-memory instance.
    var page = getInMemoryPageInstance(indexLogKey, pageReferenceToRecordPage);
    if (page != null) {
      return page;
    }

    // Fourth: Try to get from resource buffer manager.
    Page recordPageFromBuffer = getFromBufferManager(indexLogKey, pageReferenceToRecordPage);
    if (recordPageFromBuffer != null) {
      return recordPageFromBuffer;
    }

    if (pageReferenceToRecordPage.getKey() == Constants.NULL_ID_LONG) {
      // No persistent key set to load page from durable storage.
      return null;
    }

    return loadDataPageFromDurableStorageAndCombinePageFragments(indexLogKey, pageReferenceToRecordPage);
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
  private Page getFromBufferManager(@NotNull IndexLogKey indexLogKey, PageReference pageReferenceToRecordPage) {
    //if (trxIntentLog == null) {
      final Page recordPageFromBuffer = resourceBufferManager.getRecordPageCache().get(pageReferenceToRecordPage);

      if (recordPageFromBuffer != null) {
        setMostRecentlyReadRecordPage(indexLogKey, recordPageFromBuffer);
        pageReferenceToRecordPage.setPage(recordPageFromBuffer);
        return recordPageFromBuffer;
      }
    //}
    return null;
  }

  private void setMostRecentlyReadRecordPage(@NotNull IndexLogKey indexLogKey, Page recordPageFromBuffer) {
    if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
      pathSummaryRecordPage = new RecordPage(indexLogKey.getIndexNumber(),
                                             indexLogKey.getIndexType(),
                                             indexLogKey.getRecordPageKey(),
                                             indexLogKey.getRevisionNumber(),
                                             recordPageFromBuffer);
    } else {
      secondMostRecentlyReadRecordPage = mostRecentlyReadRecordPage;
      mostRecentlyReadRecordPage = new RecordPage(indexLogKey.getIndexNumber(),
                                                  indexLogKey.getIndexType(),
                                                  indexLogKey.getRecordPageKey(),
                                                  indexLogKey.getRevisionNumber(),
                                                  recordPageFromBuffer);
    }
  }

  @Nullable
  private Page loadDataPageFromDurableStorageAndCombinePageFragments(@NotNull IndexLogKey indexLogKey,
      PageReference pageReferenceToRecordPage) {
    // Load list of page "fragments" from persistent storage.
    final List<KeyValuePage<DataRecord>> pages = getPageFragments(pageReferenceToRecordPage);

    if (pages.isEmpty()) {
      return null;
    }

    final int maxRevisionsToRestore = resourceConfig.maxNumberOfRevisionsToRestore;
    final VersioningType versioningApproach = resourceConfig.versioningType;
    final Page completePage = versioningApproach.combineRecordPages(pages, maxRevisionsToRestore, this);

    if (trxIntentLog == null) {
      resourceBufferManager.getRecordPageCache().put(pageReferenceToRecordPage, completePage);
    }

    pageReferenceToRecordPage.setPage(completePage);
    setMostRecentlyReadRecordPage(indexLogKey, completePage);
    return completePage;
  }

  @Nullable
  private Page getInMemoryPageInstance(@NotNull IndexLogKey indexLogKey,
      @NotNull PageReference pageReferenceToRecordPage) {
    Page page = pageReferenceToRecordPage.getPage();

    if (page != null) {
      setMostRecentlyReadRecordPage(indexLogKey, page);
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
      page = (KeyValuePage<DataRecord>) resourceBufferManager.getPageCache().get(pageReferenceWithKey);
      //assert page == null || page.getRevision() == getRevisionNumber();
      if (page == null) {
        page = (KeyValuePage<DataRecord>) pageReader.read(pageReferenceWithKey, this);

        assert pageReferenceWithKey.getLogKey() == Constants.NULL_ID_INT;
        resourceBufferManager.getPageCache().put(pageReferenceWithKey, page);
      }
    } else {
      page = (KeyValuePage<DataRecord>) pageReader.read(pageReferenceWithKey, this);
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
        assert pageFragmentKey.revision() == ((KeyValuePage<DataRecord>) pageFromBufferManager).getRevision();
        return CompletableFuture.completedFuture((KeyValuePage<DataRecord>) pageFromBufferManager);
      }
    }
    final var pageReadOnlyTrx = resourceSession.beginPageReadOnlyTrx(pageFragmentKey.revision());
    //noinspection unchecked
    return (CompletableFuture<KeyValuePage<DataRecord>>) pageReadOnlyTrx.getReader()
                                                                        .readAsync(pageReference, pageReadOnlyTrx)
                                                                        .whenComplete((page, exception) -> {
                                                                          pageReadOnlyTrx.close();
                                                                          if (trxIntentLog == null) {
                                                                            assert pageFragmentKey.revision()
                                                                                == ((KeyValuePage<DataRecord>) page).getRevision();
                                                                            resourceBufferManager.getPageCache()
                                                                                                 .put(pageReference,
                                                                                                      page);
                                                                          }
                                                                        });
  }

  static CompletableFuture<List<KeyValuePage<DataRecord>>> sequence(
      List<CompletableFuture<KeyValuePage<DataRecord>>> listOfCompletableFutures) {
    return CompletableFuture.allOf(listOfCompletableFutures.toArray(new CompletableFuture[0]))
                            .thenApply(v -> listOfCompletableFutures.stream()
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
    PageReference reference = checkNotNull(startReference);
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
  public synchronized void close() {
    if (!isClosed) {
      if (trxIntentLog == null) {
        pageReader.close();
      }

      if (resourceSession.getNodeReadTrxByTrxId(trxId).isEmpty()) {
        resourceSession.closePageReadTransaction(trxId);
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
