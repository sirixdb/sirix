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

import com.google.common.base.MoreObjects;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.cache.*;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Page read-only transaction. The only thing shared amongst transactions is the resource manager.
 * Everything else is exclusive to this transaction. It is required that only a single thread has
 * access to this transaction.
 */
public final class NodePageReadOnlyTrx implements PageReadOnlyTrx {
  /**
   * Page reader exclusively assigned to this transaction.
   */
  private final Reader pageReader;

  /**
   * Uber page this transaction is bound to.
   */
  private final UberPage uberPage;

  /**
   * {@link InternalResourceManager} reference.
   */
  final InternalResourceManager<?, ?> resourceManager;

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

  private final ExecutorService pool;

  /**
   * Standard constructor.
   *
   * @param trxId                 the transaction-ID.
   * @param resourceManager       the resource manager
   * @param uberPage              {@link UberPage} to start reading from
   * @param revision              key of revision to read from uber page
   * @param reader                to read stored pages for this transaction
   * @param trxIntentLog          transaction intent log
   * @param resourceBufferManager caches in-memory reconstructed pages
   * @throws SirixIOException if reading of the persistent storage fails
   */
  public NodePageReadOnlyTrx(final long trxId,
      final InternalResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx> resourceManager,
      final UberPage uberPage, final @Nonnegative int revision, final Reader reader,
      final @Nullable TransactionIntentLog trxIntentLog, final BufferManager resourceBufferManager,
      final @Nonnull RevisionRootPageReader revisionRootPageReader) {
    checkArgument(revision >= 0, "Revision must be >= 0.");
    checkArgument(trxId > 0, "Transaction-ID must be >= 0.");
    this.trxId = trxId;
    this.resourceBufferManager = resourceBufferManager;
    this.trxIntentLog = trxIntentLog;
    this.isClosed = false;
    this.resourceManager = checkNotNull(resourceManager);
    this.resourceConfig = resourceManager.getResourceConfig();
    this.pageReader = checkNotNull(reader);
    this.uberPage = checkNotNull(uberPage);
    this.pool = Executors.newFixedThreadPool(resourceConfig.numberOfRevisionsToRestore - 1);

    revisionNumber = revision;
    rootPage = revisionRootPageReader.loadRevisionRootPage(this, revision);
    namePage = revisionRootPageReader.getNamePage(this, rootPage);
  }

  private Page loadPage(final PageReference reference) {
    Page page = reference.getPage();
    if (page == null) {
      if (trxIntentLog != null) {
        // Try to get it from the transaction log if it's present.
        final PageContainer cont = trxIntentLog.get(reference, this);
        page = cont == null ? null : cont.getComplete();
      }

      if (page == null) {
        if (trxIntentLog == null) {
          // Putting to the transaction log afterwards would otherwise render the cached entry invalid
          // as the reference log key is set and the key is reset to Constants.NULL_ID_LONG.
          page = resourceBufferManager.getPageCache().get(reference);
        }

        if (page == null) {
          page = pageReader.read(reference, this);

          if (page != null) {
            assert reference.getLogKey() == Constants.NULL_ID_INT
                && reference.getPersistentLogKey() == Constants.NULL_ID_LONG;
            if (trxIntentLog == null) {
              // Put page into buffer manager.
              resourceBufferManager.getPageCache().put(reference, page);
            }
            reference.setPage(page);
          }
        }
      }
    }

    return page;
  }

  @Override
  public long getTrxId() {
    assertNotClosed();
    return trxId;
  }

  @Override
  public ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceManager() {
    assertNotClosed();
    return resourceManager;
  }

  /**
   * Make sure that the transaction is not yet closed when calling this method.
   */
  void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Transaction is already closed.");
    }
  }

  @Override
  public <K, V> Optional<V> getRecord(@Nonnull final K key, @Nonnull final IndexType indexType,
      @Nonnegative final int index) {
    checkNotNull(key);
    checkNotNull(indexType);
    assertNotClosed();

    if (key instanceof Long nodeKey) {
      if (nodeKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        return Optional.empty();
      }

      final long recordPageKey = pageKey(nodeKey, indexType);

      // $CASES-OMITTED$
      final Optional<Page> page = switch (indexType) {
        case DOCUMENT, CHANGED_NODES, RECORD_TO_REVISIONS, PATH_SUMMARY, PATH, CAS, NAME -> getRecordPage(new IndexLogKey(
            indexType,
            recordPageKey,
            index,
            revisionNumber));
        default -> throw new IllegalStateException();
      };

      //noinspection unchecked
      return (Optional<V>) page.map(thePage -> ((UnorderedKeyValuePage) thePage).getValue(nodeKey))
                               .flatMap(this::checkItemIfDeleted);
    }

    return Optional.empty();
  }

  /**
   * Method to check if an {@link DataRecord} is deleted.
   *
   * @param toCheck node to check
   * @return the {@code node} if it is valid, {@code null} otherwise
   */
  Optional<DataRecord> checkItemIfDeleted(final @Nullable DataRecord toCheck) {
    if (toCheck instanceof DeletedNode) {
      return Optional.empty();
    } else {
      return Optional.ofNullable(toCheck);
    }
  }

  @Override
  public String getName(final int nameKey, @Nonnull final NodeKind nodeKind) {
    assertNotClosed();
    return namePage.getName(nameKey, nodeKind, this);
  }

  @Override
  public byte[] getRawName(final int nameKey, @Nonnull final NodeKind nodeKind) {
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
  public RevisionRootPage loadRevRoot(@Nonnegative final int revisionKey) {
    checkArgument(revisionKey >= 0 && revisionKey <= resourceManager.getMostRecentRevisionNumber(),
                  "%s must be >= 0 and <= last stored revision (%s)!",
                  revisionKey,
                  resourceManager.getMostRecentRevisionNumber());
    if (trxIntentLog == null) {
      final Cache<Integer, RevisionRootPage> cache = resourceBufferManager.getRevisionRootPageCache();
      RevisionRootPage revisionRootPage = cache.get(revisionKey);
      if (revisionRootPage == null) {
        revisionRootPage = pageReader.readRevisionRootPage(revisionKey, this);
        cache.put(revisionKey, revisionRootPage);
      }
      return revisionRootPage;
    } else {
      // The indirect page reference either fails horribly or returns a non null instance.
      final PageReference reference =
          getReferenceToLeafOfSubtree(uberPage.getIndirectPageReference(), revisionKey, -1, IndexType.REVISIONS);

      // Try to get it from the transaction log if it's present.
      final PageContainer cont = trxIntentLog.get(reference, this);
      RevisionRootPage page = cont == null ? null : (RevisionRootPage) cont.getComplete();

      if (page == null && reference != null) {
        assert reference.getKey() != Constants.NULL_ID_LONG || reference.getLogKey() != Constants.NULL_ID_INT
            || reference.getPersistentLogKey() != Constants.NULL_ID_LONG;
        page = (RevisionRootPage) loadPage(reference);
      }

      return page;
    }
  }

  @Override
  public NamePage getNamePage(@Nonnull final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (NamePage) getPage(revisionRoot.getNamePageReference());
  }

  @Override
  public PathSummaryPage getPathSummaryPage(@Nonnull final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (PathSummaryPage) getPage(revisionRoot.getPathSummaryPageReference());
  }

  @Override
  public PathPage getPathPage(@Nonnull final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (PathPage) getPage(revisionRoot.getPathPageReference());
  }

  @Override
  public CASPage getCASPage(@Nonnull final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (CASPage) getPage(revisionRoot.getCASPageReference());
  }

  @Override
  public DeweyIDPage getDeweyIDPage(@Nonnull final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (DeweyIDPage) getPage(revisionRoot.getDeweyIdPageReference());
  }

  /**
   * Set the page if it is not set already.
   *
   * @param reference page reference
   * @throws SirixIOException if an I/O error occurs
   */
  private Page getPage(final PageReference reference) {
    Page page = reference.getPage();

    if (page == null) {
      page = loadPage(reference);
      reference.setPage(page);
    }

    return page;
  }

  @Override
  public UberPage getUberPage() {
    assertNotClosed();
    return uberPage;
  }

  @Override
  public <K, V, T extends KeyValuePage<K, V>> Optional<Page> getRecordPage(@Nonnull final IndexLogKey indexLogKey) {
    assertNotClosed();
    checkArgument(indexLogKey.getRecordPageKey() >= 0, "recordPageKey must not be negative!");

    if (isMostRecentlyReadPage(indexLogKey)) {
      return Optional.of(mostRecentlyReadRecordPage.getPage());
    }

    final Optional<PageReference> pageReferenceToRecordPage = getLeafPageReference(indexLogKey.getRecordPageKey(),
                                                                                   indexLogKey.getIndex(),
                                                                                   checkNotNull(indexLogKey.getIndexType()));

    if (pageReferenceToRecordPage.isEmpty()) {
      return Optional.empty();
    }

    // Try to get from resource buffer manager.
    if (trxIntentLog == null) {
      final var page = pageReferenceToRecordPage.get().getPage();

      if (page != null) {
        mostRecentlyReadRecordPage =
            new RecordPage(indexLogKey.getIndex(), indexLogKey.getIndexType(), indexLogKey.getRecordPageKey(), page);
        return Optional.of(page);
      }

      final Page recordPageFromBuffer = resourceBufferManager.getRecordPageCache().get(pageReferenceToRecordPage.get());

      if (recordPageFromBuffer != null) {
        mostRecentlyReadRecordPage = new RecordPage(indexLogKey.getIndex(),
                                                    indexLogKey.getIndexType(),
                                                    indexLogKey.getRecordPageKey(),
                                                    recordPageFromBuffer);
        return Optional.of(recordPageFromBuffer);
      }
    }

    // Load list of page "fragments" from persistent storage.
    final List<T> pages = getPageFragments(pageReferenceToRecordPage.get());

    if (pages.isEmpty()) {
      return Optional.empty();
    }

    final int mileStoneRevision = resourceConfig.numberOfRevisionsToRestore;
    final VersioningType versioningApproach = resourceConfig.revisioningType;
    final Page completePage = versioningApproach.combineRecordPages(pages, mileStoneRevision, this);

    if (trxIntentLog == null) {
      resourceBufferManager.getRecordPageCache().put(pageReferenceToRecordPage.get(), completePage);
      pageReferenceToRecordPage.get().setPage(completePage);
    }

    mostRecentlyReadRecordPage = new RecordPage(indexLogKey.getIndex(),
                                                indexLogKey.getIndexType(),
                                                indexLogKey.getRecordPageKey(),
                                                completePage);

    return Optional.of(completePage);
  }

  private boolean isMostRecentlyReadPage(IndexLogKey indexLogKey) {
    return mostRecentlyReadRecordPage != null
        && mostRecentlyReadRecordPage.getRecordPageKey() == indexLogKey.getRecordPageKey()
        && mostRecentlyReadRecordPage.getIndex() == indexLogKey.getIndex()
        && mostRecentlyReadRecordPage.getIndexType() == indexLogKey.getIndexType();
  }

  Optional<PageReference> getLeafPageReference(final @Nonnegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    final PageReference pageReferenceToSubtree = getPageReference(rootPage, indexType, indexNumber);
    return Optional.ofNullable(getReferenceToLeafOfSubtree(pageReferenceToSubtree,
                                                           recordPageKey,
                                                           indexNumber,
                                                           indexType));
  }

  /**
   * Dereference key/value page reference and get all leaves, the {@link KeyValuePage}s from the
   * revision-trees.
   *
   * @param pageReference optional page reference pointing to the first page
   * @return dereferenced pages
   * @throws SirixIOException if an I/O-error occurs within the creation process
   */
  <K, V, T extends KeyValuePage<? extends K, ? extends V>> List<T> getPageFragments(final PageReference pageReference) {
    assert pageReference != null;
    final ResourceConfiguration config = resourceManager.getResourceConfig();
    final int revsToRestore = config.numberOfRevisionsToRestore;
    final int[] revisionsToRead = config.revisioningType.getRevisionRoots(rootPage.getRevision(), revsToRestore);
    final List<T> pages = new ArrayList<>(revisionsToRead.length);

    final var pageFragments = pageReference.getPageFragments();

    @SuppressWarnings("unchecked") final T page = (T) pageReader.read(pageReference, this);
    pages.add(page);

    if (pageFragments.isEmpty() || ((UnorderedKeyValuePage) page).entrySet().size() == Constants.NDP_NODE_COUNT) {
      return pages;
    }

    final List<PageFragmentKey> pageFragmentKeys = new ArrayList<>(pageFragments.size() + 1);
    pageFragmentKeys.addAll(pageFragments);
    pages.addAll(getPreviousPageFragments(pageFragmentKeys));

    return pages;
  }

  @SuppressWarnings("unchecked")
  private <K, V, T extends KeyValuePage<? extends K, ? extends V>> List<T> getPreviousPageFragments(
      final Collection<PageFragmentKey> pageFragments) {
    final var pages = pageFragments.stream()
                                   .map(CompletableFuture::completedFuture)
                                   .map(future -> future.thenApplyAsync(pageFragmentKey -> (T) readPage(pageFragmentKey),
                                                                        pool))
                                   .collect(Collectors.toList());

    return sequence(pages).join()
                          .stream()
                          .sorted(Comparator.<T, Integer>comparing(KeyValuePage::getRevision).reversed())
                          .collect(Collectors.toList());
  }

  private Page readPage(final PageFragmentKey pageFragmentKey) {
    try (final var pageReadOnlyTrx = resourceManager.beginPageReadOnlyTrx(pageFragmentKey.revision())) {
      return pageReadOnlyTrx.getReader().read(new PageReference().setKey(pageFragmentKey.key()), pageReadOnlyTrx);
    }
  }

  static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> listOfCompletableFutures) {
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
      default -> throw new IllegalStateException(
          "Only defined for node, path summary, text value and attribute value pages!");
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
    IndirectPage page = null;

    if (trxIntentLog != null) {
      // Try to get it from the transaction log if it's present.
      final PageContainer cont = trxIntentLog.get(reference, this);
      page = cont == null ? null : (IndirectPage) cont.getComplete();
    }

    if (page == null) {
      // Then try to get the in-memory reference.
      page = (IndirectPage) reference.getPage();
    }

    if (page == null && (reference.getKey() != Constants.NULL_ID_LONG || reference.getLogKey() != Constants.NULL_ID_INT
        || reference.getPersistentLogKey() != Constants.NULL_ID_LONG)) {
      // Then try to get it from the page cache which might read it from the persistent storage on a cache miss.
      page = (IndirectPage) loadPage(reference);
    }

    return page;
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
  public PageReference getReferenceToLeafOfSubtree(final PageReference startReference, final @Nonnegative long pageKey,
      final int indexNumber, final @Nonnull IndexType indexType) {
    assertNotClosed();

    // Initial state pointing to the indirect page of level 0.
    PageReference reference = checkNotNull(startReference);
    checkArgument(pageKey >= 0, "page key must be >= 0!");
    int offset;
    long levelKey = pageKey;
    final int[] inpLevelPageCountExp = uberPage.getPageCountExp(indexType);
    final int maxHeight = getCurrentMaxIndirectPageTreeLevel(indexType, indexNumber, null);

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
  public long pageKey(@Nonnegative final long recordKey, @Nonnull final IndexType indexType) {
    assertNotClosed();
    checkArgument(recordKey >= 0, "recordKey must not be negative!");

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
      case REVISIONS -> uberPage.getCurrentMaxLevelOfIndirectPages();
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
                      .add("Session", resourceManager)
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

      if (resourceBufferManager instanceof BufferManagerImpl) {
        ((BufferManagerImpl) resourceBufferManager).close();
      }

      if (resourceManager.getNodeReadTrxByTrxId(trxId).isEmpty()) {
        resourceManager.closePageReadTransaction(trxId);
      }

      pool.shutdownNow();
      try {
        pool.awaitTermination(2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      isClosed = true;
    }
  }

  @Override
  public int getNameCount(final int key, @Nonnull final NodeKind kind) {
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

  @Override
  public int recordPageOffset(final long key) {
    assertNotClosed();
    return (int) (key - ((key >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
  }

  private static class RecordPage {
    private final int index;

    private final IndexType indexType;

    private final long recordPageKey;

    private final Page page;

    public RecordPage(int index, IndexType indexType, long recordPageKey, Page page) {
      this.index = index;
      this.indexType = indexType;
      this.recordPageKey = recordPageKey;
      this.page = page;
    }

    public int getIndex() {
      return index;
    }

    public long getRecordPageKey() {
      return recordPageKey;
    }

    public IndexType getIndexType() {
      return indexType;
    }

    public Page getPage() {
      return page;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      RecordPage that = (RecordPage) o;
      return index == that.index && recordPageKey == that.recordPageKey && indexType == that.indexType
          && Objects.equals(page, that.page);
    }

    @Override
    public int hashCode() {
      return Objects.hash(index, indexType, recordPageKey, page);
    }
  }
}
