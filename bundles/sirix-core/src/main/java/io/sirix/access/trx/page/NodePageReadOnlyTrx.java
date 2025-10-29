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
import io.sirix.io.Reader;
import io.sirix.node.DeletedNode;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.*;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.settings.VersioningType;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
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

  // DEBUG FLAG: Enable with -Dsirix.debug.path.summary=true
  private static final boolean DEBUG_PATH_SUMMARY = 
    Boolean.getBoolean("sirix.debug.path.summary");

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
   * The unique database ID for this transaction.
   */
  private final long databaseId;

  /**
   * The unique resource ID for this transaction.
   */
  private final long resourceId;

  /**
   * Track all pages pinned by this transaction for guaranteed cleanup.
   * Maps page instance to pin count for this transaction.
   */
  private final java.util.Map<KeyValueLeafPage, Integer> pinnedPagesByThisTrx = new java.util.concurrent.ConcurrentHashMap<>();

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

    // Extract database and resource IDs for use in composite cache keys
    this.databaseId = resourceConfig.getDatabaseId();
    this.resourceId = resourceConfig.getID();

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

 //   if (trxIntentLog == null) {
      assert reference.getLogKey() == Constants.NULL_ID_INT;
      page = resourceBufferManager.getPageCache().get(reference, (_, _) -> {
        try {
          // Reader will fixup PageReference IDs during deserialization
          return pageReader.read(reference, resourceSession.getResourceConfig());
        } catch (final SirixIOException e) {
          throw new IllegalStateException(e);
        }
      });
      if (page != null) {
        reference.setPage(page);
      }
      return page;
//    }

//    if (reference.getKey() != Constants.NULL_ID_LONG || reference.getLogKey() != Constants.NULL_ID_INT) {
//      page = pageReader.read(reference, resourceSession.getResourceConfig());
//    }
//
//    if (page != null) {
//      putIntoPageCache(reference, page);
//      reference.setPage(page);
//    }
//    return page;
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
  public long getDatabaseId() {
    assertNotClosed();
    return databaseId;
  }

  @Override
  public long getResourceId() {
    assertNotClosed();
    return resourceId;
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
      var data = page.getSlot(offset);
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

  private DataRecord getDataRecord(long key, int offset, MemorySegment data, KeyValueLeafPage page) {
    var record = resourceConfig.recordPersister.deserialize(new MemorySegmentBytesIn(data),
                                                            key,
                                                            page.getDeweyIdAsByteArray(offset),
                                                            resourceConfig);
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
      final Cache<RevisionRootPageCacheKey, RevisionRootPage> cache = resourceBufferManager.getRevisionRootPageCache();
      final var cacheKey = new RevisionRootPageCacheKey(databaseId, resourceId, revisionKey);
      return cache.get(cacheKey, (_, _) -> pageReader.readRevisionRootPage(revisionKey, resourceConfig));
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
      assert page instanceof KeyValueLeafPage;
      setMostRecentlyReadRecordPage(indexLogKey, pageReferenceToRecordPage, (KeyValueLeafPage) page);
      return new PageReferenceToPage(pageReferenceToRecordPage, page);
    }

    // Fourth: Try to get from resource buffer manager.
    if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
      if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
        System.err.println("\n[PATH_SUMMARY-DECISION] Using normal cache (read-only trx):");
        System.err.println("  - recordPageKey: " + indexLogKey.getRecordPageKey());
        System.err.println("  - revision: " + indexLogKey.getRevisionNumber());
      }
      
      page = getFromBufferManager(indexLogKey, pageReferenceToRecordPage);
      
      if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY && page instanceof KeyValueLeafPage kvp) {
        System.err.println("[PATH_SUMMARY-NORMAL]   -> Got page from cache: " +
                           "pageKey=" + kvp.getPageKey() +
                           ", revision=" + kvp.getRevision());
      }

      assert page instanceof KeyValueLeafPage;
      setMostRecentlyReadRecordPage(indexLogKey, pageReferenceToRecordPage, (KeyValueLeafPage) page);
      return new PageReferenceToPage(pageReferenceToRecordPage, page);
    }

    // PATH_SUMMARY bypass for write transactions - REQUIRED due to cache key limitations
    // RecordPageCache uses PageReference (key + logKey) as cache key, which doesn't include revision.
    // Different revisions of the same page can have the same PageReference → cache returns wrong revision.
    // Bypass loads directly from disk to avoid stale cached pages.
    if (DEBUG_PATH_SUMMARY) {
      System.err.println("\n[PATH_SUMMARY-DECISION] Using bypass (write trx):");
      System.err.println("  - recordPageKey: " + indexLogKey.getRecordPageKey());
      System.err.println("  - revision: " + indexLogKey.getRevisionNumber());
    }
    
    var loadedPage = (KeyValueLeafPage) loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
    
    if (DEBUG_PATH_SUMMARY && loadedPage != null) {
      System.err.println("[PATH_SUMMARY-BYPASS]   -> Loaded from disk: " +
                         "pageKey=" + loadedPage.getPageKey() +
                         ", revision=" + loadedPage.getRevision());
    }

    setMostRecentlyReadRecordPage(indexLogKey, pageReferenceToRecordPage, loadedPage);
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
    if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
      System.err.println("[PATH_SUMMARY-CACHE-LOOKUP] Looking up in cache: " +
                         "PageRef(key=" + pageReferenceToRecordPage.getKey() +
                         ", logKey=" + pageReferenceToRecordPage.getLogKey() + ")" +
                         ", requestedRevision=" + indexLogKey.getRevisionNumber());
    }
    
    final KeyValueLeafPage recordPageFromBuffer =
        resourceBufferManager.getRecordPageCache().get(pageReferenceToRecordPage, (_, value) -> {
          if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
            if (value != null) {
              System.err.println("[PATH_SUMMARY-CACHE-LOOKUP]   -> Cache HIT: " +
                                 "pageKey=" + value.getPageKey() +
                                 ", revision=" + value.getRevision() +
                                 ", pinCount=" + value.getPinCount());
            } else {
              System.err.println("[PATH_SUMMARY-CACHE-LOOKUP]   -> Cache MISS, loading from disk");
            }
          }
          
          assert value == null || !value.isClosed();
          var kvPage = value;
          if (value == null) {
            kvPage =
                (KeyValueLeafPage) loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
            
            if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY && kvPage != null) {
              System.err.println("[PATH_SUMMARY-CACHE-LOOKUP]   -> Loaded from disk: " +
                                 "pageKey=" + kvPage.getPageKey() +
                                 ", revision=" + kvPage.getRevision());
            }
          }
          if (kvPage != null) {
            kvPage.incrementPinCount(trxId);
          }
          return kvPage;
        });

    if (recordPageFromBuffer != null) {
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
        if (DEBUG_PATH_SUMMARY) {
          System.err.println("[PATH_SUMMARY-REPLACE] Replacing old pathSummaryRecordPage: " +
                             "oldPageKey=" + pathSummaryRecordPage.page.getPageKey() +
                             ", newPageKey=" + recordPage.getPageKey() +
                             ", trxIntentLog=" + (trxIntentLog != null));
        }
        
        if (trxIntentLog == null) {
          if (resourceBufferManager.getRecordPageCache().get(pathSummaryRecordPage.pageReference) != null) {
            assert !pathSummaryRecordPage.page.isClosed();
            pathSummaryRecordPage.page.decrementPinCount(trxId);
            resourceBufferManager.getRecordPageCache()
                                 .put(pathSummaryRecordPage.pageReference, pathSummaryRecordPage.page);
            
            if (DEBUG_PATH_SUMMARY) {
              System.err.println("[PATH_SUMMARY-REPLACE]   -> Read-only: Unpinned and cached old page");
            }
          }
        } else {
          // Write transaction: Bypassed PATH_SUMMARY pages should NOT be in cache
          // But if bypass is disabled for testing, they might be cached
          // Remove from cache before closing to prevent "closed page in cache" errors
          pathSummaryRecordPage.pageReference.setPage(null);
          resourceBufferManager.getRecordPageCache().remove(pathSummaryRecordPage.pageReference);
          
          if (!pathSummaryRecordPage.page.isClosed()) {
            if (DEBUG_PATH_SUMMARY) {
              System.err.println("[PATH_SUMMARY-REPLACE]   -> Write trx: Unpinning and closing page " +
                                 "pageKey=" + pathSummaryRecordPage.page.getPageKey() +
                                 ", revision=" + pathSummaryRecordPage.page.getRevision() +
                                 ", pinCount=" + pathSummaryRecordPage.page.getPinCount());
            }
            // CRITICAL: Must unpin before closing!
            if (pathSummaryRecordPage.page.getPinCount() > 0) {
              pathSummaryRecordPage.page.decrementPinCount(trxId);
            }
            pathSummaryRecordPage.page.close();
          }
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
        secondMostRecentlyReadRecordPage.page.decrementPinCount(trxId);
        resourceBufferManager.getRecordPageCache()
                             .put(secondMostRecentlyReadRecordPage.pageReference,
                                  secondMostRecentlyReadRecordPage.page);
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

    // CRITICAL: Use try-finally to ensure page fragments are ALWAYS unpinned
    // even if combineRecordPages() throws an exception. Without this, pinned
    // fragments leak and their segments are never returned to the allocator.
    try {
      final Page completePage = versioningApproach.combineRecordPages(pages, maxRevisionsToRestore, this);
      pageReferenceToRecordPage.setPage(completePage);
      assert !completePage.isClosed();
      return completePage;
    } finally {
      // ALWAYS unpin fragments, even on exception
      unpinPageFragments(pageReferenceToRecordPage, pages);
    }
  }

  // Package-private for NodePageTrx to call
  void unpinPageFragmentsPublic(PageReference pageReference, List<KeyValuePage<DataRecord>> pages) {
    unpinPageFragments(pageReference, pages);
  }
  
  private void unpinPageFragments(PageReference pageReference, List<KeyValuePage<DataRecord>> pages) {
    if (pages.isEmpty()) {
        return;
    }

    io.sirix.cache.DiagnosticLogger.log("unpinPageFragments: " + pages.size() + " fragments, trxIntentLog=" + (trxIntentLog != null));
    
    int pathSummaryFragments = 0;
    for (var page : pages) {
      if (((KeyValueLeafPage)page).getIndexType() == io.sirix.index.IndexType.PATH_SUMMARY) {
        pathSummaryFragments++;
      }
    }

    // Track which unique page instances we've unpinned to avoid unpinning duplicates
    var unpinnedPages = new java.util.HashSet<KeyValueLeafPage>();

    var mostRecentPageFragment = (KeyValueLeafPage) pages.getFirst();
    // Only unpin ONCE per unique page instance (decrement by 1, not all)
    if (unpinnedPages.add(mostRecentPageFragment)) {
      mostRecentPageFragment.decrementPinCount(trxId);
      resourceBufferManager.getRecordPageFragmentCache().put(pageReference, mostRecentPageFragment);
      io.sirix.cache.DiagnosticLogger.log("  Fragment 0 cached (unpinned)");
    }

    var pageFragments = pageReference.getPageFragments();
    for (int i = 1; i < pages.size(); i++) {
        var pageFragment = (KeyValueLeafPage) pages.get(i);
        
        // Only unpin ONCE per unique page instance (decrement by 1, not all)
        if (unpinnedPages.add(pageFragment)) {
          pageFragment.decrementPinCount(trxId);
          
          var pageFragmentKey = pageFragments.get(i - 1);
          final var fragmentRef = new PageReference()
              .setKey(pageFragmentKey.key())
              .setDatabaseId(databaseId)
              .setResourceId(resourceId);
          resourceBufferManager.getRecordPageFragmentCache().put(fragmentRef, pageFragment);
          io.sirix.cache.DiagnosticLogger.log("  Fragment " + i + " cached (unpinned)");
        }
    }
    
    if (pathSummaryFragments > 0 && trxId >= 3 && trxId <= 10) {
      var fragmentInfo = new StringBuilder();
      for (var page : pages) {
        var kvPage = (KeyValueLeafPage) page;
        if (kvPage.getIndexType() == io.sirix.index.IndexType.PATH_SUMMARY) {
          fragmentInfo.append("Page").append(kvPage.getPageKey())
                      .append("(rev").append(kvPage.getRevision())
                      .append(",pin").append(kvPage.getPinCountByTransaction().getOrDefault(trxId, 0))
                      .append(") ");
        }
      }
      System.err.println("📦 Trx " + trxId + " unpinned " + unpinnedPages.size() + " unique page instances" +
                         " from " + pages.size() + " fragments: " + fragmentInfo);
    }
}

  @Nullable
  private Page getInMemoryPageInstance(@NonNull IndexLogKey indexLogKey,
      @NonNull PageReference pageReferenceToRecordPage) {
    Page page = pageReferenceToRecordPage.getPage();

    if (page != null) {
      // Don't cache PATH_SUMMARY pages from write transactions (bypassed pages)
      // Reason: Different revisions share same PageReference → cache collisions
      if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
        var kvLeafPage = ((KeyValueLeafPage) page);
        if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
          System.err.println("[PATH_SUMMARY-SWIZZLED] Found swizzled page: " +
                             "pageKey=" + kvLeafPage.getPageKey() +
                             ", revision=" + kvLeafPage.getRevision() +
                             ", currentPinCount=" + kvLeafPage.getPinCount() +
                             " -> Incrementing pin");
        }
        kvLeafPage.incrementPinCount(trxId);
        resourceBufferManager.getRecordPageCache().put(pageReferenceToRecordPage, kvLeafPage);
      }
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
  List<KeyValuePage<DataRecord>> getPageFragments(final PageReference pageReference) {
    assert pageReference != null;
    final ResourceConfiguration config = resourceSession.getResourceConfig();
    final int revsToRestore = config.maxNumberOfRevisionsToRestore;
    final int[] revisionsToRead = config.versioningType.getRevisionRoots(rootPage.getRevision(), revsToRestore);
    final List<KeyValuePage<DataRecord>> pages = new ArrayList<>(revisionsToRead.length);

    final var pageFragments = pageReference.getPageFragments();
    final var pageReferenceWithKey = new PageReference()
        .setKey(pageReference.getKey())
        .setDatabaseId(databaseId)
        .setResourceId(resourceId);

    KeyValuePage<DataRecord> page;

//    if (trxIntentLog == null) {
      page = resourceBufferManager.getRecordPageFragmentCache().get(pageReferenceWithKey, (_, value) -> {
        var kvPage = (value != null) ? value :
            (KeyValueLeafPage) pageReader.read(pageReferenceWithKey, resourceSession.getResourceConfig());
        
        // Pin for this transaction (fragments are unpinned after combineRecordPages)
        kvPage.incrementPinCount(trxId);
        return kvPage;
      });
      assert !page.isClosed();
//    } else {
//      page = (KeyValuePage<DataRecord>) pageReader.read(pageReferenceWithKey, resourceSession.getResourceConfig());
//      page.incrementPinCount(trxId);
//    }
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
    final var pageReference = new PageReference()
        .setKey(pageFragmentKey.key())
        .setDatabaseId(databaseId)
        .setResourceId(resourceId);
    
    //if (trxIntentLog == null) {
      final var pageFromBufferManager = resourceBufferManager.getRecordPageFragmentCache().get(pageReference);
      if (pageFromBufferManager != null) {
        assert !pageFromBufferManager.isClosed();
        assert pageFragmentKey.revision() == pageFromBufferManager.getRevision();
        pageFromBufferManager.incrementPinCount(trxId);
        resourceBufferManager.getRecordPageFragmentCache().put(pageReference, pageFromBufferManager);
        return CompletableFuture.completedFuture(pageFromBufferManager);
      }
    //}
    final var reader = resourceSession.createReader();
    return (CompletableFuture<KeyValuePage<DataRecord>>) reader.readAsync(pageReference,
                                                                          resourceSession.getResourceConfig())
                                                               .whenComplete((page, _) -> {
                                                                 reader.close();
                                                                 //if (trxIntentLog == null) {
                                                                   assert pageFragmentKey.revision()
                                                                       == ((KeyValuePage<DataRecord>) page).getRevision();
                                                                   synchronized (resourceBufferManager.getRecordPageFragmentCache()) {
                                                                     var kvPage = (KeyValueLeafPage) page;
                                                                     kvPage.incrementPinCount(trxId);
                                                                     resourceBufferManager.getRecordPageFragmentCache()
                                                                                          .put(pageReference, kvPage);
                                                                   }
                                                                 //} else {
                                                                 //  ((KeyValuePage<DataRecord>) page).incrementPinCount(trxId);
                                                                 //}
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

  /**
   * Unpin all pages that are still pinned by this transaction.
   * This ensures we don't leak pins for pages that were accessed but not in the mostRecent slots.
   */
  private void unpinAllPagesForTransaction(int transactionId) {
    int unpinnedInRecordCache = 0;
    int unpinnedInFragmentCache = 0;
    int unpinnedInPageCache = 0;
    int pathSummaryUnpinned = 0;
    
    // Scan RecordPageCache
    for (var entry : resourceBufferManager.getRecordPageCache().asMap().entrySet()) {
      var pageRef = entry.getKey();
      var page = entry.getValue();
      
      // Get this transaction's pin count
      var pinsByTrx = page.getPinCountByTransaction();
      Integer pinCount = pinsByTrx.get(transactionId);
      
      if (pinCount != null && pinCount > 0) {
        // Unpin all pins from this transaction
        for (int i = 0; i < pinCount; i++) {
          page.decrementPinCount(transactionId);
        }
        // Update cache to recalculate weight
        resourceBufferManager.getRecordPageCache().put(pageRef, page);
        unpinnedInRecordCache++;
        if (page.getIndexType() == io.sirix.index.IndexType.PATH_SUMMARY) {
          pathSummaryUnpinned++;
        }
      }
    }
    
    // Scan RecordPageFragmentCache  
    int pathSummaryFragmentsInCache = 0;
    int pathSummaryFragmentsPinnedByThisTrx = 0;
    for (var entry : resourceBufferManager.getRecordPageFragmentCache().asMap().entrySet()) {
      var pageRef = entry.getKey();
      var page = entry.getValue();
      
      if (page.getIndexType() == io.sirix.index.IndexType.PATH_SUMMARY) {
        pathSummaryFragmentsInCache++;
      }
      
      // Get this transaction's pin count
      var pinsByTrx = page.getPinCountByTransaction();
      Integer pinCount = pinsByTrx.get(transactionId);
      
      if (pinCount != null && pinCount > 0) {
        if (page.getIndexType() == io.sirix.index.IndexType.PATH_SUMMARY) {
          pathSummaryFragmentsPinnedByThisTrx++;
        }
        
        // Unpin all pins from this transaction
        for (int i = 0; i < pinCount; i++) {
          page.decrementPinCount(transactionId);
        }
        // Update cache to recalculate weight
        resourceBufferManager.getRecordPageFragmentCache().put(pageRef, page);
        unpinnedInFragmentCache++;
        if (page.getIndexType() == io.sirix.index.IndexType.PATH_SUMMARY) {
          pathSummaryUnpinned++;
        }
      }
    }
    
    if (transactionId >= 3 && transactionId <= 10) {
      // Show details of PATH_SUMMARY pages in cache
      var pathSummaryDetails = new StringBuilder();
      for (var entry : resourceBufferManager.getRecordPageFragmentCache().asMap().entrySet()) {
        var page = entry.getValue();
        if (page.getIndexType() == io.sirix.index.IndexType.PATH_SUMMARY) {
          var pinsByTrx = page.getPinCountByTransaction();
          Integer pinByThis = pinsByTrx.get(transactionId);
          if (pinByThis != null && pinByThis > 0) {
            pathSummaryDetails.append("Page").append(page.getPageKey())
                              .append("(rev").append(page.getRevision())
                              .append(",pin").append(pinByThis).append(") ");
          }
        }
      }
      
      System.err.println("🔍 Trx " + transactionId + " FragmentCache scan: " +
                         pathSummaryFragmentsInCache + " PATH_SUMMARY total, " +
                         pathSummaryFragmentsPinnedByThisTrx + " pinned by this trx" +
                         (pathSummaryFragmentsPinnedByThisTrx > 0 ? " [" + pathSummaryDetails + "]" : ""));
    }
    
    int totalUnpinned = unpinnedInRecordCache + unpinnedInFragmentCache;
    
    // CRITICAL: Also scan PageCache in case KeyValueLeafPages accidentally ended up there
    // (they shouldn't be in PageCache, but we check to be safe)
    for (var entry : resourceBufferManager.getPageCache().asMap().entrySet()) {
      var page = entry.getValue();
      
      if (page instanceof KeyValueLeafPage kvPage) {
        var pinsByTrx = kvPage.getPinCountByTransaction();
        Integer pinCount = pinsByTrx.get(transactionId);
        
        if (pinCount != null && pinCount > 0) {
          // Unpin all pins from this transaction
          for (int i = 0; i < pinCount; i++) {
            kvPage.decrementPinCount(transactionId);
          }
          // DON'T put back into PageCache - KeyValueLeafPages shouldn't be there!
          // Just unpin so the removal listener can close them
          unpinnedInPageCache++;
          if (kvPage.getIndexType() == io.sirix.index.IndexType.PATH_SUMMARY) {
            pathSummaryUnpinned++;
          }
        }
      }
    }
    
    totalUnpinned += unpinnedInPageCache;
    
    if (totalUnpinned > 0 && (trxId % 5 == 0)) {
      System.err.println("🔓 Trx " + trxId + " unpinned " + totalUnpinned + " pages (" + 
                         pathSummaryUnpinned + " PATH_SUMMARY): " +
                         "RecordCache=" + unpinnedInRecordCache + ", " +
                         "FragmentCache=" + unpinnedInFragmentCache + ", " +
                         "PageCache=" + unpinnedInPageCache);
    }
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

      // CRITICAL FIX: Unpin ALL pages still pinned by this transaction
      // This replaces the old mostRecent unpinning logic which only handled 3 pages
      unpinAllPagesForTransaction(trxId);
      
      // Handle PATH_SUMMARY bypassed pages for write transactions (they're not in cache)
      if (pathSummaryRecordPage != null && trxIntentLog != null) {
        if (!pathSummaryRecordPage.page.isClosed()) {
          // CRITICAL: Must unpin before closing, otherwise pin count prevents segment release
          if (pathSummaryRecordPage.page.getPinCount() > 0) {
            pathSummaryRecordPage.page.decrementPinCount(trxId);
          }
          pathSummaryRecordPage.page.close();
        }
      }

      // DIAGNOSTIC: Check for leaked pins after closing
      io.sirix.cache.PinCountDiagnostics.warnOnTransactionClose(resourceBufferManager, trxId);

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
