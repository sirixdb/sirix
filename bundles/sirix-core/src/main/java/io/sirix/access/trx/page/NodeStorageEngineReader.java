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
import io.sirix.api.StorageEngineReader;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class NodeStorageEngineReader implements StorageEngineReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeStorageEngineReader.class);

  // DEBUG FLAG: Enable with -Dsirix.debug.path.summary=true
  private static final boolean DEBUG_PATH_SUMMARY = Boolean.getBoolean("sirix.debug.path.summary");

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
   * Epoch tracker ticket for this transaction (for MVCC-aware eviction).
   * Registered when transaction opens, deregistered when it closes.
   */
  private final io.sirix.access.trx.RevisionEpochTracker.Ticket epochTicket;

  /**
   * Current page guard - protects the page where cursor is currently positioned.
   * 
   * Guard lifecycle:
   * - Acquired when cursor moves to a page
   * - Released when cursor moves to a DIFFERENT page
   * - Released on transaction close
   * 
   * This matches database cursor semantics: only the "current" page is guarded.
   * Node keys are primitives (copied from MemorySegments), so old pages can be
   * evicted after cursor moves away.
   */
  private PageGuard currentPageGuard;

  /**
   * Cached name page of this revision.
   */
  private final RevisionRootPage rootPage;

  /**
   * {@link NamePage} reference.
   */
  private final NamePage namePage;

  /**
   * Most recently read pages by type and index.
   * Using specific fields instead of generic cache for clear ownership and lifecycle.
   * Index-aware: NAME/PATH/CAS can have multiple indexes (0-3).
   */
  private RecordPage mostRecentDocumentPage;
  private RecordPage mostRecentChangedNodesPage;
  private RecordPage mostRecentRecordToRevisionsPage;
  private RecordPage pathSummaryRecordPage;
  private final RecordPage[] mostRecentPathPages = new RecordPage[4];
  private final RecordPage[] mostRecentCasPages = new RecordPage[4];
  private final RecordPage[] mostRecentNamePages = new RecordPage[4];
  private RecordPage mostRecentDeweyIdPage;

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
  public NodeStorageEngineReader(final int trxId,
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
    // No initialization needed - using specific fields for most recent pages
    // (mostRecentDocumentPage, mostRecentNamePages[], etc. initialized to null)

    revisionNumber = revision;
    rootPage = revisionRootPageReader.loadRevisionRootPage(this, revision);
    namePage = revisionRootPageReader.getNamePage(this, rootPage);
    
    // Register with epoch tracker for MVCC-aware eviction
    this.epochTicket = resourceSession.getRevisionEpochTracker().register(revision);
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
      // Page was in TIL but has been cleared - need to reload from disk
      // logKey might still be set, so don't assert it's NULL_ID_INT
    }

    //   if (trxIntentLog == null) {
    // REMOVED INCORRECT ASSERTION: logKey can be != NULL_ID_INT if page was in TIL then cleared
    // assert reference.getLogKey() == Constants.NULL_ID_INT;
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
  @SuppressWarnings("unchecked")
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
    final var offset = StorageEngineReader.recordPageOffset(nodeKey);
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

    // First: Check cached pages.
    if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY && isMostRecentlyReadPathSummaryPage(indexLogKey)) {
      var page = pathSummaryRecordPage.page();
      
      // CRITICAL: Validate page is still in cache and acquire guard atomically
      // The mostRecent field might hold a stale reference to an evicted page
      var guardedPage = resourceBufferManager.getRecordPageCache().getAndGuard(pathSummaryRecordPage.pageReference);
      
      if (guardedPage == page && !page.isClosed()) {
        // Same instance and not closed - safe to use
        // Single-guard: Replace current guard
        if (currentPageGuard == null || currentPageGuard.page() != page) {
          closeCurrentPageGuard();
          currentPageGuard = PageGuard.fromAcquired(page);
        } else {
          // Already guarding this page - release extra
          page.releaseGuard();
        }
        return new PageReferenceToPage(pathSummaryRecordPage.pageReference, page);
      } else {
        // Different instance or closed - mostRecent is stale, release guard if acquired
        if (guardedPage != null) {
          guardedPage.releaseGuard();
        }
        pathSummaryRecordPage = null;  // Clear stale reference
        // Fall through to reload
      }
    }

    // Check the most recent page for this type/index
    var cachedPage = getMostRecentPage(indexLogKey.getIndexType(),
                                       indexLogKey.getIndexNumber(),
                                       indexLogKey.getRecordPageKey(),
                                       indexLogKey.getRevisionNumber());
    if (cachedPage != null) {
      var page = cachedPage.page();
      
      // CRITICAL: Validate page is still in cache and acquire guard atomically
      // The mostRecent field might hold a stale reference to an evicted/reset page
      var guardedPage = resourceBufferManager.getRecordPageCache().getAndGuard(cachedPage.pageReference);
      
      if (guardedPage == page && !page.isClosed()) {
        // Same instance and not closed - safe to use
        // Single-guard: Replace current guard
        if (currentPageGuard == null || currentPageGuard.page() != page) {
          closeCurrentPageGuard();
          currentPageGuard = PageGuard.fromAcquired(page);
        } else {
          // Already guarding this page - release extra
          page.releaseGuard();
        }
        return new PageReferenceToPage(cachedPage.pageReference, page);
      } else {
        // Different instance, null, or closed - mostRecent is stale
        if (guardedPage != null) {
          guardedPage.releaseGuard();
        }
        setMostRecentPage(indexLogKey.getIndexType(), indexLogKey.getIndexNumber(), null);
        cachedPage = null;
        // Fall through to reload
      }
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

      if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY
          && page instanceof KeyValueLeafPage kvp) {
        System.err.println(
            "[PATH_SUMMARY-NORMAL]   -> Got page from cache: " + "pageKey=" + kvp.getPageKey() + ", revision="
                + kvp.getRevision());
      }

      // CRITICAL: Handle case where page doesn't exist (e.g., temporal queries accessing non-existent revisions)
      if (page == null) {
        // Page doesn't exist for this revision/index - this can happen with temporal queries
        // Return null to signal page not found (caller should handle gracefully)
        return null;
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

    var loadedPage =
        (KeyValueLeafPage) loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);

    if (loadedPage != null) {
      // Add to cache
      resourceBufferManager.getRecordPageCache().put(pageReferenceToRecordPage, loadedPage);
      
      // CRITICAL: Acquire guard atomically from cache
      var guardedPage = resourceBufferManager.getRecordPageCache().getAndGuard(pageReferenceToRecordPage);
      
      if (guardedPage != null && guardedPage == loadedPage) {
        // Successfully guarded
        // Single-guard: Replace current guard
        if (currentPageGuard == null || currentPageGuard.page() != loadedPage) {
          closeCurrentPageGuard();
          currentPageGuard = PageGuard.fromAcquired(loadedPage);
        } else {
          // Already guarding this page - release extra
          loadedPage.releaseGuard();
        }
      } else if (guardedPage != null && guardedPage != loadedPage) {
        // CRITICAL: Different page in cache - another thread added different instance
        // Our loadedPage is orphaned - close it to prevent leak
        if (!loadedPage.isClosed()) {
          loadedPage.close();
        }
        // Use the cached page instead
        if (currentPageGuard == null || currentPageGuard.page() != guardedPage) {
          closeCurrentPageGuard();
          currentPageGuard = PageGuard.fromAcquired(guardedPage);
        } else {
          // Already guarding cached page - release extra
          guardedPage.releaseGuard();
        }
        // Update loadedPage reference to the cached one
        loadedPage = guardedPage;
      } else {
        // guardedPage is null - cache remove happened, very rare
        LOGGER.warn("Failed to guard PATH_SUMMARY bypass page: key={}, loaded={}, cached=null",
            pageReferenceToRecordPage.getKey(), 
            System.identityHashCode(loadedPage));
        // Close orphaned page
        if (!loadedPage.isClosed()) {
          loadedPage.close();
        }
        loadedPage = null;  // Will return null below
      }

      if (DEBUG_PATH_SUMMARY && loadedPage != null) {
        System.err.println(
            "[PATH_SUMMARY-BYPASS]   -> Loaded from disk and cached: " + "pageKey=" + loadedPage.getPageKey()
                + ", revision=" + loadedPage.getRevision());
      }
    }

    setMostRecentlyReadRecordPage(indexLogKey, pageReferenceToRecordPage, loadedPage);
    return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
  }

  private boolean isMostRecentlyReadPathSummaryPage(IndexLogKey indexLogKey) {
    return pathSummaryRecordPage != null && pathSummaryRecordPage.recordPageKey == indexLogKey.getRecordPageKey()
        && pathSummaryRecordPage.index == indexLogKey.getIndexNumber()
        && pathSummaryRecordPage.revision == indexLogKey.getRevisionNumber();
  }

  /**
   * Get the most recent page for a given index type and index number.
   */
  @Nullable
  private RecordPage getMostRecentPage(IndexType indexType, int index, long recordPageKey, int revision) {
    RecordPage candidate = switch (indexType) {
      case DOCUMENT -> mostRecentDocumentPage;
      case CHANGED_NODES -> mostRecentChangedNodesPage;
      case RECORD_TO_REVISIONS -> mostRecentRecordToRevisionsPage;
      case PATH_SUMMARY -> pathSummaryRecordPage;
      case PATH -> index < mostRecentPathPages.length ? mostRecentPathPages[index] : null;
      case CAS -> index < mostRecentCasPages.length ? mostRecentCasPages[index] : null;
      case NAME -> index < mostRecentNamePages.length ? mostRecentNamePages[index] : null;
      case DEWEYID_TO_RECORDID -> mostRecentDeweyIdPage;
      default -> null;
    };
    
    // Verify it matches the requested page
    if (candidate != null && candidate.recordPageKey == recordPageKey && candidate.revision == revision) {
      return candidate;
    }
    return null;
  }
  
  /**
   * Set the most recent page for a given index type and index number.
   */
  private void setMostRecentPage(IndexType indexType, int index, RecordPage page) {
    // Close the previous page if it's been evicted from cache
    RecordPage previous = switch (indexType) {
      case DOCUMENT -> {
        RecordPage old = mostRecentDocumentPage;
        mostRecentDocumentPage = page;
        yield old;
      }
      case CHANGED_NODES -> {
        RecordPage old = mostRecentChangedNodesPage;
        mostRecentChangedNodesPage = page;
        yield old;
      }
      case RECORD_TO_REVISIONS -> {
        RecordPage old = mostRecentRecordToRevisionsPage;
        mostRecentRecordToRevisionsPage = page;
        yield old;
      }
      case PATH_SUMMARY -> {
        RecordPage old = pathSummaryRecordPage;
        pathSummaryRecordPage = page;
        yield old;
      }
      case PATH -> {
        RecordPage old = index < mostRecentPathPages.length ? mostRecentPathPages[index] : null;
        if (index < mostRecentPathPages.length) {
          mostRecentPathPages[index] = page;
        }
        yield old;
      }
      case CAS -> {
        RecordPage old = index < mostRecentCasPages.length ? mostRecentCasPages[index] : null;
        if (index < mostRecentCasPages.length) {
          mostRecentCasPages[index] = page;
        }
        yield old;
      }
      case NAME -> {
        RecordPage old = index < mostRecentNamePages.length ? mostRecentNamePages[index] : null;
        if (index < mostRecentNamePages.length) {
          mostRecentNamePages[index] = page;
        }
        yield old;
      }
      case DEWEYID_TO_RECORDID -> {
        RecordPage old = mostRecentDeweyIdPage;
        mostRecentDeweyIdPage = page;
        yield old;
      }
      default -> null;
    };
    
    // Close the replaced page if it's orphaned (not in cache)
    if (previous != null && previous != page) {
      closeMostRecentPageIfOrphaned(previous);
    }
  }

  /**
   * Release guard on a mostRecent page and close it if orphaned (evicted from cache).
   * CRITICAL: Must ALWAYS release the guard that was acquired when fetching the page.
   * If the page is still in cache, cache will close it on eviction.
   * If the page is NOT in cache (orphaned), we must close it explicitly.
   */
  private void closeMostRecentPageIfOrphaned(RecordPage recordPage) {
    if (recordPage == null) {
      return;
    }
    
    KeyValueLeafPage page = recordPage.page();
    if (page == null || page.isClosed()) {
      return;
    }
    
    // CRITICAL: Always release the guard that was acquired when fetching this page
    page.releaseGuard();
    
    // Check if page is still in cache
    KeyValueLeafPage cachedPage = resourceBufferManager.getRecordPageCache().get(recordPage.pageReference);
    
    if (cachedPage == page) {
      // Page is in cache - cache will close it on eviction
      // Guard released, no further action needed
      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && page.getPageKey() == 0) {
        LOGGER.debug("[CLEANUP] Released guard on mostRecent Page 0 ({}) - still in cache", page.getIndexType());
      }
    } else {
      // Page is NOT in cache (orphaned) - we must close it explicitly
      if (!page.isClosed()) {
        page.close();
        
        if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && page.getPageKey() == 0) {
          LOGGER.debug("[CLEANUP] Released guard and closed orphaned mostRecent Page 0 ({}) - not in cache", page.getIndexType());
        }
      }
    }
  }

  @Nullable
  private Page getFromBufferManager(@NonNull IndexLogKey indexLogKey, PageReference pageReferenceToRecordPage) {
    if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
      System.err.println(
          "[PATH_SUMMARY-CACHE-LOOKUP] Looking up in cache: " + "PageRef(key=" + pageReferenceToRecordPage.getKey()
              + ", logKey=" + pageReferenceToRecordPage.getLogKey() + ")" + ", requestedRevision="
              + indexLogKey.getRevisionNumber());
    }

    // CRITICAL: Use atomic getAndGuard() to prevent race with ClockSweeper
    // Between cache.get() and guard acquisition, ClockSweeper could evict the page
    // Note: getAndGuard() returns null for closed pages (no guard acquired)
    KeyValueLeafPage recordPageFromBuffer = resourceBufferManager.getRecordPageCache().getAndGuard(pageReferenceToRecordPage);
    
    if (recordPageFromBuffer == null) {
      // Cache miss - load outside of compute
      var kvPage = (KeyValueLeafPage) loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
      
      if (kvPage != null) {
        if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
          System.err.println("[PATH_SUMMARY-CACHE-LOOKUP]   -> Loaded from disk: " + "pageKey=" + kvPage.getPageKey()
                      + ", revision=" + kvPage.getRevision());
        }
        
        // Try to install in cache (putIfAbsent handles race)
        resourceBufferManager.getRecordPageCache().putIfAbsent(pageReferenceToRecordPage, kvPage);
        
        // ATOMIC: Get from cache with guard acquired
        recordPageFromBuffer = resourceBufferManager.getRecordPageCache().getAndGuard(pageReferenceToRecordPage);
        if (recordPageFromBuffer == null) {
          // Another thread removed it - use our loaded page and acquire guard manually
          kvPage.acquireGuard();
          recordPageFromBuffer = kvPage;
        } else if (recordPageFromBuffer != kvPage) {
          // CRITICAL: Another thread won the race and added a different page instance
          // Our kvPage is orphaned - close it to prevent leak
          if (!kvPage.isClosed()) {
            kvPage.close();
          }
        }
        
        if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && recordPageFromBuffer.getPageKey() == 0) {
          LOGGER.debug("Page 0 added to RecordPageCache: {} rev={} instance={}",
                       recordPageFromBuffer.getIndexType(),
                       recordPageFromBuffer.getRevision(),
                       " PageRef(key=" + pageReferenceToRecordPage.getKey() + ", logKey="
                           + pageReferenceToRecordPage.getLogKey() + ", db="
                           + pageReferenceToRecordPage.getDatabaseId() + ", res="
                           + pageReferenceToRecordPage.getResourceId() + ")"
                           + " (instance=" + System.identityHashCode(recordPageFromBuffer) + ")");
        }
      }
    } else {
      if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
        System.err.println("[PATH_SUMMARY-CACHE-LOOKUP]   -> Cache HIT: " + "pageKey=" + recordPageFromBuffer.getPageKey() 
                    + ", revision=" + recordPageFromBuffer.getRevision());
      }
    }

    if (recordPageFromBuffer != null) {
      pageReferenceToRecordPage.setPage(recordPageFromBuffer);
      assert !recordPageFromBuffer.isClosed();
      
      // Single-guard: Wrap the already-guarded page (guard was acquired by getAndGuard())
      if (currentPageGuard == null || currentPageGuard.page() != recordPageFromBuffer) {
        closeCurrentPageGuard();
        currentPageGuard = PageGuard.fromAcquired(recordPageFromBuffer);
      } else {
        // Same page as current guard - release the extra guard we just acquired
        recordPageFromBuffer.releaseGuard();
      }
      
      return recordPageFromBuffer;
    }

    return null;
  }

  private void setMostRecentlyReadRecordPage(@NonNull IndexLogKey indexLogKey, @NonNull PageReference pageReference,
      KeyValueLeafPage recordPage) {
    // Single-guard: Guard is already managed by caller (getFromBufferManager/getInMemoryPageInstance)
    // No additional guard management needed here
    
    if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
      if (pathSummaryRecordPage != null) {
        if (DEBUG_PATH_SUMMARY && recordPage != null) {
          System.err.println("[PATH_SUMMARY-REPLACE] Replacing old pathSummaryRecordPage: " + "oldPageKey="
                                 + pathSummaryRecordPage.page.getPageKey() + ", newPageKey=" + recordPage.getPageKey()
                                 + ", trxIntentLog=" + (trxIntentLog != null));
        }

        if (trxIntentLog == null) {
          if (resourceBufferManager.getRecordPageCache().get(pathSummaryRecordPage.pageReference) != null) {
            assert !pathSummaryRecordPage.page.isClosed();

            if (DEBUG_PATH_SUMMARY) {
              System.err.println("[PATH_SUMMARY-REPLACE]   -> Read-only: Old page still in cache");
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
              System.err.println("[PATH_SUMMARY-REPLACE]   -> Write trx: Closing bypassed page " + "pageKey="
                                     + pathSummaryRecordPage.page.getPageKey() + ", revision="
                                     + pathSummaryRecordPage.page.getRevision());
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
      // Set as most recent page for this type/index (auto-unpins previous)
      var newRecordPage = new RecordPage(indexLogKey.getIndexNumber(),
                                         indexLogKey.getIndexType(),
                                         indexLogKey.getRecordPageKey(),
                                         indexLogKey.getRevisionNumber(),
                                         pageReference,
                                         recordPage);
      setMostRecentPage(indexLogKey.getIndexType(), indexLogKey.getIndexNumber(), newRecordPage);
    }
  }

  @Nullable
  private Page loadDataPageFromDurableStorageAndCombinePageFragments(PageReference pageReferenceToRecordPage) {
    if (pageReferenceToRecordPage.getKey() == Constants.NULL_ID_LONG) {
      // No persistent key set to load page from durable storage.
      return null;
    }

    // Load list of page "fragments" from persistent storage.
    final var result = getPageFragments(pageReferenceToRecordPage);

    if (result.pages().isEmpty()) {
      return null;
    }

    final int maxRevisionsToRestore = resourceConfig.maxNumberOfRevisionsToRestore;
    final VersioningType versioningApproach = resourceConfig.versioningType;

    // Fragments already guarded by getPageFragments() - no need to guard again
    try {
      final Page completePage = versioningApproach.combineRecordPages(result.pages(), maxRevisionsToRestore, this);
      pageReferenceToRecordPage.setPage(completePage);
      assert !completePage.isClosed();

      // DIAGNOSTIC: Track combined Page 0 creation with stack trace
      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && completePage instanceof KeyValueLeafPage kvp
          && kvp.getPageKey() == 0) {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        String caller = stack.length > 6 ? stack[6].getMethodName() : "unknown";
        LOGGER.debug("COMBINED PAGE 0 created: " + kvp.getIndexType() + " rev=" + kvp.getRevision() + " by " + caller
                         + " (instance=" + System.identityHashCode(kvp) + ")");
      }

      return completePage;
    } finally {
      // Release fragment guards (acquired in getPageFragments)
      for (var page : result.pages()) {
        ((KeyValueLeafPage) page).releaseGuard();
      }
      
      // Close orphaned fragments (not in cache after combining)
      closeOrphanedFragments(pageReferenceToRecordPage,
                            result.pages(),
                            result.originalKeys(),
                            result.storageKeyForFirstFragment());
    }
  }

  /**
   * Close fragment pages that are not in cache after page combining.
   * 
   * After combining fragments into a complete page, the fragment pages may have been
   * evicted from cache. If so, we must close them explicitly to prevent leaks.
   */
  public void closeOrphanedFragments(PageReference pageReference, List<KeyValuePage<DataRecord>> pages,
      List<PageFragmentKey> originalPageFragmentKeys, long storageKeyForFirstFragment) {
    if (pages.isEmpty()) {
      return;
    }

    // DIAGNOSTIC: Check for duplicate instances in pages list
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      var instanceCounts = new java.util.HashMap<Integer, Integer>();
      for (var page : pages) {
        if (((KeyValueLeafPage) page).getPageKey() == 0) {
          int hash = System.identityHashCode(page);
          instanceCounts.merge(hash, 1, Integer::sum);
        }
      }
      for (var entry : instanceCounts.entrySet()) {
        if (entry.getValue() > 1) {
          LOGGER.debug("WARNING: Fragment instance " + entry.getKey() + " appears " + entry.getValue()
                           + " times in pages list!");
        }
      }
    }

    LOGGER.debug("closeOrphanedFragments: " + pages.size() + " fragments, trxIntentLog=" + (trxIntentLog != null));

    // Track which unique page instances we've processed to avoid duplicates
    var processedPages = new java.util.HashSet<KeyValueLeafPage>();

    var mostRecentPageFragment = (KeyValueLeafPage) pages.getFirst();

    final var fragmentRef0 =
        new PageReference().setKey(storageKeyForFirstFragment).setDatabaseId(databaseId).setResourceId(resourceId);

    var existing = resourceBufferManager.getRecordPageFragmentCache().get(fragmentRef0);
    if (existing != null && existing == mostRecentPageFragment) {
      // Fragment still in cache - cache will close it on eviction
      // No action needed
    } else {
      // Fragment not in cache - close it explicitly to prevent leak
      // It was either: (a) evicted and closed by removalListener (close is idempotent), OR
      //                (b) never added to cache (needs closing now)
      mostRecentPageFragment.close();
    }
    
    LOGGER.debug("  Fragment 0 processed");

    // Cache older fragments using their ORIGINAL storage keys (file offsets)
    for (int i = 1; i < pages.size(); i++) {
      var pageFragment = (KeyValueLeafPage) pages.get(i);

      // DIAGNOSTIC
      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && pageFragment.getPageKey() == 0) {
        LOGGER.debug(
            "  Older fragment " + i + ": Page 0 " + pageFragment.getIndexType() + " rev=" + pageFragment.getRevision()
                + " instance=" + System.identityHashCode(pageFragment)
                + " hasOriginalKey=" + (i - 1 < originalPageFragmentKeys.size()));
      }

      // Process each unique page instance once
      if (processedPages.add(pageFragment)) {
        // Check if in cache
        if (i - 1 < originalPageFragmentKeys.size()) {
          var originalFragmentKey = originalPageFragmentKeys.get(i - 1);
          final var olderFragmentRef =
              new PageReference().setKey(originalFragmentKey.key()).setDatabaseId(databaseId).setResourceId(resourceId);

          var existingOlder = resourceBufferManager.getRecordPageFragmentCache().get(olderFragmentRef);
          if (existingOlder != null && existingOlder == pageFragment) {
            // Fragment still in cache - cache will close it on eviction
            // No action needed
          } else {
            // Fragment not in cache - close it explicitly
            // It was either: (a) evicted and closed by removalListener (close is idempotent), OR
            //                (b) never added to cache (needs closing now)
            pageFragment.close();
          }
        } else {
          // No original key - never cached, close explicitly
          pageFragment.close();
        }
        
        LOGGER.debug("  Fragment " + i + " processed (rev=" + pageFragment.getRevision() + ")");
      } else {
        if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && pageFragment.getPageKey() == 0) {
          LOGGER.debug("  Fragment " + i + " SKIPPED (already processed this instance)");
        }
      }
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
          System.err.println(
              "[PATH_SUMMARY-SWIZZLED] Found swizzled page: " + "pageKey=" + kvLeafPage.getPageKey() + ", revision="
                  + kvLeafPage.getRevision());
        }
        
        // CRITICAL: Swizzled pages might not have guards - acquire atomically via cache
        // Even though page is swizzled, it should be in cache if it's valid
        var guardedPage = resourceBufferManager.getRecordPageCache().getAndGuard(pageReferenceToRecordPage);
        
        if (guardedPage == kvLeafPage && !kvLeafPage.isClosed()) {
          // Same instance - safe to use with guard
          // Single-guard: Replace current guard
          if (currentPageGuard == null || currentPageGuard.page() != kvLeafPage) {
            closeCurrentPageGuard();
            currentPageGuard = PageGuard.fromAcquired(kvLeafPage);
          } else {
            // Already guarding this page - release extra
            kvLeafPage.releaseGuard();
          }
        } else {
          // Swizzled page not in cache or different instance - need to reload
          // Release guard if we acquired one
          if (guardedPage != null) {
            guardedPage.releaseGuard();
          }
          pageReferenceToRecordPage.setPage(null);  // Clear stale swizzled reference
          return null;  // Signal caller to reload
        }
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
   * Result of loading page fragments, including pages, original fragment keys, and storage key for pages[0].
   */
  record PageFragmentsResult(List<KeyValuePage<DataRecord>> pages, List<PageFragmentKey> originalKeys,
                             long storageKeyForFirstFragment) {
  }

  /**
   * Dereference key/value page reference and get all leaves, the {@link KeyValuePage}s from the
   * revision-trees.
   *
   * @param pageReference optional page reference pointing to the first page
   * @return dereferenced pages and their original fragment keys
   * @throws SirixIOException if an I/O-error occurs within the creation process
   */
  PageFragmentsResult getPageFragments(final PageReference pageReference) {
    assert pageReference != null;
    final ResourceConfiguration config = resourceSession.getResourceConfig();
    final int revsToRestore = config.maxNumberOfRevisionsToRestore;
    final int[] revisionsToRead = config.versioningType.getRevisionRoots(rootPage.getRevision(), revsToRestore);
    final List<KeyValuePage<DataRecord>> pages = new ArrayList<>(revisionsToRead.length);

    // CRITICAL: Save original pageFragments AND storage key before mutation
    final var originalPageFragments = new ArrayList<>(pageReference.getPageFragments());
    final long originalStorageKey = pageReference.getKey(); // Storage key for pages[0]
    final var pageReferenceWithKey =
        new PageReference().setKey(originalStorageKey).setDatabaseId(databaseId).setResourceId(resourceId);

    // CRITICAL: Use atomic getAndGuard() to prevent race with ClockSweeper
    // Note: getAndGuard() returns null for closed pages (no guard acquired)
    KeyValuePage<DataRecord> page = resourceBufferManager.getRecordPageFragmentCache().getAndGuard(pageReferenceWithKey);
    
    if (page == null) {
      // Cache miss - load from disk
      var kvPage = (KeyValueLeafPage) pageReader.read(pageReferenceWithKey, resourceSession.getResourceConfig());
      
      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && kvPage.getPageKey() == 0) {
        LOGGER.debug("FRAGMENT Page 0 loaded: " + kvPage.getIndexType() + " rev=" + kvPage.getRevision() + " (instance="
                         + System.identityHashCode(kvPage) + ", fromCache=false)");
      }
      
      // Add to cache
      resourceBufferManager.getRecordPageFragmentCache().putIfAbsent(pageReferenceWithKey, kvPage);
      
      // ATOMIC: Get from cache with guard acquired
      page = resourceBufferManager.getRecordPageFragmentCache().getAndGuard(pageReferenceWithKey);
      if (page == null) {
        // Another thread removed it - use our loaded page and acquire guard manually
        kvPage.acquireGuard();
        page = kvPage;
      } else if (page != kvPage) {
        // CRITICAL: Another thread added a different page instance
        // Our kvPage is orphaned - close it to prevent leak
        if (!kvPage.isClosed()) {
          kvPage.close();
        }
        // Use the cached page (guard already acquired by getAndGuard)
      }
    }
    
    assert !page.isClosed();
    pages.add(page);

    if (originalPageFragments.isEmpty() || page.size() == Constants.NDP_NODE_COUNT) {
      return new PageFragmentsResult(pages, originalPageFragments, originalStorageKey);
    }

    final List<PageFragmentKey> pageFragmentKeys = new ArrayList<>(originalPageFragments.size() + 1);
    pageFragmentKeys.addAll(originalPageFragments);
    pages.addAll(getPreviousPageFragments(pageFragmentKeys));

    return new PageFragmentsResult(pages, originalPageFragments, originalStorageKey);
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
    final var pageReference =
        new PageReference().setKey(pageFragmentKey.key()).setDatabaseId(databaseId).setResourceId(resourceId);

    // CRITICAL: Check cache with compute to get atomic cache hit handling
    final var pageFromCache =
        resourceBufferManager.getRecordPageFragmentCache().get(pageReference, (key, existingPage) -> {
          if (existingPage != null && !existingPage.isClosed()) {
            // Cache HIT
            assert pageFragmentKey.revision() == existingPage.getRevision() :
                "Revision mismatch: key=" + pageFragmentKey.revision() + ", page=" + existingPage.getRevision();
            return existingPage;
          }
          // Cache MISS: Return null, we'll load async below
          return null;
        });

    if (pageFromCache != null) {
      return CompletableFuture.completedFuture(pageFromCache);
    }

    // Cache miss - load async and add to cache when complete
    final var reader = resourceSession.createReader();
    return (CompletableFuture<KeyValuePage<DataRecord>>) reader.readAsync(pageReference,
                                                                          resourceSession.getResourceConfig())
                                                               .whenComplete((page, _) -> {
                                                                 reader.close();
                                                                 assert pageFragmentKey.revision()
                                                                     == ((KeyValuePage<DataRecord>) page).getRevision() :
                                                                     "Revision mismatch: key="
                                                                         + pageFragmentKey.revision() + ", page="
                                                                         + ((KeyValuePage<DataRecord>) page).getRevision();
                                                                 // Add to cache using compute to handle race conditions
                                                                 resourceBufferManager.getRecordPageFragmentCache()
                                                                                      .get(pageReference,
                                                                                           (key, existingPage) -> {
                                                                                             if (existingPage != null
                                                                                                 && !existingPage.isClosed()) {
                                                                                               // Another thread loaded it - use existing
                                                                                               return existingPage;
                                                                                             }
                                                                                             // We're first - cache our loaded page
                                                                                             return (KeyValueLeafPage) page;
                                                                                           });
                                                               });
  }

  static <T> CompletableFuture<List<T>> sequence(
      List<CompletableFuture<T>> listOfCompletableFutures) {
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
   * Close the current page guard if one is active.
   * Should be called before fetching a different page or when transaction closes.
   * 
   * Package-private to allow NodeStorageEngineWriter to release guards before TIL operations.
   */
  void closeCurrentPageGuard() {
    if (currentPageGuard != null && !currentPageGuard.isClosed()) {
      try {
        currentPageGuard.close();
      } catch (FrameReusedException e) {
        // Page was evicted and reused - this is fine, we're done with it anyway
        LOGGER.debug("Page frame was reused while closing guard (expected): {}", e.getMessage());
      }
      currentPageGuard = null;
    }
  }
  
  /**
   * Get the page that the current page guard is protecting.
   * Used by NodeStorageEngineWriter for acquiring additional guards on the current page.
   *
   * @return the current page, or null if no page is currently guarded
   */
  public KeyValueLeafPage getCurrentPage() {
    return currentPageGuard != null ? currentPageGuard.page() : null;
  }

  @Override
  public void close() {
    if (!isClosed) {
      // Close current page guard
      closeCurrentPageGuard();
      
      // Deregister from epoch tracker (allow eviction of pages from this revision)
      resourceSession.getRevisionEpochTracker().deregister(epochTicket);
      
      if (trxIntentLog == null) {
        pageReader.close();
      }

      if (resourceSession.getNodeReadTrxByTrxId(trxId).isEmpty()) {
        resourceSession.closePageReadTransaction(trxId);
      }

      // CRITICAL: Close all mostRecent pages to release their pins
      closeMostRecentPageIfOrphaned(mostRecentDocumentPage);
      closeMostRecentPageIfOrphaned(mostRecentChangedNodesPage);
      closeMostRecentPageIfOrphaned(mostRecentRecordToRevisionsPage);
      closeMostRecentPageIfOrphaned(mostRecentDeweyIdPage);
      for (RecordPage page : mostRecentPathPages) {
        closeMostRecentPageIfOrphaned(page);
      }
      for (RecordPage page : mostRecentCasPages) {
        closeMostRecentPageIfOrphaned(page);
      }
      for (RecordPage page : mostRecentNamePages) {
        closeMostRecentPageIfOrphaned(page);
      }
      // PATH_SUMMARY handled separately below (has special bypass logic)

      // Handle PATH_SUMMARY bypassed pages for write transactions (they're not in cache)
      if (pathSummaryRecordPage != null && trxIntentLog != null) {
        if (!pathSummaryRecordPage.page.isClosed()) {
          // Guard already released by closeCurrentPageGuard()
          pathSummaryRecordPage.page.close();
        }
      }

      // CRITICAL FIX: Clear all mostRecent*Page fields to drop hard references
      // This allows GC to collect the pages if they are closed and not referenced elsewhere
      mostRecentDocumentPage = null;
      mostRecentChangedNodesPage = null;
      mostRecentRecordToRevisionsPage = null;
      mostRecentDeweyIdPage = null;
      pathSummaryRecordPage = null;
      java.util.Arrays.fill(mostRecentPathPages, null);
      java.util.Arrays.fill(mostRecentCasPages, null);
      java.util.Arrays.fill(mostRecentNamePages, null);

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
