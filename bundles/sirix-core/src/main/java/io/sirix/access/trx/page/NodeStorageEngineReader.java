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

import io.sirix.utils.ToStringHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.access.trx.node.CommitCredentials;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.ResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.cache.Cache;
import io.sirix.cache.IndexLogKey;
import io.sirix.cache.PageContainer;
import io.sirix.cache.PageGuard;
import io.sirix.cache.RevisionRootPageCacheKey;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.Reader;
import io.sirix.node.DeletedNode;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.CASPage;
import io.sirix.page.DeweyIDPage;
import io.sirix.page.FlyweightNodeFactory;
import io.sirix.cache.FrameReusedException;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.IndirectPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageKind;
import io.sirix.page.PageLayout;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.PathSummaryPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.UberPage;
import io.sirix.page.VectorPage;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;
import io.sirix.settings.DiagnosticSettings;
import io.sirix.settings.Fixed;
import io.sirix.settings.VersioningType;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;


import static io.sirix.utils.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Page read-only transaction. The only thing shared amongst transactions is the resource manager.
 * Everything else is exclusive to this transaction. It is required that only a single thread has
 * access to this transaction.
 */
public final class NodeStorageEngineReader implements StorageEngineReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeStorageEngineReader.class);

  /**
   * Enable path summary cache debugging.
   * @see DiagnosticSettings#PATH_SUMMARY_DEBUG
   */
  private static final boolean DEBUG_PATH_SUMMARY = DiagnosticSettings.PATH_SUMMARY_DEBUG;

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
  private final RevisionEpochTracker.Ticket epochTicket;

  /**
   * Current page guard - protects the page where cursor is currently positioned.
   * <p>
   * Guard lifecycle:
   * - Acquired when cursor moves to a page
   * - Released when cursor moves to a DIFFERENT page
   * - Released on transaction close
   * <p>
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
   * Reusable IndexLogKey to avoid allocations on every getRecord/lookupSlot call.
   * Safe to reuse because this transaction is single-threaded (see class javadoc).
   */
  private final IndexLogKey reusableIndexLogKey = new IndexLogKey(null, 0, 0, 0);

  /**
   * Reusable MemorySegmentBytesIn to avoid allocations on every non-flyweight record
   * deserialization. Safe to reuse because this transaction is single-threaded (see class javadoc).
   */
  private final MemorySegmentBytesIn reusableBytesIn = new MemorySegmentBytesIn(MemorySegment.NULL);

  /**
   * The keyed trie reader for navigating the IndirectPage trie structure.
   */
  private final KeyedTrieReader keyedTrieReader = new KeyedTrieReader();

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
      final UberPage uberPage, final int revision, final Reader reader,
      final BufferManager resourceBufferManager, final RevisionRootPageReader revisionRootPageReader,
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
  public <V extends DataRecord> V getRecord(final long recordKey, final IndexType indexType,
      final int index) {
    requireNonNull(indexType);
    assertNotClosed();

    if (recordKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return null;
    }

    final long recordPageKey = pageKey(recordKey, indexType);

    // OPTIMIZATION: Reuse IndexLogKey instance to avoid allocation on every getRecord call
    reusableIndexLogKey.setIndexType(indexType)
        .setRecordPageKey(recordPageKey)
        .setIndexNumber(index)
        .setRevisionNumber(revisionNumber);

    // $CASES-OMITTED$
    final PageReferenceToPage pageReferenceToPage = switch (indexType) {
      case DOCUMENT, CHANGED_NODES, RECORD_TO_REVISIONS, PATH_SUMMARY, PATH, CAS, NAME, VECTOR -> getRecordPage(reusableIndexLogKey);
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
  public DataRecord getValue(final KeyValuePage<? extends DataRecord> page, final long nodeKey) {
    final var offset = StorageEngineReader.recordPageOffset(nodeKey);
    DataRecord record = page.getRecord(offset);
    if (record == null) {
      // Unified page path: check directory for flyweight vs legacy format
      if (page instanceof KeyValueLeafPage kvlPage) {
        record = getRecordFromSlottedPage(kvlPage, nodeKey, offset);
      } else {
        var data = page.getSlot(offset);
        if (data != null) {
          record = getDataRecord(nodeKey, offset, data, page);
        }
      }
      if (record != null) {
        return record;
      }
      // Overflow page fallback
      try {
        final PageReference reference = page.getPageReference(nodeKey);
        if (reference != null && reference.getKey() != Constants.NULL_ID_LONG) {
          final var data = ((OverflowPage) pageReader.read(reference, resourceSession.getResourceConfig())).getData();
          record = getDataRecord(nodeKey, offset, data, page);
        } else {
          return null;
        }
      } catch (final SirixIOException e) {
        return null;
      }
    } else if (page instanceof KeyValueLeafPage kvlPage) {
      // Record was found in records[] cache. Ensure FSST symbol table is propagated
      // (needed when a cached page from the write transaction is reused by a read session
      // — the FSST table is built at commit time but records[] may already be populated)
      propagateFsstSymbolTableToRecord(record, kvlPage);
    }
    return record;
  }

  /**
   * Get a record from a slotted page. Flyweight records (nodeKindId > 0) are
   * created via FlyweightNodeFactory and bound directly to page memory.
   * Legacy records (nodeKindId == 0) are deserialized from the heap bytes.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private DataRecord getRecordFromSlottedPage(final KeyValueLeafPage kvlPage,
      final long nodeKey, final int offset) {
    final MemorySegment slottedPage = kvlPage.getSlottedPage();
    if (!PageLayout.isSlotPopulated(slottedPage, offset)) {
      return null;
    }
    final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, offset);
    if (nodeKindId > 0) {
      // Flyweight format: create binding shell and bind to page memory (zero-copy read)
      final FlyweightNode fn = FlyweightNodeFactory.createAndBind(
          slottedPage, offset, nodeKey, resourceConfig.nodeHashFunction);
      // Propagate DeweyID from page to flyweight node (stored inline after record data)
      if (resourceConfig.areDeweyIDsStored && fn instanceof Node node) {
        final byte[] deweyIdBytes = kvlPage.getDeweyIdAsByteArray(offset);
        if (deweyIdBytes != null) {
          node.setDeweyID(new SirixDeweyID(deweyIdBytes));
        }
      }
      // Propagate FSST symbol table to flyweight string nodes for lazy decompression
      propagateFsstSymbolTableToRecord((DataRecord) fn, kvlPage);
      // Do NOT cache FlyweightNode in page's records[] — pages are shared between transactions.
      return (DataRecord) fn;
    } else {
      // Legacy format in slotted page heap: deserialize normally
      final var data = kvlPage.getSlot(offset);
      if (data != null) {
        final var record = getDataRecord(nodeKey, offset, data, kvlPage);
        return record;
      }
      return null;
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private DataRecord getDataRecord(long key, int offset, MemorySegment data, KeyValuePage<? extends DataRecord> page) {
    reusableBytesIn.reset(data, 0);
    var record = resourceConfig.recordPersister.deserialize(reusableBytesIn,
                                                            key,
                                                            page.getDeweyIdAsByteArray(offset),
                                                            resourceConfig);

    // Propagate FSST symbol table to string nodes for lazy decompression
    // Only KeyValueLeafPage has FSST symbol table support
    if (page instanceof KeyValueLeafPage kvPage) {
      propagateFsstSymbolTableToRecord(record, kvPage);
    }

    // Do NOT cache deserialized records in the page's records[] here.
    // Pages in the buffer manager are shared between transactions.
    // Caching creates shared mutable references that violate snapshot isolation:
    // a write transaction mutating the cached record would be visible to
    // read-only transactions reading from the same committed page.
    return record;
  }

  /**
   * Propagate FSST symbol table from page to string nodes.
   * This enables lazy decompression when getValue() is called.
   */
  private void propagateFsstSymbolTableToRecord(DataRecord record, KeyValueLeafPage page) {
    if (record == null || page == null) {
      return;
    }
    final byte[] fsstSymbolTable = page.getFsstSymbolTable();
    if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
      if (record instanceof StringNode stringNode) {
        stringNode.setFsstSymbolTable(fsstSymbolTable);
      } else if (record instanceof ObjectStringNode objectStringNode) {
        objectStringNode.setFsstSymbolTable(fsstSymbolTable);
      }
    }
  }

  // ==================== FLYWEIGHT CURSOR SUPPORT ====================
  
  /**
   * Record containing slot location data for zero-allocation access.
   * Holds all information needed to read node fields directly from memory.
   *
   * @param page   the KeyValueLeafPage containing the slot
   * @param offset the slot offset within the page (for DeweyID lookup)
   * @param data   the MemorySegment containing the serialized node data
   * @param guard  the PageGuard protecting the page from eviction
   */
  public record SlotLocation(KeyValueLeafPage page, int offset, MemorySegment data, PageGuard guard) {}
  
  /**
   * Result of lookupSlotOrCached - either a cached record or a slot location.
   */
  public record SlotOrCachedResult(DataRecord cachedRecord, SlotLocation slotLocation) {
    public boolean hasCachedRecord() {
      return cachedRecord != null;
    }
    public boolean hasSlotLocation() {
      return slotLocation != null;
    }
  }
  
  /**
   * Lookup a node, returning cached record if available, otherwise slot location.
   * This does ONE page lookup and checks the cache first, avoiding double lookups.
   *
   * @param recordKey the node key to look up
   * @param indexType the index type
   * @param index     the index number
   * @return SlotOrCachedResult with either cachedRecord or slotLocation, or both null if not found
   */
  public SlotOrCachedResult lookupSlotOrCached(final long recordKey, final IndexType indexType,
      final int index) {
    requireNonNull(indexType);
    assertNotClosed();

    if (recordKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return new SlotOrCachedResult(null, null);
    }

    final long recordPageKey = pageKey(recordKey, indexType);
    // OPTIMIZATION: Reuse IndexLogKey instance to avoid allocation
    reusableIndexLogKey.setIndexType(indexType)
        .setRecordPageKey(recordPageKey)
        .setIndexNumber(index)
        .setRevisionNumber(revisionNumber);

    // Get the page reference (uses cache) - ONE lookup for both paths
    final PageReferenceToPage pageReferenceToPage = switch (indexType) {
      case DOCUMENT, CHANGED_NODES, RECORD_TO_REVISIONS, PATH_SUMMARY, PATH, CAS, NAME, VECTOR -> getRecordPage(reusableIndexLogKey);
      default -> null;
    };

    if (pageReferenceToPage == null || pageReferenceToPage.page == null) {
      return new SlotOrCachedResult(null, null);
    }

    KeyValueLeafPage page = (KeyValueLeafPage) pageReferenceToPage.page;
    int offset = StorageEngineReader.recordPageOffset(recordKey);

    // OPTIMIZATION: Check if record is already cached in the page
    DataRecord cachedRecord = page.getRecord(offset);
    if (cachedRecord != null) {
      DataRecord checked = checkItemIfDeleted(cachedRecord);
      if (checked != null) {
        return new SlotOrCachedResult(checked, null);
      }
      // Record was deleted - return not found
      return new SlotOrCachedResult(null, null);
    }
    
    // Not cached - atomically acquire guard only if page is still live.
    // Split acquireGuard + isClosed races with close() under severe eviction.
    if (!page.tryAcquireGuard()) {
      return new SlotOrCachedResult(null, null);
    }

    // Get slot data
    MemorySegment data = page.getSlot(offset);
    if (data == null) {
      // Try overflow page
      try {
        final PageReference reference = page.getPageReference(recordKey);
        if (reference != null && reference.getKey() != Constants.NULL_ID_LONG) {
          data = ((OverflowPage) pageReader.read(reference, resourceSession.getResourceConfig())).getData();
        }
      } catch (final SirixIOException e) {
        page.releaseGuard();
        return new SlotOrCachedResult(null, null);
      }
    }

    if (data == null) {
      page.releaseGuard();
      return new SlotOrCachedResult(null, null);
    }

    // Create guard wrapper (guard already acquired above)
    PageGuard guard = PageGuard.wrapAlreadyGuarded(page);
    return new SlotOrCachedResult(null, new SlotLocation(page, offset, data, guard));
  }

  /**
   * Lookup a slot directly without deserializing to a node object.
   * This is the core method for zero-allocation flyweight cursor access.
   * <p>
   * IMPORTANT: The returned PageGuard MUST be closed when the slot is no longer needed.
   * Failure to close the guard will prevent page eviction and cause memory issues.
   * <p>
   * Usage:
   * <pre>{@code
   * var location = reader.lookupSlotWithGuard(nodeKey, IndexType.DOCUMENT, -1);
   * if (location != null) {
   *     try {
   *         // Read directly from location.data()
   *         long parentKey = DeltaVarIntCodec.decodeDeltaFromSegment(location.data(), offset, nodeKey);
   *     } finally {
   *         location.guard().close();
   *     }
   * }
   * }</pre>
   *
   * @param recordKey the node key to lookup
   * @param indexType the index type (typically DOCUMENT for regular nodes)
   * @param index     the index number (-1 for DOCUMENT)
   * @return SlotLocation with page guard, or null if not found
   */
  public SlotLocation lookupSlotWithGuard(long recordKey, IndexType indexType, int index) {
    requireNonNull(indexType);
    assertNotClosed();

    if (recordKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return null;
    }

    final long recordPageKey = pageKey(recordKey, indexType);
    // OPTIMIZATION: Reuse IndexLogKey instance to avoid allocation
    reusableIndexLogKey.setIndexType(indexType)
        .setRecordPageKey(recordPageKey)
        .setIndexNumber(index)
        .setRevisionNumber(revisionNumber);

    // Get the page reference
    final PageReferenceToPage pageReferenceToPage = switch (indexType) {
      case DOCUMENT, CHANGED_NODES, RECORD_TO_REVISIONS, PATH_SUMMARY, PATH, CAS, NAME, VECTOR -> getRecordPage(reusableIndexLogKey);
      default -> throw new IllegalStateException("Unsupported index type: " + indexType);
    };

    if (pageReferenceToPage == null || pageReferenceToPage.page == null) {
      return null;
    }

    KeyValueLeafPage page = (KeyValueLeafPage) pageReferenceToPage.page;
    int offset = StorageEngineReader.recordPageOffset(recordKey);

    // tryAcquireGuard is atomic (synchronized) vs close(); split acquire +
    // isClosed races with the eviction path.
    if (!page.tryAcquireGuard()) {
      return null;
    }

    // Get slot data
    MemorySegment data = page.getSlot(offset);
    if (data == null) {
      // Try overflow page
      try {
        final PageReference reference = page.getPageReference(recordKey);
        if (reference != null && reference.getKey() != Constants.NULL_ID_LONG) {
          data = ((OverflowPage) pageReader.read(reference, resourceSession.getResourceConfig())).getData();
        }
      } catch (final SirixIOException e) {
        page.releaseGuard();
        return null;
      }
    }

    if (data == null) {
      page.releaseGuard();
      return null;
    }

    // Create guard wrapper (guard already acquired above)
    PageGuard guard = PageGuard.wrapAlreadyGuarded(page);
    return new SlotLocation(page, offset, data, guard);
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
  public String getName(final int nameKey, final NodeKind nodeKind) {
    assertNotClosed();
    return namePage.getName(nameKey, nodeKind, this);
  }

  @Override
  public byte[] getRawName(final int nameKey, final NodeKind nodeKind) {
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
  public RevisionRootPage loadRevRoot(final int revisionKey) {
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
  public NamePage getNamePage(final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (NamePage) getPage(revisionRoot.getNamePageReference());
  }

  @Override
  public PathSummaryPage getPathSummaryPage(final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (PathSummaryPage) getPage(revisionRoot.getPathSummaryPageReference());
  }

  @Override
  public PathPage getPathPage(final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (PathPage) getPage(revisionRoot.getPathPageReference());
  }

  @Override
  public CASPage getCASPage(final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (CASPage) getPage(revisionRoot.getCASPageReference());
  }

  @Override
  public DeweyIDPage getDeweyIDPage(final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (DeweyIDPage) getPage(revisionRoot.getDeweyIdPageReference());
  }

  @Override
  public VectorPage getVectorPage(final RevisionRootPage revisionRoot) {
    assertNotClosed();
    return (VectorPage) getPage(revisionRoot.getVectorPageReference());
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

  /**
   * Prefetch a page into the cache without blocking on the result.
   * <p>
   * This method loads the specified page into the buffer cache so that
   * subsequent accesses to nodes on that page will be cache hits.
   * <p>
   * Called by prefetching axes (e.g., PrefetchingDescendantAxis) to
   * asynchronously load pages that will be needed soon.
   *
   * @param recordPageKey the page key to prefetch
   * @param indexType the index type (typically DOCUMENT)
   */
  @Override
  public PageReferenceToPage getRecordPage(IndexLogKey indexLogKey) {
    assertNotClosed();
    checkArgument(indexLogKey.getRecordPageKey() >= 0, "recordPageKey must not be negative!");

    // First: Check cached pages.
    if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY && isMostRecentlyReadPathSummaryPage(indexLogKey)) {
      var page = pathSummaryRecordPage.page();

      // tryAcquireGuard is synchronized — atomically checks CLOSED_BIT +
      // ORPHANED_BIT and increments guardCount. Split acquireGuard +
      // isClosed races with close() under severe-pressure eviction.
      closeCurrentPageGuard();
      if (page.tryAcquireGuard() && page.getSlottedPage() != null) {
        currentPageGuard = PageGuard.wrapAlreadyGuarded(page);
        return new PageReferenceToPage(pathSummaryRecordPage.pageReference, page);
      }
      if (!page.isClosed()) {
        // Acquired but slottedPage was null — release and fall through.
        page.releaseGuard();
      }
      pathSummaryRecordPage = null;
    }

    // Check the most recent page for this type/index
    var cachedPage = getMostRecentPage(indexLogKey.getIndexType(),
                                       indexLogKey.getIndexNumber(),
                                       indexLogKey.getRecordPageKey(),
                                       indexLogKey.getRevisionNumber());
    if (cachedPage != null) {
      var page = cachedPage.page();

      closeCurrentPageGuard();
      if (page.tryAcquireGuard() && page.getSlottedPage() != null) {
        currentPageGuard = PageGuard.wrapAlreadyGuarded(page);
        return new PageReferenceToPage(cachedPage.pageReference, page);
      }
      if (!page.isClosed()) {
        page.releaseGuard();
      }
      setMostRecentPage(indexLogKey.getIndexType(), indexLogKey.getIndexNumber(), null);
      cachedPage = null;
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
        LOGGER.debug("\n[PATH_SUMMARY-DECISION] Using normal cache (read-only trx):");
        LOGGER.debug("  - recordPageKey: {}", indexLogKey.getRecordPageKey());
        LOGGER.debug("  - revision: {}", indexLogKey.getRevisionNumber());
      }

      page = getFromBufferManager(indexLogKey, pageReferenceToRecordPage);

      if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY
          && page instanceof KeyValueLeafPage kvp) {
        LOGGER.debug("[PATH_SUMMARY-NORMAL]   -> Got page from cache: pageKey={}, revision={}",
                     kvp.getPageKey(),
                     kvp.getRevision());
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
      LOGGER.debug("\n[PATH_SUMMARY-DECISION] Using bypass (write trx):");
      LOGGER.debug("  - recordPageKey: {}", indexLogKey.getRecordPageKey());
      LOGGER.debug("  - revision: {}", indexLogKey.getRevisionNumber());
    }

    var loadedPage =
        (KeyValueLeafPage) loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);

    if (loadedPage == null) {
      return null;
    }

    // Add to cache
    if (trxIntentLog == null) {
      resourceBufferManager.getRecordPageCache().put(pageReferenceToRecordPage, loadedPage);
    }

    closeCurrentPageGuard();
    currentPageGuard = new PageGuard(loadedPage);

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
      case VECTOR -> null;
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
      case VECTOR -> null;
      default -> null;
    };

    // Close the replaced page if it's orphaned (not in cache)
    if (previous != null && previous != page) {
      closeMostRecentPageIfOrphaned(previous);
    }
  }

  /**
   * Close a mostRecent page if it has been orphaned (evicted from cache).
   * <p>
   * Pages still in cache will be closed by the cache eviction process.
   * Orphaned pages (no longer in cache) need to have any remaining guards released
   * and then be closed.
   * <p>
   * IMPORTANT: For orphaned pages (not in cache), any remaining guards must be from
   * this transaction since:
   * 1. Pages can only be evicted/replaced in cache if guardCount == 0
   * 2. If a page has guards and is not in cache, those guards are from transactions
   *    that hold mostRecent references to a page instance that was replaced in cache
   * 3. When this transaction closes, we must release our guard to allow the page to close
   *
   * @param recordPage the record page to potentially close
   */
  private void closeMostRecentPageIfOrphaned(RecordPage recordPage) {
    if (recordPage == null) {
      return;
    }

    KeyValueLeafPage page = recordPage.page();
    if (page == null || page.isClosed()) {
      return;
    }

    // Check if page is still in cache
    KeyValueLeafPage cachedPage = resourceBufferManager.getRecordPageCache().get(recordPage.pageReference);

    if (cachedPage != page) {
      // Page is orphaned (not in cache or replaced by different instance)
      // This can happen when:
      // 1. Cache evicted the page (only possible if guardCount was 0 at eviction time)
      // 2. Another transaction replaced the page in cache with a new combined instance
      //
      // In case 2, our transaction may still have a guard on the old instance that
      // needs to be released. Since this page is orphaned, no other active transaction
      // can have guards on it (they would keep it in cache via compute()).
      //
      // Release any remaining guard (should be at most 1 from this transaction)
      // and close the page.
      if (page.getGuardCount() > 0) {
        page.releaseGuard();
      }
      if (!page.isClosed() && page.getGuardCount() == 0) {
        page.close();
      }
    }
  }

  /**
   * Load a page from the buffer manager's cache, or from storage if not cached.
   * <p>
   * Uses atomic compute() to prevent race conditions between cache lookup and
   * guard acquisition. Guards are acquired inside the compute block to ensure
   * the page cannot be evicted before this transaction has protected it.
   *
   * @param indexLogKey the index log key for lookup
   * @param pageReferenceToRecordPage reference to the page
   * @return the loaded page, or null if not found
   */
  @Nullable
  private Page getFromBufferManager(IndexLogKey indexLogKey, PageReference pageReferenceToRecordPage) {
    if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY && LOGGER.isDebugEnabled()) {
      LOGGER.debug("Path summary cache lookup: key={}, revision={}",
                   pageReferenceToRecordPage.getKey(), indexLogKey.getRevisionNumber());
    }

    final ResourceConfiguration config = resourceSession.getResourceConfig();

    // Fast path 1 — FULL versioning always stores a complete page (no
    // fragments). Fast path 2 — SLIDING_SNAPSHOT / INCREMENTAL /
    // DIFFERENTIAL with no historic fragments (typical for reads on the
    // most recent revision, which is the analytical-scan common case).
    // In both cases skip the RecordPageFragmentCache + combineRecordPages
    // round-trip and load straight into RecordPageCache. Profile showed
    // combineRecordPages at ~50% inclusive CPU before this change.
    final boolean singleFragmentFastPath =
        pageReferenceToRecordPage.getKey() != Constants.NULL_ID_LONG
            && (config.versioningType == VersioningType.FULL
                || pageReferenceToRecordPage.getPageFragments().isEmpty());
    if (singleFragmentFastPath) {
      KeyValueLeafPage page = resourceBufferManager.getRecordPageCache()
          .getOrLoadAndGuard(pageReferenceToRecordPage,
              ref -> (KeyValueLeafPage) pageReader.read(ref, config));

      if (page != null) {
        pageReferenceToRecordPage.setPage(page);
        closeCurrentPageGuard();
        currentPageGuard = PageGuard.wrapAlreadyGuarded(page);
      }
      return page;
    }

    // Other versioning types with fragment history: load fragments → combine → cache
    KeyValueLeafPage page = resourceBufferManager.getRecordPageCache()
        .getOrLoadAndGuard(pageReferenceToRecordPage,
            ref -> (KeyValueLeafPage) loadDataPageFromDurableStorageAndCombinePageFragments(ref));

    if (page != null) {
      pageReferenceToRecordPage.setPage(page);
      closeCurrentPageGuard();
      currentPageGuard = PageGuard.wrapAlreadyGuarded(page);
      return page;
    }

    return null;
  }

  private void setMostRecentlyReadRecordPage(IndexLogKey indexLogKey, PageReference pageReference,
      KeyValueLeafPage recordPage) {
    // Single-guard: Guard is already managed by caller (getFromBufferManager/getInMemoryPageInstance)
    // No additional guard management needed here

    if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
      if (pathSummaryRecordPage != null) {
        if (DEBUG_PATH_SUMMARY && recordPage != null) {
          LOGGER.debug(
              "[PATH_SUMMARY-REPLACE] Replacing old pathSummaryRecordPage: oldPageKey={}, newPageKey={}, trxIntentLog={}",
              pathSummaryRecordPage.page.getPageKey(),
              recordPage.getPageKey(),
              (trxIntentLog != null));
        }

        if (trxIntentLog == null) {
          if (resourceBufferManager.getRecordPageCache().get(pathSummaryRecordPage.pageReference) != null) {
            assert !pathSummaryRecordPage.page.isClosed();

            if (DEBUG_PATH_SUMMARY) {
              LOGGER.debug("[PATH_SUMMARY-REPLACE]   -> Read-only: Old page still in cache");
            }
          }
        } else {
          // Write transaction: Bypassed PATH_SUMMARY pages should NOT be in cache
          // But if bypass is disabled for testing, they might be cached
          // Remove from cache before closing to prevent "closed page in cache" errors
          pathSummaryRecordPage.pageReference.setPage(null);
          //resourceBufferManager.getRecordPageCache().remove(pathSummaryRecordPage.pageReference);

          if (!pathSummaryRecordPage.page.isClosed()) {
            if (DEBUG_PATH_SUMMARY) {
              LOGGER.debug("[PATH_SUMMARY-REPLACE]   -> Write trx: Closing bypassed page pageKey={}, revision={}",
                           pathSummaryRecordPage.page.getPageKey(),
                           pathSummaryRecordPage.page.getRevision());
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

  /**
   * Load a page from storage and combine with historical fragments for versioning.
   * <p>
   * This method handles the versioning reconstruction by loading the current page
   * fragment and combining it with previous revisions according to the configured
   * versioning strategy (e.g., incremental, differential, full).
   *
   * @param pageReferenceToRecordPage reference to the page to load
   * @return the combined page, or null if no page exists at this reference
   */
  @Nullable
  private Page loadDataPageFromDurableStorageAndCombinePageFragments(PageReference pageReferenceToRecordPage) {
    if (pageReferenceToRecordPage.getKey() == Constants.NULL_ID_LONG) {
      return null;
    }

    final var result = getPageFragments(pageReferenceToRecordPage);
    if (result.pages().isEmpty()) {
      return null;
    }

    final int maxRevisionsToRestore = resourceConfig.maxNumberOfRevisionsToRestore;
    final VersioningType versioningApproach = resourceConfig.versioningType;

    try {
      // Close old swizzled page before replacing with combined page
      Page oldSwizzledPage = pageReferenceToRecordPage.getPage();
      if (oldSwizzledPage instanceof KeyValueLeafPage oldKvp && !oldKvp.isClosed()) {
        oldKvp.close();
      }

      final Page completePage = versioningApproach.combineRecordPages(result.pages(), maxRevisionsToRestore, this);
      pageReferenceToRecordPage.setPage(completePage);
      assert !completePage.isClosed();

      return completePage;
    } finally {
      // Release guards on all fragments after combining
      for (KeyValuePage<DataRecord> fragment : result.pages()) {
        KeyValueLeafPage kvPage = (KeyValueLeafPage) fragment;
        kvPage.releaseGuard();
        assert kvPage.getGuardCount() >= 0 : "Guard count should never be negative";
      }
      // Fragments remain in cache for reuse by other transactions.
      // ClockSweeper handles eviction based on memory pressure and access patterns.
    }
  }

  @Nullable
  private Page getInMemoryPageInstance(IndexLogKey indexLogKey,
      PageReference pageReferenceToRecordPage) {
    Page page = pageReferenceToRecordPage.getPage();

    if (page != null) {
      // Don't cache PATH_SUMMARY pages from write transactions (bypassed pages)
      // Reason: Different revisions share same PageReference → cache collisions
      if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
        var kvLeafPage = ((KeyValueLeafPage) page);
        if (DEBUG_PATH_SUMMARY && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
          LOGGER.debug("[PATH_SUMMARY-SWIZZLED] Found swizzled page: pageKey={}, revision={}",
                       kvLeafPage.getPageKey(),
                       kvLeafPage.getRevision());
        }

        // tryAcquireGuard is atomic with close(): split acquire + isClosed
        // races under severe-pressure eviction and produces FrameReusedException.
        closeCurrentPageGuard();
        if (kvLeafPage.tryAcquireGuard() && kvLeafPage.getSlottedPage() != null) {
          currentPageGuard = PageGuard.wrapAlreadyGuarded(kvLeafPage);
        } else {
          if (!kvLeafPage.isClosed()) {
            kvLeafPage.releaseGuard();
          }
          pageReferenceToRecordPage.setPage(null);
          return null;
        }
      }
      return page;
    }

    return null;
  }

  @Override
  public PageReference getLeafPageReference(final long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    final PageReference pageReferenceToSubtree = getPageReference(rootPage, indexType, indexNumber);
    return getReferenceToLeafOfSubtree(pageReferenceToSubtree, recordPageKey, indexNumber, indexType, rootPage);
  }

  PageReference getLeafPageReference(final PageReference pageReferenceToSubtree, final long recordPageKey,
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
   * Dereference key/value page reference and get all page fragments from revision-trees.
   * <p>
   * For versioning systems (incremental, differential), a complete page may be composed
   * of multiple fragments from different revisions. This method loads all required
   * fragments and returns them in order for combining.
   *
   * @param pageReference reference pointing to the first (most recent) page fragment
   * @return result containing all page fragments and their original keys
   * @throws SirixIOException if an I/O error occurs during loading
   */
  PageFragmentsResult getPageFragments(final PageReference pageReference) {
    assert pageReference != null;
    final ResourceConfiguration config = resourceSession.getResourceConfig();

    // FULL versioning fast path: Page IS complete - use RecordPageCache directly
    // This bypasses the fragment cache since there are no fragments to combine
    if (config.versioningType == VersioningType.FULL) {
      final var pageReferenceWithKey = new PageReference()
          .setKey(pageReference.getKey())
          .setDatabaseId(databaseId)
          .setResourceId(resourceId);
      // Copy hash for checksum verification
      if (pageReference.getHash() != null) {
        pageReferenceWithKey.setHash(pageReference.getHash());
      }

      KeyValueLeafPage page = resourceBufferManager.getRecordPageCache()
          .getOrLoadAndGuard(pageReferenceWithKey,
              key -> (KeyValueLeafPage) pageReader.read(key, config));

      if (page != null && !page.isClosed()) {
        return new PageFragmentsResult(
            java.util.Collections.singletonList(page),
            java.util.Collections.emptyList(),
            pageReference.getKey()
        );
      }
      return new PageFragmentsResult(
          java.util.Collections.emptyList(),
          java.util.Collections.emptyList(),
          pageReference.getKey()
      );
    }

    // Other versioning types: load fragments from RecordPageFragmentCache
    final int revsToRestore = config.maxNumberOfRevisionsToRestore;
    final int[] revisionsToRead = config.versioningType.getRevisionRoots(rootPage.getRevision(), revsToRestore);
    final List<KeyValuePage<DataRecord>> pages = new ArrayList<>(revisionsToRead.length);

    // Save original fragment keys before any mutations
    final var originalPageFragments = new ArrayList<>(pageReference.getPageFragments());
    final long originalStorageKey = pageReference.getKey();
    final var pageReferenceWithKey =
        new PageReference().setKey(originalStorageKey).setDatabaseId(databaseId).setResourceId(resourceId);
    // Copy hash for checksum verification of the first fragment
    if (pageReference.getHash() != null) {
      pageReferenceWithKey.setHash(pageReference.getHash());
    }

    // Load first fragment atomically with guard
    KeyValueLeafPage page = resourceBufferManager.getRecordPageFragmentCache()
        .getOrLoadAndGuard(pageReferenceWithKey,
            key -> (KeyValueLeafPage) pageReader.read(key, resourceSession.getResourceConfig()));

    assert page != null && !page.isClosed();
    pages.add(page);

    if (originalPageFragments.isEmpty() || page.size() == Constants.NDP_NODE_COUNT) {
      return new PageFragmentsResult(pages, originalPageFragments, originalStorageKey);
    }

    // Load additional fragments for versioning reconstruction
    final List<PageFragmentKey> pageFragmentKeys = new ArrayList<>(originalPageFragments.size() + 1);
    pageFragmentKeys.addAll(originalPageFragments);
    pages.addAll(getPreviousPageFragments(pageFragmentKeys));

    return new PageFragmentsResult(pages, originalPageFragments, originalStorageKey);
  }

  private List<KeyValuePage<DataRecord>> getPreviousPageFragments(final List<PageFragmentKey> pageFragments) {
    final var pages = new ArrayList<CompletableFuture<KeyValuePage<DataRecord>>>(pageFragments.size());
    for (final var fragment : pageFragments) {
      pages.add(readPage(fragment));
    }
    final var result = sequence(pages).join();
    result.sort(Comparator.<KeyValuePage<DataRecord>, Integer>comparing(KeyValuePage::getRevision).reversed());
    return result;
  }

  /**
   * Thread-local scratch PageReference used as a cache-lookup key on the fast
   * path. ConcurrentHashMap.get only needs an object with matching hashCode/
   * equals — it never stores the passed-in key. Allocating a fresh
   * PageReference per cache hit was 21% of all allocations (async-profiler
   * alloc mode). On cache miss we still allocate a fresh PageReference for
   * insertion so each cache entry owns a stable key.
   */
  private static final ThreadLocal<PageReference> LOOKUP_REF = ThreadLocal.withInitial(PageReference::new);

  @SuppressWarnings("unchecked")
  private CompletableFuture<KeyValuePage<DataRecord>> readPage(final PageFragmentKey pageFragmentKey) {
    final long key = pageFragmentKey.key();
    final PageReference lookup = LOOKUP_REF.get();
    lookup.setKey(key).setDatabaseId(databaseId).setResourceId(resourceId);

    // Try to get from cache with guard using the thread-local lookup key.
    KeyValueLeafPage pageFromCache = resourceBufferManager.getRecordPageFragmentCache().getAndGuard(lookup);

    if (pageFromCache != null) {
      assert pageFragmentKey.revision() == pageFromCache.getRevision() :
          "Revision mismatch: key=" + pageFragmentKey.revision() + ", page=" + pageFromCache.getRevision();
      return CompletableFuture.completedFuture(pageFromCache);
    }

    // Cache miss — allocate a proper PageReference for insertion as the map key.
    final var pageReference =
        new PageReference().setKey(key).setDatabaseId(databaseId).setResourceId(resourceId);
    final var reader = resourceSession.createReader();
    return reader.readAsync(pageReference, resourceSession.getResourceConfig())
                 .thenApply(loadedPage -> {
                   reader.close();
                   assert pageFragmentKey.revision() == ((KeyValuePage<DataRecord>) loadedPage).getRevision() :
                       "Revision mismatch: key=" + pageFragmentKey.revision() + ", page=" + ((KeyValuePage<DataRecord>) loadedPage).getRevision();

                   // Atomic cache-or-store with guard (handles race with other threads).
                   KeyValueLeafPage cachedPage = resourceBufferManager.getRecordPageFragmentCache()
                       .getOrLoadAndGuard(pageReference, _ -> (KeyValueLeafPage) loadedPage);

                   return (KeyValuePage<DataRecord>) cachedPage;
                 });
  }

  static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> listOfCompletableFutures) {
    return CompletableFuture.allOf(listOfCompletableFutures.toArray(new CompletableFuture[0]))
                            .thenApply(_ -> {
                              final var result = new ArrayList<T>(listOfCompletableFutures.size());
                              for (final var future : listOfCompletableFutures) {
                                result.add(future.join());
                              }
                              return result;
                            });
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
      case VECTOR -> getVectorPage(revisionRoot).getIndirectPageReference(index);
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
  public PageReference getReferenceToLeafOfSubtree(final PageReference startReference, final long pageKey,
      final int indexNumber, final IndexType indexType, final RevisionRootPage revisionRootPage) {
    assertNotClosed();
    return keyedTrieReader.getReferenceToLeafOfSubtree(this, uberPage, startReference, pageKey, indexNumber,
        indexType, revisionRootPage);
  }

  @Override
  public long pageKey(final long recordKey, final IndexType indexType) {
    assertNotClosed();

    return switch (indexType) {
      case PATH_SUMMARY -> recordKey >> Constants.PATHINP_REFERENCE_COUNT_EXPONENT;
      case REVISIONS -> recordKey >> Constants.UBPINP_REFERENCE_COUNT_EXPONENT;
      case PATH, DOCUMENT, CAS, NAME, VECTOR -> recordKey >> Constants.INP_REFERENCE_COUNT_EXPONENT;
      default -> recordKey >> Constants.NDP_NODE_COUNT_EXPONENT;
    };
  }

  /**
   * Fast-path page key computation for DOCUMENT index type.
   * Skips assertNotClosed() and the IndexType switch — inlines the DOCUMENT case directly.
   *
   * @param recordKey the record key
   * @return the page key
   */
  public long pageKeyDocument(final long recordKey) {
    return recordKey >> Constants.INP_REFERENCE_COUNT_EXPONENT;
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
      case VECTOR -> getVectorPage(currentRevisionRootPage).getCurrentMaxLevelOfIndirectPages(index);
      case PROJECTION -> throw new UnsupportedOperationException(
          "PROJECTION index max-level lookup not yet implemented (task #23)");
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
    return ToStringHelper.of(this)
                      .add("Session", resourceSession)
                      .add("PageReader", pageReader)
                      .add("UberPage", uberPage)
                      .add("RevRootPage", rootPage)
                      .toString();
  }

  /**
   * Close the current page guard if one is active.
   * Should be called before fetching a different page or when transaction closes.
   * <p>
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
  public int getNameCount(final int key, final NodeKind kind) {
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
  public @Nullable HOTLeafPage getHOTLeafPage(IndexType indexType, int indexNumber) {
    assertNotClosed();
    
    // CRITICAL: Use getActualRevisionRootPage() to get the current revision root,
    // which for write transactions is the NEW revision root page where HOT pages are stored.
    // Using the old 'rootPage' field would fail for write transactions because the HOT
    // pages are stored against the new revision's PathPage/CASPage/NamePage references.
    final RevisionRootPage actualRootPage = getActualRevisionRootPage();
    
    // Get the root reference for the index
    final PageReference rootRef = switch (indexType) {
      case PATH -> {
        final PathPage pathPage = getPathPage(actualRootPage);
        if (pathPage == null || indexNumber >= pathPage.getReferences().size()) {
          yield null;
        }
        yield pathPage.getOrCreateReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = getCASPage(actualRootPage);
        if (casPage == null || indexNumber >= casPage.getReferences().size()) {
          yield null;
        }
        yield casPage.getOrCreateReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = getNamePage(actualRootPage);
        if (namePage == null || indexNumber >= namePage.getReferences().size()) {
          yield null;
        }
        yield namePage.getOrCreateReference(indexNumber);
      }
      default -> null;
    };
    
    if (rootRef == null) {
      return null;
    }
    
    // FIRST: Check transaction log for uncommitted pages (write transactions)
    // This must be checked before anything else since uncommitted pages won't
    // be on the reference or in the buffer cache
    if (trxIntentLog != null) {
      final PageContainer container = trxIntentLog.get(rootRef);
      if (container != null) {
        // Try modified first (the one being written to), then complete
        Page modified = container.getModified();
        if (modified instanceof HOTLeafPage hotLeaf) {
          return hotLeaf;
        }
        Page complete = container.getComplete();
        if (complete instanceof HOTLeafPage hotLeaf) {
          return hotLeaf;
        }
      }
    }
    
    // Check if page is swizzled (directly on reference)
    if (rootRef.getPage() instanceof HOTLeafPage hotLeaf) {
      return hotLeaf;
    }
    
    // For uncommitted pages with no log key, we're done
    if (rootRef.getKey() < 0 && rootRef.getLogKey() < 0) {
      return null;
    }
    
    // Try to get from buffer cache
    Page cachedPage = resourceBufferManager.getRecordPageCache().get(rootRef);
    if (cachedPage instanceof HOTLeafPage hotLeaf) {
      return hotLeaf;
    }
    
    // Load from storage with proper versioning fragment combining
    if (rootRef.getKey() >= 0) {
      try {
        final Page loadedPage = pageReader.read(rootRef, resourceConfig);
        if (loadedPage instanceof HOTLeafPage hotLeaf) {
          return loadHOTLeafPageWithVersioning(rootRef, hotLeaf);
        }
        return null;
      } catch (SirixIOException e) {
        // Page doesn't exist or couldn't be loaded
        return null;
      }
    }

    return null;
  }
  
  /**
   * Load a HOTLeafPage from storage with proper versioning fragment combining.
   *
   * <p>For FULL versioning, the already-loaded page is complete — no additional I/O needed.
   * For INCREMENTAL/DIFFERENTIAL/SLIDING_SNAPSHOT, loads additional fragments and combines.</p>
   *
   * @param pageRef the page reference
   * @param firstPage the already-loaded first page (avoids redundant SSD read)
   * @return the combined HOTLeafPage, or null if not found
   */
  private @Nullable HOTLeafPage loadHOTLeafPageWithVersioning(PageReference pageRef, HOTLeafPage firstPage) {
    final VersioningType versioningType = resourceConfig.versioningType;
    final int revsToRestore = resourceConfig.maxNumberOfRevisionsToRestore;

    // FULL versioning fast path: the already-loaded page IS complete — zero additional I/O
    if (versioningType == VersioningType.FULL) {
      pageRef.setPage(firstPage);
      return firstPage;
    }

    // Other versioning types: load additional fragments and combine.
    // Start with the already-loaded first page to avoid re-reading it from SSD.
    final List<HOTLeafPage> fragments = loadHOTPageFragments(pageRef, firstPage);
    if (fragments.isEmpty()) {
      return null;
    }

    // Combine fragments using VersioningType
    final HOTLeafPage combinedPage = versioningType.combineHOTLeafPages(fragments, revsToRestore, this);
    pageRef.setPage(combinedPage);
    return combinedPage;
  }
  
  /**
   * Load all HOTLeafPage fragments for versioning reconstruction.
   *
   * <p>Accepts the already-loaded first page to eliminate a redundant SSD read.
   * Additional fragments are loaded sequentially from the versioning chain.</p>
   *
   * @param pageRef the page reference to the most recent fragment
   * @param firstPage the already-loaded most recent fragment
   * @return list of HOTLeafPage fragments (newest first)
   */
  private List<HOTLeafPage> loadHOTPageFragments(PageReference pageRef, HOTLeafPage firstPage) {
    final List<HOTLeafPage> fragments = new ArrayList<>();
    fragments.add(firstPage);

    // Check if there are additional fragments to load
    final List<PageFragmentKey> pageFragments = pageRef.getPageFragments();
    if (pageFragments.isEmpty()) {
      return fragments;
    }

    // Load additional fragments from the versioning chain
    // Note: Fragment keys don't include hashes - only the first fragment can be verified
    // Future improvement: Store fragment hashes in PageFragmentKey for complete verification
    for (PageFragmentKey fragmentKey : pageFragments) {
      final PageReference fragmentRef = new PageReference()
          .setKey(fragmentKey.key())
          .setDatabaseId(databaseId)
          .setResourceId(resourceId);

      final Page fragmentPage = pageReader.read(fragmentRef, resourceConfig);
      if (fragmentPage instanceof HOTLeafPage hotFragment) {
        fragments.add(hotFragment);
      }
    }

    return fragments;
  }
  
  @Override
  public @Nullable Page loadHOTPage(PageReference reference) {
    assertNotClosed();
    
    if (reference == null) {
      return null;
    }
    
    // FIRST: Check transaction log for uncommitted pages (write transactions)
    if (trxIntentLog != null) {
      final PageContainer container = trxIntentLog.get(reference);
      if (container != null) {
        // Try modified first (the one being written to), then complete
        Page modified = container.getModified();
        if (modified instanceof HOTLeafPage || modified instanceof HOTIndirectPage) {
          return modified;
        }
        Page complete = container.getComplete();
        if (complete instanceof HOTLeafPage || complete instanceof HOTIndirectPage) {
          return complete;
        }
      }
    }
    
    // Check if page is swizzled (directly on reference)
    Page swizzled = reference.getPage();
    if (swizzled instanceof HOTLeafPage || swizzled instanceof HOTIndirectPage) {
      return swizzled;
    }
    
    // For uncommitted pages with no log key, we're done
    if (reference.getKey() < 0 && reference.getLogKey() < 0) {
      return null;
    }
    
    // Try to get from buffer cache
    Page cachedPage = resourceBufferManager.getRecordPageCache().get(reference);
    if (cachedPage instanceof HOTLeafPage || cachedPage instanceof HOTIndirectPage) {
      return cachedPage;
    }
    
    // Load from storage (only if key >= 0)
    if (reference.getKey() >= 0) {
      try {
        // First load the page to determine its type
        final Page loadedPage = pageReader.read(reference, resourceConfig);

        if (loadedPage instanceof HOTIndirectPage) {
          // HOTIndirectPage doesn't need versioning combining - it's stored complete.
          // Swizzle onto reference so future traversals skip I/O.
          reference.setPage(loadedPage);
          return loadedPage;
        }

        if (loadedPage instanceof HOTLeafPage hotLeaf) {
          // HOTLeafPage may need versioning fragment combining.
          // Pass the already-loaded first page to avoid a redundant SSD read.
          final HOTLeafPage combinedPage = loadHOTLeafPageWithVersioning(reference, hotLeaf);
          return combinedPage;
        }
      } catch (SirixIOException e) {
        // Page doesn't exist or couldn't be loaded
        return null;
      }
    }

    return null;
  }
}
