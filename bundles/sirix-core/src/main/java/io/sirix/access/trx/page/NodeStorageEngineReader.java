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
import io.sirix.access.DatabaseType;
import io.sirix.node.FsstSymbolTableNode;
import io.sirix.settings.StringCompressionType;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
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
import io.sirix.cache.NamesCacheKey;
import io.sirix.cache.PageContainer;
import io.sirix.cache.PageGuard;
import io.sirix.cache.RevisionRootPageCacheKey;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.Reader;
import io.sirix.node.DeletedNode;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.index.name.Names;
import io.sirix.node.NodeKind;

import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.json.ObjectNamedStringNode;
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
import io.sirix.page.PageLayout;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.PathSummaryPage;
import io.sirix.page.RegionsOnlyPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.UberPage;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.ValidTimeIndexPage;
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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;

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
   * One-shot latch making {@link #close()} run exactly once. {@link #isClosed} is only set at
   * the END of the close body (so {@code assertNotClosed} guards stay quiet during cleanup),
   * which used to let a concurrent or reentrant second close pass the {@code !isClosed} check
   * and deregister the epoch-tracker ticket twice — poisoning the process-wide tracker
   * (issue #1102). CASed 0 → 1 on entry; losers return immediately.
   */
  @SuppressWarnings("unused")
  private volatile int closeInitiated;

  private static final VarHandle CLOSE_INITIATED_VH;

  static {
    try {
      CLOSE_INITIATED_VH =
          MethodHandles.lookup().findVarHandle(NodeStorageEngineReader.class, "closeInitiated", int.class);
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

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
   * {@link NamePage} of this revision, resolved LAZILY through {@link #namePage()}.
   *
   * <p>Loading it in the constructor made every transaction — including the many that only
   * navigate structure or read values — pay a page load plus the surrounding revision-root
   * dereference at open. Not final, and deliberately unsynchronised: a
   * {@link NodeStorageEngineReader} is confined to its transaction, and a benign double-resolve
   * would return equal pages anyway.
   */
  private NamePage namePage;

  /**
   * This revision's name dictionaries, indexed by dictionary offset, memoized as they are reached
   * through {@link #alreadyBuiltNames(NodeKind)}. Sized to cover every offset
   * {@link NamePage#dictionaryOffset(NodeKind)} can return (0–3), with headroom.
   *
   * <p>Entries are the shared, immutable {@code NamesCache} copies for this reader's revision, so
   * holding them for the reader's lifetime is a reference, not a copy.
   */
  private final Names[] namesByOffset = new Names[5];

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
   * PATH_SUMMARY pages this reader loaded through the write-transaction BYPASS, and only those.
   *
   * <p>The bypass (see {@code getRecordPage}) deliberately caches nothing, so these instances are
   * transaction-private and this reader is their sole owner — unlike everything else that passes
   * through {@code pathSummaryRecordPage}, which can be a page the transaction-intent log owns.
   * Tracking them by identity is what makes teardown at {@link #close()} safe: the slot alone cannot
   * distinguish the two, and retiring a TIL-owned page here would free it before {@code log.close()}
   * runs.</p>
   *
   * <p>Needed because the slot can be cleared without anyone closing what was in it —
   * {@code invalidateMostRecentlyReadRecordPage} nulls it so the next read re-resolves through the
   * TIL, and a bypass page dropped that way became unreachable with its frame still allocated.</p>
   */
  private @Nullable List<KeyValueLeafPage> bypassLoadedPathSummaryPages;

  /**
   * Size at which {@link #bypassLoadedPathSummaryPages} is compacted, doubling after each sweep.
   *
   * <p>The list is a teardown backstop, not an ownership record: a page displaced from the
   * {@code pathSummaryRecordPage} slot is retired right there, and its entry here is dead weight
   * afterwards. Without a sweep the list is append-only, so a long write transaction holds one
   * CLOSED {@link KeyValueLeafPage} — records array, slot offsets and all — per bypass load, even
   * though the off-heap frame behind it was freed long ago. Compacting on a doubling threshold
   * keeps the amortized cost per load O(1): a sweep that frees less than half the list raises the
   * bar before the next one.
   */
  private int bypassLoadedPathSummaryPagesSweepAt = MIN_BYPASS_PAGE_SWEEP_SIZE;

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
      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
        LOGGER.error("getRecord: no page for recordKey={} (recordPageKey={}, indexType={}, trxRev={}, refNull={})",
            recordKey, recordPageKey, indexType, revisionNumber, pageReferenceToPage == null);
      }
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
          final var data = readOverflowPage(reference).getData();
          record = getDataRecord(nodeKey, offset, data, page);
        } else {
          if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && page instanceof KeyValueLeafPage kvl) {
            LOGGER.error(
                "getValue miss: nodeKey={} offset={} on page {} (type={}, pageRev={}, trxRev={}, closed={}, orphaned={}, guards={}, slotPopulated={}, slottedPageNull={})",
                nodeKey, offset, kvl.getPageKey(), kvl.getIndexType(), kvl.getRevision(), revisionNumber,
                kvl.isClosed(), kvl.isOrphaned(), kvl.getGuardCount(),
                kvl.getSlottedPage() != null && PageLayout.isSlotPopulated(kvl.getSlottedPage(), offset),
                kvl.getSlottedPage() == null);
          }
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
      // Propagate DeweyID from page to flyweight node (stored inline after record data).
      // setDeweyIDBytes stores raw bytes lazily — no SirixDeweyID parsing until getDeweyID().
      if (resourceConfig.areDeweyIDsStored && fn instanceof Node node) {
        final byte[] deweyIdBytes = kvlPage.getDeweyIdAsByteArray(offset);
        if (deweyIdBytes != null) {
          node.setDeweyIDBytes(deweyIdBytes);
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

  /**
   * Read an {@link OverflowPage} through its reference, swizzling the deserialized page onto the
   * reference so subsequent lookups reuse it. Overflow records are re-resolved on every
   * navigation step (they have no slot), so without the swizzle each {@code moveTo} would pay a
   * full page read + decompression — quadratic cost for sibling walks over large values (#1076).
   * The page wraps an immutable byte[], so racy swizzles by concurrent readers are benign.
   */
  private OverflowPage readOverflowPage(final PageReference reference) {
    if (reference.getPage() instanceof OverflowPage overflowPage) {
      return overflowPage;
    }
    final OverflowPage overflowPage =
        (OverflowPage) pageReader.read(reference, resourceSession.getResourceConfig());
    reference.setPage(overflowPage);
    return overflowPage;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Mirrors {@link #readOverflowPage(PageReference)}: resolve by disk offset key, swizzle
   * the immutable page onto the reference for reuse.
   */
  @Override
  public @Nullable OverflowPage readSideOverflowPage(final PageReference reference) {
    if (reference.getPage() instanceof OverflowPage segmentPage) {
      return segmentPage;
    }
    if (reference.getKey() == Constants.NULL_ID_LONG) {
      return null;
    }
    // The parent side persists a bare offset key (no page-kind tag, no hash — integrity lives
    // in the owning descriptor). A dangling/corrupted offset can decode as ANY page kind; turn
    // that into an attributable error instead of a ClassCastException deep in a scan.
    final var loadedPage = pageReader.read(reference, resourceSession.getResourceConfig());
    if (!(loadedPage instanceof OverflowPage segmentPage)) {
      throw new SirixIOException("Side-map overflow reference (offset key " + reference.getKey()
          + ") resolved to " + (loadedPage == null ? "null" : loadedPage.getClass().getSimpleName())
          + " — dangling or corrupted side-map reference.");
    }
    reference.setPage(segmentPage);
    return segmentPage;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Routes through the backend reader's COALESCED batch read (two preads per run of
   * near-adjacent offsets instead of two per segment) — the projection column fetch's
   * dominant cost on warm caches was the per-segment syscall pair.
   */
  @Override
  public OverflowPage[] readSideOverflowPageBatch(final long[] offsets) {
    final PageReference[] references = new PageReference[offsets.length];
    for (int i = 0; i < offsets.length; i++) {
      if (offsets[i] >= 0 && offsets[i] != Constants.NULL_ID_LONG) {
        final PageReference reference = new PageReference();
        reference.setKey(offsets[i]);
        references[i] = reference;
      }
    }
    final var loadedPages = pageReader.read(references, resourceSession.getResourceConfig());
    final OverflowPage[] pages = new OverflowPage[offsets.length];
    for (int i = 0; i < loadedPages.length; i++) {
      final var loadedPage = loadedPages[i];
      if (loadedPage == null) {
        continue;
      }
      if (!(loadedPage instanceof OverflowPage segmentPage)) {
        throw new SirixIOException("Side-map overflow reference (offset key " + offsets[i]
            + ") resolved to " + loadedPage.getClass().getSimpleName()
            + " — dangling or corrupted side-map reference.");
      }
      pages[i] = segmentPage;
    }
    return pages;
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
    // Resolve for the PAGE first, whatever the record's type: flyweight string reads do not
    // decode through the record at all — they go through the page's own parsed-symbols cache
    // (KeyValueLeafPage#fsstSymbols), which treats a null table as "nothing is compressed" and
    // hands back the compressed payload as the value. Resolving here is what turns the page's
    // dictionary id into the bytes that cache parses.
    final byte[] fsstSymbolTable = resolveFsstSymbolTable(page);
    if (fsstSymbolTable == null || fsstSymbolTable.length == 0) {
      return;
    }
    if (record instanceof StringNode stringNode) {
      stringNode.setFsstSymbolTable(fsstSymbolTable);
    } else if (record instanceof ObjectNamedStringNode fusedNode) {
      // Fused field strings decode through the node's own table field, exactly like StringNode —
      // and since FSST reached the fused kind they are the records that actually carry
      // compressed payloads on JSON data.
      fusedNode.setFsstSymbolTable(fsstSymbolTable);
    }
  }

  /**
   * The symbol table this page's strings were encoded against, fetching it from the dictionary
   * trie the first time and caching it on the page.
   *
   * <p>A page carries the table itself only in databases written before symbol tables moved into
   * the dictionary; since then it carries an id, and this is where that id becomes bytes. It
   * cannot happen at deserialization — that has no reader to walk the trie with — and it should
   * not happen eagerly, because most pages are never asked for a string.
   *
   * <p>Resolution goes through this reader, so it lands on the revision this reader is positioned
   * at. That is the whole point of storing the tables as versioned records: a page written at
   * revision N names the table that existed at revision N, and copy-on-write keeps that record
   * reachable no matter how many times later revisions rebuild.
   *
   * @param page the page whose symbol table is wanted
   * @return the symbol table, or {@code null} if the page has none
   */
  /**
   * Resolve the symbol table of every fragment about to be combined.
   *
   * <p>The versioning combine decodes each fragment's compressed strings through
   * {@code FsstAwareSlotCopier}, built from the fragment's table <em>bytes</em>. A fragment fresh
   * off disk carries only the dictionary id — and a copier built from {@code null} is inactive,
   * which would make the combine raw-copy still-compressed payloads into a target page that, by
   * the copier's own invariant, carries no table. Nothing would fail at that point; the strings
   * would simply read back as garbage later. So the ids are turned into bytes here, at the last
   * moment before the combine, where every caller funnels through.
   *
   * <p>Costs nothing when FSST is off: no fragment carries an id, and the loop reduces to an
   * instanceof and a field check per fragment. Package-private for the writer's combine-for-
   * modification, which funnels through the same requirement.
   *
   * @param fragments the fragments about to be combined, oldest to newest
   */
  void resolveFsstSymbolTables(final List<? extends KeyValuePage<?>> fragments) {
    for (final KeyValuePage<?> fragment : fragments) {
      if (fragment instanceof KeyValueLeafPage kvl) {
        resolveFsstSymbolTable(kvl);
      }
    }
  }

  /**
   * All FSST symbol tables reachable from this reader's revision, keyed by dictionary id.
   * {@code null} until {@link #ensureFsstSymbolTablesLoaded()} runs; stays {@code null} forever
   * when the resource does not use FSST.
   */
  private Long2ObjectOpenHashMap<byte[]> fsstSymbolTablesById;

  /**
   * The database type of this reader's resource, which fixes the NamePage dictionary offsets in
   * use. The single derivation point for reader and writer alike — the two must never
   * disagree, or tables get stored under one offset and looked up under another.
   */
  DatabaseType databaseType() {
    return resourceSession instanceof JsonResourceSession ? DatabaseType.JSON : DatabaseType.XML;
  }

  /**
   * Materialise the revision's symbol tables from the dictionary trie, once per reader.
   *
   * <p>Eager and whole rather than lazy and per-id, for a structural reason: the place that needs
   * a table most urgently — the fragment combine — runs <em>inside</em> the record-page cache's
   * compute, and walking the NAME trie from there re-enters the same cache, which its map forbids
   * outright ("recursive update"). So tables are fetched out here, where the trie can be walked
   * freely, and resolution inside the compute becomes a plain map lookup. The dictionary is tiny —
   * one table per revision that rebuilt, a couple of kilobytes each — and a resource without FSST
   * pays a single field test.
   *
   * <p>This is the same shape {@code Names} uses for the name dictionary itself: load the whole
   * thing through {@code getRecord} from outside any cache compute, then answer lookups from
   * memory.
   */
  void ensureFsstSymbolTablesLoaded() {
    if (fsstSymbolTablesById != null
        || resourceConfig.stringCompressionType != StringCompressionType.FSST) {
      return;
    }
    final Long2ObjectOpenHashMap<byte[]> tables = new Long2ObjectOpenHashMap<>(4);
    // The dictionary offset differs by database type because NamePage's bookkeeping is serialized
    // positionally and the offsets in use must stay gapless — the same test
    // StorageEngineWriterFactory uses to root the name dictionaries in the first place.
    final int offset = NamePage.fsstSymbolTableOffset(databaseType());
    final NamePage namePage = getNamePage(getActualRevisionRootPage());
    final long maxNodeKey = namePage.getMaxNodeKey(offset);
    // Publish the (possibly still empty) map BEFORE walking the trie: the walk itself re-enters
    // getRecordPage, and re-running this guard there must be a no-op rather than a second walk.
    fsstSymbolTablesById = tables;
    // The walk gets its own key object. It MUST NOT go through getRecord: this method runs from
    // inside getRecordPage while the caller's reusableIndexLogKey is live, and getRecord mutates
    // that shared instance — the nested NAME lookups would rewrite it under the outer DOCUMENT
    // lookup, which would then silently read the NAME page and hand back the wrong record.
    final IndexLogKey walkKey = new IndexLogKey(IndexType.NAME, 0, offset, revisionNumber);
    // Ids are opaque — the only contract is positive, increasing, never reused — so every key up
    // to the high-water mark is probed and anything that is not a symbol table is skipped.
    for (long id = 1; id <= maxNodeKey; id++) {
      walkKey.setRecordPageKey(pageKey(id, IndexType.NAME));
      final PageReferenceToPage referenceToPage = getRecordPage(walkKey);
      if (referenceToPage == null || referenceToPage.page == null) {
        continue;
      }
      if (getValue((KeyValueLeafPage) referenceToPage.page, id)
          instanceof FsstSymbolTableNode symbolTable) {
        tables.put(id, symbolTable.getTable());
      }
    }
  }

  /**
   * The symbol table this page's strings were encoded against, from the pre-loaded dictionary.
   *
   * <p>A page carries the table itself only when this commit's writer handed it over before
   * serialization; a page fresh off disk carries an id, and this is where the id becomes bytes.
   * It cannot happen at deserialization — no reader in scope — and the trie cannot be walked from
   * here, because this may run inside the record-page cache's compute (see
   * {@link #ensureFsstSymbolTablesLoaded()}).
   *
   * <p>Resolution lands on the tables reachable from this reader's revision. That is the point of
   * storing them as versioned records: a page written at revision N names the table that existed
   * at revision N, appends never displace it, and copy-on-write keeps it reachable from every
   * later root.
   *
   * @param page the page whose symbol table is wanted
   * @return the symbol table, or {@code null} if the page has none
   */
  @Override
  public void ensureFsstSymbolTable(final KeyValueLeafPage page) {
    if (page != null) {
      ensureFsstSymbolTablesLoaded();
      resolveFsstSymbolTable(page);
    }
  }

  private byte[] resolveFsstSymbolTable(final KeyValueLeafPage page) {
    final byte[] cached = page.getFsstSymbolTable();
    if (cached != null) {
      return cached;
    }
    final long id = page.getFsstSymbolTableId();
    if (id == KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID) {
      return null;
    }
    if (fsstSymbolTablesById == null) {
      // A page carrying an id can only come from an FSST-enabled writer, and every route to one
      // passes the DOCUMENT guard in getRecordPage that loads the dictionary. If this fires, a
      // new call path skipped that guard — fail here, at the cause, rather than hand back
      // plausible garbage from a missing table.
      throw new IllegalStateException("page " + page.getPageKey() + " references FSST symbol table "
          + id + " but the symbol-table dictionary was never loaded for this reader");
    }
    final byte[] table = fsstSymbolTablesById.get(id);
    if (table == null) {
      throw new IllegalStateException("page " + page.getPageKey() + " references FSST symbol table "
          + id + ", which does not exist at revision " + getRevisionNumber()
          + " — its compressed strings cannot be decoded");
    }
    page.setFsstSymbolTable(table);
    return table;
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
      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
        LOGGER.error(
            "lookupSlot(cached) guard-fail: recordKey={} on page {} (type={}, pageRev={}, trxRev={}, closed={}, orphaned={}, guards={})",
            recordKey, page.getPageKey(), page.getIndexType(), page.getRevision(), revisionNumber, page.isClosed(),
            page.isOrphaned(), page.getGuardCount());
      }
      return new SlotOrCachedResult(null, null);
    }

    // Get slot data
    MemorySegment data = page.getSlot(offset);
    if (data == null) {
      // Try overflow page
      try {
        final PageReference reference = page.getPageReference(recordKey);
        if (reference != null && reference.getKey() != Constants.NULL_ID_LONG) {
          data = readOverflowPage(reference).getData();
        }
      } catch (final SirixIOException e) {
        page.releaseGuard();
        return new SlotOrCachedResult(null, null);
      }
    }

    if (data == null) {
      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
        LOGGER.error(
            "lookupSlot(cached) slot-miss: recordKey={} offset={} on page {} (type={}, pageRev={}, trxRev={}, closed={}, orphaned={}, guards={}, slottedPageNull={})",
            recordKey, offset, page.getPageKey(), page.getIndexType(), page.getRevision(), revisionNumber,
            page.isClosed(), page.isOrphaned(), page.getGuardCount(), page.getSlottedPage() == null);
      }
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
      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
        LOGGER.error(
            "lookupSlot guard-fail: recordKey={} on page {} (type={}, pageRev={}, trxRev={}, closed={}, orphaned={}, guards={}, identity={})",
            recordKey, page.getPageKey(), page.getIndexType(), page.getRevision(), revisionNumber, page.isClosed(),
            page.isOrphaned(), page.getGuardCount(), System.identityHashCode(page), page.getCloseSite());
      }
      return null;
    }

    // Get slot data
    MemorySegment data = page.getSlot(offset);
    if (data == null) {
      // Try overflow page
      try {
        final PageReference reference = page.getPageReference(recordKey);
        if (reference != null && reference.getKey() != Constants.NULL_ID_LONG) {
          data = readOverflowPage(reference).getData();
        }
      } catch (final SirixIOException e) {
        page.releaseGuard();
        return null;
      }
    }

    if (data == null) {
      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
        LOGGER.error(
            "lookupSlot slot-miss: recordKey={} offset={} on page {} (type={}, pageRev={}, trxRev={}, closed={}, orphaned={}, guards={}, slottedPageNull={})",
            recordKey, offset, page.getPageKey(), page.getIndexType(), page.getRevision(), revisionNumber,
            page.isClosed(), page.isOrphaned(), page.getGuardCount(), page.getSlottedPage() == null);
      }
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
    final Names names = alreadyBuiltNames(nodeKind);
    if (names != null) {
      return names.getName(nameKey);
    }
    return namePage().getName(nameKey, nodeKind, this);
  }

  @Override
  public byte[] getRawName(final int nameKey, final NodeKind nodeKind) {
    assertNotClosed();
    final Names names = alreadyBuiltNames(nodeKind);
    if (names != null) {
      return names.getRawName(nameKey);
    }
    return namePage().getRawName(nameKey, nodeKind, this);
  }

  /**
   * This revision's name dictionary for {@code nodeKind}, but ONLY if reaching it costs nothing —
   * otherwise {@code null}, and the caller resolves it the long way through {@link #namePage()}.
   *
   * <p>Worth the detour because the long way is not cheap once per transaction: {@link NamePage} is
   * on {@code PageCache}'s index-root exclusion list, so {@link #namePage()} READS AND DESERIALIZES
   * it every time. That exclusion is correct and must stay — sharing one index-root instance would
   * let a time-travel read of revision N follow revision N+1's root — but its consequence is that
   * {@code NamePage.jsonObjectKeys} starts null in every transaction, and the first name a
   * serializer emits pays for the page underneath it.
   *
   * <p>The dictionary itself has no such problem: {@code NamesCache} is keyed by
   * {@code (database, resource, REVISION, offset)} and holds an immutable copy, and the revision in
   * that key is exactly what the reference-keyed page cache lacks. So the dictionary is safe to
   * reach directly, and the page is only needed to build it — on a miss, or for a writer.
   *
   * <p>Write transactions always take the long way. A writer's uncommitted names live in its
   * transaction-intent log, not in a committed revision's dictionary, and {@code NamePage.getNames}
   * already refuses the shared cache for exactly that reason.
   *
   * <p>The per-reader memo makes this one cache probe per dictionary per transaction rather than
   * one per name: it also keeps a {@code NamesCacheKey} from being allocated on a path the
   * serializer walks once per named node.
   *
   * @param nodeKind the kind whose names are wanted
   * @return the dictionary, or {@code null} if it must be built through the name page
   */
  private @Nullable Names alreadyBuiltNames(final NodeKind nodeKind) {
    if (trxIntentLog != null) {
      return null;
    }
    final int offset = NamePage.dictionaryOffset(nodeKind);
    // NO_DICTIONARY, or an offset this array was not sized for: the name page stays the authority.
    // getName in particular answers ARRAY and OBJECT with synthetic literals and no dictionary at
    // all, and the path summary asks it for exactly those.
    if (offset < 0 || offset >= namesByOffset.length) {
      return null;
    }
    final Names memo = namesByOffset[offset];
    if (memo != null) {
      return memo;
    }
    final Names cached = resourceBufferManager.getNamesCache()
                                              .get(new NamesCacheKey(databaseId, resourceId, revisionNumber, offset));
    if (cached != null) {
      namesByOffset[offset] = cached;
    }
    return cached;
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
    // Pipelined async commit: revision N's root exists canonically in memory after phase 1 but
    // is neither published (lastCommittedUberPage) nor recorded in the revisions file until the
    // background hardening completes. Serve it from the session's pending slot.
    final RevisionRootPage pendingRevisionRoot = resourceSession.getPendingRevisionRoot(revisionKey);
    if (pendingRevisionRoot != null) {
      return pendingRevisionRoot;
    }
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

  /**
   * This revision's {@link NamePage}, loaded on first use. {@code RevisionRootPageReader.getNamePage}
   * delegates straight back to {@link #getNamePage(RevisionRootPage)}, so resolving it here is
   * exactly what the constructor used to do eagerly — just deferred to the first caller that
   * actually needs a name.
   *
   * @return the name page of this reader's revision
   */
  private NamePage namePage() {
    NamePage page = namePage;
    if (page == null) {
      page = getNamePage(rootPage);
      namePage = page;
    }
    return page;
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
  public ProjectionIndexPage getProjectionIndexPage(final RevisionRootPage revisionRoot) {
    assertNotClosed();
    final PageReference ref = revisionRoot.getProjectionIndexPageReference();
    // Backwards compat: revisions written before PROJECTION_REFERENCE_OFFSET existed
    // have a bare PageReference with no page attached. Seed an empty container page
    // on first access so callers never get null — matches the legacy semantics for
    // CASPage/PathPage/NamePage which are always present on fresh revisions.
    if (ref.getPage() == null && ref.getKey() == Constants.NULL_ID_LONG
        && ref.getLogKey() == Constants.NULL_ID_INT) {
      final ProjectionIndexPage fresh = new ProjectionIndexPage();
      ref.setPage(fresh);
      return fresh;
    }
    return (ProjectionIndexPage) getPage(ref);
  }

  @Override
  public ValidTimeIndexPage getValidTimeIndexPage(final RevisionRootPage revisionRoot) {
    assertNotClosed();
    final PageReference ref = revisionRoot.getValidTimeIndexPageReference();
    // Backwards compat: revisions written before VALIDTIME_REFERENCE_OFFSET existed have a bare
    // PageReference with no page attached. Seed an empty container page on first access so callers
    // never get null — matches the legacy semantics for CASPage/PathPage/NamePage which are always
    // present on fresh revisions.
    if (ref.getPage() == null && ref.getKey() == Constants.NULL_ID_LONG
        && ref.getLogKey() == Constants.NULL_ID_INT) {
      final ValidTimeIndexPage fresh = new ValidTimeIndexPage();
      ref.setPage(fresh);
      return fresh;
    }
    return (ValidTimeIndexPage) getPage(ref);
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

    // Symbol tables must be in hand BEFORE the page-cache compute below runs: a document page's
    // fragments are combined inside that compute, combining decodes FSST strings, and fetching a
    // table walks the NAME trie through the same cache — a compute inside a compute, which the
    // underlying map forbids. Only document pages carry string slots, and the NAME-trie reads
    // this triggers re-enter here with indexType NAME, so the guard cannot recurse.
    if (indexLogKey.getIndexType() == IndexType.DOCUMENT) {
      ensureFsstSymbolTablesLoaded();
    }

    // First: Check cached pages.
    if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY && isMostRecentlyReadPathSummaryPage(indexLogKey)) {
      var page = pathSummaryRecordPage.page();

      // tryAcquireGuard is synchronized — atomically checks CLOSED_BIT +
      // ORPHANED_BIT and increments guardCount. Split acquireGuard +
      // isClosed races with close() under severe-pressure eviction.
      closeCurrentPageGuard();
      final boolean acquired = page.tryAcquireGuard();
      if (acquired && page.getSlottedPage() != null) {
        currentPageGuard = PageGuard.wrapAlreadyGuarded(page);
        return new PageReferenceToPage(pathSummaryRecordPage.pageReference, page);
      }
      if (acquired) {
        // Acquired but slottedPage was null — release and fall through. NEVER release on a
        // failed acquire: the count belongs to OTHER holders (a failed acquire never
        // increments), and a bogus decrement lets a deferred orphan-close free the page
        // out from under them.
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

      // Same page as the guard we already hold: the overwhelmingly common case in a sequential
      // scan, where consecutive records live on the page just read. Releasing the guard only to
      // immediately re-acquire it costs two atomics and a PageGuard allocation PER RECORD, and
      // leaves a window in which the sweeper could take the page away between the two. Reuse the
      // guard we are already holding instead. Allocation profiling of a 290k-record filter scan
      // attributed 13.3% of all allocations to PageGuard, split between this site and
      // lookupSlotWithGuard.
      final PageGuard held = currentPageGuard;
      if (held != null && !held.isClosed() && held.page() == page && page.getSlottedPage() != null) {
        return new PageReferenceToPage(cachedPage.pageReference, page);
      }

      closeCurrentPageGuard();
      final boolean acquired = page.tryAcquireGuard();
      if (acquired && page.getSlottedPage() != null) {
        currentPageGuard = PageGuard.wrapAlreadyGuarded(page);
        return new PageReferenceToPage(cachedPage.pageReference, page);
      }
      if (acquired) {
        // Release only what we acquired — a failed tryAcquireGuard never increments,
        // so releasing there would steal another holder's guard.
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

    if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY && trxIntentLog != null) {
      if (bypassLoadedPathSummaryPages == null) {
        bypassLoadedPathSummaryPages = new ArrayList<>(MIN_BYPASS_PAGE_SWEEP_SIZE);
      } else if (bypassLoadedPathSummaryPages.size() >= bypassLoadedPathSummaryPagesSweepAt) {
        dropClosedBypassLoadedPathSummaryPages();
      }
      bypassLoadedPathSummaryPages.add(loadedPage);
    }

    setMostRecentlyReadRecordPage(indexLogKey, pageReferenceToRecordPage, loadedPage);

    return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
  }

  @Override
  public @Nullable RegionsOnlyPage getRecordPageRegionsOnly(final IndexLogKey indexLogKey,
      final int regionKindMask, final int regionDeferMask) {
    assertNotClosed();
    // A write transaction reads through its intent log, where pages are uncommitted and live only
    // in memory — there is no on-disk image to read columns from.
    if (trxIntentLog != null) {
      return null;
    }
    if (indexLogKey.getIndexType() != IndexType.DOCUMENT || indexLogKey.getRecordPageKey() < 0) {
      return null;
    }

    final PageReference reference = getLeafPageReference(indexLogKey.getRecordPageKey(),
                                                        indexLogKey.getIndexNumber(),
                                                        indexLogKey.getIndexType());
    if (reference == null || reference.getKey() == Constants.NULL_ID_LONG) {
      return null;
    }

    // Already materialized? Then its regions are in hand and re-reading the image would be
    // strictly more work. Region payloads are heap byte[] arrays that outlive the page's off-heap
    // memory, so this needs no guard: even if the sweeper closes the page an instant later, the
    // columns we took stay valid.
    //
    // A cached page WITHOUT a usable region table (one the writer built in memory and never
    // deserialized, or one whose regions a mutation invalidated) falls through to the disk read
    // rather than giving up: the committed image on disk carries the columns either way, and
    // silently declining here would switch the whole fast path off exactly when a resource's
    // pages happen to be resident.
    final KeyValueLeafPage cached = resourceBufferManager.getRecordPageCache().get(reference);
    if (cached != null) {
      final RegionTable regions = cached.getRegionTable();
      // Serve from the cache only when the resident table actually holds every kind that was
      // asked for. A writer-built page typically carries the number and field-name columns and
      // nothing else, so answering a string query from it returned a table with neither sketch nor
      // dictionary; the caller then re-read, landed here again, got the same table again, and gave
      // up to the record path — two lookups spent to reach the slow answer, and the sketch never
      // usable on a resident page. Falling through instead reads the committed image, which has
      // the columns.
      if (regions != null && !regions.isEmpty() && satisfies(regions, regionKindMask)) {
        // The slot bitmap travels too: without it the page reports hasSlotBitmap() false and every
        // fragment of a multi-fragment page is refused by the column merge. Copied because the
        // cached page's array is live and this object outlives the page.
        final long[] slotBitmap = cached.getSlotBitmap();
        return new RegionsOnlyPage(indexLogKey.getRecordPageKey(), cached.getRevision(),
                                   cached.getCachedPopulatedCount(),
                                   cached.getFsstSymbolTableId(), regions,
                                   slotBitmap == null ? null : slotBitmap.clone());
      }
    }

    if (resourceConfig.versioningType != VersioningType.FULL
        && !reference.getPageFragments().isEmpty()) {
      // Multi-fragment page. Its columns still exist — one set per fragment — so hand them all back
      // and let the caller merge, instead of sending it to a reconstruction that materializes every
      // fragment into a row heap first. Only the caller knows whether it can merge the column it
      // wants; a null here just means "use the records".
      return null;
    }
    return pageReader.readRegionsOnly(reference, resourceConfig, regionKindMask, regionDeferMask);
  }

  /**
   * Whether {@code regions} holds every kind named in {@code kindMask}.
   *
   * <p>A mask bit for a kind the page genuinely does not have (no strings on the page, say) makes
   * this false and costs one image read that finds the same absence. That is the right trade: the
   * alternative is handing back a table that silently lacks what the caller asked for, which reads
   * as "this page has no such column" and is indistinguishable from the truth.
   */
  private static boolean satisfies(final RegionTable regions, final int kindMask) {
    for (int kind = 0; kind < RegionTable.KIND_COUNT; kind++) {
      if ((kindMask & (1 << kind)) != 0 && regions.payload((byte) kind) == null) {
        return false;
      }
    }
    return true;
  }

  @Override
  public RegionsOnlyPage @Nullable [] getRecordPageFragmentRegions(
      final IndexLogKey indexLogKey, final int regionKindMask) {
    assertNotClosed();
    if (trxIntentLog != null || indexLogKey.getIndexType() != IndexType.DOCUMENT
        || indexLogKey.getRecordPageKey() < 0) {
      return null;
    }
    final PageReference reference = getLeafPageReference(indexLogKey.getRecordPageKey(),
                                                        indexLogKey.getIndexNumber(),
                                                        indexLogKey.getIndexType());
    if (reference == null || reference.getKey() == Constants.NULL_ID_LONG) {
      return null;
    }
    final var fragmentKeys = reference.getPageFragments();
    if (fragmentKeys.isEmpty() || resourceConfig.versioningType == VersioningType.FULL) {
      return null;  // not a multi-fragment page — the single-page entry point serves it
    }
    // Newest first, and the chain read WHOLE — exactly what getPageFragments hands to
    // combineRecordPages. Capping it here would be a silent undercount: a slot living only in a
    // fragment past the cap would vanish from the merge, where the record path would still find
    // it. The bound below is a corruption guard, not a policy.
    if (1 + fragmentKeys.size() > MAX_FRAGMENT_CHAIN) {
      return null;
    }
    final int count = 1 + fragmentKeys.size();
    final RegionsOnlyPage[] fragments = new RegionsOnlyPage[count];
    fragments[0] = pageReader.readRegionsOnly(reference, resourceConfig, regionKindMask, 0);
    if (fragments[0] == null || !fragments[0].hasSlotBitmap()) {
      return null;
    }
    for (int i = 1; i < count; i++) {
      final PageReference fragmentRef = new PageReference().setKey(fragmentKeys.get(i - 1).key())
                                                           .setDatabaseId(databaseId)
                                                           .setResourceId(resourceId);
      final RegionsOnlyPage fragment =
          pageReader.readRegionsOnly(fragmentRef, resourceConfig, regionKindMask, 0);
      if (fragment == null || !fragment.hasSlotBitmap()) {
        return null;
      }
      fragments[i] = fragment;
    }
    return fragments;
  }

  @Override
  public byte @Nullable [] fsstSymbolTable(final long id) {
    if (id <= 0 || resourceConfig.stringCompressionType != StringCompressionType.FSST) {
      return null;
    }
    ensureFsstSymbolTablesLoaded();
    final var tables = fsstSymbolTablesById;
    return tables == null ? null : tables.get(id);
  }

  /**
   * Refuses to serve a page whose fragment chain is longer than any versioning strategy produces —
   * a corruption guard on a length read from disk, not a cap on the merge.
   */
  private static final int MAX_FRAGMENT_CHAIN = 64;

  /**
   * Accounting for versioned page reconstruction, off unless
   * {@code -Dsirix.versioning.diag=true}. A cold analytical scan pays this only on the pages that
   * span commits, so the interesting quantities are how many such pages there are, how many
   * fragments each needs, and how the time splits between decoding those fragments and merging
   * them — none of which a wall clock or a sampling profile separates on its own.
   */
  private static final boolean VERSIONING_DIAG = Boolean.getBoolean("sirix.versioning.diag");

  private static final LongAdder COMBINES =
      new LongAdder();
  private static final LongAdder FRAGMENTS_LOADED =
      new LongAdder();
  private static final LongAdder COMBINE_NANOS =
      new LongAdder();

  /**
   * Thread time spent fetching a page's fragments — the reads and their decodes — as opposed to
   * merging them. The merge is CPU the scan can spread across workers; a fragment fetch blocks the
   * worker that wants the page, which is a different kind of cost and has to be measured separately.
   */
  private static final LongAdder FRAGMENT_FETCH_NANOS =
      new LongAdder();

  /** Thread time fetching fragments (read + decode) across reconstructions. */
  public static long versioningFragmentFetchNanos() {
    return FRAGMENT_FETCH_NANOS.sum();
  }

  /** Pages reconstructed from more than their own fragment since the last reset. */
  public static long versioningCombines() {
    return COMBINES.sum();
  }

  /** Fragments loaded across those reconstructions. */
  public static long versioningFragmentsLoaded() {
    return FRAGMENTS_LOADED.sum();
  }

  /** CPU time inside {@code combineRecordPages} — the merge alone, without the fragment decodes. */
  public static long versioningCombineNanos() {
    return COMBINE_NANOS.sum();
  }

  public static void resetVersioningDiag() {
    COMBINES.reset();
    FRAGMENTS_LOADED.reset();
    COMBINE_NANOS.reset();
    FRAGMENT_FETCH_NANOS.reset();
  }

  /** Smallest list size worth sweeping, and the floor the doubling threshold resets to. */
  private static final int MIN_BYPASS_PAGE_SWEEP_SIZE = 32;

  /**
   * Drop every already-retired page from {@link #bypassLoadedPathSummaryPages} and re-arm the
   * sweep threshold at twice what survived. Compacts in place — no iterator, no lambda, no
   * intermediate list — because this runs on the page-load path.
   */
  private void dropClosedBypassLoadedPathSummaryPages() {
    final List<KeyValueLeafPage> pages = bypassLoadedPathSummaryPages;
    final int size = pages.size();
    int live = 0;
    for (int i = 0; i < size; i++) {
      final KeyValueLeafPage page = pages.get(i);
      if (!page.isClosed()) {
        pages.set(live++, page);
      }
    }
    for (int i = size - 1; i >= live; i--) {
      pages.remove(i);
    }
    bypassLoadedPathSummaryPagesSweepAt = Math.max(MIN_BYPASS_PAGE_SWEEP_SIZE, live << 1);
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
   * Invalidate the most-recently-read record page for the given index (async CoW, #1077).
   *
   * <p>On the synchronous path this cache self-invalidates: {@code TransactionIntentLog.put}
   * closes the superseded page instance, so the next read's {@code tryAcquireGuard} on the cached
   * instance fails and the lookup falls through to the trie/TIL. On the ASYNC path the superseded
   * instance is the frozen snapshot page, which must stay open for the background flush — the
   * guard succeeds and every read for the rest of the epoch keeps returning the frozen (stale)
   * instance while writes go into the CoW copy. For a hot page like the one holding a parent
   * whose {@code firstChildKey} advances with each insert, that split durably corrupts the
   * sibling chain: each new node links to the stale first child, orphaning everything inserted
   * after the epoch boundary. The writer calls this after CoW-ing a frozen container so the next
   * read re-resolves through the TIL.
   */
  void invalidateMostRecentlyReadRecordPage(final IndexType indexType, final int index) {
    setMostRecentPage(indexType, index, null);
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

    // Lifecycle note: the replaced slot page is deliberately NOT closed here. Most-recent
    // slots hold UNGUARDED references to SHARED cache instances — other transactions hold the
    // same instance in their own slots or are mid-lookup on it, and guardCount carries no
    // ownership information, so "not in cache anymore" never implies "closeable by me".
    // A reader-side close here stole in-flight guards and freed pages under concurrent
    // readers (use-after-free: sporadic 500s/NPEs and silent "node not found" under mixed
    // read/write load). The CACHE owns shared-page lifecycle: eviction closes unguarded
    // pages, and put()-replacement orphans the old instance so the last releaseGuard()
    // tears it down. Stale slot references are revalidated via tryAcquireGuard on use.
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
            // retire(), not close(): a bypassed page is transaction-private (write transactions skip
            // caching PATH_SUMMARY), so nothing else can free it — and close() alone returns early
            // while a guard is held, without orphaning, leaving the frame pinned once the field
            // below is overwritten.
            pathSummaryRecordPage.page.retire();
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

    final long reconstructStart = VERSIONING_DIAG ? System.nanoTime() : 0L;
    final var result = getPageFragments(pageReferenceToRecordPage);
    if (result.pages().isEmpty()) {
      return null;
    }
    if (VERSIONING_DIAG) {
      FRAGMENT_FETCH_NANOS.add(System.nanoTime() - reconstructStart);
    }

    final int maxRevisionsToRestore = resourceConfig.maxNumberOfRevisionsToRestore;
    final VersioningType versioningApproach = resourceConfig.versioningType;

    try {
      // Retire the old swizzled page before replacing it with the combined one. retire(), not
      // close(): close() alone RETURNS EARLY on a guarded page without orphaning it, so once the
      // setPage below overwrites the only reference to it, nothing can ever free its frame. That is
      // one of the two page-lifetime leaks the -Dsirix.debug.memory.leaks census surfaced.
      //
      // Guarded by the cache-ownership check for the same reason as
      // NodeStorageEngineWriter.closeOrphanedPage: when the swizzled instance IS the cache's, the
      // cache keeps handing it to other transactions, and retiring it would free it under them.
      final Page oldSwizzledPage = pageReferenceToRecordPage.getPage();
      if (oldSwizzledPage instanceof KeyValueLeafPage oldKvp && !oldKvp.isClosed()
          && resourceBufferManager.getRecordPageCache().get(pageReferenceToRecordPage) != oldKvp) {
        oldKvp.retire();
      }

      resolveFsstSymbolTables(result.pages());
      final long combineStart = VERSIONING_DIAG ? System.nanoTime() : 0L;
      final Page completePage = versioningApproach.combineRecordPages(result.pages(), maxRevisionsToRestore, this);
      if (VERSIONING_DIAG) {
        COMBINE_NANOS.add(System.nanoTime() - combineStart);
        COMBINES.increment();
        FRAGMENTS_LOADED.add(result.pages().size());
      }
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
        final boolean acquired = kvLeafPage.tryAcquireGuard();
        if (acquired && kvLeafPage.getSlottedPage() != null) {
          currentPageGuard = PageGuard.wrapAlreadyGuarded(kvLeafPage);
        } else {
          if (acquired) {
            // Acquired but unusable (null slottedPage) — release our own guard only. A failed
            // acquire never increments, so releasing there would steal another holder's guard.
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
            Collections.singletonList(page),
            Collections.emptyList(),
            pageReference.getKey()
        );
      }
      return new PageFragmentsResult(
          Collections.emptyList(),
          Collections.emptyList(),
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

    // Read on THIS thread with the reader this transaction already owns, rather than borrowing a
    // fresh one and hopping to a virtual thread. The caller blocks on the result either way, so the
    // async round trip bought nothing — and it cost a great deal: IOStorage.createReader takes a
    // storage-wide monitor to borrow a channel stripe, and close() takes it again to return it, so
    // every fragment of every reconstructed page put two acquisitions of one global lock on the
    // path of every scan worker. Measured on a cold scan of a store with 529 multi-fragment pages,
    // fetching their fragments cost 19.07 ms of thread time per page against 0.92 ms to merge them.
    final Page loadedPage = pageReader.read(pageReference, resourceSession.getResourceConfig());
    assert pageFragmentKey.revision() == ((KeyValuePage<DataRecord>) loadedPage).getRevision() :
        "Revision mismatch: key=" + pageFragmentKey.revision() + ", page="
            + ((KeyValuePage<DataRecord>) loadedPage).getRevision();

    // Atomic cache-or-store with guard (handles race with other threads).
    final KeyValueLeafPage cachedPage = resourceBufferManager.getRecordPageFragmentCache()
        .getOrLoadAndGuard(pageReference, _ -> (KeyValueLeafPage) loadedPage);

    // If another thread won the race, its instance is now cached and the page we loaded from disk
    // was never adopted — close it to free its off-heap segments (production builds have no
    // Cleaner fallback, so this would leak the slot).
    if (cachedPage != loadedPage) {
      ((KeyValueLeafPage) loadedPage).close();
    }

    return CompletableFuture.completedFuture((KeyValuePage<DataRecord>) cachedPage);
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
      case PROJECTION -> getProjectionIndexPage(revisionRoot).getIndirectPageReference(index);
      case VALIDTIME -> getValidTimeIndexPage(revisionRoot).getIndirectPageReference(index);
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
      case PATH, DOCUMENT, CAS, NAME, VECTOR, PROJECTION, VALIDTIME -> recordKey >> Constants.INP_REFERENCE_COUNT_EXPONENT;
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
      case PROJECTION -> getProjectionIndexPage(currentRevisionRootPage).getCurrentMaxLevelOfIndirectPages(index);
      case VALIDTIME -> getValidTimeIndexPage(currentRevisionRootPage).getCurrentMaxLevelOfIndirectPages(index);
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
    if (currentPageGuard == null) {
      return;
    }
    if (!currentPageGuard.isClosed()) {
      try {
        currentPageGuard.close();
      } catch (FrameReusedException e) {
        // Page was evicted and reused - this is fine, we're done with it anyway
        LOGGER.debug("Page frame was reused while closing guard (expected): {}", e.getMessage());
      }
    }
    // Cleared unconditionally. Clearing it only in the not-yet-closed branch left an ALREADY closed
    // guard installed, and getCurrentPage() reports a guard's page without consulting its state —
    // so callers were handed a page nothing was holding, which is indistinguishable from a page
    // that is guarded right up until the sweeper frees it.
    currentPageGuard = null;
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
    if (!CLOSE_INITIATED_VH.compareAndSet(this, 0, 1)) {
      return;
    }
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

      // Drop most-recent slot references. They are UNGUARDED references to SHARED cache
      // instances — this transaction holds no pin on them (the only guard it owns is
      // currentPageGuard, released above), so there is nothing to release and closing them
      // here would free pages other transactions are still using (see setMostRecentPage).
      mostRecentDocumentPage = null;
      mostRecentChangedNodesPage = null;
      mostRecentRecordToRevisionsPage = null;
      mostRecentDeweyIdPage = null;
      Arrays.fill(mostRecentPathPages, null);
      Arrays.fill(mostRecentCasPages, null);
      Arrays.fill(mostRecentNamePages, null);
      // PATH_SUMMARY handled separately below (has special bypass logic)

      // Handle PATH_SUMMARY bypassed pages for write transactions (they're not in cache)
      if (pathSummaryRecordPage != null && trxIntentLog != null) {
        if (!pathSummaryRecordPage.page.isClosed()) {
          // closeCurrentPageGuard() above normally released this page's guard, but it only ever held
          // ONE guard — anything else that guarded this bypassed page is still holding. retire() so
          // the frame is freed here when unguarded and at the last release otherwise, instead of
          // close() silently declining and leaking it.
          pathSummaryRecordPage.page.retire();
        }
      }

      // Every bypass-loaded PATH_SUMMARY page, including ones whose slot was invalidated or
      // overwritten earlier — nothing else ever owned them, so this is their only teardown. retire()
      // is idempotent, so the one still in the slot above is unaffected.
      if (bypassLoadedPathSummaryPages != null) {
        for (final KeyValueLeafPage bypassed : bypassLoadedPathSummaryPages) {
          if (!bypassed.isClosed()) {
            bypassed.retire();
          }
        }
        bypassLoadedPathSummaryPages = null;
        bypassLoadedPathSummaryPagesSweepAt = MIN_BYPASS_PAGE_SWEEP_SIZE;
      }

      // CRITICAL FIX: Clear all mostRecent*Page fields to drop hard references
      // This allows GC to collect the pages if they are closed and not referenced elsewhere
      mostRecentDocumentPage = null;
      mostRecentChangedNodesPage = null;
      mostRecentRecordToRevisionsPage = null;
      mostRecentDeweyIdPage = null;
      pathSummaryRecordPage = null;
      Arrays.fill(mostRecentPathPages, null);
      Arrays.fill(mostRecentCasPages, null);
      Arrays.fill(mostRecentNamePages, null);

      isClosed = true;
    }
  }

  @Override
  public int getNameCount(final int key, final NodeKind kind) {
    assertNotClosed();
    return namePage().getCount(key, kind, this);
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
        if (pathPage == null || indexNumber >= pathPage.getReferencesCount()) {
          yield null;
        }
        yield pathPage.getOrCreateReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = getCASPage(actualRootPage);
        if (casPage == null || indexNumber >= casPage.getReferencesCount()) {
          yield null;
        }
        yield casPage.getOrCreateReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = getNamePage(actualRootPage);
        if (namePage == null || indexNumber >= namePage.getReferencesCount()) {
          yield null;
        }
        yield namePage.getOrCreateReference(indexNumber);
      }
      case PROJECTION -> {
        final ProjectionIndexPage projPage = getProjectionIndexPage(actualRootPage);
        if (projPage == null || indexNumber >= projPage.getReferencesCount()) {
          yield null;
        }
        yield projPage.getOrCreateReference(indexNumber);
      }
      case VALIDTIME -> {
        final ValidTimeIndexPage vtPage = getValidTimeIndexPage(actualRootPage);
        if (vtPage == null || indexNumber >= vtPage.getReferencesCount()) {
          yield null;
        }
        yield vtPage.getOrCreateReference(indexNumber);
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
    
    // For uncommitted pages with no disk key, we're done
    if (rootRef.getKey() < 0) {
      return null;
    }

    // Build canonical cache key (logKey=-1) so writes and reads use the same key
    final PageReference cacheKey = new PageReference()
        .setKey(rootRef.getKey())
        .setDatabaseId(getDatabaseId())
        .setResourceId(getResourceId());

    final HOTLeafPage cached = resourceBufferManager.getHOTLeafPageCache().get(cacheKey);
    if (cached != null && !cached.isClosed()) {
      return cached;
    }

    try {
      final Page loadedPage = pageReader.read(rootRef, resourceConfig);
      if (loadedPage instanceof HOTLeafPage hotLeaf) {
        return loadHOTLeafPageWithVersioning(rootRef, cacheKey, hotLeaf);
      }
      return null;
    } catch (SirixIOException e) {
      return null;
    }
  }
  
  /**
   * Load a HOTLeafPage from storage with proper versioning fragment combining.
   *
   * <p>For FULL versioning, the already-loaded page is complete — no additional I/O needed.
   * For INCREMENTAL/DIFFERENTIAL/SLIDING_SNAPSHOT, loads additional fragments and combines.</p>
   *
   * <p>The fragment chain ({@link PageReference#getPageFragments()}) is read from {@code chainRef}
   * — the real index-root reference — NOT from {@code cacheKey}. The canonical {@code cacheKey}
   * carries no chain, so passing it for fragment lookup would silently drop every older revision's
   * delta fragment and lose all historical entries.</p>
   *
   * @param chainRef  the index-root reference carrying the prior-fragment chain
   * @param cacheKey  the canonical key (logKey=-1) used to store the combined page in the cache
   * @param firstPage the already-loaded newest fragment (avoids a redundant SSD read)
   * @return the combined HOTLeafPage, or null if not found
   */
  private @Nullable HOTLeafPage loadHOTLeafPageWithVersioning(PageReference chainRef, PageReference cacheKey,
      HOTLeafPage firstPage) {
    final VersioningType versioningType = resourceConfig.versioningType;
    final int revsToRestore = resourceConfig.maxNumberOfRevisionsToRestore;

    if (versioningType == VersioningType.FULL) {
      resourceBufferManager.getHOTLeafPageCache().put(cacheKey, firstPage);
      return firstPage;
    }

    final List<HOTLeafPage> fragments = loadHOTPageFragments(chainRef, firstPage);
    if (fragments.isEmpty()) {
      return null;
    }

    // The chain fragments are guarded cache entries — release, never close (see
    // releaseHOTLeafFragments). combinedPage may BE the first fragment (single-fragment /
    // complete-dump fast paths), in which case it stays open for the caller and the cache below.
    // The release must happen even if combining throws, or the guards pin their cache entries
    // permanently; a null combinedPage then simply means nothing is kept open.
    HOTLeafPage combinedPage = null;
    try {
      combinedPage = versioningType.combineHOTLeafPages(fragments, revsToRestore, this);
    } finally {
      releaseHOTLeafFragments(fragments, combinedPage);
    }

    resourceBufferManager.getHOTLeafPageCache().put(cacheKey, combinedPage);
    return combinedPage;
  }
  
  /**
   * Load all HOTLeafPage fragments for versioning reconstruction.
   *
   * <p>Accepts the already-loaded first page to eliminate a redundant SSD read.
   * Additional fragments are loaded sequentially from the versioning chain.</p>
   *
   * <p><b>An empty result means "there is no prior fragment", never "the window could not be
   * read".</b> This window is the WRITE path's input: {@code combineHOTLeafPagesForModification}
   * carries the aging fragment's still-live entries (SLIDING_SNAPSHOT) or re-emits the prior
   * cumulative delta (DIFFERENTIAL) from it, and then rotates the chain REGARDLESS. Both
   * carry-forwards no-op on an empty window, so degrading a read failure to {@code List.of()}
   * committed a sparse fragment without the entries it was required to carry, and dropped the
   * fragment that still held them — silent, permanent data loss with no exception and no log. A
   * failure therefore propagates; the commit aborts instead.
   *
   * @param chainRef the index-root reference carrying the prior-fragment chain
   * @return the window, newest first, of exactly {@code 1 + chainRef.getPageFragments().size()}
   *         fragments; empty only when {@code chainRef} has no durable key yet
   * @throws SirixIOException if any fragment cannot be read, or resolves to something that is not a
   *         {@link HOTLeafPage}
   */
  @Override
  public List<HOTLeafPage> loadHOTLeafFragments(final PageReference chainRef) {
    if (chainRef.getKey() < 0) {
      return List.of(); // nothing written yet — genuinely no prior fragment to carry forward
    }
    final Page first = pageReader.read(chainRef, resourceConfig);
    if (!(first instanceof HOTLeafPage firstPage)) {
      // Close it: a non-leaf page here is a corrupt or mis-swizzled offset, and dropping the
      // instance would leak its off-heap frame on the way out.
      if (first != null) {
        first.close();
      }
      throw new SirixIOException("HOT fragment window head at key " + chainRef.getKey()
          + " is not a HOTLeafPage but " + (first == null ? "null" : first.getClass().getSimpleName())
          + " — refusing to carry forward from an unreadable window");
    }
    return loadHOTPageFragments(chainRef, firstPage);
  }

  private List<HOTLeafPage> loadHOTPageFragments(PageReference chainRef, HOTLeafPage firstPage) {
    final List<HOTLeafPage> fragments = new ArrayList<>();
    fragments.add(firstPage);

    // Check if there are additional fragments to load
    final List<PageFragmentKey> pageFragments = chainRef.getPageFragments();
    if (pageFragments.isEmpty()) {
      return fragments;
    }

    // Load additional fragments from the versioning chain
    // Note: Fragment keys don't include hashes - only the first fragment can be verified
    // Future improvement: Store fragment hashes in PageFragmentKey for complete verification
    try {
      for (PageFragmentKey fragmentKey : pageFragments) {
        final HOTLeafPage hotFragment = loadChainFragmentGuarded(fragmentKey.key());
        if (hotFragment == null) {
          // A SHORT window is worse than none: carryForwardAgingHOTEntries takes the LAST element as
          // the fragment about to age out, so a skipped fragment silently re-points that at the
          // wrong one — carrying entries that are not aging and losing the ones that are.
          throw new SirixIOException("HOT fragment at key " + fragmentKey.key()
              + " is absent or not a HOTLeafPage — the versioning window is incomplete");
        }
        fragments.add(hotFragment);
      }
    } catch (final Throwable loadFailed) {
      // A read failing part way through leaves the window unreachable by the caller, so nothing
      // would ever release the guards taken for the fragments already loaded — and a permanently
      // guarded entry can never be evicted, pinning its off-heap slot for the JVM's lifetime.
      // Release exactly what this call acquired (elements >= 1) and let the failure propagate.
      // Throwable, not RuntimeException: the read this guards against allocates, and allocator
      // exhaustion surfaces as OutOfMemoryError — an Error — so a RuntimeException-only catch would
      // miss the very case it exists for.
      for (int i = 1; i < fragments.size(); i++) {
        fragments.get(i).releaseGuard();
      }
      // firstPage too. On the success path the caller closes it via releaseHOTLeafFragments, but a
      // window that is never returned has no caller — and both entry points read it fresh into a
      // private instance, so nothing else can free its slot.
      firstPage.close();
      throw loadFailed;
    }

    // The window's completeness is the contract every caller indexes against (element 0 = newest,
    // last = the fragment about to age out), so state it here rather than trusting the loop.
    assert fragments.size() == 1 + pageFragments.size()
        : "HOT fragment window is " + fragments.size() + " long, expected " + (1 + pageFragments.size());
    return fragments;
  }

  /**
   * Load one chain fragment through {@link BufferManager#getHOTLeafFragmentCache()}, GUARDED.
   *
   * <p>Chain fragments are re-read by every commit that copy-on-writes the same leaf while the
   * versioning window still spans them — under the default SLIDING_SNAPSHOT with
   * {@code revsToRestore=3} that is essentially every commit of a hot leaf. Reading them straight
   * off {@code pageReader} made the write path pay a synchronous, uncached page read per fragment
   * per commit where it previously did no I/O at all.</p>
   *
   * <p>Scope, precisely: this caches the CHAIN fragments only. Element 0 of a window is the page at
   * {@code reference.getKey()}, which its caller reads fresh and owns, so at the default chain cap
   * the steady-state cost goes from three reads per commit to two rather than to one. Caching
   * element 0 as well would need it to stop being caller-owned — it can become the combined page and
   * is then handed to {@code getHOTLeafPageCache()}, so one instance would sit in two caches.</p>
   *
   * <p>Fragments are treated as immutable at a given offset, which holds only while the data file is
   * append-only. Truncation (rollback / crash recovery) REUSES offsets, so both
   * {@code BufferManagerImpl.clearCachesForDatabase} and {@code clearCachesForResource} must drop
   * this cache — otherwise a reused offset serves pre-truncation bytes into a live merge.</p>
   *
   * <p>The returned page is always guarded, whether or not it was adopted by the cache, so callers
   * have ONE lifetime rule (release exactly once — see
   * {@link #releaseHOTLeafFragments}). When the cache cannot adopt it (a guard-less
   * {@code EmptyCache}, or a lost adoption race) the page is guarded and immediately orphaned, so
   * the caller's release drops the last guard and frees its off-heap slot right there.</p>
   *
   * @param fragmentKey the fragment's durable offset
   * @return the guarded fragment, or {@code null} if it is absent or not a HOT leaf
   */
  private @Nullable HOTLeafPage loadChainFragmentGuarded(final long fragmentKey) {
    final PageReference fragmentRef = new PageReference().setKey(fragmentKey)
                                                        .setDatabaseId(databaseId)
                                                        .setResourceId(resourceId);
    final Cache<PageReference, HOTLeafPage> fragmentCache =
        resourceBufferManager.getHOTLeafFragmentCache();
    try {
      final HOTLeafPage cached = fragmentCache.getAndGuard(fragmentRef);
      if (cached != null) {
        return cached;
      }
    } catch (final UnsupportedOperationException guardUnsupported) {
      // A cache implementation without guard support (EmptyCache) — fall through to an uncached read.
    }

    final Page loaded = pageReader.read(fragmentRef, resourceConfig);
    if (!(loaded instanceof HOTLeafPage fragment)) {
      return null;
    }
    HOTLeafPage adopted = null;
    try {
      adopted = fragmentCache.getOrLoadAndGuard(fragmentRef, _ -> fragment);
    } catch (final UnsupportedOperationException guardUnsupported) {
      // Same fall-through as above: keep the page, just uncached.
    }
    if (adopted == fragment) {
      return adopted; // adopted by the cache AND guarded for us
    }
    if (adopted != null) {
      // Another thread's instance won the race; ours was never adopted, so free it.
      fragment.close();
      return adopted;
    }
    // Not cached: guard it so the caller's release is uniform, then orphan it so that same release
    // drops the last guard and frees the slot instead of leaking it.
    if (!fragment.acquireGuard()) {
      fragment.close(); // unreachable for a just-read private page, but never leak its slot
      return null;
    }
    fragment.markOrphaned();
    return fragment;
  }

  /**
   * Counterpart of {@link #loadHOTLeafFragments}: end the caller's use of a fragment window.
   *
   * <p>The list's first element is the caller-supplied newest page and stays caller-owned (it is
   * closed here unless it is {@code keepOpen}, typically because it became the combined page). Every
   * later element is a GUARDED chain fragment and must be RELEASED, never closed — closing would
   * orphan the shared cache entry, so the next commit would miss and re-read it, defeating the
   * cache while still appearing to work.</p>
   *
   * @param fragments the window as returned by {@link #loadHOTLeafFragments}
   * @param keepOpen  a page the caller still owns and that must not be closed, or {@code null}
   */
  @Override
  public void releaseHOTLeafFragments(final List<HOTLeafPage> fragments,
      final @Nullable HOTLeafPage keepOpen) {
    for (int i = 0; i < fragments.size(); i++) {
      final HOTLeafPage fragment = fragments.get(i);
      if (i == 0) {
        if (fragment != keepOpen && !fragment.isClosed()) {
          fragment.close();
        }
      } else {
        fragment.releaseGuard();
      }
    }
  }
  
  public @Nullable Page loadHOTPage(PageReference reference) {
    assertNotClosed();

    if (reference == null) {
      return null;
    }

    // FIRST: Check transaction log for uncommitted pages (write transactions)
    if (trxIntentLog != null) {
      final PageContainer container = trxIntentLog.get(reference);
      if (container != null) {
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
    if (swizzled instanceof HOTLeafPage hotSwizzled) {
      if (!hotSwizzled.isClosed()) {
        return hotSwizzled;
      }
      reference.setPage(null);
    } else if (swizzled instanceof HOTIndirectPage) {
      return swizzled;
    }

    // For uncommitted pages with no log key, we're done
    if (reference.getKey() < 0 && reference.getLogKey() < 0) {
      return null;
    }

    // Load from storage (only if key >= 0)
    if (reference.getKey() >= 0) {
      final PageReference canonicalKey = new PageReference()
          .setKey(reference.getKey())
          .setDatabaseId(getDatabaseId())
          .setResourceId(getResourceId());

      final HOTLeafPage cachedHot = resourceBufferManager.getHOTLeafPageCache().get(canonicalKey);
      if (cachedHot != null && !cachedHot.isClosed()) {
        reference.setPage(cachedHot);
        return cachedHot;
      }

      try {
        final Page loadedPage = pageReader.read(reference, resourceConfig);

        if (loadedPage instanceof HOTIndirectPage) {
          reference.setPage(loadedPage);
          return loadedPage;
        }

        if (loadedPage instanceof HOTLeafPage hotLeaf) {
          final HOTLeafPage combinedPage = loadHOTLeafPageWithVersioning(reference, canonicalKey, hotLeaf);
          if (combinedPage != null) {
            reference.setPage(combinedPage);
          }
          return combinedPage;
        }
      } catch (SirixIOException e) {
        return null;
      }
    }

    return null;
  }
}
