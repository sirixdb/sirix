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

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.node.CommitCredentials;
import io.sirix.access.trx.node.IndexController;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.Allocators;
import io.sirix.cache.IndexLogKey;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.PageGuard;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.exception.SirixIOException;
import io.sirix.io.SerializationBufferPool;
import io.sirix.node.PooledBytesOut;
import io.sirix.index.IndexType;
import io.sirix.io.Writer;
import io.sirix.node.DeletedNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.page.CASPage;
import io.sirix.page.DeweyIDPage;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageLayout;
import io.sirix.page.NamePage;
import io.sirix.page.PageKind;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.PathSummaryPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.VectorPage;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.settings.VersioningType;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.sirix.utils.Preconditions.checkArgument;
import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Implements the {@link StorageEngineWriter} interface to provide write capabilities to the
 * persistent storage layer.
 * </p>
 *
 * @author Marc Kramis, Seabix AG
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
final class NodeStorageEngineWriter extends AbstractForwardingStorageEngineReader implements StorageEngineWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeStorageEngineWriter.class);

  /**
   * Buffered output for page writes.
   *
   * <p>Use 2x FLUSH_SIZE so single large page fragments do not force grow/copy on every write
   * before the subsequent flush threshold check.
   */
  private BytesOut<?> bufferBytes = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE * 2);

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
   * {@link NodeStorageEngineReader} instance.
   */
  private final NodeStorageEngineReader storageEngineReader;

  /**
   * Determines if transaction is closed.
   */
  private volatile boolean isClosed;

  /**
   * Shared Cleaner that runs the leak-detection callback on every NodeStorageEngineWriter
   * once it becomes phantom-reachable. Used as the post-Java-9 replacement for the
   * deprecated {@code finalize()} override.
   */
  private static final java.lang.ref.Cleaner LEAK_CLEANER = java.lang.ref.Cleaner.create();

  /**
   * State captured for leak diagnostics. Static class with no reference to the enclosing
   * writer — capturing {@code this} would make the writer strongly reachable through the
   * Cleaner queue and defeat the leak detection it implements.
   */
  private static final class LeakDetectorState implements Runnable {
    final int trxId;
    final int writerIdentity;
    final int logIdentity;
    final java.util.concurrent.atomic.AtomicReference<TransactionIntentLog> logRef;
    final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean(false);

    LeakDetectorState(final int trxId, final int writerIdentity, final int logIdentity,
        final TransactionIntentLog log) {
      this.trxId = trxId;
      this.writerIdentity = writerIdentity;
      this.logIdentity = logIdentity;
      this.logRef = new java.util.concurrent.atomic.AtomicReference<>(log);
    }

    @Override
    public void run() {
      if (closed.get()) {
        return;
      }
      final TransactionIntentLog log = logRef.get();
      final int containerCount = log != null ? log.getList().size() : -1;
      LOGGER.warn(
          "NodeStorageEngineWriter FINALIZED WITHOUT CLOSE: trxId={} instance={} TIL={} with {} containers in TIL",
          trxId, writerIdentity, logIdentity, containerCount);
    }
  }

  /** Non-null for the lifetime of this writer; close() flips its closed flag. */
  private final LeakDetectorState leakDetectorState;

  /**
   * Pending fsync future for async durability. For auto-commit mode, fsync runs in background; next
   * commit waits for it.
   */

  /**
   * {@link XmlIndexController} instance.
   */
  private final IndexController<?, ?> indexController;

  /**
   * The keyed trie writer - manages the trie structure of IndirectPages.
   */
  private final KeyedTrieWriter keyedTrieWriter;

  /**
   * The revision to represent.
   */
  private final int representRevision;

  /**
   * {@code true} if this storage engine writer will be bound to a node trx, {@code false} otherwise
   */
  private final boolean isBoundToNodeTrx;

  private static final class IndexLogKeyToPageContainer {
    IndexType indexType;
    long recordPageKey;
    int indexNumber;
    int revisionNumber;
    PageContainer pageContainer;

    IndexLogKeyToPageContainer(final IndexType indexType, final long recordPageKey,
        final int indexNumber, final int revisionNumber, final PageContainer pageContainer) {
      set(indexType, recordPageKey, indexNumber, revisionNumber, pageContainer);
    }

    void set(final IndexType indexType, final long recordPageKey, final int indexNumber,
        final int revisionNumber, final PageContainer pageContainer) {
      this.indexType = indexType;
      this.recordPageKey = recordPageKey;
      this.indexNumber = indexNumber;
      this.revisionNumber = revisionNumber;
      this.pageContainer = pageContainer;
    }

    void copyFrom(final IndexLogKeyToPageContainer other) {
      set(other.indexType, other.recordPageKey, other.indexNumber,
          other.revisionNumber, other.pageContainer);
    }
  }

  /**
   * The most recent page container.
   */
  private IndexLogKeyToPageContainer mostRecentPageContainer;

  /**
   * The second most recent page container.
   */
  private IndexLogKeyToPageContainer secondMostRecentPageContainer;

  /**
   * The most recent path summary page container.
   */
  private IndexLogKeyToPageContainer mostRecentPathSummaryPageContainer;

  /**
   * Most recent page container per {@link IndexType} (ordinal-indexed, lazily populated).
   * The shared {@link #mostRecentPageContainer}/{@link #secondMostRecentPageContainer} pair
   * thrashes when a commit interleaves streams of three or more index types (DOCUMENT +
   * secondary indexes during shredding): every switch to another type evicts, so lookups
   * fall through to the access-ordered {@link #pageContainerCache} probe (hashing plus LRU
   * relink per hit). One slot per type keeps the hot page of EVERY stream one comparison
   * away. PATH_SUMMARY keeps its dedicated {@link #mostRecentPathSummaryPageContainer}
   * slot; its array entry stays unused.
   */
  private IndexLogKeyToPageContainer[] mostRecentByIndexType;

  private final LinkedHashMap<IndexLogKey, PageContainer> pageContainerCache;

  /**
   * Reusable lookup key for pageContainerCache to avoid allocating a new IndexLogKey on every
   * cache probe. MUST NOT be passed to computeIfAbsent as the stored key — the map retains a
   * reference, and subsequent mutations would corrupt it.
   */
  private final IndexLogKey lookupKey = new IndexLogKey(IndexType.DOCUMENT, -1, -1, -1);

  /**
   * Optional binder for write-path singletons.
   * When set, prepareRecordForModification uses factory singletons instead of allocating new nodes.
   */
  private WriteSingletonBinder writeSingletonBinder;

  // ==================== ASYNC AUTO-COMMIT STATE ====================

  /** Backpressure: at most one background snapshot flush in-flight. */
  private final Semaphore flushPermit = new Semaphore(1);

  /** True while a background snapshot flush is running. */
  private volatile boolean asyncFlushInFlight;

  /** Error from background thread — checked and cleared by insert thread. */
  private volatile Throwable asyncFlushError;

  /** Terminal failure latch — once true, NEVER reset. Transaction is permanently failed. */
  private volatile boolean asyncTerminalFailure;

  /**
   * Constructor.
   *
   * @param writer the page writer
   * @param log the transaction intent log
   * @param revisionRootPage the revision root page
   * @param storageEngineReader the storage engine reader used as a delegate
   * @param indexController the index controller, which is used to update indexes
   * @param representRevision the revision to represent
   * @param isBoundToNodeTrx {@code true} if this storage engine writer will be bound to a node trx,
   *        {@code false} otherwise
   */
  NodeStorageEngineWriter(final Writer writer, final TransactionIntentLog log, final RevisionRootPage revisionRootPage,
      final NodeStorageEngineReader storageEngineReader, final IndexController<?, ?> indexController, final int representRevision,
      final boolean isBoundToNodeTrx) {
    this.keyedTrieWriter = new KeyedTrieWriter();
    storagePageReaderWriter = requireNonNull(writer);
    this.log = requireNonNull(log);
    newRevisionRootPage = requireNonNull(revisionRootPage);
    this.storageEngineReader = requireNonNull(storageEngineReader);
    this.indexController = requireNonNull(indexController);
    checkArgument(representRevision >= 0, "The represented revision must be >= 0.");
    this.representRevision = representRevision;
    this.isBoundToNodeTrx = isBoundToNodeTrx;
    mostRecentPageContainer = new IndexLogKeyToPageContainer(IndexType.DOCUMENT, -1, -1, -1, null);
    secondMostRecentPageContainer = new IndexLogKeyToPageContainer(IndexType.DOCUMENT, -1, -1, -1, null);
    mostRecentPathSummaryPageContainer = new IndexLogKeyToPageContainer(IndexType.PATH_SUMMARY, -1, -1, -1, null);
    mostRecentByIndexType = new IndexLogKeyToPageContainer[IndexType.values().length];
    pageContainerCache = new LinkedHashMap<>(100, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<IndexLogKey, PageContainer> eldest) {
        if (size() > 100) {
          // When evicting PageContainer from local cache, ensure pages are properly tracked
          // Pages should be in TIL (will be closed on commit/rollback) or in global cache
          PageContainer container = eldest.getValue();
          if (container != null) {
            // Pages in local cache should already be in TIL (appended via appendLogRecord)
            // No action needed - TIL will handle cleanup
          }
          return true;
        }
        return false;
      }
    };

    // Register a Cleaner-driven leak detector for this writer. Replaces the deprecated
    // finalize() override; runs on a Cleaner thread (not the GC thread), no resurrection,
    // survives finalize() removal in future JDKs. close() flips the closed flag so the
    // detector skips the leak warning on properly-closed writers.
    this.leakDetectorState = new LeakDetectorState(
        storageEngineReader.getTrxId(), System.identityHashCode(this),
        System.identityHashCode(log), log);
    LEAK_CLEANER.register(this, leakDetectorState);
  }

  @Override
  public void setWriteSingletonBinder(final WriteSingletonBinder binder) {
    this.writeSingletonBinder = binder;
  }

  @Override
  public BytesOut<?> newBufferedBytesInstance() {
    bufferBytes = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
    return bufferBytes;
  }

  @Override
  public int getRevisionToRepresent() {
    storageEngineReader.assertNotClosed();
    return representRevision;
  }

  @Override
  public TransactionIntentLog getLog() {
    storageEngineReader.assertNotClosed();
    return log;
  }

  @Override
  public int getRevisionNumber() {
    storageEngineReader.assertNotClosed();
    return newRevisionRootPage.getRevision();
  }

  @Override
  public DataRecord prepareRecordForModification(final long recordKey, final IndexType indexType,
      final int index) {
    storageEngineReader.assertNotClosed();
    checkArgument(recordKey >= 0, "recordKey must be >= 0!");
    requireNonNull(indexType);

    final long recordPageKey = storageEngineReader.pageKey(recordKey, indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final var modifiedPage = cont.getModifiedAsKeyValuePage();

    // Check records[] first — this is the fast path and returns a stable reference.
    final int recordOffset = StorageEngineReader.recordPageOffset(recordKey);
    DataRecord record = modifiedPage.getRecord(recordOffset);
    if (record != null) {
      return record;
    }

    // Zero-allocation fast path: bind write singleton to modified page's slotted page.
    // Write singletons are NOT stored in records[], so this path is hit on every access
    // to a previously created/modified record. The bind is cheap (4 field assignments).
    if (writeSingletonBinder != null && modifiedPage instanceof KeyValueLeafPage kvl
        && kvl.hasSlottedPageSlot(recordKey)) {
      record = writeSingletonBinder.bind(kvl, recordOffset, recordKey);
      if (record != null) {
        return record;
      }
    }

    // Try deserialization from modified page's slotted page (non-singleton path).
    record = storageEngineReader.getValue(modifiedPage, recordKey);
    if (record != null) {
      modifiedPage.setRecord(record);
      return record;
    }

    // Fall back to complete (on-disk) page.
    final var completePage = cont.getCompleteAsKeyValuePage();

    // Zero-copy path: copy raw slot bytes from complete page to modified page, then bind.
    if (writeSingletonBinder != null && modifiedPage instanceof KeyValueLeafPage kvlMod
        && completePage instanceof KeyValueLeafPage kvlComplete) {
      final MemorySegment srcPage = kvlComplete.getSlottedPage();
      if (srcPage != null && PageLayout.isSlotPopulated(srcPage, recordOffset)
          && PageLayout.getDirNodeKindId(srcPage, recordOffset) > 0) {
        kvlMod.copySlotFromPage(kvlComplete, recordOffset);
        record = writeSingletonBinder.bind(kvlMod, recordOffset, recordKey);
        if (record != null) {
          return record;
        }
      }
    }

    // Fallback for non-binder, binder returned null, or non-FlyweightNode (nodeKindId=0)
    final DataRecord oldRecord = storageEngineReader.getValue(completePage, recordKey);
    if (oldRecord == null) {
      final int offset = StorageEngineReader.recordPageOffset(recordKey);
      final var kvlComplete = (KeyValueLeafPage) completePage;
      final var slottedPage = kvlComplete.getSlottedPage();
      final boolean slotPopulated = slottedPage != null
          && PageLayout.isSlotPopulated(slottedPage, offset);
      final var slotData = completePage.getSlot(offset);
      final int populatedCount = slottedPage != null
          ? PageLayout.getPopulatedCount(slottedPage) : -1;
      throw new SirixIOException("Cannot retrieve record from cache: (key: " + recordKey + ") (indexType: " + indexType
          + ") (index: " + index + ") (slotPopulated: " + slotPopulated
          + ") (populatedCount: " + populatedCount
          + ") (slotData: " + (slotData != null ? slotData.byteSize() + " bytes" : "null")
          + ") (completePage.pageKey: " + completePage.getPageKey()
          + ") (completePage.revision: " + completePage.getRevision()
          + ") (modifiedPage.pageKey: " + modifiedPage.getPageKey() + ")");
    }
    record = oldRecord;

    // Unbind flyweight from complete page — ensures mutations go to Java fields,
    // not the old revision's MemorySegment. setRecord will re-serialize to modified page.
    if (record instanceof FlyweightNode fn && fn.isBound()) {
      fn.unbind();
    }

    modifiedPage.setRecord(record);

    return record;
  }

  /**
   * Fast-path variant for the DOCUMENT index type on the insert hot path.
   * Skips assertNotClosed(), argument validation, and the IndexType switch in pageKey().
   */
  @SuppressWarnings("unchecked")
  @Override
  public DataRecord prepareRecordForModificationDocument(final long recordKey) {
    final long recordPageKey = storageEngineReader.pageKeyDocument(recordKey);
    final PageContainer cont = prepareRecordPage(recordPageKey, -1, IndexType.DOCUMENT);
    final var modifiedPage = cont.getModifiedAsKeyValuePage();

    final int recordOffset = StorageEngineReader.recordPageOffset(recordKey);

    // Honour any fresher in-memory object in records[] (mixed-path safety).
    final DataRecord cached = modifiedPage.getRecord(recordOffset);
    if (cached != null) {
      return cached;
    }

    // Singleton binding fast path.
    if (writeSingletonBinder != null && modifiedPage instanceof KeyValueLeafPage kvl
        && kvl.hasSlottedPageSlot(recordKey)) {
      final DataRecord record = writeSingletonBinder.bind(kvl, recordOffset, recordKey);
      if (record != null) {
        return record;
      }
    }

    // Fallback to full method for edge cases (non-slotted page, bind failure).
    return prepareRecordForModification(recordKey, IndexType.DOCUMENT, -1);
  }

  // ==================== DIRECT-TO-HEAP CREATION ====================

  /** Reusable allocation result — zero-alloc on hot path. */
  private KeyValueLeafPage allocKvl;
  private int allocSlotOffset;
  private long allocNodeKey;

  @Override
  public void allocateForDocumentCreation() {
    final long nodeKey = newRevisionRootPage.incrementAndGetMaxNodeKeyInDocumentIndex();
    final long recordPageKey = storageEngineReader.pageKeyDocument(nodeKey);
    final PageContainer cont = prepareRecordPage(recordPageKey, -1, IndexType.DOCUMENT);
    this.allocKvl = (KeyValueLeafPage) cont.getModifiedAsKeyValuePage();
    this.allocSlotOffset = StorageEngineReader.recordPageOffset(nodeKey);
    this.allocNodeKey = nodeKey;
  }

  @Override
  public KeyValueLeafPage getAllocKvl() { return allocKvl; }

  @Override
  public int getAllocSlotOffset() { return allocSlotOffset; }

  @Override
  public long getAllocNodeKey() { return allocNodeKey; }

  @Override
  public DataRecord createRecord(final DataRecord record, final IndexType indexType,
      final int index) {
    storageEngineReader.assertNotClosed();

    // Allocate record key and increment record count.
    // For RECORD_TO_REVISIONS: Use the record's own nodeKey (document node key) for page allocation.
    // This is critical because later lookups via prepareRecordForModification use the document node
    // key.
    // Using an auto-allocated key would cause a mismatch where the record can't be found.
    // $CASES-OMITTED$
    final long createdRecordKey = switch (indexType) {
      case DOCUMENT -> newRevisionRootPage.incrementAndGetMaxNodeKeyInDocumentIndex();
      case CHANGED_NODES -> newRevisionRootPage.incrementAndGetMaxNodeKeyInChangedNodesIndex();
      case RECORD_TO_REVISIONS -> {
        // CRITICAL FIX: Use the document node key for page allocation, not an auto-allocated key.
        // The RevisionReferencesNode stores document node key as its nodeKey, and we need to
        // be able to look it up later using that same key in prepareRecordForModification.
        // Still update max key for proper tracking.
        long documentNodeKey = record.getNodeKey();
        long currentMax = newRevisionRootPage.getMaxNodeKeyInRecordToRevisionsIndex();
        if (documentNodeKey > currentMax) {
          newRevisionRootPage.setMaxNodeKeyInRecordToRevisionsIndex(documentNodeKey);
        }
        yield documentNodeKey;
      }
      case PATH_SUMMARY -> {
        // CRITICAL FIX: Use accessor method instead of direct .getPage() call
        // PageReference.getPage() can return null after TIL.put() nulls it
        // Accessor methods use loadPage() which handles TIL lookups
        final PathSummaryPage pathSummaryPage = storageEngineReader.getPathSummaryPage(newRevisionRootPage);
        yield pathSummaryPage.incrementAndGetMaxNodeKey(index);
      }
      case CAS -> {
        final CASPage casPage = storageEngineReader.getCASPage(newRevisionRootPage);
        yield casPage.incrementAndGetMaxNodeKey(index);
      }
      case PATH -> {
        final PathPage pathPage = storageEngineReader.getPathPage(newRevisionRootPage);
        yield pathPage.incrementAndGetMaxNodeKey(index);
      }
      case NAME -> {
        final NamePage namePage = storageEngineReader.getNamePage(newRevisionRootPage);
        yield namePage.incrementAndGetMaxNodeKey(index);
      }
      case VECTOR -> {
        final VectorPage vectorPage = storageEngineReader.getVectorPage(newRevisionRootPage);
        yield vectorPage.incrementAndGetMaxNodeKey(index);
      }
      case VALIDTIME -> {
        final io.sirix.page.ValidTimeIndexPage validTimePage =
            storageEngineReader.getValidTimeIndexPage(newRevisionRootPage);
        yield validTimePage.incrementAndGetMaxNodeKey(index);
      }
      default -> throw new IllegalStateException();
    };

    final long recordPageKey = storageEngineReader.pageKey(createdRecordKey, indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final KeyValuePage<DataRecord> modified = cont.getModifiedAsKeyValuePage();

    if (modified instanceof KeyValueLeafPage kvl) {
      if (record instanceof FlyweightNode fn) {
        final int offset = (int) (createdRecordKey
            - ((createdRecordKey >> Constants.NDP_NODE_COUNT_EXPONENT)
               << Constants.NDP_NODE_COUNT_EXPONENT));
        kvl.serializeNewRecord(fn, createdRecordKey, offset);
      } else {
        kvl.setNewRecord(record);
      }
    } else {
      modified.setRecord(record);
    }

    return record;
  }

  @Override
  public void persistRecord(final DataRecord record, final IndexType indexType, final int index) {
    if (record instanceof FlyweightNode fn && fn.isWriteSingleton() && fn.getOwnerPage() != null) {
      return; // Bound write singleton — mutations already on heap
    }
    storageEngineReader.assertNotClosed();
    requireNonNull(record);
    requireNonNull(indexType);

    final long recordPageKey = storageEngineReader.pageKey(record.getNodeKey(), indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    cont.getModifiedAsKeyValuePage().setRecord(record);
  }

  @Override
  public void removeRecord(final long recordKey, final IndexType indexType, final int index) {
    storageEngineReader.assertNotClosed();

    final long recordPageKey = storageEngineReader.pageKey(recordKey, indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final DataRecord node = getRecord(recordKey, indexType, index);
    if (node == null) {
      throw new IllegalStateException("Node not found: " + recordKey);
    }

    final Node delNode = new DeletedNode(
        new NodeDelegate(node.getNodeKey(), -1, null, -1, storageEngineReader.getRevisionNumber(), (SirixDeweyID) null));
    cont.getModifiedAsKeyValuePage().setRecord(delNode);
    cont.getCompleteAsKeyValuePage().setRecord(delNode);
  }

  @Override
  public <V extends DataRecord> V getRecord(final long recordKey, final IndexType indexType,
      final int index) {
    storageEngineReader.assertNotClosed();

    checkArgument(recordKey >= Fixed.NULL_NODE_KEY.getStandardProperty());
    requireNonNull(indexType);

    // Calculate page.
    final long recordPageKey = storageEngineReader.pageKey(recordKey, indexType);

    final PageContainer pageCont = getPageContainer(recordPageKey, index, indexType);

    if (pageCont == null) {
      // Fallback to underlying reader. The reader may return a FlyweightNode bound to a page
      // whose MemorySegment lifecycle is managed by the reader (guard-based eviction).
      // Since the writer cannot hold the reader's page guard, the segment may be freed and
      // reused at any time (e.g., by the clock sweeper between successive reader calls).
      // Unbinding materializes all fields to Java primitives, making the node independent
      // of the page segment and preventing use-after-free.
      final V record = storageEngineReader.getRecord(recordKey, indexType, index);
      if (record instanceof FlyweightNode fn && fn.isBound()) {
        fn.unbind();
      }
      return record;
    } else {
      DataRecord node = getRecordForWriteAccess(((KeyValueLeafPage) pageCont.getModified()), recordKey);
      if (node == null) {
        node = getRecordForWriteAccess(((KeyValueLeafPage) pageCont.getComplete()), recordKey);
      }
      return (V) storageEngineReader.checkItemIfDeleted(node);
    }
  }

  private DataRecord getRecordForWriteAccess(final KeyValuePage<? extends DataRecord> page,
      final long recordKey) {
    final int recordOffset = StorageEngineReader.recordPageOffset(recordKey);
    final DataRecord cachedRecord = page.getRecord(recordOffset);
    if (cachedRecord != null) {
      return cachedRecord;
    }

    return storageEngineReader.getValue(page, recordKey);
  }

  @Override
  public String getName(final int nameKey, final NodeKind nodeKind) {
    storageEngineReader.assertNotClosed();
    final NamePage currentNamePage = getNamePage(newRevisionRootPage);
    return (currentNamePage == null || currentNamePage.getName(nameKey, nodeKind, storageEngineReader) == null)
        ? storageEngineReader.getName(nameKey, nodeKind)
        : currentNamePage.getName(nameKey, nodeKind, storageEngineReader);
  }

  @Override
  public int createNameKey(final @Nullable String name, final NodeKind nodeKind) {
    storageEngineReader.assertNotClosed();
    requireNonNull(nodeKind);
    final String string = name == null
        ? ""
        : name;
    final NamePage namePage = getNamePage(newRevisionRootPage);
    return namePage.setName(string, nodeKind, this);
  }

  // ==================== ASYNC AUTO-COMMIT ====================

  @Override
  public void asyncFlush() {
    // Fail-fast: terminal failure is a permanent latch — transaction is unusable.
    if (asyncTerminalFailure) {
      throw new SirixIOException(
          "Transaction in terminal failure state from prior async commit error");
    }

    // Backpressure: block if previous background flush still running
    flushPermit.acquireUninterruptibly();

    // CRITICAL double-check: error may have been set by background thread
    // between our latch check above and the acquire completing.
    final Throwable priorError = asyncFlushError;
    if (priorError != null) {
      asyncFlushError = null;
      asyncTerminalFailure = true;
      flushPermit.release();
      throw new SirixIOException("Prior async commit failed", priorError);
    }

    // If previous snapshot completed, clean it up first
    if (log.getSnapshotSize() > 0 && log.isSnapshotFlushComplete()) {
      log.cleanupSnapshot();
    }

    // O(1) snapshot — array swap + generation increment
    final int snapshotSize = log.snapshot();
    if (snapshotSize == 0) {
      flushPermit.release();
      return;
    }

    // CRITICAL: Invalidate all local container caches. Cached containers point
    // to frozen-zone pages. Without invalidation, cache fast paths return frozen containers.
    clearLocalContainerCaches();

    // Re-add structural pages to fresh TIL for continued operation
    reAddStructuralPagesToTil();

    asyncFlushInFlight = true;

    // Background thread: write KVL pages to disk.
    // CRITICAL: If submission throws (RejectedExecutionException), release permit
    // and latch terminal failure — snapshot state is dangling, no bg thread to process it.
    try {
      CompletableFuture.runAsync(this::executeSnapshotWrite);
    } catch (final Throwable t) {
      asyncFlushInFlight = false;
      asyncTerminalFailure = true;
      flushPermit.release();
      throw new SirixIOException("Failed to submit async commit", t);
    }
  }

  @Override
  public void awaitPendingAsyncFlush() {
    if (!asyncFlushInFlight) {
      return;
    }

    // Block until background thread releases permit
    flushPermit.acquireUninterruptibly();
    flushPermit.release();
    asyncFlushInFlight = false;

    // Check for background thread errors — latch terminal failure
    final Throwable error = asyncFlushError;
    if (error != null) {
      asyncFlushError = null;
      asyncTerminalFailure = true;
      throw new SirixIOException("Async commit failed", error);
    }

    // Clean up: close KVL pages (written to disk), promote IndirectPages
    log.cleanupSnapshot();
  }

  /**
   * Sliding-window width of the background snapshot flush: how many KVL pages are
   * deep-copied and pre-serialized in parallel before the sequential append pass writes
   * and closes them. Two windows are in flight at once (double buffering), so the
   * transient draw on the shared segment-allocator budget is bounded by
   * {@code 2 × WINDOW} copies, each holding a pooled slotted segment (64&nbsp;KiB
   * typical, up to 256&nbsp;KiB) plus its cached encoded form — roughly 35&nbsp;MB
   * typical, ≈100&nbsp;MB worst case, per in-flight flush. The double buffering keeps
   * the flush pool's workers serializing while this thread appends; widening the window
   * past the pool's appetite only inflates the footprint.
   */
  private static final int SNAPSHOT_FLUSH_WINDOW = 128;

  /**
   * Background thread: write all KVL pages from the frozen snapshot to disk.
   * Uses thread-local buffer and shadow PageReference — NEVER writes to real refs.
   * <p>
   * CRITICAL: Each KVL page is deep-copied before serialization. The serialization path
   * mutates the page (addReferences → processEntries, FSST compression, string compression).
   * Without the copy, the insert thread's concurrent deep-copy for CoW would race against
   * these mutations, producing corrupted pages (e.g., zeroed headers, inconsistent slot data).
   * <p>
   * The flush proceeds in sliding windows: each window's pages are deep-copied and
   * pre-serialized IN PARALLEL (the encode caches its output on the copy — the same
   * mechanism the synchronous commit's {@code parallelSerializationOfKeyValuePages}
   * relies on), then a sequential pass appends the cached bytes in snapshot order,
   * records offsets and hashes, and closes the copies. A single-threaded flush cannot
   * keep pace with the insert thread (serialization dominates the flush), which turned
   * the {@code flushPermit} backpressure into a near-synchronous stall; parallel
   * pre-serialization restores the intended overlap.
   */
  private void executeSnapshotWrite() {
    try {
      final BytesOut<?> bgBuffer = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
      final PageReference shadowRef = new PageReference();
      try {
        final ResourceConfiguration config = getResourceSession().getResourceConfig();
        shadowRef.setDatabaseId(storageEngineReader.getDatabaseId());
        shadowRef.setResourceId(storageEngineReader.getResourceId());
        final int size = log.getSnapshotSize();
        // Double-buffered sliding windows: while this thread sequentially appends the
        // current window's cached bytes, the NEXT window is already deep-copying and
        // pre-serializing on SNAPSHOT_FLUSH_POOL — the append pass never leaves the
        // workers idle, so the flush keeps pace with the insert thread's rotation cadence.
        KeyValueLeafPage[] currentWindow = new KeyValueLeafPage[SNAPSHOT_FLUSH_WINDOW];
        KeyValueLeafPage[] nextWindow = new KeyValueLeafPage[SNAPSHOT_FLUSH_WINDOW];
        CompletableFuture<Void> serializeTask = null;
        try {
          serializeTask = serializeSnapshotWindowAsync(config, 0, size, currentWindow);
          for (int base = 0; base < size; base += SNAPSHOT_FLUSH_WINDOW) {
            serializeTask.join();
            serializeTask = null;
            final int nextBase = base + SNAPSHOT_FLUSH_WINDOW;
            if (nextBase < size) {
              serializeTask = serializeSnapshotWindowAsync(config, nextBase, size, nextWindow);
            }
            // Sequential pass: append cached bytes in snapshot order, record offsets, close.
            final int end = Math.min(nextBase, size);
            for (int i = base; i < end; i++) {
              final KeyValueLeafPage serializationCopy = currentWindow[i - base];
              if (serializationCopy == null) {
                continue;
              }
              shadowRef.setKey(Constants.NULL_ID_LONG);
              try {
                storagePageReaderWriter.write(config, shadowRef, serializationCopy, bgBuffer);
              } finally {
                // Null the slot only once the copy is closed — a write failure must leave
                // nothing open, and a slot nulled before the write would hide the copy from
                // closeWindowLeftovers.
                currentWindow[i - base] = null;
                serializationCopy.close();
              }
              log.setSnapshotDiskOffset(i, shadowRef.getKey());
              log.setSnapshotHash(i, shadowRef.getHash());
            }
            final KeyValueLeafPage[] swap = currentWindow;
            currentWindow = nextWindow;
            nextWindow = swap;
          }
        } finally {
          // On failure mid-flight, wait out the in-flight serialization (its copies must
          // not leak or race the cleanup below), then release everything still open.
          if (serializeTask != null) {
            try {
              serializeTask.join();
            } catch (final Throwable ignored) {
              // The primary failure is already propagating; the join only fences the workers.
            }
          }
          closeWindowLeftovers(currentWindow);
          closeWindowLeftovers(nextWindow);
        }
        storagePageReaderWriter.flushBufferedWrites(bgBuffer);
      } finally {
        bgBuffer.close();
      }
      log.markSnapshotFlushComplete();
    } catch (final Throwable t) {
      asyncFlushError = t;
      asyncTerminalFailure = true;
    } finally {
      flushPermit.release();
    }
  }

  /**
   * Dedicated pool for the background snapshot flush's parallel pre-serialization,
   * shared JVM-wide by every resource's flushes (concurrent bulk imports divide it).
   * Capped below the core count on purpose: the flush runs CONCURRENTLY with the insert
   * thread, and letting it fan out across every core halves insert throughput through
   * memory-bandwidth contention (the encode path streams 64&nbsp;KB segments through
   * LZ4/RLE codecs). Two workers keep the flush ahead of the rotation cadence on small
   * hosts while leaving the insert thread its core; larger hosts and multi-import
   * services scale it via {@code -Dsirix.asyncFlush.parallelism} (clamped to
   * ForkJoinPool's maximum of 32767 — an oversized value must degrade, not turn every
   * write transaction into an ExceptionInInitializerError).
   */
  private static final ForkJoinPool SNAPSHOT_FLUSH_POOL =
      new ForkJoinPool(Math.min(32767, Math.max(1,
          Integer.getInteger("sirix.asyncFlush.parallelism",
              Math.min(2, Runtime.getRuntime().availableProcessors() - 1)))));

  /**
   * Kick off the parallel deep-copy + pre-serialize pass for the snapshot window starting at
   * {@code base} (exclusive end {@code min(base + SNAPSHOT_FLUSH_WINDOW, size)}) on the
   * dedicated flush pool. Each produced copy carries its encoded bytes in the page-local
   * compressed cache, so the subsequent sequential append emits without re-encoding.
   */
  private CompletableFuture<Void> serializeSnapshotWindowAsync(final ResourceConfiguration config,
      final int base, final int size, final KeyValueLeafPage[] window) {
    final int end = Math.min(base + SNAPSHOT_FLUSH_WINDOW, size);
    // Parallel streams execute inside the pool that invokes the terminal operation, so
    // submitting the whole stream confines its splits to SNAPSHOT_FLUSH_POOL.
    return CompletableFuture.runAsync(() -> {
      // Leaves NEVER throw: a parallel stream propagates a leaf's exception to the root
      // WITHOUT awaiting its running/queued siblings, so a throwing leaf would release the
      // outer thread's join while stragglers still deep-copy from snapshot pages (racing
      // rollback's log.clear()) and store copies into window arrays the cleanup pass has
      // already scanned (leaking their pooled segments). Capturing the first failure and
      // rethrowing AFTER the terminal operation keeps every join a true fence.
      final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
      IntStream.range(base, end).parallel().forEach(i -> {
        if (firstFailure.get() != null) {
          return;
        }
        try {
          final PageContainer container = log.getSnapshotEntry(i);
          if (container == null) {
            return;
          }
          final Page modified = container.getModified();
          if (!(modified instanceof KeyValueLeafPage kvl)) {
            return;
          }
          final KeyValueLeafPage serializationCopy = kvl.deepCopy();
          try {
            serializeKeyValuePage(config, serializationCopy);
            if (hasUnresolvedOverflowReferences(serializationCopy)) {
              // Overlong records: serialization spilled values into OverflowPages whose
              // disk keys only exist once the recursive final commit writes them — the
              // encoded bytes here carry NULL overflow keys and the overflow payload
              // lives only on this copy (#1076). Flushing would freeze the broken bytes
              // as the page's durable image and silently lose the records. Skip the
              // flush and mark the slot so cleanupSnapshot() promotes the ORIGINAL page
              // into the live TIL, where the final commit resolves overflow correctly.
              serializationCopy.close();
              log.setSnapshotDiskOffset(i, TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL);
              return;
            }
          } catch (final Throwable t) {
            // A copy that never reached the window would be invisible to
            // closeWindowLeftovers — release its pooled segments before recording.
            serializationCopy.close();
            throw t;
          }
          window[i - base] = serializationCopy;
        } catch (final Throwable t) {
          firstFailure.compareAndSet(null, t);
        }
      });
      final Throwable failure = firstFailure.get();
      if (failure != null) {
        if (failure instanceof RuntimeException runtimeException) {
          throw runtimeException;
        }
        if (failure instanceof Error error) {
          throw error;
        }
        throw new SirixIOException(failure);
      }
    }, SNAPSHOT_FLUSH_POOL);
  }

  /**
   * {@code true} when serialization left overflow {@link PageReference}s on {@code page}
   * whose disk keys are still unassigned — such a page's encoded form is only valid after
   * the recursive commit writes its OverflowPages (#1076).
   */
  private static boolean hasUnresolvedOverflowReferences(final KeyValueLeafPage page) {
    for (final PageReference reference : page.getReferencesMap().values()) {
      if (reference.getKey() == Constants.NULL_ID_LONG) {
        return true;
      }
    }
    return false;
  }

  /** Close and null out any copies left in a snapshot window after a failed flush. */
  private static void closeWindowLeftovers(final KeyValueLeafPage[] window) {
    for (int i = 0; i < window.length; i++) {
      final KeyValueLeafPage leftover = window[i];
      if (leftover != null) {
        window[i] = null;
        leftover.close();
      }
    }
  }

  /**
   * Invalidate all local container caches to prevent stale cache hits
   * returning frozen-zone containers after snapshot.
   */
  private void clearLocalContainerCaches() {
    pageContainerCache.clear();
    mostRecentPageContainer.set(IndexType.DOCUMENT, -1, -1, -1, null);
    secondMostRecentPageContainer.set(IndexType.DOCUMENT, -1, -1, -1, null);
    mostRecentPathSummaryPageContainer.set(IndexType.PATH_SUMMARY, -1, -1, -1, null);
    clearMostRecentByIndexTypeSlots();
  }

  /**
   * Invalidate every per-{@link IndexType} most-recent slot. Holder objects are kept
   * allocated (zero-alloc steady state) — the {@code recordPageKey = -1} sentinel can
   * never match a real lookup, and dropping the {@link PageContainer} reference prevents
   * both stale hits and pinned garbage.
   */
  private void clearMostRecentByIndexTypeSlots() {
    final IndexLogKeyToPageContainer[] byType = mostRecentByIndexType;
    if (byType == null) {
      return;
    }
    for (int i = 0; i < byType.length; i++) {
      final IndexLogKeyToPageContainer slot = byType[i];
      if (slot != null) {
        slot.set(slot.indexType, -1, -1, -1, null);
      }
    }
  }

  /**
   * Re-add structural pages to the fresh TIL after snapshot.
   * <p>
   * After snapshot, the current TIL is empty. Structural pages (RevisionRootPage,
   * PathSummaryPage, NamePage, etc.) are in the frozen snapshot. We re-add them
   * to the current TIL so the insert thread can continue without CoW overhead
   * for these frequently-accessed pages.
   * <p>
   * IndirectPages in the trie are NOT re-added — they will be CoW'd on first
   * access via prepareIndirectPage() if needed.
   */
  private void reAddStructuralPagesToTil() {
    // Re-add structural pages referenced by RevisionRootPage.
    // These are the top-level index root pages that get modified during insertion
    // (e.g., NamePage for name keys, PathSummaryPage for path summaries).
    reAddPageIfFrozen(newRevisionRootPage.getPathSummaryPageReference());
    reAddPageIfFrozen(newRevisionRootPage.getNamePageReference());
    reAddPageIfFrozen(newRevisionRootPage.getCASPageReference());
    reAddPageIfFrozen(newRevisionRootPage.getPathPageReference());
    reAddPageIfFrozen(newRevisionRootPage.getDeweyIdPageReference());
    reAddPageIfFrozen(newRevisionRootPage.getValidTimeIndexPageReference());
  }

  /**
   * If a page reference is in the frozen snapshot, re-add its container to the current TIL.
   * This ensures the insert thread can continue modifying structural pages without CoW.
   */
  private void reAddPageIfFrozen(final PageReference ref) {
    if (ref != null && log.isFrozen(ref)) {
      final PageContainer container = log.get(ref);
      if (container != null) {
        log.put(ref, container);
      }
    }
  }

  /**
   * Deep-copy a frozen PageContainer for Copy-on-Write. Both complete and modified KVL pages
   * are deep-copied to ensure full independence from the frozen originals.
   *
   * @param container the frozen container to copy
   * @return a fully independent deep copy
   */
  private PageContainer deepCopyFrozenContainer(final PageContainer container) {
    final var frozenModified = (KeyValueLeafPage) container.getModified();
    final var frozenComplete = (KeyValueLeafPage) container.getComplete();
    final var cowModified = frozenModified.deepCopy();
    final var cowComplete = (frozenComplete == frozenModified)
        ? cowModified
        : frozenComplete.deepCopy();
    return PageContainer.getInstance(cowComplete, cowModified);
  }

  @Override
  public void commit(final @Nullable PageReference reference) {
    if (reference == null) {
      return;
    }

    PageContainer container = log.get(reference);

    if (container == null) {
      // Overflow pages (#1076) are created in-memory by KeyValueLeafPage#processEntries and
      // hang off the owning leaf's references map WITHOUT a TransactionIntentLog entry (their
      // logKey stays NULL, so the stale-claim guard below never applies to them). The leaf's
      // Page#commit recursion lands here for them — write the page and record its disk key so
      // the leaf serializes a resolvable key instead of NULL (the read path requires
      // reference.getKey() != NULL_ID_LONG to load the overflow record).
      // Side-map overflow pages follow the identical discipline (they hang off a HOTLeafPage's
      // side map without a TIL entry — HOTLeafPage#commit recursion lands here, exactly like
      // OverflowPage refs off a KeyValueLeafPage): write the page, record its offset key so the
      // owning leaf serializes a resolvable reference. A reference that already carries a disk
      // key with no in-memory page is an unchanged segment shared from a prior revision — it
      // falls through to the no-op return below by design.
      final var sideMapPage = reference.getPage();
      if (sideMapPage instanceof OverflowPage && reference.getKey() == Constants.NULL_ID_LONG) {
        storagePageReaderWriter.write(getResourceSession().getResourceConfig(), reference, sideMapPage,
                                      bufferBytes);
        reference.setPage(null);
        return;
      }
      // Fail loudly on an unresolvable TIL claim (#1077): a reference that still carries a
      // logKey after all three TIL layers missed — with no disk offset either — is a stale
      // CoW copy whose backing entry is gone. Returning silently here serialized the parent
      // with child key -1, making the flushed subtree vanish from the committed revision
      // without any error. (A Layer-3 resolution resets the logKey and assigns the disk key,
      // so a resolved stale copy never trips this guard.)
      if (reference.getLogKey() >= 0 && reference.getKey() == Constants.NULL_ID_LONG) {
        throw new SirixIOException(
            "Commit traversal hit an unresolvable stale page reference (logKey=" + reference.getLogKey()
                + ", generation=" + reference.getActiveTilGeneration()
                + "): the referenced page is in no TIL layer and has no disk offset — refusing to"
                + " serialize a dangling child pointer (data would silently be lost).");
      }
      return;
    }

    final var page = container.getModified();

    // Guard against double-commit: when a HOTIndirectPage is COW'd, its child
    // references are copied via new PageReference(original). The copy shares the
    // same logKey, so log.get() returns the same container. If the page was already
    // committed (and its off-heap memory freed) through the original reference,
    // skip re-serialization and copy the disk key from the original reference.
    if (page.isClosed()) {
      final int logKey = reference.getLogKey();
      if (logKey >= 0) {
        final PageReference originalRef = log.getOriginalRef(logKey);
        if (originalRef != null && originalRef != reference) {
          if (originalRef.getKey() >= 0) {
            reference.setKey(originalRef.getKey());
            reference.setHash(originalRef.getHash());
          }
        }
      }
      return;
    }

    // Recursively commit indirectly referenced pages and then write self.
    page.commit(this);
    storagePageReaderWriter.write(getResourceSession().getResourceConfig(), reference, page, bufferBytes);

    // Propagate disk offset to TIL back-reference so other PageReference copies
    // (from CoW'd indirect pages sharing the same logKey) can resolve the disk key
    // when they hit the isClosed() guard in a subsequent commit(ref) call.
    final int refLogKey = reference.getLogKey();
    if (refLogKey >= 0) {
      final PageReference backRef = log.getOriginalRef(refLogKey);
      if (backRef != null && backRef != reference && backRef.getKey() < 0) {
        backRef.setKey(reference.getKey());
        backRef.setHash(reference.getHash());
      }
    }

    container.getComplete().close();
    page.close();

    // Remove page reference.
    reference.setPage(null);
  }

  @Override
  public UberPage commit(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp,
      final boolean isAutoCommitting, final boolean isIntermediateCommit) {
    storageEngineReader.assertNotClosed();

    storageEngineReader.resourceSession.getCommitLock().lock();

    try {
      final UberPage uberPage = commitWritePages(commitMessage, commitTimestamp, isIntermediateCommit);
      hardenCommit(uberPage, isIntermediateCommit);
      return uberPage;
    } finally {
      storageEngineReader.resourceSession.getCommitLock().unlock();
    }
  }

  @Override
  public UberPage commitWritePages(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp,
      final boolean isIntermediateCommit) {
    storageEngineReader.assertNotClosed();

    {
      final boolean timing = LOGGER.isDebugEnabled();

      final Path commitFile = storageEngineReader.resourceSession.getCommitFile();

      // Issues with windows that it's not created in the first time?
      createIfAbsent(commitFile);

      final UberPage uberPage = storageEngineReader.getUberPage();

      setUserIfPresent();
      setCommitMessageAndTimestampIfRequired(commitMessage, commitTimestamp);

      // PIPELINING: Serialize pages WHILE previous fsync may still be running.
      // This overlaps CPU work (serialization) with IO work (fsync).
      final long t0 = timing ? System.nanoTime() : 0;
      parallelSerializationOfKeyValuePages();

      final long t1 = timing ? System.nanoTime() : 0;
      LOGGER.debug("TIL size before recursive commit: {}", log.getList().size());
      uberPage.commit(this);
      LOGGER.debug("TIL size after recursive commit: {} (closed entries not cleaned)", log.getList().size());

      // Flush the buffered page tail WITHOUT any barrier: after phase 1 every revision-N page
      // must be readable by offset through any reader channel (the pipelined successor epoch
      // reads the pending revision's pages before phase 2 hardens). Plain write(2), no fsync —
      // the durability barriers remain phase 2's job.
      storagePageReaderWriter.flushBufferedWrites(bufferBytes);

      if (timing) {
        LOGGER.debug("Commit phase 1 r{}: serialize={}ms recursive={}ms", uberPage.getRevisionNumber(),
            ms(t1 - t0), ms(System.nanoTime() - t1));
      }
      return uberPage;
    }
  }

  @Override
  public void hardenCommit(final UberPage uberPage, final boolean isIntermediateCommit) {
    {
      final boolean timing = LOGGER.isDebugEnabled();

      final Path commitFile = storageEngineReader.resourceSession.getCommitFile();

      final PageReference uberPageReference =
          new PageReference().setDatabaseId(storageEngineReader.getDatabaseId()).setResourceId(storageEngineReader.getResourceId());
      uberPageReference.setPage(uberPage);

      final long t2 = timing ? System.nanoTime() : 0;

      final int revision = uberPage.getRevisionNumber();

      // Persist the index catalogue BEFORE the uber-page beacon. The beacon (writeUberPageReference)
      // is the durable commit point; writing {revision}.xml AFTER it opened a crash window in which a
      // committed revision had no index catalogue, so a reopen resurrected a stale catalogue from an
      // older revision's file (the load side falls back to the most recent {N}.xml <= the requested
      // revision — see AbstractResourceSession#initializeIndexController). Serializing and fsync'ing it
      // first means either the catalogue is durable before the revision is acknowledged, or the
      // revision was never committed and the orphaned file (named for an uncommitted, higher revision)
      // is never consulted.
      //
      // Intermediate auto-commits skip this when indexes are unchanged; final/explicit commits always
      // serialize so the last revision has a valid catalogue snapshot.
      if (!isIntermediateCommit || indexController.getIndexes().isDirty()) {
        serializeIndexDefinitions(revision);
        indexController.getIndexes().clearDirty();
      }

      final long t3 = timing ? System.nanoTime() : 0;

      // CRITICAL crash-safety invariant (write-ahead property): all data pages AND the index
      // catalogue (above) MUST be durable BEFORE either uber-page beacon is written.
      // writeUberPageReference OWNS the entire data-durability protocol — it flushes the buffered
      // page tail, forces the data file, and writes both beacons through a write-through (DSYNC)
      // channel, so its RETURN is the commit acknowledge (see the Writer#writeUberPageReference
      // durability contract). The former extra barriers here (a pre-beacon forceAll that covered
      // strictly less than the internal barrier, plus a post-beacon acknowledge fsync) were
      // duplicated kernel/journal work whose accumulated pressure degraded long commit-heavy runs.
      storagePageReaderWriter.writeUberPageReference(getResourceSession().getResourceConfig(), uberPageReference,
          uberPage, bufferBytes);

      final long t4 = timing ? System.nanoTime() : 0;

      // CRITICAL: Release current page guard BEFORE TIL.clear()
      // If guard is on a TIL page, the page won't close (guardCount > 0 check)
      storageEngineReader.closeCurrentPageGuard();

      // Clear TransactionIntentLog - closes all modified pages
      log.clear();

      // Clear local cache (pages are already handled by log.clear())
      pageContainerCache.clear();

      // Reset cache references since pages have been returned to pool
      mostRecentPageContainer.set(IndexType.DOCUMENT, -1, -1, -1, null);
      secondMostRecentPageContainer.set(IndexType.DOCUMENT, -1, -1, -1, null);
      mostRecentPathSummaryPageContainer.set(IndexType.PATH_SUMMARY, -1, -1, -1, null);
      clearMostRecentByIndexTypeSlots();

      final long t5 = timing ? System.nanoTime() : 0;

      // Delete commit file which denotes that a commit must write the log in the data file.
      try {
        deleteIfExists(commitFile);
      } catch (final IOException e) {
        throw new SirixIOException("Commit file couldn't be deleted!");
      }

      if (timing) {
        LOGGER.debug("Commit phase 2 r{}: indexDefs={}ms uberWrite={}ms tilClear={}ms total={}ms",
            revision, ms(t3 - t2), ms(t4 - t3), ms(t5 - t4), ms(t5 - t2));
      }
    }
  }

  private void setCommitMessageAndTimestampIfRequired(@Nullable String commitMessage,
      @Nullable Instant commitTimestamp) {
    if (commitMessage != null) {
      newRevisionRootPage.setCommitMessage(commitMessage);
    }

    if (commitTimestamp != null) {
      newRevisionRootPage.setCommitTimestamp(commitTimestamp);
    }
  }

  private void serializeIndexDefinitions(int revision) {
    final var indexCatalog = indexController.getIndexes();
    // Persist the catalogue when it has definitions, OR when it was mutated this commit even though
    // it is now EMPTY (the last index was dropped). The latter is essential: the load-side
    // (AbstractResourceSession#initializeIndexController) falls back to the most recent {N}.xml at or
    // below the requested revision, so without an EMPTY catalogue file at the drop revision a reopen
    // would resurrect the pre-drop catalogue from an older revision's file. An empty {revision}.xml
    // ("<indexes/>") makes the drop of the last index stick across the commit, while older revisions
    // keep their own non-empty files (time-travel preserved).
    if (!indexCatalog.getIndexDefs().isEmpty() || indexCatalog.isDirty()) {
      final Path indexes = storageEngineReader.getResourceSession().getResourceConfig().resourcePath.resolve(
          ResourceConfiguration.ResourcePaths.INDEXES.getPath()).resolve(revision + ".xml");

      try (final OutputStream out = newOutputStream(indexes, CREATE)) {
        indexController.serialize(out);
      } catch (final IOException e) {
        throw new SirixIOException("Index definitions couldn't be serialized!", e);
      }

      // fsync the catalogue so it is durable BEFORE the uber-page beacon acknowledges the revision.
      // commit() serializes index definitions ahead of writeUberPageReference precisely so a crash
      // cannot leave a committed revision without its {revision}.xml; that guarantee only holds if the
      // bytes have actually reached stable storage here (a bare OutputStream.close only flushes to the
      // OS page cache).
      try (final FileChannel channel = FileChannel.open(indexes, WRITE)) {
        channel.force(true);
      } catch (final IOException e) {
        throw new SirixIOException("Index definitions couldn't be fsync'd!", e);
      }
    }
  }

  /**
   * Threshold for switching from sequential to parallel processing. For small commits, parallel
   * stream overhead exceeds benefits.
   */
  private static final int PARALLEL_SERIALIZATION_THRESHOLD =
      Integer.getInteger("sirix.commit.parallelSerializationThreshold", 4);

  private void parallelSerializationOfKeyValuePages() {
    final var resourceConfig = getResourceSession().getResourceConfig();
    final var logList = log.getList();

    if (logList.size() < PARALLEL_SERIALIZATION_THRESHOLD) {
      // Sequential: iterate directly — no intermediate collection
      for (final var container : logList) {
        final var modified = container.getModified();
        if (modified instanceof KeyValueLeafPage) {
          serializeKeyValuePage(resourceConfig, modified);
        }
      }
    } else {
      // Parallel: stream-filter avoids materializing an intermediate ArrayList
      logList.parallelStream()
          .map(PageContainer::getModified)
          .filter(p -> p instanceof KeyValueLeafPage)
          .forEach(page -> serializeKeyValuePage(resourceConfig, page));
    }
  }

  private void serializeKeyValuePage(final ResourceConfiguration resourceConfig, final Page page) {
    var pooledSeg = SerializationBufferPool.INSTANCE.acquire();
    try {
      var bytes = new PooledBytesOut(pooledSeg);
      PageKind.KEYVALUELEAFPAGE.serializePage(resourceConfig, bytes, page, SerializationType.DATA);
    } catch (final Exception e) {
      if (e instanceof RuntimeException re) {
        throw re;
      }
      throw new SirixIOException(e);
    } finally {
      SerializationBufferPool.INSTANCE.release(pooledSeg);
    }
  }

  private static long ms(final long nanos) {
    return nanos / 1_000_000;
  }

  private UberPage readUberPage() {
    return (UberPage) storagePageReaderWriter.read(storagePageReaderWriter.readUberPageReference(),
        getResourceSession().getResourceConfig());
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
    final Optional<User> optionalUser = storageEngineReader.resourceSession.getUser();
    optionalUser.ifPresent(newRevisionRootPage::setUser);
  }

  @Override
  public UberPage rollback() {
    storageEngineReader.assertNotClosed();

    // Best-effort: await + cleanup even if async errored.
    // We still need to drain the snapshot and clear TIL regardless.
    try {
      awaitPendingAsyncFlush();
    } catch (final SirixIOException e) {
      LOGGER.error("Async commit failed during rollback — cleaning up anyway", e);
    }

    // CRITICAL: Release current page guard BEFORE TIL.clear()
    // If guard is on a TIL page, the page won't close (guardCount > 0 check)
    storageEngineReader.closeCurrentPageGuard();

    // Clear TransactionIntentLog - closes all modified pages (including snapshot pages)
    log.clear();

    // Clear local cache and reset references (pages already handled by log.clear())
    clearLocalContainerCaches();

    return readUberPage();
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void close() {
    if (!isClosed) {
      storageEngineReader.assertNotClosed();

      // Best-effort: await + cleanup async commit even if errored
      try {
        awaitPendingAsyncFlush();
      } catch (final SirixIOException e) {
        LOGGER.error("Async commit failed during close — cleaning up anyway", e);
      }

      // (The former pending async acknowledge-fsync is gone: writeUberPageReference is durable
      // on return — see its Writer contract — so there is nothing to await at close.)

      // Don't clear the cached containers here - they've either been:
      // 1. Already cleared and returned to pool during commit(), or
      // 2. Will be cleared and returned to pool by log.close() below
      // Clearing them here could corrupt pages that have been returned to pool
      // and reused by other transactions.

      final UberPage lastUberPage = readUberPage();

      storageEngineReader.resourceSession.setLastCommittedUberPage(lastUberPage);

      if (!isBoundToNodeTrx) {
        storageEngineReader.resourceSession.closePageWriteTransaction(storageEngineReader.getTrxId());
      }

      // CRITICAL: Close storageEngineReader FIRST to release guards BEFORE TIL tries to close pages
      // If guards are active when TIL.close() runs, pages won't close (guardCount > 0 check)
      storageEngineReader.close();

      // Now TIL can close pages (guards released)
      log.close();

      // CRITICAL FIX: Clear cache AFTER log.close() to avoid OOM
      // log.close() needs the cache entries to properly unpin/close pages
      // Once closed, we must drop references to allow GC

      // CRITICAL: Close pages in pageContainerCache that are NOT in TIL or cache
      // This handles pages that were cached but TIL was cleared (e.g., after commit)
      for (PageContainer container : pageContainerCache.values()) {
        closeOrphanedPagesInContainer(container);
      }

      pageContainerCache.clear();
      mostRecentPageContainer = null;
      secondMostRecentPageContainer = null;
      mostRecentPathSummaryPageContainer = null;
      mostRecentByIndexType = null;

      // Close the storage writer and its three file channels (data, SYNC revisions, DSYNC
      // beacon). NOTHING else does: storageEngineReader.close() deliberately skips its
      // pageReader for write transactions (trxIntentLog != null — the pageReader IS this
      // writer), so omitting this leaked three descriptors per write transaction — every
      // commit with KEEP_OPEN swaps in a fresh writer via createPageTransaction, growing FD
      // usage without bound until the GC's channel cleaner happened to run.
      storagePageReaderWriter.close();

      isClosed = true;
      // Tell the Cleaner-registered leak detector this writer closed cleanly so the
      // post-GC callback skips its warn-log.
      leakDetectorState.closed.set(true);
    }

  }

  /**
   * Close orphaned pages in a container (pages not in cache). If page is in cache, cache will manage
   * it - we just drop our reference. If page is NOT in cache, we must release guard and close it.
   */
  private void closeOrphanedPagesInContainer(PageContainer container) {
    if (container == null) {
      return;
    }

    closeOrphanedPage(container.getComplete());
    if (container.getModified() != container.getComplete()) {
      closeOrphanedPage(container.getModified());
    }
  }

  private void closeOrphanedPage(final Page page) {
    if (page instanceof KeyValueLeafPage kvlPage && !kvlPage.isClosed()) {
      PageReference ref = new PageReference().setKey(kvlPage.getPageKey())
                                             .setDatabaseId(storageEngineReader.getDatabaseId())
                                             .setResourceId(storageEngineReader.getResourceId());
      KeyValueLeafPage cachedPage = storageEngineReader.getBufferManager().getRecordPageCache().get(ref);
      if (cachedPage != kvlPage) {
        if (kvlPage.getGuardCount() > 0) {
          kvlPage.releaseGuard();
        }
        kvlPage.close();
      }
    } else if (page instanceof HOTLeafPage hotLeaf && !hotLeaf.isClosed()) {
      // Do NOT free a HOT leaf that is still owned by the shared HOT-leaf buffer cache:
      // the combined read-side page is handed to the writer as a PageContainer's complete
      // page, so the SAME instance lives in both places. Closing it here would free the
      // off-heap MemorySegment out from under concurrent readers (use-after-free).
      if (!storageEngineReader.getBufferManager().getHOTLeafPageCache().containsPage(hotLeaf)) {
        if (hotLeaf.getGuardCount() > 0) {
          hotLeaf.releaseGuard();
        }
        hotLeaf.close();
      }
    }
  }

  @Override
  public DeweyIDPage getDeweyIDPage(RevisionRootPage revisionRoot) {
    // TODO
    return null;
  }

  private PageContainer getPageContainer(final long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    PageContainer pageContainer =
        getMostRecentPageContainer(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision());
    if (pageContainer != null) {
      return pageContainer;
    }

    final int revision = newRevisionRootPage.getRevision();
    lookupKey.setIndexType(indexType).setRecordPageKey(recordPageKey)
        .setIndexNumber(indexNumber).setRevisionNumber(revision);
    final PageContainer cached = pageContainerCache.get(lookupKey);
    if (cached != null) {
      return cached;
    }

    final PageReference pageReference = storageEngineReader.getPageReference(newRevisionRootPage, indexType, indexNumber);
    // Use writer's TIL-aware trie traversal instead of reader's disk-only traversal.
    // After async epoch rotation, IndirectPages may be in the TIL but not yet on disk;
    // the reader's getLeafPageReference would try to load them from disk with key=-1.
    final PageReference reference = keyedTrieWriter.prepareLeafOfTree(this, log,
        getUberPage().getPageCountExp(indexType), pageReference, recordPageKey, indexNumber,
        indexType, newRevisionRootPage);
    final PageContainer resolved = log.get(reference);

    // NEVER cache a FROZEN container here (#1077): this read-path helper runs between an async
    // epoch rotation and the first write to the page. Caching the frozen container would let
    // prepareRecordPageViaKeyedTrie's cache-hit fast path hand it to a WRITE without the
    // deep-copy CoW — mutating a frozen page the background thread is concurrently serializing.
    // Frozen results are returned (their content is current until the CoW) but resolved fresh on
    // each call; the container is cached once the write path has CoW'd it into the current TIL.
    if (resolved != null && !log.isFrozen(reference)) {
      pageContainerCache.put(new IndexLogKey(indexType, recordPageKey, indexNumber, revision), resolved);
    }
    return resolved;
  }

  @Nullable
  private PageContainer getMostRecentPageContainer(IndexType indexType, long recordPageKey,
      int indexNumber, int revisionNumber) {
    if (indexType == IndexType.PATH_SUMMARY) {
      return mostRecentPathSummaryPageContainer != null && mostRecentPathSummaryPageContainer.indexType == indexType
          && mostRecentPathSummaryPageContainer.indexNumber == indexNumber
          && mostRecentPathSummaryPageContainer.recordPageKey == recordPageKey
          && mostRecentPathSummaryPageContainer.revisionNumber == revisionNumber
              ? mostRecentPathSummaryPageContainer.pageContainer
              : null;
    }

    final IndexLogKeyToPageContainer[] byType = mostRecentByIndexType;
    if (byType != null) {
      final IndexLogKeyToPageContainer slot = byType[indexType.ordinal()];
      if (slot != null && slot.recordPageKey == recordPageKey && slot.indexNumber == indexNumber
          && slot.revisionNumber == revisionNumber && slot.indexType == indexType
          && slot.pageContainer != null) {
        return slot.pageContainer;
      }
    }

    var pageContainer = mostRecentPageContainer != null && mostRecentPageContainer.indexType == indexType
        && mostRecentPageContainer.recordPageKey == recordPageKey && mostRecentPageContainer.indexNumber == indexNumber
        && mostRecentPageContainer.revisionNumber == revisionNumber
            ? mostRecentPageContainer.pageContainer
            : null;
    if (pageContainer == null) {
      pageContainer = secondMostRecentPageContainer != null && secondMostRecentPageContainer.indexType == indexType
          && secondMostRecentPageContainer.recordPageKey == recordPageKey
          && secondMostRecentPageContainer.indexNumber == indexNumber
          && secondMostRecentPageContainer.revisionNumber == revisionNumber
              ? secondMostRecentPageContainer.pageContainer
              : null;
    }
    return pageContainer;
  }

  /**
   * Prepare record page.
   *
   * @param recordPageKey the key of the record page
   * @param indexNumber the index number if it's a record-page of an index, {@code -1}, else
   * @param indexType the index type
   * @return {@link PageContainer} instance
   * @throws SirixIOException if an I/O error occurs
   */
  private PageContainer prepareRecordPage(final long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    assert indexType != null;
    // Traditional KEYED_TRIE path (bit-decomposed).
    // HOT secondary indexes use dedicated HOT*IndexWriter/Reader implementations.
    return prepareRecordPageViaKeyedTrie(recordPageKey, indexNumber, indexType);
  }

  /**
   * Prepare record page using traditional bit-decomposed KEYED_TRIE.
   */
  private PageContainer prepareRecordPageViaKeyedTrie(final long recordPageKey, final int indexNumber,
      final IndexType indexType) {

    PageContainer mostRecentPageContainer1 =
        getMostRecentPageContainer(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision());

    if (mostRecentPageContainer1 != null && !mostRecentPageContainer1.getModified().isClosed()
        && !mostRecentPageContainer1.getComplete().isClosed()) {
      return mostRecentPageContainer1;
    }

    final Function<IndexLogKey, PageContainer> fetchPageContainer = _ -> {
      final PageReference pageReference = storageEngineReader.getPageReference(newRevisionRootPage, indexType, indexNumber);

      // Get the reference to the unordered key/value page storing the records.
      final PageReference reference = keyedTrieWriter.prepareLeafOfTree(this, log, getUberPage().getPageCountExp(indexType),
          pageReference, recordPageKey, indexNumber, indexType, newRevisionRootPage);

      var pageContainer = log.get(reference);

      if (pageContainer != null) {
        // CoW: if page is in frozen snapshot, deep-copy to active TIL
        if (log.isFrozen(reference)) {
          pageContainer = deepCopyFrozenContainer(pageContainer);
          log.put(reference, pageContainer);
          // The reader's most-recently-read cache may still hold the FROZEN instance, which
          // stays open for the background flush — so the guard-based invalidation that covers
          // the synchronous CoW path never fires. Without this, every read for the rest of the
          // epoch returns the frozen (stale) page while writes go into the copy (#1077).
          storageEngineReader.invalidateMostRecentlyReadRecordPage(indexType, indexNumber);
        }
        return pageContainer;
      }

      if (reference.getKey() == Constants.NULL_ID_LONG) {
        // Direct allocation (no pool)
        final MemorySegmentAllocator allocator = Allocators.getInstance();

        final KeyValueLeafPage completePage = new KeyValueLeafPage(recordPageKey, indexType,
            getResourceSession().getResourceConfig(), storageEngineReader.getRevisionNumber(), allocator.allocate(SIXTYFOUR_KB),
            getResourceSession().getResourceConfig().areDeweyIDsStored
                ? allocator.allocate(SIXTYFOUR_KB)
                : null,
            false // Memory from allocator - release on close()
        );

        final KeyValueLeafPage modifyPage = new KeyValueLeafPage(recordPageKey, indexType,
            getResourceSession().getResourceConfig(), storageEngineReader.getRevisionNumber(), allocator.allocate(SIXTYFOUR_KB),
            getResourceSession().getResourceConfig().areDeweyIDsStored
                ? allocator.allocate(SIXTYFOUR_KB)
                : null,
            false // Memory from allocator - release on close()
        );

        pageContainer = PageContainer.getInstance(completePage, modifyPage);
        appendLogRecord(reference, pageContainer);
        return pageContainer;
      } else {
        pageContainer = dereferenceRecordPageForModification(reference);
        return pageContainer;
      }
    };

    final int revision = newRevisionRootPage.getRevision();
    lookupKey.setIndexType(indexType).setRecordPageKey(recordPageKey)
        .setIndexNumber(indexNumber).setRevisionNumber(revision);
    var currPageContainer = pageContainerCache.get(lookupKey);
    if (currPageContainer == null) {
      currPageContainer = pageContainerCache.computeIfAbsent(
          new IndexLogKey(indexType, recordPageKey, indexNumber, revision), fetchPageContainer);
    }

    if (indexType == IndexType.PATH_SUMMARY) {
      mostRecentPathSummaryPageContainer.set(indexType, recordPageKey, indexNumber,
          newRevisionRootPage.getRevision(), currPageContainer);
    } else {
      // Copy mostRecent into secondMostRecent BEFORE mutating mostRecent
      secondMostRecentPageContainer.copyFrom(mostRecentPageContainer);
      mostRecentPageContainer.set(indexType, recordPageKey, indexNumber,
          newRevisionRootPage.getRevision(), currPageContainer);
      final IndexLogKeyToPageContainer[] byType = mostRecentByIndexType;
      if (byType != null) {
        final int ordinal = indexType.ordinal();
        IndexLogKeyToPageContainer byTypeUpd = byType[ordinal];
        if (byTypeUpd == null) {
          byTypeUpd = new IndexLogKeyToPageContainer(indexType, -1, -1, -1, null);
          byType[ordinal] = byTypeUpd;
        }
        byTypeUpd.set(indexType, recordPageKey, indexNumber,
            newRevisionRootPage.getRevision(), currPageContainer);
      }
    }

    return currPageContainer;
  }

  /**
   * Dereference record page reference.
   *
   * @param reference reference to leaf, that is the record page
   * @return dereferenced page
   */
  @Override
  public PageContainer dereferenceRecordPageForModification(final PageReference reference) {
    final VersioningType versioningType = storageEngineReader.resourceSession.getResourceConfig().versioningType;
    final int mileStoneRevision = storageEngineReader.resourceSession.getResourceConfig().maxNumberOfRevisionsToRestore;

    // FULL versioning: Release any reader guard before loading for modification
    // This prevents double-guarding when the page is already in RecordPageCache from a read
    if (versioningType == VersioningType.FULL) {
      storageEngineReader.closeCurrentPageGuard();
    }

    final var result = storageEngineReader.getPageFragments(reference);

    // All fragments are guarded by getPageFragments() to prevent eviction during combining
    try {
      return versioningType.combineRecordPagesForModification(result.pages(), mileStoneRevision, this, reference, log);
    } finally {
      // Release the getPageFragments() guards. The writer's own guard accounting is balanced by
      // this unconditional loop (exactly one release per fragment getPageFragments() acquired), so
      // a non-zero residual count is never a writer leak — it is a CONCURRENT READER holding a
      // guard on the fragment. That is legitimate under EVERY versioning type, not just FULL: the
      // reader read the old fragment while the writer copy-on-writes a fresh modify page, and the
      // reader releases its guard when its (try-with-resources) read transaction closes; the
      // ClockSweeper only evicts at guardCount==0, so the page is never freed under it. The former
      // `assert getGuardCount()==0` for non-FULL was a false invariant — guardCount is a single
      // shared AtomicInteger with no reader/writer ownership, so it cannot exclude a racing
      // reader's guard. It spuriously failed the concurrent-reader soak once read throughput was
      // high enough to reliably overlap a reader's guard with a writer remove (see
      // HOTVersionedLeafStressTest.soakWithConcurrentReaders with hot.soak.index=name).
      for (var page : result.pages()) {
        ((KeyValueLeafPage) page).releaseGuard();
      }
      // Note: Fragments remain in cache for potential reuse. ClockSweeper will evict them when
      // appropriate.
    }
  }

  @Override
  public RevisionRootPage getActualRevisionRootPage() {
    storageEngineReader.assertNotClosed();
    return newRevisionRootPage;
  }

  @Override
  public @Nullable HOTLeafPage getHOTLeafPage(IndexType indexType, int indexNumber) {
    storageEngineReader.assertNotClosed();

    // CRITICAL: Use newRevisionRootPage (not the delegate's rootPage) because
    // HOT pages are stored against the new revision's PathPage/CASPage/NamePage references.
    final RevisionRootPage actualRootPage = newRevisionRootPage;

    // Get the root reference for the index from the NEW revision root page
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
        final var projPage = getProjectionIndexPage(actualRootPage);
        if (projPage == null || indexNumber >= projPage.getReferencesCount()) {
          yield null;
        }
        yield projPage.getOrCreateReference(indexNumber);
      }
      case VALIDTIME -> {
        final var vtPage = getValidTimeIndexPage(actualRootPage);
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

    // Check transaction log for uncommitted pages (this is the key for write transactions!)
    final PageContainer container = log.get(rootRef);
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

    // Check if page is swizzled (directly on reference)
    if (rootRef.getPage() instanceof HOTLeafPage hotLeaf) {
      return hotLeaf;
    }

    // For uncommitted pages with no storage key, we're done
    if (rootRef.getKey() < 0 && rootRef.getLogKey() < 0) {
      return null;
    }

    // Try buffer cache or load from storage (for previously committed data)
    return storageEngineReader.getHOTLeafPage(indexType, indexNumber);
  }

  @Override
  public @Nullable OverflowPage readSideOverflowPage(final PageReference reference) {
    // In-memory (uncommitted, this-transaction) segment pages sit directly on the reference;
    // committed ones resolve through the shared reader by disk offset key.
    if (reference.getPage() instanceof OverflowPage segmentPage) {
      return segmentPage;
    }
    return storageEngineReader.readSideOverflowPage(reference);
  }

  @Override
  public io.sirix.page.interfaces.@Nullable Page loadHOTPage(PageReference reference) {
    storageEngineReader.assertNotClosed();

    if (reference == null) {
      return null;
    }

    // Check transaction log first for uncommitted pages
    final PageContainer container = log.get(reference);
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

    // Delegate to the reader
    return storageEngineReader.loadHOTPage(reference);
  }

  @Override
  protected StorageEngineReader delegate() {
    return storageEngineReader;
  }

  @Override
  public StorageEngineReader getStorageEngineReader() {
    return storageEngineReader;
  }

  @Override
  public KeyValueLeafPage getModifiedPageForRead(final long recordPageKey,
      final IndexType indexType, final int index) {
    final PageContainer pc = getPageContainer(recordPageKey, index, indexType);
    if (pc != null) {
      final var modified = pc.getModified();
      if (modified instanceof KeyValueLeafPage kvl && !kvl.isClosed()) {
        return kvl;
      }
    }
    return null;
  }

  @Override
  public StorageEngineWriter appendLogRecord(final PageReference reference,
      final PageContainer pageContainer) {
    requireNonNull(pageContainer);
    log.put(reference, pageContainer);
    return this;
  }

  @Override
  public PageContainer getLogRecord(final PageReference reference) {
    requireNonNull(reference);
    return log.get(reference);
  }

  @Override
  public StorageEngineWriter truncateTo(final int revision) {
    // Rollback semantics: any buffered (uncommitted) page writes refer to the world being
    // truncated away — discard them before touching the files.
    bufferBytes.clear();

    // An explicit rollback truncates AWAY the revision both uber beacons advertise — unlike
    // crash recovery, where one slot still matches the target and the io-layer repairs the
    // other. The serialized uber page carries only the revision count, so the rolled-back
    // page is fully reconstructible here and written through the regular dual-beacon
    // protocol. ORDER MATTERS: the beacons must be downgraded durably BEFORE the files are
    // truncated — truncating first opened a crash window in which a (still checksum-valid)
    // beacon advertised the truncated-away revision, so recovery dereferenced truncated
    // offsets and the resource was unopenable ("Truncated revisions record"). With
    // beacons-first, a crash at any instant leaves the resource at either the original or
    // the target revision: the target's revision record and pages lie BELOW the truncation
    // point, so they satisfy pre-written beacons even when the truncates themselves are lost.
    final var resourceSession = getResourceSession();
    final var rolledBackUberPage = new UberPage(revision + 1);
    storagePageReaderWriter.writeUberPageReference(resourceSession.getResourceConfig(), new PageReference(),
        rolledBackUberPage, Bytes.elasticOffHeapByteBuffer());
    ((io.sirix.access.trx.node.InternalResourceSession<?, ?>) resourceSession).setLastCommittedUberPage(
        rolledBackUberPage);

    storagePageReaderWriter.truncateTo(this, revision);

    // The truncated range's offsets are reused by the next commit — drop this database's
    // cached pages so nothing pre-truncation can be served.
    io.sirix.access.Databases.clearCachesForDatabase(resourceSession.getResourceConfig().getDatabaseId());
    return this;
  }

  @Override
  public int getTrxId() {
    return storageEngineReader.getTrxId();
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    return storageEngineReader.getCommitCredentials();
  }

  // The deprecated finalize() override was replaced by the Cleaner-driven LeakDetectorState
  // registered in the constructor. Cleaner is the post-Java-9 sanctioned way to run code on
  // phantom-reachability without the GC-thread / object-resurrection downsides of finalize.

  /**
   * Acquire a guard on the page containing the current node. This is needed when holding a reference
   * to a node across cursor movements.
   *
   * @return a PageGuard that must be closed when done with the node
   */
  public PageGuard acquireGuardForCurrentNode() {
    // The current node is in the storageEngineReader's currentPageGuard
    // We need to return a new guard on the same page
    // Get the page containing the current node from storageEngineReader
    final var currentPage = ((NodeStorageEngineReader) storageEngineReader).getCurrentPage();
    if (currentPage == null) {
      throw new IllegalStateException("No current page - cannot acquire guard");
    }
    return new PageGuard(currentPage);
  }

}
