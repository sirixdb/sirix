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

import io.sirix.HftBoundaryTelemetry;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.node.CommitCredentials;
import io.sirix.access.trx.node.IndexController;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.IndexLogKey;
import io.sirix.cache.PageContainer;
import io.sirix.cache.PageGuard;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.exception.SirixIOException;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.io.SerializationBufferPool;
import io.sirix.io.SharedArenas;
import io.sirix.io.filechannel.FileChannelWriter;
import io.sirix.node.PooledBytesOut;
import io.sirix.index.IndexType;
import io.sirix.io.Writer;
import io.sirix.node.DeletedNode;
import io.sirix.node.NodeKind;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.ValueDictionaryEntryNode;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import io.sirix.page.CASPage;
import io.sirix.page.DeweyIDPage;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.IndirectPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.pax.GlobalStringDictionaries;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageLayout;
import io.sirix.access.DatabaseType;
import io.sirix.page.NamePage;
import io.sirix.settings.StringCompressionType;
import io.sirix.utils.FSSTCompressor;
import io.sirix.page.PageKind;
import io.sirix.page.PageReference;
import io.sirix.page.PageSectionDiag;
import io.sirix.page.PathPage;
import io.sirix.page.PathSummaryPage;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.ValidTimeIndexPage;
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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongFunction;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.sirix.utils.Preconditions.checkArgument;
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
   * Maximum generation-scoped TIL entries frozen into one async-flush epoch.
   *
   * <p>
   * Snapshot pinning can remove structural entries before serialization, so this is a conservative
   * upper bound on attempted KVL pages and matches one serializer window.
   * </p>
   */
  static final int MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT = Integer.getInteger("sirix.asyncFlush.maxLogEntries", 16);

  /**
   * Buffered output for page writes.
   *
   * <p>
   * Use 2x FLUSH_SIZE so single large page fragments do not force grow/copy on every write before the
   * subsequent flush threshold check.
   */
  private BytesOut<?> bufferBytes = Bytes.borrowElasticOffHeapByteBuffer(Writer.FLUSH_SIZE * 2);

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
   * The resolver DOCUMENT record pages encode their string values against, or {@code null}.
   *
   * <p>
   * Installed once at load start and read on the page-creation path, because a page's string region
   * is built at serialization from whatever resolver the page is carrying by then. Volatile because
   * the installer runs on the load's thread and pages are created on the importer's workers.
   * </p>
   */
  private volatile @Nullable GlobalStringDictionaries documentStringDictionaries;

  /** Per-page resolver factory; when set it supersedes {@link #documentStringDictionaries}. */
  private volatile @Nullable LongFunction<GlobalStringDictionaries> documentStringDictionaryFactory;

  /**
   * The {@link NamePage} owned by {@link #newRevisionRootPage}, resolved once per active TIL epoch.
   *
   * <p>
   * Name creation used to walk the revision-root reference and re-publish the same page on every
   * named node. A writer's top-level structural pages are transaction-private and survive a full
   * async epoch as pinned pages, but the cache is still cleared at every full rotation so it never
   * relies on that implementation detail. Pages reached through any other revision-root object are
   * deliberately never cached here.
   * </p>
   */
  private @Nullable NamePage currentNamePage;

  /**
   * Determines if transaction is closed.
   */
  private volatile boolean isClosed;

  /**
   * First failure after an in-memory structural mutation crossed its publication boundary. Unlike the
   * async-flush latch, this describes a writer-thread page-graph failure. It is terminal for this
   * writer; rollback replaces the writer instead of clearing the cause in place.
   */
  private volatile @Nullable Throwable transactionRollbackOnlyCause;

  /**
   * Shared Cleaner that runs the leak-detection callback on every NodeStorageEngineWriter once it
   * becomes phantom-reachable. Used as the post-Java-9 replacement for the deprecated
   * {@code finalize()} override.
   */
  private static final java.lang.ref.Cleaner LEAK_CLEANER = java.lang.ref.Cleaner.create();

  /**
   * State captured for leak diagnostics. Static class with no reference to the enclosing writer —
   * capturing {@code this} would make the writer strongly reachable through the Cleaner queue and
   * defeat the leak detection it implements.
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
      final int containerCount = log != null
          ? log.getList().size()
          : -1;
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

    IndexLogKeyToPageContainer(final IndexType indexType, final long recordPageKey, final int indexNumber,
        final int revisionNumber, final PageContainer pageContainer) {
      set(indexType, recordPageKey, indexNumber, revisionNumber, pageContainer);
    }

    void set(final IndexType indexType, final long recordPageKey, final int indexNumber, final int revisionNumber,
        final PageContainer pageContainer) {
      this.indexType = indexType;
      this.recordPageKey = recordPageKey;
      this.indexNumber = indexNumber;
      this.revisionNumber = revisionNumber;
      this.pageContainer = pageContainer;
    }

    void copyFrom(final IndexLogKeyToPageContainer other) {
      set(other.indexType, other.recordPageKey, other.indexNumber, other.revisionNumber, other.pageContainer);
    }
  }

  private static final class ReadPageResolution {
    private @Nullable PageContainer pageContainer;
    private @Nullable PageReference durableReference;

    void clear() {
      pageContainer = null;
      durableReference = null;
    }

    void setPageContainer(final PageContainer pageContainer) {
      this.pageContainer = pageContainer;
      durableReference = null;
    }

    void setDurableReference(final PageReference durableReference) {
      pageContainer = null;
      this.durableReference = durableReference;
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
   * Most recent page container per {@link IndexType} (ordinal-indexed, lazily populated). The shared
   * {@link #mostRecentPageContainer}/{@link #secondMostRecentPageContainer} pair thrashes when a
   * commit interleaves streams of three or more index types (DOCUMENT + secondary indexes during
   * shredding): every switch to another type evicts, so lookups fall through to the access-ordered
   * {@link #pageContainerCache} probe (hashing plus LRU relink per hit). One slot per type keeps the
   * hot page of EVERY stream one comparison away. PATH_SUMMARY keeps its dedicated
   * {@link #mostRecentPathSummaryPageContainer} slot; its array entry stays unused.
   */
  private IndexLogKeyToPageContainer[] mostRecentByIndexType;

  private final LinkedHashMap<IndexLogKey, PageContainer> pageContainerCache;

  /**
   * Reusable lookup key for pageContainerCache to avoid allocating a new IndexLogKey on every cache
   * probe. MUST NOT be passed to computeIfAbsent as the stored key — the map retains a reference, and
   * subsequent mutations would corrupt it.
   */
  private final IndexLogKey lookupKey = new IndexLogKey(IndexType.DOCUMENT, -1, -1, -1);

  /**
   * Optional binder for write-path singletons. When set, prepareRecordForModification uses factory
   * singletons instead of allocating new nodes.
   */
  private WriteSingletonBinder writeSingletonBinder;

  /**
   * Exact recent document-record locations used only by structural insert linkage.
   *
   * <p>
   * Entries retain primitive TIL identities rather than pages. The log remains the authority and
   * resolves an identity to its newest modified page, including same-generation replacement and
   * dynamically pinned overflow pages.
   * </p>
   */
  private final DocumentRecordLocationCache documentRecordLocationCache = new DocumentRecordLocationCache();

  /** Fixed, allocation-free direct map. Package-private for deterministic collision tests. */
  static final class DocumentRecordLocationCache {
    static final int CAPACITY = 256;
    private static final int MASK = CAPACITY - 1;
    private static final long EMPTY_KEY = Long.MIN_VALUE;

    private final long[] nodeKeys = new long[CAPACITY];
    private final long[] transactionLogIdentities = new long[CAPACITY];

    DocumentRecordLocationCache() {
      Arrays.fill(nodeKeys, EMPTY_KEY);
    }

    long get(final long nodeKey) {
      final int index = ((int) nodeKey) & MASK;
      return nodeKeys[index] == nodeKey
          ? transactionLogIdentities[index]
          : PageContainer.NULL_TRANSACTION_LOG_IDENTITY;
    }

    void put(final long nodeKey, final long transactionLogIdentity) {
      assert nodeKey >= 0;
      assert transactionLogIdentity != PageContainer.NULL_TRANSACTION_LOG_IDENTITY;
      final int index = ((int) nodeKey) & MASK;
      nodeKeys[index] = nodeKey;
      transactionLogIdentities[index] = transactionLogIdentity;
    }

    void clear() {
      Arrays.fill(nodeKeys, EMPTY_KEY);
    }
  }

  // ==================== ASYNC AUTO-COMMIT STATE ====================

  /** Backpressure: at most one background snapshot flush in-flight. */
  private final Semaphore flushPermit = new Semaphore(1);

  /** True while a background snapshot flush is running. */
  private volatile boolean asyncFlushInFlight;

  /** True only while a submitted worker may still read frozen arrays or use the shared Writer. */
  private volatile boolean asyncFlushWorkerRunning;

  /** Last positive progress made by the sole append coordinator, for stall-aware fencing. */
  private volatile long asyncFlushProgressNanos;

  /** Once a worker stalls past the deadline, all later teardown fences fail immediately. */
  private volatile boolean asyncFlushTimedOut;

  private static final int ASYNC_TASK_IDLE = 0;
  private static final int ASYNC_TASK_ENQUEUED = 1;
  private static final int ASYNC_TASK_RUNNING = 2;
  private static final int ASYNC_TASK_COMPLETED = 3;
  private static final int ASYNC_TASK_CANCELLED = 4;

  /** CAS-owned lifecycle of the writer's one persistent append task. */
  private volatile int asyncSnapshotWriteTaskState = ASYNC_TASK_IDLE;

  private static final VarHandle ASYNC_SNAPSHOT_WRITE_TASK_STATE;

  static {
    try {
      ASYNC_SNAPSHOT_WRITE_TASK_STATE =
          MethodHandles.lookup().findVarHandle(NodeStorageEngineWriter.class, "asyncSnapshotWriteTaskState", int.class);
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /** One task identity per writer; the bounded append executor never needs an epoch task wrapper. */
  private final AsyncSnapshotWriteTask asyncSnapshotWriteTask = new AsyncSnapshotWriteTask();

  /**
   * The throwable that killed a background snapshot flush, or the one that stopped a flush from being
   * started. RETAINED, never consumed: it is the poison record, so every later request on this writer
   * can name the ORIGINAL fault instead of the latch that outlived it.
   */
  private volatile Throwable asyncFlushError;

  /** Terminal failure latch — once true, NEVER reset. Transaction is permanently failed. */
  private volatile boolean asyncTerminalFailure;

  /**
   * Native-payload budget for one immutable side-page batch. At most one frozen batch and one active
   * batch exist, each backed by one fixed reusable reservoir. A caller still owns the next encoded
   * heap array while backpressure fences the frozen batch, but staging copies it immediately and
   * replaces the pending page with a native view. The property is deliberately byte-based: a count
   * cap cannot bound projection segments, whose legal sizes span three orders of magnitude.
   */
  private static final long MAX_STAGED_SIDE_PAGE_BYTES =
      positiveLongProperty("sirix.asyncFlush.sidePageBytes", 64L * 1024 * 1024);

  /** Independent object-count bound for the smallest legal out-of-line projection segments. */
  private static final int MAX_STAGED_SIDE_PAGE_COUNT =
      positiveIntProperty("sirix.asyncFlush.sidePageCount", 128 * 1024);

  /** Expected page count for a 64 MiB batch of ClickBench projection segments. */
  private static final int INITIAL_SIDE_PAGE_BATCH_CAPACITY = 16 * 1024;

  /** Maximum live pinned slots examined by one foreground trie-spill epoch. */
  private static final int PINNED_TRIE_SPILL_SCAN_BUDGET =
      positiveIntProperty("sirix.asyncFlush.pinnedTrieSpillScanBudget", 1_024);

  /** Fixed number of exact trie-page tuples retained and serialized by one foreground epoch. */
  private static final int PINNED_TRIE_SPILL_BATCH_CAPACITY =
      positiveIntProperty("sirix.asyncFlush.pinnedTrieSpillBatchCapacity", 64);

  /** Reused bounded capture/results buffer; its object arrays never grow with the transaction. */
  private final TransactionIntentLog.PinnedSpillBatch pinnedTrieSpillBatch =
      new TransactionIntentLog.PinnedSpillBatch(PINNED_TRIE_SPILL_BATCH_CAPACITY);

  /** Scratch reference receives direct-write offsets without mutating a live tree reference. */
  private final PageReference pinnedTrieSpillShadowReference = new PageReference();

  /** Whether this writer has appended to a reclaimable tail since the last durable beacon. */
  private boolean hasUncommittedReclaimableWrites;

  private long firstUncommittedPageOffset = Long.MAX_VALUE;

  private final ReadPageResolution readPageResolution = new ReadPageResolution();

  private long cursorDurableReadOffset = Constants.NULL_ID_LONG;

  private @Nullable KeyValueLeafPage cursorDurableReadPage;

  private long secondCursorDurableReadOffset = Constants.NULL_ID_LONG;

  private @Nullable KeyValueLeafPage secondCursorDurableReadPage;

  /**
   * Owner of the two fixed native side-payload reservoirs. Lazily allocated and explicitly closed.
   */
  private @Nullable Arena sidePagePayloadArena;

  /** Foreground-owned side pages being collected for the next async rotation. Lazily allocated. */
  private @Nullable SidePageBatch activeSidePages;

  /** Frozen side pages owned by the current background append. */
  private @Nullable SidePageBatch snapshotSidePages;

  /** Whether the in-flight worker also owns a TransactionIntentLog snapshot. */
  private boolean asyncSnapshotIncludesLog;

  /**
   * Positive completion proof for the frozen epoch. Cleared before submission and set by the sole
   * append worker only after the shared buffer is flushed (and any KVL snapshot is marked complete).
   * Cleanup never infers success merely from an absent exception: an OOME in diagnostics must not
   * turn a partial append into published offsets.
   */
  private volatile boolean asyncSnapshotWriteComplete;

  /**
   * Allocation-free epoch telemetry, opt-in for ingestion profiling. The disabled branch is a static
   * final and disappears after compilation; enabled mode mutates primitive fields only on the
   * already-owning foreground/worker threads and emits one summary after the final fence.
   */
  private static final boolean HFT_TELEMETRY_ENABLED = Boolean.getBoolean("sirix.hft.telemetry");
  private static final AtomicLong HFT_GLOBAL_SUBMIT_WAIT_COUNT = new AtomicLong();
  private static final AtomicLong HFT_GLOBAL_SUBMIT_WAIT_NANOS = new AtomicLong();
  private static final AtomicLong HFT_GLOBAL_SUBMIT_WAIT_MAX_NANOS = new AtomicLong();
  private static final AtomicLong HFT_GLOBAL_CALLER_APPEND_RUNS = new AtomicLong();

  private long hftStagedSidePages;
  private long hftStagedSideBytes;
  private long hftPeakActiveSideBytes;
  private long hftCombinedEpochs;
  private long hftSideOnlyEpochs;
  private long hftSnapshotKvlPages;
  private long hftSnapshotKvlAttemptedPages;
  private long hftSnapshotKvlPromotedPages;

  /** KVL snapshot slots deferred one epoch for pending side-page carriers (never serialized). */
  private long hftSnapshotKvlDeferredPages;
  private long hftMaxSnapshotKvlAttemptedPages;
  private long hftPermitAcquires;
  private long hftPermitWaitNanos;
  private long hftMaxPermitWaitNanos;
  private long hftRotationPermitAcquires;
  private long hftRotationPermitWaitNanos;
  private long hftMaxRotationPermitWaitNanos;
  private long hftDrainPermitAcquires;
  private long hftDrainPermitWaitNanos;
  private long hftMaxDrainPermitWaitNanos;
  private long hftWorkerRuns;
  private long hftWorkerNanos;
  private long hftMaxWorkerNanos;
  private long hftSubmitWaitCount;
  private long hftSubmitWaitNanos;
  private long hftMaxSubmitWaitNanos;
  private long hftCallerThreadAppendRuns;
  private long hftStartFlushCount;
  private long hftStartFlushNanos;
  private long hftMaxStartFlushNanos;
  private long hftForegroundFlushCount;
  private long hftForegroundFlushNanos;
  private long hftMaxForegroundFlushNanos;
  private long hftFinalDrainCount;
  private long hftFinalDrainNanos;
  private long hftMaxFinalDrainNanos;

  /** Monotonic writer-local identity for non-empty async epochs. */
  private long hftEpochSequence;

  /** Foreground-published identity and submission instant of the sole active epoch. */
  private long hftActiveEpochId;
  private long hftActiveEpochSubmittedNanos;

  /**
   * Last worker result. Plain fields are safe: the worker writes them before releasing
   * {@link #flushPermit}, and the foreground reads them only after acquiring that permit. Phase
   * values are deliberately not additive: serializer workers overlap side-page and KVL append;
   * {@code SerializeJoinWait} measures only time the append coordinator actually stalled at joins.
   */
  private long hftCompletedEpochId;
  private long hftCompletedEpochQueueWaitNanos;
  private long hftCompletedEpochWorkerNanos;
  private long hftCompletedEpochSideNanos;
  private long hftCompletedEpochSerializeJoinWaitNanos;
  private long hftCompletedEpochKvlAppendNanos;
  private long hftCompletedEpochFinalFlushNanos;
  private long hftCompletedEpochDataGrowCount;
  private long hftCompletedEpochDataGrowBytes;
  private long hftCompletedEpochDataGrowNanos;
  private boolean hftCompletedEpochDataGrowExact;

  /**
   * Phase breakdown summed over every worker run.
   *
   * <p>
   * The per-epoch tuples above retain only the slowest and the most-blocking epoch, and on a bulk
   * import both are the very first one — the epoch that pays JIT warm-up. Attributing the pipeline
   * from those alone therefore describes warm-up rather than steady state. These totals answer what
   * the flush actually spends its time on across the whole transaction, which is what decides whether
   * widening the pipeline can pay: only the append phase is strictly serialized per writer, so the
   * overlap a deeper pipeline can win is bounded by it.
   * </p>
   */
  private long hftSerializeJoinWaitNanosTotal;
  private long hftKvlAppendNanosTotal;
  private long hftSideNanosTotal;
  private long hftFinalFlushNanosTotal;

  /** Phase breakdown retained for the slowest worker. */
  private long hftMaxWorkerEpochId;
  private long hftMaxWorkerEpochQueueWaitNanos;
  private long hftMaxWorkerEpochSideNanos;
  private long hftMaxWorkerEpochSerializeJoinWaitNanos;
  private long hftMaxWorkerEpochKvlAppendNanos;
  private long hftMaxWorkerEpochFinalFlushNanos;
  private long hftMaxWorkerEpochDataGrowCount;
  private long hftMaxWorkerEpochDataGrowBytes;
  private long hftMaxWorkerEpochDataGrowNanos;
  private boolean hftMaxWorkerEpochDataGrowExact;

  /** Phase breakdown retained for the epoch behind the largest foreground rotation wait. */
  private long hftMaxBlockedEpochId;
  private long hftMaxBlockedEpochForegroundWaitNanos;
  private long hftMaxBlockedEpochWorkerNanos;
  private long hftMaxBlockedEpochQueueWaitNanos;
  private long hftMaxBlockedEpochSideNanos;
  private long hftMaxBlockedEpochSerializeJoinWaitNanos;
  private long hftMaxBlockedEpochKvlAppendNanos;
  private long hftMaxBlockedEpochFinalFlushNanos;
  private long hftMaxBlockedEpochDataGrowCount;
  private long hftMaxBlockedEpochDataGrowBytes;
  private long hftMaxBlockedEpochDataGrowNanos;
  private boolean hftMaxBlockedEpochDataGrowExact;
  private boolean hftMaxBlockedEpochRotation;
  private int hftNativeReservoirCount;
  private long hftNativeReservoirBytes;
  private long hftKvlFrameCachePages;
  private long hftKvlFrameCacheBytes;
  private long hftKvlCacheFallbackPages;
  private long hftKvlCacheFallbackBytes;
  private long hftPinnedTrieSpillEpochs;
  private long hftPinnedTrieSpillPages;
  private int hftPinnedTrieSpillBatchMax;
  private int hftPinnedTrieLiveMax;
  private int hftPinnedTrieHighWater;
  private boolean hftTelemetryPrinted;

  /** Validate a positive long system property once at class initialization. */
  private static long positiveLongProperty(final String name, final long defaultValue) {
    final long value = Long.getLong(name, defaultValue);
    if (value <= 0L) {
      throw new IllegalArgumentException("-D" + name + " must be > 0, got " + value);
    }
    return value;
  }

  /** Validate a positive int system property once at class initialization. */
  private static int positiveIntProperty(final String name, final int defaultValue) {
    final int value = Integer.getInteger(name, defaultValue);
    if (value <= 0) {
      throw new IllegalArgumentException("-D" + name + " must be > 0, got " + value);
    }
    return value;
  }

  /**
   * Reusable primitive side-channel for immutable OverflowPage writes. The background thread never
   * mutates a real PageReference; it writes offsets/hashes here, then the foreground publishes them
   * only after the shared append buffer has been flushed.
   */
  private static final class SidePageBatch {
    private PageReference[] references;
    private long[] diskOffsets;
    private final MemorySegment payloadStorage;
    private final MemorySegment readOnlyPayloadStorage;
    private int size;
    private long payloadBytes;

    private SidePageBatch(final int initialCapacity, final MemorySegment payloadStorage) {
      references = new PageReference[initialCapacity];
      diskOffsets = new long[initialCapacity];
      this.payloadStorage = payloadStorage;
      readOnlyPayloadStorage = payloadStorage.asReadOnly();
    }

    /**
     * Copy a producer page into the fixed native reservoir without advancing visible batch state. Any
     * copy/view failure therefore leaves the live reference heap-backed and unstaged.
     */
    private OverflowPage copyToNative(final OverflowPage page) {
      final int payloadLength = page.dataLength();
      final long payloadOffset = payloadBytes;
      page.copyDataTo(payloadStorage, payloadOffset);
      return new OverflowPage(readOnlyPayloadStorage, payloadOffset, payloadLength);
    }

    private void addReserved(final PageReference reference, final int payloadLength) {
      if (size >= references.length) {
        throw new IllegalStateException("Immutable side-page batch slot was not reserved before publication");
      }
      references[size] = reference;
      diskOffsets[size] = Constants.NULL_ID_LONG;
      size++;
      payloadBytes += payloadLength;
    }

    private void ensureCapacity(final int required) {
      if (required <= references.length) {
        return;
      }
      final int doubled = references.length << 1;
      if (doubled <= 0) {
        throw new IllegalStateException("Too many immutable side pages in one async-flush batch");
      }
      final int newCapacity = Math.min(MAX_STAGED_SIDE_PAGE_COUNT, Math.max(required, doubled));
      references = Arrays.copyOf(references, newCapacity);
      diskOffsets = Arrays.copyOf(diskOffsets, newCapacity);
    }

    /** Validate every result before publishing any of them, avoiding a half-published batch. */
    private void publishCompletedWrites() {
      for (int i = 0; i < size; i++) {
        if (diskOffsets[i] == Constants.NULL_ID_LONG) {
          throw new SirixIOException(
              "Immutable side-page batch entry " + i + " has no disk offset — background write incomplete or failed");
        }
        if (references[i] == null || !references[i].hasPendingPageWrite()) {
          throw new SirixIOException(
              "Immutable side-page batch entry " + i + " lost its pending-write identity before publication");
        }
      }
      for (int i = 0; i < size; i++) {
        // HOT side-map wire records persist only the offset. Payload integrity is the owning
        // descriptor's XXH3, so retaining another checksum on every pinned reference would
        // recreate transaction-long metadata for no read-side benefit.
        references[i].completePendingPageWrite(diskOffsets[i]);
      }
      clear(false);
    }

    /** Drop all batch ownership; on abort, also release unresolved payload handles. */
    private void clear(final boolean cancelPendingWrites) {
      for (int i = 0; i < size; i++) {
        final PageReference reference = references[i];
        if (cancelPendingWrites && reference != null) {
          reference.cancelPendingPageWrite();
        }
        references[i] = null;
        diskOffsets[i] = Constants.NULL_ID_LONG;
      }
      size = 0;
      payloadBytes = 0L;
    }
  }

  /** Maximum interval with no append-coordinator progress before the writer is poisoned. */
  private static final long ASYNC_FLUSH_STALL_TIMEOUT_NANOS =
      TimeUnit.MILLISECONDS.toNanos(positiveLongProperty("sirix.asyncFlush.stallTimeoutMillis", 120_000L));

  /** Poll interval only while a foreground thread is already applying epoch backpressure. */
  private static final long ASYNC_FLUSH_PROGRESS_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(250L);

  /**
   * Test-only fault injection for asynchronous flush and its rollback teardown, {@code null} in
   * production. Invoked with the name of the site it guards, so a test can raise the I/O, allocator,
   * and page-close faults that are otherwise difficult to reach. Package-private on purpose: the
   * failure modes it exercises — a leaked permit, a lost cause, retained payloads, or a rollback that
   * never returns — are not reachable through the public API by any other means.
   */
  static volatile BiConsumer<NodeStorageEngineWriter, String> asyncFlushFaultHook;

  /** Raise the injected fault for {@code site}, if a test armed one. */
  private void injectAsyncFlushFault(final String site) {
    final BiConsumer<NodeStorageEngineWriter, String> hook = asyncFlushFaultHook;
    if (hook != null) {
      hook.accept(this, site);
    }
  }

  /**
   * Permits currently available on the flush semaphore, for tests that assert the acquire/release
   * balance. One means idle-and-balanced; zero means either a flush is genuinely in flight or a
   * permit has been leaked, which is the failure this writer must never reach.
   */
  int availableFlushPermits() {
    return flushPermit.availablePermits();
  }

  /**
   * Number of side-page references still owned by the active or frozen batch, for failure-path tests.
   */
  int stagedSidePageCount() {
    final int activeCount = activeSidePages == null
        ? 0
        : activeSidePages.size;
    final int snapshotCount = snapshotSidePages == null
        ? 0
        : snapshotSidePages.size;
    return activeCount + snapshotCount;
  }

  /**
   * Native payload bytes occupied in the active or frozen side-page batch, for failure-path tests.
   */
  long stagedSidePagePayloadBytes() {
    final long activeBytes = activeSidePages == null
        ? 0L
        : activeSidePages.payloadBytes;
    final long snapshotBytes = snapshotSidePages == null
        ? 0L
        : snapshotSidePages.payloadBytes;
    return activeBytes + snapshotBytes;
  }

  /**
   * Whether the exact batch visible to a trie-spill fault hook contains a HOT leaf.
   *
   * <p>
   * Package-private test observability only. The batch is valid during the before/after publish hook
   * and cleared immediately when the foreground epoch leaves the spill method.
   * </p>
   */
  boolean currentPinnedTrieSpillBatchContainsHotLeaf() {
    for (int i = 0; i < pinnedTrieSpillBatch.size(); i++) {
      if (pinnedTrieSpillBatch.pageAt(i).getClass() == HOTLeafPage.class) {
        return true;
      }
    }
    return false;
  }

  /** Emit diagnostic counters only after all worker-owned state has been fenced. */
  private void printHftTelemetry() {
    if (!HFT_TELEMETRY_ENABLED || hftTelemetryPrinted || (hftStagedSidePages == 0L && hftCombinedEpochs == 0L
        && hftSideOnlyEpochs == 0L && hftPinnedTrieSpillEpochs == 0L)) {
      return;
    }
    hftTelemetryPrinted = true;
    System.out.printf(
        "# HFT_ASYNC_FLUSH combinedEpochs=%d sideOnlyEpochs=%d kvlPages=%d "
            + "kvlAttemptedPages=%d kvlPromotedPages=%d kvlDeferredPages=%d kvlAttemptedPagesMax=%d "
            + "sidePages=%d sideBytes=%d peakActiveSideBytes=%d permitAcquires=%d "
            + "permitWaitTotalNs=%d permitWaitMaxNs=%d rotationPermitAcquires=%d "
            + "rotationPermitWaitTotalNs=%d rotationPermitWaitMaxNs=%d drainPermitAcquires=%d "
            + "drainPermitWaitTotalNs=%d drainPermitWaitMaxNs=%d workerRuns=%d workerTotalNs=%d workerMaxNs=%d "
            + "submitWaitCount=%d submitWaitTotalNs=%d submitWaitMaxNs=%d callerThreadAppendRuns=%d "
            + "startFlushCount=%d startFlushTotalNs=%d startFlushMaxNs=%d "
            + "foregroundFlushCount=%d foregroundFlushTotalNs=%d foregroundFlushMaxNs=%d "
            + "finalDrainCount=%d finalDrainTotalNs=%d finalDrainMaxNs=%d "
            + "nativeReservoirCount=%d nativeReservoirBytes=%d kvlFrameCachePages=%d "
            + "kvlFrameCacheBytes=%d kvlCacheFallbackPages=%d kvlCacheFallbackBytes=%d "
            + "pinnedTrieSpillEpochs=%d pinnedTrieSpillPages=%d pinnedTrieSpillBatchMax=%d "
            + "pinnedTrieLiveMax=%d pinnedTrieHighWater=%d "
            + "serializeJoinWaitTotalNs=%d kvlAppendTotalNs=%d sideTotalNs=%d finalFlushTotalNs=%d%n",
        hftCombinedEpochs, hftSideOnlyEpochs, hftSnapshotKvlPages, hftSnapshotKvlAttemptedPages,
        hftSnapshotKvlPromotedPages, hftSnapshotKvlDeferredPages, hftMaxSnapshotKvlAttemptedPages, hftStagedSidePages,
        hftStagedSideBytes,
        hftPeakActiveSideBytes, hftPermitAcquires, hftPermitWaitNanos, hftMaxPermitWaitNanos, hftRotationPermitAcquires,
        hftRotationPermitWaitNanos, hftMaxRotationPermitWaitNanos, hftDrainPermitAcquires, hftDrainPermitWaitNanos,
        hftMaxDrainPermitWaitNanos, hftWorkerRuns, hftWorkerNanos, hftMaxWorkerNanos, hftSubmitWaitCount,
        hftSubmitWaitNanos, hftMaxSubmitWaitNanos, hftCallerThreadAppendRuns, hftStartFlushCount, hftStartFlushNanos,
        hftMaxStartFlushNanos, hftForegroundFlushCount, hftForegroundFlushNanos, hftMaxForegroundFlushNanos,
        hftFinalDrainCount, hftFinalDrainNanos, hftMaxFinalDrainNanos, hftNativeReservoirCount, hftNativeReservoirBytes,
        hftKvlFrameCachePages, hftKvlFrameCacheBytes, hftKvlCacheFallbackPages, hftKvlCacheFallbackBytes,
        hftPinnedTrieSpillEpochs, hftPinnedTrieSpillPages, hftPinnedTrieSpillBatchMax, hftPinnedTrieLiveMax,
        hftPinnedTrieHighWater, hftSerializeJoinWaitNanosTotal, hftKvlAppendNanosTotal, hftSideNanosTotal,
        hftFinalFlushNanosTotal);
    if (hftMaxWorkerEpochId != 0L) {
      System.out.printf(
          "# HFT_ASYNC_MAX_WORKER epoch=%d queueWaitNs=%d workerNs=%d sideNs=%d "
              + "serializeJoinWaitNs=%d kvlAppendNs=%d finalFlushNs=%d dataGrowCount=%d "
              + "dataGrowBytes=%d dataGrowNs=%d dataGrowExact=%b%n",
          hftMaxWorkerEpochId, hftMaxWorkerEpochQueueWaitNanos, hftMaxWorkerNanos, hftMaxWorkerEpochSideNanos,
          hftMaxWorkerEpochSerializeJoinWaitNanos, hftMaxWorkerEpochKvlAppendNanos, hftMaxWorkerEpochFinalFlushNanos,
          hftMaxWorkerEpochDataGrowCount, hftMaxWorkerEpochDataGrowBytes, hftMaxWorkerEpochDataGrowNanos,
          hftMaxWorkerEpochDataGrowExact);
    }
    if (hftMaxBlockedEpochId != 0L) {
      System.out.printf(
          "# HFT_ASYNC_MAX_BLOCKED epoch=%d waitKind=%s foregroundWaitNs=%d queueWaitNs=%d "
              + "workerNs=%d sideNs=%d serializeJoinWaitNs=%d kvlAppendNs=%d finalFlushNs=%d "
              + "dataGrowCount=%d dataGrowBytes=%d dataGrowNs=%d dataGrowExact=%b%n",
          hftMaxBlockedEpochId, hftMaxBlockedEpochRotation
              ? "rotation"
              : "drain",
          hftMaxBlockedEpochForegroundWaitNanos, hftMaxBlockedEpochQueueWaitNanos, hftMaxBlockedEpochWorkerNanos,
          hftMaxBlockedEpochSideNanos, hftMaxBlockedEpochSerializeJoinWaitNanos, hftMaxBlockedEpochKvlAppendNanos,
          hftMaxBlockedEpochFinalFlushNanos, hftMaxBlockedEpochDataGrowCount, hftMaxBlockedEpochDataGrowBytes,
          hftMaxBlockedEpochDataGrowNanos, hftMaxBlockedEpochDataGrowExact);
    }
  }

  /** Capture allocation-free pinned-region evidence at a full TIL epoch boundary. */
  private void recordPinnedTrieFullEpochState() {
    final int live = log.pinnedSize();
    final int highWater = log.pinnedHighWater();
    hftPinnedTrieLiveMax = Math.max(hftPinnedTrieLiveMax, live);
    hftPinnedTrieHighWater = Math.max(hftPinnedTrieHighWater, highWater);
  }

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
      final NodeStorageEngineReader storageEngineReader, final IndexController<?, ?> indexController,
      final int representRevision, final boolean isBoundToNodeTrx) {
    this.keyedTrieWriter = new KeyedTrieWriter();
    storagePageReaderWriter = requireNonNull(writer);
    this.log = requireNonNull(log);
    newRevisionRootPage = requireNonNull(revisionRootPage);
    this.storageEngineReader = requireNonNull(storageEngineReader);
    this.indexController = requireNonNull(indexController);
    checkArgument(representRevision >= 0, "The represented revision must be >= 0.");
    this.representRevision = representRevision;
    this.isBoundToNodeTrx = isBoundToNodeTrx;
    pinnedTrieSpillShadowReference.setDatabaseId(storageEngineReader.getDatabaseId())
                                  .setResourceId(storageEngineReader.getResourceId());
    // Immutable per-resource configuration, resolved once — the insert hot path only branches
    // on a final field.
    this.insertFsstEnabled =
        storageEngineReader.getResourceSession()
                           .getResourceConfig().stringCompressionType == StringCompressionType.FSST;
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
    this.leakDetectorState = new LeakDetectorState(storageEngineReader.getTrxId(), System.identityHashCode(this),
        System.identityHashCode(log), log);
    LEAK_CLEANER.register(this, leakDetectorState);
  }

  @Override
  public void setWriteSingletonBinder(final WriteSingletonBinder binder) {
    this.writeSingletonBinder = binder;
  }

  @Override
  public synchronized void markTransactionRollbackOnly(final Throwable cause) {
    requireNonNull(cause);
    if (transactionRollbackOnlyCause == null) {
      transactionRollbackOnlyCause = cause;
    }
  }

  @Override
  public void assertTransactionWritable() {
    final Throwable cause = transactionRollbackOnlyCause;
    if (cause != null) {
      throw new SirixIOException(
          "Page transaction is rollback-only after a published structural mutation failed; rollback is required",
          cause);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <P extends Page> P prepareSecondaryIndexPage(final IndexType indexType) {
    storageEngineReader.assertNotClosed();
    assertTransactionWritable();
    requireNonNull(indexType);

    final PageReference reference = secondaryIndexPageReference(indexType);
    final PageContainer existingContainer = log.get(reference);
    if (existingContainer != null) {
      final Page modified = existingContainer.getModified();
      requireSecondaryIndexPageType(indexType, modified, "transaction-intent-log modified page");
      if (indexType == IndexType.NAME) {
        currentNamePage = (NamePage) modified;
      }
      return (P) modified;
    }

    // Resolve through the reader, never through this writer's typed getters: those getters route
    // current-revision access back through this method to make accidental historical mutation
    // impossible. The complete detached copy is constructed before the reference or TIL changes.
    final Page historicalPage = loadSecondaryIndexPage(indexType);
    requireSecondaryIndexPageType(indexType, historicalPage, "persisted page");
    final Page privateCopy = copySecondaryIndexPage(indexType, historicalPage);
    if (privateCopy == historicalPage) {
      final IllegalStateException failure = new IllegalStateException(
          "Secondary-index CoW returned the historical " + indexType + " page instance");
      markTransactionRollbackOnly(failure);
      throw failure;
    }

    final PageContainer privateContainer = PageContainer.getInstance(privateCopy, privateCopy);
    try {
      // TransactionIntentLog.put mutates the reference before installing the slot. Any failure from
      // this point is therefore ambiguous and permanently poisons the writer.
      log.put(reference, privateContainer);
      final PageContainer published = log.get(reference);
      if (published != privateContainer) {
        throw new IllegalStateException(
            "Secondary-index CoW publication did not retain the exact " + indexType + " container");
      }
    } catch (final RuntimeException | Error failure) {
      markTransactionRollbackOnly(failure);
      throw failure;
    }

    if (indexType == IndexType.NAME) {
      currentNamePage = (NamePage) privateCopy;
    }
    return (P) privateCopy;
  }

  private PageReference secondaryIndexPageReference(final IndexType indexType) {
    return switch (indexType) {
      case PATH -> newRevisionRootPage.getPathPageReference();
      case CAS -> newRevisionRootPage.getCASPageReference();
      case NAME -> newRevisionRootPage.getNamePageReference();
      case PROJECTION -> newRevisionRootPage.getProjectionIndexPageReference();
      case VALIDTIME -> newRevisionRootPage.getValidTimeIndexPageReference();
      default -> throw new IllegalArgumentException("Not a secondary HOT index type: " + indexType);
    };
  }

  private Page loadSecondaryIndexPage(final IndexType indexType) {
    return switch (indexType) {
      case PATH -> storageEngineReader.getPathPage(newRevisionRootPage);
      case CAS -> storageEngineReader.getCASPage(newRevisionRootPage);
      case NAME -> storageEngineReader.getNamePage(newRevisionRootPage);
      case PROJECTION -> storageEngineReader.getProjectionIndexPage(newRevisionRootPage);
      case VALIDTIME -> storageEngineReader.getValidTimeIndexPage(newRevisionRootPage);
      default -> throw new IllegalArgumentException("Not a secondary HOT index type: " + indexType);
    };
  }

  private static Page copySecondaryIndexPage(final IndexType indexType, final Page historicalPage) {
    return switch (indexType) {
      case PATH -> new PathPage((PathPage) historicalPage);
      case CAS -> new CASPage((CASPage) historicalPage);
      case NAME -> NamePage.copyForWrite((NamePage) historicalPage);
      case PROJECTION -> new ProjectionIndexPage((ProjectionIndexPage) historicalPage);
      case VALIDTIME -> new ValidTimeIndexPage((ValidTimeIndexPage) historicalPage);
      default -> throw new IllegalArgumentException("Not a secondary HOT index type: " + indexType);
    };
  }

  private void requireSecondaryIndexPageType(final IndexType indexType, final Page page,
      final String source) {
    final boolean matches = switch (indexType) {
      case PATH -> page instanceof PathPage;
      case CAS -> page instanceof CASPage;
      case NAME -> page instanceof NamePage;
      case PROJECTION -> page instanceof ProjectionIndexPage;
      case VALIDTIME -> page instanceof ValidTimeIndexPage;
      default -> false;
    };
    if (!matches) {
      final IllegalStateException failure = new IllegalStateException(source + " for " + indexType
          + " has unexpected type " + (page == null ? "null" : page.getClass().getName()));
      markTransactionRollbackOnly(failure);
      throw failure;
    }
  }

  @Override
  public BytesOut<?> newBufferedBytesInstance() {
    // The previous buffer is finished the moment a new one is handed out, so recycle it instead of
    // dropping it: this ran once per COMMIT, allocating a fresh off-heap segment each time and
    // leaving the old one for the arena to reclaim eventually. recycleOrRelease keeps it only if it
    // is small enough, so a commit that grew the segment releases rather than pins it.
    Bytes.recycleOrRelease(bufferBytes);
    bufferBytes = Bytes.borrowElasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
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
  public DataRecord prepareRecordForModification(final long recordKey, final IndexType indexType, final int index) {
    storageEngineReader.assertNotClosed();
    checkArgument(recordKey >= 0, "recordKey must be >= 0!");
    requireNonNull(indexType);
    NodeStorageEngineReader.validateKeyedTrieRoute(storageEngineReader, indexType, index);

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
          && PageLayout.getDirNodeKindId(srcPage, recordOffset) > 0
          && !kvlComplete.isFusedOverflowDescriptor(recordOffset)) {
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
      final boolean slotPopulated = slottedPage != null && PageLayout.isSlotPopulated(slottedPage, offset);
      final var slotData = completePage.getSlot(offset);
      final int populatedCount = slottedPage != null
          ? PageLayout.getPopulatedCount(slottedPage)
          : -1;
      throw new SirixIOException("Cannot retrieve record from cache: (key: " + recordKey + ") (indexType: " + indexType
          + ") (index: " + index + ") (slotPopulated: " + slotPopulated + ") (populatedCount: " + populatedCount
          + ") (slotData: " + (slotData != null
              ? slotData.byteSize() + " bytes"
              : "null")
          + ") (completePage.pageKey: " + completePage.getPageKey() + ") (completePage.revision: "
          + completePage.getRevision() + ") (modifiedPage.pageKey: " + modifiedPage.getPageKey() + ")");
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
   * Fast-path variant for the DOCUMENT index type on the insert hot path. Skips assertNotClosed(),
   * argument validation, and the IndexType switch in pageKey().
   */
  @SuppressWarnings("unchecked")
  @Override
  public DataRecord prepareRecordForModificationDocument(final long recordKey) {
    final long recordPageKey = storageEngineReader.pageKeyDocument(recordKey);
    final int recordOffset = StorageEngineReader.recordPageOffset(recordKey);

    final long transactionLogIdentity = documentRecordLocationCache.get(recordKey);
    if (transactionLogIdentity != PageContainer.NULL_TRANSACTION_LOG_IDENTITY) {
      final KeyValueLeafPage page = log.getAuthoritativeDocumentPage(transactionLogIdentity);
      if (page != null && page.getPageKey() == recordPageKey) {
        // Honour any fresher materialized object before binding a reusable singleton.
        final DataRecord cached = page.getRecord(recordOffset);
        if (cached != null && cached.getNodeKey() == recordKey) {
          return cached;
        }

        // The binder owns the populated-slot check. Calling it directly avoids repeating the
        // bitmap probe which prepareRecordForModificationDocument historically performed first.
        if (writeSingletonBinder != null && page.hasSlottedPageSlot(recordKey)) {
          final DataRecord record = writeSingletonBinder.bind(page, recordOffset, recordKey);
          if (record != null && record.getNodeKey() == recordKey) {
            return record;
          }
        }
      }
    }

    final PageContainer cont = prepareRecordPage(recordPageKey, -1, IndexType.DOCUMENT);
    final var modifiedPage = cont.getModifiedAsKeyValuePage();

    // Honour any fresher in-memory object in records[] (mixed-path safety).
    final DataRecord cached = modifiedPage.getRecord(recordOffset);
    if (cached != null) {
      rememberDocumentRecordLocation(recordKey, cont);
      return cached;
    }

    // Singleton binding fast path.
    if (writeSingletonBinder != null && modifiedPage instanceof KeyValueLeafPage kvl
        && kvl.hasSlottedPageSlot(recordKey)) {
      final DataRecord record = writeSingletonBinder.bind(kvl, recordOffset, recordKey);
      if (record != null) {
        rememberDocumentRecordLocation(recordKey, cont);
        return record;
      }
    }

    // Fallback to full method for edge cases (non-slotted page, bind failure).
    final DataRecord record = prepareRecordForModification(recordKey, IndexType.DOCUMENT, -1);
    rememberDocumentRecordLocation(recordKey, cont);
    return record;
  }

  private void rememberDocumentRecordLocation(final long recordKey, final PageContainer container) {
    final long transactionLogIdentity = container.getTransactionLogIdentity();
    if (transactionLogIdentity != PageContainer.NULL_TRANSACTION_LOG_IDENTITY) {
      documentRecordLocationCache.put(recordKey, transactionLogIdentity);
    }
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
    rememberDocumentRecordLocation(nodeKey, cont);
  }

  @Override
  public KeyValueLeafPage getAllocKvl() {
    return allocKvl;
  }

  @Override
  public int getAllocSlotOffset() {
    return allocSlotOffset;
  }

  @Override
  public long getAllocNodeKey() {
    return allocNodeKey;
  }

  @Override
  public DataRecord createRecord(final DataRecord record, final IndexType indexType, final int index) {
    storageEngineReader.assertNotClosed();
    requireNonNull(indexType);
    NodeStorageEngineReader.validateKeyedTrieRoute(storageEngineReader, indexType, index);

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
      case NAME -> {
        final NamePage namePage = currentNamePage();
        yield namePage.incrementAndGetMaxNodeKey(index);
      }
      case VECTOR -> {
        final VectorPage vectorPage = storageEngineReader.getVectorPage(newRevisionRootPage);
        yield vectorPage.incrementAndGetMaxNodeKey(index);
      }
      default -> throw new IllegalStateException();
    };

    final long recordPageKey = storageEngineReader.pageKey(createdRecordKey, indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final KeyValuePage<DataRecord> modified = cont.getModifiedAsKeyValuePage();

    if (indexType == IndexType.DOCUMENT) {
      maybeEncodeStringValueAtInsert(record, modified);
    }

    if (modified instanceof KeyValueLeafPage kvl) {
      if (record instanceof FlyweightNode fn) {
        final int offset = (int) (createdRecordKey
            - ((createdRecordKey >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
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
    storageEngineReader.assertNotClosed();
    requireNonNull(record);
    requireNonNull(indexType);
    NodeStorageEngineReader.validateKeyedTrieRoute(storageEngineReader, indexType, index);
    if (record instanceof FlyweightNode fn && fn.isWriteSingleton() && fn.getOwnerPage() != null) {
      return; // Bound write singleton — mutations already on heap
    }

    final long recordPageKey = storageEngineReader.pageKey(record.getNodeKey(), indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    cont.getModifiedAsKeyValuePage().setRecord(record);
  }

  @Override
  public void removeRecord(final long recordKey, final IndexType indexType, final int index) {
    storageEngineReader.assertNotClosed();
    requireNonNull(indexType);
    NodeStorageEngineReader.validateKeyedTrieRoute(storageEngineReader, indexType, index);

    final long recordPageKey = storageEngineReader.pageKey(recordKey, indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final DataRecord node = getRecord(recordKey, indexType, index);
    if (node == null) {
      throw new IllegalStateException("Node not found: " + recordKey);
    }

    final Node delNode = new DeletedNode(new NodeDelegate(node.getNodeKey(), -1, null, -1,
        storageEngineReader.getRevisionNumber(), (SirixDeweyID) null));
    cont.getModifiedAsKeyValuePage().setRecord(delNode);
    cont.getCompleteAsKeyValuePage().setRecord(delNode);
  }

  /**
   * Writer-scoped memo for projection value-dictionary records, keyed by node key. See the interface
   * javadoc on {@code StorageEngineReader#cachedProjectionDictionaryRecord} for why this is sound:
   * the dictionary sub-trie is copy-on-write with freshly minted keys, the one stable-key rewrite
   * (the generation header) is evicted by the put hook, and this writer is single-threaded. Records
   * memoized here are heap-materialized (bound flyweights are refused), so entries stay valid after
   * the async flush releases the pages they were decoded from — which is exactly the moment the
   * uncached path starts paying a full page decode per record and this memo starts earning its keep
   * (measured: ~16% of a bulk load's CPU was radix re-reads through that path).
   *
   * <p>
   * BOUNDED, in bytes and in entries, with least-recently-used eviction. Soundness never required a
   * bound, but retention does: the radix routes every read through here, entry LEAVES included, and
   * one of those retains its whole UTF-8 value (up to
   * {@link ValueDictionaryEntryNode#MAX_VALUE_LENGTH}). Uncapped, a high-cardinality column would
   * leave a heap-resident copy of the entire dictionary standing for the life of the write
   * transaction, invisible to the dictionary writer's own byte budget. Access order is what makes the
   * cap keep the speedup: the upper-level radix nodes the walks revisit constantly stay resident,
   * while the one-shot entry leaves that carry the bytes leave from the tail.
   */
  private @Nullable Long2ObjectLinkedOpenHashMap<DataRecord> projectionDictionaryRecordMemo;

  /** Retained-heap estimate of everything currently in {@link #projectionDictionaryRecordMemo}. */
  private long projectionDictionaryRecordMemoBytes;

  /** Retention ceiling for the memo. */
  private static final long PROJECTION_DICTIONARY_MEMO_MAX_BYTES = 32L << 20;

  /** Entry ceiling, so a stream of tiny records cannot make the map itself the leak. */
  private static final int PROJECTION_DICTIONARY_MEMO_MAX_ENTRIES = 1 << 16;

  /** Object header + node key + reference slack charged to every memoized record. */
  private static final int PROJECTION_DICTIONARY_MEMO_ENTRY_OVERHEAD = 64;

  /**
   * Retention charged to a structural dictionary node (radix fan-out array, bucket entry arrays,
   * collision node). Sized to the largest of them rather than measured per instance: the estimate
   * only has to keep the ceiling honest to within a small factor, and a per-kind measurement on this
   * path would cost more than the cap saves.
   */
  private static final int PROJECTION_DICTIONARY_MEMO_STRUCTURAL_BYTES = 2 << 10;

  @Override
  public @Nullable DataRecord cachedProjectionDictionaryRecord(final long key) {
    final Long2ObjectLinkedOpenHashMap<DataRecord> memo = projectionDictionaryRecordMemo;
    return memo == null
        ? null
        : memo.getAndMoveToFirst(key);
  }

  @Override
  public void cacheProjectionDictionaryRecord(final long key, final DataRecord record) {
    if (record instanceof final FlyweightNode flyweight && flyweight.isBound()) {
      // A bound flyweight reads through its page's memory segment, which the flush lifecycle may
      // release — memoizing it would be a use-after-free. Dictionary node classes are plain heap
      // records, so this guard is expected to never fire; it exists so a future flyweight
      // conversion fails safe (the read stays correct, merely unmemoized) instead of dangling.
      return;
    }
    final long footprint = projectionDictionaryRecordFootprint(record);
    if (footprint > PROJECTION_DICTIONARY_MEMO_MAX_BYTES >> 2) {
      // One outsized value would evict most of the working set just to house itself.
      return;
    }
    Long2ObjectLinkedOpenHashMap<DataRecord> memo = projectionDictionaryRecordMemo;
    if (memo == null) {
      memo = new Long2ObjectLinkedOpenHashMap<>(1 << 12);
      projectionDictionaryRecordMemo = memo;
    }
    final DataRecord previous = memo.putAndMoveToFirst(key, record);
    if (previous != null) {
      projectionDictionaryRecordMemoBytes -= projectionDictionaryRecordFootprint(previous);
    }
    projectionDictionaryRecordMemoBytes += footprint;
    while (memo.size() > 1 && (projectionDictionaryRecordMemoBytes > PROJECTION_DICTIONARY_MEMO_MAX_BYTES
        || memo.size() > PROJECTION_DICTIONARY_MEMO_MAX_ENTRIES)) {
      projectionDictionaryRecordMemoBytes -= projectionDictionaryRecordFootprint(memo.removeLast());
    }
  }

  @Override
  public void evictProjectionDictionaryRecord(final long key) {
    final Long2ObjectLinkedOpenHashMap<DataRecord> memo = projectionDictionaryRecordMemo;
    if (memo == null) {
      return;
    }
    final DataRecord removed = memo.remove(key);
    if (removed != null) {
      projectionDictionaryRecordMemoBytes -= projectionDictionaryRecordFootprint(removed);
    }
  }

  /** Retained-heap estimate for one memoized dictionary record. */
  private static long projectionDictionaryRecordFootprint(final DataRecord record) {
    return PROJECTION_DICTIONARY_MEMO_ENTRY_OVERHEAD + (record instanceof final ValueDictionaryEntryNode entry
        ? entry.getValueLength()
        : PROJECTION_DICTIONARY_MEMO_STRUCTURAL_BYTES);
  }

  @Override
  public <V extends DataRecord> V getRecord(final long recordKey, final IndexType indexType, final int index) {
    storageEngineReader.assertNotClosed();

    checkArgument(recordKey >= Fixed.NULL_NODE_KEY.getStandardProperty());
    requireNonNull(indexType);
    NodeStorageEngineReader.validateKeyedTrieRoute(storageEngineReader, indexType, index);

    // Calculate page.
    final long recordPageKey = storageEngineReader.pageKey(recordKey, indexType);

    final int revision = newRevisionRootPage.getRevision();
    PageContainer pageCont = getMostRecentPageContainer(indexType, recordPageKey, index, revision);
    PageReference durableReference = null;
    if (pageCont == null) {
      final ReadPageResolution resolution = resolvePageForRead(recordPageKey, index, indexType, revision);
      pageCont = resolution.pageContainer;
      durableReference = resolution.durableReference;
    }

    if (pageCont == null && durableReference == null) {
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
    } else if (pageCont != null) {
      refuseAdoptedImmutablePage(pageCont.getModified(), "read");
      DataRecord node = getRecordForWriteAccess(((KeyValueLeafPage) pageCont.getModified()), recordKey);
      if (node == null) {
        node = getRecordForWriteAccess(((KeyValueLeafPage) pageCont.getComplete()), recordKey);
      }
      return (V) storageEngineReader.checkItemIfDeleted(node);
    }

    final KeyValueLeafPage durablePage =
        storageEngineReader.readRecordPageFromExactReference(requireNonNull(durableReference));
    try {
      return (V) storageEngineReader.readDetachedRecord(durablePage, recordKey);
    } finally {
      durablePage.retire();
    }
  }

  private DataRecord getRecordForWriteAccess(final KeyValuePage<? extends DataRecord> page, final long recordKey) {
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

  /**
   * Resolve the current writer revision's NamePage without repeating the PageReference/TIL walk. The
   * caller must have already established that the requested root is {@link #newRevisionRootPage} by
   * identity.
   */
  private NamePage currentNamePage() {
    assertTransactionWritable();
    NamePage page = currentNamePage;
    if (page == null) {
      page = prepareSecondaryIndexPage(IndexType.NAME);
      currentNamePage = page;
    }
    return page;
  }

  /** Package-private lifecycle observation for focused cache tests. */
  @Nullable
  NamePage cachedCurrentNamePageForTesting() {
    return currentNamePage;
  }

  @Override
  public NamePage getNamePage(final RevisionRootPage revisionRoot) {
    storageEngineReader.assertNotClosed();
    requireNonNull(revisionRoot);
    return revisionRoot == newRevisionRootPage
        ? currentNamePage()
        : storageEngineReader.getNamePage(revisionRoot);
  }

  @Override
  public PathPage getPathPage(final RevisionRootPage revisionRoot) {
    storageEngineReader.assertNotClosed();
    requireNonNull(revisionRoot);
    return revisionRoot == newRevisionRootPage
        ? prepareSecondaryIndexPage(IndexType.PATH)
        : storageEngineReader.getPathPage(revisionRoot);
  }

  @Override
  public CASPage getCASPage(final RevisionRootPage revisionRoot) {
    storageEngineReader.assertNotClosed();
    requireNonNull(revisionRoot);
    return revisionRoot == newRevisionRootPage
        ? prepareSecondaryIndexPage(IndexType.CAS)
        : storageEngineReader.getCASPage(revisionRoot);
  }

  @Override
  public ProjectionIndexPage getProjectionIndexPage(final RevisionRootPage revisionRoot) {
    storageEngineReader.assertNotClosed();
    requireNonNull(revisionRoot);
    return revisionRoot == newRevisionRootPage
        ? prepareSecondaryIndexPage(IndexType.PROJECTION)
        : storageEngineReader.getProjectionIndexPage(revisionRoot);
  }

  @Override
  public ValidTimeIndexPage getValidTimeIndexPage(final RevisionRootPage revisionRoot) {
    storageEngineReader.assertNotClosed();
    requireNonNull(revisionRoot);
    return revisionRoot == newRevisionRootPage
        ? prepareSecondaryIndexPage(IndexType.VALIDTIME)
        : storageEngineReader.getValidTimeIndexPage(revisionRoot);
  }

  @Override
  public byte[] getRawName(final int nameKey, final NodeKind nodeKind) {
    // Mirror of getName above: the uncommitted CoW NamePage answers first. Without this override,
    // raw-name reads fell through to the COMMITTED revision's dictionary while getName consulted
    // the uncommitted one — so on a write transaction the two same-node accessors could disagree:
    // a name key created in this transaction resolved to null (or, after a freed slot was reused
    // by a hash-colliding name, to the PREVIOUS name's bytes) through the raw path.
    storageEngineReader.assertNotClosed();
    final NamePage currentNamePage = getNamePage(newRevisionRootPage);
    if (currentNamePage != null) {
      final byte[] rawName = currentNamePage.getRawName(nameKey, nodeKind, storageEngineReader);
      if (rawName != null) {
        return rawName;
      }
    }
    return storageEngineReader.getRawName(nameKey, nodeKind);
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

  @Override
  public int keyForName(final @Nullable String name, final NodeKind nodeKind) {
    storageEngineReader.assertNotClosed();
    requireNonNull(nodeKind);
    final String string = name == null
        ? ""
        : name;
    final NamePage namePage = getNamePage(newRevisionRootPage);
    return namePage.keyForName(string, nodeKind, this);
  }

  @Override
  public boolean stageUncommittedOverflowPage(final PageReference reference) {
    if (isClosed) {
      throw new IllegalStateException("The storage engine writer is already closed");
    }
    storageEngineReader.assertNotClosed();
    if (asyncTerminalFailure) {
      throw asyncFlushFailure("Transaction in terminal failure state from a prior async commit error");
    }
    requireNonNull(reference);
    final Page page = reference.getPage();
    if (!(page instanceof OverflowPage overflowPage)) {
      throw new IllegalArgumentException("Only an immutable OverflowPage can enter the side-page append batch");
    }
    if (reference.getKey() != Constants.NULL_ID_LONG || reference.getLogKey() != Constants.NULL_ID_INT
        || reference.hasPendingPageWrite()) {
      throw new IllegalArgumentException("The staged OverflowPage reference must be fresh and unresolved");
    }

    // RAM and legacy append-at-physical-size backends cannot reclaim bytes written before the root
    // is published. Leave the page resident there; recursive final commit is slower but space-safe.
    if (!storagePageReaderWriter.supportsReclaimableUncommittedWrites()) {
      return false;
    }

    // A 128 MiB per-writer reservoir must have deterministic ownership. AUTO would defer release
    // to GC/Cleaner activity (the opposite of the no-major-GC contract), while GLOBAL would leak it
    // forever. Those configurations keep the ordinary resident-page commit path until they have a
    // process-level native reservoir pool with explicit lifecycle semantics.
    if (!SharedArenas.supportsDeterministicClose()) {
      return false;
    }

    final int payloadLength = overflowPage.dataLength();
    // OverflowPage is generic and can exceed projection's 16 MiB domain cap. One item larger than
    // the entire staging budget cannot satisfy this API's bounded-retention contract; leave it on
    // the ordinary final-commit path rather than quietly invalidating the bound.
    if (payloadLength > MAX_STAGED_SIDE_PAGE_BYTES) {
      return false;
    }

    if (activeSidePages == null) {
      initializeSidePageBatches();
    }

    // Preflight BEFORE marking the new ref pending. The caller already owns this incoming byte[];
    // if it would overflow the active budget, rotate the existing capped batch first. This bounds
    // writer-owned state to two capped buffers plus the one incoming payload being backpressured.
    if (activeSidePages.size > 0 && (payloadLength > MAX_STAGED_SIDE_PAGE_BYTES - activeSidePages.payloadBytes
        || activeSidePages.size >= MAX_STAGED_SIDE_PAGE_COUNT)) {
      flushStagedSidePagesOnly();
    }

    // Reserve before marking the reference pending. Array growth is the only allocating/failing
    // operation in add(); if it failed after the marker was installed, the HOT leaf would contain a
    // pending page owned by no batch and final commit would (correctly) refuse to serialize it.
    activeSidePages.ensureCapacity(activeSidePages.size + 1);
    final OverflowPage nativePage = activeSidePages.copyToNative(overflowPage);
    reference.replaceAndBindPendingPageWrite(overflowPage, nativePage);
    activeSidePages.addReserved(reference, payloadLength);
    if (HFT_TELEMETRY_ENABLED) {
      hftStagedSidePages++;
      hftStagedSideBytes += payloadLength;
      hftPeakActiveSideBytes = Math.max(hftPeakActiveSideBytes, activeSidePages.payloadBytes);
    }

    // This is an overflow-only epoch. Projection maintenance may still be reading KVL records in
    // the same drain, so crossing the side-page budget must never rotate or clean the live TIL.
    if (activeSidePages.payloadBytes >= MAX_STAGED_SIDE_PAGE_BYTES
        || activeSidePages.size >= MAX_STAGED_SIDE_PAGE_COUNT) {
      flushStagedSidePagesOnly();
    }
    return true;
  }

  /** Allocate both fixed native reservoirs as one all-or-nothing lazy initialization. */
  private void initializeSidePageBatches() {
    if (activeSidePages != null || snapshotSidePages != null || sidePagePayloadArena != null) {
      throw new IllegalStateException("Immutable side-page reservoirs are only initialized as an empty pair");
    }
    final int initialCapacity = Math.min(INITIAL_SIDE_PAGE_BATCH_CAPACITY, MAX_STAGED_SIDE_PAGE_COUNT);
    final Arena arena = SharedArenas.newSharedArena();
    try {
      final MemorySegment activePayload = arena.allocate(MAX_STAGED_SIDE_PAGE_BYTES, Long.BYTES);
      final MemorySegment snapshotPayload = arena.allocate(MAX_STAGED_SIDE_PAGE_BYTES, Long.BYTES);
      final SidePageBatch active = new SidePageBatch(initialCapacity, activePayload);
      final SidePageBatch snapshot = new SidePageBatch(initialCapacity, snapshotPayload);
      sidePagePayloadArena = arena;
      activeSidePages = active;
      snapshotSidePages = snapshot;
      if (HFT_TELEMETRY_ENABLED) {
        hftNativeReservoirCount = 2;
        hftNativeReservoirBytes = activePayload.byteSize();
      }
    } catch (final Throwable failure) {
      try {
        SharedArenas.close(arena);
      } catch (final Throwable closeFailure) {
        retainFirstFailure(failure, closeFailure);
      }
      throw asRuntimeFailure(failure);
    }
  }

  private boolean hasActiveSidePages() {
    return activeSidePages != null && activeSidePages.size > 0;
  }

  /** Swap the foreground and background side-page buffers after the prior snapshot was cleaned. */
  private int rotateSidePageBatch() {
    if (!hasActiveSidePages()) {
      return 0;
    }
    if (snapshotSidePages == null || snapshotSidePages.size != 0) {
      throw new IllegalStateException("Prior immutable side-page batch was not cleaned before reuse");
    }
    final SidePageBatch frozen = activeSidePages;
    activeSidePages = snapshotSidePages;
    snapshotSidePages = frozen;
    return frozen.size;
  }

  /** Release every pending payload, both reusable arrays, and their fixed native arena. */
  private void discardSidePageBatches() {
    if (asyncFlushWorkerRunning) {
      throw new SirixIOException("Cannot discard immutable side-page buffers while their append worker may still "
          + "read them; the writer remains poisoned and must not be reused or closed concurrently");
    }
    Throwable failure = null;
    final SidePageBatch active = activeSidePages;
    activeSidePages = null;
    if (active != null) {
      try {
        active.clear(true);
      } catch (final Throwable t) {
        failure = retainFirstFailure(failure, t);
      }
    }
    final SidePageBatch snapshot = snapshotSidePages;
    snapshotSidePages = null;
    if (snapshot != null) {
      try {
        snapshot.clear(true);
      } catch (final Throwable t) {
        failure = retainFirstFailure(failure, t);
      }
    }
    final Arena arena = sidePagePayloadArena;
    sidePagePayloadArena = null;
    if (arena != null) {
      try {
        SharedArenas.close(arena);
      } catch (final Throwable t) {
        failure = retainFirstFailure(failure, t);
      }
    }
    asyncSnapshotIncludesLog = false;
    asyncSnapshotWriteComplete = false;
    asyncFlushInFlight = false;
    if (failure != null) {
      throw asRuntimeFailure(failure);
    }
  }

  // ==================== ASYNC AUTO-COMMIT ====================

  @Override
  public void asyncFlush() {
    startAsyncFlush(true);
  }

  @Override
  public boolean isAsyncFlushLogBoundaryReached() {
    return log.liveEntryCount() >= MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT;
  }

  static boolean isAsyncFlushLogBoundaryReached(final int liveEntryCount) {
    checkArgument(liveEntryCount >= 0, "Negative live TIL entry count is not accepted.");
    return liveEntryCount >= MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT;
  }

  @Override
  public void recordAsyncFlushForegroundNanos(final long elapsedNanos) {
    checkArgument(elapsedNanos >= 0L, "Negative async-flush foreground duration is not accepted.");
    if (HFT_TELEMETRY_ENABLED) {
      hftForegroundFlushCount++;
      hftForegroundFlushNanos += elapsedNanos;
      hftMaxForegroundFlushNanos = Math.max(hftMaxForegroundFlushNanos, elapsedNanos);
    }
  }

  /**
   * Flush only immutable side pages, leaving the live TIL untouched.
   *
   * <p>
   * Projection bulk-load drains read records from the current TIL while they build row groups.
   * Rotating that log at a side-page budget crossing would make the remaining records unreadable.
   * This path applies backpressure through the SAME permit and uses the SAME append owner as a full
   * epoch, but it neither snapshots nor cleans record pages.
   * </p>
   */
  private void flushStagedSidePagesOnly() {
    startAsyncFlush(false);
  }

  /** Start either a combined KVL+side-page epoch or a side-page-only epoch. */
  private void startAsyncFlush(final boolean includeTransactionLog) {
    final long started = HFT_TELEMETRY_ENABLED
        ? System.nanoTime()
        : 0L;
    try {
      startAsyncFlushOwned(includeTransactionLog);
    } finally {
      if (HFT_TELEMETRY_ENABLED) {
        final long elapsed = Math.max(0L, System.nanoTime() - started);
        hftStartFlushCount++;
        hftStartFlushNanos += elapsed;
        hftMaxStartFlushNanos = Math.max(hftMaxStartFlushNanos, elapsed);
      }
    }
  }

  private void startAsyncFlushOwned(final boolean includeTransactionLog) {
    // Fail-fast: terminal failure is a permanent latch — transaction is unusable.
    if (asyncTerminalFailure) {
      throw asyncFlushFailure("Transaction in terminal failure state from a prior async commit error");
    }

    // Backpressure is bounded. A dead executor/worker must poison this writer rather than park the
    // ingestion thread forever and hide the original failure behind rollback/close.
    if (!acquireFlushPermitUntilProgressStalls(true)) {
      final SirixIOException timeout = asyncFlushFailure("Background snapshot flush made no progress for "
          + TimeUnit.NANOSECONDS.toMillis(ASYNC_FLUSH_STALL_TIMEOUT_NANOS) + " ms while starting the next epoch");
      asyncFlushTimedOut = true;
      recordAsyncFlushFailure(timeout);
      cancelQueuedAsyncSnapshotWriteAfterTimeout();
      throw timeout;
    }

    // Every exit between here and a SUCCESSFUL submission must hand the permit back: on those
    // paths no background worker is ever started, so nothing else can release it, and a permit
    // left behind parks the next awaitPendingAsyncFlush() forever — turning the rollback that a
    // failure here triggers into a hang that buries the very exception that caused it.
    boolean permitTransferred = false;
    boolean admissionArmed = false;
    boolean admissionTransferred = false;
    try {
      // CRITICAL double-check: error may have been set by background thread
      // between our latch check above and the acquire completing.
      if (asyncFlushError != null) {
        asyncTerminalFailure = true;
        throw asyncFlushFailure("Prior async commit failed");
      }

      // Acquiring the permit is the happens-before edge from the prior worker. Publish its complete
      // results before reusing either side-channel buffer; on failure above, publish nothing.
      cleanupCompletedAsyncSnapshot();

      // Bottom-up structural retirement is foreground-only and runs only for a full TIL epoch.
      // At this point the prior KVL snapshot and its immutable side-page batch have both been
      // published, and this thread still owns the sole append permit. A side-only rotation may run
      // while projection maintenance is reading/mutating its current HOT leaf, so it must never
      // inspect or serialize pinned trie pages.
      if (includeTransactionLog) {
        if (HFT_TELEMETRY_ENABLED) {
          recordPinnedTrieFullEpochState();
        }
        spillEligiblePinnedTriePages();
      }

      injectAsyncFlushFault("prepare");

      final long admissionWaitStart = HFT_TELEMETRY_ENABLED
          ? System.nanoTime()
          : 0L;
      try {
        if (!SNAPSHOT_APPEND_EXECUTOR.acquireAdmissionUntilProgressStalls(ASYNC_FLUSH_STALL_TIMEOUT_NANOS)) {
          throw new SirixIOException("Snapshot append admission made no progress for "
              + TimeUnit.NANOSECONDS.toMillis(ASYNC_FLUSH_STALL_TIMEOUT_NANOS) + " ms");
        }
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new SirixIOException("Interrupted while waiting for snapshot append admission", interrupted);
      }
      try {
        asyncSnapshotWriteTask.armAdmission();
      } catch (final Throwable failure) {
        SNAPSHOT_APPEND_EXECUTOR.releaseUnassignedAdmission();
        throw failure;
      }
      admissionArmed = true;
      final long admissionWaitNanos = HFT_TELEMETRY_ENABLED
          ? Math.max(0L, System.nanoTime() - admissionWaitStart)
          : 0L;

      // O(1) snapshots — array swaps only. A side-only epoch MUST NOT rotate the TIL: it can run
      // in the middle of projection maintenance while that maintenance is still reading records.
      final int snapshotSize = includeTransactionLog
          ? log.snapshot()
          : 0;
      final int sidePageCount = rotateSidePageBatch();
      if (snapshotSize == 0 && sidePageCount == 0) {
        if (includeTransactionLog) {
          // snapshot() still installed empty frozen arrays. Retire them now rather than carrying a
          // fake pending epoch into the next real flush.
          log.cleanupSnapshot();
        }
        SNAPSHOT_APPEND_EXECUTOR.releaseTaskAdmission(asyncSnapshotWriteTask);
        admissionArmed = false;
        return;
      }

      if (HFT_TELEMETRY_ENABLED) {
        if (includeTransactionLog) {
          hftCombinedEpochs++;
        } else {
          hftSideOnlyEpochs++;
        }
        hftActiveEpochId = ++hftEpochSequence;
        hftSubmitWaitCount++;
        hftSubmitWaitNanos += admissionWaitNanos;
        hftMaxSubmitWaitNanos = Math.max(hftMaxSubmitWaitNanos, admissionWaitNanos);
        HFT_GLOBAL_SUBMIT_WAIT_COUNT.incrementAndGet();
        HFT_GLOBAL_SUBMIT_WAIT_NANOS.addAndGet(admissionWaitNanos);
        HFT_GLOBAL_SUBMIT_WAIT_MAX_NANOS.accumulateAndGet(admissionWaitNanos, Math::max);
      }

      if (includeTransactionLog) {
        // CRITICAL: Invalidate all local container caches. Cached containers point
        // to frozen-zone pages. Without invalidation, cache fast paths return frozen containers.
        clearLocalContainerCaches();

        // Re-add structural pages to fresh TIL for continued operation
        reAddStructuralPagesToTil();
        if (HFT_TELEMETRY_ENABLED) {
          recordPinnedTrieFullEpochState();
        }
      }

      armAsyncSnapshotWriteTask();
      asyncSnapshotIncludesLog = includeTransactionLog;
      asyncSnapshotWriteComplete = false;
      asyncFlushInFlight = true;
      asyncFlushWorkerRunning = true;
      markAsyncFlushProgress();

      // Background thread: append the frozen immutable pages, plus KVL pages for a combined epoch.
      // CRITICAL: If submission throws (RejectedExecutionException), the snapshot state is
      // dangling with no bg thread to process it — the outer failure handler latches the cause and
      // the finally hands the permit back.
      try {
        // Ownership transfers the instant execute returns. No completion wrapper is registered:
        // runAsyncSnapshotWriteTask catches every Throwable and always releases the permit, while the
        // persistent Runnable and array-backed queue keep the submission path allocation-free.
        if (HFT_TELEMETRY_ENABLED) {
          hftActiveEpochSubmittedNanos = System.nanoTime();
        }
        SNAPSHOT_APPEND_EXECUTOR.execute(asyncSnapshotWriteTask);
        HftBoundaryTelemetry.asyncSubmission();
        permitTransferred = true;
        admissionTransferred = true;
      } catch (final Throwable t) {
        // ThreadPoolExecutor.execute may exceptionally fail while ensuring a replacement worker
        // AFTER it has queued the command. The task-state CAS, not execute()'s return, decides who
        // owns the permit: cancellation wins only while the command is still ENQUEUED; once run()
        // has claimed it, that worker alone releases in its finally block.
        if (cancelEnqueuedAsyncSnapshotWrite()) {
          asyncFlushInFlight = false;
          asyncSnapshotIncludesLog = false;
          asyncSnapshotWriteComplete = false;
        } else {
          permitTransferred = true;
          admissionTransferred = true;
        }
        throw new SirixIOException("Failed to submit async commit", t);
      }
    } catch (final Throwable t) {
      // The log has been rotated, or half-rotated, with no worker to finish the job: the
      // transaction cannot continue. Record the cause so a later close() or rollback() reports
      // THIS instead of a bare terminal-failure latch, and say so immediately — the thread that
      // eventually observes the latch may be minutes and gigabytes of insert away.
      recordAsyncFlushFailure(t);
      try {
        LOGGER.error("Async snapshot flush could not be started — transaction is now in terminal failure state", t);
      } catch (final Throwable ignored) {
        // Poisoning is the correctness action. Diagnostics must never prevent it under OOME.
      }
      throw t;
    } finally {
      if (!permitTransferred) {
        flushPermit.release();
      }
      if (admissionArmed && !admissionTransferred) {
        SNAPSHOT_APPEND_EXECUTOR.releaseTaskAdmission(asyncSnapshotWriteTask);
      }
    }
  }

  @Override
  public void awaitPendingAsyncFlush() {
    final long started = HFT_TELEMETRY_ENABLED
        ? System.nanoTime()
        : 0L;
    try {
      awaitPendingAsyncFlushOwned();
    } finally {
      if (HFT_TELEMETRY_ENABLED) {
        final long elapsed = Math.max(0L, System.nanoTime() - started);
        hftFinalDrainCount++;
        hftFinalDrainNanos += elapsed;
        hftMaxFinalDrainNanos = Math.max(hftMaxFinalDrainNanos, elapsed);
      }
    }
  }

  private void awaitPendingAsyncFlushOwned() {
    awaitInFlightAsyncFlushOnly();

    // Final projection maintenance can stage pages AFTER the last periodic async epoch. Drain that
    // active tail through the single append owner before recursive root serialization; a pending
    // side reference is never allowed to fall back to the racing synchronous append path.
    if (hasActiveSidePages()) {
      flushStagedSidePagesOnly();
      awaitCurrentAsyncFlush();
    }

    throwIfAsyncFlushFailed();
  }

  /**
   * Fence only a submitted worker; rollback/close use this without writing the uncommitted active
   * tail.
   */
  private void awaitInFlightAsyncFlushOnly() {
    if (asyncFlushTimedOut) {
      throwIfAsyncFlushFailed();
    }
    if (asyncFlushInFlight) {
      awaitCurrentAsyncFlush();
    }
    throwIfAsyncFlushFailed();
  }

  /** Report a poisoned writer even when no worker remains in flight. */
  private void throwIfAsyncFlushFailed() {
    // Nothing is pending, but a flush that already failed must not let the transaction look
    // healthy: the writer is poisoned and every entry point has to say so.
    if (asyncFlushError != null) {
      asyncTerminalFailure = true;
      throw asyncFlushFailure("Async commit failed");
    }
  }

  /** Fence one in-flight worker, then publish its complete results on this foreground thread. */
  private void awaitCurrentAsyncFlush() {
    // Drain the in-flight flush BEFORE reporting any failure: the worker owns pages the caller is
    // about to clear or close, and the permit hand-back is the only proof it is done with them.
    if (!acquireFlushPermitUntilProgressStalls(false)) {
      // The worker is gone or wedged. Proceeding without its pages is a risk, but parking here is
      // a certainty: it is the failure this very method was hanging on in the 100M load.
      final SirixIOException timeout = asyncFlushFailure("Background snapshot flush made no progress for "
          + TimeUnit.NANOSECONDS.toMillis(ASYNC_FLUSH_STALL_TIMEOUT_NANOS)
          + " ms — its worker is gone, blocked, or wedged");
      asyncFlushTimedOut = true;
      recordAsyncFlushFailure(timeout);
      cancelQueuedAsyncSnapshotWriteAfterTimeout();
      throw timeout;
    }

    try {
      // Check for background thread errors BEFORE publication: a failed batch may have assigned
      // some shadow offsets, but none of them are valid until the entire shared buffer was flushed.
      if (asyncFlushError != null) {
        asyncTerminalFailure = true;
        throw asyncFlushFailure("Async commit failed");
      }
      cleanupCompletedAsyncSnapshot();
    } finally {
      flushPermit.release();
    }
  }

  /** Publish and clear the snapshot whose worker has handed the permit back. */
  private void cleanupCompletedAsyncSnapshot() {
    if (!asyncFlushInFlight) {
      return;
    }
    if (!asyncSnapshotWriteComplete || (asyncSnapshotIncludesLog && !log.isSnapshotFlushComplete())) {
      final SirixIOException incomplete = new SirixIOException(
          "Background snapshot worker released ownership without positively completing the whole flushed epoch");
      recordAsyncFlushFailure(incomplete);
      throw incomplete;
    }
    final SidePageBatch sidePages = snapshotSidePages;
    if (sidePages != null && sidePages.size > 0) {
      sidePages.publishCompletedWrites();
    }
    if (asyncSnapshotIncludesLog) {
      log.cleanupSnapshot();
    }
    asyncSnapshotIncludesLog = false;
    asyncSnapshotWriteComplete = false;
    asyncFlushInFlight = false;
    ASYNC_SNAPSHOT_WRITE_TASK_STATE.setRelease(this, ASYNC_TASK_IDLE);
  }

  /**
   * Serialize a fixed, bottom-up batch of cold pinned trie pages through shadow references.
   *
   * <p>
   * This method is called only by the foreground thread while it owns {@link #flushPermit}, after
   * publication of the prior KVL/side-page epoch. It deliberately calls the storage writer directly:
   * invoking {@link Page#commit(StorageEngineWriter)} would recurse into children and mutate/close
   * the live page before the batch tail was known to be flushed. Parents whose child is still claimed
   * by any TIL generation are rejected now and become eligible in a later epoch after that child has
   * been published.
   * </p>
   */
  private void spillEligiblePinnedTriePages() {
    if (!storagePageReaderWriter.supportsReclaimableUncommittedWrites() || log.pinnedSize() == 0) {
      return;
    }

    log.capturePinnedSpillCandidates(PINNED_TRIE_SPILL_SCAN_BUDGET, pinnedTrieSpillBatch);
    int index = 0;
    while (index < pinnedTrieSpillBatch.size()) {
      if (isPinnedTrieSpillPageEligible(pinnedTrieSpillBatch.pageAt(index))) {
        index++;
      } else {
        pinnedTrieSpillBatch.removeAtSwap(index);
      }
    }
    if (pinnedTrieSpillBatch.size() == 0) {
      return;
    }

    try {
      injectAsyncFlushFault("trie-spill-before-write");
      final ResourceConfiguration resourceConfiguration = getResourceSession().getResourceConfig();
      for (int i = 0; i < pinnedTrieSpillBatch.size(); i++) {
        pinnedTrieSpillShadowReference.clearHash();
        pinnedTrieSpillShadowReference.setKey(Constants.NULL_ID_LONG);
        pinnedTrieSpillShadowReference.setPage(null);
        writeUncommittedPage(resourceConfiguration, pinnedTrieSpillShadowReference, pinnedTrieSpillBatch.pageAt(i),
            bufferBytes);
        pinnedTrieSpillBatch.setWriteResult(i, pinnedTrieSpillShadowReference.getKey(),
            pinnedTrieSpillShadowReference.getHashAsLong(), pinnedTrieSpillShadowReference.hasHash());
      }
      injectAsyncFlushFault("trie-spill-after-write");

      // No live handle is exposed until every byte in the foreground buffer has been drained.
      injectAsyncFlushFault("trie-spill-before-flush");
      storagePageReaderWriter.flushBufferedWrites(bufferBytes);
      injectAsyncFlushFault("trie-spill-after-flush");

      // Validate the WHOLE batch before publishing the first result. A CoW/superseding mutation
      // detected here poisons the writer without leaving a prefix of identity-valid pages exposed.
      injectAsyncFlushFault("trie-spill-before-validation");
      for (int i = 0; i < pinnedTrieSpillBatch.size(); i++) {
        log.validatePinnedSpillCandidate(pinnedTrieSpillBatch, i);
      }

      injectAsyncFlushFault("trie-spill-before-publish");
      for (int i = 0; i < pinnedTrieSpillBatch.size(); i++) {
        log.publishPinnedSpillCandidate(pinnedTrieSpillBatch, i);
      }
      if (HFT_TELEMETRY_ENABLED) {
        final int publishedPages = pinnedTrieSpillBatch.size();
        hftPinnedTrieSpillEpochs++;
        hftPinnedTrieSpillPages += publishedPages;
        hftPinnedTrieSpillBatchMax = Math.max(hftPinnedTrieSpillBatchMax, publishedPages);
        recordPinnedTrieFullEpochState();
      }
      injectAsyncFlushFault("trie-spill-after-publish");
    } finally {
      // Never retain page/container/handle graphs beyond this bounded foreground epoch.
      pinnedTrieSpillBatch.clear();
      pinnedTrieSpillShadowReference.clearHash();
      pinnedTrieSpillShadowReference.setKey(Constants.NULL_ID_LONG);
      pinnedTrieSpillShadowReference.setPage(null);
    }
  }

  private void writeUncommittedPage(final ResourceConfiguration resourceConfiguration, final PageReference reference,
      final Page page, final BytesOut<?> bufferedBytes) {
    if (storagePageReaderWriter.supportsReclaimableUncommittedWrites()) {
      hasUncommittedReclaimableWrites = true;
    }
    try {
      storagePageReaderWriter.write(resourceConfiguration, reference, page, bufferedBytes);
    } finally {
      final long pageOffset = reference.getKey();
      if (pageOffset != Constants.NULL_ID_LONG) {
        firstUncommittedPageOffset = Math.min(firstUncommittedPageOffset, pageOffset);
      }
    }
  }

  /**
   * Exact allow-list eligibility; capture already guarantees that {@code page} is the modified page.
   */
  static boolean isPinnedTrieSpillPageEligible(final Page page) {
    final Class<?> pageClass = page.getClass();
    if (pageClass == HOTLeafPage.class) {
      final HOTLeafPage leaf = (HOTLeafPage) page;
      // A leaf in the sparse-fragment shape must not spill: the serializer would write only its
      // dirty entries (a versioned delta for the committed read path's chain combine), and the
      // spill's standalone reload has no chain — the clean entries would silently vanish from the
      // live trie. See HOTLeafPage#wouldEmitSparseFragment.
      return !leaf.wouldEmitSparseFragment() && leaf.allSideReferencesDurableAndUnclaimed();
    }

    if (pageClass == HOTIndirectPage.class) {
      final HOTIndirectPage indirectPage = (HOTIndirectPage) page;
      for (int i = 0; i < indirectPage.getNumChildren(); i++) {
        final PageReference childReference = indirectPage.getChildReference(i);
        if (childReference == null || !childReference.refreshesToUnclaimedDurableReference()) {
          return false;
        }
      }
      return true;
    }

    if (pageClass == IndirectPage.class) {
      return ((IndirectPage) page).allChildReferencesDurableAndUnclaimed();
    }

    return false;
  }

  /**
   * Interrupt-deferred, progress-aware acquire of the flush permit. A large but healthy epoch can run
   * longer than the stall window as long as its coordinator advances; a lost/dead worker cannot turn
   * the next epoch, rollback, and close into three repeated multi-minute parks.
   *
   * @param rotationAcquire {@code true} while starting the next epoch (ingestion backpressure),
   *        {@code false} while explicitly draining an in-flight worker
   * @return whether the permit was acquired; on {@code true} the caller owns it and must release it
   */
  private boolean acquireFlushPermitUntilProgressStalls(final boolean rotationAcquire) {
    final long waitStart = HFT_TELEMETRY_ENABLED
        ? System.nanoTime()
        : 0L;
    boolean interrupted = false;
    try {
      for (;;) {
        try {
          if (flushPermit.tryAcquire(ASYNC_FLUSH_PROGRESS_POLL_NANOS, TimeUnit.NANOSECONDS)) {
            recordSuccessfulFlushPermitAcquire(waitStart, rotationAcquire);
            return true;
          }
        } catch (final InterruptedException e) {
          interrupted = true;
        }
        final long lastProgress = asyncFlushProgressNanos;
        if (lastProgress == 0L || System.nanoTime() - lastProgress >= ASYNC_FLUSH_STALL_TIMEOUT_NANOS) {
          // The worker may have released between the timed acquire and the deadline check. This
          // allocation-free final probe avoids poisoning a flush that completed on that boundary.
          if (flushPermit.tryAcquire()) {
            recordSuccessfulFlushPermitAcquire(waitStart, rotationAcquire);
            return true;
          }
          // Likewise, do not time out on a stale progress sample taken as the worker advanced.
          if (asyncFlushProgressNanos != lastProgress) {
            continue;
          }
          // COMPLETED is published immediately before the worker's permit release. If the final
          // probe landed in that tiny interval, retry instead of poisoning a fully written epoch.
          if ((int) ASYNC_SNAPSHOT_WRITE_TASK_STATE.getAcquire(this) == ASYNC_TASK_COMPLETED) {
            Thread.onSpinWait();
            continue;
          }
          return false;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Record every successful foreground permit acquisition exactly once. */
  private void recordSuccessfulFlushPermitAcquire(final long waitStart, final boolean rotationAcquire) {
    if (HFT_TELEMETRY_ENABLED) {
      final long waitNanos = System.nanoTime() - waitStart;
      hftPermitAcquires++;
      hftPermitWaitNanos += waitNanos;
      hftMaxPermitWaitNanos = Math.max(hftMaxPermitWaitNanos, waitNanos);
      if (rotationAcquire) {
        hftRotationPermitAcquires++;
        hftRotationPermitWaitNanos += waitNanos;
        hftMaxRotationPermitWaitNanos = Math.max(hftMaxRotationPermitWaitNanos, waitNanos);
      } else {
        hftDrainPermitAcquires++;
        hftDrainPermitWaitNanos += waitNanos;
        hftMaxDrainPermitWaitNanos = Math.max(hftMaxDrainPermitWaitNanos, waitNanos);
      }
      final long completedEpochId = hftCompletedEpochId;
      if (completedEpochId != 0L) {
        if (waitNanos > hftMaxBlockedEpochForegroundWaitNanos) {
          hftMaxBlockedEpochId = completedEpochId;
          hftMaxBlockedEpochForegroundWaitNanos = waitNanos;
          hftMaxBlockedEpochWorkerNanos = hftCompletedEpochWorkerNanos;
          hftMaxBlockedEpochQueueWaitNanos = hftCompletedEpochQueueWaitNanos;
          hftMaxBlockedEpochSideNanos = hftCompletedEpochSideNanos;
          hftMaxBlockedEpochSerializeJoinWaitNanos = hftCompletedEpochSerializeJoinWaitNanos;
          hftMaxBlockedEpochKvlAppendNanos = hftCompletedEpochKvlAppendNanos;
          hftMaxBlockedEpochFinalFlushNanos = hftCompletedEpochFinalFlushNanos;
          hftMaxBlockedEpochDataGrowCount = hftCompletedEpochDataGrowCount;
          hftMaxBlockedEpochDataGrowBytes = hftCompletedEpochDataGrowBytes;
          hftMaxBlockedEpochDataGrowNanos = hftCompletedEpochDataGrowNanos;
          hftMaxBlockedEpochDataGrowExact = hftCompletedEpochDataGrowExact;
          hftMaxBlockedEpochRotation = rotationAcquire;
        }
        // A semaphore release can be acquired only once. Consume its completion identity at that
        // same handoff even when publication/cleanup later fails, so rollback or close cannot
        // attribute a second, already-free acquire to this epoch.
        hftCompletedEpochId = 0L;
      }
    }
  }

  /** Arm the one persistent task after the previous completed epoch has been cleaned. */
  private void armAsyncSnapshotWriteTask() {
    if (!ASYNC_SNAPSHOT_WRITE_TASK_STATE.compareAndSet(this, ASYNC_TASK_IDLE, ASYNC_TASK_ENQUEUED)) {
      throw new IllegalStateException("Async snapshot task was not idle before submission");
    }
  }

  /**
   * Cancel only an unclaimed task. A successful CAS transfers permit ownership to this foreground
   * thread; a failed CAS means a running/completed worker remains the sole releaser.
   */
  private boolean cancelEnqueuedAsyncSnapshotWrite() {
    if (!ASYNC_SNAPSHOT_WRITE_TASK_STATE.compareAndSet(this, ASYNC_TASK_ENQUEUED, ASYNC_TASK_CANCELLED)) {
      return false;
    }
    // Removal is for prompt queue-retention cleanup. Correctness comes from the state CAS: if a
    // worker dequeued immediately before it, run() observes CANCELLED and exits without releasing.
    SNAPSHOT_APPEND_EXECUTOR.removeAdmitted(asyncSnapshotWriteTask);
    SNAPSHOT_APPEND_EXECUTOR.releaseTaskAdmission(asyncSnapshotWriteTask);
    asyncFlushWorkerRunning = false;
    return true;
  }

  /** Release the permit only when timeout cancellation won before the worker claimed the task. */
  private void cancelQueuedAsyncSnapshotWriteAfterTimeout() {
    if (cancelEnqueuedAsyncSnapshotWrite()) {
      flushPermit.release();
    }
  }

  /**
   * Poison the writer with {@code cause}. The FIRST cause wins — a later failure is almost always a
   * consequence of the first, and the first is the one worth reporting.
   */
  private synchronized void recordAsyncFlushFailure(final Throwable cause) {
    if (asyncFlushError == null) {
      asyncFlushError = cause;
      // Log exactly once, on the cause that wins, passing the throwable as the throwable argument so
      // its stack trace survives. Several poisoning sites (stall timeout, no-progress timeout,
      // incomplete snapshot, executor shutdown) report nowhere else, which left the fault invisible
      // whenever the foreground exception was later stripped of its cause.
      LOGGER.error("Async snapshot flush poisoned the writer - transaction is now in terminal failure state", cause);
    }
    asyncTerminalFailure = true;
  }

  /**
   * The exception reporting an async-flush failure, carrying the captured background throwable as its
   * cause so the caller sees the real fault rather than the latch.
   */
  private SirixIOException asyncFlushFailure(final String message) {
    final Throwable cause = asyncFlushError;
    return cause == null
        ? new SirixIOException(message)
        : new SirixIOException(message, cause);
  }

  /**
   * Sliding-window width of the background snapshot flush: how many KVL pages are deep-copied and
   * pre-serialized in parallel before the sequential append pass writes and closes them. Two windows
   * are in flight at once (double buffering), so the transient draw on the shared segment-allocator
   * budget is bounded by {@code 2 × WINDOW} copies, each holding a pooled slotted segment
   * (64&nbsp;KiB typical, up to 256&nbsp;KiB) plus its cached encoded form. The double buffering
   * keeps the flush pool's workers serializing while this thread appends; widening the window past
   * the pool's appetite only inflates the footprint.
   *
   * <p>
   * <b>Equal to the epoch on purpose, and measured that way.</b> Defining the width as the epoch size
   * makes an epoch exactly one window, so the loop below joins its only serialization, appends it,
   * and finds no successor started — the double buffering never actually overlaps anything. That
   * reads like a defect, and the obvious repair is to size the window independently so several
   * windows per epoch pipeline against each other. It was tried, on a 1M-row ClickBench bulk import
   * at {@code maxLogEntries=256} (395 epochs, 264 KVL pages each). The overlap is real and visible in
   * the phase telemetry — a 64-page window cut the serialization-join stall from 8.1 s to 6.8 s and
   * total worker time from 10.6 s to 9.7 s — and it bought nothing: interleaved min-of-3 wall time
   * was 12.047 s at one window per epoch against 12.412 s at four, and a 16-page window (17 windows
   * per epoch) regressed to 13.8 s as per-window fork/join overhead took over.
   * </p>
   *
   * <p>
   * The reason is that the append pass this would overlap is the small phase: the flush spends ~77%
   * of its time waiting on the parallel serialize pool and ~23% appending, and the pool is the
   * throughput limit for the whole load. Recovering the append window hands the freed capacity
   * straight back to contention with the insert threads, which is the same effect that makes
   * oversizing {@code sirix.asyncFlush.parallelism} lose (14 serializers measured slower end to end
   * than 9, despite a strictly lower worker time). Anyone reaching for a deeper flush pipeline — a
   * wider window here, or a multi-generation intent log so consecutive epochs overlap — is aiming at
   * the 23%, and should first check what the serialize stage costs.
   * </p>
   */
  private static final int SNAPSHOT_FLUSH_WINDOW = MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT;

  /**
   * Background thread: append the frozen immutable side pages and, for a combined epoch, all KVL
   * pages. Uses one private buffer and one shadow PageReference — NEVER writes to real refs.
   * <p>
   * CRITICAL: Each KVL page is deep-copied before serialization. The serialization path mutates the
   * page (addReferences → processEntries, FSST compression, string compression). Without the copy,
   * the insert thread's concurrent deep-copy for CoW would race against these mutations, producing
   * corrupted pages (e.g., zeroed headers, inconsistent slot data).
   * <p>
   * The flush proceeds in sliding windows: each window's pages are deep-copied and pre-serialized IN
   * PARALLEL (the encode caches its output on the copy — the same mechanism the synchronous commit's
   * {@code parallelSerializationOfKeyValuePages} relies on), then a sequential pass appends the
   * cached bytes in snapshot order, records offsets and hashes, and closes the copies. A
   * single-threaded flush cannot keep pace with the insert thread (serialization dominates the
   * flush), which turned the {@code flushPermit} backpressure into a near-synchronous stall; parallel
   * pre-serialization restores the intended overlap.
   */
  private final class AsyncSnapshotWriteTask extends AdmittedSnapshotAppendTask {
    @Override
    public void run() {
      runAsyncSnapshotWriteTask();
    }

    @Override
    boolean executorReleasesAdmission() {
      // runAsyncSnapshotWriteTask releases before COMPLETED/flushPermit publication. Keeping that exact
      // ordering closes the persistent-task reuse race without delaying global append admission.
      return false;
    }

    @Override
    void cancelledByShutdown() {
      if (ASYNC_SNAPSHOT_WRITE_TASK_STATE.compareAndSet(NodeStorageEngineWriter.this, ASYNC_TASK_ENQUEUED,
          ASYNC_TASK_CANCELLED)) {
        final RejectedExecutionException shutdown =
            new RejectedExecutionException("Snapshot append executor shut down before the task ran");
        recordAsyncFlushFailure(shutdown);
        asyncFlushInFlight = false;
        asyncSnapshotIncludesLog = false;
        asyncSnapshotWriteComplete = false;
        asyncFlushWorkerRunning = false;
        flushPermit.release();
      }
    }
  }

  private void runAsyncSnapshotWriteTask() {
    if (ASYNC_SNAPSHOT_WRITE_TASK_STATE.compareAndSet(this, ASYNC_TASK_ENQUEUED, ASYNC_TASK_RUNNING)) {
      try {
        executeSnapshotWrite();
      } catch (final Throwable failure) {
        recordAsyncSnapshotWriteFailure(failure);
      } finally {
        try {
          HftBoundaryTelemetry.asyncCompletion();
        } catch (final Throwable telemetryFailure) {
          recordAsyncSnapshotWriteFailure(telemetryFailure);
        } finally {
          // Release the global admission before publishing writer-local reuse. afterExecute must
          // not repeat this release: the foreground may re-arm this persistent task identity as
          // soon as COMPLETED and flushPermit become visible.
          SNAPSHOT_APPEND_EXECUTOR.releaseTaskAdmission(asyncSnapshotWriteTask);
          ASYNC_SNAPSHOT_WRITE_TASK_STATE.setRelease(this, ASYNC_TASK_COMPLETED);
          asyncFlushWorkerRunning = false;
          flushPermit.release();
        }
      }
    }
  }

  /** Retain and report one worker failure without weakening the mandatory lifecycle release. */
  private void recordAsyncSnapshotWriteFailure(final Throwable failure) {
    recordAsyncFlushFailure(failure);
    try {
      LOGGER.error("Background snapshot flush FAILED — transaction is now in terminal failure state", failure);
    } catch (final Throwable ignored) {
      // The retained poison still carries the original append/serialization failure.
    }
  }

  private void executeSnapshotWrite() {
    if (HFT_TELEMETRY_ENABLED && !isSnapshotAppendWorkerThread()) {
      hftCallerThreadAppendRuns++;
      HFT_GLOBAL_CALLER_APPEND_RUNS.incrementAndGet();
    }
    final long workerStart = HFT_TELEMETRY_ENABLED
        ? System.nanoTime()
        : 0L;
    final long epochId = HFT_TELEMETRY_ENABLED
        ? hftActiveEpochId
        : 0L;
    final long queueWaitNanos = HFT_TELEMETRY_ENABLED && hftActiveEpochSubmittedNanos != 0L
        ? Math.max(0L, workerStart - hftActiveEpochSubmittedNanos)
        : 0L;
    final FileChannelWriter telemetryFileWriter =
        HFT_TELEMETRY_ENABLED && storagePageReaderWriter instanceof FileChannelWriter fileChannelWriter
            ? fileChannelWriter
            : null;
    final long dataGrowCountAtStart = telemetryFileWriter == null
        ? 0L
        : telemetryFileWriter.hftDataAllocationGrowCount();
    final long dataGrowBytesAtStart = telemetryFileWriter == null
        ? 0L
        : telemetryFileWriter.hftDataAllocationGrowBytes();
    final long dataGrowNanosAtStart = telemetryFileWriter == null
        ? 0L
        : telemetryFileWriter.hftDataAllocationGrowNanos();
    long sideNanos = 0L;
    long serializeJoinWaitNanos = 0L;
    long kvlAppendNanos = 0L;
    long finalFlushNanos = 0L;
    long attemptedKvlPagesInEpoch = 0L;
    markAsyncFlushProgress();
    try {
      // A timeout can publish poison just after this task wins ENQUEUED -> RUNNING. The worker now
      // owns the permit, but it need not start an append that can never be published; its finally
      // block still performs the sole release.
      if (asyncFlushTimedOut) {
        return;
      }
      injectAsyncFlushFault("write");
      // One epoch per flush. It bounds how long a refusal mark is honoured (see
      // MAX_SKIPPED_FLUSH_EPOCHS), and under the diagnostic it also separates a page encoded twice
      // inside one flush from a page encoded again in every flush it survives.
      flushEpoch++;
      if (PageSectionDiag.ENABLED) {
        PageSectionDiag.noteFlushEpoch();
      }
      final boolean ownsRetainedAppendBuffer = RETAIN_APPEND_BUFFER && appendBufferInUse.compareAndSet(false, true);
      final BytesOut<?> bgBuffer;
      if (ownsRetainedAppendBuffer) {
        BytesOut<?> retained = retainedAppendBuffer;
        if (retained == null) {
          retained = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
          retainedAppendBuffer = retained;
        }
        // Clear on ACQUIRE, not only on release: a previous epoch that failed mid-append must not
        // be able to leave a prefix for this one to write out.
        retained.clear();
        bgBuffer = retained;
      } else {
        bgBuffer = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
      }
      final PageReference shadowRef = new PageReference();
      try {
        final ResourceConfiguration config = getResourceSession().getResourceConfig();
        shadowRef.setDatabaseId(storageEngineReader.getDatabaseId());
        shadowRef.setResourceId(storageEngineReader.getResourceId());
        final int size = asyncSnapshotIncludesLog
            ? log.getSnapshotSize()
            : 0;
        // Double-buffered sliding windows: while this thread sequentially appends the
        // current window's cached bytes, the NEXT window is already deep-copying and
        // pre-serializing on SNAPSHOT_FLUSH_POOL — the append pass never leaves the
        // workers idle, so the flush keeps pace with the insert thread's rotation cadence.
        KeyValueLeafPage[] currentWindow = null;
        KeyValueLeafPage[] nextWindow = null;
        CompletableFuture<Void> serializeTask = null;
        try {
          if (size > 0) {
            currentWindow = new KeyValueLeafPage[SNAPSHOT_FLUSH_WINDOW];
            nextWindow = new KeyValueLeafPage[SNAPSHOT_FLUSH_WINDOW];
            serializeTask = serializeSnapshotWindowAsync(config, 0, size, currentWindow);
          }

          // The KVL workers can pre-serialize their first window while this sole append owner emits
          // immutable side pages. Both use this one bgBuffer, so offset assignment remains strictly
          // serialized and cannot overlap the foreground writer's buffer/frontier.
          final long sideStart = HFT_TELEMETRY_ENABLED
              ? System.nanoTime()
              : 0L;
          try {
            writeSnapshotSidePages(config, shadowRef, bgBuffer);
          } finally {
            if (HFT_TELEMETRY_ENABLED) {
              sideNanos += System.nanoTime() - sideStart;
            }
          }

          for (int base = 0; base < size; base += SNAPSHOT_FLUSH_WINDOW) {
            final long joinStart = HFT_TELEMETRY_ENABLED
                ? System.nanoTime()
                : 0L;
            try {
              serializeTask.join();
            } finally {
              if (HFT_TELEMETRY_ENABLED) {
                serializeJoinWaitNanos += System.nanoTime() - joinStart;
              }
            }
            serializeTask = null;
            markAsyncFlushProgress();
            final int nextBase = base + SNAPSHOT_FLUSH_WINDOW;
            if (nextBase < size) {
              serializeTask = serializeSnapshotWindowAsync(config, nextBase, size, nextWindow);
            }
            // Sequential pass: append cached bytes in snapshot order, record offsets, close.
            final int end = Math.min(nextBase, size);
            final long kvlAppendStart = HFT_TELEMETRY_ENABLED
                ? System.nanoTime()
                : 0L;
            try {
              int attemptedKvlPages = 0;
              int promotedKvlPages = 0;
              int deferredKvlPages = 0;
              for (int i = base; i < end; i++) {
                final KeyValueLeafPage serializationCopy = currentWindow[i - base];
                if (HFT_TELEMETRY_ENABLED) {
                  final PageContainer snapshotContainer = log.getSnapshotEntry(i);
                  if (snapshotContainer != null && snapshotContainer.getModified() instanceof KeyValueLeafPage) {
                    attemptedKvlPages++;
                    if (serializationCopy == null) {
                      final long slotOffset = log.getSnapshotDiskOffset(i);
                      if (slotOffset == TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL) {
                        promotedKvlPages++;
                      } else if (slotOffset == TransactionIntentLog.SNAPSHOT_RETRY_NEXT_EPOCH) {
                        deferredKvlPages++;
                      }
                    }
                  }
                }
                if (serializationCopy == null) {
                  continue;
                }
                shadowRef.setKey(Constants.NULL_ID_LONG);
                shadowRef.clearHash();
                try {
                  final boolean frameCache;
                  final long encodedBytes;
                  if (HFT_TELEMETRY_ENABLED) {
                    final MemorySegment encoded = serializationCopy.getCompressedSegment();
                    encodedBytes = encoded == null
                        ? 0L
                        : encoded.byteSize();
                    frameCache = compressedSegmentUsesDisposableFrame(serializationCopy);
                  } else {
                    encodedBytes = 0L;
                    frameCache = false;
                  }
                  writeUncommittedPage(config, shadowRef, serializationCopy, bgBuffer);
                  if (HFT_TELEMETRY_ENABLED) {
                    // Count only KVL pages whose synchronous writer call consumed the encoded cache.
                    // This is the exact denominator for the two mutually-exclusive cache outcomes;
                    // snapshotSize also includes structural TIL entries and must never be used here.
                    hftSnapshotKvlPages++;
                    if (frameCache) {
                      hftKvlFrameCachePages++;
                      hftKvlFrameCacheBytes += encodedBytes;
                    } else {
                      hftKvlCacheFallbackPages++;
                      hftKvlCacheFallbackBytes += encodedBytes;
                    }
                  }
                } finally {
                  // Null the slot only once the copy is closed — a write failure must leave
                  // nothing open, and a slot nulled before the write would hide the copy from
                  // closeWindowLeftovers. In-place (adopted) pages are NOT copies: the snapshot
                  // cleanup is their single closer.
                  currentWindow[i - base] = null;
                  if (!serializationCopy.isAdoptedImmutableForFlush()) {
                    serializationCopy.close();
                  }
                }
                log.setSnapshotDiskOffset(i, shadowRef.getKey());
                log.setSnapshotHash(i, shadowRef.getHashAsLong(), shadowRef.hasHash());
              }
              if (HFT_TELEMETRY_ENABLED) {
                // The joined window is the publication fence for every disjoint status-array write.
                // Aggregate once on the sole append owner: no atomic increment sits on serializer
                // workers' hot path, and promotions cannot disappear from cache-coverage telemetry.
                hftSnapshotKvlAttemptedPages += attemptedKvlPages;
                hftSnapshotKvlPromotedPages += promotedKvlPages;
                hftSnapshotKvlDeferredPages += deferredKvlPages;
                attemptedKvlPagesInEpoch += attemptedKvlPages;
              }
            } finally {
              if (HFT_TELEMETRY_ENABLED) {
                kvlAppendNanos += System.nanoTime() - kvlAppendStart;
              }
            }
            final KeyValueLeafPage[] swap = currentWindow;
            currentWindow = nextWindow;
            nextWindow = swap;
            markAsyncFlushProgress();
          }
        } finally {
          // On failure mid-flight, wait out the in-flight serialization (its copies must
          // not leak or race the cleanup below), then release everything still open.
          if (serializeTask != null) {
            final long cleanupJoinStart = HFT_TELEMETRY_ENABLED
                ? System.nanoTime()
                : 0L;
            try {
              serializeTask.join();
            } catch (final Throwable ignored) {
              // The primary failure is already propagating; the join only fences the workers.
            } finally {
              if (HFT_TELEMETRY_ENABLED) {
                serializeJoinWaitNanos += System.nanoTime() - cleanupJoinStart;
              }
            }
          }
          if (currentWindow != null) {
            closeWindowLeftovers(currentWindow);
          }
          if (nextWindow != null) {
            closeWindowLeftovers(nextWindow);
          }
        }
        injectAsyncFlushFault("before-flush");
        final long finalFlushStart = HFT_TELEMETRY_ENABLED
            ? System.nanoTime()
            : 0L;
        try {
          storagePageReaderWriter.flushBufferedWrites(bgBuffer);
        } finally {
          if (HFT_TELEMETRY_ENABLED) {
            finalFlushNanos += System.nanoTime() - finalFlushStart;
          }
        }
        markAsyncFlushProgress();
        injectAsyncFlushFault("after-flush");
      } finally {
        if (ownsRetainedAppendBuffer) {
          // Keep the buffer, drop its contents, then publish the release. The volatile write is what
          // hands the next owner a buffer it can see in full.
          bgBuffer.clear();
          appendBufferInUse.set(false);
        } else {
          bgBuffer.close();
        }
      }
      if (asyncSnapshotIncludesLog) {
        log.markSnapshotFlushComplete();
      }
      // Positive publication last. The semaphore release in finally is the happens-before edge to
      // the foreground, which refuses to publish a single real reference without this marker.
      asyncSnapshotWriteComplete = true;
    } finally {
      if (HFT_TELEMETRY_ENABLED) {
        final long workerNanos = System.nanoTime() - workerStart;
        final long dataGrowCount = telemetryFileWriter == null
            ? 0L
            : telemetryFileWriter.hftDataAllocationGrowCount() - dataGrowCountAtStart;
        final long dataGrowBytes = telemetryFileWriter == null
            ? 0L
            : telemetryFileWriter.hftDataAllocationGrowBytes() - dataGrowBytesAtStart;
        final long dataGrowNanos = telemetryFileWriter == null
            ? 0L
            : telemetryFileWriter.hftDataAllocationGrowNanos() - dataGrowNanosAtStart;

        // Publish the complete primitive phase tuple before the semaphore release below. The
        // foreground's acquire is the sole happens-before edge and can therefore attribute its wait
        // to this exact epoch without volatile fields or a per-epoch result object.
        hftCompletedEpochId = epochId;
        hftCompletedEpochQueueWaitNanos = queueWaitNanos;
        hftCompletedEpochWorkerNanos = workerNanos;
        hftCompletedEpochSideNanos = sideNanos;
        hftCompletedEpochSerializeJoinWaitNanos = serializeJoinWaitNanos;
        hftCompletedEpochKvlAppendNanos = kvlAppendNanos;
        hftCompletedEpochFinalFlushNanos = finalFlushNanos;
        hftCompletedEpochDataGrowCount = dataGrowCount;
        hftCompletedEpochDataGrowBytes = dataGrowBytes;
        hftCompletedEpochDataGrowNanos = dataGrowNanos;
        hftCompletedEpochDataGrowExact = telemetryFileWriter != null;

        hftWorkerRuns++;
        hftWorkerNanos += workerNanos;
        hftSerializeJoinWaitNanosTotal += serializeJoinWaitNanos;
        hftKvlAppendNanosTotal += kvlAppendNanos;
        hftSideNanosTotal += sideNanos;
        hftFinalFlushNanosTotal += finalFlushNanos;
        hftMaxSnapshotKvlAttemptedPages = Math.max(hftMaxSnapshotKvlAttemptedPages, attemptedKvlPagesInEpoch);
        if (workerNanos > hftMaxWorkerNanos) {
          hftMaxWorkerNanos = workerNanos;
          hftMaxWorkerEpochId = epochId;
          hftMaxWorkerEpochQueueWaitNanos = queueWaitNanos;
          hftMaxWorkerEpochSideNanos = sideNanos;
          hftMaxWorkerEpochSerializeJoinWaitNanos = serializeJoinWaitNanos;
          hftMaxWorkerEpochKvlAppendNanos = kvlAppendNanos;
          hftMaxWorkerEpochFinalFlushNanos = finalFlushNanos;
          hftMaxWorkerEpochDataGrowCount = dataGrowCount;
          hftMaxWorkerEpochDataGrowBytes = dataGrowBytes;
          hftMaxWorkerEpochDataGrowNanos = dataGrowNanos;
          hftMaxWorkerEpochDataGrowExact = telemetryFileWriter != null;
        }
      }
    }
  }

  static boolean isSnapshotAppendWorkerThread() {
    return SNAPSHOT_APPEND_WORKER.get();
  }

  static void resetGlobalAppendTelemetry() {
    HFT_GLOBAL_SUBMIT_WAIT_COUNT.set(0L);
    HFT_GLOBAL_SUBMIT_WAIT_NANOS.set(0L);
    HFT_GLOBAL_SUBMIT_WAIT_MAX_NANOS.set(0L);
    HFT_GLOBAL_CALLER_APPEND_RUNS.set(0L);
  }

  static long globalSubmitWaitCount() {
    return HFT_GLOBAL_SUBMIT_WAIT_COUNT.get();
  }

  static long globalSubmitWaitNanos() {
    return HFT_GLOBAL_SUBMIT_WAIT_NANOS.get();
  }

  static long globalSubmitWaitMaxNanos() {
    return HFT_GLOBAL_SUBMIT_WAIT_MAX_NANOS.get();
  }

  static long globalCallerAppendRuns() {
    return HFT_GLOBAL_CALLER_APPEND_RUNS.get();
  }

  static int snapshotAppendActiveWorkers() {
    return SNAPSHOT_APPEND_EXECUTOR.getActiveCount();
  }

  static int snapshotAppendQueuedTasks() {
    return SNAPSHOT_APPEND_EXECUTOR.getQueue().size();
  }

  static int snapshotAppendAdmissionWaiters() {
    return SNAPSHOT_APPEND_EXECUTOR.admissionWaiters();
  }

  static int snapshotAppendAvailableAdmissions() {
    return SNAPSHOT_APPEND_EXECUTOR.availableAdmissions();
  }

  private void markAsyncFlushProgress() {
    asyncFlushProgressNanos = System.nanoTime();
    SNAPSHOT_APPEND_EXECUTOR.signalProgress();
  }

  /**
   * Append every frozen immutable side page and store only its offset in the foreground side-channel.
   */
  private void writeSnapshotSidePages(final ResourceConfiguration config, final PageReference shadowRef,
      final BytesOut<?> bgBuffer) {
    final SidePageBatch sidePages = snapshotSidePages;
    if (sidePages == null) {
      return;
    }
    for (int i = 0; i < sidePages.size; i++) {
      final PageReference liveReference = sidePages.references[i];
      if (liveReference == null || !liveReference.hasPendingPageWrite()
          || !(liveReference.getPage() instanceof OverflowPage overflowPage)) {
        throw new SirixIOException(
            "Immutable side-page batch entry " + i + " lost its resident OverflowPage before background serialization");
      }
      shadowRef.setKey(Constants.NULL_ID_LONG);
      shadowRef.clearHash();
      writeUncommittedPage(config, shadowRef, overflowPage, bgBuffer);
      sidePages.diskOffsets[i] = shadowRef.getKey();
      if ((i & 63) == 63) {
        markAsyncFlushProgress();
      }
      injectAsyncFlushFault("side-write");
    }
  }

  /** Monotonic name source for the prestarted append coordinators. */
  private static final AtomicInteger SNAPSHOT_APPEND_THREAD_ID = new AtomicInteger();

  /**
   * Dedicated, prestarted append coordinators. The old common-pool submission allocated a
   * CompletableFuture/task graph per epoch and offered no queue bound or scheduling isolation. This
   * executor has fixed daemon workers, an array-backed bounded queue, and receives the writer's
   * persistent Runnable identity directly. The coordinator must remain separate from
   * {@link #SNAPSHOT_FLUSH_POOL}: it waits for serializer jobs and would otherwise consume one of
   * their workers (deadlocking the parallelism-one configuration).
   */
  private static final SnapshotAppendExecutor SNAPSHOT_APPEND_EXECUTOR = createSnapshotAppendExecutor();

  private static final ThreadLocal<Boolean> SNAPSHOT_APPEND_WORKER = ThreadLocal.withInitial(() -> Boolean.FALSE);

  abstract static class AdmittedSnapshotAppendTask implements Runnable {
    private final AtomicInteger admissionOwned = new AtomicInteger();

    final void armAdmission() {
      if (!admissionOwned.compareAndSet(0, 1)) {
        throw new IllegalStateException("snapshot append task already owns an admission");
      }
    }

    final boolean releaseAdmission() {
      return admissionOwned.compareAndSet(1, 0);
    }

    final boolean ownsAdmission() {
      return admissionOwned.get() == 1;
    }

    /**
     * Whether {@link SnapshotAppendExecutor#afterExecute(Runnable, Throwable)} owns the normal
     * completion release. A persistent task that releases before publishing another reuse signal must
     * override this: a stale {@code afterExecute} callback from epoch N could otherwise release the
     * same task identity's newly armed admission for epoch N + 1.
     */
    boolean executorReleasesAdmission() {
      return true;
    }

    void cancelledByShutdown() {}
  }

  static final class SnapshotAppendExecutor extends ThreadPoolExecutor {
    private final Semaphore admissions;
    private final AtomicLong progress = new AtomicLong();

    private SnapshotAppendExecutor(final int parallelism, final int queueCapacity) {
      super(parallelism, parallelism, 0L, TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<>(Math.addExact(parallelism, queueCapacity)), runnable -> {
            final Thread thread = new Thread(() -> {
              SNAPSHOT_APPEND_WORKER.set(Boolean.TRUE);
              try {
                runnable.run();
              } finally {
                SNAPSHOT_APPEND_WORKER.remove();
              }
            }, "sirix-snapshot-append-" + SNAPSHOT_APPEND_THREAD_ID.incrementAndGet());
            thread.setDaemon(true);
            return thread;
          }, new AbortPolicy());
      admissions = new Semaphore(parallelism + queueCapacity, true);
    }

    boolean acquireAdmissionUntilProgressStalls(final long stallTimeoutNanos) throws InterruptedException {
      if (stallTimeoutNanos <= 0L) {
        throw new IllegalArgumentException("stallTimeoutNanos must be positive");
      }
      long observedProgress = progress.get();
      long stallStart = System.nanoTime();
      for (;;) {
        final long elapsed = System.nanoTime() - stallStart;
        final long remaining = stallTimeoutNanos - elapsed;
        if (remaining <= 0L) {
          return false;
        }
        if (admissions.tryAcquire(Math.min(remaining, ASYNC_FLUSH_PROGRESS_POLL_NANOS), TimeUnit.NANOSECONDS)) {
          return true;
        }
        final long currentProgress = progress.get();
        if (currentProgress != observedProgress) {
          observedProgress = currentProgress;
          stallStart = System.nanoTime();
        }
      }
    }

    @Override
    public void execute(final Runnable command) {
      if (!(command instanceof final AdmittedSnapshotAppendTask task)) {
        throw new IllegalArgumentException("snapshot append executor accepts admitted tasks only");
      }
      if (!task.ownsAdmission()) {
        throw new IllegalStateException("snapshot append task has no admission");
      }
      super.execute(command);
    }

    void signalProgress() {
      progress.incrementAndGet();
    }

    long progressMarker() {
      return progress.get();
    }

    int availableAdmissions() {
      return admissions.availablePermits();
    }

    int admissionWaiters() {
      return admissions.getQueueLength();
    }

    void releaseTaskAdmission(final AdmittedSnapshotAppendTask task) {
      if (task.releaseAdmission()) {
        admissions.release();
        signalProgress();
      }
    }

    void releaseUnassignedAdmission() {
      admissions.release();
      signalProgress();
    }

    boolean removeAdmitted(final AdmittedSnapshotAppendTask task) {
      final boolean removed = super.remove(task);
      if (removed) {
        releaseTaskAdmission(task);
      }
      return removed;
    }

    @Override
    protected void beforeExecute(final Thread thread, final Runnable runnable) {
      signalProgress();
      super.beforeExecute(thread, runnable);
    }

    @Override
    protected void afterExecute(final Runnable runnable, final Throwable failure) {
      try {
        if (runnable instanceof final AdmittedSnapshotAppendTask task && task.executorReleasesAdmission()) {
          releaseTaskAdmission(task);
        }
      } finally {
        super.afterExecute(runnable, failure);
      }
    }

    @Override
    public List<Runnable> shutdownNow() {
      final List<Runnable> drained = super.shutdownNow();
      Throwable callbackFailure = null;
      for (final Runnable runnable : drained) {
        if (runnable instanceof final AdmittedSnapshotAppendTask task) {
          try {
            task.cancelledByShutdown();
          } catch (final Throwable failure) {
            callbackFailure = retainFirstFailure(callbackFailure, failure);
          } finally {
            releaseTaskAdmission(task);
          }
        }
      }
      if (callbackFailure != null) {
        throw asRuntimeFailure(callbackFailure);
      }
      return drained;
    }
  }

  private static SnapshotAppendExecutor createSnapshotAppendExecutor() {
    final int processors = Runtime.getRuntime().availableProcessors();
    final int parallelism =
        positiveIntProperty("sirix.asyncFlush.appendParallelism", Math.min(2, Math.max(1, processors / 4)));
    // One queued epoch is enough burst absorption for the default two append lanes. Every extra
    // slot can retain a writer's frozen TIL plus up to 64 MiB of side payload while a slow device
    // occupies the workers, so a deep task-count queue is not an acceptable memory bound here.
    final int queueCapacity = positiveIntProperty("sirix.asyncFlush.appendQueueCapacity", 1);
    return createSnapshotAppendExecutor(parallelism, queueCapacity);
  }

  static SnapshotAppendExecutor createSnapshotAppendExecutor(final int parallelism, final int queueCapacity) {
    if (parallelism <= 0 || queueCapacity <= 0) {
      throw new IllegalArgumentException("parallelism and queueCapacity must be positive");
    }
    final SnapshotAppendExecutor executor = new SnapshotAppendExecutor(parallelism, queueCapacity);
    executor.prestartAllCoreThreads();
    return executor;
  }

  /**
   * Dedicated pool for the background snapshot flush's parallel pre-serialization, shared JVM-wide by
   * every resource's flushes (concurrent bulk imports divide it). Capped below the core count on
   * purpose: the flush runs CONCURRENTLY with the insert thread, and letting it fan out across every
   * core halves insert throughput through memory-bandwidth contention (the encode path streams
   * 64&nbsp;KB segments through LZ4/RLE codecs). Two workers keep the flush ahead of the rotation
   * cadence on small hosts while leaving the insert thread its core; larger hosts and multi-import
   * services scale it via {@code -Dsirix.asyncFlush.parallelism} (clamped to ForkJoinPool's maximum
   * of 32767 — an oversized value must degrade, not turn every write transaction into an
   * ExceptionInInitializerError).
   */
  /**
   * The snapshot serialize pool is shared by EVERY writer in the JVM, and its size is the hard
   * ceiling on concurrent flush serialization. The old default of {@code min(2, cores-1)} was sized
   * for one writer — two serializers keep pace with one insert thread's rotation cadence — but it
   * silently capped PARTITIONED parallel ingest at ~2× one writer no matter how many partitions ran:
   * 16 writers spent ~65% of a 1M-row load parked while exactly two threads serialized every page in
   * the process (26.6 s). Sizing the pool to about half the machine restored scaling (18.0 s, 55.7k
   * rows/s, within 19% of the zero-flush bound) and leaves the single-writer full load unchanged
   * (within noise), since idle work-stealing workers cost nothing. Oversubscribing to cores (16
   * serializers + 16 writers on 20 cores) measured WORSE than 8 — hence half, not all.
   */
  private static final ForkJoinPool SNAPSHOT_FLUSH_POOL =
      new ForkJoinPool(Math.min(32767, Math.max(1, Integer.getInteger("sirix.asyncFlush.parallelism",
          Math.max(2, Runtime.getRuntime().availableProcessors() / 2 - 1)))));

  /**
   * Kick off the parallel deep-copy + pre-serialize pass for the snapshot window starting at
   * {@code base} (exclusive end {@code min(base + SNAPSHOT_FLUSH_WINDOW, size)}) on the dedicated
   * flush pool. Each produced copy carries its encoded bytes in the page-local compressed cache, so
   * the subsequent sequential append emits without re-encoding.
   */
  /**
   * Move an async snapshot copy's encoded cache into the native frame the copy already owns.
   *
   * <p>
   * This helper is package-private only for a wire-equivalence regression test. Its caller must own a
   * disposable deep copy after serialization and after ruling out unresolved overflow references: the
   * copy's logical slotted bytes are overwritten, so applying it to a transaction page would destroy
   * rollback state. The append path uses only the exact encoded cache from this point onward and
   * closes the copy immediately after the synchronous write consumes it.
   */
  static boolean relocateSnapshotCacheToDisposableFrame(final KeyValueLeafPage page) {
    requireNonNull(page);
    final MemorySegment encoded = page.getCompressedSegment();
    final int byteHandlerInputLength = page.getByteHandlerInputLength();
    final MemorySegment frame = page.getSlottedPage();
    if (encoded == null || frame == null || !frame.isNative() || frame.isReadOnly()
        || encoded.byteSize() > frame.byteSize()) {
      return false;
    }
    if (!(encoded.isNative() && encoded.address() == frame.address())) {
      MemorySegment.copy(encoded, 0, frame, 0, encoded.byteSize());
    }
    // FileChannelWriter derives the persisted length from byteSize(), so publishing a
    // capacity-sized frame would append stale tail bytes. The cache's volatile store also publishes
    // the completed copy before the window slot is joined by the append owner.
    if (byteHandlerInputLength == KeyValueLeafPage.UNKNOWN_BYTE_HANDLER_INPUT_LENGTH) {
      page.setCompressedSegment(frame.asSlice(0, encoded.byteSize()).asReadOnly());
    } else {
      page.setCompressedSegment(frame.asSlice(0, encoded.byteSize()).asReadOnly(), byteHandlerInputLength);
    }
    return true;
  }

  /** Whether the encoded cache is the exact read-only view of this disposable native frame. */
  private static boolean compressedSegmentUsesDisposableFrame(final KeyValueLeafPage page) {
    final MemorySegment encoded = page.getCompressedSegment();
    final MemorySegment frame = page.getSlottedPage();
    return encoded != null && frame != null && encoded.isNative() && frame.isNative()
        && encoded.address() == frame.address() && encoded.isReadOnly();
  }

  /**
   * Serialize one disposable snapshot copy and retain its identity bytes in the native frame it
   * already owns.
   *
   * <p>
   * The empty pipeline is the canonical ingestion path. Its pooled sink is borrowed, so the copy into
   * the page frame must complete before {@link SerializationBufferPool#release} resets or closes that
   * sink. {@link PageKind} deliberately leaves the cache unpublished for this policy and first
   * finishes {@link KeyValueLeafPage#clearRecordsForGC()}, whose flyweight unbinds still read the
   * logical frame. Only then is it safe to overwrite the disposable copy's frame.
   * </p>
   *
   * <p>
   * Non-empty pipelines keep their existing owned-cache behavior. A cache that fits is relocated
   * while the pooled serializer is still scoped here; a non-fitting owned cache remains valid exactly
   * as before.
   * </p>
   *
   * @param resourceConfig resource serialization configuration
   * @param page caller-owned disposable deep copy
   * @return {@code true} when the copy is ready to append; {@code false} when the original page must
   *         be promoted into the live TIL because its encoded identity bytes cannot safely use the
   *         frame or because overflow references remain unresolved
   */
  static boolean serializeDisposableSnapshotKeyValuePage(final ResourceConfiguration resourceConfig,
      final KeyValueLeafPage page) {
    return serializeDisposableSnapshotKeyValuePage(resourceConfig, page, false);
  }

  /**
   * @param carriersKnownResolved {@code true} when the caller has already classified every carrier
   *        reference of an adopted, immutable page as durable, so the post-serialization reference
   *        pass is provably redundant; {@code false} for any page that may gain carriers while it
   *        serializes
   */
  static boolean serializeDisposableSnapshotKeyValuePage(final ResourceConfiguration resourceConfig,
      final KeyValueLeafPage page, final boolean carriersKnownResolved) {
    requireNonNull(resourceConfig);
    requireNonNull(page);
    final boolean directIdentity = resourceConfig.byteHandlePipeline.isEmpty();
    final var pooledSegment = SerializationBufferPool.INSTANCE.acquire();
    try {
      final PooledBytesOut.IdentityCachePolicy identityCachePolicy = directIdentity
          ? PooledBytesOut.IdentityCachePolicy.CALLER_COPIES_BEFORE_RELEASE
          : PooledBytesOut.IdentityCachePolicy.RETAIN_OWNED_COPY;
      final var bytes = new PooledBytesOut(pooledSegment, identityCachePolicy);
      PageKind.KEYVALUELEAFPAGE.serializeDisposablePage(resourceConfig, bytes, page, SerializationType.DATA);

      if (!carriersKnownResolved && hasUnresolvedOverflowReferences(page)) {
        if (PageSectionDiag.ENABLED) {
          // The encode above ran in full and is about to be dropped: the leaf goes back to the live
          // TIL and the write path encodes it again. Count the work, not the (absent) bytes.
          PageSectionDiag.recordDiscardedEncode(PageSectionDiag.DISCARD_UNRESOLVED_OVERFLOW,
              pooledSegment.position());
        }
        return false;
      }
      if (!directIdentity) {
        // Preserve the configured handler's owned result and fallback semantics. Relocation is only
        // an opportunistic native-cache compaction for this non-canonical configuration.
        relocateSnapshotCacheToDisposableFrame(page);
        return true;
      }

      final long encodedLength = pooledSegment.position();
      final MemorySegment frame = page.getSlottedPage();
      if (encodedLength <= 0L || frame == null || !frame.isNative() || frame.isReadOnly()
          || encodedLength > frame.byteSize()) {
        if (PageSectionDiag.ENABLED) {
          PageSectionDiag.recordDiscardedEncode(PageSectionDiag.DISCARD_FRAME_TOO_SMALL, encodedLength);
        }
        return false;
      }

      // Use the (segment, offset, length) triple directly: asking for a source slice would create an
      // avoidable wrapper per page. The volatile cache store release-publishes the completed native
      // copy; the append owner additionally joins the window's CompletableFuture before reading it.
      MemorySegment.copy(pooledSegment.getCurrentSegment(), 0L, frame, 0L, encodedLength);
      page.setCompressedSegment(frame.asSlice(0L, encodedLength).asReadOnly(), Math.toIntExact(encodedLength));
      return true;
    } finally {
      SerializationBufferPool.INSTANCE.release(pooledSegment);
    }
  }

  private CompletableFuture<Void> serializeSnapshotWindowAsync(final ResourceConfiguration config, final int base,
      final int size, final KeyValueLeafPage[] window) {
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
          final KeyValueLeafPage.OverflowReferenceState carriers = kvl.overflowReferenceState();
          if (carriers == KeyValueLeafPage.OverflowReferenceState.PENDING_SIDE_WRITES
              && kvl.flushDeferrals() < MAX_KVL_FLUSH_DEFERRALS) {
            // Every unresolved carrier of this leaf is a staged side page whose key the next
            // cleanup publishes. Skip the serialization entirely and let cleanupSnapshot()
            // re-promote the leaf for the next epoch — no deep copy, no encoded bytes discarded.
            kvl.noteFlushDeferral();
            if (PageSectionDiag.ENABLED) {
              PageSectionDiag.recordSnapshotDeferral(kvl.getIndexType().getID());
            }
            log.setSnapshotDiskOffset(i, TransactionIntentLog.SNAPSHOT_RETRY_NEXT_EPOCH);
            return;
          }
          if (wasRefusedForUnresolvedCarriers(kvl)) {
            if (PageSectionDiag.ENABLED) {
              PageSectionDiag.recordMarkedArrival();
            }
            // An earlier epoch already encoded this leaf's records and had to drop every byte: the
            // encode itself mints the carriers, and only the recursive final commit can key them.
            // Promoting now reaches exactly that outcome without paying the encode a second time.
            BulkAdoptionDiagnostics.kvlEncodeSkippedForUnresolvedCarriers();
            if (PageSectionDiag.ENABLED) {
              PageSectionDiag.recordSnapshotPromotion(kvl.getIndexType().getID());
            }
            log.setSnapshotDiskOffset(i, TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL);
            return;
          }
          // Bulk-import ADOPTED pages are immutable post-adoption: serialize IN PLACE, skipping
          // the defensive copy (1.4 GB of memcpy per 1M rows on the flush lane). Every refusal
          // exit in the disposable serializer precedes the frame overwrite, so the promote path
          // still sees an untouched page.
          final boolean inPlace = kvl.isAdoptedImmutableForFlush();
          // An adopted leaf materialized its records at adoption, so in-place serialization adds no
          // carrier: the state classified above is final and the serializer's own reference pass
          // can be skipped for it. Every other page may gain carriers WHILE serializing.
          final boolean carriersKnownResolved =
              inPlace && carriers == KeyValueLeafPage.OverflowReferenceState.RESOLVED;
          // NO trie-lane resolution here, deliberately. This runs inside serializeSnapshotWindowAsync's
          // runAsync + parallel forEach, so many ForkJoinPool threads would enter a reader that is
          // declared single-threaded, concurrently mutating currentPageGuard, a parse scratch buffer
          // and the resolver's own maps -- guard-count corruption and torn parses, neither visible to
          // a correctness witness. deepCopy()'s own refuseUnresolvedGlobalTags throws loudly on the
          // exact state a front would have handled, so no front is strictly SAFER than a front here.
          final KeyValueLeafPage serializationCopy = inPlace
              ? kvl
              : kvl.deepCopy();
          try {
            if (!serializeDisposableSnapshotKeyValuePage(config, serializationCopy, carriersKnownResolved)) {
              // Overlong records still carrying NULL disk keys and identity encodings larger than
              // the disposable native frame cannot enter this window. Mark the slot so
              // cleanupSnapshot() promotes the ORIGINAL page into the live TIL, where recursive
              // final commit either resolves the overflow or serializes with an ordinary owned
              // cache. No borrowed pooled alias is ever published on this path.
              final KeyValueLeafPage.OverflowReferenceState refusedState =
                  serializationCopy.overflowReferenceState();
              if (PageSectionDiag.ENABLED) {
                PageSectionDiag.recordRefusalCarrierState(switch (refusedState) {
                  case RESOLVED -> PageSectionDiag.REFUSAL_CARRIERS_RESOLVED;
                  case PENDING_SIDE_WRITES -> PageSectionDiag.REFUSAL_CARRIERS_PENDING;
                  case UNRESOLVED -> PageSectionDiag.REFUSAL_CARRIERS_UNRESOLVED;
                });
              }
              if (refusedState == KeyValueLeafPage.OverflowReferenceState.UNRESOLVED) {
                // Read the copy's reference map BEFORE closing it: this is the only moment the
                // CAUSE of the refusal is observable, and it is the cause that predicts the next
                // epoch. UNRESOLVED is the permanent one — a carrier that is neither durable nor
                // staged can only be keyed by the recursive final commit. PENDING_SIDE_WRITES is
                // deliberately NOT marked: those carriers gain keys one epoch later and the
                // deferral arm above exists to let exactly those pages through. Nor does a
                // frame-size refusal mark, since a later encode of the page can fit.
                noteRefusedOverflowLeaf(serializationCopy);
                BulkAdoptionDiagnostics.kvlEncodeDiscardedForUnresolvedCarriers();
              }
              if (!inPlace) {
                serializationCopy.close();
              }
              if (carriers == KeyValueLeafPage.OverflowReferenceState.PENDING_SIDE_WRITES) {
                // Reached only past the deferral cap: the ordering that publishes staged carriers
                // before re-promotion has failed. Pinning keeps the leaf correct; the counter makes
                // the regression visible instead of letting the arena tell the story hours later.
                BulkAdoptionDiagnostics.kvlPagePinnedAfterDeferralCap();
              }
              if (PageSectionDiag.ENABLED) {
                PageSectionDiag.recordSnapshotPromotion(kvl.getIndexType().getID());
              }
              log.setSnapshotDiskOffset(i, TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL);
              return;
            }
          } catch (final Throwable t) {
            // A copy that never reached the window would be invisible to
            // closeWindowLeftovers — release its pooled segments before recording.
            if (!inPlace) {
              serializationCopy.close();
            }
            if (PageSectionDiag.ENABLED) {
              PageSectionDiag.recordDiscardedEncode(PageSectionDiag.DISCARD_SERIALIZATION_FAILED, 0L);
            }
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
   * {@code true} when serialization left overflow {@link PageReference}s on {@code page} whose disk
   * keys are still unassigned — such a page's encoded form is only valid after the recursive commit
   * writes its OverflowPages (#1076).
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
        if (!leftover.isAdoptedImmutableForFlush()) {
          leftover.close();
        }
      }
    }
  }

  /**
   * Invalidate all local container caches to prevent stale cache hits returning frozen-zone
   * containers after snapshot.
   */
  private void clearLocalContainerCaches() {
    currentNamePage = null;
    pageContainerCache.clear();
    documentRecordLocationCache.clear();
    mostRecentPageContainer.set(IndexType.DOCUMENT, -1, -1, -1, null);
    secondMostRecentPageContainer.set(IndexType.DOCUMENT, -1, -1, -1, null);
    mostRecentPathSummaryPageContainer.set(IndexType.PATH_SUMMARY, -1, -1, -1, null);
    clearMostRecentByIndexTypeSlots();
  }

  /**
   * Invalidate every per-{@link IndexType} most-recent slot. Holder objects are kept allocated
   * (zero-alloc steady state) — the {@code recordPageKey = -1} sentinel can never match a real
   * lookup, and dropping the {@link PageContainer} reference prevents both stale hits and pinned
   * garbage.
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
   * After snapshot, the current TIL is empty. Structural pages (RevisionRootPage, PathSummaryPage,
   * NamePage, etc.) are in the frozen snapshot. We re-add them to the current TIL so the insert
   * thread can continue without CoW overhead for these frequently-accessed pages.
   * <p>
   * IndirectPages in the trie are NOT re-added — they will be CoW'd on first access via
   * prepareIndirectPage() if needed.
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
    reAddPageIfFrozen(newRevisionRootPage.getProjectionIndexPageReference());
    reAddPageIfFrozen(newRevisionRootPage.getValidTimeIndexPageReference());
  }

  /**
   * If a page reference is in the frozen snapshot, re-add its container to the current TIL. This
   * ensures the insert thread can continue modifying structural pages without CoW.
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
   * Deep-copy a frozen PageContainer for Copy-on-Write. Both complete and modified KVL pages are
   * deep-copied to ensure full independence from the frozen originals.
   *
   * @param container the frozen container to copy
   * @return a fully independent deep copy
   */
  private PageContainer deepCopyFrozenContainer(final PageContainer container) {
    final var frozenModified = (KeyValueLeafPage) container.getModified();
    refuseAdoptedImmutablePage(frozenModified, "copy-on-write");
    final var frozenComplete = (KeyValueLeafPage) container.getComplete();
    // No trie-lane resolution here either. This looks like the safe synchronous prepare path, and it
    // is not one: it runs inside pageContainerCache.computeIfAbsent, so a dictionary walk from here
    // would be a map compute wrapping further cache work -- the same shape that keeps resolution out
    // of the record-page cache's loader. deepCopy()'s refusal covers it, loudly.
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
        if (reference.hasPendingPageWrite()) {
          throw new SirixIOException("Recursive commit reached an immutable side page whose background write is "
              + "still pending — awaitPendingAsyncFlush must drain every active side-page batch before root "
              + "serialization");
        }
        writeUncommittedPage(getResourceSession().getResourceConfig(), reference, sideMapPage, bufferBytes);
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
        throw new SirixIOException("Commit traversal hit an unresolvable stale page reference (logKey="
            + reference.getLogKey() + ", generation=" + reference.getActiveTilGeneration()
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
        final PageReference originalRef = log.getOriginalRef(reference);
        if (originalRef != null && originalRef != reference) {
          if (originalRef.getKey() >= 0) {
            reference.setKey(originalRef.getKey());
            reference.copyHashFrom(originalRef);
          }
        }
      }
      return;
    }

    // Recursively commit indirectly referenced pages and then write self.
    page.commit(this);
    final PageReference.TransactionLogReference logReference = reference.transactionLogReference();
    writeUncommittedPage(getResourceSession().getResourceConfig(), reference, page, bufferBytes);
    PageReference.completeTransactionLogReference(logReference, reference.getKey(), reference.getHashAsLong(),
        reference.hasHash());

    // Propagate disk offset to TIL back-reference so other PageReference copies
    // (from CoW'd indirect pages sharing the same logKey) can resolve the disk key
    // when they hit the isClosed() guard in a subsequent commit(ref) call.
    final int refLogKey = reference.getLogKey();
    if (refLogKey >= 0) {
      final PageReference backRef = log.getOriginalRef(reference);
      if (backRef != null && backRef != reference && backRef.getKey() < 0) {
        backRef.setKey(reference.getKey());
        backRef.copyHashFrom(reference);
      }
    }
    reference.refreshTransactionLogReference();

    container.getComplete().close();
    page.close();

    // Remove page reference.
    reference.setPage(null);
  }

  @Override
  public UberPage commit(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp,
      final boolean isAutoCommitting, final boolean isIntermediateCommit) {
    storageEngineReader.assertNotClosed();
    assertTransactionWritable();

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
    assertTransactionWritable();

    // Keep the page-graph invariant local to the storage writer: no public commit path may reach
    // recursive HOT serialization while a side reference still carries the pending marker. The
    // node transaction already calls this barrier, but direct StorageEngineWriter clients and
    // future orchestrators must be safe too.
    awaitPendingAsyncFlush();

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
      final long t0 = timing
          ? System.nanoTime()
          : 0;
      // Must precede the serialization pass: pages need the revision's symbol-table id in hand
      // by the time their bytes are cached, and the pass below is the last point before that.
      buildRevisionFsstSymbolTable();
      parallelSerializationOfKeyValuePages();

      final long t1 = timing
          ? System.nanoTime()
          : 0;
      LOGGER.debug("TIL size before recursive commit: {}", log.getList().size());
      uberPage.commit(this);
      LOGGER.debug("TIL size after recursive commit: {} (closed entries not cleaned)", log.getList().size());

      // Flush the buffered page tail WITHOUT any barrier: after phase 1 every revision-N page
      // must be readable by offset through any reader channel (the pipelined successor epoch
      // reads the pending revision's pages before phase 2 hardens). Plain write(2), no fsync —
      // the durability barriers remain phase 2's job.
      storagePageReaderWriter.flushBufferedWrites(bufferBytes);

      if (timing) {
        LOGGER.debug("Commit phase 1 r{}: serialize={}ms recursive={}ms", uberPage.getRevisionNumber(), ms(t1 - t0),
            ms(System.nanoTime() - t1));
      }
      return uberPage;
    }
  }

  @Override
  public void hardenCommit(final UberPage uberPage, final boolean isIntermediateCommit) {
    {
      final boolean timing = LOGGER.isDebugEnabled();

      final Path commitFile = storageEngineReader.resourceSession.getCommitFile();

      final PageReference uberPageReference = new PageReference().setDatabaseId(storageEngineReader.getDatabaseId())
                                                                 .setResourceId(storageEngineReader.getResourceId());
      uberPageReference.setPage(uberPage);

      final long t2 = timing
          ? System.nanoTime()
          : 0;

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

      final long t3 = timing
          ? System.nanoTime()
          : 0;

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
      // The beacon now makes every foreground prewrite part of a durable reachable revision; an
      // eventual close must not invalidate those committed offsets.
      hasUncommittedReclaimableWrites = false;
      firstUncommittedPageOffset = Long.MAX_VALUE;
      closeCursorDurableReadPage();

      final long t4 = timing
          ? System.nanoTime()
          : 0;

      // CRITICAL: Release current page guard BEFORE TIL.clear()
      // If guard is on a TIL page, the page won't close (guardCount > 0 check)
      currentNamePage = null;
      storageEngineReader.closeCurrentPageGuard();

      // Clear TransactionIntentLog - closes all modified pages. clear() drains the WHOLE log
      // before rethrowing a retained page-close failure, so by the time it throws the memoized
      // container slots below already reference closed, pooled pages — they must be reset on
      // both exits (rollback and close guard their clears the same way), or a transaction that
      // survives the rethrow keeps serving recycled frames out of its memo.
      try {
        log.clear();
      } finally {
        clearLocalContainerCaches();
      }

      final long t5 = timing
          ? System.nanoTime()
          : 0;

      // Delete commit file which denotes that a commit must write the log in the data file.
      try {
        deleteIfExists(commitFile);
      } catch (final IOException e) {
        throw new SirixIOException("Commit file couldn't be deleted!");
      }

      if (timing) {
        LOGGER.debug("Commit phase 2 r{}: indexDefs={}ms uberWrite={}ms tilClear={}ms total={}ms", revision,
            ms(t3 - t2), ms(t4 - t3), ms(t5 - t4), ms(t5 - t2));
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

  /**
   * How many string samples the commit-wide sweep gathers before it stops looking. Generous relative
   * to {@link FSSTCompressor#MAX_SAMPLES_TO_ANALYZE} (which further filters by length), yet a fixed
   * bound, so the sweep's cost never scales with commit size.
   */
  private static final int FSST_REVISION_SAMPLE_CAP = 1024;

  /**
   * The database type of this writer's resource; delegated so reader and writer share one derivation.
   */
  private DatabaseType databaseTypeOfSession() {
    return storageEngineReader.databaseType();
  }

  /**
   * Build this revision's FSST symbol table — once, from strings pooled across the whole commit —
   * store it as a record in the name dictionary's trie, and hand every document page the table plus
   * its dictionary id before serialization begins.
   *
   * <p>
   * This replaces the per-page build that {@code PageKind.serializePage} used to run, which failed in
   * both directions at once: a full slot scan plus frequency analysis per page made ingest 18×
   * slower, and a single page rarely holds the {@link FSSTCompressor#MIN_SAMPLES_FOR_TABLE} strings a
   * table needs before it beats raw bytes, so the table was rejected on essentially every page
   * anyway. Pooling inverts both: one build per commit, fed by more samples than any page could
   * supply.
   *
   * <p>
   * Handing the pages the table itself (not just the id) is what serialization needs —
   * {@code compressStringValues} encodes against it, and the id is what {@code writeFsstSymbolTable}
   * emits in place of the table's bytes. The table record is created <em>between</em> the sampling
   * sweep and the hand-out sweep, because creating it mutates the transaction intent log this method
   * iterates.
   *
   * <p>
   * When the samples are too few or compression would not pay, no table is stored and pages serialize
   * their strings raw — which is also exactly what happens for resources whose configuration disables
   * FSST.
   */
  private void buildRevisionFsstSymbolTable() {
    final var resourceConfig = getResourceSession().getResourceConfig();
    if (resourceConfig.stringCompressionType != StringCompressionType.FSST) {
      return;
    }

    // Sampling skips pages already bound to a table: their raw leftovers will be encoded against
    // THEIR table at commit, so they say nothing about what an unbound page needs. Known bias:
    // with insert-time encoding, a page gets bound the moment the current table compresses one
    // of its strings, so the samples that remain skew toward strings that table did NOT engage
    // with — the reuse trial below judges the previous table on its hardest cases. That is a
    // conservative error (worst case one unnecessary rebuild), and measured on real ingest the
    // trial still reuses on the overwhelming majority of commits.
    final List<byte[]> samples = new ArrayList<>(FSST_REVISION_SAMPLE_CAP);
    int documentPages = 0;
    for (final PageContainer container : log.getList()) {
      if (container.getModified() instanceof KeyValueLeafPage kvl && needsSymbolTable(kvl)) {
        documentPages++;
        kvl.collectFsstStringSamples(samples, FSST_REVISION_SAMPLE_CAP);
        if (samples.size() >= FSST_REVISION_SAMPLE_CAP) {
          break;
        }
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("FSST commit sweep: docPages={} samples={}", documentPages, samples.size());
    }
    if (samples.size() < FSSTCompressor.MIN_SAMPLES_FOR_TABLE) {
      return;
    }

    final NamePage namePage = getNamePage(newRevisionRootPage);
    final DatabaseType databaseType = databaseTypeOfSession();

    // Reuse the previous revision's table when it still compresses this commit's strings well.
    // Vocabulary is extremely stable within a resource, so on a long ingest almost every commit
    // reuses — which skips the five-pass fixed-point build (the most expensive step of FSST-on
    // ingest), stores nothing new, and lets every page of the resource share one parsed table
    // and one matcher through the identity caches.
    final long previousId = namePage.getLatestFsstSymbolTableId(databaseType);
    if (previousId > 0) {
      // The insert path usually resolved this exact table already this transaction — reusing
      // ITS byte[] both skips a per-commit trie walk and keeps one array identity flowing to
      // every page, which is what the parse/matcher identity caches key on.
      final byte[] previousTable = insertFsstResolved && insertFsstTableId == previousId
          ? insertFsstTable
          : namePage.getFsstSymbolTable(previousId, databaseType, this);
      if (previousTable != null && FSSTCompressor.isCompressionBeneficial(samples, previousTable)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("FSST reusing table id {} ({}B)", previousId, previousTable.length);
        }
        distributeFsstSymbolTable(previousTable, previousId);
        return;
      }
    }

    final byte[] table = FSSTCompressor.buildSymbolTable(samples);
    final boolean beneficial =
        table != null && table.length > 0 && FSSTCompressor.isCompressionBeneficial(samples, table);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("FSST built table: {}B beneficial={}", table == null
          ? -1
          : table.length, beneficial);
    }
    if (!beneficial) {
      return;
    }

    // Store the table as a versioned record. Appended, never overwritten: pages of earlier
    // revisions keep decoding against the tables they name, no matter how often this rebuilds.
    final long id = namePage.setFsstSymbolTable(table, databaseType, this, log);

    distributeFsstSymbolTable(table, id);
  }

  /**
   * The resource's current FSST symbol table, loaded once per transaction for insert-time encoding.
   * {@code insertFsstResolved} distinguishes "not looked up yet" from "looked up, resource has none"
   * so absence costs one lookup, not one per string.
   */
  private boolean insertFsstResolved;
  private byte[] insertFsstTable;
  private long insertFsstTableId;
  private byte[][] insertFsstParsed;

  /** Whether this resource FSST-compresses strings; final, so the insert path pays one field read. */
  private final boolean insertFsstEnabled;

  /**
   * Encode a string-carrying record against the resource's current symbol table as it is inserted,
   * and hand its page the table at the same moment.
   *
   * <p>
   * This is where FSST's ingest cost belongs. Commit-time compression re-reads, re-encodes and
   * rewrites every string of the commit after the fact — a full extra pass that made FSST slower end
   * to end than the generic byte codec it was meant to beat. Encoding here rides work the insert
   * already does (the value is in hand, the record is being serialized anyway), so the marginal cost
   * is the encode itself; the commit pass then skips every slot that arrives already compressed. The
   * first-ever commit still bootstraps through the commit-time path, because no table exists until it
   * stores one.
   *
   * <p>
   * Tagging the page immediately — bytes and id together — keeps every in-transaction read correct
   * (flyweight and record decodes resolve through the page) and makes the commit-time distribution
   * skip the page ({@code carriesSymbolTable}), preserving the rule that a page binds to exactly one
   * table for life.
   */
  private void maybeEncodeStringValueAtInsert(final DataRecord record, final KeyValuePage<DataRecord> page) {
    if (!insertFsstEnabled) {
      return;
    }
    // Only pages that can persist a symbol-table reference may receive encoded bytes. A record
    // mutated to compressed form on any other page kind would reach disk with no table named
    // anywhere — unreadable after reopen.
    if (!(page instanceof KeyValueLeafPage kvl)) {
      return;
    }
    // Extract, encode and store within one typed branch — the record's type is established
    // once, not re-dispatched after the encode.
    if (record instanceof ObjectNamedStringNode fusedNode && !fusedNode.isCompressed()) {
      final byte[] raw = fusedNode.getRawValueWithoutDecompression();
      if (raw != null) {
        final byte[] encoded = encodeForInsert(kvl, raw, 0, raw.length);
        if (encoded != null) {
          fusedNode.setRawValue(encoded, true, insertFsstTable);
        }
      }
    } else if (record instanceof StringNode stringNode && !stringNode.isCompressed()) {
      final byte[] raw = stringNode.getRawValueWithoutDecompression();
      if (raw != null) {
        final byte[] encoded = encodeForInsert(kvl, raw, 0, raw.length);
        if (encoded != null) {
          stringNode.setRawValue(encoded, true, insertFsstTable);
        }
      }
    }
  }

  @Override
  public byte[] encodeStringValueForInsert(final KeyValueLeafPage page, final byte[] value, final int off,
      final int len) {
    if (page == null || value == null || !insertFsstEnabled) {
      return null;
    }
    return encodeForInsert(page, value, off, len);
  }

  /**
   * The shared insert-time encode core: encode {@code raw[off..off+len)} against the transaction's
   * table if — and only if — the target page can legally hold the result.
   *
   * <p>
   * The table is a property of the PAGE the record lands on, not of the transaction: a page binds to
   * exactly one table for life, and the record's bytes must be decodable through that binding. Three
   * cases:
   * <ul>
   * <li>page bound to this transaction's table: encode.</li>
   * <li>page unbound: encode, then bind it (bytes and id together).</li>
   * <li>page bound to a DIFFERENT table (a tail page from before a vocabulary-shift rebuild): leave
   * the value raw. Encoding with the latest table would store bytes the page's persisted table id
   * cannot decode — silent corruption after reload — and encoding with the page's own table would
   * require resolving it here, for a case reuse makes rare. Raw is always correct; commit-time
   * compression picks it up when the page's bytes are resolved, and never otherwise.</li>
   * </ul>
   *
   * @return the encoded bytes (strictly shorter than {@code len}), or {@code null} to store raw
   */
  private byte[] encodeForInsert(final KeyValueLeafPage kvl, final byte[] raw, final int off, final int len) {
    if (len < FSSTCompressor.MIN_COMPRESSION_SIZE) {
      return null;
    }
    // A value whose BEST-case encoding (8-byte symbols, 8:1) still cannot fit the largest
    // slotted page is bound for overflow storage no matter what — encoding it here is O(len)
    // work thrown away. It stays raw now; the commit-time records[] pass compresses it if
    // worthwhile.
    if ((len >>> 3) > KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY) {
      return null;
    }
    if (!insertFsstResolved) {
      loadInsertFsstTable();
    }
    if (insertFsstTable == null) {
      return null;
    }
    final long pageTableId = kvl.getFsstSymbolTableId();
    final boolean pageUnbound =
        pageTableId == KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID && kvl.getFsstSymbolTable() == null;
    if (!pageUnbound && pageTableId != insertFsstTableId) {
      return null;
    }
    // Slice-encode straight from the caller's buffer; null means "did not shrink, store raw".
    final byte[] encoded = FSSTCompressor.encodeOrNull(raw, off, len, insertFsstParsed);
    if (encoded == null) {
      return null;
    }
    if (pageUnbound) {
      kvl.setFsstSymbolTable(insertFsstTable);
      kvl.setFsstSymbolTableId(insertFsstTableId);
    } else if (kvl.getFsstSymbolTable() == null) {
      // Bound by id with bytes not yet resolved (same id as ours, so same table). Fill the
      // bytes in by construction: every in-transaction read of the value we just compressed
      // resolves through the page, and leaving resolution to the combine funnel would make
      // this write's readability depend on an invariant enforced two files away.
      kvl.setFsstSymbolTable(insertFsstTable);
    }
    return encoded;
  }

  /**
   * Resolve the latest stored table from the new revision root's NamePage, whose bookkeeping is
   * copied from the previous revision.
   */
  private void loadInsertFsstTable() {
    insertFsstResolved = true;
    final NamePage namePage = getNamePage(newRevisionRootPage);
    final DatabaseType databaseType = databaseTypeOfSession();
    final long previousId = namePage.getLatestFsstSymbolTableId(databaseType);
    if (previousId <= 0) {
      return;
    }
    final byte[] table = namePage.getFsstSymbolTable(previousId, databaseType, this);
    if (table == null || table.length == 0) {
      return;
    }
    insertFsstTable = table;
    insertFsstTableId = previousId;
    insertFsstParsed = FSSTCompressor.parsedFor(table);
  }

  /**
   * Hand every eligible document page in the log the revision's table and its id.
   */
  private void distributeFsstSymbolTable(final byte[] table, final long id) {
    // Second sweep, after the record creation above has finished mutating the log. Document pages
    // only — dictionary and index pages hold no STRING_VALUE records, so a table would be dead
    // weight in their bytes.
    //
    // Pages that already carry a table are left strictly alone. A page can hold exactly one
    // table, and a modified page's untouched slots are still compressed against the table it was
    // read with (compressStringValues skips compressed slots — it cannot re-encode them).
    // Handing such a page the new table would put old-table bytes under a new-table claim: every
    // string written before this commit would silently decode to garbage — and since the page
    // instance may be the cached one, even reads of OLDER revisions can see the poisoned claim.
    // Keeping the old table is fully correct the other way around: the page's new raw strings
    // are encoded against it, its id stays on the page, and append-only storage guarantees that
    // table remains reachable forever.
    for (final PageContainer container : log.getList()) {
      if (container.getModified() instanceof KeyValueLeafPage kvl && needsSymbolTable(kvl)) {
        kvl.setFsstSymbolTable(table);
        kvl.setFsstSymbolTableId(id);
      }
    }
  }

  /**
   * Whether this page may still be given a symbol table: it can hold string-compressed records
   * (document index) and is not already bound to a table. The single statement of the eligibility
   * rule shared by the sampling sweep and the distribution sweep.
   */
  private static boolean needsSymbolTable(final KeyValueLeafPage page) {
    return page.getIndexType() == IndexType.DOCUMENT && !carriesSymbolTable(page);
  }

  /**
   * Whether this page is already bound to a symbol table — by bytes, by reference, or both.
   *
   * <p>
   * Such a page keeps that binding for life: its compressed slots were encoded against it and cannot
   * be re-encoded in place, so both the revision build's sampling (its raw strings will be encoded
   * against the old table, not the new one) and its distribution (see above) must pass the page over.
   */
  private static boolean carriesSymbolTable(final KeyValueLeafPage page) {
    return page.getFsstSymbolTable() != null || page.getFsstSymbolTableId() != KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID;
  }

  private void parallelSerializationOfKeyValuePages() {
    final var resourceConfig = getResourceSession().getResourceConfig();
    final var logList = log.getList();

    if (logList.size() < PARALLEL_SERIALIZATION_THRESHOLD) {
      // Sequential: iterate directly — no intermediate collection
      for (final var container : logList) {
        final var modified = container.getModified();
        if (modified instanceof KeyValueLeafPage) {
          prepareFinalCommitKeyValuePage(resourceConfig, modified);
        }
      }
    } else {
      // Parallel: stream-filter avoids materializing an intermediate ArrayList
      logList.parallelStream()
             .map(PageContainer::getModified)
             .filter(p -> p instanceof KeyValueLeafPage)
             .forEach(page -> prepareFinalCommitKeyValuePage(resourceConfig, page));
    }
  }

  /**
   * Prepare one live KVL page for final recursive commit.
   *
   * <p>
   * Configured handlers retain the parallel pre-serialization path: their encoded cache avoids
   * running the handler again during the sequential append. An empty pipeline has no transform to
   * amortize and retaining its identity result creates one page-sized heap object per live tail page.
   * For that case, this pass performs only the page preparation which benefits from parallel
   * execution: materialize the frame, compress strings, discover/materialize overflow references, and
   * release record objects. String compression must precede
   * {@link KeyValueLeafPage#addReferences(ResourceConfiguration)} so records which FSST can shrink
   * are not prematurely diverted to overflow storage.
   * </p>
   *
   * <p>
   * Recursive commit subsequently calls {@code addReferences} idempotently, recursively appends every
   * overflow page discovered here, and only then does the writer encode the owning leaf once into its
   * reusable synchronous scratch. This avoids both a second wire/PAX/body encode and an owned
   * identity copy. The logical frame is never repurposed, so an I/O failure still leaves
   * rollback/retry state available.
   * </p>
   *
   * @return {@code true} when the page holds an encoded cache after preparation; {@code false}
   *         otherwise (including a configured handler that declined an unresolved-overflow page)
   */
  static boolean prepareFinalCommitKeyValuePage(final ResourceConfiguration resourceConfig, final Page page) {
    if (resourceConfig.byteHandlePipeline.isEmpty()) {
      final KeyValueLeafPage keyValueLeafPage = (KeyValueLeafPage) page;
      keyValueLeafPage.ensureSlottedPage();
      keyValueLeafPage.compressStringValues();
      keyValueLeafPage.addReferences(resourceConfig);
      keyValueLeafPage.clearRecordsForGC();
      return false;
    }

    final var pooledSeg = SerializationBufferPool.INSTANCE.acquire();
    try {
      final var bytes = new PooledBytesOut(pooledSeg);
      PageKind.KEYVALUELEAFPAGE.serializePage(resourceConfig, bytes, page, SerializationType.DATA);
      final KeyValueLeafPage keyValueLeafPage = (KeyValueLeafPage) page;
      return keyValueLeafPage.getCompressedSegment() != null || keyValueLeafPage.getBytes() != null;
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

  /**
   * Create the commit marker, tolerating a marker that is already there.
   *
   * <p>
   * This runs at the head of EVERY commit, before any data is written. The previous
   * {@code while (!Files.exists(file)) Files.createFile(file)} paid a {@code stat} on top of the
   * {@code create} — and the loop could only ever run twice, since {@code createFile} either succeeds
   * or throws. Creating directly and treating "already exists" as success is the same outcome in one
   * syscall, and it is also race-free: two callers no longer both observe "absent" and race into
   * {@code createFile}, where the loser previously surfaced the {@link FileAlreadyExistsException} as
   * a commit failure.
   *
   * @param file the commit marker path
   */
  private void createIfAbsent(final Path file) {
    try {
      Files.createFile(file);
    } catch (final FileAlreadyExistsException alreadyThere) {
      // A marker left by an interrupted commit, or a concurrent creator — either way it is present,
      // which is all this method promises.
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private void setUserIfPresent() {
    final Optional<User> optionalUser = storageEngineReader.resourceSession.getUser();
    optionalUser.ifPresent(newRevisionRootPage::setUser);
  }

  /**
   * Retain the first teardown failure and attach later failures without letting diagnostics replace
   * it.
   */
  private static Throwable retainFirstFailure(final @Nullable Throwable first, final Throwable next) {
    if (first == null) {
      return next;
    }
    if (first != next) {
      try {
        first.addSuppressed(next);
      } catch (final Throwable ignored) {
        // Suppression is diagnostic only. Under OOME the original failure must still win.
      }
    }
    return first;
  }

  /** Preserve the original unchecked failure type after best-effort teardown has completed. */
  private static RuntimeException asRuntimeFailure(final Throwable failure) {
    if (failure instanceof Error error) {
      throw error;
    }
    if (failure instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    return new SirixIOException(failure);
  }

  /**
   * Failure logging is best-effort and must never interrupt rollback cleanup under memory pressure.
   */
  private static void logRollbackFailure(final Throwable failure) {
    try {
      LOGGER.error("Rollback cleanup completed with failure", failure);
    } catch (final Throwable ignored) {
      // The retained throwable is the authoritative diagnostic.
    }
  }

  /**
   * Close diagnostics are best-effort; resource release and terminal flags always take precedence.
   */
  private static void logCloseFailure(final String message, final Throwable failure) {
    try {
      LOGGER.error(message, failure);
    } catch (final Throwable ignored) {
      // The retained throwable is the authoritative diagnostic.
    }
  }

  /**
   * Drop this resource's global page-cache entries before an aborted backend tail can be reused.
   *
   * <p>
   * A reclaimable backend starts the next writer at the prior durable frontier, so an abort
   * intentionally overwrites every page appended ahead of the commit beacon. Any cache entry
   * populated from those bytes must be gone before that reuse.
   * </p>
   */
  private void invalidateCachesForAbortedUncommittedWrites() {
    if (!hasUncommittedReclaimableWrites) {
      firstUncommittedPageOffset = Long.MAX_VALUE;
      return;
    }
    // Use the retained immutable session/config directly: close() invokes this after the reader has
    // been closed, where the forwarding accessor correctly refuses normal read operations.
    final ResourceConfiguration resourceConfiguration = storageEngineReader.resourceSession.getResourceConfig();
    Databases.clearCachesForResource(resourceConfiguration.getDatabaseId(), resourceConfiguration.getID());
    hasUncommittedReclaimableWrites = false;
    firstUncommittedPageOffset = Long.MAX_VALUE;
  }

  @Override
  public UberPage rollback() {
    storageEngineReader.assertNotClosed();

    Throwable asyncFailure = null;

    // Fence the append owner before touching either reusable batch. A failed worker is still fully
    // fenced once its permit is acquired; retain its error for diagnostics, cancel the payloads, and
    // complete the abort. A latched write failure does not make a successfully completed rollback
    // fail — the caller is discarding that write precisely because it failed.
    try {
      awaitInFlightAsyncFlushOnly();
    } catch (final Throwable t) {
      asyncFailure = t;
    }
    if (asyncFlushWorkerRunning) {
      final Throwable unfencedFailure =
          asyncFlushFailure("Rollback cannot tear down pages while the async flush worker is still running");
      if (asyncFailure != null && asyncFailure != unfencedFailure.getCause()) {
        retainFirstFailure(unfencedFailure, asyncFailure);
      }
      logRollbackFailure(unfencedFailure);
      throw asRuntimeFailure(unfencedFailure);
    }

    Throwable teardownFailure = null;

    // Cancel pending references and release their payloads BEFORE guard/TIL cleanup. Those later
    // operations can invoke cache/page close machinery and throw; no such failure may leave the
    // transaction's bounded-but-large side batches retained.
    try {
      discardSidePageBatches();
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    try {
      closeCursorDurableReadPage();
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    // CRITICAL: Release current page guard BEFORE TIL.clear()
    // If guard is on a TIL page, the page won't close (guardCount > 0 check)
    try {
      storageEngineReader.closeCurrentPageGuard();
      injectAsyncFlushFault("rollback-after-guard-close");
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    // Clear TransactionIntentLog - closes all modified pages (including snapshot pages)
    try {
      log.clear();
      injectAsyncFlushFault("rollback-after-log-clear");
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    // Clear local cache and reset references (pages already handled by log.clear())
    try {
      clearLocalContainerCaches();
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    try {
      invalidateCachesForAbortedUncommittedWrites();
      injectAsyncFlushFault("rollback-after-trie-cache-invalidate");
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    if (teardownFailure != null) {
      if (asyncFailure != null) {
        retainFirstFailure(teardownFailure, asyncFailure);
      }
      logRollbackFailure(teardownFailure);
      throw asRuntimeFailure(teardownFailure);
    }
    if (asyncFailure != null) {
      logRollbackFailure(asyncFailure);
    }

    return readUberPage();
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void close() {
    if (isClosed) {
      return;
    }
    projectionDictionaryRecordMemo = null;
    projectionDictionaryRecordMemoBytes = 0L;

    // The retained append buffer is off-heap and outlives every epoch by design, so this close is
    // the one place it is released. awaitInFlightAsyncFlushOnly() below fences any owner first;
    // releasing the flag as well keeps a post-close reuse from resurrecting a closed buffer.
    final BytesOut<?> retainedBuffer = retainedAppendBuffer;
    if (retainedBuffer != null) {
      retainedAppendBuffer = null;
      appendBufferInUse.set(true);
    }

    Throwable asyncFailure = null;
    try {
      awaitInFlightAsyncFlushOnly();
    } catch (final Throwable t) {
      asyncFailure = t;
    }
    if (retainedBuffer != null) {
      try {
        retainedBuffer.close();
      } catch (final RuntimeException releaseFailure) {
        LOGGER.warn("Releasing the retained append buffer failed", releaseFailure);
      }
    }
    if (asyncFlushWorkerRunning) {
      final Throwable unfencedFailure =
          asyncFlushFailure("Close cannot release pages or channels while the async flush worker is still running");
      if (asyncFailure != null && asyncFailure != unfencedFailure.getCause()) {
        retainFirstFailure(unfencedFailure, asyncFailure);
      }
      logCloseFailure("Close refused to release resources owned by an unfenced async worker", unfencedFailure);
      throw asRuntimeFailure(unfencedFailure);
    }

    Throwable teardownFailure = null;
    int unboundTrxId = Constants.NULL_ID_INT;
    if (!isBoundToNodeTrx) {
      try {
        // Capture before storageEngineReader.close(): its accessor correctly rejects every later
        // read, while the session needs this immutable id only after the two tail-safety barriers.
        unboundTrxId = storageEngineReader.getTrxId();
      } catch (final Throwable t) {
        teardownFailure = retainFirstFailure(teardownFailure, t);
      }
    }

    // From here on, the append owner is positively fenced. Every cleanup owner is isolated: an OOME,
    // I/O failure, or broken Page.close implementation must not pin the remaining payloads, channels,
    // off-heap buffer, or Cleaner state. The first teardown failure wins and later ones are suppressed.
    try {
      discardSidePageBatches();
      injectAsyncFlushFault("close-after-side-batch-discard");
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }
    try {
      printHftTelemetry();
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    // (The former pending async acknowledge-fsync is gone: writeUberPageReference is durable
    // on return — see its Writer contract — so there is nothing to await at close.)
    UberPage lastUberPage = null;
    try {
      lastUberPage = readUberPage();
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }
    if (lastUberPage != null) {
      try {
        storageEngineReader.resourceSession.setLastCommittedUberPage(lastUberPage);
      } catch (final Throwable t) {
        teardownFailure = retainFirstFailure(teardownFailure, t);
      }
    }
    try {
      closeCursorDurableReadPage();
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }
    // Release reader guards before asking the TIL to close its pages. A reader-close failure is
    // retained, but must not prevent the log from making its own best-effort release.
    try {
      storageEngineReader.close();
      injectAsyncFlushFault("close-after-reader-close");
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }
    try {
      log.close();
      injectAsyncFlushFault("close-after-log-close");
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    // Close every orphan independently, then sever the cache roots even if an individual page close
    // failed. log.close() normally handled TIL pages; these are the remaining cache-only containers.
    try {
      for (final PageContainer container : pageContainerCache.values()) {
        try {
          closeOrphanedPagesInContainer(container);
        } catch (final Throwable t) {
          teardownFailure = retainFirstFailure(teardownFailure, t);
        }
      }
    } catch (final Throwable t) {
      // Iterator creation/advance itself can fail under memory pressure. Cache severing and every
      // later resource owner still have to run.
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }
    try {
      pageContainerCache.clear();
      documentRecordLocationCache.clear();
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    } finally {
      currentNamePage = null;
      mostRecentPageContainer = null;
      secondMostRecentPageContainer = null;
      mostRecentPathSummaryPageContainer = null;
      mostRecentByIndexType = null;
    }

    try {
      invalidateCachesForAbortedUncommittedWrites();
      injectAsyncFlushFault("close-after-trie-cache-invalidate");
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    // storageEngineReader.close() deliberately does not own a write transaction's backend. Close it
    // independently so a reader/log/page failure cannot leak the data, revision, or beacon channel.
    boolean storageWriterClosed = false;
    try {
      storagePageReaderWriter.close();
      storageWriterClosed = true;
      injectAsyncFlushFault("close-after-storage-writer-close");
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    // An unbound writer owns the resource write permit itself. Keep that permit until BOTH safety
    // barriers for reclaimable prewrites have completed: resource caches no longer contain an
    // aborted offset, and the old backend writer can no longer touch the tail. Releasing earlier
    // lets a successor derive the last durable frontier and overwrite that tail while stale cache
    // entries are still addressable. If either barrier fails, retain the permit deliberately: a
    // wedged resource is fail-closed, whereas handing it to a successor can return wrong pages.
    if (!isBoundToNodeTrx && unboundTrxId != Constants.NULL_ID_INT && !hasUncommittedReclaimableWrites
        && storageWriterClosed) {
      try {
        storageEngineReader.resourceSession.closePageWriteTransaction(unboundTrxId, this);
      } catch (final Throwable t) {
        teardownFailure = retainFirstFailure(teardownFailure, t);
      }
    }

    // Detach first: even if pool release itself fails, this writer must not retain the off-heap root.
    final BytesOut<?> closeBuffer = bufferBytes;
    bufferBytes = null;
    if (closeBuffer != null) {
      try {
        Bytes.recycleOrRelease(closeBuffer);
        injectAsyncFlushFault("close-after-buffer-release");
      } catch (final Throwable t) {
        teardownFailure = retainFirstFailure(teardownFailure, t);
      }
    }

    // Terminal publication is unconditional after the worker fence: a caller may see the teardown
    // exception, but neither retry nor the Cleaner may treat this already-drained writer as live.
    isClosed = true;
    try {
      leakDetectorState.closed.set(true);
      injectAsyncFlushFault("close-after-terminal-flags");
    } catch (final Throwable t) {
      teardownFailure = retainFirstFailure(teardownFailure, t);
    }

    if (teardownFailure != null) {
      if (asyncFailure != null) {
        retainFirstFailure(teardownFailure, asyncFailure);
      }
      logCloseFailure("Writer close completed with teardown failure", teardownFailure);
      throw asRuntimeFailure(teardownFailure);
    }
    if (asyncFailure != null) {
      logCloseFailure("Async commit failed during close — resources were released anyway", asyncFailure);
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
        // retire(), not a drain + close. The old single releaseGuard() was wrong twice over: with one
        // holder it stole that holder's guard and freed the frame under it, and with more than one it
        // left the count positive, so close() returned early WITHOUT orphaning and the frame leaked
        // for the process's lifetime. retire() orphans first, so an unguarded page frees here and a
        // guarded one frees at its holder's last release.
        kvlPage.retire();
      }
    } else if (page instanceof HOTLeafPage hotLeaf && !hotLeaf.isClosed()) {
      // Do NOT free a HOT leaf that is still owned by the shared HOT-leaf buffer cache:
      // the combined read-side page is handed to the writer as a PageContainer's complete
      // page, so the SAME instance lives in both places. Closing it here would free the
      // off-heap MemorySegment out from under concurrent readers (use-after-free).
      if (!storageEngineReader.getBufferManager().getHOTLeafPageCache().containsPage(hotLeaf)) {
        hotLeaf.retire();
      }
    }
  }

  @Override
  public DeweyIDPage getDeweyIDPage(RevisionRootPage revisionRoot) {
    // TODO
    return null;
  }

  /**
   * Cache/trie resolution after the tiny most-recent-page fast path misses. Kept out of
   * {@link #getModifiedPageForRead} so C2 can inline its overwhelmingly common cache hit without also
   * inlining this cold resolver's mutable-key lookup and keyed-trie traversal.
   */
  private ReadPageResolution resolvePageForRead(final long recordPageKey, final int indexNumber,
      final IndexType indexType, final int revision) {
    readPageResolution.clear();
    lookupKey.setIndexType(indexType)
             .setRecordPageKey(recordPageKey)
             .setIndexNumber(indexNumber)
             .setRevisionNumber(revision);
    final PageContainer cached = pageContainerCache.get(lookupKey);
    if (cached != null) {
      readPageResolution.setPageContainer(cached);
      return readPageResolution;
    }

    final PageReference pageReference =
        storageEngineReader.getPageReference(newRevisionRootPage, indexType, indexNumber);
    final PageReference reference = storageEngineReader.getReferenceToLeafOfSubtree(pageReference, recordPageKey,
        indexNumber, indexType, newRevisionRootPage);
    if (reference == null) {
      return readPageResolution;
    }
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
    if (resolved != null) {
      readPageResolution.setPageContainer(resolved);
    } else if (firstUncommittedPageOffset != Long.MAX_VALUE && reference.getKey() >= firstUncommittedPageOffset) {
      readPageResolution.setDurableReference(reference);
    }
    return readPageResolution;
  }

  @Nullable
  private PageContainer getMostRecentPageContainer(IndexType indexType, long recordPageKey, int indexNumber,
      int revisionNumber) {
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
          && slot.revisionNumber == revisionNumber && slot.indexType == indexType && slot.pageContainer != null) {
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
  private PageContainer prepareRecordPage(final long recordPageKey, final int indexNumber, final IndexType indexType) {
    assert indexType != null;
    final PageContainer container = prepareRecordPageViaKeyedTrie(recordPageKey, indexNumber, indexType);
    refuseAdoptedImmutablePage(container.getModified(), "prepare-for-modification");
    return container;
  }

  /**
   * Refuse to hand out a bulk-ADOPTED page for reading or modification.
   *
   * <p>
   * {@code markAdoptedImmutableForFlush()} buys the flush lane 1.4 GB of memcpy per million rows by
   * letting the async snapshot serializer encode the page IN PLACE — the encoded image is copied over
   * the page's own slotted frame, so from that moment the slot region no longer holds slot data. The
   * page nevertheless stays resolvable through the intent log's snapshot layer until
   * {@code cleanupSnapshot()} runs, so the window between the frame overwrite and cleanup is one in
   * which a read would return garbage and a copy-on-write would propagate it — silently, because
   * nothing in the record path can tell an encoded frame from a slotted one.
   *
   * <p>
   * The bulk loaders never re-touch an adopted page (tails are adopted only once complete, page 0
   * goes through {@code prepareDocumentLeafForBlit} and is never marked, and the projection feed no
   * longer re-reads records), so this can only fire on a caller that kept using the write transaction
   * between {@code assemble()} and {@code commit()}. Turning that into a refusal is the whole point:
   * the alternative is corruption with no error.
   */
  private static void refuseAdoptedImmutablePage(final @Nullable Page page, final String operation) {
    if (page instanceof final KeyValueLeafPage keyValueLeafPage && keyValueLeafPage.isAdoptedImmutableForFlush()) {
      throw new IllegalStateException("Document page " + keyValueLeafPage.getPageKey()
          + " was adopted immutable for the bulk flush and must not be used for " + operation
          + "; its slotted frame may already hold the serialized page image");
    }
  }

  /**
   * Prepare a record page in the bit-decomposed keyed trie.
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
      final PageReference pageReference =
          storageEngineReader.getPageReference(newRevisionRootPage, indexType, indexNumber);

      // Get the reference to the unordered key/value page storing the records.
      final PageReference reference =
          keyedTrieWriter.prepareLeafOfTree(this, log, getUberPage().getPageCountExp(indexType), pageReference,
              recordPageKey, indexNumber, indexType, newRevisionRootPage);

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
        pageContainer = createFreshRecordPage(recordPageKey, indexType, getResourceSession().getResourceConfig(),
            storageEngineReader.getRevisionNumber());
        if (indexType == IndexType.DOCUMENT) {
          // Only DOCUMENT pages carry the projected columns' values, and only the MODIFIED half is
          // ever written to — the complete twin starts empty and is replaced by the combine.
          ((KeyValueLeafPage) pageContainer.getModified()).setGlobalStringDictionaries(
              documentStringResolverFor(recordPageKey));
        }
        appendLogRecord(reference, pageContainer);
        return pageContainer;
      } else {
        pageContainer = dereferenceRecordPageForModification(reference);
        return pageContainer;
      }
    };

    final int revision = newRevisionRootPage.getRevision();
    lookupKey.setIndexType(indexType)
             .setRecordPageKey(recordPageKey)
             .setIndexNumber(indexNumber)
             .setRevisionNumber(revision);
    var currPageContainer = pageContainerCache.get(lookupKey);
    if (currPageContainer == null) {
      currPageContainer = pageContainerCache.computeIfAbsent(
          new IndexLogKey(indexType, recordPageKey, indexNumber, revision), fetchPageContainer);
    }

    if (indexType == IndexType.PATH_SUMMARY) {
      mostRecentPathSummaryPageContainer.set(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision(),
          currPageContainer);
    } else {
      // Copy mostRecent into secondMostRecent BEFORE mutating mostRecent
      secondMostRecentPageContainer.copyFrom(mostRecentPageContainer);
      mostRecentPageContainer.set(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision(),
          currPageContainer);
      final IndexLogKeyToPageContainer[] byType = mostRecentByIndexType;
      if (byType != null) {
        final int ordinal = indexType.ordinal();
        IndexLogKeyToPageContainer byTypeUpd = byType[ordinal];
        if (byTypeUpd == null) {
          byTypeUpd = new IndexLogKeyToPageContainer(indexType, -1, -1, -1, null);
          byType[ordinal] = byTypeUpd;
        }
        byTypeUpd.set(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision(), currPageContainer);
      }
    }

    return currPageContainer;
  }

  /**
   * Create both writer-owned halves of a record page that has no persisted predecessor.
   *
   * <p>
   * {@link KeyValueLeafPage} owns and eagerly allocates its slotted frame. Its two memory parameters
   * are compatibility inputs which the writer-owned constructor immediately releases before
   * allocating that frame; supplying legacy 64 KiB buffers here therefore performed an
   * allocate/release round trip per input without contributing any page storage. Dewey IDs are inline
   * in the slotted frame, so the resource flag is preserved by the configuration rather than by a
   * separate buffer.
   * </p>
   *
   * <p>
   * Construction is all-or-nothing: should the second page (or container publication) fail, every
   * page whose frame was acquired here is closed before the failure escapes.
   * </p>
   */
  static PageContainer createFreshRecordPage(final long recordPageKey, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final int revisionNumber) {
    requireNonNull(indexType);
    requireNonNull(resourceConfig);

    KeyValueLeafPage completePage = null;
    KeyValueLeafPage modifyPage = null;
    try {
      completePage = new KeyValueLeafPage(recordPageKey, indexType, resourceConfig, revisionNumber, null, null, false);
      modifyPage = new KeyValueLeafPage(recordPageKey, indexType, resourceConfig, revisionNumber, null, null, false);
      return PageContainer.getInstance(completePage, modifyPage);
    } catch (final RuntimeException | Error failure) {
      closeFreshPageAfterFailure(modifyPage, failure);
      closeFreshPageAfterFailure(completePage, failure);
      throw failure;
    }
  }

  /**
   * Best-effort cleanup which cannot replace a fresh-page construction failure.
   *
   * <p>
   * Kept package-private for a focused failure-path regression. Production calls it only after
   * construction has already failed, so the successful fresh-page path carries no extra dispatch or
   * allocation.
   * </p>
   */
  static void closeFreshPageAfterFailure(final @Nullable AutoCloseable page, final Throwable primaryFailure) {
    requireNonNull(primaryFailure);
    if (page == null) {
      return;
    }
    try {
      page.close();
    } catch (final Throwable closeFailure) {
      if (closeFailure == primaryFailure) {
        return;
      }
      try {
        primaryFailure.addSuppressed(closeFailure);
      } catch (final Throwable ignored) {
        // Retaining the original construction failure is more important than diagnostics.
      }
    }
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

    final boolean ownedFragments =
        firstUncommittedPageOffset != Long.MAX_VALUE && reference.getKey() >= firstUncommittedPageOffset;
    final var result = ownedFragments
        ? storageEngineReader.getOwnedPageFragments(reference)
        : storageEngineReader.getPageFragments(reference);

    try {
      // A fragment fresh off disk may carry only its symbol table's dictionary id; the combine
      // decodes through the table bytes, so they must be resolved first — see the reader's
      // combine site for the failure this prevents. The dictionary load happens HERE, in plain
      // transaction context, because resolveFsstSymbolTables itself may run inside the record-
      // page cache's compute, where walking the NAME trie would re-enter the cache.
      storageEngineReader.ensureFsstSymbolTablesLoaded();
      storageEngineReader.resolveFsstSymbolTables(result.pages());
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
        if (ownedFragments) {
          ((KeyValueLeafPage) page).close();
        } else {
          ((KeyValueLeafPage) page).releaseGuard();
        }
      }
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
        yield pathPage == null ? null : pathPage.getIndexReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = getCASPage(actualRootPage);
        yield casPage == null ? null : casPage.getIndexReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = getNamePage(actualRootPage);
        yield namePage == null ? null : namePage.getIndexReference(databaseTypeOfSession(), indexNumber);
      }
      case PROJECTION -> {
        final var projPage = getProjectionIndexPage(actualRootPage);
        yield projPage == null ? null : projPage.getIndexReference(indexNumber);
      }
      case VALIDTIME -> {
        final var vtPage = getValidTimeIndexPage(actualRootPage);
        yield vtPage == null ? null : vtPage.getIndexReference(indexNumber);
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
  public KeyValueLeafPage getModifiedPageForRead(final long recordPageKey, final IndexType indexType, final int index) {
    NodeStorageEngineReader.validateKeyedTrieRoute(storageEngineReader, indexType, index);
    final int revision = newRevisionRootPage.getRevision();
    PageContainer pc = getMostRecentPageContainer(indexType, recordPageKey, index, revision);
    if (pc == null) {
      final ReadPageResolution resolution = resolvePageForRead(recordPageKey, index, indexType, revision);
      pc = resolution.pageContainer;
      if (pc == null && resolution.durableReference != null) {
        return cursorPageFromExactReference(resolution.durableReference);
      }
    }
    if (pc != null) {
      final var modified = pc.getModified();
      if (modified instanceof KeyValueLeafPage kvl && !kvl.isClosed()) {
        return kvl;
      }
    }
    return null;
  }

  private KeyValueLeafPage cursorPageFromExactReference(final PageReference reference) {
    final KeyValueLeafPage currentPage = cursorDurableReadPage;
    if (currentPage != null && !currentPage.isClosed() && cursorDurableReadOffset == reference.getKey()) {
      return currentPage;
    }
    final KeyValueLeafPage secondPage = secondCursorDurableReadPage;
    if (secondPage != null && !secondPage.isClosed() && secondCursorDurableReadOffset == reference.getKey()) {
      return secondPage;
    }
    final KeyValueLeafPage loadedPage = storageEngineReader.readRecordPageFromExactReference(reference);
    if (currentPage == null) {
      cursorDurableReadOffset = reference.getKey();
      cursorDurableReadPage = loadedPage;
    } else if (secondPage == null) {
      secondCursorDurableReadOffset = reference.getKey();
      secondCursorDurableReadPage = loadedPage;
    } else {
      loadedPage.retire();
      throw new IllegalStateException("The write cursor retained more than two durable read pages");
    }
    return loadedPage;
  }

  @Override
  public boolean isReadOnlyPageForRead(final KeyValueLeafPage page) {
    return cursorDurableReadPage == page || secondCursorDurableReadPage == page;
  }

  @Override
  public @Nullable DataRecord getDetachedRecordForRead(final KeyValueLeafPage page, final long recordKey) {
    return storageEngineReader.readDetachedRecord(page, recordKey);
  }

  @Override
  public void releasePageForRead(final @Nullable KeyValueLeafPage page) {
    if (page == null) {
      return;
    }
    if (cursorDurableReadPage == page) {
      cursorDurableReadPage = null;
      cursorDurableReadOffset = Constants.NULL_ID_LONG;
      if (!page.isClosed()) {
        page.retire();
      }
    } else if (secondCursorDurableReadPage == page) {
      secondCursorDurableReadPage = null;
      secondCursorDurableReadOffset = Constants.NULL_ID_LONG;
      if (!page.isClosed()) {
        page.retire();
      }
    }
  }

  private void closeCursorDurableReadPage() {
    final KeyValueLeafPage firstPage = cursorDurableReadPage;
    final KeyValueLeafPage secondPage = secondCursorDurableReadPage;
    cursorDurableReadPage = null;
    cursorDurableReadOffset = Constants.NULL_ID_LONG;
    secondCursorDurableReadPage = null;
    secondCursorDurableReadOffset = Constants.NULL_ID_LONG;
    if (firstPage != null && !firstPage.isClosed()) {
      firstPage.retire();
    }
    if (secondPage != null && !secondPage.isClosed()) {
      secondPage.retire();
    }
  }

  @Override
  public void addNameCount(final int key, final int delta, final NodeKind nodeKind) {
    storageEngineReader.assertNotClosed();
    getNamePage(newRevisionRootPage).addCount(key, delta, nodeKind, this);
  }

  @Override
  public void installDocumentStringDictionaries(final @Nullable GlobalStringDictionaries dictionaries) {
    this.documentStringDictionaries = dictionaries;
  }

  @Override
  public void installDocumentStringDictionaryFactory(
      final @Nullable LongFunction<GlobalStringDictionaries> factory) {
    this.documentStringDictionaryFactory = factory;
  }

  /**
   * The resolver a document leaf must carry: the per-page factory's answer when one is installed,
   * else the single resource-wide resolver.
   *
   * <p>
   * Called where the page is CREATED, never from the flush lane. That is the whole point: a
   * segment-scoped resolver has to be chosen while the writer still knows which page this is, because
   * by encode time the writer has moved on and the page's own segment is no longer "the current" one.
   * </p>
   */
  private @Nullable GlobalStringDictionaries documentStringResolverFor(final long recordPageKey) {
    final LongFunction<GlobalStringDictionaries> factory = documentStringDictionaryFactory;
    return factory == null
        ? documentStringDictionaries
        : factory.apply(recordPageKey);
  }

  @Override
  public void adoptDocumentLeafPage(final KeyValueLeafPage page) {
    storageEngineReader.assertNotClosed();
    // Before the page reaches the flush lane, which is where its string region is built and where
    // nothing can be handed to it any more. An adopted page is marked immutable-for-flush at the end
    // of this method, so this is the last moment it can be told anything at all.
    final long recordPageKey = page.getPageKey();
    page.setGlobalStringDictionaries(documentStringResolverFor(recordPageKey));
    final ResourceConfiguration resourceConfiguration = getResourceSession().getResourceConfig();
    // The builder's cold direct-write fallbacks are still heap snapshots in records[]. Serialize them
    // NOW, on the adopting thread and BEFORE the trie is touched (a serialization failure then leaves
    // no CoW'd parent behind): inline where the fused image fits, otherwise as canonical overflow
    // carriers. Left to the flush lane they cost a defensive deep copy per epoch and, as carriers
    // with no durable key, made the background flush decline the page into the pinned region until
    // final commit — on a 100M-row load that kept every such page's frame resident and exhausted
    // the arena at a few percent of the corpus.
    // Ownership contract: until appendLogRecord succeeds the CALLER's page is nobody else's, so a
    // failure before that point retires it here (frame freed now, or at the last guard release);
    // from appendLogRecord on, the intent log owns it and rollback closes it.
    final PageReference reference;
    final KeyValueLeafPage completeTwin;
    try {
      page.materializePendingRecords(resourceConfiguration);
      final PageReference indexReference =
          storageEngineReader.getPageReference(newRevisionRootPage, IndexType.DOCUMENT, -1);
      // Walking the trie CoWs the indirect spine into the log exactly as an ordinary insert would.
      reference = keyedTrieWriter.prepareLeafOfTree(this, log, getUberPage().getPageCountExp(IndexType.DOCUMENT),
          indexReference, recordPageKey, -1, IndexType.DOCUMENT, newRevisionRootPage);
      if (reference.getKey() != Constants.NULL_ID_LONG || log.get(reference) != null) {
        throw new IllegalStateException(
            "bulk adoption requires unwritten territory, but document page " + recordPageKey + " already exists");
      }
      // Mirror createFreshRecordPage's container shape: empty COMPLETE twin + the built MODIFIED
      // page — read-back, flush eligibility and commit all key off that structure.
      completeTwin = new KeyValueLeafPage(recordPageKey, IndexType.DOCUMENT, resourceConfiguration,
          storageEngineReader.getRevisionNumber(), null, null, false);
    } catch (final RuntimeException | Error failure) {
      page.retire();
      throw failure;
    }
    appendLogRecord(reference, PageContainer.getInstance(completeTwin, page));
    storageEngineReader.invalidateMostRecentlyReadRecordPage(IndexType.DOCUMENT, -1);
    // Every carrier the materialization installed is an immutable page: stage it in the bounded
    // side-page append batch, so the epoch that flushes this leaf finds the carrier's key published
    // (one epoch of deferral, see SNAPSHOT_RETRY_NEXT_EPOCH) instead of pinning the leaf.
    stageLeafOverflowCarriers(page);
    // Materialization consumed every heap record (it throws otherwise), so the page's frame, slots
    // and reference-map structure never change again: the flush lane serializes it in place.
    page.markAdoptedImmutableForFlush();
  }

  /**
   * Test seam: {@code false} restores the pre-fix behaviour in which an adopted page's overflow
   * carriers stay resident until final commit, so the background flush pins the page. Only
   * {@code AdoptedOverflowCarrierStagingTest} flips it, to prove its guard is not vacuous.
   */
  static volatile boolean STAGE_ADOPTED_OVERFLOW_CARRIERS = true;

  /** Whether a "carriers stay resident" warning was already logged in this JVM. */
  private static final AtomicBoolean CARRIER_STAGING_WARNED = new AtomicBoolean();

  /** Memo of {@link #carrierStagingSupported()}: 0 unknown, 1 supported, 2 unsupported. */
  private byte carrierStagingSupport;

  /**
   * Whether this writer's backend can stage an immutable page before the root is published — the
   * same two gates {@link #stageUncommittedOverflowPage} applies, evaluated once per writer so an
   * unsupported configuration is announced instead of silently restoring the leaf-pinning route.
   */
  private boolean carrierStagingSupported() {
    if (carrierStagingSupport == 0) {
      final boolean reclaimable = storagePageReaderWriter.supportsReclaimableUncommittedWrites();
      final boolean deterministicClose = SharedArenas.supportsDeterministicClose();
      carrierStagingSupport = reclaimable && deterministicClose
          ? (byte) 1
          : (byte) 2;
      if (carrierStagingSupport == 2 && CARRIER_STAGING_WARNED.compareAndSet(false, true)) {
        LOGGER.warn("Adopted-page overflow carriers stay resident until final commit on this configuration: "
            + (reclaimable
                ? ""
                : "the storage backend cannot reclaim uncommitted writes (storage type / sirix.commit.preallocated); ")
            + (deterministicClose
                ? ""
                : "the arena strategy has no deterministic close (sirix.arena.strategy); ")
            + "every bulk-adopted leaf holding such a carrier is pinned in the intent log for the life of its"
            + " transaction, which bounds the load size by the arena");
      }
    }
    return carrierStagingSupport == 1;
  }

  @Override
  public void stageOverflowCarriersOfLiveLeaf(final KeyValueLeafPage page) {
    storageEngineReader.assertNotClosed();
    requireNonNull(page);
    // The live leaf is mutable foreground state (its generation is current), so materializing here
    // is the same kind of mutation the blit itself was. A survivor throws inside.
    page.materializePendingRecords(getResourceSession().getResourceConfig());
    stageLeafOverflowCarriers(page);
  }

  /**
   * Stage every fresh, unresolved {@link OverflowPage} carrier of a leaf into the immutable
   * side-page append batch. A carrier the backend declines (counted, announced once) or one larger
   * than a whole batch stays resident, and the page takes the ordinary promote-and-pin route.
   */
  private void stageLeafOverflowCarriers(final KeyValueLeafPage page) {
    final Map<Long, PageReference> references = page.getReferencesMap();
    if (references.isEmpty()) {
      return;
    }
    final boolean staging = STAGE_ADOPTED_OVERFLOW_CARRIERS && carrierStagingSupported();
    for (final PageReference reference : references.values()) {
      if (reference.getKey() != Constants.NULL_ID_LONG || reference.hasPendingPageWrite()
          || !(reference.getPage() instanceof OverflowPage carrier)) {
        continue;
      }
      if (!staging) {
        BulkAdoptionDiagnostics.carrierUnstaged();
      } else if (carrier.dataLength() > MAX_STAGED_SIDE_PAGE_BYTES) {
        BulkAdoptionDiagnostics.carrierOversized();
      } else if (reference.getLogKey() != Constants.NULL_ID_INT || reference.transactionLogReference() != null) {
        // The staging lane THROWS on a reference that already carries a log identity. The page is in
        // the live log by now, so a throw here would poison the transaction for a carrier that can
        // still take the resident route: count it and move on.
        BulkAdoptionDiagnostics.carrierRefused();
      } else if (stageUncommittedOverflowPage(reference)) {
        BulkAdoptionDiagnostics.carrierStaged();
      } else {
        BulkAdoptionDiagnostics.carrierRefused();
      }
    }
  }

  /**
   * Epochs a leaf may be skipped for pending carriers before the flush lane stops waiting and lets
   * the ordinary promote-and-pin path take it. A carrier staged before an epoch's snapshot is in
   * that epoch's side batch and is published by the cleanup that re-promotes the leaf, so exactly
   * ONE deferral is reachable; the second is slack against a future ordering regression, after which
   * the cap turns the regression into a pin (counted by {@link #kvlPagesPinnedAfterDeferralCap()})
   * rather than an unbounded retry. Package-private and non-final for the test's cap-zero arm.
   */
  static int MAX_KVL_FLUSH_DEFERRALS = 2;

  /**
   * Whether the flush lane declines to re-encode a leaf whose previous pre-serialization minted
   * overflow carriers with no durable key.
   *
   * <p>
   * Such an encode is unpublishable by construction: {@code addReferences} discovers the overlong
   * records DURING the encode, so the carrier keys are assigned only by the recursive final commit,
   * and the copy is dropped the moment the encode finishes. The leaf is then promoted into the live
   * intent log — where the next epoch finds it and encodes it again. The refusal is a property of
   * the page's records, and records are only added to a leaf inside a flush window, never removed,
   * so the second encode is guaranteed to end the same way as the first. Declining it reaches the
   * identical outcome (promote to the intent log, write at the final commit) without the body
   * staging, region build and codec pass — and without the deep copy that feeds them.
   * </p>
   *
   * <p>
   * Kill switch {@code -Dsirix.flush.skipRefusedOverflowLeaves=false} restores the unconditional
   * re-encode. Non-final and package-private so a test can drive both arms in one JVM, exactly as
   * {@link #MAX_KVL_FLUSH_DEFERRALS} is; production reads it once per snapshot entry, never per
   * record.
   * </p>
   */
  static boolean skipRefusedOverflowLeaves =
      !"false".equalsIgnoreCase(System.getProperty("sirix.flush.skipRefusedOverflowLeaves"));

  /**
   * How many flush epochs a refusal may be honoured for before the leaf is offered to the encoder
   * again.
   *
   * <p>
   * The two ways of being wrong are not symmetric, and neither is unsafe. Encoding a leaf that would
   * have been refused costs one encode — today's behaviour. Skipping a leaf that could now be written
   * defers it to the recursive final commit, which is the SAME outcome the refusal it stands in for
   * produces; but a mark that never expired would make that deferral unbounded, and an unbounded
   * deferral is how intent-log residency turns into an exhausted arena. This bound makes the
   * divergence from unmodified behaviour finite by construction rather than by argument: a leaf can
   * be excluded from the async flush for at most this many epochs, after which it is encoded and
   * either flushes or refuses again. At 64 the safety valve costs about 1.6 % of the encodes the
   * lever removes.
   * </p>
   *
   * <p>
   * Package-private and non-final for the test's bound-zero arm, exactly as
   * {@link #MAX_KVL_FLUSH_DEFERRALS} is.
   * </p>
   */
  static int MAX_SKIPPED_FLUSH_EPOCHS = 64;

  /**
   * Flush epochs this writer has begun. Incremented by the flush driver before each snapshot epoch
   * and read by its serializer workers, hence {@code volatile}; it feeds only the refusal table's
   * expiry, so a missed increment costs at most one deferred encode.
   */
  private volatile long flushEpoch;

  /**
   * Whether the sole append owner reuses one off-heap append buffer across flush epochs instead of
   * allocating a {@link Writer#FLUSH_SIZE} buffer per epoch.
   *
   * <p>
   * The allocation profile put 0.40 GB per 1M-row load on this one statement — a fresh buffer for
   * every epoch, on a lane that by construction has exactly ONE owner at a time. Kill switch
   * {@code -Dsirix.flush.retainAppendBuffer=false} restores the per-epoch allocation.
   * </p>
   */
  private static final boolean RETAIN_APPEND_BUFFER =
      !"false".equalsIgnoreCase(System.getProperty("sirix.flush.retainAppendBuffer"));

  /**
   * The reused append buffer. Written and read by whichever thread wins {@link #appendBufferInUse},
   * whose CAS/release pair is the happens-before edge; {@code volatile} so that edge is explicit
   * rather than inferred.
   */
  private volatile @Nullable BytesOut<?> retainedAppendBuffer;

  /**
   * Guards {@link #retainedAppendBuffer} against an overlapping flush. The admission control should
   * make one impossible, so this never contends — but reuse must be an OPTIMISATION that cannot
   * corrupt an append if that assumption ever weakens, and a loser simply allocates its own buffer
   * exactly as before.
   */
  private final AtomicBoolean appendBufferInUse = new AtomicBoolean();

  /**
   * Leaf identities whose pre-serialization minted overflow carriers only the recursive final commit
   * can key. Always allocated so the kill switch can be flipped inside one JVM.
   */
  private final RefusedOverflowLeafTable refusedOverflowLeaves =
      new RefusedOverflowLeafTable(RefusedOverflowLeafTable.DEFAULT_SLOTS);

  /**
   * Remember that this leaf's content pre-serializes into carriers the background flush cannot key.
   *
   * @param page the refused snapshot copy — it carries the same identity as the live leaf
   */
  private void noteRefusedOverflowLeaf(final KeyValueLeafPage page) {
    if (skipRefusedOverflowLeaves) {
      refusedOverflowLeaves.note(page.getPageKey(), page.getIndexType().getID(), flushEpoch);
    }
  }

  /**
   * Whether an earlier epoch already proved this leaf's encode unpublishable.
   *
   * @param page the live leaf the flush lane is about to pre-serialize
   * @return {@code true} when the encode is known to end in a discard
   */
  private boolean wasRefusedForUnresolvedCarriers(final KeyValueLeafPage page) {
    return skipRefusedOverflowLeaves && refusedOverflowLeaves.shouldSkip(page.getPageKey(),
        page.getIndexType().getID(), flushEpoch, MAX_SKIPPED_FLUSH_EPOCHS);
  }

  @Override
  public KeyValueLeafPage prepareDocumentLeafForBlit(final long recordPageKey) {
    storageEngineReader.assertNotClosed();
    final PageContainer container = prepareRecordPage(recordPageKey, -1, IndexType.DOCUMENT);
    return (KeyValueLeafPage) container.getModifiedAsKeyValuePage();
  }

  @Override
  public StorageEngineWriter appendLogRecord(final PageReference reference, final PageContainer pageContainer) {
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
    // Refuse an unsupported backend while NOTHING has been mutated yet. Everything below is
    // ordered so the resource is at either the original or the target revision at any instant;
    // a storage that throws from its own truncateTo lands outside that set — the beacons and
    // the session's last-committed uber page already downgraded, the pages never discarded, and
    // the cache never cleared, because the throw jumps past clearCachesForDatabase.
    if (!storagePageReaderWriter.supportsTruncateTo()) {
      throw new UnsupportedOperationException(
          "Storage backend " + storagePageReaderWriter.getClass().getSimpleName() + " cannot truncate to revision "
              + revision + " — rollback and crash recovery need a persistent StorageType.");
    }
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
    ((InternalResourceSession<?, ?>) resourceSession).setLastCommittedUberPage(rolledBackUberPage);

    storagePageReaderWriter.truncateTo(revision);

    // The truncated range's offsets are reused by the next commit — drop THIS RESOURCE's cached
    // pages so nothing pre-truncation can be served. Resource-scoped, not database-scoped: a
    // sibling resource's file was not touched, its pages cannot be stale, and it has live readers
    // that the "run this before opening anything that reads the file" precondition never covered.
    Databases.clearCachesForResource(resourceSession.getResourceConfig().getDatabaseId(),
        storageEngineReader.getResourceId());
    hasUncommittedReclaimableWrites = false;
    firstUncommittedPageOffset = Long.MAX_VALUE;
    closeCursorDurableReadPage();
    // Path-class records are cached per (resource, revision), and truncateTo RE-ISSUES the
    // truncated revision numbers over different content -- the same offset-reuse hazard the page
    // caches are dropped for above. Without this a PathFilter/CASFilter at a re-issued revision is
    // served the discarded history's PCRs and matches records of an unrelated path: reproduced as
    // {12} returned for a path whose true PCR set is empty, with PCR 12 now owned by another path.
    JsonPCRCollector.invalidateCache();
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
  public PageGuard acquireGuardForNode(final long nodeKey) {
    final var reader = (NodeStorageEngineReader) storageEngineReader;
    var currentPage = reader.getCurrentPage();
    if (currentPage == null || currentPage.getPageKey() != reader.pageKey(nodeKey, IndexType.DOCUMENT)) {
      // Nothing currently guards the node's page. That is not an error state: a preceding mutation
      // releases the guard it held, and the cursor movements in between can be answered from the
      // transaction's own record cache without ever going through the page layer — so the second
      // remove() of a transaction arrives here with no guard, which used to throw outright ("No
      // current page - cannot acquire guard"; reproducible under FULL versioning with two field
      // deletions in one commit). Fetch the page that holds the node instead. The node key is
      // exactly what is needed to find it, and fetching also re-establishes the reader's guard.
      //
      // The page-key comparison matters as much as the null check: a leftover guard on some OTHER
      // page would satisfy the old code and then protect the wrong page, which is the very thing
      // this guard exists to prevent.
      reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, reader.pageKey(nodeKey, IndexType.DOCUMENT), 0,
          reader.getRevisionNumber()));
      currentPage = reader.getCurrentPage();
    }
    // Re-check the page KEY, not just for null. getRecordPage returns null without touching the
    // reader's guard when the page cannot be loaded, so on that path getCurrentPage() still reports
    // whatever the previous navigation left installed -- a null check alone accepts it and hands
    // back a guard on an unrelated page, which is the exact failure the comparison above exists to
    // prevent. Guarding the wrong page is worse than failing: the caller believes the pages it is
    // about to mutate are pinned, and they are not.
    final long expectedPageKey = reader.pageKey(nodeKey, IndexType.DOCUMENT);
    if (currentPage == null || currentPage.getPageKey() != expectedPageKey) {
      throw new IllegalStateException("No page holds node " + nodeKey + " - cannot acquire guard (expected page "
          + expectedPageKey + ", got " + (currentPage == null
              ? "none"
              : Long.toString(currentPage.getPageKey()))
          + ")");
    }
    return new PageGuard(currentPage);
  }

}
