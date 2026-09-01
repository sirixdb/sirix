package io.sirix.page;

import io.sirix.node.LE;
import java.math.BigInteger;
import java.math.BigDecimal;
import io.sirix.utils.ToStringHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.node.DeltaVarIntCodec;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Arrays;
import java.util.Objects;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.pax.BooleanRegion;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.NumberZoneMapRegion;
import io.sirix.page.pax.ObjectKeyNameKeyRegion;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import io.sirix.page.pax.DoubleRegion;
import io.sirix.page.pax.RecordOrdinalRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringDictSketch;
import io.sirix.page.pax.GlobalStringDictionaries;
import io.sirix.page.pax.ResolvedGlobalStrings;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
import io.sirix.settings.DiagnosticSettings;
import io.sirix.settings.Fixed;
import io.sirix.utils.WeakIdentitySet;
import io.sirix.utils.FSSTCompressor;
import io.sirix.utils.ArrayIterator;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Cleaner;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * <p>
 * An UnorderedKeyValuePage stores a set of records, commonly nodes in an unordered data structure.
 * </p>
 * <p>
 * The page currently is not thread safe (might have to be for concurrent write-transactions)!
 * </p>
 */
@SuppressWarnings({"unchecked"})
public final class KeyValueLeafPage implements KeyValuePage<DataRecord>, io.sirix.cache.CacheablePage {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueLeafPage.class);
  /**
   * SIMD vector species for bitmap operations. Uses the preferred species for the current platform
   * (256-bit AVX2 or 512-bit AVX-512).
   */
  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;


  /**
   * Enable detailed memory leak tracking. Accessed via centralized
   * {@link DiagnosticSettings#MEMORY_LEAK_TRACKING}.
   * 
   * @see DiagnosticSettings#isMemoryLeakTrackingEnabled()
   */
  public static final boolean DEBUG_MEMORY_LEAKS = DiagnosticSettings.MEMORY_LEAK_TRACKING;

  // DIAGNOSTIC COUNTERS (enabled via DEBUG_MEMORY_LEAKS)
  public static final java.util.concurrent.atomic.AtomicLong PAGES_CREATED =
      new java.util.concurrent.atomic.AtomicLong(0);
  public static final java.util.concurrent.atomic.AtomicLong PAGES_CLOSED =
      new java.util.concurrent.atomic.AtomicLong(0);
  public static final java.util.concurrent.ConcurrentHashMap<IndexType, java.util.concurrent.atomic.AtomicLong> PAGES_BY_TYPE =
      new java.util.concurrent.ConcurrentHashMap<>();
  public static final java.util.concurrent.ConcurrentHashMap<IndexType, java.util.concurrent.atomic.AtomicLong> PAGES_CLOSED_BY_TYPE =
      new java.util.concurrent.ConcurrentHashMap<>();

  /**
   * Every page that has been created and not yet closed, held WEAKLY and keyed by IDENTITY.
   *
   * <p>
   * Weakly, because this registry used to hold strong references: registering a page made it
   * immortal, so {@link #PAGES_FINALIZED_WITHOUT_CLOSE} — which counts pages collected without
   * {@code close()} — could never be anything but zero while leak tracking was ON, the only time it
   * is populated at all. The Cleaner literally could not fire. Weak references restore the split the
   * two mechanisms were designed for: what remains here is the set of pages still reachable from
   * somewhere (retained leaks, reported at shutdown with their creation stacks), and what leaves here
   * without being closed is a page that became garbage while still holding an off-heap frame
   * (unreachable leaks, reported by the Cleaner).
   * </p>
   *
   * <p>
   * By identity, because {@link #equals(Object)} is overridden on this type: two distinct instances
   * of the same page key and revision are equal, and an equality-keyed registry would drop one of
   * them from the census.
   * </p>
   */
  public static final java.util.Set<KeyValueLeafPage> ALL_LIVE_PAGES = new WeakIdentitySet<>();

  // LEAK DETECTION: Track finalized pages
  public static final java.util.concurrent.atomic.AtomicLong PAGES_FINALIZED_WITHOUT_CLOSE =
      new java.util.concurrent.atomic.AtomicLong(0);

  // Track finalized pages by type and pageKey for diagnostics
  public static final java.util.concurrent.ConcurrentHashMap<IndexType, java.util.concurrent.atomic.AtomicLong> FINALIZED_BY_TYPE =
      new java.util.concurrent.ConcurrentHashMap<>();
  public static final java.util.concurrent.ConcurrentHashMap<Long, java.util.concurrent.atomic.AtomicLong> FINALIZED_BY_PAGE_KEY =
      new java.util.concurrent.ConcurrentHashMap<>();

  /**
   * Page-0 instances specifically — same weak-identity registry as {@link #ALL_LIVE_PAGES}, and for
   * the same reasons. Page 0 gets its own because multiple instances legitimately share that key
   * across revisions, which is precisely when an equality-keyed set loses track of them.
   */
  public static final java.util.Set<KeyValueLeafPage> ALL_PAGE_0_INSTANCES = new WeakIdentitySet<>();

  /**
   * Version counter for detecting page reuse (LeanStore/Umbra approach). Incremented when page is
   * evicted and reused for a different logical page.
   */
  private final AtomicInteger version = new AtomicInteger(0);

  // ========== LOCK-FREE STATE FLAGS (HFT-optimized) ==========
  // Pack HOT, orphaned, and closed bits into a single int for cache locality.
  // Uses VarHandle with opaque access for the HOT bit (no memory barriers on hot path).
  // This eliminates volatile write overhead on every page access.

  /** Bit 0: HOT flag for clock-based eviction */
  private static final int HOT_BIT = 1;
  /** Bit 1: Orphan flag for deterministic cleanup */
  private static final int ORPHANED_BIT = 2;
  /** Bit 2: Closed flag */
  private static final int CLOSED_BIT = 4;

  /**
   * Packed state flags: HOT (bit 0), orphaned (bit 1), closed (bit 2). Accessed via VarHandle for
   * lock-free CAS operations.
   */
  @SuppressWarnings("unused") // Accessed via VarHandle
  private volatile int stateFlags = 0;

  /** VarHandle for lock-free state flag operations */
  private static final VarHandle STATE_FLAGS_HANDLE;

  static {
    try {
      STATE_FLAGS_HANDLE = MethodHandles.lookup().findVarHandle(KeyValueLeafPage.class, "stateFlags", int.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Guard count for preventing eviction during active use (LeanStore/Umbra pattern). Pages with
   * guardCount > 0 cannot be evicted. This is simpler than per-transaction pinning - it's just a
   * reference count.
   */
  private final AtomicInteger guardCount = new AtomicInteger(0);

  /**
   * DIAGNOSTIC: Stack trace of where this page was created (only captured when
   * DEBUG_MEMORY_LEAKS=true). Used to trace where leaked pages come from.
   */
  private final StackTraceElement[] creationStackTrace;

  /**
   * DIAGNOSTIC: Where and on which thread this page was closed (only captured when
   * DEBUG_MEMORY_LEAKS=true). Lets a reader that observes a closed page report WHO freed it out from
   * under it (use-after-free triage).
   */
  private volatile Throwable closeSite;

  /** Diagnostic accessor for the close-site capture (null unless DEBUG_MEMORY_LEAKS). */
  public Throwable getCloseSite() {
    return closeSite;
  }

  /** Shared Cleaner for all KeyValueLeafPage leak-detection registrations. */
  private static final Cleaner LEAK_CLEANER = Cleaner.create();

  /**
   * Heap-allocated state captured by the leak-detection {@link Cleaner.Cleanable} so it does NOT
   * capture the enclosing {@code KeyValueLeafPage} instance — capturing {@code this} would make the
   * page strongly reachable via the Cleaner queue, defeating the very leak detection it implements.
   * Holds:
   * <ul>
   * <li>The diagnostic facts the leak log needs (pageKey, indexType, revision,
   * creationStackTrace).</li>
   * <li>A {@code closed} flag the page's {@link #close()} flips — the Cleaner action reads it and
   * skips logging when the page was closed properly.</li>
   * </ul>
   * Non-static state class so users can read the closed flag through the {@code Cleaner.Cleanable}
   * owner; reference is held by the page only when {@link #DEBUG_MEMORY_LEAKS} is on, so production
   * builds pay zero overhead.
   */
  private static final class LeakDetectorState implements Runnable {
    final long pageKey;
    final IndexType indexType;
    final int revision;
    final StackTraceElement[] creationStackTrace;
    final AtomicBoolean closed = new AtomicBoolean(false);

    LeakDetectorState(final long pageKey, final IndexType indexType, final int revision,
        final StackTraceElement[] creationStackTrace) {
      this.pageKey = pageKey;
      this.indexType = indexType;
      this.revision = revision;
      this.creationStackTrace = creationStackTrace;
    }

    @Override
    public void run() {
      if (closed.get()) {
        return; // closed properly — no leak.
      }
      PAGES_FINALIZED_WITHOUT_CLOSE.incrementAndGet();
      if (indexType != null) {
        FINALIZED_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0))
                         .incrementAndGet();
      }
      FINALIZED_BY_PAGE_KEY.computeIfAbsent(pageKey, _ -> new java.util.concurrent.atomic.AtomicLong(0))
                           .incrementAndGet();
      if (LOGGER.isWarnEnabled()) {
        final StringBuilder leakMsg = new StringBuilder().append(
            String.format("Page leak detected: pageKey=%d, type=%s, revision=%d - not closed explicitly", pageKey,
                indexType, revision));
        if (creationStackTrace != null && LOGGER.isDebugEnabled()) {
          leakMsg.append("\n  Creation stack trace:");
          for (int i = 2; i < Math.min(creationStackTrace.length, 8); i++) {
            final StackTraceElement frame = creationStackTrace[i];
            leakMsg.append(String.format("\n    at %s.%s(%s:%d)", frame.getClassName(), frame.getMethodName(),
                frame.getFileName(), frame.getLineNumber()));
          }
        }
        LOGGER.warn(leakMsg.toString());
      }
    }
  }

  /**
   * Non-null only when {@link #DEBUG_MEMORY_LEAKS} is on; the page sets {@code .closed} on this state
   * in {@link #close()} so the Cleaner action skips the leak log.
   */
  private final LeakDetectorState leakDetectorState;

  /**
   * Get the creation stack trace for leak diagnostics.
   * 
   * @return stack trace from constructor, or null if DEBUG_MEMORY_LEAKS disabled
   */
  public StackTraceElement[] getCreationStackTrace() {
    return creationStackTrace;
  }

  /**
   * The current revision.
   */
  private int revision;

  /**
   * Determines if DeweyIDs are stored or not.
   */
  private boolean areDeweyIDsStored;



  /**
   * The index of the last slot (the slot with the largest offset).
   */
  private int lastSlotIndex;


  /**
   * Determines if references to {@link OverflowPage}s have been added or not.
   */
  private boolean addedReferences;

  /**
   * Hard ceiling for the slotted-page backing memory: the largest {@link FrameSlotAllocator} size
   * class (256 KiB). {@link #growSlottedPage()} doubles the capacity, so any growth past this ceiling
   * would throw in the allocator. Records that cannot fit within it are diverted to
   * {@link OverflowPage}s instead (#1076).
   */
  public static final int MAX_SLOTTED_PAGE_CAPACITY =
      (int) FrameSlotAllocator.SIZE_CLASSES[FrameSlotAllocator.SIZE_CLASSES.length - 1];

  /** Reused cold-path workspace for constructing one bounded side-slot image without young-gen churn. */
  private static final ThreadLocal<MemorySegment> SIDE_SLOT_IMAGE_SCRATCH =
      ThreadLocal.withInitial(() -> MemorySegment.ofArray(new byte[OverflowSlotSidecar.MAX_IMAGE_BYTES]));

  /** Maximum old flyweight image plus DeltaVarIntCodec's documented nine-byte field growth. */
  private static final ThreadLocal<MemorySegment> RESIZE_OVERFLOW_SCRATCH = ThreadLocal.withInitial(
      () -> MemorySegment.ofArray(new byte[PageConstants.MAX_RECORD_SIZE + 9]));

  /**
   * Sentinel returned by {@link #prepareHeapForDirectWriteOrOverflow(int, int)} when the record
   * cannot fit into the slotted page and must be routed to overflow storage.
   */
  public static final long DIRECT_WRITE_OVERFLOW = -1L;

  /**
   * References to overflow pages.
   */
  private final Map<Long, PageReference> references;

  /**
   * Key of record page. This is the base key of all contained nodes.
   */
  private long recordPageKey;

  /**
   * Set when a bulk-import ADOPTED page enters the intent log: nothing mutates the page after
   * adoption, so the async snapshot flush may serialize it IN PLACE (skipping the defensive deep copy
   * that exists to protect concurrently mutated pages) — the disposable encode then clobbers this
   * page's own frame, which is fine because the snapshot cleanup is its single closer and nothing
   * reads the frame after the flush.
   */
  private boolean adoptedImmutableForFlush;

  /**
   * How many async-flush epochs skipped this page because its out-of-line carriers were still
   * pending immutable side-page writes; see {@link #overflowReferenceState()}. Bounded by the flush
   * lane, which pins after a handful of deferrals rather than retrying forever.
   */
  private byte flushDeferrals;

  /**
   * The record-ID mapped to the records. Lazily allocated on first write to save ~8KB per page when
   * FlyweightNode records go directly to the slotted page heap (zero records[] path).
   */
  private DataRecord[] records;

  private static final DataRecord[] EMPTY_RECORDS = new DataRecord[0];

  private void ensureRecords() {
    if (records == null) {
      records = new DataRecord[Constants.NDP_NODE_COUNT];
    }
  }



  /**
   * FSST symbol table for string compression (shared across all strings in page). Null if FSST
   * compression is not used.
   */
  private byte[] fsstSymbolTable;

  /** {@link #fsstSymbolTableId} when the page references no symbol table. */
  public static final long NO_FSST_SYMBOL_TABLE_ID = 0L;

  /**
   * The dictionary id of this page's symbol table, or {@link #NO_FSST_SYMBOL_TABLE_ID}.
   *
   * <p>
   * Set on the write path before the page is serialized, and on the read path from the page's bytes.
   * When it is set and {@link #fsstSymbolTable} is still null, the table has not been fetched from
   * the dictionary trie yet.
   */
  private long fsstSymbolTableId = NO_FSST_SYMBOL_TABLE_ID;

  /**
   * PAX region table appended to every KVL page. Null when no regions have been populated. Populated
   * with number / string / struct / DeweyID regions; scan operators read contiguous payload buffers
   * from it instead of decoding varints per slot.
   *
   * <p>
   * {@code volatile}: minted and populated under the page monitor by the synchronized region builders
   * but read by concurrent scan workers that take no lock — a plain field gives those readers neither
   * a guaranteed sight of the install nor a happens-before edge to the payloads behind it.
   */
  private volatile RegionTable regionTable;

  /**
   * FSST-compression flyweight (StringNode), lazy-init only on the write-path where FSST compression
   * actually runs. For analytical scan workloads these objects were the top non-page allocator — 7.6%
   * of samples (async-profiler alloc mode) per KVLP constructor. Using a shared sentinel so the
   * null-check in the read path is elided.
   */
  private StringNode fsstStringFlyweight;

  private StringNode fsstStringFlyweight() {
    StringNode f = fsstStringFlyweight;
    if (f == null) {
      f = new StringNode(0, null);
      fsstStringFlyweight = f;
    }
    return f;
  }

  /**
   * Fused-string sibling of {@link #fsstStringFlyweight} — same lazy-init rationale, for the kind-50
   * compress pass. On real JSON the fused records hold nearly all string bytes, so this is the
   * flyweight that matters for FSST's reach.
   */
  private ObjectNamedStringNode fsstFusedStringFlyweight;

  private ObjectNamedStringNode fsstFusedStringFlyweight() {
    ObjectNamedStringNode f = fsstFusedStringFlyweight;
    if (f == null) {
      f = new ObjectNamedStringNode(0, null);
      fsstFusedStringFlyweight = f;
    }
    return f;
  }



  /**
   * Number of words in the slot bitmap (16 words * 64 bits = 1024 slots).
   */
  private static final int BITMAP_WORDS = 16;


  /**
   * Reference to the complete page for lazy slot copying at commit time. Set during
   * combineRecordPagesForModification, used by addReferences() to copy slots that need preservation
   * but weren't modified (records[i] == null).
   */
  private KeyValueLeafPage completePageRef;

  /**
   * The index type.
   */
  private IndexType indexType;

  /**
   * Persistenter.
   */
  private RecordSerializer recordPersister;

  /**
   * The resource configuration.
   */
  private ResourceConfiguration resourceConfig;

  private volatile BytesOut<?> bytes;

  /** Encoded page data (owned pipeline result or exact view of a disposable page frame). */
  private volatile MemorySegment compressedSegment;

  /** No exact pre-byte-handler length is associated with the currently published wire cache. */
  public static final int UNKNOWN_BYTE_HANDLER_INPUT_LENGTH = -1;

  /**
   * Exact serialized length before the outer byte-handler pipeline.
   *
   * <p>
   * This plain field is published by the subsequent volatile write to either {@link #bytes} or
   * {@link #compressedSegment}. Consumers must first acquire a non-null cache reference and only then
   * read this metadata. Keeping one primitive beside the existing caches avoids a wrapper allocation
   * on every leaf page.
   */
  private int byteHandlerInputLength = UNKNOWN_BYTE_HANDLER_INPUT_LENGTH;


  private volatile byte[] hashCode;

  // Note: isClosed flag is now packed into stateFlags (bit 2) for lock-free access

  /**
   * Flag indicating whether memory was externally allocated (e.g., by Arena in tests). If true,
   * close() should NOT release memory to segmentAllocator since it wasn't allocated by it.
   */
  private final boolean externallyAllocatedMemory;

  private MemorySegmentAllocator segmentAllocator = Allocators.getInstance();

  /**
   * Backing buffer from decompression (for zero-copy deserialization). When non-null, this buffer
   * must be released on close().
   */
  private MemorySegment backingBuffer;

  /**
   * Releaser to return backing buffer to allocator. Called on close() to return the decompression
   * buffer to the allocator pool.
   */
  private Runnable backingBufferReleaser;

  // ==================== UNIFIED PAGE (LeanStore-style) ====================

  /**
   * Slotted page MemorySegment (PostgreSQL/LeanStore-style: Header + Bitmap + Directory + Heap).
   * Stores records in a heap with per-record offset tables, enabling O(1) field access via flyweight
   * binding. The page layout is defined by {@link PageLayout}: header (32 B) + bitmap (128 B) +
   * directory (8 KB) + heap.
   *
   * <p>
   * FlyweightNode records are serialized directly to the heap at createRecord time and bound for
   * in-place mutation. Non-FlyweightNode records are serialized to the heap at commit time via
   * processEntries.
   */
  private MemorySegment slottedPage;

  /**
   * Lazily allocated scan-visible images for logical slots whose carrier could not fit in the
   * bounded slotted-page frame. The ordinary page therefore pays only this nullable reference.
   */
  private OverflowSlotSidecar overflowSlotSidecar;

  /**
   * Actual capacity in bytes of the slottedPage segment. Tracked separately because slottedPage is
   * reinterpreted to Long.MAX_VALUE to eliminate JIT bounds checks on MemorySegment get/set
   * operations.
   */
  private int slottedPageCapacity;

  /**
   * The chunks of this page's body that have not been expanded into the heap yet, or {@code null} on
   * a page that was decoded whole — which is every page unless the loader asked for
   * {@link PageKind#deserializePageLazily} and the body was chunk-framed.
   *
   * <p>
   * Read on the way to every heap byte this page hands out, so what it costs matters. Deliberately
   * <em>not</em> volatile and deliberately never cleared: the reference is written once, before the
   * page is published, so an eager page pays one plain null check per accessor and nothing else, and
   * a lazy page's one-way transition to fully-expanded is published by the flag inside the object
   * rather than by nulling the field. Clearing it would be the unsafe publication in reverse — a
   * reader could observe the null a materializing thread wrote without observing the record bytes it
   * wrote first.
   */
  private LazyChunkedBody lazyChunkedBody;

  // ==================== CACHED PAGE HEADER VALUES ====================
  // Mirror of header fields from slottedPage MemorySegment.
  // All hot-path reads use these Java fields (zero MemorySegment overhead).
  // Writes use write-through helpers that update both field and segment.

  private int cachedHeapEnd;
  private int cachedHeapUsed;
  private int cachedPopulatedCount;

  /**
   * Constructor which initializes a new {@link KeyValueLeafPage}. Memory is externally provided
   * (e.g., by Arena in tests) and will NOT be released by close().
   *
   * @param recordPageKey base key assigned to this node page
   * @param indexType the index type
   * @param resourceConfig the resource configuration
   */
  public KeyValueLeafPage(final long recordPageKey, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final int revisionNumber, final MemorySegment slotMemory,
      final MemorySegment deweyIdMemory) {
    this(recordPageKey, indexType, resourceConfig, revisionNumber, slotMemory, deweyIdMemory, true);
  }

  /**
   * Constructor which initializes a new {@link KeyValueLeafPage}.
   *
   * @param recordPageKey base key assigned to this node page
   * @param indexType the index type
   * @param resourceConfig the resource configuration
   * @param externallyAllocatedMemory if true, memory was allocated externally and won't be released
   *        by close()
   */
  public KeyValueLeafPage(final long recordPageKey, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final int revisionNumber, final MemorySegment slotMemory,
      final MemorySegment deweyIdMemory, final boolean externallyAllocatedMemory) {
    this(recordPageKey, indexType, resourceConfig, revisionNumber, slotMemory, deweyIdMemory, externallyAllocatedMemory,
        null, null);
  }

  /**
   * Constructor-stage injection seam for verifying ownership when eager frame initialization fails.
   * Both additional arguments are {@code null} on every production path.
   */
  KeyValueLeafPage(final long recordPageKey, final IndexType indexType, final ResourceConfiguration resourceConfig,
      final int revisionNumber, final MemorySegment slotMemory, final MemorySegment deweyIdMemory,
      final boolean externallyAllocatedMemory, final @Nullable MemorySegmentAllocator allocatorForTesting,
      final @Nullable Runnable afterFrameAcquireForTesting) {
    // Assertions instead of requireNonNull(...) checks as it's part of the
    // internal flow.
    assert resourceConfig != null : "The resource config must not be null!";

    if (allocatorForTesting != null) {
      segmentAllocator = allocatorForTesting;
    }

    this.references = new ConcurrentHashMap<>();
    this.recordPageKey = recordPageKey;
    this.records = null;
    this.areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
    this.indexType = indexType;
    this.resourceConfig = resourceConfig;
    this.recordPersister = resourceConfig.recordPersister;
    this.revision = revisionNumber;

    this.lastSlotIndex = -1;
    this.externallyAllocatedMemory = externallyAllocatedMemory;

    // Release passed-in legacy memory if not externally allocated (callers still pass it)
    if (!externallyAllocatedMemory) {
      if (slotMemory != null && slotMemory.byteSize() > 0) {
        segmentAllocator.release(slotMemory);
      }
      if (deweyIdMemory != null && deweyIdMemory.byteSize() > 0) {
        segmentAllocator.release(deweyIdMemory);
      }
    }

    StackTraceElement[] constructedCreationStackTrace = null;
    LeakDetectorState constructedLeakDetectorState = null;
    java.util.concurrent.atomic.AtomicLong pagesByTypeCounter = null;
    boolean pageCreatedCounted = false;
    boolean pageTypeCounted = false;
    boolean livePageRegistered = false;
    boolean pageZeroRegistered = false;
    try {
      // Eagerly allocate slotted page — all pages use slotted page format.
      ensureSlottedPage();
      if (afterFrameAcquireForTesting != null) {
        afterFrameAcquireForTesting.run();
      }

      // Capture creation stack trace for leak tracing (only when diagnostics enabled).
      if (DEBUG_MEMORY_LEAKS) {
        constructedCreationStackTrace = Thread.currentThread().getStackTrace();
        PAGES_CREATED.incrementAndGet();
        pageCreatedCounted = true;
        pagesByTypeCounter =
            PAGES_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0));
        pagesByTypeCounter.incrementAndGet();
        pageTypeCounted = true;
        livePageRegistered = true;
        ALL_LIVE_PAGES.add(this);
        if (recordPageKey == 0) {
          pageZeroRegistered = true;
          ALL_PAGE_0_INSTANCES.add(this);
        }
        constructedLeakDetectorState =
            new LeakDetectorState(recordPageKey, indexType, revision, constructedCreationStackTrace);
        LEAK_CLEANER.register(this, constructedLeakDetectorState);
      }
      this.creationStackTrace = constructedCreationStackTrace;
      this.leakDetectorState = constructedLeakDetectorState;
    } catch (final RuntimeException | Error failure) {
      if (constructedLeakDetectorState != null) {
        try {
          constructedLeakDetectorState.closed.set(true);
        } catch (final Throwable cleanupFailure) {
          addSuppressedBestEffort(failure, cleanupFailure);
        }
      }
      if (pageZeroRegistered) {
        try {
          ALL_PAGE_0_INSTANCES.remove(this);
        } catch (final Throwable cleanupFailure) {
          addSuppressedBestEffort(failure, cleanupFailure);
        }
      }
      if (livePageRegistered) {
        try {
          ALL_LIVE_PAGES.remove(this);
        } catch (final Throwable cleanupFailure) {
          addSuppressedBestEffort(failure, cleanupFailure);
        }
      }
      if (pageTypeCounted && pagesByTypeCounter != null) {
        try {
          pagesByTypeCounter.decrementAndGet();
        } catch (final Throwable cleanupFailure) {
          addSuppressedBestEffort(failure, cleanupFailure);
        }
      }
      if (pageCreatedCounted) {
        try {
          PAGES_CREATED.decrementAndGet();
        } catch (final Throwable cleanupFailure) {
          addSuppressedBestEffort(failure, cleanupFailure);
        }
      }
      releaseConstructorFrameAfterFailure(failure);
      throw failure;
    }
  }

  /**
   * Constructor which reads deserialized data to the {@link KeyValueLeafPage} from the storage. The
   * slotted page will be set by the caller via {@link #setSlottedPage(MemorySegment)}.
   *
   * @param recordPageKey This is the base key of all contained nodes.
   * @param revision The current revision.
   * @param indexType The index type.
   * @param resourceConfig The resource configuration.
   * @param areDeweyIDsStored Determines if DeweyIDs are stored or not.
   * @param recordPersister Persistenter.
   * @param references References to overflow pages.
   */
  public KeyValueLeafPage(final long recordPageKey, final int revision, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final boolean areDeweyIDsStored,
      final RecordSerializer recordPersister, final Map<Long, PageReference> references, final MemorySegment slotMemory,
      final MemorySegment deweyIdMemory, final int lastSlotIndex) {
    this.recordPageKey = recordPageKey;
    this.revision = revision;
    this.indexType = indexType;
    this.resourceConfig = resourceConfig;
    this.areDeweyIDsStored = areDeweyIDsStored;
    this.recordPersister = recordPersister;
    this.references = references;
    this.records = null;

    this.lastSlotIndex = lastSlotIndex;
    // Memory allocated by global allocator (e.g., during deserialization) - release on close()
    this.externallyAllocatedMemory = false;

    // Release dummy slotMemory passed by callers (e.g., PageKind allocates a 1-byte dummy)
    if (slotMemory != null && slotMemory.byteSize() > 0) {
      segmentAllocator.release(slotMemory);
    }
    if (deweyIdMemory != null && deweyIdMemory.byteSize() > 0) {
      segmentAllocator.release(deweyIdMemory);
    }

    // Slotted page is set by caller via setSlottedPage() after construction.

    // Capture creation stack trace for leak tracing (only when diagnostics enabled)
    if (DEBUG_MEMORY_LEAKS) {
      this.creationStackTrace = Thread.currentThread().getStackTrace();
      PAGES_CREATED.incrementAndGet();
      PAGES_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
      ALL_LIVE_PAGES.add(this);
      if (recordPageKey == 0) {
        ALL_PAGE_0_INSTANCES.add(this);
      }
      this.leakDetectorState = new LeakDetectorState(recordPageKey, indexType, revision, creationStackTrace);
      LEAK_CLEANER.register(this, leakDetectorState);
    } else {
      this.creationStackTrace = null;
      this.leakDetectorState = null;
    }
  }

  /**
   * Create a deep copy of this page for Copy-on-Write during async epoch boundaries. Copies slotted
   * page MemorySegment, references map and the records ARRAY, and shares the FSST symbol table.
   *
   * <p>
   * Uses the deserialization constructor to set lastSlotIndex directly (no public setter). Slotted
   * page is deep-copied via allocate + MemorySegment.copy, then set via setSlottedPage().
   * Serialization caches (compressedSegment, bytes, hashCode) are left null — copy is dirty.
   * </p>
   *
   * <p>
   * Pending {@link FlyweightNode}s receive independent snapshots. A flyweight's binding is mutable
   * even when its logical fields are not: sharing one across this boundary lets serializing the copy
   * rebind the source record to the copy's disposable frame. Non-flyweight records remain shared, so
   * a record whose serializer mutates it — as {@link io.sirix.index.path.summary.PathStats#writeTo}
   * does when it calls {@code RoaringBitmap.runOptimize()} — must make that mutation safe.
   * {@code PathStats} does, by guarding its page-key bitmap with its own monitor.
   * </p>
   *
   * @return a structurally independent copy with independent flyweight bindings
   */
  public KeyValueLeafPage deepCopy() {
    // Deep-copy the references map (each PageReference cloned via copy constructor). A carrier that
    // is staged in the writer's immutable side-page batch is SHARED instead: the copy constructor
    // refuses a pending reference by design, and every copy must observe the durable key that
    // publication installs on the one shared handle — the HOT leaf CoW follows the same rule.
    final var refsCopy = new ConcurrentHashMap<Long, PageReference>(references.size());
    for (final var entry : references.entrySet()) {
      final PageReference reference = entry.getValue();
      refsCopy.put(entry.getKey(), reference.hasPendingPageWrite()
          ? reference
          : new PageReference(reference));
    }

    // Use deserialization constructor:
    // - sets lastSlotIndex, externallyAllocatedMemory=false
    // - records=null, no slotted page allocation (caller sets via setSlottedPage)
    // - releases slotMemory/deweyIdMemory if non-null (we pass null)
    final var copy = new KeyValueLeafPage(recordPageKey, revision, indexType, resourceConfig, areDeweyIDsStored,
        recordPersister, refsCopy, null, null, lastSlotIndex);

    // Deep-copy slotted page MemorySegment (primary data store)
    if (slottedPage != null) {
      // A whole-segment copy carries whatever the heap holds, poison included, so the source must
      // hold records rather than chunks first.
      refuseUnresolvedGlobalTags("copy-on-write deepCopy()");
      ensureAllChunks();
      final MemorySegment freshSegment = segmentAllocator.allocate(slottedPageCapacity);
      MemorySegment.copy(slottedPage, 0, freshSegment, 0, slottedPageCapacity);
      copy.setSlottedPage(freshSegment);
    }

    try {
      // Pending records are the cold path: direct flyweight writes leave records[] null. Preserve
      // the existing shallow copy for non-flyweights, whose type-specific synchronization contract
      // is documented above, but never share a mutable page binding. processEntries() is allowed to
      // bind the independent snapshot to the COPY's slotted page without changing the live record.
      if (records != null) {
        final DataRecord[] recordsCopy = Arrays.copyOf(records, records.length);
        for (int slot = 0; slot < recordsCopy.length; slot++) {
          if (recordsCopy[slot] instanceof FlyweightNode flyweight) {
            recordsCopy[slot] = flyweight.toSnapshot();
          }
        }
        copy.records = recordsCopy;
      }

      copySideSlotsInto(copy);
      materializePreservedSlotsInto(copy);
    } catch (final RuntimeException | Error failure) {
      copy.close();
      throw failure;
    }

    // Share the FSST symbol table reference — the byte[] is immutable once bound, and the
    // parse/matcher caches key on its identity. Cloning it here made every snapshot-flushed
    // page a cache miss, re-parsing the table and rebuilding a ~½ MB matcher index per page.
    copy.fsstSymbolTable = fsstSymbolTable;
    // The reference travels with the copy even when the table itself has not been fetched yet.
    // Dropping it would leave a copy-on-written page holding FSST-encoded string bytes with no
    // way left to say which symbols they were encoded against.
    copy.fsstSymbolTableId = fsstSymbolTableId;

    return copy;
  }

  private void copySideSlotsInto(final KeyValueLeafPage copy) {
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar == null || sidecar.isEmpty()) {
      return;
    }
    for (int wordIndex = 0; wordIndex < BITMAP_WORDS; wordIndex++) {
      long word = sidecar.bitmapWord(wordIndex);
      final int baseSlot = wordIndex << 6;
      while (word != 0L) {
        final int slot = baseSlot + Long.numberOfTrailingZeros(word);
        copy.copySideSlotFrom(this, slot);
        word &= word - 1;
      }
    }
  }

  private void materializePreservedSlotsInto(final KeyValueLeafPage copy) {
    final MemorySegment copyPage = copy.slottedPage;
    if (copyPage == null || !PageLayout.hasPreservedSlots(copyPage)) {
      return;
    }

    final KeyValueLeafPage completePage = completePageRef;
    if (completePage == null) {
      throw new IllegalStateException("Page " + recordPageKey + " has preserved slots without a complete page");
    }

    for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
      if (!PageLayout.isSlotPreserved(copy.slottedPage, slot)
          || copy.hasLogicalCarrierShadowingPreservation(slot, completePage)) {
        continue;
      }

      final MemorySegment completeSlottedPage = completePage.getSlottedPage();
      if ((completeSlottedPage == null || !PageLayout.isSlotPopulated(completeSlottedPage, slot))
          && !completePage.hasSideSlot(slot)) {
        throw new IllegalStateException("Page " + recordPageKey + " cannot preserve missing slot " + slot);
      }
      copy.copySlotFromPage(completePage, slot);
    }

    PageLayout.clearPreservationBitmap(copy.slottedPage);
  }

  @Override
  public int hashCode() {
    // Manual primitive math — no Objects.hashCode() varargs/boxing
    return (int) (recordPageKey ^ (recordPageKey >>> 32)) * 31 + revision;
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof KeyValueLeafPage other) {
      return recordPageKey == other.recordPageKey && revision == other.revision;
    }
    return false;
  }

  public void markAdoptedImmutableForFlush() {
    this.adoptedImmutableForFlush = true;
  }

  public boolean isAdoptedImmutableForFlush() {
    return adoptedImmutableForFlush;
  }

  /** Where this page's out-of-line record carriers stand with respect to a background flush. */
  public enum OverflowReferenceState {
    /** Every carrier reference already has a durable key (or there are none): flushable as is. */
    RESOLVED,
    /**
     * Every unresolved carrier is an immutable side page staged in the writer's append batch. Its
     * durable key is published by the epoch that writes it, so the page becomes flushable one epoch
     * later without any serialization work now.
     */
    PENDING_SIDE_WRITES,
    /** At least one carrier is neither durable nor staged: only the recursive final commit can write it. */
    UNRESOLVED
  }

  /**
   * Classify the carrier references of this page for the async flush lane. Allocation-free apart
   * from the map iterator; called once per page per epoch, never per record.
   */
  public OverflowReferenceState overflowReferenceState() {
    if (references.isEmpty()) {
      return OverflowReferenceState.RESOLVED;
    }
    boolean pending = false;
    for (final PageReference reference : references.values()) {
      if (reference.getKey() != Constants.NULL_ID_LONG) {
        continue;
      }
      if (!reference.hasPendingPageWrite()) {
        return OverflowReferenceState.UNRESOLVED;
      }
      pending = true;
    }
    return pending
        ? OverflowReferenceState.PENDING_SIDE_WRITES
        : OverflowReferenceState.RESOLVED;
  }

  /** Epochs that deferred this page while its carriers were pending; see {@link #noteFlushDeferral()}. */
  public int flushDeferrals() {
    return flushDeferrals;
  }

  /** Record one more async-flush epoch that skipped this page for pending carriers. */
  public void noteFlushDeferral() {
    if (flushDeferrals < Byte.MAX_VALUE) {
      flushDeferrals++;
    }
  }

  /**
   * Serialize every record still held in {@code records[]} into this page now — inline where the
   * fused image fits, otherwise as a canonical overflow carrier — exactly as the first serialization
   * would, but on the thread that owns the page rather than inside the background flush.
   *
   * <p>
   * The bulk lane's cold direct-write fallbacks arrive here as heap snapshots. Left for the flush lane,
   * they force a defensive deep copy per epoch and, once they turn into overflow carriers, leave the
   * page with unresolved references that the background flush cannot write: the page is then pinned
   * until the final commit, and a large load keeps every such page resident. Materializing at
   * adoption lets the writer stage the carriers as immutable side pages instead.
   * </p>
   *
   * @param resourceConfiguration the resource's serialization configuration
   */
  public void materializePendingRecords(final ResourceConfiguration resourceConfiguration) {
    if (resourceConfiguration == null) {
      throw new NullPointerException("resourceConfiguration");
    }
    final DataRecord[] pending = records;
    if (pending == null) {
      return;
    }
    processEntries(resourceConfiguration, pending);
    // processEntries consumes every entry by contract (inline, carrier, or the bound-flyweight
    // shortcut all null the slot). The page is about to be marked immutable for an in-place flush,
    // so a survivor would be a programming error, not a state to carry: fail loudly.
    for (int i = 0; i < pending.length; i++) {
      if (pending[i] != null) {
        throw new IllegalStateException("materializePendingRecords left record " + pending[i].getNodeKey()
            + " of page " + recordPageKey + " unprocessed");
      }
    }
    records = null;
  }

  @Override
  public long getPageKey() {
    return recordPageKey;
  }

  @Override
  public DataRecord getRecord(int offset) {
    return records != null
        ? records[offset]
        : null;
  }

  @Override
  public void setRecord(final DataRecord record) {
    addedReferences = false;
    // Invalidate stale compressed cache — record mutation means cached bytes are outdated
    clearSerializedCache();
    final var key = record.getNodeKey();
    final var offset = (int) (key - ((key >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));

    // Writer-side query insurance: if a number record currently lives in this slot, the
    // cached PAX number region is about to become stale (replacement, deletion via
    // DeletedNode, etc.). Fast-path no-op when no region is currently cached.
    maybeInvalidateRegionsForExistingSlot(offset);

    if (record instanceof FlyweightNode fn) {
      if (fn.isWriteSingleton()) {
        // Write singleton: serialize to heap, never store in records[] (aliasing risk).
        if (slottedPage != null && fn.isBoundTo(slottedPage)) {
          fn.setOwnerPage(this);
          return;
        }
        ensureSlottedPage();
        if (fn.isBound()) {
          fn.unbind();
        }
        if (!serializeToHeap(fn, key, offset)) {
          // The record does not fit within the largest slotted-page size class (#1076).
          // Snapshot the singleton (it will be reused for the next node) into records[] so the
          // record stays readable/mutable in-transaction; processEntries diverts it to an
          // OverflowPage at serialization time.
          ensureRecords();
          records[offset] = fn.toSnapshot();
          clearSlotPreservation(offset);
        }
        return;
      }
      // Non-singleton FlyweightNode: unbind if bound, store in records[] for processEntries.
      if (fn.isBound()) {
        fn.unbind();
      }
    }

    ensureRecords();
    records[offset] = record;
    clearSlotPreservation(offset);
  }

  /**
   * Store a newly created record, serializing non-FlyweightNode data to the slotted page heap
   * immediately. This is called from the createRecord path where node factories may reuse singleton
   * objects. By serializing now and nulling records[], we preserve data before the singleton is
   * reused for the next node creation.
   *
   * <p>
   * For FlyweightNode records, this delegates to {@link #setRecord} which handles heap serialization
   * and binding. For non-FlyweightNode on slotted pages, the record is serialized to the heap and
   * records[offset] is nulled — prepareRecordForModification will deserialize a fresh object from the
   * heap when mutation is needed.
   *
   * @param record the newly created record
   */
  public void setNewRecord(final DataRecord record) {
    assert !(record instanceof FlyweightNode)
        : "FlyweightNode must not go through setNewRecord — use serializeNewRecord";
    addedReferences = false;
    clearSerializedCache();
    final var key = record.getNodeKey();
    final var offset = (int) (key - ((key >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
    // Defensive: a non-flyweight record may overwrite a number- or string-typed slot.
    maybeInvalidateRegionsForExistingSlot(offset);
    ensureRecords();
    records[offset] = record;
    clearSlotPreservation(offset);
  }

  public void serializeNewRecord(final FlyweightNode fn, final long nodeKey, final int offset) {
    addedReferences = false;
    clearSerializedCache();
    if (!serializeToHeap(fn, nodeKey, offset)) {
      // Record does not fit within the largest slotted-page size class (#1076): keep a snapshot
      // in records[] (the flyweight may be reused) — processEntries diverts it to an
      // OverflowPage at serialization time. The node stays unbound in this case.
      ensureRecords();
      records[offset] = fn.toSnapshot();
      clearSlotPreservation(offset);
      return;
    }
    // Node stays bound after creation — next factory clearBinding() handles transition
  }

  /**
   * Serialize a FlyweightNode to the slotted page heap, update directory/bitmap, and bind.
   *
   * <p>
   * After this call the node is bound: getters/setters operate on page memory. processEntries will
   * skip this record at commit time because {@code fn.isBound()} is true.
   *
   * @param fn the flyweight node to serialize
   * @param nodeKey the node's key
   * @param offset the slot index within the page (0-1023)
   * @return {@code true} if the record was serialized to the heap; {@code false} if it does not fit
   *         within {@link #MAX_SLOTTED_PAGE_CAPACITY} and must be diverted to an {@link OverflowPage}
   *         by the caller (#1076) — the page is left unchanged then
   */
  private boolean serializeToHeap(final FlyweightNode fn, final long nodeKey, final int offset) {
    // Get DeweyID bytes if stored (must capture BEFORE binding overwrites the node state)
    final byte[] deweyIdBytes = areDeweyIDsStored
        ? fn.getDeweyIDAsBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;

    // Enforce the out-of-line threshold on every flyweight path, not only processEntries(). Before
    // this guard a fused string update could grow its containing page to 256 KiB while the generic
    // path spilled the same record, making storage shape depend on the insertion API.
    final int estimatedRecordSize = fn.estimateSerializedSize();
    if (estimatedRecordSize < 0) {
      throw new IllegalStateException("negative serialized-size estimate for " + fn.getKind() + ": "
          + estimatedRecordSize);
    }
    final int deweyIdOverhead = areDeweyIDsStored
        ? deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0;
    final long estimatedSize = (long) estimatedRecordSize + deweyIdOverhead;
    // Refuse WITHOUT attempting only when the record cannot possibly fit: the refusal keys on the
    // guaranteed floor, not the padded ceiling. A ceiling-keyed refusal sent every record in the
    // (floor <= cap < ceiling) band to the commit-time generic serializer, which stores it under
    // the raw-record sentinel kind — correct to read, but invisible to the PAX region builders and
    // therefore to every anchored scan (6146 of 1,000,000 ClickBench hits records). Attempting the
    // band is safe by the two guards below: the mid-write catch and the actual-size check both
    // leave directory, bitmap and heap counters untouched on failure.
    final long guaranteedSize = (long) fn.estimateSerializedSizeLowerBound() + deweyIdOverhead;
    if (guaranteedSize > PageConstants.MAX_RECORD_SIZE) {
      return false;
    }

    ensureSlottedPage();
    // Ensure heap has enough space for this record (value nodes can be large).
    final int heapEnd = cachedHeapEnd;
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < estimatedSize) {
      if ((long) slottedPageCapacity << 1 > MAX_SLOTTED_PAGE_CAPACITY) {
        // Growing further would exceed the largest allocator size class — divert to overflow.
        return false;
      }
      growSlottedPage();
    }

    // Write directly to heap at current end
    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    final int recordBytes;
    try {
      recordBytes = fn.serializeToHeap(slottedPage, absOffset);
    } catch (final IndexOutOfBoundsException e) {
      // A node type with an inaccurate estimateSerializedSize() outgrew the segment mid-write.
      // Nothing past this point ran, so heapEnd/directory/bitmap are untouched and the partial
      // bytes sit in unclaimed heap space — safe to divert the record to an OverflowPage
      // instead of failing the commit (issue #1076).
      return false;
    }
    if (recordBytes < 0) {
      throw new IllegalStateException("negative serialized size for " + fn.getKind() + ": " + recordBytes);
    }
    final long actualSize = (long) recordBytes + deweyIdOverhead;
    if (actualSize > PageConstants.MAX_RECORD_SIZE) {
      // An underestimated flyweight wrote only into unclaimed heap space: directory, bitmap and
      // heap counters are still untouched, so the caller can safely route its snapshot to overflow.
      return false;
    }
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < actualSize) {
      // The estimator was inside the 512-byte policy but still too small for this frame's tail.
      // Growth preserves the unpublished bytes at the same heap offset; at the final frame class we
      // can still divert safely because no directory or counters have been published.
      if ((long) slottedPageCapacity << 1 > MAX_SLOTTED_PAGE_CAPACITY) {
        return false;
      }
      growSlottedPage();
    }

    // When DeweyIDs are stored, append DeweyID data + 2-byte trailer
    final int totalBytes;
    if (areDeweyIDsStored) {
      if (deweyIdLen > 0) {
        MemorySegment.copy(deweyIdBytes, 0, slottedPage, java.lang.foreign.ValueLayout.JAVA_BYTE,
            absOffset + recordBytes, deweyIdLen);
      }
      totalBytes = recordBytes + deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE;
      PageLayout.writeDeweyIdTrailer(slottedPage, absOffset + totalBytes, deweyIdLen);
    } else {
      totalBytes = recordBytes;
    }

    // Update heap end and used counters. Rewrites use bump allocation, but the previous allocation
    // is dead as soon as the directory moves, so it must leave heapUsed even though heapEnd remains
    // the high-water mark until compaction.
    final boolean replacingInlineSlot = PageLayout.isSlotPopulated(slottedPage, offset);
    final int replacedBytes = replacingInlineSlot
        ? PageLayout.getDirDataLength(slottedPage, offset)
        : 0;
    updateHeapEnd(heapEnd + totalBytes);
    updateHeapUsed(cachedHeapUsed + totalBytes - replacedBytes);

    // Update directory entry: [heapOffset][dataLength | nodeKindId]
    final int nodeKindId = ((NodeKind) fn.getKind()).getId();
    PageLayout.setDirEntry(slottedPage, offset, heapEnd, totalBytes, nodeKindId);
    clearSlotPreservation(offset);

    // Mark slot populated in bitmap and track last slot index (new slots only)
    if (!replacingInlineSlot) {
      PageLayout.markSlotPopulated(slottedPage, offset);
      updatePopulatedCount(cachedPopulatedCount + 1);
      lastSlotIndex = offset;
    }

    // Column write: drop every cached PAX region this kind feeds so the next reader rebuilds.
    invalidateRegionsForKindId(nodeKindId);

    // A successful ordinary inline publication supersedes every previous overflow carrier.
    if (overflowSlotSidecar != null) {
      removeSideSlot(offset);
    }
    if (!references.isEmpty()) {
      references.remove(nodeKey);
    }

    // Bind flyweight — all subsequent mutations go directly to page memory
    fn.bind(slottedPage, absOffset, nodeKey, offset);
    fn.setOwnerPage(this);
    return true;
  }

  // ==================== DIRECT-TO-HEAP CREATION ====================

  /**
   * Prepare the heap for a direct record write. Ensures slotted page exists and has enough space.
   * Returns the absolute offset where the caller should write.
   *
   * @param estimatedRecordSize upper bound on record bytes (from estimateSerializedSize)
   * @param deweyIdLen length of DeweyID bytes (0 if none)
   * @return absolute byte offset in the slotted page MemorySegment to write at
   * @throws SirixIOException if the record cannot fit within the largest slotted-page size class —
   *         value-carrying factories should use
   *         {@link #prepareHeapForDirectWriteOrOverflow(int, int)} and divert to overflow storage
   */
  public long prepareHeapForDirectWrite(final int estimatedRecordSize, final int deweyIdLen) {
    final long absOffset = prepareHeapForDirectWriteOrOverflow(estimatedRecordSize, deweyIdLen);
    if (absOffset == DIRECT_WRITE_OVERFLOW) {
      throw new SirixIOException("Record of estimated size " + estimatedRecordSize
          + " bytes does not fit into the slotted page (capacity ceiling " + MAX_SLOTTED_PAGE_CAPACITY
          + " bytes) and this record kind has no overflow-storage fallback");
    }
    return absOffset;
  }

  /**
   * Like {@link #prepareHeapForDirectWrite(int, int)}, but returns {@link #DIRECT_WRITE_OVERFLOW}
   * instead of throwing when the record cannot fit within {@link #MAX_SLOTTED_PAGE_CAPACITY} (either
   * because the record alone is too large, or because the page heap is too full). The caller must
   * then store the record as a heap object via {@link #setRecord(DataRecord)} so
   * {@code processEntries} diverts it to an {@link OverflowPage} at serialization time (#1076).
   *
   * @param estimatedRecordSize upper bound on record bytes (from estimateSerializedSize)
   * @param deweyIdLen length of DeweyID bytes (0 if none)
   * @return absolute byte offset to write at, or {@link #DIRECT_WRITE_OVERFLOW}
   */
  public long prepareHeapForDirectWriteOrOverflow(final int estimatedRecordSize, final int deweyIdLen) {
    if (estimatedRecordSize < 0) {
      throw new IllegalArgumentException("estimatedRecordSize must be non-negative: " + estimatedRecordSize);
    }
    if (deweyIdLen < 0) {
      throw new IllegalArgumentException("deweyIdLen must be non-negative: " + deweyIdLen);
    }
    final int deweyOverhead = areDeweyIDsStored
        ? deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0;
    final long totalEstimated = (long) estimatedRecordSize + deweyOverhead;
    if (totalEstimated > PageConstants.MAX_RECORD_SIZE) {
      return DIRECT_WRITE_OVERFLOW;
    }
    ensureSlottedPage();
    final int heapEnd = cachedHeapEnd;
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < totalEstimated) {
      if ((long) slottedPageCapacity << 1 > MAX_SLOTTED_PAGE_CAPACITY) {
        return DIRECT_WRITE_OVERFLOW;
      }
      growSlottedPage();
    }
    return PageLayout.heapAbsoluteOffset(heapEnd);
  }

  /** Ensure append capacity without ever asking the allocator for a frame above its final class. */
  private boolean ensureInlineAppendCapacity(final int totalBytes) {
    if (totalBytes < 0 || totalBytes > PageConstants.MAX_RECORD_SIZE) {
      return false;
    }
    ensureSlottedPage();
    while (slottedPageCapacity - PageLayout.HEAP_START - cachedHeapEnd < totalBytes) {
      if ((long) slottedPageCapacity << 1 > MAX_SLOTTED_PAGE_CAPACITY) {
        return false;
      }
      growSlottedPage();
    }
    return true;
  }

  /**
   * Complete a direct record write. Handles DeweyID trailer, directory entry, bitmap, heap counters,
   * and flyweight binding. Called after the caller has written the record bytes via a static
   * writeNewRecord method.
   *
   * @param nodeKindId the node kind ID (e.g. NodeKind.OBJECT.getId())
   * @param nodeKey the node key
   * @param slotOffset the slot index (0-1023)
   * @param recordBytes number of bytes written by writeNewRecord
   * @param deweyIdBytes DeweyID bytes (null if not stored)
   */
  public void completeDirectWrite(final int nodeKindId, final long nodeKey, final int slotOffset, final int recordBytes,
      final byte[] deweyIdBytes) {
    if (slottedPage == null) {
      throw new IllegalStateException("Direct write was not prepared on a slotted page");
    }
    checkSideSlotNumber(slotOffset);
    if (nodeKindId < 0 || nodeKindId > 0xFF) {
      throw new IllegalArgumentException("nodeKindId out of unsigned-byte range: " + nodeKindId);
    }
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final int deweyOverhead = areDeweyIDsStored
        ? deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0;
    if (recordBytes < 0 || (long) recordBytes + deweyOverhead > PageConstants.MAX_RECORD_SIZE) {
      throw new IllegalArgumentException("direct record size is outside the inline range: " + recordBytes);
    }
    final int totalBytes = Math.addExact(recordBytes, deweyOverhead);
    final int heapEnd = cachedHeapEnd;
    if (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < totalBytes) {
      throw new IllegalStateException("Direct write exceeded its prepared inline reservation for slot "
          + slotOffset + ": " + totalBytes + " bytes");
    }
    addedReferences = false;
    clearSerializedCache();

    maybeInvalidateRegionsForExistingSlot(slotOffset);

    final boolean replacingInlineSlot = PageLayout.isSlotPopulated(slottedPage, slotOffset);
    final int replacedBytes = replacingInlineSlot
        ? PageLayout.getDirDataLength(slottedPage, slotOffset)
        : 0;

    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    // DeweyID trailer
    if (areDeweyIDsStored) {
      if (deweyIdLen > 0) {
        MemorySegment.copy(deweyIdBytes, 0, slottedPage, java.lang.foreign.ValueLayout.JAVA_BYTE,
            absOffset + recordBytes, deweyIdLen);
      }
      PageLayout.writeDeweyIdTrailer(slottedPage, absOffset + totalBytes, deweyIdLen);
    }

    // Update heap counters
    updateHeapEnd(heapEnd + totalBytes);
    updateHeapUsed(cachedHeapUsed + totalBytes - replacedBytes);

    // Directory entry
    PageLayout.setDirEntry(slottedPage, slotOffset, heapEnd, totalBytes, nodeKindId);
    clearSlotPreservation(slotOffset);

    // Bitmap
    if (!replacingInlineSlot) {
      PageLayout.markSlotPopulated(slottedPage, slotOffset);
      updatePopulatedCount(cachedPopulatedCount + 1);
      lastSlotIndex = slotOffset;
    }

    // Column write: drop every cached PAX region this kind feeds so the next reader rebuilds.
    invalidateRegionsForKindId(nodeKindId);

    if (overflowSlotSidecar != null) {
      removeSideSlot(slotOffset);
    }
    if (!references.isEmpty()
        && !isFusedOverflowDescriptor(slottedPage, absOffset, recordBytes, nodeKindId)) {
      references.remove(nodeKey);
    }

    // NOTE: Caller is responsible for binding the flyweight and setting ownerPage.
    // This eliminates interface dispatch (itable stubs) by letting the caller call
    // bind()/setOwnerPage() on the concrete type directly.
  }

  /**
   * Check whether DeweyIDs are stored on this page.
   */
  public boolean areDeweyIDsStored() {
    return areDeweyIDsStored;
  }

  /**
   * Resize a record whose varint width changed. Appends new version at heap end, updates directory,
   * re-binds, and sets ownerPage. Old space becomes dead (reclaimed on page compaction/rewrite at
   * commit time).
   *
   * @param fn the flyweight node (unbound, with updated Java fields)
   * @param nodeKey the node's key
   * @param offset the slot index within the page (0-1023)
   */
  public void resizeRecord(final FlyweightNode fn, final long nodeKey, final int offset) {
    addedReferences = false;
    clearSerializedCache();
    if (serializeToHeap(fn, nodeKey, offset)) {
      return;
    }

    final DataRecord retainedRecord = fn.isWriteSingleton()
        ? fn.toSnapshot()
        : (DataRecord) fn;
    ensureRecords();
    // Crossing the inline ceiling must make the old slot unreachable immediately. Leaving its
    // bitmap bit set makes every reader prefer stale inline bytes over the new overflow record.
    clearInlineSlotForOverflow(offset);
    records[offset] = retainedRecord;
    clearSlotPreservation(offset);
    if (overflowSlotSidecar != null) {
      removeSideSlot(offset);
    }
    if (!references.isEmpty()) {
      references.remove(nodeKey);
    }
    objectKeySlotsByName = null;
  }

  /**
   * Remove the live inline representation of a record that is moving to an OverflowPage.
   *
   * <p>The old heap allocation remains below {@code heapEnd} as ordinary bump-allocation garbage,
   * while {@code heapUsed}, the directory and the bitmap describe only live inline records. DeweyID
   * bytes are re-installed separately by {@link #processEntries(ResourceConfiguration, DataRecord[])}
   * because they are record metadata rather than part of the overflow payload.</p>
   */
  private void clearInlineSlotForOverflow(final int offset) {
    if (slottedPage == null || !PageLayout.isSlotPopulated(slottedPage, offset)) {
      return;
    }
    maybeInvalidateRegionsForExistingSlot(offset);
    final int oldDataLength = PageLayout.getDirDataLength(slottedPage, offset);
    PageLayout.clearSlotPopulated(slottedPage, offset);
    PageLayout.clearDirEntry(slottedPage, offset);
    updateHeapUsed(cachedHeapUsed - oldDataLength);
    updatePopulatedCount(cachedPopulatedCount - 1);
  }

  /**
   * Resize a single field in a bound record by raw-copying unchanged fields and re-encoding only the
   * changed field. Avoids the full unbind/re-serialize round-trip of {@link #resizeRecord}.
   *
   * <p>
   * Bump-allocates new heap space, calls {@link DeltaVarIntCodec#resizeField} to perform
   * three-segment copy (before + changed + after), preserves DeweyID trailer, updates directory, and
   * re-binds the flyweight to the new location.
   *
   * <p>
   * <b>HFT note</b>: Zero allocations. Uses {@link MemorySegment#copy} (AVX/SSE intrinsics). Cold
   * path — only called on varint width change, which is rare (~5% of mutations).
   *
   * @param fn the bound flyweight node (must be bound to this page's slotted page)
   * @param nodeKey the node's key
   * @param slotIndex the slot index within the page (0-1023)
   * @param fieldIndex the index of the field to resize (0 to fieldCount-1)
   * @param fieldCount total number of fields in this record type's offset table
   * @param encoder encodes the new field value at the target offset
   */
  public void resizeRecordField(final FlyweightNode fn, final long nodeKey, final int slotIndex, final int fieldIndex,
      final int fieldCount, final DeltaVarIntCodec.FieldEncoder encoder) {
    assert slottedPage != null : "resizeRecordField requires slotted page";
    assert PageLayout.isSlotPopulated(slottedPage, slotIndex) : "slot not populated: " + slotIndex;

    addedReferences = false;
    clearSerializedCache();

    // --- Read old record metadata from directory ---
    final int oldHeapOffset = heapOffsetOf(slottedPage, slotIndex);
    final int oldTotalLen = PageLayout.getDirDataLength(slottedPage, slotIndex);
    final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, slotIndex);
    final long oldRecordBase = PageLayout.heapAbsoluteOffset(oldHeapOffset);

    // --- Compute record-only length (excluding DeweyID trailer) ---
    final int oldRecordOnlyLen = PageLayout.getRecordOnlyLength(slottedPage, slotIndex);

    // DeweyID portion (between record data and trailer)
    final int deweyIdLen;
    final int deweyIdTrailerSize;
    if (areDeweyIDsStored) {
      deweyIdLen = PageLayout.getDeweyIdLength(slottedPage, slotIndex);
      deweyIdTrailerSize = PageLayout.DEWEY_ID_TRAILER_SIZE;
    } else {
      deweyIdLen = 0;
      deweyIdTrailerSize = 0;
    }

    // --- Estimate new size (old size ± max varint growth of 9 bytes) ---
    final int maxNewRecordLen = oldRecordOnlyLen + 9;
    final int maxNewTotalLen = maxNewRecordLen + deweyIdLen + deweyIdTrailerSize;

    // If the conservative +9-byte bound can cross the 512-byte record ceiling, the actual width
    // must be known before writing into the page's unclaimed tail. The same off-frame calculation
    // is required when a max-sized frame cannot reserve the conservative bound. It leaves the old
    // directory and the bound flyweight untouched until either an exact-size inline append or a
    // fully materialized pending overflow record is ready to publish.
    final int remaining = slottedPageCapacity - PageLayout.HEAP_START - cachedHeapEnd;
    if (maxNewTotalLen > PageConstants.MAX_RECORD_SIZE
        || remaining < maxNewTotalLen && slottedPageCapacity >= MAX_SLOTTED_PAGE_CAPACITY) {
      resizeRecordFieldOffFrame(fn, nodeKey, slotIndex, fieldIndex, fieldCount, encoder, oldRecordBase,
          oldRecordOnlyLen, oldTotalLen, nodeKindId, deweyIdLen, deweyIdTrailerSize);
      return;
    }

    // --- Ensure heap capacity ---
    final int heapEnd = cachedHeapEnd;
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < maxNewTotalLen) {
      growSlottedPage();
    }

    // --- Raw-copy resize: copy unchanged fields, re-encode changed field ---
    final long newRecordBase = PageLayout.heapAbsoluteOffset(heapEnd);
    final int newRecordLen = DeltaVarIntCodec.resizeField(slottedPage, oldRecordBase, oldRecordOnlyLen, fieldCount,
        fieldIndex, slottedPage, newRecordBase, encoder);

    // --- Copy DeweyID data + trailer from old location ---
    final int newTotalLen;
    if (areDeweyIDsStored) {
      // Copy DeweyID bytes (may be 0 length)
      if (deweyIdLen > 0) {
        final long oldDeweyStart = oldRecordBase + oldRecordOnlyLen;
        final long newDeweyStart = newRecordBase + newRecordLen;
        MemorySegment.copy(slottedPage, oldDeweyStart, slottedPage, newDeweyStart, deweyIdLen);
      }
      newTotalLen = newRecordLen + deweyIdLen + deweyIdTrailerSize;
      PageLayout.writeDeweyIdTrailer(slottedPage, newRecordBase + newTotalLen, deweyIdLen);
    } else {
      newTotalLen = newRecordLen;
    }

    // --- Update heap counters (old space becomes dead) ---
    updateHeapEnd(heapEnd + newTotalLen);
    // heapUsed: subtract old, add new (net change = newTotalLen - oldTotalLen)
    updateHeapUsed(cachedHeapUsed + newTotalLen - oldTotalLen);

    // --- Update directory entry ---
    PageLayout.setDirEntry(slottedPage, slotIndex, heapEnd, newTotalLen, nodeKindId);
    clearSlotPreservation(slotIndex);

    // --- Re-bind flyweight to new location ---
    fn.bind(slottedPage, newRecordBase, nodeKey, slotIndex);
    fn.setOwnerPage(this);

    // In-place field rewrite changed a value every cached region of this kind snapshotted.
    invalidateRegionsForKindId(nodeKindId);
    objectKeySlotsByName = null;
    if (overflowSlotSidecar != null) {
      removeSideSlot(slotIndex);
    }
    if (!references.isEmpty()) {
      references.remove(nodeKey);
    }
  }

  /**
   * Compute a field-width rewrite in bounded thread-local storage when its exact size is required.
   * The result is either appended inline at its exact size or retained for canonical overflow
   * publication, without ever exposing a partially written page slot.
   */
  private void resizeRecordFieldOffFrame(final FlyweightNode fn, final long nodeKey, final int slotIndex,
      final int fieldIndex, final int fieldCount, final DeltaVarIntCodec.FieldEncoder encoder,
      final long oldRecordBase, final int oldRecordOnlyLength, final int oldTotalLength, final int nodeKindId,
      final int deweyIdLength, final int deweyIdTrailerSize) {
    final MemorySegment scratch = RESIZE_OVERFLOW_SCRATCH.get();
    final int newRecordLength = DeltaVarIntCodec.resizeField(slottedPage, oldRecordBase, oldRecordOnlyLength,
        fieldCount, fieldIndex, scratch, 0L, encoder);
    if (newRecordLength <= 0 || newRecordLength > scratch.byteSize()) {
      throw new IllegalStateException("Invalid resized flyweight length for slot " + slotIndex + ": "
          + newRecordLength);
    }
    final int newTotalLength = Math.addExact(newRecordLength, deweyIdLength + deweyIdTrailerSize);
    if (newTotalLength <= PageConstants.MAX_RECORD_SIZE && ensureInlineAppendCapacity(newTotalLength)) {
      final int heapEnd = cachedHeapEnd;
      final long newRecordBase = PageLayout.heapAbsoluteOffset(heapEnd);
      MemorySegment.copy(scratch, 0L, slottedPage, newRecordBase, newRecordLength);
      if (areDeweyIDsStored) {
        if (deweyIdLength > 0) {
          MemorySegment.copy(slottedPage, oldRecordBase + oldRecordOnlyLength, slottedPage,
              newRecordBase + newRecordLength, deweyIdLength);
        }
        PageLayout.writeDeweyIdTrailer(slottedPage, newRecordBase + newTotalLength, deweyIdLength);
      }

      updateHeapEnd(heapEnd + newTotalLength);
      updateHeapUsed(cachedHeapUsed + newTotalLength - oldTotalLength);
      PageLayout.setDirEntry(slottedPage, slotIndex, heapEnd, newTotalLength, nodeKindId);
      clearSlotPreservation(slotIndex);
      fn.bind(slottedPage, newRecordBase, nodeKey, slotIndex);
      fn.setOwnerPage(this);
      invalidateRegionsForKindId(nodeKindId);
      objectKeySlotsByName = null;
      if (overflowSlotSidecar != null) {
        removeSideSlot(slotIndex);
      }
      if (!references.isEmpty()) {
        references.remove(nodeKey);
      }
      return;
    }

    retainResizedRecordForOverflow(fn, nodeKey, slotIndex, scratch, oldRecordBase,
        oldRecordOnlyLength, deweyIdLength);
  }

  /**
   * Materialize an already rewritten flyweight for read-your-writes. Normal
   * {@link #processEntries(ResourceConfiguration, DataRecord[])} installs its canonical overflow
   * carrier at commit.
   */
  private void retainResizedRecordForOverflow(final FlyweightNode fn, final long nodeKey, final int slotIndex,
      final MemorySegment scratch, final long oldRecordBase,
      final int oldRecordOnlyLength, final int deweyIdLength) {
    final byte[] deweyIdBytes;
    if (areDeweyIDsStored && deweyIdLength > 0) {
      deweyIdBytes = new byte[deweyIdLength];
      MemorySegment.copy(slottedPage, oldRecordBase + oldRecordOnlyLength, MemorySegment.ofArray(deweyIdBytes),
          0L, deweyIdLength);
    } else {
      deweyIdBytes = null;
    }

    try {
      fn.bind(scratch, 0L, nodeKey, slotIndex);
      if (fn instanceof Node node) {
        node.setDeweyIDBytes(deweyIdBytes);
      }
      fn.unbind();
    } catch (final RuntimeException | Error failure) {
      // The old directory and bytes are still untouched. Restore the caller's binding before
      // propagating the failure so a failed cold-path conversion cannot leave a singleton pointing
      // at thread-local scratch that will be overwritten by the next resize.
      fn.bind(slottedPage, oldRecordBase, nodeKey, slotIndex);
      fn.setOwnerPage(this);
      throw failure;
    }

    final DataRecord retainedRecord;
    try {
      retainedRecord = fn.isWriteSingleton()
          ? fn.toSnapshot()
          : (DataRecord) fn;
      ensureRecords();
    } catch (final RuntimeException | Error failure) {
      fn.bind(slottedPage, oldRecordBase, nodeKey, slotIndex);
      fn.setOwnerPage(this);
      throw failure;
    }

    clearInlineSlotForOverflow(slotIndex);
    records[slotIndex] = retainedRecord;
    clearSlotPreservation(slotIndex);
    if (overflowSlotSidecar != null) {
      removeSideSlot(slotIndex);
    }
    if (!references.isEmpty()) {
      references.remove(nodeKey);
    }
    objectKeySlotsByName = null;
    addedReferences = false;
    clearSerializedCache();
  }

  /**
   * Zero-copy raw slot bytes from source page to this page's heap. Copies the record body + DeweyID
   * trailer verbatim, avoiding deserialize-serialize round-trip.
   *
   * @param sourcePage the source page to copy from
   * @param slotIndex the slot index to copy
   */
  public void copySlotFromPage(final KeyValueLeafPage sourcePage, final int slotIndex) {
    if (sourcePage == null) {
      throw new NullPointerException("sourcePage");
    }
    if (sourcePage == this) {
      return;
    }
    if (sourcePage.recordPageKey != recordPageKey) {
      throw new IllegalArgumentException("Source and target page keys differ: " + sourcePage.recordPageKey
          + " != " + recordPageKey);
    }
    if (sourcePage.areDeweyIDsStored != areDeweyIDsStored) {
      throw new IllegalArgumentException("Source and target disagree about DeweyID storage");
    }
    MemorySegment srcPage = sourcePage.getSlottedPage();
    if ((srcPage == null || !PageLayout.isSlotPopulated(srcPage, slotIndex)) && sourcePage.hasSideSlot(slotIndex)) {
      final long recordKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotIndex;
      final PageReference companion = sourcePage.getPageReference(recordKey);
      if (companion == null) {
        throw new IllegalStateException("Side-slot carrier for record " + recordKey
            + " has no companion page reference");
      }
      final OverflowSlotSidecar sourceSidecar = sourcePage.overflowSlotSidecar;
      final MemorySegment sourceImage = sourceSidecar.image(slotIndex);
      final int sourceLength = sourceSidecar.imageLength(slotIndex);
      final long token = prepareSideSlot(sourceSidecar.kind(slotIndex), sourceImage, sourceLength);
      references.put(recordKey, companion);
      publishSideSlot(slotIndex, token);
      clearInlineSlotForOverflow(slotIndex);
      return;
    }
    if (srcPage == null || !PageLayout.isSlotPopulated(srcPage, slotIndex)) {
      return;
    }
    // The bytes come from the SOURCE page's heap, so it is the source that has to have them.
    sourcePage.ensureChunkFor(slotIndex);
    srcPage = sourcePage.getSlottedPage();
    if (srcPage == null || !PageLayout.isSlotPopulated(srcPage, slotIndex)) {
      throw new IllegalStateException("Source slot disappeared during materialization: " + slotIndex);
    }
    ensureSlottedPage();

    // Read source slot metadata
    final int srcHeapOffset = PageLayout.getDirHeapOffset(srcPage, slotIndex);
    final int srcTotalLen = PageLayout.getDirDataLength(srcPage, slotIndex);
    final int srcNodeKindId = PageLayout.getDirNodeKindId(srcPage, slotIndex);
    final long srcAbs = PageLayout.heapAbsoluteOffset(srcHeapOffset);
    if (srcTotalLen <= 0) {
      throw new IllegalStateException("Source slot has invalid length " + srcTotalLen + ": " + slotIndex);
    }
    final long recordKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotIndex;
    final boolean sourceOverflowDescriptor = sourcePage.isFusedOverflowDescriptor(slotIndex);
    final PageReference sourceCompanion;
    if (sourceOverflowDescriptor) {
      sourceCompanion = sourcePage.getPageReference(recordKey);
      if (sourceCompanion == null) {
        throw new IllegalStateException("Overflow descriptor for record " + recordKey
            + " has no companion page reference");
      }
    } else {
      sourceCompanion = null;
    }

    if (srcTotalLen > PageConstants.MAX_RECORD_SIZE) {
      spillCopiedInlineSlotToOverflow(sourcePage, slotIndex, srcPage, srcAbs, srcTotalLen, srcNodeKindId);
      return;
    }

    // Ensure destination has enough space. Version reconstruction can merge individually bounded
    // source slots into a target whose 256-KiB frame is already full; growing past the allocator's
    // final size class is not an option, so preserve the record through its canonical overflow
    // carrier instead.
    final int heapEnd = cachedHeapEnd;
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < srcTotalLen) {
      if ((long) slottedPageCapacity << 1 > MAX_SLOTTED_PAGE_CAPACITY) {
        spillCopiedInlineSlotToOverflow(sourcePage, slotIndex, srcPage, srcAbs, srcTotalLen, srcNodeKindId);
        return;
      }
      growSlottedPage();
    }

    // A descriptor and its same-key reference publish as one logical record. All failable frame
    // growth and companion validation completed above; install the map entry before the fixed-size
    // memory/directory publication below.
    if (sourceCompanion != null) {
      references.put(recordKey, sourceCompanion);
    }

    // Copy raw bytes (record body + DeweyID trailer) from source to destination heap
    final long dstAbs = PageLayout.heapAbsoluteOffset(heapEnd);
    MemorySegment.copy(srcPage, srcAbs, slottedPage, dstAbs, srcTotalLen);

    final boolean replacingSlot = PageLayout.isSlotPopulated(slottedPage, slotIndex);
    final int replacedBytes = replacingSlot
        ? PageLayout.getDirDataLength(slottedPage, slotIndex)
        : 0;

    // Update destination heap end and used counters
    updateHeapEnd(heapEnd + srcTotalLen);
    updateHeapUsed(cachedHeapUsed + srcTotalLen - replacedBytes);

    // Update destination directory entry
    PageLayout.setDirEntry(slottedPage, slotIndex, heapEnd, srcTotalLen, srcNodeKindId);
    clearSlotPreservation(slotIndex);

    // Mark slot populated in bitmap
    if (!replacingSlot) {
      PageLayout.markSlotPopulated(slottedPage, slotIndex);
      updatePopulatedCount(cachedPopulatedCount + 1);
      lastSlotIndex = slotIndex;
    }

    // Invalidate compressed cache
    clearSerializedCache();
    addedReferences = false;

    // Column copy: a new value lands in this page's heap. The source's region is not carried, and
    // any region cached for THIS page is now incomplete.
    invalidateRegionsForKindId(srcNodeKindId);

    // A fused large-string descriptor and its same-key overflow reference are one logical carrier.
    // Copying just the metadata half makes the field discoverable but unreadable; copying an
    // ordinary inline record must retire any stale overflow half already present on the target.
    removeSideSlot(slotIndex);
    if (sourceCompanion == null) {
      references.remove(recordKey);
    }
  }

  /** Divert one complete source-inline slot when a version-combined target frame is at its ceiling. */
  private void spillCopiedInlineSlotToOverflow(final KeyValueLeafPage sourcePage, final int slotIndex,
      final MemorySegment srcPage, final long srcAbs, final int srcTotalLen, final int srcNodeKindId) {
    final long recordKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotIndex;
    if (sourcePage.isFusedOverflowDescriptor(slotIndex)) {
      final PageReference companion = sourcePage.getPageReference(recordKey);
      if (companion == null) {
        throw new IllegalStateException("Overflow descriptor for record " + recordKey
            + " has no companion page reference");
      }
      final MemorySegment sourceImage = srcPage.asSlice(srcAbs, srcTotalLen);
      final long token = prepareSideSlot(srcNodeKindId, sourceImage, srcTotalLen);
      references.put(recordKey, companion);
      publishSideSlot(slotIndex, token);
      clearInlineSlotForOverflow(slotIndex);
      return;
    }

    final MemorySegment sourceDeweyId = sourcePage.getDeweyId(slotIndex);
    final byte[] deweyIdBytes = sourceDeweyId == null
        ? null
        : sourceDeweyId.toArray(ValueLayout.JAVA_BYTE);
    final DataRecord snapshot;
    if (srcNodeKindId > 0) {
      final FlyweightNode flyweight = FlyweightNodeFactory.createAndBind(srcPage, slotIndex, recordKey,
          resourceConfig.nodeHashFunction);
      try {
        sourcePage.attachFsstSymbolTable((DataRecord) flyweight);
        if (deweyIdBytes != null && flyweight instanceof Node node) {
          node.setDeweyIDBytes(deweyIdBytes);
        }
        snapshot = flyweight.toSnapshot();
      } finally {
        flyweight.clearBinding();
      }
    } else {
      final int recordLength = PageLayout.getRecordOnlyLength(srcPage, slotIndex);
      final MemorySegment recordImage = srcPage.asSlice(srcAbs, recordLength);
      snapshot = recordPersister.deserialize(new MemorySegmentBytesIn(recordImage), recordKey, deweyIdBytes,
          resourceConfig);
      sourcePage.attachFsstSymbolTable(snapshot);
    }
    canonicalizeOverflowString(snapshot);
    installCanonicalOverflowCarrier(snapshot, serializeOverflowRecord(snapshot), slotIndex, deweyIdBytes);
  }

  /**
   * Check if the slotted page has a populated slot for the given record key.
   *
   * @param recordKey the record key
   * @return true if the slot contains a complete inline record; metadata-only overflow descriptors
   *         deliberately return false so write/read singleton binders resolve their companion page
   */
  public boolean hasSlottedPageSlot(final long recordKey) {
    if (slottedPage == null) {
      return false;
    }
    final int offset =
        (int) (recordKey - ((recordKey >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
    return PageLayout.isSlotPopulated(slottedPage, offset)
        && !isFusedOverflowDescriptor(offset);
  }

  /**
   * Allocate and initialize the slotted page if not yet present.
   */
  public void ensureSlottedPage() {
    if (slottedPage != null) {
      return;
    }
    final MemorySegment allocated = segmentAllocator.allocate(PageLayout.INITIAL_PAGE_SIZE);
    try {
      final int allocatedCapacity = (int) allocated.byteSize();
      PageLayout.initializePage(allocated, recordPageKey, revision, indexType.getID(), areDeweyIDsStored);
      // Bound the view to the REAL capacity: reinterpret(Long.MAX_VALUE) disabled every FFM
      // bounds check on the hottest read/write surface, turning any directory/heap-offset bug
      // into silent cross-segment corruption inside the shared region instead of an exception.
      publishSlottedPage(allocated.reinterpret(allocatedCapacity));
      slottedPageCapacity = allocatedCapacity;
      cachedHeapEnd = 0;
      cachedHeapUsed = 0;
      cachedPopulatedCount = 0;
    } catch (final RuntimeException | Error failure) {
      // A constructor which throws is never observable, so close() cannot recover this frame.
      // Release it here, including the unlikely case where publication itself failed after storing
      // the segment. Cleanup diagnostics must never replace the original initialization failure.
      try {
        final MemorySegment published = slottedPage;
        if (published != null && published.address() == allocated.address()) {
          stampBaseSegment = null;
          slottedPage = null;
          stampCoordinates = STAMP_COORDINATES_UNBOUND;
          final long generation = stampBindingGeneration;
          stampBindingGeneration = (generation & 1L) == 0L
              ? generation + 2L
              : generation + 1L;
        }
        slottedPageCapacity = 0;
      } catch (final Throwable rollbackFailure) {
        addSuppressedBestEffort(failure, rollbackFailure);
      }
      try {
        segmentAllocator.release(allocated);
      } catch (final Throwable releaseFailure) {
        addSuppressedBestEffort(failure, releaseFailure);
      }
      throw failure;
    }
  }

  /** Release a fully published eager frame when later constructor work fails. */
  private void releaseConstructorFrameAfterFailure(final Throwable primaryFailure) {
    final MemorySegment frame = slottedPage;
    if (frame == null) {
      return;
    }

    // This instance was never published, but leave its internal binding coherent before returning
    // the frame so a diagnostic/cleanup reference cannot retain a dangling segment.
    stampBaseSegment = null;
    slottedPage = null;
    stampCoordinates = STAMP_COORDINATES_UNBOUND;
    final long generation = stampBindingGeneration;
    stampBindingGeneration = (generation & 1L) == 0L
        ? generation + 2L
        : generation + 1L;
    slottedPageCapacity = 0;
    try {
      segmentAllocator.release(frame);
    } catch (final Throwable releaseFailure) {
      addSuppressedBestEffort(primaryFailure, releaseFailure);
    }
  }

  private static void addSuppressedBestEffort(final Throwable primaryFailure, final Throwable cleanupFailure) {
    if (cleanupFailure == primaryFailure) {
      return;
    }
    try {
      primaryFailure.addSuppressed(cleanupFailure);
    } catch (final Throwable ignored) {
      // Retaining the original frame-initialization failure is more important than diagnostics.
    }
  }

  /**
   * Bulk-copy the slotted-page state from {@code src} into a buffer owned by this page. Used by the
   * single-fragment combine fast path to avoid the per-slot {@code setSlotWithNodeKind} loop; one
   * MemorySegment.copy replaces ~1024 small copies + directory writes + bitmap updates.
   *
   * <p>
   * If {@code this} already has a slotted page (via eager {@code ensureSlottedPage} in the
   * constructor) and its capacity does not match, the replacement is allocated and published and only
   * then is the old one released — the constructor's allocation is wasted for the combine path, but
   * reusing it in place requires handling size-class mismatches that rarely hit. Net: trade one 64
   * KiB release for a 1024× loop skip.
   *
   * <p>
   * Overwrites the header's revision field after the copy so downstream readers observe this page's
   * target revision (not the donor fragment's).
   */
  public void copySlottedPageFrom(final KeyValueLeafPage src) {
    src.refuseUnresolvedGlobalTags("the versioning combine's slotted-page copy");
    src.ensureAllChunks();
    final MemorySegment srcSp = src.slottedPage;
    if (srcSp == null) {
      return;
    }
    final int srcCap = src.slottedPageCapacity;
    final MemorySegment dst;
    // Reuse the constructor's eagerly-allocated slotted page when capacity
    // matches — saves a release+reallocate round-trip through the frame
    // allocator. Capacities almost always match (both sides default to
    // INITIAL_PAGE_SIZE = 64 KiB) so this is the hot path.
    if (slottedPage != null && slottedPageCapacity == srcCap) {
      dst = slottedPage.reinterpret(srcCap);
    } else {
      // Allocate and PUBLISH before releasing the old segment — see growSlottedPage for why a slot
      // must not be released while the page still points at it. Also leaves the page holding its
      // previous segment rather than nothing if the allocation throws, and saves the intermediate
      // publish of null, which was a second rebind for no reader's benefit.
      final MemorySegment previous = slottedPage;
      final int previousCapacity = slottedPageCapacity;
      dst = segmentAllocator.allocate(srcCap);
      slottedPageCapacity = (int) dst.byteSize();
      publishSlottedPage(dst.reinterpret(slottedPageCapacity)); // capacity-bounded: keep FFM bounds checks
      if (previous != null) {
        try {
          segmentAllocator.release(previous.reinterpret(previousCapacity));
        } catch (final Throwable e) {
          LOGGER.debug("Release of pre-existing slottedPage before copy failed: {}", e.getMessage());
        }
      }
    }
    MemorySegment.copy(srcSp, 0, dst, 0, srcCap);
    cachedHeapEnd = src.cachedHeapEnd;
    cachedHeapUsed = src.cachedHeapUsed;
    cachedPopulatedCount = src.cachedPopulatedCount;
    lastSlotIndex = src.lastSlotIndex;
    // src's header carries src's revision — overwrite with target revision.
    PageLayout.setRevision(dst, revision);
  }

  /**
   * Grow the slotted page by doubling its size. Copies all existing data (header + bitmap + directory
   * + heap) to the new segment.
   */
  private void growSlottedPage() {
    final int currentSize = slottedPageCapacity;
    if (slottedPage == null || currentSize <= 0) {
      throw new IllegalStateException("Cannot grow an uninitialized slotted page");
    }
    if (currentSize >= MAX_SLOTTED_PAGE_CAPACITY
        || currentSize > MAX_SLOTTED_PAGE_CAPACITY >>> 1) {
      throw new IllegalStateException("Slotted page is already at its capacity ceiling: " + currentSize);
    }
    final int newSize = currentSize << 1;
    if (newSize > MAX_SLOTTED_PAGE_CAPACITY) {
      throw new IllegalStateException("Slotted-page growth exceeds capacity ceiling: " + newSize);
    }
    final MemorySegment grown = segmentAllocator.allocate(newSize);
    if (grown.byteSize() < newSize || grown.byteSize() > MAX_SLOTTED_PAGE_CAPACITY) {
      final IllegalStateException failure = new IllegalStateException(
          "Allocator returned invalid slotted-page frame size: " + grown.byteSize());
      try {
        segmentAllocator.release(grown);
      } catch (final Throwable cleanupFailure) {
        addSuppressedBestEffort(failure, cleanupFailure);
      }
      throw failure;
    }
    final MemorySegment previous = slottedPage;
    try {
      // Copy all existing data before publication; an allocation/copy failure leaves the old page
      // authoritative and releases the unpublished frame.
      MemorySegment.copy(previous, 0, grown, 0, currentSize);
    } catch (final RuntimeException | Error failure) {
      try {
        segmentAllocator.release(grown);
      } catch (final Throwable cleanupFailure) {
        addSuppressedBestEffort(failure, cleanupFailure);
      }
      throw failure;
    }
    slottedPageCapacity = (int) grown.byteSize();
    publishSlottedPage(grown.reinterpret(slottedPageCapacity)); // capacity-bounded: keep FFM bounds checks
    // Release the old segment only AFTER the new one is published, not before. While the page still
    // points at a segment, an optimistic reader's stamp resolves to that segment's slot — and a slot
    // released under a live binding is one whose bytes can be handed to another page while the
    // binding still certifies reads of them. Publishing first means every such reader has already
    // been invalidated by the rebind. Costs one extra live slot for the duration of the copy.
    // (Reinterpret back to the actual size for the allocator.)
    segmentAllocator.release(previous.reinterpret(currentSize));
    // No rebind needed: the caller (serializeToHeap) will rebind the active flyweight.
    // Cached header values remain valid — grow copies all data including header.
  }

  // ==================== WRITE-THROUGH HELPERS ====================

  private void updateHeapEnd(final int val) {
    cachedHeapEnd = val;
    PageLayout.setHeapEnd(slottedPage, val);
  }

  private void updateHeapUsed(final int val) {
    cachedHeapUsed = val;
    PageLayout.setHeapUsed(slottedPage, val);
  }

  private void updatePopulatedCount(final int val) {
    cachedPopulatedCount = val;
    PageLayout.setPopulatedCount(slottedPage, val);
  }

  int getCachedHeapEnd() {
    return cachedHeapEnd;
  }

  int getCachedHeapUsed() {
    return cachedHeapUsed;
  }

  public int getCachedPopulatedCount() {
    return cachedPopulatedCount;
  }

  void assertNoDrift() {
    assert cachedHeapEnd == PageLayout.getHeapEnd(slottedPage)
        : "heapEnd drift: cached=" + cachedHeapEnd + " segment=" + PageLayout.getHeapEnd(slottedPage);
    assert cachedHeapUsed == PageLayout.getHeapUsed(slottedPage)
        : "heapUsed drift: cached=" + cachedHeapUsed + " segment=" + PageLayout.getHeapUsed(slottedPage);
    assert cachedPopulatedCount == PageLayout.getPopulatedCount(slottedPage) : "populatedCount drift: cached="
        + cachedPopulatedCount + " segment=" + PageLayout.getPopulatedCount(slottedPage);
  }

  /**
   * Write raw slot data to the slotted page heap. Used by setSlot() and addReferences() when
   * slottedPage is active. Data is stored without a length prefix — the directory entry holds the
   * length.
   *
   * @param data the raw slot data to store
   * @param slotNumber the slot index (0-1023)
   * @param nodeKindId the node kind ID (0 for legacy format, &gt;0 for flyweight)
   */
  private void setSlotToHeap(final MemorySegment data, final int slotNumber, final int nodeKindId) {
    if (data == null) {
      throw new NullPointerException("data");
    }
    checkSideSlotNumber(slotNumber);
    if (nodeKindId < 0 || nodeKindId > 0xFF) {
      throw new IllegalArgumentException("nodeKindId out of unsigned-byte range: " + nodeKindId);
    }
    final int recordSize = Math.toIntExact(data.byteSize());
    if (recordSize <= 0) {
      return;
    }

    // Total allocation includes DeweyID trailer when DeweyIDs are stored
    final int totalSize = Math.addExact(recordSize, areDeweyIDsStored
        ? PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0);

    // This private sink is also its own invariant boundary. Public callers preflight for overflow,
    // but no future raw call site may silently grow beyond the allocator's final frame class.
    if (!ensureInlineAppendCapacity(totalSize)) {
      throw new IllegalStateException("No inline capacity remains for raw slot " + slotNumber
          + "; caller must publish a canonical overflow carrier");
    }
    final int heapEnd = cachedHeapEnd;

    // Bump-allocate and copy record data to heap
    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    MemorySegment.copy(data, 0, slottedPage, absOffset, recordSize);

    // Append DeweyID trailer (initially 0 = no DeweyID yet)
    if (areDeweyIDsStored) {
      PageLayout.writeDeweyIdTrailer(slottedPage, absOffset + totalSize, 0);
    }

    final boolean replacingSlot = PageLayout.isSlotPopulated(slottedPage, slotNumber);
    final int replacedBytes = replacingSlot
        ? PageLayout.getDirDataLength(slottedPage, slotNumber)
        : 0;

    // Update heap end and used counters
    updateHeapEnd(heapEnd + totalSize);
    updateHeapUsed(cachedHeapUsed + totalSize - replacedBytes);

    // Update directory entry with the provided nodeKindId
    PageLayout.setDirEntry(slottedPage, slotNumber, heapEnd, totalSize, nodeKindId);
    clearSlotPreservation(slotNumber);

    // Mark slot populated in bitmap and track last slot index (new slots only)
    if (!replacingSlot) {
      PageLayout.markSlotPopulated(slottedPage, slotNumber);
      updatePopulatedCount(cachedPopulatedCount + 1);
      lastSlotIndex = slotNumber;
    }
  }

  /**
   * Write raw slot data from a source segment at a given offset to the slotted page heap. Zero-copy
   * variant for direct page deserialization.
   *
   * @param source the source MemorySegment containing the data
   * @param sourceOffset byte offset within source where data starts
   * @param dataSize number of bytes to copy
   * @param slotNumber the slot index (0-1023)
   * @param nodeKindId the node kind ID (0 for legacy format, 24-43 for flyweight)
   */
  void setSlotToHeapDirect(final MemorySegment source, final long sourceOffset, final int dataSize,
      final int slotNumber, final int nodeKindId) {
    if (source == null) {
      throw new NullPointerException("source");
    }
    checkSideSlotNumber(slotNumber);
    if (nodeKindId < 0 || nodeKindId > 0xFF) {
      throw new IllegalArgumentException("nodeKindId out of unsigned-byte range: " + nodeKindId);
    }
    if (dataSize <= 0) {
      return;
    }
    if (sourceOffset < 0 || sourceOffset > source.byteSize() - dataSize) {
      throw new IndexOutOfBoundsException("source range: offset=" + sourceOffset + ", size=" + dataSize
          + ", capacity=" + source.byteSize());
    }

    // Total allocation includes DeweyID trailer when DeweyIDs are stored — mirrors setSlotToHeap.
    // Without this, getRecordOnlyLength misreads the directory entry as record+trailer and
    // returns a negative recordLength, which downstream callers (e.g. getSlot) surface as null
    // and crash the SLIDING_SNAPSHOT page-combine path (UpdateTest regression seen on 6eaa56d25).
    final int totalSize = Math.addExact(dataSize, areDeweyIDsStored
        ? PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0);

    if (!ensureInlineAppendCapacity(totalSize)) {
      throw new IllegalStateException("No inline capacity remains for raw slot " + slotNumber
          + "; caller must publish a canonical overflow carrier");
    }
    final int heapEnd = cachedHeapEnd;

    // Bump-allocate and copy data to heap
    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    MemorySegment.copy(source, sourceOffset, slottedPage, absOffset, dataSize);

    // Append DeweyID trailer (initially 0 = no DeweyID yet)
    if (areDeweyIDsStored) {
      PageLayout.writeDeweyIdTrailer(slottedPage, absOffset + totalSize, 0);
    }

    final boolean replacingSlot = PageLayout.isSlotPopulated(slottedPage, slotNumber);
    final int replacedBytes = replacingSlot
        ? PageLayout.getDirDataLength(slottedPage, slotNumber)
        : 0;

    // Update heap end and used counters — by totalSize so the trailer is accounted for.
    updateHeapEnd(heapEnd + totalSize);
    updateHeapUsed(cachedHeapUsed + totalSize - replacedBytes);

    // Directory entry length is totalSize (record + trailer); getRecordOnlyLength subtracts
    // the trailer + DeweyID payload to recover the record-only span on read.
    PageLayout.setDirEntry(slottedPage, slotNumber, heapEnd, totalSize, nodeKindId);
    clearSlotPreservation(slotNumber);

    // Mark slot populated in bitmap
    if (!replacingSlot) {
      PageLayout.markSlotPopulated(slottedPage, slotNumber);
      updatePopulatedCount(cachedPopulatedCount + 1);
    }
  }

  /**
   * Get bytes to serialize.
   *
   * @return bytes
   */
  public BytesOut<?> getBytes() {
    return bytes;
  }

  /**
   * Set bytes after serialization (legacy byte[] path).
   *
   * @param bytes bytes
   */
  public void setBytes(final BytesOut<?> bytes) {
    publishBytes(bytes, UNKNOWN_BYTE_HANDLER_INPUT_LENGTH);
  }

  /**
   * Publish the legacy wire cache together with its exact pre-byte-handler length.
   *
   * @param bytes encoded wire bytes
   * @param inputLength exact serialized length before the outer byte-handler pipeline
   */
  public void setBytes(final BytesOut<?> bytes, final int inputLength) {
    if (inputLength < 0) {
      throw new IllegalArgumentException("byte-handler input length must be non-negative: " + inputLength);
    }
    publishBytes(bytes, inputLength);
  }

  private void publishBytes(final BytesOut<?> newBytes, final int inputLength) {
    if (newBytes == null) {
      clearSerializedCache();
      return;
    }
    compressedSegment = null;
    byteHandlerInputLength = inputLength;
    // Volatile release-publishes byteHandlerInputLength.
    bytes = newBytes;
  }

  /**
   * Get the compressed page data as a MemorySegment (zero-copy path).
   *
   * @return the compressed segment, or null if not set
   */
  public MemorySegment getCompressedSegment() {
    return compressedSegment;
  }

  /**
   * Set compressed page data as a MemorySegment (zero-copy path). Clears the legacy bytes cache.
   *
   * @param segment encoded segment with a lifetime owned by the publishing page/pipeline
   */
  public void setCompressedSegment(final MemorySegment segment) {
    publishCompressedSegment(segment, UNKNOWN_BYTE_HANDLER_INPUT_LENGTH);
  }

  /**
   * Publish the zero-copy wire cache together with its exact pre-byte-handler length.
   *
   * @param segment encoded wire bytes
   * @param inputLength exact serialized length before the outer byte-handler pipeline
   */
  public void setCompressedSegment(final MemorySegment segment, final int inputLength) {
    if (inputLength < 0) {
      throw new IllegalArgumentException("byte-handler input length must be non-negative: " + inputLength);
    }
    publishCompressedSegment(segment, inputLength);
  }

  private void publishCompressedSegment(final MemorySegment segment, final int inputLength) {
    if (segment == null) {
      clearSerializedCache();
      return;
    }
    bytes = null;
    byteHandlerInputLength = inputLength;
    // Volatile release-publishes byteHandlerInputLength.
    compressedSegment = segment;
  }

  /**
   * Return the exact serialized length before the outer byte-handler pipeline, or
   * {@link #UNKNOWN_BYTE_HANDLER_INPUT_LENGTH}. Read only after acquiring a non-null wire cache.
   */
  public int getByteHandlerInputLength() {
    return byteHandlerInputLength;
  }

  /** Drop both wire-cache representations and their associated profiling metadata. */
  private void clearSerializedCache() {
    compressedSegment = null;
    bytes = null;
    byteHandlerInputLength = UNKNOWN_BYTE_HANDLER_INPUT_LENGTH;
  }

  /**
   * Release node object references to allow GC to reclaim them.
   * <p>
   * MUST only be called after {@code addReferences()} has serialized all records into
   * {@code slotMemory}. Normally the compressed form is already cached via
   * {@code setCompressedSegment()} or {@code setBytes()}; the async disposable-copy path instead
   * keeps the serialized sink alive until it copies that exact prefix into the page's frame after
   * this method returns. After this call, individual records can still be reconstructed on demand
   * from {@code slotMemory} via {@code getSlot(offset)} in
   * {@link io.sirix.access.trx.page.NodeStorageEngineReader#getValue}.
   */
  public void clearRecordsForGC() {
    if (records == null) {
      return;
    }
    // Unbind flyweight nodes BEFORE clearing — cursors may still hold references.
    // Unbinding materializes all fields from page memory (still valid at this point)
    // into Java primitives, so reads after page release use correct field values.
    unbindFlyweightsOwnedBy(slottedPage);
    Arrays.fill(records, null);
  }

  /** Materialize only bindings owned by this page; a foreign binding belongs to another frame. */
  private void unbindFlyweightsOwnedBy(final MemorySegment owner) {
    if (owner == null || records == null) {
      return;
    }
    for (final DataRecord record : records) {
      if (record instanceof FlyweightNode flyweight && flyweight.isBoundTo(owner)) {
        flyweight.unbind();
      }
    }
  }

  /**
   * Check whether all non-null records have been serialized to slotMemory.
   *
   * @return {@code true} if {@link #addReferences} has been called and no subsequent
   *         {@link #setRecord} has invalidated the serialized state
   */
  public boolean isAddedReferences() {
    return addedReferences;
  }

  @Override
  public DataRecord[] records() {
    ensureRecords();
    return records;
  }

  public byte[] getHashCode() {
    return hashCode;
  }

  public void setHashCode(byte[] hashCode) {
    this.hashCode = hashCode;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public <I extends Iterable<DataRecord>> I values() {
    final DataRecord[] r = records;
    return (I) new ArrayIterator(r != null
        ? r
        : EMPTY_RECORDS,
        r != null
            ? r.length
            : 0);
  }

  public Map<Long, PageReference> getReferencesMap() {
    return references;
  }

  /**
   * Set reference to the complete page for lazy slot copying at commit time. Used by DIFFERENTIAL,
   * INCREMENTAL (full-dump), and SLIDING_SNAPSHOT versioning.
   *
   * @param completePage the complete page to copy slots from
   */
  public void setCompletePageRef(KeyValueLeafPage completePage) {
    this.completePageRef = completePage;
  }

  /**
   * Get the complete page reference for lazy copying.
   *
   * @return the complete page reference, or null if not set
   */
  public KeyValueLeafPage getCompletePageRef() {
    return completePageRef;
  }

  /**
   * Mark a slot for preservation during lazy copy at commit time. At addReferences(), if this slot
   * has records[i] == null, it will be copied from completePageRef.
   *
   * @param slotNumber the slot number to mark for preservation (0 to Constants.NDP_NODE_COUNT-1)
   */
  public void markSlotForPreservation(int slotNumber) {
    checkSideSlotNumber(slotNumber);
    ensureSlottedPage();
    PageLayout.markSlotPreserved(slottedPage, slotNumber);
  }

  /** A newly published carrier shadows the deferred copy of this slot from an older complete page. */
  private void clearSlotPreservation(final int slotNumber) {
    final MemorySegment page = slottedPage;
    if (page == null) {
      return;
    }
    final int wordIndex = slotNumber >>> 6;
    final long wordOffset = PageLayout.PRESERVATION_BITMAP_OFF + ((long) wordIndex << 3);
    final long word = page.get(LE.LONG, wordOffset);
    final long bit = 1L << (slotNumber & 63);
    if ((word & bit) != 0L) {
      page.set(LE.LONG, wordOffset, word & ~bit);
    }
  }

  /**
   * Whether this page already owns the current logical value of a deferred-preservation slot.
   * A bare reference does not suffice when the complete page says it is the companion of an inline
   * fused descriptor; that descriptor must still be copied. New reference-only publications clear
   * the marker at publication time, so this distinction is only a defensive reconstruction check.
   */
  private boolean hasLogicalCarrierShadowingPreservation(final int slotNumber,
      final KeyValueLeafPage completePage) {
    if (records != null && records[slotNumber] != null
        || slottedPage != null && PageLayout.isSlotPopulated(slottedPage, slotNumber)
        || hasSideSlot(slotNumber)) {
      return true;
    }
    final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber;
    if (references.get(nodeKey) == null) {
      return false;
    }
    // A reference alone is not the complete logical carrier when the authoritative page has a
    // side image: fused scan metadata and/or Dewey bytes still have to be materialized beside it.
    if (completePage != null && completePage.hasSideSlot(slotNumber)) {
      return false;
    }
    final MemorySegment completeSlottedPage = completePage == null
        ? null
        : completePage.slottedPage;
    return completeSlottedPage == null
        || !PageLayout.isSlotPopulated(completeSlottedPage, slotNumber)
        || !completePage.isFusedOverflowDescriptor(slotNumber);
  }

  /**
   * Check if a slot is marked for preservation.
   *
   * @param slotNumber the slot number to check
   * @return true if the slot needs preservation
   */
  public boolean isSlotMarkedForPreservation(int slotNumber) {
    return slottedPage != null && PageLayout.isSlotPreserved(slottedPage, slotNumber);
  }

  /**
   * Get the preservation bitmap for testing/debugging. Returns a fresh copy from the slotted page
   * MemorySegment.
   *
   * @return a fresh long[16] copy, or null if slotted page is not initialized
   */
  public long[] getPreservationBitmap() {
    if (slottedPage == null) {
      return null;
    }
    final long[] copy = new long[BITMAP_WORDS];
    for (int i = 0; i < BITMAP_WORDS; i++) {
      copy[i] = slottedPage.get(LE.LONG, PageLayout.PRESERVATION_BITMAP_OFF + ((long) i << 3));
    }
    return copy;
  }

  /**
   * Check if any slots are marked for preservation.
   *
   * @return true if any slot in the preservation bitmap is set
   */
  public boolean hasPreservationSlots() {
    return slottedPage != null && PageLayout.hasPreservedSlots(slottedPage);
  }


  @Override
  public void setSlot(byte[] recordData, int slotNumber) {
    setSlotWithNodeKind(MemorySegment.ofArray(recordData), slotNumber, 0);
  }

  @Override
  public void setSlot(MemorySegment data, int slotNumber) {
    setSlotWithNodeKind(data, slotNumber, 0);
  }

  /**
   * Set slot data with an explicit nodeKindId. Used during page combining to preserve the flyweight
   * format indicator from the source page.
   *
   * @param data the raw slot data to store
   * @param slotNumber the slot index (0-1023)
   * @param nodeKindId the node kind ID (0 for legacy, &gt;0 for flyweight)
   */
  public void setSlotWithNodeKind(final MemorySegment data, final int slotNumber, final int nodeKindId) {
    final long inlineBytes = data.byteSize() + (areDeweyIDsStored
        ? PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0);
    if (inlineBytes > PageConstants.MAX_RECORD_SIZE) {
      throw new IllegalArgumentException("Raw slotted-page bytes cannot be installed as an OverflowPage: "
          + inlineBytes + " bytes for slot " + slotNumber);
    }
    if (!ensureInlineAppendCapacity(Math.toIntExact(inlineBytes))) {
      throw new IllegalStateException("No inline capacity remains for raw slot " + slotNumber
          + "; caller must publish a canonical overflow carrier");
    }
    setSlotToHeap(data, slotNumber, nodeKindId);
    if (overflowSlotSidecar != null) {
      removeSideSlot(slotNumber);
    }
    if (!isFusedOverflowDescriptor(data, nodeKindId)) {
      references.remove((recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber);
    }
  }

  /** Install a canonical generic record and its complete inline companion, if one is required. */
  private void installCanonicalOverflowCarrier(final DataRecord record, final byte[] recordBytes,
      final int slotNumber, final byte[] deweyIdBytes) {
    if (record == null) {
      throw new NullPointerException("record");
    }
    if (recordBytes == null) {
      throw new NullPointerException("recordBytes");
    }
    if (isCompressedStringRecord(record)) {
      throw new IllegalStateException("Overflow carrier must be canonical raw before installation: "
          + record.getNodeKey());
    }
    checkSideSlotNumber(slotNumber);
    final int deweyIdLength = deweyIdBytes == null
        ? 0
        : deweyIdBytes.length;
    final int deweyTrailerBytes = areDeweyIDsStored
        ? PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0;
    if ((long) deweyIdLength + deweyTrailerBytes > OverflowSlotSidecar.MAX_IMAGE_BYTES) {
      throw new IllegalArgumentException("DeweyID metadata exceeds the side-slot ceiling: " + deweyIdLength);
    }

    final PageReference replacementReference = new PageReference();
    replacementReference.setPage(new OverflowPage(recordBytes));

    final MemorySegment scratch = SIDE_SLOT_IMAGE_SCRATCH.get();
    final int kindId = ((NodeKind) record.getKind()).getId();
    long sideToken = 0L;
    int inlineDescriptorBytes = 0;

    if (record instanceof FlyweightNode flyweight && isFusedAnyObjectNamedKindId(kindId)) {
      final int fullImageBytes = tryBuildCompleteSideImage(flyweight, scratch, deweyIdBytes);
      if (fullImageBytes >= 0) {
        // A record that is intrinsically inline-sized but hit the page-frame capacity ceiling keeps
        // its complete canonical flyweight image. Projection/name scans therefore lose nothing.
        sideToken = prepareSideSlot(kindId, scratch, fullImageBytes);
      } else if (record instanceof ObjectNamedStringNode fusedStringNode) {
        // The string value itself is intrinsically large. Its fixed scan metadata remains sufficient;
        // value readers deliberately resolve the same-key OverflowPage.
        final int descriptorBytes = fusedStringNode.serializeOverflowDescriptorToHeap(scratch, 0L);
        final int descriptorImageBytes = appendSideDeweyTrailer(scratch, descriptorBytes, deweyIdBytes);
        final long descriptorOffset = prepareHeapForDirectWriteOrOverflow(descriptorBytes, deweyIdLength);
        if (descriptorOffset == DIRECT_WRITE_OVERFLOW) {
          sideToken = prepareSideSlot(kindId, scratch, descriptorImageBytes);
        } else {
          MemorySegment.copy(scratch, 0L, slottedPage, descriptorOffset, descriptorBytes);
          inlineDescriptorBytes = descriptorBytes;
        }
      } else if (record instanceof ObjectNamedNumberNode fusedNumberNode) {
        // Arbitrary-precision numeric payloads can themselves exceed 512 bytes. Keep their complete
        // scan metadata and the reserved unknown-value marker; scalar accessors decline that marker
        // and point reads resolve the authoritative same-key OverflowPage.
        final int descriptorBytes = fusedNumberNode.serializeOverflowDescriptorToHeap(scratch, 0L);
        final int descriptorImageBytes = appendSideDeweyTrailer(scratch, descriptorBytes, deweyIdBytes);
        final long descriptorOffset = prepareHeapForDirectWriteOrOverflow(descriptorBytes, deweyIdLength);
        if (descriptorOffset == DIRECT_WRITE_OVERFLOW) {
          sideToken = prepareSideSlot(kindId, scratch, descriptorImageBytes);
        } else {
          MemorySegment.copy(scratch, 0L, slottedPage, descriptorOffset, descriptorBytes);
          inlineDescriptorBytes = descriptorBytes;
        }
      } else {
        throw new IllegalArgumentException("Fused overflow record has no bounded metadata carrier: kind="
            + kindId + ", nodeKey=" + record.getNodeKey());
      }
    } else if (areDeweyIDsStored) {
      // Generic/non-fused records are read from their OverflowPage. Only their Dewey metadata needs
      // a local carrier; kind zero prevents factories from treating it as bindable flyweight bytes.
      final int imageBytes = appendSideDeweyTrailer(scratch, 0, deweyIdBytes);
      sideToken = prepareSideSlot(0, scratch, imageBytes);
    }

    // Everything that can allocate or fail has completed. Publish the same-key authority first,
    // then switch the scan-visible carrier with metadata-only operations.
    addedReferences = false;
    clearSerializedCache();
    references.put((recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber, replacementReference);
    clearSlotPreservation(slotNumber);
    if (sideToken != 0L) {
      publishSideSlot(slotNumber, sideToken);
      clearInlineSlotForOverflow(slotNumber);
    } else {
      removeSideSlot(slotNumber);
      clearInlineSlotForOverflow(slotNumber);
      if (inlineDescriptorBytes > 0) {
        completeDirectWrite(kindId, record.getNodeKey(), slotNumber, inlineDescriptorBytes, deweyIdBytes);
      }
    }
  }

  /** Return total complete-image bytes, or {@code -1} when the bounded scratch is insufficient. */
  private int tryBuildCompleteSideImage(final FlyweightNode flyweight, final MemorySegment scratch,
      final byte[] deweyIdBytes) {
    final int estimatedRecordBytes = flyweight.estimateSerializedSize();
    if (estimatedRecordBytes < 0) {
      throw new IllegalStateException("negative serialized-size estimate for " + flyweight.getKind() + ": "
          + estimatedRecordBytes);
    }
    final int deweyIdLength = deweyIdBytes == null
        ? 0
        : deweyIdBytes.length;
    final int deweyTrailerBytes = areDeweyIDsStored
        ? PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0;
    final long guaranteedImageBytes =
        (long) flyweight.estimateSerializedSizeLowerBound() + deweyIdLength + deweyTrailerBytes;
    if (guaranteedImageBytes > OverflowSlotSidecar.MAX_IMAGE_BYTES) {
      // The guaranteed floor already exceeds the image cap: refuse without touching the scratch.
      // Keyed on the floor, not the padded ceiling, for the same reason as serializeToHeap — a
      // ceiling-keyed refusal here silently demotes every record in the (floor <= cap < ceiling)
      // band. Records whose floor fits but whose actual bytes overflow the 512-byte scratch are
      // rare and settled by the exception below.
      return -1;
    }

    final int recordBytes;
    try {
      recordBytes = flyweight.serializeToHeap(scratch, 0L);
    } catch (final IndexOutOfBoundsException ignored) {
      // Cold safety net for a broken estimator contract. This must not be the normal size probe.
      return -1;
    }
    if (recordBytes <= 0 || recordBytes > OverflowSlotSidecar.MAX_IMAGE_BYTES) {
      return -1;
    }
    final long total = (long) recordBytes + (areDeweyIDsStored
        ? (deweyIdBytes == null ? 0 : deweyIdBytes.length) + PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0);
    if (total > OverflowSlotSidecar.MAX_IMAGE_BYTES) {
      return -1;
    }
    return appendSideDeweyTrailer(scratch, recordBytes, deweyIdBytes);
  }

  /** Append the page-format Dewey bytes/trailer to a bounded scratch record. */
  private int appendSideDeweyTrailer(final MemorySegment scratch, final int recordBytes,
      final byte[] deweyIdBytes) {
    if (!areDeweyIDsStored) {
      return recordBytes;
    }
    final int deweyIdLength = deweyIdBytes == null
        ? 0
        : deweyIdBytes.length;
    final int totalBytes = recordBytes + deweyIdLength + PageLayout.DEWEY_ID_TRAILER_SIZE;
    if (totalBytes > OverflowSlotSidecar.MAX_IMAGE_BYTES) {
      throw new IllegalArgumentException("Side-slot image exceeds " + OverflowSlotSidecar.MAX_IMAGE_BYTES
          + " bytes: " + totalBytes);
    }
    if (deweyIdLength > 0) {
      MemorySegment.copy(deweyIdBytes, 0, scratch, ValueLayout.JAVA_BYTE, recordBytes, deweyIdLength);
    }
    PageLayout.writeDeweyIdTrailer(scratch, totalBytes, deweyIdLength);
    return totalBytes;
  }

  /**
   * Copy an FSST-decoded string slot during a multi-fragment version merge.
   *
   * <p>The rewritten bytes are still in flyweight slotted-page format. If decompression expands the
   * record past the inline ceiling, those bytes must never be handed directly to an
   * {@link OverflowPage}: overflow readers consume the generic {@link RecordSerializer} format. In
   * that case this method materializes the source record against the source fragment's dictionary,
   * serializes a canonical raw snapshot, and installs the correct logical carrier.</p>
   */
  public void copyDecompressedStringSlotFrom(final KeyValueLeafPage source, final int slotNumber,
      final int nodeKindId, final byte[] rewritten) {
    if (source == null) {
      throw new NullPointerException("source must not be null");
    }
    if (rewritten == null) {
      throw new NullPointerException("rewritten must not be null");
    }
    if (slotNumber < 0 || slotNumber >= Constants.NDP_NODE_COUNT) {
      throw new IllegalArgumentException("slotNumber out of range: " + slotNumber);
    }
    if (nodeKindId != NodeKind.STRING_VALUE.getId()
        && nodeKindId != NodeKind.OBJECT_NAMED_STRING.getId() && nodeKindId != 0) {
      throw new IllegalArgumentException("Not a string slot kind: " + nodeKindId);
    }
    if (areDeweyIDsStored != source.areDeweyIDsStored) {
      throw new IllegalArgumentException("Source and target disagree about DeweyID storage");
    }

    if (recordPageKey != source.recordPageKey) {
      throw new IllegalArgumentException("Source and target page keys differ: " + source.recordPageKey + " != "
          + recordPageKey);
    }
    source.ensureChunkFor(slotNumber);
    validateDecompressedStringSlot(source, slotNumber, nodeKindId, rewritten);
    final MemorySegment sourceDeweyId = source.getDeweyId(slotNumber);
    final int deweyIdLength = sourceDeweyId == null
        ? 0
        : (int) sourceDeweyId.byteSize();
    final long inlineBytes = (long) rewritten.length + (areDeweyIDsStored
        ? deweyIdLength + PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0);
    if (inlineBytes <= PageConstants.MAX_RECORD_SIZE && ensureInlineAppendCapacity((int) inlineBytes)) {
      setSlotWithNodeKind(MemorySegment.ofArray(rewritten), slotNumber, nodeKindId);
      if (sourceDeweyId != null) {
        setDeweyId(sourceDeweyId, slotNumber);
      }
      return;
    }

    final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber;
    final byte[] deweyIdBytes = sourceDeweyId == null
        ? null
        : sourceDeweyId.toArray(ValueLayout.JAVA_BYTE);
    final DataRecord snapshot = source.snapshotStringSlot(nodeKey, slotNumber, nodeKindId, deweyIdBytes);
    canonicalizeOverflowString(snapshot);
    installCanonicalOverflowCarrier(snapshot, serializeOverflowRecord(snapshot), slotNumber, deweyIdBytes);
  }

  private void validateDecompressedStringSlot(final KeyValueLeafPage source, final int slotNumber,
      final int nodeKindId, final byte[] rewritten) {
    final MemorySegment sourcePage = source.slottedPage;
    if (sourcePage == null || !PageLayout.isSlotPopulated(sourcePage, slotNumber)) {
      throw new IllegalArgumentException("Source slot is not populated: " + slotNumber);
    }
    final int sourceNodeKindId = PageLayout.getDirNodeKindId(sourcePage, slotNumber);
    if (sourceNodeKindId != nodeKindId) {
      throw new IllegalArgumentException("Source slot kind changed: expected " + nodeKindId + " but found "
          + sourceNodeKindId);
    }
    if (source.isFusedObjectNamedStringOverflowDescriptor(slotNumber)) {
      throw new IllegalArgumentException("An overflow descriptor is not a complete string slot: " + slotNumber);
    }
    if (rewritten.length == 0) {
      throw new IllegalArgumentException("Rewritten string slot is empty");
    }

    final int recordKindId = rewritten[0] & 0xFF;
    final int expectedRecordKindId = nodeKindId == 0
        ? NodeKind.STRING_VALUE.getId()
        : nodeKindId;
    if (recordKindId != expectedRecordKindId) {
      throw new IllegalArgumentException("Rewritten slot kind changed: expected " + expectedRecordKindId
          + " but found " + recordKindId);
    }
    if (nodeKindId == 0) {
      final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber;
      final DataRecord legacyRecord = recordPersister.deserialize(
          new MemorySegmentBytesIn(MemorySegment.ofArray(rewritten)), nodeKey, source.getDeweyIdAsByteArray(slotNumber),
          resourceConfig);
      if (!(legacyRecord instanceof StringNode stringNode) || stringNode.isCompressed()) {
        throw new IllegalArgumentException("Legacy rewritten slot is not a raw string record: " + slotNumber);
      }
      return;
    }

    final int fieldCount = NodeFieldLayout.fieldCountForKind(nodeKindId);
    final int payloadField = nodeKindId == NodeKind.STRING_VALUE.getId()
        ? NodeFieldLayout.STRVAL_PAYLOAD
        : NodeFieldLayout.OBJNAMEDSTR_PAYLOAD;
    if (rewritten.length <= 1 + fieldCount) {
      throw new IllegalArgumentException("Truncated rewritten string slot: " + slotNumber);
    }
    final int payloadOffset = rewritten[1 + payloadField] & 0xFF;
    final int payloadStart = 1 + fieldCount + payloadOffset;
    if (payloadStart >= rewritten.length || rewritten[payloadStart] != ObjectNamedStringNode.PAYLOAD_FLAG_RAW) {
      throw new IllegalArgumentException("Rewritten string slot is not in canonical raw form: " + slotNumber);
    }
  }

  private DataRecord snapshotStringSlot(final long nodeKey, final int slotNumber, final int nodeKindId,
      final byte[] deweyIdBytes) {
    if (nodeKindId == 0) {
      final MemorySegment data = getSlot(slotNumber);
      if (data == null) {
        throw new IllegalStateException("Missing legacy string slot " + slotNumber + " on page " + recordPageKey);
      }
      final DataRecord record = recordPersister.deserialize(new MemorySegmentBytesIn(data), nodeKey, deweyIdBytes,
          resourceConfig);
      attachFsstSymbolTable(record);
      return record;
    }

    final FlyweightNode flyweight = FlyweightNodeFactory.createAndBind(slottedPage, slotNumber, nodeKey,
        resourceConfig.nodeHashFunction);
    try {
      if (deweyIdBytes != null && flyweight instanceof Node node) {
        node.setDeweyIDBytes(deweyIdBytes);
      }
      attachFsstSymbolTable((DataRecord) flyweight);
      return flyweight.toSnapshot();
    } finally {
      flyweight.clearBinding();
    }
  }

  private void attachFsstSymbolTable(final DataRecord record) {
    if (record instanceof StringNode stringNode) {
      stringNode.setFsstSymbolTable(fsstSymbolTable);
    } else if (record instanceof ObjectNamedStringNode fusedStringNode) {
      fusedStringNode.setFsstSymbolTable(fsstSymbolTable);
    }
  }

  /**
   * Whether a populated fused named-string slot is metadata for a same-key overflow record rather
   * than a complete inline value. This is a single flag-byte read after the slot's lazy chunk has
   * been expanded.
   */
  public boolean isFusedObjectNamedStringOverflowDescriptor(final int slotNumber) {
    final MemorySegment page = scanRecordSegment(slotNumber);
    if (page == null || getSlotNodeKindId(slotNumber) != FUSED_OBJECT_NAMED_STRING_KIND_ID) {
      return false;
    }
    final long recordBase = scanRecordBase(page, slotNumber);
    final int fieldOffset = page.get(ValueLayout.JAVA_BYTE,
        recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long payloadStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT + fieldOffset;
    if (page.get(ValueLayout.JAVA_BYTE, payloadStart) != ObjectNamedStringNode.PAYLOAD_FLAG_OVERFLOW) {
      return false;
    }
    final long recordKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber;
    if (references.get(recordKey) == null) {
      throw new IllegalStateException("Overflow descriptor for record " + recordKey
          + " has no same-page companion reference");
    }
    return true;
  }

  /** Whether a logical fused-number slot carries metadata for a same-key OverflowPage. */
  public boolean isFusedObjectNamedNumberOverflowDescriptor(final int slotNumber) {
    final MemorySegment page = scanRecordSegment(slotNumber);
    if (page == null || getSlotNodeKindId(slotNumber) != FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
      return false;
    }
    final long recordBase = scanRecordBase(page, slotNumber);
    final int fieldOffset = page.get(ValueLayout.JAVA_BYTE,
        recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
    final long payloadStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT + fieldOffset;
    if (page.get(ValueLayout.JAVA_BYTE, payloadStart) != ObjectNamedNumberNode.PAYLOAD_TYPE_OVERFLOW) {
      return false;
    }
    final long recordKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber;
    if (references.get(recordKey) == null) {
      throw new IllegalStateException("Overflow descriptor for record " + recordKey
          + " has no same-page companion reference");
    }
    return true;
  }

  /** Metadata descriptors are not complete inline values and must never be flyweight-bound. */
  public boolean isFusedOverflowDescriptor(final int slotNumber) {
    final int kindId = getSlotNodeKindId(slotNumber);
    return kindId == FUSED_OBJECT_NAMED_STRING_KIND_ID && isFusedObjectNamedStringOverflowDescriptor(slotNumber)
        || kindId == FUSED_OBJECT_NAMED_NUMBER_KIND_ID && isFusedObjectNamedNumberOverflowDescriptor(slotNumber);
  }

  private static boolean isFusedObjectNamedStringOverflowDescriptor(final MemorySegment record,
      final int nodeKindId) {
    return isFusedObjectNamedStringOverflowDescriptor(record, 0L, record.byteSize(), nodeKindId);
  }

  private static boolean isFusedObjectNamedStringOverflowDescriptor(final MemorySegment record,
      final long recordBase, final long recordLength, final int nodeKindId) {
    if (nodeKindId != FUSED_OBJECT_NAMED_STRING_KIND_ID
        || recordLength < 1L + NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT) {
      return false;
    }
    final int fieldOffset = record.get(ValueLayout.JAVA_BYTE,
        recordBase + 1L + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long relativePayloadStart = 1L + NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT + fieldOffset;
    final long payloadStart = recordBase + relativePayloadStart;
    return relativePayloadStart < recordLength
        && record.get(ValueLayout.JAVA_BYTE, payloadStart) == ObjectNamedStringNode.PAYLOAD_FLAG_OVERFLOW;
  }

  private static boolean isFusedObjectNamedNumberOverflowDescriptor(final MemorySegment record,
      final int nodeKindId) {
    return isFusedObjectNamedNumberOverflowDescriptor(record, 0L, record.byteSize(), nodeKindId);
  }

  private static boolean isFusedObjectNamedNumberOverflowDescriptor(final MemorySegment record,
      final long recordBase, final long recordLength, final int nodeKindId) {
    if (nodeKindId != FUSED_OBJECT_NAMED_NUMBER_KIND_ID
        || recordLength < 1L + NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT) {
      return false;
    }
    final int fieldOffset = record.get(ValueLayout.JAVA_BYTE,
        recordBase + 1L + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
    final long relativePayloadStart = 1L + NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT + fieldOffset;
    final long payloadStart = recordBase + relativePayloadStart;
    return relativePayloadStart < recordLength
        && record.get(ValueLayout.JAVA_BYTE, payloadStart) == ObjectNamedNumberNode.PAYLOAD_TYPE_OVERFLOW;
  }

  private static boolean isFusedOverflowDescriptor(final MemorySegment record, final int nodeKindId) {
    return isFusedObjectNamedStringOverflowDescriptor(record, nodeKindId)
        || isFusedObjectNamedNumberOverflowDescriptor(record, nodeKindId);
  }

  private static boolean isFusedOverflowDescriptor(final MemorySegment record, final long recordBase,
      final long recordLength, final int nodeKindId) {
    return isFusedObjectNamedStringOverflowDescriptor(record, recordBase, recordLength, nodeKindId)
        || isFusedObjectNamedNumberOverflowDescriptor(record, recordBase, recordLength, nodeKindId);
  }

  /** Whether this logical slot is carried by the page's cold overflow sidecar. */
  public boolean hasSideSlot(final int slotNumber) {
    checkSideSlotNumber(slotNumber);
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    return sidecar != null && sidecar.has(slotNumber);
  }

  /** Number of logical slot images in the cold overflow sidecar. */
  public int getSideSlotCount() {
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    return sidecar == null
        ? 0
        : sidecar.count();
  }

  /** Copy the 1024-bit side-slot presence bitmap into caller-owned storage. */
  public void copySideSlotBitmapTo(final long[] destination) {
    if (destination == null) {
      throw new NullPointerException("destination");
    }
    if (destination.length < BITMAP_WORDS) {
      throw new IllegalArgumentException("Side-slot bitmap needs " + BITMAP_WORDS + " words");
    }
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar == null) {
      Arrays.fill(destination, 0, BITMAP_WORDS, 0L);
    } else {
      sidecar.copyBitmapTo(destination);
    }
  }

  /** Return a side slot's physical kind, or zero when no side image exists. */
  public int getSideSlotNodeKindId(final int slotNumber) {
    checkSideSlotNumber(slotNumber);
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    return sidecar == null
        ? 0
        : sidecar.kind(slotNumber);
  }

  /**
   * Return a bounded read-only view of a side slot's complete image, including any Dewey trailer.
   * Callers must not retain the view across a mutation of this page.
   */
  public MemorySegment getSideSlotImage(final int slotNumber) {
    checkSideSlotNumber(slotNumber);
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    final MemorySegment image = sidecar == null
        ? null
        : sidecar.image(slotNumber);
    return image == null
        ? null
        : image.asReadOnly();
  }

  /**
   * Copy a prospective side-slot image into unpublished native storage. The returned opaque token
   * is consumed by {@link #publishSideSlot(int, long)}; this method does not alter the logical slot.
   */
  public long prepareSideSlot(final int nodeKindId, final MemorySegment image, final int length) {
    OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar == null) {
      final OverflowSlotSidecar created = new OverflowSlotSidecar(segmentAllocator);
      try {
        final long token = created.prepare(nodeKindId, image, length);
        overflowSlotSidecar = created;
        return token;
      } catch (final RuntimeException | Error failure) {
        try {
          created.close();
        } catch (final Throwable cleanupFailure) {
          addSuppressedBestEffort(failure, cleanupFailure);
        }
        throw failure;
      }
    }
    return sidecar.prepare(nodeKindId, image, length);
  }

  /** Publish a side-slot prepare token without allocating or copying. */
  public void publishSideSlot(final int slotNumber, final long prepareToken) {
    checkSideSlotNumber(slotNumber);
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar == null) {
      throw new IllegalStateException("No side-slot storage has been prepared");
    }
    final boolean replacingSideSlot = sidecar.has(slotNumber);
    sidecar.publish(slotNumber, prepareToken);
    clearSlotPreservation(slotNumber);
    if (!replacingSideSlot) {
      lastSlotIndex = slotNumber;
    }
    dropColumnRegionsForSideSlots();
    objectKeySlotsByName = null;
    addedReferences = false;
    clearSerializedCache();
  }

  /** Remove a side image, releasing all side storage immediately when the last one disappears. */
  public void removeSideSlot(final int slotNumber) {
    checkSideSlotNumber(slotNumber);
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar == null) {
      return;
    }
    final int oldKind = sidecar.kind(slotNumber);
    if (!sidecar.remove(slotNumber)) {
      return;
    }
    if (oldKind != 0) {
      invalidateRegionsForKindId(oldKind);
    }
    objectKeySlotsByName = null;
    addedReferences = false;
    clearSerializedCache();
    if (sidecar.isEmpty()) {
      try {
        sidecar.close();
      } finally {
        overflowSlotSidecar = null;
      }
    }
  }

  /** Copy one side image from another page without exposing its native storage to a flyweight. */
  public void copySideSlotFrom(final KeyValueLeafPage sourcePage, final int slotNumber) {
    if (sourcePage == null) {
      throw new NullPointerException("sourcePage");
    }
    checkSideSlotNumber(slotNumber);
    if (sourcePage == this) {
      return;
    }
    final OverflowSlotSidecar sourceSidecar = sourcePage.overflowSlotSidecar;
    if (sourceSidecar == null || !sourceSidecar.has(slotNumber)) {
      removeSideSlot(slotNumber);
      return;
    }
    final MemorySegment image = sourceSidecar.image(slotNumber);
    final int length = sourceSidecar.imageLength(slotNumber);
    final long token = prepareSideSlot(sourceSidecar.kind(slotNumber), image, length);
    publishSideSlot(slotNumber, token);
    clearInlineSlotForOverflow(slotNumber);
  }

  /** Release every side-slot allocation owned by this page. */
  public void clearSideSlots() {
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar == null) {
      return;
    }
    try {
      sidecar.close();
    } finally {
      overflowSlotSidecar = null;
      objectKeySlotsByName = null;
      addedReferences = false;
      clearSerializedCache();
    }
  }

  private static void checkSideSlotNumber(final int slotNumber) {
    if (slotNumber < 0 || slotNumber >= Constants.NDP_NODE_COUNT) {
      throw new IndexOutOfBoundsException("slotNumber=" + slotNumber);
    }
  }

  /** Physical directory kind, intentionally ignoring the logical overflow sidecar. */
  public int getInlineSlotNodeKindId(final int slotNumber) {
    if (slottedPage == null || !PageLayout.isSlotPopulated(slottedPage, slotNumber)) {
      return 0;
    }
    return PageLayout.getDirNodeKindId(slottedPage, slotNumber);
  }

  /** Resolve scan bytes without ever binding a flyweight to sidecar-owned memory. */
  private MemorySegment scanRecordSegment(final int slotNumber) {
    final MemorySegment page = slottedPage;
    if (page != null && PageLayout.isSlotPopulated(page, slotNumber)) {
      return page;
    }
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    return sidecar == null
        ? null
        : sidecar.segment(slotNumber);
  }

  /** Absolute record start within a segment returned by {@link #scanRecordSegment(int)}. */
  private long scanRecordBase(final MemorySegment segment, final int slotNumber) {
    if (segment == slottedPage) {
      return PageLayout.HEAP_START + heapOffsetOf(segment, slotNumber);
    }
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar == null) {
      return -1L;
    }
    return sidecar.offset(slotNumber);
  }

  /**
   * Get the logical nodeKindId for a slot. Sidecar images are consulted only when no inline image is
   * populated, preserving the ordinary directory-only fast path.
   *
   * @param slotNumber the slot index (0-1023)
   * @return the nodeKindId (&gt;0 for flyweight format, 0 for legacy)
   */
  public int getSlotNodeKindId(final int slotNumber) {
    final MemorySegment page = slottedPage;
    if (page != null && PageLayout.isSlotPopulated(page, slotNumber)) {
      return PageLayout.getDirNodeKindId(page, slotNumber);
    }
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    return sidecar == null
        ? 0
        : sidecar.kind(slotNumber);
  }

  /**
   * Read a slot's parent node key straight from the slot bytes, without binding a flyweight or moving
   * a transaction cursor.
   *
   * <p>
   * This is what lets a scan decide whether a record belongs to a given parent — the children of one
   * JSON array, say — while inspecting every slot of a page. Doing it by materializing each node
   * would cost far more than the scan it is filtering for: on a corpus with ~15 nodes per array
   * element, the filter runs 15x more often than it succeeds.
   *
   * <p>
   * Works for every node kind because PARENT_KEY is field index 0 in all of them (see the
   * {@code *_PARENT_KEY} constants in {@link NodeFieldLayout}); only the offset table's length
   * varies, and that comes from {@link NodeFieldLayout#fieldCountForKind(int)}.
   *
   * @param slotNumber the slot index (0-1023)
   * @return the parent node key, or {@link Fixed#NULL_NODE_KEY} when the slot is unpopulated or the
   *         page holds no slotted image
   */
  public long getSlotParentKey(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
    if (fieldCount <= 0 || isParentless(kindId)) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJECT_PARENT_KEY) & 0xFF;
    final long dataStart = recordBase + 1 + fieldCount;
    // Parent keys are delta-encoded against the record's own node key.
    final long nodeKey = (getPageKey() << PageLayout.SLOT_COUNT_EXPONENT) | slotNumber;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, nodeKey);
  }

  /**
   * Whether a node kind has no PARENT_KEY at field index 0.
   *
   * <p>
   * Only the document roots, and getting this wrong is quiet rather than loud: their field 0 is
   * FIRST_CHILD_KEY, so reading it as a parent reports the root's own first child as its parent. On a
   * JSON document that child is the top-level array, which makes the root look like a member of that
   * array — a scan filtering "children of the array" then admits the document root and hands a
   * consumer a node kind it has no case for.
   */
  private static boolean isParentless(final int kindId) {
    return kindId == NodeKind.JSON_DOCUMENT.getId() || kindId == NodeKind.XML_DOCUMENT.getId();
  }

  /**
   * Read a fused-named slot's nameKey directly from the slot bytes without binding a flyweight
   * singleton or moving a transaction cursor. Callers MUST verify the slot is populated and holds a
   * fused {@code OBJECT_NAMED_*} record (kindId 48-53) — the method does no validation for the
   * vectorized scan hot path.
   *
   * @param slotNumber the slot index (assumed populated + fused-named kind)
   * @return the signed nameKey from the slot
   */
  public int getObjectKeyNameKeyFromSlot(final int slotNumber) {
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    final MemorySegment nameKeyPayload = sidecar == null || !sidecar.has(slotNumber)
        ? regionPayload(RegionTable.KIND_OBJECT_KEY_NAMEKEY)
        : null;
    if (nameKeyPayload != null) {
      return ObjectKeyNameKeyRegion.nameKeyForSlot(nameKeyPayload, slotNumber);
    }
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return -1;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    if (isFusedStructuralKindId(kindId)) {
      return getFusedStructuralNameKeyFromSlot(slotNumber);
    }
    if (isFusedObjectNamedKindId(kindId)) {
      return getFusedObjectNamedNameKeyFromSlot(slotNumber);
    }
    return -1;
  }

  /**
   * Read the inline nameKey from a fused {@code OBJECT_NAMED_*} slot (kindIds 48-51). Same varint
   * layout as {@link #getObjectKeyNameKeyFromSlot} but different field count (offset-table size
   * varies between 8 and 9 across the four kinds). The accessor picks up the correct field-count for
   * the slot's kind from {@link NodeFieldLayout}.
   */
  public int getFusedObjectNamedNameKeyFromSlot(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return -1;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.FUSED_PRIMITIVE_NAME_KEY_FIELD) & 0xFF;
    final long dataStart = recordBase + 1 + fieldCount;
    return DeltaVarIntCodec.decodeSignedFromSegment(sp, dataStart + fieldOff);
  }

  /**
   * Read the inline nameKey from a fused structural {@code OBJECT_NAMED_OBJECT/ARRAY} slot (kindIds
   * 52/53). NAME_KEY is at field index 5 for these (vs index 3 for primitive-fused 48-51). Caller
   * must verify the slot holds a structural-fused record.
   */
  public int getFusedStructuralNameKeyFromSlot(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return -1;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.FUSED_STRUCTURAL_NAME_KEY_FIELD) & 0xFF;
    final long dataStart = recordBase + 1 + fieldCount;
    return DeltaVarIntCodec.decodeSignedFromSegment(sp, dataStart + fieldOff);
  }

  /**
   * True for the kinds (52, 53). Layout-dependent — these have a 12-field offset table with NAME_KEY
   * at index 5; do NOT use them on hot paths that assume the primitive-fused layout (kindIds 48-51).
   */
  public static boolean isFusedStructuralKindId(final int kindId) {
    return kindId == FUSED_OBJECT_NAMED_OBJECT_KIND_ID || kindId == FUSED_OBJECT_NAMED_ARRAY_KIND_ID;
  }

  /**
   * Read the boolean payload of an OBJECT_NAMED_BOOLEAN slot (kindId 48) directly off the slotted
   * page. Caller must verify the slot holds an OBJECT_NAMED_BOOLEAN.
   */
  public boolean getFusedObjectNamedBooleanValueFromSlot(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return false;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_VALUE) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_BOOLEAN_FIELD_COUNT;
    return sp.get(ValueLayout.JAVA_BYTE, dataStart + fieldOff) != 0;
  }

  /**
   * Decode the inline numeric value from an OBJECT_NAMED_NUMBER slot (kindId 49) directly off the
   * slotted page. Returns {@link Long#MIN_VALUE} if the payload's number type is not Integer or Long
   * (e.g. float/double/BigDecimal) — caller falls back to the cursor path. Mirrors
   * {@link #getNumberValueLongFromSlot} for the fused shape.
   *
   * <p>
   * Caller must verify the slot holds {@code OBJECT_NAMED_NUMBER}.
   */
  public long getFusedObjectNamedNumberValueLongFromSlot(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return Long.MIN_VALUE;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    // Payload is field index 8 (OBJNAMEDNUM_PAYLOAD).
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT;
    final long payloadStart = dataStart + fieldOff;
    final byte numberType = sp.get(ValueLayout.JAVA_BYTE, payloadStart);
    if (numberType == NUMBER_TYPE_INTEGER) {
      return DeltaVarIntCodec.decodeSignedFromSegment(sp, payloadStart + 1);
    }
    if (numberType == NUMBER_TYPE_LONG) {
      return DeltaVarIntCodec.decodeSignedLongFromSegment(sp, payloadStart + 1);
    }
    return Long.MIN_VALUE; // Sentinel: caller falls back to full path
  }

  /**
   * Decode the inline value from an OBJECT_NAMED_NUMBER slot as a double, or {@link Double#NaN} when
   * the payload is not Double/Float-typed. NaN is a safe sentinel: JSON has no NaN literal, so no
   * stored value can collide with it. The companion of
   * {@link #getFusedObjectNamedNumberValueLongFromSlot}, for the values that method declines —
   * together they cover every numeric type the double-region writer can column-ize.
   */
  /** Written to {@code scaleOut[0]} when a slot holds no decimal this column can carry exactly. */
  public static final int DECIMAL_SCALE_UNAVAILABLE = Integer.MIN_VALUE;

  /**
   * Decode an OBJECT_NAMED_NUMBER slot's BigDecimal payload as its EXACT {@code (unscaled, scale)},
   * allocating nothing.
   *
   * <p>
   * Returns the unscaled value and writes the scale to {@code scaleOut[0]}, or
   * {@link #DECIMAL_SCALE_UNAVAILABLE} when the slot cannot be carried exactly — a non-decimal type,
   * an unscaled magnitude wider than a {@code long}, or a scale outside
   * {@code [0, }{@link DoubleRegion#MAX_DECIMAL_SCALE}{@code ]}. A negative scale ({@code 1E+3}) is
   * declined rather than normalized: rescaling it would multiply the unscaled value and can overflow,
   * and such literals are vanishingly rare in JSON.
   *
   * <h2>Why this exists next to {@link #getFusedObjectNamedNumberValueDoubleFromSlot}</h2> That
   * method answers the same payload as a {@code double}, and must return {@link Double#NaN} whenever
   * the double image is inexact — which is almost every real decimal, because only dyadic rationals
   * survive the conversion. Going straight to the unscaled integer skips the lossy hop entirely, so a
   * price like {@code 19.99} is carried exactly instead of being turned away.
   *
   * <h2>HFT</h2> The double-typed path built a {@code byte[]}, a {@code BigInteger} and a
   * {@code BigDecimal} per value per page build, purely to answer "is this exact?". This reads the
   * two's-complement bytes straight into a {@code long} — no allocation, no GC pressure on page
   * reconstruction.
   */
  public long getFusedObjectNamedNumberValueDecimalFromSlot(final int slotNumber, final int[] scaleOut) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      scaleOut[0] = DECIMAL_SCALE_UNAVAILABLE;
      return 0L;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
    final long payloadStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT + fieldOff;
    if (sp.get(ValueLayout.JAVA_BYTE, payloadStart) != NUMBER_TYPE_BIG_DECIMAL) {
      scaleOut[0] = DECIMAL_SCALE_UNAVAILABLE;
      return 0L;
    }
    long pos = payloadStart + 1;
    long len = 0;
    int shift = 0;
    byte b;
    do {
      b = sp.get(ValueLayout.JAVA_BYTE, pos++);
      len |= (long) (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    if (len <= 0 || len > Long.BYTES) {
      scaleOut[0] = DECIMAL_SCALE_UNAVAILABLE; // wider than a long — the record path keeps it
      return 0L;
    }
    // Big-endian two's complement, exactly as BigInteger(byte[]) reads it, then sign-extended from
    // len*8 bits by a pair of shifts. At len == 8 the shift count is zero and this is a no-op.
    long unscaled = 0L;
    for (int i = 0; i < (int) len; i++) {
      unscaled = (unscaled << 8) | (sp.get(ValueLayout.JAVA_BYTE, pos + i) & 0xFFL);
    }
    final int signShift = 64 - ((int) len << 3);
    unscaled = unscaled << signShift >> signShift;
    final int scale = DeltaVarIntCodec.decodeSignedFromSegment(sp, pos + len);
    scaleOut[0] = scale >= 0 && scale <= DoubleRegion.MAX_DECIMAL_SCALE
        ? scale
        : DECIMAL_SCALE_UNAVAILABLE;
    return unscaled;
  }

  /**
   * Whether an OBJECT_NAMED_NUMBER slot's payload is BigDecimal-typed.
   *
   * <p>
   * The column a value joins is decided by its DECLARED type, exactly as {@code DECIMAL(P,S)} and
   * {@code DOUBLE} are separate physical columns in a relational engine — never by whether that one
   * value's double image happens to round-trip. Two decimals can share a double image, so a tag
   * mixing an exact-as-double decimal with an inexact one would otherwise be encoded over double
   * images, and a decimal predicate over it would put rows on the wrong side of the threshold.
   *
   * <p>
   * Reads one byte off the slot's payload header; allocates nothing.
   *
   * <p>
   * Caller must verify the slot holds {@code OBJECT_NAMED_NUMBER}.
   */
  public boolean isFusedObjectNamedNumberDecimalSlot(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return false;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
    final long payloadStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT + fieldOff;
    return sp.get(ValueLayout.JAVA_BYTE, payloadStart) == NUMBER_TYPE_BIG_DECIMAL;
  }

  public double getFusedObjectNamedNumberValueDoubleFromSlot(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return Double.NaN;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
    final long payloadStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT + fieldOff;
    final byte numberType = sp.get(ValueLayout.JAVA_BYTE, payloadStart);
    if (numberType == NUMBER_TYPE_DOUBLE) {
      return Double.longBitsToDouble(sp.get(LE.LONG, payloadStart + 1));
    }
    if (numberType == NUMBER_TYPE_FLOAT) {
      return Float.intBitsToFloat(sp.get(LE.INT, payloadStart + 1));
    }
    if (numberType == NUMBER_TYPE_BIG_DECIMAL) {
      // jn:store parses every fractional JSON number into a BigDecimal, so WITHOUT this arm the
      // double column would only ever exist for shredder-ingested resources. A decimal joins the
      // column only when its double image is EXACT — `new BigDecimal(d)` is the exact-value
      // constructor, so compareTo == 0 proves the conversion lost nothing and every comparison
      // against it in double space equals the comparison in decimal space. Anything inexact stays
      // out, fails the completeness sum, and keeps its record path.
      long pos = payloadStart + 1;
      long len = 0;
      int shift = 0;
      byte b;
      do {
        b = sp.get(ValueLayout.JAVA_BYTE, pos++);
        len |= (long) (b & 0x7F) << shift;
        shift += 7;
      } while ((b & 0x80) != 0);
      if (len <= 0 || len > 12) {
        return Double.NaN; // an unscaled value this wide has no exact double image anyway
      }
      final byte[] unscaled = new byte[(int) len];
      MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, pos, unscaled, 0, (int) len);
      final int scale = DeltaVarIntCodec.decodeSignedFromSegment(sp, pos + len);
      final BigDecimal bd = new BigDecimal(new BigInteger(unscaled), scale);
      final double d = bd.doubleValue();
      return Double.isFinite(d) && bd.compareTo(new BigDecimal(d)) == 0
          ? d
          : Double.NaN;
    }
    return Double.NaN;
  }

  /**
   * Read the inline string bytes from an OBJECT_NAMED_STRING slot (kindId 50). Goes through the
   * thread-local {@code STRING_REGION_BUILD_SCRATCH} and returns a trimmed copy. Caller must verify
   * the slot holds {@code OBJECT_NAMED_STRING}.
   */
  /**
   * Whether the fused OBJECT_NAMED_STRING slot's stored payload bytes are FSST-encoded.
   *
   * <p>
   * One flag-byte read; pairs with {@link #readFusedObjectNamedStringStoredBytes} for callers that
   * must see the stored form rather than the value — the region builder above all, whose dictionaries
   * mirror the heap verbatim so value elision stays a pure byte copy.
   */
  public boolean isFusedObjectNamedStringValueCompressed(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null)
      return false;
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long payloadStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT + fieldOff;
    return sp.get(ValueLayout.JAVA_BYTE, payloadStart) == ObjectNamedStringNode.PAYLOAD_FLAG_FSST;
  }

  /**
   * The fused OBJECT_NAMED_STRING slot's stored payload bytes, verbatim — FSST-encoded when the slot
   * was compressed, raw otherwise — with no decode attempted.
   *
   * <p>
   * {@link #readFusedObjectNamedStringBytes} answers "what is the value"; this answers "what is
   * stored". The region builder must use this one: its dictionary entries have to be bit-identical to
   * the heap so that eliding the heap copy and re-injecting it from the region is a straight copy in
   * both directions, decodable later through the page's symbol table by whoever actually materialises
   * the value.
   *
   * <p>
   * A zero-length payload answers the shared empty array, not {@code null}: the empty string is a
   * value like any other, and the caller drops from the string column every slot this method
   * declines. Dropping the empty ones left the column holding fewer values than the field has
   * occurrences on the page — which is exactly what the column consumers' completeness oracle refuses
   * to serve, so those pages fell back to the records, and what the dictionary sketch (consulted
   * BEFORE that oracle) turned into a confident {@code count(f eq "") = 0}. Only an unpopulated slot
   * or a negative length — neither of which is a value — answers {@code null}.
   *
   * @return the stored bytes, or {@code null} when the slot holds no payload at all
   */
  public byte[] readFusedObjectNamedStringStoredBytes(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null)
      return null;
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long payloadStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT + fieldOff;
    if (sp.get(ValueLayout.JAVA_BYTE, payloadStart) == ObjectNamedStringNode.PAYLOAD_FLAG_OVERFLOW) {
      return null;
    }
    final long lenOff = payloadStart + 1;
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(sp, lenOff);
    if (length < 0)
      return null;
    if (length == 0)
      return EMPTY_STRING_VALUE_BYTES;
    final int lenBytes = DeltaVarIntCodec.readSignedVarintWidth(sp, lenOff);
    final byte[] out = new byte[length];
    MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, lenOff + lenBytes, out, 0, length);
    return out;
  }

  /**
   * Copy a fused {@code OBJECT_NAMED_STRING} slot's stored payload into caller-owned scratch.
   *
   * <p>
   * The stored representation is copied verbatim: raw UTF-8 stays raw and FSST bytes stay encoded. A
   * non-negative return value is always the exact payload length. If it exceeds
   * {@code destination.length}, the destination is left untouched so a grow-only caller can resize
   * and retry without preserving a partial value. Zero is the valid empty-string length; {@code -1}
   * means that the slot has no payload. No view of the page's native memory escapes this method.
   *
   * <p>
   * The caller must already have established that {@code slotNumber} contains a fused
   * {@code OBJECT_NAMED_STRING}; this is the allocation-free companion to
   * {@link #readFusedObjectNamedStringStoredBytes(int)} for the page-seal path.
   *
   * @param slotNumber slot to read
   * @param destination caller-owned destination starting at offset zero
   * @return exact required/copied length, or {@code -1} when the slot has no payload
   */
  int copyFusedObjectNamedStringStoredBytes(final int slotNumber, final byte[] destination) {
    if (destination == null) {
      throw new NullPointerException("destination");
    }
    if (slotNumber < 0 || slotNumber >= PageLayout.SLOT_COUNT) {
      throw new IndexOutOfBoundsException("slotNumber=" + slotNumber);
    }
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return -1;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long payloadStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT + fieldOff;
    if (sp.get(ValueLayout.JAVA_BYTE, payloadStart) == ObjectNamedStringNode.PAYLOAD_FLAG_OVERFLOW) {
      return -1;
    }
    final long lenOff = payloadStart + 1;
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(sp, lenOff);
    if (length < 0) {
      return -1;
    }
    if (length > destination.length) {
      return length;
    }
    if (length > 0) {
      final int lenBytes = DeltaVarIntCodec.readSignedVarintWidth(sp, lenOff);
      MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, lenOff + lenBytes, destination, 0, length);
    }
    return length;
  }

  /**
   * Shared result for a zero-length string payload: an empty string is a legitimate value and must
   * stay distinguishable from a corrupt or absent slot, which answers {@code null}.
   */
  private static final byte[] EMPTY_STRING_VALUE_BYTES = new byte[0];

  /**
   * The UTF-8 bytes of a standalone {@code STRING_VALUE} slot — an ARRAY ELEMENT, the one string
   * shape the PAX string column never held.
   *
   * <p>
   * Same payload layout as the fused object-named string ({@code [isCompressed][len][bytes]}), a
   * different field table: {@link NodeFieldLayout#STRING_VALUE_FIELD_COUNT} fields with the payload
   * at {@link NodeFieldLayout#STRVAL_PAYLOAD}.
   *
   * @return the value — the shared empty array for a zero-length value — or {@code null} when the
   *         slot is unpopulated, carries a negative length, or is FSST-compressed with no symbol
   *         table resolved on this instance
   */
  public byte[] readStringValueBytes(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null || getSlotNodeKindId(slotNumber) != STRING_VALUE_KIND_ID) {
      return null;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.STRVAL_PAYLOAD) & 0xFF;
    final long payloadStart = recordBase + 1 + NodeFieldLayout.STRING_VALUE_FIELD_COUNT + fieldOff;
    final boolean compressed = sp.get(ValueLayout.JAVA_BYTE, payloadStart) == 1;
    final long lenOff = payloadStart + 1;
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(sp, lenOff);
    if (length < 0) {
      return null;
    }
    if (length == 0) {
      return EMPTY_STRING_VALUE_BYTES;
    }
    final int lenBytes = DeltaVarIntCodec.readSignedVarintWidth(sp, lenOff);
    final long dataOff = lenOff + lenBytes;
    final byte[] stored = new byte[length];
    MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, dataOff, stored, 0, length);
    if (!compressed) {
      return stored;
    }
    final byte[][] symbols = fsstSymbols();
    if (symbols.length == 0) {
      return null;
    }
    return FSSTCompressor.decode(stored, symbols);
  }

  public byte[] readFusedObjectNamedStringBytes(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null)
      return null;
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT;
    final long payloadStart = dataStart + fieldOff;
    // Payload layout: [isCompressed:1][length:varint][bytes].
    final byte payloadFlag = sp.get(ValueLayout.JAVA_BYTE, payloadStart);
    if (payloadFlag == ObjectNamedStringNode.PAYLOAD_FLAG_OVERFLOW) {
      return null;
    }
    final boolean compressed = payloadFlag == ObjectNamedStringNode.PAYLOAD_FLAG_FSST;
    final long lenOff = payloadStart + 1;
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(sp, lenOff);
    if (length <= 0)
      return null;
    final int lenBytes = DeltaVarIntCodec.readSignedVarintWidth(sp, lenOff);
    final long dataOff = lenOff + lenBytes;
    if (!compressed) {
      final byte[] out = new byte[length];
      MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, dataOff, out, 0, length);
      return out;
    }
    final byte[][] symbols = fsstSymbols();
    if (symbols.length == 0)
      return null;
    // Share the legacy-path thread-local scratch so fused region-builds don't churn
    // the young generation with per-slot byte[] allocations (see agent review E2/R2).
    byte[] scratch = STRING_REGION_BUILD_SCRATCH.get();
    final int needed = Math.max(length << 3, 64);
    if (scratch.length < needed) {
      scratch = new byte[needed];
      STRING_REGION_BUILD_SCRATCH.set(scratch);
    }
    final int decoded = decodeFsstInto(sp, dataOff, length, symbols, scratch);
    if (decoded < 0)
      return null;
    final byte[] out = new byte[decoded];
    System.arraycopy(scratch, 0, out, 0, decoded);
    return out;
  }

  // ============== Phase 1 fused-structural getters (OBJECT_NAMED_OBJECT / OBJECT_NAMED_ARRAY)
  // ==============
  // Phase 1 reserves the field accessors but no production path emits these kinds yet — the
  // getters are dormant. Each getter trusts the caller has already validated the slot's
  // kindId is 52 or 53. When P2 enables emission, callers can use these to read structural
  // fields from a slotted page without binding the flyweight node.

  /**
   * Read {@code firstChildKey} from a fused {@code OBJECT_NAMED_OBJECT} or {@code OBJECT_NAMED_ARRAY}
   * slot (kindIds 52/53). Both kinds share the field layout defined by
   * {@link NodeFieldLayout#OBJNAMEDOBJ_FIRST_CHILD_KEY}.
   *
   * @param slotNumber the populated slot index
   * @return the firstChildKey for the record at {@code slotNumber}
   */
  public long getFusedObjectNamedStructuralFirstChildKeyFromSlot(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return -1L;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final long nodeKey = nodeKeyForSlot(slotNumber);
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_FIRST_CHILD_KEY) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, nodeKey);
  }

  /**
   * Read {@code lastChildKey} from a fused {@code OBJECT_NAMED_OBJECT} or {@code OBJECT_NAMED_ARRAY}
   * slot (kindIds 52/53).
   */
  public long getFusedObjectNamedStructuralLastChildKeyFromSlot(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return -1L;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final long nodeKey = nodeKeyForSlot(slotNumber);
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_LAST_CHILD_KEY) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, nodeKey);
  }

  /**
   * Read {@code childCount} from a fused {@code OBJECT_NAMED_OBJECT} or {@code OBJECT_NAMED_ARRAY}
   * slot (kindIds 52/53).
   */
  public long getFusedObjectNamedStructuralChildCountFromSlot(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return -1L;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_CHILD_COUNT) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
    return DeltaVarIntCodec.decodeSignedLongFromSegment(sp, dataStart + fieldOff);
  }

  /**
   * Read {@code descendantCount} from a fused {@code OBJECT_NAMED_OBJECT} or
   * {@code OBJECT_NAMED_ARRAY} slot (kindIds 52/53).
   */
  public long getFusedObjectNamedStructuralDescendantCountFromSlot(final int slotNumber) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return -1L;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_DESCENDANT_COUNT) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
    return DeltaVarIntCodec.decodeSignedLongFromSegment(sp, dataStart + fieldOff);
  }

  /**
   * Compute the per-slot record nodeKey: pageKeyBase derived from {@link #recordPageKey} shifted by
   * the page-record exponent. Used by structural-fused getters that need the delta base to decode
   * delta-varint fields.
   */
  private long nodeKeyForSlot(final int slotNumber) {
    return (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber;
  }

  /**
   * Return the distinct OBJECT_KEY {@code nameKey}s present on this page. Fast path: reads directly
   * from the PAX dictKeys header (one VarHandle load per distinct nameKey — typically 3 to 10
   * entries). Slow path (region absent): iterates populated slots via the bitmap and collects
   * distinct nameKeys into a growable array.
   *
   * <p>
   * Used by the page-skip index builder to decide which pages are candidates for a given anchor
   * nameKey, so analytical scans can skip pages that hold no slot with that field instead of fetching
   * each page only to bail out on empty {@code getObjectKeySlotsForNameKey}.
   */
  public int[] getDistinctObjectKeyNameKeys() {
    final MemorySegment payload = regionPayload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
    if (payload != null) {
      return ObjectKeyNameKeyRegion.uniqueNameKeys(payload);
    }
    // Slow path: no region — walk populated slots via the page layout
    // helpers, decode each OBJECT_KEY's nameKey, dedupe in-place. Kept
    // simple (and allocating) because the fast path covers every page
    // produced by the current writer; this branch only executes on
    // legacy-format pages read from older stores.
    final MemorySegment sp = slottedPage;
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sp == null && sidecar == null)
      return EMPTY_INT_ARRAY;
    int[] distinct = new int[8];
    int n = 0;
    for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
      final boolean inline = sp != null && PageLayout.isSlotPopulated(sp, slot);
      if (!inline && (sidecar == null || !sidecar.has(slot)))
        continue;
      final int kindId = inline
          ? PageLayout.getDirNodeKindId(sp, slot)
          : sidecar.kind(slot);
      if (!isFusedAnyObjectNamedKindId(kindId))
        continue;
      final int nameKey = getObjectKeyNameKeyFromSlot(slot);
      // -1 is the not-a-named-slot sentinel; other negative values are
      // legitimate nameKeys (String hashes — 'active'/'amount' hash negative).
      if (nameKey == -1)
        continue;
      boolean seen = false;
      for (int i = 0; i < n; i++) {
        if (distinct[i] == nameKey) {
          seen = true;
          break;
        }
      }
      if (!seen) {
        if (n == distinct.length)
          distinct = Arrays.copyOf(distinct, distinct.length * 2);
        distinct[n++] = nameKey;
      }
    }
    if (n == 0)
      return EMPTY_INT_ARRAY;
    return n == distinct.length
        ? distinct
        : Arrays.copyOf(distinct, n);
  }

  /** Number payload type code for Integer (varint). See NodeKind.serializeNumber. */
  private static final byte NUMBER_TYPE_DOUBLE = 0;
  private static final byte NUMBER_TYPE_FLOAT = 1;
  private static final byte NUMBER_TYPE_BIG_DECIMAL = 5;
  private static final byte NUMBER_TYPE_INTEGER = 2;
  private static final byte NUMBER_TYPE_LONG = 3;
  private static final int NUMBER_VALUE_KIND_ID = 28;
  private static final int STRING_VALUE_KIND_ID = 30;
  /** {@link #STRING_VALUE_KIND_ID}, for the serializer in {@code PageKind}. */
  public static final int STRING_VALUE_KIND_ID_PUBLIC = STRING_VALUE_KIND_ID;
  /** Fused OBJECT_NAMED_* kind ids. Public so {@link PageKind} can dispatch on them. */
  public static final int FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID = 48;
  public static final int FUSED_OBJECT_NAMED_NUMBER_KIND_ID = 49;
  public static final int FUSED_OBJECT_NAMED_STRING_KIND_ID = 50;
  public static final int FUSED_OBJECT_NAMED_NULL_KIND_ID = 51;
  /**
   * Phase 1 reserved fused-structural kindIds. Recognized by {@link #isFusedAnyObjectNamedKindId} but
   * NOT by the iter#30 {@link #isFusedObjectNamedKindId} primitive-only predicate, since the
   * primitive-fused hot path assumes a 9-field layout with NAME_KEY at index 3 — the structural-fused
   * records have a 12-field layout with NAME_KEY at index 5.
   */
  public static final int FUSED_OBJECT_NAMED_OBJECT_KIND_ID = 52;
  public static final int FUSED_OBJECT_NAMED_ARRAY_KIND_ID = 53;

  /**
   * True when {@code kindId} identifies a record whose value participates in the PAX
   * {@link RegionTable#KIND_NUMBER} region. Used by mutation paths to gate cache invalidation: only
   * number-affecting writes pay the (already-cheap) invalidation cost.
   */
  static boolean isNumberValueKindId(final int kindId) {
    return kindId == NUMBER_VALUE_KIND_ID || kindId == FUSED_OBJECT_NAMED_NUMBER_KIND_ID;
  }

  /**
   * True when {@code kindId} identifies a record whose value participates in the PAX
   * {@link RegionTable#KIND_STRING} region.
   */
  static boolean isStringValueKindId(final int kindId) {
    return kindId == STRING_VALUE_KIND_ID || kindId == FUSED_OBJECT_NAMED_STRING_KIND_ID;
  }

  /**
   * True when {@code kindId} identifies a record whose value participates in the PAX
   * {@link RegionTable#KIND_BOOLEAN} region.
   *
   * <p>
   * Only the FUSED kind qualifies, and deliberately so: {@link #collectAndEncodeBooleanRegion}
   * matches on {@link #FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID} alone, so a standalone
   * {@code BOOLEAN_VALUE} contributes no column row and writing one invalidates nothing. This
   * predicate must keep tracking the builder's selection exactly — a drop-set wider than the
   * derive-set throws away columns for nothing, and a narrower one leaves stale ones behind.
   */
  static boolean isBooleanValueKindId(final int kindId) {
    return kindId == FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID;
  }

  /**
   * Lazy pre-parsed FSST symbol table, built once per page on first access. Resolved through
   * {@code FSSTCompressor.parsedFor}, so pages sharing one table byte[] (the common case —
   * distribution and copy paths share the reference) also share one parsed {@code byte[][]}, which is
   * the identity the encode-side matcher cache keys on.
   */
  private volatile byte[][] parsedFsstSymbols;
  private static final byte[][] EMPTY_FSST_SYMBOLS = new byte[0][];

  /**
   * The page's parsed FSST symbols for direct-byte consumers (vectorized scans decoding region
   * dictionary entries). Empty when the page has no resolved table — callers that meet a compressed
   * entry with empty symbols must fall back to a record-level read, which resolves.
   */
  public byte[][] parsedFsstSymbols() {
    return fsstSymbols();
  }

  private byte[][] fsstSymbols() {
    byte[][] s = parsedFsstSymbols;
    if (s != null) {
      return s;
    }
    final byte[] tbl = fsstSymbolTable;
    if (tbl == null || tbl.length == 0) {
      s = EMPTY_FSST_SYMBOLS;
    } else {
      s = FSSTCompressor.parsedFor(tbl);
    }
    parsedFsstSymbols = s;
    return s;
  }

  /**
   * Decode raw bytes if this page's values are FSST-compressed; otherwise copy verbatim.
   * {@code in[0..inLen)} is the raw bytes; decoded output goes to {@code out[outOff..)}. Returns
   * decoded length or -1 on failure. Fixed-in-place decode: safe when
   * {@code in == out && outOff == 0} for non-compressed passthrough (we write the same bytes). For
   * compressed input, caller should pass distinct buffers or accept in-place overwrite since FSST
   * expands (decoded ≥ encoded).
   */
  public int decodeRawIfCompressed(final byte[] in, final int inLen, final byte[] out, final int outOff) {
    if (fsstSymbolTable == null) {
      if (in != out || outOff != 0) {
        System.arraycopy(in, 0, out, outOff, inLen);
      }
      return inLen;
    }
    final byte[][] symbols = fsstSymbols();
    if (symbols.length == 0) {
      if (in != out || outOff != 0) {
        System.arraycopy(in, 0, out, outOff, inLen);
      }
      return inLen;
    }
    // Decode into a temp: FSST expands, so writing into `in` in-place risks
    // overwriting unread bytes. Small (<=256) so stack-ish.
    final byte[] tmp = in == out
        ? new byte[inLen * 3 + 8]
        : null;
    final byte[] dst = tmp != null
        ? tmp
        : out;
    final int dstOff = tmp != null
        ? 0
        : outOff;
    int outPos = dstOff;
    for (int pos = 0; pos < inLen;) {
      final int b = in[pos++] & 0xFF;
      if (b == 0xFF) {
        if (pos >= inLen || outPos >= dst.length)
          return -1;
        dst[outPos++] = in[pos++];
      } else if (b < symbols.length) {
        final byte[] sym = symbols[b];
        final int sl = sym.length;
        if (outPos + sl > dst.length)
          return -1;
        System.arraycopy(sym, 0, dst, outPos, sl);
        outPos += sl;
      } else {
        return -1;
      }
    }
    final int decLen = outPos - dstOff;
    if (tmp != null) {
      if (decLen > out.length - outOff)
        return -1;
      System.arraycopy(tmp, 0, out, outOff, decLen);
    }
    return decLen;
  }

  /**
   * Thread-local staging buffer for FSST-compressed source bytes — one bulk copy from the
   * MemorySegment into this scratch avoids N byte- sized {@code sp.get} calls inside
   * {@link #decodeFsstInto}, which profile-dominated via MemorySegment safety-check overhead
   * (isAlignedForElement / checkValidStateRaw / VarHandle dispatch).
   */
  private static final ThreadLocal<byte[]> FSST_SRC_BUF = ThreadLocal.withInitial(() -> new byte[512]);

  /**
   * Decode {@code length} FSST-compressed bytes starting at {@code dataOff} of {@code sp} into
   * {@code scratch}. Mirrors {@code FSSTCompressor.decodeRawCompressed} but reads from a
   * MemorySegment and writes into a caller-provided buffer — no allocation. Returns decoded byte
   * count, or -1 if output overflows.
   *
   * <p>
   * Copies the compressed source into a thread-local byte[] via one {@link MemorySegment#copy} up
   * front so the symbol-dispatch loop reads plain array bytes instead of paying per-byte
   * MemorySegment safety checks (alignment/session/bounds). For short FSST payloads — typical of JSON
   * string columns — the bulk copy is essentially free and the tight array loop JITs cleanly.
   */
  private static int decodeFsstInto(final MemorySegment sp, final long dataOff, final int length,
      final byte[][] symbols, final byte[] scratch) {
    byte[] src = FSST_SRC_BUF.get();
    if (src.length < length) {
      src = new byte[Math.max(length, src.length * 2)];
      FSST_SRC_BUF.set(src);
    }
    MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, dataOff, src, 0, length);
    int outPos = 0;
    final int scratchLen = scratch.length;
    int pos = 0;
    while (pos < length) {
      final int b = src[pos++] & 0xFF;
      if (b == 0xFF) {
        if (pos >= length || outPos >= scratchLen) {
          return -1;
        }
        scratch[outPos++] = src[pos++];
      } else if (b < symbols.length) {
        final byte[] symbol = symbols[b];
        final int sl = symbol.length;
        if (outPos + sl > scratchLen) {
          return -1;
        }
        System.arraycopy(symbol, 0, scratch, outPos, sl);
        outPos += sl;
      } else {
        return -1; // corrupted FSST data
      }
    }
    return outPos;
  }

  /**
   * Read the delta-encoded firstChildKey from a fused structural slot
   * ({@code OBJECT_NAMED_OBJECT}/{@code OBJECT_NAMED_ARRAY}, kindIds 52/53) without moving any cursor
   * or binding a singleton. Returns the raw nodeKey.
   *
   * <p>
   * Phase 4 — the legacy OBJECT_KEY (kindId 26) record is gone, so this helper now reads from
   * fused-structural slots only. Primitive-fused records (48-51) carry NO firstChild and callers must
   * short-circuit on those.
   *
   * <p>
   * Caller must verify the slot holds a fused-structural record first.
   */
  public long getObjectKeyFirstChildKeyFromSlot(final int slotNumber, final long objectKeyNodeKey) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return -1L;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    if (!isFusedStructuralKindId(kindId)) {
      // Primitive-fused or other kinds carry no firstChild — surface sentinel so
      // the caller can fall back to an rtx walk.
      return -1L;
    }
    // Structural-fused (52/53): FIRST_CHILD_KEY is field index 3.
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_FIRST_CHILD_KEY) & 0xFF;
    final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
    final long dataStart = recordBase + 1 + fieldCount;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, objectKeyNodeKey);
  }

  /**
   * Read the delta-encoded parentKey (enclosing OBJECT's nodeKey) from any fused-named slot without
   * moving any cursor or binding a singleton. Decoded directly off the slotted page so the vectorized
   * scan can join sibling fields in Pass 2 by parent-OBJECT nodeKey in O(1) per slot.
   *
   * <p>
   * All fused-named layouts (primitive 48-51 and structural 52-53) place PARENT_KEY at field-table
   * index 0; only the field-count differs.
   *
   * <p>
   * Caller must verify the slot holds a fused-named record; no validation is performed to keep the
   * hot path branch-free beyond the kind-id dispatch that selects the field-count constant.
   *
   * @param slotNumber the slot index (assumed populated + fused-named kind)
   * @param objectKeyNodeKey the slot's nodeKey (base + slotNumber) — the delta-decoder reconstructs
   *        parentKey against it
   * @return parentKey (enclosing OBJECT nodeKey); {@code -1L} if the page was evicted mid-scan
   */
  public long getObjectKeyParentKeyFromSlot(final int slotNumber, final long objectKeyNodeKey) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      return -1L;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
    if (fieldCount <= 0) {
      return -1L;
    }
    // PARENT_KEY = field index 0 across all fused-named layouts.
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1) & 0xFF;
    final long dataStart = recordBase + 1 + fieldCount;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, objectKeyNodeKey);
  }

  /**
   * Read the pathNodeKey stored on a fused-named slot — the fully-qualified path identifier pointing
   * into the PathSummary. Decoded directly off the slotted page without cursor movement or singleton
   * binding, so the vectorized scan can filter matched slots by scope in O(1) per slot.
   *
   * <p>
   * Phase 4 — the legacy OBJECT_KEY (kind 26) and OBJECT_KEY_PAX (kind 126) records have been
   * removed; this helper now dispatches purely on fused-named layouts.
   *
   * <p>
   * Caller must verify the slot holds a fused-named record; no validation is performed to keep the
   * per-slot cost down to a single byte read for the offset and a varint decode for the value.
   *
   * @param slotNumber the slot index (assumed populated + fused-named kind)
   * @param objectKeyNodeKey the slot's nodeKey (base + slotNumber) — the delta-decoder reconstructs
   *        pathNodeKey against it
   * @return the pathNodeKey; {@code 0L} if the slot has no path statistics (resource opened without
   *         path summary)
   */
  public long getObjectKeyPathNodeKeyFromSlot(final int slotNumber, final long objectKeyNodeKey) {
    final MemorySegment sp = scanRecordSegment(slotNumber);
    if (sp == null) {
      // Page was evicted from the cache while a scan was holding a reference.
      // Signal unresolvable; callers either skip the slot or retry the page.
      return -1L;
    }
    final long recordBase = scanRecordBase(sp, slotNumber);
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    final int fieldIdx = NodeFieldLayout.pathNodeKeyFieldIndexForKind(kindId);
    if (fieldIdx < 0) {
      return -1L;
    }
    final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
    final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIdx) & 0xFF;
    final long dataStart = recordBase + 1 + fieldCount;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, objectKeyNodeKey);
  }

  /**
   * Bulk-decode the {@code pathNodeKey}, {@code parentKey}, and {@code firstChildKey} columns for
   * {@code count} OBJECT_KEY slots in one tight loop. Mirrors the semantics of
   * {@link #getObjectKeyPathNodeKeyFromSlot}, {@link #getObjectKeyParentKeyFromSlot}, and
   * {@link #getObjectKeyFirstChildKeyFromSlot}, but:
   *
   * <ul>
   * <li>Hoists the slotted-page null check + session checks out of the per-slot loop so the JIT can
   * peel loop-invariant guards.</li>
   * <li>Shares the heap-offset lookup and record base across the three varint decodes per slot —
   * dropping two byte reads that each of the three getters would pay independently.</li>
   * <li>Probes the kind id once per slot so a page that happens to mix dense and PAX OBJECT_KEY
   * encodings (same kind family, different field layout) stays correct.</li>
   * </ul>
   *
   * <p>
   * If the page has been evicted mid-scan ({@code slottedPage == null}), the method fills the output
   * arrays with {@code -1L} so callers can skip the slot via the same sentinel contract as the
   * per-slot getters.
   *
   * <p>
   * CPU profile on 10M cold filterCount showed the three per-slot getters accounting for ~4.5% of the
   * worker thread. A single bulk call in the scan driver (see
   * {@code SirixVectorizedExecutor.collectColumns}) removes the per-iteration method-dispatch +
   * per-call MemorySegment session checks so the JIT can keep the tight inner loop in registers.
   *
   * @param slots slot indices to decode (valid for {@code 0..count})
   * @param count number of slots to decode
   * @param pageBase base nodeKey for this page (pageKey {@literal <<}
   *        {@link Constants#INP_REFERENCE_COUNT_EXPONENT})
   * @param outPathNodeKeys result column — pathNodeKey per slot, sized {@code >= count}. Values match
   *        {@link #getObjectKeyPathNodeKeyFromSlot}.
   * @param outParentKeys result column — parentKey per slot.
   * @param outFirstChildKeys result column — firstChildKey per slot.
   */
  public void bulkDecodeObjectKeyColumns(final int[] slots, final int count, final long pageBase,
      final long[] outPathNodeKeys, final long[] outParentKeys, final long[] outFirstChildKeys) {
    if (overflowSlotSidecar != null) {
      for (int i = 0; i < count; i++) {
        final int slot = slots[i];
        final long nodeKey = pageBase + slot;
        outPathNodeKeys[i] = getObjectKeyPathNodeKeyFromSlot(slot, nodeKey);
        outParentKeys[i] = getObjectKeyParentKeyFromSlot(slot, nodeKey);
        outFirstChildKeys[i] = getObjectKeyFirstChildKeyFromSlot(slot, nodeKey);
      }
      return;
    }
    final MemorySegment sp = slottedPage;
    if (sp == null || count == 0) {
      for (int i = 0; i < count; i++) {
        outPathNodeKeys[i] = -1L;
        outParentKeys[i] = -1L;
        outFirstChildKeys[i] = -1L;
      }
      return;
    }
    // Per-slot dispatch: pages contain only fused OBJECT_NAMED_* records (kindIds 48-53)
    // after Phase 4 deleted the legacy OBJECT_KEY (kind 26 / 126). Primitive-fused records
    // (48-51) carry NO firstChildKey — surface -1L so the caller's direct-slot fast path
    // bails out and routes through the rtx fallback. Structural-fused (52-53) carries one.
    for (int i = 0; i < count; i++) {
      final int slot = slots[i];
      final long nodeKey = pageBase + slot;
      final int heapOffset = heapOffsetOf(sp, slot);
      final long recordBase = PageLayout.HEAP_START + heapOffset;
      final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
      final long offsetTable = recordBase + 1;
      if (isFusedObjectNamedKindId(kindId)) {
        // Primitive-fused: parentKey at field 0, pathNodeKey at field 4, no firstChildKey.
        final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
        final long dataStart = offsetTable + fieldCount;
        final int parentFieldOff = sp.get(ValueLayout.JAVA_BYTE, offsetTable + 0) & 0xFF;
        final int pathFieldOff = sp.get(ValueLayout.JAVA_BYTE, offsetTable + 4) & 0xFF;
        outParentKeys[i] = DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + parentFieldOff, nodeKey);
        outFirstChildKeys[i] = -1L;
        outPathNodeKeys[i] = DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + pathFieldOff, nodeKey);
        continue;
      }
      if (isFusedStructuralKindId(kindId)) {
        // Structural-fused (52, 53): parent=0, firstChild=3, pathNodeKey=6.
        final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
        final long dataStart = offsetTable + fieldCount;
        final int parentFieldOff =
            sp.get(ValueLayout.JAVA_BYTE, offsetTable + NodeFieldLayout.OBJNAMEDOBJ_PARENT_KEY) & 0xFF;
        final int firstChildFieldOff =
            sp.get(ValueLayout.JAVA_BYTE, offsetTable + NodeFieldLayout.OBJNAMEDOBJ_FIRST_CHILD_KEY) & 0xFF;
        final int pathFieldOff =
            sp.get(ValueLayout.JAVA_BYTE, offsetTable + NodeFieldLayout.OBJNAMEDOBJ_PATH_NODE_KEY) & 0xFF;
        outParentKeys[i] = DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + parentFieldOff, nodeKey);
        outFirstChildKeys[i] = DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + firstChildFieldOff, nodeKey);
        outPathNodeKeys[i] = DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + pathFieldOff, nodeKey);
        continue;
      }
      // Non-fused-named slot: surface sentinel.
      outParentKeys[i] = -1L;
      outFirstChildKeys[i] = -1L;
      outPathNodeKeys[i] = -1L;
    }
  }

  /**
   * Two-column variant of {@link #bulkDecodeObjectKeyColumns} that skips the {@code pathNodeKey}
   * decode — used by {@code collectColumns} pass 2, which only needs {@code parentKey} (for the batch
   * parent-row join) and {@code firstChildKey} (for the sibling value read). Decoding the third
   * column there would waste one varint read per sibling slot per field per page.
   */
  public void bulkDecodeObjectKeyParentAndChildKeys(final int[] slots, final int count, final long pageBase,
      final long[] outParentKeys, final long[] outFirstChildKeys) {
    if (overflowSlotSidecar != null) {
      for (int i = 0; i < count; i++) {
        final int slot = slots[i];
        final long nodeKey = pageBase + slot;
        outParentKeys[i] = getObjectKeyParentKeyFromSlot(slot, nodeKey);
        outFirstChildKeys[i] = getObjectKeyFirstChildKeyFromSlot(slot, nodeKey);
      }
      return;
    }
    final MemorySegment sp = slottedPage;
    if (sp == null || count == 0) {
      for (int i = 0; i < count; i++) {
        outParentKeys[i] = -1L;
        outFirstChildKeys[i] = -1L;
      }
      return;
    }
    // Per-slot dispatch — see bulkDecodeObjectKeyColumns for rationale.
    for (int i = 0; i < count; i++) {
      final int slot = slots[i];
      final long nodeKey = pageBase + slot;
      final int heapOffset = heapOffsetOf(sp, slot);
      final long recordBase = PageLayout.HEAP_START + heapOffset;
      final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
      final long offsetTable = recordBase + 1;
      if (isFusedObjectNamedKindId(kindId)) {
        final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
        final long dataStart = offsetTable + fieldCount;
        final int parentFieldOff = sp.get(ValueLayout.JAVA_BYTE, offsetTable + 0) & 0xFF;
        outParentKeys[i] = DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + parentFieldOff, nodeKey);
        outFirstChildKeys[i] = -1L;
        continue;
      }
      if (isFusedStructuralKindId(kindId)) {
        final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
        final long dataStart = offsetTable + fieldCount;
        final int parentFieldOff =
            sp.get(ValueLayout.JAVA_BYTE, offsetTable + NodeFieldLayout.OBJNAMEDOBJ_PARENT_KEY) & 0xFF;
        final int firstChildFieldOff =
            sp.get(ValueLayout.JAVA_BYTE, offsetTable + NodeFieldLayout.OBJNAMEDOBJ_FIRST_CHILD_KEY) & 0xFF;
        outParentKeys[i] = DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + parentFieldOff, nodeKey);
        outFirstChildKeys[i] = DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + firstChildFieldOff, nodeKey);
        continue;
      }
      outParentKeys[i] = -1L;
      outFirstChildKeys[i] = -1L;
    }
  }


  /**
   * Cache of matching-slot arrays keyed by nameKey (primitive int → int[], no Integer boxing). Built
   * lazily the first time a vectorized scan asks for a given field, reused by every subsequent query
   * on the same page. Memory: one int[] per distinct queried nameKey; for a JSON-array workload
   * that's typically ~5 arrays of ~150 entries each.
   *
   * <p>
   * Immutable once built. For a read-only resource session the page's content doesn't change, so
   * invalidation isn't needed.
   */
  private volatile Int2ObjectOpenHashMap<int[]> objectKeySlotsByName;
  private static final int[] EMPTY_INT_ARRAY = new int[0];

  /** Thread-local scratch for SIMD findMatchingSlots output. Avoids per-page int[] alloc. */
  private static final ThreadLocal<int[]> MATCHING_SLOTS_SCRATCH = ThreadLocal.withInitial(() -> new int[256]);

  /**
   * {@code true} when the slot kind is a fused {@code OBJECT_NAMED_*} record (kindIds 48-51). Fused
   * records play the OBJECT_KEY role and carry the nameKey inline, so
   * {@link #getObjectKeySlotsForNameKey} includes them when searching by name.
   */
  public static boolean isFusedObjectNamedKindId(final int kindId) {
    return kindId >= FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID && kindId <= FUSED_OBJECT_NAMED_NULL_KIND_ID;
  }

  /**
   * {@code true} when the slot kind is ANY fused named record — the iter#30 primitive-fused leaves
   * (kindIds 48-51, see {@link #isFusedObjectNamedKindId}) OR the Phase 1 reserved structural-fused
   * kinds (52, 53). Used by predicates that classify "any record carrying both a fieldname and inline
   * payload/sub-tree" without assuming the primitive-leaf field layout. Phase 1 doesn't emit 52/53,
   * so this predicate is dormant on the wire path.
   */
  public static boolean isFusedAnyObjectNamedKindId(final int kindId) {
    return kindId >= FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID && kindId <= FUSED_OBJECT_NAMED_ARRAY_KIND_ID;
  }

  /**
   * Return the slot indices whose OBJECT_KEY has the given nameKey. Single-pass bitmap walk the first
   * time; array reuse thereafter. Borrowed from DuckDB / ClickHouse column pre-scan: pay the per-slot
   * decode cost once, amortize across the many scans any realistic analytical query does.
   *
   * <p>
   * Zero-allocation on the hot path once built — all subsequent calls just return the cached int[].
   */
  public int[] getObjectKeySlotsForNameKey(final int fieldKey) {
    Int2ObjectOpenHashMap<int[]> cache = objectKeySlotsByName;
    if (cache == null) {
      synchronized (this) {
        cache = objectKeySlotsByName;
        if (cache == null) {
          cache = new Int2ObjectOpenHashMap<>(8);
          objectKeySlotsByName = cache;
        }
      }
    }
    final int[] cached = cache.get(fieldKey);
    if (cached != null)
      return cached;
    return buildObjectKeySlotsForNameKey(cache, fieldKey);
  }

  private int[] buildObjectKeySlotsForNameKey(final Int2ObjectOpenHashMap<int[]> cache, final int fieldKey) {
    final MemorySegment sp = slottedPage;
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sp == null && sidecar == null)
      return EMPTY_INT_ARRAY;

    // Fast path: ObjectKeyNameKeyRegion lets us SIMD-scan the dict-encoded nameKey
    // column instead of walking every populated slot, decoding kind-id, and decoding
    // the per-record nameKey via varint. Profile (Temurin 25, 100M records) showed
    // ObjectKeyNameKeyRegion.nameKeyForSlot at ~8% CPU on the slot-walk path; the
    // findMatchingSlots SIMD scan replaces all of that with one tight ByteVector loop.
    final MemorySegment nameKeyPayload = sidecar == null
        ? regionPayload(RegionTable.KIND_OBJECT_KEY_NAMEKEY)
        : null;
    if (nameKeyPayload != null) {
      final int upperBound = ObjectKeyNameKeyRegion.count(nameKeyPayload);
      if (upperBound == 0) {
        return cachePut(cache, fieldKey, EMPTY_INT_ARRAY);
      }
      // Thread-local scratch — avoids per-page int[] alloc during cache rebuild.
      // Alloc-profile at 100M records showed ~76K samples from this site before
      // the scratch was introduced.
      int[] tmp = MATCHING_SLOTS_SCRATCH.get();
      if (tmp.length < upperBound) {
        tmp = new int[Math.max(upperBound, tmp.length * 2)];
        MATCHING_SLOTS_SCRATCH.set(tmp);
      }
      final int matched = ObjectKeyNameKeyRegion.findMatchingSlots(nameKeyPayload, fieldKey, tmp);
      if (matched == 0)
        return cachePut(cache, fieldKey, EMPTY_INT_ARRAY);
      final int[] result = Arrays.copyOf(tmp, matched);
      return cachePut(cache, fieldKey, result);
    }

    // Slow path (region absent): walk populated-slot bitmap in-line (direct bit scan inlines
    // cleanly, no lambda). Phase 4 — only fused OBJECT_NAMED_* records (kindIds 48-53) carry
    // an inline nameKey; legacy OBJECT_KEY (26) is gone.
    int[] buf = new int[32];
    int count = 0;
    if (sp != null) {
      for (int wordIndex = 0; wordIndex < PageLayout.BITMAP_WORDS; wordIndex++) {
        long word = PageLayout.getBitmapWord(sp, wordIndex);
        final int baseSlot = wordIndex << 6;
        while (word != 0) {
          final int bit = Long.numberOfTrailingZeros(word);
          final int slot = baseSlot + bit;
          final int kindId = PageLayout.getDirNodeKindId(sp, slot);
          boolean matches = false;
          if (isFusedObjectNamedKindId(kindId)) {
            matches = getFusedObjectNamedNameKeyFromSlot(slot) == fieldKey;
          } else if (isFusedStructuralKindId(kindId)) {
            matches = getFusedStructuralNameKeyFromSlot(slot) == fieldKey;
          }
          if (matches) {
            if (count == buf.length) {
              buf = Arrays.copyOf(buf, buf.length << 1);
            }
            buf[count++] = slot;
          }
          word &= word - 1;
        }
      }
    }
    if (sidecar != null) {
      for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
        if (!sidecar.has(slot) || sp != null && PageLayout.isSlotPopulated(sp, slot)) {
          continue;
        }
        final int kindId = sidecar.kind(slot);
        final boolean matches = isFusedObjectNamedKindId(kindId)
            ? getFusedObjectNamedNameKeyFromSlot(slot) == fieldKey
            : isFusedStructuralKindId(kindId) && getFusedStructuralNameKeyFromSlot(slot) == fieldKey;
        if (matches) {
          if (count == buf.length) {
            buf = Arrays.copyOf(buf, buf.length << 1);
          }
          buf[count++] = slot;
        }
      }
    }
    final int[] result = (count == buf.length)
        ? buf
        : Arrays.copyOf(buf, count);
    return cachePut(cache, fieldKey, result);
  }

  private static int[] cachePut(final Int2ObjectOpenHashMap<int[]> cache, final int fieldKey, final int[] result) {
    synchronized (cache) {
      final int[] existing = cache.get(fieldKey);
      if (existing != null)
        return existing;
      cache.put(fieldKey, result);
    }
    return result;
  }

  /**
   * Set slot data by copying directly from a source MemorySegment. Zero-copy path for page
   * deserialization.
   *
   * @param source the source MemorySegment containing the data
   * @param sourceOffset the byte offset within source where data starts
   * @param dataSize the number of bytes to copy (must be &gt; 0)
   * @param slotNumber the slot number (0 to Constants.NDP_NODE_COUNT-1)
   */
  public void setSlotDirect(MemorySegment source, long sourceOffset, int dataSize, int slotNumber) {
    final int totalBytes = Math.addExact(dataSize, areDeweyIDsStored
        ? PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0);
    if (!ensureInlineAppendCapacity(totalBytes)) {
      throw new IllegalStateException("No inline capacity remains for raw slot " + slotNumber
          + "; caller must publish a canonical overflow carrier");
    }
    setSlotToHeapDirect(source, sourceOffset, dataSize, slotNumber, 0);
    if (overflowSlotSidecar != null) {
      removeSideSlot(slotNumber);
    }
    if (!references.isEmpty()) {
      references.remove((recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber);
    }
  }



  public int getLastSlotIndex() {
    return lastSlotIndex;
  }



  /**
   * Get the slot bitmap for O(k) iteration over populated slots. Returns a mutable copy — callers may
   * modify the returned array without affecting page state. Each call allocates a fresh array.
   *
   * @return a fresh long[16] copy of the bitmap (all zeros if page is closed)
   */
  public long[] getSlotBitmap() {
    final long[] copy = new long[BITMAP_WORDS];
    // One read of the field: a concurrent close() nulls it between a check and a second read,
    // and copying through the local at worst reads the recycled frame — which callers on shared
    // pages detect by validating isClosed() AFTER the copy (close sets its flag first).
    final MemorySegment sp = slottedPage;
    if (sp != null) {
      PageLayout.copyBitmapTo(sp, copy);
    }
    return copy;
  }

  /**
   * Return one 64-slot word of the logical record bitmap without allocating a merged bitmap.
   * Inline records are the ordinary one-load path; cold side images, same-page overflow references,
   * and pending records are ORed in only when present.
   *
   * @param wordIndex bitmap word in {@code [0, 15]}
   * @return one word whose set bits each identify a logical record exactly once
   */
  public long logicalSlotBitmapWord(final int wordIndex) {
    if (wordIndex < 0 || wordIndex >= BITMAP_WORDS) {
      throw new IndexOutOfBoundsException("wordIndex=" + wordIndex);
    }
    final MemorySegment page = slottedPage;
    long word = page == null
        ? 0L
        : PageLayout.getBitmapWord(page, wordIndex);
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    final DataRecord[] pendingRecords = records;
    final boolean referencesEmpty = references.isEmpty();
    if (sidecar == null && referencesEmpty && pendingRecords == null) {
      return word;
    }
    return mergeColdLogicalSlotBitmapWord(wordIndex, word, sidecar, pendingRecords, referencesEmpty);
  }

  /** Cold merge for pages carrying non-inline logical records. */
  private long mergeColdLogicalSlotBitmapWord(final int wordIndex, long word,
      final OverflowSlotSidecar sidecar, final DataRecord[] pendingRecords, final boolean referencesEmpty) {
    if (sidecar != null) {
      word |= sidecar.bitmapWord(wordIndex);
    }
    if (!referencesEmpty) {
      for (final long recordKey : references.keySet()) {
        if ((recordKey >> Constants.NDP_NODE_COUNT_EXPONENT) != recordPageKey) {
          continue;
        }
        final int slot = StorageEngineReader.recordPageOffset(recordKey);
        if (slot >>> 6 == wordIndex) {
          word |= 1L << (slot & 63);
        }
      }
    }
    if (pendingRecords != null) {
      final int baseSlot = wordIndex << 6;
      for (int bit = 0; bit < Long.SIZE; bit++) {
        if (pendingRecords[baseSlot + bit] != null) {
          word |= 1L << bit;
        }
      }
    }
    return word;
  }

  /**
   * Check if a specific slot is populated using the bitmap. This is O(1) and avoids memory access to
   * slotOffsets.
   *
   * @param slotNumber the slot index (0-1023)
   * @return true if the slot is populated
   */
  public boolean hasSlot(int slotNumber) {
    return slottedPage != null && PageLayout.isSlotPopulated(slottedPage, slotNumber);
  }

  /**
   * Returns a primitive int array of populated slot indices for O(k) iteration.
   * <p>
   * This enables efficient iteration over only populated slots instead of iterating all 1024 slots
   * and checking for null. For sparse pages with k populated slots, this is O(k) instead of O(1024).
   * <p>
   * Note: This allocates a new array on each call. For hot paths where the same page is iterated
   * multiple times, consider using {@link #forEachPopulatedSlot}.
   * <p>
   * Example usage:
   * 
   * <pre>{@code
   * int[] slots = page.populatedSlots();
   * for (int i = 0; i < slots.length; i++) {
   *   int slot = slots[i];
   *   MemorySegment data = page.getSlot(slot);
   *   // process data - no null check needed
   * }
   * }</pre>
   * 
   * @return primitive int array of populated slot indices in ascending order
   */
  public int[] populatedSlots() {
    // First pass: count populated slots using SIMD
    int count = populatedSlotCount();

    // Allocate exact-sized array
    int[] result = new int[count];
    int idx = 0;

    // Second pass: collect slot indices using Brian Kernighan's algorithm
    for (int wordIndex = 0; wordIndex < BITMAP_WORDS; wordIndex++) {
      if (slottedPage == null)
        break;
      final long word = PageLayout.getBitmapWord(slottedPage, wordIndex);
      long remaining = word;
      final int baseSlot = wordIndex << 6; // wordIndex * 64
      while (remaining != 0) {
        final int bit = Long.numberOfTrailingZeros(remaining);
        result[idx++] = baseSlot + bit;
        remaining &= remaining - 1; // Clear lowest set bit
      }
    }
    return result;
  }

  /**
   * Functional interface for slot consumer to enable zero-allocation iteration.
   */
  @FunctionalInterface
  public interface SlotConsumer {
    /**
     * Process a populated slot.
     * 
     * @param slotIndex the slot index
     * @return true to continue iteration, false to stop early
     */
    boolean accept(int slotIndex);
  }

  /**
   * Zero-allocation iteration over logical records on the page.
   * <p>
   * The ordinary path is the inline bitmap walk. Cold overflow side images, reference-only
   * records, and pending heap records are appended exactly once when present. The consumer returns
   * false to stop iteration early. {@link #populatedSlots()} and {@link #populatedSlotCount()} stay
   * physical-inline APIs for version reconstruction.
   * <p>
   * Example usage:
   * 
   * <pre>{@code
   * page.forEachPopulatedSlot(slot -> {
   *   MemorySegment data = page.getSlot(slot);
   *   // process data
   *   return true; // continue iteration
   * });
   * }</pre>
   * 
   * @param consumer the consumer to process each populated slot
   * @return the number of slots processed
   */
  public int forEachPopulatedSlot(SlotConsumer consumer) {
    if (consumer == null) {
      throw new NullPointerException("consumer");
    }
    int processed = 0;
    final MemorySegment page = slottedPage;
    for (int wordIndex = 0; wordIndex < BITMAP_WORDS; wordIndex++) {
      if (page == null)
        break;
      long word = PageLayout.getBitmapWord(page, wordIndex);
      final int baseSlot = wordIndex << 6; // wordIndex * 64
      while (word != 0) {
        final int bit = Long.numberOfTrailingZeros(word);
        final int slot = baseSlot + bit;
        processed++;
        if (!consumer.accept(slot)) {
          return processed;
        }
        word &= word - 1; // Clear lowest set bit
      }
    }

    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    final boolean hasReferences = !references.isEmpty();
    final DataRecord[] pendingRecords = records;
    if (sidecar == null && !hasReferences && pendingRecords == null) {
      return processed;
    }
    return forEachColdLogicalSlot(consumer, processed, page, sidecar, hasReferences, pendingRecords);
  }

  /** Cold continuation for side, reference-only, and pending logical carriers. */
  private int forEachColdLogicalSlot(final SlotConsumer consumer, int processed, final MemorySegment page,
      final OverflowSlotSidecar sidecar, final boolean hasReferences, final DataRecord[] pendingRecords) {
    if (sidecar != null) {
      for (int wordIndex = 0; wordIndex < BITMAP_WORDS; wordIndex++) {
        long word = sidecar.bitmapWord(wordIndex);
        final int baseSlot = wordIndex << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          final int slot = baseSlot + bit;
          if (page == null || !PageLayout.isSlotPopulated(page, slot)) {
            processed++;
            if (!consumer.accept(slot)) {
              return processed;
            }
          }
          word &= word - 1;
        }
      }
    }

    if (hasReferences) {
      for (final long recordKey : references.keySet()) {
        if ((recordKey >> Constants.NDP_NODE_COUNT_EXPONENT) != recordPageKey) {
          continue;
        }
        final int slot = StorageEngineReader.recordPageOffset(recordKey);
        if (page != null && PageLayout.isSlotPopulated(page, slot)
            || sidecar != null && sidecar.has(slot)) {
          continue;
        }
        processed++;
        if (!consumer.accept(slot)) {
          return processed;
        }
      }
    }

    if (pendingRecords != null) {
      final long baseRecordKey = recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT;
      for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
        if (pendingRecords[slot] == null
            || page != null && PageLayout.isSlotPopulated(page, slot)
            || sidecar != null && sidecar.has(slot)
            || hasReferences && references.containsKey(baseRecordKey + slot)) {
          continue;
        }
        processed++;
        if (!consumer.accept(slot)) {
          return processed;
        }
      }
    }
    return processed;
  }

  /**
   * Get the count of populated slots using SIMD-accelerated population count. Uses Vector API for
   * parallel bitCount across multiple longs. This is O(BITMAP_WORDS / SIMD_WIDTH) instead of O(1024).
   * 
   * @return number of populated slots
   */
  public int populatedSlotCount() {
    return slottedPage != null
        ? PageLayout.countPopulatedSlots(slottedPage)
        : 0;
  }

  /**
   * Check if all slots are populated using SIMD-accelerated comparison.
   * 
   * @return true if all 1024 slots are populated
   */
  public boolean isFullyPopulated() {
    return slottedPage != null && PageLayout.countPopulatedSlots(slottedPage) == PageLayout.SLOT_COUNT;
  }

  /**
   * SIMD-accelerated bitmap OR into destination array. Computes: dest[i] |= src[i] for all bitmap
   * words.
   * 
   * @param dest destination bitmap (modified in place)
   * @param src source bitmap to OR into dest
   */
  public static void bitmapOr(long[] dest, long[] src) {
    int i = 0;
    final int simdWidth = LONG_SPECIES.length();
    final int simdBound = BITMAP_WORDS - (BITMAP_WORDS % simdWidth);

    for (; i < simdBound; i += simdWidth) {
      LongVector destVec = LongVector.fromArray(LONG_SPECIES, dest, i);
      LongVector srcVec = LongVector.fromArray(LONG_SPECIES, src, i);
      destVec.or(srcVec).intoArray(dest, i);
    }

    // Scalar tail
    for (; i < BITMAP_WORDS; i++) {
      dest[i] |= src[i];
    }
  }

  /**
   * Check if any bits in src are NOT set in dest using SIMD. Returns true if there exist slots in src
   * that are not yet in dest. Useful for early termination in page combining.
   * 
   * @param dest the "filled" bitmap
   * @param src the source bitmap to check
   * @return true if src has bits not present in dest
   */
  public static boolean hasNewBits(long[] dest, long[] src) {
    int i = 0;
    final int simdWidth = LONG_SPECIES.length();
    final int simdBound = BITMAP_WORDS - (BITMAP_WORDS % simdWidth);

    for (; i < simdBound; i += simdWidth) {
      LongVector destVec = LongVector.fromArray(LONG_SPECIES, dest, i);
      LongVector srcVec = LongVector.fromArray(LONG_SPECIES, src, i);
      // newBits = src & ~dest (bits in src but not in dest)
      LongVector newBits = srcVec.and(destVec.not());
      if (newBits.reduceLanes(VectorOperators.OR) != 0) {
        return true;
      }
    }

    // Scalar tail
    for (; i < BITMAP_WORDS; i++) {
      if ((src[i] & ~dest[i]) != 0) {
        return true;
      }
    }
    return false;
  }


  /**
   * Get the slotted page MemorySegment for serialization. When non-null, the page uses
   * LeanStore-style heap storage instead of legacy slotMemory.
   *
   * @return the slotted page segment, or null if not yet initialized
   */
  public MemorySegment getSlottedPage() {
    final MemorySegment sp = slottedPage;
    if (DEBUG_MEMORY_LEAKS && sp == null && isClosed()) {
      LOGGER.error("Use-after-close: null slottedPage observed on page {} ({}, rev={}, guards={}) by thread {}",
          recordPageKey, indexType, revision, guardCount.get(), Thread.currentThread().getName(), closeSite);
    }
    return sp;
  }

  /**
   * Set the slotted page MemorySegment (used during deserialization). Releases any previously
   * allocated slotted page.
   *
   * @param slottedPage the slotted page segment
   */
  public void setSlottedPage(final MemorySegment newSlottedPage) {
    final MemorySegment previous = this.slottedPage;
    final int previousCapacity = this.slottedPageCapacity;
    this.slottedPageCapacity = (int) newSlottedPage.byteSize();
    publishSlottedPage(newSlottedPage.reinterpret(slottedPageCapacity)); // capacity-bounded view
    // Release the old slotted page — if there was one, and if it is not the segment just published —
    // only AFTER the publication. See growSlottedPage: a slot released while the page still points
    // at it can be handed to another page under a reader whose binding still certifies it.
    //
    // Compared by ADDRESS, not identity: a segment stored here has been through reinterpret, which
    // returns a fresh object over the same memory, so an identity test would answer "different" for
    // a caller re-publishing the very segment this page already holds — and then free it out from
    // under the publication one line above.
    if (previous != null && previous.address() != newSlottedPage.address()) {
      segmentAllocator.release(previous.reinterpret(previousCapacity));
    }
    this.cachedHeapEnd = PageLayout.getHeapEnd(this.slottedPage);
    this.cachedHeapUsed = PageLayout.getHeapUsed(this.slottedPage);
    this.cachedPopulatedCount = PageLayout.getPopulatedCount(this.slottedPage);
  }



  @Override
  public int getUsedDeweyIdSize() {
    // DeweyIDs are inline in the slotted page heap — no separate memory
    return 0;
  }

  @Override
  public int getUsedSlotsSize() {
    final int inlineBytes = slottedPage != null
        ? cachedHeapUsed
        : 0;
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    return sidecar == null
        ? inlineBytes
        : Math.addExact(inlineBytes, sidecar.liveBytes());
  }

  public int getSlotMemoryByteSize() {
    final int inlineBytes = slottedPage != null
        ? PageLayout.HEAP_START + cachedHeapEnd
        : 0;
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    return sidecar == null
        ? inlineBytes
        : Math.toIntExact(Math.addExact((long) inlineBytes, sidecar.retainedBytes()));
  }


  @Override
  public byte[] getSlotAsByteArray(int slotNumber) {
    var memorySegment = getSlot(slotNumber);

    if (memorySegment == null) {
      return null;
    }

    var data = memorySegment.toArray(ValueLayout.JAVA_BYTE);
    assert data.length != 0;
    return data;
  }

  public boolean isSlotSet(int slotNumber) {
    return slottedPage != null && PageLayout.isSlotPopulated(slottedPage, slotNumber);
  }

  @Override
  public MemorySegment getSlot(int slotNumber) {
    if (slottedPage == null || !PageLayout.isSlotPopulated(slottedPage, slotNumber)) {
      return null;
    }
    final int heapOffset = heapOffsetOf(slottedPage, slotNumber);
    // Use record-only length (excludes inline DeweyID data + 2-byte trailer)
    final int recordLength = PageLayout.getRecordOnlyLength(slottedPage, slotNumber);
    if (recordLength <= 0) {
      return null;
    }
    return slottedPage.asSlice(PageLayout.HEAP_START + heapOffset, recordLength);
  }

  /**
   * Where a slot's record starts in the heap, after making sure the record is actually there.
   *
   * <p>
   * This is the gate. A lazily loaded page has a complete directory over a heap whose bytes arrive a
   * chunk at a time, so every read that resolves a slot to a heap address has to come through here —
   * and by resolving the address <em>and</em> expanding the chunk in one call, a reader cannot get
   * one without the other. Directory-only reads (a slot's kind, its length, whether it is populated)
   * are answerable from the META section alone and deliberately do not gate.
   *
   * <p>
   * On the overwhelmingly common eager page this is a null check against a field that was written
   * before the page was published, which the JIT folds into the surrounding load.
   */
  private int heapOffsetOf(final MemorySegment sp, final int slotNumber) {
    final LazyChunkedBody lazy = lazyChunkedBody;
    if (lazy != null) {
      lazy.ensureChunkFor(this, slotNumber);
      if (ChunkedBodyConfig.poisonEnabled()) {
        assertChunkMaterialized(lazy, slotNumber);
      }
    }
    return PageLayout.getDirHeapOffset(sp, slotNumber);
  }

  /**
   * Test-mode check that the gate resolved to the right chunk.
   *
   * <p>
   * Poison-filling catches a reader that never gated at all — it reads {@code 0xCC} and fails its
   * comparison. This catches the subtler failure the fill cannot: a gate that consulted the wrong
   * chunk, expanded it, and then read a slot that is still poison.
   */
  private void assertChunkMaterialized(final LazyChunkedBody lazy, final int slotNumber) {
    final int chunk = lazy.chunkOf(slotNumber);
    if (chunk >= 0 && !lazy.isMaterialized(chunk)) {
      throw new IllegalStateException("page " + recordPageKey + " slot " + slotNumber + " was read while chunk " + chunk
          + ", the chunk holding it, was still unexpanded");
    }
  }

  /**
   * Expand the chunk holding {@code slotNumber}, if this page still holds chunks and that one is not
   * expanded yet. A no-op on an eagerly decoded page.
   */
  public void ensureChunkFor(final int slotNumber) {
    final LazyChunkedBody lazy = lazyChunkedBody;
    if (lazy != null) {
      lazy.ensureChunkFor(this, slotNumber);
    }
  }

  /**
   * Expand every chunk this page still holds compressed, so the whole heap is readable without
   * further gating. What a consumer that walks the page from end to end calls once, instead of gating
   * per slot.
   */
  public void ensureAllChunks() {
    final LazyChunkedBody lazy = lazyChunkedBody;
    if (lazy != null) {
      lazy.ensureAllChunks(this);
    }
  }

  /** Whether every record of this page is in the heap. True on any eagerly decoded page. */
  public boolean isFullyMaterialized() {
    final LazyChunkedBody lazy = lazyChunkedBody;
    return lazy == null || lazy.isAllMaterialized();
  }

  /** Encoded bytes this page still holds on behalf of chunks it has not expanded. */
  public int pendingChunkBytes() {
    final LazyChunkedBody lazy = lazyChunkedBody;
    return lazy == null
        ? 0
        : lazy.pendingBytes();
  }

  /** Chunks this page's body was framed into, or {@code 0} when it was decoded whole. */
  public int chunkCount() {
    final LazyChunkedBody lazy = lazyChunkedBody;
    return lazy == null
        ? 0
        : lazy.chunkCount();
  }

  /**
   * Hand this page the chunks of its body that have not been expanded. Called by the deserializer
   * before the page is published and by nothing else.
   */
  void setLazyChunkedBody(final LazyChunkedBody body) {
    this.lazyChunkedBody = body;
  }



  private static String createStackTraceMessage(String message) {
    // Only capture stack trace when diagnostics enabled to avoid overhead in production
    if (!DEBUG_MEMORY_LEAKS) {
      return message;
    }
    StringBuilder stackTraceBuilder = new StringBuilder(message + "\n");
    for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
      stackTraceBuilder.append("\t").append(element).append("\n");
    }
    return stackTraceBuilder.toString();
  }

  @Override
  public void setDeweyId(byte[] deweyId, int offset) {
    if (deweyId == null) {
      return;
    }
    ensureSlottedPage();
    setDeweyIdToHeap(MemorySegment.ofArray(deweyId), offset);
  }

  @Override
  public void setDeweyId(MemorySegment deweyId, int offset) {
    if (deweyId == null) {
      return;
    }
    ensureSlottedPage();
    setDeweyIdToHeap(deweyId, offset);
  }

  /**
   * Set a DeweyID for a slot by re-allocating the slot's heap region with DeweyID data appended.
   * Format: [record data][deweyId data][deweyIdLen:2 bytes (u16)]. The old allocation becomes dead
   * heap space.
   */
  private void setDeweyIdToHeap(final MemorySegment deweyId, final int slotNumber) {
    final int deweyIdLen = (int) deweyId.byteSize();
    if (deweyIdLen == 0) {
      return;
    }

    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar != null && sidecar.has(slotNumber)
        && (slottedPage == null || !PageLayout.isSlotPopulated(slottedPage, slotNumber))) {
      final int oldTotalLength = sidecar.imageLength(slotNumber);
      if (oldTotalLength < PageLayout.DEWEY_ID_TRAILER_SIZE) {
        throw new IllegalStateException("Truncated side-slot Dewey trailer for slot " + slotNumber);
      }
      final MemorySegment oldSegment = sidecar.segment(slotNumber);
      final long oldBase = sidecar.offset(slotNumber);
      final int oldDeweyLength = Short.toUnsignedInt(
          oldSegment.get(LE.SHORT, oldBase + oldTotalLength - PageLayout.DEWEY_ID_TRAILER_SIZE));
      final int recordLength = oldTotalLength - oldDeweyLength - PageLayout.DEWEY_ID_TRAILER_SIZE;
      if (recordLength < 0) {
        throw new IllegalStateException("Corrupt side-slot Dewey length " + oldDeweyLength + " for slot "
            + slotNumber);
      }
      final int replacementLength = recordLength + deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE;
      if (replacementLength > OverflowSlotSidecar.MAX_IMAGE_BYTES) {
        throw new IllegalArgumentException("DeweyID update exceeds the side-slot ceiling: " + replacementLength);
      }
      final MemorySegment scratch = SIDE_SLOT_IMAGE_SCRATCH.get();
      if (recordLength > 0) {
        MemorySegment.copy(oldSegment, oldBase, scratch, 0L, recordLength);
      }
      MemorySegment.copy(deweyId, 0L, scratch, recordLength, deweyIdLen);
      PageLayout.writeDeweyIdTrailer(scratch, replacementLength, deweyIdLen);
      final long token = prepareSideSlot(sidecar.kind(slotNumber), scratch, replacementLength);
      publishSideSlot(slotNumber, token);
      return;
    }

    final boolean slotExists = PageLayout.isSlotPopulated(slottedPage, slotNumber);
    final int oldDataLength;
    final int recordLen;
    final int nodeKindId;
    final long oldAbsStart;

    if (slotExists) {
      // Existing slot — read current allocation info
      final int oldHeapOffset = heapOffsetOf(slottedPage, slotNumber);
      oldDataLength = PageLayout.getDirDataLength(slottedPage, slotNumber);
      nodeKindId = PageLayout.getDirNodeKindId(slottedPage, slotNumber);
      recordLen = PageLayout.getRecordOnlyLength(slottedPage, slotNumber);
      oldAbsStart = PageLayout.heapAbsoluteOffset(oldHeapOffset);
    } else {
      // No record yet — DeweyID-only allocation (nodeKindId = 0)
      oldDataLength = 0;
      recordLen = 0;
      nodeKindId = 0;
      oldAbsStart = 0; // unused
    }

    // New total: record + deweyId + 2-byte trailer
    final int newTotalLen = recordLen + deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE;
    final boolean canPublishInline = newTotalLen <= PageConstants.MAX_RECORD_SIZE
        && ensureInlineAppendCapacity(newTotalLen);
    if (!canPublishInline) {
      if (recordLen == 0) {
        if (newTotalLen > OverflowSlotSidecar.MAX_IMAGE_BYTES) {
          throw new IllegalArgumentException("DeweyID metadata exceeds the side-slot ceiling: " + newTotalLen);
        }
        final MemorySegment scratch = SIDE_SLOT_IMAGE_SCRATCH.get();
        MemorySegment.copy(deweyId, 0L, scratch, 0L, deweyIdLen);
        PageLayout.writeDeweyIdTrailer(scratch, newTotalLen, deweyIdLen);
        final long token = prepareSideSlot(0, scratch, newTotalLen);
        publishSideSlot(slotNumber, token);
        return;
      }

      // The input can be a view into this very page. Copy it before clearing or growing the page so
      // an allocator rebind cannot invalidate the source segment.
      final byte[] deweyIdBytes = deweyId.toArray(ValueLayout.JAVA_BYTE);
      final DataRecord overflowRecord;
      final byte[] recordBytes;
      if (nodeKindId > 0) {
        // Flyweight heap bytes include an offset table and are NOT the generic RecordSerializer
        // format consumed by OverflowPage reads. Materialize one cold snapshot and serialize that
        // canonical format instead of copying the slot bytes verbatim.
        final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber;
        final FlyweightNode flyweight = FlyweightNodeFactory.createAndBind(slottedPage, slotNumber, nodeKey,
            resourceConfig.nodeHashFunction);
        try {
          attachFsstSymbolTable((DataRecord) flyweight);
          overflowRecord = flyweight.toSnapshot();
        } finally {
          flyweight.clearBinding();
        }
        canonicalizeOverflowString(overflowRecord);
        recordBytes = serializeOverflowRecord(overflowRecord);
      } else {
        // Kind zero is generic RecordSerializer format, but it can still carry an FSST-compressed
        // legacy StringNode tied to this page's dictionary. Materialize and canonicalize it before
        // moving it to an independently versioned OverflowPage; copying the bytes verbatim would
        // strand the value when a later fragment has a different table.
        final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber;
        final MemorySegment boundedRecord = slottedPage.asSlice(oldAbsStart, recordLen);
        overflowRecord = recordPersister.deserialize(new MemorySegmentBytesIn(boundedRecord), nodeKey,
            deweyIdBytes, resourceConfig);
        attachFsstSymbolTable(overflowRecord);
        canonicalizeOverflowString(overflowRecord);
        recordBytes = serializeOverflowRecord(overflowRecord);
      }

      installCanonicalOverflowCarrier(overflowRecord, recordBytes, slotNumber, deweyIdBytes);
      return;
    }

    // ensureInlineAppendCapacity above already reserved the exact append. Keep an invariant check at
    // the raw sink so a future control-flow change cannot grow past the final frame class here.
    final int heapEnd = cachedHeapEnd;
    if (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < newTotalLen) {
      throw new IllegalStateException("DeweyID append lost its inline reservation for slot " + slotNumber);
    }

    // Bump-allocate new space
    final long newAbsStart = PageLayout.heapAbsoluteOffset(heapEnd);

    // Copy record data from old location (if any)
    if (recordLen > 0) {
      MemorySegment.copy(slottedPage, oldAbsStart, slottedPage, newAbsStart, recordLen);
    }

    // Copy DeweyID data
    MemorySegment.copy(deweyId, 0, slottedPage, newAbsStart + recordLen, deweyIdLen);

    // Write DeweyID length trailer (u16 at end)
    PageLayout.writeDeweyIdTrailer(slottedPage, newAbsStart + newTotalLen, deweyIdLen);

    // Update heap end (heapUsed: add new, subtract old dead space)
    updateHeapEnd(heapEnd + newTotalLen);
    updateHeapUsed(cachedHeapUsed + newTotalLen - oldDataLength);

    // Update directory entry
    PageLayout.setDirEntry(slottedPage, slotNumber, heapEnd, newTotalLen, nodeKindId);
    clearSlotPreservation(slotNumber);

    // Mark slot populated if new
    if (!slotExists) {
      PageLayout.markSlotPopulated(slottedPage, slotNumber);
      updatePopulatedCount(cachedPopulatedCount + 1);
    }
  }

  /** Overflow records cannot depend on the record page's revision-local FSST dictionary. */
  private void canonicalizeOverflowString(final DataRecord record) {
    if (record instanceof StringNode stringNode && stringNode.isCompressed()) {
      requireFsstTableForOverflow(stringNode.getFsstSymbolTable(), stringNode.getNodeKey());
      final byte[] rawValue = stringNode.getRawValue();
      stringNode.setRawValue(rawValue, false, null);
    } else if (record instanceof ObjectNamedStringNode fusedStringNode && fusedStringNode.isCompressed()) {
      requireFsstTableForOverflow(fusedStringNode.getFsstSymbolTable(), fusedStringNode.getNodeKey());
      final byte[] rawValue = fusedStringNode.getRawValue();
      fusedStringNode.setRawValue(rawValue, false, null);
    }
  }

  private void requireFsstTableForOverflow(final byte[] symbolTable, final long nodeKey) {
    if (symbolTable == null || symbolTable.length == 0) {
      throw new IllegalStateException("Compressed record " + nodeKey
          + " cannot enter OverflowPage without its FSST symbol table");
    }
  }

  private byte[] serializeOverflowRecord(final DataRecord record) {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegmentBytesOut output = new MemorySegmentBytesOut(arena, 256);
      recordPersister.serialize(output, record, resourceConfig);
      final long length = output.position();
      if (length > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Record is too large for OverflowPage: " + length);
      }
      final byte[] serialized = new byte[(int) length];
      MemorySegment.copy(output.baseSegment(), 0L, MemorySegment.ofArray(serialized), 0L, length);
      return serialized;
    }
  }

  @Override
  public MemorySegment getDeweyId(int offset) {
    final MemorySegment page = slottedPage;
    if (page != null && PageLayout.isSlotPopulated(page, offset)) {
      return PageLayout.getDeweyId(page, offset);
    }
    if (!areDeweyIDsStored) {
      return null;
    }
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar == null || !sidecar.has(offset)) {
      return null;
    }
    final int totalLength = sidecar.imageLength(offset);
    if (totalLength < PageLayout.DEWEY_ID_TRAILER_SIZE) {
      throw new IllegalStateException("Truncated side-slot Dewey trailer for slot " + offset);
    }
    final MemorySegment segment = sidecar.segment(offset);
    final long base = sidecar.offset(offset);
    final int deweyIdLength = Short.toUnsignedInt(
        segment.get(LE.SHORT, base + totalLength - PageLayout.DEWEY_ID_TRAILER_SIZE));
    if (deweyIdLength == 0) {
      return null;
    }
    if (deweyIdLength > totalLength - PageLayout.DEWEY_ID_TRAILER_SIZE) {
      throw new IllegalStateException("Corrupt side-slot Dewey length " + deweyIdLength + " for slot " + offset);
    }
    return segment.asSlice(base + totalLength - PageLayout.DEWEY_ID_TRAILER_SIZE - deweyIdLength,
        deweyIdLength).asReadOnly();
  }

  @Override
  public byte[] getDeweyIdAsByteArray(int slotNumber) {
    // Fast path: skip segment lookup + flag read if DeweyIDs aren't stored
    // for this resource. Hot during shred — called on every bindWriteSingleton.
    if (!areDeweyIDsStored) {
      return null;
    }
    var memorySegment = getDeweyId(slotNumber);

    if (memorySegment == null) {
      return null;
    }

    return memorySegment.toArray(ValueLayout.JAVA_BYTE);
  }


  @Override
  public <C extends KeyValuePage<DataRecord>> C newInstance(long recordPageKey, IndexType indexType,
      StorageEngineReader storageEngineReader) {
    final ResourceConfiguration config = storageEngineReader.getResourceSession().getResourceConfig();
    return (C) new KeyValueLeafPage(recordPageKey, indexType, config, storageEngineReader.getRevisionNumber(), null,
        null, false);
  }

  @Override
  public String toString() {
    final ToStringHelper helper = ToStringHelper.of(this).add("pagekey", recordPageKey);
    if (records != null) {
      for (final DataRecord record : records) {
        if (record != null) {
          helper.add("record", record);
        }
      }
    }
    return helper.toString();
  }

  @Override
  public int size() {
    int count = getNumberOfNonNullEntries();
    if (references.isEmpty()) {
      return count;
    }
    // A DeweyID-only slot and its OverflowPage reference are two physical pieces of one logical
    // record. Count unique record keys, not physical carriers; otherwise a page can appear to reach
    // 1024 entries early and version reconstruction stops before older, still-live records arrive.
    for (final long recordKey : references.keySet()) {
      final int offset = StorageEngineReader.recordPageOffset(recordKey);
      if ((records == null || records[offset] == null) && !isSlotSet(offset)) {
        count++;
      }
    }
    return count;
  }

  private int getNumberOfNonNullEntries() {
    if (records == null) {
      return populatedSlotCount();
    }
    int count = 0;
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      if (records[i] != null || isSlotSet(i)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public boolean isClosed() {
    return ((int) STATE_FLAGS_HANDLE.getVolatile(this) & CLOSED_BIT) != 0;
  }

  // ===== Optimistic read stamps (Umbra/LeanStore-style, backed by FrameSlotAllocator slot versions)
  //
  // A pin (tryAcquireGuard) is a synchronized block plus an atomic RMW on a line every concurrent
  // reader of the page shares — a STORE, so it invalidates that line in every other core. A version
  // snapshot is a plain acquire-load that dirties nothing, so readers of one hot page stop contending
  // with each other entirely. Identical protocol and identical hazards to HOTLeafPage's,
  // deliberately: the two were kept the same shape so the ABA lesson below only has to be learned
  // once.
  //
  // NO READER OF THIS PAGE TYPE DEPENDS ON IT YET, and that is a decision rather than an omission.
  // It was built to de-pin the record cursor, and the measurement said not to: a guard is paid per
  // PAGE SWITCH (a record page holds 1024 records, and moveToSingleton has a within-page fast path
  // that takes no guard at all) while a stamp would be paid per VALIDATED READ, so the trade loses by
  // orders of magnitude on any cursor with locality. HOTLeafPage's identical protocol DOES pay,
  // because a trie descent touches a different leaf at every level. Kept, tested and correct because
  // the reasoning turns on locality rather than on the protocol, and a reader without locality would
  // revive it. See docs/RECORD_PATH_DEPINNING.md.

  /** Coordinates of a page with no segment published at all — nothing to read, nothing to tear. */
  private static final long STAMP_COORDINATES_UNBOUND = Long.MIN_VALUE;

  /**
   * Packed {@code (classIdx << 32) | slotIdx} of the allocator slot backing {@link #slottedPage}, or
   * {@link FrameSlotAllocator#NO_SLOT_COORDINATES} for a page whose memory is not a live frame slot
   * (heap-backed test pages, pool-allocator rollback).
   *
   * <p>
   * Written ONLY by {@link #publishSlottedPage}, in the same rebind window as the segment itself, and
   * never by a reader. It used to be bound lazily by the first {@link #readStamp()}, which is one map
   * probe cheaper for a page nobody ever stamps — but it also put a STORE on the reader side, and a
   * reader's store cannot be ordered against a concurrent publisher's: a reader that computed
   * coordinates from the old segment could land them after the publisher had already reset them,
   * leaving the OLD slot's coordinates describing the NEW segment. Every later reader would then
   * validate its reads against a counter belonging to memory it is not reading — the exact false
   * positive this whole protocol exists to rule out. Binding in the publisher costs one map probe per
   * segment swap — none of which is on a read path — and takes the store off the read path entirely.
   * </p>
   */
  private volatile long stampCoordinates = STAMP_COORDINATES_UNBOUND;

  /**
   * Sequence number identifying the CURRENT binding: EVEN when {@link #stampCoordinates} and
   * {@link #slottedPage} agree, ODD while {@link #publishSlottedPage} is swapping them.
   *
   * <p>
   * The piece that makes a stamp self-describing without packing two quantities into one
   * {@code long}. A slot version is meaningless on its own — it is a sequence private to one slot,
   * and two slots can carry equal values at the same moment — so a reader must prove it is validating
   * against the SAME binding it read under. It carries this alongside the stamp and hands both back;
   * a rebind in between changes the generation and the read is retried. A counter rather than a flag
   * so that a rebind back to the same slot is still detected.
   * </p>
   *
   * <p>
   * The parity is what turns the check from a likelihood into a proof, and it is worth stating why a
   * single post-swap increment is not enough. With one increment there is no marker published BEFORE
   * the segment store, so a reader whose data load returned the new bytes can still observe the old
   * generation at validation time and certify bytes the publisher was mid-way through writing. With
   * the odd marker published first — and a {@code storeStore} fence pinning it there — a data load
   * that returned the new bytes proves the odd store was globally visible before it, hence before the
   * reader's own validation load, which coherence then forces to observe at least that value. Both
   * the odd generation and the later even one fail the check.
   * </p>
   */
  private volatile long stampBindingGeneration;

  /**
   * Segment whose ADDRESS identifies the allocator slot, when that is not {@link #slottedPage} itself
   * — a zero-copy deserialized page takes its slotted region as a MID-BUFFER SLICE, and the allocator
   * keys its live-slot map by the address it handed out. Binding from a slice misses and degrades
   * {@link #validateStamp} to a bare closed-flag test for the page's whole lifetime, which would make
   * the protocol inert on the dominant read path.
   */
  private volatile MemorySegment stampBaseSegment;

  /** Stamp for a page whose memory cannot be torn by slot reuse. Even, so it validates. */
  private static final long STAMP_UNBACKED = 0L;

  /** Stamp that never validates: returned while the page is closed or its slot is mid-teardown. */
  public static final long STAMP_INVALID = 1L;

  /**
   * Tell this page which segment's address identifies its allocator slot. Called by the page
   * deserializer right after construction, before publication, when {@link #slottedPage} is a slice
   * of a larger allocation rather than the allocation itself.
   *
   * <p>
   * NO CALLER TODAY, and package-private so it cannot acquire one from outside: this page's
   * deserializer allocates its slotted region whole rather than slicing a shared decompression
   * buffer, so {@link #slottedPage} already IS the allocation. Kept rather than deleted because the
   * failure it prevents is silent — a slicing deserializer would bind from an address the allocator
   * never handed out, miss, and degrade {@link #validateStamp} to a bare closed-flag test for the
   * page's whole lifetime. Any future zero-copy path here must call this.
   * </p>
   *
   * @param base the segment as returned by the allocator; {@code null} clears the override
   */
  void setStampBaseSegment(final @Nullable MemorySegment base) {
    // Changing the base changes which slot the coordinates resolve to, so it is a rebind like any
    // other: same odd/even window, same fence, same invalidation of outstanding stamps.
    final long generation = stampBindingGeneration;
    stampBindingGeneration = generation + 1;
    VarHandle.storeStoreFence();
    stampBaseSegment = base;
    stampCoordinates = slotCoordinatesOf(slottedPage);
    stampBindingGeneration = generation + 2;
  }

  /**
   * The binding this page's stamps are currently issued against — snapshot it BEFORE
   * {@link #readStamp()} and hand both back to {@link #validateStamp(long, long)}.
   *
   * <p>
   * A stamp alone cannot be validated, and that is not an API wart but the shape of the problem. A
   * stamp is a per-SLOT sequence number, so it only means anything paired with the slot it came from;
   * two slots' counters are unrelated and can hold equal values at the same instant. Validating a
   * stamp against whatever slot the page happens to be bound to NOW therefore compares a version to a
   * counter that never produced it, and can return {@code true} by coincidence — a reader certifying
   * bytes from one allocation against another's sequence. Carrying the binding in the reader's own
   * frame costs one extra {@code long} on the stack and makes that impossible: any rebind between
   * snapshot and validation changes the generation, so validation fails and the read is retried.
   * </p>
   *
   * @return the current binding generation; ODD means a rebind is in flight and no read taken now can
   *         be proved, which {@link #validateStamp(long, long)} rejects
   */
  public long readStampBinding() {
    return stampBindingGeneration;
  }

  /**
   * Snapshot this page's read stamp. Protocol (seqlock, reader side):
   *
   * <pre>{@code
   * long binding = page.readStampBinding();   // the slot generation the stamp will belong to
   * long stamp = page.readStamp();            // odd => closed/teardown in progress: re-resolve
   * ... any number of reads of page content ...
   * if (!page.validateStamp(binding, stamp)) retry;   // torn or rebound: re-resolve, redo
   * }</pre>
   *
   * A validated stamp proves every read since the snapshot saw stable bytes — one validation covers
   * an arbitrary batch of reads, so a cursor may snapshot once per {@code moveTo} and validate at the
   * end of a whole accessor rather than per field.
   *
   * <p>
   * Reading the binding FIRST is what makes the pair safe. A rebind landing between the two reads
   * bumps the generation, so the stamp is validated against a binding that no longer matches and the
   * reader retries — conservative in the only direction that is safe.
   * </p>
   *
   * @return the stamp to hand back to {@link #validateStamp(long, long)}
   */
  public long readStamp() {
    final long coordinates = stampCoordinates;
    if (coordinates == FrameSlotAllocator.NO_SLOT_COORDINATES || coordinates == STAMP_COORDINATES_UNBOUND) {
      // Not frame-slot-backed, or nothing published yet. A live page's memory cannot be torn by slot
      // reuse; a closed one must still never validate.
      return isClosed()
          ? STAMP_INVALID
          : STAMP_UNBACKED;
    }
    final long stamp = FrameSlotAllocator.getInstance().acquireVersion((int) (coordinates >>> 32), (int) coordinates);
    // ORDER IS LOAD-BEARING: the closed check must come AFTER the version acquire, never before.
    // Teardown publishes CLOSED_BIT before releasing the slot, so observing it clear HERE proves the
    // slot had not been released when the version was acquired — any later release bumps it and
    // validateStamp fails. With the check first there is an ABA window: a slot torn down AND
    // re-issued between check and acquire hands back a fresh, stable, EVEN version over another
    // page's bytes, and every read of this page's stale segment then "validates". On HOTLeafPage
    // that surfaced as silent key loss under eviction pressure (HOTLeafUseAfterCloseTest).
    if (isClosed()) {
      return STAMP_INVALID;
    }
    return stamp;
  }

  /**
   * Whether every read since {@code stamp} was taken saw stable bytes.
   *
   * @param bindingAtRead the value {@link #readStampBinding()} returned before the stamp was taken
   * @param stamp a value previously returned by {@link #readStamp()}
   * @return {@code true} iff reads under {@code stamp} are trustworthy
   */
  public boolean validateStamp(final long bindingAtRead, final long stamp) {
    // No load taken under the stamp may sink below the generation check, or the check would be
    // answering about a moment earlier than the reads it is certifying. A volatile load is an
    // ACQUIRE, which stops later accesses floating up but does nothing to stop earlier ones sinking
    // down — so the fence is not redundant with the volatile field below.
    VarHandle.acquireFence();
    // FIRST, and for both stamp kinds. The binding is what makes a stamp interpretable at all: if the
    // page has been re-bound since it was taken, the stamp belongs to a slot this page no longer
    // reads, and the version check below would compare it against an unrelated counter. This also
    // covers the segment swap itself, so the UNBACKED branch is not a special case — a page that
    // swapped from unbacked to frame-backed, or the reverse, changes generation like any other.
    // An ODD binding was snapshotted mid-swap and can never be certified, whatever it compares to.
    if (stampBindingGeneration != bindingAtRead || (bindingAtRead & 1L) != 0L) {
      return false;
    }
    if (stamp == STAMP_UNBACKED) {
      // Unbacked memory has no slot version to consult; detecting a close between snapshot and
      // validation is the strongest check available, and all the non-frame allocators need — their
      // release path is what recycles the memory.
      return !isClosed();
    }
    if ((stamp & 1L) != 0L) {
      return false;
    }
    final long coordinates = stampCoordinates;
    if (coordinates == STAMP_COORDINATES_UNBOUND || coordinates == FrameSlotAllocator.NO_SLOT_COORDINATES) {
      // A backed stamp was issued, so coordinates were bound when it was read; reaching here means
      // the binding was reset by a concurrent segment swap — the old stamp is no longer provable.
      return false;
    }
    return FrameSlotAllocator.getInstance().validateVersion((int) (coordinates >>> 32), (int) coordinates, stamp);
  }

  /**
   * The allocator slot {@code segment} lives in, for {@link #stampCoordinates}. Publisher-side only —
   * called inside a rebind window, never by a reader.
   *
   * @param segment the segment about to become {@link #slottedPage}, or {@code null} when the page is
   *        releasing it
   * @return packed slot coordinates, {@link FrameSlotAllocator#NO_SLOT_COORDINATES} when the memory
   *         is not a live frame slot, or {@link #STAMP_COORDINATES_UNBOUND} when there is no segment
   */
  private long slotCoordinatesOf(final @Nullable MemorySegment segment) {
    if (segment == null) {
      return STAMP_COORDINATES_UNBOUND;
    }
    final MemorySegmentAllocator allocator = Allocators.getInstance();
    if (!(allocator instanceof final FrameSlotAllocator frameSlotAllocator)) {
      return FrameSlotAllocator.NO_SLOT_COORDINATES;
    }
    // Resolve against the segment the ALLOCATOR handed out, which is the whole allocation — for a
    // zero-copy page that is stampBaseSegment, not the slice in slottedPage. See the field javadoc.
    final MemorySegment base = stampBaseSegment;
    return frameSlotAllocator.slotCoordinates(base != null
        ? base
        : segment);
  }

  /**
   * Swap {@link #slottedPage}, dropping the optimistic-read slot binding on both sides of the swap.
   *
   * <p>
   * The ONLY place {@link #slottedPage} may be assigned. A stale binding points at the OLD slot,
   * whose version moves independently of the bytes now being read, so a reader would validate against
   * a slot it is no longer reading — the false positive the protocol must never produce. Routing
   * every site through one setter is what keeps that from depending on each caller remembering.
   * </p>
   *
   * <p>
   * <b>This is a seqlock write, and every part of the sequence is load-bearing.</b> The generation is
   * driven ODD before anything else moves and EVEN again once everything has, so the window in which
   * segment and coordinates disagree is exactly the window in which no stamp can validate. The order
   * cannot be relaxed into a single trailing increment: a volatile store has RELEASE semantics, which
   * stops earlier accesses sinking below it but does nothing to stop the PLAIN store to
   * {@link #slottedPage} being hoisted above it — so with only a trailing bump, a reader whose data
   * load already returned the NEW bytes could still read the OLD generation at validation time and
   * certify bytes the publisher was mid-way through writing. With the odd marker published first, and
   * the {@code storeStore} fence pinning it there, that is impossible: the data store cannot become
   * visible before the odd store, so a reader that saw the new bytes must, by coherence, see a
   * generation of at least the odd one when it validates.
   * </p>
   *
   * <p>
   * What this replaced was an INVARIANT rather than a proof — every caller here is a write or
   * teardown path ({@code ensureSlottedPage}, {@code growSlottedPage}, the bulk copy, {@code close}),
   * and teardown publishes {@code CLOSED_BIT}, which makes {@link #readStamp} answer
   * {@link #STAMP_INVALID}. That was adequate while no optimistic reader could be walking a page mid
   * swap, and stops being adequate the moment the record-read path stops pinning.
   * </p>
   *
   * @param segment the new backing segment, or {@code null} when the page is releasing it
   */
  private void publishSlottedPage(final @Nullable MemorySegment segment) {
    final long generation = stampBindingGeneration;
    // Enter the rebind. Published BEFORE the segment moves, and fenced there, which is what lets a
    // reader conclude anything at all from having observed the new bytes (see the javadoc).
    stampBindingGeneration = generation + 1;
    VarHandle.storeStoreFence();
    // BOTH binding fields, not just the coordinates. stampBaseSegment overrides which address the
    // coordinates resolve against, so leaving it set would resolve the OLD, already-released
    // allocation and hand back coordinates for a slot this page no longer reads — the very false
    // positive the reset exists to prevent, reintroduced by the field that was supposed to make the
    // binding accurate. HOTLeafPage nulls it for the same reason.
    stampBaseSegment = null;
    slottedPage = segment;
    stampCoordinates = slotCoordinatesOf(segment);
    // Leave the rebind. Everything above is ordered before this volatile store, so a reader that
    // observes this generation observes the segment and the coordinates that go with it. A stamp
    // taken before the swap carries the old generation and can no longer validate.
    stampBindingGeneration = generation + 2;
  }

  // Leak detection lives in LeakDetectorState above (registered with LEAK_CLEANER in
  // each constructor when DEBUG_MEMORY_LEAKS is on). The deprecated finalize() override
  // was removed — Cleaner is the sanctioned post-Java-9 replacement: it doesn't run on
  // the GC thread, doesn't resurrect objects, and survives finalize() being removed in
  // a future JDK. close() flips the LeakDetectorState.closed flag so the Cleaner action
  // skips the leak log on a properly-closed page.

  /**
   * Closes this page and releases associated memory resources.
   * <p>
   * This method is thread-safe and idempotent. If the page has active guards (indicating it's in use
   * by a transaction), the close operation is skipped to prevent data corruption.
   * <p>
   * Memory segments allocated by the global allocator are returned to the pool. Externally allocated
   * memory (e.g., test arenas) is not released.
   * <p>
   * For zero-copy pages, the backing buffer (from decompression) is released via the
   * backingBufferReleaser callback.
   */
  @Override
  public synchronized void close() {
    // Check if already closed using VarHandle
    int currentFlags = (int) STATE_FLAGS_HANDLE.getVolatile(this);
    if ((currentFlags & CLOSED_BIT) != 0) {
      return;
    }

    // Check guard count - pages in active use cannot be closed
    int currentGuardCount = guardCount.get();
    if (currentGuardCount > 0) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Close skipped for guarded page: pageKey={}, type={}, guardCount={}", recordPageKey, indexType,
            currentGuardCount);
      }
      return;
    }

    // Set closed flag using CAS (synchronized provides mutual exclusion, but CAS is still correct)
    int newFlags;
    do {
      currentFlags = (int) STATE_FLAGS_HANDLE.getVolatile(this);
      newFlags = currentFlags | CLOSED_BIT;
    } while (!STATE_FLAGS_HANDLE.compareAndSet(this, currentFlags, newFlags));

    if (DEBUG_MEMORY_LEAKS) {
      closeSite = new Throwable("page " + recordPageKey + " (" + indexType + ", rev=" + revision + ") closed by thread "
          + Thread.currentThread().getName());
    }

    // Tell the Cleaner-registered leak detector that this page closed cleanly. The
    // detector's run() reads this flag and skips the leak log. Only present when
    // DEBUG_MEMORY_LEAKS is on; null in production builds.
    if (leakDetectorState != null) {
      leakDetectorState.closed.set(true);
    }

    // Update diagnostic counters if tracking is enabled
    if (DEBUG_MEMORY_LEAKS) {
      PAGES_CLOSED.incrementAndGet();
      PAGES_CLOSED_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0))
                          .incrementAndGet();
      ALL_LIVE_PAGES.remove(this);
      if (recordPageKey == 0) {
        ALL_PAGE_0_INSTANCES.remove(this);
      }
    }

    // Release backing buffer for zero-copy pages (has priority over segment release)
    if (backingBufferReleaser != null) {
      try {
        backingBufferReleaser.run();
      } catch (Throwable e) {
        LOGGER.debug("Failed to release backing buffer for page {}: {}", recordPageKey, e.getMessage());
      }
      backingBufferReleaser = null;
      backingBuffer = null;
    }

    // Unbind all flyweight nodes BEFORE releasing memory — they may still be
    // referenced by cursors/transactions and must fall back to Java field values.
    if (slottedPage != null) {
      unbindFlyweightsOwnedBy(slottedPage);
      try {
        segmentAllocator.release(slottedPage.reinterpret(slottedPageCapacity));
      } catch (Throwable e) {
        LOGGER.debug("Failed to release slotted page for page {}: {}", recordPageKey, e.getMessage());
      }
      publishSlottedPage(null);
      slottedPageCapacity = 0;
    }

    clearFsstBinding();

    // Drop any chunk this page never had to expand. Under the page monitor, which close() already
    // holds and which an expansion takes — so a chunk cannot be decoding into the segment that is
    // being released one line above.
    if (lazyChunkedBody != null) {
      lazyChunkedBody.release();
    }

    // Clear references to aid garbage collection
    if (records != null) {
      Arrays.fill(records, null);
    }
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar != null) {
      try {
        sidecar.close();
      } catch (Throwable e) {
        LOGGER.debug("Failed to release side-slot storage for page {}: {}", recordPageKey, e.getMessage());
      }
      overflowSlotSidecar = null;
    }
    references.clear();
    clearSerializedCache();
    hashCode = null;

    releaseRegionTableOwnership();
  }

  /**
   * Get the actual memory size used by this page's memory segments. Used for accurate Caffeine cache
   * weighing.
   * 
   * @return Total size in bytes of all memory segments used by this page
   */
  public long getActualMemorySize() {
    // The heap segment is allocated at full size the moment the page exists, lazily loaded or not,
    // so a lazy page's extra weight is exactly the chunks it still holds encoded. Counted from the
    // chunk table's lengths, never by decoding: sizing is an accounting question, and asking a page
    // how big it is must not be what expands it.
    long pageBytes = slottedPage != null
        ? slottedPageCapacity + pendingChunkBytes()
        : 0L;
    final OverflowSlotSidecar sidecar = overflowSlotSidecar;
    if (sidecar != null) {
      pageBytes = Math.addExact(pageBytes, sidecar.retainedBytes());
    }
    final RegionTable regions = regionTable;
    return regions == null
        ? pageBytes
        : Math.addExact(pageBytes, regions.retainedFootprintBytes());
  }

  /**
   * Get the FSST symbol table for string compression.
   * 
   * @return the symbol table bytes, or null if FSST is not used
   */
  public byte[] getFsstSymbolTable() {
    return fsstSymbolTable;
  }

  /**
   * Set the FSST symbol table for string compression.
   *
   * @param symbolTable the symbol table bytes
   */
  public void setFsstSymbolTable(byte[] symbolTable) {
    // Bind-once for the bytes, symmetric to the id guard below. Identity may legitimately
    // differ when two readers race to resolve the same id (each deserializes its own copy of
    // the same table), so only a CONTENT change is a rebind — the corruption the guard exists
    // to catch. Arrays.equals runs only on that benign race, never on the common paths.
    if (this.fsstSymbolTable != null && symbolTable != this.fsstSymbolTable
        && !Arrays.equals(this.fsstSymbolTable, symbolTable)) {
      throw new IllegalStateException("page " + recordPageKey + " (revision " + revision
          + ") already holds a different FSST symbol table and cannot be rebound");
    }
    this.fsstSymbolTable = symbolTable;
    this.parsedFsstSymbols = null;
  }

  /**
   * Drop the FSST binding as one unit — table bytes, id, and parsed cache. Split clearing is how
   * stale-binding bugs happen: a surviving id with no bytes claims an encoding the page no longer
   * holds, and a pooled frame's next occupant would trip the rebind guard on it.
   */
  /**
   * Refuse to expand a page whose global-dictionary tags nobody resolved, naming the SITE.
   *
   * <p>
   * The pre-pass inside the injector already refuses such a page, so this adds no safety — it adds
   * an answer to "which route reached expansion without a reader". Chunk expansion is reached from
   * places with no reader on the stack at all: the writer's copy-on-write {@code deepCopy()} on the
   * flush lane, the versioning combine, and the two commit-time FSST passes. A failure reported from
   * inside the injector names a page and a tag; a failure reported here names the route that has to
   * be fronted, which is the thing anyone reading the message needs.
   * </p>
   *
   * <p>
   * A throw and not an assert. Skipping the injection would leave the elided slots holding the
   * placeholder zeros expansion starts from, and a record with an absent value is not a record with
   * an empty value — the substitution this format is arranged to prevent.
   * </p>
   *
   * @param site what is about to expand this page, for the message
   */
  public void refuseUnresolvedGlobalTags(final String site) {
    if (needsGlobalStringResolution()) {
      throw new IllegalStateException("record page " + recordPageKey + " (revision " + revision
          + ") stores string values as global dictionary ids, but " + site + " reached it before any reader "
          + "resolved them. That route holds no storage-engine reader, and expansion cannot walk a dictionary "
          + "itself -- it runs under the page monitor. Resolve the page where a reader IS held, before it "
          + "reaches this route.");
    }
  }

  private void clearFsstBinding() {
    fsstSymbolTable = null;
    fsstSymbolTableId = NO_FSST_SYMBOL_TABLE_ID;
    parsedFsstSymbols = null;
  }

  /**
   * Drop the trie-lane binding as one unit, for the same reason {@link #clearFsstBinding()} exists.
   *
   * <p>
   * All three fields describe the SAME string region, so a reused frame that kept any of them would
   * describe its previous occupant. The resolved table is the dangerous one: its entries are indexed
   * by the old page's tag positions, and a survivor would answer the new page's lookups with the old
   * page's values — plausible bytes of the right shape, which is the failure this format is arranged
   * to prevent. The resolver reference is dropped here too, so a page in a pool cannot hold a
   * transaction's reader alive past that transaction.
   * </p>
   */
  private void clearGlobalStringBinding() {
    resolvedGlobalStrings = null;
    hasGlobalStringTags = false;
    globalStringDictionaries = null;
  }

  /**
   * The id of the symbol table this page's strings were encoded against, or
   * {@link #NO_FSST_SYMBOL_TABLE_ID} when the page carries no reference.
   *
   * <p>
   * A page holds the id rather than the table because the table lives in the dictionary trie, shared
   * by every page of the revision. It is resolved on the first string the page is asked to decode —
   * deserialization has no storage-engine reader to walk the trie with, and a page whose strings are
   * never read should not pay for the lookup.
   *
   * @return the symbol table id, or {@link #NO_FSST_SYMBOL_TABLE_ID}
   */
  public long getFsstSymbolTableId() {
    return fsstSymbolTableId;
  }

  /**
   * Record which symbol table this page's strings were encoded against. A binding is set at most once
   * per page life; it is cleared only by the page-recycling paths ({@code reset}, teardown), never
   * through this setter — "unbind" has no meaning while encoded bytes remain.
   *
   * @param id the symbol table id; must be positive
   * @throws IllegalArgumentException if {@code id} is not positive
   * @throws IllegalStateException if the page is already bound to a different table
   */
  /**
   * Install the dictionary resolver used when this page's string region is (re)built.
   *
   * <p>
   * Only the PATH-tagged encoder consults it, because the projection's anchors are keyed by path
   * node key; a page that ends up name-tagged converts nothing, which is correct rather than a
   * missed opportunity — a name key does not identify a column.
   * </p>
   */
  public void setGlobalStringDictionaries(final @Nullable GlobalStringDictionaries resolver) {
    this.globalStringDictionaries = resolver;
  }

  /** The resolver this page was given, or {@code null}; consulted by the string-region ENCODER. */
  public @Nullable GlobalStringDictionaries globalStringDictionaries() {
    return globalStringDictionaries;
  }

  /**
   * Record that this page's string region carries at least one tag storing global dictionary ids.
   *
   * <p>
   * Set by deserialization, which is the only place that can know it cheaply: the lazy path already
   * parses the string-region header to build its injector, so the flag costs a loop over the tag
   * metadata that was read anyway. Every other route to a page — a writer building one in memory, a
   * combine assembling one from fragments — leaves it false, which is right: those pages hold real
   * values in their heap and have nothing to resolve.
   * </p>
   *
   * <p>
   * It exists so that {@link #needsGlobalStringResolution()} is a field compare. That predicate runs
   * on the return of every record-page lookup, which is a per-record hot path, and re-parsing a
   * string-region header there to discover that the answer is almost always "no" would be a tax on
   * every scan in the system for a lane almost no page uses.
   * </p>
   */
  public void setHasGlobalStringTags(final boolean present) {
    this.hasGlobalStringTags = present;
  }

  /** Whether this page's string region stores any tag as global dictionary ids. */
  public boolean hasGlobalStringTags() {
    return hasGlobalStringTags;
  }

  /**
   * Whether a reader still owes this page a resolution pass before its chunks may expand.
   *
   * <p>
   * Two field reads, in the order that makes the common answer cheapest: a page with no global tags
   * — which is every page of every resource that does not use the trie lane — answers on the first.
   * </p>
   */
  public boolean needsGlobalStringResolution() {
    return hasGlobalStringTags && resolvedGlobalStrings == null;
  }

  /**
   * Publish the bytes a reader resolved for this page's global tags.
   *
   * <p>
   * BYTES, never the resolver that produced them — a resolver holds a transaction's reader and a
   * page outlives transactions in the buffer cache. It is also why this is safe to publish once and
   * share: resolution is page-determined (the page names its dictionary, and a rank-ordered
   * dictionary only appends), so the first transaction to resolve a page fixes values that every
   * later transaction would have computed identically.
   * </p>
   *
   * <p>
   * Idempotent by first-writer-wins rather than by rebinding. Two transactions can race to resolve
   * the same page; both compute the same bytes, so keeping the first costs nothing and avoids
   * publishing a second array to readers already walking the first.
   * </p>
   *
   * @param resolved the table; {@link ResolvedGlobalStrings#NONE} when nothing needed resolving
   */
  public void setResolvedGlobalStrings(final ResolvedGlobalStrings resolved) {
    Objects.requireNonNull(resolved, "resolved global strings must not be null");
    if (this.resolvedGlobalStrings == null) {
      this.resolvedGlobalStrings = resolved;
    }
  }

  /** The resolved global-tag bytes, or {@code null} when no reader has resolved this page yet. */
  public @Nullable ResolvedGlobalStrings resolvedGlobalStrings() {
    return resolvedGlobalStrings;
  }

  public void setFsstSymbolTableId(final long id) {
    if (id <= NO_FSST_SYMBOL_TABLE_ID) {
      throw new IllegalArgumentException("symbol table id must be positive, got " + id);
    }
    // A page binds to one table for life: its already-encoded strings carry no per-record
    // table reference, so rebinding would silently reinterpret them against foreign symbols.
    if (fsstSymbolTableId != NO_FSST_SYMBOL_TABLE_ID && id != fsstSymbolTableId) {
      throw new IllegalStateException("page " + recordPageKey + " (revision " + revision
          + ") is bound to FSST symbol table " + fsstSymbolTableId + " and cannot be rebound to " + id);
    }
    this.fsstSymbolTableId = id;
  }

  /**
   * Returns the PAX region table. {@code null} indicates V0 format or that no regions have been
   * attached. Callers on the hot path should prefer {@link #regionPayload(byte)} to avoid a nullcheck
   * at each slot read.
   */
  public RegionTable getRegionTable() {
    return regionTable;
  }

  public synchronized void setRegionTable(final RegionTable table) {
    if (isClosed()) {
      throw new IllegalStateException("cannot attach a region table to a closed page");
    }
    final RegionTable previous = this.regionTable;
    if (previous == table) {
      return;
    }
    // A wholesale table swap makes every previous derivation verdict meaningless — the memo must
    // restart with the new table or it would claim regions of the OLD table were attempted.
    clearRegionDeriveAttempted(~0);
    this.regionTable = table;
    if (previous != null) {
      previous.close();
    }
  }

  /**
   * Drop this page's one ownership, if present. Callers already hold the page monitor or are
   * quiescent.
   */
  private synchronized void releaseRegionTableOwnership() {
    final RegionTable previous = regionTable;
    regionTable = null;
    if (previous != null) {
      previous.close();
    }
  }

  /**
   * A sidecar slot is intentionally absent from the inline bitmap walked by every PAX builder.
   * Until builders consume a merged inline+side iterator, retaining or deriving any column would be
   * an incomplete-column wrong answer, so fail closed as one operation.
   */
  private void dropColumnRegionsForSideSlots() {
    releaseRegionTableOwnership();
    clearRegionDeriveAttempted(~0);
    cachedNumberHeader = null;
    cachedStringHeader = null;
  }

  /**
   * Direct payload lookup for a region kind. Returns {@code null} when no table is present or the
   * region is absent. Inlineable one-branch hot-path shim.
   */
  public MemorySegment regionPayload(final byte kind) {
    if (overflowSlotSidecar != null) {
      return null;
    }
    final RegionTable t = regionTable;
    return t == null
        ? null
        : t.payload(kind);
  }

  /**
   * Lazily parsed number-region header. {@code null} if the page has no number region (e.g.
   * path-summary pages, index pages, pages with no numeric values). Cached after first parse — zero
   * allocation on subsequent calls.
   */
  private volatile NumberRegion.Header cachedNumberHeader;

  /**
   * Drop the cached {@link NumberRegion.Header} and every payload the number builder installs —
   * {@link RegionTable#KIND_NUMBER}, {@link RegionTable#KIND_NUMBER_ZONEMAP} and
   * {@link RegionTable#KIND_DOUBLE}, i.e. exactly {@code NUMBER_DERIVE_MASK} — so the next reader
   * rebuilds from the slotted page. Called from every mutation path that adds, modifies, or removes a
   * NUMBER_VALUE / OBJECT_NAMED_NUMBER record.
   *
   * <h2>HFT cost model</h2> Steady-state cost when no region is currently cached: one volatile read +
   * one branch. On the first invalidation per page after a region was built: one volatile write + one
   * payload-slot store. After that, calls collapse to the fast-path until the next reader rebuilds.
   *
   * <p>
   * Package-private so unit tests can verify the contract without reflection.
   */
  void invalidateNumberRegion() {
    final RegionTable rt = regionTable;
    // Presence is asked of the table rather than inferred from the cached header: a caller can
    // reach the payload without ever parsing a header, and inferring from the header alone would
    // leave such a region installed across a mutation that invalidated it.
    // Probed over the WHOLE derive mask: the builder installs the long column, its zone map and the
    // double column together, and an all-double page carries KIND_DOUBLE alone.
    final boolean present = rt != null && (rt.hasRegion(RegionTable.KIND_NUMBER)
        || rt.hasRegion(RegionTable.KIND_NUMBER_ZONEMAP) || rt.hasRegion(RegionTable.KIND_DOUBLE));
    if (cachedNumberHeader == null && !present && (regionDeriveAttempted & NUMBER_DERIVE_MASK) == 0) {
      return;
    }
    cachedNumberHeader = null;
    // The derive memo must fall with the region: a memo that survives the drop reads as
    // "derived and refused", and the next reader would skip the rebuild forever.
    clearRegionDeriveAttempted(NUMBER_DERIVE_MASK);
    if (rt != null) {
      rt.set(RegionTable.KIND_NUMBER, null);
      // The zone map summarises the region just dropped. Leaving it behind would not merely be
      // stale — a scan would prune against the bounds of a column that no longer exists and return
      // a wrong count, which is the one failure mode this whole path must not have.
      rt.set(RegionTable.KIND_NUMBER_ZONEMAP, null);
      // The doubles are the other half of the same column: same walk, same builder, same mask. A
      // surviving payload is stale, and pendingDerivations() reads a present double column as "the
      // number builder has nothing to do" — which would latch the whole rebuild off for good.
      rt.set(RegionTable.KIND_DOUBLE, null);
    }
  }

  /**
   * Atomically clear derive-memo bits; the read-modify-write needs the same monitor the setters hold.
   */
  private synchronized void clearRegionDeriveAttempted(final int mask) {
    regionDeriveAttempted &= ~mask;
  }

  /**
   * Drop every column region a record of {@code nodeKindId} contributes a row to.
   *
   * <p>
   * The single definition of "which columns does this kind feed", shared by all five mutation entry
   * points. It exists because the dispatch used to be copied per site as a number/string if-else
   * chain, and a copied chain is how a region ends up with a drop-set narrower than its derive-set:
   * the chain was exclusive, yet a fused {@code OBJECT_NAMED_NUMBER} row belongs to the number column
   * AND to the field-name column, so at most one of the two was ever dropped.
   *
   * <p>
   * The value columns are mutually exclusive by kind, so they stay an if-else. The names column is
   * not: every fused {@code OBJECT_NAMED_*} record is one of its rows whatever its value type, and
   * its row POSITION is what {@link RegionTable#KIND_RECORD_ORDINAL} indexes — the alignment a fused
   * cross-column predicate trusts. A stale ordinal column is a wrong answer rather than a stale
   * bound, so it falls with the names it indexes.
   */
  private void invalidateRegionsForKindId(final int nodeKindId) {
    if (isNumberValueKindId(nodeKindId)) {
      invalidateNumberRegion();
    } else if (isStringValueKindId(nodeKindId)) {
      invalidateStringRegion();
    } else if (isBooleanValueKindId(nodeKindId)) {
      invalidateBooleanRegion();
    }
    if (isFusedObjectNamedKindId(nodeKindId)) {
      invalidateNamesRegion();
    }
  }

  /**
   * Invalidate every column region the record CURRENTLY in {@code slotOffset} belongs to. Called from
   * {@link #setRecord} and {@link #setNewRecord} before mutation, so deletion or replacement of an
   * existing column-bearing record is detected even when the new record kind is something else (e.g.
   * {@code DeletedNode}).
   *
   * <p>
   * The "new record IS column-bearing" case is handled separately by {@link #serializeToHeap} and
   * {@link #completeDirectWrite} which already know the kind id being written.
   */
  private void maybeInvalidateRegionsForExistingSlot(final int slotOffset) {
    // Deliberately NOT gated on the cached headers alone: a page read from disk has its regions
    // installed with no header ever parsed, and returning here left both the stale column and its
    // stale zone map in place across the very mutation that invalidated them.
    //
    // The derive memo is part of that test for the same reason, and it is the case the headers and
    // the table between them cannot see. A builder records its attempt UNCONDITIONALLY — the names
    // builder sets NAMES_DERIVE_MASK at the end of its critical section even when the encoder
    // refused the page (more distinct names than the region encodes), and no table is minted on
    // that path because regionTableForInstall() is reached only when a payload exists. Such a page
    // carries a memo with regionTable and both cached headers null, so returning here skipped
    // invalidateNamesRegion, the memo was never cleared, and pendingDerivations kept answering
    // "already tried, nothing to do" — latching the field-name AND record-ordinal columns off for
    // the rest of the page's life, across the very mutations that would have made them derivable.
    // Tested against the whole memo rather than a named subset: any bit set means some builder
    // recorded an attempt that a mutation must retract, and a future mask gets this for free.
    if (regionTable == null && cachedNumberHeader == null && cachedStringHeader == null && regionDeriveAttempted == 0) {
      return;
    }
    final MemorySegment sp = slottedPage;
    if (sp == null || !PageLayout.isSlotPopulated(sp, slotOffset)) {
      return;
    }
    invalidateRegionsForKindId(PageLayout.getDirNodeKindId(sp, slotOffset));
  }

  public NumberRegion.Header getNumberRegionHeader() {
    if (overflowSlotSidecar != null) {
      return null;
    }
    NumberRegion.Header h = cachedNumberHeader;
    if (h != null) {
      return h;
    }
    MemorySegment payload = regionPayload(RegionTable.KIND_NUMBER);
    if (payload == null) {
      // VersioningType.combineRecordPages produces a fresh KVLP whose slotted
      // page is reconstructed from multiple fragments — no region travels with
      // it. Build the region lazily from the combined slots. Idempotent: the
      // payload is cached via setRegionTable so subsequent queries skip this.
      payload = tryBuildNumberRegionFromSlottedPage();
      if (payload == null) {
        return null;
      }
    }
    // The per-tag directory may live in the zone map; a page read whole always carries both, and a
    // page that carries only the values is declined rather than mis-decoded.
    final MemorySegment directory = regionPayload(RegionTable.KIND_NUMBER_ZONEMAP);
    if (directory == null && NumberRegion.needsExternalDirectory(payload)) {
      return null;
    }
    synchronized (this) {
      h = cachedNumberHeader;
      if (h == null) {
        h = new NumberRegion.Header().parseInto(payload, directory);
        cachedNumberHeader = h;
      }
    }
    return h;
  }

  /**
   * Ensure the number region is attached. Called from the versioning layer's
   * {@code combineRecordPages} after a new KVLP has been reconstructed from one or more fragments.
   * The argument carries the donor page's region — typically the first (or only) fragment.
   *
   * <p>
   * <b>Caller contract.</b> The donor shortcut — copying {@code donor.regionTable} by reference — is
   * only correct when the target is a byte-identical copy of the donor (i.e. single-fragment
   * combine). For multi-fragment combines the caller <b>must</b> pass {@code null} (or use
   * {@link #ensureNumberRegion()}) so the region is rebuilt from the combined slotted heap. Passing a
   * donor in a multi-fragment context silently corrupts aggregates that lean on the PAX number region
   * (zone maps, sum, min/max), so a fail-fast assertion guards the invariant in debug builds.
   */
  public void ensureNumberRegion(final KeyValueLeafPage donor) {
    if (overflowSlotSidecar != null) {
      dropColumnRegionsForSideSlots();
      return;
    }
    if (regionTable != null && regionTable.payload(RegionTable.KIND_NUMBER) != null) {
      return;
    }
    if (donor != null) {
      final RegionTable donorTable = donor.regionTable;
      if (donorTable != null && donorTable.payload(RegionTable.KIND_NUMBER) != null) {
        assert donor.getCachedPopulatedCount() == this.getCachedPopulatedCount()
            : "ensureNumberRegion(donor) called with a multi-fragment target: donor slots="
                + donor.getCachedPopulatedCount() + ", target slots=" + this.getCachedPopulatedCount()
                + ". Caller must pass null in multi-fragment combines.";
        // A wholesale table swap, like setRegionTable: the memo must restart with it, and the
        // assignment takes the monitor so it cannot interleave with a builder's install.
        if (!donorTable.tryRetain()) {
          throw new IllegalStateException("donor region table was released during version reconstruction");
        }
        final RegionTable previous;
        synchronized (this) {
          if (isClosed()) {
            donorTable.close();
            return;
          }
          if (this.regionTable == donorTable) {
            donorTable.close();
            return;
          }
          regionDeriveAttempted = 0;
          previous = this.regionTable;
          this.regionTable = donorTable;
        }
        if (previous != null) {
          previous.close();
        }
        return;
      }
    }
    tryBuildNumberRegionFromSlottedPage();
  }

  /** Backward-compat overload that builds from the slotted page only. */
  public void ensureNumberRegion() {
    ensureNumberRegion(null);
  }

  /**
   * Walk the slotted page, collect each fused {@code OBJECT_NAMED_NUMBER} slot's value + its inline
   * nameKey/pathNodeKey, encode into a NumberRegion payload. Returns {@code null} when the page has
   * no numeric values or no slotted page yet.
   *
   * <p>
   * Side-effect: on success, attaches the region to the page so subsequent lookups skip this build.
   *
   * <p>
   * Two-phase, like every region builder. The WALK runs under the page GUARD — pinning the frame
   * against close() and the recycling that follows, without serializing record readers' guard traffic
   * behind a 1024-slot derivation — and entirely off the page monitor. The INSTALL takes the monitor
   * for a few table stores; that short section is what keeps the table unique and the memo coherent
   * when concurrent workers reach the same page through different doors (the region-only ensure, this
   * builder's getter, the header getters), where an unsynchronized check-then-act once let two
   * workers mint separate tables and the second install orphaned the first thread's derivations. Two
   * workers racing the walk cost one redundant walk, never a torn table: the install re-checks the
   * memo and the loser discards its result.
   */
  private MemorySegment tryBuildNumberRegionFromSlottedPage() {
    if ((regionDeriveAttempted & NUMBER_DERIVE_MASK) != 0) {
      return regionPayload(RegionTable.KIND_NUMBER);
    }
    final byte[] encoded;
    final byte[] doubles;
    if (!acquireGuard()) {
      return null; // mid-close — nothing to derive from
    }
    try {
      if (slottedPage == null) {
        return null; // deliberately unmemoized: there was nothing to walk
      }
      encoded = collectAndEncodeNumberRegion();
      doubles = tryBuildDoubleRegionFromSlottedPage();
    } finally {
      releaseGuard();
    }
    synchronized (this) {
      if (isClosed()) {
        return null;
      }
      if ((regionDeriveAttempted & NUMBER_DERIVE_MASK) != 0) {
        return regionPayload(RegionTable.KIND_NUMBER); // a racing walk installed first
      }
      if (encoded == null) {
        // No longs — but the page may still be ALL-double. Returning before installing the double
        // column left such a page permanently failing the summed completeness oracle: every later
        // scan fell back to record decoding over values sitting right there in the slotted page,
        // the exact trap this rebuild exists to avoid.
        if (doubles != null) {
          regionTableForInstall().set(RegionTable.KIND_DOUBLE, doubles);
        }
        regionDeriveAttempted |= NUMBER_DERIVE_MASK;
        return null;
      }
      final RegionTable table = regionTableForInstall();
      table.set(RegionTable.KIND_NUMBER, encoded);
      final MemorySegment installed = table.payload(RegionTable.KIND_NUMBER);
      // The writer emits an independently framed zone-map region alongside every number region; a
      // region rebuilt here must carry one too. Narrow maps stay raw and wide maps may elect their
      // own LZ77 frame, but neither form materializes the number column. Without it a page that went
      // through versioning reconstruction would still answer correctly but would have to decompress
      // its number column to find bounds every other page hands over for free. Set unconditionally,
      // including to null: leaving a previous zone map beside a number column it no longer describes
      // is the stale-bounds failure in its most direct form — and the same argument covers the double
      // column below.
      table.set(RegionTable.KIND_NUMBER_ZONEMAP,
          NumberZoneMapRegion.encode(new NumberRegion.Header().parseInto(installed)));
      table.set(RegionTable.KIND_DOUBLE, doubles);
      regionDeriveAttempted |= NUMBER_DERIVE_MASK;
      return installed;
    }
  }

  /** The unique table to install into, minted at most once. Callers hold the page monitor. */
  private RegionTable regionTableForInstall() {
    assert Thread.holdsLock(this) : "region table installation requires the page monitor";
    if (isClosed()) {
      throw new IllegalStateException("cannot create a region table for a closed page");
    }
    RegionTable table = this.regionTable;
    if (table == null) {
      table = new RegionTable();
      this.regionTable = table;
    }
    return table;
  }

  /**
   * The number builder's walk-and-encode phase, run under the page guard: collect every fused
   * {@code OBJECT_NAMED_NUMBER} slot's value and tag, and encode them.
   *
   * @return the encoded payload, or {@code null} when the page holds no long-typed values
   */
  private byte[] collectAndEncodeNumberRegion() {
    final MemorySegment sp = slottedPage;
    if (sp == null) {
      return null;
    }
    final long pageKeyBase = recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT;
    long[] valBuf = new long[64];
    int[] nameBuf = new int[64];
    int[] pathBuf = new int[64];
    int count = 0;
    boolean allPathNodeKeysValid = resourceConfig != null && resourceConfig.withPathSummary;
    for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
      long word = PageLayout.getBitmapWord(sp, w);
      final int baseSlot = w << 6;
      while (word != 0) {
        final int bit = Long.numberOfTrailingZeros(word);
        final int slot = baseSlot + bit;
        final int kindId = PageLayout.getDirNodeKindId(sp, slot);
        final long value;
        int parentNameKey = -1;
        int parentPathNodeKeyInt = -1;
        boolean matched = false;
        if (kindId == FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
          value = getFusedObjectNamedNumberValueLongFromSlot(slot);
          if (value != Long.MIN_VALUE) {
            parentNameKey = getFusedObjectNamedNameKeyFromSlot(slot);
            if (allPathNodeKeysValid) {
              final long fusedNodeKey = pageKeyBase + slot;
              final long pnk = getObjectKeyPathNodeKeyFromSlot(slot, fusedNodeKey);
              if (pnk > 0L && pnk <= (long) Integer.MAX_VALUE) {
                parentPathNodeKeyInt = (int) pnk;
              } else {
                allPathNodeKeysValid = false;
              }
            }
            matched = true;
          }
        } else {
          value = Long.MIN_VALUE;
        }
        if (matched) {
          if (count == valBuf.length) {
            final long[] grownV = new long[valBuf.length << 1];
            System.arraycopy(valBuf, 0, grownV, 0, count);
            valBuf = grownV;
            final int[] grownN = new int[nameBuf.length << 1];
            System.arraycopy(nameBuf, 0, grownN, 0, count);
            nameBuf = grownN;
            final int[] grownPath = new int[pathBuf.length << 1];
            System.arraycopy(pathBuf, 0, grownPath, 0, count);
            pathBuf = grownPath;
          }
          valBuf[count] = value;
          nameBuf[count] = parentNameKey;
          pathBuf[count] = parentPathNodeKeyInt;
          count++;
        }
        word &= word - 1;
      }
    }
    if (count == 0) {
      return null;
    }
    final byte tagKind = allPathNodeKeysValid
        ? NumberRegion.TAG_KIND_PATH_NODE
        : NumberRegion.TAG_KIND_NAME;
    final int[] tagBuf = allPathNodeKeysValid
        ? pathBuf
        : nameBuf;
    return NumberRegion.encode(valBuf, tagBuf, count, tagKind);
  }

  /**
   * Rebuild the double column from the slotted page — the reconstruction-path counterpart of the
   * writer's collection in {@code PageKind.buildRegionTable}, selecting exactly what it selects:
   * Double-, Float- and exactly-representable-BigDecimal-typed fused number slots, with the per-field
   * ordinal counted across BOTH numeric types so a later merge can split a liveness bitmap between
   * the columns.
   *
   * @return the payload bytes, or {@code null} when the page holds no such values
   */
  private static final ThreadLocal<double[]> REBUILD_DOUBLE_VALUE_SCRATCH =
      ThreadLocal.withInitial(() -> new double[PageLayout.SLOT_COUNT]);
  /** Exact unscaled value per collected slot; meaningful where the scale is not UNAVAILABLE. */
  private static final ThreadLocal<long[]> REBUILD_DECIMAL_UNSCALED_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);
  /** Exact scale per collected slot, or {@link #DECIMAL_SCALE_UNAVAILABLE} for a plain double. */
  private static final ThreadLocal<int[]> REBUILD_DECIMAL_SCALE_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);
  /** One-element out-parameter for the decimal decoder, so the hot loop allocates nothing. */
  private static final ThreadLocal<int[]> REBUILD_DECIMAL_OUT_SCRATCH = ThreadLocal.withInitial(() -> new int[1]);
  private static final ThreadLocal<int[]> REBUILD_DOUBLE_NAME_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);
  private static final ThreadLocal<int[]> REBUILD_DOUBLE_PATH_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);
  private static final ThreadLocal<int[]> REBUILD_DOUBLE_ORDINAL_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);
  private static final ThreadLocal<Int2IntOpenHashMap> REBUILD_FIELD_ORDINAL_SCRATCH =
      ThreadLocal.withInitial(() -> new Int2IntOpenHashMap(16));

  private byte @Nullable [] tryBuildDoubleRegionFromSlottedPage() {
    final MemorySegment sp = slottedPage;
    if (sp == null) {
      return null;
    }
    final long pageKeyBase = recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT;
    // Per-thread scratch, mirroring the writer's DOUBLE_*_SCRATCH pattern: page reconstruction is
    // a per-page event and SLOT_COUNT bounds everything, so growth-by-doubling here was pure
    // allocation churn.
    final double[] valBuf = REBUILD_DOUBLE_VALUE_SCRATCH.get();
    final long[] decUnscaledBuf = REBUILD_DECIMAL_UNSCALED_SCRATCH.get();
    final int[] decScaleBuf = REBUILD_DECIMAL_SCALE_SCRATCH.get();
    final int[] decOut = REBUILD_DECIMAL_OUT_SCRATCH.get();
    final int[] nameBuf = REBUILD_DOUBLE_NAME_SCRATCH.get();
    final int[] pathBuf = REBUILD_DOUBLE_PATH_SCRATCH.get();
    final int[] ordBuf = REBUILD_DOUBLE_ORDINAL_SCRATCH.get();
    int count = 0;
    boolean allPathNodeKeysValid = resourceConfig != null && resourceConfig.withPathSummary;
    final Int2IntOpenHashMap fieldOrdinal = REBUILD_FIELD_ORDINAL_SCRATCH.get();
    fieldOrdinal.clear();
    fieldOrdinal.defaultReturnValue(0);
    for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
      long word = PageLayout.getBitmapWord(sp, w);
      final int baseSlot = w << 6;
      while (word != 0) {
        final int slot = baseSlot + Long.numberOfTrailingZeros(word);
        word &= word - 1;
        if (PageLayout.getDirNodeKindId(sp, slot) != FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
          continue;
        }
        final int nameKey = getFusedObjectNamedNameKeyFromSlot(slot);
        final int ordinal = DoubleRegion.nextFieldOrdinal(fieldOrdinal, nameKey);
        if (getFusedObjectNamedNumberValueLongFromSlot(slot) != Long.MIN_VALUE) {
          continue; // the long column's value; only the ordinal counter needed to see it
        }
        int decScale = DECIMAL_SCALE_UNAVAILABLE;
        long decUnscaled = 0L;
        final double value;
        if (isFusedObjectNamedNumberDecimalSlot(slot)) {
          // A decimal is carried EXACTLY as its own unscaled integer, whatever its double image
          // happens to be: the column stores it at e = scale, f = 0, so the kernel's integer
          // comparison is a decimal-space comparison and no double ever stands in for the value.
          decUnscaled = getFusedObjectNamedNumberValueDecimalFromSlot(slot, decOut);
          decScale = decOut[0];
          if (decScale == DECIMAL_SCALE_UNAVAILABLE) {
            continue; // a Big* value neither column takes — the oracle will refuse the page
          }
          // Only ever a zone-map bound, and every bound derived from these is widened outward
          // before use, so the ulp this division can cost cannot prune a matching page.
          value = decUnscaled / DoubleRegion.exp10(decScale);
        } else {
          value = getFusedObjectNamedNumberValueDoubleFromSlot(slot);
          if (Double.isNaN(value)) {
            continue; // neither Double/Float nor decimal — the oracle will refuse the page
          }
        }
        int pathNodeKeyInt = -1;
        if (allPathNodeKeysValid) {
          final long pnk = getObjectKeyPathNodeKeyFromSlot(slot, pageKeyBase + slot);
          if (pnk > 0L && pnk <= (long) Integer.MAX_VALUE) {
            pathNodeKeyInt = (int) pnk;
          } else {
            allPathNodeKeysValid = false;
          }
        }
        valBuf[count] = value;
        decUnscaledBuf[count] = decUnscaled;
        decScaleBuf[count] = decScale;
        nameBuf[count] = nameKey;
        pathBuf[count] = pathNodeKeyInt;
        ordBuf[count] = ordinal;
        count++;
      }
    }
    if (count == 0) {
      return null;
    }
    return DoubleRegion.encode(valBuf, decUnscaledBuf, decScaleBuf, allPathNodeKeysValid
        ? pathBuf
        : nameBuf, ordBuf, count,
        allPathNodeKeysValid
            ? NumberRegion.TAG_KIND_PATH_NODE
            : NumberRegion.TAG_KIND_NAME);
  }

  /**
   * Returns the raw number-region payload bytes or {@code null}. Paired with
   * {@link #getNumberRegionHeader()} for scan operators that decode inline.
   */
  public MemorySegment getNumberRegionPayload() {
    return regionPayload(RegionTable.KIND_NUMBER);
  }

  // ============================================================
  // KIND_STRING region — dictionary-encoded string column (STRING_VALUE / OBJECT_NAMED_STRING).
  // ============================================================

  /**
   * Lazily parsed {@link StringRegion.Header} for the page's dictionary-encoded string column.
   * {@code null} if the page has no string records or the region hasn't been built yet.
   */
  private volatile StringRegion.Header cachedStringHeader;

  /**
   * Resolver for tags whose values live in a resource-wide dictionary, or {@code null}.
   *
   * <p>
   * Handed to the page by whoever holds the context, exactly as the FSST symbol table is: the page
   * layer cannot reach a dictionary itself, and giving it a reader would recurse into the NamePage
   * sub-trie the dictionary lives in. Null means every tag keeps its bytes, which is the behaviour
   * that existed before the trie lane.
   * </p>
   *
   * <p>
   * An ACCESSOR, not a value: F1 of the cache review is that a cache may hold what a resolver
   * produces and never the resolver itself, which holds a reader and must not outlive its
   * transaction.
   * </p>
   */
  private volatile @Nullable GlobalStringDictionaries globalStringDictionaries;

  /**
   * True when this page's string region stores at least one tag as global dictionary ids.
   *
   * <p>
   * Written once by deserialization, read on the return of every record-page lookup. Volatile
   * because the writing thread (a cache loader) and the reading thread (whoever the cache hands the
   * page to) are routinely different, and the loader's publication of the page does not by itself
   * order this field for a reader that got the page from a later lookup.
   * </p>
   */
  private volatile boolean hasGlobalStringTags;

  /**
   * This page's global-tag values, already resolved to bytes; {@code null} until a reader resolves.
   *
   * <p>
   * This is the field {@link #globalStringDictionaries} deliberately is NOT. F1 of the cache review
   * says a cache may hold what a resolver produced and never the resolver itself — a resolver holds
   * a transaction's reader, and a page in the buffer manager outlives any transaction. Value
   * re-injection reads THIS and nothing else, so nothing on the expansion path can reach a reader.
   * </p>
   *
   * <p>
   * The distinction between {@code null} and {@link ResolvedGlobalStrings#NONE} is load-bearing:
   * {@code null} means "no reader has resolved this page", which for a page with global tags is a
   * wiring failure and must throw, while {@code NONE} means "resolved, and there was nothing to
   * resolve". Collapsing the two would turn a missing install into a page whose values are silently
   * absent.
   * </p>
   */
  private volatile @Nullable ResolvedGlobalStrings resolvedGlobalStrings;

  /**
   * Drop the cached string-region parsed header and payload so the next reader rebuilds. Called from
   * every mutation path that adds, modifies, or removes a STRING_VALUE / OBJECT_NAMED_STRING record.
   */
  void invalidateStringRegion() {
    final RegionTable rt = regionTable;
    // Same reasoning as invalidateNumberRegion: presence is asked of the table, because a caller
    // can reach the payload without ever parsing a header, and the old guard inferred it from the
    // header alone.
    final boolean present =
        rt != null && (rt.hasRegion(RegionTable.KIND_STRING) || rt.hasRegion(RegionTable.KIND_STRING_DICT_SKETCH));
    if (cachedStringHeader == null && !present && (regionDeriveAttempted & STRING_DERIVE_MASK) == 0) {
      return;
    }
    cachedStringHeader = null;
    // See invalidateNumberRegion: the derive memo must fall with the region.
    clearRegionDeriveAttempted(STRING_DERIVE_MASK);
    if (rt != null) {
      rt.set(RegionTable.KIND_STRING, null);
      // The sketch summarises the dictionary just dropped. A stale Bloom filter is not
      // conservatively wrong: a newly written string is absent from it, mayContain answers false,
      // and the scan rules the page out — rows disappear from the count.
      rt.set(RegionTable.KIND_STRING_DICT_SKETCH, null);
    }
  }

  /**
   * Drop the boolean column so the next reader rebuilds it. Its drop-set is exactly
   * {@code BOOL_DERIVE_MASK} — {@link RegionTable#KIND_BOOLEAN}, the one kind
   * {@link #tryBuildBooleanRegionFromSlottedPage} installs.
   *
   * <p>
   * Cheap for the same reason as its number and string twins: a page with no boolean column installed
   * and nothing memoised pays one field read plus a branch, which is the steady state of the write
   * path, where the modify page is built fresh and carries no region table at all.
   */
  void invalidateBooleanRegion() {
    final RegionTable rt = regionTable;
    // Presence is asked of the table, not of a cached header: this column has no parsed-header
    // cache at all, so the table is the only place a stale payload can hide.
    final boolean present = rt != null && rt.hasRegion(RegionTable.KIND_BOOLEAN);
    if (!present && (regionDeriveAttempted & BOOL_DERIVE_MASK) == 0) {
      return;
    }
    // The derive memo must fall with the region, or the next reader reads "derived and refused"
    // and skips the rebuild forever.
    clearRegionDeriveAttempted(BOOL_DERIVE_MASK);
    if (rt != null) {
      rt.set(RegionTable.KIND_BOOLEAN, null);
    }
  }

  /**
   * Drop the field-name column and the record linkage indexed by it — exactly
   * {@code NAMES_DERIVE_MASK}, the pair {@link #tryBuildObjectKeyNameKeyRegionFromSlottedPage}
   * installs in one walk.
   *
   * <p>
   * The two must fall TOGETHER. {@link RegionTable#KIND_RECORD_ORDINAL} numbers records by position
   * in the name column collected in the same pass, so a linkage that outlives the names it indexes
   * still aligns — against a column that no longer exists. A fused cross-column predicate takes that
   * alignment as a certificate, so the failure mode is a wrong answer, not a stale bound. Dropping
   * only one is worse than dropping neither.
   */
  void invalidateNamesRegion() {
    final RegionTable rt = regionTable;
    final boolean present = rt != null
        && (rt.hasRegion(RegionTable.KIND_OBJECT_KEY_NAMEKEY) || rt.hasRegion(RegionTable.KIND_RECORD_ORDINAL));
    if (!present && (regionDeriveAttempted & NAMES_DERIVE_MASK) == 0) {
      return;
    }
    clearRegionDeriveAttempted(NAMES_DERIVE_MASK);
    if (rt != null) {
      rt.set(RegionTable.KIND_OBJECT_KEY_NAMEKEY, null);
      rt.set(RegionTable.KIND_RECORD_ORDINAL, null);
    }
  }

  /**
   * Thread-local scratch for {@link #readFusedObjectNamedStringBytes} FSST decode. One array per
   * worker thread, reused for every value read during a region build. 1 KiB matches typical string
   * max length on JSON-like workloads; grows on first oversize.
   */
  private static final ThreadLocal<byte[]> STRING_REGION_BUILD_SCRATCH = ThreadLocal.withInitial(() -> new byte[1024]);

  /**
   * Walk the slotted page, collect each fused OBJECT_NAMED_STRING slot's value + its parent
   * OBJECT_KEY's nameKey, encode into a StringRegion payload.
   *
   * <p>
   * Returns {@code null} when the page has no string values or no slotted page yet. Side-effect: on
   * success, attaches the region to the page so subsequent lookups skip this build.
   */
  private MemorySegment tryBuildStringRegionFromSlottedPage() {
    if ((regionDeriveAttempted & STRING_DERIVE_MASK) != 0) {
      return regionPayload(RegionTable.KIND_STRING);
    }
    final byte[] encoded;
    if (!acquireGuard()) {
      return null; // mid-close — nothing to derive from
    }
    try {
      if (slottedPage == null) {
        return null; // deliberately unmemoized: there was nothing to walk
      }
      encoded = collectAndEncodeStringRegion();
    } finally {
      releaseGuard();
    }
    if (encoded == STRING_BUILD_RETRY) {
      return null; // undecodable FSST slot — retryable, deliberately unmemoized; see the walk
    }
    synchronized (this) {
      if (isClosed()) {
        return null;
      }
      if ((regionDeriveAttempted & STRING_DERIVE_MASK) != 0) {
        return regionPayload(RegionTable.KIND_STRING); // a racing walk installed first
      }
      if (encoded == null) {
        regionDeriveAttempted |= STRING_DERIVE_MASK;
        return null; // no strings on the page — a permanent refusal
      }
      final RegionTable table = regionTableForInstall();
      table.set(RegionTable.KIND_STRING, encoded);
      final MemorySegment installed = table.payload(RegionTable.KIND_STRING);
      // The writer emits a dictionary sketch alongside every string region; a region rebuilt here
      // must carry one too, or a page that went through versioning reconstruction would silently
      // lose the ability to rule itself out of a string equality — correct answers, but every such
      // page paying a dictionary decode forever after. The header is read from the installed
      // segment; the entries are hashed from the array, which is what the sketch builder takes.
      //
      // A suppressed tag forfeits the sketch, exactly as it does on the writer: a sketch negative is
      // exact and PAGE-wide, and the suppressed tag's strings are on the page but not in this
      // dictionary.
      final StringRegion.Header installedHeader = new StringRegion.Header().parseInto(installed);
      if (installedHeader.suppressedTagCount == 0) {
        table.set(RegionTable.KIND_STRING_DICT_SKETCH,
            StringDictSketch.encodeFromStringRegion(encoded, installedHeader));
      }
      regionDeriveAttempted |= STRING_DERIVE_MASK;
      return installed;
    }
  }

  /**
   * Sentinel for {@link #collectAndEncodeStringRegion}: the walk hit a slot it cannot decode, so
   * nothing may be installed AND nothing may be memoized — the refusal is retryable once the FSST
   * symbol table is resolved.
   */
  private static final byte[] STRING_BUILD_RETRY = new byte[0];

  /**
   * The string builder's walk-and-encode phase, run under the page guard.
   *
   * @return the encoded payload; {@code null} when the page holds no strings (a permanent refusal);
   *         {@link #STRING_BUILD_RETRY} when a slot could not be decoded (retryable)
   */
  /**
   * This slot's enclosing object's path node key as an int, or {@code -1} when there is none that
   * fits — which drops the whole page's path-tagged encoder, since a partial tagging would key some
   * values by path and others by name.
   */
  private int pathNodeKeyIntForSlot(final int slot, final long pageKeyBase) {
    final long pnk = getObjectKeyPathNodeKeyFromSlot(slot, pageKeyBase + slot);
    return pnk > 0L && pnk <= (long) Integer.MAX_VALUE
        ? (int) pnk
        : -1;
  }

  private byte[] collectAndEncodeStringRegion() {
    final MemorySegment sp = slottedPage;
    if (sp == null) {
      return STRING_BUILD_RETRY;
    }
    final long pageKeyBase = recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT;
    final boolean withPathSummary = resourceConfig != null && resourceConfig.withPathSummary;
    // Build two encoders in parallel so the final tagKind decision is a pure
    // pick — avoids a second pass. Path-tagged encoder only gets populated
    // while {@code allPathNodeKeysValid} holds.
    final StringRegion.Encoder nameEnc = new StringRegion.Encoder();
    final StringRegion.Encoder pathEnc = withPathSummary
        ? new StringRegion.Encoder()
        : null;
    if (pathEnc != null) {
      // The trie lane rides the PATH-tagged encoder alone: the projection's dictionary anchors are
      // keyed by path node key, so a name-tagged region has nothing to look them up with.
      pathEnc.setDictionaries(globalStringDictionaries);
    }
    boolean allPathNodeKeysValid = withPathSummary;
    int count = 0;
    // Array-element values are staged rather than added straight through: they are published only
    // if EVERY element on the page resolved its enclosing array, so the tag they land under is
    // always the complete set of that path's values on this page.
    int elementCount = 0;
    int orphanCount = 0;
    boolean elementsUsable = ARRAY_ELEMENT_STRINGS_IN_REGION;
    int[] elementNameKeys = new int[16];
    int[] elementPathKeys = new int[16];
    byte[][] elementValues = new byte[16][];
    for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
      long word = PageLayout.getBitmapWord(sp, w);
      final int baseSlot = w << 6;
      while (word != 0) {
        final int bit = Long.numberOfTrailingZeros(word);
        final int slot = baseSlot + bit;
        word &= word - 1;
        final int kindId = PageLayout.getDirNodeKindId(sp, slot);
        final byte[] value;
        int parentNameKey = -1;
        int parentPathNodeKeyInt = -1;
        if (elementsUsable && isElementPurityKindId(kindId) && !elementStagingStaysPure(slot, kindId, pageKeyBase)) {
          elementsUsable = false; // a non-string element the certificate cannot model: no staging
        }
        if (kindId == STRING_VALUE_KIND_ID) {
          // An ARRAY ELEMENT. It carries no path node key of its own — measured on the movie
          // corpus, every one reads back as pathNodeKey -1 — which is exactly why the column
          // never held them: a path-tagged region has nothing to tag them by. Its enclosing
          // array DOES have one (the fused OBJECT_NAMED_ARRAY slot), and that is the right tag:
          // it is the path a query names when it writes `$m.genres[]`.
          if (!elementsUsable) {
            continue;
          }
          final byte[] elementValue = readStringValueBytes(slot);
          if (elementValue == null) {
            // An undecodable value makes the element set for this page incomplete, and a tag that
            // covers most of its values is worse than absent: every reader here treats tagCount as
            // the complete count of that path's values on the page. Drop the element contribution
            // for the whole page rather than publish a partial one.
            elementsUsable = false;
            continue;
          }
          final int parentSlot = onPageParentSlot(slot, pageKeyBase);
          if (parentSlot < 0) {
            // An element whose array opens on the PREVIOUS page. Slot order is node-key order, so
            // those form a LEADING RUN at the head of the page and nowhere else — kept under the
            // reserved orphan tag, exactly as PageKind.buildRegionTable keeps them, so the page
            // holding the array can settle its own last record.
            if (elementCount > orphanCount) {
              elementsUsable = false;
              continue;
            }
                    elementNameKeys = grow(elementNameKeys, elementCount);
            elementPathKeys = grow(elementPathKeys, elementCount);
            elementValues = grow(elementValues, elementCount);
            elementNameKeys[elementCount] = StringRegion.TAG_ORPHAN_ELEMENTS;
            elementPathKeys[elementCount] = StringRegion.TAG_ORPHAN_ELEMENTS;
            elementValues[elementCount] = elementValue;
            elementCount++;
            orphanCount++;
            continue;
          }
          // The enclosing array is a STRUCTURAL fused record (12 fields, NAME_KEY at index 5) or a
          // fused OBJECT_NAMED record (NAME_KEY at index 3); a non-fused parent carries no nameKey
          // in either layout, so decoding one would read a sibling-key field.
          final int parentKind = PageLayout.getDirNodeKindId(sp, parentSlot);
          if (!isFusedStructuralKindId(parentKind) && !isFusedObjectNamedKindId(parentKind)) {
            elementsUsable = false;
            continue;
          }
          elementNameKeys = grow(elementNameKeys, elementCount);
          elementPathKeys = grow(elementPathKeys, elementCount);
          elementValues = grow(elementValues, elementCount);
          elementNameKeys[elementCount] = isFusedStructuralKindId(parentKind)
              ? getFusedStructuralNameKeyFromSlot(parentSlot)
              : getFusedObjectNamedNameKeyFromSlot(parentSlot);
          elementPathKeys[elementCount] = pathNodeKeyIntForSlot(parentSlot, pageKeyBase);
          elementValues[elementCount] = elementValue;
          elementCount++;
          continue;
        }
        if (kindId == FUSED_OBJECT_NAMED_STRING_KIND_ID) {
          if (isFusedObjectNamedStringOverflowDescriptor(slot)) {
            // The field metadata is complete, but its value is deliberately out of line. A partial
            // TAG is never safe — every reader takes tagCount as the complete count of that tag's
            // values on the page — so the tag leaves the region and its slots keep their values in
            // the heap. Every other field on the page still gets its column. Under the kill switch
            // the page is memoized as not derivable, exactly as before.
            if (!PageKind.STRING_REGION_PER_TAG_COMPLETENESS) {
              return null;
            }
            nameEnc.suppressTag(getFusedObjectNamedNameKeyFromSlot(slot));
            if (pathEnc != null && allPathNodeKeysValid) {
              final int suppressedPathNodeKey = pathNodeKeyIntForSlot(slot, pageKeyBase);
              allPathNodeKeysValid = suppressedPathNodeKey >= 0;
              if (allPathNodeKeysValid) {
                pathEnc.suppressTag(suppressedPathNodeKey);
              }
            }
            continue;
          }
          value = readFusedObjectNamedStringBytes(slot);
          if (value == null) {
            // A string slot the page cannot decode — an FSST-compressed value whose symbol table
            // is not resolved on this instance. Skipping it would install a column and a sketch
            // that are right for most slots, and a sketch negative is treated as EXACT: the page
            // would silently count 0 for literals it actually holds. No region at all is the only
            // safe answer, and the refusal stays unmemoized — the region-only reader resolves the
            // symbol table first and the next build succeeds.
            return STRING_BUILD_RETRY;
          }
          parentNameKey = getFusedObjectNamedNameKeyFromSlot(slot);
          if (allPathNodeKeysValid) {
            parentPathNodeKeyInt = pathNodeKeyIntForSlot(slot, pageKeyBase);
            allPathNodeKeysValid = parentPathNodeKeyInt >= 0;
          }
        } else {
          continue;
        }
        nameEnc.addValue(parentNameKey, value);
        if (pathEnc != null && allPathNodeKeysValid) {
          pathEnc.addValue(parentPathNodeKeyInt, value);
        }
        count++;
      }
    }
    // Array elements go in only when EVERY one of them resolved; see the branch above. The orphan
    // tag is deliberately negative and is NOT an unresolved path — see PageKind.buildRegionTable.
    for (int i = 0; elementsUsable && i < elementCount; i++) {
      if (elementPathKeys[i] < 0 && elementPathKeys[i] != StringRegion.TAG_ORPHAN_ELEMENTS) {
        allPathNodeKeysValid = false;
      }
    }
    for (int i = 0; elementsUsable && i < elementCount; i++) {
      nameEnc.addValue(elementNameKeys[i], elementValues[i]);
      if (pathEnc != null && allPathNodeKeysValid) {
        pathEnc.addValue(elementPathKeys[i], elementValues[i]);
      }
      count++;
    }
    if (count == 0) {
      return null;
    }
    final byte[] encoded = allPathNodeKeysValid && pathEnc != null
        ? pathEnc.finish(StringRegion.TAG_KIND_PATH_NODE, elementsUsable)
        : nameEnc.finish(StringRegion.TAG_KIND_NAME, elementsUsable);
    // Every value on the page belonged to a suppressed tag: the encoder produced no payload, which
    // is a page with no derivable region rather than a zero-length one.
    return encoded.length == 0
        ? null
        : encoded;
  }

  /**
   * Whether array-element strings join the PAX string column.
   *
   * <p>
   * Not final: the value is read once at class initialization, and a test that wants the other
   * setting cannot arrange to load the class after setting the property. Tests flip it directly.
   *
   * <p>
   * Public because the column it fills is read from {@code sirix-query} — the array-membership column
   * route is the only reason it exists — so the differential test that holds that route to the record
   * route's answers has to write it from another module.
   *
   * <p>
   * Off by default because it changes what a page WRITES: a resource built with it carries tags a
   * resource built without it does not, so the two are not byte-comparable, and every benchmark
   * corpus has to be re-ingested to get the new columns. Readers are unaffected either way — an
   * absent tag is a state they all already handle.
   */
  public static boolean ARRAY_ELEMENT_STRINGS_IN_REGION = Boolean.getBoolean("sirix.page.arrayElementStrings");
  /**
   * Mutation seam for the element-staging purity rule ({@link #elementStagingStaysPure}). Tests
   * only; {@code false} restores the refuted writer behaviour.
   */
  public static volatile boolean ELEMENT_STAGING_PURITY = true;

  /** This slot's parent slot when the parent lives on THIS page, else {@code -1}. */
  /** Bare slot kinds the element-staging purity rule inspects: OBJECT and ARRAY. */
  static boolean isElementPurityKindId(final int kindId) {
    return kindId == NodeKind.OBJECT.getId() || kindId == NodeKind.ARRAY.getId();
  }

  /**
   * Whether staging this page's array-element strings stays representable after seeing a bare
   * OBJECT or ARRAY slot — the array-membership column route's purity rule, applied identically by
   * the writer ({@code PageKind.buildRegionTable}) and by the derive path here.
   *
   * <p>
   * A bare OBJECT or ARRAY whose parent is an array is an ELEMENT the record-ordinal certificate
   * cannot model: its own fields open a new ordinal and its own elements carry a different tag, so
   * the page's WHOLE element set is refused and the reader declines the page to the records
   * (per-array refusal would make that array's strings invisible outside its truncated gap —
   * unsound). With an on-page parent the parent's kind decides; with an off-page parent only the
   * top-level container (document root or the first node) is known not to be an array. Scalars are
   * never inspected: inside a gap they over-count into a decline, and an all-scalar orphan run
   * leaves the page without a string column, which the orphan lookup already treats as undecidable.
   * </p>
   */
  boolean elementStagingStaysPure(final int slot, final int kindId, final long pageKeyBase) {
    if (!ELEMENT_STAGING_PURITY) {
      return true;
    }
    final long parentKey = getSlotParentKey(slot);
    if (parentKey <= 1L) {
      return true; // no parent, the document root, or the top-level container
    }
    final long parentSlot = parentKey - pageKeyBase;
    if (parentSlot >= 0L && parentSlot < Constants.NDP_NODE_COUNT) {
      final int parentKind = PageLayout.getDirNodeKindId(slottedPage, (int) parentSlot);
      return parentKind != FUSED_OBJECT_NAMED_ARRAY_KIND_ID && parentKind != NodeKind.ARRAY.getId();
    }
    return false; // spilled from the previous page under a parent this page cannot classify
  }

  private int onPageParentSlot(final int slot, final long pageKeyBase) {
    final long parentKey = getSlotParentKey(slot);
    if (parentKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return -1;
    }
    final long parentSlot = parentKey - pageKeyBase;
    return parentSlot >= 0 && parentSlot < Constants.NDP_NODE_COUNT
        ? (int) parentSlot
        : -1;
  }

  private static int[] grow(final int[] buf, final int used) {
    if (used < buf.length) {
      return buf;
    }
    final int[] grown = new int[Math.max(16, buf.length << 1)];
    System.arraycopy(buf, 0, grown, 0, used);
    return grown;
  }

  private static byte[][] grow(final byte[][] buf, final int used) {
    if (used < buf.length) {
      return buf;
    }
    final byte[][] grown = new byte[Math.max(16, buf.length << 1)][];
    System.arraycopy(buf, 0, grown, 0, used);
    return grown;
  }

  /**
   * Rebuild the field-name column from the slotted page. The counterpart of
   * {@link #tryBuildNumberRegionFromSlottedPage} for {@link RegionTable#KIND_OBJECT_KEY_NAMEKEY}, and
   * it must select exactly what the writer selects — the fused primitive OBJECT_NAMED_* kinds —
   * because the scan's completeness oracle compares this column's slot count against a value region's
   * tag count.
   *
   * @return the payload, or {@code null} when the page has no such slots or the dictionary exceeds
   *         what the region can encode
   */
  private byte[] tryBuildObjectKeyNameKeyRegionFromSlottedPage() {
    if ((regionDeriveAttempted & NAMES_DERIVE_MASK) != 0) {
      return null; // completed once already; the installed payloads, if any, are in the table
    }
    final byte[][] built; // {names payload, ordinals payload}, either possibly null
    if (!acquireGuard()) {
      return null; // mid-close — nothing to derive from
    }
    try {
      if (slottedPage == null) {
        return null; // deliberately unmemoized: there was nothing to walk
      }
      built = collectAndEncodeNameKeyRegion();
    } finally {
      releaseGuard();
    }
    synchronized (this) {
      if (isClosed()) {
        return null;
      }
      if ((regionDeriveAttempted & NAMES_DERIVE_MASK) != 0) {
        return null; // a racing walk installed first
      }
      final byte[] payload = built[0];
      if (payload != null) {
        final RegionTable table = regionTableForInstall();
        table.set(RegionTable.KIND_OBJECT_KEY_NAMEKEY, payload);
        // A reconstructed page's records are complete, so its linkage is derivable exactly as the
        // writer derives it. Emitting it here is what lets a merged multi-fragment page serve a
        // two-field predicate instead of dropping to records the moment any page was ever updated.
        if (built[1] != null) {
          table.set(RegionTable.KIND_RECORD_ORDINAL, built[1]);
        }
      }
      regionDeriveAttempted |= NAMES_DERIVE_MASK;
      return payload;
    }
  }

  /**
   * The field-name builder's walk-and-encode phase, run under the page guard.
   *
   * @return {@code {namesPayload, ordinalsPayload}} — the first {@code null} when the page has no
   *         named slots or more distinct names than the region encodes, the second {@code null}
   *         whenever {@link RecordOrdinalRegion#encode} refuses the linkage
   */
  private byte[][] collectAndEncodeNameKeyRegion() {
    final MemorySegment sp = slottedPage;
    if (sp == null) {
      return new byte[2][];
    }
    int[] nameKeys = new int[64];
    int[] slots = new int[64];
    // Enclosing object per slot, for the record-linkage column. Collected in the same pass and the
    // same order as the names, because RecordOrdinalRegion's ordinals are indexed by position in
    // this column — a second pass could see a different slot order and link values to the wrong
    // records with nothing to signal it. Raw node keys, deliberately unclassified: deciding what
    // is an on-page parent, the spanning record's tail, or a shape that refuses the region is
    // RecordOrdinalRegion.encode's contract, kept in that one place.
    long[] parentKeys = new long[64];
    int count = 0;
    final long pageKeyBase = recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT;
    for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
      long word = PageLayout.getBitmapWord(sp, w);
      final int baseSlot = w << 6;
      while (word != 0) {
        final int slot = baseSlot + Long.numberOfTrailingZeros(word);
        word &= word - 1;
        // The ROLE predicate, not the LAYOUT one. isFusedObjectNamedKindId names the PRIMITIVE
        // fused layout (9 fields, NAME_KEY at index 3); the structural kinds OBJECT/ARRAY are a
        // 12-field layout with NAME_KEY at index 5, so widening that predicate would decode them
        // with the wrong field table. What this column wants is every slot that CARRIES A FIELD
        // NAME, which is what isFusedAnyObjectNamedKindId says — and the name is then read through
        // the accessor that dispatches on layout.
        //
        // Leaving array- and object-valued fields out made them invisible to every anchored scan:
        // `getObjectKeySlotsForNameKey("genres")` answered EMPTY, so a predicate over an array
        // visited no records at all. It also kept them out of the record-ordinal linkage built in
        // this same pass, which is what an element-to-record attribution needs.
        final int slotKind = PageLayout.getDirNodeKindId(sp, slot);
        if (!isFusedAnyObjectNamedKindId(slotKind)) {
          continue;
        }
        if (count == nameKeys.length) {
          nameKeys = Arrays.copyOf(nameKeys, count << 1);
          slots = Arrays.copyOf(slots, count << 1);
          parentKeys = Arrays.copyOf(parentKeys, count << 1);
        }
        // Not getObjectKeyNameKeyFromSlot: that one consults the very region this pass is
        // building, and would answer from a stale payload (or none).
        nameKeys[count] = isFusedStructuralKindId(slotKind)
            ? getFusedStructuralNameKeyFromSlot(slot)
            : getFusedObjectNamedNameKeyFromSlot(slot);
        slots[count] = slot;
        parentKeys[count] = getObjectKeyParentKeyFromSlot(slot, pageKeyBase + slot);
        count++;
      }
    }
    if (count == 0) {
      return new byte[2][];
    }
    final byte[] payload = ObjectKeyNameKeyRegion.encode(nameKeys, slots, count);
    if (payload == null) {
      return new byte[2][]; // more distinct names than the region encodes; callers walk slots
    }
    return new byte[][] {payload, RecordOrdinalRegion.encode(parentKeys, pageKeyBase, count)};
  }

  /**
   * Restore the column regions of a page assembled from versioning fragments.
   *
   * <p>
   * A reconstructed page is built slot by slot from several fragments, so it starts with no columns
   * of its own even though its records are complete. Rebuilding them here keeps the invariant every
   * other page in the cache satisfies — that a materialized page carries its columns — and is what
   * lets a later column scan serve it instead of walking its records.
   *
   * <p>
   * Only the two cheap ones are eager: the field-name column (which every column scan probes first)
   * and the numeric column. String and boolean columns rebuild on demand, and now bring their sketch
   * with them.
   */
  public void ensureColumnRegions() {
    if (overflowSlotSidecar != null) {
      dropColumnRegionsForSideSlots();
      return;
    }
    // One definition of "derive what is missing": the mask dispatcher below. Keeping a second
    // hand-rolled builder dispatch here is how the string getter-vs-builder bug survived — two
    // entry points drifting on which builder derives which kind.
    ensureRegionsFor(NAMES_DERIVE_MASK | NUMBER_DERIVE_MASK);
  }

  /** Kinds the field-name builder derives together: the names column and the record linkage. */
  private static final int NAMES_DERIVE_MASK =
      RegionTable.maskOf(RegionTable.KIND_OBJECT_KEY_NAMEKEY) | RegionTable.maskOf(RegionTable.KIND_RECORD_ORDINAL);
  /** Kinds the number builder derives together: the long column, its zone map, and the doubles. */
  private static final int NUMBER_DERIVE_MASK = RegionTable.maskOf(RegionTable.KIND_NUMBER)
      | RegionTable.maskOf(RegionTable.KIND_NUMBER_ZONEMAP) | RegionTable.maskOf(RegionTable.KIND_DOUBLE);
  /** Kind the boolean builder derives. */
  private static final int BOOL_DERIVE_MASK = RegionTable.maskOf(RegionTable.KIND_BOOLEAN);
  /** Kinds the string builder derives together: the dictionary column and its sketch. */
  private static final int STRING_DERIVE_MASK =
      RegionTable.maskOf(RegionTable.KIND_STRING) | RegionTable.maskOf(RegionTable.KIND_STRING_DICT_SKETCH);

  /**
   * Derive every region {@code kindMask} asks for that the table does not yet hold, from the slotted
   * page.
   *
   * <p>
   * This is what makes a RECONSTRUCTED page a first-class column source. The versioning layer merges
   * a multi-fragment page into one slotted page — every slot in one coordinate space, which is
   * precisely the alignment a cross-column (fused) predicate needs and the per-fragment merge cannot
   * provide — but the merged page starts with an EMPTY region table that only grows when a lazy
   * getter happens to run. A region-only read that found the resident page missing a requested kind
   * used to fall through to "no columns" even though every one of them was sitting derivable in the
   * slots; this call closes that gap on demand, per kind: the field-name column brings
   * {@link RegionTable#KIND_RECORD_ORDINAL} with it, the number rebuild installs zone maps and the
   * double column alongside, and string brings its sketch.
   *
   * <p>
   * Lock-free in the steady state: after the first read of a page, everything requested is either
   * present or recorded as attempted in {@link #regionDeriveAttempted}, and the method returns on two
   * volatile reads without touching any lock — N scan workers re-reading a hot resident page must not
   * serialize on it. When something IS missing, the builders themselves do the pinning: each runs its
   * walk under the page guard (the frame must not be recycled mid-walk) and takes the page monitor
   * only for its install, so two workers racing the precheck cost one redundant walk, never a torn
   * table — and record readers' guard traffic never queues behind a derivation.
   */
  public void ensureRegionsFor(final int kindMask) {
    if (overflowSlotSidecar != null) {
      dropColumnRegionsForSideSlots();
      return;
    }
    final int pending = pendingDerivations(kindMask);
    if (pending == 0) {
      return;
    }
    if ((pending & NAMES_DERIVE_MASK) != 0) {
      tryBuildObjectKeyNameKeyRegionFromSlottedPage();
    }
    if ((pending & NUMBER_DERIVE_MASK) != 0) {
      tryBuildNumberRegionFromSlottedPage();
    }
    if ((pending & BOOL_DERIVE_MASK) != 0) {
      tryBuildBooleanRegionFromSlottedPage();
    }
    if ((pending & STRING_DERIVE_MASK) != 0) {
      tryBuildStringRegionFromSlottedPage();
    }
  }

  /**
   * The subset of {@code kindMask} whose builders still have something to do — zero in the steady
   * state, decided from two volatile reads and a few payload probes with no lock.
   */
  private int pendingDerivations(final int kindMask) {
    if (slottedPage == null || overflowSlotSidecar != null) {
      return 0;
    }
    final int attempted = regionDeriveAttempted;
    int pending = 0;
    if (namesDerivationPending(kindMask, attempted)) {
      pending |= NAMES_DERIVE_MASK;
    }
    // Checked on BOTH columns: an all-double page installs KIND_DOUBLE with no KIND_NUMBER at
    // all, and re-walking its slots on every ensure would penalize exactly the page shape the
    // rebuild was fixed for.
    if ((kindMask & NUMBER_DERIVE_MASK) != 0 && (attempted & NUMBER_DERIVE_MASK) == 0
        && regionPayload(RegionTable.KIND_NUMBER) == null && regionPayload(RegionTable.KIND_DOUBLE) == null) {
      pending |= NUMBER_DERIVE_MASK;
    }
    if ((kindMask & BOOL_DERIVE_MASK) != 0 && (attempted & BOOL_DERIVE_MASK) == 0
        && regionPayload(RegionTable.KIND_BOOLEAN) == null) {
      pending |= BOOL_DERIVE_MASK;
    }
    if ((kindMask & STRING_DERIVE_MASK) != 0 && (attempted & STRING_DERIVE_MASK) == 0
        && regionPayload(RegionTable.KIND_STRING) == null) {
      pending |= STRING_DERIVE_MASK;
    }
    return pending;
  }

  /**
   * Names are pending not only when the column is missing but also when the record-ordinal linkage
   * is: several older paths installed the names WITHOUT the ordinals, and a fused predicate's
   * alignment certificate is exactly the ordinals. The builder re-derives both.
   */
  private boolean namesDerivationPending(final int kindMask, final int attempted) {
    if ((kindMask & NAMES_DERIVE_MASK) == 0 || (attempted & NAMES_DERIVE_MASK) != 0) {
      return false;
    }
    if (regionPayload(RegionTable.KIND_OBJECT_KEY_NAMEKEY) == null) {
      return true;
    }
    return (kindMask & RegionTable.maskOf(RegionTable.KIND_RECORD_ORDINAL)) != 0
        && regionPayload(RegionTable.KIND_RECORD_ORDINAL) == null;
  }

  /**
   * Region kinds whose builder has RUN TO COMPLETION on this page, successful or refused.
   *
   * <p>
   * A completed refusal is permanent — the slots never change under a resident page — so the memo is
   * what stops a page whose ordinals legitimately cannot encode, or that simply holds no booleans,
   * from re-walking all of its slots and re-installing identical payloads into the table's arena on
   * EVERY region-only read for as long as it stays cached.
   *
   * <p>
   * Three deliberate properties. Bits are set by the builders themselves, in their install sections —
   * never before the walk, so an exception does not latch a transient failure into a permanent
   * refusal — and under the page monitor those sections hold, so all entry points share the memo.
   * Bits are CLEARED wherever the corresponding payloads are dropped or replaced
   * ({@link #invalidateNumberRegion}, {@link #invalidateStringRegion}, {@link #setRegionTable},
   * {@link #reset}), because a memo that outlives its table reads as "derived and refused" for
   * regions that were merely thrown away. And the field is volatile: the lock-free precheck above
   * reads it without the monitor, and the volatile read of a bit a builder set after installing its
   * payloads is the happens-before edge that makes those payloads safely readable.
   */
  private volatile int regionDeriveAttempted;

  public StringRegion.Header getStringRegionHeader() {
    if (overflowSlotSidecar != null) {
      return null;
    }
    StringRegion.Header h = cachedStringHeader;
    if (h != null) {
      return h;
    }
    MemorySegment payload = regionPayload(RegionTable.KIND_STRING);
    if (payload == null) {
      payload = tryBuildStringRegionFromSlottedPage();
      if (payload == null) {
        return null;
      }
    }
    synchronized (this) {
      h = cachedStringHeader;
      if (h == null) {
        h = new StringRegion.Header().parseInto(payload);
        cachedStringHeader = h;
      }
    }
    return h;
  }

  /** Raw string-region payload bytes, or {@code null}. */
  public MemorySegment getStringRegionPayload() {
    return regionPayload(RegionTable.KIND_STRING);
  }

  /**
   * Build (and cache) the page's BooleanRegion by walking all populated fused
   * {@code OBJECT_NAMED_BOOLEAN} slots and collecting each slot's inline value + its inline name/path
   * tag. Returns {@code null} when the page has no booleans. See
   * {@link #tryBuildNumberRegionFromSlottedPage} for the template — this method mirrors it for the
   * boolean column.
   */
  private MemorySegment tryBuildBooleanRegionFromSlottedPage() {
    if ((regionDeriveAttempted & BOOL_DERIVE_MASK) != 0) {
      return regionPayload(RegionTable.KIND_BOOLEAN);
    }
    final byte[] encoded;
    if (!acquireGuard()) {
      return null; // mid-close — nothing to derive from
    }
    try {
      if (slottedPage == null) {
        return null; // deliberately unmemoized: there was nothing to walk
      }
      encoded = collectAndEncodeBooleanRegion();
    } finally {
      releaseGuard();
    }
    synchronized (this) {
      if (isClosed()) {
        return null;
      }
      if ((regionDeriveAttempted & BOOL_DERIVE_MASK) != 0) {
        return regionPayload(RegionTable.KIND_BOOLEAN); // a racing walk installed first
      }
      if (encoded == null) {
        regionDeriveAttempted |= BOOL_DERIVE_MASK;
        return null; // no booleans, or a dictionary the region cannot encode — permanent
      }
      final RegionTable table = regionTableForInstall();
      table.set(RegionTable.KIND_BOOLEAN, encoded);
      regionDeriveAttempted |= BOOL_DERIVE_MASK;
      return table.payload(RegionTable.KIND_BOOLEAN);
    }
  }

  /**
   * The boolean builder's walk-and-encode phase, run under the page guard.
   *
   * @return the encoded payload, or {@code null} when the page holds no booleans or the tag
   *         dictionary overflows what the region encodes
   */
  private byte[] collectAndEncodeBooleanRegion() {
    final MemorySegment sp = slottedPage;
    if (sp == null) {
      return null;
    }
    final long pageKeyBase = recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT;
    boolean[] valBuf = new boolean[64];
    int[] nameBuf = new int[64];
    int[] pathBuf = new int[64];
    int count = 0;
    boolean allPathNodeKeysValid = resourceConfig != null && resourceConfig.withPathSummary;
    for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
      long word = PageLayout.getBitmapWord(sp, w);
      final int baseSlot = w << 6;
      while (word != 0) {
        final int bit = Long.numberOfTrailingZeros(word);
        final int slot = baseSlot + bit;
        final int kindId = PageLayout.getDirNodeKindId(sp, slot);
        final boolean value;
        int parentNameKey = -1;
        int parentPathNodeKeyInt = -1;
        boolean matched = false;
        if (kindId == FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID) {
          // Fused OBJECT_NAMED_BOOLEAN: the slot is both the OBJECT_KEY (name tag) and the
          // boolean leaf. No parent indirection — name/path come from the same slot.
          value = getFusedObjectNamedBooleanValueFromSlot(slot);
          parentNameKey = getFusedObjectNamedNameKeyFromSlot(slot);
          if (allPathNodeKeysValid) {
            parentPathNodeKeyInt = pathNodeKeyIntForSlot(slot, pageKeyBase);
            allPathNodeKeysValid = parentPathNodeKeyInt >= 0;
          }
          matched = true;
        } else {
          value = false;
        }
        if (matched) {
          if (count == valBuf.length) {
            final boolean[] grownV = new boolean[valBuf.length << 1];
            System.arraycopy(valBuf, 0, grownV, 0, count);
            valBuf = grownV;
            final int[] grownN = new int[nameBuf.length << 1];
            System.arraycopy(nameBuf, 0, grownN, 0, count);
            nameBuf = grownN;
            final int[] grownPath = new int[pathBuf.length << 1];
            System.arraycopy(pathBuf, 0, grownPath, 0, count);
            pathBuf = grownPath;
          }
          valBuf[count] = value;
          nameBuf[count] = parentNameKey;
          pathBuf[count] = parentPathNodeKeyInt;
          count++;
        }
        word &= word - 1;
      }
    }
    if (count == 0) {
      return null;
    }
    final byte tagKind = allPathNodeKeysValid
        ? BooleanRegion.TAG_KIND_PATH_NODE
        : BooleanRegion.TAG_KIND_NAME;
    final int[] tagBuf = allPathNodeKeysValid
        ? pathBuf
        : nameBuf;
    return BooleanRegion.encode(valBuf, tagBuf, count, tagKind);
  }

  /** Raw boolean-region payload bytes, or {@code null}. */
  public MemorySegment getBooleanRegionPayload() {
    if (overflowSlotSidecar != null) {
      return null;
    }
    MemorySegment payload = regionPayload(RegionTable.KIND_BOOLEAN);
    if (payload == null) {
      payload = tryBuildBooleanRegionFromSlottedPage();
    }
    return payload;
  }


  @Override
  public List<PageReference> getReferences() {
    return List.of(references.values().toArray(new PageReference[0]));
  }

  @Override
  public void commit(final StorageEngineWriter pageWriteTrx) {
    addReferences(pageWriteTrx.getResourceSession().getResourceConfig());
    for (final PageReference reference : references.values()) {
      if (!(reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
          && reference.getLogKey() == Constants.NULL_ID_LONG)) {
        pageWriteTrx.commit(reference);
      }
    }
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setPageReference(final long key, final PageReference reference) {
    references.put(key, reference);
  }

  @Override
  public Set<Entry<Long, PageReference>> referenceEntrySet() {
    return references.entrySet();
  }

  @Override
  public PageReference getPageReference(final long key) {
    return references.get(key);
  }

  @Override
  public MemorySegment slots() {
    return slottedPage;
  }

  @Override
  public MemorySegment deweyIds() {
    // DeweyIDs are inline in the slotted page heap — no separate memory
    return null;
  }

  @Override
  public IndexType getIndexType() {
    return indexType;
  }

  @Override
  public int getRevision() {
    return revision;
  }

  /**
   * Get the current version of this page frame. Used for detecting page reuse via version counter
   * check.
   *
   * @return current version number
   */
  public int getVersion() {
    return version.get();
  }

  /**
   * Increment the version counter. Called when the page frame is reused for a different logical page.
   */
  public void incrementVersion() {
    version.incrementAndGet();
  }

  /**
   * Try to acquire a guard on this page. Returns false if the page is orphaned or closed (cannot be
   * used). This is the synchronized version that prevents race conditions with close().
   *
   * @return true if guard was acquired, false if page is orphaned/closed
   */
  @Override
  public synchronized boolean acquireGuard() {
    int flags = (int) STATE_FLAGS_HANDLE.getVolatile(this);
    if ((flags & CLOSED_BIT) != 0) {
      return false;
    }
    if ((flags & ORPHANED_BIT) != 0) {
      // An orphaned page stays alive until its LAST guard releases (deferred close). Taking an
      // ADDITIONAL guard while one is held is safe and required — e.g. the write cursor guards
      // its current page, the page is orphaned by a TIL copy-on-write, and remove() then needs
      // a second guard on that same page. Only resurrection from ZERO is forbidden: at zero an
      // orphan may already be mid-teardown. (Both this method and releaseGuard are synchronized,
      // so the count check cannot race the deferred close.)
      if (guardCount.get() <= 0) {
        return false;
      }
      guardCount.incrementAndGet();
      return true;
    }
    guardCount.incrementAndGet();
    return true;
  }

  public synchronized boolean tryAcquireGuard() {
    return acquireGuard();
  }

  /**
   * Release a guard on this page (decrement guard count). If the page is orphaned and this was the
   * last guard, the page is closed. This ensures deterministic cleanup without relying on
   * GC/finalizers.
   */
  public synchronized void releaseGuard() {
    guardCount.decrementAndGet();
    int flags = (int) STATE_FLAGS_HANDLE.getVolatile(this);
    if ((flags & ORPHANED_BIT) != 0) {
      // close() checks guardCount > 0 and CLOSED_BIT internally
      close();
    }
  }

  /**
   * Mark this page as orphaned using lock-free CAS. Called when the page is removed from cache but
   * still has active guards. The page will be closed when the last guard is released.
   */
  public void markOrphaned() {
    int current;
    do {
      current = (int) STATE_FLAGS_HANDLE.getVolatile(this);
      if ((current & ORPHANED_BIT) != 0) {
        return; // Already orphaned
      }
    } while (!STATE_FLAGS_HANDLE.compareAndSet(this, current, current | ORPHANED_BIT));
  }

  /**
   * Check if this page is orphaned.
   *
   * @return true if the page has been marked as orphaned
   */
  public boolean isOrphaned() {
    return ((int) STATE_FLAGS_HANDLE.getVolatile(this) & ORPHANED_BIT) != 0;
  }

  /**
   * Get the current guard count. Used by ClockSweeper to check if page can be evicted.
   *
   * @return current guard count
   */
  public int getGuardCount() {
    return guardCount.get();
  }

  /**
   * Mark this page as recently accessed (set HOT bit). Called on every page access for clock eviction
   * algorithm.
   * <p>
   * Uses opaque memory access (no memory barriers) for maximum performance. The HOT bit is advisory -
   * stale reads are acceptable and will at worst give a page an extra second chance during eviction.
   * </p>
   */
  public void markAccessed() {
    // Lock-free: use opaque OR to set HOT bit without memory barriers
    // This is the hot path - called on every page access
    int current;
    do {
      current = (int) STATE_FLAGS_HANDLE.getOpaque(this);
      if ((current & HOT_BIT) != 0) {
        return; // Already hot, avoid unnecessary CAS
      }
    } while (!STATE_FLAGS_HANDLE.weakCompareAndSetPlain(this, current, current | HOT_BIT));
  }

  /**
   * Check if this page is HOT (recently accessed).
   * <p>
   * Uses opaque memory access for maximum performance on the read path.
   * </p>
   *
   * @return true if page is hot, false otherwise
   */
  public boolean isHot() {
    return ((int) STATE_FLAGS_HANDLE.getOpaque(this) & HOT_BIT) != 0;
  }

  /**
   * Clear the HOT bit (for clock sweeper second-chance algorithm).
   * <p>
   * Uses lock-free CAS to atomically clear the HOT bit.
   * </p>
   */
  public void clearHot() {
    // Lock-free: use CAS to clear HOT bit
    int current;
    do {
      current = (int) STATE_FLAGS_HANDLE.getOpaque(this);
      if ((current & HOT_BIT) == 0) {
        return; // Already cold, avoid unnecessary CAS
      }
    } while (!STATE_FLAGS_HANDLE.weakCompareAndSetPlain(this, current, current & ~HOT_BIT));
  }

  /**
   * Reset page data structures for reuse. Clears records and internal state but keeps MemorySegments
   * allocated. Used when evicting a page to prepare frame for reuse.
   */
  public void reset() {
    // Clear record arrays
    if (records != null) {
      Arrays.fill(records, null);
    }

    // Reset slotted page state (bitmap and heap pointers)
    if (slottedPage != null) {
      PageLayout.initializePage(slottedPage, recordPageKey, revision, indexType.getID(), areDeweyIDsStored);
      cachedHeapEnd = 0;
      cachedHeapUsed = 0;
      cachedPopulatedCount = 0;
    }

    // Reset index trackers
    lastSlotIndex = -1;

    // A reused frame hosts a NEW logical page: the old occupant's region table and its derive
    // memo describe slots that no longer exist. An inherited memo is the nastier half — the new
    // page would silently skip derivations the OLD page refused, forever.
    releaseRegionTableOwnership();
    clearRegionDeriveAttempted(~0);
    cachedNumberHeader = null;
    cachedStringHeader = null;

    clearFsstBinding();
    clearGlobalStringBinding();

    // A reused frame must not retain native payload or metadata from its previous logical page.
    clearSideSlots();

    // Clear references
    references.clear();
    addedReferences = false;

    // Clear cached data
    clearSerializedCache();
    hashCode = null;

    // CRITICAL: Guard count MUST be 0 before reset
    int currentGuardCount = guardCount.get();
    if (currentGuardCount != 0) {
      throw new IllegalStateException(String.format(
          "CRITICAL BUG: reset() called on page with active guards! "
              + "Page %d (%s) rev=%d guardCount=%d - this will cause guard count corruption!",
          recordPageKey, indexType, revision, currentGuardCount));
    }

    // Clear HOT bit using lock-free operation
    clearHot();

    // NOTE: We do NOT release MemorySegments here - they stay allocated
    // The allocator's release() method is called separately if needed
  }

  // Add references to OverflowPages.
  public void addReferences(final ResourceConfiguration resourceConfiguration) {
    if (!addedReferences) {
      // Lazy copy: copy preserved slots that weren't modified from completePageRef
      // This is the deferred work from combineRecordPagesForModification for DIFFERENTIAL,
      // INCREMENTAL (full-dump), and SLIDING_SNAPSHOT versioning types.
      if (completePageRef != null && slottedPage != null && PageLayout.hasPreservedSlots(slottedPage)) {
        for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
          // Side images and reference-only overflow records are intentionally invisible to
          // getSlot(). Conversely, a bare companion reference is not enough to shadow an inline
          // fused descriptor that still has to be preserved.
          final boolean needsPreservation = PageLayout.isSlotPreserved(slottedPage, i);
          if (needsPreservation && !hasLogicalCarrierShadowingPreservation(i, completePageRef)) {
            // Copies inline or sidecar carrier, preserving kind, Dewey metadata and companion ref.
            copySlotFromPage(completePageRef, i);
          }
        }
      }

      if (records != null) {
        processEntries(resourceConfiguration, records);
      }

      addedReferences = true;
    }
  }

  private void processEntries(final ResourceConfiguration resourceConfiguration, final DataRecord[] records) {
    // Use a confined arena for temporary serialization buffers.
    // This allows immediate cleanup of memory for normal records (which are copied to slotMemory).
    // For overflow records, we copy to a persistent arena since they need to outlive this method.
    try (var tempArena = Arena.ofConfined()) {
      // PERFORMANCE OPTIMIZATION: Reuse a single buffer for all records instead of allocating per-record.
      // Initial size of 256 bytes covers most nodes; will grow automatically if needed.
      // This eliminates ~N allocations where N = number of non-null records.
      var reusableOut = new MemorySegmentBytesOut(tempArena, 256);

      for (int i = 0; i < records.length; i++) {
        final DataRecord record = records[i];
        if (record == null) {
          // Write singletons (FlyweightNode.isWriteSingleton()) are never stored in records[] —
          // their data is already serialized to the slotted page heap via serializeToHeap() in setRecord().
          continue;
        }
        if (record instanceof FlyweightNode fn) {
          if (fn.isBound()) {
            // Record data is already in the heap via serializeToHeap() — skip serialization.
            // However, DeweyIDs are stored separately and may have been updated after binding
            // (e.g., by computeNewDeweyIDs during moveSubtreeToFirstChild). Since records[i]
            // is about to be nulled, persist the DeweyID to the heap now.
            if (areDeweyIDsStored && fn.getDeweyID() != null && fn.getNodeKey() != 0) {
              final byte[] deweyIdBytes = fn.getDeweyIDAsBytes();
              if (deweyIdBytes != null && deweyIdBytes.length > 0) {
                setDeweyIdToHeap(MemorySegment.ofArray(deweyIdBytes), i);
              }
            }
            records[i] = null;
            continue;
          }
          // Unbound flyweight (e.g., value mutation caused unbind): re-serialize to slotted page
          // heap. When the record does not fit within the largest slotted-page size class
          // (#1076), fall through to the generic path below, which diverts it to an OverflowPage.
          // The slotted page is created if absent: a fused record must get its fused-inline
          // attempt, because the generic stage below never inlines fused kinds.
          if (slottedPage == null && isFusedAnyObjectNamedKindId(((NodeKind) fn.getKind()).getId())) {
            ensureSlottedPage();
          }
          if (slottedPage != null) {
            final long nodeKey = record.getNodeKey();
            final int offset = StorageEngineReader.recordPageOffset(nodeKey);
            if (serializeToHeap(fn, nodeKey, offset)) {
              references.remove(nodeKey);
              records[i] = null;
              continue;
            }
          }
        }
        final var recordID = record.getNodeKey();
        final var offset = StorageEngineReader.recordPageOffset(recordID);
        final byte[] deweyIdBytes = areDeweyIDsStored && recordID != 0
            ? record.getDeweyIDAsBytes()
            : null;
        final int deweyIdLen = deweyIdBytes == null
            ? 0
            : deweyIdBytes.length;

        // Clear buffer for reuse (reset position to 0, keeps capacity)
        reusableOut.clear();

        // Serialize into the reusable buffer
        recordPersister.serialize(reusableOut, record, resourceConfiguration);
        // Zero-alloc destination read: baseSegment() returns the unsliced growable segment;
        // position() is the used byte count. Avoids the per-record MemorySegment.asSlice view
        // that getDestination() would allocate — see baseSegment() doc on MemorySegmentBytesOut.
        long usedSize = reusableOut.position();
        MemorySegment base = reusableOut.baseSegment();

        final long slotTotalSize = areDeweyIDsStored
            ? usedSize + deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE
            : usedSize;
        // A fused named record reaching this point had its fused-inline write REFUSED (the retry
        // above, or no slotted page to retry against). Its generic serialization may still fit
        // inline — the generic layout is a couple dozen bytes leaner — but storing it that way
        // files the slot under the raw-record sentinel kind, and every PAX region builder and
        // anchored scan classifies by that directory kind: the record stays readable and silently
        // drops out of every column. 1428 of 1,000,000 ClickBench hits records sat in exactly this
        // band (fused form over the 512-byte cap, generic form under it). Such records take the
        // overflow-carrier route instead, which installs a fused kind-id descriptor carrying the
        // full scan metadata beside an OverflowPage holding the value.
        final boolean fusedInlineRefused = record instanceof FlyweightNode
            && isFusedAnyObjectNamedKindId(((NodeKind) record.getKind()).getId());
        if (fusedInlineRefused || slotTotalSize > PageConstants.MAX_RECORD_SIZE
            || slotTotalSize > (long) MAX_SLOTTED_PAGE_CAPACITY - PageLayout.HEAP_START - cachedHeapEnd) {
          // Overflow page (#1076): the record is either larger than the per-record threshold or
          // would not fit into the slotted page heap within the largest allocator size class.
          // An OverflowPage is an independently versioned record carrier: it may later be copied
          // into a fragment whose revision-local FSST table differs from this page's table. Store
          // string values in canonical raw form for *every* spill reason, including an otherwise
          // small compressed record that merely encountered a full page heap.
          if (isCompressedStringRecord(record)) {
            canonicalizeOverflowString(record);
            reusableOut.clear();
            recordPersister.serialize(reusableOut, record, resourceConfiguration);
            usedSize = reusableOut.position();
            base = reusableOut.baseSegment();
          }

          // Copy the serialized bytes into a persistent buffer; the OverflowPage is written to
          // disk in NodeStorageEngineWriter#commit and the read path falls back to it when the
          // slot is empty but a reference with a valid disk key exists.
          if (usedSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Record is too large for OverflowPage: " + usedSize);
          }
          byte[] persistentBuffer = new byte[(int) usedSize];
          MemorySegment.copy(base, 0L, MemorySegment.ofArray(persistentBuffer), 0L, usedSize);

          installCanonicalOverflowCarrier(record, persistentBuffer, offset, deweyIdBytes);
        } else {
          // Normal record: setSlotDirect copies the leading {usedSize} bytes from {base}
          // into the slotted page heap. No intermediate slice.
          setSlotDirect(base, 0L, (int) usedSize, offset);
          if (deweyIdLen > 0) {
            setDeweyId(deweyIdBytes, offset);
          }
          // A previous oversized version of this record may have left an overflow reference —
          // the slot now carries the current version, so drop the stale reference.
          references.remove(recordID);
        }
        // Clear record reference after serialization — snapshot isolation.
        // Data is now in slotMemory/slottedPage; prevents cross-transaction aliasing.
        records[i] = null;
      }
    } // Confined arena automatically closes here, freeing all temporary buffers
  }

  private boolean isCompressedStringRecord(final DataRecord record) {
    return record instanceof StringNode stringNode && stringNode.isCompressed()
        || record instanceof ObjectNamedStringNode fusedStringNode && fusedStringNode.isCompressed();
  }

  /**
   * Append this page's raw string values to {@code samples}, stopping once {@code cap} samples have
   * been gathered in total.
   *
   * <p>
   * This feeds {@code NodeStorageEngineWriter#buildRevisionFsstSymbolTable}, which pools samples from
   * <em>many</em> pages and builds one symbol table for the whole revision. The table used to be
   * built here, per page, and that failed in both directions at once: a full slot scan plus frequency
   * analysis per page made ingest 18× slower, and one page rarely holds the
   * {@link FSSTCompressor#MIN_SAMPLES_FOR_TABLE} strings a table needs before it beats raw bytes, so
   * the per-page table was rejected on essentially every page anyway.
   *
   * <p>
   * Only uncompressed values are gathered: sampling an already-FSST-encoded value would feed the next
   * table's frequency analysis bytes that are not text.
   *
   * @param samples the list to append to; not cleared first
   * @param cap the total size {@code samples} may reach, bounding the cost of a commit-wide sweep
   */
  public void collectFsstStringSamples(final java.util.List<byte[]> samples, final int cap) {
    final int stringValueId = NodeKind.STRING_VALUE.getId();

    // Collect string values from StringNode (the only on-disk string carrier
    // outside fused OBJECT_NAMED_STRING). Fused records embed the value inline;
    // their FSST integration is handled by the fused write path itself.

    // Scan records[] for non-FlyweightNode string records (legacy path)
    if (records != null) {
      for (final DataRecord record : records) {
        if (samples.size() >= cap) {
          return;
        }
        if (record == null) {
          continue;
        }
        if (record instanceof StringNode stringNode && !stringNode.isCompressed()) {
          byte[] value = stringNode.getRawValueWithoutDecompression();
          if (value != null && value.length > 0) {
            samples.add(value);
          }
        } else if (record instanceof ObjectNamedStringNode fusedNode && !fusedNode.isCompressed()) {
          // Fresh-insert fused nodes live here in records[], not on the slotted page — sampling
          // only the slot path missed every one of them on first commit, which is exactly when
          // the revision table is built.
          final byte[] value = fusedNode.getRawValueWithoutDecompression();
          if (value != null && value.length > 0) {
            samples.add(value);
          }
        }
      }
    }

    // Scan slotted page for FlyweightNode strings (zero records[] path)
    if (slottedPage != null) {
      // Every populated slot is inspected, so hoist the expansion out of the walk rather than
      // gating a thousand times.
      refuseUnresolvedGlobalTags("the commit-time FSST sampling pass");
      ensureAllChunks();
      final int fusedStringId = NodeKind.OBJECT_NAMED_STRING.getId();
      for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
        if (samples.size() >= cap) {
          return;
        }
        if (records != null && records[i] != null)
          continue; // Already scanned above
        if (!PageLayout.isSlotPopulated(slottedPage, i))
          continue;
        final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, i);
        if (nodeKindId == fusedStringId) {
          if (isFusedObjectNamedStringOverflowDescriptor(i)) {
            continue;
          }
          // Fused field values are where the string bytes actually are on JSON data; a sampler
          // that skipped them never gathered enough eligible text for a table to build, which
          // made FSST a measured no-op end to end.
          if (!isFusedObjectNamedStringValueCompressed(i)) {
            final byte[] value = readFusedObjectNamedStringBytes(i);
            if (value != null && value.length > 0) {
              samples.add(value);
            }
          }
          continue;
        }
        if (nodeKindId == stringValueId) {
          final int heapOff = PageLayout.getDirHeapOffset(slottedPage, i);
          final long recordBase = PageLayout.heapAbsoluteOffset(heapOff);
          final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + i;
          fsstStringFlyweight().bind(slottedPage, recordBase, nodeKey, i);
          try {
            if (!fsstStringFlyweight().isCompressed()) {
              byte[] value = fsstStringFlyweight().getRawValueWithoutDecompression();
              if (value != null && value.length > 0)
                samples.add(value);
            }
          } finally {
            fsstStringFlyweight().clearBinding();
          }
        }
      }
    }
  }

  /**
   * Compress all string values in the page using the revision's FSST symbol table. This modifies the
   * string nodes in place to use compressed values. A no-op until
   * {@code NodeStorageEngineWriter#buildRevisionFsstSymbolTable} has handed the page a table —
   * without one, strings serialize raw.
   */
  public void compressStringValues() {
    if (fsstSymbolTable == null || fsstSymbolTable.length == 0) {
      return;
    }

    final int stringValueId = NodeKind.STRING_VALUE.getId();
    // One parse (and one matcher build, via the encoder's identity cache) for the whole page —
    // the per-value encode used to re-parse the table for every string it compressed.
    final byte[][] parsedSymbols = FSSTCompressor.parsedFor(fsstSymbolTable);
    if (parsedSymbols.length == 0) {
      return;
    }

    // Compress records[] strings (legacy path)
    if (records != null) {
      for (final DataRecord record : records) {
        if (record == null) {
          continue;
        }
        if (record instanceof StringNode stringNode) {
          if (!stringNode.isCompressed()) {
            byte[] originalValue = stringNode.getRawValueWithoutDecompression();
            if (originalValue != null && originalValue.length > 0) {
              byte[] compressedValue =
                  FSSTCompressor.encodeOrNull(originalValue, 0, originalValue.length, parsedSymbols);
              if (compressedValue != null
                  && compressedRecordFitsInline(stringNode, originalValue.length, compressedValue.length)) {
                stringNode.setRawValue(compressedValue, true, fsstSymbolTable);
              }
            }
          }
        } else if (record instanceof ObjectNamedStringNode fusedNode) {
          if (!fusedNode.isCompressed()) {
            final byte[] originalValue = fusedNode.getRawValueWithoutDecompression();
            if (originalValue != null && originalValue.length > 0) {
              final byte[] compressedValue =
                  FSSTCompressor.encodeOrNull(originalValue, 0, originalValue.length, parsedSymbols);
              if (compressedValue != null
                  && compressedRecordFitsInline(fusedNode, originalValue.length, compressedValue.length)) {
                fusedNode.setRawValue(compressedValue, true, fsstSymbolTable);
              }
            }
          }
        }
      }
    }

    // Compress slotted page strings (zero records[] path)
    if (slottedPage != null) {
      // Rewrites records in place across the whole page; nothing here is selective enough for the
      // per-slot gate to buy anything.
      refuseUnresolvedGlobalTags("the commit-time FSST compression pass");
      ensureAllChunks();
      final int fusedStringId = NodeKind.OBJECT_NAMED_STRING.getId();
      for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
        if (records != null && records[i] != null)
          continue; // Already handled above
        if (!PageLayout.isSlotPopulated(slottedPage, i))
          continue;
        final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, i);
        if (nodeKindId == stringValueId) {
          final int heapOff = PageLayout.getDirHeapOffset(slottedPage, i);
          final long recordBase = PageLayout.heapAbsoluteOffset(heapOff);
          final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + i;

          fsstStringFlyweight().bind(slottedPage, recordBase, nodeKey, i);
          fsstStringFlyweight().setOwnerPage(this); // Enable write-through
          try {
            byte[] originalValue = fsstStringFlyweight().getRawValueWithoutDecompression();
            if (originalValue != null && originalValue.length > 0 && !fsstStringFlyweight().isCompressed()) {
              byte[] compressed = FSSTCompressor.encodeOrNull(originalValue, 0, originalValue.length, parsedSymbols);
              if (compressed != null) {
                fsstStringFlyweight().setRawValue(compressed, true, fsstSymbolTable);
              }
            }
          } finally {
            fsstStringFlyweight().setOwnerPage(null);
            fsstStringFlyweight().clearBinding();
          }
        } else if (nodeKindId == fusedStringId) {
          if (isFusedObjectNamedStringOverflowDescriptor(i)) {
            continue;
          }
          // Fused OBJECT_NAMED_STRING — on real JSON these hold nearly all string bytes, and
          // skipping them was why FSST used to be a no-op end to end. Same write-through rewrite
          // as above; the region builder later copies whatever form the slot ends up in,
          // verbatim, so heap and region stay bit-identical for value elision.
          final int heapOff = PageLayout.getDirHeapOffset(slottedPage, i);
          final long recordBase = PageLayout.heapAbsoluteOffset(heapOff);
          final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + i;

          final ObjectNamedStringNode fused = fsstFusedStringFlyweight();
          fused.bind(slottedPage, recordBase, nodeKey, i);
          fused.setOwnerPage(this); // Enable write-through
          try {
            final byte[] originalValue = fused.getRawValueWithoutDecompression();
            if (originalValue != null && originalValue.length > 0 && !fused.isCompressed()) {
              final byte[] compressed =
                  FSSTCompressor.encodeOrNull(originalValue, 0, originalValue.length, parsedSymbols);
              if (compressed != null) {
                fused.setRawValue(compressed, true, fsstSymbolTable);
              }
            }
          } finally {
            fused.setOwnerPage(null);
            fused.clearBinding();
          }
        }
      }
    }
  }

  /**
   * Page-level FSST dictionaries may change when version fragments are combined. Therefore a value
   * that will remain out of line must stay in canonical raw form; only install compressed bytes when
   * the resulting record can actually use this page's inline slot and dictionary reference.
   */
  private boolean compressedRecordFitsInline(final FlyweightNode node, final int rawLength,
      final int compressedLength) {
    final byte[] deweyId = areDeweyIDsStored
        ? node.getDeweyIDAsBytes()
        : null;
    final int deweyOverhead = areDeweyIDsStored
        ? (deweyId == null
            ? 0
            : deweyId.length) + PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0;
    final long compressedEstimate = (long) node.estimateSerializedSize() - rawLength + compressedLength
        + deweyOverhead;
    return compressedEstimate <= PageConstants.MAX_RECORD_SIZE;
  }

  /**
   * Set the FSST symbol table on all string nodes after deserialization. This allows nodes to use
   * lazy decompression.
   */
  public void propagateFsstSymbolTableToNodes() {
    if (fsstSymbolTable == null || fsstSymbolTable.length == 0) {
      return;
    }
    if (records == null) {
      return;
    }

    for (final DataRecord record : records) {
      if (record == null) {
        continue;
      }
      if (record instanceof StringNode stringNode) {
        stringNode.setFsstSymbolTable(fsstSymbolTable);
      } else if (record instanceof ObjectNamedStringNode fusedNode) {
        fusedNode.setFsstSymbolTable(fsstSymbolTable);
      }
    }
  }

}
