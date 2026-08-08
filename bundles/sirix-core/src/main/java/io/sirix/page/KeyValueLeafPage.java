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
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.node.json.ObjectNamedStringNode;
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
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
import io.sirix.settings.DiagnosticSettings;
import io.sirix.utils.WeakIdentitySet;
import io.sirix.utils.FSSTCompressor;
import io.sirix.utils.ArrayIterator;
import io.sirix.node.BytesOut;
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
@SuppressWarnings({ "unchecked" })
public final class KeyValueLeafPage implements KeyValuePage<DataRecord>, io.sirix.cache.CacheablePage {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueLeafPage.class);
  /**
   * SIMD vector species for bitmap operations.
   * Uses the preferred species for the current platform (256-bit AVX2 or 512-bit AVX-512).
   */
  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
  
  
  /**
   * Enable detailed memory leak tracking.
   * Accessed via centralized {@link DiagnosticSettings#MEMORY_LEAK_TRACKING}.
   * 
   * @see DiagnosticSettings#isMemoryLeakTrackingEnabled()
   */
  public static final boolean DEBUG_MEMORY_LEAKS = DiagnosticSettings.MEMORY_LEAK_TRACKING;
  
  // DIAGNOSTIC COUNTERS (enabled via DEBUG_MEMORY_LEAKS)
  public static final java.util.concurrent.atomic.AtomicLong PAGES_CREATED = new java.util.concurrent.atomic.AtomicLong(0);
  public static final java.util.concurrent.atomic.AtomicLong PAGES_CLOSED = new java.util.concurrent.atomic.AtomicLong(0);
  public static final java.util.concurrent.ConcurrentHashMap<IndexType, java.util.concurrent.atomic.AtomicLong> PAGES_BY_TYPE = 
    new java.util.concurrent.ConcurrentHashMap<>();
  public static final java.util.concurrent.ConcurrentHashMap<IndexType, java.util.concurrent.atomic.AtomicLong> PAGES_CLOSED_BY_TYPE = 
    new java.util.concurrent.ConcurrentHashMap<>();
  
  /**
   * Every page that has been created and not yet closed, held WEAKLY and keyed by IDENTITY.
   *
   * <p>Weakly, because this registry used to hold strong references: registering a page made it
   * immortal, so {@link #PAGES_FINALIZED_WITHOUT_CLOSE} — which counts pages collected without
   * {@code close()} — could never be anything but zero while leak tracking was ON, the only time it
   * is populated at all. The Cleaner literally could not fire. Weak references restore the split the
   * two mechanisms were designed for: what remains here is the set of pages still reachable from
   * somewhere (retained leaks, reported at shutdown with their creation stacks), and what leaves here
   * without being closed is a page that became garbage while still holding an off-heap frame
   * (unreachable leaks, reported by the Cleaner).</p>
   *
   * <p>By identity, because {@link #equals(Object)} is overridden on this type: two distinct
   * instances of the same page key and revision are equal, and an equality-keyed registry would drop
   * one of them from the census.</p>
   */
  public static final java.util.Set<KeyValueLeafPage> ALL_LIVE_PAGES = new WeakIdentitySet<>();
  
  // LEAK DETECTION: Track finalized pages
  public static final java.util.concurrent.atomic.AtomicLong PAGES_FINALIZED_WITHOUT_CLOSE = new java.util.concurrent.atomic.AtomicLong(0);
  
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
   * Version counter for detecting page reuse (LeanStore/Umbra approach).
   * Incremented when page is evicted and reused for a different logical page.
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
   * Packed state flags: HOT (bit 0), orphaned (bit 1), closed (bit 2).
   * Accessed via VarHandle for lock-free CAS operations.
   */
  @SuppressWarnings("unused") // Accessed via VarHandle
  private volatile int stateFlags = 0;

  /** VarHandle for lock-free state flag operations */
  private static final VarHandle STATE_FLAGS_HANDLE;

  static {
    try {
      STATE_FLAGS_HANDLE = MethodHandles.lookup()
          .findVarHandle(KeyValueLeafPage.class, "stateFlags", int.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Guard count for preventing eviction during active use (LeanStore/Umbra pattern).
   * Pages with guardCount > 0 cannot be evicted.
   * This is simpler than per-transaction pinning - it's just a reference count.
   */
  private final AtomicInteger guardCount = new AtomicInteger(0);

  /**
   * DIAGNOSTIC: Stack trace of where this page was created (only captured when DEBUG_MEMORY_LEAKS=true).
   * Used to trace where leaked pages come from.
   */
  private final StackTraceElement[] creationStackTrace;

  /**
   * DIAGNOSTIC: Where and on which thread this page was closed (only captured when
   * DEBUG_MEMORY_LEAKS=true). Lets a reader that observes a closed page report WHO freed it
   * out from under it (use-after-free triage).
   */
  private volatile Throwable closeSite;

  /** Diagnostic accessor for the close-site capture (null unless DEBUG_MEMORY_LEAKS). */
  public Throwable getCloseSite() {
    return closeSite;
  }

  /** Shared Cleaner for all KeyValueLeafPage leak-detection registrations. */
  private static final Cleaner LEAK_CLEANER = Cleaner.create();

  /**
   * Heap-allocated state captured by the leak-detection {@link Cleaner.Cleanable} so it
   * does NOT capture the enclosing {@code KeyValueLeafPage} instance — capturing
   * {@code this} would make the page strongly reachable via the Cleaner queue, defeating
   * the very leak detection it implements. Holds:
   * <ul>
   *   <li>The diagnostic facts the leak log needs (pageKey, indexType, revision,
   *       creationStackTrace).</li>
   *   <li>A {@code closed} flag the page's {@link #close()} flips — the Cleaner action
   *       reads it and skips logging when the page was closed properly.</li>
   * </ul>
   * Non-static state class so users can read the closed flag through the
   * {@code Cleaner.Cleanable} owner; reference is held by the page only when
   * {@link #DEBUG_MEMORY_LEAKS} is on, so production builds pay zero overhead.
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
        FINALIZED_BY_TYPE.computeIfAbsent(indexType,
            _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
      }
      FINALIZED_BY_PAGE_KEY.computeIfAbsent(pageKey,
          _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
      if (LOGGER.isWarnEnabled()) {
        final StringBuilder leakMsg = new StringBuilder()
            .append(String.format(
                "Page leak detected: pageKey=%d, type=%s, revision=%d - not closed explicitly",
                pageKey, indexType, revision));
        if (creationStackTrace != null && LOGGER.isDebugEnabled()) {
          leakMsg.append("\n  Creation stack trace:");
          for (int i = 2; i < Math.min(creationStackTrace.length, 8); i++) {
            final StackTraceElement frame = creationStackTrace[i];
            leakMsg.append(String.format("\n    at %s.%s(%s:%d)",
                frame.getClassName(), frame.getMethodName(),
                frame.getFileName(), frame.getLineNumber()));
          }
        }
        LOGGER.warn(leakMsg.toString());
      }
    }
  }

  /**
   * Non-null only when {@link #DEBUG_MEMORY_LEAKS} is on; the page sets {@code .closed}
   * on this state in {@link #close()} so the Cleaner action skips the leak log.
   */
  private final LeakDetectorState leakDetectorState;

  /**
   * Get the creation stack trace for leak diagnostics.
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
   * Hard ceiling for the slotted-page backing memory: the largest {@link FrameSlotAllocator}
   * size class (256 KiB). {@link #growSlottedPage()} doubles the capacity, so any growth past
   * this ceiling would throw in the allocator. Records that cannot fit within it are diverted
   * to {@link OverflowPage}s instead (#1076).
   */
  public static final int MAX_SLOTTED_PAGE_CAPACITY =
      (int) FrameSlotAllocator.SIZE_CLASSES[FrameSlotAllocator.SIZE_CLASSES.length - 1];

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
   * The record-ID mapped to the records.
   * Lazily allocated on first write to save ~8KB per page when FlyweightNode
   * records go directly to the slotted page heap (zero records[] path).
   */
  private DataRecord[] records;

  private static final DataRecord[] EMPTY_RECORDS = new DataRecord[0];

  private void ensureRecords() {
    if (records == null) {
      records = new DataRecord[Constants.NDP_NODE_COUNT];
    }
  }



  /**
   * FSST symbol table for string compression (shared across all strings in page).
   * Null if FSST compression is not used.
   */
  private byte[] fsstSymbolTable;

  /** {@link #fsstSymbolTableId} when the page references no symbol table. */
  public static final long NO_FSST_SYMBOL_TABLE_ID = 0L;

  /**
   * The dictionary id of this page's symbol table, or {@link #NO_FSST_SYMBOL_TABLE_ID}.
   *
   * <p>Set on the write path before the page is serialized, and on the read path from the page's
   * bytes. When it is set and {@link #fsstSymbolTable} is still null, the table has not been
   * fetched from the dictionary trie yet.
   */
  private long fsstSymbolTableId = NO_FSST_SYMBOL_TABLE_ID;

  /**
   * PAX region table appended to every KVL page. Null when no regions have
   * been populated. Populated with number / string / struct / DeweyID
   * regions; scan operators read contiguous payload buffers from it instead
   * of decoding varints per slot.
   *
   * <p>{@code volatile}: minted and populated under the page monitor by the
   * synchronized region builders but read by concurrent scan workers that
   * take no lock — a plain field gives those readers neither a guaranteed
   * sight of the install nor a happens-before edge to the payloads behind it.
   */
  private volatile RegionTable regionTable;

  /**
   * FSST-compression flyweight (StringNode), lazy-init only on the write-path where
   * FSST compression actually runs. For analytical scan workloads these objects were
   * the top non-page allocator — 7.6% of samples (async-profiler alloc mode) per KVLP
   * constructor. Using a shared sentinel so the null-check in the read path is elided.
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
   * Fused-string sibling of {@link #fsstStringFlyweight} — same lazy-init rationale, for the
   * kind-50 compress pass. On real JSON the fused records hold nearly all string bytes, so this
   * is the flyweight that matters for FSST's reach.
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
   * Reference to the complete page for lazy slot copying at commit time.
   * Set during combineRecordPagesForModification, used by addReferences() to copy
   * slots that need preservation but weren't modified (records[i] == null).
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

  /** Compressed page data as MemorySegment (zero-copy path). Arena.ofAuto()-managed. */
  private volatile MemorySegment compressedSegment;


  private volatile byte[] hashCode;

  // Note: isClosed flag is now packed into stateFlags (bit 2) for lock-free access

  /**
   * Flag indicating whether memory was externally allocated (e.g., by Arena in tests).
   * If true, close() should NOT release memory to segmentAllocator since it wasn't allocated by it.
   */
  private final boolean externallyAllocatedMemory;

  private MemorySegmentAllocator segmentAllocator = Allocators.getInstance();

  /**
   * Backing buffer from decompression (for zero-copy deserialization).
   * When non-null, this buffer must be released on close().
   */
  private MemorySegment backingBuffer;

  /**
   * Releaser to return backing buffer to allocator.
   * Called on close() to return the decompression buffer to the allocator pool.
   */
  private Runnable backingBufferReleaser;

  // ==================== UNIFIED PAGE (LeanStore-style) ====================

  /**
   * Slotted page MemorySegment (PostgreSQL/LeanStore-style: Header + Bitmap + Directory + Heap).
   * Stores records in a heap with per-record offset tables,
   * enabling O(1) field access via flyweight binding. The page layout is defined by
   * {@link PageLayout}: header (32 B) + bitmap (128 B) + directory (8 KB) + heap.
   *
   * <p>FlyweightNode records are serialized directly to the heap at createRecord time
   * and bound for in-place mutation. Non-FlyweightNode records are serialized to the
   * heap at commit time via processEntries.
   */
  private MemorySegment slottedPage;

  /**
   * Actual capacity in bytes of the slottedPage segment.
   * Tracked separately because slottedPage is reinterpreted to Long.MAX_VALUE
   * to eliminate JIT bounds checks on MemorySegment get/set operations.
   */
  private int slottedPageCapacity;

  // ==================== CACHED PAGE HEADER VALUES ====================
  // Mirror of header fields from slottedPage MemorySegment.
  // All hot-path reads use these Java fields (zero MemorySegment overhead).
  // Writes use write-through helpers that update both field and segment.

  private int cachedHeapEnd;
  private int cachedHeapUsed;
  private int cachedPopulatedCount;

  /**
   * Constructor which initializes a new {@link KeyValueLeafPage}.
   * Memory is externally provided (e.g., by Arena in tests) and will NOT be released by close().
   *
   * @param recordPageKey  base key assigned to this node page
   * @param indexType      the index type
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
   * @param recordPageKey              base key assigned to this node page
   * @param indexType                  the index type
   * @param resourceConfig             the resource configuration
   * @param externallyAllocatedMemory  if true, memory was allocated externally and won't be released by close()
   */
  public KeyValueLeafPage(final long recordPageKey, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final int revisionNumber, final MemorySegment slotMemory,
      final MemorySegment deweyIdMemory, final boolean externallyAllocatedMemory) {
    // Assertions instead of requireNonNull(...) checks as it's part of the
    // internal flow.
    assert resourceConfig != null : "The resource config must not be null!";

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

    // Eagerly allocate slotted page — all pages use slotted page format
    ensureSlottedPage();

    // Capture creation stack trace for leak tracing (only when diagnostics enabled)
    if (DEBUG_MEMORY_LEAKS) {
      this.creationStackTrace = Thread.currentThread().getStackTrace();
      PAGES_CREATED.incrementAndGet();
      PAGES_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
      ALL_LIVE_PAGES.add(this);
      if (recordPageKey == 0) {
        ALL_PAGE_0_INSTANCES.add(this);
      }
      this.leakDetectorState =
          new LeakDetectorState(recordPageKey, indexType, revision, creationStackTrace);
      LEAK_CLEANER.register(this, leakDetectorState);
    } else {
      this.creationStackTrace = null;
      this.leakDetectorState = null;
    }
  }

  /**
   * Constructor which reads deserialized data to the {@link KeyValueLeafPage} from the storage.
   * The slotted page will be set by the caller via {@link #setSlottedPage(MemorySegment)}.
   *
   * @param recordPageKey     This is the base key of all contained nodes.
   * @param revision          The current revision.
   * @param indexType         The index type.
   * @param resourceConfig    The resource configuration.
   * @param areDeweyIDsStored Determines if DeweyIDs are stored or not.
   * @param recordPersister   Persistenter.
   * @param references        References to overflow pages.
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
      this.leakDetectorState =
          new LeakDetectorState(recordPageKey, indexType, revision, creationStackTrace);
      LEAK_CLEANER.register(this, leakDetectorState);
    } else {
      this.creationStackTrace = null;
      this.leakDetectorState = null;
    }
  }

  /**
   * Create a deep copy of this page for Copy-on-Write during async epoch boundaries.
   * Copies slotted page MemorySegment, records[], references map, FSST symbol table.
   * The copy is fully independent — no shared mutable state with the original.
   *
   * <p>Uses the deserialization constructor to set lastSlotIndex directly (no public setter).
   * Slotted page is deep-copied via allocate + MemorySegment.copy, then set via setSlottedPage().
   * records[] is shallow-copied (DataRecord objects are not mutated concurrently).
   * Serialization caches (compressedSegment, bytes, hashCode) are left null — copy is dirty.</p>
   *
   * @return a fully independent deep copy of this page
   */
  public KeyValueLeafPage deepCopy() {
    // Deep-copy the references map (each PageReference cloned via copy constructor)
    final var refsCopy = new ConcurrentHashMap<Long, PageReference>(references.size());
    for (final var entry : references.entrySet()) {
      refsCopy.put(entry.getKey(), new PageReference(entry.getValue()));
    }

    // Use deserialization constructor:
    //   - sets lastSlotIndex, externallyAllocatedMemory=false
    //   - records=null, no slotted page allocation (caller sets via setSlottedPage)
    //   - releases slotMemory/deweyIdMemory if non-null (we pass null)
    final var copy = new KeyValueLeafPage(
        recordPageKey, revision, indexType, resourceConfig,
        areDeweyIDsStored, recordPersister, refsCopy,
        null, null,
        lastSlotIndex);

    // Deep-copy slotted page MemorySegment (primary data store)
    if (slottedPage != null) {
      final MemorySegment freshSegment = segmentAllocator.allocate(slottedPageCapacity);
      MemorySegment.copy(slottedPage, 0, freshSegment, 0, slottedPageCapacity);
      copy.setSlottedPage(freshSegment);
    }

    // Shallow-copy records[] if non-null (pending unflushed mutations from setRecord).
    // DataRecord objects are not mutated concurrently — safe to share references.
    // processEntries() at commit time will serialize them to the COPY's slotted page.
    if (records != null) {
      copy.records = Arrays.copyOf(records, records.length);
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

  @Override
  public long getPageKey() {
    return recordPageKey;
  }

  @Override
  public DataRecord getRecord(int offset) {
    return records != null ? records[offset] : null;
  }

  @Override
  public void setRecord(final DataRecord record) {
    addedReferences = false;
    // Invalidate stale compressed cache — record mutation means cached bytes are outdated
    compressedSegment = null;
    bytes = null;
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
  }

  /**
   * Store a newly created record, serializing non-FlyweightNode data to the slotted page heap
   * immediately. This is called from the createRecord path where node factories may reuse
   * singleton objects. By serializing now and nulling records[], we preserve data before the
   * singleton is reused for the next node creation.
   *
   * <p>For FlyweightNode records, this delegates to {@link #setRecord} which handles heap
   * serialization and binding. For non-FlyweightNode on slotted pages, the record is serialized
   * to the heap and records[offset] is nulled — prepareRecordForModification will deserialize
   * a fresh object from the heap when mutation is needed.
   *
   * @param record the newly created record
   */
  public void setNewRecord(final DataRecord record) {
    assert !(record instanceof FlyweightNode)
        : "FlyweightNode must not go through setNewRecord — use serializeNewRecord";
    addedReferences = false;
    compressedSegment = null;
    bytes = null;
    final var key = record.getNodeKey();
    final var offset = (int) (key - ((key >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
    // Defensive: a non-flyweight record may overwrite a number- or string-typed slot.
    maybeInvalidateRegionsForExistingSlot(offset);
    ensureRecords();
    records[offset] = record;
  }

  public void serializeNewRecord(final FlyweightNode fn, final long nodeKey, final int offset) {
    addedReferences = false;
    compressedSegment = null;
    bytes = null;
    if (!serializeToHeap(fn, nodeKey, offset)) {
      // Record does not fit within the largest slotted-page size class (#1076): keep a snapshot
      // in records[] (the flyweight may be reused) — processEntries diverts it to an
      // OverflowPage at serialization time. The node stays unbound in this case.
      ensureRecords();
      records[offset] = fn.toSnapshot();
      return;
    }
    // Node stays bound after creation — next factory clearBinding() handles transition
  }

  /**
   * Serialize a FlyweightNode to the slotted page heap, update directory/bitmap, and bind.
   *
   * <p>After this call the node is bound: getters/setters operate on page memory.
   * processEntries will skip this record at commit time because {@code fn.isBound()} is true.
   *
   * @param fn      the flyweight node to serialize
   * @param nodeKey the node's key
   * @param offset  the slot index within the page (0-1023)
   * @return {@code true} if the record was serialized to the heap; {@code false} if it does not
   *         fit within {@link #MAX_SLOTTED_PAGE_CAPACITY} and must be diverted to an
   *         {@link OverflowPage} by the caller (#1076) — the page is left unchanged then
   */
  private boolean serializeToHeap(final FlyweightNode fn, final long nodeKey, final int offset) {
    ensureSlottedPage();
    // Get DeweyID bytes if stored (must capture BEFORE binding overwrites the node state)
    final byte[] deweyIdBytes = areDeweyIDsStored ? fn.getDeweyIDAsBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;

    // Ensure heap has enough space for this record (value nodes can be large)
    final int heapEnd = cachedHeapEnd;
    final int estimatedSize = fn.estimateSerializedSize() + deweyIdLen
        + (areDeweyIDsStored ? PageLayout.DEWEY_ID_TRAILER_SIZE : 0);
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < estimatedSize) {
      if (slottedPageCapacity * 2 > MAX_SLOTTED_PAGE_CAPACITY) {
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

    // When DeweyIDs are stored, append DeweyID data + 2-byte trailer
    final int totalBytes;
    if (areDeweyIDsStored) {
      if (deweyIdLen > 0) {
        MemorySegment.copy(deweyIdBytes, 0, slottedPage,
            java.lang.foreign.ValueLayout.JAVA_BYTE, absOffset + recordBytes, deweyIdLen);
      }
      totalBytes = recordBytes + deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE;
      PageLayout.writeDeweyIdTrailer(slottedPage, absOffset + totalBytes, deweyIdLen);
    } else {
      totalBytes = recordBytes;
    }

    // Update heap end and used counters
    updateHeapEnd(heapEnd + totalBytes);
    updateHeapUsed(cachedHeapUsed + totalBytes);

    // Update directory entry: [heapOffset][dataLength | nodeKindId]
    final int nodeKindId = ((NodeKind) fn.getKind()).getId();
    PageLayout.setDirEntry(slottedPage, offset, heapEnd, totalBytes, nodeKindId);

    // Mark slot populated in bitmap and track last slot index (new slots only)
    if (!PageLayout.isSlotPopulated(slottedPage, offset)) {
      PageLayout.markSlotPopulated(slottedPage, offset);
      updatePopulatedCount(cachedPopulatedCount + 1);
      lastSlotIndex = offset;
    }

    // Column write: drop every cached PAX region this kind feeds so the next reader rebuilds.
    invalidateRegionsForKindId(nodeKindId);

    // Bind flyweight — all subsequent mutations go directly to page memory
    fn.bind(slottedPage, absOffset, nodeKey, offset);
    fn.setOwnerPage(this);
    return true;
  }

  // ==================== DIRECT-TO-HEAP CREATION ====================

  /**
   * Prepare the heap for a direct record write. Ensures slotted page exists and has
   * enough space. Returns the absolute offset where the caller should write.
   *
   * @param estimatedRecordSize upper bound on record bytes (from estimateSerializedSize)
   * @param deweyIdLen          length of DeweyID bytes (0 if none)
   * @return absolute byte offset in the slotted page MemorySegment to write at
   * @throws SirixIOException if the record cannot fit within the largest slotted-page size
   *         class — value-carrying factories should use
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
   * instead of throwing when the record cannot fit within {@link #MAX_SLOTTED_PAGE_CAPACITY}
   * (either because the record alone is too large, or because the page heap is too full). The
   * caller must then store the record as a heap object via {@link #setRecord(DataRecord)} so
   * {@code processEntries} diverts it to an {@link OverflowPage} at serialization time (#1076).
   *
   * @param estimatedRecordSize upper bound on record bytes (from estimateSerializedSize)
   * @param deweyIdLen          length of DeweyID bytes (0 if none)
   * @return absolute byte offset to write at, or {@link #DIRECT_WRITE_OVERFLOW}
   */
  public long prepareHeapForDirectWriteOrOverflow(final int estimatedRecordSize, final int deweyIdLen) {
    ensureSlottedPage();
    final int deweyOverhead = areDeweyIDsStored
        ? deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE : 0;
    final int totalEstimated = estimatedRecordSize + deweyOverhead;
    final int heapEnd = cachedHeapEnd;
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < totalEstimated) {
      if (slottedPageCapacity * 2 > MAX_SLOTTED_PAGE_CAPACITY) {
        return DIRECT_WRITE_OVERFLOW;
      }
      growSlottedPage();
    }
    return PageLayout.heapAbsoluteOffset(heapEnd);
  }

  /**
   * Complete a direct record write. Handles DeweyID trailer, directory entry, bitmap,
   * heap counters, and flyweight binding. Called after the caller has written the record
   * bytes via a static writeNewRecord method.
   *
   * @param nodeKindId   the node kind ID (e.g. NodeKind.OBJECT.getId())
   * @param nodeKey      the node key
   * @param slotOffset   the slot index (0-1023)
   * @param recordBytes  number of bytes written by writeNewRecord
   * @param deweyIdBytes DeweyID bytes (null if not stored)
   */
  public void completeDirectWrite(final int nodeKindId, final long nodeKey,
      final int slotOffset, final int recordBytes, final byte[] deweyIdBytes) {
    addedReferences = false;
    compressedSegment = null;
    bytes = null;

    final int heapEnd = cachedHeapEnd;
    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;

    // DeweyID trailer
    final int totalBytes;
    if (areDeweyIDsStored) {
      if (deweyIdLen > 0) {
        MemorySegment.copy(deweyIdBytes, 0, slottedPage,
            java.lang.foreign.ValueLayout.JAVA_BYTE, absOffset + recordBytes, deweyIdLen);
      }
      totalBytes = recordBytes + deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE;
      PageLayout.writeDeweyIdTrailer(slottedPage, absOffset + totalBytes, deweyIdLen);
    } else {
      totalBytes = recordBytes;
    }

    // Update heap counters
    updateHeapEnd(heapEnd + totalBytes);
    updateHeapUsed(cachedHeapUsed + totalBytes);

    // Directory entry
    PageLayout.setDirEntry(slottedPage, slotOffset, heapEnd, totalBytes, nodeKindId);

    // Bitmap
    if (!PageLayout.isSlotPopulated(slottedPage, slotOffset)) {
      PageLayout.markSlotPopulated(slottedPage, slotOffset);
      updatePopulatedCount(cachedPopulatedCount + 1);
      lastSlotIndex = slotOffset;
    }

    // Column write: drop every cached PAX region this kind feeds so the next reader rebuilds.
    invalidateRegionsForKindId(nodeKindId);

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
   * Resize a record whose varint width changed. Appends new version at heap end,
   * updates directory, re-binds, and sets ownerPage. Old space becomes dead
   * (reclaimed on page compaction/rewrite at commit time).
   *
   * @param fn      the flyweight node (unbound, with updated Java fields)
   * @param nodeKey the node's key
   * @param offset  the slot index within the page (0-1023)
   */
  public void resizeRecord(final FlyweightNode fn, final long nodeKey, final int offset) {
    compressedSegment = null;
    bytes = null;
    serializeToHeap(fn, nodeKey, offset);
  }

  /**
   * Resize a single field in a bound record by raw-copying unchanged fields and re-encoding
   * only the changed field. Avoids the full unbind/re-serialize round-trip of {@link #resizeRecord}.
   *
   * <p>Bump-allocates new heap space, calls {@link DeltaVarIntCodec#resizeField} to perform
   * three-segment copy (before + changed + after), preserves DeweyID trailer, updates directory,
   * and re-binds the flyweight to the new location.
   *
   * <p><b>HFT note</b>: Zero allocations. Uses {@link MemorySegment#copy} (AVX/SSE intrinsics).
   * Cold path — only called on varint width change, which is rare (~5% of mutations).
   *
   * @param fn         the bound flyweight node (must be bound to this page's slotted page)
   * @param nodeKey    the node's key
   * @param slotIndex  the slot index within the page (0-1023)
   * @param fieldIndex the index of the field to resize (0 to fieldCount-1)
   * @param fieldCount total number of fields in this record type's offset table
   * @param encoder    encodes the new field value at the target offset
   */
  public void resizeRecordField(final FlyweightNode fn, final long nodeKey, final int slotIndex,
      final int fieldIndex, final int fieldCount, final DeltaVarIntCodec.FieldEncoder encoder) {
    assert slottedPage != null : "resizeRecordField requires slotted page";
    assert PageLayout.isSlotPopulated(slottedPage, slotIndex) : "slot not populated: " + slotIndex;

    compressedSegment = null;
    bytes = null;

    // --- Read old record metadata from directory ---
    final int oldHeapOffset = PageLayout.getDirHeapOffset(slottedPage, slotIndex);
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

    // --- Ensure heap capacity ---
    final int heapEnd = cachedHeapEnd;
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < maxNewTotalLen) {
      growSlottedPage();
    }

    // --- Raw-copy resize: copy unchanged fields, re-encode changed field ---
    final long newRecordBase = PageLayout.heapAbsoluteOffset(heapEnd);
    final int newRecordLen = DeltaVarIntCodec.resizeField(
        slottedPage, oldRecordBase, oldRecordOnlyLen,
        fieldCount, fieldIndex,
        slottedPage, newRecordBase,
        encoder);

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

    // --- Re-bind flyweight to new location ---
    fn.bind(slottedPage, newRecordBase, nodeKey, slotIndex);
    fn.setOwnerPage(this);

    // In-place field rewrite changed a value every cached region of this kind snapshotted.
    invalidateRegionsForKindId(nodeKindId);
  }

  /**
   * Zero-copy raw slot bytes from source page to this page's heap.
   * Copies the record body + DeweyID trailer verbatim, avoiding deserialize-serialize round-trip.
   *
   * @param sourcePage the source page to copy from
   * @param slotIndex  the slot index to copy
   */
  public void copySlotFromPage(final KeyValueLeafPage sourcePage, final int slotIndex) {
    final MemorySegment srcPage = sourcePage.getSlottedPage();
    if (srcPage == null || !PageLayout.isSlotPopulated(srcPage, slotIndex)) {
      return;
    }
    ensureSlottedPage();

    // Read source slot metadata
    final int srcHeapOffset = PageLayout.getDirHeapOffset(srcPage, slotIndex);
    final int srcTotalLen = PageLayout.getDirDataLength(srcPage, slotIndex);
    final int srcNodeKindId = PageLayout.getDirNodeKindId(srcPage, slotIndex);

    // Ensure destination has enough space
    final int heapEnd = cachedHeapEnd;
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < srcTotalLen) {
      growSlottedPage();
    }

    // Copy raw bytes (record body + DeweyID trailer) from source to destination heap
    final long srcAbs = PageLayout.heapAbsoluteOffset(srcHeapOffset);
    final long dstAbs = PageLayout.heapAbsoluteOffset(heapEnd);
    MemorySegment.copy(srcPage, srcAbs, slottedPage, dstAbs, srcTotalLen);

    // Update destination heap end and used counters
    updateHeapEnd(heapEnd + srcTotalLen);
    updateHeapUsed(cachedHeapUsed + srcTotalLen);

    // Update destination directory entry
    PageLayout.setDirEntry(slottedPage, slotIndex, heapEnd, srcTotalLen, srcNodeKindId);

    // Mark slot populated in bitmap
    if (!PageLayout.isSlotPopulated(slottedPage, slotIndex)) {
      PageLayout.markSlotPopulated(slottedPage, slotIndex);
      updatePopulatedCount(cachedPopulatedCount + 1);
      lastSlotIndex = slotIndex;
    }

    // Invalidate compressed cache
    compressedSegment = null;
    bytes = null;
    addedReferences = false;

    // Column copy: a new value lands in this page's heap. The source's region is not carried, and
    // any region cached for THIS page is now incomplete.
    invalidateRegionsForKindId(srcNodeKindId);
  }

  /**
   * Check if the slotted page has a populated slot for the given record key.
   *
   * @param recordKey the record key
   * @return true if the slot is populated on the slotted page
   */
  public boolean hasSlottedPageSlot(final long recordKey) {
    if (slottedPage == null) {
      return false;
    }
    final int offset = (int) (recordKey - ((recordKey >> Constants.NDP_NODE_COUNT_EXPONENT)
        << Constants.NDP_NODE_COUNT_EXPONENT));
    return PageLayout.isSlotPopulated(slottedPage, offset);
  }

  /**
   * Allocate and initialize the slotted page if not yet present.
   */
  public void ensureSlottedPage() {
    if (slottedPage != null) {
      return;
    }
    final MemorySegment allocated = segmentAllocator.allocate(PageLayout.INITIAL_PAGE_SIZE);
    slottedPageCapacity = (int) allocated.byteSize();
    PageLayout.initializePage(allocated, recordPageKey, revision, indexType.getID(), areDeweyIDsStored);
    // Bound the view to the REAL capacity: reinterpret(Long.MAX_VALUE) disabled every FFM
    // bounds check on the hottest read/write surface, turning any directory/heap-offset bug
    // into silent cross-segment corruption inside the shared region instead of an exception.
    slottedPage = allocated.reinterpret(slottedPageCapacity);
    cachedHeapEnd = 0;
    cachedHeapUsed = 0;
    cachedPopulatedCount = 0;
  }

  /**
   * Bulk-copy the slotted-page state from {@code src} into a buffer owned by
   * this page. Used by the single-fragment combine fast path to avoid the
   * per-slot {@code setSlotWithNodeKind} loop; one MemorySegment.copy
   * replaces ~1024 small copies + directory writes + bitmap updates.
   *
   * <p>If {@code this} already has a slotted page (via eager
   * {@code ensureSlottedPage} in the constructor), it is released first —
   * the constructor's allocation is wasted for the combine path, but
   * reusing it in place requires handling size-class mismatches that rarely
   * hit. Net: trade one 64 KiB release for a 1024× loop skip.
   *
   * <p>Overwrites the header's revision field after the copy so downstream
   * readers observe this page's target revision (not the donor fragment's).
   */
  public void copySlottedPageFrom(final KeyValueLeafPage src) {
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
      if (slottedPage != null) {
        try {
          segmentAllocator.release(slottedPage.reinterpret(slottedPageCapacity));
        } catch (final Throwable e) {
          LOGGER.debug("Release of pre-existing slottedPage before copy failed: {}", e.getMessage());
        }
        slottedPage = null;
      }
      dst = segmentAllocator.allocate(srcCap);
      slottedPageCapacity = (int) dst.byteSize();
      slottedPage = dst.reinterpret(slottedPageCapacity); // capacity-bounded: keep FFM bounds checks
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
   * Grow the slotted page by doubling its size.
   * Copies all existing data (header + bitmap + directory + heap) to the new segment.
   */
  private void growSlottedPage() {
    final int currentSize = slottedPageCapacity;
    final int newSize = currentSize * 2;
    final MemorySegment grown = segmentAllocator.allocate(newSize);
    // Copy all existing data
    MemorySegment.copy(slottedPage, 0, grown, 0, currentSize);
    // Release old segment (reinterpret back to actual size for allocator)
    segmentAllocator.release(slottedPage.reinterpret(currentSize));
    slottedPageCapacity = (int) grown.byteSize();
    slottedPage = grown.reinterpret(slottedPageCapacity); // capacity-bounded: keep FFM bounds checks
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
    assert cachedPopulatedCount == PageLayout.getPopulatedCount(slottedPage)
        : "populatedCount drift: cached=" + cachedPopulatedCount + " segment=" + PageLayout.getPopulatedCount(slottedPage);
  }

  /**
   * Write raw slot data to the slotted page heap.
   * Used by setSlot() and addReferences() when slottedPage is active.
   * Data is stored without a length prefix — the directory entry holds the length.
   *
   * @param data       the raw slot data to store
   * @param slotNumber the slot index (0-1023)
   * @param nodeKindId the node kind ID (0 for legacy format, &gt;0 for flyweight)
   */
  private void setSlotToHeap(final MemorySegment data, final int slotNumber, final int nodeKindId) {
    final int recordSize = (int) data.byteSize();
    if (recordSize <= 0) {
      return;
    }

    // Total allocation includes DeweyID trailer when DeweyIDs are stored
    final int totalSize = areDeweyIDsStored
        ? recordSize + PageLayout.DEWEY_ID_TRAILER_SIZE
        : recordSize;

    // Ensure heap has enough space
    int heapEnd = cachedHeapEnd;
    final int remaining = slottedPageCapacity - PageLayout.HEAP_START - heapEnd;
    if (remaining < totalSize) {
      while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < totalSize) {
        growSlottedPage();
      }
      heapEnd = cachedHeapEnd;
    }

    // Bump-allocate and copy record data to heap
    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    MemorySegment.copy(data, 0, slottedPage, absOffset, recordSize);

    // Append DeweyID trailer (initially 0 = no DeweyID yet)
    if (areDeweyIDsStored) {
      PageLayout.writeDeweyIdTrailer(slottedPage, absOffset + totalSize, 0);
    }

    // Update heap end and used counters
    updateHeapEnd(heapEnd + totalSize);
    updateHeapUsed(cachedHeapUsed + totalSize);

    // Update directory entry with the provided nodeKindId
    PageLayout.setDirEntry(slottedPage, slotNumber, heapEnd, totalSize, nodeKindId);

    // Mark slot populated in bitmap and track last slot index (new slots only)
    if (!PageLayout.isSlotPopulated(slottedPage, slotNumber)) {
      PageLayout.markSlotPopulated(slottedPage, slotNumber);
      updatePopulatedCount(cachedPopulatedCount + 1);
      lastSlotIndex = slotNumber;
    }
  }

  /**
   * Write raw slot data from a source segment at a given offset to the slotted page heap.
   * Zero-copy variant for direct page deserialization.
   *
   * @param source       the source MemorySegment containing the data
   * @param sourceOffset byte offset within source where data starts
   * @param dataSize     number of bytes to copy
   * @param slotNumber   the slot index (0-1023)
   * @param nodeKindId   the node kind ID (0 for legacy format, 24-43 for flyweight)
   */
  void setSlotToHeapDirect(final MemorySegment source, final long sourceOffset,
      final int dataSize, final int slotNumber, final int nodeKindId) {
    if (dataSize <= 0) {
      return;
    }

    // Total allocation includes DeweyID trailer when DeweyIDs are stored — mirrors setSlotToHeap.
    // Without this, getRecordOnlyLength misreads the directory entry as record+trailer and
    // returns a negative recordLength, which downstream callers (e.g. getSlot) surface as null
    // and crash the SLIDING_SNAPSHOT page-combine path (UpdateTest regression seen on 6eaa56d25).
    final int totalSize = areDeweyIDsStored
        ? dataSize + PageLayout.DEWEY_ID_TRAILER_SIZE
        : dataSize;

    // Ensure heap has enough space
    int heapEnd = cachedHeapEnd;
    final int remaining = slottedPageCapacity - PageLayout.HEAP_START - heapEnd;
    if (remaining < totalSize) {
      while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < totalSize) {
        growSlottedPage();
      }
      heapEnd = cachedHeapEnd;
    }

    // Bump-allocate and copy data to heap
    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    MemorySegment.copy(source, sourceOffset, slottedPage, absOffset, dataSize);

    // Append DeweyID trailer (initially 0 = no DeweyID yet)
    if (areDeweyIDsStored) {
      PageLayout.writeDeweyIdTrailer(slottedPage, absOffset + totalSize, 0);
    }

    // Update heap end and used counters — by totalSize so the trailer is accounted for.
    updateHeapEnd(heapEnd + totalSize);
    updateHeapUsed(cachedHeapUsed + totalSize);

    // Directory entry length is totalSize (record + trailer); getRecordOnlyLength subtracts
    // the trailer + DeweyID payload to recover the record-only span on read.
    PageLayout.setDirEntry(slottedPage, slotNumber, heapEnd, totalSize, nodeKindId);

    // Mark slot populated in bitmap
    if (!PageLayout.isSlotPopulated(slottedPage, slotNumber)) {
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
    this.bytes = bytes;
    this.compressedSegment = null;
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
   * Set compressed page data as a MemorySegment (zero-copy path).
   * Clears the legacy bytes cache.
   *
   * @param segment the compressed segment (Arena.ofAuto()-managed)
   */
  public void setCompressedSegment(final MemorySegment segment) {
    this.compressedSegment = segment;
    this.bytes = null;
  }

  /**
   * Release node object references to allow GC to reclaim them.
   * <p>
   * MUST only be called after {@code addReferences()} has serialized all records into
   * {@code slotMemory} and the compressed form is cached via {@code setCompressedSegment()}
   * or {@code setBytes()}. After this call, individual records can still be reconstructed
   * on demand from {@code slotMemory} via {@code getSlot(offset)} in
   * {@link io.sirix.access.trx.page.NodeStorageEngineReader#getValue}.
   */
  public void clearRecordsForGC() {
    if (records == null) {
      return;
    }
    // Unbind flyweight nodes BEFORE clearing — cursors may still hold references.
    // Unbinding materializes all fields from page memory (still valid at this point)
    // into Java primitives, so reads after page release use correct field values.
    if (slottedPage != null) {
      for (final DataRecord record : records) {
        if (record instanceof FlyweightNode fn && fn.isBound()) {
          fn.unbind();
        }
      }
    }
    Arrays.fill(records, null);
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
    return (I) new ArrayIterator(r != null ? r : EMPTY_RECORDS, r != null ? r.length : 0);
  }

  public Map<Long, PageReference> getReferencesMap() {
    return references;
  }

  /**
   * Set reference to the complete page for lazy slot copying at commit time.
   * Used by DIFFERENTIAL, INCREMENTAL (full-dump), and SLIDING_SNAPSHOT versioning.
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
   * Mark a slot for preservation during lazy copy at commit time.
   * At addReferences(), if this slot has records[i] == null, it will be copied from completePageRef.
   *
   * @param slotNumber the slot number to mark for preservation (0 to Constants.NDP_NODE_COUNT-1)
   */
  public void markSlotForPreservation(int slotNumber) {
    ensureSlottedPage();
    PageLayout.markSlotPreserved(slottedPage, slotNumber);
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
   * Get the preservation bitmap for testing/debugging.
   * Returns a fresh copy from the slotted page MemorySegment.
   *
   * @return a fresh long[16] copy, or null if slotted page is not initialized
   */
  public long[] getPreservationBitmap() {
    if (slottedPage == null) {
      return null;
    }
    final long[] copy = new long[BITMAP_WORDS];
    for (int i = 0; i < BITMAP_WORDS; i++) {
      copy[i] = slottedPage.get(LE.LONG,
          PageLayout.PRESERVATION_BITMAP_OFF + ((long) i << 3));
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
    ensureSlottedPage();
    setSlotToHeap(MemorySegment.ofArray(recordData), slotNumber, 0);
  }

  @Override
  public void setSlot(MemorySegment data, int slotNumber) {
    ensureSlottedPage();
    setSlotToHeap(data, slotNumber, 0);
  }

  /**
   * Set slot data with an explicit nodeKindId. Used during page combining
   * to preserve the flyweight format indicator from the source page.
   *
   * @param data       the raw slot data to store
   * @param slotNumber the slot index (0-1023)
   * @param nodeKindId the node kind ID (0 for legacy, &gt;0 for flyweight)
   */
  public void setSlotWithNodeKind(final MemorySegment data, final int slotNumber, final int nodeKindId) {
    ensureSlottedPage();
    setSlotToHeap(data, slotNumber, nodeKindId);
  }

  /**
   * Get the nodeKindId for a slot from the slotted page directory.
   * Returns 0 if the slotted page is not initialized or the slot is unpopulated.
   *
   * @param slotNumber the slot index (0-1023)
   * @return the nodeKindId (&gt;0 for flyweight format, 0 for legacy)
   */
  public int getSlotNodeKindId(final int slotNumber) {
    if (slottedPage == null || !PageLayout.isSlotPopulated(slottedPage, slotNumber)) {
      return 0;
    }
    return PageLayout.getDirNodeKindId(slottedPage, slotNumber);
  }

  /**
   * Read a fused-named slot's nameKey directly from the slot bytes without binding
   * a flyweight singleton or moving a transaction cursor. Callers MUST verify
   * the slot is populated and holds a fused {@code OBJECT_NAMED_*} record (kindId 48-53)
   * — the method does no validation for the vectorized scan hot path.
   *
   * @param slotNumber the slot index (assumed populated + fused-named kind)
   * @return the signed nameKey from the slot
   */
  public int getObjectKeyNameKeyFromSlot(final int slotNumber) {
    final MemorySegment nameKeyPayload = regionPayload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
    if (nameKeyPayload != null) {
      return ObjectKeyNameKeyRegion.nameKeyForSlot(nameKeyPayload, slotNumber);
    }
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
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
   * Read the inline nameKey from a fused {@code OBJECT_NAMED_*} slot (kindIds 48-51).
   * Same varint layout as {@link #getObjectKeyNameKeyFromSlot} but different field count
   * (offset-table size varies between 8 and 9 across the four kinds). The accessor picks
   * up the correct field-count for the slot's kind from {@link NodeFieldLayout}.
   */
  public int getFusedObjectNamedNameKeyFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.FUSED_PRIMITIVE_NAME_KEY_FIELD) & 0xFF;
    final long dataStart = recordBase + 1 + fieldCount;
    return DeltaVarIntCodec.decodeSignedFromSegment(sp, dataStart + fieldOff);
  }

  /**
   * Read the inline nameKey from a fused structural {@code OBJECT_NAMED_OBJECT/ARRAY} slot
   * (kindIds 52/53). NAME_KEY is at field index 5 for these (vs index 3 for primitive-fused
   * 48-51). Caller must verify the slot holds a structural-fused record.
   */
  public int getFusedStructuralNameKeyFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.FUSED_STRUCTURAL_NAME_KEY_FIELD) & 0xFF;
    final long dataStart = recordBase + 1 + fieldCount;
    return DeltaVarIntCodec.decodeSignedFromSegment(sp, dataStart + fieldOff);
  }

  /**
   * True for the kinds (52, 53). Layout-dependent — these have a
   * 12-field offset table with NAME_KEY at index 5; do NOT use them on hot paths that assume
   * the primitive-fused layout (kindIds 48-51).
   */
  public static boolean isFusedStructuralKindId(final int kindId) {
    return kindId == FUSED_OBJECT_NAMED_OBJECT_KIND_ID
        || kindId == FUSED_OBJECT_NAMED_ARRAY_KIND_ID;
  }

  /**
   * Read the boolean payload of an OBJECT_NAMED_BOOLEAN slot (kindId 48) directly off
   * the slotted page. Caller must verify the slot holds an OBJECT_NAMED_BOOLEAN.
   */
  public boolean getFusedObjectNamedBooleanValueFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_VALUE) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_BOOLEAN_FIELD_COUNT;
    return sp.get(ValueLayout.JAVA_BYTE, dataStart + fieldOff) != 0;
  }

  /**
   * Decode the inline numeric value from an OBJECT_NAMED_NUMBER slot (kindId 49) directly
   * off the slotted page. Returns {@link Long#MIN_VALUE} if the payload's number type is
   * not Integer or Long (e.g. float/double/BigDecimal) — caller falls back to the cursor
   * path. Mirrors {@link #getNumberValueLongFromSlot} for the fused shape.
   *
   * <p>Caller must verify the slot holds {@code OBJECT_NAMED_NUMBER}.
   */
  public long getFusedObjectNamedNumberValueLongFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    // Payload is field index 8 (OBJNAMEDNUM_PAYLOAD).
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
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
   * Decode the inline value from an OBJECT_NAMED_NUMBER slot as a double, or {@link Double#NaN}
   * when the payload is not Double/Float-typed. NaN is a safe sentinel: JSON has no NaN literal,
   * so no stored value can collide with it. The companion of
   * {@link #getFusedObjectNamedNumberValueLongFromSlot}, for the values that method declines —
   * together they cover every numeric type the double-region writer can column-ize.
   */
  /** Written to {@code scaleOut[0]} when a slot holds no decimal this column can carry exactly. */
  public static final int DECIMAL_SCALE_UNAVAILABLE = Integer.MIN_VALUE;

  /**
   * Decode an OBJECT_NAMED_NUMBER slot's BigDecimal payload as its EXACT {@code (unscaled, scale)},
   * allocating nothing.
   *
   * <p>Returns the unscaled value and writes the scale to {@code scaleOut[0]}, or
   * {@link #DECIMAL_SCALE_UNAVAILABLE} when the slot cannot be carried exactly — a non-decimal
   * type, an unscaled magnitude wider than a {@code long}, or a scale outside
   * {@code [0, }{@link DoubleRegion#MAX_DECIMAL_SCALE}{@code ]}. A negative scale ({@code 1E+3})
   * is declined rather than normalized: rescaling it would multiply the unscaled value and can
   * overflow, and such literals are vanishingly rare in JSON.
   *
   * <h2>Why this exists next to {@link #getFusedObjectNamedNumberValueDoubleFromSlot}</h2>
   * That method answers the same payload as a {@code double}, and must return {@link Double#NaN}
   * whenever the double image is inexact — which is almost every real decimal, because only dyadic
   * rationals survive the conversion. Going straight to the unscaled integer skips the lossy hop
   * entirely, so a price like {@code 19.99} is carried exactly instead of being turned away.
   *
   * <h2>HFT</h2>
   * The double-typed path built a {@code byte[]}, a {@code BigInteger} and a {@code BigDecimal} per
   * value per page build, purely to answer "is this exact?". This reads the two's-complement bytes
   * straight into a {@code long} — no allocation, no GC pressure on page reconstruction.
   */
  public long getFusedObjectNamedNumberValueDecimalFromSlot(final int slotNumber,
      final int[] scaleOut) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
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
      scaleOut[0] = DECIMAL_SCALE_UNAVAILABLE;  // wider than a long — the record path keeps it
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
   * <p>The column a value joins is decided by its DECLARED type, exactly as {@code DECIMAL(P,S)} and
   * {@code DOUBLE} are separate physical columns in a relational engine — never by whether that one
   * value's double image happens to round-trip. Two decimals can share a double image, so a tag
   * mixing an exact-as-double decimal with an inexact one would otherwise be encoded over double
   * images, and a decimal predicate over it would put rows on the wrong side of the threshold.
   *
   * <p>Reads one byte off the slot's payload header; allocates nothing.
   *
   * <p>Caller must verify the slot holds {@code OBJECT_NAMED_NUMBER}.
   */
  public boolean isFusedObjectNamedNumberDecimalSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
    final long payloadStart =
        recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT + fieldOff;
    return sp.get(ValueLayout.JAVA_BYTE, payloadStart) == NUMBER_TYPE_BIG_DECIMAL;
  }

  public double getFusedObjectNamedNumberValueDoubleFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
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
        return Double.NaN;  // an unscaled value this wide has no exact double image anyway
      }
      final byte[] unscaled = new byte[(int) len];
      MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, pos, unscaled, 0, (int) len);
      final int scale = DeltaVarIntCodec.decodeSignedFromSegment(sp, pos + len);
      final BigDecimal bd = new BigDecimal(new BigInteger(unscaled), scale);
      final double d = bd.doubleValue();
      return Double.isFinite(d) && bd.compareTo(new BigDecimal(d)) == 0 ? d : Double.NaN;
    }
    return Double.NaN;
  }

  /**
   * Read the inline string bytes from an OBJECT_NAMED_STRING slot (kindId 50). Goes
   * through the thread-local {@code STRING_REGION_BUILD_SCRATCH} and returns a trimmed
   * copy. Caller must verify the slot holds {@code OBJECT_NAMED_STRING}.
   */
  /**
   * Whether the fused OBJECT_NAMED_STRING slot's stored payload bytes are FSST-encoded.
   *
   * <p>One flag-byte read; pairs with {@link #readFusedObjectNamedStringStoredBytes} for callers
   * that must see the stored form rather than the value — the region builder above all, whose
   * dictionaries mirror the heap verbatim so value elision stays a pure byte copy.
   */
  public boolean isFusedObjectNamedStringValueCompressed(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    if (sp == null || !PageLayout.isSlotPopulated(sp, slotNumber)) return false;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long payloadStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT + fieldOff;
    return sp.get(ValueLayout.JAVA_BYTE, payloadStart) == 1;
  }

  /**
   * The fused OBJECT_NAMED_STRING slot's stored payload bytes, verbatim — FSST-encoded when the
   * slot was compressed, raw otherwise — with no decode attempted.
   *
   * <p>{@link #readFusedObjectNamedStringBytes} answers "what is the value"; this answers "what
   * is stored". The region builder must use this one: its dictionary entries have to be
   * bit-identical to the heap so that eliding the heap copy and re-injecting it from the region
   * is a straight copy in both directions, decodable later through the page's symbol table by
   * whoever actually materialises the value.
   *
   * @return the stored bytes, or {@code null} for an absent/empty payload
   */
  public byte[] readFusedObjectNamedStringStoredBytes(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    if (sp == null || !PageLayout.isSlotPopulated(sp, slotNumber)) return null;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long payloadStart =
        recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT + fieldOff;
    final long lenOff = payloadStart + 1;
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(sp, lenOff);
    if (length <= 0) return null;
    final int lenBytes = DeltaVarIntCodec.readSignedVarintWidth(sp, lenOff);
    final byte[] out = new byte[length];
    MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, lenOff + lenBytes, out, 0, length);
    return out;
  }

  public byte[] readFusedObjectNamedStringBytes(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    if (sp == null || !PageLayout.isSlotPopulated(sp, slotNumber)) return null;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT;
    final long payloadStart = dataStart + fieldOff;
    // Payload layout: [isCompressed:1][length:varint][bytes].
    final boolean compressed = sp.get(ValueLayout.JAVA_BYTE, payloadStart) == 1;
    final long lenOff = payloadStart + 1;
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(sp, lenOff);
    if (length <= 0) return null;
    final int lenBytes = DeltaVarIntCodec.readSignedVarintWidth(sp, lenOff);
    final long dataOff = lenOff + lenBytes;
    if (!compressed) {
      final byte[] out = new byte[length];
      MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, dataOff, out, 0, length);
      return out;
    }
    final byte[][] symbols = fsstSymbols();
    if (symbols.length == 0) return null;
    // Share the legacy-path thread-local scratch so fused region-builds don't churn
    // the young generation with per-slot byte[] allocations (see agent review E2/R2).
    byte[] scratch = STRING_REGION_BUILD_SCRATCH.get();
    final int needed = Math.max(length << 3, 64);
    if (scratch.length < needed) {
      scratch = new byte[needed];
      STRING_REGION_BUILD_SCRATCH.set(scratch);
    }
    final int decoded = decodeFsstInto(sp, dataOff, length, symbols, scratch);
    if (decoded < 0) return null;
    final byte[] out = new byte[decoded];
    System.arraycopy(scratch, 0, out, 0, decoded);
    return out;
  }

  // ============== Phase 1 fused-structural getters (OBJECT_NAMED_OBJECT / OBJECT_NAMED_ARRAY) ==============
  // Phase 1 reserves the field accessors but no production path emits these kinds yet — the
  // getters are dormant. Each getter trusts the caller has already validated the slot's
  // kindId is 52 or 53. When P2 enables emission, callers can use these to read structural
  // fields from a slotted page without binding the flyweight node.

  /**
   * Read {@code firstChildKey} from a fused {@code OBJECT_NAMED_OBJECT} or
   * {@code OBJECT_NAMED_ARRAY} slot (kindIds 52/53). Both kinds share the field layout
   * defined by {@link NodeFieldLayout#OBJNAMEDOBJ_FIRST_CHILD_KEY}.
   *
   * @param slotNumber the populated slot index
   * @return the firstChildKey for the record at {@code slotNumber}
   */
  public long getFusedObjectNamedStructuralFirstChildKeyFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final long nodeKey = nodeKeyForSlot(slotNumber);
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_FIRST_CHILD_KEY) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, nodeKey);
  }

  /**
   * Read {@code lastChildKey} from a fused {@code OBJECT_NAMED_OBJECT} or
   * {@code OBJECT_NAMED_ARRAY} slot (kindIds 52/53).
   */
  public long getFusedObjectNamedStructuralLastChildKeyFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final long nodeKey = nodeKeyForSlot(slotNumber);
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_LAST_CHILD_KEY) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, nodeKey);
  }

  /**
   * Read {@code childCount} from a fused {@code OBJECT_NAMED_OBJECT} or
   * {@code OBJECT_NAMED_ARRAY} slot (kindIds 52/53).
   */
  public long getFusedObjectNamedStructuralChildCountFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_CHILD_COUNT) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
    return DeltaVarIntCodec.decodeSignedLongFromSegment(sp, dataStart + fieldOff);
  }

  /**
   * Read {@code descendantCount} from a fused {@code OBJECT_NAMED_OBJECT} or
   * {@code OBJECT_NAMED_ARRAY} slot (kindIds 52/53).
   */
  public long getFusedObjectNamedStructuralDescendantCountFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_DESCENDANT_COUNT) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
    return DeltaVarIntCodec.decodeSignedLongFromSegment(sp, dataStart + fieldOff);
  }

  /** Compute the per-slot record nodeKey: pageKeyBase derived from {@link #recordPageKey}
   *  shifted by the page-record exponent. Used by structural-fused getters that need the
   *  delta base to decode delta-varint fields. */
  private long nodeKeyForSlot(final int slotNumber) {
    return (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotNumber;
  }

  /**
   * Return the distinct OBJECT_KEY {@code nameKey}s present on this page.
   * Fast path: reads directly from the PAX dictKeys header (one VarHandle
   * load per distinct nameKey — typically 3 to 10 entries). Slow path
   * (region absent): iterates populated slots via the bitmap and
   * collects distinct nameKeys into a growable array.
   *
   * <p>Used by the page-skip index builder to decide which pages are
   * candidates for a given anchor nameKey, so analytical scans can skip
   * pages that hold no slot with that field instead of fetching each
   * page only to bail out on empty {@code getObjectKeySlotsForNameKey}.
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
    if (sp == null) return EMPTY_INT_ARRAY;
    int[] distinct = new int[8];
    int n = 0;
    for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
      if (!PageLayout.isSlotPopulated(sp, slot)) continue;
      final int heapOffset = PageLayout.getDirHeapOffset(sp, slot);
      final long recordBase = PageLayout.HEAP_START + heapOffset;
      final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
      if (!isFusedAnyObjectNamedKindId(kindId)) continue;
      final int nameKey = getObjectKeyNameKeyFromSlot(slot);
      // -1 is the not-a-named-slot sentinel; other negative values are
      // legitimate nameKeys (String hashes — 'active'/'amount' hash negative).
      if (nameKey == -1) continue;
      boolean seen = false;
      for (int i = 0; i < n; i++) {
        if (distinct[i] == nameKey) { seen = true; break; }
      }
      if (!seen) {
        if (n == distinct.length) distinct = Arrays.copyOf(distinct, distinct.length * 2);
        distinct[n++] = nameKey;
      }
    }
    if (n == 0) return EMPTY_INT_ARRAY;
    return n == distinct.length ? distinct : Arrays.copyOf(distinct, n);
  }

  /** Number payload type code for Integer (varint). See NodeKind.serializeNumber. */
  private static final byte NUMBER_TYPE_DOUBLE = 0;
  private static final byte NUMBER_TYPE_FLOAT = 1;
  private static final byte NUMBER_TYPE_BIG_DECIMAL = 5;
  private static final byte NUMBER_TYPE_INTEGER = 2;
  private static final byte NUMBER_TYPE_LONG = 3;
  private static final int NUMBER_VALUE_KIND_ID = 28;
  private static final int STRING_VALUE_KIND_ID = 30;
  /** Fused OBJECT_NAMED_* kind ids. Public so {@link PageKind} can dispatch on them. */
  public static final int FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID = 48;
  public static final int FUSED_OBJECT_NAMED_NUMBER_KIND_ID = 49;
  public static final int FUSED_OBJECT_NAMED_STRING_KIND_ID = 50;
  public static final int FUSED_OBJECT_NAMED_NULL_KIND_ID = 51;
  /** Phase 1 reserved fused-structural kindIds. Recognized by {@link
   *  #isFusedAnyObjectNamedKindId} but NOT by the iter#30
   *  {@link #isFusedObjectNamedKindId} primitive-only predicate, since the primitive-fused
   *  hot path assumes a 9-field layout with NAME_KEY at index 3 — the structural-fused
   *  records have a 12-field layout with NAME_KEY at index 5. */
  public static final int FUSED_OBJECT_NAMED_OBJECT_KIND_ID = 52;
  public static final int FUSED_OBJECT_NAMED_ARRAY_KIND_ID = 53;

  /**
   * True when {@code kindId} identifies a record whose value participates in the PAX
   * {@link RegionTable#KIND_NUMBER} region. Used by mutation paths to gate cache
   * invalidation: only number-affecting writes pay the (already-cheap) invalidation cost.
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
   * <p>Only the FUSED kind qualifies, and deliberately so: {@link #collectAndEncodeBooleanRegion}
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
   * distribution and copy paths share the reference) also share one parsed {@code byte[][]},
   * which is the identity the encode-side matcher cache keys on.
   */
  private volatile byte[][] parsedFsstSymbols;
  private static final byte[][] EMPTY_FSST_SYMBOLS = new byte[0][];

  /**
   * The page's parsed FSST symbols for direct-byte consumers (vectorized scans decoding region
   * dictionary entries). Empty when the page has no resolved table — callers that meet a
   * compressed entry with empty symbols must fall back to a record-level read, which resolves.
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
   * Decode raw bytes if this page's values are FSST-compressed; otherwise
   * copy verbatim. {@code in[0..inLen)} is the raw bytes; decoded output
   * goes to {@code out[outOff..)}. Returns decoded length or -1 on failure.
   * Fixed-in-place decode: safe when {@code in == out && outOff == 0} for
   * non-compressed passthrough (we write the same bytes). For compressed
   * input, caller should pass distinct buffers or accept in-place overwrite
   * since FSST expands (decoded ≥ encoded).
   */
  public int decodeRawIfCompressed(final byte[] in, final int inLen,
      final byte[] out, final int outOff) {
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
    final byte[] tmp = in == out ? new byte[inLen * 3 + 8] : null;
    final byte[] dst = tmp != null ? tmp : out;
    final int dstOff = tmp != null ? 0 : outOff;
    int outPos = dstOff;
    for (int pos = 0; pos < inLen; ) {
      final int b = in[pos++] & 0xFF;
      if (b == 0xFF) {
        if (pos >= inLen || outPos >= dst.length) return -1;
        dst[outPos++] = in[pos++];
      } else if (b < symbols.length) {
        final byte[] sym = symbols[b];
        final int sl = sym.length;
        if (outPos + sl > dst.length) return -1;
        System.arraycopy(sym, 0, dst, outPos, sl);
        outPos += sl;
      } else {
        return -1;
      }
    }
    final int decLen = outPos - dstOff;
    if (tmp != null) {
      if (decLen > out.length - outOff) return -1;
      System.arraycopy(tmp, 0, out, outOff, decLen);
    }
    return decLen;
  }

  /**
   * Thread-local staging buffer for FSST-compressed source bytes — one
   * bulk copy from the MemorySegment into this scratch avoids N byte-
   * sized {@code sp.get} calls inside {@link #decodeFsstInto}, which
   * profile-dominated via MemorySegment safety-check overhead
   * (isAlignedForElement / checkValidStateRaw / VarHandle dispatch).
   */
  private static final ThreadLocal<byte[]> FSST_SRC_BUF =
      ThreadLocal.withInitial(() -> new byte[512]);

  /**
   * Decode {@code length} FSST-compressed bytes starting at {@code dataOff}
   * of {@code sp} into {@code scratch}. Mirrors
   * {@code FSSTCompressor.decodeRawCompressed} but reads from a
   * MemorySegment and writes into a caller-provided buffer — no allocation.
   * Returns decoded byte count, or -1 if output overflows.
   *
   * <p>Copies the compressed source into a thread-local byte[] via one
   * {@link MemorySegment#copy} up front so the symbol-dispatch loop reads
   * plain array bytes instead of paying per-byte MemorySegment safety
   * checks (alignment/session/bounds). For short FSST payloads — typical
   * of JSON string columns — the bulk copy is essentially free and the
   * tight array loop JITs cleanly.
   */
  private static int decodeFsstInto(final MemorySegment sp, final long dataOff,
      final int length, final byte[][] symbols, final byte[] scratch) {
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
   * ({@code OBJECT_NAMED_OBJECT}/{@code OBJECT_NAMED_ARRAY}, kindIds 52/53)
   * without moving any cursor or binding a singleton. Returns the raw nodeKey.
   *
   * <p>Phase 4 — the legacy OBJECT_KEY (kindId 26) record is gone, so this helper
   * now reads from fused-structural slots only. Primitive-fused records (48-51)
   * carry NO firstChild and callers must short-circuit on those.
   *
   * <p>Caller must verify the slot holds a fused-structural record first.
   */
  public long getObjectKeyFirstChildKeyFromSlot(final int slotNumber, final long objectKeyNodeKey) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
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
   * Read the delta-encoded parentKey (enclosing OBJECT's nodeKey) from any fused-named
   * slot without moving any cursor or binding a singleton.
   * Decoded directly off the slotted page so the vectorized scan can join
   * sibling fields in Pass 2 by parent-OBJECT nodeKey in O(1) per slot.
   *
   * <p>All fused-named layouts (primitive 48-51 and structural 52-53) place PARENT_KEY
   * at field-table index 0; only the field-count differs.
   *
   * <p>Caller must verify the slot holds a fused-named record; no validation is
   * performed to keep the hot path branch-free beyond the kind-id dispatch that
   * selects the field-count constant.
   *
   * @param slotNumber       the slot index (assumed populated + fused-named kind)
   * @param objectKeyNodeKey the slot's nodeKey (base + slotNumber) — the
   *                         delta-decoder reconstructs parentKey against it
   * @return parentKey (enclosing OBJECT nodeKey); {@code -1L} if the page was
   *         evicted mid-scan
   */
  public long getObjectKeyParentKeyFromSlot(final int slotNumber, final long objectKeyNodeKey) {
    final MemorySegment sp = slottedPage;
    if (sp == null) {
      return -1L;
    }
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
    if (fieldCount <= 0) {
      return -1L;
    }
    // PARENT_KEY = field index 0 across all fused-named layouts.
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1) & 0xFF;
    final long dataStart = recordBase + 1 + fieldCount;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, objectKeyNodeKey);
  }

  /**
   * Read the pathNodeKey stored on a fused-named slot — the fully-qualified
   * path identifier pointing into the PathSummary. Decoded directly off the
   * slotted page without cursor movement or singleton binding, so the
   * vectorized scan can filter matched slots by scope in O(1) per slot.
   *
   * <p>Phase 4 — the legacy OBJECT_KEY (kind 26) and OBJECT_KEY_PAX (kind 126) records
   * have been removed; this helper now dispatches purely on fused-named layouts.
   *
   * <p>Caller must verify the slot holds a fused-named record; no validation is
   * performed to keep the per-slot cost down to a single byte read for the
   * offset and a varint decode for the value.
   *
   * @param slotNumber       the slot index (assumed populated + fused-named kind)
   * @param objectKeyNodeKey the slot's nodeKey (base + slotNumber) — the
   *                         delta-decoder reconstructs pathNodeKey against it
   * @return the pathNodeKey; {@code 0L} if the slot has no path statistics
   *         (resource opened without path summary)
   */
  public long getObjectKeyPathNodeKeyFromSlot(final int slotNumber, final long objectKeyNodeKey) {
    final MemorySegment sp = slottedPage;
    if (sp == null) {
      // Page was evicted from the cache while a scan was holding a reference.
      // Signal unresolvable; callers either skip the slot or retry the page.
      return -1L;
    }
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
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
   * Bulk-decode the {@code pathNodeKey}, {@code parentKey}, and
   * {@code firstChildKey} columns for {@code count} OBJECT_KEY slots in one
   * tight loop. Mirrors the semantics of
   * {@link #getObjectKeyPathNodeKeyFromSlot},
   * {@link #getObjectKeyParentKeyFromSlot}, and
   * {@link #getObjectKeyFirstChildKeyFromSlot}, but:
   *
   * <ul>
   *   <li>Hoists the slotted-page null check + session checks out of the
   *       per-slot loop so the JIT can peel loop-invariant guards.</li>
   *   <li>Shares the heap-offset lookup and record base across the three
   *       varint decodes per slot — dropping two byte reads that each of
   *       the three getters would pay independently.</li>
   *   <li>Probes the kind id once per slot so a page that happens to mix
   *       dense and PAX OBJECT_KEY encodings (same kind family, different
   *       field layout) stays correct.</li>
   * </ul>
   *
   * <p>If the page has been evicted mid-scan ({@code slottedPage == null}),
   * the method fills the output arrays with {@code -1L} so callers can skip
   * the slot via the same sentinel contract as the per-slot getters.
   *
   * <p>CPU profile on 10M cold filterCount showed the three per-slot getters
   * accounting for ~4.5% of the worker thread. A single bulk call in the
   * scan driver (see {@code SirixVectorizedExecutor.collectColumns}) removes
   * the per-iteration method-dispatch + per-call MemorySegment session
   * checks so the JIT can keep the tight inner loop in registers.
   *
   * @param slots             slot indices to decode (valid for {@code 0..count})
   * @param count             number of slots to decode
   * @param pageBase          base nodeKey for this page (pageKey {@literal <<}
   *                          {@link Constants#INP_REFERENCE_COUNT_EXPONENT})
   * @param outPathNodeKeys   result column — pathNodeKey per slot, sized
   *                          {@code >= count}. Values match
   *                          {@link #getObjectKeyPathNodeKeyFromSlot}.
   * @param outParentKeys     result column — parentKey per slot.
   * @param outFirstChildKeys result column — firstChildKey per slot.
   */
  public void bulkDecodeObjectKeyColumns(final int[] slots, final int count, final long pageBase,
      final long[] outPathNodeKeys, final long[] outParentKeys, final long[] outFirstChildKeys) {
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
      final int heapOffset = PageLayout.getDirHeapOffset(sp, slot);
      final long recordBase = PageLayout.HEAP_START + heapOffset;
      final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
      final long offsetTable = recordBase + 1;
      if (isFusedObjectNamedKindId(kindId)) {
        // Primitive-fused: parentKey at field 0, pathNodeKey at field 4, no firstChildKey.
        final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
        final long dataStart = offsetTable + fieldCount;
        final int parentFieldOff =
            sp.get(ValueLayout.JAVA_BYTE, offsetTable + 0) & 0xFF;
        final int pathFieldOff =
            sp.get(ValueLayout.JAVA_BYTE, offsetTable + 4) & 0xFF;
        outParentKeys[i] =
            DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + parentFieldOff, nodeKey);
        outFirstChildKeys[i] = -1L;
        outPathNodeKeys[i] =
            DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + pathFieldOff, nodeKey);
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
        outParentKeys[i] =
            DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + parentFieldOff, nodeKey);
        outFirstChildKeys[i] =
            DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + firstChildFieldOff, nodeKey);
        outPathNodeKeys[i] =
            DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + pathFieldOff, nodeKey);
        continue;
      }
      // Non-fused-named slot: surface sentinel.
      outParentKeys[i] = -1L;
      outFirstChildKeys[i] = -1L;
      outPathNodeKeys[i] = -1L;
    }
  }

  /**
   * Two-column variant of {@link #bulkDecodeObjectKeyColumns} that skips the
   * {@code pathNodeKey} decode — used by {@code collectColumns} pass 2,
   * which only needs {@code parentKey} (for the batch parent-row join) and
   * {@code firstChildKey} (for the sibling value read). Decoding the
   * third column there would waste one varint read per sibling slot per
   * field per page.
   */
  public void bulkDecodeObjectKeyParentAndChildKeys(final int[] slots, final int count,
      final long pageBase, final long[] outParentKeys, final long[] outFirstChildKeys) {
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
      final int heapOffset = PageLayout.getDirHeapOffset(sp, slot);
      final long recordBase = PageLayout.HEAP_START + heapOffset;
      final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
      final long offsetTable = recordBase + 1;
      if (isFusedObjectNamedKindId(kindId)) {
        final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
        final long dataStart = offsetTable + fieldCount;
        final int parentFieldOff =
            sp.get(ValueLayout.JAVA_BYTE, offsetTable + 0) & 0xFF;
        outParentKeys[i] =
            DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + parentFieldOff, nodeKey);
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
        outParentKeys[i] =
            DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + parentFieldOff, nodeKey);
        outFirstChildKeys[i] =
            DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + firstChildFieldOff, nodeKey);
        continue;
      }
      outParentKeys[i] = -1L;
      outFirstChildKeys[i] = -1L;
    }
  }


  /**
   * Cache of matching-slot arrays keyed by nameKey (primitive int → int[], no
   * Integer boxing). Built lazily the first time a vectorized scan asks for a
   * given field, reused by every subsequent query on the same page. Memory:
   * one int[] per distinct queried nameKey; for a JSON-array workload that's
   * typically ~5 arrays of ~150 entries each.
   *
   * <p>Immutable once built. For a read-only resource session the page's
   * content doesn't change, so invalidation isn't needed.
   */
  private volatile Int2ObjectOpenHashMap<int[]> objectKeySlotsByName;
  private static final int[] EMPTY_INT_ARRAY = new int[0];

  /** Thread-local scratch for SIMD findMatchingSlots output. Avoids per-page int[] alloc. */
  private static final ThreadLocal<int[]> MATCHING_SLOTS_SCRATCH =
      ThreadLocal.withInitial(() -> new int[256]);

  /**
   * {@code true} when the slot kind is a fused {@code OBJECT_NAMED_*} record (kindIds 48-51).
   * Fused records play the OBJECT_KEY role and carry the nameKey inline, so
   * {@link #getObjectKeySlotsForNameKey} includes them when searching by name.
   */
  public static boolean isFusedObjectNamedKindId(final int kindId) {
    return kindId >= FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID && kindId <= FUSED_OBJECT_NAMED_NULL_KIND_ID;
  }

  /**
   * {@code true} when the slot kind is ANY fused named record — the iter#30 primitive-fused
   * leaves (kindIds 48-51, see {@link #isFusedObjectNamedKindId}) OR the Phase 1 reserved
   * structural-fused kinds (52, 53). Used by predicates that classify "any record carrying
   * both a fieldname and inline payload/sub-tree" without assuming the primitive-leaf field
   * layout. Phase 1 doesn't emit 52/53, so this predicate is dormant on the wire path.
   */
  public static boolean isFusedAnyObjectNamedKindId(final int kindId) {
    return kindId >= FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID && kindId <= FUSED_OBJECT_NAMED_ARRAY_KIND_ID;
  }

  /**
   * Return the slot indices whose OBJECT_KEY has the given nameKey. Single-pass
   * bitmap walk the first time; array reuse thereafter. Borrowed from DuckDB /
   * ClickHouse column pre-scan: pay the per-slot decode cost once, amortize
   * across the many scans any realistic analytical query does.
   *
   * <p>Zero-allocation on the hot path once built — all subsequent calls just
   * return the cached int[].
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
    if (cached != null) return cached;
    return buildObjectKeySlotsForNameKey(cache, fieldKey);
  }

  private int[] buildObjectKeySlotsForNameKey(final Int2ObjectOpenHashMap<int[]> cache,
      final int fieldKey) {
    final MemorySegment sp = slottedPage;
    if (sp == null) return EMPTY_INT_ARRAY;

    // Fast path: ObjectKeyNameKeyRegion lets us SIMD-scan the dict-encoded nameKey
    // column instead of walking every populated slot, decoding kind-id, and decoding
    // the per-record nameKey via varint. Profile (Temurin 25, 100M records) showed
    // ObjectKeyNameKeyRegion.nameKeyForSlot at ~8% CPU on the slot-walk path; the
    // findMatchingSlots SIMD scan replaces all of that with one tight ByteVector loop.
    final MemorySegment nameKeyPayload = regionPayload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
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
      final int matched = ObjectKeyNameKeyRegion.findMatchingSlots(
          nameKeyPayload, fieldKey, tmp);
      if (matched == 0) return cachePut(cache, fieldKey, EMPTY_INT_ARRAY);
      final int[] result = Arrays.copyOf(tmp, matched);
      return cachePut(cache, fieldKey, result);
    }

    // Slow path (region absent): walk populated-slot bitmap in-line (direct bit scan inlines
    // cleanly, no lambda). Phase 4 — only fused OBJECT_NAMED_* records (kindIds 48-53) carry
    // an inline nameKey; legacy OBJECT_KEY (26) is gone.
    int[] buf = new int[32];
    int count = 0;
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
          // Structural-fused (52, 53) — different layout (NAME_KEY at field index 5 vs 3 for
          // primitive-fused), so use the dedicated accessor.
          matches = getFusedStructuralNameKeyFromSlot(slot) == fieldKey;
        }
        if (matches) {
          if (count == buf.length) {
            final int[] grown = new int[buf.length << 1];
            System.arraycopy(buf, 0, grown, 0, count);
            buf = grown;
          }
          buf[count++] = slot;
        }
        word &= word - 1;
      }
    }
    final int[] result = (count == buf.length) ? buf : Arrays.copyOf(buf, count);
    return cachePut(cache, fieldKey, result);
  }

  private static int[] cachePut(final Int2ObjectOpenHashMap<int[]> cache, final int fieldKey,
      final int[] result) {
    synchronized (cache) {
      final int[] existing = cache.get(fieldKey);
      if (existing != null) return existing;
      cache.put(fieldKey, result);
    }
    return result;
  }

  /**
   * Set slot data by copying directly from a source MemorySegment.
   * Zero-copy path for page deserialization.
   *
   * @param source the source MemorySegment containing the data
   * @param sourceOffset the byte offset within source where data starts
   * @param dataSize the number of bytes to copy (must be &gt; 0)
   * @param slotNumber the slot number (0 to Constants.NDP_NODE_COUNT-1)
   */
  public void setSlotDirect(MemorySegment source, long sourceOffset, int dataSize, int slotNumber) {
    ensureSlottedPage();
    setSlotToHeapDirect(source, sourceOffset, dataSize, slotNumber, 0);
  }



  public int getLastSlotIndex() {
    return lastSlotIndex;
  }



  /**
   * Get the slot bitmap for O(k) iteration over populated slots.
   * Returns a mutable copy — callers may modify the returned array without
   * affecting page state. Each call allocates a fresh array.
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
   * Check if a specific slot is populated using the bitmap.
   * This is O(1) and avoids memory access to slotOffsets.
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
   * This enables efficient iteration over only populated slots instead of
   * iterating all 1024 slots and checking for null. For sparse pages with
   * k populated slots, this is O(k) instead of O(1024).
   * <p>
   * Note: This allocates a new array on each call. For hot paths where the
   * same page is iterated multiple times, consider using {@link #forEachPopulatedSlot}.
   * <p>
   * Example usage:
   * <pre>{@code
   * int[] slots = page.populatedSlots();
   * for (int i = 0; i < slots.length; i++) {
   *     int slot = slots[i];
   *     MemorySegment data = page.getSlot(slot);
   *     // process data - no null check needed
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
      if (slottedPage == null) break;
      final long word = PageLayout.getBitmapWord(slottedPage, wordIndex);
      long remaining = word;
      final int baseSlot = wordIndex << 6;  // wordIndex * 64
      while (remaining != 0) {
        final int bit = Long.numberOfTrailingZeros(remaining);
        result[idx++] = baseSlot + bit;
        remaining &= remaining - 1;  // Clear lowest set bit
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
     * @param slotIndex the slot index
     * @return true to continue iteration, false to stop early
     */
    boolean accept(int slotIndex);
  }
  
  /**
   * Zero-allocation iteration over populated slots.
   * <p>
   * This method iterates over populated slots without allocating any arrays.
   * The consumer returns false to stop iteration early.
   * <p>
   * Example usage:
   * <pre>{@code
   * page.forEachPopulatedSlot(slot -> {
   *     MemorySegment data = page.getSlot(slot);
   *     // process data
   *     return true;  // continue iteration
   * });
   * }</pre>
   * 
   * @param consumer the consumer to process each populated slot
   * @return the number of slots processed
   */
  public int forEachPopulatedSlot(SlotConsumer consumer) {
    int processed = 0;
    for (int wordIndex = 0; wordIndex < BITMAP_WORDS; wordIndex++) {
      if (slottedPage == null) break;
      long word = PageLayout.getBitmapWord(slottedPage, wordIndex);
      final int baseSlot = wordIndex << 6;  // wordIndex * 64
      while (word != 0) {
        final int bit = Long.numberOfTrailingZeros(word);
        final int slot = baseSlot + bit;
        processed++;
        if (!consumer.accept(slot)) {
          return processed;
        }
        word &= word - 1;  // Clear lowest set bit
      }
    }
    return processed;
  }

  /**
   * Get the count of populated slots using SIMD-accelerated population count.
   * Uses Vector API for parallel bitCount across multiple longs.
   * This is O(BITMAP_WORDS / SIMD_WIDTH) instead of O(1024).
   * 
   * @return number of populated slots
   */
  public int populatedSlotCount() {
    return slottedPage != null ? PageLayout.countPopulatedSlots(slottedPage) : 0;
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
   * SIMD-accelerated bitmap OR into destination array.
   * Computes: dest[i] |= src[i] for all bitmap words.
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
   * Check if any bits in src are NOT set in dest using SIMD.
   * Returns true if there exist slots in src that are not yet in dest.
   * Useful for early termination in page combining.
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
   * Get the slotted page MemorySegment for serialization.
   * When non-null, the page uses LeanStore-style heap storage instead of legacy slotMemory.
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
   * Set the slotted page MemorySegment (used during deserialization).
   * Releases any previously allocated slotted page.
   *
   * @param slottedPage the slotted page segment
   */
  public void setSlottedPage(final MemorySegment newSlottedPage) {
    // Release old slotted page if different from the new one
    if (this.slottedPage != null && this.slottedPage != newSlottedPage) {
      segmentAllocator.release(this.slottedPage.reinterpret(slottedPageCapacity));
    }
    this.slottedPageCapacity = (int) newSlottedPage.byteSize();
    this.slottedPage = newSlottedPage.reinterpret(slottedPageCapacity); // capacity-bounded view
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
    return slottedPage != null ? cachedHeapUsed : 0;
  }

  public int getSlotMemoryByteSize() {
    return slottedPage != null ? PageLayout.HEAP_START + cachedHeapEnd : 0;
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
    final int heapOffset = PageLayout.getDirHeapOffset(slottedPage, slotNumber);
    // Use record-only length (excludes inline DeweyID data + 2-byte trailer)
    final int recordLength = PageLayout.getRecordOnlyLength(slottedPage, slotNumber);
    if (recordLength <= 0) {
      return null;
    }
    return slottedPage.asSlice(PageLayout.HEAP_START + heapOffset, recordLength);
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
   * Format: [record data][deweyId data][deweyIdLen:2 bytes (u16)].
   * The old allocation becomes dead heap space.
   */
  private void setDeweyIdToHeap(final MemorySegment deweyId, final int slotNumber) {
    final int deweyIdLen = (int) deweyId.byteSize();
    if (deweyIdLen == 0) {
      return;
    }

    final boolean slotExists = PageLayout.isSlotPopulated(slottedPage, slotNumber);
    final int oldDataLength;
    final int recordLen;
    final int nodeKindId;
    final long oldAbsStart;

    if (slotExists) {
      // Existing slot — read current allocation info
      final int oldHeapOffset = PageLayout.getDirHeapOffset(slottedPage, slotNumber);
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

    // Ensure heap has enough space
    int heapEnd = cachedHeapEnd;
    int remaining = slottedPageCapacity - PageLayout.HEAP_START - heapEnd;
    while (remaining < newTotalLen) {
      growSlottedPage();
      heapEnd = cachedHeapEnd;
      remaining = slottedPageCapacity - PageLayout.HEAP_START - heapEnd;
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

    // Mark slot populated if new
    if (!slotExists) {
      PageLayout.markSlotPopulated(slottedPage, slotNumber);
      updatePopulatedCount(cachedPopulatedCount + 1);
    }
  }

  @Override
  public MemorySegment getDeweyId(int offset) {
    if (slottedPage == null || !PageLayout.isSlotPopulated(slottedPage, offset)) {
      return null;
    }
    return PageLayout.getDeweyId(slottedPage, offset);
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
  public <C extends KeyValuePage<DataRecord>> C newInstance(long recordPageKey,
      IndexType indexType, StorageEngineReader storageEngineReader) {
    final ResourceConfiguration config = storageEngineReader.getResourceSession().getResourceConfig();
    return (C) new KeyValueLeafPage(
        recordPageKey,
        indexType,
        config,
        storageEngineReader.getRevisionNumber(),
        null,
        null,
        false
    );
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
    return getNumberOfNonNullEntries() + references.size();
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

  // Leak detection lives in LeakDetectorState above (registered with LEAK_CLEANER in
  // each constructor when DEBUG_MEMORY_LEAKS is on). The deprecated finalize() override
  // was removed — Cleaner is the sanctioned post-Java-9 replacement: it doesn't run on
  // the GC thread, doesn't resurrect objects, and survives finalize() being removed in
  // a future JDK. close() flips the LeakDetectorState.closed flag so the Cleaner action
  // skips the leak log on a properly-closed page.

  /**
   * Closes this page and releases associated memory resources.
   * <p>
   * This method is thread-safe and idempotent. If the page has active guards
   * (indicating it's in use by a transaction), the close operation is skipped
   * to prevent data corruption.
   * <p>
   * Memory segments allocated by the global allocator are returned to the pool.
   * Externally allocated memory (e.g., test arenas) is not released.
   * <p>
   * For zero-copy pages, the backing buffer (from decompression) is released
   * via the backingBufferReleaser callback.
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
        LOGGER.debug("Close skipped for guarded page: pageKey={}, type={}, guardCount={}",
            recordPageKey, indexType, currentGuardCount);
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
      closeSite = new Throwable(
          "page " + recordPageKey + " (" + indexType + ", rev=" + revision + ") closed by thread "
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
      PAGES_CLOSED_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
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
      if (records != null) {
        for (final DataRecord record : records) {
          if (record instanceof FlyweightNode fn && fn.isBound()) {
            fn.unbind();
          }
        }
      }
      try {
        segmentAllocator.release(slottedPage.reinterpret(slottedPageCapacity));
      } catch (Throwable e) {
        LOGGER.debug("Failed to release slotted page for page {}: {}", recordPageKey, e.getMessage());
      }
      slottedPage = null;
      slottedPageCapacity = 0;
    }

    clearFsstBinding();

    // Clear references to aid garbage collection
    if (records != null) {
      Arrays.fill(records, null);
    }
    references.clear();
    bytes = null;
    compressedSegment = null;
    hashCode = null;
  }

  /**
   * Get the actual memory size used by this page's memory segments.
   * Used for accurate Caffeine cache weighing.
   * 
   * @return Total size in bytes of all memory segments used by this page
   */
  public long getActualMemorySize() {
    return slottedPage != null ? slottedPageCapacity : 0;
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
   * Drop the FSST binding as one unit — table bytes, id, and parsed cache. Split clearing is
   * how stale-binding bugs happen: a surviving id with no bytes claims an encoding the page no
   * longer holds, and a pooled frame's next occupant would trip the rebind guard on it.
   */
  private void clearFsstBinding() {
    fsstSymbolTable = null;
    fsstSymbolTableId = NO_FSST_SYMBOL_TABLE_ID;
    parsedFsstSymbols = null;
  }

  /**
   * The id of the symbol table this page's strings were encoded against, or
   * {@link #NO_FSST_SYMBOL_TABLE_ID} when the page carries no reference.
   *
   * <p>A page holds the id rather than the table because the table lives in the dictionary trie,
   * shared by every page of the revision. It is resolved on the first string the page is asked to
   * decode — deserialization has no storage-engine reader to walk the trie with, and a page whose
   * strings are never read should not pay for the lookup.
   *
   * @return the symbol table id, or {@link #NO_FSST_SYMBOL_TABLE_ID}
   */
  public long getFsstSymbolTableId() {
    return fsstSymbolTableId;
  }

  /**
   * Record which symbol table this page's strings were encoded against. A binding is set at
   * most once per page life; it is cleared only by the page-recycling paths ({@code reset},
   * teardown), never through this setter — "unbind" has no meaning while encoded bytes remain.
   *
   * @param id the symbol table id; must be positive
   * @throws IllegalArgumentException if {@code id} is not positive
   * @throws IllegalStateException if the page is already bound to a different table
   */
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
   * Returns the PAX region table. {@code null} indicates V0 format or that no
   * regions have been attached. Callers on the hot path should prefer
   * {@link #regionPayload(byte)} to avoid a nullcheck at each slot read.
   */
  public RegionTable getRegionTable() {
    return regionTable;
  }

  public void setRegionTable(final RegionTable table) {
    // A wholesale table swap makes every previous derivation verdict meaningless — the memo must
    // restart with the new table or it would claim regions of the OLD table were attempted.
    clearRegionDeriveAttempted(~0);
    this.regionTable = table;
  }

  /**
   * Direct payload lookup for a region kind. Returns {@code null} when no table
   * is present or the region is absent. Inlineable one-branch hot-path shim.
   */
  public MemorySegment regionPayload(final byte kind) {
    final RegionTable t = regionTable;
    return t == null ? null : t.payload(kind);
  }

  /**
   * Lazily parsed number-region header. {@code null} if the page has no number
   * region (e.g. path-summary pages, index pages, pages with no numeric values).
   * Cached after first parse — zero allocation on subsequent calls.
   */
  private volatile NumberRegion.Header cachedNumberHeader;

  /**
   * Drop the cached {@link NumberRegion.Header} and every payload the number builder installs —
   * {@link RegionTable#KIND_NUMBER}, {@link RegionTable#KIND_NUMBER_ZONEMAP} and
   * {@link RegionTable#KIND_DOUBLE}, i.e. exactly {@code NUMBER_DERIVE_MASK} — so the next reader
   * rebuilds from the slotted page. Called from every mutation path that adds, modifies, or
   * removes a NUMBER_VALUE / OBJECT_NAMED_NUMBER record.
   *
   * <h2>HFT cost model</h2>
   * Steady-state cost when no region is currently cached: one volatile read + one branch.
   * On the first invalidation per page after a region was built: one volatile write +
   * one payload-slot store. After that, calls collapse to the fast-path until
   * the next reader rebuilds.
   *
   * <p>Package-private so unit tests can verify the contract without reflection.
   */
  void invalidateNumberRegion() {
    final RegionTable rt = regionTable;
    // Presence is asked of the table rather than inferred from the cached header: a caller can
    // reach the payload without ever parsing a header, and inferring from the header alone would
    // leave such a region installed across a mutation that invalidated it.
    // Probed over the WHOLE derive mask: the builder installs the long column, its zone map and the
    // double column together, and an all-double page carries KIND_DOUBLE alone.
    final boolean present = rt != null
        && (rt.hasRegion(RegionTable.KIND_NUMBER) || rt.hasRegion(RegionTable.KIND_NUMBER_ZONEMAP)
            || rt.hasRegion(RegionTable.KIND_DOUBLE));
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

  /** Atomically clear derive-memo bits; the read-modify-write needs the same monitor the setters hold. */
  private synchronized void clearRegionDeriveAttempted(final int mask) {
    regionDeriveAttempted &= ~mask;
  }

  /**
   * Drop every column region a record of {@code nodeKindId} contributes a row to.
   *
   * <p>The single definition of "which columns does this kind feed", shared by all five mutation
   * entry points. It exists because the dispatch used to be copied per site as a number/string
   * if-else chain, and a copied chain is how a region ends up with a drop-set narrower than its
   * derive-set: the chain was exclusive, yet a fused {@code OBJECT_NAMED_NUMBER} row belongs to the
   * number column AND to the field-name column, so at most one of the two was ever dropped.
   *
   * <p>The value columns are mutually exclusive by kind, so they stay an if-else. The names column
   * is not: every fused {@code OBJECT_NAMED_*} record is one of its rows whatever its value type,
   * and its row POSITION is what {@link RegionTable#KIND_RECORD_ORDINAL} indexes — the alignment a
   * fused cross-column predicate trusts. A stale ordinal column is a wrong answer rather than a
   * stale bound, so it falls with the names it indexes.
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
   * Invalidate every column region the record CURRENTLY in {@code slotOffset} belongs to. Called
   * from {@link #setRecord} and {@link #setNewRecord} before mutation, so deletion or replacement
   * of an existing column-bearing record is detected even when the new record kind is something
   * else (e.g. {@code DeletedNode}).
   *
   * <p>The "new record IS column-bearing" case is handled separately by {@link #serializeToHeap}
   * and {@link #completeDirectWrite} which already know the kind id being written.
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
    if (regionTable == null && cachedNumberHeader == null && cachedStringHeader == null
        && regionDeriveAttempted == 0) {
      return;
    }
    final MemorySegment sp = slottedPage;
    if (sp == null || !PageLayout.isSlotPopulated(sp, slotOffset)) {
      return;
    }
    invalidateRegionsForKindId(PageLayout.getDirNodeKindId(sp, slotOffset));
  }

  public NumberRegion.Header getNumberRegionHeader() {
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
    synchronized (this) {
      h = cachedNumberHeader;
      if (h == null) {
        h = new NumberRegion.Header().parseInto(payload);
        cachedNumberHeader = h;
      }
    }
    return h;
  }

  /**
   * Ensure the number region is attached. Called from the versioning layer's
   * {@code combineRecordPages} after a new KVLP has been reconstructed from
   * one or more fragments. The argument carries the donor page's region —
   * typically the first (or only) fragment.
   *
   * <p><b>Caller contract.</b> The donor shortcut — copying
   * {@code donor.regionTable} by reference — is only correct when the target
   * is a byte-identical copy of the donor (i.e. single-fragment combine). For
   * multi-fragment combines the caller <b>must</b> pass {@code null} (or use
   * {@link #ensureNumberRegion()}) so the region is rebuilt from the combined
   * slotted heap. Passing a donor in a multi-fragment context silently
   * corrupts aggregates that lean on the PAX number region (zone maps, sum,
   * min/max), so a fail-fast assertion guards the invariant in debug builds.
   */
  public void ensureNumberRegion(final KeyValueLeafPage donor) {
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
        synchronized (this) {
          regionDeriveAttempted = 0;
          this.regionTable = donorTable;
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
   * Walk the slotted page, collect each fused {@code OBJECT_NAMED_NUMBER} slot's value
   * + its inline nameKey/pathNodeKey, encode into a NumberRegion payload. Returns
   * {@code null} when the page has no numeric values or no slotted page yet.
   *
   * <p>Side-effect: on success, attaches the region to the page so subsequent
   * lookups skip this build.
   *
   * <p>Two-phase, like every region builder. The WALK runs under the page GUARD — pinning the
   * frame against close() and the recycling that follows, without serializing record readers'
   * guard traffic behind a 1024-slot derivation — and entirely off the page monitor. The INSTALL
   * takes the monitor for a few table stores; that short section is what keeps the table unique
   * and the memo coherent when concurrent workers reach the same page through different doors
   * (the region-only ensure, this builder's getter, the header getters), where an unsynchronized
   * check-then-act once let two workers mint separate tables and the second install orphaned the
   * first thread's derivations. Two workers racing the walk cost one redundant walk, never a torn
   * table: the install re-checks the memo and the loser discards its result.
   */
  private MemorySegment tryBuildNumberRegionFromSlottedPage() {
    if ((regionDeriveAttempted & NUMBER_DERIVE_MASK) != 0) {
      return regionPayload(RegionTable.KIND_NUMBER);
    }
    final byte[] encoded;
    final byte[] doubles;
    if (!acquireGuard()) {
      return null;  // mid-close — nothing to derive from
    }
    try {
      if (slottedPage == null) {
        return null;  // deliberately unmemoized: there was nothing to walk
      }
      encoded = collectAndEncodeNumberRegion();
      doubles = tryBuildDoubleRegionFromSlottedPage();
    } finally {
      releaseGuard();
    }
    synchronized (this) {
      if ((regionDeriveAttempted & NUMBER_DERIVE_MASK) != 0) {
        return regionPayload(RegionTable.KIND_NUMBER);  // a racing walk installed first
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
      // The writer emits an uncompressed zone-map region alongside every number region; a region
      // rebuilt here must carry one too. Without it a page that went through versioning
      // reconstruction would still answer correctly but would have to decompress its number column
      // to find bounds every other page hands over for free. Set unconditionally, including to
      // null: leaving a previous zone map beside a number column it no longer describes is the
      // stale-bounds failure in its most direct form — and the same argument covers the double
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
    final byte tagKind = allPathNodeKeysValid ? NumberRegion.TAG_KIND_PATH_NODE : NumberRegion.TAG_KIND_NAME;
    final int[] tagBuf = allPathNodeKeysValid ? pathBuf : nameBuf;
    return NumberRegion.encode(valBuf, tagBuf, count, tagKind);
  }

  /**
   * Rebuild the double column from the slotted page — the reconstruction-path counterpart of the
   * writer's collection in {@code PageKind.buildRegionTable}, selecting exactly what it selects:
   * Double-, Float- and exactly-representable-BigDecimal-typed fused number slots, with the
   * per-field ordinal counted across BOTH numeric types so a later merge can split a liveness
   * bitmap between the columns.
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
  private static final ThreadLocal<int[]> REBUILD_DECIMAL_OUT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[1]);
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
          continue;  // the long column's value; only the ordinal counter needed to see it
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
            continue;  // a Big* value neither column takes — the oracle will refuse the page
          }
          // Only ever a zone-map bound, and every bound derived from these is widened outward
          // before use, so the ulp this division can cost cannot prune a matching page.
          value = decUnscaled / DoubleRegion.exp10(decScale);
        } else {
          value = getFusedObjectNamedNumberValueDoubleFromSlot(slot);
          if (Double.isNaN(value)) {
            continue;  // neither Double/Float nor decimal — the oracle will refuse the page
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
    return DoubleRegion.encode(valBuf, decUnscaledBuf, decScaleBuf,
                              allPathNodeKeysValid ? pathBuf : nameBuf, ordBuf, count,
                              allPathNodeKeysValid ? NumberRegion.TAG_KIND_PATH_NODE
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
   * Lazily parsed {@link StringRegion.Header} for the page's
   * dictionary-encoded string column. {@code null} if the page has no
   * string records or the region hasn't been built yet.
   */
  private volatile StringRegion.Header cachedStringHeader;

  /**
   * Drop the cached string-region parsed header and payload so the next
   * reader rebuilds. Called from every mutation path that adds, modifies, or removes
   * a STRING_VALUE / OBJECT_NAMED_STRING record.
   */
  void invalidateStringRegion() {
    final RegionTable rt = regionTable;
    // Same reasoning as invalidateNumberRegion: presence is asked of the table, because a caller
    // can reach the payload without ever parsing a header, and the old guard inferred it from the
    // header alone.
    final boolean present = rt != null
        && (rt.hasRegion(RegionTable.KIND_STRING)
            || rt.hasRegion(RegionTable.KIND_STRING_DICT_SKETCH));
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
   * <p>Cheap for the same reason as its number and string twins: a page with no boolean column
   * installed and nothing memoised pays one field read plus a branch, which is the steady state of
   * the write path, where the modify page is built fresh and carries no region table at all.
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
   * <p>The two must fall TOGETHER. {@link RegionTable#KIND_RECORD_ORDINAL} numbers records by
   * position in the name column collected in the same pass, so a linkage that outlives the names it
   * indexes still aligns — against a column that no longer exists. A fused cross-column predicate
   * takes that alignment as a certificate, so the failure mode is a wrong answer, not a stale
   * bound. Dropping only one is worse than dropping neither.
   */
  void invalidateNamesRegion() {
    final RegionTable rt = regionTable;
    final boolean present = rt != null
        && (rt.hasRegion(RegionTable.KIND_OBJECT_KEY_NAMEKEY)
            || rt.hasRegion(RegionTable.KIND_RECORD_ORDINAL));
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
   * Thread-local scratch for {@link #readFusedObjectNamedStringBytes} FSST decode.
   * One array per worker thread, reused for every value read during a region build.
   * 1 KiB matches typical string max length on JSON-like workloads; grows on first oversize.
   */
  private static final ThreadLocal<byte[]> STRING_REGION_BUILD_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[1024]);

  /**
   * Walk the slotted page, collect each fused OBJECT_NAMED_STRING slot's value + its
   * parent OBJECT_KEY's nameKey, encode into a StringRegion payload.
   *
   * <p>Returns {@code null} when the page has no string values or no slotted page yet.
   * Side-effect: on success, attaches the region to the page so subsequent lookups
   * skip this build.
   */
  private MemorySegment tryBuildStringRegionFromSlottedPage() {
    if ((regionDeriveAttempted & STRING_DERIVE_MASK) != 0) {
      return regionPayload(RegionTable.KIND_STRING);
    }
    final byte[] encoded;
    if (!acquireGuard()) {
      return null;  // mid-close — nothing to derive from
    }
    try {
      if (slottedPage == null) {
        return null;  // deliberately unmemoized: there was nothing to walk
      }
      encoded = collectAndEncodeStringRegion();
    } finally {
      releaseGuard();
    }
    if (encoded == STRING_BUILD_RETRY) {
      return null;  // undecodable FSST slot — retryable, deliberately unmemoized; see the walk
    }
    synchronized (this) {
      if ((regionDeriveAttempted & STRING_DERIVE_MASK) != 0) {
        return regionPayload(RegionTable.KIND_STRING);  // a racing walk installed first
      }
      if (encoded == null) {
        regionDeriveAttempted |= STRING_DERIVE_MASK;
        return null;  // no strings on the page — a permanent refusal
      }
      final RegionTable table = regionTableForInstall();
      table.set(RegionTable.KIND_STRING, encoded);
      final MemorySegment installed = table.payload(RegionTable.KIND_STRING);
      // The writer emits a dictionary sketch alongside every string region; a region rebuilt here
      // must carry one too, or a page that went through versioning reconstruction would silently
      // lose the ability to rule itself out of a string equality — correct answers, but every such
      // page paying a dictionary decode forever after. The header is read from the installed
      // segment; the entries are hashed from the array, which is what the sketch builder takes.
      table.set(RegionTable.KIND_STRING_DICT_SKETCH,
                StringDictSketch.encodeFromStringRegion(
                    encoded, new StringRegion.Header().parseInto(installed)));
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
   * @return the encoded payload; {@code null} when the page holds no strings (a permanent
   *         refusal); {@link #STRING_BUILD_RETRY} when a slot could not be decoded (retryable)
   */
  /**
   * This slot's enclosing object's path node key as an int, or {@code -1} when there is none that
   * fits — which drops the whole page's path-tagged encoder, since a partial tagging would key
   * some values by path and others by name.
   */
  private int pathNodeKeyIntForSlot(final int slot, final long pageKeyBase) {
    final long pnk = getObjectKeyPathNodeKeyFromSlot(slot, pageKeyBase + slot);
    return pnk > 0L && pnk <= (long) Integer.MAX_VALUE ? (int) pnk : -1;
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
    final StringRegion.Encoder pathEnc = withPathSummary ? new StringRegion.Encoder() : null;
    boolean allPathNodeKeysValid = withPathSummary;
    int count = 0;
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
        if (kindId == FUSED_OBJECT_NAMED_STRING_KIND_ID) {
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
    if (count == 0) {
      return null;
    }
    return allPathNodeKeysValid && pathEnc != null
        ? pathEnc.finish(StringRegion.TAG_KIND_PATH_NODE)
        : nameEnc.finish(StringRegion.TAG_KIND_NAME);
  }

  /**
   * Rebuild the field-name column from the slotted page. The counterpart of
   * {@link #tryBuildNumberRegionFromSlottedPage} for {@link RegionTable#KIND_OBJECT_KEY_NAMEKEY},
   * and it must select exactly what the writer selects — the fused primitive OBJECT_NAMED_* kinds —
   * because the scan's completeness oracle compares this column's slot count against a value
   * region's tag count.
   *
   * @return the payload, or {@code null} when the page has no such slots or the dictionary exceeds
   *         what the region can encode
   */
  private byte[] tryBuildObjectKeyNameKeyRegionFromSlottedPage() {
    if ((regionDeriveAttempted & NAMES_DERIVE_MASK) != 0) {
      return null;  // completed once already; the installed payloads, if any, are in the table
    }
    final byte[][] built;  // {names payload, ordinals payload}, either possibly null
    if (!acquireGuard()) {
      return null;  // mid-close — nothing to derive from
    }
    try {
      if (slottedPage == null) {
        return null;  // deliberately unmemoized: there was nothing to walk
      }
      built = collectAndEncodeNameKeyRegion();
    } finally {
      releaseGuard();
    }
    synchronized (this) {
      if ((regionDeriveAttempted & NAMES_DERIVE_MASK) != 0) {
        return null;  // a racing walk installed first
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
        if (!isFusedObjectNamedKindId(PageLayout.getDirNodeKindId(sp, slot))) {
          continue;
        }
        if (count == nameKeys.length) {
          nameKeys = Arrays.copyOf(nameKeys, count << 1);
          slots = Arrays.copyOf(slots, count << 1);
          parentKeys = Arrays.copyOf(parentKeys, count << 1);
        }
        nameKeys[count] = getFusedObjectNamedNameKeyFromSlot(slot);
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
      return new byte[2][];  // more distinct names than the region encodes; callers walk slots
    }
    return new byte[][] { payload, RecordOrdinalRegion.encode(parentKeys, pageKeyBase, count) };
  }

  /**
   * Restore the column regions of a page assembled from versioning fragments.
   *
   * <p>A reconstructed page is built slot by slot from several fragments, so it starts with no
   * columns of its own even though its records are complete. Rebuilding them here keeps the
   * invariant every other page in the cache satisfies — that a materialized page carries its
   * columns — and is what lets a later column scan serve it instead of walking its records.
   *
   * <p>Only the two cheap ones are eager: the field-name column (which every column scan probes
   * first) and the numeric column. String and boolean columns rebuild on demand, and now bring
   * their sketch with them.
   */
  public void ensureColumnRegions() {
    // One definition of "derive what is missing": the mask dispatcher below. Keeping a second
    // hand-rolled builder dispatch here is how the string getter-vs-builder bug survived — two
    // entry points drifting on which builder derives which kind.
    ensureRegionsFor(NAMES_DERIVE_MASK | NUMBER_DERIVE_MASK);
  }

  /** Kinds the field-name builder derives together: the names column and the record linkage. */
  private static final int NAMES_DERIVE_MASK =
      RegionTable.maskOf(RegionTable.KIND_OBJECT_KEY_NAMEKEY)
          | RegionTable.maskOf(RegionTable.KIND_RECORD_ORDINAL);
  /** Kinds the number builder derives together: the long column, its zone map, and the doubles. */
  private static final int NUMBER_DERIVE_MASK =
      RegionTable.maskOf(RegionTable.KIND_NUMBER)
          | RegionTable.maskOf(RegionTable.KIND_NUMBER_ZONEMAP)
          | RegionTable.maskOf(RegionTable.KIND_DOUBLE);
  /** Kind the boolean builder derives. */
  private static final int BOOL_DERIVE_MASK = RegionTable.maskOf(RegionTable.KIND_BOOLEAN);
  /** Kinds the string builder derives together: the dictionary column and its sketch. */
  private static final int STRING_DERIVE_MASK =
      RegionTable.maskOf(RegionTable.KIND_STRING)
          | RegionTable.maskOf(RegionTable.KIND_STRING_DICT_SKETCH);

  /**
   * Derive every region {@code kindMask} asks for that the table does not yet hold, from the
   * slotted page.
   *
   * <p>This is what makes a RECONSTRUCTED page a first-class column source. The versioning layer
   * merges a multi-fragment page into one slotted page — every slot in one coordinate space, which
   * is precisely the alignment a cross-column (fused) predicate needs and the per-fragment merge
   * cannot provide — but the merged page starts with an EMPTY region table that only grows when a
   * lazy getter happens to run. A region-only read that found the resident page missing a
   * requested kind used to fall through to "no columns" even though every one of them was sitting
   * derivable in the slots; this call closes that gap on demand, per kind:
   * the field-name column brings {@link RegionTable#KIND_RECORD_ORDINAL} with it, the number
   * rebuild installs zone maps and the double column alongside, and string brings its sketch.
   *
   * <p>Lock-free in the steady state: after the first read of a page, everything requested is
   * either present or recorded as attempted in {@link #regionDeriveAttempted}, and the method
   * returns on two volatile reads without touching any lock — N scan workers re-reading a hot
   * resident page must not serialize on it. When something IS missing, the builders themselves do
   * the pinning: each runs its walk under the page guard (the frame must not be recycled
   * mid-walk) and takes the page monitor only for its install, so two workers racing the precheck
   * cost one redundant walk, never a torn table — and record readers' guard traffic never queues
   * behind a derivation.
   */
  public void ensureRegionsFor(final int kindMask) {
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
    if (slottedPage == null) {
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
        && regionPayload(RegionTable.KIND_NUMBER) == null
        && regionPayload(RegionTable.KIND_DOUBLE) == null) {
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
   * <p>A completed refusal is permanent — the slots never change under a resident page — so the
   * memo is what stops a page whose ordinals legitimately cannot encode, or that simply holds no
   * booleans, from re-walking all of its slots and re-installing identical payloads into the
   * table's arena on EVERY region-only read for as long as it stays cached.
   *
   * <p>Three deliberate properties. Bits are set by the builders themselves, in their install
   * sections — never before the walk, so an exception does not latch a transient failure into a
   * permanent refusal — and under the page monitor those sections hold, so all entry points share
   * the memo. Bits are CLEARED wherever the corresponding payloads are dropped or replaced
   * ({@link #invalidateNumberRegion}, {@link #invalidateStringRegion}, {@link #setRegionTable},
   * {@link #reset}), because a memo that outlives its table reads as "derived and refused" for
   * regions that were merely thrown away. And the field is volatile: the lock-free precheck above
   * reads it without the monitor, and the volatile read of a bit a builder set after installing
   * its payloads is the happens-before edge that makes those payloads safely readable.
   */
  private volatile int regionDeriveAttempted;

  public StringRegion.Header getStringRegionHeader() {
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
   * {@code OBJECT_NAMED_BOOLEAN} slots and collecting each slot's inline value +
   * its inline name/path tag. Returns {@code null} when the page has no booleans.
   * See {@link #tryBuildNumberRegionFromSlottedPage} for the template —
   * this method mirrors it for the boolean column.
   */
  private MemorySegment tryBuildBooleanRegionFromSlottedPage() {
    if ((regionDeriveAttempted & BOOL_DERIVE_MASK) != 0) {
      return regionPayload(RegionTable.KIND_BOOLEAN);
    }
    final byte[] encoded;
    if (!acquireGuard()) {
      return null;  // mid-close — nothing to derive from
    }
    try {
      if (slottedPage == null) {
        return null;  // deliberately unmemoized: there was nothing to walk
      }
      encoded = collectAndEncodeBooleanRegion();
    } finally {
      releaseGuard();
    }
    synchronized (this) {
      if ((regionDeriveAttempted & BOOL_DERIVE_MASK) != 0) {
        return regionPayload(RegionTable.KIND_BOOLEAN);  // a racing walk installed first
      }
      if (encoded == null) {
        regionDeriveAttempted |= BOOL_DERIVE_MASK;
        return null;  // no booleans, or a dictionary the region cannot encode — permanent
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
    final int[] tagBuf = allPathNodeKeysValid ? pathBuf : nameBuf;
    return BooleanRegion.encode(valBuf, tagBuf, count, tagKind);
  }

  /** Raw boolean-region payload bytes, or {@code null}. */
  public MemorySegment getBooleanRegionPayload() {
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
   * Get the current version of this page frame.
   * Used for detecting page reuse via version counter check.
   *
   * @return current version number
   */
  public int getVersion() {
    return version.get();
  }

  /**
   * Increment the version counter.
   * Called when the page frame is reused for a different logical page.
   */
  public void incrementVersion() {
    version.incrementAndGet();
  }

  /**
   * Try to acquire a guard on this page.
   * Returns false if the page is orphaned or closed (cannot be used).
   * This is the synchronized version that prevents race conditions with close().
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
   * Release a guard on this page (decrement guard count).
   * If the page is orphaned and this was the last guard, the page is closed.
   * This ensures deterministic cleanup without relying on GC/finalizers.
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
   * Mark this page as orphaned using lock-free CAS.
   * Called when the page is removed from cache but still has active guards.
   * The page will be closed when the last guard is released.
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
   * Get the current guard count.
   * Used by ClockSweeper to check if page can be evicted.
   *
   * @return current guard count
   */
  public int getGuardCount() {
    return guardCount.get();
  }

  /**
   * Mark this page as recently accessed (set HOT bit).
   * Called on every page access for clock eviction algorithm.
   * <p>
   * Uses opaque memory access (no memory barriers) for maximum performance.
   * The HOT bit is advisory - stale reads are acceptable and will at worst
   * give a page an extra second chance during eviction.
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
   * Reset page data structures for reuse.
   * Clears records and internal state but keeps MemorySegments allocated.
   * Used when evicting a page to prepare frame for reuse.
   */
  public void reset() {
    // Clear record arrays
    if (records != null) {
      Arrays.fill(records, null);
    }

    // Reset slotted page state (bitmap and heap pointers)
    if (slottedPage != null) {
      PageLayout.initializePage(slottedPage, recordPageKey, revision,
          indexType.getID(), areDeweyIDsStored);
      cachedHeapEnd = 0;
      cachedHeapUsed = 0;
      cachedPopulatedCount = 0;
    }

    // Reset index trackers
    lastSlotIndex = -1;

    // A reused frame hosts a NEW logical page: the old occupant's region table and its derive
    // memo describe slots that no longer exist. An inherited memo is the nastier half — the new
    // page would silently skip derivations the OLD page refused, forever.
    regionTable = null;
    clearRegionDeriveAttempted(~0);
    cachedNumberHeader = null;
    cachedStringHeader = null;

    clearFsstBinding();

    // Clear references
    references.clear();
    addedReferences = false;
    
    // Clear cached data
    bytes = null;
    hashCode = null;
    
    // CRITICAL: Guard count MUST be 0 before reset
    int currentGuardCount = guardCount.get();
    if (currentGuardCount != 0) {
      throw new IllegalStateException(
          String.format("CRITICAL BUG: reset() called on page with active guards! " +
              "Page %d (%s) rev=%d guardCount=%d - this will cause guard count corruption!",
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
          // Check if slot needs preservation AND wasn't modified (neither in records[] nor in slot data)
          final boolean needsPreservation = PageLayout.isSlotPreserved(slottedPage, i);
          if (needsPreservation && (records == null || records[i] == null) && getSlot(i) == null) {
            // Copy slot from completePage, preserving nodeKindId
            MemorySegment slotData = completePageRef.getSlot(i);
            if (slotData != null) {
              setSlotWithNodeKind(slotData, i, completePageRef.getSlotNodeKindId(i));
            }
            // Copy deweyId too if stored
            if (areDeweyIDsStored) {
              MemorySegment deweyId = completePageRef.getDeweyId(i);
              if (deweyId != null) {
                setDeweyId(deweyId, i);
              }
            }
          }
        }
      }

      if (records != null) {
        if (areDeweyIDsStored && recordPersister instanceof DeweyIdSerializer) {
          processEntries(resourceConfiguration, records);
          for (int i = 0; i < records.length; i++) {
            final DataRecord record = records[i];
            if (record != null && record.getDeweyID() != null && record.getNodeKey() != 0) {
              setDeweyId(record.getDeweyID().toBytes(), i);
            }
          }
        } else {
          processEntries(resourceConfiguration, records);
        }
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

        // Clear buffer for reuse (reset position to 0, keeps capacity)
        reusableOut.clear();

        // Serialize into the reusable buffer
        recordPersister.serialize(reusableOut, record, resourceConfiguration);
        // Zero-alloc destination read: baseSegment() returns the unsliced growable segment;
        // position() is the used byte count. Avoids the per-record MemorySegment.asSlice view
        // that getDestination() would allocate — see baseSegment() doc on MemorySegmentBytesOut.
        final long usedSize = reusableOut.position();
        final MemorySegment base = reusableOut.baseSegment();

        final long slotTotalSize = areDeweyIDsStored
            ? usedSize + PageLayout.DEWEY_ID_TRAILER_SIZE
            : usedSize;
        if (usedSize > PageConstants.MAX_RECORD_SIZE
            || slotTotalSize > (long) MAX_SLOTTED_PAGE_CAPACITY - PageLayout.HEAP_START - cachedHeapEnd) {
          // Overflow page (#1076): the record is either larger than the per-record threshold or
          // would not fit into the slotted page heap within the largest allocator size class.
          // Copy the serialized bytes into a persistent buffer; the OverflowPage is written to
          // disk in NodeStorageEngineWriter#commit and the read path falls back to it when the
          // slot is empty but a reference with a valid disk key exists.
          byte[] persistentBuffer = new byte[(int) usedSize];
          MemorySegment.copy(base, 0L, MemorySegment.ofArray(persistentBuffer), 0L, usedSize);

          final var reference = new PageReference();
          reference.setPage(new OverflowPage(persistentBuffer));
          references.put(recordID, reference);
          // An older, slot-resident version of this record may have been carried into the page
          // by the versioning reconstruction — clear it so the read path falls through to the
          // overflow reference instead of returning the stale slot bytes.
          if (slottedPage != null && PageLayout.isSlotPopulated(slottedPage, offset)) {
            PageLayout.clearSlotPopulated(slottedPage, offset);
            updatePopulatedCount(cachedPopulatedCount - 1);
          }
          // Persist the DeweyID in the page's DeweyID region — the read path reconstructs the
          // record from the overflow bytes + page.getDeweyIdAsByteArray(offset).
          if (areDeweyIDsStored && record.getDeweyID() != null && record.getNodeKey() != 0) {
            setDeweyId(record.getDeweyID().toBytes(), i);
          }
        } else {
          // Normal record: setSlotDirect copies the leading {usedSize} bytes from {base}
          // into the slotted page heap. No intermediate slice.
          setSlotDirect(base, 0L, (int) usedSize, offset);
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

  /**
   * Append this page's raw string values to {@code samples}, stopping once {@code cap} samples
   * have been gathered in total.
   *
   * <p>This feeds {@code NodeStorageEngineWriter#buildRevisionFsstSymbolTable}, which pools
   * samples from <em>many</em> pages and builds one symbol table for the whole revision. The
   * table used to be built here, per page, and that failed in both directions at once: a full
   * slot scan plus frequency analysis per page made ingest 18× slower, and one page rarely holds
   * the {@link FSSTCompressor#MIN_SAMPLES_FOR_TABLE} strings a table needs before it beats raw
   * bytes, so the per-page table was rejected on essentially every page anyway.
   *
   * <p>Only uncompressed values are gathered: sampling an already-FSST-encoded value would feed
   * the next table's frequency analysis bytes that are not text.
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
      final int fusedStringId = NodeKind.OBJECT_NAMED_STRING.getId();
      for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
        if (samples.size() >= cap) {
          return;
        }
        if (records != null && records[i] != null) continue; // Already scanned above
        if (!PageLayout.isSlotPopulated(slottedPage, i)) continue;
        final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, i);
        if (nodeKindId == fusedStringId) {
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
              if (value != null && value.length > 0) samples.add(value);
            }
          } finally {
            fsstStringFlyweight().clearBinding();
          }
        }
      }
    }
  }

  /**
   * Compress all string values in the page using the revision's FSST symbol table.
   * This modifies the string nodes in place to use compressed values.
   * A no-op until {@code NodeStorageEngineWriter#buildRevisionFsstSymbolTable} has handed the
   * page a table — without one, strings serialize raw.
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
              if (compressedValue != null) {
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
              if (compressedValue != null) {
                fusedNode.setRawValue(compressedValue, true, fsstSymbolTable);
              }
            }
          }
        }
      }
    }

    // Compress slotted page strings (zero records[] path)
    if (slottedPage != null) {
      final int fusedStringId = NodeKind.OBJECT_NAMED_STRING.getId();
      for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
        if (records != null && records[i] != null) continue; // Already handled above
        if (!PageLayout.isSlotPopulated(slottedPage, i)) continue;
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
              byte[] compressed =
                  FSSTCompressor.encodeOrNull(originalValue, 0, originalValue.length, parsedSymbols);
              if (compressed != null) {
                fsstStringFlyweight().setRawValue(compressed, true, fsstSymbolTable);
              }
            }
          } finally {
            fsstStringFlyweight().setOwnerPage(null);
            fsstStringFlyweight().clearBinding();
          }
        } else if (nodeKindId == fusedStringId) {
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
   * Set the FSST symbol table on all string nodes after deserialization.
   * This allows nodes to use lazy decompression.
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

