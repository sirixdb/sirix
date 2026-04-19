package io.sirix.page;

import io.sirix.utils.ToStringHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.DeltaVarIntCodec;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Arrays;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
import io.sirix.settings.DiagnosticSettings;
import io.sirix.settings.StringCompressionType;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
public final class KeyValueLeafPage implements KeyValuePage<DataRecord> {

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
  
  // TRACK ALL LIVE PAGES - for leak detection (use object identity, not recordPageKey)
  // CRITICAL: Use IdentityHashMap to track by object identity, not equals/hashCode
  public static final java.util.Set<KeyValueLeafPage> ALL_LIVE_PAGES = 
    java.util.Collections.synchronizedSet(
      java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>())
    );
  
  // LEAK DETECTION: Track finalized pages
  public static final java.util.concurrent.atomic.AtomicLong PAGES_FINALIZED_WITHOUT_CLOSE = new java.util.concurrent.atomic.AtomicLong(0);
  
  // Track finalized pages by type and pageKey for diagnostics
  public static final java.util.concurrent.ConcurrentHashMap<IndexType, java.util.concurrent.atomic.AtomicLong> FINALIZED_BY_TYPE = 
    new java.util.concurrent.ConcurrentHashMap<>();
  public static final java.util.concurrent.ConcurrentHashMap<Long, java.util.concurrent.atomic.AtomicLong> FINALIZED_BY_PAGE_KEY = 
    new java.util.concurrent.ConcurrentHashMap<>();
    
  // Track all Page 0 instances for explicit cleanup
  // CRITICAL: Use synchronized IdentityHashSet to track by object identity, not equals/hashCode
  // (Multiple Page 0 instances with same recordPageKey/revision would collide in regular Set)
  public static final java.util.Set<KeyValueLeafPage> ALL_PAGE_0_INSTANCES = 
    java.util.Collections.synchronizedSet(
      java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>())
    );
  
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

  /**
   * PAX region table appended in {@link io.sirix.BinaryEncodingVersion#V1}. Null
   * when the page was written in V0 format or when no regions have been populated.
   * Later tasks populate this with number / string / struct / DeweyID regions;
   * scan operators read contiguous payload buffers from it instead of decoding
   * varints per slot.
   */
  private io.sirix.page.pax.RegionTable regionTable;

  /**
   * FSST-compression flyweights (StringNode / ObjectStringNode), lazy-init
   * only on the write-path where FSST compression actually runs. For
   * analytical scan workloads these objects were the top non-page allocator —
   * 7.6% of samples (async-profiler alloc mode) at 2× per KVLP constructor.
   * Using a shared sentinel so the null-check in the read path is elided.
   */
  private StringNode fsstStringFlyweight;
  private ObjectStringNode fsstObjStringFlyweight;

  private StringNode fsstStringFlyweight() {
    StringNode f = fsstStringFlyweight;
    if (f == null) {
      f = new StringNode(0, null);
      fsstStringFlyweight = f;
    }
    return f;
  }

  private ObjectStringNode fsstObjStringFlyweight() {
    ObjectStringNode f = fsstObjStringFlyweight;
    if (f == null) {
      f = new ObjectStringNode(0, null);
      fsstObjStringFlyweight = f;
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
    } else {
      this.creationStackTrace = null;
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
    } else {
      this.creationStackTrace = null;
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

    // Copy FSST symbol table if present (byte[] treated as immutable after construction)
    if (fsstSymbolTable != null) {
      copy.fsstSymbolTable = Arrays.copyOf(fsstSymbolTable, fsstSymbolTable.length);
    }

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
    maybeInvalidateNumberRegionForExistingSlot(offset);
    maybeInvalidateStringRegionForExistingSlot(offset);

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
        serializeToHeap(fn, key, offset);
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
    maybeInvalidateNumberRegionForExistingSlot(offset);
    maybeInvalidateStringRegionForExistingSlot(offset);
    ensureRecords();
    records[offset] = record;
  }

  public void serializeNewRecord(final FlyweightNode fn, final long nodeKey, final int offset) {
    addedReferences = false;
    compressedSegment = null;
    bytes = null;
    serializeToHeap(fn, nodeKey, offset);
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
   */
  private void serializeToHeap(final FlyweightNode fn, final long nodeKey, final int offset) {
    ensureSlottedPage();
    // Get DeweyID bytes if stored (must capture BEFORE binding overwrites the node state)
    final byte[] deweyIdBytes = areDeweyIDsStored ? fn.getDeweyIDAsBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;

    // Ensure heap has enough space for this record (value nodes can be large)
    final int heapEnd = cachedHeapEnd;
    final int estimatedSize = fn.estimateSerializedSize() + deweyIdLen
        + (areDeweyIDsStored ? PageLayout.DEWEY_ID_TRAILER_SIZE : 0);
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < estimatedSize) {
      growSlottedPage();
    }

    // Write directly to heap at current end
    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    final int recordBytes = fn.serializeToHeap(slottedPage, absOffset);

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

    // Number/string write: drop the cached PAX region so the next reader rebuilds.
    if (isNumberValueKindId(nodeKindId)) {
      invalidateNumberRegion();
    } else if (isStringValueKindId(nodeKindId)) {
      invalidateStringRegion();
    }

    // Bind flyweight — all subsequent mutations go directly to page memory
    fn.bind(slottedPage, absOffset, nodeKey, offset);
    fn.setOwnerPage(this);
  }

  // ==================== DIRECT-TO-HEAP CREATION ====================

  /**
   * Prepare the heap for a direct record write. Ensures slotted page exists and has
   * enough space. Returns the absolute offset where the caller should write.
   *
   * @param estimatedRecordSize upper bound on record bytes (from estimateSerializedSize)
   * @param deweyIdLen          length of DeweyID bytes (0 if none)
   * @return absolute byte offset in the slotted page MemorySegment to write at
   */
  public long prepareHeapForDirectWrite(final int estimatedRecordSize, final int deweyIdLen) {
    ensureSlottedPage();
    final int deweyOverhead = areDeweyIDsStored
        ? deweyIdLen + PageLayout.DEWEY_ID_TRAILER_SIZE : 0;
    final int totalEstimated = estimatedRecordSize + deweyOverhead;
    final int heapEnd = cachedHeapEnd;
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < totalEstimated) {
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

    // Number/string write: drop the cached PAX region so the next reader rebuilds.
    if (isNumberValueKindId(nodeKindId)) {
      invalidateNumberRegion();
    } else if (isStringValueKindId(nodeKindId)) {
      invalidateStringRegion();
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

    // Number/string write: in-place field rewrite changed a value the cached region snapshotted.
    if (isNumberValueKindId(nodeKindId)) {
      invalidateNumberRegion();
    } else if (isStringValueKindId(nodeKindId)) {
      invalidateStringRegion();
    }
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

    // Number/string copy: a new value lands in this page's heap. The source's region is
    // not carried, and any region cached for THIS page is now incomplete.
    if (isNumberValueKindId(srcNodeKindId)) {
      invalidateNumberRegion();
    } else if (isStringValueKindId(srcNodeKindId)) {
      invalidateStringRegion();
    }
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
    slottedPage = allocated.reinterpret(Long.MAX_VALUE);
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
      slottedPage = dst.reinterpret(Long.MAX_VALUE);
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
    slottedPage = grown.reinterpret(Long.MAX_VALUE);
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

    // Ensure heap has enough space
    int heapEnd = cachedHeapEnd;
    final int remaining = slottedPageCapacity - PageLayout.HEAP_START - heapEnd;
    if (remaining < dataSize) {
      while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < dataSize) {
        growSlottedPage();
      }
      heapEnd = cachedHeapEnd;
    }

    // Bump-allocate and copy data to heap
    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    MemorySegment.copy(source, sourceOffset, slottedPage, absOffset, dataSize);

    // Update heap end and used counters
    updateHeapEnd(heapEnd + dataSize);
    updateHeapUsed(cachedHeapUsed + dataSize);

    // Update directory entry
    PageLayout.setDirEntry(slottedPage, slotNumber, heapEnd, dataSize, nodeKindId);

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
      copy[i] = slottedPage.get(ValueLayout.JAVA_LONG_UNALIGNED,
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
   * Read an ObjectKeyNode's nameKey directly from the slot bytes without binding
   * a flyweight singleton or moving a transaction cursor. Callers MUST verify
   * the slot is populated and holds an OBJECT_KEY (kind id 26 or 126) before calling —
   * the method does no validation for the vectorized scan hot path.
   *
   * <p>Used by SirixVectorizedExecutor to filter/count on {@code nameKey} without
   * paying the per-slot {@code moveTo → bind singleton} cost; non-matching slots
   * (~4/5 of OBJECT_KEY slots in a typical JSON record) short-circuit before the
   * cursor move.
   *
   * @param slotNumber the slot index (assumed populated + OBJECT_KEY kind)
   * @return the signed nameKey from the slot
   */
  public int getObjectKeyNameKeyFromSlot(final int slotNumber) {
    final byte[] nameKeyPayload = regionPayload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
    if (nameKeyPayload != null) {
      return io.sirix.page.pax.ObjectKeyNameKeyRegion.nameKeyForSlot(nameKeyPayload, slotNumber);
    }
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    if (kindId == NodeFieldLayout.OBJECT_KEY_PAX_KIND_ID) {
      return -1;
    }
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJKEY_NAME_KEY) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_KEY_FIELD_COUNT;
    return DeltaVarIntCodec.decodeSignedFromSegment(sp, dataStart + fieldOff);
  }

  public int getObjectKeyNameKeyFromRegion(final int slotIndex) {
    final byte[] payload = regionPayload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
    if (payload != null) {
      return io.sirix.page.pax.ObjectKeyNameKeyRegion.nameKeyForSlot(payload, slotIndex);
    }
    return getObjectKeyNameKeyFromSlot(slotIndex);
  }

  /** OBJECT_NUMBER_VALUE payload type code for Integer (varint). See NodeKind.serializeNumber. */
  private static final byte NUMBER_TYPE_INTEGER = 2;
  private static final byte NUMBER_TYPE_LONG = 3;
  private static final int OBJECT_NUMBER_VALUE_KIND_ID = 42;
  private static final int NUMBER_VALUE_KIND_ID = 28;
  private static final int OBJECT_STRING_VALUE_KIND_ID = 40;
  private static final int STRING_VALUE_KIND_ID = 30;

  /**
   * True when {@code kindId} identifies a record whose value participates in the PAX
   * {@link RegionTable#KIND_NUMBER} region. Used by mutation paths to gate cache
   * invalidation: only number-affecting writes pay the (already-cheap) invalidation cost.
   */
  static boolean isNumberValueKindId(final int kindId) {
    return kindId == OBJECT_NUMBER_VALUE_KIND_ID || kindId == NUMBER_VALUE_KIND_ID;
  }

  /**
   * True when {@code kindId} identifies a record whose value participates in the PAX
   * {@link RegionTable#KIND_STRING} region.
   */
  static boolean isStringValueKindId(final int kindId) {
    return kindId == OBJECT_STRING_VALUE_KIND_ID || kindId == STRING_VALUE_KIND_ID;
  }

  /**
   * Lazy pre-parsed FSST symbol table, built once per page on first access.
   * {@code FSSTCompressor.parseSymbolTable} walks the serialized symbol
   * table bytes into a {@code byte[][]} suitable for {@code decodeRaw}.
   */
  private volatile byte[][] parsedFsstSymbols;
  private static final byte[][] EMPTY_FSST_SYMBOLS = new byte[0][];

  private byte[][] fsstSymbols() {
    byte[][] s = parsedFsstSymbols;
    if (s != null) {
      return s;
    }
    final byte[] tbl = fsstSymbolTable;
    if (tbl == null || tbl.length == 0) {
      s = EMPTY_FSST_SYMBOLS;
    } else {
      s = io.sirix.utils.FSSTCompressor.parseSymbolTable(tbl);
    }
    parsedFsstSymbols = s;
    return s;
  }

  /**
   * Read the UTF-8 value bytes of an OBJECT_STRING_VALUE slot directly off the
   * slotted page — no moveTo, no singleton binding, no
   * {@code ObjectStringNode.toSnapshot}, no per-record byte[] allocation if the
   * caller provides {@code scratch}. Handles FSST-compressed values inline
   * using the page's pre-parsed symbol table.
   *
   * <p>Caller must verify the slot holds an OBJECT_STRING_VALUE (kind id 40)
   * before invoking. Hot path for analytical group-by over string fields.
   *
   * @param slotNumber slot index on this page
   * @param scratch    caller-owned byte[] to write into; sized ≥ max expected
   *                   value length (decompressed for FSST values). Returns the
   *                   number of bytes written; caller reads {@code scratch[0..n)}.
   * @return bytes written, or -1 if the slot is unavailable / oversized.
   */
  public int readObjectStringValueBytesFromSlot(final int slotNumber, final byte[] scratch) {
    final MemorySegment sp = slottedPage;
    if (sp == null || !PageLayout.isSlotPopulated(sp, slotNumber)) {
      return -1;
    }
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    // ObjectStringNode layout: [kind byte][4 field-offset bytes][data region]
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJSTRVAL_PAYLOAD) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_STRING_VALUE_FIELD_COUNT;
    final long payloadStart = dataStart + fieldOff;

    // Payload: [isCompressed:1][length:varint][value bytes]
    final boolean compressed = sp.get(ValueLayout.JAVA_BYTE, payloadStart) == 1;
    final long lenOff = payloadStart + 1;
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(sp, lenOff);
    if (length <= 0) {
      return -1;
    }
    final int lenBytes = DeltaVarIntCodec.readSignedVarintWidth(sp, lenOff);
    final long dataOff = lenOff + lenBytes;

    if (!compressed) {
      if (length > scratch.length) {
        return -1;
      }
      MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, dataOff, scratch, 0, length);
      return length;
    }

    // FSST-compressed: inline decode into scratch using the page's pre-parsed
    // symbol table. Saves the moveTo / toSnapshot / byte[] allocation pipeline
    // that the rtx slow path would trigger.
    final byte[][] symbols = fsstSymbols();
    if (symbols.length == 0) {
      return -1;
    }
    return decodeFsstInto(sp, dataOff, length, symbols, scratch);
  }

  /**
   * Read the RAW stored value bytes of an OBJECT_STRING_VALUE slot into the
   * caller's scratch — does NOT decompress. Callers that do intra-page run
   * aggregation can compare raw compressed bytes directly (same page →
   * same FSST table → byte-identical encoding for equal values) and
   * decompress only at flush time via {@link #decodeRawIfCompressed}.
   * Returns bytes written or -1 on unavailable/oversized.
   */
  public int readObjectStringValueRawBytesFromSlot(final int slotNumber, final byte[] scratch) {
    final MemorySegment sp = slottedPage;
    if (sp == null || !PageLayout.isSlotPopulated(sp, slotNumber)) {
      return -1;
    }
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJSTRVAL_PAYLOAD) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_STRING_VALUE_FIELD_COUNT;
    final long payloadStart = dataStart + fieldOff;
    // Skip isCompressed byte — caller infers from page's fsstSymbolTable().
    final long lenOff = payloadStart + 1;
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(sp, lenOff);
    if (length <= 0 || length > scratch.length) {
      return -1;
    }
    final int lenBytes = DeltaVarIntCodec.readSignedVarintWidth(sp, lenOff);
    MemorySegment.copy(sp, ValueLayout.JAVA_BYTE, lenOff + lenBytes, scratch, 0, length);
    return length;
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
   * Read the delta-encoded firstChildKey from an OBJECT_KEY slot without
   * moving any cursor or binding a singleton. Returns the raw nodeKey.
   *
   * <p>Caller must verify the slot holds an OBJECT_KEY (kind id 26) first.
   */
  public long getObjectKeyFirstChildKeyFromSlot(final int slotNumber, final long objectKeyNodeKey) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJKEY_FIRST_CHILD_KEY) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_KEY_FIELD_COUNT;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, objectKeyNodeKey);
  }

  /**
   * Read the delta-encoded parentKey (enclosing OBJECT's nodeKey) from an
   * OBJECT_KEY slot without moving any cursor or binding a singleton.
   * Decoded directly off the slotted page so the vectorized scan can join
   * sibling fields in Pass 2 by parent-OBJECT nodeKey in O(1) per slot.
   *
   * <p>Handles both the dense OBJECT_KEY encoding (kind 26) and the PAX
   * variant (kind 126 — nameKey hoisted to an external region; field index 0
   * remains {@code OBJKEY_PARENT_KEY}, only the field count changes).
   *
   * <p>Caller must verify the slot holds an OBJECT_KEY (kind 26 or 126); no
   * validation is performed to keep the hot path branch-free beyond the
   * kind-id dispatch that selects the field-count constant.
   *
   * @param slotNumber       the slot index (assumed populated + OBJECT_KEY kind)
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
    final int fieldCount;
    if (kindId == NodeFieldLayout.OBJECT_KEY_PAX_KIND_ID) {
      fieldCount = NodeFieldLayout.OBJECT_KEY_PAX_FIELD_COUNT;
    } else {
      fieldCount = NodeFieldLayout.OBJECT_KEY_FIELD_COUNT;
    }
    // OBJKEY_PARENT_KEY = 0 for both dense and PAX layouts — the PAX variant
    // only hoists nameKey out of the record, the leading parent/sibling/
    // firstChild offsets are unchanged.
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJKEY_PARENT_KEY) & 0xFF;
    final long dataStart = recordBase + 1 + fieldCount;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, objectKeyNodeKey);
  }

  /**
   * Read the pathNodeKey stored on an OBJECT_KEY slot — the fully-qualified
   * path identifier pointing into the PathSummary. Decoded directly off the
   * slotted page without cursor movement or singleton binding, so the
   * vectorized scan can filter matched slots by scope in O(1) per slot.
   *
   * <p>Handles both the dense OBJECT_KEY encoding (kind 26, field index
   * {@link NodeFieldLayout#OBJKEY_PATH_NODE_KEY}) and the PAX-mode variant
   * (kind 126, field index {@link NodeFieldLayout#OBJKEY_PAX_PATH_NODE_KEY})
   * where the nameKey column has been hoisted into an external region.
   *
   * <p>Caller must verify the slot holds an OBJECT_KEY (kind 26 or 126); no
   * validation is performed to keep the per-slot cost down to a single byte
   * read for the offset and a varint decode for the value.
   *
   * @param slotNumber       the slot index (assumed populated + OBJECT_KEY kind)
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
      // At high memory pressure (e.g. 100M-record workloads with a cache
      // hit-rate near 10%) this can race with concurrent scans; without the
      // guard we NPE inside PageLayout.getDirHeapOffset.
      return -1L;
    }
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
    final int fieldIdx;
    final int fieldCount;
    if (kindId == NodeFieldLayout.OBJECT_KEY_PAX_KIND_ID) {
      fieldIdx = NodeFieldLayout.OBJKEY_PAX_PATH_NODE_KEY;
      fieldCount = NodeFieldLayout.OBJECT_KEY_PAX_FIELD_COUNT;
    } else {
      fieldIdx = NodeFieldLayout.OBJKEY_PATH_NODE_KEY;
      fieldCount = NodeFieldLayout.OBJECT_KEY_FIELD_COUNT;
    }
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
    if (sp == null) {
      for (int i = 0; i < count; i++) {
        outPathNodeKeys[i] = -1L;
        outParentKeys[i] = -1L;
        outFirstChildKeys[i] = -1L;
      }
      return;
    }
    for (int i = 0; i < count; i++) {
      final int slot = slots[i];
      final long nodeKey = pageBase + slot;
      final int heapOffset = PageLayout.getDirHeapOffset(sp, slot);
      final long recordBase = PageLayout.HEAP_START + heapOffset;
      final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
      final int pathIdx;
      final int fieldCount;
      if (kindId == NodeFieldLayout.OBJECT_KEY_PAX_KIND_ID) {
        pathIdx = NodeFieldLayout.OBJKEY_PAX_PATH_NODE_KEY;
        fieldCount = NodeFieldLayout.OBJECT_KEY_PAX_FIELD_COUNT;
      } else {
        pathIdx = NodeFieldLayout.OBJKEY_PATH_NODE_KEY;
        fieldCount = NodeFieldLayout.OBJECT_KEY_FIELD_COUNT;
      }
      final long offsetTable = recordBase + 1;
      final long dataStart = offsetTable + fieldCount;
      final int parentFieldOff =
          sp.get(ValueLayout.JAVA_BYTE, offsetTable + NodeFieldLayout.OBJKEY_PARENT_KEY) & 0xFF;
      final int firstChildFieldOff =
          sp.get(ValueLayout.JAVA_BYTE, offsetTable + NodeFieldLayout.OBJKEY_FIRST_CHILD_KEY) & 0xFF;
      final int pathFieldOff = sp.get(ValueLayout.JAVA_BYTE, offsetTable + pathIdx) & 0xFF;
      outParentKeys[i] =
          DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + parentFieldOff, nodeKey);
      outFirstChildKeys[i] =
          DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + firstChildFieldOff, nodeKey);
      outPathNodeKeys[i] =
          DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + pathFieldOff, nodeKey);
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
    if (sp == null) {
      for (int i = 0; i < count; i++) {
        outParentKeys[i] = -1L;
        outFirstChildKeys[i] = -1L;
      }
      return;
    }
    for (int i = 0; i < count; i++) {
      final int slot = slots[i];
      final long nodeKey = pageBase + slot;
      final int heapOffset = PageLayout.getDirHeapOffset(sp, slot);
      final long recordBase = PageLayout.HEAP_START + heapOffset;
      final int kindId = sp.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
      final int fieldCount = (kindId == NodeFieldLayout.OBJECT_KEY_PAX_KIND_ID)
          ? NodeFieldLayout.OBJECT_KEY_PAX_FIELD_COUNT
          : NodeFieldLayout.OBJECT_KEY_FIELD_COUNT;
      final long offsetTable = recordBase + 1;
      final long dataStart = offsetTable + fieldCount;
      final int parentFieldOff =
          sp.get(ValueLayout.JAVA_BYTE, offsetTable + NodeFieldLayout.OBJKEY_PARENT_KEY) & 0xFF;
      final int firstChildFieldOff =
          sp.get(ValueLayout.JAVA_BYTE, offsetTable + NodeFieldLayout.OBJKEY_FIRST_CHILD_KEY) & 0xFF;
      outParentKeys[i] =
          DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + parentFieldOff, nodeKey);
      outFirstChildKeys[i] =
          DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + firstChildFieldOff, nodeKey);
    }
  }

  /**
   * Decode a numeric value from an OBJECT_NUMBER_VALUE slot directly off the
   * slotted page — no moveTo, no singleton binding, no Number boxing. Returns
   * {@code Long.MIN_VALUE} if the payload's number type isn't Integer or Long
   * (i.e. float/double/BigDecimal — caller should fall back to the full
   * cursor path).
   *
   * <p>Caller must verify the slot holds an OBJECT_NUMBER_VALUE (kind id 42).
   */
  public long getNumberValueLongFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNUMVAL_PAYLOAD) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NUMBER_VALUE_FIELD_COUNT;
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
   * Read the boolean payload of an OBJECT_BOOLEAN_VALUE slot directly off the
   * slotted page without going through rtx / node materialisation. Two byte
   * reads: the offset-table entry for the value field, then the value byte
   * itself. Caller is responsible for verifying the slot holds an
   * OBJECT_BOOLEAN_VALUE (e.g. via {@link #getSlotNodeKindId}); this method
   * does not double-check to keep the hot path branchless.
   */
  public boolean getObjectBooleanValueFromSlot(final int slotNumber) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    // Offset table lives at recordBase+1..recordBase+FIELD_COUNT, one byte per field.
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJBOOLVAL_VALUE) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_BOOLEAN_VALUE_FIELD_COUNT;
    return sp.get(ValueLayout.JAVA_BYTE, dataStart + fieldOff) != 0;
  }

  /** Node kind id for OBJECT_NUMBER_VALUE. */
  public static int objectNumberValueKindId() {
    return OBJECT_NUMBER_VALUE_KIND_ID;
  }

  /** Node kind id for OBJECT_KEY. */
  public static int objectKeyKindId() {
    return OBJECT_KEY_KIND_ID;
  }

  /** Node kind id for OBJECT_STRING_VALUE. */
  public static int objectStringValueKindId() {
    return OBJECT_STRING_VALUE_KIND_ID;
  }

  /**
   * Package-private view of {@link #readObjectStringValueBytesForRegionBuild} exposed
   * so {@code PageKind}'s slotted-page writer can pre-encode the StringRegion at
   * commit time (moving the cost off the scan hot path).
   */
  byte[] readObjectStringValueBytesForRegionBuildPkg(final int slotNumber) {
    return readObjectStringValueBytesForRegionBuild(slotNumber);
  }

  /**
   * Read the delta-encoded parent nodeKey from an OBJECT_NUMBER_VALUE slot
   * directly off the slotted page. Used by the writer to tag each numeric
   * region entry with its parent OBJECT_KEY's nameKey.
   *
   * <p>Caller must verify the slot holds an OBJECT_NUMBER_VALUE (kind id 42).
   */
  public long getObjectNumberValueParentKeyFromSlot(final int slotNumber, final long valueNodeKey) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNUMVAL_PARENT_KEY) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_NUMBER_VALUE_FIELD_COUNT;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, valueNodeKey);
  }

  /**
   * Read the delta-encoded parent nodeKey from an OBJECT_STRING_VALUE slot. Used by
   * {@link #tryBuildStringRegionFromSlottedPage} to group each string under its parent
   * OBJECT_KEY's nameKey (the {@code StringRegion} tag).
   *
   * <p>Caller must verify the slot holds an OBJECT_STRING_VALUE (kind id 40).
   */
  public long getObjectStringValueParentKeyFromSlot(final int slotNumber, final long valueNodeKey) {
    final MemorySegment sp = slottedPage;
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotNumber);
    final long recordBase = PageLayout.HEAP_START + heapOffset;
    final int fieldOff =
        sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJSTRVAL_PARENT_KEY) & 0xFF;
    final long dataStart = recordBase + 1 + NodeFieldLayout.OBJECT_STRING_VALUE_FIELD_COUNT;
    return DeltaVarIntCodec.decodeDeltaFromSegment(sp, dataStart + fieldOff, valueNodeKey);
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
  private static final int OBJECT_KEY_KIND_ID = 26;

  /** Thread-local scratch for SIMD findMatchingSlots output. Avoids per-page int[] alloc. */
  private static final ThreadLocal<int[]> MATCHING_SLOTS_SCRATCH =
      ThreadLocal.withInitial(() -> new int[256]);

  static boolean isObjectKeyKindId(final int kindId) {
    return kindId == OBJECT_KEY_KIND_ID || kindId == NodeFieldLayout.OBJECT_KEY_PAX_KIND_ID;
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
    final byte[] nameKeyPayload = regionPayload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
    if (nameKeyPayload != null) {
      final int upperBound = io.sirix.page.pax.ObjectKeyNameKeyRegion.count(nameKeyPayload);
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
      final int matched = io.sirix.page.pax.ObjectKeyNameKeyRegion.findMatchingSlots(
          nameKeyPayload, fieldKey, tmp);
      if (matched == 0) return cachePut(cache, fieldKey, EMPTY_INT_ARRAY);
      final int[] result = Arrays.copyOf(tmp, matched);
      return cachePut(cache, fieldKey, result);
    }

    // Slow path (region absent): walk populated-slot bitmap in-line. Avoids
    // forEachPopulatedSlot's lambda — direct bit scan inlines cleanly.
    int[] buf = new int[32];
    int count = 0;
    for (int wordIndex = 0; wordIndex < PageLayout.BITMAP_WORDS; wordIndex++) {
      long word = PageLayout.getBitmapWord(sp, wordIndex);
      final int baseSlot = wordIndex << 6;
      while (word != 0) {
        final int bit = Long.numberOfTrailingZeros(word);
        final int slot = baseSlot + bit;
        if (isObjectKeyKindId(PageLayout.getDirNodeKindId(sp, slot))
            && getObjectKeyNameKeyFromSlot(slot) == fieldKey) {
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
    if (slottedPage != null) {
      PageLayout.copyBitmapTo(slottedPage, copy);
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
    return slottedPage;
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
    this.slottedPage = newSlottedPage.reinterpret(Long.MAX_VALUE);
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

  /**
   * Finalizer for detecting page leaks during development.
   * <p>
   * This method logs a warning if a page is garbage collected without being
   * properly closed, indicating a potential memory leak. The warning is only
   * generated when diagnostic settings are enabled.
   * <p>
   * <b>Note:</b> Finalizers are deprecated in modern Java. This is retained
   * solely for leak detection during development and testing.
   *
   * @deprecated Finalizers are discouraged. This exists only for leak detection.
   */
  @Override
  @Deprecated(forRemoval = false)
  protected void finalize() {
    if (!isClosed() && DEBUG_MEMORY_LEAKS) {
      PAGES_FINALIZED_WITHOUT_CLOSE.incrementAndGet();
      
      // Track by type and pageKey for detailed leak analysis
      if (indexType != null) {
        FINALIZED_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
      }
      FINALIZED_BY_PAGE_KEY.computeIfAbsent(recordPageKey, _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
      
      // Log leak information (only when diagnostics enabled)
      if (LOGGER.isWarnEnabled()) {
        StringBuilder leakMsg = new StringBuilder();
        leakMsg.append(String.format("Page leak detected: pageKey=%d, type=%s, revision=%d - not closed explicitly",
            recordPageKey, indexType, revision));
        
        if (creationStackTrace != null && LOGGER.isDebugEnabled()) {
          leakMsg.append("\n  Creation stack trace:");
          for (int i = 2; i < Math.min(creationStackTrace.length, 8); i++) {
            StackTraceElement frame = creationStackTrace[i];
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

    // Clear FSST symbol table
    fsstSymbolTable = null;

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
    this.fsstSymbolTable = symbolTable;
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
    this.regionTable = table;
  }

  /**
   * Direct payload lookup for a region kind. Returns {@code null} when no table
   * is present or the region is absent. Inlineable one-branch hot-path shim.
   */
  public byte[] regionPayload(final byte kind) {
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
   * Cached {@link MemorySegment} view over the {@link RegionTable#KIND_NUMBER} payload.
   * Wrapping a {@code byte[]} via {@code MemorySegment.ofArray} allocates a small
   * {@code HeapMemorySegmentImpl} instance per call (~24 B). On the SIMD scan hot path
   * that fires once per page per query, which compounds into real GC pressure on
   * 100M-record workloads. Caching the view alongside the parsed header collapses the
   * per-page cost to one volatile read.
   *
   * <p>Lifecycle: populated by {@link #getNumberRegionPayloadSegment()} on first call,
   * cleared by {@link #invalidateNumberRegion()} together with the parsed header.
   */
  private volatile MemorySegment cachedNumberPayloadSegment;

  /**
   * Drop the cached {@link NumberRegion.Header} and the {@link RegionTable#KIND_NUMBER}
   * payload so the next reader rebuilds from the slotted page. Called from every mutation
   * path that adds, modifies, or removes an OBJECT_NUMBER_VALUE / NUMBER_VALUE record.
   *
   * <h2>HFT cost model</h2>
   * Steady-state cost when no region is currently cached: one volatile read + one branch.
   * On the first invalidation per page after a region was built: one volatile write +
   * one {@code byte[][]} slot store. After that, calls collapse to the fast-path until
   * the next reader rebuilds.
   *
   * <p>Package-private so unit tests can verify the contract without reflection.
   */
  void invalidateNumberRegion() {
    if (cachedNumberHeader == null && cachedNumberPayloadSegment == null) {
      return;
    }
    cachedNumberHeader = null;
    cachedNumberPayloadSegment = null;
    final RegionTable rt = regionTable;
    if (rt != null) {
      rt.set(RegionTable.KIND_NUMBER, null);
    }
  }

  /**
   * Invalidate the cached number region iff the slot at {@code slotOffset} currently
   * holds a number-typed record. Called from {@link #setRecord} and {@link #setNewRecord}
   * before mutation, so deletion or replacement of an existing number record is detected
   * even when the new record kind is something else (e.g. {@code DeletedNode}).
   *
   * <p>The "new record IS a number" case is handled separately by {@link #serializeToHeap}
   * and {@link #completeDirectWrite} which already know the kind id being written.
   */
  private void maybeInvalidateNumberRegionForExistingSlot(final int slotOffset) {
    if (cachedNumberHeader == null) {
      return;
    }
    final MemorySegment sp = slottedPage;
    if (sp == null || !PageLayout.isSlotPopulated(sp, slotOffset)) {
      return;
    }
    if (isNumberValueKindId(PageLayout.getDirNodeKindId(sp, slotOffset))) {
      invalidateNumberRegion();
    }
  }

  public NumberRegion.Header getNumberRegionHeader() {
    NumberRegion.Header h = cachedNumberHeader;
    if (h != null) {
      return h;
    }
    byte[] payload = regionPayload(RegionTable.KIND_NUMBER);
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
        this.regionTable = donorTable;
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
   * Walk the slotted page, collect each OBJECT_NUMBER_VALUE slot's value + its
   * parent OBJECT_KEY's nameKey, encode into a NumberRegion payload. Returns
   * {@code null} when the page has no numeric values or no slotted page yet.
   *
   * <p>Side-effect: on success, attaches the region to the page so subsequent
   * lookups skip this build.
   */
  private byte[] tryBuildNumberRegionFromSlottedPage() {
    final MemorySegment sp = slottedPage;
    if (sp == null) {
      return null;
    }
    final int numberKindId = OBJECT_NUMBER_VALUE_KIND_ID;
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
        if (PageLayout.getDirNodeKindId(sp, slot) == numberKindId) {
          final long value = getNumberValueLongFromSlot(slot);
          if (value != Long.MIN_VALUE) {
            final long valueNodeKey = pageKeyBase + slot;
            final long parentKey = getObjectNumberValueParentKeyFromSlot(slot, valueNodeKey);
            int parentNameKey = -1;
            int parentPathNodeKeyInt = -1;
            if ((parentKey >>> Constants.NDP_NODE_COUNT_EXPONENT) == recordPageKey) {
              final int parentSlot = (int) (parentKey & (PageLayout.SLOT_COUNT - 1));
              if (isObjectKeyKindId(PageLayout.getDirNodeKindId(sp, parentSlot))) {
                parentNameKey = getObjectKeyNameKeyFromSlot(parentSlot);
                if (allPathNodeKeysValid) {
                  final long pnk = getObjectKeyPathNodeKeyFromSlot(parentSlot, parentKey);
                  if (pnk > 0L && pnk <= (long) Integer.MAX_VALUE) {
                    parentPathNodeKeyInt = (int) pnk;
                  } else {
                    allPathNodeKeysValid = false;
                  }
                }
              } else {
                allPathNodeKeysValid = false;
              }
            } else {
              allPathNodeKeysValid = false;
            }
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
        }
        word &= word - 1;
      }
    }
    if (count == 0) {
      return null;
    }
    final byte tagKind = allPathNodeKeysValid ? NumberRegion.TAG_KIND_PATH_NODE : NumberRegion.TAG_KIND_NAME;
    final int[] tagBuf = allPathNodeKeysValid ? pathBuf : nameBuf;
    final byte[] payload = NumberRegion.encode(valBuf, tagBuf, count, tagKind);
    // Preserve any existing regionTable (e.g. KIND_OBJECT_KEY_NAMEKEY) — overwriting
    // with a fresh RegionTable would silently drop other regions on this page.
    RegionTable table = this.regionTable;
    if (table == null) {
      table = new RegionTable();
      this.regionTable = table;
    }
    table.set(RegionTable.KIND_NUMBER, payload);
    return payload;
  }

  /**
   * Returns the raw number-region payload bytes or {@code null}. Paired with
   * {@link #getNumberRegionHeader()} for scan operators that decode inline.
   */
  public byte[] getNumberRegionPayload() {
    return regionPayload(RegionTable.KIND_NUMBER);
  }

  // ============================================================
  // KIND_STRING region — dictionary-encoded OBJECT_STRING_VALUE column.
  // ============================================================

  /**
   * Lazily parsed {@link StringRegion.Header} for the page's
   * dictionary-encoded string column. {@code null} if the page has no
   * OBJECT_STRING_VALUE records or the region hasn't been built yet.
   */
  private volatile StringRegion.Header cachedStringHeader;

  /** Cached {@link MemorySegment} view over the {@link RegionTable#KIND_STRING} payload. */
  private volatile MemorySegment cachedStringPayloadSegment;

  /**
   * Drop the cached string-region parsed header + payload-segment view so the next
   * reader rebuilds. Called from every mutation path that adds, modifies, or removes
   * an OBJECT_STRING_VALUE / STRING_VALUE record.
   */
  void invalidateStringRegion() {
    if (cachedStringHeader == null && cachedStringPayloadSegment == null) {
      return;
    }
    cachedStringHeader = null;
    cachedStringPayloadSegment = null;
    final RegionTable rt = regionTable;
    if (rt != null) {
      rt.set(RegionTable.KIND_STRING, null);
    }
  }

  /**
   * Invalidate the cached string region iff the slot at {@code slotOffset} currently
   * holds a string-typed record (same pattern as number variant).
   */
  private void maybeInvalidateStringRegionForExistingSlot(final int slotOffset) {
    if (cachedStringHeader == null) {
      return;
    }
    final MemorySegment sp = slottedPage;
    if (sp == null || !PageLayout.isSlotPopulated(sp, slotOffset)) {
      return;
    }
    if (isStringValueKindId(PageLayout.getDirNodeKindId(sp, slotOffset))) {
      invalidateStringRegion();
    }
  }

  /**
   * Thread-local scratch for {@link #readObjectStringValueBytesForRegionBuild}.
   * One array per worker thread, reused for every value read during a region build.
   * 1 KiB matches typical OBJECT_STRING_VALUE max length on JSON-like workloads;
   * grows on first oversize.
   */
  private static final ThreadLocal<byte[]> STRING_REGION_BUILD_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[1024]);

  /**
   * Read the OBJECT_STRING_VALUE payload at {@code slotNumber} as raw UTF-8 bytes,
   * decompressing the FSST-encoded form when the value is compressed.
   *
   * <p>Used only by {@link #tryBuildStringRegionFromSlottedPage} (called once per
   * page-cache miss). Allocates a fresh trimmed {@code byte[]} for the value
   * because the {@code StringRegion.Encoder} retains references to keep them
   * stable through {@code finish()}; the returned array's lifetime matches the
   * region payload's lifetime.
   */
  private byte[] readObjectStringValueBytesForRegionBuild(final int slotNumber) {
    byte[] scratch = STRING_REGION_BUILD_SCRATCH.get();
    int n = readObjectStringValueBytesFromSlot(slotNumber, scratch);
    if (n == -1) {
      // Returned -1 means scratch too small or other read failure — try once with
      // a larger scratch (grow + retry) before giving up.
      if (scratch.length < (64 * 1024)) {
        scratch = new byte[scratch.length * 4];
        STRING_REGION_BUILD_SCRATCH.set(scratch);
        n = readObjectStringValueBytesFromSlot(slotNumber, scratch);
      }
    }
    if (n <= 0) return null;
    // Trimmed copy is required: the encoder keeps the reference until finish().
    final byte[] out = new byte[n];
    System.arraycopy(scratch, 0, out, 0, n);
    return out;
  }

  /**
   * Walk the slotted page, collect each OBJECT_STRING_VALUE slot's value + its
   * parent OBJECT_KEY's nameKey, encode into a StringRegion payload.
   *
   * <p>Returns {@code null} when the page has no string values or no slotted page yet.
   * Side-effect: on success, attaches the region to the page so subsequent lookups
   * skip this build.
   */
  private byte[] tryBuildStringRegionFromSlottedPage() {
    final MemorySegment sp = slottedPage;
    if (sp == null) {
      return null;
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
        if (PageLayout.getDirNodeKindId(sp, slot) != OBJECT_STRING_VALUE_KIND_ID) {
          continue;
        }
        final byte[] value = readObjectStringValueBytesForRegionBuild(slot);
        if (value == null) continue;
        final long valueNodeKey = pageKeyBase + slot;
        final long parentKey = getObjectStringValueParentKeyFromSlot(slot, valueNodeKey);
        int parentNameKey = -1;
        int parentPathNodeKeyInt = -1;
        if ((parentKey >>> Constants.NDP_NODE_COUNT_EXPONENT) == recordPageKey) {
          final int parentSlot = (int) (parentKey & (PageLayout.SLOT_COUNT - 1));
          if (isObjectKeyKindId(PageLayout.getDirNodeKindId(sp, parentSlot))) {
            parentNameKey = getObjectKeyNameKeyFromSlot(parentSlot);
            if (allPathNodeKeysValid) {
              final long pnk = getObjectKeyPathNodeKeyFromSlot(parentSlot, parentKey);
              if (pnk > 0L && pnk <= (long) Integer.MAX_VALUE) {
                parentPathNodeKeyInt = (int) pnk;
              } else {
                allPathNodeKeysValid = false;
              }
            }
          } else {
            allPathNodeKeysValid = false;
          }
        } else {
          allPathNodeKeysValid = false;
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
    final byte[] payload;
    if (allPathNodeKeysValid && pathEnc != null) {
      payload = pathEnc.finish(StringRegion.TAG_KIND_PATH_NODE);
    } else {
      payload = nameEnc.finish(StringRegion.TAG_KIND_NAME);
    }
    RegionTable table = this.regionTable;
    if (table == null) {
      table = new RegionTable();
      this.regionTable = table;
    }
    table.set(RegionTable.KIND_STRING, payload);
    return payload;
  }

  public StringRegion.Header getStringRegionHeader() {
    StringRegion.Header h = cachedStringHeader;
    if (h != null) {
      return h;
    }
    byte[] payload = regionPayload(RegionTable.KIND_STRING);
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
  public byte[] getStringRegionPayload() {
    return regionPayload(RegionTable.KIND_STRING);
  }

  /** Cached {@link MemorySegment} view over the string-region payload. */
  public MemorySegment getStringRegionPayloadSegment() {
    MemorySegment seg = cachedStringPayloadSegment;
    if (seg != null) return seg;
    final byte[] payload = regionPayload(RegionTable.KIND_STRING);
    if (payload == null) return null;
    seg = MemorySegment.ofArray(payload);
    cachedStringPayloadSegment = seg;
    return seg;
  }

  /**
   * Returns a cached {@link MemorySegment} view over the number-region payload, or
   * {@code null} when the page has no number region. The view is built once per page
   * (on first call after the region is materialised) and reused across every scan.
   *
   * <p>HFT motivation: {@code MemorySegment.ofArray(byte[])} allocates a fresh
   * {@code HeapMemorySegmentImpl} wrapper on every call. On a 100M-record scan that
   * touches ~6K pages, paying that allocation per query is GC pressure we don't need.
   * Caching collapses it to one read of a volatile reference.
   */
  public MemorySegment getNumberRegionPayloadSegment() {
    MemorySegment seg = cachedNumberPayloadSegment;
    if (seg != null) {
      return seg;
    }
    final byte[] payload = regionPayload(RegionTable.KIND_NUMBER);
    if (payload == null) {
      return null;
    }
    // Benign race: parallel callers may build duplicate views, but the resulting
    // segment is identical and the last write wins. Avoids synchronised block on
    // the hot read path.
    seg = MemorySegment.ofArray(payload);
    cachedNumberPayloadSegment = seg;
    return seg;
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
   * Acquire a guard on this page (increment guard count).
   * Pages with active guards cannot be evicted.
   */
  public void acquireGuard() {
    guardCount.incrementAndGet();
  }

  /**
   * Try to acquire a guard on this page.
   * Returns false if the page is orphaned or closed (cannot be used).
   * This is the synchronized version that prevents race conditions with close().
   *
   * @return true if guard was acquired, false if page is orphaned/closed
   */
  public synchronized boolean tryAcquireGuard() {
    int flags = (int) STATE_FLAGS_HANDLE.getVolatile(this);
    if ((flags & (ORPHANED_BIT | CLOSED_BIT)) != 0) {
      return false;
    }
    guardCount.incrementAndGet();
    return true;
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
          // Unbound flyweight (e.g., value mutation caused unbind): re-serialize to slotted page heap.
          if (slottedPage != null) {
            final long nodeKey = record.getNodeKey();
            final int offset = StorageEngineReader.recordPageOffset(nodeKey);
            serializeToHeap(fn, nodeKey, offset);
            records[i] = null;
            continue;
          }
        }
        final var recordID = record.getNodeKey();
        final var offset = StorageEngineReader.recordPageOffset(recordID);

        // Clear buffer for reuse (reset position to 0, keeps capacity)
        reusableOut.clear();

        // Serialize into the reusable buffer
        recordPersister.serialize(reusableOut, record, resourceConfiguration);
        final var buffer = reusableOut.getDestination();

        if (buffer.byteSize() > PageConstants.MAX_RECORD_SIZE) {
          // Overflow page: copy to byte array for storage
          byte[] persistentBuffer = new byte[(int) buffer.byteSize()];
          MemorySegment.copy(buffer, 0, MemorySegment.ofArray(persistentBuffer), 0, buffer.byteSize());

          final var reference = new PageReference();
          reference.setPage(new OverflowPage(persistentBuffer));
          references.put(recordID, reference);
        } else {
          // Normal record: setSlot copies data to slotted page heap (slotted page heap)
          setSlot(buffer, offset);
        }
        // Clear record reference after serialization — snapshot isolation.
        // Data is now in slotMemory/slottedPage; prevents cross-transaction aliasing.
        records[i] = null;
      }
    } // Confined arena automatically closes here, freeing all temporary buffers
  }

  /**
   * Build FSST symbol table from all string values in this page.
   * This should be called before serialization to enable page-level compression.
   *
   * @param resourceConfig the resource configuration
   * @return true if FSST compression is enabled and symbol table was built
   */
  public boolean buildFsstSymbolTable(ResourceConfiguration resourceConfig) {
    if (resourceConfig.stringCompressionType != StringCompressionType.FSST) {
      return false;
    }

    final int stringValueId = NodeKind.STRING_VALUE.getId();
    final int objectStringValueId = NodeKind.OBJECT_STRING_VALUE.getId();

    // Collect all string values from StringNode and ObjectStringNode
    java.util.ArrayList<byte[]> stringSamples = new java.util.ArrayList<>();

    // Scan records[] for non-FlyweightNode string records (legacy path)
    if (records != null) {
      for (final DataRecord record : records) {
        if (record == null) {
          continue;
        }
        if (record instanceof StringNode stringNode) {
          byte[] value = stringNode.getRawValueWithoutDecompression();
          if (value != null && value.length > 0) {
            stringSamples.add(value);
          }
        } else if (record instanceof ObjectStringNode objectStringNode) {
          byte[] value = objectStringNode.getRawValueWithoutDecompression();
          if (value != null && value.length > 0) {
            stringSamples.add(value);
          }
        }
      }
    }

    // Scan slotted page for FlyweightNode strings (zero records[] path)
    if (slottedPage != null) {
      for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
        if (records != null && records[i] != null) continue; // Already scanned above
        if (!PageLayout.isSlotPopulated(slottedPage, i)) continue;
        final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, i);
        if (nodeKindId == stringValueId || nodeKindId == objectStringValueId) {
          final int heapOff = PageLayout.getDirHeapOffset(slottedPage, i);
          final long recordBase = PageLayout.heapAbsoluteOffset(heapOff);
          final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + i;
          if (nodeKindId == stringValueId) {
            fsstStringFlyweight().bind(slottedPage, recordBase, nodeKey, i);
            try {
              byte[] value = fsstStringFlyweight().getRawValueWithoutDecompression();
              if (value != null && value.length > 0) stringSamples.add(value);
            } finally {
              fsstStringFlyweight().clearBinding();
            }
          } else {
            fsstObjStringFlyweight().bind(slottedPage, recordBase, nodeKey, i);
            try {
              byte[] value = fsstObjStringFlyweight().getRawValueWithoutDecompression();
              if (value != null && value.length > 0) stringSamples.add(value);
            } finally {
              fsstObjStringFlyweight().clearBinding();
            }
          }
        }
      }
    }

    // Build symbol table only if we have enough strings to make it worthwhile
    if (stringSamples.size() >= FSSTCompressor.MIN_SAMPLES_FOR_TABLE) {
      byte[] candidateTable = FSSTCompressor.buildSymbolTable(stringSamples);

      // Only apply FSST if trial compression shows >= 15% savings (adaptive threshold)
      // This prevents overhead from exceeding savings for low-entropy data
      if (candidateTable != null && candidateTable.length > 0
          && FSSTCompressor.isCompressionBeneficial(stringSamples, candidateTable)) {
        this.fsstSymbolTable = candidateTable;
        return true;
      }
    }

    return false;
  }

  /**
   * Compress all string values in the page using the pre-built FSST symbol table.
   * This modifies the string nodes in place to use compressed values.
   * Must be called after buildFsstSymbolTable().
   */
  public void compressStringValues() {
    if (fsstSymbolTable == null || fsstSymbolTable.length == 0) {
      return;
    }

    final int stringValueId = NodeKind.STRING_VALUE.getId();
    final int objectStringValueId = NodeKind.OBJECT_STRING_VALUE.getId();

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
              byte[] compressedValue = FSSTCompressor.encode(originalValue, fsstSymbolTable);
              if (compressedValue.length < originalValue.length) {
                stringNode.setRawValue(compressedValue, true, fsstSymbolTable);
              }
            }
          }
        } else if (record instanceof ObjectStringNode objectStringNode) {
          if (!objectStringNode.isCompressed()) {
            byte[] originalValue = objectStringNode.getRawValueWithoutDecompression();
            if (originalValue != null && originalValue.length > 0) {
              byte[] compressedValue = FSSTCompressor.encode(originalValue, fsstSymbolTable);
              if (compressedValue.length < originalValue.length) {
                objectStringNode.setRawValue(compressedValue, true, fsstSymbolTable);
              }
            }
          }
        }
      }
    }

    // Compress slotted page strings (zero records[] path)
    if (slottedPage != null) {
      for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
        if (records != null && records[i] != null) continue; // Already handled above
        if (!PageLayout.isSlotPopulated(slottedPage, i)) continue;
        final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, i);
        if (nodeKindId != stringValueId && nodeKindId != objectStringValueId) continue;

        final int heapOff = PageLayout.getDirHeapOffset(slottedPage, i);
        final long recordBase = PageLayout.heapAbsoluteOffset(heapOff);
        final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + i;

        if (nodeKindId == stringValueId) {
          fsstStringFlyweight().bind(slottedPage, recordBase, nodeKey, i);
          fsstStringFlyweight().setOwnerPage(this); // Enable write-through
          try {
            byte[] originalValue = fsstStringFlyweight().getRawValueWithoutDecompression();
            if (originalValue != null && originalValue.length > 0 && !fsstStringFlyweight().isCompressed()) {
              byte[] compressed = FSSTCompressor.encode(originalValue, fsstSymbolTable);
              if (compressed.length < originalValue.length) {
                fsstStringFlyweight().setRawValue(compressed, true, fsstSymbolTable);
              }
            }
          } finally {
            fsstStringFlyweight().setOwnerPage(null);
            fsstStringFlyweight().clearBinding();
          }
        } else {
          fsstObjStringFlyweight().bind(slottedPage, recordBase, nodeKey, i);
          fsstObjStringFlyweight().setOwnerPage(this); // Enable write-through
          try {
            byte[] originalValue = fsstObjStringFlyweight().getRawValueWithoutDecompression();
            if (originalValue != null && originalValue.length > 0 && !fsstObjStringFlyweight().isCompressed()) {
              byte[] compressed = FSSTCompressor.encode(originalValue, fsstSymbolTable);
              if (compressed.length < originalValue.length) {
                fsstObjStringFlyweight().setRawValue(compressed, true, fsstSymbolTable);
              }
            }
          } finally {
            fsstObjStringFlyweight().setOwnerPage(null);
            fsstObjStringFlyweight().clearBinding();
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
      } else if (record instanceof ObjectStringNode objectStringNode) {
        objectStringNode.setFsstSymbolTable(fsstSymbolTable);
      }
    }
  }

}

