package io.sirix.page;

import com.google.common.base.MoreObjects;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.settings.Constants;
import io.sirix.settings.DiagnosticSettings;
import io.sirix.settings.StringCompressionType;
import io.sirix.utils.FSSTCompressor;
import io.sirix.utils.ArrayIterator;
import io.sirix.utils.OS;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesOut;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
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

  /** Reusable flyweight for FSST compression of StringNode slots on slotted page. */
  private final StringNode fsstStringFlyweight = new StringNode(0, null);

  /** Reusable flyweight for FSST compression of ObjectStringNode slots on slotted page. */
  private final ObjectStringNode fsstObjStringFlyweight = new ObjectStringNode(0, null);



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

  private MemorySegmentAllocator segmentAllocator =
      OS.isWindows() ? WindowsMemorySegmentAllocator.getInstance() : LinuxMemorySegmentAllocator.getInstance();

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

  /**
   * Constructor which initializes a new {@link KeyValueLeafPage}.
   * Memory is externally provided (e.g., by Arena in tests) and will NOT be released by close().
   *
   * @param recordPageKey  base key assigned to this node page
   * @param indexType      the index type
   * @param resourceConfig the resource configuration
   */
  public KeyValueLeafPage(final @NonNegative long recordPageKey, final IndexType indexType,
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
  public KeyValueLeafPage(final @NonNegative long recordPageKey, final IndexType indexType,
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
  public void setRecord(@NonNull final DataRecord record) {
    addedReferences = false;
    // Invalidate stale compressed cache — record mutation means cached bytes are outdated
    compressedSegment = null;
    bytes = null;
    final var key = record.getNodeKey();
    final var offset = (int) (key - ((key >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));

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
  public void setNewRecord(@NonNull final DataRecord record) {
    assert !(record instanceof FlyweightNode)
        : "FlyweightNode must not go through setNewRecord — use serializeNewRecord";
    addedReferences = false;
    compressedSegment = null;
    bytes = null;
    final var key = record.getNodeKey();
    final var offset = (int) (key - ((key >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
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
    final int heapEnd = PageLayout.getHeapEnd(slottedPage);
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
    PageLayout.setHeapEnd(slottedPage, heapEnd + totalBytes);
    PageLayout.setHeapUsed(slottedPage, PageLayout.getHeapUsed(slottedPage) + totalBytes);

    // Update directory entry: [heapOffset][dataLength | nodeKindId]
    PageLayout.setDirEntry(slottedPage, offset, heapEnd, totalBytes,
        ((NodeKind) fn.getKind()).getId());

    // Mark slot populated in bitmap and track last slot index (new slots only)
    if (!PageLayout.isSlotPopulated(slottedPage, offset)) {
      PageLayout.markSlotPopulated(slottedPage, offset);
      PageLayout.setPopulatedCount(slottedPage,
          PageLayout.getPopulatedCount(slottedPage) + 1);
      lastSlotIndex = offset;
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
    int heapEnd = PageLayout.getHeapEnd(slottedPage);
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < totalEstimated) {
      growSlottedPage();
      heapEnd = PageLayout.getHeapEnd(slottedPage);
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

    final int heapEnd = PageLayout.getHeapEnd(slottedPage);
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
    PageLayout.setHeapEnd(slottedPage, heapEnd + totalBytes);
    PageLayout.setHeapUsed(slottedPage, PageLayout.getHeapUsed(slottedPage) + totalBytes);

    // Directory entry
    PageLayout.setDirEntry(slottedPage, slotOffset, heapEnd, totalBytes, nodeKindId);

    // Bitmap
    if (!PageLayout.isSlotPopulated(slottedPage, slotOffset)) {
      PageLayout.markSlotPopulated(slottedPage, slotOffset);
      PageLayout.setPopulatedCount(slottedPage,
          PageLayout.getPopulatedCount(slottedPage) + 1);
      lastSlotIndex = slotOffset;
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
    final int heapEnd = PageLayout.getHeapEnd(slottedPage);
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
    PageLayout.setHeapEnd(slottedPage, heapEnd + newTotalLen);
    // heapUsed: subtract old, add new (net change = newTotalLen - oldTotalLen)
    PageLayout.setHeapUsed(slottedPage,
        PageLayout.getHeapUsed(slottedPage) + newTotalLen - oldTotalLen);

    // --- Update directory entry ---
    PageLayout.setDirEntry(slottedPage, slotIndex, heapEnd, newTotalLen, nodeKindId);

    // --- Re-bind flyweight to new location ---
    fn.bind(slottedPage, newRecordBase, nodeKey, slotIndex);
    fn.setOwnerPage(this);
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
    final int heapEnd = PageLayout.getHeapEnd(slottedPage);
    while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < srcTotalLen) {
      growSlottedPage();
    }

    // Copy raw bytes (record body + DeweyID trailer) from source to destination heap
    final long srcAbs = PageLayout.heapAbsoluteOffset(srcHeapOffset);
    final long dstAbs = PageLayout.heapAbsoluteOffset(heapEnd);
    MemorySegment.copy(srcPage, srcAbs, slottedPage, dstAbs, srcTotalLen);

    // Update destination heap end and used counters
    PageLayout.setHeapEnd(slottedPage, heapEnd + srcTotalLen);
    PageLayout.setHeapUsed(slottedPage, PageLayout.getHeapUsed(slottedPage) + srcTotalLen);

    // Update destination directory entry
    PageLayout.setDirEntry(slottedPage, slotIndex, heapEnd, srcTotalLen, srcNodeKindId);

    // Mark slot populated in bitmap
    if (!PageLayout.isSlotPopulated(slottedPage, slotIndex)) {
      PageLayout.markSlotPopulated(slottedPage, slotIndex);
      PageLayout.setPopulatedCount(slottedPage, PageLayout.getPopulatedCount(slottedPage) + 1);
      lastSlotIndex = slotIndex;
    }

    // Invalidate compressed cache
    compressedSegment = null;
    bytes = null;
    addedReferences = false;
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
    int heapEnd = PageLayout.getHeapEnd(slottedPage);
    int remaining = slottedPageCapacity - PageLayout.HEAP_START - heapEnd;
    if (remaining < totalSize) {
      while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < totalSize) {
        growSlottedPage();
      }
      heapEnd = PageLayout.getHeapEnd(slottedPage);
    }

    // Bump-allocate and copy record data to heap
    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    MemorySegment.copy(data, 0, slottedPage, absOffset, recordSize);

    // Append DeweyID trailer (initially 0 = no DeweyID yet)
    if (areDeweyIDsStored) {
      PageLayout.writeDeweyIdTrailer(slottedPage, absOffset + totalSize, 0);
    }

    // Update heap end and used counters
    PageLayout.setHeapEnd(slottedPage, heapEnd + totalSize);
    PageLayout.setHeapUsed(slottedPage, PageLayout.getHeapUsed(slottedPage) + totalSize);

    // Update directory entry with the provided nodeKindId
    PageLayout.setDirEntry(slottedPage, slotNumber, heapEnd, totalSize, nodeKindId);

    // Mark slot populated in bitmap and track last slot index (new slots only)
    if (!PageLayout.isSlotPopulated(slottedPage, slotNumber)) {
      PageLayout.markSlotPopulated(slottedPage, slotNumber);
      PageLayout.setPopulatedCount(slottedPage,
          PageLayout.getPopulatedCount(slottedPage) + 1);
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
    int heapEnd = PageLayout.getHeapEnd(slottedPage);
    final int remaining = slottedPageCapacity - PageLayout.HEAP_START - heapEnd;
    if (remaining < dataSize) {
      while (slottedPageCapacity - PageLayout.HEAP_START - heapEnd < dataSize) {
        growSlottedPage();
      }
      heapEnd = PageLayout.getHeapEnd(slottedPage);
    }

    // Bump-allocate and copy data to heap
    final long absOffset = PageLayout.heapAbsoluteOffset(heapEnd);
    MemorySegment.copy(source, sourceOffset, slottedPage, absOffset, dataSize);

    // Update heap end and used counters
    PageLayout.setHeapEnd(slottedPage, heapEnd + dataSize);
    PageLayout.setHeapUsed(slottedPage, PageLayout.getHeapUsed(slottedPage) + dataSize);

    // Update directory entry
    PageLayout.setDirEntry(slottedPage, slotNumber, heapEnd, dataSize, nodeKindId);

    // Mark slot populated in bitmap
    if (!PageLayout.isSlotPopulated(slottedPage, slotNumber)) {
      PageLayout.markSlotPopulated(slottedPage, slotNumber);
      PageLayout.setPopulatedCount(slottedPage,
          PageLayout.getPopulatedCount(slottedPage) + 1);
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
  }



  @Override
  public int getUsedDeweyIdSize() {
    // DeweyIDs are inline in the slotted page heap — no separate memory
    return 0;
  }

  @Override
  public int getUsedSlotsSize() {
    return slottedPage != null ? PageLayout.getHeapUsed(slottedPage) : 0;
  }

  public int getSlotMemoryByteSize() {
    return slottedPage != null ? PageLayout.HEAP_START + PageLayout.getHeapEnd(slottedPage) : 0;
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
    int heapEnd = PageLayout.getHeapEnd(slottedPage);
    int remaining = slottedPageCapacity - PageLayout.HEAP_START - heapEnd;
    while (remaining < newTotalLen) {
      growSlottedPage();
      heapEnd = PageLayout.getHeapEnd(slottedPage);
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
    PageLayout.setHeapEnd(slottedPage, heapEnd + newTotalLen);
    PageLayout.setHeapUsed(slottedPage,
        PageLayout.getHeapUsed(slottedPage) + newTotalLen - oldDataLength);

    // Update directory entry
    PageLayout.setDirEntry(slottedPage, slotNumber, heapEnd, newTotalLen, nodeKindId);

    // Mark slot populated if new
    if (!slotExists) {
      PageLayout.markSlotPopulated(slottedPage, slotNumber);
      PageLayout.setPopulatedCount(slottedPage,
          PageLayout.getPopulatedCount(slottedPage) + 1);
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
    var memorySegment = getDeweyId(slotNumber);

    if (memorySegment == null) {
      return null;
    }

    return memorySegment.toArray(ValueLayout.JAVA_BYTE);
  }


  @Override
  public <C extends KeyValuePage<DataRecord>> C newInstance(@NonNegative long recordPageKey,
      @NonNull IndexType indexType, @NonNull StorageEngineReader storageEngineReader) {
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
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this).add("pagekey", recordPageKey);
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

  @Override
  public List<PageReference> getReferences() {
    return List.of(references.values().toArray(new PageReference[0]));
  }

  @Override
  public void commit(final @NonNull StorageEngineWriter pageWriteTrx) {
    addReferences(pageWriteTrx.getResourceSession().getResourceConfig());
    for (final PageReference reference : references.values()) {
      if (!(reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
          && reference.getLogKey() == Constants.NULL_ID_LONG)) {
        pageWriteTrx.commit(reference);
      }
    }
  }

  @Override
  public PageReference getOrCreateReference(@NonNegative int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setPageReference(final long key, @NonNull final PageReference reference) {
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
            fsstStringFlyweight.bind(slottedPage, recordBase, nodeKey, i);
            try {
              byte[] value = fsstStringFlyweight.getRawValueWithoutDecompression();
              if (value != null && value.length > 0) stringSamples.add(value);
            } finally {
              fsstStringFlyweight.clearBinding();
            }
          } else {
            fsstObjStringFlyweight.bind(slottedPage, recordBase, nodeKey, i);
            try {
              byte[] value = fsstObjStringFlyweight.getRawValueWithoutDecompression();
              if (value != null && value.length > 0) stringSamples.add(value);
            } finally {
              fsstObjStringFlyweight.clearBinding();
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
          fsstStringFlyweight.bind(slottedPage, recordBase, nodeKey, i);
          fsstStringFlyweight.setOwnerPage(this); // Enable write-through
          try {
            byte[] originalValue = fsstStringFlyweight.getRawValueWithoutDecompression();
            if (originalValue != null && originalValue.length > 0 && !fsstStringFlyweight.isCompressed()) {
              byte[] compressed = FSSTCompressor.encode(originalValue, fsstSymbolTable);
              if (compressed.length < originalValue.length) {
                fsstStringFlyweight.setRawValue(compressed, true, fsstSymbolTable);
              }
            }
          } finally {
            fsstStringFlyweight.setOwnerPage(null);
            fsstStringFlyweight.clearBinding();
          }
        } else {
          fsstObjStringFlyweight.bind(slottedPage, recordBase, nodeKey, i);
          fsstObjStringFlyweight.setOwnerPage(this); // Enable write-through
          try {
            byte[] originalValue = fsstObjStringFlyweight.getRawValueWithoutDecompression();
            if (originalValue != null && originalValue.length > 0 && !fsstObjStringFlyweight.isCompressed()) {
              byte[] compressed = FSSTCompressor.encode(originalValue, fsstSymbolTable);
              if (compressed.length < originalValue.length) {
                fsstObjStringFlyweight.setRawValue(compressed, true, fsstSymbolTable);
              }
            }
          } finally {
            fsstObjStringFlyweight.setOwnerPage(null);
            fsstObjStringFlyweight.clearBinding();
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

