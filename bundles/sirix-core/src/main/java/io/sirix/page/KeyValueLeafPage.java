package io.sirix.page;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.layout.FixedToCompactTransformer;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.settings.Constants;
import io.sirix.settings.DiagnosticSettings;
import io.sirix.settings.StringCompressionType;
import io.sirix.utils.FSSTCompressor;
import io.sirix.utils.ArrayIterator;
import io.sirix.utils.OS;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesIn;
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
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;

/**
 * <p>
 * An UnorderedKeyValuePage stores a set of records, commonly nodes in an unordered data structure.
 * </p>
 * <p>
 * The page currently is not thread safe (might have to be for concurrent write-transactions)!
 * </p>
 */
@SuppressWarnings({"unchecked"})
public final class KeyValueLeafPage implements KeyValuePage<DataRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueLeafPage.class);
  private static final int INT_SIZE = Integer.BYTES;

  /**
   * SIMD vector species for bitmap operations. Uses the preferred species for the current platform
   * (256-bit AVX2 or 512-bit AVX-512).
   */
  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;

  /**
   * Unaligned int layout for zero-copy deserialization. When slotMemory is a slice of the
   * decompression buffer, it may not be 4-byte aligned.
   */
  private static final ValueLayout.OfInt JAVA_INT_UNALIGNED = ValueLayout.JAVA_INT.withByteAlignment(1);

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

  // TRACK ALL LIVE PAGES - for leak detection (use object identity, not recordPageKey)
  // CRITICAL: Use IdentityHashMap to track by object identity, not equals/hashCode
  public static final java.util.Set<KeyValueLeafPage> ALL_LIVE_PAGES =
      java.util.Collections.synchronizedSet(java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()));

  // LEAK DETECTION: Track finalized pages
  public static final java.util.concurrent.atomic.AtomicLong PAGES_FINALIZED_WITHOUT_CLOSE =
      new java.util.concurrent.atomic.AtomicLong(0);

  // Track finalized pages by type and pageKey for diagnostics
  public static final java.util.concurrent.ConcurrentHashMap<IndexType, java.util.concurrent.atomic.AtomicLong> FINALIZED_BY_TYPE =
      new java.util.concurrent.ConcurrentHashMap<>();
  public static final java.util.concurrent.ConcurrentHashMap<Long, java.util.concurrent.atomic.AtomicLong> FINALIZED_BY_PAGE_KEY =
      new java.util.concurrent.ConcurrentHashMap<>();

  // Track all Page 0 instances for explicit cleanup
  // CRITICAL: Use synchronized IdentityHashSet to track by object identity, not equals/hashCode
  // (Multiple Page 0 instances with same recordPageKey/revision would collide in regular Set)
  public static final java.util.Set<KeyValueLeafPage> ALL_PAGE_0_INSTANCES =
      java.util.Collections.synchronizedSet(java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()));

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

  private final boolean doResizeMemorySegmentsIfNeeded;

  /**
   * Start of free space.
   */
  private int slotMemoryFreeSpaceStart;

  /**
   * Start of free space.
   */
  private int deweyIdMemoryFreeSpaceStart;

  /**
   * Start of free space for string values.
   */
  private int stringValueMemoryFreeSpaceStart;

  /**
   * The index of the last slot (the slot with the largest offset).
   */
  private int lastSlotIndex;

  /**
   * The index of the last slot (the slot with the largest offset).
   */
  private int lastDeweyIdIndex;

  /**
   * The index of the last string value slot.
   */
  private int lastStringValueIndex;

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
   */
  private final DataRecord[] records;

  /**
   * Number of records currently materialized in {@link #records}.
   */
  private int inMemoryRecordCount;

  /**
   * Adaptive in-memory demotion policy constants.
   */
  private static final int MIN_DEMOTION_THRESHOLD = 64;
  private static final int MAX_DEMOTION_THRESHOLD = 256;
  private static final int DEMOTION_STEP = 32;

  /**
   * Current adaptive threshold for demoting materialized records to slot memory.
   */
  private int demotionThreshold = MIN_DEMOTION_THRESHOLD;

  /**
   * Number of records re-materialized from slot memory since last demotion.
   */
  private int rematerializedRecordsSinceLastDemotion;

  /**
   * Memory segment for slots and Dewey IDs.
   */
  private MemorySegment slotMemory;
  private MemorySegment deweyIdMemory;

  /**
   * Memory segment for string values (columnar storage for better compression). Stores all string
   * content contiguously, separate from node metadata in slotMemory.
   */
  private MemorySegment stringValueMemory;

  /**
   * FSST symbol table for string compression (shared across all strings in page). Null if FSST
   * compression is not used.
   */
  private byte[] fsstSymbolTable;

  /**
   * Offset arrays to manage positions within memory segments.
   */
  private final int[] slotOffsets;
  private final int[] deweyIdOffsets;

  /**
   * Offset array for string values (maps slot -> offset in stringValueMemory).
   */
  private final int[] stringValueOffsets;

  /**
   * Bitmap tracking which slots are populated (16 longs = 1024 bits). Bit i is set iff slotOffsets[i]
   * >= 0. Used for O(k) iteration over populated slots instead of O(1024).
   */
  private final long[] slotBitmap;

  /**
   * Bitmap tracking which populated slots are currently stored in fixed in-memory layout. Bit i set
   * => slot i contains fixed-layout bytes (not compact varint/delta bytes).
   */
  private final long[] fixedFormatBitmap;

  /**
   * Node kind id for fixed-format slots, indexed by slot number. Entries are
   * {@link #NO_FIXED_SLOT_KIND} for compact slots or unknown fixed slots.
   */
  private final byte[] fixedSlotKinds;

  /**
   * Number of words in the slot bitmap (16 words * 64 bits = 1024 slots).
   */
  private static final int BITMAP_WORDS = 16;

  /**
   * Sentinel for "no fixed slot kind assigned".
   */
  private static final byte NO_FIXED_SLOT_KIND = (byte) -1;

  /**
   * Bitmap tracking which slots need preservation during lazy copy (16 longs = 1024 bits). Used by
   * DIFFERENTIAL, INCREMENTAL (full-dump), and SLIDING_SNAPSHOT versioning. Null means no
   * preservation needed (e.g., INCREMENTAL non-full-dump or FULL versioning). At commit time, slots
   * marked here but not in records[] are copied from completePageRef.
   */
  private long[] preservationBitmap;

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

  private volatile byte[] hashCode;

  private int hash;

  // Note: isClosed flag is now packed into stateFlags (bit 2) for lock-free access

  /**
   * Flag indicating whether memory was externally allocated (e.g., by Arena in tests). If true,
   * close() should NOT release memory to segmentAllocator since it wasn't allocated by it.
   */
  private final boolean externallyAllocatedMemory;

  /**
   * Thread-local serialization buffer for compactFixedSlotsForCommit (avoids per-call allocation).
   */
  private static final ThreadLocal<MemorySegmentBytesOut> COMPACT_BUFFER =
      ThreadLocal.withInitial(() -> new MemorySegmentBytesOut(256));

  private MemorySegmentAllocator segmentAllocator = OS.isWindows()
      ? WindowsMemorySegmentAllocator.getInstance()
      : LinuxMemorySegmentAllocator.getInstance();

  /**
   * Backing buffer from decompression (for zero-copy deserialization). When non-null, this buffer
   * contains the slotMemory as a slice and must be released on close(). This enables true zero-copy
   * where the decompressed data becomes the page's storage directly.
   */
  private MemorySegment backingBuffer;

  /**
   * Releaser to return backing buffer to allocator. Called on close() to return the decompression
   * buffer to the allocator pool.
   */
  private Runnable backingBufferReleaser;

  /**
   * Constructor which initializes a new {@link KeyValueLeafPage}. Memory is externally provided
   * (e.g., by Arena in tests) and will NOT be released by close().
   *
   * @param recordPageKey base key assigned to this node page
   * @param indexType the index type
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
   * @param recordPageKey base key assigned to this node page
   * @param indexType the index type
   * @param resourceConfig the resource configuration
   * @param externallyAllocatedMemory if true, memory was allocated externally and won't be released
   *        by close()
   */
  public KeyValueLeafPage(final @NonNegative long recordPageKey, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final int revisionNumber, final MemorySegment slotMemory,
      final MemorySegment deweyIdMemory, final boolean externallyAllocatedMemory) {
    // Assertions instead of requireNonNull(...) checks as it's part of the
    // internal flow.
    assert resourceConfig != null : "The resource config must not be null!";

    this.references = new ConcurrentHashMap<>();
    this.recordPageKey = recordPageKey;
    this.records = new DataRecord[Constants.NDP_NODE_COUNT];
    this.areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
    this.indexType = indexType;
    this.resourceConfig = resourceConfig;
    this.recordPersister = resourceConfig.recordPersister;
    this.revision = revisionNumber;
    this.slotOffsets = new int[Constants.NDP_NODE_COUNT];
    this.deweyIdOffsets = new int[Constants.NDP_NODE_COUNT];
    this.stringValueOffsets = new int[Constants.NDP_NODE_COUNT];
    this.slotBitmap = new long[BITMAP_WORDS]; // All bits initially 0 (no slots populated)
    this.fixedFormatBitmap = new long[BITMAP_WORDS];
    this.fixedSlotKinds = new byte[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);
    Arrays.fill(deweyIdOffsets, -1);
    Arrays.fill(stringValueOffsets, -1);
    Arrays.fill(fixedSlotKinds, NO_FIXED_SLOT_KIND);
    this.doResizeMemorySegmentsIfNeeded = true;
    this.slotMemoryFreeSpaceStart = 0;
    this.lastSlotIndex = -1;
    this.deweyIdMemoryFreeSpaceStart = 0;
    this.lastDeweyIdIndex = -1;
    this.stringValueMemoryFreeSpaceStart = 0;
    this.lastStringValueIndex = -1;
    this.slotMemory = slotMemory;
    this.deweyIdMemory = deweyIdMemory;
    this.externallyAllocatedMemory = externallyAllocatedMemory;

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
   * Memory is allocated by the global allocator and WILL be released by close().
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
      final MemorySegment deweyIdMemory, final int lastSlotIndex, final int lastDeweyIdIndex) {
    this.recordPageKey = recordPageKey;
    this.revision = revision;
    this.indexType = indexType;
    this.resourceConfig = resourceConfig;
    this.areDeweyIDsStored = areDeweyIDsStored;
    this.recordPersister = recordPersister;
    this.slotMemory = slotMemory;
    this.deweyIdMemory = deweyIdMemory;
    this.references = references;
    this.records = new DataRecord[Constants.NDP_NODE_COUNT];
    this.slotOffsets = new int[Constants.NDP_NODE_COUNT];
    this.deweyIdOffsets = new int[Constants.NDP_NODE_COUNT];
    this.stringValueOffsets = new int[Constants.NDP_NODE_COUNT];
    this.slotBitmap = new long[BITMAP_WORDS]; // Will be populated during deserialization
    this.fixedFormatBitmap = new long[BITMAP_WORDS];
    this.fixedSlotKinds = new byte[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);
    Arrays.fill(deweyIdOffsets, -1);
    Arrays.fill(stringValueOffsets, -1);
    Arrays.fill(fixedSlotKinds, NO_FIXED_SLOT_KIND);
    this.stringValueMemoryFreeSpaceStart = 0;
    this.lastStringValueIndex = -1;
    this.doResizeMemorySegmentsIfNeeded = true;
    this.slotMemoryFreeSpaceStart = 0;
    this.lastSlotIndex = lastSlotIndex;
    // Memory allocated by global allocator (e.g., during deserialization) - release on close()
    this.externallyAllocatedMemory = false;

    if (areDeweyIDsStored) {
      this.deweyIdMemoryFreeSpaceStart = 0;
      this.lastDeweyIdIndex = lastDeweyIdIndex;
    } else {
      this.deweyIdMemoryFreeSpaceStart = 0;
      this.lastDeweyIdIndex = -1;
    }

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
   * Zero-copy constructor - slotMemory IS a slice of the decompression buffer. The backing buffer is
   * released when this page is closed.
   * 
   * <p>
   * This constructor enables true zero-copy page deserialization where the decompressed data becomes
   * the page's storage directly, eliminating the per-slot MemorySegment.copy() calls that were a
   * major performance bottleneck.
   *
   * @param recordPageKey base key assigned to this node page
   * @param revision the current revision
   * @param indexType the index type
   * @param resourceConfig the resource configuration
   * @param slotOffsets pre-loaded slot offset array from serialized data
   * @param slotMemory slice of decompression buffer (NOT copied)
   * @param lastSlotIndex index of the last slot
   * @param deweyIdOffsets pre-loaded dewey ID offset array (or null)
   * @param deweyIdMemory slice for dewey IDs (or null)
   * @param lastDeweyIdIndex index of the last dewey ID slot
   * @param references overflow page references
   * @param backingBuffer full decompression buffer (for lifecycle management)
   * @param backingBufferReleaser returns buffer to allocator on close()
   */
  public KeyValueLeafPage(final long recordPageKey, final int revision, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final int[] slotOffsets, final MemorySegment slotMemory,
      final int lastSlotIndex, final int[] deweyIdOffsets, final MemorySegment deweyIdMemory,
      final int lastDeweyIdIndex, final Map<Long, PageReference> references, final MemorySegment backingBuffer,
      final Runnable backingBufferReleaser) {
    this.recordPageKey = recordPageKey;
    this.revision = revision;
    this.indexType = indexType;
    this.resourceConfig = resourceConfig;
    this.recordPersister = resourceConfig.recordPersister;
    this.areDeweyIDsStored = resourceConfig.areDeweyIDsStored;

    // Zero-copy: use provided arrays and memory segments directly
    this.slotOffsets = slotOffsets;
    this.slotMemory = slotMemory;
    this.lastSlotIndex = lastSlotIndex;

    this.deweyIdOffsets = deweyIdOffsets != null
        ? deweyIdOffsets
        : new int[Constants.NDP_NODE_COUNT];
    if (deweyIdOffsets == null) {
      Arrays.fill(this.deweyIdOffsets, -1);
    }
    this.deweyIdMemory = deweyIdMemory;
    this.lastDeweyIdIndex = lastDeweyIdIndex;

    // String value memory (columnar storage) - not yet used in legacy constructor
    this.stringValueOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(this.stringValueOffsets, -1);
    this.stringValueMemory = null;
    this.lastStringValueIndex = -1;
    this.stringValueMemoryFreeSpaceStart = 0;
    this.fsstSymbolTable = null;

    // Build bitmap from provided slotOffsets for O(k) iteration
    this.slotBitmap = new long[BITMAP_WORDS];
    this.fixedFormatBitmap = new long[BITMAP_WORDS];
    this.fixedSlotKinds = new byte[Constants.NDP_NODE_COUNT];
    Arrays.fill(this.fixedSlotKinds, NO_FIXED_SLOT_KIND);
    for (int i = 0; i < slotOffsets.length; i++) {
      if (slotOffsets[i] >= 0) {
        slotBitmap[i >>> 6] |= (1L << (i & 63));
      }
    }

    // Zero-copy: slotMemory is part of backingBuffer, track for release
    this.backingBuffer = backingBuffer;
    this.backingBufferReleaser = backingBufferReleaser;

    // If we have a backing buffer, it owns the memory (zero-copy path)
    // Otherwise, slotMemory was allocated separately and should be released via allocator
    this.externallyAllocatedMemory = (backingBuffer != null);

    this.references = references != null
        ? references
        : new ConcurrentHashMap<>();
    this.records = new DataRecord[Constants.NDP_NODE_COUNT];

    // Zero-copy pages are read-only snapshots, don't resize
    this.doResizeMemorySegmentsIfNeeded = false;

    // Free space tracking not needed for read-only zero-copy pages
    this.slotMemoryFreeSpaceStart = 0;
    this.deweyIdMemoryFreeSpaceStart = 0;

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

  // Update the last slot index after setting a slot.
  private void updateLastSlotIndex(int slotNumber, boolean isSlotMemory) {
    if (isSlotMemory) {
      if (lastSlotIndex >= 0) {
        if (slotOffsets[slotNumber] > slotOffsets[lastSlotIndex]) {
          lastSlotIndex = slotNumber;
        }
      } else {
        lastSlotIndex = slotNumber;
      }
    } else {
      if (lastDeweyIdIndex >= 0) {
        if (deweyIdOffsets[slotNumber] > deweyIdOffsets[lastDeweyIdIndex]) {
          lastDeweyIdIndex = slotNumber;
        }
      } else {
        lastDeweyIdIndex = slotNumber;
      }
    }
  }

  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = Objects.hashCode(recordPageKey, revision);
    }
    return hash;
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
    return records[offset];
  }

  /**
   * Clear the cached record reference at the given offset.
   * <p>
   * Used after fixed-slot projection to prevent stale pooled object references from being returned by
   * {@link #getRecord(int)}. Fixed-slot bytes are the authoritative source for fixed-format slots, so
   * the cached record must be cleared to force re-materialization from those bytes on the next read.
   *
   * @param offset the slot offset to clear
   */
  public void clearRecord(final int offset) {
    if (records[offset] != null) {
      records[offset] = null;
      inMemoryRecordCount--;
    }
  }

  @Override
  public void setRecord(@NonNull final DataRecord record) {
    addedReferences = false;
    final var key = record.getNodeKey();
    final var offset = (int) (key - ((key >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
    if (records[offset] == null) {
      inMemoryRecordCount++;
    }
    records[offset] = record;
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
   * Set bytes after serialization.
   *
   * @param bytes bytes
   */
  public void setBytes(BytesOut<?> bytes) {
    this.bytes = bytes;
  }

  @Override
  public DataRecord[] records() {
    return records;
  }

  public int getInMemoryRecordCount() {
    return inMemoryRecordCount;
  }

  public int getDemotionThreshold() {
    return demotionThreshold;
  }

  public boolean shouldDemoteRecords(IndexType currentIndexType) {
    return currentIndexType == IndexType.DOCUMENT && inMemoryRecordCount >= demotionThreshold;
  }

  public void onRecordRematerialized() {
    rematerializedRecordsSinceLastDemotion++;
    if (rematerializedRecordsSinceLastDemotion > demotionThreshold * 2) {
      demotionThreshold = Math.min(MAX_DEMOTION_THRESHOLD, demotionThreshold + DEMOTION_STEP);
      rematerializedRecordsSinceLastDemotion = 0;
    }
  }

  public int demoteRecordsToSlots(ResourceConfiguration config, MemorySegmentBytesOut reusableOut) {
    if (inMemoryRecordCount <= MIN_DEMOTION_THRESHOLD) {
      return 0;
    }

    final RecordSerializer serializer = config.recordPersister;
    int demoted = 0;

    for (int i = 0; i < records.length && inMemoryRecordCount > MIN_DEMOTION_THRESHOLD; i++) {
      final DataRecord record = records[i];
      if (record == null) {
        continue;
      }

      reusableOut.clear();
      serializer.serialize(reusableOut, record, config);
      final MemorySegment segment = reusableOut.getDestination();

      if (segment.byteSize() > PageConstants.MAX_RECORD_SIZE) {
        continue;
      }

      setSlot(segment, i);
      markSlotAsCompactFormat(i);

      if (config.areDeweyIDsStored && record.getDeweyID() != null && record.getNodeKey() != 0) {
        setDeweyId(record.getDeweyID().toBytes(), i);
      }

      records[i] = null;
      inMemoryRecordCount--;
      demoted++;
    }

    if (demoted > 0) {
      if (rematerializedRecordsSinceLastDemotion < demoted / 4) {
        demotionThreshold = Math.max(MIN_DEMOTION_THRESHOLD, demotionThreshold - DEMOTION_STEP);
      }
      rematerializedRecordsSinceLastDemotion = 0;
    }

    return demoted;
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
    return (I) new ArrayIterator(records, records.length);
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
    if (preservationBitmap == null) {
      preservationBitmap = new long[BITMAP_WORDS];
    }
    preservationBitmap[slotNumber >>> 6] |= (1L << (slotNumber & 63));
  }

  /**
   * Check if a slot is marked for preservation.
   *
   * @param slotNumber the slot number to check
   * @return true if the slot needs preservation
   */
  public boolean isSlotMarkedForPreservation(int slotNumber) {
    return preservationBitmap != null && (preservationBitmap[slotNumber >>> 6] & (1L << (slotNumber & 63))) != 0;
  }

  /**
   * Get the preservation bitmap for testing/debugging.
   *
   * @return the preservation bitmap, or null if no slots are marked
   */
  public long[] getPreservationBitmap() {
    return preservationBitmap;
  }

  /**
   * Check if any slots are marked for preservation.
   *
   * @return true if preservation bitmap is set and has marked slots
   */
  public boolean hasPreservationSlots() {
    return preservationBitmap != null;
  }

  private static final int ALIGNMENT = 4; // 4-byte alignment for int

  private static int alignOffset(int offset) {
    return (offset + ALIGNMENT - 1) & -ALIGNMENT;
  }

  @Override
  public void setSlot(byte[] recordData, int slotNumber) {
    setData(recordData, slotNumber, slotOffsets, slotMemory);
  }

  public void setSlotMemory(MemorySegment slotMemory) {
    this.slotMemory = slotMemory;
  }

  public void setDeweyIdMemory(MemorySegment deweyIdMemory) {
    this.deweyIdMemory = deweyIdMemory;
  }

  @Override
  public void setSlot(MemorySegment data, int slotNumber) {
    setData(data, slotNumber, slotOffsets, slotMemory);
  }

  /**
   * Set slot data by copying directly from a source MemorySegment. This is the zero-copy path that
   * avoids intermediate byte[] allocations during deserialization.
   *
   * <p>
   * Memory layout written to slotMemory: [length (4 bytes)][data (dataSize bytes)]
   * </p>
   *
   * <p>
   * This method is optimized for the page deserialization hot path where we can copy directly from
   * the decompressed page data segment to the target slot memory without creating temporary byte[]
   * arrays.
   * </p>
   *
   * @param source the source MemorySegment containing the data
   * @param sourceOffset the byte offset within source where data starts
   * @param dataSize the number of bytes to copy (must be &gt; 0)
   * @param slotNumber the slot number (0 to Constants.NDP_NODE_COUNT-1)
   * @throws IllegalArgumentException if dataSize &lt;= 0 or slotNumber out of range
   * @throws IndexOutOfBoundsException if source bounds would be exceeded
   */
  public void setSlotDirect(MemorySegment source, long sourceOffset, int dataSize, int slotNumber) {
    // Validate inputs
    if (dataSize <= 0) {
      throw new IllegalArgumentException("dataSize must be positive: " + dataSize);
    }
    if (slotNumber < 0 || slotNumber >= Constants.NDP_NODE_COUNT) {
      throw new IllegalArgumentException("slotNumber out of range: " + slotNumber);
    }
    if (sourceOffset < 0 || sourceOffset + dataSize > source.byteSize()) {
      throw new IndexOutOfBoundsException(String.format("Source bounds exceeded: offset=%d, size=%d, segmentSize=%d",
          sourceOffset, dataSize, source.byteSize()));
    }

    int requiredSize = INT_SIZE + dataSize; // 4 bytes for length prefix + actual data
    int currentOffset = slotOffsets[slotNumber];
    int sizeDelta = 0;

    // Check if resizing is needed
    if (!hasEnoughSpace(slotOffsets, slotMemory, requiredSize)) {
      int newSize = Math.max((int) slotMemory.byteSize() * 2, (int) slotMemory.byteSize() + requiredSize);
      slotMemory = resizeMemorySegment(slotMemory, newSize, slotOffsets, true);
    }

    if (currentOffset >= 0) {
      // Existing slot - check if size changed
      int alignedOffset = alignOffset(currentOffset);
      int currentSize = INT_SIZE + slotMemory.get(ValueLayout.JAVA_INT, alignedOffset);

      if (currentSize == requiredSize) {
        // Same size - overwrite in place (fast path)
        slotMemory.set(ValueLayout.JAVA_INT, alignedOffset, dataSize);
        MemorySegment.copy(source, sourceOffset, slotMemory, alignedOffset + INT_SIZE, dataSize);
        return;
      }
      sizeDelta = requiredSize - currentSize;
      slotOffsets[slotNumber] = alignOffset(currentOffset);
    } else {
      // New slot - find free space
      currentOffset = findFreeSpaceForSlots(requiredSize, true);
      slotOffsets[slotNumber] = alignOffset(currentOffset);
      updateLastSlotIndex(slotNumber, true);
      // Update bitmap for newly populated slot
      slotBitmap[slotNumber >>> 6] |= (1L << (slotNumber & 63));
    }

    // Perform shifting if size changed for existing slot
    if (sizeDelta != 0) {
      shiftSlotMemory(slotNumber, sizeDelta, slotOffsets, slotMemory);
    }

    // Write length prefix
    int alignedOffset = alignOffset(currentOffset);
    slotMemory.set(ValueLayout.JAVA_INT, alignedOffset, dataSize);

    // Verify the write
    int verifiedSize = slotMemory.get(ValueLayout.JAVA_INT, alignedOffset);
    if (verifiedSize != dataSize) {
      throw new IllegalStateException(
          String.format("Slot size verification failed: expected=%d, actual=%d (slot: %d, offset: %d)", dataSize,
              verifiedSize, slotNumber, alignedOffset));
    }

    // Copy data directly from source segment to slot memory (ZERO-COPY from caller's perspective!)
    MemorySegment.copy(source, sourceOffset, slotMemory, alignedOffset + INT_SIZE, dataSize);

    // Update free space tracking
    updateFreeSpaceStart(slotOffsets, slotMemory, true);
  }

  private MemorySegment setData(Object data, int slotNumber, int[] offsets, MemorySegment memory) {
    if (data == null) {
      return null;
    }

    int dataSize;

    if (data instanceof MemorySegment) {
      dataSize = (int) ((MemorySegment) data).byteSize();

      if (dataSize == 0) {
        return null;
      }
    } else if (data instanceof byte[]) {
      dataSize = ((byte[]) data).length;

      if (dataSize == 0) {
        return null;
      }
    } else {
      throw new IllegalArgumentException("Data must be either a MemorySegment or a byte array.");
    }

    int requiredSize = INT_SIZE + dataSize;
    int currentOffset = offsets[slotNumber];

    int sizeDelta = 0;

    boolean resized = false;
    boolean isSlotMemory = memory == slotMemory;

    // Check if resizing is needed.
    if (!hasEnoughSpace(offsets, memory, requiredSize + sizeDelta)) {
      // Resize the memory segment.
      int newSize = Math.max(((int) memory.byteSize()) * 2, ((int) memory.byteSize()) + requiredSize + sizeDelta);

      memory = resizeMemorySegment(memory, newSize, offsets, isSlotMemory);

      resized = true;
    }

    if (currentOffset >= 0) {
      // Existing slot, check if there's enough space to accommodate the new data.
      long alignedOffset = alignOffset(currentOffset);
      int currentSize = INT_SIZE + memory.get(ValueLayout.JAVA_INT, alignedOffset);

      if (currentSize == requiredSize) {
        // If the size is the same, update it directly.
        memory.set(ValueLayout.JAVA_INT, alignedOffset, dataSize);
        if (data instanceof MemorySegment) {
          MemorySegment.copy((MemorySegment) data, 0, memory, alignedOffset + INT_SIZE, dataSize);
        } else {
          MemorySegment.copy(data, 0, memory, ValueLayout.JAVA_BYTE, alignedOffset + INT_SIZE, dataSize);
        }

        return null; // No resizing needed
      } else {
        // Calculate sizeDelta based on whether the new data is larger or smaller.
        sizeDelta = requiredSize - currentSize;
      }

      offsets[slotNumber] = alignOffset(currentOffset);
    } else {
      // If the slot is empty, determine where to place the new data.
      currentOffset = findFreeSpaceForSlots(requiredSize, isSlotMemory);
      offsets[slotNumber] = alignOffset(currentOffset);
      updateLastSlotIndex(slotNumber, isSlotMemory);
      // Update bitmap for newly populated slot (slot memory only)
      if (isSlotMemory) {
        slotBitmap[slotNumber >>> 6] |= (1L << (slotNumber & 63));
      }
    }

    // Perform any necessary shifting.
    if (sizeDelta != 0) {
      shiftSlotMemory(slotNumber, sizeDelta, offsets, memory);
    }

    // Write the new data into the slot.
    int alignedOffset = alignOffset(currentOffset);
    memory.set(ValueLayout.JAVA_INT, alignedOffset, dataSize);

    // Verify the write
    int verifiedSize = memory.get(ValueLayout.JAVA_INT, alignedOffset);
    if (verifiedSize <= 0) {
      throw new IllegalStateException(String.format("Invalid slot size written: %d (slot: %d, offset: %d)",
          verifiedSize, slotNumber, alignedOffset));
    }


    if (data instanceof MemorySegment) {
      MemorySegment.copy((MemorySegment) data, 0, memory, alignedOffset + INT_SIZE, dataSize);
    } else {
      MemorySegment.copy(data, 0, memory, ValueLayout.JAVA_BYTE, alignedOffset + INT_SIZE, dataSize);
    }

    // Update slotMemoryFreeSpaceStart after adding the slot.
    updateFreeSpaceStart(offsets, memory, isSlotMemory);

    return resized
        ? memory
        : null;
  }

  private void updateFreeSpaceStart(int[] offsets, MemorySegment memory, boolean isSlotMemory) {
    int freeSpaceStart = (int) memory.byteSize() - getAvailableSpace(offsets, memory);
    if (isSlotMemory) {
      slotMemoryFreeSpaceStart = freeSpaceStart;
    } else {
      deweyIdMemoryFreeSpaceStart = freeSpaceStart;
    }
  }

  boolean hasEnoughSpace(int[] offsets, MemorySegment memory, int requiredDataSize) {
    if (!doResizeMemorySegmentsIfNeeded) {
      return true;
    }

    // Check if the available space can accommodate the new slot.
    return getAvailableSpace(offsets, memory) >= requiredDataSize;
  }

  int getAvailableSpace(int[] offsets, MemorySegment memory) {
    boolean isSlotMemory = memory == slotMemory;

    int lastSlotIndex = getLastIndex(isSlotMemory);

    // If no slots are set yet, start from the beginning of the memory.
    int lastOffset = (lastSlotIndex >= 0)
        ? offsets[lastSlotIndex]
        : 0;

    // Align the last offset
    int alignedLastOffset = alignOffset(lastOffset);

    // If there is a valid last slot, add its size to the aligned offset.
    int lastSlotSize = 0;
    if (lastSlotIndex >= 0) {
      // The size of the last slot (including the size of the integer that stores the data length)
      lastSlotSize = INT_SIZE + memory.get(ValueLayout.JAVA_INT, alignedLastOffset);
    }

    // Calculate available space from the end of the last slot to the end of memory.
    return (int) memory.byteSize() - alignOffset(alignedLastOffset + lastSlotSize);
  }

  int getLastIndex(boolean isSlotMemory) {
    if (isSlotMemory) {
      return lastSlotIndex;
    } else {
      return lastDeweyIdIndex;
    }
  }

  public int getLastSlotIndex() {
    return lastSlotIndex;
  }

  public int getLastDeweyIdIndex() {
    return lastDeweyIdIndex;
  }

  /**
   * Get the slot offsets array for zero-copy serialization. Each element is the byte offset within
   * slotMemory where the slot's data begins, or -1 if the slot is empty.
   * 
   * @return the slot offsets array (do not modify)
   */
  public int[] getSlotOffsets() {
    return slotOffsets;
  }

  /**
   * Get the slot bitmap for O(k) iteration over populated slots. Bit i is set (1) iff slot i is
   * populated (slotOffsets[i] >= 0).
   * 
   * @return the slot bitmap array (16 longs = 1024 bits, do not modify)
   */
  public long[] getSlotBitmap() {
    return slotBitmap;
  }

  /**
   * Returns bitmap of slots stored in fixed in-memory layout.
   */
  public long[] getFixedFormatBitmap() {
    return fixedFormatBitmap;
  }

  /**
   * Check if a specific slot is populated using the bitmap. This is O(1) and avoids memory access to
   * slotOffsets.
   * 
   * @param slotNumber the slot index (0-1023)
   * @return true if the slot is populated
   */
  public boolean hasSlot(int slotNumber) {
    return (slotBitmap[slotNumber >>> 6] & (1L << (slotNumber & 63))) != 0;
  }

  /**
   * Returns {@code true} if slot data is in fixed in-memory layout.
   */
  public boolean isFixedSlotFormat(int slotNumber) {
    return (fixedFormatBitmap[slotNumber >>> 6] & (1L << (slotNumber & 63))) != 0;
  }

  /**
   * Returns node kind for a fixed-format slot, or {@code null} if the slot is compact or fixed
   * metadata is unavailable.
   */
  public NodeKind getFixedSlotNodeKind(final int slotNumber) {
    if (!isFixedSlotFormat(slotNumber)) {
      return null;
    }
    final byte kindId = fixedSlotKinds[slotNumber];
    if (kindId == NO_FIXED_SLOT_KIND) {
      return null;
    }
    return NodeKind.getKind(kindId);
  }

  /**
   * Mark slot as fixed-layout in-memory representation.
   */
  public void markSlotAsFixedFormat(final int slotNumber) {
    fixedFormatBitmap[slotNumber >>> 6] |= (1L << (slotNumber & 63));
    fixedSlotKinds[slotNumber] = NO_FIXED_SLOT_KIND;
  }

  /**
   * Mark slot as fixed-layout in-memory representation with explicit kind metadata.
   */
  public void markSlotAsFixedFormat(final int slotNumber, final NodeKind nodeKind) {
    if (nodeKind == null) {
      throw new IllegalArgumentException("nodeKind must not be null");
    }
    fixedFormatBitmap[slotNumber >>> 6] |= (1L << (slotNumber & 63));
    fixedSlotKinds[slotNumber] = nodeKind.getId();
  }

  /**
   * Mark slot as compact serialized representation.
   */
  public void markSlotAsCompactFormat(final int slotNumber) {
    fixedFormatBitmap[slotNumber >>> 6] &= ~(1L << (slotNumber & 63));
    fixedSlotKinds[slotNumber] = NO_FIXED_SLOT_KIND;
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
      long word = slotBitmap[wordIndex];
      int baseSlot = wordIndex << 6; // wordIndex * 64
      while (word != 0) {
        int bit = Long.numberOfTrailingZeros(word);
        result[idx++] = baseSlot + bit;
        word &= word - 1; // Clear lowest set bit
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
   * Zero-allocation iteration over populated slots.
   * <p>
   * This method iterates over populated slots without allocating any arrays. The consumer returns
   * false to stop iteration early.
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
    int processed = 0;
    for (int wordIndex = 0; wordIndex < BITMAP_WORDS; wordIndex++) {
      long word = slotBitmap[wordIndex];
      int baseSlot = wordIndex << 6; // wordIndex * 64
      while (word != 0) {
        int bit = Long.numberOfTrailingZeros(word);
        int slot = baseSlot + bit;
        processed++;
        if (!consumer.accept(slot)) {
          return processed;
        }
        word &= word - 1; // Clear lowest set bit
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
    int count = 0;
    int i = 0;

    // SIMD loop: process LONG_SPECIES.length() longs at a time
    final int simdWidth = LONG_SPECIES.length();
    final int simdBound = BITMAP_WORDS - (BITMAP_WORDS % simdWidth);

    for (; i < simdBound; i += simdWidth) {
      LongVector vec = LongVector.fromArray(LONG_SPECIES, slotBitmap, i);
      // BITCOUNT lane operation - counts bits in each lane
      LongVector popcnt = vec.lanewise(VectorOperators.BIT_COUNT);
      count += (int) popcnt.reduceLanes(VectorOperators.ADD);
    }

    // Scalar tail: process remaining longs
    for (; i < BITMAP_WORDS; i++) {
      count += Long.bitCount(slotBitmap[i]);
    }

    return count;
  }

  /**
   * Check if all slots are populated using SIMD-accelerated comparison.
   * 
   * @return true if all 1024 slots are populated
   */
  public boolean isFullyPopulated() {
    // All bits set = 0xFFFFFFFFFFFFFFFF = -1L
    int i = 0;
    final int simdWidth = LONG_SPECIES.length();
    final int simdBound = BITMAP_WORDS - (BITMAP_WORDS % simdWidth);
    final LongVector allOnes = LongVector.broadcast(LONG_SPECIES, -1L);

    for (; i < simdBound; i += simdWidth) {
      LongVector vec = LongVector.fromArray(LONG_SPECIES, slotBitmap, i);
      if (!vec.eq(allOnes).allTrue()) {
        return false;
      }
    }

    // Scalar tail
    for (; i < BITMAP_WORDS; i++) {
      if (slotBitmap[i] != -1L) {
        return false;
      }
    }
    return true;
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
   * Get the slot memory segment for zero-copy serialization. Contains the raw serialized slot data.
   * 
   * @return the slot memory segment
   */
  public MemorySegment getSlotMemory() {
    return slotMemory;
  }

  /**
   * Get the dewey ID offsets array for zero-copy serialization. Each element is the byte offset
   * within deweyIdMemory where the dewey ID's data begins, or -1 if empty.
   * 
   * @return the dewey ID offsets array (do not modify)
   */
  public int[] getDeweyIdOffsets() {
    return deweyIdOffsets;
  }

  /**
   * Get the dewey ID memory segment for zero-copy serialization. Contains the raw serialized dewey ID
   * data.
   * 
   * @return the dewey ID memory segment (may be null if not stored)
   */
  public MemorySegment getDeweyIdMemory() {
    return deweyIdMemory;
  }

  MemorySegment resizeMemorySegment(MemorySegment oldMemory, int newSize, int[] offsets, boolean isSlotMemory) {
    MemorySegment newMemory = segmentAllocator.allocate(newSize);
    MemorySegment.copy(oldMemory, 0, newMemory, 0, oldMemory.byteSize());
    segmentAllocator.release(oldMemory);

    if (isSlotMemory) {
      slotMemory = newMemory;
    } else {
      deweyIdMemory = newMemory;
    }

    // Update offsets to reference the new memory segment.
    for (int i = 0; i < offsets.length; i++) {
      if (offsets[i] >= 0) {
        offsets[i] = alignOffset(offsets[i]);
        updateLastSlotIndex(i, isSlotMemory);
      }
    }

    // Update slotMemoryFreeSpaceStart to reflect the new free space start position.
    updateFreeSpaceStart(offsets, newMemory, isSlotMemory);

    return newMemory;
  }

  @Override
  public int getUsedDeweyIdSize() {
    return getUsedByteSize(deweyIdOffsets, deweyIdMemory);
  }

  @Override
  public int getUsedSlotsSize() {
    return getUsedByteSize(slotOffsets, slotMemory);
  }

  public int getSlotMemoryByteSize() {
    return (int) slotMemory.byteSize();
  }

  public int getDeweyIdMemoryByteSize() {
    return (int) deweyIdMemory.byteSize();
  }

  int getUsedByteSize(int[] offsets, MemorySegment memory) {
    if (memory == null) {
      return 0;
    }
    return (int) memory.byteSize() - getAvailableSpace(offsets, memory);
  }

  private void shiftSlotMemory(int slotNumber, int sizeDelta, int[] offsets, MemorySegment memory) {
    if (sizeDelta == 0) {
      return; // No shift needed if there's no size change.
    }

    boolean isSlotMemory = memory == slotMemory;

    // Find the start offset of the slot to be shifted.
    int startOffset = offsets[slotNumber];
    int alignedStartOffset = alignOffset(startOffset);

    // Find the smallest offset greater than the current slot's offset.
    int shiftStartOffset = Integer.MAX_VALUE;
    for (int i = 0; i < offsets.length; i++) {
      if (i != slotNumber && offsets[i] >= alignedStartOffset && offsets[i] < shiftStartOffset) {
        shiftStartOffset = offsets[i];
      }
    }

    if (shiftStartOffset == Integer.MAX_VALUE) {
      return;
    }
    int alignedShiftStartOffset = alignOffset(shiftStartOffset);

    // Calculate the end offset of the memory region to shift.
    int lastSlotIndex = getLastIndex(isSlotMemory);
    int alignedEndOffset = alignOffset(offsets[lastSlotIndex]);

    // Calculate the size of the last slot, ensuring it is aligned.
    int lastSlotSize = INT_SIZE + memory.get(ValueLayout.JAVA_INT, alignedEndOffset);

    // Calculate the end offset of the shift.
    int shiftEndOffset = alignedEndOffset + lastSlotSize;

    // Ensure the target slice also stays within bounds.
    long targetEndOffset =
        alignOffset(alignedShiftStartOffset + sizeDelta) + (shiftEndOffset - alignedShiftStartOffset);
    if (targetEndOffset > memory.byteSize()) {
      throw new IndexOutOfBoundsException("Calculated targetEndOffset exceeds memory bounds. " + "targetEndOffset: "
          + targetEndOffset + ", memory size: " + memory.byteSize() + ", slotNumber: " + (slotNumber - 1));
    }

    // Shift the memory.
    // Bulk copy: MemorySegment.copy handles overlapping regions safely (memmove semantics).
    final int dstOffset = alignOffset(alignedShiftStartOffset + sizeDelta);
    final long copyLength = shiftEndOffset - alignedShiftStartOffset;
    MemorySegment.copy(memory, alignedShiftStartOffset, memory, dstOffset, copyLength);

    // Adjust the offsets for all affected slots.
    for (int i = 0; i < offsets.length; i++) {
      if (i != slotNumber && offsets[i] >= alignedStartOffset) {
        offsets[i] = alignOffset(offsets[i] + sizeDelta);
        updateLastSlotIndex(i, isSlotMemory);
      }
    }
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
    return slotOffsets[slotNumber] != -1;
  }

  /**
   * Get the absolute data offset for a slot within {@link #getSlotMemory()}, skipping the 4-byte
   * length prefix. Use together with {@link #getSlotDataLength(int)} and {@link #getSlotMemory()} to
   * avoid allocating a {@code MemorySegment} slice on the hot path.
   *
   * @param slotNumber the slot index
   * @return absolute byte offset into slotMemory where data begins, or {@code -1} if the slot is
   *         empty
   */
  public long getSlotDataOffset(final int slotNumber) {
    assert slotNumber >= 0 && slotNumber < slotOffsets.length : "Invalid slot number: " + slotNumber;
    final int slotOffset = slotOffsets[slotNumber];
    if (slotOffset < 0) {
      return -1;
    }
    return slotOffset + INT_SIZE;
  }

  /**
   * Get the data length (in bytes) stored in the given slot, excluding the 4-byte length prefix. Use
   * together with {@link #getSlotDataOffset(int)} and {@link #getSlotMemory()} to avoid allocating a
   * {@code MemorySegment} slice on the hot path.
   *
   * @param slotNumber the slot index
   * @return data length in bytes, or {@code -1} if the slot is empty
   */
  public int getSlotDataLength(final int slotNumber) {
    assert slotNumber >= 0 && slotNumber < slotOffsets.length : "Invalid slot number: " + slotNumber;
    final int slotOffset = slotOffsets[slotNumber];
    if (slotOffset < 0) {
      return -1;
    }
    return slotMemory.get(JAVA_INT_UNALIGNED, slotOffset);
  }

  @Override
  public MemorySegment getSlot(int slotNumber) {
    // Validate slot memory segment
    assert slotMemory != null : "Slot memory segment is null";
    assert slotMemory.byteSize() > 0 : "Slot memory segment has zero length. Page key: " + recordPageKey
        + ", revision: " + revision + ", index type: " + indexType;

    // Validate slot number
    assert slotNumber >= 0 && slotNumber < slotOffsets.length : "Invalid slot number: " + slotNumber;

    int slotOffset = slotOffsets[slotNumber];
    if (slotOffset < 0) {
      return null;
    }

    // CRITICAL: Validate memory segment state before reading
    if (slotMemory == null) {
      throw new IllegalStateException("Slot memory is null for page " + recordPageKey);
    }

    // DEFENSIVE: Ensure offset is within segment bounds BEFORE reading
    if (slotOffset + INT_SIZE > slotMemory.byteSize()) {
      throw new IllegalStateException(
          String.format("CORRUPT OFFSET: slot %d has offset %d but would exceed segment (size %d, page %d, rev %d)",
              slotNumber, slotOffset, slotMemory.byteSize(), recordPageKey, revision));
    }

    // Read the length from the first 4 bytes at the offset
    // Use unaligned access because zero-copy slices may not be 4-byte aligned
    int length;
    try {
      length = slotMemory.get(JAVA_INT_UNALIGNED, slotOffset);
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format("Failed to read length at offset %d (page %d, slot %d, memory size %d)", slotOffset,
              recordPageKey, slotNumber, slotMemory.byteSize()),
          e);
    }

    // DEFENSIVE: Sanity check the length value before using it
    if (length < 0 || length > slotMemory.byteSize()) {
      throw new IllegalStateException(
          String.format("CORRUPT LENGTH at offset %d: %d (segment size: %d, page %d, slot %d, revision: %d)",
              slotOffset, length, slotMemory.byteSize(), recordPageKey, slotNumber, revision));
    }

    if (length <= 0) {
      // Print memory segment contents around the failing offset
      String memoryDump = dumpMemorySegmentAroundOffset(slotOffset, slotNumber);

      String errorMessage = String.format(
          "Slot length must be greater than 0, but is %d (slotNumber: %d, offset: %d, revision: %d, page: %d)", length,
          slotNumber, slotOffset, revision, recordPageKey);

      // Add comprehensive debugging info
      String debugInfo = String.format(
          "%s\nMemory segment: size=%d, closed=%s\nSlot offsets around %d: [%d, %d, %d]\nLast slot index: %d, Free space: %d\n%s",
          errorMessage, slotMemory.byteSize(), isClosed(), slotNumber, slotNumber > 0
              ? slotOffsets[slotNumber - 1]
              : -1,
          slotOffsets[slotNumber], slotNumber < slotOffsets.length - 1
              ? slotOffsets[slotNumber + 1]
              : -1,
          lastSlotIndex, slotMemoryFreeSpaceStart, memoryDump);

      throw new AssertionError(createStackTraceMessage(debugInfo));
    }


    // Validate that we can read the full data
    if (slotOffset + INT_SIZE + length > slotMemory.byteSize()) {
      throw new IllegalStateException(
          String.format("Slot data extends beyond memory segment: offset=%d, length=%d, total=%d, memory_size=%d",
              slotOffset, length, slotOffset + INT_SIZE + length, slotMemory.byteSize()));
    }

    // Return the memory segment containing just the data (skip the 4-byte length prefix)
    return slotMemory.asSlice(slotOffset + INT_SIZE, length);
  }

  /**
   * Dump the memory segment contents around a specific offset for debugging purposes.
   *
   * @param offset the offset where the issue occurred
   * @param slotNumber the slot number for context
   * @return a formatted string showing the memory contents
   */
  private String dumpMemorySegmentAroundOffset(int offset, int slotNumber) {
    StringBuilder sb = new StringBuilder();
    sb.append("Memory segment dump around failing offset:\n");

    // Show 64 bytes around the offset (32 before, 32 after)
    int startOffset = Math.max(0, offset - 32);
    int endOffset = Math.min((int) slotMemory.byteSize(), offset + 64);

    sb.append(
        String.format("Dumping bytes %d to %d (offset %d marked with **):\n", startOffset, endOffset - 1, offset));

    // Hex dump with 16 bytes per line
    for (int i = startOffset; i < endOffset; i += 16) {
      sb.append(String.format("%04X: ", i));

      // Hex bytes
      for (int j = 0; j < 16 && i + j < endOffset; j++) {
        byte b = slotMemory.get(ValueLayout.JAVA_BYTE, i + j);
        if (i + j == offset) {
          sb.append(String.format("**%02X** ", b & 0xFF));
        } else {
          sb.append(String.format("%02X ", b & 0xFF));
        }
      }

      // ASCII representation
      sb.append(" |");
      for (int j = 0; j < 16 && i + j < endOffset; j++) {
        byte b = slotMemory.get(ValueLayout.JAVA_BYTE, i + j);
        char c = (b >= 32 && b < 127)
            ? (char) b
            : '.';
        if (i + j == offset) {
          sb.append('*');
        } else {
          sb.append(c);
        }
      }
      sb.append("|\n");
    }

    // Also show the specific 4 bytes that should contain the length
    sb.append(String.format("\nSpecific 4-byte length value at offset %d:\n", offset));
    if (offset + 4 <= slotMemory.byteSize()) {
      for (int i = 0; i < 4; i++) {
        byte b = slotMemory.get(ValueLayout.JAVA_BYTE, offset + i);
        sb.append(String.format("Byte %d: 0x%02X (%d)\n", i, b & 0xFF, b));
      }

      // Show as little-endian and big-endian integers
      try {
        int littleEndian = slotMemory.get(ValueLayout.JAVA_INT.withOrder(ByteOrder.LITTLE_ENDIAN), offset);
        int bigEndian = slotMemory.get(ValueLayout.JAVA_INT.withOrder(ByteOrder.BIG_ENDIAN), offset);
        sb.append(String.format("As little-endian int: %d\n", littleEndian));
        sb.append(String.format("As big-endian int: %d\n", bigEndian));
      } catch (Exception e) {
        sb.append("Failed to read as integer: ").append(e.getMessage()).append('\n');
      }
    }

    // Show all slot offsets for context
    sb.append("\nAll slot offsets:\n");
    for (int i = 0; i < slotOffsets.length; i++) {
      if (slotOffsets[i] >= 0) {
        sb.append(String.format("Slot %d: offset %d", i, slotOffsets[i]));
        if (i == slotNumber) {
          sb.append(" <- FAILING SLOT");
        }
        sb.append('\n');
      }
    }

    return sb.toString();
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
    var memorySegment = setData(MemorySegment.ofArray(deweyId), offset, deweyIdOffsets, deweyIdMemory);

    if (memorySegment != null) {
      deweyIdMemory = memorySegment;
    }
  }

  @Override
  public void setDeweyId(MemorySegment deweyId, int offset) {
    var memorySegment = setData(deweyId, offset, deweyIdOffsets, deweyIdMemory);

    if (memorySegment != null) {
      deweyIdMemory = memorySegment;
    }
  }

  @Override
  public MemorySegment getDeweyId(int offset) {
    int deweyIdOffset = deweyIdOffsets[offset];
    if (deweyIdOffset < 0) {
      return null;
    }
    // Use unaligned access because zero-copy slices may not be 4-byte aligned
    int deweyIdLength = deweyIdMemory.get(JAVA_INT_UNALIGNED, deweyIdOffset);
    deweyIdOffset += INT_SIZE;
    return deweyIdMemory.asSlice(deweyIdOffset, deweyIdLength);
  }

  @Override
  public byte[] getDeweyIdAsByteArray(int slotNumber) {
    var memorySegment = getDeweyId(slotNumber);

    if (memorySegment == null) {
      return null;
    }

    return memorySegment.toArray(ValueLayout.JAVA_BYTE);
  }

  public int findFreeSpaceForSlots(int requiredSize, boolean isSlotMemory) {
    // Align the start of the free space
    int alignedFreeSpaceStart = alignOffset(isSlotMemory
        ? slotMemoryFreeSpaceStart
        : deweyIdMemoryFreeSpaceStart);
    int freeSpaceEnd = isSlotMemory
        ? (int) slotMemory.byteSize()
        : (int) deweyIdMemory.byteSize();

    // Check if there's enough space in the current free space range
    if (freeSpaceEnd - alignedFreeSpaceStart >= requiredSize) {
      return alignedFreeSpaceStart;
    }

    int freeMemoryStart = isSlotMemory
        ? slotMemoryFreeSpaceStart
        : deweyIdMemoryFreeSpaceStart;
    int freeMemoryEnd = isSlotMemory
        ? (int) slotMemory.byteSize()
        : (int) deweyIdMemory.byteSize();
    throw new IllegalStateException("Not enough space in memory segment to store the data (freeSpaceStart "
        + freeMemoryStart + " requiredSize: " + requiredSize + ", maxLength: " + freeMemoryEnd + ")");
  }

  @Override
  public <C extends KeyValuePage<DataRecord>> C newInstance(@NonNegative long recordPageKey,
      @NonNull IndexType indexType, @NonNull StorageEngineReader pageReadTrx) {
    // Direct allocation (no pool)
    ResourceConfiguration config = pageReadTrx.getResourceSession().getResourceConfig();
    MemorySegmentAllocator allocator = OS.isWindows()
        ? WindowsMemorySegmentAllocator.getInstance()
        : LinuxMemorySegmentAllocator.getInstance();

    MemorySegment slotMemory = allocator.allocate(SIXTYFOUR_KB);
    MemorySegment deweyIdMemory = config.areDeweyIDsStored
        ? allocator.allocate(SIXTYFOUR_KB)
        : null;

    // Memory allocated from global allocator - should be released on close()
    return (C) new KeyValueLeafPage(recordPageKey, indexType, config, pageReadTrx.getRevisionNumber(), slotMemory,
        deweyIdMemory, false // NOT externally allocated - release memory on close()
    );
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this).add("pagekey", recordPageKey);
    for (final DataRecord record : records) {
      if (record != null) {
        helper.add("record", record);
      }
    }
    return helper.toString();
  }

  @Override
  public int size() {
    return getNumberOfNonNullEntries(records) + references.size();
  }

  private int getNumberOfNonNullEntries(final DataRecord[] entries) {
    int count = 0;
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      final DataRecord record = entries[i];
      if (record != null || isSlotSet(i)) {
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
   * This method logs a warning if a page is garbage collected without being properly closed,
   * indicating a potential memory leak. The warning is only generated when diagnostic settings are
   * enabled.
   * <p>
   * <b>Note:</b> Finalizers are deprecated in modern Java. This is retained solely for leak detection
   * during development and testing.
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
        FINALIZED_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0))
                         .incrementAndGet();
      }
      FINALIZED_BY_PAGE_KEY.computeIfAbsent(recordPageKey, _ -> new java.util.concurrent.atomic.AtomicLong(0))
                           .incrementAndGet();

      // Log leak information (only when diagnostics enabled)
      if (LOGGER.isWarnEnabled()) {
        StringBuilder leakMsg = new StringBuilder();
        leakMsg.append(String.format("Page leak detected: pageKey=%d, type=%s, revision=%d - not closed explicitly",
            recordPageKey, indexType, revision));

        if (creationStackTrace != null && LOGGER.isDebugEnabled()) {
          leakMsg.append("\n  Creation stack trace:");
          for (int i = 2; i < Math.min(creationStackTrace.length, 8); i++) {
            StackTraceElement frame = creationStackTrace[i];
            leakMsg.append(String.format("\n    at %s.%s(%s:%d)", frame.getClassName(), frame.getMethodName(),
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
      // For zero-copy pages, all memory segments are slices of backingBuffer, don't release separately
      slotMemory = null;
      deweyIdMemory = null;
      stringValueMemory = null; // CRITICAL: Must be nulled for columnar string storage
    } else if (!externallyAllocatedMemory) {
      // Release memory segments to the allocator pool (non-zero-copy path)
      try {
        if (slotMemory != null && slotMemory.byteSize() > 0) {
          segmentAllocator.release(slotMemory);
        }
        if (deweyIdMemory != null && deweyIdMemory.byteSize() > 0) {
          segmentAllocator.release(deweyIdMemory);
        }
        if (stringValueMemory != null && stringValueMemory.byteSize() > 0) {
          segmentAllocator.release(stringValueMemory);
        }
      } catch (Throwable e) {
        LOGGER.debug("Failed to release memory segments for page {}: {}", recordPageKey, e.getMessage());
      }
      slotMemory = null;
      deweyIdMemory = null;
      stringValueMemory = null;
    }

    // Clear FSST symbol table
    fsstSymbolTable = null;

    // Clear references to aid garbage collection
    Arrays.fill(records, null);
    inMemoryRecordCount = 0;
    demotionThreshold = MIN_DEMOTION_THRESHOLD;
    rematerializedRecordsSinceLastDemotion = 0;
    references.clear();
    bytes = null;
    hashCode = null;
  }

  /**
   * Get the actual memory size used by this page's memory segments. Used for accurate Caffeine cache
   * weighing.
   * 
   * @return Total size in bytes of all memory segments used by this page
   */
  public long getActualMemorySize() {
    long total = 0;
    if (slotMemory != null) {
      total += slotMemory.byteSize();
    }
    if (deweyIdMemory != null) {
      total += deweyIdMemory.byteSize();
    }
    if (stringValueMemory != null) {
      total += stringValueMemory.byteSize();
    }
    return total;
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
   * Get a string value segment for a slot (zero-copy access).
   * 
   * @param slotNumber the slot number (0 to NDP_NODE_COUNT-1)
   * @return MemorySegment slice for the string value, or null if not set
   */
  public MemorySegment getStringValueSegment(int slotNumber) {
    if (stringValueMemory == null || slotNumber < 0 || slotNumber >= stringValueOffsets.length) {
      return null;
    }

    int offset = stringValueOffsets[slotNumber];
    if (offset < 0) {
      return null;
    }

    // Calculate length: either to next offset or to end of used space
    int length;
    if (slotNumber == lastStringValueIndex) {
      length = stringValueMemoryFreeSpaceStart - offset;
    } else {
      // Find next valid offset
      int nextOffset = -1;
      for (int i = slotNumber + 1; i < stringValueOffsets.length; i++) {
        if (stringValueOffsets[i] >= 0) {
          nextOffset = stringValueOffsets[i];
          break;
        }
      }
      if (nextOffset >= 0) {
        length = nextOffset - offset;
      } else {
        length = stringValueMemoryFreeSpaceStart - offset;
      }
    }

    if (length <= 0) {
      return null;
    }

    return stringValueMemory.asSlice(offset, length);
  }

  /**
   * Set the string value memory and offsets (for deserialization).
   * 
   * @param stringValueMemory the memory segment containing all string values
   * @param stringValueOffsets the offset array mapping slots to string positions
   * @param lastStringValueIndex the index of the last string value slot
   * @param stringValueMemoryFreeSpaceStart the end of used space in stringValueMemory
   */
  public void setStringValueData(MemorySegment stringValueMemory, int[] stringValueOffsets, int lastStringValueIndex,
      int stringValueMemoryFreeSpaceStart) {
    this.stringValueMemory = stringValueMemory;
    if (stringValueOffsets != null) {
      System.arraycopy(stringValueOffsets, 0, this.stringValueOffsets, 0,
          Math.min(stringValueOffsets.length, this.stringValueOffsets.length));
    }
    this.lastStringValueIndex = lastStringValueIndex;
    this.stringValueMemoryFreeSpaceStart = stringValueMemoryFreeSpaceStart;
  }

  /**
   * Check if this page has string value columnar storage.
   * 
   * @return true if stringValueMemory is present
   */
  public boolean hasStringValueMemory() {
    return stringValueMemory != null && stringValueMemory.byteSize() > 0;
  }

  /**
   * Get the string value memory segment.
   * 
   * @return the memory segment, or null if not present
   */
  public MemorySegment getStringValueMemory() {
    return stringValueMemory;
  }

  /**
   * Get the string value offsets array.
   * 
   * @return the offsets array (copy to prevent modification)
   */
  public int[] getStringValueOffsets() {
    return stringValueOffsets.clone();
  }

  /**
   * Get the last string value index.
   * 
   * @return the index of the last slot with a string value, or -1 if none
   */
  public int getLastStringValueIndex() {
    return lastStringValueIndex;
  }

  /**
   * Get the end of used space in stringValueMemory.
   * 
   * @return the free space start position
   */
  public int getStringValueMemoryFreeSpaceStart() {
    return stringValueMemoryFreeSpaceStart;
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
    return slotMemory;
  }

  @Override
  public MemorySegment deweyIds() {
    return deweyIdMemory;
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
   * Acquire a guard on this page (increment guard count). Pages with active guards cannot be evicted.
   */
  public void acquireGuard() {
    guardCount.incrementAndGet();
  }

  /**
   * Try to acquire a guard on this page. Returns false if the page is orphaned or closed (cannot be
   * used). This is the synchronized version that prevents race conditions with close().
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
    Arrays.fill(records, null);
    inMemoryRecordCount = 0;
    demotionThreshold = MIN_DEMOTION_THRESHOLD;
    rematerializedRecordsSinceLastDemotion = 0;

    // Clear offsets
    Arrays.fill(slotOffsets, -1);
    Arrays.fill(deweyIdOffsets, -1);

    // Clear slot bitmap (all slots now empty)
    Arrays.fill(slotBitmap, 0L);
    Arrays.fill(fixedFormatBitmap, 0L);
    Arrays.fill(fixedSlotKinds, NO_FIXED_SLOT_KIND);

    // Reset free space pointers
    slotMemoryFreeSpaceStart = 0;
    deweyIdMemoryFreeSpaceStart = 0;
    lastSlotIndex = -1;
    lastDeweyIdIndex = areDeweyIDsStored
        ? -1
        : -1;

    // Clear references
    references.clear();
    addedReferences = false;

    // Clear cached data
    bytes = null;
    hashCode = null;

    // CRITICAL: Guard count MUST be 0 before reset
    int currentGuardCount = guardCount.get();
    if (currentGuardCount != 0) {
      throw new IllegalStateException(String.format(
          "CRITICAL BUG: reset() called on page with active guards! "
              + "Page %d (%s) rev=%d guardCount=%d - this will cause guard count corruption!",
          recordPageKey, indexType, revision, currentGuardCount));
    }
    hash = 0;

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
      if (preservationBitmap != null && completePageRef != null) {
        for (int wordIndex = 0; wordIndex < BITMAP_WORDS; wordIndex++) {
          long word = preservationBitmap[wordIndex];
          int baseSlot = wordIndex << 6;
          while (word != 0) {
            int bit = Long.numberOfTrailingZeros(word);
            int slotIndex = baseSlot + bit;

            // Preserve only when the slot is still absent from the modified page.
            // This keeps write-intent data authoritative and avoids rematerialization churn.
            if (records[slotIndex] == null && !hasSlot(slotIndex)) {
              MemorySegment slotData = completePageRef.getSlot(slotIndex);
              if (slotData != null) {
                setSlot(slotData, slotIndex);
                final NodeKind fixedNodeKind = completePageRef.getFixedSlotNodeKind(slotIndex);
                if (fixedNodeKind != null) {
                  markSlotAsFixedFormat(slotIndex, fixedNodeKind);
                } else {
                  markSlotAsCompactFormat(slotIndex);
                }
              }

              if (areDeweyIDsStored) {
                MemorySegment deweyId = completePageRef.getDeweyId(slotIndex);
                if (deweyId != null) {
                  setDeweyId(deweyId, slotIndex);
                }
              }
            }

            word &= word - 1;
          }
        }
      }

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

      for (final DataRecord record : records) {
        if (record == null) {
          continue;
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
          // Normal record: setSlot copies data to slotMemory, so temp buffer is fine
          setSlot(buffer, offset);
          markSlotAsCompactFormat(offset);
        }
      }
    } // Confined arena automatically closes here, freeing all temporary buffers
  }

  void compactFixedSlotsForCommit(final ResourceConfiguration resourceConfiguration) {
    // Quick exit: no fixed-format slots → nothing to compact.
    boolean hasFixedSlots = false;
    for (int w = 0; w < BITMAP_WORDS; w++) {
      if (fixedFormatBitmap[w] != 0) {
        hasFixedSlots = true;
        break;
      }
    }
    if (!hasFixedSlots) {
      return;
    }

    // In-place compaction: compact format is always <= fixed format in size,
    // so we overwrite each fixed slot at its current offset. No buffer allocation needed.
    // Any gaps left by shrunk slots are eliminated by downstream compactLengthPrefixedRegion().
    final MemorySegmentBytesOut compactBuffer = COMPACT_BUFFER.get();

    for (int wordIndex = 0; wordIndex < BITMAP_WORDS; wordIndex++) {
      long word = fixedFormatBitmap[wordIndex];
      final int baseSlot = wordIndex << 6;
      while (word != 0) {
        final int bit = Long.numberOfTrailingZeros(word);
        final int slotIndex = baseSlot + bit;
        final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + slotIndex;

        final NodeKind nodeKind = getFixedSlotNodeKind(slotIndex);
        final int slotOffset = slotOffsets[slotIndex];
        final int fixedLength = slotMemory.get(JAVA_INT_UNALIGNED, slotOffset);
        final MemorySegment fixedSlotBytes = slotMemory.asSlice(slotOffset + INT_SIZE, fixedLength);

        if (nodeKind == null) {
          throw new IllegalStateException("Missing fixed-slot metadata for node key " + nodeKey);
        }

        // Direct byte-level transformation: fixed → compact without materializing a DataRecord.
        compactBuffer.clear();
        FixedToCompactTransformer.transform(nodeKind, nodeKey, fixedSlotBytes, resourceConfiguration, compactBuffer);
        final MemorySegment compactBytes = compactBuffer.getDestination();
        final int compactSize = (int) compactBytes.byteSize();
        if (compactSize > PageConstants.MAX_RECORD_SIZE) {
          throw new IllegalStateException("Compacted record exceeds max size for node key " + nodeKey);
        }

        // Overwrite in-place: compact bytes fit within the fixed slot's footprint.
        // Offset unchanged — only the length prefix and data bytes are rewritten.
        slotMemory.set(JAVA_INT_UNALIGNED, slotOffset, compactSize);
        MemorySegment.copy(compactBytes, 0, slotMemory, slotOffset + INT_SIZE, compactSize);

        word &= word - 1;
      }
    }

    // Clear fixed-format tracking — all slots are now compact format.
    Arrays.fill(fixedFormatBitmap, 0L);
    Arrays.fill(fixedSlotKinds, NO_FIXED_SLOT_KIND);
  }

  /**
   * Build FSST symbol table from all string values in this page. This should be called before
   * serialization to enable page-level compression.
   *
   * @param resourceConfig the resource configuration
   * @return true if FSST compression is enabled and symbol table was built
   */
  public boolean buildFsstSymbolTable(ResourceConfiguration resourceConfig) {
    if (resourceConfig.stringCompressionType != StringCompressionType.FSST) {
      return false;
    }

    // Collect all string values from StringNode and ObjectStringNode
    java.util.ArrayList<byte[]> stringSamples = new java.util.ArrayList<>();

    for (final DataRecord record : records) {
      if (record == null) {
        continue;
      }
      if (record instanceof StringNode stringNode) {
        addStringSample(stringSamples, stringNode.getRawValueWithoutDecompression());
      } else if (record instanceof ObjectStringNode objectStringNode) {
        addStringSample(stringSamples, objectStringNode.getRawValueWithoutDecompression());
      } else if (record instanceof TextNode textNode) {
        addStringSample(stringSamples, textNode.getRawValueWithoutDecompression());
      } else if (record instanceof CommentNode commentNode) {
        addStringSample(stringSamples, commentNode.getRawValueWithoutDecompression());
      } else if (record instanceof PINode piNode) {
        addStringSample(stringSamples, piNode.getRawValueWithoutDecompression());
      } else if (record instanceof AttributeNode attrNode) {
        addStringSample(stringSamples, attrNode.getRawValueWithoutDecompression());
      }
    }

    // Include string values only present in slot memory (demoted records).
    // This is commit-path work and preserves FSST sample completeness.
    // Fixed-format slots are already compacted before this method runs.
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      if (records[i] != null) {
        continue;
      }

      final MemorySegment slot = getSlot(i);
      if (slot == null) {
        continue;
      }

      final long nodeKey = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + i;
      final DataRecord record = resourceConfig.recordPersister.deserialize(new MemorySegmentBytesIn(slot), nodeKey,
          getDeweyIdAsByteArray(i), resourceConfig);

      if (record instanceof StringNode stringNode) {
        addStringSample(stringSamples, stringNode.getRawValueWithoutDecompression());
      } else if (record instanceof ObjectStringNode objectStringNode) {
        addStringSample(stringSamples, objectStringNode.getRawValueWithoutDecompression());
      } else if (record instanceof TextNode textNode) {
        addStringSample(stringSamples, textNode.getRawValueWithoutDecompression());
      } else if (record instanceof CommentNode commentNode) {
        addStringSample(stringSamples, commentNode.getRawValueWithoutDecompression());
      } else if (record instanceof PINode piNode) {
        addStringSample(stringSamples, piNode.getRawValueWithoutDecompression());
      } else if (record instanceof AttributeNode attrNode) {
        addStringSample(stringSamples, attrNode.getRawValueWithoutDecompression());
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

  private static void addStringSample(java.util.ArrayList<byte[]> samples, byte[] value) {
    if (value != null && value.length > 0) {
      samples.add(value);
    }
  }

  /**
   * Compress all string values in the page using the pre-built FSST symbol table. This modifies the
   * string nodes in place to use compressed values. Must be called after buildFsstSymbolTable().
   */
  public void compressStringValues() {
    if (fsstSymbolTable == null || fsstSymbolTable.length == 0) {
      return;
    }

    for (final DataRecord record : records) {
      if (record == null) {
        continue;
      }
      if (record instanceof StringNode stringNode) {
        if (!stringNode.isCompressed()) {
          byte[] originalValue = stringNode.getRawValueWithoutDecompression();
          if (originalValue != null && originalValue.length > 0) {
            byte[] compressedValue = FSSTCompressor.encode(originalValue, fsstSymbolTable);
            // Only use compressed value if it's actually smaller
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
            // Only use compressed value if it's actually smaller
            if (compressedValue.length < originalValue.length) {
              objectStringNode.setRawValue(compressedValue, true, fsstSymbolTable);
            }
          }
        }
      }
    }
  }

  /**
   * Set the FSST symbol table on all string nodes after deserialization. This allows nodes to use
   * lazy decompression.
   */
  public void propagateFsstSymbolTableToNodes() {
    if (fsstSymbolTable == null || fsstSymbolTable.length == 0) {
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

  /**
   * Entry for tracking string values during columnar storage collection.
   * 
   * @param slotNumber the slot number in the page (0 to NDP_NODE_COUNT-1)
   * @param value the raw string value bytes
   */
  private record StringValueEntry(int slotNumber, byte[] value) {
  }

  /**
   * Collect string values into columnar storage for better compression. Groups all string data
   * contiguously in stringValueMemory, which enables better FSST compression patterns and more
   * efficient storage.
   * 
   * <p>
   * This should be called before serialization when columnar storage is desired. The columnar layout
   * stores: [length1:4][data1:N][length2:4][data2:M]... with stringValueOffsets pointing to each
   * entry's start.
   * 
   * <p>
   * Invariants maintained:
   * <ul>
   * <li>P4: All offsets are valid: 0 ≤ offset < stringValueMemory.byteSize()</li>
   * <li>No overlapping entries</li>
   * <li>Sequential layout with no gaps</li>
   * </ul>
   */
  public void collectStringsForColumnarStorage() {
    java.util.List<StringValueEntry> entries = new java.util.ArrayList<>();
    int totalSize = 0;

    for (int i = 0; i < records.length; i++) {
      DataRecord record = records[i];
      byte[] value = null;

      if (record instanceof StringNode sn) {
        value = sn.getRawValueWithoutDecompression();
      } else if (record instanceof ObjectStringNode osn) {
        value = osn.getRawValueWithoutDecompression();
      }

      if (value != null && value.length > 0) {
        entries.add(new StringValueEntry(i, value));
        totalSize += INT_SIZE + value.length; // 4 bytes length prefix + data
      }
    }

    if (entries.isEmpty()) {
      return;
    }

    // Allocate columnar segment if needed
    if (stringValueMemory == null || stringValueMemory.byteSize() < totalSize) {
      if (stringValueMemory != null && !externallyAllocatedMemory) {
        segmentAllocator.release(stringValueMemory);
      }
      stringValueMemory = segmentAllocator.allocate(totalSize);
    }

    // Store all string values contiguously
    int offset = 0;
    for (StringValueEntry entry : entries) {
      // Validate offset bounds (P4 invariant)
      if (offset + INT_SIZE + entry.value.length > stringValueMemory.byteSize()) {
        throw new IllegalStateException(
            String.format("Columnar storage overflow: offset=%d, entrySize=%d, memorySize=%d", offset,
                INT_SIZE + entry.value.length, stringValueMemory.byteSize()));
      }

      stringValueOffsets[entry.slotNumber] = offset;

      // Write length prefix
      stringValueMemory.set(java.lang.foreign.ValueLayout.JAVA_INT, offset, entry.value.length);

      // Write data using bulk copy
      MemorySegment.copy(entry.value, 0, stringValueMemory, java.lang.foreign.ValueLayout.JAVA_BYTE, offset + INT_SIZE,
          entry.value.length);

      offset += INT_SIZE + entry.value.length;
      lastStringValueIndex = Math.max(lastStringValueIndex, entry.slotNumber);
    }

    stringValueMemoryFreeSpaceStart = offset;
  }

  /**
   * Check if this page has populated columnar string storage.
   * 
   * @return true if stringValueMemory contains data
   */
  public boolean hasColumnarStringStorage() {
    return stringValueMemory != null && stringValueMemoryFreeSpaceStart > 0;
  }
}

