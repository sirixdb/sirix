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
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.utils.ArrayIterator;
import io.sirix.utils.OS;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesOut;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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
@SuppressWarnings({ "unchecked" })
public final class KeyValueLeafPage implements KeyValuePage<DataRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueLeafPage.class);
  private static final int INT_SIZE = Integer.BYTES;
  
  // DEBUG FLAG: Enable with -Dsirix.debug.memory.leaks=true
  public static final boolean DEBUG_MEMORY_LEAKS = 
    Boolean.getBoolean("sirix.debug.memory.leaks");
  
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

  /**
   * HOT bit for clock-based eviction (second-chance algorithm).
   * Set to true on page access, cleared by clock sweeper.
   */
  private volatile boolean hot = false;

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
   * The index of the last slot (the slot with the largest offset).
   */
  private int lastSlotIndex;

  /**
   * The index of the last slot (the slot with the largest offset).
   */
  private int lastDeweyIdIndex;

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
   * Memory segment for slots and Dewey IDs.
   */
  private MemorySegment slotMemory;
  private MemorySegment deweyIdMemory;

  /**
   * Offset arrays to manage positions within memory segments.
   */
  private final int[] slotOffsets;
  private final int[] deweyIdOffsets;

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

  private volatile boolean isClosed;

  private MemorySegmentAllocator segmentAllocator =
      OS.isWindows() ? WindowsMemorySegmentAllocator.getInstance() : LinuxMemorySegmentAllocator.getInstance();

  /**
   * Constructor which initializes a new {@link KeyValueLeafPage}.
   *
   * @param recordPageKey  base key assigned to this node page
   * @param indexType      the index type
   * @param resourceConfig the resource configuration
   */
  public KeyValueLeafPage(final @NonNegative long recordPageKey, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final int revisionNumber, final MemorySegment slotMemory,
      final MemorySegment deweyIdMemory) {
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
    Arrays.fill(slotOffsets, -1);
    Arrays.fill(deweyIdOffsets, -1);
    this.doResizeMemorySegmentsIfNeeded = true;
    this.slotMemoryFreeSpaceStart = 0;
    this.lastSlotIndex = -1;
    this.deweyIdMemoryFreeSpaceStart = 0;
    this.lastDeweyIdIndex = -1;
    this.slotMemory = slotMemory;
    this.deweyIdMemory = deweyIdMemory;
    
    // DIAGNOSTIC: Capture creation stack trace for leak tracing
    if (DEBUG_MEMORY_LEAKS) {
      this.creationStackTrace = Thread.currentThread().getStackTrace();
      long created = PAGES_CREATED.incrementAndGet();
      PAGES_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
      boolean addedToLivePages = ALL_LIVE_PAGES.add(this);
      if (!addedToLivePages) {
        LOGGER.warn("⚠️  DUPLICATE ADD to ALL_LIVE_PAGES: Page {} ({}) rev={} instance={} totalCreated={}",
            recordPageKey, indexType, revisionNumber, System.identityHashCode(this), created);
      }
      
      // Track Page 0 creation source and register for cleanup
      if (recordPageKey == 0) {
        boolean wasAdded = ALL_PAGE_0_INSTANCES.add(this); // Register for explicit cleanup
        
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        String source = "UNKNOWN";
        String caller = "UNKNOWN";
        for (int i = 0; i < Math.min(stack.length, 15); i++) {
          String methodName = stack[i].getMethodName();
          String className = stack[i].getClassName();
          if (methodName.equals("createTree") || methodName.equals("prepareRecordPage")) {
            source = "NEW_PAGE_CREATE";
            caller = className + "." + methodName;
            break;
          } else if (methodName.equals("deserializePage") || methodName.equals("read")) {
            source = "DISK_LOAD";
            caller = className + "." + methodName;
            break;
          } else if (methodName.equals("newInstance") || methodName.equals("combineRecordPagesForModification")) {
            source = "COMBINED";
            caller = className + "." + methodName;
            break;
          }
        }
        
        String addStatus = wasAdded ? "ADDED" : "DUPLICATE";
        LOGGER.info("[CREATE] Page 0 from {} via {} ({}): {} rev={} instance={} totalCreated={}",
            source, caller, addStatus, indexType, revisionNumber, System.identityHashCode(this), created);
      }
    } else {
      this.creationStackTrace = null;
    }
  }

  /**
   * Constructor which reads deserialized data to the {@link KeyValueLeafPage} from the storage.
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
    Arrays.fill(slotOffsets, -1);
    Arrays.fill(deweyIdOffsets, -1);
    this.doResizeMemorySegmentsIfNeeded = true;
    this.slotMemoryFreeSpaceStart = 0;
    this.lastSlotIndex = lastSlotIndex;

    if (areDeweyIDsStored) {
      this.deweyIdMemoryFreeSpaceStart = 0;
      this.lastDeweyIdIndex = lastDeweyIdIndex;
    } else {
      this.deweyIdMemoryFreeSpaceStart = 0;
      this.lastDeweyIdIndex = -1;
    }
    
    // DIAGNOSTIC: Capture creation stack trace for leak tracing
    if (DEBUG_MEMORY_LEAKS) {
      this.creationStackTrace = Thread.currentThread().getStackTrace();
      long created = PAGES_CREATED.incrementAndGet();
      PAGES_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
      boolean addedToLivePages = ALL_LIVE_PAGES.add(this);
      if (!addedToLivePages) {
        LOGGER.warn("⚠️  DUPLICATE ADD to ALL_LIVE_PAGES: Page {} ({}) rev={} instance={} totalCreated={}",
            recordPageKey, indexType, revision, System.identityHashCode(this), created);
      }
      
      // Track Page 0 creation source and register for cleanup
      if (recordPageKey == 0) {
        boolean wasAdded = ALL_PAGE_0_INSTANCES.add(this); // Register for explicit cleanup
        
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        String source = "UNKNOWN";
        String caller = "UNKNOWN";
        for (int i = 0; i < Math.min(stack.length, 15); i++) {
          String methodName = stack[i].getMethodName();
          String className = stack[i].getClassName();
          if (methodName.equals("createTree") || methodName.equals("prepareRecordPage")) {
            source = "NEW_PAGE_CREATE";
            caller = className + "." + methodName;
            break;
          } else if (methodName.equals("deserializePage") || methodName.equals("read")) {
            source = "DISK_LOAD";
            caller = className + "." + methodName;
            break;
          } else if (methodName.equals("newInstance") || methodName.equals("combineRecordPagesForModification")) {
            source = "COMBINED";
            caller = className + "." + methodName;
            break;
          }
        }
        
        String addStatus = wasAdded ? "ADDED" : "DUPLICATE";
        LOGGER.debug("[CREATE] Page 0 from {} via {} ({}): {} rev={} instance={} totalCreated={}",
            source, caller, addStatus, indexType, revision, System.identityHashCode(this), created);
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

  @Override
  public void setRecord(@NonNull final DataRecord record) {
    addedReferences = false;
    final var key = record.getNodeKey();
    final var offset = (int) (key - ((key >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
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
      throw new IllegalStateException(
          String.format("Invalid slot size written: %d (slot: %d, offset: %d)",
                        verifiedSize, slotNumber, alignedOffset));
    }


    if (data instanceof MemorySegment) {
      MemorySegment.copy((MemorySegment) data, 0, memory, alignedOffset + INT_SIZE, dataSize);
    } else {
      MemorySegment.copy(data, 0, memory, ValueLayout.JAVA_BYTE, alignedOffset + INT_SIZE, dataSize);
    }

    // Update slotMemoryFreeSpaceStart after adding the slot.
    updateFreeSpaceStart(offsets, memory, isSlotMemory);

    return resized ? memory : null;
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
    int lastOffset = (lastSlotIndex >= 0) ? offsets[lastSlotIndex] : 0;

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
      throw new IndexOutOfBoundsException(
          "Calculated targetEndOffset exceeds memory bounds. " + "targetEndOffset: " + targetEndOffset
              + ", memory size: " + memory.byteSize() + ", slotNumber: " + (slotNumber - 1));
    }

    // Shift the memory.
    if (sizeDelta > 0) {
      // Shifting to the right
      MemorySegment source = memory.asSlice(alignedShiftStartOffset, shiftEndOffset - alignedShiftStartOffset);
      MemorySegment target =
          memory.asSlice(alignOffset(alignedShiftStartOffset + sizeDelta), shiftEndOffset - alignedShiftStartOffset);
      target.copyFrom(source);
    } else {
      // Shifting to the left: move from start to end.
      for (int i = alignOffset(alignedShiftStartOffset + sizeDelta), j = alignedShiftStartOffset;
           i < shiftEndOffset; i++, j++) {
        byte value = memory.get(ValueLayout.JAVA_BYTE, j);
        memory.set(ValueLayout.JAVA_BYTE, i, value);
      }
    }

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

  @Override
  public MemorySegment getSlot(int slotNumber) {
    // Validate slot memory segment
    assert slotMemory != null : "Slot memory segment is null";
    assert slotMemory.byteSize() > 0 :
        "Slot memory segment has zero length. Page key: " + recordPageKey +
            ", revision: " + revision + ", index type: " + indexType;

    // Validate slot number
    assert slotNumber >= 0 && slotNumber < slotOffsets.length :
        "Invalid slot number: " + slotNumber;

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
      throw new IllegalStateException(String.format(
          "CORRUPT OFFSET: slot %d has offset %d but would exceed segment (size %d, page %d, rev %d)",
          slotNumber, slotOffset, slotMemory.byteSize(), recordPageKey, revision));
    }

    // Read the length from the first 4 bytes at the offset
    int length;
    try {
      length = slotMemory.get(ValueLayout.JAVA_INT, slotOffset);
    } catch (Exception e) {
      throw new IllegalStateException(String.format(
          "Failed to read length at offset %d (page %d, slot %d, memory size %d)",
          slotOffset, recordPageKey, slotNumber, slotMemory.byteSize()), e);
    }

    // DEFENSIVE: Sanity check the length value before using it
    if (length < 0 || length > slotMemory.byteSize()) {
      throw new IllegalStateException(String.format(
          "CORRUPT LENGTH at offset %d: %d (segment size: %d, page %d, slot %d, revision: %d)",
          slotOffset, length, slotMemory.byteSize(), recordPageKey, slotNumber, revision));
    }

    if (length <= 0) {
      // Print memory segment contents around the failing offset
      String memoryDump = dumpMemorySegmentAroundOffset(slotOffset, slotNumber);

      String errorMessage = String.format(
          "Slot length must be greater than 0, but is %d (slotNumber: %d, offset: %d, revision: %d, page: %d)",
          length, slotNumber, slotOffset, revision, recordPageKey);

      // Add comprehensive debugging info
      String debugInfo = String.format(
          "%s\nMemory segment: size=%d, closed=%s\nSlot offsets around %d: [%d, %d, %d]\nLast slot index: %d, Free space: %d\n%s",
          errorMessage, slotMemory.byteSize(), isClosed,
          slotNumber,
          slotNumber > 0 ? slotOffsets[slotNumber-1] : -1,
          slotOffsets[slotNumber],
          slotNumber < slotOffsets.length-1 ? slotOffsets[slotNumber+1] : -1,
          lastSlotIndex, slotMemoryFreeSpaceStart,
          memoryDump);

      throw new AssertionError(createStackTraceMessage(debugInfo));
    }


    // Validate that we can read the full data
    if (slotOffset + INT_SIZE + length > slotMemory.byteSize()) {
      throw new IllegalStateException(String.format(
          "Slot data extends beyond memory segment: offset=%d, length=%d, total=%d, memory_size=%d",
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

    sb.append(String.format("Dumping bytes %d to %d (offset %d marked with **):\n",
                            startOffset, endOffset - 1, offset));

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
        char c = (b >= 32 && b < 127) ? (char) b : '.';
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
    int deweyIdLength = deweyIdMemory.get(ValueLayout.JAVA_INT, deweyIdOffset);
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
    int alignedFreeSpaceStart = alignOffset(isSlotMemory ? slotMemoryFreeSpaceStart : deweyIdMemoryFreeSpaceStart);
    int freeSpaceEnd = isSlotMemory ? (int) slotMemory.byteSize() : (int) deweyIdMemory.byteSize();

    // Check if there's enough space in the current free space range
    if (freeSpaceEnd - alignedFreeSpaceStart >= requiredSize) {
      return alignedFreeSpaceStart;
    }

    int freeMemoryStart = isSlotMemory ? slotMemoryFreeSpaceStart : deweyIdMemoryFreeSpaceStart;
    int freeMemoryEnd = isSlotMemory ? (int) slotMemory.byteSize() : (int) deweyIdMemory.byteSize();
    throw new IllegalStateException(
        "Not enough space in memory segment to store the data (freeSpaceStart " + freeMemoryStart + " requiredSize: "
            + requiredSize + ", maxLength: " + freeMemoryEnd + ")");
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
    
    return (C) new KeyValueLeafPage(
        recordPageKey,
        indexType,
        config,
        pageReadTrx.getRevisionNumber(),
        slotMemory,
        deweyIdMemory
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
    return isClosed;
  }

  @Override
  @SuppressWarnings("emerging")
  protected void finalize() {
    if (!isClosed) {
      PAGES_FINALIZED_WITHOUT_CLOSE.incrementAndGet();
      
      // Track by type and pageKey for detailed leak analysis
      if (indexType != null) {
        FINALIZED_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
      }
      FINALIZED_BY_PAGE_KEY.computeIfAbsent(recordPageKey, _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
      
      // Print leak information with creation stack trace
      StringBuilder leakMsg = new StringBuilder();
      leakMsg.append(String.format("FINALIZER LEAK CAUGHT: Page %d (%s) revision=%d (instance=%d) - not closed explicitly!",
          recordPageKey, indexType, revision, System.identityHashCode(this)));
      
      if (creationStackTrace != null) {
        leakMsg.append("\n  Created at:");
        // Skip first 2 frames (getStackTrace, constructor), show next 8 frames
        for (int i = 2; i < Math.min(creationStackTrace.length, 10); i++) {
          StackTraceElement frame = creationStackTrace[i];
          leakMsg.append(String.format("\n    %s.%s(%s:%d)",
              frame.getClassName(), frame.getMethodName(),
              frame.getFileName(), frame.getLineNumber()));
        }
      }
      
      LOGGER.warn(leakMsg.toString());
      
      // NOTE: We don't call close() from finalizer because:
      // 1. Finalizer runs on GC thread - race conditions with main threads
      // 2. Modern Java discourages finalizers (deprecated)
      // 3. MemorySegments are cleaned up when Arena/allocator is freed
      // 
      // This counter tracks pages that should have been explicitly closed.
      // The real fix is in unpinAllPagesForTransaction() and TIL.clear()
    }
  }

  @Override
  public synchronized void close() {
    // CRITICAL: synchronized to prevent race with acquireGuard/releaseGuard
    if (!isClosed) {
      // CRITICAL FIX: Check guard count before closing to prevent closing guarded pages
      int currentGuardCount = guardCount.get();
      if (currentGuardCount > 0) {
        // Page is still guarded - cannot close yet
        // This can happen if cache eviction tries to close a page that's still in use
        LOGGER.warn("⚠️  GUARD PROTECTED: Attempted to close guarded page {} ({}) rev={} with guardCount={} - skipping close",
            recordPageKey, indexType, revision, currentGuardCount);
        return;
      }
      
      if (DEBUG_MEMORY_LEAKS && recordPageKey == 0) {
        boolean wasInLivePages = ALL_LIVE_PAGES.contains(this);
        LOGGER.info("[CLOSE] Page 0 ({}) rev={} instance={} wasInLivePages={} guardCount={}",
            indexType, revision, System.identityHashCode(this), wasInLivePages, currentGuardCount);
      }
      
      isClosed = true;
      
      // DIAGNOSTIC: Verify isClosed was actually set
      if (DEBUG_MEMORY_LEAKS && !isClosed) {
        LOGGER.error("⚠️⚠️⚠️ CRITICAL: isClosed still false after setting to true! {} rev={} instance={}", 
            indexType, revision, System.identityHashCode(this));
      }
      
      // DIAGNOSTIC
      if (DEBUG_MEMORY_LEAKS) {
        long closedCount = PAGES_CLOSED.incrementAndGet();
        PAGES_CLOSED_BY_TYPE.computeIfAbsent(indexType, _ -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
        boolean removed = ALL_LIVE_PAGES.remove(this);
        
        if (!removed) {
          LOGGER.warn("⚠️  Page {} ({}) rev={} instance={} was NOT in ALL_LIVE_PAGES when closing! closedCount={}",
              recordPageKey, indexType, revision, System.identityHashCode(this), closedCount);
        }
        
        // Maintain Page 0 tracking for tests/debugging
        if (recordPageKey == 0) {
          ALL_PAGE_0_INSTANCES.remove(this);
        }
      }
      
      // Release segments back to allocator
      try {
        if (slotMemory != null) {
          segmentAllocator.release(slotMemory);
          slotMemory = null;
        }
        if (deweyIdMemory != null) {
          segmentAllocator.release(deweyIdMemory);
          deweyIdMemory = null;
        }
      } catch (Exception e) {
        // Log but don't throw - we're in cleanup
        LOGGER.error("Failed to release segments for page {}", recordPageKey, e);
      }
      
      // Clear references to help GC
      Arrays.fill(records, null);
      references.clear();
      bytes = null;
      hashCode = null;
    }
  }

  /**
   * Get the actual memory size used by this page's memory segments.
   * Used for accurate Caffeine cache weighing.
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
    return total;
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
    int newCount = guardCount.incrementAndGet();
    if (DEBUG_MEMORY_LEAKS && recordPageKey == 0) {
      LOGGER.debug("[GUARD-ACQUIRE] Page 0 ({}) guardCount={}", indexType, newCount);
    }
  }

  /**
   * Release a guard on this page (decrement guard count).
   */
  public void releaseGuard() {
    int newCount = guardCount.decrementAndGet();
    if (DEBUG_MEMORY_LEAKS && recordPageKey == 0) {
      LOGGER.debug("[GUARD-RELEASE] Page 0 ({}) guardCount={}", indexType, newCount);
    }
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
   */
  public void markAccessed() {
    hot = true;
  }

  /**
   * Check if this page is HOT (recently accessed).
   *
   * @return true if page is hot, false otherwise
   */
  public boolean isHot() {
    return hot;
  }

  /**
   * Clear the HOT bit (for clock sweeper second-chance algorithm).
   */
  public void clearHot() {
    hot = false;
  }

  /**
   * Reset page data structures for reuse.
   * Clears records and internal state but keeps MemorySegments allocated.
   * Used when evicting a page to prepare frame for reuse.
   */
  public void reset() {
    // Clear record arrays
    Arrays.fill(records, null);
    
    // Clear offsets
    Arrays.fill(slotOffsets, -1);
    Arrays.fill(deweyIdOffsets, -1);
    
    // Reset free space pointers
    slotMemoryFreeSpaceStart = 0;
    deweyIdMemoryFreeSpaceStart = 0;
    lastSlotIndex = -1;
    lastDeweyIdIndex = areDeweyIDsStored ? -1 : -1;
    
    // Clear references
    references.clear();
    addedReferences = false;
    
    // Clear cached data
    bytes = null;
    hashCode = null;
    
    // Reset guard count (should already be 0, but ensure it)
    guardCount.set(0);
    hash = 0;
    
    // Clear HOT bit
    hot = false;
    
    // NOTE: We do NOT release MemorySegments here - they stay allocated
    // The allocator's release() method is called separately if needed
  }

  // Add references to OverflowPages.
  public void addReferences(final ResourceConfiguration resourceConfiguration) {
    if (!addedReferences) {
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
      for (final DataRecord record : records) {
        if (record == null) {
          continue;
        }
        final var recordID = record.getNodeKey();
        final var offset = StorageEngineReader.recordPageOffset(recordID);

        // Must be either a normal record or one which requires an overflow page.
        // Use confined arena for temporary serialization
        var out = new MemorySegmentBytesOut(tempArena, 60);
        recordPersister.serialize(out, record, resourceConfiguration);
        final var buffer = out.getDestination();
        
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
        }
      }
    } // Confined arena automatically closes here, freeing all temporary buffers
  }
}

