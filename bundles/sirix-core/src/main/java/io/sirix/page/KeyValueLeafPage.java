package io.sirix.page;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.api.PageTrx;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.utils.ArrayIterator;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesOut;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * An UnorderedKeyValuePage stores a set of records, commonly nodes in an unordered data structure.
 * </p>
 * <p>
 * The page currently is not thread safe (might have to be for concurrent write-transactions)!
 * </p>
 */
@SuppressWarnings({ "unchecked", "ConstantValue" })
public final class KeyValueLeafPage implements KeyValuePage<DataRecord> {

  private static final int INT_SIZE = Integer.BYTES;



  public final Arena arena = Arena.ofAuto();

  /**
   * The current revision.
   */
  private final int revision;

  /**
   * Determines if DeweyIDs are stored or not.
   */
  private final boolean areDeweyIDsStored;

  private final boolean doResizeMemorySegmentsIfNeeded;

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
  private final long recordPageKey;

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
  private final IndexType indexType;

  /**
   * Persistenter.
   */
  private final RecordSerializer recordPersister;

  /**
   * The resource configuration.
   */
  private final ResourceConfiguration resourceConfig;

  private volatile BytesOut<?> bytes;

  private volatile byte[] hashCode;

  private int hash;

  /**
   * Copy constructor.
   *
   * @param pageToClone the page to clone
   */
  @SuppressWarnings("CopyConstructorMissesField")
  public KeyValueLeafPage(final KeyValueLeafPage pageToClone) {
    this.addedReferences = false;
    this.references = pageToClone.references;
    this.recordPageKey = pageToClone.recordPageKey;
    this.records = Arrays.copyOf(pageToClone.records, pageToClone.records.length);
    this.slotMemory = arena.allocate(pageToClone.slotMemory.byteSize());
    MemorySegment.copy(pageToClone.slotMemory, 0, this.slotMemory, 0, pageToClone.slotMemory.byteSize());
    this.deweyIdMemory = arena.allocate(pageToClone.deweyIdMemory.byteSize());
    MemorySegment.copy(pageToClone.deweyIdMemory, 0, this.deweyIdMemory, 0, pageToClone.deweyIdMemory.byteSize());
    this.indexType = pageToClone.indexType;
    this.recordPersister = pageToClone.recordPersister;
    this.resourceConfig = pageToClone.resourceConfig;
    this.revision = pageToClone.revision;
    this.areDeweyIDsStored = pageToClone.areDeweyIDsStored;
    this.slotOffsets = Arrays.copyOf(pageToClone.slotOffsets, pageToClone.slotOffsets.length);
    this.deweyIdOffsets = Arrays.copyOf(pageToClone.deweyIdOffsets, pageToClone.deweyIdOffsets.length);
    this.doResizeMemorySegmentsIfNeeded = true;
  }

  /**
   * Constructor which initializes a new {@link KeyValueLeafPage}.
   *
   * @param recordPageKey  base key assigned to this node page
   * @param indexType      the index type
   * @param resourceConfig the resource configuration
   */
  public KeyValueLeafPage(final @NonNegative long recordPageKey, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final int revisionNumber) {
    // Assertions instead of requireNonNull(...) checks as it's part of the
    // internal flow.
    assert resourceConfig != null : "The resource config must not be null!";

    this.references = new ConcurrentHashMap<>();
    this.recordPageKey = recordPageKey;
    this.records = new DataRecord[Constants.NDP_NODE_COUNT];
    this.areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
    this.slotMemory = arena.allocate(Constants.NDP_NODE_COUNT * Constants.MAX_RECORD_SIZE);
    if (areDeweyIDsStored) {
      this.deweyIdMemory = arena.allocate(Constants.NDP_NODE_COUNT * Constants.MAX_DEWEYID_SIZE);
    } else {
      this.deweyIdMemory = null;
    }
    this.indexType = indexType;
    this.resourceConfig = resourceConfig;
    this.recordPersister = resourceConfig.recordPersister;
    this.revision = revisionNumber;
    this.slotOffsets = new int[Constants.NDP_NODE_COUNT];
    this.deweyIdOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);
    Arrays.fill(deweyIdOffsets, -1);
    this.doResizeMemorySegmentsIfNeeded = true;
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
  KeyValueLeafPage(final long recordPageKey, final int revision, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final boolean areDeweyIDsStored,
      final RecordSerializer recordPersister, final Map<Long, PageReference> references, final int slotMemorySize,
      final int deweyIdMemorySize) {
    this.recordPageKey = recordPageKey;
    this.revision = revision;
    this.indexType = indexType;
    this.resourceConfig = resourceConfig;
    this.areDeweyIDsStored = areDeweyIDsStored;
    this.recordPersister = recordPersister;
    this.slotMemory = arena.allocate(slotMemorySize);

    if (areDeweyIDsStored) {
      this.deweyIdMemory = arena.allocate(deweyIdMemorySize);
    } else {
      this.deweyIdMemory = null;
    }

    this.references = references;
    this.records = new DataRecord[Constants.NDP_NODE_COUNT];
    this.slotOffsets = new int[Constants.NDP_NODE_COUNT];
    this.deweyIdOffsets = new int[Constants.NDP_NODE_COUNT];
    Arrays.fill(slotOffsets, -1);
    Arrays.fill(deweyIdOffsets, -1);
    this.doResizeMemorySegmentsIfNeeded = false;
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
  public <C extends KeyValuePage<DataRecord>> C copy() {
    return (C) new KeyValueLeafPage(this);
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

  private int alignOffset(int offset) {
    return (offset + ALIGNMENT - 1) & -ALIGNMENT;
  }

  @Override
  public void setSlot(byte[] recordData, int slotNumber) {
    var memorySegment = setData(MemorySegment.ofArray(recordData), slotNumber, slotOffsets, slotMemory);

    if (memorySegment != null) {
      slotMemory = memorySegment;
    }
  }

  // For testing.
  public void setSlotMemory(MemorySegment slotMemory) {
    this.slotMemory = slotMemory;
  }

  @Override
  public void setSlot(MemorySegment data, int slotNumber) {
    var memorySegment = setData(data, slotNumber, slotOffsets, slotMemory);

    if (memorySegment != null) {
      slotMemory = memorySegment;
    }
  }

  private MemorySegment setData(MemorySegment data, int slotNumber, int[] offsets, MemorySegment memory) {
    if (data == null || data.byteSize() == 0) {
      return null;
    }

    var dataSize = (int) data.byteSize();
    int requiredSize = INT_SIZE + dataSize;
    int currentOffset = offsets[slotNumber];

    int sizeDelta = 0;

    boolean resized = false;

    // Check if resizing is needed.
    if (!hasEnoughSpace(offsets, memory, requiredSize + sizeDelta)) {
      // Resize the memory segment.
      long newSize = Math.max(memory.byteSize() * 2, memory.byteSize() + requiredSize + sizeDelta);
      memory = resizeMemorySegment(memory, newSize, offsets);
      resized = true;
    }

    if (currentOffset >= 0) {
      // Existing slot, check if there's enough space to accommodate the new data.
      long alignedOffset = alignOffset(currentOffset);
      int currentSize = INT_SIZE + memory.get(ValueLayout.JAVA_INT, alignedOffset);

      if (currentSize == requiredSize) {
        // If the size is the same, update it directly.
        memory.set(ValueLayout.JAVA_INT, alignedOffset, dataSize);
        memory.asSlice(alignedOffset + INT_SIZE, dataSize).copyFrom(data);

        return null; // No resizing needed
      } else {
        // Calculate sizeDelta based on whether the new data is larger or smaller.
        sizeDelta = requiredSize - currentSize;
      }
    } else {
      // If the slot is empty, determine where to place the new data.
      currentOffset = findFreeSpace(offsets, memory, requiredSize, (int) memory.byteSize());
      offsets[slotNumber] = alignOffset(currentOffset);
    }

    // Perform any necessary shifting.
    if (sizeDelta != 0) {
      shiftSlotMemory(slotNumber, sizeDelta, offsets, memory);
    }

    // Write the new data into the slot.
    int alignedOffset = alignOffset(currentOffset);
    memory.set(ValueLayout.JAVA_INT, alignedOffset, dataSize);
    memory.asSlice(alignedOffset + INT_SIZE, dataSize).copyFrom(data);

    return resized ? memory : null;
  }

  boolean hasEnoughSpace(int[] offsets, MemorySegment memory, int requiredDataSize) {
    if (!doResizeMemorySegmentsIfNeeded) {
      return true;
    }

    // Check if the available space can accommodate the new slot.
    return getAvailableSpace(offsets, memory, true) >= requiredDataSize;
  }

  int getAvailableSpace(int[] offsets, MemorySegment memory, boolean doAlign) {
    int lastSlotIndex = getLastSlotIndex(offsets);

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
    if (doAlign) {
      return (int) memory.byteSize() - alignOffset(alignedLastOffset + lastSlotSize);
    } else {
      return (int) memory.byteSize() - (alignedLastOffset + lastSlotSize);
    }
  }

  MemorySegment resizeMemorySegment(MemorySegment oldMemory, long newSize, int[] offsets) {
    MemorySegment newMemory = arena.allocate(newSize);
    MemorySegment.copy(oldMemory, 0, newMemory, 0, oldMemory.byteSize());

    // Update offsets to reference the new memory segment.
    for (int i = 0; i < offsets.length; i++) {
      if (offsets[i] >= 0) {
        offsets[i] = alignOffset(offsets[i]);
      }
    }

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

  int getUsedByteSize(int[] offsets, MemorySegment memory) {
    if (memory == null) {
      return 0;
    }
    return (int) memory.byteSize() - getAvailableSpace(offsets, memory, true);
  }

  private void shiftSlotMemory(int slotNumber, int sizeDelta, int[] offsets, MemorySegment memory) {
    if (sizeDelta == 0) {
      return; // No shift needed if there's no size change.
    }

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
    int lastSlotIndex = getLastSlotIndex(offsets);
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
      }
    }
  }

  private int getLastSlotIndex(int[] offsets) {
    int lastSlotIndex = -1;
    int maxOffset = Integer.MIN_VALUE;

    for (int i = 0; i < offsets.length; i++) {
      if (offsets[i] >= 0 && offsets[i] > maxOffset) {
        maxOffset = offsets[i];
        lastSlotIndex = i;
      }
    }

    return lastSlotIndex;
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
    return slotOffsets[slotNumber] >= 0;
  }

  @Override
  public MemorySegment getSlot(int slotNumber) {
    int slotOffset = slotOffsets[slotNumber];
    if (slotOffset < 0) {
      return null;
    }
    int slotLength = slotMemory.get(ValueLayout.JAVA_INT, slotOffset);
    slotOffset += INT_SIZE;
    assert slotLength > 0;
    return slotMemory.asSlice(slotOffset, slotLength);
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

  private int findFreeSpace(int[] offsets, MemorySegment memorySegment, int requiredSize, int maxLength) {
    int currentOffset = 0;

    // Sort the offsets to ensure we're checking in order of memory layout.
    int[] sortedOffsets = Arrays.stream(offsets).filter(offset -> offset >= 0) // Filter out unassigned slots
                                .sorted().toArray();

    // Iterate through existing segments to find free space between them.
    for (int offset : sortedOffsets) {
      int length = memorySegment.get(ValueLayout.JAVA_INT, offset);
      int endOffset = offset + INT_SIZE + length;

      // Align the current offset to the next 4-byte boundary.
      currentOffset = alignOffset(currentOffset);

      // Check if there's enough space before the current segment.
      if (offset - currentOffset >= requiredSize) {
        return currentOffset;
      }

      // Move current offset to the end of the current segment.
      currentOffset = endOffset;
    }

    // Align the current offset to the next 4-byte boundary.
    currentOffset = alignOffset(currentOffset);

    // Check if there's enough space at the end of the memory.
    if (maxLength - currentOffset >= requiredSize) {
      return currentOffset;
    }

    // If no free space is found.
    throw new IllegalStateException(
        "Not enough space in memory segment to store the data (currentOffset " + currentOffset + " requiredSize: "
            + requiredSize + ", maxLength: " + maxLength + ")");
  }

  @Override
  public <C extends KeyValuePage<DataRecord>> C newInstance(@NonNegative long recordPageKey,
      @NonNull IndexType indexType, @NonNull PageReadOnlyTrx pageReadTrx) {
    return (C) new KeyValueLeafPage(recordPageKey,
                                    indexType,
                                    pageReadTrx.getResourceSession().getResourceConfig(),
                                    pageReadTrx.getRevisionNumber());
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
      if (record != null || getSlot(i) != null) {
        count++;
      }
    }
    return count;
  }

  @Override
  public Page clearPage() {
    return KeyValuePage.super.clearPage();
  }

  @Override
  public List<PageReference> getReferences() {
    return List.of(references.values().toArray(new PageReference[0]));
  }

  @Override
  public void commit(final @NonNull PageTrx pageWriteTrx) {
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
    var out = Bytes.elasticHeapByteBuffer(30);
    for (final DataRecord record : records) {
      if (record == null) {
        continue;
      }
      final var recordID = record.getNodeKey();
      final var offset = PageReadOnlyTrx.recordPageOffset(recordID);

      // Must be either a normal record or one which requires an overflow page.
      recordPersister.serialize(out, record, resourceConfiguration);
      final var data = out.toByteArray();
      out.clear();
      if (data.length > PageConstants.MAX_RECORD_SIZE) {
        final var reference = new PageReference();
        reference.setPage(new OverflowPage(data));
        references.put(recordID, reference);
      } else {
        setSlot(data, offset);
      }
    }
  }
}
