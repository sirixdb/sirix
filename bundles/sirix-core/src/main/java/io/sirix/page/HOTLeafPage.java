/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.index.hot.NodeReferencesSerializer;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.settings.DiagnosticSettings;
import io.sirix.utils.OS;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HOT (Height Optimized Trie) leaf page for cache-friendly secondary indexes.
 * 
 * <p>Stores sorted key-value entries with off-heap MemorySegment storage.
 * Implements KeyValuePage for versioning compatibility with existing infrastructure.</p>
 * 
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li>Off-heap storage via MemorySegment (zero-copy deserialization)</li>
 *   <li>Sorted entries for O(log n) binary search within page</li>
 *   <li>Guard-based lifetime management (LeanStore/Umbra pattern)</li>
 *   <li>No sibling pointers (COW-compatible)</li>
 *   <li>SIMD-optimized key comparison via MemorySegment.mismatch()</li>
 * </ul>
 * 
 * <p><b>Memory Layout:</b></p>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │ Entry format: [u16 keyLen][key bytes][u16 valueLen][value bytes]            │
 * │ Entries are stored contiguously in slotMemory, offsets in slotOffsets array │
 * └─────────────────────────────────────────────────────────────────────────────┘
 * </pre>
 * 
 * @author Johannes Lichtenberger
 * @see KeyValuePage
 * @see HOTIndirectPage
 */
public final class HOTLeafPage implements KeyValuePage<DataRecord> {

  /** Sentinel value for "not found" in binary search. */
  public static final int NOT_FOUND = -1;
  
  /** Default page size for off-heap allocation (64KB). */
  public static final int DEFAULT_SIZE = 64 * 1024;
  
  /** Maximum entries per page before split. */
  public static final int MAX_ENTRIES = 512;
  
  /**
   * Unaligned short layout for zero-copy deserialization.
   * When slotMemory is a slice, it may not be 2-byte aligned.
   */
  private static final ValueLayout.OfShort JAVA_SHORT_UNALIGNED = ValueLayout.JAVA_SHORT.withByteAlignment(1);
  
  // ===== Page identity =====
  private final long recordPageKey;
  private final int revision;
  private final IndexType indexType;
  
  // ===== Off-heap storage =====
  private final MemorySegment slotMemory;
  private final Runnable releaser;
  private final int[] slotOffsets;
  private int entryCount;
  private int usedSlotMemorySize;
  
  // ===== Guard-based lifetime management (LeanStore/Umbra pattern) =====
  // Note: For production, consider using @Contended annotation to avoid false sharing
  // Padding fields prevent false sharing on guardCount AtomicInteger
  @SuppressWarnings("unused")
  private long p1, p2, p3, p4, p5, p6, p7; // Cache line padding (56 bytes before)
  private final AtomicInteger guardCount = new AtomicInteger(0);
  @SuppressWarnings("unused")
  private long p8, p9, p10, p11, p12, p13, p14; // Cache line padding (56 bytes after)
  private volatile boolean closed = false;
  private volatile boolean isOrphaned = false;
  
  // ===== Version for detecting page reuse =====
  private final AtomicInteger version = new AtomicInteger(0);
  private volatile boolean hot = false;
  
  // ===== Page references for overflow entries =====
  private final it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<PageReference> pageReferences;
  
  // ===== Diagnostic tracking =====
  @SuppressWarnings("unused")
  private static final boolean DEBUG_MEMORY_LEAKS = DiagnosticSettings.MEMORY_LEAK_TRACKING;
  
  /**
   * Create a new HOTLeafPage with allocated off-heap memory.
   *
   * @param recordPageKey the page key
   * @param revision the revision number
   * @param indexType the index type (PATH, CAS, NAME)
   */
  public HOTLeafPage(long recordPageKey, int revision, @NonNull IndexType indexType) {
    this.recordPageKey = recordPageKey;
    this.revision = revision;
    this.indexType = Objects.requireNonNull(indexType);
    
    // Allocate off-heap memory
    MemorySegmentAllocator allocator = OS.isWindows() 
        ? WindowsMemorySegmentAllocator.getInstance() 
        : LinuxMemorySegmentAllocator.getInstance();
    this.slotMemory = allocator.allocate(DEFAULT_SIZE);
    this.releaser = () -> allocator.release(slotMemory);
    
    this.slotOffsets = new int[MAX_ENTRIES];
    this.entryCount = 0;
    this.usedSlotMemorySize = 0;
    this.pageReferences = new it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<>();
  }
  
  /**
   * Create a HOTLeafPage with provided memory segment (for deserialization).
   *
   * @param recordPageKey the page key
   * @param revision the revision number
   * @param indexType the index type
   * @param slotMemory the off-heap memory segment
   * @param releaser the releaser for the memory segment
   * @param slotOffsets the slot offsets array
   * @param entryCount the number of entries
   * @param usedSlotMemorySize the used slot memory size
   */
  public HOTLeafPage(long recordPageKey, int revision, @NonNull IndexType indexType,
                     @NonNull MemorySegment slotMemory, @Nullable Runnable releaser,
                     int[] slotOffsets, int entryCount, int usedSlotMemorySize) {
    this.recordPageKey = recordPageKey;
    this.revision = revision;
    this.indexType = Objects.requireNonNull(indexType);
    this.slotMemory = Objects.requireNonNull(slotMemory);
    this.releaser = releaser;
    this.slotOffsets = slotOffsets;
    this.entryCount = entryCount;
    this.usedSlotMemorySize = usedSlotMemorySize;
    this.pageReferences = new it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<>();
  }
  
  // ===== Binary Search with SIMD-optimized comparison =====
  
  /**
   * Find entry index for key using binary search.
   * Uses branchless comparison for better branch prediction.
   *
   * @param key the search key
   * @return index if found, or -(insertionPoint + 1) if not found
   */
  public int findEntry(byte[] key) {
    Objects.requireNonNull(key);
    int low = 0;
    int high = entryCount;
    
    while (low < high) {
      int mid = (low + high) >>> 1;
      int cmp = compareKeysSimd(getKeySlice(mid), key);
      // Branchless update (cmov-friendly)
      low = cmp < 0 ? mid + 1 : low;
      high = cmp > 0 ? mid : high;
      if (cmp == 0) {
        return mid;
      }
    }
    return -(low + 1);
  }
  
  /**
   * SIMD-optimized key comparison using MemorySegment.mismatch().
   *
   * @param a first key as MemorySegment
   * @param b second key as byte array
   * @return negative if a < b, positive if a > b, zero if equal
   */
  private static int compareKeysSimd(MemorySegment a, byte[] b) {
    MemorySegment bSeg = MemorySegment.ofArray(b);
    long mismatch = a.mismatch(bSeg);
    if (mismatch == -1) {
      return 0;
    }
    if (mismatch == a.byteSize()) {
      return -1;
    }
    if (mismatch == bSeg.byteSize()) {
      return 1;
    }
    return Byte.compareUnsigned(
        a.get(ValueLayout.JAVA_BYTE, mismatch),
        bSeg.get(ValueLayout.JAVA_BYTE, mismatch)
    );
  }
  
  // ===== Zero-copy key/value access =====
  
  /**
   * Get key slice from off-heap segment (zero-copy).
   *
   * @param index the entry index
   * @return the key as a MemorySegment slice
   */
  public MemorySegment getKeySlice(int index) {
    Objects.checkIndex(index, entryCount);
    int offset = slotOffsets[index];
    int keyLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    return slotMemory.asSlice(offset + 2, keyLen);
  }
  
  /**
   * Get value slice from off-heap segment (zero-copy).
   *
   * @param index the entry index
   * @return the value as a MemorySegment slice
   */
  public MemorySegment getValueSlice(int index) {
    Objects.checkIndex(index, entryCount);
    int offset = slotOffsets[index];
    int keyLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    int valueOffset = offset + 2 + keyLen;
    int valueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueOffset));
    return slotMemory.asSlice(valueOffset + 2, valueLen);
  }
  
  /**
   * Get key as byte array (copies data).
   *
   * @param index the entry index
   * @return the key as byte array
   */
  public byte[] getKey(int index) {
    MemorySegment slice = getKeySlice(index);
    byte[] key = new byte[(int) slice.byteSize()];
    MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, key, 0, key.length);
    return key;
  }
  
  /**
   * Get value as byte array (copies data).
   *
   * @param index the entry index
   * @return the value as byte array
   */
  public byte[] getValue(int index) {
    MemorySegment slice = getValueSlice(index);
    byte[] value = new byte[(int) slice.byteSize()];
    MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, value, 0, value.length);
    return value;
  }
  
  // ===== Insert/Update operations =====
  
  /**
   * Insert or update an entry.
   *
   * @param key the key
   * @param value the value
   * @return true if inserted, false if updated
   */
  public boolean put(byte[] key, byte[] value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    
    int index = findEntry(key);
    if (index >= 0) {
      // Update existing entry - for now, just mark as updated
      // A more sophisticated implementation would handle in-place updates
      return false;
    }
    
    // Insert new entry
    int insertPos = -(index + 1);
    return insertAt(insertPos, key, value);
  }
  
  /**
   * Insert entry at specified position.
   *
   * @param pos insertion position
   * @param key the key
   * @param value the value
   * @return true if successful
   */
  private boolean insertAt(int pos, byte[] key, byte[] value) {
    if (entryCount >= MAX_ENTRIES) {
      return false; // Page full, needs split
    }
    
    // Calculate entry size: [u16 keyLen][key][u16 valueLen][value]
    int entrySize = 2 + key.length + 2 + value.length;
    
    if (usedSlotMemorySize + entrySize > slotMemory.byteSize()) {
      return false; // No space left
    }
    
    // Shift offsets to make room
    if (pos < entryCount) {
      System.arraycopy(slotOffsets, pos, slotOffsets, pos + 1, entryCount - pos);
    }
    
    // Write entry to slotMemory
    int offset = usedSlotMemorySize;
    slotOffsets[pos] = offset;
    
    slotMemory.set(JAVA_SHORT_UNALIGNED, offset, (short) key.length);
    offset += 2;
    MemorySegment.copy(key, 0, slotMemory, ValueLayout.JAVA_BYTE, offset, key.length);
    offset += key.length;
    slotMemory.set(JAVA_SHORT_UNALIGNED, offset, (short) value.length);
    offset += 2;
    MemorySegment.copy(value, 0, slotMemory, ValueLayout.JAVA_BYTE, offset, value.length);
    
    usedSlotMemorySize += entrySize;
    entryCount++;
    
    return true;
  }
  
  /**
   * Check if the page needs to be split.
   *
   * @return true if page is full
   */
  public boolean needsSplit() {
    return entryCount >= MAX_ENTRIES;
  }
  
  /**
   * Get entry count.
   *
   * @return number of entries
   */
  public int getEntryCount() {
    return entryCount;
  }
  
  // ===== Merge, Update, and Copy operations for HOT index =====
  
  /**
   * Update the value at a given index.
   *
   * <p>This is used for merging NodeReferences - the new value replaces the old.</p>
   *
   * @param index the entry index
   * @param newValue the new value
   * @return true if updated, false if there wasn't enough space
   */
  public boolean updateValue(int index, byte[] newValue) {
    Objects.checkIndex(index, entryCount);
    Objects.requireNonNull(newValue);
    
    // Get old entry info
    int offset = slotOffsets[index];
    int keyLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    int valueOffset = offset + 2 + keyLen;
    int oldValueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueOffset));
    
    // If new value same size or smaller, update in-place
    if (newValue.length <= oldValueLen) {
      slotMemory.set(JAVA_SHORT_UNALIGNED, valueOffset, (short) newValue.length);
      MemorySegment.copy(newValue, 0, slotMemory, ValueLayout.JAVA_BYTE, valueOffset + 2, newValue.length);
      return true;
    }
    
    // New value is larger - need to append and update offset
    int newEntrySize = 2 + keyLen + 2 + newValue.length;
    if (usedSlotMemorySize + newEntrySize > slotMemory.byteSize()) {
      return false; // No space for larger value
    }
    
    // Copy key and new value to end of used space
    int newOffset = usedSlotMemorySize;
    byte[] key = getKey(index);
    
    slotMemory.set(JAVA_SHORT_UNALIGNED, newOffset, (short) keyLen);
    MemorySegment.copy(key, 0, slotMemory, ValueLayout.JAVA_BYTE, newOffset + 2, keyLen);
    slotMemory.set(JAVA_SHORT_UNALIGNED, newOffset + 2 + keyLen, (short) newValue.length);
    MemorySegment.copy(newValue, 0, slotMemory, ValueLayout.JAVA_BYTE, newOffset + 2 + keyLen + 2, newValue.length);
    
    // Update offset pointer
    slotOffsets[index] = newOffset;
    usedSlotMemorySize += newEntrySize;
    
    return true;
  }
  
  /**
   * Merge a value with existing entry using NodeReferences OR semantics.
   *
   * <p>If key exists, merges the NodeReferences (OR operation on bitmaps).
   * If key doesn't exist, inserts new entry.</p>
   *
   * @param key the key bytes
   * @param keyLen the key length
   * @param value the value bytes (serialized NodeReferences)
   * @param valueLen the value length
   * @return true if merged/inserted successfully
   */
  public boolean mergeWithNodeRefs(byte[] key, int keyLen, byte[] value, int valueLen) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    
    // Search for existing key
    byte[] keySlice = keyLen == key.length ? key : java.util.Arrays.copyOf(key, keyLen);
    int index = findEntry(keySlice);
    
    if (index >= 0) {
      // Key exists - merge NodeReferences
      byte[] existingValue = getValue(index);
      
      // Deserialize both and merge
      var existingRefs = NodeReferencesSerializer.deserialize(existingValue);
      var newRefs = NodeReferencesSerializer.deserialize(value, 0, valueLen);
      
      // Check for tombstone in new value
      if (!newRefs.hasNodeKeys()) {
        // Tombstone - set empty value
        return updateValue(index, new byte[] { (byte) 0xFE }); // TOMBSTONE_FORMAT
      }
      
      // Merge bitmaps (OR operation)
      NodeReferencesSerializer.merge(existingRefs, newRefs);
      
      // Serialize merged result
      byte[] mergedBytes = NodeReferencesSerializer.serialize(existingRefs);
      return updateValue(index, mergedBytes);
    } else {
      // Key doesn't exist - insert new entry
      byte[] valueSlice = valueLen == value.length ? value : java.util.Arrays.copyOf(value, valueLen);
      int insertPos = -(index + 1);
      return insertAt(insertPos, keySlice, valueSlice);
    }
  }
  
  /**
   * Create a deep copy of this page for COW (Copy-on-Write).
   *
   * <p>The copy has its own off-heap memory segment and independent state.</p>
   *
   * @return a new HOTLeafPage with copied data
   */
  public HOTLeafPage copy() {
    // Allocate new off-heap memory
    MemorySegmentAllocator allocator = OS.isWindows() 
        ? WindowsMemorySegmentAllocator.getInstance() 
        : LinuxMemorySegmentAllocator.getInstance();
    MemorySegment newSlotMemory = allocator.allocate(DEFAULT_SIZE);
    Runnable newReleaser = () -> allocator.release(newSlotMemory);
    
    // Bulk copy off-heap data
    MemorySegment.copy(slotMemory, 0, newSlotMemory, 0, usedSlotMemorySize);
    
    // Deep copy on-heap arrays
    int[] newSlotOffsets = java.util.Arrays.copyOf(slotOffsets, slotOffsets.length);
    
    // Create new page with copied data
    HOTLeafPage copy = new HOTLeafPage(
        recordPageKey, revision, indexType,
        newSlotMemory, newReleaser, newSlotOffsets, entryCount, usedSlotMemorySize);
    
    // Deep copy page references (for overflow entries)
    for (var entry : pageReferences.long2ObjectEntrySet()) {
      copy.pageReferences.put(entry.getLongKey(), entry.getValue());
    }
    
    return copy;
  }
  
  /**
   * Split this page, moving the right half of entries to another page.
   *
   * <p>After split:</p>
   * <ul>
   *   <li>This page keeps entries [0, splitPoint)</li>
   *   <li>Target page gets entries [splitPoint, entryCount)</li>
   * </ul>
   *
   * @param target the page to receive the right half of entries
   * @return the first key in the target page (split key for parent navigation)
   */
  public byte[] splitTo(@NonNull HOTLeafPage target) {
    Objects.requireNonNull(target);
    
    if (entryCount < 2) {
      throw new IllegalStateException("Cannot split page with less than 2 entries");
    }
    
    // Split at midpoint
    int splitPoint = entryCount / 2;
    
    // Copy right half to target
    for (int i = splitPoint; i < entryCount; i++) {
      byte[] key = getKey(i);
      byte[] value = getValue(i);
      
      boolean inserted = target.insertAt(target.entryCount, key, value);
      if (!inserted) {
        throw new IllegalStateException("Failed to insert entry into split target - target page full");
      }
    }
    
    // Get the split key (first key in target)
    byte[] splitKey = target.getKey(0);
    
    // Truncate this page to keep only left half
    // Note: We don't reclaim memory, just reduce entry count
    // The old entries become "garbage" that will be reclaimed on next COW
    entryCount = splitPoint;
    
    return splitKey;
  }
  
  /**
   * Get the first (minimum) key in this page.
   *
   * @return the first key, or null if page is empty
   */
  public @Nullable byte[] getFirstKey() {
    if (entryCount == 0) {
      return null;
    }
    return getKey(0);
  }
  
  /**
   * Get the last (maximum) key in this page.
   *
   * @return the last key, or null if page is empty
   */
  public @Nullable byte[] getLastKey() {
    if (entryCount == 0) {
      return null;
    }
    return getKey(entryCount - 1);
  }

  /**
   * Get all keys in this page as an array.
   *
   * <p>Keys are returned in sorted order.</p>
   *
   * @return array of all keys (never null, may be empty)
   */
  public byte[][] getAllKeys() {
    byte[][] keys = new byte[entryCount][];
    for (int i = 0; i < entryCount; i++) {
      keys[i] = getKey(i);
    }
    return keys;
  }

  /**
   * Merge another HOTLeafPage into this one.
   *
   * <p>Used for versioning - combines entries from multiple page fragments.
   * Newer entries take precedence. NodeReferences are OR-merged.</p>
   *
   * @param other the page to merge from
   * @return true if all entries merged successfully
   */
  public boolean mergeFrom(HOTLeafPage other) {
    Objects.requireNonNull(other);
    
    for (int i = 0; i < other.entryCount; i++) {
      byte[] key = other.getKey(i);
      byte[] value = other.getValue(i);
      
      if (!mergeWithNodeRefs(key, key.length, value, value.length)) {
        return false; // Page full
      }
    }
    return true;
  }
  
  // ===== Guard-based lifetime management =====
  
  /**
   * Acquire a guard (increment reference count).
   * Pages with guards cannot be evicted.
   */
  public void acquireGuard() {
    guardCount.incrementAndGet();
    hot = true;
  }
  
  /**
   * Release a guard (decrement reference count).
   * If orphaned and guard count reaches zero, close the page.
   */
  public void releaseGuard() {
    int remaining = guardCount.decrementAndGet();
    if (remaining < 0) {
      throw new IllegalStateException("Guard count underflow for page " + recordPageKey);
    }
    if (remaining == 0 && isOrphaned) {
      close();
    }
  }
  
  /**
   * Get current guard count.
   *
   * @return guard count
   */
  public int getGuardCount() {
    return guardCount.get();
  }
  
  /**
   * Mark page as orphaned (removed from cache but still guarded).
   */
  public void markOrphaned() {
    isOrphaned = true;
    if (guardCount.get() == 0) {
      close();
    }
  }
  
  /**
   * Check if page is orphaned.
   *
   * @return true if orphaned
   */
  public boolean isOrphaned() {
    return isOrphaned;
  }
  
  /**
   * Close the page and release off-heap memory.
   */
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    
    if (releaser != null) {
      releaser.run();
    }
  }
  
  // ===== KeyValuePage interface implementation =====
  
  @Override
  public long getPageKey() {
    return recordPageKey;
  }
  
  @Override
  public IndexType getIndexType() {
    return indexType;
  }
  
  @Override
  public int getRevision() {
    return revision;
  }
  
  @Override
  public int size() {
    return entryCount;
  }
  
  @Override
  public boolean isClosed() {
    return closed;
  }
  
  @Override
  public MemorySegment slots() {
    return slotMemory;
  }
  
  @Override
  public int getUsedSlotsSize() {
    return usedSlotMemorySize;
  }
  
  @Override
  public MemorySegment getSlot(int slotNumber) {
    if (slotNumber < 0 || slotNumber >= entryCount) {
      return null;
    }
    int offset = slotOffsets[slotNumber];
    int keyLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, offset));
    int valueOffset = offset + 2 + keyLen;
    int valueLen = Short.toUnsignedInt(slotMemory.get(JAVA_SHORT_UNALIGNED, valueOffset));
    int totalLen = 2 + keyLen + 2 + valueLen;
    return slotMemory.asSlice(offset, totalLen);
  }
  
  @Override
  public byte[] getSlotAsByteArray(int slotNumber) {
    MemorySegment slot = getSlot(slotNumber);
    if (slot == null) {
      return null;
    }
    byte[] data = new byte[(int) slot.byteSize()];
    MemorySegment.copy(slot, ValueLayout.JAVA_BYTE, 0, data, 0, data.length);
    return data;
  }
  
  @Override
  public void setSlot(MemorySegment data, int slotNumber) {
    // For HOTLeafPage, this is handled by put()
    throw new UnsupportedOperationException("Use put() for HOTLeafPage");
  }
  
  @Override
  public void setSlot(byte[] recordData, int offset) {
    // For HOTLeafPage, this is handled by put()
    throw new UnsupportedOperationException("Use put() for HOTLeafPage");
  }
  
  @Override
  public MemorySegment deweyIds() {
    return null; // HOTLeafPage doesn't use Dewey IDs
  }
  
  @Override
  public int getUsedDeweyIdSize() {
    return 0;
  }
  
  @Override
  public byte[] getDeweyIdAsByteArray(int offset) {
    return null;
  }
  
  @Override
  public MemorySegment getDeweyId(int offset) {
    return null;
  }
  
  @Override
  public void setDeweyId(byte[] deweyId, int offset) {
    // Not supported
  }
  
  @Override
  public void setDeweyId(MemorySegment deweyId, int offset) {
    // Not supported
  }
  
  @Override
  public void setRecord(@NonNull DataRecord record) {
    throw new UnsupportedOperationException("HOTLeafPage uses put() instead of setRecord()");
  }
  
  @Override
  public DataRecord[] records() {
    return null; // HOTLeafPage stores raw key-value pairs, not DataRecords
  }
  
  @Override
  public DataRecord getRecord(int offset) {
    return null;
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public <I extends Iterable<DataRecord>> I values() {
    // Return empty list - HOTLeafPage stores raw key-value pairs
    List<DataRecord> emptyList = Collections.emptyList();
    return (I) emptyList;
  }
  
  @Override
  public void setPageReference(long key, @NonNull PageReference reference) {
    pageReferences.put(key, reference);
  }
  
  @Override
  public PageReference getPageReference(long key) {
    return pageReferences.get(key);
  }
  
  @Override
  public Set<Map.Entry<Long, PageReference>> referenceEntrySet() {
    // Convert fastutil entry set to standard Set<Map.Entry>
    Set<Map.Entry<Long, PageReference>> result = new HashSet<>();
    for (var entry : pageReferences.long2ObjectEntrySet()) {
      result.add(Map.entry(entry.getLongKey(), entry.getValue()));
    }
    return result;
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public <C extends KeyValuePage<DataRecord>> C newInstance(@NonNegative long recordPageKey,
      @NonNull IndexType indexType, @NonNull StorageEngineReader pageReadTrx) {
    return (C) new HOTLeafPage(recordPageKey, pageReadTrx.getRevisionNumber(), indexType);
  }
  
  // ===== Page interface =====
  
  @Override
  public List<PageReference> getReferences() {
    // HOTLeafPage doesn't have child page references in the traditional sense
    // Return the overflow page references
    return new ArrayList<>(pageReferences.values());
  }
  
  @Override
  public PageReference getOrCreateReference(int offset) {
    return null; // HOTLeafPage doesn't have child references
  }
  
  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    return false;
  }
  
  // ===== Utility methods =====
  
  /**
   * Get the hot flag for clock-based eviction.
   *
   * @return true if recently accessed
   */
  public boolean isHot() {
    return hot;
  }
  
  /**
   * Clear the hot flag (called by clock sweeper).
   */
  public void clearHot() {
    hot = false;
  }
  
  /**
   * Increment version (called when page is reused).
   *
   * @return new version
   */
  public int incrementVersion() {
    return version.incrementAndGet();
  }
  
  /**
   * Get current version.
   *
   * @return version number
   */
  public int getVersion() {
    return version.get();
  }
  
  @Override
  public String toString() {
    return "HOTLeafPage{" +
        "pageKey=" + recordPageKey +
        ", revision=" + revision +
        ", indexType=" + indexType +
        ", entryCount=" + entryCount +
        ", usedSlotMemorySize=" + usedSlotMemorySize +
        ", guardCount=" + guardCount.get() +
        ", closed=" + closed +
        '}';
  }
}

