# Memory Optimization Plan: Zero-Copy Deserialization

## Executive Summary

Analysis of CPU flame graphs and code review revealed that despite moving `slotMemory` and `deweyIdMemory` off-heap, **significant GC pressure remains** due to:

1. **Per-slot `byte[]` allocations during deserialization** (biggest issue - 500-1000 per page)
2. **Intermediate copy buffers** in the read path
3. **On-heap metadata arrays** (`slotOffsets[]`, `deweyIdOffsets[]`, `records[]`)

This plan outlines a **safe, incremental approach** with fallback paths and thorough validation.

---

## Current Flow Analysis

### Read Path: Disk → Final Memory Location

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Step 1: FileChannelReader.read()                                                │
│   ByteBuffer buffer = ByteBuffer.allocate(dataLength);    // ALLOC #1          │
│   dataFileChannel.read(buffer, position);                                       │
│   byte[] page = buffer.array();                           // backing array      │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Step 2: AbstractReader.deserialize() - Two paths available:                     │
│                                                                                 │
│   Path A (MemorySegment-aware, used by FFILz4Compressor):                       │
│     MemorySegment segment = MemorySegment.ofArray(page);                        │
│     return deserializeFromSegment(resourceConfig, segment);                     │
│                                                                                 │
│   Path B (Stream-based fallback):                                               │
│     inputStream = byteHandler.deserialize(ByteArrayInputStream);                │
│     bytes = inputStream.readAllBytes();                   // ALLOC #2 + COPY    │
│     wrappedForRead.write(bytes);                          // COPY               │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Step 3: FFILz4Compressor.decompress() [if using MemorySegment path]             │
│   int decompressedSize = compressed.get(JAVA_INT, 0);                           │
│   MemorySegment decompressed = Arena.ofAuto().allocate(decompressedSize);       │
│   decompressSegment(compressed.asSlice(4), decompressed, ...);                  │
│   return decompressed;                                    // GC-managed segment │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Step 4: PageKind.KEYVALUELEAFPAGE.deserializePage()                             │
│                                                                                 │
│   // Page header parsing (metadata) - minimal allocations                       │
│   long recordPageKey = Utils.getVarLong(source);                                │
│   int revision = source.readInt();                                              │
│   ...                                                                           │
│                                                                                 │
│   // Allocate target slot memory from pool                                      │
│   MemorySegment slotMemory = allocator.allocate(slotsMemorySize);               │
│                                                                                 │
│   // 🔴 PROBLEM: Per-slot allocations                                           │
│   for (int index = 0; index < normalEntrySize; index++) {                       │
│       int dataSize = source.readInt();                                          │
│       byte[] data = new byte[dataSize];    // 💀 ALLOC per slot (500-1000x)    │
│       source.read(data);                   // COPY #1 per slot                  │
│       page.setSlot(data, setBit);          // COPY #2 per slot                  │
│   }                                                                             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Memory Layout in slotMemory

Each slot is stored as: `[length: 4 bytes][data: N bytes]` with 4-byte alignment.

```
slotMemory layout:
┌─────────────┬──────────────┬─────────────┬──────────────┬─────┐
│ len₀ (4B)   │ data₀ (N₀B)  │ len₁ (4B)   │ data₁ (N₁B)  │ ... │
├─────────────┴──────────────┼─────────────┴──────────────┼─────┤
│ offset[0] points here      │ offset[1] points here      │     │
└────────────────────────────┴────────────────────────────┴─────┘

slotOffsets[i] = offset of slot i within slotMemory (-1 if empty)
```

---

## Phase 1: Zero-Copy Slot Deserialization

### Goal
Eliminate per-slot `byte[]` allocations by copying directly from source MemorySegment to target slotMemory.

### Prerequisites Verified ✅
1. **Source segment lifecycle**: `Arena.ofAuto().allocate()` creates GC-managed segments that remain valid during deserialization
2. **`BytesIn.getSource()`**: Already returns the underlying `MemorySegment`
3. **`BytesIn.position()`**: Already tracks current read position
4. **`MemorySegment.copy()`**: Supports direct segment-to-segment copy

### Implementation Details

#### Step 1.1: Add `skip()` method to BytesIn interface

**File**: `bundles/sirix-core/src/main/java/io/sirix/node/BytesIn.java`

```java
/**
 * Skip forward by the specified number of bytes.
 * @param bytes number of bytes to skip
 */
void skip(long bytes);
```

**File**: `bundles/sirix-core/src/main/java/io/sirix/node/MemorySegmentBytesIn.java`

```java
@Override
public void skip(long bytes) {
    if (bytes < 0) {
        throw new IllegalArgumentException("Cannot skip negative bytes: " + bytes);
    }
    if (position + bytes > memorySegment.byteSize()) {
        throw new IndexOutOfBoundsException("Skip would exceed segment bounds");
    }
    position += bytes;
}
```

#### Step 1.2: Add `setSlotDirect()` method to KeyValueLeafPage

**File**: `bundles/sirix-core/src/main/java/io/sirix/page/KeyValueLeafPage.java`

```java
/**
 * Set slot data by copying directly from a source MemorySegment.
 * This is the zero-copy path that avoids intermediate byte[] allocations.
 *
 * <p>Memory layout written to slotMemory: [length (4 bytes)][data (dataSize bytes)]</p>
 *
 * @param source the source MemorySegment containing the data
 * @param sourceOffset the offset within source where data starts
 * @param dataSize the number of bytes to copy (must be > 0)
 * @param slotNumber the slot number (0-1023)
 * @throws IllegalArgumentException if dataSize <= 0 or slotNumber out of range
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
        throw new IndexOutOfBoundsException(
            String.format("Source bounds exceeded: offset=%d, size=%d, segmentSize=%d",
                sourceOffset, dataSize, source.byteSize()));
    }

    int requiredSize = INT_SIZE + dataSize;  // 4 bytes for length + data
    int currentOffset = slotOffsets[slotNumber];
    int sizeDelta = 0;
    boolean resized = false;

    // Check if resizing is needed
    if (!hasEnoughSpace(slotOffsets, slotMemory, requiredSize)) {
        int newSize = Math.max(
            (int) slotMemory.byteSize() * 2,
            (int) slotMemory.byteSize() + requiredSize
        );
        slotMemory = resizeMemorySegment(slotMemory, newSize, slotOffsets, true);
        resized = true;
    }

    if (currentOffset >= 0) {
        // Existing slot - check if size changed
        int alignedOffset = alignOffset(currentOffset);
        int currentSize = INT_SIZE + slotMemory.get(ValueLayout.JAVA_INT, alignedOffset);
        
        if (currentSize == requiredSize) {
            // Same size - overwrite in place
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
    }

    // Perform shifting if needed
    if (sizeDelta != 0) {
        shiftSlotMemory(slotNumber, sizeDelta, slotOffsets, slotMemory);
    }

    // Write length prefix
    int alignedOffset = alignOffset(currentOffset);
    slotMemory.set(ValueLayout.JAVA_INT, alignedOffset, dataSize);

    // Copy data directly from source segment to slot memory (ZERO-COPY!)
    MemorySegment.copy(source, sourceOffset, slotMemory, alignedOffset + INT_SIZE, dataSize);

    // Update free space tracking
    updateFreeSpaceStart(slotOffsets, slotMemory, true);
}
```

#### Step 1.3: Update PageKind.KEYVALUELEAFPAGE deserialization

**File**: `bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java`

```java
// In deserializePage() method, replace:
//
// for (int index = 0; index < normalEntrySize; index++) {
//     setBit = entriesBitmap.nextSetBit(setBit + 1);
//     assert setBit >= 0;
//     final int dataSize = source.readInt();
//     assert dataSize > 0;
//     final byte[] data = new byte[dataSize];
//     source.read(data);
//     page.setSlot(data, setBit);
// }
//
// With:

// Check if source supports zero-copy (is MemorySegment-based)
final boolean useZeroCopy = source instanceof MemorySegmentBytesIn;
final MemorySegment sourceSegment = useZeroCopy ? source.getSource() : null;

for (int index = 0; index < normalEntrySize; index++) {
    setBit = entriesBitmap.nextSetBit(setBit + 1);
    assert setBit >= 0;

    final int dataSize = source.readInt();
    assert dataSize > 0;

    if (useZeroCopy) {
        // Zero-copy path: copy directly from source segment to slot memory
        page.setSlotDirect(sourceSegment, source.position(), dataSize, setBit);
        source.skip(dataSize);
    } else {
        // Fallback path: use intermediate byte[] (for non-MemorySegment sources)
        final byte[] data = new byte[dataSize];
        source.read(data);
        page.setSlot(data, setBit);
    }
}
```

### Correctness Verification Checklist

| Aspect | Verification |
|--------|--------------|
| **Alignment** | `alignOffset()` used consistently; 4-byte aligned |
| **Bounds checking** | Source and destination bounds validated before copy |
| **Memory lifecycle** | Source segment (Arena.ofAuto) valid during deser; target from allocator |
| **Slot layout** | `[len:4][data:N]` format maintained |
| **Offset tracking** | `slotOffsets[]` updated correctly |
| **Resize handling** | `hasEnoughSpace()` + `resizeMemorySegment()` called |
| **Shift handling** | `shiftSlotMemory()` called when size changes |
| **Fallback path** | Non-MemorySegment sources still work via byte[] |

### Testing Strategy

1. **Unit tests**:
   - `setSlotDirect()` with various sizes (1 byte, 100 bytes, 64KB)
   - Boundary conditions (slot 0, slot 1023)
   - Resize triggering
   - Invalid inputs (negative size, out of bounds)

2. **Integration tests**:
   - Deserialize page with zero-copy path
   - Verify slot contents match original
   - Round-trip: serialize → deserialize → verify

3. **Existing test suite**:
   - Run full test suite to ensure no regressions
   - `ConcurrentAxisTest` (the test from flame graph)

---

## Phase 2: Optimize DeweyId Deserialization (Lower Priority)

The DeweyId deserialization also creates temporary `byte[]` arrays, but:
- DeweyIds are optional (only when `areDeweyIDsStored=true`)
- Fewer DeweyIds than slots typically
- Uses delta encoding, more complex to optimize

**Defer until Phase 1 is validated.**

---

## Phase 3: Move Offset Arrays Off-Heap (Future)

Current on-heap arrays:
```java
private final int[] slotOffsets = new int[1024];     // 4KB
private final int[] deweyIdOffsets = new int[1024];  // 4KB
```

**Possible approaches:**
1. Store at fixed position within slotMemory
2. Use separate off-heap segment
3. Use fastutil `Int2IntOpenHashMap` for sparse pages

**Defer until Phase 1 impact is measured.**

---

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| Source segment freed during copy | Arena.ofAuto() keeps segment alive while referenced; deser completes synchronously |
| Memory corruption on resize | Existing `resizeMemorySegment()` tested; new code uses same logic |
| Performance regression | Benchmark before/after; fallback path preserved |
| Alignment issues | Use existing `alignOffset()` consistently |
| Test coverage gaps | Run full test suite; add targeted unit tests |

---

## Implementation Order

1. ✅ Create branch `feature/zero-copy-deserialization`
2. ⬜ Add `skip()` to `BytesIn` interface and implementation
3. ⬜ Add `setSlotDirect()` to `KeyValueLeafPage`
4. ⬜ Unit tests for new methods
5. ⬜ Update `PageKind.KEYVALUELEAFPAGE.deserializePage()`
6. ⬜ Run full test suite
7. ⬜ Benchmark with `ConcurrentAxisTest`
8. ⬜ Code review and merge

---

## Expected Impact

| Metric | Before | After |
|--------|--------|-------|
| Allocations per page read | 500-1000 `byte[]` | ~0 (zero-copy path) |
| Copies per slot | 2 (source→byte[]→slot) | 1 (source→slot) |
| GC young gen pressure | High | **-50% to -70%** |
| CPU in G1 marking | ~15% (from flame graph) | **Significantly reduced** |

---

## Code Change Summary

| File | Change |
|------|--------|
| `BytesIn.java` | Add `skip(long bytes)` method |
| `MemorySegmentBytesIn.java` | Implement `skip()` |
| `KeyValueLeafPage.java` | Add `setSlotDirect()` method |
| `PageKind.java` | Conditional zero-copy in deserialization loop |
