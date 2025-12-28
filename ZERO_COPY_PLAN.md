# Zero-Copy Page Deserialization

## Problem Statement

During page deserialization, data is copied from the decompression buffer to page's `slotMemory`. This is a significant performance bottleneck identified via JFR profiling.

**Current hot path in [`PageKind.java`](bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java) lines 165-183:**

```java
for (int index = 0; index < normalEntrySize; index++) {
    setBit = entriesBitmap.nextSetBit(setBit + 1);
    final int dataSize = source.readInt();
    
    // THIS IS THE BOTTLENECK - MemorySegment.copy() called per slot
    page.setSlotDirect(sourceSegment, source.position(), dataSize, setBit);
    source.skip(dataSize);
}
```

Each `setSlotDirect()` call invokes `MemorySegment.copy()` (see `KeyValueLeafPage.java` line 549).

## Solution: Match On-Disk Format to In-Memory Layout

### Current Serialization Format (Packed)

```
[pageKind:1][version:1][recordPageKey:varlong][revision:4][indexType:1]
[slotsMemorySize:4][lastSlotIndex:4]
[deweyIdData...]
[entriesBitmap][overlongBitmap]
[entryCount:4]
  [size1:4][data1:N]    <- packed, no alignment
  [size2:4][data2:M]
  ...
[overlongCount:4][overlongRefs...]
```

### New Zero-Copy Format (Aligned)

```
[pageKind:1][version:1][recordPageKey:varlong][revision:4][indexType:1]
[lastSlotIndex:4]
[slotOffsets:512×4=2048]   <- int[512] array, directly usable
[slotMemorySize:4]
[slotMemory:N]             <- raw bytes, identical to in-memory layout!
[deweyIdData...]
[entriesBitmap][overlongBitmap]
[overlongCount:4][overlongRefs...]
```

**Key insight:** slotMemory region in the file IS the slotMemory in RAM - just slice it!

---

## Implementation Details

### Step 1: Extend DecompressionResult

**File:** [`ByteHandler.java`](bundles/sirix-core/src/main/java/io/sirix/io/bytepipe/ByteHandler.java) lines 30-37

**Current:**

```java
record DecompressionResult(MemorySegment segment, Runnable releaser) implements AutoCloseable {
  @Override
  public void close() {
    if (releaser != null) {
      releaser.run();
    }
  }
}
```

**New:**

```java
record DecompressionResult(
    MemorySegment segment,           // Decompressed data (may be slice of backingBuffer)
    MemorySegment backingBuffer,     // Full allocated buffer
    Runnable releaser,               // Returns buffer to allocator
    AtomicBoolean ownershipTransferred  // Prevents double-release
) implements AutoCloseable {
  
  /**
   * Transfer buffer ownership to caller.
   * After this call, close() becomes a no-op and caller is responsible for releasing.
   * 
   * @return the releaser to call when done, or null if already transferred
   */
  public Runnable transferOwnership() {
    if (ownershipTransferred.compareAndSet(false, true)) {
      return releaser;
    }
    return null;  // Already transferred
  }
  
  @Override
  public void close() {
    // Only release if ownership wasn't transferred
    if (!ownershipTransferred.get() && releaser != null) {
      releaser.run();
    }
  }
}
```

**Imports to add:**

```java
import java.util.concurrent.atomic.AtomicBoolean;
```

---

### Step 2: Update FFILz4Compressor

**File:** [`FFILz4Compressor.java`](bundles/sirix-core/src/main/java/io/sirix/io/bytepipe/FFILz4Compressor.java) lines 316-378

**Current (uses pool):**

```java
public DecompressionResult decompressScoped(MemorySegment compressed) {
    // ... allocate from pool ...
    return new DecompressionResult(
        buffer.asSlice(0, actualSize),
        shouldReturn ? () -> BUFFER_POOL.offer(poolBuffer) : null
    );
}
```

**New (uses unified allocator for longer buffer lifetime):**

```java
public DecompressionResult decompressScoped(MemorySegment compressed) {
    int decompressedSize = compressed.get(JAVA_INT_UNALIGNED, 0);
    
    // Use unified allocator - buffer lifetime matches page lifetime for zero-copy
    MemorySegmentAllocator allocator = OS.isWindows() 
        ? WindowsMemorySegmentAllocator.getInstance() 
        : LinuxMemorySegmentAllocator.getInstance();
    
    MemorySegment buffer = allocator.allocate(decompressedSize);
    
    // ... decompress into buffer ...
    
    return new DecompressionResult(
        buffer.asSlice(0, actualSize),   // segment
        buffer,                           // backingBuffer
        () -> allocator.release(buffer),  // releaser
        new AtomicBoolean(false)          // ownershipTransferred
    );
}
```

**Imports to add:**

```java
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.utils.OS;
import java.util.concurrent.atomic.AtomicBoolean;
```

---

### Step 3: Update ByteHandlerPipeline

**File:** [`ByteHandlerPipeline.java`](bundles/sirix-core/src/main/java/io/sirix/io/bytepipe/ByteHandlerPipeline.java) line 146

Update to construct new DecompressionResult format:

```java
return new DecompressionResult(
    current,                      // segment
    current,                      // backingBuffer (same for pipeline)
    finalReleaser,                // releaser
    new AtomicBoolean(false)      // ownershipTransferred
);
```

---

### Step 4: Add Zero-Copy Constructor to KeyValueLeafPage

**File:** [`KeyValueLeafPage.java`](bundles/sirix-core/src/main/java/io/sirix/page/KeyValueLeafPage.java)

**Add fields after line 119:**

```java
/** Backing buffer from decompression (for zero-copy). Released on close(). */
private MemorySegment backingBuffer;

/** Releaser to return backing buffer to allocator. */
private Runnable backingBufferReleaser;
```

**Add new zero-copy constructor:**

```java
/**
 * Zero-copy constructor - slotMemory IS a slice of the decompression buffer.
 * The backing buffer is released when this page is closed.
 */
public KeyValueLeafPage(
    long recordPageKey,
    int revision,
    IndexType indexType,
    ResourceConfiguration resourceConfig,
    int[] slotOffsets,              // Pre-loaded from serialized data
    MemorySegment slotMemory,       // Slice of decompression buffer
    int lastSlotIndex,
    MemorySegment backingBuffer,    // Full decompression buffer
    Runnable backingBufferReleaser  // Returns buffer to allocator
) {
  this.recordPageKey = recordPageKey;
  this.revision = revision;
  this.indexType = indexType;
  this.resourceConfig = resourceConfig;
  this.recordPersister = resourceConfig.recordPersister;
  this.areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
  this.slotOffsets = slotOffsets;
  this.slotMemory = slotMemory;
  this.lastSlotIndex = lastSlotIndex;
  this.backingBuffer = backingBuffer;
  this.backingBufferReleaser = backingBufferReleaser;
  
  // Zero-copy: slotMemory is part of backingBuffer, don't release separately
  this.externallyAllocatedMemory = true;
  
  this.references = new ConcurrentHashMap<>();
  this.records = new DataRecord[Constants.NDP_NODE_COUNT];
  this.deweyIdOffsets = new int[Constants.NDP_NODE_COUNT];
  Arrays.fill(deweyIdOffsets, -1);
  this.doResizeMemorySegmentsIfNeeded = false;  // Zero-copy pages are immutable
  
  // ... diagnostic tracking if enabled ...
}
```

**Update close() method to release backing buffer:**

```java
@Override
public synchronized void close() {
  if (isClosed) {
    return;
  }
  
  // ... existing close logic ...
  
  // Release backing buffer to allocator
  if (backingBufferReleaser != null) {
    backingBufferReleaser.run();
    backingBufferReleaser = null;
    backingBuffer = null;
  }
  
  isClosed = true;
}
```

---

### Step 5: Modify PageKind Serialization (Bulk Copy)

**File:** [`PageKind.java`](bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java) lines 218-334

**Replace current serialization (lines 236-314) with:**

```java
// Write page key
Utils.putVarLong(sink, recordPageKey);
// Write revision
sink.writeInt(keyValueLeafPage.getRevision());
// Write index type
sink.writeByte(indexType.getID());
// Write last slot index
sink.writeInt(keyValueLeafPage.getLastSlotIndex());

// Write slot offsets array (int[512] = 2048 bytes) - BULK
int[] slotOffsets = keyValueLeafPage.getSlotOffsets();
for (int offset : slotOffsets) {
    sink.writeInt(offset);
}

// Write slotMemory region - BULK COPY (not byte-by-byte!)
int slotMemoryUsedSize = keyValueLeafPage.getUsedSlotsSize();
if (slotMemoryUsedSize == 0) slotMemoryUsedSize = 1;
sink.writeInt(slotMemoryUsedSize);
MemorySegment slotMem = keyValueLeafPage.getSlotMemory();

// Bulk copy slotMemory (fast path)
byte[] slotBytes = new byte[slotMemoryUsedSize];
MemorySegment.copy(slotMem, ValueLayout.JAVA_BYTE, 0, slotBytes, 0, slotMemoryUsedSize);
sink.write(slotBytes);

// Write deweyId offsets array (int[512] = 2048 bytes) if stored
if (resourceConfig.areDeweyIDsStored) {
    int[] deweyIdOffsets = keyValueLeafPage.getDeweyIdOffsets();
    for (int offset : deweyIdOffsets) {
        sink.writeInt(offset);
    }
    
    // Write deweyIdMemory region - BULK COPY
    int deweyIdMemoryUsedSize = keyValueLeafPage.getUsedDeweyIdSize();
    if (deweyIdMemoryUsedSize == 0) deweyIdMemoryUsedSize = 1;
    sink.writeInt(deweyIdMemoryUsedSize);
    MemorySegment deweyMem = keyValueLeafPage.getDeweyIdMemory();
    
    byte[] deweyBytes = new byte[deweyIdMemoryUsedSize];
    MemorySegment.copy(deweyMem, ValueLayout.JAVA_BYTE, 0, deweyBytes, 0, deweyIdMemoryUsedSize);
    sink.write(deweyBytes);
    
    sink.writeInt(keyValueLeafPage.getLastDeweyIdIndex());
}

// [bitmaps - still needed for slot presence checking...]
// [overlong refs unchanged...]
```

**Add getters to KeyValueLeafPage:**

```java
public int[] getSlotOffsets() {
    return slotOffsets;
}

public MemorySegment getSlotMemory() {
    return slotMemory;
}

public int[] getDeweyIdOffsets() {
    return deweyIdOffsets;
}

public MemorySegment getDeweyIdMemory() {
    return deweyIdMemory;
}
```

---

### Step 6: Modify PageKind Deserialization (CORE CHANGE)

**File:** [`PageKind.java`](bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java) lines 85-200

**Replace current deserialization with zero-copy version:**

```java
case V0 -> {
    final long recordPageKey = Utils.getVarLong(source);
    final int revision = source.readInt();
    final IndexType indexType = IndexType.getType(source.readByte());
    final int lastSlotIndex = source.readInt();
    
    // Read slot offsets array (2048 bytes)
    final int[] slotOffsets = new int[Constants.NDP_NODE_COUNT];
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
        slotOffsets[i] = source.readInt();
    }
    
    // ZERO-COPY: Slice decompression buffer directly as slotMemory
    final int slotMemorySize = source.readInt();
    final MemorySegment sourceSegment = ((MemorySegmentBytesIn) source).getSource();
    final MemorySegment slotMemory = sourceSegment.asSlice(source.position(), slotMemorySize);
    source.skip(slotMemorySize);
    
    // Transfer buffer ownership to page
    // NOTE: decompressionResult must be passed to this method (see Step 7)
    final Runnable releaser = decompressionResult.transferOwnership();
    
    var page = new KeyValueLeafPage(
        recordPageKey,
        revision,
        indexType,
        resourceConfig,
        slotOffsets,
        slotMemory,
        lastSlotIndex,
        decompressionResult.backingBuffer(),
        releaser
    );
    
    // [deweyId deserialization unchanged...]
    // [bitmap reading still needed for getSlot() to work...]
    // [overlong refs unchanged...]
    
    return page;
}
```

**Key insight:** No more loop with `setSlotDirect()`. The slotMemory IS the buffer slice!

---

### Step 7: Update AbstractReader to Pass DecompressionResult

**File:** [`AbstractReader.java`](bundles/sirix-core/src/main/java/io/sirix/io/AbstractReader.java) lines 80-99

**Current:**

```java
public Page deserializeFromSegment(ResourceConfiguration resourceConfiguration, MemorySegment compressedPage) {
    try (var decompressionResult = byteHandler.decompressScoped(compressedPage)) {
        Page deserializedPage = pagePersister.deserializePage(resourceConfiguration, 
            new MemorySegmentBytesIn(decompressionResult.segment()), type);
        // ...
        return deserializedPage;
    }
}
```

**New:**

```java
public Page deserializeFromSegment(ResourceConfiguration resourceConfiguration, MemorySegment compressedPage) {
    var decompressionResult = byteHandler.decompressScoped(compressedPage);
    
    try {
        Page deserializedPage = pagePersister.deserializePage(
            resourceConfiguration, 
            new MemorySegmentBytesIn(decompressionResult.segment()), 
            type,
            decompressionResult  // Pass for ownership transfer
        );
        
        if (resourceConfiguration != null) {
            PageUtils.fixupPageReferenceIds(deserializedPage, 
                resourceConfiguration.getDatabaseId(), resourceConfiguration.getID());
        }
        
        return deserializedPage;
    } finally {
        // If page didn't take ownership (non-KVLP), release now
        if (!decompressionResult.ownershipTransferred().get()) {
            decompressionResult.close();
        }
    }
}
```

---

### Step 8: Update PagePersister Interface

**File:** [`PagePersister.java`](bundles/sirix-core/src/main/java/io/sirix/page/PagePersister.java)

**Add overloaded method:**

```java
Page deserializePage(
    ResourceConfiguration resourceConfiguration, 
    BytesIn<?> source, 
    SerializationType type,
    ByteHandler.DecompressionResult decompressionResult  // For ownership transfer
);
```

---

## Memory Lifecycle Diagram

```
BEFORE (with copy):
┌─────────────────────┐    copy     ┌─────────────────────┐
│ Decompression       │ ─────────>  │ slotMemory          │
│ Buffer (Pool)       │             │ (Allocator)         │
└─────────────────────┘             └─────────────────────┘
         │                                   │
         v                                   v
  returned to pool               released on page.close()
  immediately

AFTER (zero-copy):
┌─────────────────────────────────────────┐
│ Decompression Buffer (Allocator)        │
│  ┌─────────────────────────────────┐    │
│  │ slotMemory = buffer.asSlice()   │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘
                    │
                    v
         released on page.close()
         (ownership transferred)
```

---

## Performance Impact

| Metric | Before | After |
|--------|--------|-------|
| `MemorySegment.copy()` calls | N per page (one per slot) | 0 |
| Memory allocations | 2 (decompress + slotMemory) | 1 (decompress only) |
| Per-slot loop iterations | N | 0 (bulk read offsets array) |
| Format overhead | 0 | +2KB (offsets array) |

**Trade-off:** 2KB larger pages, but zero copy operations during read.

---

## Verification

1. **Delete test databases** - format changed, old data incompatible
2. **Run JsonShredderTest** - exercises full serialization/deserialization
3. **Run VersioningTest** - tests page combining (zero-copy fragments -> fresh combined page)
4. **JFR Profile** - confirm `MemorySegment.copy()` eliminated from hot path
5. **Memory leak check** - verify LinuxMemorySegmentAllocator shows 0 leaks

---

## Implementation Todos

- [ ] Extend DecompressionResult with backingBuffer, ownershipTransferred, and transferOwnership() method
- [ ] Update FFILz4Compressor to use unified allocator and new DecompressionResult format
- [ ] Update ByteHandlerPipeline to construct new DecompressionResult format
- [ ] Add backingBuffer and backingBufferReleaser fields to KeyValueLeafPage
- [ ] Add zero-copy constructor to KeyValueLeafPage
- [ ] Update KeyValueLeafPage.close() to release backing buffer
- [ ] Add getSlotOffsets(), getSlotMemory(), getDeweyIdOffsets(), getDeweyIdMemory() getters
- [ ] Modify PageKind.serializePage() to write offsets array + raw slotMemory (bulk copy)
- [ ] Modify PageKind.deserializePage() to slice buffer as slotMemory (zero-copy)
- [ ] Add overloaded deserializePage() with DecompressionResult parameter to PagePersister
- [ ] Update AbstractReader.deserializeFromSegment() to pass DecompressionResult
- [ ] Delete test data and run JsonShredderTest + VersioningTest








