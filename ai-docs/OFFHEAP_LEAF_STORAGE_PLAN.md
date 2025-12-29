# Off-Heap Leaf Storage via MemorySegment

> **Note**: This is a standalone summary of Appendix B from `CACHE_FRIENDLY_INDEX_PLAN.md`.  
> This enhancement can be combined with any of the three index options (ART, B+Tree, or Sorted Arrays).

## Goal

Eliminate on-heap `byte[]` allocations for leaf node payloads by storing them in off-heap `MemorySegment` buffers, leveraging the existing `LinuxMemorySegmentAllocator` infrastructure. This enables zero-copy deserialization and reduces GC pressure by 60-80%.

## Motivation

| Problem | Impact |
|---------|--------|
| Per-entry `byte[]` allocations during deserialization | High GC pressure, 500-1000 allocations per page |
| On-heap leaf storage | Competes with application heap, G1 marking overhead |
| Copy-heavy hydration path | CPU cycles wasted copying decompressed bytes |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       OFF-HEAP LEAF STORAGE MODEL                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ALLOCATOR LAYER                    LEAF PAGE LAYER                        │
│   ┌─────────────────────────┐       ┌─────────────────────────────────────┐ │
│   │ LinuxMemorySegmentAlloc │       │  OffHeapLeafPage                    │ │
│   │ ├── 4KB size class      │──────>│  ├── MemorySegment buffer (off-heap)│ │
│   │ ├── 8KB size class      │       │  ├── Releaser (returns to pool)    │ │
│   │ ├── 16KB size class     │       │  ├── int entryCount (on-heap)      │ │
│   │ ├── 32KB size class     │       │  ├── int usedBytes (on-heap)       │ │
│   │ ├── 64KB size class  ◄──┘       │  └── guard count (on-heap)         │ │
│   │ ├── 128KB size class    │       └─────────────────────────────────────┘ │
│   │ └── 256KB size class    │                                               │
│   └─────────────────────────┘                                               │
│                                                                             │
│   ON CLOSE: releaser.run() → segment returns to allocator pool             │
│   ON COW:   allocate new segment → copy active region → mutate             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Off-Heap Leaf Node Layout

Each leaf node gets a fixed-size segment from the allocator's size class. Packed layout (little-endian):

```
Offset 0:
┌────────────────────────────────────────────────────────────────────────────┐
│ Header (24 bytes, fixed)                                                   │
├────────────────────────────────────────────────────────────────────────────┤
│ [u8  nodeKind=LEAF]                     - Node type discriminator          │
│ [u8  flags]                             - Dirty, overflow, etc.            │
│ [u16 entryCount]                        - Number of key-value pairs        │
│ [u16 prefixLen]                         - Path compression prefix length   │
│ [u16 reserved]                          - Alignment padding                │
│ [u32 nextLeafKey]                       - Sibling link for range scans     │
│ [u32 prevLeafKey]                       - Reverse sibling link             │
│ [u32 keyAreaEnd]                        - Keys grow from header downward   │
│ [u32 valAreaStart]                      - Values grow from end upward      │
├────────────────────────────────────────────────────────────────────────────┤
│ Entry Index (4 bytes × entryCount, sorted by key)                          │
├────────────────────────────────────────────────────────────────────────────┤
│ [u32 entryOffset[0]]  → points to key record 0                             │
│ [u32 entryOffset[1]]  → points to key record 1                             │
│ ...                                                                        │
│ [u32 entryOffset[n-1]] → points to key record n-1                          │
├────────────────────────────────────────────────────────────────────────────┤
│ Key Area (grows downward from header)                                      │
├────────────────────────────────────────────────────────────────────────────┤
│ Key Record Layout:                                                         │
│   [u16 keyLen][key bytes...][u32 valueOffset]                              │
│                                         │                                  │
│                                         ▼                                  │
├────────────────────────────────────────────────────────────────────────────┤
│ Value Area (grows upward from segment end)                                 │
├────────────────────────────────────────────────────────────────────────────┤
│ Value Record Layout:                                                       │
│   [u16 valueLen][value bytes...]                                           │
│   (e.g., NodeReferences serialized bytes)                                  │
└────────────────────────────────────────────────────────────────────────────┘
```

**Design rationale:**
- Entry index enables binary search (O(log n) within page)
- Keys and values in separate areas → better cache locality for key-only scans
- Grows from opposite ends → simple compaction, no fragmentation until full
- 4-byte alignment for all offsets → matches `MemorySegment.get(ValueLayout.JAVA_INT, ...)`

## Java Implementation

```java
package io.sirix.index.offheap;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.Runnable;

/**
 * Off-heap leaf page for secondary indexes.
 * Stores key-value pairs in a MemorySegment allocated from LinuxMemorySegmentAllocator.
 */
public final class OffHeapLeafPage implements AutoCloseable {
    
    // Header layout constants
    private static final long OFFSET_NODE_KIND = 0;
    private static final long OFFSET_FLAGS = 1;
    private static final long OFFSET_ENTRY_COUNT = 2;
    private static final long OFFSET_PREFIX_LEN = 4;
    private static final long OFFSET_NEXT_LEAF = 8;
    private static final long OFFSET_PREV_LEAF = 12;
    private static final long OFFSET_KEY_AREA_END = 16;
    private static final long OFFSET_VAL_AREA_START = 20;
    private static final int HEADER_SIZE = 24;
    
    private static final int ENTRY_INDEX_ENTRY_SIZE = 4; // u32 offset per entry
    
    private final MemorySegment segment;
    private final Runnable releaser;
    private final int capacity;
    
    private volatile boolean closed = false;
    private final java.util.concurrent.atomic.AtomicInteger guardCount = 
        new java.util.concurrent.atomic.AtomicInteger(0);
    
    public OffHeapLeafPage(MemorySegment segment, Runnable releaser) {
        if (segment == null || !segment.isNative()) {
            throw new IllegalArgumentException("Segment must be native (off-heap)");
        }
        this.segment = segment;
        this.releaser = releaser;
        this.capacity = (int) segment.byteSize();
    }
    
    public void initEmpty(int nodeKind, long nextLeafKey, long prevLeafKey) {
        segment.set(ValueLayout.JAVA_BYTE, OFFSET_NODE_KIND, (byte) nodeKind);
        segment.set(ValueLayout.JAVA_BYTE, OFFSET_FLAGS, (byte) 0);
        segment.set(ValueLayout.JAVA_SHORT, OFFSET_ENTRY_COUNT, (short) 0);
        segment.set(ValueLayout.JAVA_SHORT, OFFSET_PREFIX_LEN, (short) 0);
        segment.set(ValueLayout.JAVA_INT, OFFSET_NEXT_LEAF, (int) nextLeafKey);
        segment.set(ValueLayout.JAVA_INT, OFFSET_PREV_LEAF, (int) prevLeafKey);
        segment.set(ValueLayout.JAVA_INT, OFFSET_KEY_AREA_END, HEADER_SIZE);
        segment.set(ValueLayout.JAVA_INT, OFFSET_VAL_AREA_START, capacity);
    }
    
    public int getEntryCount() {
        return Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT, OFFSET_ENTRY_COUNT));
    }
    
    public MemorySegment getKeySlice(int entryIndex) {
        int entryOffset = getEntryOffset(entryIndex);
        int keyLen = Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT, entryOffset));
        return segment.asSlice(entryOffset + 2, keyLen);
    }
    
    public MemorySegment getValueSlice(int entryIndex) {
        int entryOffset = getEntryOffset(entryIndex);
        int keyLen = Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT, entryOffset));
        int valueOffset = segment.get(ValueLayout.JAVA_INT, entryOffset + 2 + keyLen);
        int valueLen = Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT, valueOffset));
        return segment.asSlice(valueOffset + 2, valueLen);
    }
    
    private int getEntryOffset(int entryIndex) {
        long indexOffset = HEADER_SIZE + (long) entryIndex * ENTRY_INDEX_ENTRY_SIZE;
        return segment.get(ValueLayout.JAVA_INT, indexOffset);
    }
    
    public boolean insert(MemorySegment key, MemorySegment value) {
        // ... (see full implementation in CACHE_FRIENDLY_INDEX_PLAN.md Appendix B)
        return false;
    }
    
    public int acquireGuard() { return guardCount.incrementAndGet(); }
    public int releaseGuard() { return guardCount.decrementAndGet(); }
    public int getGuardCount() { return guardCount.get(); }
    public boolean isClosed() { return closed; }
    public MemorySegment getSegment() { return segment; }
    
    public int getUsedBytes() {
        int keyAreaEnd = segment.get(ValueLayout.JAVA_INT, OFFSET_KEY_AREA_END);
        int valAreaStart = segment.get(ValueLayout.JAVA_INT, OFFSET_VAL_AREA_START);
        return keyAreaEnd + (capacity - valAreaStart);
    }
    
    @Override
    public void close() {
        if (closed) return;
        if (guardCount.get() != 0) {
            throw new IllegalStateException("Cannot close with active guards");
        }
        closed = true;
        if (releaser != null) releaser.run();
    }
}
```

## Serialization & Zero-Copy Hydration

```java
public class OffHeapLeafPageSerializer {
    
    public void serialize(BytesOut sink, OffHeapLeafPage page) {
        int usedBytes = page.getUsedBytes();
        sink.writeInt(usedBytes);
        // Write raw segment bytes (header + key area + value area)
        // ... bulk copy from segment
    }
    
    public OffHeapLeafPage deserializeZeroCopy(
        MemorySegment decompressedSource,
        long offset,
        LinuxMemorySegmentAllocator allocator
    ) {
        int usedBytes = decompressedSource.get(ValueLayout.JAVA_INT, offset);
        var allocation = allocator.allocate(usedBytes);
        MemorySegment.copy(decompressedSource, offset + 4, allocation.segment(), 0, usedBytes);
        return new OffHeapLeafPage(allocation.segment(), allocation.releaser());
    }
}
```

## COW (Copy-on-Write) Handling

```java
public OffHeapLeafPage copyOnWrite(OffHeapLeafPage original, LinuxMemorySegmentAllocator allocator) {
    int usedBytes = original.getUsedBytes();
    var allocation = allocator.allocate(usedBytes);
    MemorySegment newSeg = allocation.segment();
    MemorySegment origSeg = original.getSegment();
    
    // Copy active regions only
    int keyAreaEnd = origSeg.get(ValueLayout.JAVA_INT, OFFSET_KEY_AREA_END);
    int valAreaStart = origSeg.get(ValueLayout.JAVA_INT, OFFSET_VAL_AREA_START);
    
    MemorySegment.copy(origSeg, 0, newSeg, 0, keyAreaEnd);
    MemorySegment.copy(origSeg, valAreaStart, newSeg, valAreaStart, capacity - valAreaStart);
    
    return new OffHeapLeafPage(newSeg, allocation.releaser());
}
```

## Integration with Index Options

| Option | Off-Heap Integration |
|--------|---------------------|
| **A: ART** | ART leaf nodes store values in `OffHeapLeafPage` segments |
| **B: B+Tree** | `BPlusLeafNode` stores key-value arrays in a single `OffHeapLeafPage` |
| **C: Sorted Arrays** | Each `SortedArrayIndex` page backed by an `OffHeapLeafPage` segment |

## Migration Steps

1. Add `OffHeapLeafPage` class with layout and accessors
2. Wire `LinuxMemorySegmentAllocator` in leaf factory
3. Update index reader/writer to operate on `MemorySegment`-backed leaves
4. Adjust serialization to stream raw segment bytes
5. Update caches to weight by `segment.byteSize()`, ensure guards manage lifetime
6. Tests:
   - Unit: insert/update/delete, split/merge, range scan
   - Persistence: serialize→deserialize→validate byte equality
   - Stress: allocator exhaustion, COW churn, parallel reads

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Use-after-free | Guard counts gate `releaser.run()`; assertions in `close()` |
| Fragmentation | Rely on allocator size classes; compact only on leaf rebuild |
| Alignment errors | Centralize offset math helpers; fuzz test with random lengths |
| GC visibility | Keep on-heap metadata minimal (counts/offsets only) |

## Expected Impact

| Metric | Before (on-heap) | After (off-heap) |
|--------|------------------|------------------|
| `byte[]` per leaf | 1 per entry | **0** |
| Hydration copies | Per-entry copy | **Bulk/zero-copy** |
| GC young gen pressure | High | **-60% to -80%** |
| Cache locality | Poor (scattered) | **Excellent (contiguous)** |

## Success Criteria

- No on-heap `byte[]` for leaf storage in steady state
- Hydration path is zero-copy from decompressed `MemorySegment`
- Benchmarks show lower allocation rate and stable throughput vs. on-heap baseline

## Implementation Effort

**1-2 weeks** (can be done in parallel with ART or B+Tree implementation)

## See Also

- `ai-docs/CACHE_FRIENDLY_INDEX_PLAN.md` - Full plan with all options
- `LinuxMemorySegmentAllocator` - Existing allocator infrastructure
- `KeyValueLeafPage` - Reference implementation for guard/lifecycle patterns

