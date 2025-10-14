# Arena Memory Leak Fix

## Problem

The `LinuxMemorySegmentAllocator` was experiencing a memory leak because memory segments created from raw `mmap` system calls were not associated with any Java `Arena`. Without an arena, the memory segments lacked proper lifecycle management, causing them to leak even when `munmap` was called.

## Root Cause

In the original `mapMemory()` method (line 157-171), the code was:

```java
public MemorySegment mapMemory(long totalSize) {
    MemorySegment addr;
    try {
        addr = (MemorySegment) MMAP.invoke(MemorySegment.NULL,
                                           totalSize,
                                           PROT_READ | PROT_WRITE,
                                           MAP_PRIVATE | MAP_ANONYMOUS,
                                           -1,
                                           0);
    } catch (Throwable e) {
        throw new RuntimeException("Failed to allocate memory via mmap", e);
    }
    if (addr == MemorySegment.NULL) {
        throw new OutOfMemoryError("Failed to allocate memory via mmap");
    }
    return addr.reinterpret(totalSize);  // ❌ NO ARENA ASSOCIATED
}
```

The `addr.reinterpret(totalSize)` call creates a memory segment, but it has no associated Arena. This means:

1. The JVM doesn't properly track the segment's lifecycle
2. The segment isn't eligible for proper cleanup even when `munmap` is called
3. Memory leaks accumulate over time

## Solution

The fix involves three key changes:

### 1. Import Arena
```java
import java.lang.foreign.Arena;
```

### 2. Track Arenas
Added a list to track all arenas created:
```java
private final List<Arena> arenas = new CopyOnWriteArrayList<>();
```

### 3. Associate Memory Segments with Arenas

Modified `mapMemory()` to create an `Arena.ofShared()` for each mapped segment:

```java
public MemorySegment mapMemory(long totalSize) {
    MemorySegment addr;
    try {
        addr = (MemorySegment) MMAP.invoke(MemorySegment.NULL,
                                           totalSize,
                                           PROT_READ | PROT_WRITE,
                                           MAP_PRIVATE | MAP_ANONYMOUS,
                                           -1,
                                           0);
    } catch (Throwable e) {
        throw new RuntimeException("Failed to allocate memory via mmap", e);
    }
    if (addr == MemorySegment.NULL) {
        throw new OutOfMemoryError("Failed to allocate memory via mmap");
    }
    
    // ✅ Create an Arena for this segment to enable proper lifecycle management
    Arena arena = Arena.ofShared();
    arenas.add(arena);
    
    // ✅ Associate the native address with the arena
    return MemorySegment.ofAddress(addr.address()).reinterpret(totalSize, arena, null);
}
```

### 4. Close Arenas During Cleanup

Updated the `free()` method to close all arenas:

```java
@Override
public void free() {
    if (!isInitialized.get()) {
        LOGGER.debug("Allocator is not initialized, nothing to free.");
        return;
    }

    LOGGER.info("Cleaning up mmap'd memory segments...");
    
    // First, release the native memory via munmap
    for (MemorySegment segment : topLevelMappedSegments) {
        try {
            releaseMemory(segment, segment.byteSize());
        } catch (Exception e) {
            LOGGER.error("Failed to release segment: {}", e.getMessage());
        }
    }
    topLevelMappedSegments.clear();

    // Clear all pools
    for (Deque<MemorySegment> pool : segmentPools) {
        pool.clear();
    }

    // ✅ Close all arenas to release associated resources
    LOGGER.info("Closing {} arenas...", arenas.size());
    for (Arena arena : arenas) {
        try {
            arena.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close arena: {}", e.getMessage());
        }
    }
    arenas.clear();

    isInitialized.set(false);
    LOGGER.info("LinuxMemorySegmentAllocator cleanup complete.");
}
```

## Why Arena.ofShared()?

`Arena.ofShared()` is used because:

1. **Thread-safe**: The memory segments are accessed from multiple threads (via the concurrent segment pools)
2. **Explicit lifecycle**: We control when to close the arena (during `free()`)
3. **Proper resource tracking**: The JVM can properly track and manage the memory segments associated with the arena

## Key Points

- Each top-level memory segment from `mmap` now has its own `Arena.ofShared()`
- All sliced segments inherit the arena from their parent segment
- Arenas are closed during cleanup, ensuring all resources are properly released
- The order of cleanup is important:
  1. Call `munmap` to release native memory
  2. Clear segment pools
  3. Close arenas to release Java-side resources

## Expected Behavior

With this fix:
- Memory segments are properly tracked by the JVM
- No memory leaks should occur
- Proper cleanup happens both during explicit `free()` calls and JVM shutdown
- The allocator can be reinitialized after being freed

## Testing

To verify the fix:
1. Run your application with memory profiling enabled
2. Monitor native memory usage over time
3. Verify that memory is released when databases/transactions are closed
4. Check that the shutdown hook properly releases all resources



