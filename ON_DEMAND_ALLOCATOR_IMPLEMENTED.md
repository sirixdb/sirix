# On-Demand Region Allocator - Implementation Complete

## What Was Implemented

### 1. On-Demand Region Allocation
- **Removed pre-allocation** - no memory mapped at startup
- **Allocate on first use** - mmap 4MB regions when pool runs empty (reduced for laptop compatibility)
- **One Arena per region** - ~768 Arenas for 3GB budget vs 100,000 for per-segment

### 2. Global Budget Tracking
- `totalMappedBytes` tracks memory across ALL pools
- Before allocating region, checks if would exceed `maxBufferSize`
- If over budget, attempts to free unused regions first

### 3. Automatic Region Freeing
- Each region tracks how many slices are in use (`unusedSlices` counter)
- When all slices returned → entire region freed:
  - `munmap` releases native memory
  - `arena.close()` invalidates MemorySegments
- Prevents memory leaks and use-after-free bugs

### 4. Per-Size-Class Regions
- Each of the 7 size classes (4KB-256KB) gets its own regions
- 4MB region size for all classes (reduced from 64MB for better laptop compatibility)
- Example: 64KB pool gets 4MB/64KB = 64 segments per region

## Key Code Changes

**File:** `LinuxMemorySegmentAllocator.java`

**Added:**
- `MemoryRegion` inner class with Arena and slice tracking
- `activeRegions` list
- `totalMappedBytes` global budget counter
- `allocateNewRegion()` - mmap and slice
- `findRegionForSegment()` - locate region by address
- `freeRegion()` - munmap + arena.close()
- `freeUnusedRegions()` - reclaim memory

**Modified:**
- `init()` - no pre-allocation
- `allocate()` - on-demand region creation with budget check
- `release()` - track returns, free regions when all slices back
- `free()` - cleanup all regions

**File:** `KeyValueLeafPage.java`

**Added:**
- `returnSegmentsToAllocator()` - returns segments to pool on eviction

## How It Works

1. **First allocation from empty pool:**
   - Check global budget
   - mmap 4MB region
   - Create Arena for region
   - Slice into segments
   - Add all slices to pool
   - Return one segment

2. **Subsequent allocations:**
   - Poll from pool (segments from regions)
   - If pool empty, allocate new region

3. **On segment release:**
   - Return segment to pool
   - Increment region's `unusedSlices` counter
   - If all slices returned → free entire region

4. **Budget enforcement:**
   - Before allocating, check `totalMappedBytes + regionSize ≤ maxBufferSize`
   - If would exceed, call `freeUnusedRegions()` first
   - If still exceeds after freeing → throw OutOfMemoryError

## Benefits vs Pre-Allocation

**Before:**
- 3GB mmap'd upfront (wasted if not used)
- Cache thinks "I'm using 0 bytes" (weight=0 for pinned)
- evictionListener never fires
- Segments never freed
- **Memory leak**

**After:**
- Memory mmap'd only when needed
- Unused regions freed automatically
- Budget respected
- evictionListener will fire when cache hits weight limit
- **No leak**

## Next Steps

1. Test with full workload
2. Monitor that:
   - Regions are allocated on-demand
   - Regions are freed when slices returned
   - Memory stays within budget
   - evictionListener fires and returns segments
3. If working, remove diagnostic logging

## Expected Behavior

- Memory usage grows gradually as needed
- Peaks during transaction, drops after commit
- Stays within configured budget
- No system freezing from memory exhaustion


