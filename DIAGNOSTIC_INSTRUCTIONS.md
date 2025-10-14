# Memory Leak Diagnostic Instructions

## Overview

Comprehensive diagnostic logging has been added to track memory segment allocation and release throughout the system.

## What Was Added

### 1. Diagnostic Logging in Key Classes

All allocation and release operations now print `[DIAGNOSTIC]` messages:

- **KeyValueLeafPage.returnSegmentsToAllocator()** - Tracks when page segments are returned
- **KeyValueLeafPagePool.returnPage()** - Tracks when pages are returned to the pool
- **LinuxMemorySegmentAllocator.allocate()** - Tracks every segment allocation
- **LinuxMemorySegmentAllocator.release()** - Tracks every segment release

### 2. DiagnosticHelper Utility

A new test utility class at `src/test/java/io/sirix/cache/DiagnosticHelper.java` provides:
- `printPoolStatistics()` - Shows page borrowing/returning stats
- `printMemorySegmentPools()` - Shows current segment pool sizes
- `printAllocatorDiagnostics()` - Detailed allocator state

### 3. Additional Methods

- `LinuxMemorySegmentAllocator.getPoolSizes()` - Returns array of current pool sizes
- `LinuxMemorySegmentAllocator.printPoolDiagnostics()` - Prints pool state

## How to Use

### In Your Tests

Add diagnostic output before and after your test:

```java
@Test
public void testYourOperation() {
    // Print initial state
    DiagnosticHelper.printPoolStatistics();
    DiagnosticHelper.printMemorySegmentPools();
    
    // Run your test
    // ... your test code ...
    
    // Print final state
    DiagnosticHelper.printPoolStatistics();
    DiagnosticHelper.printMemorySegmentPools();
}
```

### What to Look For

1. **Page Leak Detection**
   - Compare "Total borrowed pages" vs "Total returned pages"
   - If borrowed > returned, pages are leaking

2. **Segment Pool Shrinkage**
   - Compare pool sizes before and after test
   - If pools shrink and don't recover, segments are leaking

3. **[DIAGNOSTIC] Messages**
   - Look for allocate() calls without corresponding release() calls
   - Look for returnPage() calls that DON'T result in release() calls
   - Look for pages where `already returned: true` but segments weren't released

4. **Common Patterns**
   - **Fragment not returned**: Page loaded but `returnSegmentsToAllocator()` never called
   - **Already returned**: Duplicate return attempt (idempotency working)
   - **Exception in release**: Error preventing segment return

## Example Output

```
========== POOL STATISTICS ==========
KeyValueLeafPagePool:
  Total borrowed pages: 1500
  Total returned pages: 1498
  Difference (leaked):  2 ⚠️  LEAK!

LinuxMemorySegmentAllocator:
  Max buffer size: 1073741824 bytes
=====================================

========== MEMORY SEGMENT POOLS ==========
Pool 0 (4096 bytes): 98304 segments available
Pool 1 (8192 bytes): 49152 segments available
Pool 2 (16384 bytes): 24576 segments available
Pool 3 (32768 bytes): 12288 segments available
Pool 4 (65536 bytes): 6144 segments available    ← Compare before/after
Pool 5 (131072 bytes): 3072 segments available
Pool 6 (262144 bytes): 1536 segments available
=========================================
```

## Finding the Leak

1. **Run a simple test** with diagnostic output
2. **Count allocations vs releases** in the [DIAGNOSTIC] output
3. **Track specific pages** that are borrowed but never returned
4. **Look for exception patterns** that prevent cleanup
5. **Check fragment handling** in write transactions

## Removing Diagnostic Logging

Once the leak is identified, you can:
1. Remove `System.out.println("[DIAGNOSTIC] ...")` statements
2. Keep the DiagnosticHelper for future debugging
3. Keep the new utility methods (getPoolSizes, printPoolDiagnostics)

## Current Hypotheses Being Tested

1. **Fragment pages in write transactions** - Loaded but not returned
2. **Exception during combine** - try-finally should prevent this
3. **TransactionIntentLog cleanup** - Pages not returned on clear/close
4. **Cache removal listener** - Pinned pages not being cleaned up later



