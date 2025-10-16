# How to Remove Diagnostic Logging

Once the memory leak is confirmed fixed, you can remove the diagnostic logging overhead.

## Files with Diagnostic Logging

1. `KeyValueLeafPage.returnSegmentsToAllocator()` - DiagnosticLogger calls
2. `KeyValueLeafPagePool.returnPage()` - DiagnosticLogger calls
3. `KeyValueLeafPagePool.borrowPage()` - DiagnosticLogger calls (both methods)
4. `LinuxMemorySegmentAllocator.allocate()` - DiagnosticLogger calls
5. `LinuxMemorySegmentAllocator.release()` - DiagnosticLogger calls
6. `KeyValueLeafPage.resizeMemorySegment()` - DiagnosticLogger calls
7. `TransactionIntentLog.clear()` - DiagnosticLogger calls
8. `AbstractNodeTrxImpl.intermediateCommitIfRequired()` - DiagnosticLogger calls
9. `NodePageReadOnlyTrx.unpinPageFragments()` - DiagnosticLogger calls
10. `PageCache` removal listener - DiagnosticLogger calls
11. `RecordPageCache` removal listener - DiagnosticLogger calls

## Quick Removal

Search and remove all lines containing:
```
DiagnosticLogger.log(
DiagnosticLogger.error(
DiagnosticLogger.separator(
```

## Files to Delete

- `src/main/java/io/sirix/cache/DiagnosticLogger.java`
- `src/test/java/io/sirix/cache/DiagnosticHelper.java`
- `analyze-memory-leak.sh`
- `DIAGNOSTIC_INSTRUCTIONS.md`
- `RUN_DIAGNOSTIC_TEST.md`

## Keep These Utility Methods

In `LinuxMemorySegmentAllocator`:
- `getPoolSizes()` - useful for monitoring
- `printPoolDiagnostics()` - useful for debugging

In `KeyValueLeafPagePool`:
- `getDetailedStatistics()` - already existed, keep it

## Command to Remove Logging

```bash
# Remove DiagnosticLogger calls
find bundles/sirix-core/src -name "*.java" -type f -exec sed -i '/DiagnosticLogger\./d' {} \;

# Recompile
./gradlew :sirix-core:compileJava
```

## What to Keep

The actual FIXES should remain:
- ✅ `.removalListener()` instead of `.evictionListener()` in both caches
- ✅ PinCount checks in removal listeners
- ✅ Fragment cleanup in write transactions
- ✅ try-finally in loadDataPageFromDurableStorageAndCombinePageFragments()
- ✅ No Arena interference with mmap/munmap

These are the real fixes - the logging is just for diagnosis.






