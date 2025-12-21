# Sirix Memory Leak Diagnostic Usage Guide

## Available Debug Flags

Three system properties control diagnostic output:

### 1. Memory Leak Diagnostics
```bash
-Dsirix.debug.memory.leaks=true
```

**Enables:**
- Page creation/closure tracking in KeyValueLeafPage
- Counters by IndexType (NAME, DOCUMENT, PATH_SUMMARY, etc.)
- Live pages tracking (ALL_LIVE_PAGES set)
- Finalizer detection

**Output:** Silent unless combined with test diagnostics

### 2. Test Leak Diagnostics  
```bash
-Dsirix.debug.leak.diagnostics=true
```

**Enables in ConcurrentAxisTest:**
- Page statistics after each test
- Created/Closed/Leaked counts by type
- Live pages analysis (in cache vs swizzled)
- Per-page details (pinCount, closed status)
- Accounting analysis

**Output:** Printed to stderr after each test

### 3. PATH_SUMMARY Diagnostics
```bash
-Dsirix.debug.path.summary=true
```

**Enables:**
- PATH_SUMMARY page loading (bypass vs normal cache)
- PathSummaryReader initialization tracking
- pathNodeMapping population logging
- moveTo() success/failure logging

**Output:** Detailed trace of PATH_SUMMARY operations

## Usage Examples

### Investigate General Memory Leaks

```bash
./gradlew :sirix-core:test --tests "ConcurrentAxisTest" \
    -Dsirix.debug.memory.leaks=true \
    -Dsirix.debug.leak.diagnostics=true
```

**Output shows:**
```
========== PAGE STATISTICS ==========
PAGES_CREATED: 1329
PAGES_CLOSED: 1224  
UNCLOSED LEAK: 105

Pages by type (CREATED / CLOSED / LEAKED):
  NAME: 44 / 20 / 24
  PATH_SUMMARY: 8 / 4 / 4
  DOCUMENT: 1274 / 1197 / 77
  ...

=== LEAKED PAGE ANALYSIS ===
Live pages still in memory: 75
  In RecordPageCache: 45
  In RecordPageFragmentCache: 12
  Only swizzled (not in cache): 18
  ...
```

### Investigate PATH_SUMMARY Bypass Issue

```bash
./gradlew :sirix-core:test --tests "VersioningTest.testFull1" \
    -Dsirix.debug.path.summary=true \
    2>&1 | grep PATH_SUMMARY | tee path-summary-trace.log
```

**Output shows:**
```
[PATH_SUMMARY-INIT] Initializing pathNodeMapping: hasTrxIntentLog=true, revision=1
[PATH_SUMMARY-INIT]   -> Loaded 15 nodes into pathNodeMapping, arraySize=16
[PATH_SUMMARY-BYPASS] Loading page for write trx: recordPageKey=0, revision=1, trxId=16
[PATH_SUMMARY-BYPASS]   -> Combined page: pageKey=0, pinCount=0, revision=1, closed=false
[PATH_SUMMARY-MOVETO] Found in pathNodeMapping: nodeKey=5
[PATH_SUMMARY-MOVETO] Found in pathNodeMapping: nodeKey=3
[PATH_SUMMARY-MOVETO] NOT in pathNodeMapping: nodeKey=3073, arrayLength=16, init=false
...
```

### Test With and Without Bypass

**Test WITH bypass (current code):**
```bash
# Should pass
./gradlew :sirix-core:test --tests "VersioningTest.testFull1" \
    -Dsirix.debug.path.summary=true
```

**Test WITHOUT bypass:**
1. Edit `NodePageReadOnlyTrx.java` line 455:
   ```java
   // Change this line:
   if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
   
   // To remove PATH_SUMMARY check (test without bypass):
   if (true) {  // Always use normal caching
   ```

2. Run test:
   ```bash
   ./gradlew :sirix-core:cleanTest :sirix-core:test --tests "VersioningTest.testFull1" \
       -Dsirix.debug.path.summary=true \
       2>&1 | grep PATH_SUMMARY | tee path-summary-no-bypass.log
   ```

3. Compare outputs to see what's different

### Comprehensive Memory Leak Analysis

**Enable all diagnostics:**
```bash
./gradlew :sirix-core:test --tests "ConcurrentAxisTest.testConcurrent" \
    -Dsirix.debug.memory.leaks=true \
    -Dsirix.debug.leak.diagnostics=true \
    -Dsirix.debug.path.summary=true \
    2>&1 | tee full-diagnostic.log
```

**Analyze:**
```bash
# General leak stats
grep "PAGE STATISTICS" full-diagnostic.log -A 15

# PATH_SUMMARY operations
grep "PATH_SUMMARY" full-diagnostic.log > path-summary-operations.log

# Page lifecycle
grep -E "BYPASS|REPLACE|MOVETO" full-diagnostic.log
```

## What to Look For

### Normal Operation

**With bypass enabled (working):**
- PathSummaryReader initialization loads N nodes
- PATH_SUMMARY pages accessed via BYPASS path
- Combined pages created for each access
- moveTo() finds nodes in pathNodeMapping
- All tests pass

**Leak characteristics:**
- PATH_SUMMARY: ~50% leak rate (but only 4-8 pages)
- Pages unpinned (pinCount=0)
- Pages swizzled, not cached

### When Bypass is Removed

**Expected failure pattern:**
- PathSummaryReader initialization loads N nodes
- PATH_SUMMARY pages accessed via NORMAL cache path
- Pages go into RecordPageCache
- Pages get evicted (under memory pressure)
- Later: moveTo(nodeKey) → NOT in pathNodeMapping
- Test fails: "Failed to move to nodeKey: XXXX"

**What diagnostics will show:**
- [PATH_SUMMARY-INIT] Loaded X nodes
- [PATH_SUMMARY-NORMAL] Using normal cache path
- [PATH_SUMMARY-MOVETO] NOT in pathNodeMapping: nodeKey=3073
- Error: Failed to move to nodeKey: 3073

## Investigation Questions

With these diagnostics, we can answer:

1. **Are all nodes loaded into pathNodeMapping initially?**
   - Check INIT message for nodes loaded count
   - Compare with maxNodeKey

2. **When does pathNodeMapping become invalid?**
   - Check MOVETO messages for first "NOT in pathNodeMapping"
   - Correlate with page eviction events

3. **Does bypass keep pages alive longer?**
   - Count BYPASS messages
   - Check if same page is reused vs new pages created

4. **What's different between bypass and normal caching?**
   - Compare page keys, pinCounts, revisions
   - Look for page replacement patterns

## Debug Workflow

1. **Run with bypass (baseline):**
   ```bash
   ./gradlew test --tests "VersioningTest.testFull1" -Dsirix.debug.path.summary=true > with-bypass.log 2>&1
   ```

2. **Disable bypass and run:**
   ```bash
   # Edit code to remove bypass, then:
   ./gradlew cleanTest test --tests "VersioningTest.testFull1" -Dsirix.debug.path.summary=true > without-bypass.log 2>&1
   ```

3. **Compare:**
   ```bash
   diff <(grep PATH_SUMMARY with-bypass.log) <(grep PATH_SUMMARY without-bypass.log)
   ```

4. **Analyze differences** to understand what the bypass actually does

This will reveal the exact mechanism of why the bypass is required!

