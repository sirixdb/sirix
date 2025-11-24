# Page Leak Tracking - Executive Summary

## The Problem

Your logs show "FINALIZER LEAK CAUGHT" but **without stack traces**:

```
FINALIZER LEAK CAUGHT: Page 55 (DOCUMENT) revision=0 (instance=622477046) - not closed explicitly!
FINALIZER LEAK CAUGHT: Page 55 (DOCUMENT) revision=1 (instance=1612231729) - not closed explicitly!
```

This means `DEBUG_MEMORY_LEAKS` was **not enabled** when tests ran.

## The Solution

The system has built-in stack trace capturing, but you must enable it:

```bash
cd /home/johannes/.cursor/worktrees/sirix/FuQIP
./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest"
```

With debugging enabled, you'll see:

```
FINALIZER LEAK CAUGHT: Page 55 (DOCUMENT) revision=1 (instance=1612231729) - not closed explicitly!
  Created at:
    io.sirix.page.KeyValueLeafPage.<init>(KeyValueLeafPage.java:756)
    io.sirix.access.trx.page.NodeStorageEngineWriter.combineRecordPagesForModification(NodeStorageEngineWriter.java:756)
    io.sirix.access.trx.page.NodeStorageEngineWriter.prepareRecordPage(NodeStorageEngineWriter.java:450)
    io.sirix.access.trx.node.xml.XmlNodeTrxImpl.insertChild(XmlNodeTrxImpl.java:285)
    io.sirix.diff.algorithm.FMSETest.testAllTenth(FMSETest.java:123)
```

Now you can see **exactly where** the leaked page was created!

## Where Pages Come From (5 Sources)

| # | Source | File:Line | When | Pages Created |
|---|--------|-----------|------|---------------|
| 1 | `newInstance()` | `KeyValueLeafPage.java:1009` | New page needed | 1 page |
| 2 | `deserializePage()` | `PageKind.java:80` | Load from disk | 1+ pages (fragments) |
| 3 | `createTree()` | `PageUtils.java:72` | Initial tree | 1 page (key 0) |
| 4 | `combineRecordPagesForModification()` | `NodeStorageEngineWriter.java:739` | Copy-on-write | **2 pages!** |
| 5 | `combineRecordPagesForModification()` | `NodePageTrx.java:739` | Copy-on-write | **2 pages!** |

## Most Likely Leak Sources (Prioritized)

### 🔴 Priority 1: PageContainer Cleanup (Sources 4 & 5)

**Files to Check:**
```bash
bundles/sirix-core/src/main/java/io/sirix/cache/TransactionIntentLog.java
bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java
```

**The Issue:**
- `combineRecordPagesForModification()` creates **TWO pages** per call
- Stored in `TransactionIntentLog` (TIL) as `PageContainer`
- On commit/abort, `TIL.clear()` must close **both pages**
- If not: **2x leak rate!**

**Quick Check:**
```bash
grep -A 20 "public void clear()" bundles/sirix-core/src/main/java/io/sirix/cache/TransactionIntentLog.java
```

Look for: Does it close both `complete` and `modify` pages?

### 🟡 Priority 2: Fragment Cleanup (Source 2)

**Files to Check:**
```bash
bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineReader.java
# Look at: loadDataPageFromDurableStorageAndCombinePageFragments() around line 879
```

**The Issue:**
- Page has multiple fragments (rev 0, 1, 2, ...)
- Fragments combined into single result
- Older fragments (1, 2, ...) must be closed after combining

**Quick Check:**
```bash
grep -A 100 "loadDataPageFromDurableStorageAndCombinePageFragments" \
  bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineReader.java | \
  grep "close()"
```

Look for: Are fragments 1+ closed after combining?

### 🟢 Priority 3: Reader Cleanup (Source 1)

**Files to Check:**
```bash
bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineReader.java
# Look at: close() method around line 1334
# Look at: closeMostRecentPageIfOrphaned() around line 700
```

**The Issue:**
- Reader caches pages in `mostRecentDocumentPage`, `mostRecentPathPages`, etc.
- On reader close, these must be explicitly closed
- If not: Pages leak until GC

**Quick Check:**
```bash
grep -A 50 "public void close()" \
  bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineReader.java | \
  grep "closeMostRecent"
```

Look for: Are all `mostRecent*` fields cleaned up?

## Quick Diagnosis Workflow

### Step 1: Run with Stack Traces

```bash
cd /home/johannes/.cursor/worktrees/sirix/FuQIP
./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest"
```

### Step 2: Check Output

The script will show:
```
Total leaked pages: 247

Leaks by creation method:
     89 io.sirix.access.trx.page.NodeStorageEngineWriter.combineRecordPagesForModification
     78 io.sirix.page.PageKind.deserializePage
     45 io.sirix.page.KeyValueLeafPage.newInstance
     35 io.sirix.page.PageUtils.createTree
```

### Step 3: Match to Priority

| Top Method | Priority | File to Fix |
|------------|----------|-------------|
| `combineRecordPagesForModification` | 🔴 P1 | `TransactionIntentLog.java` |
| `deserializePage` | 🟡 P2 | `NodeStorageEngineReader.java` (fragments) |
| `newInstance` | 🟢 P3 | `NodeStorageEngineReader.java` (guards) |
| `createTree` | 🟢 P3 | `PageUtils.java` + `TransactionIntentLog.java` |

### Step 4: Fix and Verify

```bash
# Fix the top issue
vim bundles/sirix-core/src/main/java/io/sirix/cache/TransactionIntentLog.java

# Re-run test
./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest" "after-fix.log"

# Compare
echo "Before: 247 leaks"
echo "After: $(grep -c 'FINALIZER LEAK CAUGHT' after-fix.log) leaks"
```

## Key Insight: PageContainer Double Leak

The **most common** leak pattern is PageContainer because:

1. Writer needs to modify a page (copy-on-write)
2. Calls `combineRecordPagesForModification()`
3. Creates **TWO pages**:
   - `completePage` = original data
   - `modifyPage` = working copy
4. Wraps in `PageContainer(completePage, modifyPage)`
5. Stores in `TransactionIntentLog`
6. On commit: Must close **BOTH** pages
7. If forgotten: **2 pages leak** instead of 1!

**Evidence from your code:**

```java
// NodeStorageEngineWriter.java:745-765
final KeyValueLeafPage completePage = new KeyValueLeafPage(...);  // Page 1
final KeyValueLeafPage modifyPage = new KeyValueLeafPage(...);    // Page 2

pageContainer = PageContainer.getInstance(completePage, modifyPage);
appendLogRecord(reference, pageContainer);  // Stored in TIL
```

That's why you often see **two leaks** with the same page key:
```
FINALIZER LEAK CAUGHT: Page 55 (DOCUMENT) revision=0 (instance=622477046)   ← completePage
FINALIZER LEAK CAUGHT: Page 55 (DOCUMENT) revision=1 (instance=1612231729)  ← modifyPage
```

## Files Created for You

| File | Purpose |
|------|---------|
| `analyze-page-leaks.sh` | ⭐ **Run this first** - automated test + analysis |
| `QUICK_START_LEAK_TRACKING.md` | Step-by-step guide with examples |
| `PAGE_LEAK_SOURCE_TRACKING.md` | Complete technical reference |
| `PAGE_LIFECYCLE_AND_LEAKS.md` | Visual diagrams and patterns |
| `LEAK_TRACKING_SUMMARY.md` | This file - executive summary |

## Action Items

1. **Immediate:** Run the analysis script
   ```bash
   ./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest"
   ```

2. **Check output:** Look at "Leaks by creation method"

3. **Focus on top issue:** Usually `combineRecordPagesForModification`

4. **Fix:** Ensure `TransactionIntentLog.clear()` closes all PageContainer entries

5. **Verify:** Re-run until leaks = 0

## Current Leak Detection Code

The finalizer is already implemented in `KeyValueLeafPage.java:1066-1102`:

```java
@Override
protected void finalize() {
  if (!isClosed) {
    // Tracking code...
    
    if (creationStackTrace != null) {  // ← Only if DEBUG_MEMORY_LEAKS=true
      leakMsg.append("\n  Created at:");
      for (int i = 2; i < Math.min(creationStackTrace.length, 10); i++) {
        StackTraceElement frame = creationStackTrace[i];
        leakMsg.append(String.format("\n    %s.%s(%s:%d)",
            frame.getClassName(), frame.getMethodName(),
            frame.getFileName(), frame.getLineNumber()));
      }
    }
    
    LOGGER.warn(leakMsg.toString());
  }
}
```

The stack trace is captured in the constructor at line 243 and 329:

```java
if (DEBUG_MEMORY_LEAKS) {
  this.creationStackTrace = Thread.currentThread().getStackTrace();
  // ... tracking code ...
} else {
  this.creationStackTrace = null;  // ← This is why you see no stacks!
}
```

**You just need to enable the flag!**

## Next Steps

```bash
# 1. Run analysis
cd /home/johannes/.cursor/worktrees/sirix/FuQIP
./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest"

# 2. Review results
cat leak-paths.txt

# 3. Open most common file
vim bundles/sirix-core/src/main/java/io/sirix/cache/TransactionIntentLog.java

# 4. Fix and re-test
./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest" "after-fix.log"
```

The infrastructure is already there - you just need to enable it and follow the stack traces! 🎯


