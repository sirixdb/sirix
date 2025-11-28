# Quick Start: Track Down Page Leak Sources

## TL;DR - 3 Steps

```bash
# 1. Run test with leak tracking enabled
./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest"

# 2. Check the output for creation paths
cat leak-paths.txt

# 3. Fix the most common leak source
```

## Option A: Use the Helper Script

We've created `analyze-page-leaks.sh` to automate everything:

```bash
cd /home/johannes/.cursor/worktrees/sirix/FuQIP

# Run specific test
./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest"

# Run different test
./analyze-page-leaks.sh "io.sirix.axis.ConcurrentAxisTest"

# Custom output file
./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest" "my-leak-analysis.log"
```

**Output:**
```
===================================
Page Leak Source Tracking Script
===================================

Test class: io.sirix.diff.algorithm.FMSETest
Output log: leak-trace-20240321-143022.log

Running test with DEBUG_MEMORY_LEAKS enabled...
...

===================================
Leak Analysis
===================================

Total leaked pages: 247

Leaks by index type:
    185 DOCUMENT
     42 PATH_SUMMARY
     20 PATH

Top 10 leaked page keys:
     15 Page 0
     12 Page 55
     10 Page 42
      8 Page 23
      ...

Leaks by creation method:
     89 io.sirix.access.trx.page.NodeStorageEngineWriter.combineRecordPagesForModification
     78 io.sirix.page.PageKind.deserializePage
     45 io.sirix.page.KeyValueLeafPage.newInstance
     35 io.sirix.page.PageUtils.createTree
      ...
```

## Option B: Manual Gradle Command

```bash
cd /home/johannes/.cursor/worktrees/sirix/FuQIP

# Run test with debug flag
./gradlew :bundles:sirix-core:test \
  --tests "io.sirix.diff.algorithm.FMSETest" \
  -Dsirix.debug.memory.leaks=true \
  > leak-analysis.log 2>&1

# Extract leak information
grep -A 10 "FINALIZER LEAK CAUGHT" leak-analysis.log > leaks.txt
```

## Interpreting Results

### 1. Look at Creation Method Distribution

```bash
grep "Created at:" -A 1 leak-analysis.log | \
  grep -E "^\s+io.sirix" | \
  sed -E 's/^\s+([^(]+).*/\1/' | \
  sort | uniq -c | sort -rn
```

**Example Output:**
```
     89 io.sirix.access.trx.page.NodeStorageEngineWriter.combineRecordPagesForModification
     78 io.sirix.page.PageKind.deserializePage
     45 io.sirix.page.KeyValueLeafPage.newInstance
     35 io.sirix.page.PageUtils.createTree
```

**Interpretation:**

| Top Method | What It Means | Where to Fix |
|------------|---------------|--------------|
| `combineRecordPagesForModification` | PageContainer leaks (2 pages each!) | `TransactionIntentLog.clear()` |
| `deserializePage` | Disk-loaded pages not closed | Fragment combining cleanup |
| `newInstance` | New pages not released | Guard release in reader |
| `createTree` | Initial page 0 not closed | Tree initialization cleanup |

### 2. Check for Container Leaks (Pairs)

```bash
# Find page keys that leak multiple times with same revision
grep "FINALIZER LEAK CAUGHT" leak-analysis.log | \
  awk '{print $6, $8}' | \
  sort | uniq -c | \
  awk '$1 >= 2 {print}'
```

**If you see pairs:**
```
  2 55 revision=0
  2 42 revision=1
  2 23 revision=0
```

**Diagnosis:** PageContainer leak (complete + modify pages both leaked)

**Fix:** Ensure `TransactionIntentLog.clear()` closes all PageContainer entries.

### 3. Check for Guard Issues

```bash
grep "GUARD PROTECTED" leak-analysis.log
```

**If you see:**
```
⚠️  GUARD PROTECTED: Attempted to close guarded page 42 (DOCUMENT) 
   rev=1 with guardCount=3 - skipping close
```

**Diagnosis:** Pages still in use when cache tries to evict them.

**Fix:** Ensure all PageGuards are released (use try-with-resources).

## Common Patterns and Fixes

### Pattern 1: HIGH PageContainer Leaks

**Symptom:**
```
     89 io.sirix.access.trx.page.NodeStorageEngineWriter.combineRecordPagesForModification
```

**What's Happening:**
- Writer creates PageContainer with TWO pages (complete + modify)
- Transaction commits but TIL.clear() doesn't close entries
- Both pages leak

**Where to Look:**
```bash
# Find TIL.clear() implementation
grep -rn "class TransactionIntentLog" bundles/sirix-core/src/main/java/
grep -A 20 "public void clear()" bundles/sirix-core/src/main/java/io/sirix/cache/TransactionIntentLog.java
```

**Expected Fix:**
```java
public void clear() {
    for (PageContainer container : map.values()) {
        if (container.complete == container.modify) {
            // Same instance - close once
            container.complete.close();
        } else {
            // Different instances - close both
            container.complete.close();
            container.modify.close();
        }
    }
    map.clear();
}
```

### Pattern 2: MEDIUM Fragment Leaks

**Symptom:**
```
     78 io.sirix.page.PageKind.deserializePage
```

**What's Happening:**
- Page has multiple fragments (rev 0, rev 1, rev 2)
- Fragments combined into single page
- Older fragments (1, 2, ...) not closed

**Where to Look:**
```bash
grep -A 50 "loadDataPageFromDurableStorageAndCombinePageFragments" \
  bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineReader.java
```

**Expected Fix:**
```java
// After combining fragments
List<KeyValueLeafPage> pages = loadAllFragments();
var mostRecentPage = pages.get(0); // Keep this

// Close older fragments
for (int i = 1; i < pages.size(); i++) {
    var fragment = pages.get(i);
    // Check if in cache first
    var cached = fragmentCache.get(fragmentRef);
    if (cached != fragment) {
        // Not in cache - close explicitly
        fragment.close();
    }
}
```

### Pattern 3: MEDIUM Reader Leaks

**Symptom:**
```
     45 io.sirix.page.KeyValueLeafPage.newInstance
```

**What's Happening:**
- Reader acquires page with guard
- Guard never released
- Page can't be evicted

**Where to Look:**
```bash
grep -B 5 -A 10 "closeCurrentPageGuard" \
  bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineReader.java
```

**Expected Fix:**
```java
// BEFORE: Manual guard management
page.acquireGuard();
// ... use page ...
page.releaseGuard(); // ❌ Might forget or skip on exception

// AFTER: Automatic with try-with-resources
try (PageGuard guard = new PageGuard(page)) {
    KeyValueLeafPage p = guard.page();
    // ... use page ...
}  // ✅ Always released
```

### Pattern 4: LOW Tree Initialization Leaks

**Symptom:**
```
     35 io.sirix.page.PageUtils.createTree
```

**What's Happening:**
- Initial Page 0 created for each index
- Same page used for both complete AND modify
- Not properly closed on cleanup

**Where to Look:**
```bash
grep -A 30 "public static void createTree" \
  bundles/sirix-core/src/main/java/io/sirix/page/PageUtils.java
```

**Expected Fix:**
```java
// In createTree()
var recordPage = new KeyValueLeafPage(...);
recordPage.setRecord(documentRoot);

// Uses SAME page for both roles
var container = PageContainer.getInstance(recordPage, recordPage);
til.put(ref, container);

// In TIL.clear() - handle this special case
if (container.complete == container.modify) {
    container.complete.close(); // Only once!
}
```

## Real Example Walkthrough

Let's say you run the script and see:

```
Total leaked pages: 247

Leaks by creation method:
     89 io.sirix.access.trx.page.NodeStorageEngineWriter.combineRecordPagesForModification
     78 io.sirix.page.PageKind.deserializePage
     45 io.sirix.page.KeyValueLeafPage.newInstance
     35 io.sirix.page.PageUtils.createTree
```

### Step 1: Fix the Biggest Issue (89 leaks)

Open `TransactionIntentLog.java`:

```bash
vim bundles/sirix-core/src/main/java/io/sirix/cache/TransactionIntentLog.java
```

Find the `clear()` method and ensure it closes all PageContainer entries.

### Step 2: Re-run Test

```bash
./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest" "after-til-fix.log"
```

Expected improvement:
```
Total leaked pages: 69  # Down from 247!

Leaks by creation method:
     78 io.sirix.page.PageKind.deserializePage  # Now the top issue
     45 io.sirix.page.KeyValueLeafPage.newInstance
     35 io.sirix.page.PageUtils.createTree
```

### Step 3: Fix Fragment Leaks (78 leaks)

Open `NodeStorageEngineReader.java`:

```bash
vim bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineReader.java
```

Find `loadDataPageFromDurableStorageAndCombinePageFragments()` and add fragment cleanup.

### Step 4: Re-run Again

```bash
./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest" "after-fragment-fix.log"
```

Continue until leaks are zero or minimal.

## Enabling Debug Flag Permanently

If you want stack traces in all test runs:

Edit `bundles/sirix-core/build.gradle`:

```groovy
test {
    // ... existing config ...
    
    // Change this line:
    systemProperty "sirix.debug.memory.leaks", System.getProperty("sirix.debug.memory.leaks", "true")
    //                                                                                           ^^^^
    // Was "false", now "true"
}
```

Then all test runs will capture stack traces automatically.

## Files Reference

| File | Purpose |
|------|---------|
| `PAGE_LEAK_SOURCE_TRACKING.md` | Complete guide on leak detection system |
| `PAGE_LIFECYCLE_AND_LEAKS.md` | Visual flow charts and leak patterns |
| `QUICK_START_LEAK_TRACKING.md` | This file - quick start guide |
| `analyze-page-leaks.sh` | Automated test runner and analyzer |

## Next Steps

1. **Run the script:**
   ```bash
   ./analyze-page-leaks.sh "io.sirix.diff.algorithm.FMSETest"
   ```

2. **Review results:**
   - Check `leak-trace-*.log` for full output
   - Check `leak-paths.txt` for creation paths

3. **Identify top leak source:**
   - Look at "Leaks by creation method" section

4. **Fix and iterate:**
   - Fix the most common source
   - Re-run to verify improvement
   - Move to next source

5. **Verify fix:**
   ```bash
   # After fixes, should see:
   Total leaked pages: 0
   ✅ No leaks detected!
   ```

## Troubleshooting

### No Stack Traces in Output

**Problem:** Logs show "FINALIZER LEAK CAUGHT" but no "Created at:" section.

**Solution:** Debug flag wasn't enabled. Check that:
```bash
grep "sirix.debug.memory.leaks=true" leak-trace-*.log
```

If missing, re-run with explicit flag:
```bash
./gradlew test --tests "YourTest" -Dsirix.debug.memory.leaks=true
```

### Too Many Leaks to Analyze

**Problem:** 1000+ leaked pages, hard to see pattern.

**Solution:** Focus on top 3 creation methods:
```bash
grep "Created at:" -A 1 leak-analysis.log | \
  grep -E "^\s+io.sirix" | \
  sed -E 's/^\s+([^(]+).*/\1/' | \
  sort | uniq -c | sort -rn | head -3
```

Fix just those three sources first.

### Script Permission Denied

**Problem:** `./analyze-page-leaks.sh: Permission denied`

**Solution:**
```bash
chmod +x analyze-page-leaks.sh
```

## Summary

1. Run: `./analyze-page-leaks.sh "TestClass"`
2. Check: "Leaks by creation method" 
3. Fix: Top method first
4. Repeat: Until leaks = 0

The most common issues are:
- **PageContainer cleanup** → Fix `TIL.clear()`
- **Fragment cleanup** → Fix `loadDataPageFromDurableStorageAndCombinePageFragments()`
- **Guard release** → Use try-with-resources

Good luck! 🚀


