# How to Run Diagnostic Test and Analyze Results

## Step 1: Run Your Test

Run your failing test (the one that shows the memory leak):

```bash
./gradlew :sirix-core:test --tests "YourTestClass.yourTestMethod"
```

Or just run your specific test from your IDE.

## Step 2: Check the Diagnostic Log

After the test runs, the diagnostic log will be created at:
```
/home/johannes/IdeaProjects/sirix/memory-leak-diagnostic.log
```

## Step 3: Analyze the Log

Run the analysis script:

```bash
./analyze-memory-leak.sh
```

This will show you:
1. Total allocations vs releases
2. Pool size progression (beginning and end)
3. Pages returned multiple times
4. Any errors during segment return
5. Statistics summary

## Step 4: Manual Analysis

### Find the leak pattern:

```bash
# See initial pool sizes
grep 'segments remaining' memory-leak-diagnostic.log | head -50

# See final pool sizes
grep 'segments remaining' memory-leak-diagnostic.log | tail -50

# Find pages with different segment sizes
grep 'returnSegmentsToAllocator called for page' memory-leak-diagnostic.log | \
    grep 'page 3762'  # Replace with any page number you saw twice
```

### Count net allocations per pool:

```bash
# Pool 4 (65536 bytes) - count allocations
grep 'pool 4 now has.*remaining' memory-leak-diagnostic.log | wc -l

# Pool 4 - count releases
grep 'pool 4 now has.*segments$' memory-leak-diagnostic.log | wc -l
```

### Find pattern of decreasing pool:

```bash
# Extract just the pool sizes for pool 4
grep 'pool 4 now has' memory-leak-diagnostic.log | \
    awk '{for(i=1;i<=NF;i++) if($i=="has") print $(i+1)}' | \
    head -50
```

## What the Diagnostic Will Reveal

The log will tell us:

1. **Are segments being released?** 
   - If yes: Look for imbalance in allocate/release counts
   - If no: Look for where returnSegmentsToAllocator() isn't being called

2. **Are pages being returned multiple times?**
   - Same page, same segments → idempotency working
   - Same page, different segments → multiple instances exist (completePage + modifiedPage)

3. **What's the pattern?**
   - Pool shrinks and stays small → leak
   - Pool shrinks then recovers → temporary usage, no leak
   - Pool fluctuates → normal behavior

4. **Where's the leak?**
   - Compare allocations at start vs releases at end
   - Track which pages never have their segments returned
   - Identify code paths that skip cleanup

## Quick Commands

```bash
# Watch log in real-time (run in separate terminal before test)
tail -f memory-leak-diagnostic.log

# After test - quick summary
echo "Allocations: $(grep -c 'allocate()' memory-leak-diagnostic.log)"
echo "Releases: $(grep -c 'release()' memory-leak-diagnostic.log)"
grep 'Difference (leaked)' memory-leak-diagnostic.log
```

## Next Steps

Once you have the diagnostic output, we can:
1. Identify which specific pages/segments are leaking
2. Trace back to the code path that created them
3. Find the missing cleanup call
4. Apply the targeted fix







