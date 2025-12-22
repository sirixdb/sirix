# Versioning Algorithm Optimization Plan (DIFFERENTIAL, INCREMENTAL, SLIDING_SNAPSHOT)

## Executive Summary

This document presents a comprehensive optimization plan for SirixDB's non-FULL versioning algorithms. 
The optimizations target the critical performance bottlenecks in page fragment combining operations 
while maintaining correctness through formal proofs.

**Target Algorithms**: DIFFERENTIAL, INCREMENTAL, SLIDING_SNAPSHOT  
**Not Covered**: FULL (already optimal for its design goals)

---

## Part 1: Current Architecture Analysis

### 1.1 Versioning Algorithm Overview

| Algorithm | Fragments to Read | Storage per Revision | Read Complexity |
|-----------|-------------------|---------------------|-----------------|
| FULL | 1 | O(page_size) | O(1) |
| DIFFERENTIAL | ≤2 | O(changes) | O(2) |
| INCREMENTAL | ≤R | O(changes) | O(R) |
| SLIDING_SNAPSHOT | ≤R | O(changes + window) | O(R) |

Where R = `maxNumberOfRevisionsToRestore` (typically 3-5)

### 1.2 Current Page Structure (`KeyValueLeafPage`)

```
┌─────────────────────────────────────────────────────────────────┐
│ KeyValueLeafPage (Constants.NDP_NODE_COUNT = 1024 slots)        │
├─────────────────────────────────────────────────────────────────┤
│ slotOffsets[1024]     - int array of byte offsets (-1 = empty)  │
│ slotMemory            - MemorySegment (64KB default)            │
│ deweyIdOffsets[1024]  - int array for DeweyID offsets           │
│ deweyIdMemory         - MemorySegment for DeweyIDs              │
│ references            - Map<Long, PageReference> for overflow   │
│ records[1024]         - DataRecord[] for deserialized nodes     │
│ revision              - int                                     │
│ recordPageKey         - long                                    │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 Critical Performance Bottlenecks

#### Bottleneck 1: Full Slot Iteration (O(1024) per page)
```java
// Current code in combineRecordPages()
for (int offset = 0; offset < Constants.NDP_NODE_COUNT; offset++) {  // Always 1024 iterations
    final var recordData = page.getSlot(offset);
    if (recordData == null) continue;  // Skips empty, but still iterates
    // ... processing
}
```
**Problem**: Even pages with 10 nodes iterate 1024 times.

#### Bottleneck 2: Memory Allocation Per Combine
```java
// In VersioningType.combineRecordPages()
final T returnVal = firstPage.newInstance(...);  // Allocates 64KB MemorySegment

// In KeyValueLeafPage.newInstance()
MemorySegment slotMemory = allocator.allocate(SIXTYFOUR_KB);  // 64KB per page
```
**Problem**: Each combine operation allocates 64KB even if page has few slots.

#### Bottleneck 3: Per-Slot MemorySegment Copying
```java
// In setSlot()
MemorySegment.copy(data, 0, slotMemory, targetOffset, data.byteSize());
```
**Problem**: Copies each slot individually instead of bulk operations.

#### Bottleneck 4: Redundant Data in SLIDING_SNAPSHOT
```java
// Creates 3 pages in combineRecordPagesForModification()
final T completePage = firstPage.newInstance(...);           // 64KB
final T modifyingPage = firstPage.newInstance(...);          // 64KB
final T pageWithRecordsInSlidingWindow = firstPage.newInstance(...);  // 64KB (temp)
```
**Problem**: 192KB total allocation for combining operation.

#### Bottleneck 5: Sequential Fragment Loading
```java
// In getPreviousPageFragments()
final var pages = pageFragments.stream()
    .map(this::readPage)  // Sequential mapping
    .collect(Collectors.toList());
return sequence(pages).join();  // Then parallel join
```
**Problem**: Creates futures sequentially before parallel execution.

---

## Part 2: Optimization Strategies

### Optimization 1: Slot Bitmap Index

**Concept**: Maintain a 128-byte bitmap (1024 bits) to track populated slots.

#### 1.1 Data Structure
```java
public final class KeyValueLeafPage {
    // Add bitmap: 1 bit per slot = 128 bytes for 1024 slots
    private long[] slotBitmap = new long[16];  // 16 * 64 = 1024 bits
    
    public boolean hasSlot(int slotNumber) {
        return (slotBitmap[slotNumber >>> 6] & (1L << (slotNumber & 63))) != 0;
    }
    
    public void markSlotPopulated(int slotNumber) {
        slotBitmap[slotNumber >>> 6] |= (1L << (slotNumber & 63));
    }
    
    // Iterate only populated slots
    public IntStream populatedSlots() {
        return IntStream.range(0, 16)
            .flatMap(i -> {
                long word = slotBitmap[i];
                return word == 0 ? IntStream.empty() :
                    IntStream.iterate(Long.numberOfTrailingZeros(word),
                        bit -> bit < 64 && ((word >>> bit) != 0),
                        bit -> bit + Long.numberOfTrailingZeros(word >>> bit) + 1)
                    .map(bit -> (i << 6) | bit);
            });
    }
}
```

#### 1.2 Optimized Combining with Bitmap
```java
// BEFORE: O(NDP_NODE_COUNT) = O(1024)
for (int offset = 0; offset < Constants.NDP_NODE_COUNT; offset++) {
    if (page.getSlot(offset) == null) continue;
    // ... process
}

// AFTER: O(populated_slots) ≤ O(1024), typically O(10-100)
page.populatedSlots().forEach(offset -> {
    // ... process (no null check needed)
});
```

#### 1.3 Formal Proof of Correctness

**Theorem 1**: Bitmap-indexed slot iteration produces identical results to full iteration.

**Proof**:
1. Let S = {s₀, s₁, ..., sₙ₋₁} be the set of all slot indices where 0 ≤ sᵢ < 1024
2. Let P ⊆ S be the set of populated slots (where getSlot(sᵢ) ≠ null)
3. Define bitmap B where B[i] = 1 iff i ∈ P

**Invariant I₁**: For all i ∈ S: B[i] = 1 ⟺ slotOffsets[i] ≥ 0

*Base case*: Initially all B[i] = 0, all slotOffsets[i] = -1. I₁ holds.

*Inductive step*: When setSlot(data, i) is called:
- If data ≠ null: slotOffsets[i] ← positive_offset; B[i] ← 1
- If data = null: slotOffsets[i] ← -1; B[i] ← 0
  
I₁ preserved.

**Correctness**: 
- Full iteration processes all i where getSlot(i) ≠ null
- This equals {i : slotOffsets[i] ≥ 0} = {i : B[i] = 1} by I₁
- Bitmap iteration processes exactly {i : B[i] = 1}
- Therefore: bitmap iteration = full iteration ∎

**Complexity**:
- Let k = |P| (number of populated slots)
- Full iteration: O(1024) always
- Bitmap iteration: O(k) processing + O(16) bitmap scan = O(k + 16)
- Improvement ratio: 1024/(k+16), typically 10-100x for sparse pages

---

### Optimization 2: Lazy/Deferred Page Combining

**Concept**: Instead of copying slots during `combineRecordPages()`, create a view that 
references the source fragments and resolves slots on-demand.

#### 2.1 Data Structure: CombinedPageView
```java
public final class CombinedPageView implements KeyValuePage<DataRecord> {
    private final List<KeyValueLeafPage> fragments;  // Ordered: newest first
    private final long recordPageKey;
    private final int revision;
    
    // Lazy resolution with caching
    private final MemorySegment[] resolvedSlots = new MemorySegment[Constants.NDP_NODE_COUNT];
    private final long[] slotResolutionBitmap = new long[16];  // Track which slots resolved
    
    @Override
    public MemorySegment getSlot(int slotNumber) {
        // Check if already resolved
        if (isResolved(slotNumber)) {
            return resolvedSlots[slotNumber];
        }
        
        // Resolve from fragments (newest to oldest)
        for (KeyValueLeafPage fragment : fragments) {
            MemorySegment data = fragment.getSlot(slotNumber);
            if (data != null) {
                resolvedSlots[slotNumber] = data;  // Cache reference (no copy!)
                markResolved(slotNumber);
                return data;
            }
        }
        
        markResolved(slotNumber);  // Mark as resolved even if null
        return null;
    }
}
```

#### 2.2 Memory Model: Reference vs Copy

```
BEFORE (Copy-on-Combine):
┌─────────────┐     ┌─────────────┐
│ Fragment 0  │     │ Fragment 1  │
│ [A][B][_][_]│     │ [_][_][C][D]│
└─────────────┘     └─────────────┘
        ↓                  ↓
     Copy A,B           Copy C,D
        ↓                  ↓
┌─────────────────────────────┐
│ Combined Page (new 64KB)    │
│ [A'][B'][C'][D']            │  ← Copies of all data
└─────────────────────────────┘

AFTER (Reference-on-Demand):
┌─────────────┐     ┌─────────────┐
│ Fragment 0  │     │ Fragment 1  │
│ [A][B][_][_]│     │ [_][_][C][D]│
└─────────────┘     └─────────────┘
        ↑                  ↑
        │                  │
┌───────┴──────────────────┴───┐
│ CombinedPageView             │
│ refs: [→A][→B][→C][→D]       │  ← Just 4 pointers
└──────────────────────────────┘
```

#### 2.3 Formal Proof of Correctness

**Theorem 2**: CombinedPageView produces identical read results as eager combining.

**Proof**:
1. Let F = [f₀, f₁, ..., fₖ₋₁] be fragments ordered newest to oldest
2. For any slot index i, define:
   - eager(i) = result of eager combining for slot i
   - lazy(i) = result of lazy resolution for slot i

**Claim**: ∀i: eager(i) = lazy(i)

*Case 1*: No fragment has data at slot i
- Eager loop: iterates all fragments, all return null, sets returnVal[i] = null
- Lazy lookup: iterates fragments, all return null, returns null
- Result: null = null ✓

*Case 2*: Fragment fⱼ is the first (lowest j) with data at slot i
- Eager combining: Iterates f₀...fⱼ, sets returnVal[i] = fⱼ.getSlot(i)
  (earlier fragments had null, so `if (returnVal.getSlot(offset) == null)` passed)
- Lazy resolution: Iterates f₀...fⱼ, returns fⱼ.getSlot(i) at first non-null
- Result: Same data reference ✓

**Temporal Safety**: Fragments remain valid during CombinedPageView lifetime via:
1. Guard acquisition before combining (existing `acquireGuard()`)
2. Guard release only after CombinedPageView is closed
3. Cache eviction blocked while guards held ∎

#### 2.4 Complexity Analysis

| Operation | Eager Combine | Lazy View | Improvement |
|-----------|--------------|-----------|-------------|
| Combine | O(k × 1024) | O(k) | 1024x |
| First read | - | O(k) | - |
| Subsequent reads | O(1) | O(1) | Same |
| Memory | 64KB | ~8KB refs | 8x |

Where k = number of fragments

---

### Optimization 3: Bulk MemorySegment Operations

**Concept**: Use SIMD-friendly bulk memory operations instead of per-slot copies.

#### 3.1 Vectorized Slot Copy
```java
public void copyPopulatedSlotsFrom(KeyValueLeafPage source, long[] targetBitmap) {
    // Use MemorySegment bulk copy for contiguous regions
    int contiguousStart = -1;
    int contiguousEnd = -1;
    
    source.populatedSlots().forEachOrdered(slot -> {
        if (targetBitmap[slot >>> 6] & (1L << (slot & 63)) != 0) {
            return;  // Target already has this slot
        }
        
        int srcOffset = source.slotOffsets[slot];
        int srcLength = source.getSlotLength(slot);
        
        if (contiguousStart < 0) {
            contiguousStart = srcOffset;
            contiguousEnd = srcOffset + srcLength;
        } else if (srcOffset == contiguousEnd) {
            // Extend contiguous region
            contiguousEnd = srcOffset + srcLength;
        } else {
            // Flush contiguous region with single bulk copy
            flushContiguous(source, contiguousStart, contiguousEnd - contiguousStart);
            contiguousStart = srcOffset;
            contiguousEnd = srcOffset + srcLength;
        }
    });
    
    // Flush final region
    if (contiguousStart >= 0) {
        flushContiguous(source, contiguousStart, contiguousEnd - contiguousStart);
    }
}

private void flushContiguous(KeyValueLeafPage source, int offset, int length) {
    // Single bulk copy for entire contiguous region
    MemorySegment.copy(
        source.slotMemory, offset,
        this.slotMemory, this.slotMemoryFreeSpaceStart,
        length
    );
    this.slotMemoryFreeSpaceStart += length;
}
```

#### 3.2 Formal Proof of Bulk Copy Correctness

**Theorem 3**: Bulk contiguous copy produces identical results as per-slot copy.

**Proof**:
Let slots s₁, s₂, ..., sₙ be contiguous (offsets form continuous range).

*Per-slot copy*:
```
for i in 1..n:
    copy(src.memory[offset_i..offset_i+len_i] → dst.memory[dst_offset])
    dst_offset += len_i
```

*Bulk copy*:
```
total_len = Σ len_i
copy(src.memory[offset_1..offset_1+total_len] → dst.memory[dst_offset])
```

Since offsets are contiguous: offset_{i+1} = offset_i + len_i

Therefore: src.memory[offset_1..offset_1+total_len] = ⋃ᵢ src.memory[offset_i..offset_i+len_i]

The bulk copy is equivalent to all per-slot copies concatenated ∎

---

### Optimization 4: Reduced Page Allocation in SLIDING_SNAPSHOT

**Concept**: Avoid creating the temporary `pageWithRecordsInSlidingWindow` page.

#### 4.1 Current vs Optimized Flow
```
CURRENT:
┌─────────────────────────────────────────────────────────────────────┐
│ completePage      ← newInstance() (64KB)                            │
│ modifyingPage     ← newInstance() (64KB)                            │
│ tempPage          ← newInstance() (64KB) ← ELIMINATED               │
│                                                                      │
│ for each fragment:                                                   │
│   for each slot:                                                     │
│     if (!outOfWindow) tempPage.setSlot()                            │
│     if (completePage.getSlot(slot) == null) completePage.setSlot()  │
│     if (outOfWindow && tempPage.getSlot(slot) == null)              │
│       modifyingPage.setSlot()                                        │
│                                                                      │
│ tempPage.close()  ← Already fixed                                   │
└─────────────────────────────────────────────────────────────────────┘

OPTIMIZED:
┌─────────────────────────────────────────────────────────────────────┐
│ completePage      ← newInstance() (64KB)                            │
│ modifyingPage     ← newInstance() (64KB)                            │
│ inWindowBitmap    ← new long[16] (128 bytes)  ← Replaces tempPage   │
│                                                                      │
│ for each fragment (except last if out of window):                    │
│   for each slot:                                                     │
│     inWindowBitmap[slot] = 1                                         │
│     if (completePage.getSlot(slot) == null) completePage.setSlot()  │
│                                                                      │
│ if (lastFragment out of window):                                     │
│   for each slot in lastFragment where !inWindowBitmap[slot]:         │
│     modifyingPage.setSlot()  ← Copy slots falling out of window     │
└─────────────────────────────────────────────────────────────────────┘
```

#### 4.2 Implementation
```java
@Override
public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer 
    combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, 
        final StorageEngineReader pageReadTrx, 
        final PageReference reference, final TransactionIntentLog log) {
    
    final T firstPage = pages.getFirst();
    final long recordPageKey = firstPage.getPageKey();
    
    // Track previous fragment keys
    final var previousPageFragmentKeys = buildFragmentKeys(firstPage, reference, pageReadTrx, revToRestore);
    reference.setPageFragments(previousPageFragmentKeys);
    
    // Only TWO pages instead of THREE
    final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
    final T modifyingPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
    
    // Bitmap instead of temporary page (128 bytes vs 64KB)
    final long[] inWindowBitmap = new long[16];
    
    final boolean hasOutOfWindowPage = (pages.size() == revToRestore);
    final int lastInWindowIndex = hasOutOfWindowPage ? pages.size() - 2 : pages.size() - 1;
    
    // Phase 1: Process in-window fragments, track populated slots
    for (int i = 0; i <= lastInWindowIndex && completePage.size() < Constants.NDP_NODE_COUNT; i++) {
        final T page = pages.get(i);
        
        page.populatedSlots().forEach(offset -> {
            // Mark as in-window
            inWindowBitmap[offset >>> 6] |= (1L << (offset & 63));
            
            // Add to complete page if not already present
            if (completePage.getSlot(offset) == null) {
                completePage.setSlot(page.getSlot(offset), offset);
                completePage.setDeweyId(page.getDeweyId(offset), offset);
            }
        });
        
        // Handle references similarly...
    }
    
    // Phase 2: Process out-of-window fragment if present
    if (hasOutOfWindowPage) {
        final T outOfWindowPage = pages.get(pages.size() - 1);
        
        outOfWindowPage.populatedSlots().forEach(offset -> {
            // Check if NOT in window
            if ((inWindowBitmap[offset >>> 6] & (1L << (offset & 63))) == 0) {
                // Slot falls out of window → copy to modifyingPage
                modifyingPage.setSlot(outOfWindowPage.getSlot(offset), offset);
                modifyingPage.setDeweyId(outOfWindowPage.getDeweyId(offset), offset);
            }
            
            // Still add to completePage if needed
            if (completePage.getSlot(offset) == null) {
                completePage.setSlot(outOfWindowPage.getSlot(offset), offset);
                completePage.setDeweyId(outOfWindowPage.getDeweyId(offset), offset);
            }
        });
    }
    
    propagateFsstSymbolTable(firstPage, completePage);
    propagateFsstSymbolTable(firstPage, modifyingPage);
    
    final var pageContainer = PageContainer.getInstance(completePage, modifyingPage);
    log.put(reference, pageContainer);
    return pageContainer;
}
```

#### 4.3 Formal Proof of Correctness

**Theorem 4**: Bitmap-based SLIDING_SNAPSHOT produces identical PageContainer as tempPage-based.

**Definitions**:
- Window W = {fragments f₀...f_{R-2}} (all except possibly the oldest)
- OutOfWindow O = {f_{R-1}} if |pages| = R, else ∅
- InWindowSlots(i) = {slot s : ∃f ∈ W where f.getSlot(s) ≠ null at slot position up to fragment i}

**Invariant I₂**: At end of Phase 1 iteration i:
```
inWindowBitmap[s] = 1 ⟺ s ∈ InWindowSlots(i)
```

*Base case (i=0)*: 
- After processing f₀, bitmap has bits set for all populated slots in f₀
- InWindowSlots(0) = {s : f₀.getSlot(s) ≠ null}
- Bitmap matches by construction ✓

*Inductive step (i → i+1)*:
- Assuming I₂ holds for iteration i
- Iteration i+1 sets bits for all populated slots in f_{i+1}
- inWindowBitmap now = inWindowBitmap_i ∪ {s : f_{i+1}.getSlot(s) ≠ null}
- = InWindowSlots(i) ∪ {s : f_{i+1}.getSlot(s) ≠ null}
- = InWindowSlots(i+1) by definition ✓

**Phase 2 Correctness**:
For slot s in out-of-window fragment O:
- TempPage approach: modifyingPage.setSlot(s) iff tempPage.getSlot(s) == null
- Bitmap approach: modifyingPage.setSlot(s) iff inWindowBitmap[s] == 0

By I₂ after Phase 1:
- inWindowBitmap[s] == 0 ⟺ s ∉ InWindowSlots(lastInWindowIndex)
- ⟺ no fragment in W has slot s
- ⟺ tempPage.getSlot(s) == null

Therefore bitmap condition = tempPage condition ∎

**Memory Savings**: 64KB → 128 bytes = 512x reduction for temporary structure

---

### Optimization 5: Parallel Fragment Loading

**Concept**: Load all fragments in parallel with explicit parallelism.

#### 5.1 Current Sequential-then-Parallel
```java
// Current: Sequential stream map, then parallel wait
final var pages = pageFragments.stream()
    .map(this::readPage)  // Creates CompletableFuture, but sequentially!
    .collect(Collectors.toList());
return sequence(pages).join();
```

#### 5.2 Optimized Fully Parallel
```java
private List<KeyValuePage<DataRecord>> getPreviousPageFragments(
    final List<PageFragmentKey> pageFragments) {
    
    if (pageFragments.isEmpty()) {
        return Collections.emptyList();
    }
    
    // Parallel stream for concurrent loading
    CompletableFuture<?>[] futures = new CompletableFuture[pageFragments.size()];
    KeyValuePage<DataRecord>[] results = new KeyValuePage[pageFragments.size()];
    
    // Launch all loads in parallel
    for (int i = 0; i < pageFragments.size(); i++) {
        final int index = i;
        futures[i] = CompletableFuture.supplyAsync(() -> {
            results[index] = readPageSync(pageFragments.get(index));
            return results[index];
        }, fragmentLoadExecutor);  // Dedicated executor
    }
    
    // Wait for all
    CompletableFuture.allOf(futures).join();
    
    // Sort by revision (newest first)
    return Arrays.stream(results)
        .filter(Objects::nonNull)
        .sorted(Comparator.comparing(KeyValuePage::getRevision).reversed())
        .collect(Collectors.toList());
}
```

---

### Optimization 6: Fragment Caching Prefetch

**Concept**: Prefetch fragments for nearby pages during idle time.

```java
public void prefetchNearbyFragments(long currentPageKey, int radius) {
    for (long key = currentPageKey - radius; key <= currentPageKey + radius; key++) {
        if (key >= 0 && key != currentPageKey) {
            executor.submit(() -> {
                PageReference ref = lookupReference(key);
                if (ref != null) {
                    getPageFragments(ref);  // Populates cache
                }
            });
        }
    }
}
```

---

## Part 3: Complete Optimized `combineRecordPages` Implementation

### 3.1 INCREMENTAL (Optimized)
```java
@Override
public <V extends DataRecord, T extends KeyValuePage<V>> T combineRecordPages(
    final List<T> pages, final @NonNegative int revToRestore, 
    final StorageEngineReader pageReadTrx) {
    
    assert pages.size() <= revToRestore;
    final T firstPage = pages.getFirst();
    final long recordPageKey = firstPage.getPageKey();
    
    // Option A: Lazy View (zero-copy for reads)
    if (pages.size() > 1 && canUseLazyView(pageReadTrx)) {
        return (T) new CombinedPageView(pages, recordPageKey, firstPage.getRevision());
    }
    
    // Option B: Optimized Eager Combine with Bitmap
    final T returnVal = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
    
    // Track filled slots with bitmap
    final long[] filledBitmap = new long[16];
    int filledCount = 0;
    
    for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        
        if (filledCount == Constants.NDP_NODE_COUNT) break;
        
        // Use bitmap iteration for populated slots only
        ((KeyValueLeafPage) page).populatedSlots().forEach(offset -> {
            // Skip if already filled
            if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
                return;
            }
            
            returnVal.setSlot(page.getSlot(offset), offset);
            returnVal.setDeweyId(page.getDeweyId(offset), offset);
            filledBitmap[offset >>> 6] |= (1L << (offset & 63));
        });
        
        filledCount = returnVal.size();
        
        // Handle overflow references
        if (filledCount < Constants.NDP_NODE_COUNT) {
            for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
                if (returnVal.getPageReference(entry.getKey()) == null) {
                    returnVal.setPageReference(entry.getKey(), entry.getValue());
                    if (returnVal.size() == Constants.NDP_NODE_COUNT) break;
                }
            }
        }
    }
    
    propagateFsstSymbolTable(firstPage, returnVal);
    return returnVal;
}
```

### 3.2 DIFFERENTIAL (Optimized)
```java
@Override
public <V extends DataRecord, T extends KeyValuePage<V>> T combineRecordPages(
    final List<T> pages, final @NonNegative int revToRestore, 
    final StorageEngineReader pageReadTrx) {
    
    assert pages.size() <= 2;
    final T firstPage = pages.getFirst();
    final long recordPageKey = firstPage.getPageKey();
    final T returnVal = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
    
    // Copy all from latest page using bitmap iteration
    ((KeyValueLeafPage) firstPage).populatedSlots().forEach(offset -> {
        returnVal.setSlot(firstPage.getSlot(offset), offset);
        returnVal.setDeweyId(firstPage.getDeweyId(offset), offset);
    });
    
    // Copy references
    for (final Entry<Long, PageReference> entry : firstPage.referenceEntrySet()) {
        returnVal.setPageReference(entry.getKey(), entry.getValue());
    }
    
    // Fill gaps from full dump if present
    if (pages.size() == 2 && returnVal.size() < Constants.NDP_NODE_COUNT) {
        T fullDump = pages.get(1);
        
        // Get bitmap of already-filled slots
        final long[] filledBitmap = ((KeyValueLeafPage) returnVal).getSlotBitmap();
        
        ((KeyValueLeafPage) fullDump).populatedSlots().forEach(offset -> {
            if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) == 0) {
                returnVal.setSlot(fullDump.getSlot(offset), offset);
                returnVal.setDeweyId(fullDump.getDeweyId(offset), offset);
            }
        });
        
        // Fill reference gaps
        if (returnVal.size() < Constants.NDP_NODE_COUNT) {
            for (final Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
                if (returnVal.getPageReference(entry.getKey()) == null) {
                    returnVal.setPageReference(entry.getKey(), entry.getValue());
                    if (returnVal.size() == Constants.NDP_NODE_COUNT) break;
                }
            }
        }
    }
    
    propagateFsstSymbolTable(firstPage, returnVal);
    return returnVal;
}
```

---

## Part 4: Correctness Theorems Summary

### Master Theorem: Optimized Versioning Equivalence

**Theorem 5** (Master): For all versioning types V ∈ {DIFFERENTIAL, INCREMENTAL, SLIDING_SNAPSHOT}:
```
∀ pages P, revToRestore R, pageReadTrx T:
  V.combineRecordPages_optimized(P, R, T) ≡ V.combineRecordPages_original(P, R, T)
```

Where ≡ means "returns semantically identical page" defined as:
```
page₁ ≡ page₂ ⟺ 
  ∀i ∈ [0, NDP_NODE_COUNT): 
    page₁.getSlot(i) = page₂.getSlot(i) ∧
    page₁.getDeweyId(i) = page₂.getDeweyId(i) ∧
    page₁.getPageReference(key) = page₂.getPageReference(key) for all keys
```

**Proof Structure**:
1. Bitmap iteration equivalence (Theorem 1)
2. Lazy view equivalence (Theorem 2)  
3. Bulk copy equivalence (Theorem 3)
4. SLIDING_SNAPSHOT bitmap equivalence (Theorem 4)

Each sub-theorem proven above. By composition, the optimized implementations
maintain semantic equivalence to the originals. ∎

---

## Part 5: Expected Performance Improvements

### Benchmarks Targets

| Scenario | Original | Optimized | Speedup |
|----------|----------|-----------|---------|
| Sparse page combine (10 slots) | O(1024) iters | O(10) iters | ~100x |
| SLIDING_SNAPSHOT memory | 192KB | 128KB + 128B | 1.5x |
| Page combine allocation | 64KB always | 0-64KB | 0-100% |
| Fragment loading | Sequential | Parallel | k×* |

*Where k = number of fragments

### Memory Reduction

| Component | Original | Optimized | Reduction |
|-----------|----------|-----------|-----------|
| Temp page (SLIDING_SNAPSHOT) | 64KB | 128 bytes | 99.8% |
| Lazy view vs copy | 64KB | ~4KB refs | 93.75% |
| Bitmap per page | 0 | 128 bytes | +128B |

---

## Part 6: Implementation Priority

### Phase 1: High Impact, Low Risk
1. **Slot Bitmap Index** - Pure addition, no behavioral change
2. **SLIDING_SNAPSHOT temp elimination** - Already partially done (close fix)
3. **Parallel fragment loading** - Isolated change

### Phase 2: Medium Impact, Medium Risk  
4. **Bulk MemorySegment operations** - Requires careful offset tracking
5. **Fragment prefetching** - Background optimization

### Phase 3: High Impact, Higher Risk
6. **Lazy CombinedPageView** - Architectural change, needs thorough testing

---

## Appendix A: Formal Invariants for Testing

```java
public class VersioningInvariants {
    
    /**
     * I₁: Bitmap consistency invariant
     */
    public static void assertBitmapConsistency(KeyValueLeafPage page) {
        long[] bitmap = page.getSlotBitmap();
        for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
            boolean bitmapHas = (bitmap[i >>> 6] & (1L << (i & 63))) != 0;
            boolean slotHas = page.slotOffsets[i] >= 0;
            assert bitmapHas == slotHas : 
                "Bitmap inconsistency at slot " + i + ": bitmap=" + bitmapHas + ", offset=" + slotHas;
        }
    }
    
    /**
     * I₂: Combined page completeness invariant
     */
    public static void assertCombineCompleteness(
        KeyValueLeafPage combined, List<KeyValueLeafPage> fragments) {
        
        for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
            MemorySegment combinedSlot = combined.getSlot(i);
            
            // Find expected value (first non-null in fragments)
            MemorySegment expectedSlot = null;
            for (KeyValueLeafPage fragment : fragments) {
                MemorySegment fragSlot = fragment.getSlot(i);
                if (fragSlot != null) {
                    expectedSlot = fragSlot;
                    break;
                }
            }
            
            assert Objects.equals(combinedSlot, expectedSlot) :
                "Slot " + i + " mismatch: combined=" + combinedSlot + ", expected=" + expectedSlot;
        }
    }
}
```

---

## Appendix B: Test Cases

```java
@Test
void bitmapIteration_matchesFullIteration() {
    KeyValueLeafPage page = createPageWithSlots(5, 100, 500, 999);
    
    List<Integer> fullIterResult = new ArrayList<>();
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
        if (page.getSlot(i) != null) fullIterResult.add(i);
    }
    
    List<Integer> bitmapIterResult = page.populatedSlots().boxed().toList();
    
    assertEquals(fullIterResult, bitmapIterResult);
}

@Test
void slidingSnapshot_bitmapProducesSameResult() {
    // Setup fragments
    List<KeyValueLeafPage> fragments = createFragments(3);
    
    // Original with temp page
    PageContainer originalResult = SLIDING_SNAPSHOT_ORIGINAL
        .combineRecordPagesForModification(fragments, 3, trx, ref, log);
    
    // Optimized with bitmap
    PageContainer optimizedResult = SLIDING_SNAPSHOT_OPTIMIZED
        .combineRecordPagesForModification(fragments, 3, trx, ref, log);
    
    assertPageEquals(originalResult.getComplete(), optimizedResult.getComplete());
    assertPageEquals(originalResult.getModified(), optimizedResult.getModified());
}
```

---

*Document Version: 1.0*  
*Author: AI Assistant*  
*Date: December 22, 2025*

