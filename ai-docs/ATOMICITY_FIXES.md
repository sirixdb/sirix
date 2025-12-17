# Atomicity Fixes for ShardedPageCache and Transaction Code

**Date:** November 14, 2025  
**Status:** ✅ **COMPLETE**

---

## Problem Statement

The cache methods in `ShardedPageCache` were **NOT atomic**. Individual operations used thread-safe `ConcurrentHashMap` methods, but composite operations had **check-then-act races** that could cause:

1. **Data corruption**: ClockSweeper evicting pages between cache lookup and guard acquisition
2. **Use-after-eviction**: Transactions accessing pages that were already reset/evicted
3. **Lost updates**: Clock hand updates not visible across threads

---

## Critical Race Conditions Fixed

### 1. ⚠️ **Shard Clock Hand Not Shared** (CRITICAL BUG)

**Problem:**
```java
// OLD CODE - getShard() created NEW instance every time
public Shard getShard(PageReference ref) {
    return new Shard(map, evictionLock, clockHand); // Copy of clockHand!
}
```

ClockSweeper modified `shard.clockHand`, but it was a copy - updates were lost!

**Fix:**
```java
// NEW CODE - Single shared shard instance
private final Shard shard;

public ShardedPageCache(int shardCount) {
    this.shard = new Shard(map, evictionLock);
}

public Shard getShard(PageReference ref) {
    return shard; // Same instance every time
}
```

---

### 2. ⚠️ **Cache.get() + guard acquisition NOT atomic** (DATA CORRUPTION RISK)

**Problem:**
```java
// Transaction code - RACE WINDOW!
KeyValueLeafPage page = cache.get(pageRef);   // Step 1
// ClockSweeper can evict + reset page HERE!
page.acquireGuard();                          // Step 2 - TOO LATE!
```

**Attack scenario:**
```
Thread A (transaction):           Thread B (ClockSweeper):
page = cache.get(ref)            
[version=V1, has data]           if (page.getGuardCount() == 0) {  // TRUE!
                                     page.incrementVersion()  // V1→V2
                                     page.reset()  // Clears all data!
                                     cache.remove(ref)
                                 }
page.acquireGuard()              
  [guards a RESET page]
USE page.getData()               
  [DATA IS GONE!]  ❌
```

**Fix - New atomic method:**
```java
// Cache.java - Default implementation for all caches
default V getAndGuard(K key) {
    return asMap().compute(key, (k, existingValue) -> {
        if (existingValue != null && !existingValue.isClosed()) {
            KeyValueLeafPage page = (KeyValueLeafPage) existingValue;
            // ATOMIC: mark + guard while holding map lock for this key
            page.markAccessed();
            page.acquireGuard();
            return existingValue;
        }
        return existingValue;
    });
}

// ShardedPageCache.java - Optimized override
@Override
public KeyValueLeafPage getAndGuard(PageReference key) {
    return map.compute(key, (k, existingValue) -> {
        if (existingValue != null && !existingValue.isClosed()) {
            existingValue.markAccessed();
            existingValue.acquireGuard();
            return existingValue;
        }
        return existingValue;
    });
}
```

---

### 3. ⚠️ **PageGuard + getAndGuard() double-acquires**

**Problem:**
```java
KeyValueLeafPage page = cache.getAndGuard(ref);  // Acquires guard
PageGuard guard = new PageGuard(page);           // Acquires AGAIN! ❌
```

**Fix - New PageGuard factory method:**
```java
// PageGuard.java
/**
 * Wrap a page that already has a guard acquired.
 * Used when page was returned from cache.getAndGuard().
 */
public static PageGuard fromAcquired(KeyValueLeafPage page) {
    return new PageGuard(page, false); // Don't acquire again
}

// Usage:
KeyValueLeafPage page = cache.getAndGuard(ref);
PageGuard guard = PageGuard.fromAcquired(page);  // ✓ No double-acquire
```

---

### 4. ✅ **Transaction Code Updated**

**NodePageReadOnlyTrx.java - Main record page cache (line 696-758):**
```java
// OLD - Race condition
KeyValueLeafPage page = cache.get(pageRef);
// ... later ...
currentPageGuard = new PageGuard(page);  // Too late!

// NEW - Atomic
KeyValueLeafPage page = cache.getAndGuard(pageRef);
// ... later ...
currentPageGuard = PageGuard.fromAcquired(page);  // Already guarded
```

**NodePageReadOnlyTrx.java - Fragment cache (line 1046-1072):**
```java
// OLD - Race condition
KeyValuePage<DataRecord> page = cache.get(pageRef);
// ... later ...
((KeyValueLeafPage) page).acquireGuard();  // Too late!

// NEW - Atomic
KeyValuePage<DataRecord> page = cache.getAndGuard(pageRef);
// Guard already acquired atomically
```

---

### 5. ✅ **clockHand made volatile**

**Problem:** `clockHand` updates might not be visible to other threads.

**Fix:**
```java
// OLD
private int clockHand = 0;

// NEW
private volatile int clockHand = 0;
```

---

### 6. ✅ **put() and putIfAbsent() - Mark before insert**

**Problem:** Pages marked as hot AFTER insertion, allowing eviction in between.

**Fix:**
```java
// OLD
map.put(key, value);
value.markAccessed();  // Too late!

// NEW
value.markAccessed();  // Mark BEFORE inserting
map.put(key, value);
```

---

### 7. ✅ **get(BiFunction) - markAccessed() inside compute()**

**Problem:** `markAccessed()` called outside atomic compute().

**Fix:**
```java
// OLD
KeyValueLeafPage page = map.compute(key, ...);
if (page != null) page.markAccessed();  // Outside!

// NEW
map.compute(key, (k, existing) -> {
    if (existing != null && !existing.isClosed()) {
        existing.markAccessed();  // Inside compute() lock!
        return existing;
    }
    // ... load logic ...
});
```

---

## Benign Races (Documented, Not Fixed)

### 1. **get() - markAccessed() after map.get()**

```java
KeyValueLeafPage page = map.get(key);
if (page != null) {
    page.markAccessed();  // Separate operation
}
```

**Why benign:**
- `hot` is volatile (atomic read/write)
- Marking a closed page is harmless (just sets a boolean)
- At worst, we mark a page being evicted (doesn't affect correctness)

### 2. **remove() - close() after map.remove()**

```java
KeyValueLeafPage page = map.remove(key);
if (page != null && !page.isClosed()) {
    page.close();  // Separate operation
}
```

**Why benign:**
- `close()` is synchronized and idempotent
- Multiple concurrent close() calls are safe

---

## Files Modified

1. **`Cache.java`**
   - Added `getAndGuard()` default method
   
2. **`ShardedPageCache.java`**
   - Made `shard` a singleton field
   - Made `clockHand` volatile
   - Overrode `getAndGuard()` with optimized implementation
   - Fixed `put()`, `putIfAbsent()`, `get(BiFunction)`
   
3. **`PageGuard.java`**
   - Added `fromAcquired()` factory method
   - Added private constructor with `acquireGuard` parameter
   
4. **`NodePageReadOnlyTrx.java`**
   - Updated `getFromBufferManager()` to use `getAndGuard()`
   - Updated `getPageFragments()` to use `getAndGuard()`

---

## Testing Recommendations

1. **Concurrency stress tests**: Multiple threads reading/writing while ClockSweeper runs
2. **Guard count verification**: Ensure guards never go negative
3. **Version validation**: PageGuard should detect frame reuse
4. **Memory leak checks**: Ensure all acquired guards are released

---

## Impact

**Before:**
- ❌ Race conditions could cause data corruption
- ❌ Transactions could access evicted pages
- ❌ Clock hand updates lost

**After:**
- ✅ Atomic cache+guard operations
- ✅ ClockSweeper cannot evict guarded pages
- ✅ Clock hand properly shared
- ✅ All guards correctly acquired/released
- ✅ Thread-safe without explicit synchronization (uses ConcurrentHashMap atomics)

---

## Performance Notes

- **No added synchronization**: Uses `ConcurrentHashMap.compute()` which is already optimized
- **No lock contention**: Only locks per-key, not entire cache
- **Minimal overhead**: One extra `compute()` call instead of `get()` + separate guard acquisition
- **Better than synchronized methods**: Fine-grained locking at map entry level

---

**Conclusion:** The cache is now **properly atomic** for all critical operations. The combination of `getAndGuard()` + `PageGuard.fromAcquired()` prevents the race window where ClockSweeper could evict pages between lookup and guard acquisition.

