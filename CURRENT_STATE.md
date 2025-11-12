# Current State: INCOMPLETE / BROKEN

## ⚠️ Branch Status: NON-FUNCTIONAL

Branch: `refactor/buffer-manager-guards`  
Status: **Infrastructure in place, integration incomplete**

---

## What's Broken Right Now

### 1. ❌ Guards Not Used Consistently
**Problem**: Guards only acquired in ONE place (`setMostRecentlyReadRecordPage`)

**What's missing**:
- Guards not acquired in `getFromBufferManager()`
- Guards not acquired in fragment loading
- Guards not acquired for PATH_SUMMARY pages
- Guards not acquired for swizzled pages
- Many code paths load pages WITHOUT guards

**Result**: Pages can be evicted while being accessed (use-after-free potential)

### 2. ❌ No Eviction Running
**Problem**: Replaced Caffeine with ShardedPageCache but no ClockSweeper threads started

**What's missing**:
- No background eviction threads
- Memory will grow unbounded
- Pages never recycled

**Result**: Memory leak - all pages stay in cache forever

### 3. ❌ Caffeine Weigher Issue (if we kept Caffeine)
**Problem**: Weights calculated once at insertion, never recalculated

**Issue**: 
```java
// Page inserted with guard active
cache.put(ref, page); // weight = 0 (guardCount=1)
// Guard released later
guard.close(); // guardCount=0, but Caffeine still thinks weight=0
// Page NEVER evicted because Caffeine cached the zero weight!
```

---

## What IS Working

✅ Infrastructure classes exist:
- `PageGuard` class
- `RevisionEpochTracker` class  
- `ShardedPageCache` class
- `ClockSweeper` class
- Guard count on `PageReference`
- Version counter on `KeyValueLeafPage`

✅ Code compiles

❌ But it doesn't actually work yet

---

## What Needs to Happen

### Critical Path (Must Complete):

#### 1. Revert to Caffeine with Guard Checks (SAFER)
**Option A**: Go back to commit before ShardedPageCache replacement
- Caffeine works with guard count checks in eviction listener
- Guards prevent closing while active
- Add guards consistently everywhere
- Test and validate
- THEN consider replacing Caffeine later

#### 2. Complete ShardedPageCache Integration (HARDER)
**Option B**: Finish what we started
- Start ClockSweeper threads in AbstractResourceSession
- Add guards consistently in ALL page access paths:
  - `getFromBufferManager()`
  - `loadDataPageFragments()`
  - Fragment loading in `readPage()`
  - PATH_SUMMARY special cases
  - Swizzled page access
- Test and debug
- Much more work, higher risk

---

## Recommendation

**REVERT to Caffeine + guard checks** (commit 730a9e89e - before ShardedPageCache replacement)

Why:
1. That version had guard checks in eviction listeners (wouldn't close guarded pages)
2. Caffeine's LRU eviction works (even if not optimal)
3. Can add guards consistently with stable eviction
4. Test incrementally
5. Replace Caffeine later once guards work

Current state with ShardedPageCache but no sweepers and incomplete guards is unusable.

---

## Command to Revert

```bash
git reset --hard 730a9e89e  # Caffeine + guard checks, before ShardedPageCache
```

Then next steps:
1. Add guards consistently in all page access paths
2. Test thoroughly
3. Once stable, THEN replace Caffeine with ShardedPageCache + sweepers

---

## The Real Issue

I tried to do too much at once:
- ❌ Remove pinning
- ❌ Add guards  
- ❌ Replace Caffeine
- ❌ Add ClockSweeper

Should have been:
1. Remove pinning, add guards, TEST
2. Then replace Caffeine, TEST
3. Then optimize eviction

---

**Decision Point**: Revert to Caffeine+guards, or push forward with ShardedPageCache+sweepers?

