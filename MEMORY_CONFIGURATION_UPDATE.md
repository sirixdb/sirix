# Memory Configuration Update for Global BufferManager

## Changes Made

### Increased Default Memory Budget

**Previous (Per-Resource BufferManagers):**
```java
private long maxSegmentAllocationSize = 2L * (1L << 30); // 2GB
```
- Each resource had separate BufferManager
- 2GB was sufficient for single resource

**Current (Global BufferManager):**
```java
private long maxSegmentAllocationSize = 8L * (1L << 30); // 8GB
```
- One BufferManager serves ALL databases and resources
- 8GB shared budget is more appropriate

### Increased Virtual Memory Regions

**Previous:**
```java
private static final long VIRTUAL_REGION_SIZE = 2L * 1024 * 1024 * 1024; // 2GB
// Total virtual: 2GB * 7 classes = 14GB
```

**Current:**
```java
private static final long VIRTUAL_REGION_SIZE = 4L * 1024 * 1024 * 1024; // 4GB
// Total virtual: 4GB * 7 classes = 28GB
```

## Rationale

### Why 8GB Physical Budget?

With **global BufferManager**, the budget is shared across:
- Multiple databases (e.g., db1, db2, db3)
- Multiple resources per database (e.g., users, orders, products)
- All concurrent transactions

**Example Scenario:**
```
Database 1:
  - Resource "users" → uses RecordPageCache
  - Resource "orders" → uses RecordPageCache
  - Resource "products" → uses RecordPageCache

Database 2:
  - Resource "analytics" → uses RecordPageCache
  - Resource "logs" → uses RecordPageCache

All share ONE RecordPageCache with weight: 65_536 * 100 * 8 = 52,428,800
```

8GB allows reasonable headroom for multiple active databases/resources.

### Why 28GB Virtual Memory?

**Virtual ≠ Physical:**
- Virtual: Address space reservation (doesn't use RAM)
- Physical: Actual memory committed (uses RAM)

**Allocation Strategy:**
```
Virtual Region per Class: 4GB
  - 4KB class: 4GB / 4KB = 1,048,576 segments
  - 8KB class: 4GB / 8KB = 524,288 segments
  - 16KB class: 4GB / 16KB = 262,144 segments
  - 32KB class: 4GB / 32KB = 131,072 segments
  - 64KB class: 4GB / 64KB = 65,536 segments
  - 128KB class: 4GB / 128KB = 32,768 segments
  - 256KB class: 4GB / 256KB = 16,384 segments

Total segments available: 2+ million
Physical memory tracked: Capped at 8GB
```

With global BufferManager, having 2+ million segment slots available prevents "pool exhausted" errors during heavy workloads.

## Configuration Output

### Initialization Logs

```
INFO - ========== Initializing UmbraDB-Style Memory Allocator ==========
INFO - Physical memory limit: 8192 MB
INFO - Virtual address space required: 28 GB
INFO - Virtual memory mapped: 28 GB (across 7 size classes)
INFO - Physical memory limit: 8192 MB

INFO - Initializing global BufferManager with memory budget: 8 GB
INFO -   - RecordPageCache weight: 52428800 (scaled 8x)
INFO -   - RecordPageFragmentCache weight: 26214400 (scaled 8x)
INFO -   - PageCache weight: 500000 (fixed)
INFO -   - RevisionRootPageCache size: 5000 (fixed)
INFO -   - RBTreeNodeCache size: 50000 (fixed)
INFO -   - NamesCache size: 500 (fixed)
INFO -   - PathSummaryCache size: 20 (fixed)
```

### Memory Breakdown

**With 8GB Budget:**

| Cache | Type | Scaling | Size | Uses Allocator? |
|-------|------|---------|------|-----------------|
| RecordPageCache | KeyValueLeafPages | 8x | 52,428,800 weight | YES ✅ |
| RecordPageFragmentCache | KeyValueLeafPages | 8x | 26,214,400 weight | YES ✅ |
| PageCache | IndirectPages, etc. | Fixed | 500,000 weight | NO |
| RevisionRootPageCache | RevisionRootPages | Fixed | 5,000 entries | NO |
| RBTreeNodeCache | RB Nodes | Fixed | 50,000 entries | NO |
| NamesCache | Names | Fixed | 500 entries | NO |
| PathSummaryCache | PathSummaryData | Fixed | 20 entries | NO |

Only caches that store MemorySegment-backed pages scale with budget.

## Benefits

### ✅ Appropriate for Global Usage
- 8GB shared across all databases/resources
- Sufficient for production workloads
- Still configurable per deployment

### ✅ Prevents Pool Exhaustion
- 28GB virtual space (2+ million segment slots)
- Reduces "pool exhausted" errors
- Better handling of concurrent workloads

### ✅ Configurable
Users can still adjust:
```java
DatabaseConfiguration dbConfig = new DatabaseConfiguration(path)
    .setMaxSegmentAllocationSize(16L * (1L << 30));  // 16GB for large deployments
```

## Deployment Recommendations

### Small Deployments (1-2 databases, light load)
```java
dbConfig.setMaxSegmentAllocationSize(4L * (1L << 30));  // 4GB
```

### Medium Deployments (Default - multiple databases)
```java
dbConfig.setMaxSegmentAllocationSize(8L * (1L << 30));  // 8GB (default)
```

### Large Deployments (many databases, heavy load)
```java
dbConfig.setMaxSegmentAllocationSize(16L * (1L << 30));  // 16GB
```

### Very Large Deployments (server with lots of RAM)
```java
dbConfig.setMaxSegmentAllocationSize(32L * (1L << 30));  // 32GB
```

## Validation

✅ **Tests Pass:** All integration tests passing with 8GB budget
✅ **Logs Correct:** Shows 8GB physical, 28GB virtual
✅ **Compilation:** Successful
✅ **Architecture:** Aligned with global BufferManager requirements

---

**Status:** ✅ Memory configuration properly sized for global BufferManager
**Default Physical:** 8GB (up from 2GB)
**Default Virtual:** 28GB (up from 14GB)
**Rationale:** Global pool serves all databases/resources, needs larger budget





