# Memory Optimization Plan v2 - Based on Profiling Data

## Executive Summary

Profiling with async-profiler reveals the **remaining allocation hotspots** after Phases 1-5.
This plan addresses them with production-ready, correct implementations.

---

## Profiling Results Summary

### Top Allocators (by sample count)

| Rank | Type | Samples | Source |
|------|------|---------|--------|
| 1 | `byte[]` | 815 | I/O, decompression, serialization |
| 2 | `int[]` | 292 | Internal arrays |
| 3 | `NativeMemorySegmentImpl` | 163 | Off-heap allocations |
| 4 | `IndexLogKey` | 120 | Cache key objects |
| 5 | `long[]` | 116 | Internal arrays |
| 6 | `DataRecord[]` | 100 | Node arrays |
| 7 | `GrowingMemorySegment` | 47 | Serialization buffers |
| 8 | `PageReference` | 43 | Page refs |
| 9 | `PageGuard` | 38 | Guard objects |
| 10 | `LongArrayList` | 35 | Node attribute lists |

### byte[] Allocation Call Stacks (Primary Sources)

1. **`AbstractReader.deserialize` → `InputStream.readAllBytes`** (58 samples)
   - LZ4 Java decompression creates intermediate buffers
   - `readAllBytes()` allocates new array per call

2. **`FileChannelReader.read` → `ByteBuffer.allocate`** (6 samples)
   - Per-read ByteBuffer allocation

3. **`LZ4BlockInputStream.refill`** (15 samples)
   - LZ4 Java library internal buffer allocation

---

## Phase 6: Eliminate InputStream.readAllBytes() Allocation

### Problem
```java
// Current in AbstractReader.deserialize():
byte[] decompressedBytes;
try (var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page))) {
    decompressedBytes = inputStream.readAllBytes();  // ALLOCATES NEW byte[] EVERY TIME
}
```

### Solution
Use a **ThreadLocal reusable buffer** for decompression output:

```java
private static final ThreadLocal<byte[]> DECOMPRESS_BUFFER = 
    ThreadLocal.withInitial(() -> new byte[128 * 1024]);

byte[] decompressedBytes;
try (var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page))) {
    byte[] buffer = DECOMPRESS_BUFFER.get();
    int totalRead = 0;
    int bytesRead;
    while ((bytesRead = inputStream.read(buffer, totalRead, buffer.length - totalRead)) != -1) {
        totalRead += bytesRead;
        if (totalRead == buffer.length) {
            // Grow buffer if needed
            buffer = Arrays.copyOf(buffer, buffer.length * 2);
            DECOMPRESS_BUFFER.set(buffer);
        }
    }
    decompressedBytes = Arrays.copyOf(buffer, totalRead);  // Only allocate exact size needed
}
```

**Wait** - this still allocates. Better approach: **Read directly into MemorySegment**.

### Better Solution: Direct Decompression to MemorySegment

Since we already have `FFILz4Compressor` with native LZ4 that decompresses directly to MemorySegment,
we should use that path exclusively and remove the Java LZ4 fallback path.

**Implementation:**
1. Ensure `FFILz4Compressor.decompress(MemorySegment)` is always used
2. The ThreadLocal buffer in FFILz4Compressor already handles this
3. Remove/bypass `LZ4BlockInputStream` code path

**Files to modify:**
- `io/sirix/io/AbstractReader.java`
- `io/sirix/io/bytepipe/ByteHandler.java`

### Impact: **High** - Eliminates ~60 byte[] allocations per page read

---

## Phase 7: Pool IndexLogKey Objects

### Problem
```java
// Created per cache lookup:
new IndexLogKey(recordPageKey, revision, indexType)  // 120 allocations in profile
```

### Solution
Use a **flyweight pattern** with interning or primitive-based keys:

**Option A: Primitive composite key**
```java
// Replace IndexLogKey with long composite key
long key = ((long)recordPageKey << 32) | ((long)revision << 8) | indexType;
```

**Option B: ThreadLocal pool**
```java
private static final ThreadLocal<IndexLogKey> REUSABLE_KEY = 
    ThreadLocal.withInitial(IndexLogKey::new);

IndexLogKey key = REUSABLE_KEY.get();
key.set(recordPageKey, revision, indexType);
return cache.get(key, ...);
```

**Note:** Option A is cleaner but requires cache key type change.
Option B works but requires careful handling (key must be copied before storing).

### Recommended: Option A (primitive composite key)

**Files to modify:**
- `io/sirix/cache/IndexLogKey.java` - Add `toLong()` method
- `io/sirix/cache/PageCache.java` - Use `Long` keys internally

### Impact: **Medium** - Eliminates 120 object allocations per test run

---

## Phase 8: Pool GrowingMemorySegment for Serialization

### Problem
```java
// Created per serialization:
new GrowingMemorySegment(initialCapacity)  // 47 allocations
```

### Solution
Use **ThreadLocal pooled segments** for serialization:

```java
private static final ThreadLocal<GrowingMemorySegment> SERIALIZATION_BUFFER = 
    ThreadLocal.withInitial(() -> new GrowingMemorySegment(64 * 1024));

// In serialization code:
GrowingMemorySegment buffer = SERIALIZATION_BUFFER.get();
buffer.clear();  // Reset position to 0
// ... use buffer for serialization ...
```

**Files to modify:**
- `io/sirix/node/Bytes.java` - Add pooled buffer accessor
- `io/sirix/page/PageKind.java` - Use pooled buffers in `serializePage`

### Impact: **Medium** - Eliminates 47 object allocations per test run

---

## Phase 9: Optimize FileChannelReader Buffer Allocation

### Problem
```java
// In FileChannelReader.read():
ByteBuffer buffer = ByteBuffer.allocate(size);  // Per-read allocation
```

### Solution
Use **direct ByteBuffer pool** or reuse buffers:

```java
private static final ThreadLocal<ByteBuffer> READ_BUFFER = 
    ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(128 * 1024));

ByteBuffer buffer = READ_BUFFER.get();
buffer.clear();
if (buffer.capacity() < size) {
    buffer = ByteBuffer.allocateDirect(size);
    READ_BUFFER.set(buffer);
}
buffer.limit(size);
channel.read(buffer, position);
```

**Files to modify:**
- `io/sirix/io/filechannel/FileChannelReader.java`

### Impact: **Low-Medium** - Eliminates per-read ByteBuffer allocations

---

## Implementation Order (by impact)

| Priority | Phase | Effort | Impact | Risk |
|----------|-------|--------|--------|------|
| 1 | 6 | Medium | High | Low |
| 2 | 7 | Low | Medium | Low |
| 3 | 8 | Low | Medium | Low |
| 4 | 9 | Low | Low | Low |

---

## Safety Measures

1. **Thread safety**: All ThreadLocal buffers are thread-confined
2. **Memory bounds**: Buffers grow but don't shrink (prevents thrashing)
3. **Cleanup methods**: Add `clearBuffers()` for explicit cleanup
4. **Fallback paths**: Keep original code path for edge cases
5. **Tests**: Run full test suite after each phase

---

## Expected Results

After all phases:
- **byte[] allocations**: Reduced by ~80%
- **Object allocations**: Reduced by ~50%
- **GC pressure**: Significantly reduced
- **Throughput**: 10-30% improvement for read-heavy workloads

---

## Verification

After implementation, re-run profiler:
```bash
export ASYNC_PROFILER=/tmp/async-profiler-4.0-linux-x64
export PROFILE_EVENT=alloc
export PROFILE_OUTPUT=/tmp/sirix-alloc-after.txt
./gradlew :sirix-core:test --tests "io.sirix.axis.concurrent.ConcurrentAxisTest" --no-daemon
```

Compare before/after allocation counts.

