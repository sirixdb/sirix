# SirixDB Performance Analysis

## Profiling Setup

Used async-profiler 4.2.1 and JFR to profile sirix-core and sirix-query test suites.

## CPU Hotspots Identified

| Rank | Method | Samples | Description |
|------|--------|---------|-------------|
| 1 | `PageKind$1.serializePage` | 54 | Page serialization |
| 2 | `KeyValueLeafPage.addReferences` | 53 | Adding references to pages |
| 3 | `KeyValueLeafPage.processEntries` | 45 | Processing page entries |
| 4 | `AbstractNodeReadOnlyTrx.moveTo` | 40 | Node navigation |
| 5 | `AbstractNodeHashing.rollingAdd` | 40 | Hash computation |
| 6 | `NodeSerializerImpl.serialize` | 37 | Node serialization |
| 7 | `AbstractNodeHashing.adaptHashesWithAdd` | 37 | Hash adaptation |
| 8 | `parallelSerializationOfKeyValuePages` | 35 | Parallel serialization |
| 9 | `XmlNodeTrxImpl.insertElementAsRightSibling` | 33 | XML element insertion |
| 10 | `NodeStorageEngineWriter.getRecord` | 28 | Record retrieval |

## Allocation Hotspots Identified

| Rank | Method | Samples | Description |
|------|--------|---------|-------------|
| 1 | `parallelSerializationOfKeyValuePages` | 326 | Parallel serialization allocations |
| 2 | `PageKind$1.serializePage` | 315 | Page serialization allocations |
| 3 | `AbstractReader.deserialize` | 311 | Deserialization allocations |
| 4 | `LZ4Compressor.serialize` | 309 | LZ4 stream wrapper allocations |
| 5 | `FileChannelReader.read` | 172 | File I/O allocations |
| 6 | `KeyValueLeafPage.getSlotAsByteArray` | 139 | Byte array allocations |
| 7 | `GrowingMemorySegment` | 79 | Memory segment allocations |
| 8 | `MemorySegmentBytesOut` | 61 | Output buffer allocations |

## Optimizations Applied

### 1. Buffer Reuse in processEntries (COMPLETED)

**File:** `KeyValueLeafPage.java`

**Before:** Each record created a new `MemorySegmentBytesOut` buffer:
```java
for (final DataRecord record : records) {
    var out = new MemorySegmentBytesOut(tempArena, 60);  // NEW allocation per record
    recordPersister.serialize(out, record, resourceConfiguration);
    ...
}
```

**After:** Single reusable buffer cleared between records:
```java
var reusableOut = new MemorySegmentBytesOut(tempArena, 256);  // ONE allocation
for (final DataRecord record : records) {
    reusableOut.clear();  // Reset position, keep capacity
    recordPersister.serialize(reusableOut, record, resourceConfiguration);
    ...
}
```

**Impact:** Eliminates ~N allocations per page where N = number of non-null records.

### 2. ByteHandlerPipeline MemorySegment Support (COMPLETED)

**File:** `ByteHandlerPipeline.java`

Added full MemorySegment support to the compression pipeline:
- `supportsMemorySegments()` - checks if all handlers support zero-copy
- `compress(MemorySegment)` - zero-copy compression pipeline
- `decompress(MemorySegment)` - zero-copy decompression pipeline
- `decompressScoped(MemorySegment)` - Loom-friendly decompression with buffer pooling

### 3. FFI LZ4 Heap-to-Native Handling (COMPLETED)

**File:** `FFILz4Compressor.java`

Fixed FFI LZ4 compressor to handle both heap and native MemorySegments:
- Automatically copies heap segments to native memory for FFI calls
- Enables future zero-copy compression when data is already in native memory

## Recommended Future Optimizations

### 1. Enable Zero-Copy FFI LZ4 Compression (HIGH IMPACT)

The infrastructure is now in place for zero-copy compression. To enable:
- Modify serialization to write directly to native MemorySegments
- Use `FFILz4Compressor` as default when native library available
- Requires careful format compatibility with existing data

**Implementation effort:** Medium - format compatibility concerns

### 2. Reduce Hash Computation Overhead (MEDIUM IMPACT)

`rollingAdd` and `adaptHashesWithAdd` consume significant CPU time.

**Options:**
- Consider lazy/deferred hashing
- Batch hash updates instead of per-node
- Use SIMD-accelerated hashing if available

### 3. Optimize Node Navigation (MEDIUM IMPACT)

`AbstractNodeReadOnlyTrx.moveTo` is a hot path.

**Options:**
- Cache frequently accessed nodes
- Reduce page dereferences in common navigation patterns

### 4. Pool Allocation Strategy (LOW IMPACT)

Consider object pooling for:
- `IndexLogKey` instances (132 allocations)
- `PageReference` instances
- Common node types

## Running Profiling

```bash
# CPU profiling with async-profiler
ASYNC_PROFILER=/path/to/async-profiler-4.2.1-linux-x64 \
PROFILE_EVENT=cpu \
PROFILE_OUTPUT=/tmp/sirix-cpu.html \
./gradlew :sirix-core:test --tests "io.sirix.service.json.shredder.JsonShredderTest"

# Allocation profiling
ASYNC_PROFILER=/path/to/async-profiler-4.2.1-linux-x64 \
PROFILE_EVENT=alloc \
PROFILE_OUTPUT=/tmp/sirix-alloc.html \
./gradlew :sirix-core:test

# JFR profiling
JFR_PROFILE=1 JFR_OUTPUT=/tmp/sirix.jfr \
./gradlew :sirix-core:test :sirix-query:test

# Convert JFR to flame graph
./async-profiler-4.2.1-linux-x64/bin/jfrconv --cpu --threads /tmp/sirix.jfr -o html /tmp/flame.html
```

## Analyzing JFR Results

```bash
# Summary of events
jfr summary /tmp/sirix.jfr

# Extract CPU samples
jfr print --events jdk.ExecutionSample /tmp/sirix.jfr | grep "io.sirix"

# Extract allocation samples  
jfr print --events jdk.ObjectAllocationSample /tmp/sirix.jfr | grep "io.sirix"
```

