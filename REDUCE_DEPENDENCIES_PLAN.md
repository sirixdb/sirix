# Plan: Reduce SirixDB Dependencies

## Executive Summary

SirixDB currently ships **31 compile-scope external dependencies** in sirix-core alone.
This plan removes or internalizes **12 dependencies** while preserving (and in some cases
improving) performance, latency, and throughput. Every replacement uses JDK 25 built-ins
or tiny, zero-allocation internal utilities.

---

## Dependency Inventory & Verdict

| # | Dependency | Files | Verdict | Rationale |
|---|-----------|-------|---------|-----------|
| 1 | `aspectjrt` | 0 | **REMOVE** | Completely unused |
| 2 | `jcommander` | 0 | **REMOVE** | Completely unused |
| 3 | `jsoup` | 0 | **REMOVE** | Completely unused |
| 4 | `guava-testlib` | 0 | **REMOVE** | Completely unused |
| 5 | `perfidix` | 5 (test) | **REMOVE** | Test-only; JMH benchmarks already exist |
| 6 | `xmlunit` | 11 (test) | **MOVE TO TEST** | Already test-only but declared as `api` |
| 7 | `guava` | 148 | **REMOVE** | All usages replaceable with JDK 25 + small internal utils |
| 8 | `gson` | 28 | **REMOVE** | Replace with Jackson (already a dependency, faster streaming) |
| 9 | `dagger` + `dagger-compiler` | 26 | **REMOVE** | Simple DI graph; replace with manual factories |
| 10 | `google-tink` | 2 | **REMOVE** | Replace with JDK `javax.crypto` (AES-GCM streaming) |
| 11 | `brownies-collections` | 2 | **REMOVE** | `GapList` used in 2 files; replace with `ArrayList` + gap logic or fastutil |
| 12 | `fast-object-pool` | 2 | **REPLACE** | Replace with lock-free internal pool (~60 lines) |

### Dependencies to KEEP (with justification)

| Dependency | Why Keep |
|-----------|----------|
| `brackit` | IS the query engine (1,439 files). Non-negotiable. |
| `jackson-core` | Streaming JSON parser. Already minimal (jackson-core only, no databind). Fastest streaming parser on JVM. |
| `fastutil` | Primitive collections critical for zero-GC hot paths. No JDK equivalent. |
| `roaringbitmap` | Compressed bitmap indexes. Specialized, no JDK equivalent. |
| `caffeine` | Weight-based eviction, async caches, removal listeners. Highest-perf cache on JVM. |
| `lz4-java` | Compression codec. Native performance, no JDK equivalent. |
| `snappy-java` | Compression codec. Native performance, no JDK equivalent. |
| `JavaFastPFOR` | Integer compression for posting lists. Specialized, no equivalent. |
| `zero-allocation-hashing` | xxHash/murmur3 with zero allocations. Critical for hot-path hashing. |
| `slf4j-api` | Logging facade. Industry standard, tiny. |
| `logback-classic/core` | Logging implementation. Could be swapped but not worth the churn. |
| `checker-framework` | `@Nullable`/`@NonNull` annotations in 273 files. Zero runtime cost. |
| `jasyncfio` (io_uring) | Optional Linux async I/O backend. Already pluggable. |
| Vert.x stack | REST API module only. Not in sirix-core. |
| Kotlin stdlib/coroutines | Kotlin modules only. Not in sirix-core. |

---

## Detailed Implementation Plan

### Phase 1: Dead Dependencies (Zero Code Changes)

**Remove 4 completely unused dependencies.**

#### 1.1 Remove `aspectjrt`
- **Action**: Delete from `libraries.gradle` and `sirix-core/build.gradle`
- **Risk**: None. Zero imports found anywhere.
- **Files changed**: 2 (build files only)

#### 1.2 Remove `jcommander`
- **Action**: Delete from `libraries.gradle` and `sirix-core/build.gradle`
- **Risk**: None. Zero imports found anywhere.
- **Files changed**: 2 (build files only)

#### 1.3 Remove `jsoup`
- **Action**: Delete from `libraries.gradle` and `sirix-rest-api/build.gradle`
- **Risk**: None. Zero imports found anywhere.
- **Files changed**: 2 (build files only)

#### 1.4 Remove `guava-testlib`
- **Action**: Delete from `libraries.gradle` and `sirix-core/build.gradle`
- **Risk**: None. Zero imports found anywhere.
- **Files changed**: 2 (build files only)

---

### Phase 2: Test-Only Dependency Fixes

#### 2.1 Move `xmlunit` from `api` to `testImplementation`
- **Action**: Change scope in `sirix-core/build.gradle` from `api` to `testImplementation`
- **Risk**: None. Only used in 11 test files.
- **Files changed**: 1

#### 2.2 Move `perfidix` from `api` to `testImplementation` (or remove)
- **Action**: Change scope to `testImplementation`. Consider removing entirely since JMH benchmarks (sirix-benchmarks module) are the proper benchmarking solution.
- **Risk**: Low. Only used in 5 test benchmark files. Those tests can be converted to JMH or deleted.
- **Files changed**: 1 (build file) + optionally 5 test files if removing

---

### Phase 3: Remove Google Guava (148 files)

This is the largest change. Guava is used for utilities that JDK 25 provides natively.

#### 3.1 Replace `MoreObjects.toStringHelper()` (71 files)

**Current pattern:**
```java
import com.google.common.base.MoreObjects;
// ...
return MoreObjects.toStringHelper(this)
    .add("field1", field1)
    .add("field2", field2)
    .toString();
```

**Replacement**: Create a minimal internal `ToStringHelper` (~30 lines):
```java
package io.sirix.utils;

public final class ToStringHelper {
    private final StringBuilder sb;
    private boolean first = true;

    private ToStringHelper(final String className) {
        sb = new StringBuilder(64);
        sb.append(className).append('{');
    }

    public static ToStringHelper of(final Object self) {
        return new ToStringHelper(self.getClass().getSimpleName());
    }

    public ToStringHelper add(final String name, final Object value) {
        if (!first) sb.append(", ");
        first = false;
        sb.append(name).append('=').append(value);
        return this;
    }

    public ToStringHelper add(final String name, final long value) {
        if (!first) sb.append(", ");
        first = false;
        sb.append(name).append('=').append(value);
        return this;
    }

    @Override public String toString() {
        return sb.append('}').toString();
    }
}
```

**Performance**: Equivalent or better (pre-sized StringBuilder, primitive overloads).
**Corner cases**: Must handle `null` values in `add()` — print "null" like Guava does.

#### 3.2 Replace `Preconditions.checkArgument/checkNotNull` (87 usages)

**Replacement**:
- `Preconditions.checkNotNull(x)` → `java.util.Objects.requireNonNull(x)`
- `Preconditions.checkNotNull(x, msg)` → `java.util.Objects.requireNonNull(x, msg)`
- `Preconditions.checkArgument(cond)` → `if (!cond) throw new IllegalArgumentException()`
- `Preconditions.checkArgument(cond, msg)` → `if (!cond) throw new IllegalArgumentException(msg)`

Create a tiny utility if needed:
```java
package io.sirix.utils;

public final class Preconditions {
    public static void checkArgument(final boolean expression) {
        if (!expression) throw new IllegalArgumentException();
    }
    public static void checkArgument(final boolean expression, final String message) {
        if (!expression) throw new IllegalArgumentException(message);
    }
}
```

**Performance**: Identical (Guava does the same thing internally).

#### 3.3 Replace `ImmutableSet/ImmutableList/ImmutableMap` (28 usages)

**Replacement**: JDK 9+ factory methods:
- `ImmutableSet.of(a, b)` → `Set.of(a, b)`
- `ImmutableList.of(a, b)` → `List.of(a, b)`
- `ImmutableMap.of(k, v)` → `Map.of(k, v)`
- `ImmutableSet.Builder` → `Stream.collect(Collectors.toUnmodifiableSet())`

**Corner cases**:
- `Set.of()` throws on `null` elements (same as Guava `ImmutableSet`). Safe.
- `Set.of()` throws on duplicates (Guava silently deduplicates). Must verify no duplicate inputs exist.
- Return type is `Set<T>` not `ImmutableSet<T>`. Any code checking `instanceof ImmutableSet` will break — search for this.

#### 3.4 Replace `BiMap/HashBiMap` in `LocalDatabase.java`

**Current usage**:
```java
BiMap<Long, String> resourceIDsToResourceNames = Maps.synchronizedBiMap(HashBiMap.create());
resourceIDsToResourceNames.forcePut(id, name);
resourceIDsToResourceNames.inverse().get(name);
resourceIDsToResourceNames.inverse().remove(name);
```

**Replacement**: Internal `SynchronizedBiMap<K, V>` (~50 lines) backed by two `ConcurrentHashMap`s:
```java
package io.sirix.utils;

public final class SynchronizedBiMap<K, V> {
    private final ConcurrentHashMap<K, V> forward = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<V, K> inverse = new ConcurrentHashMap<>();

    public synchronized void forcePut(final K key, final V value) {
        // Remove old mappings in both directions
        final V oldValue = forward.remove(key);
        if (oldValue != null) inverse.remove(oldValue);
        final K oldKey = inverse.remove(value);
        if (oldKey != null) forward.remove(oldKey);
        forward.put(key, value);
        inverse.put(value, key);
    }

    public V get(final K key) { return forward.get(key); }
    public K inverseGet(final V value) { return inverse.get(value); }
    public synchronized void inverseRemove(final V value) {
        final K key = inverse.remove(value);
        if (key != null) forward.remove(key);
    }
}
```

**Corner cases**:
- `forcePut` must atomically remove old mappings in both directions before inserting.
- Must be `synchronized` for `forcePut`/`inverseRemove` but reads can be lock-free via `ConcurrentHashMap`.
- Thread-safety matches Guava's `Maps.synchronizedBiMap()` behavior.

#### 3.5 Replace `Hashing.sha256()` (2 files: `Reader.java`, `FileReader.java`)

**Replacement**: JDK `MessageDigest`:
```java
private static final MessageDigest SHA256 = MessageDigest.getInstance("SHA-256");
// Or for thread-safety, create per-call:
byte[] hash = MessageDigest.getInstance("SHA-256").digest(data);
```

**Performance consideration**: `MessageDigest.getInstance()` is cached by the JVM. For hot paths, clone a template instance:
```java
private static final MessageDigest TEMPLATE = MessageDigest.getInstance("SHA-256");
// In hot path:
MessageDigest md = (MessageDigest) TEMPLATE.clone();
md.update(buffer);
byte[] hash = md.digest();
```

**Corner cases**:
- Guava's `HashFunction` is thread-safe and stateless. JDK `MessageDigest` is NOT thread-safe.
  Must use `ThreadLocal<MessageDigest>` or clone-per-use pattern.
- Verify the hash output format matches (both produce `byte[]`).

#### 3.6 Replace `ForwardingObject` (6 files)

**Replacement**: Simple abstract class:
```java
package io.sirix.utils;

public abstract class ForwardingObject {
    protected abstract Object delegate();

    @Override public String toString() { return delegate().toString(); }
}
```

This is literally all Guava's `ForwardingObject` does. 4 lines of code.

#### 3.7 Replace `AbstractIterator` (5 files)

**Replacement**: Internal abstract class (~25 lines):
```java
package io.sirix.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractIterator<T> implements Iterator<T> {
    private enum State { READY, NOT_READY, DONE, FAILED }
    private State state = State.NOT_READY;
    private T next;

    protected abstract T computeNext();

    protected final T endOfData() {
        state = State.DONE;
        return null;
    }

    @Override public final boolean hasNext() {
        switch (state) {
            case READY: return true;
            case DONE: return false;
            default:
        }
        state = State.FAILED;
        next = computeNext();
        if (state != State.DONE) {
            state = State.READY;
            return true;
        }
        return false;
    }

    @Override public final T next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.NOT_READY;
        final T result = next;
        next = null;
        return result;
    }
}
```

**Performance**: Identical to Guava's implementation. Zero allocations per iteration.

#### 3.8 Replace `ComparisonChain` (1 file: `CASValue.java`)

**Replacement**: `Comparator.comparing().thenComparing()` chain or manual compare:
```java
int result = Integer.compare(a.x, b.x);
if (result != 0) return result;
return Integer.compare(a.y, b.y);
```

#### 3.9 Replace `Guava Optional` (in sirix-fs, sirix-gui — legacy modules)

**Replacement**: `java.util.Optional` (direct drop-in):
- `Optional.absent()` → `Optional.empty()`
- `Optional.of(x)` → `Optional.of(x)`
- `Optional.fromNullable(x)` → `Optional.ofNullable(x)`

These are legacy/inactive modules but should be cleaned up.

#### 3.10 Replace `ByteArrayDataInput/Output` (1 file: `Utils.java`)

**Replacement**: `java.nio.ByteBuffer` (already used extensively in sirix):
```java
ByteBuffer buf = ByteBuffer.wrap(bytes);
int value = buf.getInt();
```

---

### Phase 4: Remove Google Gson (28 files)

Replace all Gson streaming usage with Jackson streaming (already a dependency).

#### 4.1 Mapping Table

| Gson Class | Jackson Equivalent |
|-----------|-------------------|
| `com.google.gson.stream.JsonReader` | `com.fasterxml.jackson.core.JsonParser` |
| `com.google.gson.stream.JsonWriter` | `com.fasterxml.jackson.core.JsonGenerator` |
| `com.google.gson.stream.JsonToken` | `com.fasterxml.jackson.core.JsonToken` |
| `com.google.gson.JsonObject` | Build with `JsonGenerator` or use `ObjectNode` (would need jackson-databind) |
| `com.google.gson.JsonArray` | Build with `JsonGenerator` or use `ArrayNode` |
| `com.google.gson.JsonParser` | `JsonFactory.createParser(string)` |
| `com.google.gson.JsonElement` | Not needed with streaming |

#### 4.2 Strategy

**For streaming reads** (13+ files using `JsonReader`/`JsonToken`):
- Direct 1:1 replacement with Jackson `JsonParser`/`JsonToken`
- `JacksonJsonShredder.java` already demonstrates the pattern
- Token names differ slightly: `JsonToken.BEGIN_OBJECT` → `JsonToken.START_OBJECT`

**For `JsonObject`/`JsonArray` construction** (5 files: `JsonDiffSerializer`, `JsonResourceCopy`, `DiffHandler`, etc.):
- Replace with Jackson streaming `JsonGenerator` writing to `StringWriter`
- Or add `jackson-databind` (single additional jar) for `ObjectNode`/`ArrayNode`
- **Recommendation**: Use streaming `JsonGenerator` to avoid adding jackson-databind

**For configuration serialization** (2 files: `DatabaseConfiguration`, `ResourceConfiguration`):
- Replace `JsonWriter` with Jackson `JsonGenerator`
- Replace `JsonReader` with Jackson `JsonParser`

#### 4.3 Corner Cases

- **Lenient parsing**: Gson's `JsonReader.setLenient(true)` allows comments, unquoted names. Jackson supports these via `JsonParser.Feature` flags. `JacksonJsonShredder` already configures these.
- **Number handling**: Gson reads numbers as strings by default; Jackson provides typed `getIntValue()`, `getLongValue()`, etc. Must verify each call site.
- **Peek vs. next**: Gson uses `reader.peek()` to look ahead; Jackson uses `parser.nextToken()` and `parser.currentToken()`. Different state machine — each call site needs careful review.
- **Auto-closing**: Both auto-close underlying streams. Behavior is identical.
- **String interning**: Jackson can intern field names (`JsonFactory.Feature.INTERN_FIELD_NAMES`). This is a performance win for sirix's repeated field patterns.

#### 4.4 Performance Impact

**Positive**: Jackson streaming is ~20-40% faster than Gson streaming in benchmarks (particularly for large documents). This directly benefits JSON shredding throughput.

---

### Phase 5: Remove Dagger (26 files)

Replace compile-time DI with manual factory pattern.

#### 5.1 Current Dagger Architecture

```
DatabaseManager (@Component, @Singleton)
  └─ DatabaseModule (@Module)
       ├─ JsonLocalDatabaseComponent (@Subcomponent, @DatabaseScope)
       │    ├─ JsonLocalDatabaseModule
       │    ├─ LocalDatabaseModule
       │    └─ JsonResourceSessionComponent (@Subcomponent, @ResourceSessionScope)
       │         └─ ResourceSessionModule
       └─ XmlLocalDatabaseComponent (@Subcomponent, @DatabaseScope)
            ├─ XmlLocalDatabaseModule
            ├─ LocalDatabaseModule
            └─ XmlResourceManagerComponent (@Subcomponent, @ResourceSessionScope)
                 └─ ResourceSessionModule
```

#### 5.2 Replacement: Manual Factory Pattern

Create `DatabaseManagerFactory` that replaces `DaggerDatabaseManager`:

```java
public final class DatabaseManagerImpl implements DatabaseManager {
    private final PathBasedPool<Database<?>> databasePool;
    private final LocalDatabaseFactory<JsonResourceSession> jsonFactory;
    private final LocalDatabaseFactory<XmlResourceSession> xmlFactory;

    DatabaseManagerImpl() {
        this.databasePool = new PathBasedPool<>();
        PathBasedPool<ResourceSession<?, ?>> sessionPool = new PathBasedPool<>();
        WriteLocksRegistry writeLocks = new WriteLocksRegistry();
        this.jsonFactory = config -> createJsonDatabase(config, sessionPool, writeLocks);
        this.xmlFactory = config -> createXmlDatabase(config, sessionPool, writeLocks);
    }
    // ... factory methods
}
```

#### 5.3 Files to Modify/Delete

**Delete** (Dagger-only files):
- `io/sirix/dagger/DatabaseScope.java`
- `io/sirix/dagger/ResourceSessionScope.java`
- `io/sirix/dagger/DatabaseName.java`
- `io/sirix/dagger/ResourceName.java`
- `io/sirix/access/DatabaseModule.java`
- `io/sirix/access/LocalDatabaseModule.java`
- `io/sirix/access/ResourceSessionModule.java`
- `io/sirix/access/json/JsonLocalDatabaseModule.java`
- `io/sirix/access/json/JsonLocalDatabaseComponent.java`
- `io/sirix/access/json/JsonResourceSessionModule.java`
- `io/sirix/access/json/JsonResourceSessionComponent.java`
- `io/sirix/access/xml/XmlLocalDatabaseModule.java`
- `io/sirix/access/xml/XmlLocalDatabaseComponent.java`
- `io/sirix/access/xml/XmlResourceManagerModule.java`
- `io/sirix/access/xml/XmlResourceManagerComponent.java`

**Modify**:
- `DatabaseManager.java` — remove `@Component`, add factory method
- `Databases.java` — replace `DaggerDatabaseManager.create()` with `new DatabaseManagerImpl()`
- `LocalJsonDatabaseFactory.java` — remove `@Inject`, accept deps via constructor
- `LocalXmlDatabaseFactory.java` — remove `@Inject`, accept deps via constructor
- `JsonResourceSessionImpl.java` — remove `@Inject`, accept deps via constructor
- `XmlResourceSessionImpl.java` — remove `@Inject`, accept deps via constructor
- `TransactionManagerImpl.java` — remove `@Inject`
- `WriteLocksRegistry.java` — remove `@Inject`
- `StorageEngineWriterFactory.java` — remove `@Inject`

#### 5.4 Corner Cases

- **Scope management**: Dagger ensures `@DatabaseScope` objects are singletons within a database session. Manual factories must replicate this — store scoped instances in the factory/session object.
- **Provider<Builder>**: Dagger's `Provider<JsonLocalDatabaseComponent.Builder>` enables lazy subcomponent creation. Replace with `Supplier<Builder>` or direct factory calls.
- **Thread safety**: Dagger components are thread-safe by default. Manual factories using `ConcurrentHashMap` for caching scoped instances will match this.

#### 5.5 Build Impact

- **Removes annotation processor** (`dagger-compiler`). Faster compilation.
- **Removes generated code** (`DaggerDatabaseManager` class). Smaller JAR.

---

### Phase 6: Remove Google Tink (2 files)

Replace with JDK `javax.crypto` streaming encryption.

#### 6.1 Current Usage

- `LocalDatabase.java`: Creates keyset with `StreamingAeadKeyTemplates.AES256_CTR_HMAC_SHA256_1MB`
- `Encryptor.java`: Encrypts/decrypts using `StreamingAead`

#### 6.2 Replacement

Use JDK's `javax.crypto.CipherInputStream` / `CipherOutputStream` with AES-GCM:

```java
Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(128, iv));
CipherOutputStream cos = new CipherOutputStream(outputStream, cipher);
```

#### 6.3 Corner Cases

- **Key management**: Tink handles key rotation and versioning. JDK crypto does not. Must implement key storage format (or keep using JSON key files with a simple custom format).
- **Streaming**: Tink's `StreamingAead` processes data in segments (1MB default). JDK's `CipherInputStream`/`CipherOutputStream` handle this transparently.
- **IV/nonce generation**: Must use `SecureRandom` for IV generation (Tink does this internally).
- **Backward compatibility**: Existing encrypted databases use Tink's key format. Need a migration path or keep Tink as optional for reading legacy encrypted data.
- **IMPORTANT**: If backward compatibility with existing encrypted databases is required, this should be deferred or done with a migration tool.

---

### Phase 7: Replace brownies-collections (2 files)

#### 7.1 Current Usage

`GapList` used in `BitmapReferencesPage.java` and `SerializationType.java` for page reference lists.

#### 7.2 Replacement

`GapList` is essentially an `ArrayList` optimized for insertions in the middle. Options:
- Use `ArrayList` directly (simpler, sufficient if middle-insertions are rare)
- Use fastutil's `ObjectArrayList` (already a dependency, slightly more memory-efficient)

#### 7.3 Corner Cases

- Verify that `GapList`-specific methods aren't used (e.g., `init()`, gap manipulation)
- Check if middle-insertion performance matters for page references

---

### Phase 8: Replace fast-object-pool (2 files)

#### 8.1 Current Usage

`ObjectPool` used in `AbstractResourceSession.java` for pooling `StorageEngineReader` instances.

#### 8.2 Replacement

Internal lock-free pool using `ConcurrentLinkedDeque` (~40 lines):

```java
package io.sirix.utils;

public final class ObjectPool<T> {
    private final ConcurrentLinkedDeque<T> pool = new ConcurrentLinkedDeque<>();
    private final Supplier<T> factory;
    private final int maxSize;
    private final AtomicInteger size = new AtomicInteger(0);

    public T borrowObject() {
        T obj = pool.pollFirst();
        if (obj != null) return obj;
        if (size.incrementAndGet() <= maxSize) return factory.get();
        size.decrementAndGet();
        throw new PoolExhaustedException();
    }

    public void returnObject(T obj) {
        pool.offerFirst(obj);
    }
}
```

**Performance**: Lock-free via `ConcurrentLinkedDeque`. Lower latency than `fast-object-pool`'s partitioned design for the small pool sizes sirix uses.

#### 8.3 Corner Cases

- Must handle pool exhaustion (current code catches `PoolExhaustedException`)
- Must handle object validation on borrow (check if reader is still valid)
- Must handle pool shutdown (close all pooled readers)

---

## Impact Summary

### Dependencies Removed from sirix-core compile classpath

| Before | After | Removed |
|--------|-------|---------|
| 21 `api` deps | 12 `api` deps | 9 removed |
| 5 `implementation` deps | 3 `implementation` deps | 2 removed |
| 1 `annotationProcessor` | 0 | 1 removed |
| **27 total** | **15 total** | **12 removed** |

### Remaining sirix-core compile deps (15)

1. `jackson-core` (streaming JSON)
2. `brackit` (query engine)
3. `slf4j-api` (logging facade)
4. `logback-classic` (logging impl)
5. `logback-core` (logging impl)
6. `checker-framework` (annotations, zero runtime)
7. `caffeine` (caching)
8. `fastutil` (primitive collections)
9. `roaringbitmap` (bitmap indexes)
10. `lz4-java` (compression)
11. `snappy-java` (compression)
12. `JavaFastPFOR` (integer compression)
13. `zero-allocation-hashing` (hashing)
14. `jasyncfio` (io_uring, optional)
15. `kotlin-stdlib` (if needed by Kotlin modules)

### New Internal Utilities Created (~200 lines total)

1. `io.sirix.utils.ToStringHelper` (~30 lines)
2. `io.sirix.utils.Preconditions` (~10 lines)
3. `io.sirix.utils.ForwardingObject` (~5 lines)
4. `io.sirix.utils.AbstractIterator` (~35 lines)
5. `io.sirix.utils.SynchronizedBiMap` (~50 lines)
6. `io.sirix.utils.ObjectPool` (~40 lines)
7. Encryption utilities (~30 lines)

### Performance Impact

| Change | Impact |
|--------|--------|
| Gson → Jackson streaming | **+20-40% JSON parse throughput** |
| Dagger → manual factories | **Faster startup** (no annotation processing at build time) |
| Guava → JDK utilities | **Neutral** (identical implementations) |
| fast-object-pool → lock-free pool | **Lower latency** for small pool sizes |
| Tink → JDK crypto | **Neutral** (same AES-GCM under the hood) |

### Risk Assessment

| Phase | Risk | Mitigation |
|-------|------|------------|
| Phase 1 (dead deps) | None | No code changes |
| Phase 2 (test scope) | None | Scope change only |
| Phase 3 (Guava) | Low | Mechanical replacement, well-understood patterns |
| Phase 4 (Gson) | Medium | Different streaming API semantics (peek vs next). Each of 28 files needs careful token-state review |
| Phase 5 (Dagger) | Medium | Must preserve scope semantics. Thorough testing of database/session lifecycle required |
| Phase 6 (Tink) | High | Backward compat with encrypted databases. Consider deferring. |
| Phase 7 (brownies) | Low | Direct ArrayList replacement |
| Phase 8 (fast-object-pool) | Low | Small, well-contained change |

---

## Recommended Execution Order

1. **Phase 1** — Dead dependencies (5 min, zero risk)
2. **Phase 2** — Test scope fixes (5 min, zero risk)
3. **Phase 7** — brownies-collections (30 min, low risk)
4. **Phase 8** — fast-object-pool (1 hr, low risk)
5. **Phase 3** — Guava removal (largest, do sub-phases 3.1-3.10 incrementally, test after each)
6. **Phase 4** — Gson → Jackson (careful, file-by-file migration)
7. **Phase 5** — Dagger removal (requires integration testing)
8. **Phase 6** — Tink removal (defer if backward compat needed)

Each phase should be a separate commit with all tests passing.
