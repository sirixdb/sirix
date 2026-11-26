/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.WrongMethodTypeException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * The one place where the MEM step-3 tests (MEM-301, MEM-302) touch the compact-image pool, which
 * does not exist on the baseline (it was withdrawn from PR 1497 and is re-introduced by step 3).
 * The tests assert behavior; this class only binds them to the members the chapter requires, so
 * that they compile today and run the day the pool exists. It follows {@link MemPoolSeam}: a
 * missing member fails the test with a message naming the member, the requirement and the step.
 *
 * <p>
 * The chapter fixes behavior, not names. An implementer binds a different name by editing the
 * string constants below; the assertions of the tests stay as they are. The reflective calls are
 * resolved once into {@link MethodHandle}s of a fixed, generalized type, so a warm lease, release
 * or decode through this seam allocates nothing itself (the allocation-budget test measures a
 * control to prove it).
 *
 * <h2>Seam contract (package {@code io.sirix.cache}, step 3)</h2>
 *
 * <table>
 * <caption>Required members of {@value #POOL_CLASS}; all {@code long} parameters, no
 * boxing</caption>
 * <tr>
 * <th>Member</th>
 * <th>Meaning</th>
 * <th>Required by</th>
 * </tr>
 * <tr>
 * <td>constructor {@code (FrameSlotAllocator allocator, long shareBytes)}</td>
 * <td>the pool and its cache half, charged to {@code allocator}, with the owner's share (the
 * heap-derived {@code HOT_IMAGES} ceiling)</td>
 * <td>MEM-301</td>
 * </tr>
 * <tr>
 * <td>{@code static long slabUnitBytes()}</td>
 * <td>the unit in which the account grows</td>
 * <td>MEM-301</td>
 * </tr>
 * <tr>
 * <td>{@code long shareBytes()}</td>
 * <td>the account's ceiling</td>
 * <td>MEM-301</td>
 * </tr>
 * <tr>
 * <td>{@code long slabBytes()}, {@code long fallbackImageBytes()},
 * {@code long metadataBytes()}</td>
 * <td>the three parts of the one account</td>
 * <td>MEM-301</td>
 * </tr>
 * <tr>
 * <td>{@code long cachedImageBytes()}, {@code int cachedImageCount()},
 * {@code long evictedImageBytes()}</td>
 * <td>what the cache half retains; bytes it evicted so far (monotonic)</td>
 * <td>MEM-301</td>
 * </tr>
 * <tr>
 * <td>{@code Lease lease(long bytes)}</td>
 * <td>a pool lease of {@code bytes}; {@code null} when the account or the allocator refuses (the
 * pool never grows past the share)</td>
 * <td>MEM-301</td>
 * </tr>
 * <tr>
 * <td>{@code Lease.close()}</td>
 * <td>release the lease; the pool keeps its slabs</td>
 * <td>MEM-301, MEM-302</td>
 * </tr>
 * <tr>
 * <td>{@code Decoded decode(long key, long imageBytes)}</td>
 * <td>decode the compact image of {@code key}: into the pool and retained by the cache when the
 * account admits it (evicting compact images up to the shortfall first); otherwise uncached into a
 * scratch lease</td>
 * <td>MEM-301</td>
 * </tr>
 * <tr>
 * <td>{@code Decoded.retainedByCache()}, {@code Decoded.scratchBacked()},
 * {@code Decoded.segment()}, {@code Decoded.bytes()}, {@code Decoded.close()}</td>
 * <td>how the decode was served; an open {@code Decoded} guards its cache entry against
 * eviction</td>
 * <td>MEM-301</td>
 * </tr>
 * <tr>
 * <td>{@code void shutdown()}</td>
 * <td>unmap every slab, free every image, refund the account</td>
 * <td>MEM-301, MEM-302</td>
 * </tr>
 * </table>
 */
final class MemCompactSeam {

  static final String POOL_CLASS = "io.sirix.cache.CompactImagePool";
  static final String SLAB_UNIT = "slabUnitBytes";
  static final String SHARE = "shareBytes";
  static final String SLAB_BYTES = "slabBytes";
  static final String FALLBACK_BYTES = "fallbackImageBytes";
  static final String METADATA_BYTES = "metadataBytes";
  static final String CACHED_BYTES = "cachedImageBytes";
  static final String CACHED_COUNT = "cachedImageCount";
  static final String EVICTED_BYTES = "evictedImageBytes";
  static final String LEASE = "lease";
  static final String DECODE = "decode";
  static final String SHUTDOWN = "shutdown";
  static final String CLOSE = "close";
  static final String SEGMENT = "segment";
  static final String RETAINED = "retainedByCache";
  static final String SCRATCH_BACKED = "scratchBacked";
  static final String DECODED_BYTES = "bytes";

  private static final String REQUIREMENTS = "MEM-301, MEM-302";

  private MemCompactSeam() {}

  /** Raised for a required member that does not exist yet; carries the requirement and step. */
  static AssertionError missing(final String member) {
    return new AssertionError("MEM seam missing: " + member + " (needed by " + REQUIREMENTS
        + ", added by step 3; see MemCompactSeam for the contract)");
  }

  /** The unit in which the account grows. */
  static long slabUnitBytes() {
    final Class<?> poolClass = poolClass();
    final Method method = declared(poolClass, SLAB_UNIT, 0);
    if (method == null || !Modifier.isStatic(method.getModifiers())) {
      throw missing("static long " + POOL_CLASS + "." + SLAB_UNIT + "()");
    }
    try {
      return ((Number) method.invoke(null)).longValue();
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  /** A pool charged to {@code allocator} with the owner's {@code shareBytes}. */
  static Pool create(final FrameSlotAllocator allocator, final long shareBytes) {
    final Class<?> poolClass = poolClass();
    for (final Constructor<?> constructor : poolClass.getDeclaredConstructors()) {
      final Class<?>[] types = constructor.getParameterTypes();
      if (types.length == 2 && types[0] == FrameSlotAllocator.class && types[1] == long.class) {
        constructor.setAccessible(true);
        try {
          return new Pool(poolClass, constructor.newInstance(allocator, shareBytes));
        } catch (final ReflectiveOperationException e) {
          throw new AssertionError(e);
        }
      }
    }
    throw missing(POOL_CLASS + "(FrameSlotAllocator allocator, long shareBytes)");
  }

  private static Class<?> poolClass() {
    try {
      return Class.forName(POOL_CLASS);
    } catch (final ClassNotFoundException e) {
      throw missing("class " + POOL_CLASS);
    }
  }

  private static Method declared(final Class<?> owner, final String name, final int parameters) {
    for (final Method method : owner.getDeclaredMethods()) {
      if (method.getName().equals(name) && method.getParameterCount() == parameters) {
        method.setAccessible(true);
        return method;
      }
    }
    return null;
  }

  /**
   * {@code (receiver, longs...)} to generic references, primitives kept: the type the wrappers call
   * with.
   */
  private static MethodHandle bind(final Class<?> owner, final String name, final int parameters,
      final MethodType wanted) {
    final Method method = declared(owner, name, parameters);
    if (method == null) {
      throw missing(owner.getSimpleName() + "." + name + "(" + parameters + " parameter(s))");
    }
    try {
      return MethodHandles.lookup().unreflect(method).asType(wanted);
    } catch (final IllegalAccessException | WrongMethodTypeException e) {
      throw missing(owner.getSimpleName() + "." + name + " with a shape of " + wanted + " (" + e.getMessage() + ")");
    }
  }

  private static AssertionError unchecked(final Throwable failure) {
    if (failure instanceof Error error) {
      throw error;
    }
    if (failure instanceof RuntimeException runtime) {
      throw runtime;
    }
    return new AssertionError(failure);
  }

  /** The compact-image pool and its cache half, bound to the contract above. */
  static final class Pool {
    private final Object pool;
    private final MethodHandle share;
    private final MethodHandle slab;
    private final MethodHandle fallback;
    private final MethodHandle metadata;
    private final MethodHandle cachedBytes;
    private final MethodHandle cachedCount;
    private final MethodHandle evicted;
    private final MethodHandle lease;
    private final MethodHandle decode;
    private final MethodHandle shutdown;
    private final MethodHandle leaseClose;
    private final MethodHandle leaseSegment;
    private final MethodHandle decodedClose;
    private final MethodHandle decodedSegment;
    private final MethodHandle decodedBytes;
    private final MethodHandle decodedRetained;
    private final MethodHandle decodedScratch;

    private Pool(final Class<?> poolClass, final Object pool) {
      this.pool = pool;
      final MethodType longGetter = MethodType.methodType(long.class, Object.class);
      share = bind(poolClass, SHARE, 0, longGetter);
      slab = bind(poolClass, SLAB_BYTES, 0, longGetter);
      fallback = bind(poolClass, FALLBACK_BYTES, 0, longGetter);
      metadata = bind(poolClass, METADATA_BYTES, 0, longGetter);
      cachedBytes = bind(poolClass, CACHED_BYTES, 0, longGetter);
      cachedCount = bind(poolClass, CACHED_COUNT, 0, longGetter);
      evicted = bind(poolClass, EVICTED_BYTES, 0, longGetter);
      lease = bind(poolClass, LEASE, 1, MethodType.methodType(Object.class, Object.class, long.class));
      decode = bind(poolClass, DECODE, 2, MethodType.methodType(Object.class, Object.class, long.class, long.class));
      shutdown = bind(poolClass, SHUTDOWN, 0, MethodType.methodType(void.class, Object.class));
      final Class<?> leaseType = declared(poolClass, LEASE, 1).getReturnType();
      final Class<?> decodedType = declared(poolClass, DECODE, 2).getReturnType();
      final MethodType closer = MethodType.methodType(void.class, Object.class);
      final MethodType segmentGetter = MethodType.methodType(MemorySegment.class, Object.class);
      leaseClose = bind(leaseType, CLOSE, 0, closer);
      leaseSegment = bind(leaseType, SEGMENT, 0, segmentGetter);
      decodedClose = bind(decodedType, CLOSE, 0, closer);
      decodedSegment = bind(decodedType, SEGMENT, 0, segmentGetter);
      decodedBytes = bind(decodedType, DECODED_BYTES, 0, longGetter);
      decodedRetained = bind(decodedType, RETAINED, 0, MethodType.methodType(boolean.class, Object.class));
      decodedScratch = bind(decodedType, SCRATCH_BACKED, 0, MethodType.methodType(boolean.class, Object.class));
    }

    long shareBytes() {
      return getter(share);
    }

    long slabBytes() {
      return getter(slab);
    }

    long fallbackImageBytes() {
      return getter(fallback);
    }

    long metadataBytes() {
      return getter(metadata);
    }

    /** Pool slab bytes + fallback image bytes + pool metadata bytes: the one account of MEM-301. */
    long accountBytes() {
      return slabBytes() + fallbackImageBytes() + metadataBytes();
    }

    long cachedImageBytes() {
      return getter(cachedBytes);
    }

    long cachedImageCount() {
      return getter(cachedCount);
    }

    long evictedImageBytes() {
      return getter(evicted);
    }

    /** A pool lease of {@code bytes}, or {@code null} when refused. */
    Object lease(final long bytes) {
      try {
        return (Object) lease.invokeExact(pool, bytes);
      } catch (final Throwable failure) {
        throw unchecked(failure);
      }
    }

    void close(final Object leaseHandle) {
      try {
        leaseClose.invokeExact(leaseHandle);
      } catch (final Throwable failure) {
        throw unchecked(failure);
      }
    }

    MemorySegment leaseSegment(final Object leaseHandle) {
      try {
        return (MemorySegment) leaseSegment.invokeExact(leaseHandle);
      } catch (final Throwable failure) {
        throw unchecked(failure);
      }
    }

    /** The decode of {@code key} as {@code imageBytes}, however the account lets it be served. */
    Object decode(final long key, final long imageBytes) {
      try {
        return (Object) decode.invokeExact(pool, key, imageBytes);
      } catch (final Throwable failure) {
        throw unchecked(failure);
      }
    }

    void closeDecoded(final Object decoded) {
      try {
        decodedClose.invokeExact(decoded);
      } catch (final Throwable failure) {
        throw unchecked(failure);
      }
    }

    MemorySegment decodedSegment(final Object decoded) {
      try {
        return (MemorySegment) decodedSegment.invokeExact(decoded);
      } catch (final Throwable failure) {
        throw unchecked(failure);
      }
    }

    long decodedBytes(final Object decoded) {
      return getter(decodedBytes, decoded);
    }

    boolean retainedByCache(final Object decoded) {
      try {
        return (boolean) decodedRetained.invokeExact(decoded);
      } catch (final Throwable failure) {
        throw unchecked(failure);
      }
    }

    boolean scratchBacked(final Object decoded) {
      try {
        return (boolean) decodedScratch.invokeExact(decoded);
      } catch (final Throwable failure) {
        throw unchecked(failure);
      }
    }

    void shutdown() {
      try {
        shutdown.invokeExact(pool);
      } catch (final Throwable failure) {
        throw unchecked(failure);
      }
    }

    private long getter(final MethodHandle handle) {
      return getter(handle, pool);
    }

    private static long getter(final MethodHandle handle, final Object receiver) {
      try {
        return (long) handle.invokeExact(receiver);
      } catch (final Throwable failure) {
        throw unchecked(failure);
      }
    }
  }
}
