/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.index.hot.cache.HOTFragmentCache;
import io.sirix.page.PageReference;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Locale;
import java.util.Map;

/**
 * The one place where the MEM step-2 tests touch buffer-manager members that do not exist yet. The
 * tests assert behavior; this class only <em>binds</em> them, so that they compile today and run
 * the day the members exist. Same convention as {@link MemPoolSeam}: a missing member fails the
 * test with a message naming the member, the requirement and the step (see
 * {@link MemPoolSeam#missing}); an implementer who names a member differently edits the binding
 * here and leaves the assertions alone.
 *
 * <h2>Seam contract (step 2)</h2>
 *
 * <table>
 * <caption>Required members</caption>
 * <tr>
 * <th>Member</th>
 * <th>Meaning</th>
 * <th>Required by</th>
 * </tr>
 * <tr>
 * <td>on {@link BufferManagerImpl}: a constructor or static factory
 * {@code (long frameBudgetBytes, HeapCacheBudget.PropertyLookup properties, HeapCacheBudget heap)}
 * </td>
 * <td>a buffer manager whose frame-holding caches share the one frame budget
 * {@code frameBudgetBytes} (<em>L_f</em>) and apply {@code sirix.cache.recordPage} and
 * {@code sirix.cache.recordPageFragment}, read from {@code properties}, only as maximum caps; the
 * metadata caches keep their current bounds</td>
 * <td>MEM-201, MEM-202</td>
 * </tr>
 * <tr>
 * <td>on {@link FrameSlotAllocator.PressureListener}: one method taking one {@code long}, the
 * shortfall in bytes</td>
 * <td>the pressure listener the buffer manager registers evicts by priority up to that
 * shortfall</td>
 * <td>MEM-202</td>
 * </tr>
 * </table>
 *
 * <p>
 * Everything else the step-2 tests use exists today: the cache getters of
 * {@link BufferManagerImpl}, {@link ShardedPageCache#getCurrentWeightBytes()},
 * {@link HOTFragmentCache#nativeImages()}, the seven-argument {@link BufferManagerImpl} constructor
 * and {@link FrameSlotAllocator}.
 */
final class MemCacheSeam {

  /**
   * The batch of MEM-202: eviction stops once the frames it freed cover the shortfall rounded up to
   * one batch.
   */
  static final long BATCH_BYTES = MemPoolSeam.BATCH_BYTES;

  private MemCacheSeam() {}

  /** A property lookup over {@code properties}; absent names are unset. */
  static HeapCacheBudget.PropertyLookup lookup(final Map<String, String> properties) {
    return properties::get;
  }

  /**
   * A buffer manager with one shared frame budget of {@code frameBudgetBytes}, whose optional caps
   * come from {@code properties}.
   *
   * @throws AssertionError naming the missing seam when no such constructor or factory exists
   */
  static BufferManagerImpl newBufferManager(final long frameBudgetBytes, final Map<String, String> properties,
      final HeapCacheBudget heap) {
    final HeapCacheBudget.PropertyLookup lookup = lookup(properties);
    for (final Constructor<?> constructor : BufferManagerImpl.class.getDeclaredConstructors()) {
      if (matches(constructor.getParameterTypes())) {
        constructor.setAccessible(true);
        try {
          return (BufferManagerImpl) constructor.newInstance(frameBudgetBytes, lookup, heap);
        } catch (final InvocationTargetException e) {
          throw rethrowCause(e);
        } catch (final ReflectiveOperationException e) {
          throw new AssertionError(e);
        }
      }
    }
    for (final Method method : BufferManagerImpl.class.getDeclaredMethods()) {
      if (Modifier.isStatic(method.getModifiers()) && method.getReturnType() == BufferManagerImpl.class
          && matches(method.getParameterTypes())) {
        method.setAccessible(true);
        try {
          return (BufferManagerImpl) method.invoke(null, frameBudgetBytes, lookup, heap);
        } catch (final InvocationTargetException e) {
          throw rethrowCause(e);
        } catch (final ReflectiveOperationException e) {
          throw new AssertionError(e);
        }
      }
    }
    throw missing(
        "BufferManagerImpl(long frameBudgetBytes, HeapCacheBudget.PropertyLookup properties, HeapCacheBudget heap)",
        "MEM-201 (one frame budget, optional caps)", 2);
  }

  /**
   * Hands the pressure listener that {@code BufferManagerImpl} registered a <em>byte</em> shortfall
   * of {@code shortfallBytes} for a request of frame class {@code requestingClass}, as the allocator
   * does when a reservation is refused (MEM-103, MEM-202).
   */
  static void firePressureBytes(final long shortfallBytes, final int requestingClass) {
    fire(false, shortfallBytes, requestingClass);
  }

  /**
   * Hands that listener a <em>slot</em> shortfall of one slot for class {@code requestingClass}: the
   * class has no slot left although budget does (MEM-103, MEM-202).
   */
  static void fireSlotPressure(final int requestingClass) {
    fire(true, 1L, requestingClass);
  }

  private static void fire(final boolean slotShortfall, final long amount, final int requestingClass) {
    final FrameSlotAllocator.PressureListener listener = FrameSlotAllocator.pressureListener();
    if (listener == null) {
      throw new AssertionError("no pressure listener is registered; construct the BufferManagerImpl first");
    }
    for (final Method method : FrameSlotAllocator.PressureListener.class.getMethods()) {
      if (Modifier.isStatic(method.getModifiers()) || method.isDefault()) {
        continue;
      }
      final Object[] arguments = argumentsFor(method, slotShortfall, amount, requestingClass);
      if (arguments == null) {
        continue;
      }
      try {
        method.invoke(listener, arguments);
        return;
      } catch (final InvocationTargetException e) {
        throw rethrowCause(e);
      } catch (final ReflectiveOperationException e) {
        throw new AssertionError(e);
      }
    }
    throw missing("FrameSlotAllocator.PressureListener#onPressure(shortfall kind, long amount, int requestingClass)",
        "MEM-103 / MEM-202 (the listener receives the shortfall kind, amount and requesting class)", 2);
  }

  /**
   * The arguments for {@code method}, built by parameter shape like
   * {@link MemPoolSeam.PressureRequest} decodes them: a {@code long} is the amount, an {@code int}
   * the requesting class, a {@code boolean} or an enum the kind (the enum constant whose name
   * contains {@code SLOT} or {@code BYTE}). Returns {@code null} when the method has no {@code long}
   * amount or a parameter of another shape.
   */
  private static Object[] argumentsFor(final Method method, final boolean slotShortfall, final long amount,
      final int requestingClass) {
    final Class<?>[] types = method.getParameterTypes();
    final Object[] arguments = new Object[types.length];
    boolean hasAmount = false;
    for (int i = 0; i < types.length; i++) {
      final Class<?> type = types[i];
      if (type == long.class && !hasAmount) {
        arguments[i] = amount;
        hasAmount = true;
      } else if (type == int.class) {
        arguments[i] = requestingClass;
      } else if (type == boolean.class) {
        arguments[i] = slotShortfall;
      } else if (type.isEnum()) {
        arguments[i] = kindConstant(type, slotShortfall);
        if (arguments[i] == null) {
          return null;
        }
      } else {
        return null;
      }
    }
    return hasAmount
        ? arguments
        : null;
  }

  private static Object kindConstant(final Class<?> enumType, final boolean slotShortfall) {
    final String wanted = slotShortfall
        ? "SLOT"
        : "BYTE";
    for (final Object constant : enumType.getEnumConstants()) {
      if (constant.toString().toUpperCase(Locale.ROOT).contains(wanted)) {
        return constant;
      }
    }
    return null;
  }

  /**
   * An allocator of {@code budgetBytes} whose size class {@code classIdx} has address space for only
   * {@code slots} slots, built over {@code vm} (MEM-103's bounded-capacity seam). The class is
   * exhausted by {@code slots} frames while the budget has room left.
   *
   * <p>
   * Contract: {@link RecordingVirtualMemory#capClassSlots(int, long)} makes the fake's
   * {@code reserve} for that class return a region of only {@code slots * slotSize} bytes, and
   * {@link MemPoolSeam#newAllocator} derives the class's capacity from the region it got. The
   * {@code BufferManagerImpl} needs no binding to such an allocator: it reaches the allocator only
   * through the static pressure listener, and the pages in its caches carry their own allocator.
   *
   * @throws AssertionError naming the missing seam when the allocator cannot be injected
   */
  static FrameSlotAllocator newBoundedAllocator(final long budgetBytes, final RecordingVirtualMemory vm,
      final int classIdx, final long slots) {
    vm.capClassSlots(classIdx, slots);
    return MemPoolSeam.newAllocator(budgetBytes, vm);
  }

  /** The combined record-page cache of {@code manager}, typed for {@link MemCachePageStub}s. */
  static ShardedPageCache<CacheablePage> recordPages(final BufferManagerImpl manager) {
    return asStubCache(manager.getRecordPageCache());
  }

  /** The record-fragment cache of {@code manager}. */
  static ShardedPageCache<CacheablePage> recordFragments(final BufferManagerImpl manager) {
    return asStubCache(manager.getRecordPageFragmentCache());
  }

  /** The combined HOT-leaf cache of {@code manager}. */
  static ShardedPageCache<CacheablePage> hotLeaves(final BufferManagerImpl manager) {
    return asStubCache(manager.getHOTLeafPageCache());
  }

  /** The allocator-frame half of the HOT fragment cache of {@code manager}. */
  static ShardedPageCache<CacheablePage> nativeHotFragments(final BufferManagerImpl manager) {
    return asStubCache(((HOTFragmentCache) manager.getHOTLeafFragmentCache()).nativeImages());
  }

  /**
   * Whether a cache with maximum weight {@code maxWeightBytes} carries no cap below the shared frame
   * budget: a non-positive maximum reads as unbounded in {@link ShardedPageCache}.
   */
  static boolean isUncapped(final long maxWeightBytes, final long frameBudgetBytes) {
    return maxWeightBytes <= 0 || maxWeightBytes >= frameBudgetBytes;
  }

  /** Puts a weight-only page of {@code weightBytes} under key number {@code index}; returns it. */
  static MemCachePageStub putWeighing(final ShardedPageCache<CacheablePage> cache, final long index,
      final long weightBytes) {
    final MemCachePageStub page = MemCachePageStub.weighing(index, weightBytes);
    final PageReference key = MemCachePageStub.keyOf(index);
    cache.put(key, page);
    return page;
  }

  /**
   * The failure for a required member that does not exist yet; same shape as
   * {@link MemPoolSeam#missing}.
   */
  private static AssertionError missing(final String member, final String requirement, final int step) {
    return new AssertionError("MEM seam missing: " + member + " (needed by " + requirement + ", added by step " + step
        + "; see MemCacheSeam for the contract)");
  }

  private static RuntimeException rethrowCause(final InvocationTargetException failure) {
    final Throwable cause = failure.getCause();
    if (cause instanceof RuntimeException runtime) {
      return runtime;
    }
    if (cause instanceof Error error) {
      throw error;
    }
    throw new AssertionError(cause);
  }

  private static boolean matches(final Class<?>[] types) {
    return types.length == 3 && types[0] == long.class && types[1] == HeapCacheBudget.PropertyLookup.class
        && types[2] == HeapCacheBudget.class;
  }

  // The caches of BufferManagerImpl are typed for their production pages. Their sharded caches only
  // use the CacheablePage contract, which the stubs implement, so the stub caches are views of them.
  @SuppressWarnings("unchecked")
  private static ShardedPageCache<CacheablePage> asStubCache(final Object cache) {
    return (ShardedPageCache<CacheablePage>) cache;
  }
}
