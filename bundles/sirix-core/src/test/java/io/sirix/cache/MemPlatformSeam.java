/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.Set;

/**
 * Binds the platform release primitives of MEM-106 on a real {@link VirtualMemory} backend. The
 * chapter fixes the behavior (a granule-aligned run goes back to the OS; macOS re-charges before
 * reuse), not method names, so this class finds the methods by <em>shape</em>, the same
 * classification as {@link RecordingVirtualMemory}:
 *
 * <ul>
 * <li>a {@link VirtualMemory} method that is not {@code reserve}, {@code commitFresh},
 * {@code release} or {@code populate}, whose first parameter is a {@link MemorySegment} (optionally
 * followed by one {@code long}, the run length) and whose name contains {@code recharge},
 * {@code reuse} or {@code recommit}: <em>re-charge advice</em>;</li>
 * <li>the same shape with any other name: the <em>release run</em>;</li>
 * <li>a no-argument method whose name contains {@code page} and {@code size} or {@code bytes}: the
 * OS page size.</li>
 * </ul>
 *
 * A result of {@code boolean} is the success flag; an {@code int} or {@code long} is a success when
 * it is not negative; {@code void} succeeds unless it throws.
 */
final class MemPlatformSeam {

  private static final Set<String> KNOWN = Set.of("reserve", "commitFresh", "release", "populate");

  private MemPlatformSeam() {}

  /** The OS page size the backend reports, in bytes. */
  static long osPageBytes(final VirtualMemory vm) {
    for (final Method method : vm.getClass().getMethods()) {
      final String name = method.getName().toLowerCase(Locale.ROOT);
      if (method.getParameterCount() == 0 && name.contains("page") && (name.contains("size") || name.contains("bytes"))
          && (method.getReturnType() == long.class || method.getReturnType() == int.class)) {
        return ((Number) invoke(method, vm)).longValue();
      }
    }
    throw MemPoolSeam.missing("VirtualMemory OS page size query (no-argument method named *page*size*)",
        "MEM-106, MEM-104 (granule)", 1);
  }

  /** Whether the backend has a release primitive. */
  static boolean hasReleaseRun(final VirtualMemory vm) {
    return find(vm, false) != null;
  }

  /** Whether the backend has re-charge advice. */
  static boolean hasRecharge(final VirtualMemory vm) {
    return find(vm, true) != null;
  }

  /**
   * Give the granule-aligned {@code run} back to the OS.
   *
   * @return whether the backend reported success
   */
  static boolean releaseRun(final VirtualMemory vm, final MemorySegment run) {
    final Method method = find(vm, false);
    if (method == null) {
      throw MemPoolSeam.missing("VirtualMemory release primitive (a method taking a MemorySegment run)", "MEM-106", 1);
    }
    return succeeded(method, invoke(method, vm, run));
  }

  /**
   * Advise the backend that {@code run} is about to be used again (macOS {@code MADV_FREE_REUSE}).
   *
   * @return whether the backend reported success
   */
  static boolean recharge(final VirtualMemory vm, final MemorySegment run) {
    final Method method = find(vm, true);
    if (method == null) {
      throw MemPoolSeam.missing("VirtualMemory re-charge advice (a method named *recharge*|*reuse*|*recommit*)",
          "MEM-106 (5)", 1);
    }
    return succeeded(method, invoke(method, vm, run));
  }

  private static Method find(final VirtualMemory vm, final boolean recharge) {
    for (final Method method : vm.getClass().getMethods()) {
      final Class<?>[] types = method.getParameterTypes();
      if (KNOWN.contains(method.getName()) || types.length < 1 || types.length > 2 || types[0] != MemorySegment.class
          || (types.length == 2 && types[1] != long.class) || method.getDeclaringClass() == Object.class) {
        continue;
      }
      final String name = method.getName().toLowerCase(Locale.ROOT);
      final boolean isRecharge = name.contains("recharge") || name.contains("reuse") || name.contains("recommit");
      if (isRecharge == recharge) {
        return method;
      }
    }
    return null;
  }

  private static boolean succeeded(final Method method, final Object result) {
    if (result instanceof Boolean flag) {
      return flag;
    }
    if (result instanceof Number number) {
      return number.longValue() >= 0L;
    }
    return method.getReturnType() == void.class;
  }

  private static Object invoke(final Method method, final VirtualMemory vm, final MemorySegment... run) {
    try {
      method.setAccessible(true);
      if (method.getParameterCount() == 0) {
        return method.invoke(vm);
      }
      return method.getParameterCount() == 1
          ? method.invoke(vm, run[0])
          : method.invoke(vm, run[0], run[0].byteSize());
    } catch (final InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException runtime) {
        throw runtime;
      }
      if (e.getCause() instanceof Error error) {
        throw error;
      }
      throw new AssertionError(e.getCause());
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
