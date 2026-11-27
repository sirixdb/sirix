/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The fixed VirtualMemory bindings for MEM-104 and MEM-106.
 *
 * <table>
 * <caption>Step-1 VirtualMemory contract</caption>
 * <tr><th>Member</th><th>Meaning</th></tr>
 * <tr><td>{@code long osPageBytes()}</td><td>OS page size in bytes</td></tr>
 * <tr><td>{@code boolean releaseRun(MemorySegment run)}</td><td>release backing while keeping the run accessible</td></tr>
 * <tr><td>{@code boolean recharge(MemorySegment run)}</td><td>re-charge advice before reuse</td></tr>
 * </table>
 */
final class MemPlatformSeam {

  static final String OS_PAGE_BYTES = "osPageBytes";
  static final String RELEASE_RUN = "releaseRun";
  static final String RECHARGE = "recharge";

  private MemPlatformSeam() {}

  static long osPageBytes(final VirtualMemory vm) {
    return (long) invoke(required(OS_PAGE_BYTES), vm);
  }

  static boolean releaseRun(final VirtualMemory vm, final MemorySegment run) {
    return (boolean) invoke(required(RELEASE_RUN, MemorySegment.class), vm, run);
  }

  static boolean recharge(final VirtualMemory vm, final MemorySegment run) {
    return (boolean) invoke(required(RECHARGE, MemorySegment.class), vm, run);
  }

  private static Method required(final String name, final Class<?>... types) {
    final Method method = find(name, types);
    if (method == null) {
      throw MemPoolSeam.missing("VirtualMemory." + name, "MEM-104, MEM-106", 1);
    }
    return method;
  }

  private static Method find(final String name, final Class<?>... types) {
    try {
      return VirtualMemory.class.getMethod(name, types);
    } catch (final NoSuchMethodException e) {
      return null;
    }
  }

  private static Object invoke(final Method method, final VirtualMemory vm, final Object... args) {
    try {
      return method.invoke(vm, args);
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
