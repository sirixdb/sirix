/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.utils.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * {@link VirtualMemory} over POSIX {@code mmap}/{@code madvise}/{@code munmap}. Linux and macOS
 * differ only in mmap FLAG VALUES ({@code MAP_ANON} is {@code 0x20} on Linux but {@code 0x1000}
 * on Darwin, and Darwin has no {@code MAP_NORESERVE}); the calls are otherwise identical.
 *
 * <p>Symbols are bound SOFTLY: class-loading never throws on a platform that lacks them (Windows
 * class-loads this via {@link VirtualMemory#forCurrentPlatform} resolution paths); actually using
 * an unbound operation fails fast with an actionable message.
 */
final class PosixVirtualMemory implements VirtualMemory {

  static final PosixVirtualMemory INSTANCE = new PosixVirtualMemory();

  private static final Logger LOGGER = LoggerFactory.getLogger(PosixVirtualMemory.class);

  private static final Linker LINKER = Linker.nativeLinker();
  private static final MethodHandle MMAP;
  private static final MethodHandle MUNMAP;
  private static final MethodHandle MADVISE;

  private static final int PROT_READ = 0x1;
  private static final int PROT_WRITE = 0x2;
  private static final int MAP_PRIVATE = 0x02;
  private static final int MAP_ANONYMOUS = OS.isMacOSX() ? 0x1000 : 0x20;
  private static final int MAP_NORESERVE = OS.isMacOSX() ? 0 : 0x4000;
  private static final int MADV_DONTNEED = 4;

  static {
    MMAP = bind("mmap",
        FunctionDescriptor.of(ValueLayout.ADDRESS,
            ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
    MUNMAP = bind("munmap",
        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    MADVISE = bind("madvise",
        FunctionDescriptor.of(ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
  }

  private static MethodHandle bind(final String symbol, final FunctionDescriptor descriptor) {
    return LINKER.defaultLookup().find(symbol).map(s -> LINKER.downcallHandle(s, descriptor)).orElse(null);
  }

  private PosixVirtualMemory() {
  }

  @Override
  public MemorySegment reserve(final long bytes) {
    if (MMAP == null) {
      throw new IllegalStateException("FrameSlotAllocator requires a POSIX libc (mmap); "
          + "on Windows the Windows virtual-memory backend is selected automatically");
    }
    try {
      final MemorySegment addr = (MemorySegment) MMAP.invokeExact(
          MemorySegment.NULL, bytes,
          PROT_READ | PROT_WRITE,
          MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
          -1, 0L);
      // mmap signals failure with MAP_FAILED (-1), not NULL.
      if (addr.address() == 0 || addr.address() == -1L) {
        throw new OutOfMemoryError("mmap failed for " + bytes + " bytes");
      }
      return addr.reinterpret(bytes);
    } catch (final OutOfMemoryError oom) {
      throw oom;
    } catch (final Throwable t) {
      throw new RuntimeException("Failed to mmap allocator region", t);
    }
  }

  @Override
  public void commitFresh(final MemorySegment slot) {
    // POSIX overcommits: the first write faults in a zero page. No syscall needed.
  }

  @Override
  public void discardToZeros(final MemorySegment segment) {
    if (MADVISE == null) {
      return;
    }
    try {
      final int rc = (int) MADVISE.invokeExact(segment, segment.byteSize(), MADV_DONTNEED);
      if (rc != 0) {
        LOGGER.debug("discardToZeros MADV_DONTNEED rc={} on address 0x{}", rc,
            Long.toHexString(segment.address()));
      }
    } catch (final Throwable t) {
      LOGGER.debug("discardToZeros madvise failed: {}", t.getMessage());
    }
  }

  @Override
  public void release(final MemorySegment region) {
    if (MUNMAP == null) {
      return;
    }
    try {
      final int rc = (int) MUNMAP.invokeExact(region, region.byteSize());
      if (rc != 0) {
        LOGGER.warn("munmap returned {} for region size {}", rc, region.byteSize());
      }
    } catch (final Throwable t) {
      LOGGER.warn("munmap failed on shutdown: {}", t.getMessage());
    }
  }
}
