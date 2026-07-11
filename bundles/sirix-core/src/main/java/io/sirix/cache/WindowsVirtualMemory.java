/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.utils.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * {@link VirtualMemory} over kernel32 {@code VirtualAlloc}/{@code VirtualFree} — the faithful
 * Windows port of the frame-slot allocator's plumbing:
 *
 * <ul>
 *   <li>{@code VirtualAlloc(MEM_RESERVE)} reserves the per-size-class region: address space only,
 *       no commit charge — the analogue of {@code mmap(MAP_NORESERVE)}.</li>
 *   <li>{@code VirtualAlloc(addr, size, MEM_COMMIT)} commits a fresh slot before first use.
 *       Windows has NO overcommit: touching reserved-but-uncommitted pages faults, so unlike
 *       POSIX this step is mandatory. Commit charge is bounded by the allocator's physical
 *       budget, never the 32 GiB-per-class reservation.</li>
 *   <li>{@code VirtualFree(MEM_DECOMMIT)} + recommit implements discard-to-zeros (Windows
 *       guarantees committed pages start zeroed; {@code MEM_RESET} would NOT guarantee that).</li>
 *   <li>{@code VirtualFree(MEM_RELEASE)} drops a whole region at shutdown.</li>
 * </ul>
 *
 * <p>Bindings are attempted only on Windows, so class-loading is side-effect free elsewhere.
 */
final class WindowsVirtualMemory implements VirtualMemory {

  static final WindowsVirtualMemory INSTANCE = new WindowsVirtualMemory();

  private static final Logger LOGGER = LoggerFactory.getLogger(WindowsVirtualMemory.class);

  private static final long MEM_COMMIT = 0x1000;
  private static final long MEM_RESERVE = 0x2000;
  private static final long MEM_DECOMMIT = 0x4000;
  private static final long MEM_RELEASE = 0x8000;
  private static final long PAGE_READWRITE = 0x04;

  private static final MethodHandle VIRTUAL_ALLOC;
  private static final MethodHandle VIRTUAL_FREE;

  static {
    MethodHandle alloc = null;
    MethodHandle free = null;
    if (OS.isWindows()) {
      // VirtualAlloc/VirtualFree live in kernel32.dll; the nativeLinker default lookup only
      // covers the C runtime.
      final Linker linker = Linker.nativeLinker();
      final SymbolLookup kernel32 = SymbolLookup.libraryLookup("kernel32.dll", Arena.global());
      alloc = linker.downcallHandle(
          kernel32.find("VirtualAlloc").orElseThrow(() -> new RuntimeException("VirtualAlloc not found in kernel32.dll")),
          FunctionDescriptor.of(ValueLayout.ADDRESS,
              ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
      free = linker.downcallHandle(
          kernel32.find("VirtualFree").orElseThrow(() -> new RuntimeException("VirtualFree not found in kernel32.dll")),
          FunctionDescriptor.of(ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
    }
    VIRTUAL_ALLOC = alloc;
    VIRTUAL_FREE = free;
  }

  private WindowsVirtualMemory() {
  }

  @Override
  public MemorySegment reserve(final long bytes) {
    if (VIRTUAL_ALLOC == null) {
      throw new IllegalStateException("WindowsVirtualMemory requires Windows (kernel32 VirtualAlloc)");
    }
    try {
      final MemorySegment addr = (MemorySegment) VIRTUAL_ALLOC.invokeExact(
          MemorySegment.NULL, bytes, MEM_RESERVE, PAGE_READWRITE);
      if (addr.address() == 0) {
        throw new OutOfMemoryError("VirtualAlloc(MEM_RESERVE) failed for " + bytes + " bytes");
      }
      return addr.reinterpret(bytes);
    } catch (final OutOfMemoryError oom) {
      throw oom;
    } catch (final Throwable t) {
      throw new RuntimeException("Failed to reserve allocator region", t);
    }
  }

  @Override
  public void commitFresh(final MemorySegment slot) {
    try {
      final MemorySegment addr = (MemorySegment) VIRTUAL_ALLOC.invokeExact(
          slot, slot.byteSize(), MEM_COMMIT, PAGE_READWRITE);
      if (addr.address() == 0) {
        throw new OutOfMemoryError("VirtualAlloc(MEM_COMMIT) failed for slot of "
            + slot.byteSize() + " bytes — physical memory (commit charge) exhausted");
      }
    } catch (final OutOfMemoryError oom) {
      throw oom;
    } catch (final Throwable t) {
      throw new RuntimeException("Failed to commit slot", t);
    }
  }

  @Override
  public void discardToZeros(final MemorySegment segment) {
    try {
      final int freed = (int) VIRTUAL_FREE.invokeExact(segment, segment.byteSize(), MEM_DECOMMIT);
      if (freed == 0) {
        LOGGER.debug("VirtualFree(MEM_DECOMMIT) failed on address 0x{}", Long.toHexString(segment.address()));
        return;
      }
      // Recommit immediately: committed pages are guaranteed zeroed, and the slot must stay
      // touch-safe for its next owner (nothing else in the allocator commits recycled slots).
      final MemorySegment addr = (MemorySegment) VIRTUAL_ALLOC.invokeExact(
          segment, segment.byteSize(), MEM_COMMIT, PAGE_READWRITE);
      if (addr.address() == 0) {
        throw new OutOfMemoryError("VirtualAlloc(MEM_COMMIT) failed re-committing a discarded slot");
      }
    } catch (final OutOfMemoryError oom) {
      throw oom;
    } catch (final Throwable t) {
      throw new RuntimeException("Failed to discard slot contents", t);
    }
  }

  @Override
  public void release(final MemorySegment region) {
    try {
      final int freed = (int) VIRTUAL_FREE.invokeExact(region, 0L, MEM_RELEASE);
      if (freed == 0) {
        LOGGER.warn("VirtualFree(MEM_RELEASE) failed for region size {}", region.byteSize());
      }
    } catch (final Throwable t) {
      LOGGER.warn("VirtualFree failed on shutdown: {}", t.getMessage());
    }
  }
}
