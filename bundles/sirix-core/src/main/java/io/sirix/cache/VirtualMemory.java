/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.utils.OS;

import java.lang.foreign.MemorySegment;

/**
 * The platform-specific virtual-memory plumbing behind {@link FrameSlotAllocator} — everything
 * else in the allocator (size classes, fixed-position slots, optimistic version counters, the
 * free-slot stacks) is portable Java. All four operations are COLD paths: a region is reserved
 * once per size class at init, a slot is committed once when first handed out, and regions are
 * released once at shutdown. The allocator's hot path never enters this interface.
 *
 * <p>Semantics the allocator relies on:
 *
 * <ul>
 *   <li>{@link #reserve} returns a region of stable virtual addresses with no physical memory
 *       committed up front (POSIX: {@code mmap(MAP_NORESERVE)}; Windows:
 *       {@code VirtualAlloc(MEM_RESERVE)}).</li>
 *   <li>{@link #commitFresh} makes a slot writable before its FIRST use. POSIX overcommits, so
 *       first-touch faults are served from the zero page and this is a no-op; Windows has no
 *       overcommit — reserved pages must be committed explicitly or the first touch faults.</li>
 *   <li>Once committed, a slot STAYS committed across recycle cycles — nothing here decommits,
 *       so recycled slots are always touch-safe on every platform.</li>
 *   <li>{@link #discardToZeros} discards a segment's contents such that the next read observes
 *       zeros, without invalidating the address (POSIX anonymous memory: {@code MADV_DONTNEED};
 *       Windows: decommit + recommit).</li>
 * </ul>
 */
public interface VirtualMemory {

  /** Reserve {@code bytes} of stable virtual address space without committing physical memory. */
  MemorySegment reserve(long bytes);

  /** Make a freshly handed-out slot writable; throws {@link OutOfMemoryError} when memory is exhausted. */
  void commitFresh(MemorySegment slot);

  /** Discard contents so the next read observes zeros; the address stays valid and committed. */
  void discardToZeros(MemorySegment segment);

  /** Release a whole region reserved by {@link #reserve} (shutdown only). */
  void release(MemorySegment region);

  /** The backend for the OS this JVM runs on. */
  static VirtualMemory forCurrentPlatform() {
    return OS.isWindows() ? WindowsVirtualMemory.INSTANCE : PosixVirtualMemory.INSTANCE;
  }
}
