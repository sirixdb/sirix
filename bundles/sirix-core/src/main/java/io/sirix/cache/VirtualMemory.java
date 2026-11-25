/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.utils.OS;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;

/**
 * The platform-specific virtual-memory plumbing behind {@link FrameSlotAllocator} — everything else
 * in the allocator (size classes, fixed-position slots, optimistic version counters, the free-slot
 * stacks) is portable Java. Reservations, first commitments, scratch reclamation and shutdown are
 * cold paths. Warm frame-slot and scratch-buffer reuse never enters this interface.
 *
 * <p>
 * Semantics the allocator relies on:
 *
 * <ul>
 * <li>{@link #reserve} returns a region of stable virtual addresses with no physical memory
 * committed up front (POSIX: {@code mmap(MAP_NORESERVE)}; Windows:
 * {@code VirtualAlloc(MEM_RESERVE)}).</li>
 * <li>{@link #commitFresh} makes a slot writable before its FIRST use. POSIX overcommits, so
 * first-touch faults are served from the zero page and this is a no-op; Windows has no overcommit —
 * reserved pages must be committed explicitly or the first touch faults.</li>
 * <li>Once committed, a slot STAYS committed across recycle cycles — nothing here decommits, so
 * recycled slots are always touch-safe on every platform.</li>
 * <li>{@link #discardToZeros} discards a segment's contents such that the next read observes zeros,
 * without invalidating the address (Linux: {@code MADV_DONTNEED}; macOS: {@code MADV_FREE_REUSABLE},
 * because Darwin's {@code MADV_DONTNEED} is advisory and leaves the old contents readable; Windows:
 * decommit + recommit).</li>
 * </ul>
 */
public interface VirtualMemory {

  int POPULATE_REFUSED = -1;
  int POPULATE_UNAVAILABLE = 0;
  int POPULATED = 1;

  /** Reserve {@code bytes} of stable virtual address space without committing physical memory. */
  MemorySegment reserve(long bytes);

  /**
   * Like {@link #reserve(long)}, but returns {@code null} for native reservation refusal. Java heap
   * and backend failures still propagate.
   */
  @Nullable
  MemorySegment tryReserve(long bytes);

  /**
   * Make a freshly handed-out slot writable.
   *
   * @return {@code true} when this call performed a native commit operation
   */
  boolean commitFresh(MemorySegment slot);

  /**
   * Make a fresh slot writable, returning {@code false} only for native commitment refusal. Returns
   * {@code true} when writable, including POSIX's no-op; heap and backend failures propagate.
   */
  boolean tryCommitFresh(MemorySegment slot);

  /** Discard contents so the next read observes zeros; the address stays valid and committed. */
  void discardToZeros(MemorySegment segment);

  /**
   * Release a whole region reserved by {@link #reserve} or {@link #tryReserve}, once no reader can
   * access it, including failed allocation rollback and slab reclamation.
   *
   * @return {@code true} when this call performed a successful native release
   */
  boolean release(MemorySegment region);

  /**
   * Pre-fault {@code segment}'s pages writable in one call (Linux {@code MADV_POPULATE_WRITE}), for a
   * consumer that writes the whole segment anyway, such as a slab of decoded lanes: one call instead
   * of one fault per 4 KiB page (2.4 us each measured, half the cost of a lane fill's first touch).
   *
   * @return {@code true} when the pages were populated; {@code false} when the platform or kernel has
   *         no such call and the pages fault lazily on first touch
   * @throws OutOfMemoryError when the kernel reports it cannot back the pages ({@code ENOMEM})
   */
  default boolean populate(final MemorySegment segment) {
    final int result = tryPopulate(segment);
    if (result == POPULATE_REFUSED) {
      throw new OutOfMemoryError("Could not back " + segment.byteSize() + " bytes with physical pages");
    }
    return result == POPULATED;
  }

  /**
   * Like {@link #populate(MemorySegment)}, reporting native refusal without throwing.
   *
   * @return {@link #POPULATED}, {@link #POPULATE_UNAVAILABLE} for lazy first touch, or
   *         {@link #POPULATE_REFUSED} when the kernel cannot back the pages; Java heap failures still
   *         propagate
   */
  default int tryPopulate(final MemorySegment segment) {
    return POPULATE_UNAVAILABLE;
  }

  /** The backend for the OS this JVM runs on. */
  static VirtualMemory forCurrentPlatform() {
    return OS.isWindows()
        ? WindowsVirtualMemory.INSTANCE
        : PosixVirtualMemory.INSTANCE;
  }
}
