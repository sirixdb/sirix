/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.jspecify.annotations.Nullable;

/**
 * Single entry-point for obtaining the {@link MemorySegmentAllocator}. Callers go through
 * {@link #getInstance()} instead of naming the implementation, so page frames, slabs and scratch
 * buffers are all charged to one allocator and one budget.
 *
 * <p>
 * There is one implementation, {@link FrameSlotAllocator}, on every platform: Umbra/LeanStore-style
 * fixed-address frame slots with optimistic versioned reads. Slot addresses are stable for the
 * lifetime of the process; the platform-specific reserve and commit calls live behind
 * {@link VirtualMemory} (POSIX mmap, Windows VirtualAlloc).
 *
 * <h2>The {@code sirix.allocator} property</h2>
 *
 * The property used to select between this allocator and two pool-based ones, which are removed. It
 * may be left unset or set to {@code frame}. Any other value, the former {@code pool} and
 * {@code windowspool} included, is refused when this class is first used: a request for an
 * allocator that no longer exists must not silently run on another one.
 */
public final class Allocators {

  /** The system property that once selected the allocator; only {@value #FRAME} is accepted. */
  static final String ALLOCATOR_PROPERTY = "sirix.allocator";

  static final String FRAME = "frame";

  private Allocators() {}

  private static final MemorySegmentAllocator INSTANCE = resolve(System.getProperty(ALLOCATOR_PROPERTY));

  /**
   * The allocator for the configured {@code choice}.
   *
   * @param choice the value of {@code sirix.allocator}, or {@code null} when it is not set
   * @throws IllegalArgumentException for any value other than {@code frame}
   */
  static MemorySegmentAllocator resolve(final @Nullable String choice) {
    if (choice != null && !FRAME.equalsIgnoreCase(choice)) {
      throw new IllegalArgumentException("-D" + ALLOCATOR_PROPERTY + "=" + choice + " is not supported: the pool "
          + "allocators were removed and the frame-slot allocator is the only one. Remove the property.");
    }
    return FrameSlotAllocator.getInstance();
  }

  /**
   * Returns the process-wide {@link MemorySegmentAllocator}. The caller is responsible for invoking
   * {@link MemorySegmentAllocator#init(long)} before first use (typically done by
   * {@code Databases.initAllocator}).
   */
  public static MemorySegmentAllocator getInstance() {
    return INSTANCE;
  }
}
