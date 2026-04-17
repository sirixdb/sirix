/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.utils.OS;

/**
 * Single entry-point for obtaining the configured {@link MemorySegmentAllocator}.
 * Callers should prefer {@link #getInstance()} over referencing a specific
 * implementation class directly, so the allocator choice can be toggled in
 * one place.
 *
 * <h2>Dispatch</h2>
 *
 * <ul>
 *   <li><b>Windows</b>: always {@link WindowsMemorySegmentAllocator}.</li>
 *   <li><b>Linux default</b>: {@link FrameSlotAllocator} — Umbra/LeanStore-style
 *       fixed-address frame slots with optimistic versioned reads. Stable slot
 *       addresses for the lifetime of the process eliminate the cross-thread
 *       recycling race that surfaces under 20-thread parallel scans.</li>
 *   <li><b>Linux with {@code -Dsirix.allocator=pool}</b>:
 *       {@link LinuxMemorySegmentAllocator} — the legacy pool-based allocator.
 *       Retained for emergency rollback only.</li>
 * </ul>
 *
 * <p>The returned allocator is a process-wide singleton per implementation.
 * All callers that need native buffer memory must route through this factory
 * rather than referencing a specific {@code ...Allocator.getInstance()}
 * directly — otherwise two different allocators end up serving different
 * code paths and the toggle is ineffective.
 */
public final class Allocators {

  private Allocators() {
  }

  private static final MemorySegmentAllocator INSTANCE = resolve();

  private static MemorySegmentAllocator resolve() {
    if (OS.isWindows()) {
      return WindowsMemorySegmentAllocator.getInstance();
    }
    final String choice = System.getProperty("sirix.allocator", "frame");
    if ("pool".equalsIgnoreCase(choice)) {
      return LinuxMemorySegmentAllocator.getInstance();
    }
    return FrameSlotAllocator.getInstance();
  }

  /**
   * Returns the process-wide {@link MemorySegmentAllocator}. Initialized once
   * at class-load time from the {@code sirix.allocator} system property; the
   * caller is responsible for invoking {@link MemorySegmentAllocator#init(long)}
   * before first use (typically done by {@code Databases.initAllocator}).
   */
  public static MemorySegmentAllocator getInstance() {
    return INSTANCE;
  }
}
