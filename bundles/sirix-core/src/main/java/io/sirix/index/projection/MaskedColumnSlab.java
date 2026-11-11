package io.sirix.index.projection;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import io.sirix.cache.MemorySegmentAllocator;
import java.util.concurrent.atomic.AtomicBoolean;

final class MaskedColumnSlab implements Runnable {
  private final MemorySegment allocation;
  private final MemorySegmentAllocator allocator;
  private final AtomicBoolean released = new AtomicBoolean();

  MaskedColumnSlab(final MemorySegmentAllocator allocator, final MemorySegment allocation) {
    this.allocator = allocator;
    this.allocation = allocation;
  }

  MemorySegment segment() {
    return allocation.reinterpret(allocation.byteSize(), Arena.ofAuto(), ignored -> run());
  }

  @Override
  public void run() {
    if (released.compareAndSet(false, true)) {
      allocator.releaseSlab(allocation);
    }
  }
}
