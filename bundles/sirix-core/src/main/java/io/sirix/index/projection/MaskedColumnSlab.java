package io.sirix.index.projection;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicInteger;

final class MaskedColumnSlab implements Runnable {
  private final MemorySegment allocation;
  private final ColumnSlabs slabs = new ColumnSlabs(1);
  private final AtomicInteger readers = new AtomicInteger();

  MaskedColumnSlab(final MemorySegment allocation) {
    this.allocation = allocation;
    slabs.set(0, allocation);
  }

  MemorySegment segment() {
    return allocation.reinterpret(allocation.byteSize(), Arena.ofAuto(), ignored -> run());
  }

  void retain() {
    readers.incrementAndGet();
  }

  void release() {
    if (readers.decrementAndGet() == 0) {
      slabs.run();
    }
  }

  @Override
  public void run() {
    if (readers.get() == 0) {
      slabs.run();
    }
  }
}
