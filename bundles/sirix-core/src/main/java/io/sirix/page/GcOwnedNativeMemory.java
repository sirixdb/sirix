package io.sirix.page;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

final class GcOwnedNativeMemory {

  private GcOwnedNativeMemory() {}

  static MemorySegment allocate(final long byteSize) {
    if (byteSize < 0L) {
      throw new IllegalArgumentException("byteSize=" + byteSize);
    }
    return Arena.ofAuto().allocate(byteSize, 8L);
  }
}
