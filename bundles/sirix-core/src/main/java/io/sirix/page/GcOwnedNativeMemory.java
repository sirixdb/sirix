package io.sirix.page;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Native storage with one automatic arena per allocation. Independent scopes keep a retained image
 * from pinning unrelated consumers' bytes, so its cache charge matches the storage it retains.
 *
 * <p>
 * One arena per allocation is a Java allocation per call, so this is for storage acquired per PAGE,
 * never per element: an overflow page's native mirror, and the compact HOT-leaf decoder's fallback
 * for when {@link io.sirix.cache.CompactImageStore} has no block to lease. The compact decoder
 * itself takes a pooled block, because it runs per decoded fragment on a read path.
 * </p>
 */
final class GcOwnedNativeMemory {

  private GcOwnedNativeMemory() {}

  static MemorySegment allocate(final long byteSize) {
    if (byteSize < 0L) {
      throw new IllegalArgumentException("byteSize=" + byteSize);
    }
    return Arena.ofAuto().allocate(byteSize, 8L);
  }
}
