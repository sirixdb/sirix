package io.sirix.page;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Native memory whose lifetime is that of the Java objects referencing it, carved from shared
 * automatic-arena slabs.
 *
 * <p>
 * A compact HOT leaf image or an overflow page's native mirror wants exactly the semantics a heap
 * byte array has — readable for as long as anything references it, freed by the collector, no
 * explicit release to get wrong — but on native memory, so that the segment accessors every page
 * shares never see a heap receiver (see {@link PageLayout#getBitmapWord(byte[], int)}). One
 * {@link Arena#ofAuto()} allocation per image would give that at about a microsecond each (a
 * cleaner registration and a malloc per page). This allocator bump-allocates slices out of 1 MiB
 * automatic-arena slabs instead: a slice costs a few hundred nanoseconds, every slice of a slab
 * shares the slab's session, and the slab is freed once no slice of it is reachable. The cost of
 * that sharing is retention: a slab stays mapped while any of its slices lives, so an image can
 * keep up to a slab of memory alive on its own; images are allocated in page-load order and evicted
 * in roughly that order, which keeps the overhang small. Sizes above {@link #DEDICATED_THRESHOLD}
 * get an arena of their own.
 * </p>
 *
 * <p>
 * Thread-safe without a thread-local: slices are claimed with one atomic add on the current slab,
 * and an exhausted slab is replaced with a compare-and-set. Deliberately no {@code ThreadLocal}:
 * every thread-local constructed shifts the hash-slot layout of every later one, and a hot
 * {@code ThreadLocal.get} elsewhere can start probing because of it.
 * </p>
 */
final class GcOwnedNativeMemory {

  /** Slab size; also the unit of memory an image can keep alive on its own. */
  static final long SLAB_BYTES = 1L << 20;

  /** Requests larger than this get their own automatic arena instead of a slab slice. */
  static final long DEDICATED_THRESHOLD = SLAB_BYTES >>> 3;

  /** Slices start on this boundary; every record and leaf reader uses unaligned layouts anyway. */
  private static final long SLICE_ALIGNMENT = 8L;

  private static final class Slab {
    final MemorySegment memory = Arena.ofAuto().allocate(SLAB_BYTES, 64L);
    final AtomicLong used = new AtomicLong();
  }

  private static final AtomicReference<Slab> CURRENT = new AtomicReference<>(new Slab());

  private GcOwnedNativeMemory() {}

  /**
   * A zero-initialised native segment of exactly {@code byteSize} bytes, alive while referenced.
   *
   * @param byteSize the size, at least zero
   * @return the segment; never shared with any other caller
   */
  static MemorySegment allocate(final long byteSize) {
    if (byteSize < 0L) {
      throw new IllegalArgumentException("byteSize=" + byteSize);
    }
    if (byteSize > DEDICATED_THRESHOLD) {
      return Arena.ofAuto().allocate(byteSize, SLICE_ALIGNMENT);
    }
    final long claimed = (byteSize + SLICE_ALIGNMENT - 1L) & -SLICE_ALIGNMENT;
    for (;;) {
      final Slab slab = CURRENT.get();
      final long offset = slab.used.getAndAdd(claimed);
      if (offset + claimed <= SLAB_BYTES) {
        return slab.memory.asSlice(offset, byteSize);
      }
      // Exhausted (this claim or a racing one overran the end): install a fresh slab unless a racing
      // thread already has, then claim again from whatever is current. Nothing is lost but the tail.
      CURRENT.compareAndSet(slab, new Slab());
    }
  }
}
