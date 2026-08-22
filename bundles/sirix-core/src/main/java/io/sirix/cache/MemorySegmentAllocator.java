package io.sirix.cache;

import com.sun.management.OperatingSystemMXBean;

import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;

public interface MemorySegmentAllocator {

  int FOUR_KB = 4096;
  int EIGHT_KB = 8192;
  int SIXTEEN_KB = 16384;
  int THIRTYTWO_KB = 32768;
  int SIXTYFOUR_KB = 65536;
  int ONE_TWENTYEIGHT_KB = 131072;
  int TWO_FIFTYSIX_KB = 262144;

  int[] SEGMENT_SIZES =
      {FOUR_KB, EIGHT_KB, SIXTEEN_KB, THIRTYTWO_KB, SIXTYFOUR_KB, ONE_TWENTYEIGHT_KB, TWO_FIFTYSIX_KB};

  void init(long maxBufferSize);

  /** Physical RAM left to the OS and page cache by {@link #clampToPhysicalHeadroom(long)}. */
  long MIN_OS_RESERVE_BYTES = 2L << 30;

  /** Arena floor: below this the size classes cannot hold a working set and every read thrashes. */
  long MIN_OFFHEAP_BYTES = 512L << 20;

  /**
   * Cap an arena request at what this machine can actually back, leaving room for the Java heap and
   * the OS. Implementations must apply this in {@link #init(long)} — the request reaching them is a
   * fixed size chosen without knowledge of the host (a persisted database config, or a benchmark
   * runner's default), and committing all of it on a smaller box gets the process OOM-killed by the
   * kernel. That failure is invisible to every in-process budget, because nothing in the JVM is over
   * ITS budget.
   *
   * @param requestedBytes the arena size the caller asked for
   * @return the size to actually reserve, never above the host's headroom
   */
  static long clampToPhysicalHeadroom(final long requestedBytes) {
    if (!(ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean os)) {
      return requestedBytes; // unknown host — honour the request rather than guess
    }
    final long physical = os.getTotalMemorySize();
    if (physical <= 0) {
      return requestedBytes;
    }
    final long headroom = physical - Runtime.getRuntime().maxMemory() - Math.max(MIN_OS_RESERVE_BYTES, physical / 10);
    if (headroom < MIN_OFFHEAP_BYTES) {
      // The heap alone already crowds the box. Give the arena the floor it needs to function, so the
      // operator's -Xmx is what gets blamed rather than a silently unusable buffer pool.
      return Math.min(requestedBytes, MIN_OFFHEAP_BYTES);
    }
    return Math.min(requestedBytes, headroom);
  }

  /**
   * Check if the allocator has been initialized.
   * 
   * @return true if init() has been called and completed successfully
   */
  boolean isInitialized();

  void free();

  MemorySegment allocate(long size);

  void release(MemorySegment segment);

  long getMaxBufferSize();

  /**
   * Physical off-heap bytes currently committed by this allocator. Used by metrics; implementations
   * that don't track physical commitment return 0.
   *
   * @return committed physical bytes, or 0 if not tracked
   */
  default long getPhysicalMemoryBytes() {
    return 0L;
  }

  /**
   * Reset a memory segment by clearing its contents. Implementations should use the most efficient
   * approach available (e.g., madvise on Linux). Thread-safe: can be called concurrently on different
   * segments.
   * 
   * @param segment the segment to reset
   */
  void resetSegment(MemorySegment segment);
}
