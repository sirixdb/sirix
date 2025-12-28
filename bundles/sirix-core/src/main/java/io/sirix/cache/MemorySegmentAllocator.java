package io.sirix.cache;

import java.lang.foreign.MemorySegment;

public interface MemorySegmentAllocator {

  int FOUR_KB = 4096;
  int EIGHT_KB = 8192;
  int SIXTEEN_KB = 16384;
  int THIRTYTWO_KB = 32768;
  int SIXTYFOUR_KB = 65536;
  int ONE_TWENTYEIGHT_KB = 131072;
  int TWO_FIFTYSIX_KB = 262144;

  int[] SEGMENT_SIZES = { FOUR_KB, EIGHT_KB, SIXTEEN_KB, THIRTYTWO_KB, SIXTYFOUR_KB, ONE_TWENTYEIGHT_KB, TWO_FIFTYSIX_KB };

  void init(long maxBufferSize);
  
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
   * Reset a memory segment by clearing its contents.
   * Implementations should use the most efficient approach available (e.g., madvise on Linux).
   * Thread-safe: can be called concurrently on different segments.
   * 
   * @param segment the segment to reset
   */
  void resetSegment(MemorySegment segment);
}
