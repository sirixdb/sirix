package io.sirix.cache;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.foreign.ValueLayout.*;

public final class LinuxMemorySegmentAllocator implements MemorySegmentAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(LinuxMemorySegmentAllocator.class.getName());

  private static final Linker LINKER = Linker.nativeLinker();

  // Static final MethodHandles for mmap and munmap to avoid repeated lookups
  private static final MethodHandle MMAP;
  private static final MethodHandle MUNMAP;

  private static final int PROT_READ = 0x1;   // Page can be read
  private static final int PROT_WRITE = 0x2;  // Page can be written
  private static final int MAP_PRIVATE = 0x02; // Changes are private
  private static final int MAP_ANONYMOUS = 0x20; // Anonymous mapping

  static {
    MMAP = LINKER.downcallHandle(LINKER.defaultLookup()
                                       .find("mmap")
                                       .orElseThrow(() -> new RuntimeException("mmap not found")),
                                 FunctionDescriptor.of(ADDRESS,
                                                       ADDRESS,
                                                       JAVA_LONG,
                                                       JAVA_INT,
                                                       JAVA_INT,
                                                       JAVA_INT,
                                                       JAVA_LONG));

    MUNMAP = LINKER.downcallHandle(LINKER.defaultLookup()
                                         .find("munmap")
                                         .orElseThrow(() -> new RuntimeException("munmap not found")),
                                   FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG));
  }

  // Define power-of-two sizes for mapping: 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB
  private static final long[] SEGMENT_SIZES =
      { FOUR_KB, EIGHT_KB, SIXTEEN_KB, THIRTYTWO_KB, SIXTYFOUR_KB, ONE_TWENTYEIGHT_KB, TWO_FIFTYSIX_KB };
  private static final int PRE_ALLOCATE_COUNT = 100; // Number of segments to touch upfront
  private static final long MAX_BUFFER_SIZE = 1L << 30; // 1GB   FIXME: Make this configurable

  private final List<MemorySegment> topLevelMappedSegments = new CopyOnWriteArrayList<>();

  private final Deque<MemorySegment>[] segmentPools = new Deque[SEGMENT_SIZES.length];

  private static final LinuxMemorySegmentAllocator INSTANCE = new LinuxMemorySegmentAllocator();

  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  private final AtomicLong maxBufferSize = new AtomicLong(MAX_BUFFER_SIZE);

  /**
   * Private constructor to enforce singleton pattern.
   * Use getInstance() to obtain the singleton instance.
   */
  private LinuxMemorySegmentAllocator() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOGGER.info("Shutting down LinuxMemorySegmentAllocator...");
      free();
      LOGGER.info("LinuxMemorySegmentAllocator shutdown complete.");
    }));
  }

  public static LinuxMemorySegmentAllocator getInstance() {
    return INSTANCE;
  }

  @Override
  public void init(long maxBufferSize) {
    if (!isInitialized.compareAndSet(false, true)) {
      return;
    }

    LOGGER.info("Initializing LinuxMemorySegmentAllocator...");

    // Initialize segment pools for each size class
    for (int i = 0; i < SEGMENT_SIZES.length; i++) {
      segmentPools[i] = new ConcurrentLinkedDeque<>();
    }

    // Pre-allocate and touch memory segments
    preAllocateAndTouchMemory(maxBufferSize);
  }

  private void preAllocateAndTouchMemory(long maxBufferSize) {
    for (int index = 0; index < SEGMENT_SIZES.length; index++) {
      long segmentSize = SEGMENT_SIZES[index];
      Deque<MemorySegment> pool = segmentPools[index];

      // Map a large memory segment and split it into smaller segments
      MemorySegment hugeSegment = mapMemory(maxBufferSize);
      this.maxBufferSize.set(maxBufferSize);

      topLevelMappedSegments.add(hugeSegment);

      for (long l = 0, max = maxBufferSize / segmentSize; l < max; l++) {
        long actualOffset = l * segmentSize;
        MemorySegment segment = hugeSegment.asSlice(actualOffset, segmentSize);

        if (l < PRE_ALLOCATE_COUNT) {
          // "Touch" the segment to ensure it is mapped into physical memory
          segment.set(JAVA_BYTE, 0, (byte) 0);
        }

        pool.add(segment);
      }
    }
  }

  @Override
  public long getMaxBufferSize() {
    return maxBufferSize.get();
  }

  @Override
  public MemorySegment allocate(long size) {
    int index = SegmentAllocators.getIndexForSize(size);

    if (index < 0 || index >= segmentPools.length) {
      throw new IllegalArgumentException("Unsupported size: " + size);
    }

    Deque<MemorySegment> pool = segmentPools[index];
    MemorySegment segment = pool.poll();

    if (segment == null) {
      throw new OutOfMemoryError("No preallocated segments available for size: " + size);
    }

    segment.fill((byte) 0); // Clear the segment
    return segment;
  }

  public MemorySegment mapMemory(long totalSize) {
    MemorySegment addr;
    try {
      addr = (MemorySegment) MMAP.invoke(MemorySegment.NULL,
                                         totalSize,
                                         PROT_READ | PROT_WRITE,
                                         MAP_PRIVATE | MAP_ANONYMOUS,
                                         -1,
                                         0);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to allocate memory via mmap", e);
    }
    if (addr == MemorySegment.NULL) {
      throw new OutOfMemoryError("Failed to allocate memory via mmap");
    }
    return addr.reinterpret(totalSize);
  }

  public void releaseMemory(MemorySegment addr, long size) {
    try {
      int result = (int) MUNMAP.invoke(addr, size);
      if (result != 0) {
        throw new RuntimeException("Failed to release memory via munmap");
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to release memory via munmap", e);
    }
  }

  @Override
  public void release(MemorySegment segment) {
    long size = segment.byteSize();
    int index = SegmentAllocators.getIndexForSize(size);

    if (index >= 0 && index < segmentPools.length) {
      segmentPools[index].offer(segment);
    } else {
      throw new IllegalArgumentException("Segment size not supported for reuse: " + size);
    }
  }

  /**
   * Cleanup all mmap'd top-level segments.
   * Called automatically during JVM shutdown.
   */
  @Override
  public void free() {
    if (!isInitialized.get()) {
      LOGGER.debug("Allocator is not initialized, nothing to free.");
      return;
    }

    LOGGER.info("Cleaning up mmap'd memory segments...");
    for (MemorySegment segment : topLevelMappedSegments) {
      try {
        releaseMemory(segment, segment.byteSize());
      } catch (Exception e) {
        LOGGER.error("Failed to release segment: {}", e.getMessage());
      }
    }
    topLevelMappedSegments.clear();

    for (Deque<MemorySegment> pool : segmentPools) {
      pool.clear();
    }

    isInitialized.set(false);
  }

  public static void main(String[] args) {
    LinuxMemorySegmentAllocator allocator = LinuxMemorySegmentAllocator.getInstance();

    MemorySegment segment4KB = allocator.allocate(4096);
    System.out.println("Allocated 4KB segment: " + segment4KB);

    MemorySegment segment128KB = allocator.allocate(131072);
    System.out.println("Allocated 128KB segment: " + segment128KB);

    allocator.release(segment4KB);
    allocator.release(segment128KB);
  }
}
