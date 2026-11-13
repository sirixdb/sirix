package io.sirix.cache;

import io.sirix.utils.OS;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public final class WindowsMemorySegmentAllocator implements MemorySegmentAllocator {

  // Constants for Windows VirtualAlloc and VirtualFree flags
  private static final long MEM_COMMIT = 0x1000;
  private static final long MEM_RESERVE = 0x2000;
  private static final long MEM_RELEASE = 0x8000;
  private static final long PAGE_READWRITE = 0x04;

  private static final long[] PAGE_SIZES = {4096, 8192, 16384, 32768, 65536, 131072, 262144};

  // Deques for different size classes
  private final Deque<MemorySegment>[] segmentPools = new Deque[PAGE_SIZES.length];

  private static final MethodHandle virtualAllocHandle;
  private static final MethodHandle virtualFreeHandle;

  // Obtain handles to the Windows API functions
  // Method handles for VirtualAlloc and VirtualFree
  private static final Linker linker = Linker.nativeLinker();

  static {
    // VirtualAlloc/VirtualFree live in kernel32.dll. The nativeLinker's default lookup only
    // covers the C runtime, so binding them through it threw at class-init on real Windows —
    // the allocator had never actually been loadable there. Bind from kernel32 explicitly,
    // and only on Windows, so class-loading on other platforms stays side-effect free.
    MethodHandle alloc = null;
    MethodHandle free = null;
    if (OS.isWindows()) {
      final SymbolLookup kernel32 = SymbolLookup.libraryLookup("kernel32.dll", Arena.global());
      alloc = linker.downcallHandle(
          kernel32.find("VirtualAlloc")
                  .orElseThrow(() -> new RuntimeException("VirtualAlloc not found in kernel32.dll")),
          FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG, JAVA_LONG, JAVA_LONG));
      free = linker.downcallHandle(
          kernel32.find("VirtualFree").orElseThrow(() -> new RuntimeException("VirtualFree not found in kernel32.dll")),
          FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));
    }
    virtualAllocHandle = alloc;
    virtualFreeHandle = free;
  }

  private static final WindowsMemorySegmentAllocator INSTANCE = new WindowsMemorySegmentAllocator();
  private final AtomicLong maxBufferSize = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong committedBytes = new AtomicLong();
  private final ConcurrentHashMap<Long, Long> allocations = new ConcurrentHashMap<>();
  private volatile boolean initialized = false;

  private WindowsMemorySegmentAllocator() {}

  public static WindowsMemorySegmentAllocator getInstance() {
    return INSTANCE;
  }

  @Override
  public synchronized void init(long maxSegmentAllocationSize) {
    if (virtualAllocHandle == null) {
      throw new IllegalStateException("WindowsMemorySegmentAllocator requires Windows (kernel32 VirtualAlloc)");
    }
    if (maxSegmentAllocationSize <= 0) {
      throw new IllegalArgumentException("allocator budget must be positive");
    }
    if (initialized) {
      return;
    }
    // Initialize Deques for each size class
    for (int i = 0; i < PAGE_SIZES.length; i++) {
      segmentPools[i] = new ConcurrentLinkedDeque<>();
    }

    this.maxBufferSize.set(MemorySegmentAllocator.clampToPhysicalHeadroom(maxSegmentAllocationSize));
    this.initialized = true;
  }

  @Override
  public boolean isInitialized() {
    return initialized;
  }

  @Override
  public long getMaxBufferSize() {
    return this.maxBufferSize.get();
  }

  /**
   * Borrow a memory segment of the given size. If a segment is available in the pool, reuse it.
   * Otherwise, allocate a new one.
   *
   * @param size The size of the segment to allocate.
   * @return A MemorySegment of the specified size.
   */
  public MemorySegment borrowSegment(final long size) {
    return allocate(SegmentAllocators.roundUpToPowerOfTwo(size));
  }

  /**
   * Return a memory segment back to the pool.
   *
   * @param segment The MemorySegment to return.
   */
  public void release(MemorySegment segment) {
    long size = segment.byteSize();
    if (!isPoolable(size)) {
      // Oversized segments were VirtualAlloc'd outside the pool; free them immediately.
      free(segment);
      return;
    }
    int index = SegmentAllocators.getIndexForSize(size);

    if (index < 0 || index >= segmentPools.length) {
      throw new IllegalStateException("Returned segment with invalid size: " + size);
    }

    segmentPools[index].offer(segment);
  }

  private static boolean isPoolable(final long size) {
    return size >= PAGE_SIZES[0] && size <= PAGE_SIZES[PAGE_SIZES.length - 1];
  }

  @Override
  public void free() {

  }

  /**
   * Reset a memory segment by clearing its contents. Windows implementation: Falls back to fill() for
   * now. Could be optimized with VirtualFree/VirtualAlloc with MEM_RESET in the future.
   */
  @Override
  public void resetSegment(MemorySegment segment) {
    // Windows: Could use VirtualFree with MEM_DECOMMIT or just skip
    // For now, fall back to fill for Windows
    if (segment != null && segment.byteSize() > 0) {
      segment.fill((byte) 0);
    }
  }

  /**
   * Allocate a memory segment using VirtualAlloc.
   *
   * @param size The size to allocate.
   * @return The allocated MemorySegment.
   */
  // Sonar S135: the exits of this hot loop are its compiled shape. Funnelling them through one exit
  // adds a flag test for each row.
  // Sonar S3776: runs for every page and is kept as one compiled unit. Splitting it changes what the
  // compiler inlines on that path.
  @SuppressWarnings({"java:S135", "java:S3776"})
  @Override
  public MemorySegment allocate(final long size) {
    if (!initialized) {
      throw new IllegalStateException("allocator must be initialized before borrowing memory");
    }
    if (size <= 0 || size > Long.MAX_VALUE - 4095L) {
      throw new IllegalArgumentException("invalid allocation size: " + size);
    }
    final long capacity = size <= PAGE_SIZES[PAGE_SIZES.length - 1]
        ? SegmentAllocators.roundUpToPowerOfTwo(size)
        : (size + 4095L) & -4096L;
    final int index = isPoolable(capacity)
        ? SegmentAllocators.getIndexForSize(capacity)
        : -1;
    if (index >= 0) {
      final MemorySegment pooled = segmentPools[index].poll();
      if (pooled != null) {
        return pooled;
      }
    }
    final long limit = FallbackScratch.pageAllowance(maxBufferSize.get());
    for (;;) {
      final long committed = committedBytes.get();
      if (capacity > limit - committed) {
        if (capacity <= limit && reclaimIdleSegment()) {
          continue;
        }
        throw new OutOfMemoryError("Windows allocator page commitment exceeds " + limit + " bytes");
      }
      if (committedBytes.compareAndSet(committed, committed + capacity)) {
        break;
      }
    }
    MemorySegment address = MemorySegment.NULL;
    try {
      address = (MemorySegment) virtualAllocHandle.invoke(MemorySegment.NULL, capacity, MEM_COMMIT | MEM_RESERVE,
          PAGE_READWRITE);
      if (address.equals(MemorySegment.NULL)) {
        throw new OutOfMemoryError("Failed to allocate memory segment of size " + size);
      }
      final MemorySegment segment = address.reinterpret(capacity);
      allocations.put(address.address(), capacity);
      return segment;
    } catch (Throwable failure) {
      if (!address.equals(MemorySegment.NULL)) {
        try {
          virtualFreeHandle.invoke(address, 0L, MEM_RELEASE);
        } catch (Throwable cleanup) {
          failure.addSuppressed(cleanup);
        }
      }
      committedBytes.addAndGet(-capacity);
      if (failure instanceof Error error) {
        throw error;
      }
      throw new IllegalStateException("Allocation failed", failure);
    }
  }

  private boolean reclaimIdleSegment() {
    for (int index = segmentPools.length - 1; index >= 0; index--) {
      final MemorySegment idle = segmentPools[index].pollLast();
      if (idle != null) {
        free(idle);
        return true;
      }
    }
    return false;
  }

  @Override
  public long getPhysicalMemoryBytes() {
    return committedBytes.get() + FallbackScratch.of(this).mappedBytes();
  }

  /**
   * Free a memory segment using VirtualFree.
   *
   * @param segment The segment to free.
   */
  public void free(MemorySegment segment) {
    final long address = segment.address();
    final Long bytes = allocations.remove(address);
    if (bytes == null) {
      throw new IllegalArgumentException("segment is not owned by this allocator");
    }
    try {
      final long result = (long) virtualFreeHandle.invoke(segment, 0L, MEM_RELEASE);
      if (result == 0) {
        throw new IllegalStateException("Memory release failed");
      }
      committedBytes.addAndGet(-bytes);
    } catch (Throwable failure) {
      allocations.put(address, bytes);
      throw new IllegalStateException("Memory release failed", failure);
    }
  }
}
