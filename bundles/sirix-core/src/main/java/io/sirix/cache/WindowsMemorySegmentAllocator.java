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
          kernel32.find("VirtualAlloc").orElseThrow(() -> new RuntimeException("VirtualAlloc not found in kernel32.dll")),
          FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG, JAVA_LONG, JAVA_LONG));
      free = linker.downcallHandle(
          kernel32.find("VirtualFree").orElseThrow(() -> new RuntimeException("VirtualFree not found in kernel32.dll")),
          FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));
    }
    virtualAllocHandle = alloc;
    virtualFreeHandle = free;
  }

  private static final WindowsMemorySegmentAllocator INSTANCE = new WindowsMemorySegmentAllocator();
  private AtomicLong maxBufferSize = new AtomicLong(Long.MAX_VALUE);
  private volatile boolean initialized = false;

  private WindowsMemorySegmentAllocator() {}

  public static WindowsMemorySegmentAllocator getInstance() {
    return INSTANCE;
  }

  @Override
  public void init(long maxSegmentAllocationSize) {
    if (virtualAllocHandle == null) {
      throw new IllegalStateException(
          "WindowsMemorySegmentAllocator requires Windows (kernel32 VirtualAlloc)");
    }
    // Initialize Deques for each size class
    for (int i = 0; i < PAGE_SIZES.length; i++) {
      segmentPools[i] = new ConcurrentLinkedDeque<>();
    }

    this.maxBufferSize.set(maxSegmentAllocationSize);
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
  public MemorySegment borrowSegment(long size) {
    long roundedSize = SegmentAllocators.roundUpToPowerOfTwo(size);
    int index = SegmentAllocators.getIndexForSize(roundedSize);

    if (index < 0 || index >= segmentPools.length) {
      throw new IllegalArgumentException("Unsupported size: " + size);
    }

    // Attempt to reuse a segment from the pool
    MemorySegment segment = segmentPools[index].poll();
    if (segment != null) {
      return segment;
    }

    // No reusable segment, allocate a new one
    return allocate(roundedSize);
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
      try {
        virtualFreeHandle.invoke(segment, 0L, MEM_RELEASE);
      } catch (Throwable t) {
        throw new IllegalStateException("VirtualFree failed for oversized segment of size " + size, t);
      }
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
  public MemorySegment allocate(long size) {
    // Reuse a released segment before VirtualAlloc'ing a new one: release() pools segments by size
    // class, but this method never polled that pool, so every allocation was a fresh VirtualAlloc
    // and every release parked a segment forever — native memory grew unbounded with read traffic.
    // Only reuse a pooled segment that is at least `size` bytes (the pool may hold a smaller raw
    // segment filed under the same rounded class); slice it to the requested size for the caller.
    // Oversized requests (e.g. large-value overflow decompression) bypass the size-class pool —
    // getIndexForSize rejects anything outside [4 KiB, 256 KiB], but VirtualAlloc has no such limit.
    final int index = isPoolable(size) ? SegmentAllocators.getIndexForSize(size) : -1;
    if (index >= 0 && index < segmentPools.length) {
      final MemorySegment pooled = segmentPools[index].poll();
      if (pooled != null) {
        if (pooled.byteSize() >= size) {
          return pooled.byteSize() == size ? pooled : pooled.asSlice(0, size);
        }
        // Too small to satisfy this request — hand it back and fall through to a fresh allocation.
        segmentPools[index].offer(pooled);
      }
    }
    try {
      MemorySegment address =
          (MemorySegment) virtualAllocHandle.invoke(MemorySegment.NULL, size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);

      if (address.equals(MemorySegment.NULL)) {
        throw new OutOfMemoryError("Failed to allocate memory segment of size " + size);
      }

      return address.reinterpret(size);
    } catch (Throwable e) {
      throw new RuntimeException("Allocation failed", e);
    }
  }

  /**
   * Free a memory segment using VirtualFree.
   *
   * @param segment The segment to free.
   */
  public void free(MemorySegment segment) {
    try {
      long result = (long) virtualFreeHandle.invoke(segment.address(), 0, MEM_RELEASE);
      if (result == 0) {
        throw new IllegalStateException("Memory release failed");
      }
    } catch (Throwable e) {
      throw new RuntimeException("Memory release failed", e);
    }
  }
}
