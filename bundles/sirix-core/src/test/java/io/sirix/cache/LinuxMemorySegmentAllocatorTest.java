package io.sirix.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.*;

public class LinuxMemorySegmentAllocatorTest {

  private final LinuxMemorySegmentAllocator allocator = LinuxMemorySegmentAllocator.getInstance();

  @BeforeEach
  public void setUp() {
    // Initialize the allocator with a large enough buffer size to accommodate
    // accumulated memory from previous tests (allocator is a singleton)
    allocator.init(8L * 1024 * 1024 * 1024); // Set max buffer size to 8GB
  }

  @AfterEach
  public void tearDown() {
    // Optional: Perform any necessary cleanup after each test
  }

  @Test
  public void testAllocate4KB() {
    MemorySegment segment = allocator.allocate(4096);
    assertNotNull(segment, "Allocated segment should not be null");
    assertEquals(4096, segment.byteSize(), "Segment size should be 4096 bytes");
    allocator.release(segment);
  }

  @Test
  public void testAllocate16KB() {
    MemorySegment segment = allocator.allocate(16384);
    assertNotNull(segment, "Allocated segment should not be null");
    assertEquals(16384, segment.byteSize(), "Segment size should be 16384 bytes");
    allocator.release(segment);
  }

  @Test
  public void testAllocate32KB() {
    MemorySegment segment = allocator.allocate(32768);
    assertNotNull(segment, "Allocated segment should not be null");
    assertEquals(32768, segment.byteSize(), "Segment size should be 32768 bytes");
    allocator.release(segment);
  }

  @Test
  public void testAllocate64KB() {
    MemorySegment segment = allocator.allocate(65536);
    assertNotNull(segment, "Allocated segment should not be null");
    assertEquals(65536, segment.byteSize(), "Segment size should be 65536 bytes");
    allocator.release(segment);
  }

  @Test
  public void testAllocate128KB() {
    MemorySegment segment = allocator.allocate(131072);
    assertNotNull(segment, "Allocated segment should not be null");
    assertEquals(131072, segment.byteSize(), "Segment size should be 131072 bytes");
    allocator.release(segment);
  }

  @Test
  public void testAllocate256KB() {
    MemorySegment segment = allocator.allocate(262144);
    assertNotNull(segment, "Allocated segment should not be null");
    assertEquals(262144, segment.byteSize(), "Segment size should be 262144 bytes");
    allocator.release(segment);
  }

  @Test
  public void testAllocateMaximumSize() {
    // Test allocation for the maximum buffer size defined in the allocator
    MemorySegment segment = allocator.mapMemory(1 << 30);
    assertNotNull(segment, "Allocated maximum segment should not be null");
    assertEquals(1 << 30, segment.byteSize(), "Segment size should be 1GB");
    allocator.releaseMemory(segment, 1 << 30);
  }

  @Test
  public void testReleaseSegment() {
    MemorySegment segment = allocator.allocate(4096);
    assertNotNull(segment, "Allocated segment should not be null");

    allocator.release(segment);

    MemorySegment reusedSegment = allocator.allocate(4096);
    assertEquals(segment.byteSize(), reusedSegment.byteSize(), "Reused segment should have the same size");
    assertNotSame(segment, reusedSegment, "Reused segment should be a new instance");

    allocator.release(reusedSegment);
  }

  @Test
  public void testReleaseUnsupportedSize() {
    MemorySegment segment = allocator.allocate(4096);
    allocator.release(segment);

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment unsupportedSegment = arena.allocate(550000);

      assertThrows(IllegalArgumentException.class, () -> {
        allocator.release(unsupportedSegment);
      }, "Should throw IllegalArgumentException for segment not managed by the allocator");
    }
  }

  @Test
  public void testAllocateUnsupportedSize() {
    assertThrows(IllegalArgumentException.class, () -> {
      allocator.allocate(550000);
    }, "Should throw IllegalArgumentException for unsupported size");
  }

  @Test
  public void testAllocateZeroSize() {
    assertThrows(AssertionError.class, () -> {
      allocator.allocate(0);
    }, "Should throw IllegalArgumentException for zero size");
  }

  @Test
  public void testAllocateAboveMaximumSize() {
    assertThrows(IllegalArgumentException.class, () -> {
      allocator.allocate(1024 * 1024); // Attempt to allocate 1MB
    }, "Should throw IllegalArgumentException for size above maximum supported size");
  }

  @Test
  public void testMemoryLeak() {
    for (int i = 0; i < 1000; i++) {
      MemorySegment segment = allocator.allocate(4096);
      allocator.release(segment);
    }
  }

  @Test
  public void testMultipleAllocationsAndReleases() {
    for (int i = 0; i < 100; i++) {
      MemorySegment segment = allocator.allocate(4096);
      assertNotNull(segment, "Allocated segment should not be null");
      assertEquals(4096, segment.byteSize(), "Segment size should be 4096 bytes");
      allocator.release(segment);
    }
  }

  @Test
  public void testReleaseAllSegmentSizes() {
    int[] sizes = {4096, 16384, 32768, 65536, 131072, 262144};
    for (int size : sizes) {
      MemorySegment segment = allocator.allocate(size);
      assertNotNull(segment, "Allocated segment should not be null for size " + size);
      assertEquals(size, segment.byteSize(), "Segment size should be " + size + " bytes");
      allocator.release(segment);
    }
  }
}
