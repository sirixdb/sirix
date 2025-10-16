package io.sirix.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test duplicate return detection in the allocator.
 */
public class AllocatorDuplicateReturnTest {

  private final LinuxMemorySegmentAllocator allocator = LinuxMemorySegmentAllocator.getInstance();

  @BeforeEach
  public void setUp() {
    allocator.init(1L << 30); // 1GB budget
  }

  @AfterEach
  public void tearDown() {
    allocator.free();
  }

  @Test
  public void testDuplicateReturnIsIgnored() {
    // Allocate a segment
    MemorySegment segment = allocator.allocate(4096);
    assertNotNull(segment);

    // Return it once (should succeed)
    allocator.release(segment);

    // Try to return it again (should be ignored)
    allocator.release(segment);

    // Should not crash or corrupt pool
    MemorySegment newSegment = allocator.allocate(4096);
    assertNotNull(newSegment);
    allocator.release(newSegment);
  }

  @Test
  public void testMultipleReturnsInSequence() {
    MemorySegment seg1 = allocator.allocate(4096);
    MemorySegment seg2 = allocator.allocate(4096);
    MemorySegment seg3 = allocator.allocate(4096);

    // Return in order
    allocator.release(seg1);
    allocator.release(seg2);
    allocator.release(seg3);

    // Try duplicate returns
    allocator.release(seg1); // Should be ignored
    allocator.release(seg2); // Should be ignored
    allocator.release(seg3); // Should be ignored

    // Verify pool still works
    for (int i = 0; i < 10; i++) {
      MemorySegment seg = allocator.allocate(4096);
      assertNotNull(seg);
      allocator.release(seg);
    }
  }

  @Test
  public void testConcurrentDuplicateReturns() throws InterruptedException {
    MemorySegment segment = allocator.allocate(65536);

    // Create multiple threads trying to return the same segment
    Thread[] threads = new Thread[5];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        allocator.release(segment);
      });
    }

    // Start all threads
    for (Thread t : threads) {
      t.start();
    }

    // Wait for all to complete
    for (Thread t : threads) {
      t.join();
    }

    // Only one should succeed, others ignored
    // Pool should still be functional
    MemorySegment newSeg = allocator.allocate(65536);
    assertNotNull(newSeg);
    allocator.release(newSeg);
  }
}


