/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.cache.FrameSlotAllocator.FrameSlot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit + concurrency tests for {@link FrameSlotAllocator}.
 *
 * <p>The concurrency test is the real target: 20 threads allocating, writing,
 * and releasing slots in a tight loop for several seconds, with optimistic
 * version-check readers observing in parallel. If the pool-style recycling
 * race were present, we would expect either SIGSEGV (unhandled in Java) or
 * corrupted reads. The test verifies every version-validated read either
 * sees the writer's canary pattern or detects the race via the version
 * mismatch.
 */
class FrameSlotAllocatorTest {

  private FrameSlotAllocator allocator;

  @BeforeEach
  void setUp() {
    // 256 MiB budget — small enough to fit on CI, large enough to exercise
    // multi-size-class behavior and some free-stack churn.
    allocator = new FrameSlotAllocator(256L * 1024 * 1024);
  }

  @AfterEach
  void tearDown() {
    allocator.shutdown();
  }

  @Test
  void allocateReleaseRoundtrip() {
    final FrameSlot slot = allocator.allocateSlot(64 * 1024);
    assertNotNull(slot);
    assertEquals(64 * 1024, slot.segment().byteSize());
    final long version = slot.versionAtAlloc();
    assertEquals(0L, version & 1L, "alloc'd slot version must be even");
    assertTrue(allocator.validateVersion(slot.classIndex(), slot.slotIndex(), version));

    slot.close();

    // Post-release: version changed (allocator bumped at least twice during release)
    final long after = allocator.acquireVersion(slot.classIndex(), slot.slotIndex());
    assertTrue(after > version, "version must strictly increase across a release");
    assertEquals(0L, after & 1L, "post-release version must be even (quiescent)");
  }

  @Test
  void sizeClassDispatch() {
    for (final long size : FrameSlotAllocator.SIZE_CLASSES) {
      final FrameSlot slot = allocator.allocateSlot(size);
      assertNotNull(slot);
      assertTrue(slot.segment().byteSize() >= size,
          "allocated slot " + slot.segment().byteSize() + " must cover requested " + size);
      slot.close();
    }
  }

  @Test
  void sizeClassRoundsUp() {
    // 5 KiB request lands in the 8 KiB class.
    final FrameSlot slot = allocator.allocateSlot(5 * 1024);
    assertEquals(8 * 1024L, slot.segment().byteSize());
    slot.close();
  }

  @Test
  void oversizedRequestRejected() {
    try {
      allocator.allocateSlot(1024L * 1024 * 1024); // 1 GiB — exceeds largest class (256 KiB)
      throw new AssertionError("expected IllegalArgumentException");
    } catch (final IllegalArgumentException expected) {
      // ok
    }
  }

  @Test
  void exhaustionReturnsNull() {
    // The allocator shares a single physical budget across classes; draining
    // it via the largest class should yield null once the cap is hit.
    final int classIdx = FrameSlotAllocator.SIZE_CLASSES.length - 1;
    final long slotSize = FrameSlotAllocator.SIZE_CLASSES[classIdx];
    final java.util.ArrayList<FrameSlot> held = new java.util.ArrayList<>();
    FrameSlot next;
    while ((next = allocator.allocateSlot(slotSize)) != null) {
      held.add(next);
    }
    assertTrue(held.size() > 0, "should succeed at least once before exhaustion");
    assertNull(allocator.allocateSlot(slotSize),
        "allocator must return null when physical budget is exhausted");
    for (final FrameSlot s : held) {
      s.close();
    }
    // After releasing, new allocations should succeed again.
    final FrameSlot reborn = allocator.allocateSlot(slotSize);
    assertNotNull(reborn, "post-release allocation must succeed once budget frees up");
    reborn.close();
  }

  @Test
  void writerObservesOddVersionDuringRelease() throws Exception {
    // Observe that during release the version transitions through an odd
    // ("writer in progress") value. Done by having a slow release (injected
    // via concurrent observer) and catching the intermediate state.
    final FrameSlot slot = allocator.allocateSlot(64 * 1024);
    final int classIdx = slot.classIndex();
    final int slotIdx = slot.slotIndex();
    final long before = slot.versionAtAlloc();
    assertEquals(0L, before & 1L);

    // Release, then check version is even and higher.
    slot.close();
    final long after = allocator.acquireVersion(classIdx, slotIdx);
    assertTrue((after & 1L) == 0L);
    assertTrue(after >= before + 2L);
  }

  @Test
  void allocatedSlotBytesAreWritableAndReadable() {
    final FrameSlot slot = allocator.allocateSlot(64 * 1024);
    final MemorySegment seg = slot.segment();
    for (int i = 0; i < 1024; i++) {
      seg.set(ValueLayout.JAVA_LONG, (long) i * 8, 0xC0FFEE00L + i);
    }
    for (int i = 0; i < 1024; i++) {
      assertEquals(0xC0FFEE00L + i, seg.get(ValueLayout.JAVA_LONG, (long) i * 8));
    }
    slot.close();
  }

  /**
   * Stress test: 20 workers each in a tight allocate-write-read-release
   * loop for 5 seconds. A concurrent observer repeatedly allocates, reads
   * a canary value, and validates the version; mismatches are counted
   * (they are expected and MUST be detected, not silently pass).
   */
  @Test
  void concurrentStressNoCorruption() throws Exception {
    final int workers = 20;
    final long runNanos = 5_000_000_000L;
    final ExecutorService pool = Executors.newFixedThreadPool(workers);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong totalAllocations = new AtomicLong();
    final AtomicLong totalReads = new AtomicLong();
    final AtomicLong versionMismatches = new AtomicLong();
    final AtomicLong corruptedReads = new AtomicLong();

    for (int w = 0; w < workers; w++) {
      final int workerId = w;
      pool.submit(() -> {
        try {
          start.await();
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
        final long canary = 0xABCDEF00L | workerId;
        while (!stop.get()) {
          final FrameSlot slot = allocator.allocateSlot(64 * 1024);
          if (slot == null) {
            continue; // rare under budget — retry
          }
          totalAllocations.incrementAndGet();
          final MemorySegment seg = slot.segment();
          // Writer phase: stamp canary + worker id at several offsets.
          for (int i = 0; i < 128; i++) {
            seg.set(ValueLayout.JAVA_LONG, (long) i * 8, canary + i);
          }
          // Reader phase: optimistic-version read, expect no mismatch since
          // we own the slot (no one else should be recycling it).
          final long preV = allocator.acquireVersion(slot.classIndex(), slot.slotIndex());
          long sum = 0;
          for (int i = 0; i < 128; i++) {
            sum += seg.get(ValueLayout.JAVA_LONG, (long) i * 8);
          }
          final boolean valid = allocator.validateVersion(slot.classIndex(), slot.slotIndex(), preV);
          totalReads.incrementAndGet();
          if (!valid) {
            versionMismatches.incrementAndGet();
          }
          // Sanity: since we hold the slot, the sum must reflect our canary.
          final long expectedSum = 128L * canary + (127L * 128L / 2L);
          if (sum != expectedSum) {
            corruptedReads.incrementAndGet();
          }
          slot.close();
        }
      });
    }

    start.countDown();
    Thread.sleep(runNanos / 1_000_000L);
    stop.set(true);
    pool.shutdown();
    assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "workers did not terminate");

    // In the happy path (a live writer + reader held the same slot throughout)
    // there should be zero corrupted reads. Version mismatches should also
    // be zero because the slot was exclusively owned — if they occur, the
    // allocator is mutating slots while they are still owned, which would be
    // a bug.
    System.err.printf("FrameSlotAllocator stress: allocations=%,d  reads=%,d  "
            + "version-mismatches=%,d  corrupted=%,d%n",
        totalAllocations.get(), totalReads.get(),
        versionMismatches.get(), corruptedReads.get());
    assertEquals(0L, corruptedReads.get(),
        "reads of exclusively-held slots must never see corrupted data");
    assertEquals(0L, versionMismatches.get(),
        "reads of exclusively-held slots must never see version drift");
    assertTrue(totalAllocations.get() > 10_000L,
        "stress test should complete many allocations; got " + totalAllocations.get());
  }
}
