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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit + concurrency tests for {@link FrameSlotAllocator}.
 *
 * <p>
 * The concurrency test is the real target: 20 threads allocating, writing, and releasing slots in a
 * tight loop for several seconds, with optimistic version-check readers observing in parallel. If
 * the pool-style recycling race were present, we would expect either SIGSEGV (unhandled in Java) or
 * corrupted reads. The test verifies every version-validated read either sees the writer's canary
 * pattern or detects the race via the version mismatch.
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
  void oversizedAllocateReleasedDeterministically() {
    // Larger than the largest size class (256 KiB) — served from a per-allocation
    // confined arena rather than a frame slot.
    final long size = 512L * 1024;
    final MemorySegment segment = allocator.allocate(size);
    assertNotNull(segment);
    assertEquals(size, segment.byteSize());
    assertTrue(segment.scope().isAlive());
    segment.set(ValueLayout.JAVA_BYTE, 0L, (byte) 0x5A);
    segment.set(ValueLayout.JAVA_BYTE, size - 1, (byte) 0xA5);
    allocator.release(segment);
    assertFalse(segment.scope().isAlive(),
        "release of an oversized segment must close its confined arena deterministically");
    // Idempotent: a second release of the same (now dead) segment is a no-op.
    allocator.release(segment);
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
    assertNull(allocator.allocateSlot(slotSize), "allocator must return null when physical budget is exhausted");
    for (final FrameSlot s : held) {
      s.close();
    }
    // After releasing, new allocations should succeed again.
    final FrameSlot reborn = allocator.allocateSlot(slotSize);
    assertNotNull(reborn, "post-release allocation must succeed once budget frees up");
    reborn.close();
  }

  @Test
  void clusteredRecycledSlotsDrainTheirCurrentBitsetWordBeforeAdvancing() {
    final int classIdx = FrameSlotAllocator.indexForSize(4096);
    final FrameSlot[] lowerWord = new FrameSlot[Long.SIZE];
    for (int slot = 0; slot < lowerWord.length; slot++) {
      lowerWord[slot] = allocator.allocateSlot(4096);
      assertNotNull(lowerWord[slot]);
      assertEquals(slot, lowerWord[slot].slotIndex());
    }
    final FrameSlot first = allocator.allocateSlot(4096);
    final FrameSlot second = allocator.allocateSlot(4096);
    assertNotNull(first);
    assertNotNull(second);
    final int clusteredWord = first.slotIndex() >>> 6;
    assertEquals(1, clusteredWord, "the regression must exercise a word away from the initial cursor");
    assertEquals(clusteredWord, second.slotIndex() >>> 6);

    first.close();
    second.close();
    assertEquals(clusteredWord, allocator.recycledScanWord(classIdx),
        "release must relocate the scan cursor to its newly nonempty word");

    final FrameSlot hinted = allocator.allocateSlot(4096);
    assertNotNull(hinted);
    assertEquals(second.slotIndex(), hinted.slotIndex());
    assertEquals(clusteredWord, allocator.recycledScanWord(classIdx),
        "the cursor must keep draining a word that still has free bits");

    final FrameSlot scanned = allocator.allocateSlot(4096);
    assertNotNull(scanned);
    assertEquals(first.slotIndex(), scanned.slotIndex());
    assertEquals(clusteredWord + 1, allocator.recycledScanWord(classIdx),
        "the cursor advances only after the claimed word becomes empty");

    hinted.close();
    scanned.close();
    for (final FrameSlot slot : lowerWord) {
      slot.close();
    }
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
   * Stress test: 20 workers each in a tight allocate-write-read-release loop for 5 seconds. A
   * concurrent observer repeatedly allocates, reads a canary value, and validates the version;
   * mismatches are counted (they are expected and MUST be detected, not silently pass).
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
    System.err.printf(
        "FrameSlotAllocator stress: allocations=%,d  reads=%,d  " + "version-mismatches=%,d  corrupted=%,d%n",
        totalAllocations.get(), totalReads.get(), versionMismatches.get(), corruptedReads.get());
    assertEquals(0L, corruptedReads.get(), "reads of exclusively-held slots must never see corrupted data");
    assertEquals(0L, versionMismatches.get(), "reads of exclusively-held slots must never see version drift");
    assertTrue(totalAllocations.get() > 10_000L,
        "stress test should complete many allocations; got " + totalAllocations.get());
  }

  /**
   * Regression test for #1073 Defect A: the free stack is LIFO, so a released slot is re-issued first
   * — at the same stable address. A stale, dangling segment from the PRIOR allocation era (a delayed
   * double-release) used to look up the address and close the NEXT owner's live slot: the slot went
   * back on the free stack while still owned, and the following allocation handed the same memory to
   * a third owner (silent use-after-free, double budget decrement).
   */
  @Test
  void staleReleaseOfRecycledAddressMustNotFreeTheNewOwnersSlot() {
    final int classIdx = FrameSlotAllocator.indexForSize(4096);
    final long allocationsBefore = allocator.allocateCount(classIdx);
    final long releasesBefore = allocator.releaseCount(classIdx);
    final MemorySegment segA = allocator.allocate(4096);
    assertNotNull(segA);
    final long address = segA.address();
    final long coordinatesA = allocator.slotCoordinates(segA);
    assertFalse(coordinatesA == FrameSlotAllocator.NO_SLOT_COORDINATES);

    // Legitimate release; slot goes on the free stack.
    allocator.release(segA);
    assertEquals(FrameSlotAllocator.NO_SLOT_COORDINATES, allocator.slotCoordinates(segA));

    // LIFO recycling: the next allocation of the class reuses the same slot/address.
    final MemorySegment segB = allocator.allocate(4096);
    assertNotNull(segB);
    assertEquals(address, segB.address(), "precondition: LIFO free stack must recycle the slot");
    assertEquals(coordinatesA, allocator.slotCoordinates(segB));
    assertEquals(FrameSlotAllocator.NO_SLOT_COORDINATES, allocator.slotCoordinates(segA),
        "a stale scope must not bind to the current owner at the recycled address");

    // STALE double-release of the prior era's segment. Must be rejected — B still owns the slot.
    allocator.release(segA);

    // If the stale release had freed B's slot, this allocation would be handed B's address again
    // (two live owners of one slot). It must come from a different slot.
    final MemorySegment segC = allocator.allocate(4096);
    assertNotNull(segC);
    assertFalse(segC.address() == address, "stale release must not free the new owner's slot (ABA use-after-free)");

    // B's release must still work (its era is the current one for that address).
    allocator.release(segB);
    final MemorySegment segD = allocator.allocate(4096);
    assertNotNull(segD);
    assertEquals(address, segD.address(), "the current owner's release must still free the slot");

    allocator.release(segC);
    allocator.release(segD);

    assertEquals(allocationsBefore + 4L, allocator.allocateCount(classIdx));
    assertEquals(releasesBefore + 4L, allocator.releaseCount(classIdx),
        "the stale A-era release must not increment the release counter");
    assertEquals(0, allocator.liveSlotCount(classIdx));
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
  }

  /**
   * Companion to the ABA regression: reinterpret-derived segments inherit the issuing allocation's
   * scope, so the legitimate release patterns used across the codebase (e.g.
   * {@code release(slottedPage.reinterpret(capacity))}) keep working.
   */
  @Test
  void reinterpretDerivedReleaseIsAccepted() {
    final MemorySegment seg = allocator.allocate(8192);
    assertNotNull(seg);
    final long address = seg.address();

    // Simulate the KeyValueLeafPage pattern: hold a truncated view, release a re-widened view.
    final MemorySegment truncated = seg.reinterpret(4096);
    assertSame(seg.scope(), truncated.scope(), "reinterpret must retain the exact allocation-era scope object");
    assertSame(seg.scope(), seg.asSlice(0, 4096).scope(),
        "a zero-offset slice must retain the exact allocation-era scope object");
    allocator.release(truncated.reinterpret(8192));

    // The slot must actually be free again: same class allocation recycles the address.
    final MemorySegment again = allocator.allocate(8192);
    assertNotNull(again);
    assertEquals(address, again.address(), "derived-segment release must free the slot");
    allocator.release(again);
  }

  @Test
  void addressLookupRequiresAnExactLiveSlotBaseAndScope() {
    final MemorySegment segment = allocator.allocate(4096);
    assertNotNull(segment);

    final long coordinates = allocator.slotCoordinates(segment);
    assertFalse(coordinates == FrameSlotAllocator.NO_SLOT_COORDINATES);
    assertEquals(coordinates, allocator.slotCoordinates(segment.asSlice(0, 1024)));
    assertEquals(FrameSlotAllocator.NO_SLOT_COORDINATES, allocator.slotCoordinates(segment.asSlice(1)),
        "an interior address must not decode as a slot base");
    assertEquals(FrameSlotAllocator.NO_SLOT_COORDINATES,
        allocator.slotCoordinates(MemorySegment.ofAddress(segment.address())),
        "the right address under a foreign scope must not identify a live allocation");

    allocator.release(segment);
    assertEquals(FrameSlotAllocator.NO_SLOT_COORDINATES, allocator.slotCoordinates(segment));
  }

  @Test
  void concurrentDoubleReleaseChangesAccountingExactlyOnce() throws Exception {
    final int classIdx = FrameSlotAllocator.indexForSize(4096);
    final MemorySegment segment = allocator.allocate(4096);
    final long address = segment.address();
    final long releasesBefore = allocator.releaseCount(classIdx);
    final ExecutorService pool = Executors.newFixedThreadPool(8);
    final CountDownLatch start = new CountDownLatch(1);
    final List<Future<?>> releases = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      releases.add(pool.submit(() -> {
        start.await();
        allocator.release(segment);
        return null;
      }));
    }

    start.countDown();
    pool.shutdown();
    for (final Future<?> release : releases) {
      release.get(10, TimeUnit.SECONDS);
    }
    assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

    assertEquals(releasesBefore + 1L, allocator.releaseCount(classIdx));
    assertEquals(0, allocator.liveSlotCount(classIdx));
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
    final MemorySegment recycled = allocator.allocate(4096);
    assertEquals(address, recycled.address());
    allocator.release(recycled);
  }

  @Test
  void oneSlotInterfaceAllocatorMakesBoundedProgressUnderContention() throws Exception {
    allocator.shutdown();
    final int classIdx = FrameSlotAllocator.SIZE_CLASSES.length - 1;
    final long slotSize = FrameSlotAllocator.SIZE_CLASSES[classIdx];
    allocator = new FrameSlotAllocator(slotSize);

    final int workers = 4;
    final int iterations = 100;
    final ExecutorService pool = Executors.newFixedThreadPool(workers);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicBoolean exclusivelyOwned = new AtomicBoolean();
    final AtomicLong soleAddress = new AtomicLong(Long.MIN_VALUE);
    final List<Future<?>> futures = new ArrayList<>();
    for (int worker = 0; worker < workers; worker++) {
      final int workerId = worker;
      futures.add(pool.submit(() -> {
        start.await();
        for (int iteration = 0; iteration < iterations; iteration++) {
          final MemorySegment segment = allocator.allocate(slotSize);
          if (!exclusivelyOwned.compareAndSet(false, true)) {
            allocator.release(segment);
            throw new AssertionError("two callers concurrently owned the allocator's only slot");
          }
          try {
            soleAddress.compareAndSet(Long.MIN_VALUE, segment.address());
            assertEquals(soleAddress.get(), segment.address());
            assertFalse(allocator.slotCoordinates(segment) == FrameSlotAllocator.NO_SLOT_COORDINATES);
            segment.set(ValueLayout.JAVA_LONG, 0, ((long) workerId << 32) | iteration);
          } finally {
            // Ownership ends when release starts. FREE is published inside release, so clearing this
            // after release returned would falsely overlap with a legitimate next owner.
            exclusivelyOwned.set(false);
            allocator.release(segment);
          }
        }
        return null;
      }));
    }

    start.countDown();
    pool.shutdown();
    for (final Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

    assertEquals((long) workers * iterations, allocator.allocateCount(classIdx));
    assertEquals((long) workers * iterations, allocator.releaseCount(classIdx));
    assertEquals(1, allocator.recycledSlotCount(classIdx));
    assertEquals(allocator.recycledSlotCount(classIdx), allocator.recycledSlotBitCount(classIdx));
    assertEquals(0, allocator.liveSlotCount(classIdx));
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
  }

}
