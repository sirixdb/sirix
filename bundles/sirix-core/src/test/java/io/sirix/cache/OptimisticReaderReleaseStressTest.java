/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-104 reader safety under the worst case: optimistic readers spin
 * {@code acquireVersion}/read/{@code validateVersion} on slots a writer releases, re-charges and
 * reissues, on real memory, with the test-only switch that returns every slot to the OS on every
 * release. No fault, and no <em>validated</em> read observes bytes other than the owner's.
 *
 * <p>
 * Protocol of the fixture. A slot's owner writes the words {@code 1..N-1} of its slot, then
 * publishes word 0 last with release semantics: {@code mix(versionAtAlloc)}, a non-zero value that
 * differs for every version. A reader that snapshotted an even version {@code v} and finds word 0
 * equal to {@code mix(v)} must find every word equal to {@code mix(v)} if its validation succeeds.
 * A mismatch of word 0 means the owner has not published yet, or the bytes are stale or zero (a
 * released slot reads zeros or its old contents, MEM-104): the reader discards the read.
 */
@ResourceLock("FrameSlotAllocator.pressureListener")
@MemPlanned(step = 1, value = {"MEM-104"})
final class OptimisticReaderReleaseStressTest {

  private static final int SLOTS = 8;
  private static final int STABLE_SLOTS = SLOTS / 2;
  private static final int READERS = 3;
  private static final int CHURN_ROUNDS = 20_000;
  private static final long SEED = 0x5EED_104L;
  private static final VarHandle WORD = ValueLayout.JAVA_LONG.varHandle();

  @Test
  @DisplayName("MEM-104: readers never validate bytes other than the owner's while slots are released and reissued")
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void validatedReadsOnlyObserveTheOwnersBytes() throws InterruptedException {
    MemPoolSeam.releaseEverythingOnRelease(true);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(32L * 1024 * 1024);
    try {
      final long pageBytes = MemPlatformSeam.osPageBytes(VirtualMemory.forCurrentPlatform());
      int classIdx = 0;
      while (classIdx < FrameSlotAllocator.SIZE_CLASSES.length
          && FrameSlotAllocator.SIZE_CLASSES[classIdx] < pageBytes) {
        classIdx++;
      }
      assertTrue(classIdx < FrameSlotAllocator.SIZE_CLASSES.length, "a slot class must cover an OS page");
      run(allocator, classIdx);
    } finally {
      MemPoolSeam.releaseEverythingOnRelease(false);
      allocator.shutdown();
    }
  }

  private static void run(final FrameSlotAllocator allocator, final int classIdx) throws InterruptedException {
    final long slotBytes = FrameSlotAllocator.SIZE_CLASSES[classIdx];
    final long returnedBefore = MemPoolSeam.slotsReturned(allocator, classIdx);
    final FrameSlotAllocator.FrameSlot[] owned = new FrameSlotAllocator.FrameSlot[SLOTS];
    final AtomicReferenceArray<MemorySegment> segments = new AtomicReferenceArray<>(SLOTS);
    for (int index = 0; index < SLOTS; index++) {
      owned[index] = allocator.allocateSlot(slotBytes);
      assertEquals(index, owned[index].slotIndex(), "the first slots of a class are minted in order");
      segments.set(index, owned[index].segment());
      publish(owned[index]);
    }

    final AtomicBoolean done = new AtomicBoolean();
    final AtomicLong validatedStable = new AtomicLong();
    final AtomicLong validatedChurn = new AtomicLong();
    final AtomicLong discarded = new AtomicLong();
    final AtomicInteger violations = new AtomicInteger();
    final AtomicInteger rechargedOwner = new AtomicInteger(-1);
    final CountDownLatch rechargedValidated = new CountDownLatch(1);
    final List<Throwable> failures = new ArrayList<>();
    final List<Thread> threads = new ArrayList<>();
    for (int reader = 0; reader < READERS; reader++) {
      final SplittableRandom random = new SplittableRandom(SEED + reader);
      final Thread thread = new Thread(() -> {
        try {
          while (!done.get()) {
            final int checkpoint = rechargedOwner.get();
            final int index = checkpoint >= 0 ? checkpoint : random.nextInt(SLOTS);
            final long result = readOnce(allocator, segments.get(index), classIdx, index);
            if (result == 0L) {
              discarded.incrementAndGet();
            } else if (result == 1L) {
              (index < STABLE_SLOTS
                  ? validatedStable
                  : validatedChurn).incrementAndGet();
              if (checkpoint >= 0) {
                rechargedValidated.countDown();
              }
            } else {
              violations.incrementAndGet();
            }
          }
        } catch (final Throwable failure) {
          synchronized (failures) {
            failures.add(failure);
          }
        }
      }, "mem104-reader-" + reader);
      thread.setDaemon(true);
      threads.add(thread);
      thread.start();
    }

    // The writer churns only the upper slots: release, then reissue and publish. The lower slots stay
    // owned so that readers always have validated reads to check.
    final SplittableRandom writer = new SplittableRandom(SEED);
    try {
      for (int round = 0; round < CHURN_ROUNDS; round++) {
        final int index = STABLE_SLOTS + writer.nextInt(SLOTS - STABLE_SLOTS);
        owned[index].close();
        final int heldSpins = 1 + writer.nextInt(64);
        for (int spin = 0; spin < heldSpins; spin++) {
          Thread.onSpinWait();
        }
        final FrameSlotAllocator.FrameSlot reissued = allocator.allocateSlot(slotBytes);
        assertTrue(reissued.slotIndex() < SLOTS, "a reissued slot is one of the " + SLOTS + " minted slots, was "
            + reissued.slotIndex() + " (MEM-105: a returned slot is re-charged before a fresh index is minted)");
        owned[reissued.slotIndex()] = reissued;
        publish(reissued);
        if (round == 0) {
          assertEquals(1L, readOnce(allocator, reissued.segment(), classIdx, reissued.slotIndex()),
              "a known published recharged owner validates and contains its published bytes");
          rechargedOwner.set(reissued.slotIndex());
          assertTrue(rechargedValidated.await(10, TimeUnit.SECONDS),
              "a reader validates the published recharged owner before the writer continues");
          rechargedOwner.set(-1);
        }
        final int ownedSpins = writer.nextInt(64);
        for (int spin = 0; spin < ownedSpins; spin++) {
          Thread.onSpinWait();
        }
      }
      assertTrue(MemPoolSeam.slotsReturned(allocator, classIdx) > returnedBefore,
          "the worst-case churn actually returned slots to the OS");
    } finally {
      done.set(true);
      for (final Thread thread : threads) {
        thread.join(30_000L);
      }
      for (final FrameSlotAllocator.FrameSlot slot : owned) {
        if (slot != null) {
          slot.close();
        }
      }
    }
    synchronized (failures) {
      assertTrue(failures.isEmpty(), "a reader failed: " + failures);
    }
    assertTrue(validatedChurn.get() > 0L, "readers validated reads of recharged owners");
    assertEquals(0, violations.get(), "a validated read observed bytes other than its owner's");
    assertTrue(validatedStable.get() > 0L,
        "readers validated reads of the stable owners (" + discarded.get() + " reads discarded)");
  }

  /** The owner writes words 1..N-1 and then publishes word 0 with release semantics. */
  private static void publish(final FrameSlotAllocator.FrameSlot slot) {
    final long pattern = mix(slot.versionAtAlloc());
    final MemorySegment segment = slot.segment();
    for (int word = 1; word < segment.byteSize() / Long.BYTES; word++) {
      segment.set(ValueLayout.JAVA_LONG, (long) word * Long.BYTES, pattern);
    }
    WORD.setRelease(segment, 0L, pattern);
  }

  /**
   * One optimistic read.
   *
   * @return {@code 0} discarded (odd or changed version, unpublished or foreign word 0, failed
   *         validation), {@code 1} validated and every word is the owner's, {@code 2} validated but a
   *         word is not the owner's
   */
  private static long readOnce(final FrameSlotAllocator allocator, final MemorySegment segment,
      final int classIdx, final int index) {
    final long version = allocator.acquireVersion(classIdx, index);
    if ((version & 1L) != 0L) {
      return 0L;
    }
    final long expected = mix(version);
    if ((long) WORD.getAcquire(segment, 0L) != expected) {
      return 0L;
    }
    boolean foreign = false;
    for (int word = 1; word < segment.byteSize() / Long.BYTES; word++) {
      if (segment.get(ValueLayout.JAVA_LONG, (long) word * Long.BYTES) != expected) {
        foreign = true;
      }
    }
    VarHandle.acquireFence();
    if (!allocator.validateVersion(classIdx, index, version)) {
      return 0L;
    }
    return foreign
        ? 2L
        : 1L;
  }

  /** A non-zero value that differs for different versions. */
  private static long mix(final long version) {
    final long mixed = (version + 1L) * 0x9E37_79B9_7F4A_7C15L;
    return mixed == 0L
        ? 1L
        : mixed;
  }
}
