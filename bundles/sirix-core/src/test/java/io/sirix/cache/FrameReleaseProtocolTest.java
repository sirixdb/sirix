/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.cache.RecordingVirtualMemory.Call;
import io.sirix.cache.RecordingVirtualMemory.Kind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-104 — Release protocol and reader safety, the protocol half (the reader half is
 * {@code OptimisticReaderReleaseStressTest}). Over the recording {@link VirtualMemory} fake the
 * call order per batch is claim, then every version odd, then one OS call per contiguous
 * granule-aligned run, then versions even, then refund; each version ends strictly greater than it
 * began; a run never covers an unclaimed (owned) slot.
 *
 * <p>
 * The fake looks at the allocator from inside each release call, which is where the order is
 * visible: the claimed slots are already out of the recycled set, their versions are already odd
 * and their bytes are still charged. A second batch's first call proves the first batch's versions
 * went even and its refund happened before.
 */
@MemPlanned(step = 1, value = {"MEM-104"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class FrameReleaseProtocolTest {

  private static final int CLASS_IDX = 4;
  private static final long SLOT = FrameSlotAllocator.SIZE_CLASSES[CLASS_IDX];

  /** What the allocator looked like from inside one release call. */
  private record Snapshot(Call call, boolean allOdd, long committedAtCall, int recycledAtCall, long bytesBefore,
      List<String> batchViolations) {
  }

  @Test
  @DisplayName("MEM-104 claim, odd versions, one OS call per run, even versions, refund; versions only grow; owned slots untouched")
  void callOrderAndVersionTransitions() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      final FrameSlotAllocator allocator = fixture.allocator;
      final List<FrameSlotAllocator.FrameSlot> all = fixture.fill(CLASS_IDX);
      final Set<Integer> heldIndexes = new HashSet<>();
      final List<FrameSlotAllocator.FrameSlot> free = new ArrayList<>();
      for (final FrameSlotAllocator.FrameSlot slot : all) {
        if (slot.slotIndex() % 5 == 0) {
          heldIndexes.add(slot.slotIndex());
          slot.segment().set(ValueLayout.JAVA_LONG, 0L, 0xABCD0000L + slot.slotIndex());
        } else {
          free.add(slot);
        }
      }
      fixture.releaseAll(free);
      final long[] versionBefore = new long[all.size()];
      for (int slot = 0; slot < versionBefore.length; slot++) {
        versionBefore[slot] = allocator.acquireVersion(CLASS_IDX, slot);
      }
      final long committedBefore = allocator.getPhysicalMemoryBytes();
      final int recycledBefore = allocator.recycledSlotCount(CLASS_IDX);
      final List<Snapshot> snapshots = new ArrayList<>();
      final long[] bytesSoFar = {0L};
      fixture.vm.observeRuns(run -> {
        boolean allOdd = true;
        for (long slot = run.firstSlot(); slot < run.firstSlot() + run.slotCount(); slot++) {
          allOdd &= (allocator.acquireVersion(CLASS_IDX, (int) slot) & 1L) == 1L;
        }
        final List<String> violations = new ArrayList<>();
        final long completedBatchBytes = bytesSoFar[0] / MemPoolSeam.BATCH_BYTES * MemPoolSeam.BATCH_BYTES;
        long covered = 0L;
        for (final Snapshot earlier : snapshots) {
          covered += earlier.call().bytes();
          if (covered > completedBatchBytes) {
            break;
          }
          for (long slot = earlier.call().firstSlot(); slot < earlier.call().firstSlot()
              + earlier.call().slotCount(); slot++) {
            final long now = allocator.acquireVersion(CLASS_IDX, (int) slot);
            if ((now & 1L) != 0L || now <= versionBefore[(int) slot]) {
              violations.add("slot " + slot + " of a finished batch has version " + now);
            }
          }
          if (allocator.getPhysicalMemoryBytes() > committedBefore - completedBatchBytes) {
            violations.add("a finished batch was not refunded before the next batch's OS call");
          }
        }
        snapshots.add(new Snapshot(run, allOdd, allocator.getPhysicalMemoryBytes(),
            allocator.recycledSlotCount(CLASS_IDX), bytesSoFar[0], violations));
        bytesSoFar[0] += run.bytes();
      });

      assertTrue(fixture.reserveNonFrame(5 * MemPoolFixture.MIB), "a 5 MiB reservation spans three batches");

      assertTrue(snapshots.size() > 1, "the transfer issued OS calls");
      long runBytes = 0L;
      long runSlots = 0L;
      final Set<Integer> returnedIndexes = new HashSet<>();
      for (final Snapshot snapshot : snapshots) {
        final Call run = snapshot.call();
        assertEquals(CLASS_IDX, run.classIdx());
        assertTrue(snapshot.allOdd(), "MEM-104: every version of the run is odd before its OS call: " + run);
        assertTrue(snapshot.committedAtCall() >= committedBefore - snapshot.bytesBefore(),
            "MEM-104: the refund follows the OS call, never precedes it: " + run);
        assertTrue(snapshot.recycledAtCall() <= recycledBefore - run.slotCount(),
            "MEM-104: the run's slots are claimed (out of the recycled set) before the OS call: " + run);
        assertEquals(List.of(), snapshot.batchViolations(), "MEM-104: versions go even and the refund lands per batch");
        for (long slot = run.firstSlot(); slot < run.firstSlot() + run.slotCount(); slot++) {
          assertTrue(!heldIndexes.contains((int) slot), "MEM-104: a run never covers an owned slot: " + slot);
          assertTrue(returnedIndexes.add((int) slot), "no slot is released to the OS twice: " + slot);
        }
        runBytes += run.bytes();
        runSlots += run.slotCount();
      }
      assertEquals(snapshots.get(0).call().address(), fixture.vm.callsOf(Kind.RELEASE_RUN).get(0).address());
      assertEquals(committedBefore, snapshots.get(0).committedAtCall(), "nothing is refunded before the first OS call");
      assertEquals(runSlots, MemPoolSeam.returned(allocator, CLASS_IDX), "every released slot is counted returned");
      assertEquals(committedBefore - runBytes, FrameBudgetOracle.chargedFrameBytes(allocator),
          "the refund equals the bytes of the runs, no more");
      for (final int slot : returnedIndexes) {
        final long now = allocator.acquireVersion(CLASS_IDX, slot);
        assertEquals(0L, now & 1L, "MEM-104: the version is even after the transfer");
        assertTrue(now > versionBefore[slot], "MEM-104: the version never decreases and ends strictly greater");
      }
      for (final FrameSlotAllocator.FrameSlot slot : all) {
        if (heldIndexes.contains(slot.slotIndex())) {
          assertEquals(versionBefore[slot.slotIndex()], allocator.acquireVersion(CLASS_IDX, slot.slotIndex()),
              "an owned slot's version is untouched");
          assertEquals(0xABCD0000L + slot.slotIndex(), slot.segment().get(ValueLayout.JAVA_LONG, 0L),
              "an owned slot's bytes are untouched");
        }
      }
      assertEquals(heldIndexes.size(), allocator.liveSlotCount(CLASS_IDX));
      assertTrue(SLOT <= MemPoolSeam.BATCH_BYTES);
      fixture.assertAccounting("the transfer");
    }
  }

  @Test
  @DisplayName("MEM-104 the slot's address stays readable and writable after its memory was returned")
  void returnedSlotsStayReadableAndWritable() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      final List<FrameSlotAllocator.FrameSlot> all = fixture.fill(CLASS_IDX);
      final FrameSlotAllocator.FrameSlot witness = all.get(0);
      fixture.releaseAll(new ArrayList<>(all));
      assertTrue(fixture.reserveNonFrame(28L * MemPoolFixture.MIB), "the whole class is returned");
      assertEquals(448L, MemPoolSeam.returned(fixture.allocator, CLASS_IDX));
      assertTrue(fixture.vm.callsOf(Kind.RELEASE_REGION).isEmpty(), "no region went back before shutdown");
      // A reader holding the old address reads without faulting (zeros or stale bytes) and may write.
      witness.segment().get(ValueLayout.JAVA_LONG, 0L);
      witness.segment().set(ValueLayout.JAVA_LONG, 0L, 42L);
      assertEquals(42L, witness.segment().get(ValueLayout.JAVA_LONG, 0L));
    }
  }
}
