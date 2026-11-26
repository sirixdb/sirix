/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.cache.MemPoolSeam.PressureProbe;
import io.sirix.cache.MemPoolSeam.PressureRequest;
import io.sirix.cache.RecordingVirtualMemory.Kind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-103 (class-specific relief) and MEM-108 — a request for class <em>c</em> that the budget
 * admits but that finds <em>c</em> without a free slot is a <em>slot shortfall</em>: the allocator
 * passes the requesting class, the kind and the amount to the pressure listener, which evicts
 * unguarded frames of that class until one slot is recycled. It never evicts frames of other
 * classes or transfers their budget for slot relief, and the request succeeds without parking. A
 * refused byte reservation is a <em>byte shortfall</em> and is told apart again on every retry.
 *
 * <p>
 * The class is exhausted through a bounded-capacity fake
 * ({@link RecordingVirtualMemory#capClassSlots}): the 64 KiB class has eight slots while the budget
 * has room for thousands. The pressure listener is a miniature cache holding unguarded frames of
 * several classes; it decodes what the allocator tells it ({@link PressureRequest}) and evicts
 * accordingly, so a request that carries the wrong kind or class evicts the wrong frames and the
 * assertions below catch it.
 */
@MemPlanned(step = 1, value = {"MEM-103", "MEM-108"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class ClassSlotReliefTest {

  private static final int SLOT_CLASS = 4;
  private static final long SLOT_SIZE = FrameSlotAllocator.SIZE_CLASSES[SLOT_CLASS];
  private static final int CAPACITY = 8;

  /** Frames cached per class; unguarded, so the listener may evict any of them. */
  private static final class MiniCache {
    final List<List<FrameSlotAllocator.FrameSlot>> byClass = new ArrayList<>();
    final List<PressureRequest> requests = new ArrayList<>();
    int evictedFromOtherClassesOnSlotPressure;

    MiniCache() {
      for (int classIdx = 0; classIdx < FrameSlotAllocator.SIZE_CLASSES.length; classIdx++) {
        byClass.add(new ArrayList<>());
      }
    }

    void evictOne(final int classIdx) {
      final List<FrameSlotAllocator.FrameSlot> frames = byClass.get(classIdx);
      if (!frames.isEmpty()) {
        frames.remove(frames.size() - 1).close();
      }
    }

    /**
     * What a cache does with a request: a slot request frees one frame of that class, a byte request
     * frees bytes.
     */
    void onPressure(final PressureRequest request) {
      requests.add(request);
      if (request.slotShortfall()) {
        evictOne(request.requestingClass());
        return;
      }
      long freed = 0L;
      for (int classIdx = byClassSize() - 1; classIdx >= 0 && freed < request.amount(); classIdx--) {
        while (!byClass.get(classIdx).isEmpty() && freed < request.amount()) {
          evictOne(classIdx);
          freed += FrameSlotAllocator.SIZE_CLASSES[classIdx];
        }
      }
    }

    int byClassSize() {
      return byClass.size();
    }
  }

  private static void cacheFrames(final MemPoolFixture fixture, final MiniCache cache, final int classIdx,
      final int count) {
    for (int i = 0; i < count; i++) {
      final FrameSlotAllocator.FrameSlot slot =
          fixture.allocator.allocateSlot(FrameSlotAllocator.SIZE_CLASSES[classIdx]);
      assertNotNull(slot, "setup: frame " + i + " of class " + classIdx);
      cache.byClass.get(classIdx).add(slot);
    }
  }

  private static MemPoolFixture boundedFixture(final long budget) {
    final RecordingVirtualMemory vm = RecordingVirtualMemory.withOsPageBytes(4096L);
    vm.capClassSlots(SLOT_CLASS, CAPACITY);
    return MemPoolFixture.create(budget, vm);
  }

  @Test
  @DisplayName("MEM-103 a slot shortfall recycles one slot of the requesting class and touches no other class")
  void slotShortfallEvictsOnlyTheRequestingClass() {
    try (MemPoolFixture fixture = boundedFixture(32L * MemPoolFixture.MIB);
        PressureProbe pressure = MemPoolSeam.installPressureProbe()) {
      final MiniCache cache = new MiniCache();
      cacheFrames(fixture, cache, SLOT_CLASS, CAPACITY);
      cacheFrames(fixture, cache, 0, 100);
      cacheFrames(fixture, cache, 2, 50);
      assertNull64KiBExhausted(fixture);
      pressure.onPressure(() -> cache.onPressure(PressureRequest.decode(lastInvocation(pressure))));
      final int mark = fixture.vm.mark();

      final MemorySegment frame = fixture.allocator.allocate(SLOT_SIZE);

      assertNotNull(frame, "MEM-108: the request for the exhausted class succeeds");
      assertEquals(1, pressure.fired(), "one round of relief, no parking and no second round");
      final PressureRequest request = cache.requests.get(0);
      assertTrue(request.slotShortfall(), "MEM-103: the budget admitted the bytes, so this is a slot shortfall");
      assertEquals(SLOT_CLASS, request.requestingClass(), "the listener learns the requesting class");
      assertEquals(1L, request.amount(), "one slot");
      assertEquals(CAPACITY - 1, cache.byClass.get(SLOT_CLASS).size(), "exactly one frame of the class was evicted");
      assertEquals(100, cache.byClass.get(0).size(), "MEM-103: frames of other classes are retained");
      assertEquals(50, cache.byClass.get(2).size());
      assertEquals(100, fixture.allocator.liveSlotCount(0));
      assertEquals(List.of(),
          fixture.vm.callsSince(mark).stream().filter(call -> call.kind() == Kind.RELEASE_RUN).toList(),
          "MEM-103: no budget is transferred for slot relief");
      assertEquals(0L, MemPoolSeam.returnedTotal(fixture.allocator));
      fixture.allocator.release(frame);
      cache.byClass.forEach(frames -> frames.forEach(FrameSlotAllocator.FrameSlot::close));
    }
  }

  @Test
  @DisplayName("MEM-103 slot relief leaves recycled slots of other classes alone")
  void slotShortfallDoesNotTransferBytesFromRecycledSlotsOfOtherClasses() {
    try (MemPoolFixture fixture = boundedFixture(32L * MemPoolFixture.MIB);
        PressureProbe pressure = MemPoolSeam.installPressureProbe()) {
      final MiniCache cache = new MiniCache();
      cacheFrames(fixture, cache, SLOT_CLASS, CAPACITY);
      cacheFrames(fixture, cache, 0, 100);
      final List<FrameSlotAllocator.FrameSlot> recycledNeighbours = new ArrayList<>(cache.byClass.get(0));
      cache.byClass.get(0).clear();
      fixture.releaseAll(recycledNeighbours);
      assertEquals(100, fixture.allocator.recycledSlotCount(0));
      pressure.onPressure(() -> cache.onPressure(PressureRequest.decode(lastInvocation(pressure))));
      final int mark = fixture.vm.mark();

      final MemorySegment frame = fixture.allocator.allocate(SLOT_SIZE);

      assertNotNull(frame);
      assertEquals(100, fixture.allocator.recycledSlotCount(0),
          "MEM-103: the other class's recycled slots stay recycled");
      assertEquals(0L, MemPoolSeam.transfers(fixture.allocator, 0));
      assertEquals(List.of(),
          fixture.vm.callsSince(mark).stream().filter(call -> call.kind() == Kind.RELEASE_RUN).toList());
      fixture.allocator.release(frame);
      cache.byClass.forEach(frames -> frames.forEach(FrameSlotAllocator.FrameSlot::close));
    }
  }

  @Test
  @DisplayName("MEM-103 a byte shortfall is reported as bytes with the requesting class, and transfers budget once frames are evicted")
  void byteShortfallIsToldApartFromSlotShortfall() {
    try (MemPoolFixture fixture = MemPoolFixture.create(32L * MemPoolFixture.MIB, 4096L);
        PressureProbe pressure = MemPoolSeam.installPressureProbe()) {
      final MiniCache cache = new MiniCache();
      // The 256 KiB class holds all of L_f (28 MiB = 112 frames): the next 64 KiB frame has a byte
      // shortfall.
      cacheFrames(fixture, cache, 6, 112);
      assertEquals(fixture.frameLimit(), FrameBudgetOracle.chargedFrameBytes(fixture.allocator));
      pressure.onPressure(() -> cache.onPressure(PressureRequest.decode(lastInvocation(pressure))));

      final MemorySegment frame = fixture.allocator.allocate(SLOT_SIZE);

      assertNotNull(frame,
          "MEM-108: the byte shortfall is covered by evicting unguarded frames and moving their budget");
      assertFalse(cache.requests.isEmpty());
      final PressureRequest first = cache.requests.get(0);
      assertFalse(first.slotShortfall(), "MEM-103: the budget refused the bytes, so this is a byte shortfall");
      assertEquals(SLOT_CLASS, first.requestingClass(), "the listener learns the requesting class");
      assertEquals(SLOT_SIZE, first.amount(), "the byte shortfall: charged - (L - s) with the class full is s");
      assertTrue(MemPoolSeam.returned(fixture.allocator, 6) > 0L,
          "the evicted 256 KiB frames' budget moved across classes");
      fixture.allocator.release(frame);
      cache.byClass.forEach(frames -> frames.forEach(FrameSlotAllocator.FrameSlot::close));
    }
  }

  private static void assertNull64KiBExhausted(final MemPoolFixture fixture) {
    assertEquals(null, fixture.allocator.allocateSlot(SLOT_SIZE),
        "setup: the 64 KiB class has no free slot although the budget has room");
  }

  private static Object[] lastInvocation(final PressureProbe pressure) {
    final List<Object[]> all = pressure.invocations();
    return all.get(all.size() - 1);
  }
}
