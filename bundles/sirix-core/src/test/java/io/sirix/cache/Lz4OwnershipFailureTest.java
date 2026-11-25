package io.sirix.cache;

import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.FFILz4Compressor;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Ownership of the scoped LZ4 decompression path when a read fails: the output frame this call
 * allocated and the scratch buffer a heap input is staged in must both go back exactly once, on
 * every failure path (#1407).
 *
 * <p>
 * Isolated: the assertions read the process allocator's own counters.
 */
@Isolated
final class Lz4OwnershipFailureTest {
  /** Failures injected per leak check: one call cannot show accumulation, a run of them can. */
  private static final int INJECTED_FAILURES = 64;

  @Test
  void failedInputReturnDoesNotPublishOrAbandonTheOutputFrame() {
    assumeTrue(FFILz4Compressor.isNativeAvailable());
    final FrameSlotAllocator allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    final Runnable[] afterDecode = {null};
    final FFILz4Compressor codec = spy(new FFILz4Compressor());
    final Answer<Integer> completed = invocation -> {
      final int produced = (int) invocation.callRealMethod();
      final Runnable action = afterDecode[0];
      if (action != null) {
        action.run();
      }
      return produced;
    };
    doAnswer(completed).when(codec).decompressSegment(any(MemorySegment.class), any(MemorySegment.class), anyInt());
    doAnswer(completed).when(codec).decompressSegmentFast(any(MemorySegment.class), any(MemorySegment.class), anyInt());
    final byte[] raw = new byte[50000];
    Arrays.fill(raw, (byte) 7);
    final byte[] encoded = codec.compress(MemorySegment.ofArray(raw)).toArray(ValueLayout.JAVA_BYTE);
    try (ByteHandler.DecompressionResult warm = codec.decompressScoped(MemorySegment.ofArray(encoded))) {
      assertArrayEquals(raw, warm.segment().toArray(ValueLayout.JAVA_BYTE));
    }
    final ScratchStats initial = allocator.scratchStats();
    final int capacity = initial.capacityIndex(encoded.length);
    final int buffers = Math.max(257, Math.toIntExact(initial.idleBuffers()[capacity]) + 1);
    final ScratchLease[] fillers = new ScratchLease[buffers];
    try {
      for (int i = 0; i < buffers; i++) {
        fillers[i] = ScratchLease.of(encoded.length, ScratchUse.CODEC);
      }
      for (final ScratchLease filler : fillers) {
        filler.close();
      }
      final long committed = allocator.getPhysicalMemoryBytes();
      final long active = allocator.getActiveMemoryBytes();
      final long lent = allocator.scratchStats().lentBytes();
      for (final ScratchLease filler : fillers) {
        filler.acquire(encoded.length, ScratchUse.CODEC);
      }
      final OutOfMemoryError failure = new OutOfMemoryError("injected input return failure");
      afterDecode[0] = () -> {
        for (final ScratchLease filler : fillers) {
          filler.close();
        }
        allocator.scratch().setSparePublicationHookForTesting(() -> {
          throw failure;
        });
      };
      final MemorySegment encodedSegment = MemorySegment.ofArray(encoded);
      assertSame(failure, assertThrows(OutOfMemoryError.class, () -> codec.decompressScoped(encodedSegment)));
      afterDecode[0] = null;
      allocator.scratch().setSparePublicationHookForTesting(null);
      assertEquals(committed, allocator.getPhysicalMemoryBytes());
      assertEquals(active, allocator.getActiveMemoryBytes());
      assertEquals(lent, allocator.scratchStats().lentBytes());
      try (ByteHandler.DecompressionResult following = codec.decompressScoped(MemorySegment.ofArray(encoded))) {
        assertArrayEquals(raw, following.segment().toArray(ValueLayout.JAVA_BYTE));
      }
      assertEquals(lent, allocator.scratchStats().lentBytes());
    } finally {
      allocator.scratch().setSparePublicationHookForTesting(null);
      for (final ScratchLease filler : fillers) {
        if (filler != null) {
          filler.close();
        }
      }
    }
  }

  @Test
  @DisplayName("a decode failure after heap staging returns both the staging buffer and the output frame once")
  void decodeFailureAfterHeapStagingReturnsTheStagingBufferAndTheOutputFrame() {
    assumeTrue(FFILz4Compressor.isNativeAvailable());
    final FrameSlotAllocator allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    final FFILz4Compressor codec = spy(new FFILz4Compressor());
    final byte[] raw = compressiblePayload();
    final MemorySegment heapEncoded = MemorySegment.ofArray(encode(codec, raw));
    assertFalse(heapEncoded.isNative(), "the staged path needs a heap input");
    // Warm: the counters below must start from a steady state, not from this capacity's first borrow.
    assertRoundTrip(codec, heapEncoded, raw);

    final int frameClass = FrameSlotAllocator.indexForSize(raw.length);
    final Ownership before = Ownership.of(allocator, frameClass);
    final MemorySegment[] decoderSource = new MemorySegment[1];
    final IllegalStateException injected = failEveryDecode(codec, decoderSource);
    repeatUntilInjectedFailures(codec, heapEncoded, injected);

    // The decoder's source is the staged native copy, not the caller's heap segment: proof that this
    // run reached the staging path the leak lived on.
    assertNotNull(decoderSource[0], "the decoder must have been reached");
    assertTrue(decoderSource[0].isNative(), "the heap input must be staged into native scratch before the decode");
    assertEverythingWentBack(allocator, frameClass, before, INJECTED_FAILURES);

    restoreDecode(codec);
    assertRoundTrip(codec, heapEncoded, raw);
    assertEquals(before.live(), allocator.liveSlotCount(frameClass), "the following read must own no frame either");
  }

  @Test
  @DisplayName("a decode failure with a native input returns the output frame once and borrows no staging buffer")
  void decodeFailureWithoutHeapStagingReturnsTheOutputFrame() {
    assumeTrue(FFILz4Compressor.isNativeAvailable());
    final FrameSlotAllocator allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    final FFILz4Compressor codec = spy(new FFILz4Compressor());
    final byte[] raw = compressiblePayload();
    final byte[] encoded = encode(codec, raw);
    final MemorySegment nativeEncoded = Arena.ofAuto().allocate(encoded.length);
    MemorySegment.copy(MemorySegment.ofArray(encoded), 0, nativeEncoded, 0, encoded.length);
    assertTrue(nativeEncoded.isNative(), "this check covers the window before any staging buffer exists");
    assertRoundTrip(codec, nativeEncoded, raw);

    final int frameClass = FrameSlotAllocator.indexForSize(raw.length);
    final Ownership before = Ownership.of(allocator, frameClass);
    final MemorySegment[] decoderSource = new MemorySegment[1];
    final IllegalStateException injected = failEveryDecode(codec, decoderSource);
    repeatUntilInjectedFailures(codec, nativeEncoded, injected);

    assertNotNull(decoderSource[0], "the decoder must have been reached");
    assertEquals(nativeEncoded.address() + Integer.BYTES, decoderSource[0].address(),
        "a native input is decoded in place, from just past its own size header: no staging copy");
    assertEverythingWentBack(allocator, frameClass, before, 0L);
  }

  /** Payload of one output frame; identical bytes so LZ4 never falls back to a stored frame. */
  private static byte[] compressiblePayload() {
    final byte[] raw = new byte[50000];
    Arrays.fill(raw, (byte) 7);
    return raw;
  }

  /** The compressed frame of {@code raw}, as the codec itself writes it. */
  private static byte[] encode(final FFILz4Compressor codec, final byte[] raw) {
    return codec.compress(MemorySegment.ofArray(raw)).toArray(ValueLayout.JAVA_BYTE);
  }

  private static void assertRoundTrip(final FFILz4Compressor codec, final MemorySegment encoded, final byte[] raw) {
    try (ByteHandler.DecompressionResult decoded = codec.decompressScoped(encoded)) {
      assertArrayEquals(raw, decoded.segment().toArray(ValueLayout.JAVA_BYTE));
    }
  }

  /**
   * Make both decoder entry points throw, recording the source segment they were handed.
   *
   * @return the failure they throw, for an identity assertion on what escapes
   */
  private static IllegalStateException failEveryDecode(final FFILz4Compressor codec,
      final MemorySegment[] decoderSource) {
    final IllegalStateException injected = new IllegalStateException("injected decode failure");
    final Answer<Integer> fail = invocation -> {
      decoderSource[0] = invocation.getArgument(0, MemorySegment.class);
      throw injected;
    };
    doAnswer(fail).when(codec).decompressSegment(any(MemorySegment.class), any(MemorySegment.class), anyInt());
    doAnswer(fail).when(codec).decompressSegmentFast(any(MemorySegment.class), any(MemorySegment.class), anyInt());
    return injected;
  }

  private static void restoreDecode(final FFILz4Compressor codec) {
    doCallRealMethod().when(codec).decompressSegment(any(MemorySegment.class), any(MemorySegment.class), anyInt());
    doCallRealMethod().when(codec).decompressSegmentFast(any(MemorySegment.class), any(MemorySegment.class), anyInt());
  }

  /** One failure cannot show accumulation; {@link #INJECTED_FAILURES} consecutive ones can. */
  private static void repeatUntilInjectedFailures(final FFILz4Compressor codec, final MemorySegment encoded,
      final IllegalStateException injected) {
    for (int attempt = 0; attempt < INJECTED_FAILURES; attempt++) {
      assertSame(injected, assertThrows(IllegalStateException.class, () -> codec.decompressScoped(encoded)));
    }
  }

  /**
   * Every failed call allocated one output frame and returned it, and borrowed
   * {@code expectedStagingBorrows} staging buffers and returned those: exactly once each, with
   * nothing else moved.
   */
  private static void assertEverythingWentBack(final FrameSlotAllocator allocator, final int frameClass,
      final Ownership before, final long expectedStagingBorrows) {
    final ScratchStats after = allocator.scratchStats();
    assertEquals(expectedStagingBorrows,
        after.acquisitions(ScratchUse.CODEC) - before.scratch().acquisitions(ScratchUse.CODEC),
        "staging borrows of the failed calls");
    assertEquals(before.scratch().totalLentBuffers(), after.totalLentBuffers(), "no staging buffer may stay lent");
    assertEquals(before.scratch().staleReleases(), after.staleReleases(), "no buffer may be returned twice");
    assertEquals(INJECTED_FAILURES, allocator.allocateCount(frameClass) - before.allocations(),
        "every call must allocate exactly one output frame");
    assertEquals(INJECTED_FAILURES, allocator.releaseCount(frameClass) - before.releases(),
        "every failed decode must return its output frame exactly once");
    assertEquals(before.live(), allocator.liveSlotCount(frameClass), "no output frame may stay live");
    assertEquals(before.committed(), allocator.getPhysicalMemoryBytes(), "committed bytes must not move");
    assertEquals(before.active(), allocator.getActiveMemoryBytes(), "active bytes must not move");
  }

  /** The allocator state a run of failed reads must leave exactly where it found it. */
  private record Ownership(ScratchStats scratch, long allocations, long releases, int live, long committed,
      long active) {
    static Ownership of(final FrameSlotAllocator allocator, final int frameClass) {
      return new Ownership(allocator.scratchStats(), allocator.allocateCount(frameClass),
          allocator.releaseCount(frameClass), allocator.liveSlotCount(frameClass), allocator.getPhysicalMemoryBytes(),
          allocator.getActiveMemoryBytes());
    }
  }
}
