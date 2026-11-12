package io.sirix.cache;

import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.FFILz4Compressor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Isolated
final class Lz4OwnershipFailureTest {
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
      assertSame(failure,
          assertThrows(OutOfMemoryError.class, () -> codec.decompressScoped(MemorySegment.ofArray(encoded))));
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
}
