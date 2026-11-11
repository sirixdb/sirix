package io.sirix.io.bytepipe;

import io.sirix.io.bytepipe.ByteHandler.DecompressionResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class ByteHandlerPipelineOwnershipTest {
  @Test
  void singleHandlerReturnsItsOwnedResultDirectly() {
    final MemorySegment input = MemorySegment.ofArray(new byte[] {1});
    final AtomicInteger releases = new AtomicInteger();
    final ByteHandler handler = mock(ByteHandler.class);
    when(handler.supportsMemorySegments()).thenReturn(true);
    final DecompressionResult owned = new DecompressionResult(input, releases::incrementAndGet);
    when(handler.decompressScoped(input)).thenReturn(owned);

    try (final DecompressionResult result = new ByteHandlerPipeline(handler).decompressScoped(input)) {
      assertSame(owned, result);
      assertEquals(0, releases.get());
    }
    assertEquals(1, releases.get());
  }

  @Test
  void chainedHandlersReleaseIntermediatesAndKeepTheFinalBuffer() {
    final MemorySegment[] segments = segments();
    final ByteHandler[] handlers = new ByteHandler[3];
    final int[] releases = new int[3];
    for (int i = 0; i < handlers.length; i++) {
      final int index = i;
      handlers[i] = mock(ByteHandler.class);
      when(handlers[i].supportsMemorySegments()).thenReturn(true);
      when(handlers[i].decompressScoped(segments[i])).thenReturn(
          new DecompressionResult(segments[i + 1], () -> releases[index]++));
    }

    try (final DecompressionResult result = new ByteHandlerPipeline(handlers).decompressScoped(segments[0])) {
      assertSame(segments[3], result.segment());
      assertSame(segments[3], result.backingBuffer());
      assertEquals(1, releases[0]);
      assertEquals(1, releases[1]);
      assertEquals(0, releases[2]);
    }
    assertEquals(1, releases[0]);
    assertEquals(1, releases[1]);
    assertEquals(1, releases[2]);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void handlerFailureReleasesEveryPreviouslyProducedBuffer(final int failedHandler) {
    final MemorySegment[] segments = segments();
    final ByteHandler[] handlers = new ByteHandler[3];
    final int[] releases = new int[3];
    final IllegalStateException failure = new IllegalStateException("decode failed");
    for (int i = 0; i < handlers.length; i++) {
      final int index = i;
      handlers[i] = mock(ByteHandler.class);
      when(handlers[i].supportsMemorySegments()).thenReturn(true);
      if (i == failedHandler) {
        when(handlers[i].decompressScoped(segments[i])).thenThrow(failure);
      } else {
        when(handlers[i].decompressScoped(segments[i])).thenReturn(
            new DecompressionResult(segments[i + 1], () -> releases[index]++));
      }
    }

    final ByteHandlerPipeline pipeline = new ByteHandlerPipeline(handlers);
    assertSame(failure, assertThrows(IllegalStateException.class, () -> pipeline.decompressScoped(segments[0])));
    for (int i = 0; i < releases.length; i++) {
      assertEquals(i < failedHandler
          ? 1
          : 0, releases[i]);
    }
  }

  @Test
  void ownershipCanBeTransferredOnlyOnce() {
    final MemorySegment segment = MemorySegment.ofArray(new byte[] {1});
    final AtomicInteger releases = new AtomicInteger();
    final Runnable release = releases::incrementAndGet;
    final DecompressionResult result = new DecompressionResult(segment, segment, release, new AtomicBoolean());

    assertSame(release, result.transferOwnership());
    assertNull(result.transferOwnership());
    result.close();
    assertEquals(0, releases.get());
    release.run();
    assertEquals(1, releases.get());
  }

  private static MemorySegment[] segments() {
    return new MemorySegment[] {MemorySegment.ofArray(new byte[] {0}), MemorySegment.ofArray(new byte[] {1}),
        MemorySegment.ofArray(new byte[] {2}), MemorySegment.ofArray(new byte[] {3})};
  }
}
