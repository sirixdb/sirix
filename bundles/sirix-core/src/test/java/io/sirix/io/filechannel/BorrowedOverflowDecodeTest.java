package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageKind;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.page.SirixLZ77Codec;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Borrowing is safe only when the decoder finishes with independently owned payload bytes. */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class BorrowedOverflowDecodeTest {
  private static final ResourceConfiguration CONFIG = ResourceConfiguration.newBuilder("borrowed-overflow").build();
  private static final String SWITCH = "sirix.io.borrowOverflowInput";

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rawAndCompressedPayloadsOutliveHeapAndNativeInput(final boolean compressed) throws IOException {
    final ByteHandlerPipeline pipeline = spy(new ByteHandlerPipeline());
    try (FileChannelReader reader = reader(pipeline)) {
      for (final int length : new int[] {0, 1, 127, 4096, 32768}) {
        for (final boolean repeated : new boolean[] {false, true}) {
          final byte[] payload = new byte[length];
          if (repeated) {
            Arrays.fill(payload, (byte) 37);
          } else {
            new Random(length).nextBytes(payload);
          }
          for (final boolean lazy : new boolean[] {false, true}) {
            final byte[] heapWire = frame(payload, compressed);
            final OverflowPage heapPage = assertInstanceOf(OverflowPage.class,
                reader.deserializeFromSegment(CONFIG, MemorySegment.ofArray(heapWire).asReadOnly(), null, lazy));
            Arrays.fill(heapWire, (byte) 0xA5);
            final OverflowPage nativePage;
            try (Arena arena = Arena.ofConfined()) {
              final byte[] wire = frame(payload, compressed);
              final MemorySegment reservoir = arena.allocate(wire.length + 7L);
              final MemorySegment input = reservoir.asSlice(3, wire.length);
              input.copyFrom(MemorySegment.ofArray(wire));
              nativePage = assertInstanceOf(OverflowPage.class,
                  reader.deserializeFromSegment(CONFIG, input.asReadOnly(), null, lazy));
              reservoir.fill((byte) 0xA5);
            }
            assertArrayEquals(payload, heapPage.getDataBytes());
            assertArrayEquals(payload, nativePage.getDataBytes());
          }
        }
      }
      verify(pipeline, never()).decompressScoped(any(MemorySegment.class));
    }
  }

  @Test
  void configuredHandlerStillTransformsAnInputStartingWithTheOverflowKind() throws IOException {
    final byte[] payload = {41, 42, 43};
    final MemorySegment transformed = MemorySegment.ofArray(frame(payload, false));
    final ByteHandler handler = mock(ByteHandler.class);
    final Runnable release = mock(Runnable.class);
    when(handler.supportsMemorySegments()).thenReturn(true);
    when(handler.decompressScoped(any(MemorySegment.class))).thenReturn(
        new ByteHandler.DecompressionResult(transformed, transformed, release, new AtomicBoolean(false)));
    try (FileChannelReader reader = reader(new ByteHandlerPipeline(handler))) {
      final MemorySegment input = MemorySegment.ofArray(frame(new byte[] {1, 2, 3}, false));
      final OverflowPage page = assertInstanceOf(OverflowPage.class, reader.deserializeFromSegment(CONFIG, input));
      transformed.fill((byte) 0);
      assertArrayEquals(payload, page.getDataBytes());
      verify(handler).decompressScoped(input);
      verify(release).run();
    }
  }

  @Test
  void otherPageKindsKeepTheOwnedPipelinePath() throws IOException {
    final ByteHandlerPipeline pipeline = spy(new ByteHandlerPipeline());
    try (FileChannelReader reader = reader(pipeline); MemorySegmentBytesOut out = new MemorySegmentBytesOut(1024)) {
      new PagePersister().serializePage(CONFIG, out, new UberPage(), SerializationType.DATA);
      final MemorySegment input = MemorySegment.ofArray(out.toByteArray());
      assertInstanceOf(UberPage.class, reader.deserializeFromSegment(CONFIG, input));
      verify(pipeline).decompressScoped(input);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void truncatedPayloadsStillFailBeforeDecode(final boolean compressed) throws IOException {
    final ByteHandlerPipeline pipeline = spy(new ByteHandlerPipeline());
    final byte[] wire = frame(new byte[127], compressed);
    try (FileChannelReader reader = reader(pipeline)) {
      assertThrows(IllegalStateException.class,
          () -> reader.deserializeFromSegment(CONFIG, MemorySegment.ofArray(Arrays.copyOf(wire, wire.length - 1))));
      verify(pipeline, never()).decompressScoped(any(MemorySegment.class));
    }
  }

  @Test
  void diagnosticSwitchRestoresTheOwnedPipeline() throws IOException {
    final String previous = System.getProperty(SWITCH);
    System.setProperty(SWITCH, "false");
    try {
      final ByteHandlerPipeline pipeline = spy(new ByteHandlerPipeline());
      final byte[] payload = {7, 8, 9};
      try (FileChannelReader reader = reader(pipeline)) {
        final MemorySegment input = MemorySegment.ofArray(frame(payload, false));
        final Page page = reader.deserializeFromSegment(CONFIG, input);
        input.fill((byte) 0);
        assertArrayEquals(payload, assertInstanceOf(OverflowPage.class, page).getDataBytes());
        verify(pipeline).decompressScoped(input);
      }
    } finally {
      if (previous == null) {
        System.clearProperty(SWITCH);
      } else {
        System.setProperty(SWITCH, previous);
      }
    }
  }

  private static FileChannelReader reader(final ByteHandler handler) {
    return new FileChannelReader(mock(FileChannel.class), mock(FileChannel.class), handler, SerializationType.DATA,
        new PagePersister(), Caffeine.newBuilder().build());
  }

  private static byte[] frame(final byte[] payload, final boolean compressed) throws IOException {
    // Explicit wire frames cover both formats independently of the writer's codec election.
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(payload.length + 128)) {
      out.writeByte(PageKind.OVERFLOWPAGE.getID());
      out.writeByte((byte) 0); // BinaryEncodingVersion.V0.
      out.writeByte((byte) (compressed
          ? 1
          : 0));
      out.writeInt(payload.length);
      if (compressed) {
        final byte[] encoded = new byte[SirixLZ77Codec.maxEncodedSize(payload.length)];
        final int length = SirixLZ77Codec.encode(MemorySegment.ofArray(payload), 0, payload.length, encoded, 0);
        out.writeInt(length);
        out.writeByte((byte) 3); // LZ77 overflow payload codec.
        out.write(encoded, 0, length);
      } else {
        out.write(payload);
      }
      return out.toByteArray();
    }
  }
}
