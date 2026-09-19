package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.exception.SirixIOException;
import io.sirix.io.PageHasher;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.index.IndexType;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class FileChannelReaderBatchBoundsTest {
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("batch-bounds").byteHandlerPipeline(new ByteHandlerPipeline()).build();

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @ResourceLock(Resources.SYSTEM_PROPERTIES)
  void mixedBatchKeepsHotPagesOwnedAfterBatchBufferReuse(final boolean borrowInput) throws IOException {
    final String property = "sirix.filechannel.borrowBatchInput";
    final String previous = System.getProperty(property);
    System.setProperty(property, Boolean.toString(borrowInput));
    try {
      final byte[] key = {1, 2, 3};
      final byte[] first = payload(257, 31);
      final byte[] last = payload(733, 67);
      final byte[] middle = payload(127, 91);
      final int[] offsets = {64, 4096, 8192};
      final byte[][] frames;
      try (HOTLeafPage firstPage = new HOTLeafPage(11, 1, IndexType.PROJECTION);
          HOTLeafPage lastPage = new HOTLeafPage(12, 1, IndexType.PROJECTION)) {
        assertTrue(firstPage.put(key, first));
        assertTrue(lastPage.put(key, last));
        frames = new byte[][] {frame(firstPage), frame(middle), frame(lastPage)};
      }
      final byte[] initial = new byte[offsets[2] + frames[2].length];
      final PageReference[] references = new PageReference[3];
      for (int i = 0; i < frames.length; i++) {
        System.arraycopy(frames[i], 0, initial, offsets[i], frames[i].length);
        references[i] = reference(offsets[i]);
        references[i].setHash(PageHasher.computeLong(Arrays.copyOfRange(frames[i], Integer.BYTES, frames[i].length),
            CONFIG.hashAlgorithm));
      }
      final AtomicReference<byte[]> contents = new AtomicReference<>(initial);
      try (FileChannelReader reader = reader(channel(contents))) {
        final Page[] pages = reader.read(references, CONFIG);
        try {
          final byte[] replacementFrame = frame(payload(1024, 123));
          final byte[] replacement = new byte[offsets[2] + replacementFrame.length];
          final PageReference[] replacementReferences = new PageReference[3];
          for (int i = 0; i < offsets.length; i++) {
            System.arraycopy(replacementFrame, 0, replacement, offsets[i], replacementFrame.length);
            replacementReferences[i] = reference(offsets[i]);
          }
          contents.set(replacement);
          // Reuse the same span and final-body buffers, not only a differently sized single read.
          for (int reuse = 0; reuse < 12; reuse++) {
            for (final Page page : reader.read(replacementReferences, CONFIG)) {
              page.close();
            }
          }
          final HOTLeafPage firstRead = assertInstanceOf(HOTLeafPage.class, pages[0]);
          final HOTLeafPage lastRead = assertInstanceOf(HOTLeafPage.class, pages[2]);
          assertArrayEquals(first, firstRead.copyStoredValue(firstRead.findEntry(key)));
          assertArrayEquals(middle, assertInstanceOf(OverflowPage.class, pages[1]).getDataBytes());
          assertArrayEquals(last, lastRead.copyStoredValue(lastRead.findEntry(key)));
        } finally {
          for (final Page page : pages) {
            page.close();
          }
        }
      }
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void coalescedBodiesAreChecksummedAndRemainOwnedAfterBufferReuse(final boolean segmentHandler) throws IOException {
    final int[] offsets = {64, 1024, 8192};
    final byte[][] payloads = {payload(127, 19), payload(1000, 37), payload(16384, 71)};
    final byte[][] frames = {frame(payloads[0]), frame(payloads[1]), frame(payloads[2])};
    final byte[] bytes = new byte[offsets[2] + frames[2].length];
    final PageReference[] references = new PageReference[3];
    for (int i = 0; i < 3; i++) {
      System.arraycopy(frames[i], 0, bytes, offsets[i], frames[i].length);
      references[i] = reference(offsets[i]);
      references[i].setHash(
          PageHasher.computeLong(Arrays.copyOfRange(frames[i], Integer.BYTES, frames[i].length), CONFIG.hashAlgorithm));
    }
    final ByteHandler handler;
    if (segmentHandler) {
      handler = new ByteHandlerPipeline();
    } else {
      handler = mock(ByteHandler.class);
      when(handler.supportsMemorySegments()).thenReturn(false);
      when(handler.deserialize(any(InputStream.class))).thenAnswer(invocation -> invocation.getArgument(0));
    }
    final AtomicReference<byte[]> contents = new AtomicReference<>(bytes);
    final FileChannel channel = channel(contents);
    try (FileChannelReader reader = new FileChannelReader(channel, mock(FileChannel.class), handler,
        SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build())) {
      final Page[] pages = reader.read(new PageReference[] {references[2], references[0], references[1]}, CONFIG);
      contents.set(frame(payload(32768, 103)));
      for (int reuse = 0; reuse < 80; reuse++) {
        reader.read(reference(0), CONFIG);
      }
      for (int i = 0; i < 3; i++) {
        assertArrayEquals(payloads[i], assertInstanceOf(OverflowPage.class, pages[(i + 1) % 3]).getDataBytes());
      }
      // Both non-final slices and the separately read final body must enforce their own hashes.
      contents.set(bytes);
      for (int corrupt = 0; corrupt < 3; corrupt++) {
        final int at = offsets[corrupt] + frames[corrupt].length - 1;
        bytes[at] ^= 1;
        assertThrows(SirixCorruptionException.class, () -> reader.read(references, CONFIG));
        bytes[at] ^= 1;
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 127, 1010, 1024, 4096, 262144})
  void prefixAndRemainderVerifyExactlyTheDeclaredBody(final int count) throws IOException {
    final byte[] payload = payload(count, 97);
    final byte[] frame = frame(payload);
    final byte[] bytes = new byte[64 + frame.length + 2048];
    Arrays.fill(bytes, (byte) 0xA5);
    System.arraycopy(frame, 0, bytes, 64, frame.length);
    final FileChannel channel = channel(new AtomicReference<>(bytes));
    final PageReference reference = reference(64);
    reference.setHash(
        PageHasher.computeLong(Arrays.copyOfRange(frame, Integer.BYTES, frame.length), CONFIG.hashAlgorithm));
    try (FileChannelReader reader = reader(channel)) {
      assertArrayEquals(payload, assertInstanceOf(OverflowPage.class, reader.read(reference, CONFIG)).getDataBytes());
      if (count == 127) {
        verify(channel, times(1)).read(any(ByteBuffer.class), anyLong());
      }
      // A following page's bytes are outside this page's checksum even if the prefix fetched them.
      bytes[64 + frame.length] ^= 1;
      assertArrayEquals(payload, assertInstanceOf(OverflowPage.class, reader.read(reference, CONFIG)).getDataBytes());
      bytes[64 + frame.length - 1] ^= 1;
      assertThrows(SirixCorruptionException.class, () -> reader.read(reference, CONFIG));
    }
  }

  @Test
  void speculativeSuffixMayEndAtEofAfterTheSizeWasCaptured() throws IOException {
    final byte[] payload = payload(127, 61);
    final FileChannel channel = channel(new AtomicReference<>(frame(payload)));
    when(channel.size()).thenReturn(4096L);
    try (FileChannelReader reader = reader(channel)) {
      assertArrayEquals(payload,
          assertInstanceOf(OverflowPage.class, reader.read(reference(0), CONFIG)).getDataBytes());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3})
  void shortLengthHeadersStillFailCleanly(final int bytes) throws IOException {
    final FileChannel channel = channel(new AtomicReference<>(new byte[bytes]));
    try (FileChannelReader reader = reader(channel)) {
      final SirixIOException error = assertThrows(SirixIOException.class, () -> reader.read(reference(0), CONFIG));
      assertTrue(error.getMessage().contains("Truncated page length header"));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {512, 131072})
  void coalescedAndSparseBatchesRefreshTheirBoundAfterAppend(final int secondOffset) throws IOException {
    final byte[] first = payload(127, 31);
    final byte[] second = payload(251, 79);
    final byte[] firstFrame = frame(first);
    final byte[] secondFrame = frame(second);
    final byte[] initial = new byte[secondOffset + secondFrame.length];
    System.arraycopy(firstFrame, 0, initial, 64, firstFrame.length);
    System.arraycopy(secondFrame, 0, initial, secondOffset, secondFrame.length);
    final AtomicReference<byte[]> contents = new AtomicReference<>(initial);
    final FileChannel channel = channel(contents);
    try (FileChannelReader reader = reader(channel)) {
      final Page[] pages = reader.read(new PageReference[] {reference(secondOffset), null, reference(64)}, CONFIG);
      assertArrayEquals(second, assertInstanceOf(OverflowPage.class, pages[0]).getDataBytes());
      assertNull(pages[1]);
      assertArrayEquals(first, assertInstanceOf(OverflowPage.class, pages[2]).getDataBytes());
      verify(channel, times(1)).size();

      // The appended page itself exceeds the previous file size. A reader-lifetime bound would
      // incorrectly reject it; a new batch must capture the new frontier.
      final byte[] appendedPayload = payload(initial.length + 17, 131);
      final byte[] appendedFrame = frame(appendedPayload);
      final byte[] appended = Arrays.copyOf(initial, initial.length + appendedFrame.length);
      System.arraycopy(appendedFrame, 0, appended, initial.length, appendedFrame.length);
      contents.set(appended);
      final Page[] later = reader.read(new PageReference[] {reference(initial.length), reference(64)}, CONFIG);
      assertArrayEquals(appendedPayload, assertInstanceOf(OverflowPage.class, later[0]).getDataBytes());
      assertArrayEquals(first, assertInstanceOf(OverflowPage.class, later[1]).getDataBytes());
      verify(channel, times(2)).size();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, Integer.MAX_VALUE})
  void invalidLengthsFailBeforeBodyAllocation(final int length) throws IOException {
    final byte[] bytes = new byte[1024];
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(64, length);
    final FileChannel channel = channel(new AtomicReference<>(bytes));
    try (FileChannelReader reader = reader(channel)) {
      final SirixIOException error = assertThrows(SirixIOException.class,
          () -> reader.read(new PageReference[] {reference(64), reference(512)}, CONFIG));
      assertTrue(error.getMessage().contains("out of bounds"));
      verify(channel, times(1)).size();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {512, 131072})
  void aTruncatedFinalBodyStillFailsCleanly(final int secondOffset) throws IOException {
    final byte[] encoded = frame(payload(127, 43));
    final byte[] bytes = new byte[secondOffset + encoded.length - 1];
    System.arraycopy(encoded, 0, bytes, 64, encoded.length);
    System.arraycopy(encoded, 0, bytes, secondOffset, encoded.length - 1);
    final FileChannel channel = channel(new AtomicReference<>(bytes));
    try (FileChannelReader reader = reader(channel)) {
      final SirixIOException error = assertThrows(SirixIOException.class,
          () -> reader.read(new PageReference[] {reference(64), reference(secondOffset)}, CONFIG));
      assertTrue(error.getMessage().contains("Truncated"));
      verify(channel, times(1)).size();
    }
  }

  @Test
  void shrinkingBetweenCallsDoesNotReuseAnOldAllocationBound() throws IOException {
    final byte[] encoded = frame(payload(257, 19));
    final AtomicReference<byte[]> contents = new AtomicReference<>(encoded);
    final FileChannel channel = channel(contents);
    try (FileChannelReader reader = reader(channel)) {
      reader.read(new PageReference[] {reference(0)}, CONFIG);
      contents.set(Arrays.copyOf(encoded, 16));
      final SirixIOException error =
          assertThrows(SirixIOException.class, () -> reader.read(new PageReference[] {reference(0)}, CONFIG));
      assertTrue(error.getMessage().contains("out of bounds"));
      verify(channel, times(2)).size();
    }
  }

  @Test
  void emptyBatchesDoNotAccessTheChannel() throws IOException {
    final FileChannel channel = channel(new AtomicReference<>(new byte[0]));
    try (FileChannelReader reader = reader(channel)) {
      assertEquals(0, reader.read(new PageReference[0], CONFIG).length);
      assertArrayEquals(new Page[2], reader.read(new PageReference[] {null, reference(-1)}, CONFIG));
      verify(channel, times(0)).size();
      verify(channel, times(0)).read(any(ByteBuffer.class), anyLong());
    }
  }

  private static FileChannel channel(final AtomicReference<byte[]> contents) throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    when(channel.size()).thenAnswer(ignored -> (long) contents.get().length);
    when(channel.read(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer target = invocation.getArgument(0);
      final long offset = invocation.getArgument(1);
      final byte[] bytes = contents.get();
      if (offset >= bytes.length) {
        return -1;
      }
      final int from = Math.toIntExact(offset);
      final int count = Math.min(target.remaining(), bytes.length - from);
      target.put(bytes, from, count);
      return count;
    });
    return channel;
  }

  private static FileChannelReader reader(final FileChannel channel) {
    return new FileChannelReader(channel, mock(FileChannel.class), new ByteHandlerPipeline(), SerializationType.DATA,
        new PagePersister(), Caffeine.newBuilder().build());
  }

  private static PageReference reference(final long offset) {
    final PageReference reference = new PageReference();
    reference.setKey(offset);
    return reference;
  }

  private static byte[] frame(final byte[] payload) throws IOException {
    return frame(new OverflowPage(payload));
  }

  private static byte[] frame(final Page page) throws IOException {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(1024)) {
      new PagePersister().serializePage(CONFIG, out, page, SerializationType.DATA);
      final byte[] body = out.toByteArray();
      return ByteBuffer.allocate(Integer.BYTES + body.length)
                       .order(ByteOrder.LITTLE_ENDIAN)
                       .putInt(body.length)
                       .put(body)
                       .array();
    }
  }

  private static byte[] payload(final int count, final long seed) {
    final byte[] payload = new byte[count];
    new Random(seed).nextBytes(payload);
    return payload;
  }
}
