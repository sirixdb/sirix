package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.exception.SirixIOException;
import io.sirix.io.PageHasher;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.OverflowPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

final class FileChannelReadAheadTest {
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("prefetch").byteHandlerPipeline(new ByteHandlerPipeline()).build();

  @TempDir
  Path directory;

  @Test
  void advicePreservesPositionAndReadsCurrentBytesAfterAppend() throws IOException {
    final byte[] first = payload(123, 19);
    final byte[] second = payload(200_003, 23);
    try (FileChannel channel = channel(); FileChannelReader reader = reader(channel)) {
      final PageReference a = write(channel, 0, first);
      channel.position(17);
      final PageReference[] scratch = {a, null, new PageReference()};
      final Page[] beforeAppend = reader.read(scratch, CONFIG);
      assertEquals(17, channel.position());
      scratch[0] = null;
      assertArrayEquals(first, assertInstanceOf(OverflowPage.class, beforeAppend[0]).getDataBytes());
      final PageReference b = write(channel, channel.size() + 8192, second);
      final Page[] afterAppend = reader.read(new PageReference[] {b, a}, CONFIG);
      assertArrayEquals(second, assertInstanceOf(OverflowPage.class, afterAppend[0]).getDataBytes());
      assertArrayEquals(first, assertInstanceOf(OverflowPage.class, afterAppend[1]).getDataBytes());
      final int fd = PosixFadvise.extractFd(channel);
      if (fd >= 0) {
        assertTrue(PosixFadvise.adviseWillNeed(fd, 0, 4096));
      }
      assertEquals(17, channel.position());
    }
  }

  @Test
  void hintedPagesStillRejectCorruptionAndTruncation() throws IOException {
    try (FileChannel channel = channel(); FileChannelReader reader = reader(channel)) {
      final PageReference reference = write(channel, 0, payload(4097, 31));
      final PageReference other = write(channel, 128 * 1024, payload(257, 37));
      final PageReference[] references = {reference, other};
      reader.read(references, CONFIG);
      final long end = channel.size();
      final ByteBuffer last = ByteBuffer.allocate(1);
      assertEquals(1, channel.read(last, end - 1));
      last.flip();
      last.put(0, (byte) (last.get(0) ^ 1));
      channel.write(last, end - 1);
      assertThrows(SirixCorruptionException.class, () -> reader.read(references, CONFIG));
      channel.truncate(2);
      assertThrows(SirixIOException.class, () -> reader.read(references, CONFIG));
      reader.close();
      assertEquals(-1, PosixFadvise.extractFd(channel));
      assertThrows(SirixIOException.class, () -> reader.read(references, CONFIG));
    }
  }

  @Test
  void unsupportedChannelsAndInvalidHintsCannotReadOrAllocatePages() {
    assertEquals(-1, PosixFadvise.extractFd(mock(FileChannel.class)));
    assertFalse(PosixFadvise.adviseWillNeed(-1, 0, 4096));
    assertFalse(PosixFadvise.adviseWillNeed(0, -1, 4096));
    assertFalse(PosixFadvise.adviseWillNeed(0, 0, 0));
    assertFalse(PosixFadvise.adviseWillNeed(0, Long.MAX_VALUE, 4096));
  }

  @Test
  void disjointBatchesCrossReadAheadWindowsAndPreserveCallerOrder() throws IOException {
    final int count = 40;
    final byte[][] expected = new byte[count][];
    final PageReference[] references = new PageReference[count];
    try (FileChannel channel = channel(); FileChannelReader reader = reader(channel)) {
      for (int index = 0; index < count; index++) {
        expected[index] = payload(129 + index, 71L + index);
        references[index] = write(channel, (long) (count - index) * 128 * 1024, expected[index]);
      }
      channel.position(29);
      final Page[] actual = reader.read(references, CONFIG);
      assertEquals(29, channel.position());
      for (int index = 0; index < count; index++) {
        assertArrayEquals(expected[index], assertInstanceOf(OverflowPage.class, actual[index]).getDataBytes());
      }
    }
  }

  private FileChannel channel() throws IOException {
    return FileChannel.open(directory.resolve("pages"), StandardOpenOption.CREATE, StandardOpenOption.READ,
        StandardOpenOption.WRITE);
  }

  private static FileChannelReader reader(final FileChannel channel) {
    return new FileChannelReader(channel, mock(FileChannel.class), new ByteHandlerPipeline(), SerializationType.DATA,
        new PagePersister(), Caffeine.newBuilder().build());
  }

  private static PageReference write(final FileChannel channel, final long offset, final byte[] payload)
      throws IOException {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(payload.length + 128)) {
      new PagePersister().serializePage(CONFIG, out, new OverflowPage(payload), SerializationType.DATA);
      final byte[] body = out.toByteArray();
      final ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + body.length).order(ByteOrder.LITTLE_ENDIAN);
      frame.putInt(body.length).put(body).flip();
      while (frame.hasRemaining()) {
        channel.write(frame, offset + frame.position());
      }
      final PageReference reference = new PageReference();
      reference.setKey(offset);
      reference.setHash(PageHasher.computeLong(body, CONFIG.hashAlgorithm));
      return reference;
    }
  }

  private static byte[] payload(final int length, final long seed) {
    final byte[] bytes = new byte[length];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }
}
