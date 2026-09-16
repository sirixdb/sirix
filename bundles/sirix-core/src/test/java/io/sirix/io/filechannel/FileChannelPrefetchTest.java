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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

/**
 * The standard backend's advisory prefetch: it must be advertised only where the advice can reach
 * the kernel, must never change what a read returns, and must never fail a read when it is declined.
 */
final class FileChannelPrefetchTest {
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("advisory-prefetch").byteHandlerPipeline(new ByteHandlerPipeline()).build();

  @TempDir
  Path directory;

  @Test
  void advertisesABatchOnlyWhereAdviceCanReachTheKernel() throws IOException {
    try (FileChannel channel = channel(); FileChannelReader reader = reader(channel)) {
      final int batch = reader.preferredPrefetchBatch();
      assertEquals(PosixFadvise.extractFd(channel) >= 0, batch > 0,
          "the batch is advertised exactly when the descriptor is extractable");
      assertEquals(batch, reader.preferredPrefetchBatch(), "resolved once, stable afterwards");
    }
    try (FileChannelReader reader = reader(mock(FileChannel.class))) {
      assertEquals(0, reader.preferredPrefetchBatch(), "an unsupported channel advertises nothing");
    }
    final FileChannel closed = channel();
    final FileChannelReader closedReader = reader(closed);
    closedReader.close();
    assertEquals(0, closedReader.preferredPrefetchBatch(), "a closed channel advertises nothing");
    assertDoesNotThrow(() -> closedReader.prefetch(new PageReference[] {new PageReference().setKey(0)}, 1));
  }

  @Test
  void hintedPagesReadIdenticallyAndLeaveThePositionAlone() throws IOException {
    final int count = 48;
    final byte[][] expected = new byte[count][];
    final PageReference[] references = new PageReference[count + 3];
    try (FileChannel channel = channel(); FileChannelReader reader = reader(channel)) {
      // Pages adjacent within the tail (exact extents), pages far apart (tail-bounded extents), and a
      // shuffled hand-over order, so the sort, the extent merge and the gap flush all run.
      long offset = 0;
      for (int index = 0; index < count; index++) {
        expected[index] = payload(200 + 37 * index, 11L + index);
        references[(index * 7) % count] = write(channel, offset, expected[index]);
        offset = channel.size() + (index % 5 == 4
            ? 300 * 1024
            : 128 + (index % 3) * 1000);
      }
      references[count] = null;
      references[count + 1] = new PageReference();
      references[count + 2] = new PageReference().setKey(-7);
      channel.position(31);
      reader.prefetch(references, references.length);
      reader.prefetch(references, 1);
      assertEquals(31, channel.position());
      for (int slot = 0; slot < count; slot++) {
        final PageReference reference = references[slot];
        final int index = slotToIndex(slot, count);
        assertArrayEquals(expected[index],
            assertInstanceOf(OverflowPage.class, reader.read(reference, CONFIG)).getDataBytes());
      }
      final Page[] batch = reader.read(references, CONFIG);
      assertEquals(references.length, batch.length);
      for (int slot = 0; slot < count; slot++) {
        assertArrayEquals(expected[slotToIndex(slot, count)],
            assertInstanceOf(OverflowPage.class, batch[slot]).getDataBytes());
      }
      assertNull(batch[count]);
      assertNull(batch[count + 1]);
      assertNull(batch[count + 2]);
      assertEquals(31, channel.position());
    }
  }

  @Test
  void declinedOrPointlessHintsNeverFailAReadAndCorruptionStillSurfaces() throws IOException {
    try (FileChannel channel = channel(); FileChannelReader reader = reader(channel)) {
      final byte[] first = payload(5000, 3);
      final PageReference a = write(channel, 0, first);
      final PageReference b = write(channel, 192 * 1024, payload(600, 5));
      final PageReference pastEnd = new PageReference().setKey(channel.size() + 64L * 1024 * 1024);
      final PageReference[] hints = {pastEnd, b, null, a, new PageReference().setKey(-1), b, b};
      assertDoesNotThrow(() -> reader.prefetch(hints, 0));
      assertDoesNotThrow(() -> reader.prefetch(hints, hints.length));
      assertDoesNotThrow(() -> reader.prefetch(hints, hints.length + 5));
      assertDoesNotThrow(() -> reader.prefetch(new PageReference[] {pastEnd}, 1));
      assertDoesNotThrow(() -> reader.prefetch(null, 1));
      assertArrayEquals(first, assertInstanceOf(OverflowPage.class, reader.read(a, CONFIG)).getDataBytes());
      // Duplicates in one batch: both slots are served, input-aligned.
      final Page[] duplicated = reader.read(new PageReference[] {b, a, b}, CONFIG);
      assertArrayEquals(assertInstanceOf(OverflowPage.class, duplicated[0]).getDataBytes(),
          assertInstanceOf(OverflowPage.class, duplicated[2]).getDataBytes());
      assertArrayEquals(first, assertInstanceOf(OverflowPage.class, duplicated[1]).getDataBytes());
      // A hinted page is verified exactly like an unhinted one.
      final long end = channel.size();
      final ByteBuffer last = ByteBuffer.allocate(1);
      assertEquals(1, channel.read(last, end - 1));
      last.flip();
      last.put(0, (byte) (last.get(0) ^ 1));
      channel.write(last, end - 1);
      reader.prefetch(hints, hints.length);
      assertThrows(SirixCorruptionException.class, () -> reader.read(b, CONFIG));
      assertThrows(SirixCorruptionException.class, () -> reader.read(new PageReference[] {a, b}, CONFIG));
      channel.truncate(2);
      reader.prefetch(hints, hints.length);
      assertThrows(SirixIOException.class, () -> reader.read(a, CONFIG));
      assertThrows(SirixIOException.class, () -> reader.read(new PageReference[] {a, b}, CONFIG));
    }
  }

  @Test
  void batchesBeyondTheHintCursorBoundReturnInputAlignedPages() throws IOException {
    final int count = 1500;
    final byte[][] expected = new byte[count][];
    final PageReference[] references = new PageReference[count];
    try (FileChannel channel = channel(); FileChannelReader reader = reader(channel)) {
      // Descending hand-over order; runs of near-adjacent pages alternate with isolated pages so the
      // hint cursor advances run by run past the 1,024-page bound.
      long offset = 0;
      for (int index = 0; index < count; index++) {
        expected[index] = payload(64 + (index * 97) % 900, 1000L + index);
        references[count - 1 - index] = write(channel, offset, expected[index]);
        // The serialized frame is longer than the payload (compression framing): place the next page
        // after the frame actually written, plus a gap that is 0, 512, 1024 or 1536 bytes.
        offset = channel.size() + (index % 16 == 15
            ? 256 * 1024
            : (index % 4) * 512);
      }
      channel.position(5);
      final Page[] pages = reader.read(references, CONFIG);
      assertEquals(5, channel.position());
      for (int slot = 0; slot < count; slot++) {
        assertArrayEquals(expected[count - 1 - slot], assertInstanceOf(OverflowPage.class, pages[slot]).getDataBytes(),
            "slot " + slot);
      }
    }
  }

  private static int slotToIndex(final int slot, final int count) {
    for (int index = 0; index < count; index++) {
      if ((index * 7) % count == slot) {
        return index;
      }
    }
    throw new IllegalStateException("slot " + slot + " has no index");
  }

  private FileChannel channel() throws IOException {
    return FileChannel.open(directory.resolve("pages-" + System.nanoTime()), StandardOpenOption.CREATE,
        StandardOpenOption.READ, StandardOpenOption.WRITE);
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
