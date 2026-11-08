package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.io.PageHasher;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A coalesced run spanning several MiB needs a run buffer far above the pool's 128 KiB defaults.
 * The first such read may allocate one; the pool must keep it, so the same batch read again borrows
 * it instead of allocating a fresh direct buffer per run (which was 17 MB of direct allocation per
 * query on a 50 ms query, re-faulted after every GC cycle).
 */
final class FileChannelRunBufferReuseTest {

  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("run-buffer-reuse").byteHandlerPipeline(new ByteHandlerPipeline()).build();

  private static final byte[] KEY = {3, 1, 4};

  /** Below the 64 KiB coalescing gap, so every member joins one run; 40 members span ~1.9 MiB. */
  private static final int STRIDE = 48 * 1024;

  private static final int MEMBERS = 40;

  @BeforeAll
  static void initializeAllocator() {
    assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance()).init(8L * 1024 * 1024 * 1024);
  }

  @Test
  void aRunBufferAllocatedForOneBatchServesTheNextBatchesOfTheSameShape() throws IOException {
    final long[] offsets = new long[MEMBERS];
    final byte[][] values = new byte[MEMBERS][];
    final byte[][] frames = new byte[MEMBERS][];
    for (int i = 0; i < MEMBERS; i++) {
      offsets[i] = 64L + (long) i * STRIDE;
      values[i] = value(97 + 7 * i, i);
      try (HOTLeafPage leaf = new HOTLeafPage(100L + i, 1, IndexType.PROJECTION)) {
        assertTrue(leaf.put(KEY, values[i]));
        frames[i] = frame(leaf);
      }
    }
    final byte[] file = new byte[Math.toIntExact(offsets[MEMBERS - 1] + frames[MEMBERS - 1].length)];
    final PageReference[] references = new PageReference[MEMBERS];
    for (int i = 0; i < MEMBERS; i++) {
      System.arraycopy(frames[i], 0, file, Math.toIntExact(offsets[i]), frames[i].length);
      references[i] = new PageReference();
      references[i].setKey(offsets[i]);
      references[i].setHash(
          PageHasher.computeLong(Arrays.copyOfRange(frames[i], Integer.BYTES, frames[i].length), CONFIG.hashAlgorithm));
    }
    assertTrue(offsets[MEMBERS - 1] - offsets[0] > 1024 * 1024, "the run must exceed every default pooled buffer");

    try (FileChannelReader reader = new FileChannelReader(channel(file), mock(FileChannel.class),
        new ByteHandlerPipeline(), SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build())) {
      readAndVerify(reader, references, values);
      final long missesAfterFirstBatch = FileChannelReader.bufferPoolMisses();
      final long pooledAfterFirstBatch = FileChannelReader.pooledBufferBytes();
      assertTrue(pooledAfterFirstBatch >= offsets[MEMBERS - 1] - offsets[0],
          "the pool keeps a buffer that covers the run: " + pooledAfterFirstBatch + " bytes pooled");
      for (int repeat = 0; repeat < 3; repeat++) {
        readAndVerify(reader, references, values);
      }
      assertEquals(missesAfterFirstBatch, FileChannelReader.bufferPoolMisses(),
          "repeated batches of the same shape borrow the pooled run buffer instead of allocating");
    }
  }

  private static void readAndVerify(final FileChannelReader reader, final PageReference[] references,
      final byte[][] values) {
    final Page[] pages = reader.read(references, CONFIG);
    try {
      for (int i = 0; i < references.length; i++) {
        final HOTLeafPage leaf = assertInstanceOf(HOTLeafPage.class, pages[i]);
        assertArrayEquals(values[i], leaf.copyStoredValue(leaf.findEntry(KEY)));
      }
    } finally {
      for (final Page page : pages) {
        page.close();
      }
    }
  }

  private static FileChannel channel(final byte[] contents) throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    when(channel.size()).thenReturn((long) contents.length);
    when(channel.read(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer target = invocation.getArgument(0);
      final long offset = invocation.getArgument(1);
      if (offset >= contents.length) {
        return -1;
      }
      final int from = Math.toIntExact(offset);
      final int count = Math.min(target.remaining(), contents.length - from);
      target.put(contents, from, count);
      return count;
    });
    return channel;
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

  private static byte[] value(final int length, final int seed) {
    final byte[] value = new byte[length];
    for (int i = 0; i < length; i++) {
      value[i] = (byte) (31 * i + seed);
    }
    return value;
  }
}
