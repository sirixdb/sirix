package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ScratchStats;
import io.sirix.cache.ScratchUse;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.io.PageHasher;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.OverflowPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.settings.StringCompressionType;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The file-channel reader keeps no buffers of its own: every read borrows allocator scratch, reuses
 * what the allocator keeps resident, and returns it on every way out.
 *
 * <p>
 * A coalesced run spanning several MiB needs a buffer far above an ordinary page's. The first such
 * read maps a region; the allocator must keep it, so the same batch read again borrows it instead
 * of taking fresh memory per run (which was 17 MB of direct allocation per query on a 50 ms query,
 * re-faulted after every GC cycle, when the reader pooled only 128 KiB buffers).
 *
 * <p>
 * The assertions are deltas of the process allocator's scratch counters, so the class runs
 * isolated; none of them measures time.
 */
@Isolated
final class FileChannelReadScratchTest {

  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("read-scratch").byteHandlerPipeline(new ByteHandlerPipeline()).build();

  private static final byte[] KEY = {3, 1, 4};

  /** Below the 64 KiB coalescing gap, so every member joins one run; 40 members span ~1.9 MiB. */
  private static final int STRIDE = 48 * 1024;

  private static final int MEMBERS = 40;

  /** The floor of every read's request: {@code sirix.filechannel.bufferBytes}. */
  private static final long PAGE_BUFFER_BYTES = 128L * 1024;

  private long lentBuffersBefore;

  @BeforeAll
  static void initializeAllocator() {
    assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance()).init(8L * 1024 * 1024 * 1024);
  }

  @BeforeEach
  void snapshotOutstandingBorrows() {
    lentBuffersBefore = stats().totalLentBuffers();
  }

  private static ScratchStats stats() {
    return Allocators.getInstance().scratchStats();
  }

  /** A batch of HOT leaves laid out {@link #STRIDE} bytes apart, with the file that holds them. */
  private record Batch(byte[] file, PageReference[] references, byte[][] values, long[] offsets, byte[][] frames) {

    static Batch create() throws IOException {
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
        references[i] = reference(offsets[i], frames[i]);
      }
      return new Batch(file, references, values, offsets, frames);
    }

    long spanBytes() {
      return offsets[MEMBERS - 1] + Integer.BYTES - offsets[0];
    }
  }

  @Test
  void aRunRegionMappedForOneBatchServesTheNextBatchesOfTheSameShape() throws IOException {
    final Batch batch = Batch.create();
    assertTrue(batch.spanBytes() > 1024 * 1024, "the run must exceed every frame-slot capacity");
    try (FileChannelReader reader = reader(batch.file())) {
      readAndVerify(reader, batch);
      final ScratchStats warm = stats();
      final int region = warm.capacityIndex(batch.spanBytes());
      assertEquals(2L * 1024 * 1024, warm.capacityBytes()[region], "a 1.9 MiB span is served by the 2 MiB capacity");
      assertTrue(warm.idleBuffers()[region] >= 1, "the allocator keeps the run's region resident");
      assertEquals(lentBuffersBefore, warm.totalLentBuffers(), "the batch returned every buffer");
      final long runsBefore = FileChannelReader.runCount();

      for (int repeat = 0; repeat < 3; repeat++) {
        readAndVerify(reader, batch);
      }
      final ScratchStats after = stats();
      assertEquals(3, FileChannelReader.runCount() - runsBefore, "each batch is one coalesced run");
      assertEquals(0, after.totalCreated() - warm.totalCreated(),
          "repeated batches of the same shape borrow the resident region instead of mapping one");
      assertEquals(3, after.acquisitions()[region] - warm.acquisitions()[region], "one span borrow per batch");
      assertEquals(lentBuffersBefore, after.totalLentBuffers());
      assertEquals(0, after.refusals() - warm.refusals());
      assertEquals(0, after.staleReleases() - warm.staleReleases());
    }
  }

  @Test
  void aRefusedSpanIsReadPageByPageWithTheSameAnswer() throws IOException {
    final Batch batch = Batch.create();
    try (FileChannelReader reader = reader(batch.file())) {
      readAndVerify(reader, batch);
      final ScratchStats before = stats();
      final int region = before.capacityIndex(batch.spanBytes());
      final long refusalsBefore = FileChannelReader.runScratchRefusals();
      final long fallbacksBefore = FileChannelReader.runFallbacks();
      final long runsBefore = FileChannelReader.runCount();
      final boolean previous = FileChannelReader.setRefuseSpanScratchForTesting(true);
      try {
        readAndVerify(reader, batch);
      } finally {
        FileChannelReader.setRefuseSpanScratchForTesting(previous);
      }
      final ScratchStats after = stats();
      assertEquals(1, FileChannelReader.runScratchRefusals() - refusalsBefore, "the one run was refused its span");
      assertEquals(MEMBERS, FileChannelReader.runFallbacks() - fallbacksBefore, "and read member by member");
      assertEquals(0, FileChannelReader.runCount() - runsBefore, "no span was read");
      assertEquals(0, after.acquisitions()[region] - before.acquisitions()[region], "no span buffer was borrowed");
      assertEquals(MEMBERS, after.acquisitions(ScratchUse.READ) - before.acquisitions(ScratchUse.READ),
          "each member was read through a buffer of its own");
      assertEquals(MEMBERS * PAGE_BUFFER_BYTES,
          after.requestedBytes(ScratchUse.READ) - before.requestedBytes(ScratchUse.READ), "an ordinary page buffer");
      assertEquals(lentBuffersBefore, after.totalLentBuffers());

      readAndVerify(reader, batch);
      assertEquals(1, FileChannelReader.runCount() - runsBefore, "with room again the batch coalesces");
    }
  }

  @Test
  void ordinaryPageReadsShareOneResidentBuffer() throws IOException {
    final Batch batch = Batch.create();
    try (FileChannelReader reader = reader(batch.file())) {
      readSingle(reader, batch, 0);
      final ScratchStats warm = stats();
      for (int i = 0; i < MEMBERS; i++) {
        readSingle(reader, batch, i);
      }
      final ScratchStats after = stats();
      assertEquals(MEMBERS, after.acquisitions(ScratchUse.READ) - warm.acquisitions(ScratchUse.READ),
          "one borrow per page read");
      assertEquals(MEMBERS * PAGE_BUFFER_BYTES,
          after.requestedBytes(ScratchUse.READ) - warm.requestedBytes(ScratchUse.READ),
          "every page read asks for the common request floor, whatever the page's size");
      assertEquals(0, after.totalCreated() - warm.totalCreated(), "and reuses the buffer the last read returned");
      assertEquals(lentBuffersBefore, after.totalLentBuffers());
    }
  }

  @Test
  void aPageLargerThanTheRequestFloorIsReadThroughALargerBufferAndBothAreReturned() throws IOException {
    final byte[] blob = value(300 * 1024, 11);
    final byte[] frame;
    try (OverflowPage page = new OverflowPage(blob)) {
      frame = frame(page);
    }
    final long offset = 512L;
    final byte[] file = new byte[Math.toIntExact(offset + frame.length)];
    System.arraycopy(frame, 0, file, Math.toIntExact(offset), frame.length);
    final PageReference reference = reference(offset, frame);
    try (FileChannelReader reader = reader(file)) {
      final ScratchStats before = stats();
      for (int repeat = 0; repeat < 3; repeat++) {
        try (OverflowPage page = assertInstanceOf(OverflowPage.class, reader.read(reference, CONFIG))) {
          assertArrayEquals(blob, page.getDataBytes());
        }
      }
      final ScratchStats after = stats();
      final int large = after.capacityIndex(frame.length);
      assertEquals(512L * 1024, after.capacityBytes()[large], "a 300 KiB body needs the 512 KiB capacity");
      assertEquals(3, after.acquisitions()[large] - before.acquisitions()[large],
          "the body moved to a larger buffer once per read");
      assertEquals(6, after.acquisitions(ScratchUse.READ) - before.acquisitions(ScratchUse.READ),
          "prefix buffer, then body buffer");
      assertTrue(after.created()[large] - before.created()[large] <= 1, "the larger buffer is mapped once at most");
      assertEquals(lentBuffersBefore, after.totalLentBuffers(), "the prefix buffer and the body buffer were both returned");
    }
  }

  @Test
  void everyFailingReadReturnsItsBuffer() throws IOException {
    final Batch batch = Batch.create();
    final ScratchStats before = stats();

    // A member's body fails its checksum in the middle of a coalesced run.
    final byte[] corrupted = batch.file().clone();
    corrupted[Math.toIntExact(batch.offsets()[MEMBERS / 2] + batch.frames()[MEMBERS / 2].length - 1)] ^= 1;
    try (FileChannelReader reader = reader(corrupted)) {
      assertThrows(SirixRuntimeException.class, () -> reader.read(batch.references(), CONFIG));
      assertEquals(lentBuffersBefore, stats().totalLentBuffers(), "a failed run returned its span buffer");
      assertThrows(SirixRuntimeException.class, () -> reader.read(batch.references()[MEMBERS / 2], CONFIG));
      assertEquals(lentBuffersBefore, stats().totalLentBuffers(), "a failed page read returned its buffer");
    }

    // The file ends inside a page body: the body read comes up short.
    final byte[] truncated = Arrays.copyOf(batch.file(), batch.file().length - 8);
    try (FileChannelReader reader = reader(truncated)) {
      assertThrows(SirixRuntimeException.class, () -> reader.read(batch.references()[MEMBERS - 1], CONFIG));
      assertEquals(lentBuffersBefore, stats().totalLentBuffers(), "a short read returned its buffer");
      assertThrows(SirixRuntimeException.class, () -> reader.read(batch.references(), CONFIG));
      assertEquals(lentBuffersBefore, stats().totalLentBuffers(), "a run whose last body comes up short returned its buffer");
    }

    // A length header that no page of this file can have.
    final byte[] garbled = batch.file().clone();
    ByteBuffer.wrap(garbled).order(ByteOrder.LITTLE_ENDIAN).putInt(Math.toIntExact(batch.offsets()[3]), -5);
    try (FileChannelReader reader = reader(garbled)) {
      assertThrows(SirixRuntimeException.class, () -> reader.read(batch.references()[3], CONFIG));
      assertEquals(lentBuffersBefore, stats().totalLentBuffers(), "a rejected length header returned its buffer");
    }

    final ScratchStats after = stats();
    assertTrue(after.acquisitions(ScratchUse.READ) > before.acquisitions(ScratchUse.READ),
        "the failing reads did borrow buffers");
    assertEquals(0, after.staleReleases() - before.staleReleases(), "and returned each exactly once");
  }

  private static void readAndVerify(final FileChannelReader reader, final Batch batch) {
    final Page[] pages = reader.read(batch.references(), CONFIG);
    try {
      for (int i = 0; i < pages.length; i++) {
        final HOTLeafPage leaf = assertInstanceOf(HOTLeafPage.class, pages[i]);
        assertArrayEquals(batch.values()[i], leaf.copyStoredValue(leaf.findEntry(KEY)));
      }
    } finally {
      for (final Page page : pages) {
        page.close();
      }
    }
  }

  private static void readSingle(final FileChannelReader reader, final Batch batch, final int member) {
    try (HOTLeafPage leaf = assertInstanceOf(HOTLeafPage.class, reader.read(batch.references()[member], CONFIG))) {
      assertArrayEquals(batch.values()[member], leaf.copyStoredValue(leaf.findEntry(KEY)));
    }
  }

  @Test
  void failedRegionProbeReturnsItsBorrowAndAllowsTheFollowingRead() throws IOException {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("probe-failure")
                                                              .byteHandlerPipeline(new ByteHandlerPipeline())
                                                              .verifyChecksumsOnRead(false)
                                                              .stringCompressionType(StringCompressionType.NONE)
                                                              .build();
    final byte[] file = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putInt(12).array();
    final PageReference reference = new PageReference();
    reference.setKey(0);
    final PagePersister persister = mock(PagePersister.class);
    final OutOfMemoryError failure = new OutOfMemoryError("injected probe failure");
    when(persister.probeRegionTableOffset(any(), any(), any())).thenThrow(failure);
    final ScratchStats before = stats();
    try (FileChannelReader reader = new FileChannelReader(channel(file), mock(FileChannel.class),
        new ByteHandlerPipeline(), SerializationType.DATA, persister, Caffeine.newBuilder().build())) {
      assertSame(failure, assertThrows(OutOfMemoryError.class, () -> reader.readRegionsOnly(reference, config, 1, 0)));
      assertEquals(before.lentBytes(), stats().lentBytes());
    }
    final Batch batch = Batch.create();
    try (FileChannelReader following = reader(batch.file())) {
      readSingle(following, batch, 0);
    }
    assertEquals(before.lentBytes(), stats().lentBytes());
  }

  private static FileChannelReader reader(final byte[] file) throws IOException {
    return new FileChannelReader(channel(file), mock(FileChannel.class), new ByteHandlerPipeline(),
        SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build());
  }

  private static PageReference reference(final long offset, final byte[] frame) {
    final PageReference reference = new PageReference();
    reference.setKey(offset);
    reference.setHash(
        PageHasher.computeLong(Arrays.copyOfRange(frame, Integer.BYTES, frame.length), CONFIG.hashAlgorithm));
    return reference;
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
