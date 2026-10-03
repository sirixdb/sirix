package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.PageHasher;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Raw images retain all committed slots, but borrow neither buffers nor mutable search metadata.
 */
final class HOTCompactFragmentReadTest {
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("compact-fragment")
                           .versioningApproach(VersioningType.FULL)
                           .byteHandlerPipeline(new ByteHandlerPipeline())
                           .build();

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 17, 32, 33, 256, 512})
  void packedImageOutlivesPooledInputAndCopiesToOrdinaryWritableCapacity(final int count) throws IOException {
    final byte[] wire;
    try (HOTLeafPage original = leaf(count)) {
      wire = serialize(original);
    }
    final AtomicReference<byte[]> contents = new AtomicReference<>(frame(wire));
    final ByteHandlerPipeline pipeline = spy(new ByteHandlerPipeline());
    try (FileChannelReader reader = reader(contents, pipeline);
        HOTLeafPage compact = assertInstanceOf(HOTLeafPage.class, reader.readHOTLeafFragment(reference(), CONFIG))) {
      assertFalse(compact.slots().isNative(), "the raw image must not acquire a writable allocator frame");
      assertEquals(compact.getUsedSlotsSize(), compact.slots().byteSize());
      assertEquals(count, compact.size());
      assertTrue(compact.isCompleteDump());
      assertEquals(9, compact.getRevision());
      assertEquals(777, compact.getPageReference(42).getKey());
      final byte[] replacement;
      try (HOTLeafPage other = leaf(1)) {
        replacement = frame(serialize(other));
      }
      contents.set(replacement);
      for (int reuse = 0; reuse < 12; reuse++) {
        reader.readHOTLeafFragment(reference(), CONFIG).close();
      }
      Arrays.fill(wire, (byte) 0xA5);
      for (int row = 0; row < count; row++) {
        assertArrayEquals(value(row), compact.copyStoredValue(compact.findEntry(key(row))));
      }
      assertTrue(compact.findEntry(new byte[] {0}) < 0);
      assertTrue(compact.findEntry(key(999)) < 0);
      try (HOTLeafPage copy = compact.copyForRevision(10)) {
        assertEquals(HOTLeafPage.DEFAULT_SIZE, copy.slots().byteSize());
        assertNotSame(compact.getPageReference(42), copy.getPageReference(42));
        if (count < HOTLeafPage.MAX_ENTRIES) {
          assertTrue(copy.put(key(999), new byte[] {88}));
          assertTrue(compact.findEntry(key(999)) < 0);
        }
      }
      verify(pipeline, never()).decompressScoped(any(MemorySegment.class));
    }
  }

  @Test
  void outerTransformationReleasesItsOwnerAndReturnedSlotsStayIndependent() throws IOException {
    final MemorySegment decoded;
    try (HOTLeafPage original = leaf(17)) {
      decoded = MemorySegment.ofArray(serialize(original));
    }
    final ByteHandler handler = mock(ByteHandler.class);
    final Runnable release = mock(Runnable.class);
    when(handler.supportsMemorySegments()).thenReturn(true);
    when(handler.decompressScoped(any(MemorySegment.class))).thenReturn(
        new ByteHandler.DecompressionResult(decoded, decoded, release, new AtomicBoolean(false)));
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(new byte[] {1, 2, 3})), handler);
        HOTLeafPage compact = assertInstanceOf(HOTLeafPage.class, reader.readHOTLeafFragment(reference(), CONFIG))) {
      verify(release).run();
      decoded.fill((byte) 0);
      for (int row = 0; row < 17; row++) {
        assertArrayEquals(value(row), compact.copyStoredValue(compact.findEntry(key(row))));
      }
    }
  }

  @Test
  void checksumIsVerifiedBeforeRawFragmentDecode() throws IOException {
    final byte[] body;
    try (HOTLeafPage original = leaf(2)) {
      body = serialize(original);
    }
    final ResourceConfiguration checked =
        ResourceConfiguration.newBuilder("checked-compact").verifyChecksumsOnRead(true).build();
    final PageReference reference = reference();
    reference.setHash(PageHasher.computeLong(body, checked.hashAlgorithm));
    body[body.length - 1] ^= 1;
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(body)), new ByteHandlerPipeline())) {
      assertThrows(SirixCorruptionException.class, () -> reader.readHOTLeafFragment(reference, checked));
    }
  }

  @Test
  void corruptTrailerFailsAndReturnsDecompressionOwner() throws IOException {
    final byte[] body;
    try (HOTLeafPage original = leaf(2)) {
      body = serialize(original);
    }
    final MemorySegment decoded = MemorySegment.ofArray(Arrays.copyOf(body, body.length - 1));
    final ByteHandler handler = mock(ByteHandler.class);
    final Runnable release = mock(Runnable.class);
    when(handler.supportsMemorySegments()).thenReturn(true);
    when(handler.decompressScoped(any(MemorySegment.class))).thenReturn(
        new ByteHandler.DecompressionResult(decoded, decoded, release, new AtomicBoolean(false)));
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(new byte[] {1})), handler)) {
      assertThrows(IndexOutOfBoundsException.class, () -> reader.readHOTLeafFragment(reference(), CONFIG));
      verify(release).run();
    }
  }

  @Test
  void chainExtentReadsMatchPerCallReadsAndKeepEveryTruncationFailure() throws IOException {
    final byte[] wire;
    try (HOTLeafPage original = leaf(33)) {
      wire = serialize(original);
    }
    final AtomicReference<byte[]> contents = new AtomicReference<>(frame(wire));
    final FileChannel channel = channel(contents);
    try (FileChannelReader reader = reader(channel, new ByteHandlerPipeline())) {
      final long extent = reader.committedDataExtent();
      assertEquals(contents.get().length, extent);
      verify(channel, times(1)).size();
      try (HOTLeafPage compact =
          assertInstanceOf(HOTLeafPage.class, reader.readHOTLeafFragment(reference(), CONFIG, extent))) {
        assertEquals(33, compact.size());
        assertTrue(compact.isCompleteDump());
        for (int row = 0; row < 33; row++) {
          assertArrayEquals(value(row), compact.copyStoredValue(compact.findEntry(key(row))));
        }
      }
      verify(channel, times(1)).size(); // the bounded read never asked the file for its size
      reader.readHOTLeafFragment(reference(), CONFIG).close();
      verify(channel, times(2)).size(); // the two-argument form still asks once per call
      reader.readHOTLeafFragment(reference(), CONFIG, -1L).close();
      verify(channel, times(3)).size(); // a negative bound reads exactly like the two-argument form
      // A bound below the declared length fails closed before any body allocation.
      final SirixIOException bounded =
          assertThrows(SirixIOException.class, () -> reader.readHOTLeafFragment(reference(), CONFIG, 8L));
      assertTrue(bounded.getMessage().contains("out of bounds"), bounded.getMessage());
      // Truncation after the bound was taken is still detected by the real short read.
      contents.set(Arrays.copyOf(contents.get(), contents.get().length / 2));
      final SirixIOException truncated =
          assertThrows(SirixIOException.class, () -> reader.readHOTLeafFragment(reference(), CONFIG, extent));
      assertTrue(truncated.getMessage().contains("Truncated"), truncated.getMessage());
      assertThrows(SirixIOException.class, () -> reader.readHOTLeafFragment(reference(), CONFIG));
    }
  }

  @Test
  void compactImageOwnsAnExactOffsetDirectoryUntilItIsCopiedOrPromoted() throws Exception {
    final byte[] wire;
    try (HOTLeafPage original = leaf(17)) {
      wire = serialize(original);
    }
    try (FileChannelReader reader = reader(new AtomicReference<>(frame(wire)), new ByteHandlerPipeline());
        HOTLeafPage compact = assertInstanceOf(HOTLeafPage.class, reader.readHOTLeafFragment(reference(), CONFIG))) {
      assertEquals(17, offsetDirectoryLength(compact));
      try (HOTLeafPage copy = compact.copy()) {
        assertEquals(HOTLeafPage.MAX_ENTRIES, offsetDirectoryLength(copy));
        for (int row = 17; row < HOTLeafPage.MAX_ENTRIES; row++) {
          assertTrue(copy.put(key(row), value(row)));
        }
        assertEquals(HOTLeafPage.MAX_ENTRIES, copy.size());
        assertFalse(copy.put(key(HOTLeafPage.MAX_ENTRIES), value(1)));
      }
      assertEquals(17, offsetDirectoryLength(compact));
      // Descending keys insert in front of existing entries: every insert shifts the directory,
      // which the in-place promotion must have widened first.
      for (int row = 60; row >= 17; row--) {
        assertTrue(compact.put(key(row), value(row)));
      }
      assertEquals(HOTLeafPage.MAX_ENTRIES, offsetDirectoryLength(compact));
      assertTrue(compact.slots().isNative());
      assertEquals(61, compact.size());
      for (int row = 0; row <= 60; row++) {
        assertArrayEquals(value(row), compact.copyStoredValue(compact.findEntry(key(row))));
      }
    }
  }

  private static int offsetDirectoryLength(final HOTLeafPage page) throws Exception {
    final Field directory = HOTLeafPage.class.getDeclaredField("slotOffsets");
    directory.setAccessible(true);
    return ((int[]) directory.get(page)).length;
  }

  private static HOTLeafPage leaf(final int count) {
    final HOTLeafPage page = new HOTLeafPage(123, 9, IndexType.PROJECTION);
    for (int row = 0; row < count; row++) {
      assertTrue(page.put(key(row), value(row)));
    }
    page.setPageReference(42, new PageReference().setKey(777));
    page.setCompleteDump(true);
    return page;
  }

  private static byte[] key(final int row) {
    return new byte[] {12, 13, 14, 15, (byte) (row >>> 8), (byte) row};
  }

  private static byte[] value(final int row) {
    final byte[] value = new byte[row % 31];
    Arrays.fill(value, (byte) row);
    return value;
  }

  private static byte[] serialize(final HOTLeafPage page) throws IOException {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(128 * 1024)) {
      new PagePersister().serializePage(CONFIG, out, page, SerializationType.DATA);
      return out.toByteArray();
    }
  }

  private static byte[] frame(final byte[] body) {
    return ByteBuffer.allocate(body.length + 4).order(ByteOrder.LITTLE_ENDIAN).putInt(body.length).put(body).array();
  }

  private static PageReference reference() {
    return new PageReference().setKey(0);
  }

  private static FileChannelReader reader(final AtomicReference<byte[]> contents, final ByteHandler handler)
      throws IOException {
    return reader(channel(contents), handler);
  }

  private static FileChannelReader reader(final FileChannel channel, final ByteHandler handler) {
    return new FileChannelReader(channel, mock(FileChannel.class), handler, SerializationType.DATA, new PagePersister(),
        Caffeine.newBuilder().build());
  }

  private static FileChannel channel(final AtomicReference<byte[]> contents) throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    when(channel.size()).thenAnswer(_ -> (long) contents.get().length);
    when(channel.read(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer target = invocation.getArgument(0);
      final int from = Math.toIntExact(invocation.<Long>getArgument(1));
      final byte[] bytes = contents.get();
      if (from >= bytes.length) {
        return -1;
      }
      final int length = Math.min(target.remaining(), bytes.length - from);
      target.put(bytes, from, length);
      return length;
    });
    return channel;
  }
}
