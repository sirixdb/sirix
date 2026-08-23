package io.sirix.io.filechannel;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.io.IOStorage;
import io.sirix.io.IOTestHelper;
import io.sirix.io.PageHasher;
import io.sirix.io.Reader;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.StorageType;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class FileChannelWriterEmptyPipelineTest {

  @Test
  void identityWireBytesAndHashesSurviveAColdReaderReopen(@TempDir final Path tempDir) throws IOException {
    final ResourceConfiguration config = emptyPipelineConfig(tempDir);
    final byte[] firstPayload = patternedBytes(257, 17);
    final byte[] secondPayload = patternedBytes(733, 91);
    final byte[] firstBody = serialize(config, new OverflowPage(firstPayload));
    final byte[] secondBody = serialize(config, new OverflowPage(secondPayload));
    final PageReference firstReference = new PageReference();
    final PageReference secondReference = new PageReference();

    final FileChannelStorage storage = createStorage(config);
    try {
      try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(); Writer writer = storage.createWriter()) {
        writer.write(config, firstReference, new OverflowPage(firstPayload), appendBuffer);
        writer.write(config, secondReference, new OverflowPage(secondPayload), appendBuffer);
        IOTestHelper.writeRevisionZeroRoot(writer, config, appendBuffer);

        final UberPage uberPage = new UberPage();
        final PageReference uberReference = new PageReference();
        uberReference.setPage(uberPage);
        writer.writeUberPageReference(config, uberReference, uberPage, appendBuffer);
      }
    } finally {
      storage.close();
    }

    final Path dataFile = dataFile(tempDir);
    assertWireFrame(dataFile, firstReference.getKey(), firstBody);
    assertWireFrame(dataFile, secondReference.getKey(), secondBody);
    assertEquals(PageHasher.computeLong(firstBody), firstReference.getHashAsLong());
    assertEquals(PageHasher.computeLong(secondBody), secondReference.getHashAsLong());

    // A distinct storage and reader ensure this is a disk/decompression/deserialization read, not
    // the writer delegate or a swizzled page object.
    final FileChannelStorage reopened = createStorage(config);
    try {
      try (Reader reader = reopened.createReader()) {
        final OverflowPage firstRead = (OverflowPage) reader.read(firstReference, config);
        final OverflowPage secondRead = (OverflowPage) reader.read(secondReference, config);
        assertArrayEquals(firstPayload, firstRead.getDataBytes());
        assertArrayEquals(secondPayload, secondRead.getDataBytes());
      }
    } finally {
      reopened.close();
    }
  }

  @Test
  void appendBufferOwnsTheFirstIdentityRangeBeforeScratchIsReused(@TempDir final Path tempDir) {
    final ResourceConfiguration config = emptyPipelineConfig(tempDir);
    final OverflowPage firstPage = new OverflowPage(patternedBytes(503, 3));
    final OverflowPage secondPage = new OverflowPage(patternedBytes(1_019, 61));
    final byte[] firstBody = serialize(config, firstPage);
    final byte[] secondBody = serialize(config, secondPage);
    final PageReference firstReference = new PageReference();
    final PageReference secondReference = new PageReference();
    final FileChannelStorage storage = createStorage(config);

    try {
      try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(); Writer writer = storage.createWriter()) {
        writer.write(config, firstReference, firstPage, appendBuffer);
        final byte[] bufferAfterFirstWrite = appendBuffer.toByteArray();
        assertBufferedFrame(bufferAfterFirstWrite, 0, firstBody);

        // This serialization overwrites FileChannelWriter's reusable scratch buffer with a different,
        // longer page. The already-appended first frame must remain byte-for-byte unchanged.
        writer.write(config, secondReference, secondPage, appendBuffer);
        final byte[] bufferAfterScratchReuse = appendBuffer.toByteArray();
        assertArrayEquals(bufferAfterFirstWrite, Arrays.copyOf(bufferAfterScratchReuse, bufferAfterFirstWrite.length));
        assertBufferedFrame(bufferAfterScratchReuse,
            Math.toIntExact(secondReference.getKey() - firstReference.getKey()), secondBody);
      }
    } finally {
      storage.close();
    }
  }

  @Test
  void existingKeyValueLeafCacheKeepsItsOwnedIdentity(@TempDir final Path tempDir) {
    final ResourceConfiguration config = emptyPipelineConfig(tempDir);
    final byte[] cachedBytes = patternedBytes(127, 44);
    final MemorySegment cachedSegment = MemorySegment.ofArray(cachedBytes).asReadOnly();
    final KeyValueLeafPage page = mock(KeyValueLeafPage.class);
    when(page.getCompressedSegment()).thenReturn(cachedSegment);
    final PageReference reference = new PageReference();
    final FileChannelStorage storage = createStorage(config);

    try {
      try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(); Writer writer = storage.createWriter()) {
        writer.write(config, reference, page, appendBuffer);
        // A subsequent identity write reuses the writer scratch but must neither replace nor mutate
        // the page-owned KVL cache chosen before the writer's empty-pipeline fast path.
        writer.write(config, new PageReference(), new OverflowPage(patternedBytes(389, 7)), appendBuffer);

        assertSame(cachedSegment, page.getCompressedSegment());
        assertArrayEquals(patternedBytes(127, 44), cachedBytes);
        assertBufferedFrame(appendBuffer.toByteArray(), 0, cachedBytes);
        assertEquals(PageHasher.computeLong(cachedBytes), reference.getHashAsLong());
      }
    } finally {
      storage.close();
    }
  }

  @Test
  void failedSerializationCannotLeakItsScratchPrefixIntoTheNextPage(@TempDir final Path tempDir) {
    final ResourceConfiguration config = emptyPipelineConfig(tempDir);
    final byte[] validPayload = patternedBytes(211, 29);
    final byte[] validBody = serialize(config, new OverflowPage(validPayload));
    final FileChannelStorage storage = createStorage(config);

    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment nativePayload = arena.allocate(32);
      final OverflowPage closedPage = new OverflowPage(nativePayload.asReadOnly(), 0, 32);
      closedPage.close();

      try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(); Writer writer = storage.createWriter()) {
        assertThrows(IllegalStateException.class,
            () -> writer.write(config, new PageReference(), closedPage, appendBuffer));

        final PageReference validReference = new PageReference();
        writer.write(config, validReference, new OverflowPage(validPayload), appendBuffer);
        assertBufferedFrame(appendBuffer.toByteArray(), 0, validBody);
        assertEquals(PageHasher.computeLong(validBody), validReference.getHashAsLong());
      }
    } finally {
      storage.close();
    }
  }

  private static ResourceConfiguration emptyPipelineConfig(final Path resourcePath) {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("empty-pipeline-writer")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .byteHandlerPipeline(new ByteHandlerPipeline())
                                                              .build();
    config.resourcePath = resourcePath;
    return config;
  }

  private static FileChannelStorage createStorage(final ResourceConfiguration config) {
    return new FileChannelStorage(config, Caffeine.newBuilder().buildAsync(), new RevisionIndexHolder());
  }

  private static Path dataFile(final Path resourcePath) {
    return resourcePath.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve(IOStorage.FILENAME);
  }

  private static byte[] serialize(final ResourceConfiguration config, final OverflowPage page) {
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(1_024)) {
      try {
        new PagePersister().serializePage(config, sink, page, SerializationType.DATA);
      } catch (final IOException e) {
        throw new AssertionError(e);
      }
      return sink.toByteArray();
    }
  }

  private static byte[] patternedBytes(final int length, final int seed) {
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (seed + i * 31);
    }
    return bytes;
  }

  private static void assertWireFrame(final Path file, final long offset, final byte[] expectedBody)
      throws IOException {
    final ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + expectedBody.length).order(ByteOrder.LITTLE_ENDIAN);
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      readFully(channel, frame, offset);
    }
    frame.flip();
    assertEquals(expectedBody.length, frame.getInt());
    final byte[] actualBody = new byte[expectedBody.length];
    frame.get(actualBody);
    assertArrayEquals(expectedBody, actualBody);
  }

  private static void assertBufferedFrame(final byte[] bytes, final int offset, final byte[] expectedBody) {
    final ByteBuffer frame = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    frame.position(offset);
    assertEquals(expectedBody.length, frame.getInt());
    final byte[] actualBody = new byte[expectedBody.length];
    frame.get(actualBody);
    assertArrayEquals(expectedBody, actualBody);
  }

  private static void readFully(final FileChannel channel, final ByteBuffer target, final long offset)
      throws IOException {
    while (target.hasRemaining()) {
      final int read = channel.read(target, offset + target.position());
      if (read < 0) {
        throw new EOFException("short test read at file offset " + (offset + target.position()));
      }
      if (read == 0) {
        throw new IOException("test read made no progress at file offset " + (offset + target.position()));
      }
    }
  }
}
