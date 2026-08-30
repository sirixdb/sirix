package io.sirix.io.filechannel;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
import io.sirix.io.bytepipe.DeflateCompressor;
import io.sirix.io.file.StorageProfile;
import io.sirix.index.IndexType;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

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
  void preSerializedKeyValueLeafBypassesThePagePersister(@TempDir final Path tempDir) throws IOException {
    assertPreSerializedKeyValueLeafBypassesThePagePersister(emptyPipelineConfig(tempDir), tempDir);
  }

  @Test
  void preSerializedKeyValueLeafBypassesThePagePersisterForANonEmptyPipeline(@TempDir final Path tempDir)
      throws IOException {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("encoded-pipeline-writer")
                             .storageType(StorageType.FILE_CHANNEL)
                             .byteHandlerPipeline(new ByteHandlerPipeline(new DeflateCompressor()))
                             .build();
    config.resourcePath = tempDir;
    assertPreSerializedKeyValueLeafBypassesThePagePersister(config, tempDir);
  }

  private static void assertPreSerializedKeyValueLeafBypassesThePagePersister(final ResourceConfiguration config,
      final Path tempDir) throws IOException {
    assertPreSerializedKeyValueLeafBypassesThePagePersister(config, tempDir,
        KeyValueLeafPage.UNKNOWN_BYTE_HANDLER_INPUT_LENGTH);
  }

  private static void assertPreSerializedKeyValueLeafBypassesThePagePersister(final ResourceConfiguration config,
      final Path tempDir, final int byteHandlerInputLength) throws IOException {
    final byte[] cachedBytes = patternedBytes(257, 73);
    final MemorySegment cachedSegment = MemorySegment.ofArray(cachedBytes).asReadOnly();
    final KeyValueLeafPage page = mock(KeyValueLeafPage.class);
    when(page.getCompressedSegment()).thenReturn(cachedSegment);
    when(page.getByteHandlerInputLength()).thenReturn(byteHandlerInputLength);
    final PagePersister pagePersister = mock(PagePersister.class);
    final FileChannelReader reader = mock(FileChannelReader.class);
    final Path dataPath = tempDir.resolve("cached-data");
    final Path revisionsPath = tempDir.resolve("cached-revisions");

    try (
        FileChannel data =
            FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannel revisions = FileChannel.open(revisionsPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        FileChannel beacon =
            FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannelWriter writer = new FileChannelWriter(data, revisions, beacon, SerializationType.DATA, pagePersister,
            Caffeine.newBuilder().buildAsync(), new RevisionIndexHolder(), reader, false, false, revisionsPath, 0L, 0L);
        BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer()) {
      final PageReference reference = new PageReference();

      writer.write(config, reference, page, appendBuffer);

      verifyNoInteractions(pagePersister);
      assertBufferedFrame(appendBuffer.toByteArray(), 0, cachedBytes);
      assertEquals(PageHasher.computeLong(cachedBytes), reference.getHashAsLong());
    }
  }

  @Test
  void cachedRawLengthReachesStorageProfileWithoutReserialization(@TempDir final Path tempDir) throws IOException {
    try (MockedStatic<StorageProfile> profile = mockStatic(StorageProfile.class)) {
      profile.when(StorageProfile::isEnabled).thenReturn(true);

      assertPreSerializedKeyValueLeafBypassesThePagePersister(emptyPipelineConfig(tempDir), tempDir, 513);

      profile.verify(() -> StorageProfile.record("KeyValueLeafPage", 513, 257));
    }
  }

  @Test
  void unknownRawLengthUsesIdentityFallbackOnlyForAnEmptyPipeline(@TempDir final Path tempDir) throws IOException {
    try (MockedStatic<StorageProfile> profile = mockStatic(StorageProfile.class)) {
      profile.when(StorageProfile::isEnabled).thenReturn(true);

      assertPreSerializedKeyValueLeafBypassesThePagePersister(emptyPipelineConfig(tempDir), tempDir,
          KeyValueLeafPage.UNKNOWN_BYTE_HANDLER_INPUT_LENGTH);

      profile.verify(() -> StorageProfile.record("KeyValueLeafPage", 257, 257));
    }
  }

  @Test
  void unknownRawLengthIsExplicitForANonEmptyPipeline(@TempDir final Path tempDir) throws IOException {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("unknown-raw-pipeline-writer")
                             .storageType(StorageType.FILE_CHANNEL)
                             .byteHandlerPipeline(new ByteHandlerPipeline(new DeflateCompressor()))
                             .build();
    config.resourcePath = tempDir;
    try (MockedStatic<StorageProfile> profile = mockStatic(StorageProfile.class)) {
      profile.when(StorageProfile::isEnabled).thenReturn(true);

      assertPreSerializedKeyValueLeafBypassesThePagePersister(config, tempDir,
          KeyValueLeafPage.UNKNOWN_BYTE_HANDLER_INPUT_LENGTH);

      profile.verify(() -> StorageProfile.recordUnknownRaw("KeyValueLeafPage", 257));
    }
  }

  @Test
  void legacyBytesCacheStillTraversesThePagePersister(@TempDir final Path tempDir) throws IOException {
    final ResourceConfiguration config = emptyPipelineConfig(tempDir);
    final byte[] cachedBytes = patternedBytes(193, 41);
    final KeyValueLeafPage page = mock(KeyValueLeafPage.class);
    doReturn(Bytes.wrapForWrite(cachedBytes)).when(page).getBytes();
    final PagePersister pagePersister = mock(PagePersister.class);
    final FileChannelReader reader = mock(FileChannelReader.class);
    final Path dataPath = tempDir.resolve("legacy-cache-data");
    final Path revisionsPath = tempDir.resolve("legacy-cache-revisions");

    try (
        FileChannel data =
            FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannel revisions = FileChannel.open(revisionsPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        FileChannel beacon =
            FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannelWriter writer = new FileChannelWriter(data, revisions, beacon, SerializationType.DATA, pagePersister,
            Caffeine.newBuilder().buildAsync(), new RevisionIndexHolder(), reader, false, false, revisionsPath, 0L, 0L);
        BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer()) {
      writer.write(config, new PageReference(), page, appendBuffer);

      verify(pagePersister).serializePage(eq(config), any(), same(page), eq(SerializationType.DATA));
      assertBufferedFrame(appendBuffer.toByteArray(), 0, cachedBytes);
    }
  }

  @Test
  void closedUncachedKeyValueLeafFailsBeforeSerialization(@TempDir final Path tempDir) throws IOException {
    final ResourceConfiguration config = emptyPipelineConfig(tempDir);
    final KeyValueLeafPage page = mock(KeyValueLeafPage.class);
    when(page.isClosed()).thenAnswer(_ -> {
      assertTrue(Thread.holdsLock(page), "closed state must be checked while the page monitor excludes close()");
      return true;
    });
    when(page.getCompressedSegment()).thenReturn(null);
    when(page.getBytes()).thenReturn(null);
    final PagePersister pagePersister = mock(PagePersister.class);
    final FileChannelReader reader = mock(FileChannelReader.class);
    final Path dataPath = tempDir.resolve("closed-uncached-data");
    final Path revisionsPath = tempDir.resolve("closed-uncached-revisions");

    try (
        FileChannel data =
            FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannel revisions = FileChannel.open(revisionsPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        FileChannel beacon =
            FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannelWriter writer = new FileChannelWriter(data, revisions, beacon, SerializationType.DATA, pagePersister,
            Caffeine.newBuilder().buildAsync(), new RevisionIndexHolder(), reader, false, false, revisionsPath, 0L, 0L);
        BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer()) {
      appendBuffer.writeByte((byte) 0x5a);

      assertThrows(IllegalStateException.class, () -> writer.write(config, new PageReference(), page, appendBuffer));
      verifyNoInteractions(pagePersister);
      assertArrayEquals(new byte[] {(byte) 0x5a}, appendBuffer.toByteArray(),
          "a closed uncached page must be rejected before alignment, serialization, hashing, or copying");
    }
  }

  @Test
  void keyValueLeafCannotCloseWhileItsUncachedBodyIsSerializedAndCopied(@TempDir final Path tempDir) throws Exception {
    final ResourceConfiguration config = emptyPipelineConfig(tempDir);
    final KeyValueLeafPage page = new KeyValueLeafPage(71L, 1, IndexType.DOCUMENT, config, false,
        config.recordPersister, new ConcurrentHashMap<>(), null, null, -1);
    final byte[] serializedBody = patternedBytes(257, 83);
    final PagePersister pagePersister = mock(PagePersister.class);
    final FileChannelReader reader = mock(FileChannelReader.class);
    final CountDownLatch closeAttemptStarted = new CountDownLatch(1);
    final CompletableFuture<Void> closeCompleted = new CompletableFuture<>();
    final Thread closer = new Thread(() -> {
      closeAttemptStarted.countDown();
      try {
        page.close();
        closeCompleted.complete(null);
      } catch (final Throwable throwable) {
        closeCompleted.completeExceptionally(throwable);
      }
    }, "kvl-close-during-write");
    doAnswer(invocation -> {
      assertTrue(Thread.holdsLock(page), "the page monitor must cover PagePersister serialization");
      closer.start();
      assertTrue(closeAttemptStarted.await(5, TimeUnit.SECONDS), "the close attempt did not start");
      assertEventuallyBlocked(closer);
      assertFalse(page.isClosed(), "close must not set CLOSED_BIT while serialization owns the page monitor");
      final BytesOut<?> sink = invocation.getArgument(1);
      sink.write(serializedBody);
      return null;
    }).when(pagePersister).serializePage(eq(config), any(), same(page), eq(SerializationType.DATA));
    final Path dataPath = tempDir.resolve("close-race-data");
    final Path revisionsPath = tempDir.resolve("close-race-revisions");

    try (
        FileChannel data =
            FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannel revisions = FileChannel.open(revisionsPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        FileChannel beacon =
            FileChannel.open(dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannelWriter writer = new FileChannelWriter(data, revisions, beacon, SerializationType.DATA, pagePersister,
            Caffeine.newBuilder().buildAsync(), new RevisionIndexHolder(), reader, false, false, revisionsPath, 0L, 0L);
        BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer()) {
      appendBuffer.writeByte((byte) 0x5a);
      final PageReference reference = new PageReference();

      try {
        writer.write(config, reference, page, appendBuffer);
        closeCompleted.get(5, TimeUnit.SECONDS);
        closer.join(5_000L);

        assertFalse(closer.isAlive(), "close must complete after the writer releases the page monitor");
        assertTrue(page.isClosed());
        final byte[] bufferedFrame = appendBuffer.toByteArray();
        assertArrayEquals(serializedBody,
            Arrays.copyOfRange(bufferedFrame, bufferedFrame.length - serializedBody.length, bufferedFrame.length));
        assertEquals(PageHasher.computeLong(serializedBody), reference.getHashAsLong());
        verify(pagePersister).serializePage(eq(config), any(), same(page), eq(SerializationType.DATA));
      } finally {
        if (closer.getState() != Thread.State.NEW) {
          closer.join(5_000L);
        }
        if (!page.isClosed()) {
          page.close();
        }
      }
    }
  }

  @Test
  void closedPreSerializedKeyValueLeafFailsBeforeMutatingTheAppendBuffer(@TempDir final Path tempDir) {
    final ResourceConfiguration config = emptyPipelineConfig(tempDir);
    final MemorySegment closedSegment;
    try (Arena arena = Arena.ofConfined()) {
      closedSegment = arena.allocate(32).asReadOnly();
    }
    final KeyValueLeafPage page = mock(KeyValueLeafPage.class);
    when(page.getCompressedSegment()).thenReturn(closedSegment);
    final FileChannelStorage storage = createStorage(config);

    try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(); Writer writer = storage.createWriter()) {
      appendBuffer.writeByte((byte) 0x5a);

      assertThrows(IllegalStateException.class, () -> writer.write(config, new PageReference(), page, appendBuffer));
      assertEquals(1L, appendBuffer.writePosition(), "a rejected cache must not leave alignment padding behind");
    } finally {
      storage.close();
    }
  }

  @Test
  void changedKeyValueLeafCacheIsRejectedBeforeHashingOrCopying(@TempDir final Path tempDir) {
    final ResourceConfiguration config = emptyPipelineConfig(tempDir);
    final KeyValueLeafPage page = mock(KeyValueLeafPage.class);
    final AtomicInteger cacheReads = new AtomicInteger();
    final FileChannelStorage storage = createStorage(config);

    try (Arena arena = Arena.ofShared()) {
      final MemorySegment oldFrame = arena.allocate(64);
      oldFrame.fill((byte) 0x3c);
      final MemorySegment staleCache = oldFrame.asSlice(0, 32).asReadOnly();
      final MemorySegment replacementCache = arena.allocate(32).asReadOnly();
      when(page.getCompressedSegment()).thenAnswer(_ -> {
        if (cacheReads.getAndIncrement() == 0) {
          return staleCache;
        }
        assertTrue(Thread.holdsLock(page), "the cache identity must be revalidated under the page monitor");
        return replacementCache;
      });
      when(page.getSlottedPage()).thenReturn(oldFrame);

      try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(); Writer writer = storage.createWriter()) {
        appendBuffer.writeByte((byte) 0x5a);

        assertThrows(IllegalStateException.class, () -> writer.write(config, new PageReference(), page, appendBuffer));
        assertTrue(staleCache.scope().isAlive(), "the stale address remains live and could already have been reused");
        assertEquals(2, cacheReads.get(), "the writer must acquire and then revalidate the cache exactly once");
        verify(page, never()).getSlottedPage();
        assertArrayEquals(new byte[] {(byte) 0x5a}, appendBuffer.toByteArray(),
            "a stale cache must be rejected before its frame header or payload is appended");
      }
    } finally {
      storage.close();
    }
  }

  @Test
  void closedKeyValueLeafWithPublishedCacheIsRejectedBeforeHashingOrCopying(@TempDir final Path tempDir) {
    final ResourceConfiguration config = emptyPipelineConfig(tempDir);
    final KeyValueLeafPage page = mock(KeyValueLeafPage.class);
    final FileChannelStorage storage = createStorage(config);

    try (Arena arena = Arena.ofShared()) {
      final MemorySegment oldFrame = arena.allocate(64);
      oldFrame.fill((byte) 0x6d);
      final MemorySegment stillPublishedCache = oldFrame.asSlice(0, 32).asReadOnly();
      when(page.getCompressedSegment()).thenReturn(stillPublishedCache);
      when(page.isClosed()).thenAnswer(_ -> {
        assertTrue(Thread.holdsLock(page), "closed state must be checked under the page monitor");
        return true;
      });
      when(page.getSlottedPage()).thenReturn(oldFrame);

      try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(); Writer writer = storage.createWriter()) {
        appendBuffer.writeByte((byte) 0x5a);

        assertThrows(IllegalStateException.class, () -> writer.write(config, new PageReference(), page, appendBuffer));
        assertTrue(stillPublishedCache.scope().isAlive(),
            "close may set CLOSED_BIT before clearing or releasing the still-accessible cache");
        verify(page).isClosed();
        verify(page, never()).getSlottedPage();
        assertArrayEquals(new byte[] {(byte) 0x5a}, appendBuffer.toByteArray(),
            "a closed page must be rejected before alignment, frame header, or payload bytes are appended");
      }
    } finally {
      storage.close();
    }
  }

  @Test
  void wrongThreadPreSerializedKeyValueLeafFailsBeforeMutatingTheAppendBuffer(@TempDir final Path tempDir)
      throws Exception {
    final CompletableFuture<MemorySegment> publishedSegment = new CompletableFuture<>();
    final CountDownLatch releaseOwner = new CountDownLatch(1);
    final Thread owner = new Thread(() -> {
      try (Arena arena = Arena.ofConfined()) {
        publishedSegment.complete(arena.allocate(32).asReadOnly());
        releaseOwner.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        publishedSegment.completeExceptionally(e);
      } catch (final Throwable throwable) {
        publishedSegment.completeExceptionally(throwable);
      }
    }, "confined-page-cache-owner");
    owner.start();

    try {
      final ResourceConfiguration config = emptyPipelineConfig(tempDir);
      final KeyValueLeafPage page = mock(KeyValueLeafPage.class);
      when(page.getCompressedSegment()).thenReturn(publishedSegment.get(5, TimeUnit.SECONDS));
      final FileChannelStorage storage = createStorage(config);
      try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(); Writer writer = storage.createWriter()) {
        appendBuffer.writeByte((byte) 0x5a);

        assertThrows(WrongThreadException.class, () -> writer.write(config, new PageReference(), page, appendBuffer));
        assertEquals(1L, appendBuffer.writePosition(), "a rejected cache must not leave alignment padding behind");
      } finally {
        storage.close();
      }
    } finally {
      releaseOwner.countDown();
      owner.join();
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

  private static void assertEventuallyBlocked(final Thread thread) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (thread.isAlive() && thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
      Thread.sleep(1L);
    }
    assertEquals(Thread.State.BLOCKED, thread.getState(),
        "close must block on the KVL monitor until serialization, hashing, and copying finish");
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
