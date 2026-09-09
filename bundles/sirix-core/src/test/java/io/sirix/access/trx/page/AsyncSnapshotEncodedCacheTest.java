/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.BufferManager;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.io.IOTestHelper;
import io.sirix.io.Reader;
import io.sirix.io.SerializationBufferPool;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.DeflateCompressor;
import io.sirix.io.filechannel.FileChannelStorage;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.PooledBytesOut;
import io.sirix.node.PooledGrowingSegment;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageConstants;
import io.sirix.page.PageKind;
import io.sirix.page.PageLayout;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.pax.RegionTable;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

/** End-to-end ownership and wire invariants for the disposable async-snapshot encoded cache. */
final class AsyncSnapshotEncodedCacheTest {

  private static final LongHashFunction HASH_FUNCTION = LongHashFunction.xx3();

  private static MemorySegmentAllocator allocator;

  @BeforeAll
  static void initializeAllocator() {
    allocator = Allocators.getInstance();
    allocator.init(8L * 1024 * 1024 * 1024);
  }

  @Test
  @DisplayName("Async disposable copy appends through FileChannel, cold-reads, and recycles its frame")
  void realEncodeFileAppendColdReadAndFrameReuse(@TempDir final Path tempDir) {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-test").byteHandlerPipeline(new ByteHandlerPipeline()).build();
    config.resourcePath = tempDir.resolve("resource");

    final byte[] first = {1, 2, 3, 4};
    final byte[] second = {9, 8, 7};
    final byte[] third = new byte[257];
    for (int i = 0; i < third.length; i++) {
      third[i] = (byte) (i * 31 + 7);
    }

    final KeyValueLeafPage original = new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, null, null, false);
    KeyValueLeafPage serializationCopy = null;
    KeyValueLeafPage coldPage = null;
    final FileChannelStorage storage = new FileChannelStorage(config, Caffeine.newBuilder().buildAsync());
    try {
      original.setSlot(first, 1);
      original.setSlot(second, 10);
      original.setSlot(third, 100);

      // This is the production publication shape: a pool worker deep-copies and fully encodes the
      // page, copies the borrowed identity result into the frame before releasing the pool buffer,
      // and the append owner observes the result through CompletableFuture.join().
      serializationCopy = CompletableFuture.supplyAsync(() -> encodeDisposableCopy(config, original)).join();
      final MemorySegment encoded = serializationCopy.getCompressedSegment();
      final MemorySegment frame = serializationCopy.getSlottedPage();
      assertNotNull(encoded);
      assertNotNull(frame);
      assertTrue(encoded.isNative());
      assertTrue(encoded.isReadOnly());
      assertEquals(frame.address(), encoded.address());
      assertTrue(encoded.byteSize() < frame.byteSize(), "test page must exercise an exact-length frame view");
      assertEquals(Math.toIntExact(encoded.byteSize()), serializationCopy.getByteHandlerInputLength(),
          "the identity pipeline's persisted and pre-handler lengths are equal");

      final PageReference writtenReference = new PageReference();
      try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
          Writer writer = storage.createWriter()) {
        writer.write(config, writtenReference, serializationCopy, appendBuffer);
        writer.flushBufferedWrites(appendBuffer);
        // A direct page append intentionally bypasses the normal commit root. Install a valid first
        // revision root and beacon so opening a genuinely fresh FileChannelReader performs (and
        // passes) superblock validation instead of treating the file as an interrupted first commit.
        IOTestHelper.writeRevisionZeroRoot(writer, config, appendBuffer);
        writer.writeUberPageReference(config, new PageReference(), new UberPage(), appendBuffer);
      }

      // The write call has synchronously hashed and copied the exact cache view. Closing the copy is
      // therefore the ownership hand-off point. FrameSlotAllocator is LIFO, so the immediate same-size
      // allocation must receive precisely the frame just returned by close().
      final long disposableFrameAddress = frame.address();
      final long disposableFrameSize = frame.byteSize();
      serializationCopy.close();
      serializationCopy = null;
      final MemorySegment reusedFrame = allocator.allocate(disposableFrameSize);
      try {
        assertEquals(disposableFrameAddress, reusedFrame.address(),
            "closing the disposable copy must return its full frame to the allocator");
      } finally {
        allocator.release(reusedFrame);
      }

      // A fresh reference and reader prevent an in-memory swizzle from satisfying the assertion. If
      // FileChannelWriter re-read the overwritten logical slotted headers instead of consuming the
      // cache, or retained the alias past write(), this cold decode fails or returns corrupt slots.
      final PageReference coldReference = new PageReference(writtenReference);
      try (Reader reader = storage.createReader()) {
        coldPage = (KeyValueLeafPage) reader.read(coldReference, config);
      }
      assertEquals(original.getPageKey(), coldPage.getPageKey());
      assertEquals(original.getRevision(), coldPage.getRevision());
      assertArrayEquals(first, coldPage.getSlotAsByteArray(1));
      assertArrayEquals(second, coldPage.getSlotAsByteArray(10));
      assertArrayEquals(third, coldPage.getSlotAsByteArray(100));
    } finally {
      if (serializationCopy != null) {
        serializationCopy.close();
      }
      if (coldPage != null) {
        coldPage.close();
      }
      original.close();
      storage.close();
    }
  }

  @Test
  @DisplayName("An identity encoding larger than the disposable frame requests promotion without overwriting it")
  void directIdentityCapacityFailurePreservesCacheAndFrame() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-fallback").byteHandlerPipeline(new ByteHandlerPipeline())
                                                                 .build();
    final KeyValueLeafPage page = new KeyValueLeafPage(2L, IndexType.DOCUMENT, config, 1, null, null, false);
    try {
      final MemorySegment frame = page.getSlottedPage();
      int remainingHeapBytes = Math.toIntExact(frame.byteSize()) - PageLayout.HEAP_START;
      int random = 0x6D2B79F5;
      int slot = 0;
      while (remainingHeapBytes > 0) {
        final byte[] inlineRecord = new byte[Math.min(remainingHeapBytes, PageConstants.MAX_RECORD_SIZE)];
        for (int i = 0; i < inlineRecord.length; i++) {
          random ^= random << 13;
          random ^= random >>> 17;
          random ^= random << 5;
          inlineRecord[i] = (byte) random;
        }
        page.setSlot(inlineRecord, slot++);
        remainingHeapBytes -= inlineRecord.length;
      }
      assertEquals(frame.address(), page.getSlottedPage().address(),
          "legal inline records must fill the original frame without growing it");
      final long firstNodeKey = page.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT;
      final int firstOverflowSlot = slot;
      for (int referencedSlot = firstOverflowSlot; referencedSlot < PageLayout.SLOT_COUNT; referencedSlot++) {
        page.getReferencesMap().put(firstNodeKey + referencedSlot, new PageReference().setKey(referencedSlot + 1L));
      }

      // A full row frame can legitimately acquire cold side images when later values have no inline
      // capacity. Keep inline slots and overflow references disjoint, and add enough valid complete
      // images to make the persisted identity page exceed the frame without ever manufacturing an
      // illegal >512-byte record.
      final MemorySegment sideScratch = MemorySegment.ofArray(new byte[PageConstants.MAX_RECORD_SIZE]);
      final int sideSlotLimit = Math.min(firstOverflowSlot + 64, PageLayout.SLOT_COUNT);
      for (int sideSlot = firstOverflowSlot; sideSlot < sideSlotLimit; sideSlot++) {
        final ObjectNamedNumberNode sideNode = new ObjectNamedNumberNode(firstNodeKey + sideSlot,
            Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 17, -1L, 0, 0, 0L, 42L, HASH_FUNCTION, (byte[]) null);
        final int sideImageLength = sideNode.serializeToHeap(sideScratch, 0L);
        assertTrue(sideImageLength > 0 && sideImageLength <= PageConstants.MAX_RECORD_SIZE);
        final MemorySegment sideImage = sideScratch.asSlice(0L, sideImageLength);
        final long prepareToken =
            page.prepareSideSlot(NodeKind.OBJECT_NAMED_NUMBER.getId(), sideImage, sideImageLength);
        page.publishSideSlot(sideSlot, prepareToken);
      }
      final byte[] framePrefix = frame.asSlice(0, PageLayout.HEADER_SIZE).toArray(ValueLayout.JAVA_BYTE);
      assertNull(page.getCompressedSegment(), "test must enter the serializer instead of hitting a stale cache");

      assertFalse(NodeStorageEngineWriter.serializeDisposableSnapshotKeyValuePage(config, page),
          "an over-capacity identity encoding must promote the original instead of retaining heap fallback");
      assertNull(page.getCompressedSegment(), "the borrowed oversized identity result must never be published");
      assertArrayEquals(framePrefix, frame.asSlice(0, PageLayout.HEADER_SIZE).toArray(ValueLayout.JAVA_BYTE),
          "capacity fallback must not partially overwrite the logical frame");
    } finally {
      page.close();
    }
  }

  @Test
  @DisplayName("Reusing and poisoning the released serialization buffer cannot mutate the frame cache")
  void pooledSourceReuseCannotMutatePublishedFrameCache() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-pool-reuse").byteHandlerPipeline(new ByteHandlerPipeline())
                                                                   .build();
    final KeyValueLeafPage original = new KeyValueLeafPage(3L, IndexType.DOCUMENT, config, 1, null, null, false);
    final SerializationBufferPool pool = SerializationBufferPool.INSTANCE;
    // A stripe holds at most two buffers. Holding both makes the production serializer allocate a
    // temporary buffer; release inserts that exact buffer into the empty stripe, so the subsequent
    // acquire reuses the source whose lifetime the test is pinning down.
    final PooledGrowingSegment heldFirst = pool.acquire();
    final PooledGrowingSegment heldSecond = pool.acquire();
    PooledGrowingSegment reusedSource = null;
    KeyValueLeafPage copy = null;
    try {
      original.setSlot(new byte[] {11, 22, 33, 44, 55}, 9);
      copy = original.deepCopy();
      assertTrue(NodeStorageEngineWriter.serializeDisposableSnapshotKeyValuePage(config, copy));

      final MemorySegment encoded = copy.getCompressedSegment();
      final byte[] expected = encoded.toArray(ValueLayout.JAVA_BYTE);
      reusedSource = pool.acquire();
      reusedSource.getCurrentSegment().fill((byte) 0xA5);

      assertArrayEquals(expected, encoded.toArray(ValueLayout.JAVA_BYTE),
          "the published cache must own the frame, never alias the resettable pooled source");
      assertEquals(copy.getSlottedPage().address(), encoded.address());
    } finally {
      if (reusedSource != null) {
        pool.release(reusedSource);
      }
      pool.release(heldFirst);
      pool.release(heldSecond);
      if (copy != null) {
        copy.close();
      }
      original.close();
    }
  }

  @Test
  @DisplayName("Disposable numeric-region serialization has bounded direct memory and publishes no table")
  void disposableNumericRegionsAreReleasedBeforePublication() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-confined-regions").byteHandlerPipeline(new ByteHandlerPipeline())
                                                                         .build();
    final KeyValueLeafPage original = new KeyValueLeafPage(8L, IndexType.DOCUMENT, config, 1, null, null, false);
    try {
      writeObjectNumber(original, 0, 17, 42L);

      // Warm every serializer-local scratch before taking the direct-buffer baseline. The loop then
      // measures only per-page RegionTable ownership: frame slots and pooled sinks use the bounded
      // Sirix allocator, while the confined region arena is visible in the JVM direct-buffer pool.
      serializeAndCloseNumericCopy(config, original);
      final long directCapacityBefore = directBufferCapacity();

      for (int i = 0; i < 64; i++) {
        serializeAndCloseNumericCopy(config, original);
      }

      final long directCapacityAfter = directBufferCapacity();
      assertTrue(directCapacityAfter <= directCapacityBefore, "disposable RegionTables retained "
          + (directCapacityAfter - directCapacityBefore) + " direct bytes after their serializer calls returned");
    } finally {
      original.close();
    }
  }

  @Test
  @DisplayName("A downstream handler failure still closes the disposable numeric-region arena")
  void disposableNumericRegionClosesOnSerializationFailure() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-confined-region-failure").byteHandlerPipeline(
            new ByteHandlerPipeline(new FailingMemorySegmentHandler())).build();
    final KeyValueLeafPage original = new KeyValueLeafPage(9L, IndexType.DOCUMENT, config, 1, null, null, false);
    try {
      writeObjectNumber(original, 0, 23, 99L);

      serializeFailingCopy(config, original); // Warm serializer-local scratch before the baseline.
      final long directCapacityBefore = directBufferCapacity();

      for (int i = 0; i < 32; i++) {
        serializeFailingCopy(config, original);
      }

      final long directCapacityAfter = directBufferCapacity();
      assertTrue(directCapacityAfter <= directCapacityBefore, "failed disposable serializations retained "
          + (directCapacityAfter - directCapacityBefore) + " direct bytes after unwinding");
    } finally {
      original.close();
    }
  }

  @Test
  @DisplayName("An Error after PAX encoding still closes the disposable numeric-region arena")
  void disposableNumericRegionClosesOnError() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-confined-region-error").byteHandlerPipeline(
            new ByteHandlerPipeline(new ErrorMemorySegmentHandler())).build();
    final KeyValueLeafPage original = new KeyValueLeafPage(10L, IndexType.DOCUMENT, config, 1, null, null, false);
    try {
      writeObjectNumber(original, 0, 31, 101L);

      serializeErrorCopy(config, original); // Warm serializer-local scratch before the baseline.
      final long directCapacityBefore = directBufferCapacity();

      for (int i = 0; i < 16; i++) {
        serializeErrorCopy(config, original);
      }

      final long directCapacityAfter = directBufferCapacity();
      assertTrue(directCapacityAfter <= directCapacityBefore,
          "Error unwinds retained " + (directCapacityAfter - directCapacityBefore) + " disposable region bytes");
    } finally {
      original.close();
    }
  }

  @Test
  @DisplayName("Disposable and ordinary numeric pages have identical wire bytes")
  void disposableNumericWireMatchesOrdinarySerialization() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-region-wire-equality")
                                                                             .byteHandlerPipeline(
                                                                                 new ByteHandlerPipeline())
                                                                             .build();
    final KeyValueLeafPage original = new KeyValueLeafPage(11L, IndexType.DOCUMENT, config, 1, null, null, false);
    KeyValueLeafPage ordinaryCopy = null;
    KeyValueLeafPage disposableCopy = null;
    try {
      writeObjectNumber(original, 0, 37, 202L);
      ordinaryCopy = original.deepCopy();
      disposableCopy = original.deepCopy();

      final byte[] ordinaryWire;
      final byte[] disposableWire;
      try (BytesOut<?> ordinarySink = Bytes.elasticHeapByteBuffer();
          BytesOut<?> disposableSink = Bytes.elasticHeapByteBuffer()) {
        PageKind.KEYVALUELEAFPAGE.serializePage(config, ordinarySink, ordinaryCopy, SerializationType.DATA);
        PageKind.KEYVALUELEAFPAGE.serializeDisposablePage(config, disposableSink, disposableCopy,
            SerializationType.DATA);
        ordinaryWire = ordinarySink.toByteArray();
        disposableWire = disposableSink.toByteArray();
      }

      assertNotNull(ordinaryCopy.getRegionTable(), "ordinary serialization must attach its PAX table");
      assertNotNull(ordinaryCopy.getRegionTable().payload(RegionTable.KIND_NUMBER));
      assertNull(disposableCopy.getRegionTable(), "disposable serialization must not publish its PAX table");
      assertArrayEquals(ordinaryWire, disposableWire,
          "changing only the PAX arena lifetime must not change the persisted page");
    } finally {
      if (disposableCopy != null) {
        disposableCopy.close();
      }
      if (ordinaryCopy != null) {
        ordinaryCopy.close();
      }
      original.close();
    }
  }

  @Test
  @DisplayName("An unresolved overflow reference requests promotion and leaves the logical frame readable")
  void unresolvedOverflowReferenceRequestsPromotion() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-unresolved-overflow")
                                                                            .byteHandlerPipeline(
                                                                                new ByteHandlerPipeline())
                                                                            .build();
    final KeyValueLeafPage page = new KeyValueLeafPage(4L, IndexType.DOCUMENT, config, 1, null, null, false);
    try {
      writeObjectNumber(page, 17, 29, 123L);
      final byte[] slotBefore = page.getSlotAsByteArray(17);

      // Warm persistent serializer scratch so the direct-buffer delta below isolates the confined
      // PAX table built on the false-return path.
      serializeAndCloseNumericCopy(config, page);
      final PageReference unresolved = new PageReference();
      assertEquals(Constants.NULL_ID_LONG, unresolved.getKey());
      final long nodeKey = (page.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT) + 17L;
      page.getReferencesMap().put(nodeKey, unresolved);
      final long directCapacityBefore = directBufferCapacity();

      assertFalse(NodeStorageEngineWriter.serializeDisposableSnapshotKeyValuePage(config, page));
      final long directCapacityAfter = directBufferCapacity();
      assertNull(page.getCompressedSegment(), "no bytes containing a NULL overflow key may be published");
      assertNull(page.getRegionTable(), "promotion must not strand the disposable PAX table on the page");
      assertTrue(directCapacityAfter <= directCapacityBefore,
          "promotion retained " + (directCapacityAfter - directCapacityBefore) + " disposable region bytes");
      assertArrayEquals(slotBefore, page.getSlotAsByteArray(17),
          "declining the snapshot must not overwrite the original logical frame");
    } finally {
      page.close();
    }
  }

  @Test
  @DisplayName("A non-empty handler pipeline keeps its ordinary owned compressed cache")
  void nonEmptyPipelineIsNotBypassed() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-nonempty-pipeline")
                                                                          .byteHandlerPipeline(new ByteHandlerPipeline(
                                                                              new PrefixMemorySegmentHandler()))
                                                                          .build();
    final KeyValueLeafPage original = new KeyValueLeafPage(5L, IndexType.DOCUMENT, config, 1, null, null, false);
    KeyValueLeafPage expectedCopy = null;
    KeyValueLeafPage serializationCopy = null;
    KeyValueLeafPage expectedDecoded = null;
    KeyValueLeafPage actualDecoded = null;
    try {
      original.setSlot(new byte[] {3, 1, 4, 1, 5, 9}, 23);
      expectedCopy = original.deepCopy();
      final byte[] expectedWire;
      try (BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer()) {
        PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, expectedCopy, SerializationType.DATA);
        expectedWire = sink.toByteArray();
      }

      serializationCopy = original.deepCopy();
      assertTrue(NodeStorageEngineWriter.serializeDisposableSnapshotKeyValuePage(config, serializationCopy));
      final MemorySegment encoded = serializationCopy.getCompressedSegment();
      assertNotNull(encoded);
      assertNull(serializationCopy.getBytes());
      assertEquals(serializationCopy.getSlottedPage().address(), encoded.address());
      assertEquals(PrefixMemorySegmentHandler.MARKER, encoded.get(ValueLayout.JAVA_BYTE, 0L));
      assertEquals(Math.toIntExact(encoded.byteSize()) - 1, serializationCopy.getByteHandlerInputLength(),
          "relocating an owned cache must preserve the pre-handler length");

      // The inner page-body codec uses a per-thread sticky-winner election, so two equivalent
      // serializations may legally choose different self-describing representations (for example,
      // ZeroRun followed by ByteRun). Compare the decoded page semantics instead of requiring the
      // raw inner wire to be byte-identical; the marker above still proves the non-empty outer
      // handler ran and was retained in the relocated cache.
      final BytesIn<?> expectedSource = Bytes.wrapForRead(expectedWire);
      expectedSource.readByte();
      expectedDecoded =
          (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, expectedSource, SerializationType.DATA);
      final BytesIn<?> actualSource = Bytes.wrapForRead(encoded.asSlice(1L));
      actualSource.readByte();
      actualDecoded =
          (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, actualSource, SerializationType.DATA);
      assertEquals(expectedDecoded.getPageKey(), actualDecoded.getPageKey());
      assertEquals(expectedDecoded.getRevision(), actualDecoded.getRevision());
      assertEquals(expectedDecoded.getIndexType(), actualDecoded.getIndexType());
      assertArrayEquals(expectedDecoded.getSlotAsByteArray(23), actualDecoded.getSlotAsByteArray(23),
          "the configured non-empty handler must retain a semantically identical page payload");
    } finally {
      if (actualDecoded != null) {
        actualDecoded.close();
      }
      if (expectedDecoded != null) {
        expectedDecoded.close();
      }
      if (serializationCopy != null) {
        serializationCopy.close();
      }
      if (expectedCopy != null) {
        expectedCopy.close();
      }
      original.close();
    }
  }

  @Test
  @DisplayName("The default pooled writer policy still retains an owned identity copy")
  void defaultPooledWriterPolicyRemainsOwned() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-default-policy").byteHandlerPipeline(new ByteHandlerPipeline())
                                                                       .build();
    final KeyValueLeafPage page = new KeyValueLeafPage(6L, IndexType.DOCUMENT, config, 1, null, null, false);
    final MemorySegment backing = MemorySegment.ofArray(new byte[128 * 1024]);
    final PooledGrowingSegment pooledSegment = new PooledGrowingSegment(backing);
    try {
      page.setSlot(new byte[] {2, 7, 1, 8, 2, 8}, 31);
      final PooledBytesOut sink = new PooledBytesOut(pooledSegment);
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      final MemorySegment cache = page.getCompressedSegment();
      final byte[] expected = cache.toArray(ValueLayout.JAVA_BYTE);
      assertEquals(Math.toIntExact(cache.byteSize()), page.getByteHandlerInputLength());

      backing.fill((byte) 0xCC);

      assertArrayEquals(expected, cache.toArray(ValueLayout.JAVA_BYTE),
          "the default policy must preserve the global owned-copy contract");
    } finally {
      page.close();
    }
  }

  @Test
  @DisplayName("A stream-only pipeline publishes the exact pre-handler cache length")
  void streamPipelinePublishesExactInputLength() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-stream-metadata")
                                                                        .byteHandlerPipeline(new ByteHandlerPipeline(
                                                                            new DeflateCompressor()))
                                                                        .build();
    final KeyValueLeafPage page = new KeyValueLeafPage(12L, IndexType.DOCUMENT, config, 1, null, null, false);
    try (BytesOut<?> sink = Bytes.elasticHeapByteBuffer()) {
      page.setSlot(new byte[] {1, 1, 2, 3, 5, 8, 13, 21}, 7);

      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);

      assertNull(page.getCompressedSegment());
      assertNotNull(page.getBytes());
      assertEquals(Math.toIntExact(sink.writePosition()), page.getByteHandlerInputLength(),
          "the legacy cache metadata must describe the bytes before Deflate");
    } finally {
      page.close();
    }
  }

  @Test
  @DisplayName("A joined serializer's promotion outcome is visible to allocation-free window accounting")
  void joinedPromotionOutcomeIsVisibleToWindowAccounting() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("async-cache-promotion-status").build();
    final KeyValueLeafPage page = new KeyValueLeafPage(7L, IndexType.DOCUMENT, config, 1, null, null, false);
    final TransactionIntentLog log = new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
    try {
      final PageReference reference = new PageReference();
      log.put(reference, PageContainer.getInstance(page, page));
      assertEquals(1, log.snapshot());
      assertEquals(Constants.NULL_ID_LONG, log.getSnapshotDiskOffset(0));

      CompletableFuture.runAsync(() -> log.setSnapshotDiskOffset(0, TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL))
                       .join();

      assertEquals(TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL, log.getSnapshotDiskOffset(0),
          "CompletableFuture.join must publish the serializer's disjoint status-slot write");
    } finally {
      log.clear();
    }
  }

  @Test
  @DisplayName("A declined snapshot never hijacks a forwarded successor and pins only the live page")
  void declinedSnapshotPinsOnlyAuthoritativeSuccessor() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-forwarded-promotion")
                                                                            .byteHandlerPipeline(
                                                                                new ByteHandlerPipeline())
                                                                            .build();
    final KeyValueLeafPage frozenPage = new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, null, null, false);
    final KeyValueLeafPage successorPage = new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, null, null, false);
    final KeyValueLeafPage replacementPage = new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, null, null, false);
    final TransactionIntentLog log = new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
    try {
      final PageReference frozenReference = new PageReference();
      final PageContainer frozenContainer = PageContainer.getInstance(frozenPage, frozenPage);
      log.put(frozenReference, frozenContainer);
      assertEquals(1, log.snapshot());

      // A copied indirect-page child has independent raw generation/key fields but shares the
      // captured reachability handle. Rebinding the copy forwards that handle without changing
      // frozenReference's raw fields — the exact state that used to make cleanup resurrect page 0.
      final PageReference liveReference = new PageReference(frozenReference);
      final PageContainer successorContainer = PageContainer.getInstance(successorPage, successorPage);
      log.put(liveReference, successorContainer);
      log.setSnapshotDiskOffset(0, TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL);
      log.cleanupSnapshot();

      assertSame(successorContainer, log.get(liveReference),
          "the stale declined image must not append a forwarding edge back over its live successor");
      assertSame(successorContainer, log.get(frozenReference),
          "the captured reference must follow the already-forwarded handle to the live successor");
      assertEquals(1, log.liveEntryCount());
      assertEquals(0, log.pinnedSize(), "a superseded declined page must be closed, never pinned");

      // Once the authoritative successor itself is declined, it becomes a stable pinned entry.
      assertEquals(1, log.snapshot());
      log.setSnapshotDiskOffset(0, TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL);
      log.cleanupSnapshot();
      assertEquals(TransactionIntentLog.PINNED_GENERATION, liveReference.getActiveTilGeneration());
      assertEquals(0, log.liveEntryCount());
      assertEquals(1, log.pinnedSize());
      assertSame(successorContainer, log.get(liveReference));

      // Later mutation/replacement must update that same stable slot and every older reachable copy.
      final PageContainer replacementContainer = PageContainer.getInstance(replacementPage, replacementPage);
      log.put(liveReference, replacementContainer);
      assertEquals(TransactionIntentLog.PINNED_GENERATION, liveReference.getActiveTilGeneration());
      assertEquals(1, log.pinnedSize());
      assertSame(replacementContainer, log.get(liveReference));
      assertSame(replacementContainer, log.get(frozenReference));

      final TransactionIntentLog.PinnedSpillBatch spillBatch = new TransactionIntentLog.PinnedSpillBatch(1);
      assertEquals(0, log.capturePinnedSpillCandidates(1, spillBatch),
          "an unresolved-overflow KVL must never enter the structural prewrite path");
      assertEquals(0, log.snapshot(), "a pinned KVL must not rotate through another async snapshot");
      log.cleanupSnapshot();
      assertSame(replacementContainer, log.get(liveReference));
    } finally {
      log.close();
    }
  }

  @Test
  @DisplayName("A declined snapshot never hijacks a handleless manually copied successor")
  void declinedSnapshotDoesNotHijackHandlelessSuccessor() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("async-cache-legacy-promotion").byteHandlerPipeline(new ByteHandlerPipeline())
                                                                         .build();
    final KeyValueLeafPage frozenPage = new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, null, null, false);
    final KeyValueLeafPage successorPage = new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, null, null, false);
    final TransactionIntentLog log = new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
    try {
      final PageReference frozenReference = new PageReference();
      log.put(frozenReference, PageContainer.getInstance(frozenPage, frozenPage));
      assertEquals(1, log.snapshot());

      // Compatibility path: copy only the historical raw identity fields and deliberately omit
      // shareTransactionLogReference(). put() must then record old→new in forwardedEntries.
      final PageReference liveReference =
          new PageReference().setLogKey(frozenReference.getLogKey())
                             .setActiveTilGeneration(frozenReference.getActiveTilGeneration());
      final PageContainer successorContainer = PageContainer.getInstance(successorPage, successorPage);
      log.put(liveReference, successorContainer);
      assertEquals(1, log.forwardedEntryCount());

      log.setSnapshotDiskOffset(0, TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL);
      log.cleanupSnapshot();

      assertSame(successorContainer, log.get(liveReference),
          "the legacy forwarding entry must prevent stale promotion over the handleless successor");
      assertSame(successorContainer, log.get(frozenReference),
          "the captured original must resolve through the primitive compatibility forwarding map");
      assertEquals(1, log.liveEntryCount());
      assertEquals(0, log.pinnedSize(), "a compatibility-superseded declined page must never be pinned");
    } finally {
      log.close();
    }
  }

  private static KeyValueLeafPage encodeDisposableCopy(final ResourceConfiguration config,
      final KeyValueLeafPage original) {
    final KeyValueLeafPage copy = original.deepCopy();
    try {
      assertTrue(NodeStorageEngineWriter.serializeDisposableSnapshotKeyValuePage(config, copy),
          "real encoded page must fit in the disposable snapshot frame");
      return copy;
    } catch (final Throwable failure) {
      copy.close();
      if (failure instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (failure instanceof Error error) {
        throw error;
      }
      throw new AssertionError(failure);
    }
  }

  private static void serializeAndCloseNumericCopy(final ResourceConfiguration config,
      final KeyValueLeafPage original) {
    final KeyValueLeafPage copy = original.deepCopy();
    try {
      assertTrue(NodeStorageEngineWriter.serializeDisposableSnapshotKeyValuePage(config, copy));
      assertNull(copy.getRegionTable(),
          "a disposable writer table must be consumed and closed before the copy crosses threads");
    } finally {
      copy.close();
    }
  }

  private static void serializeFailingCopy(final ResourceConfiguration config, final KeyValueLeafPage original) {
    final KeyValueLeafPage copy = original.deepCopy();
    try {
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> NodeStorageEngineWriter.serializeDisposableSnapshotKeyValuePage(config, copy));
      assertEquals(FailingMemorySegmentHandler.FAILURE_MESSAGE, failure.getMessage());
      assertNull(copy.getRegionTable(),
          "a failed disposable serialization must not strand its writer table on the copy");
    } finally {
      copy.close();
    }
  }

  private static void serializeErrorCopy(final ResourceConfiguration config, final KeyValueLeafPage original) {
    final KeyValueLeafPage copy = original.deepCopy();
    try {
      final AssertionError failure = assertThrows(AssertionError.class,
          () -> NodeStorageEngineWriter.serializeDisposableSnapshotKeyValuePage(config, copy));
      assertEquals(ErrorMemorySegmentHandler.FAILURE_MESSAGE, failure.getMessage());
      assertNull(copy.getRegionTable(), "an Error unwind must not strand its writer table on the copy");
    } finally {
      copy.close();
    }
  }

  private static void writeObjectNumber(final KeyValueLeafPage page, final int slot, final int nameKey,
      final long value) {
    final long nodeKey = (page.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
    final ObjectNamedNumberNode node = new ObjectNamedNumberNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, -1L, 0, 0, 0L,
        value, HASH_FUNCTION, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);
  }

  private static long directBufferCapacity() {
    for (final BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      if ("direct".equals(pool.getName())) {
        return pool.getTotalCapacity();
      }
    }
    throw new AssertionError("JVM direct buffer pool is unavailable");
  }

  /**
   * Deterministic owned-result handler used to prove that only an empty pipeline takes the new path.
   */
  private static final class PrefixMemorySegmentHandler implements ByteHandler {

    private static final byte MARKER = (byte) 0xD3;

    @Override
    public OutputStream serialize(final OutputStream toSerialize) {
      return toSerialize;
    }

    @Override
    public InputStream deserialize(final InputStream toDeserialize) {
      return toDeserialize;
    }

    @Override
    public ByteHandler getInstance() {
      return new PrefixMemorySegmentHandler();
    }

    @Override
    public MemorySegment compress(final MemorySegment source) {
      final int sourceLength = Math.toIntExact(source.byteSize());
      final MemorySegment encoded = MemorySegment.ofArray(new byte[sourceLength + 1]);
      encoded.set(ValueLayout.JAVA_BYTE, 0L, MARKER);
      MemorySegment.copy(source, 0L, encoded, 1L, sourceLength);
      return encoded;
    }

    @Override
    public boolean supportsMemorySegments() {
      return true;
    }
  }

  /** Fails only after PageKind has built and written the PAX table into the pooled sink. */
  private static final class FailingMemorySegmentHandler implements ByteHandler {

    private static final String FAILURE_MESSAGE = "injected disposable cache compression failure";

    @Override
    public OutputStream serialize(final OutputStream toSerialize) {
      return toSerialize;
    }

    @Override
    public InputStream deserialize(final InputStream toDeserialize) {
      return toDeserialize;
    }

    @Override
    public ByteHandler getInstance() {
      return new FailingMemorySegmentHandler();
    }

    @Override
    public MemorySegment compress(final MemorySegment source) {
      throw new IllegalStateException(FAILURE_MESSAGE);
    }

    @Override
    public boolean supportsMemorySegments() {
      return true;
    }
  }

  /** Error twin of {@link FailingMemorySegmentHandler}; exercises try-with-resources' Error path. */
  private static final class ErrorMemorySegmentHandler implements ByteHandler {

    private static final String FAILURE_MESSAGE = "injected disposable cache compression Error";

    @Override
    public OutputStream serialize(final OutputStream toSerialize) {
      return toSerialize;
    }

    @Override
    public InputStream deserialize(final InputStream toDeserialize) {
      return toDeserialize;
    }

    @Override
    public ByteHandler getInstance() {
      return new ErrorMemorySegmentHandler();
    }

    @Override
    public MemorySegment compress(final MemorySegment source) {
      throw new AssertionError(FAILURE_MESSAGE);
    }

    @Override
    public boolean supportsMemorySegments() {
      return true;
    }
  }
}
