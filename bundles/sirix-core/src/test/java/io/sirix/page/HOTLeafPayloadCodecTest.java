package io.sirix.page;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ShardedPageCache;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.hot.page.HOTProjectionImage;
import io.sirix.io.PageHasher;
import io.sirix.io.bytepipe.ByteHandler.DecompressionResult;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.io.memorymapped.MMFileReader;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.Utils;
import io.sirix.settings.VersioningType;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.SplittableRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Isolated
final class HOTLeafPayloadCodecTest {
  private static final ResourceConfiguration CONFIG =
      new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).versioningApproach(VersioningType.FULL).build();

  @BeforeAll
  static void allocator() {
    Allocators.getInstance().init(1L << 30);
  }

  @Test
  void encodedImagesRejectInvalidStoredLengths() {
    for (final int stored : new int[] {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE}) {
      try (MemorySegmentBytesOut wire = new MemorySegmentBytesOut()) {
        writeHeader(wire, (byte) 2, 0, 8);
        wire.writeInt(stored);
        wire.writeByte((byte) 0);
        final byte[] bytes = wire.toByteArray();
        assertThrows(RuntimeException.class, () -> HOTLeafPayloadCodec.expandImage(bytes, 1));
        final byte[] scratch = Arrays.copyOf(bytes, bytes.length + 100);
        assertThrows(RuntimeException.class, () -> HOTLeafPayloadCodec.expandImage(scratch, 1, bytes.length));
      }
    }
  }

  @Test
  void zeroStoredLengthIsRejectedEvenForAnEmptyDecodedPayload() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment output = arena.allocate(1);
      output.fill((byte) 42);
      for (byte codec = 0; codec <= 1; codec++) {
        final byte[] frame = {0, 0, 0, 0, codec};
        final MemorySegment nativeFrame = arena.allocate(frame.length);
        MemorySegment.copy(frame, 0, nativeFrame, ValueLayout.JAVA_BYTE, 0, frame.length);
        assertThrows(IllegalStateException.class,
            () -> HOTLeafPayloadCodec.decode(new ByteArrayBytesIn(frame), output, 0));
        assertThrows(IllegalStateException.class,
            () -> HOTLeafPayloadCodec.decode(new MemorySegmentBytesIn(nativeFrame), output, 0));
        final byte[] heap = {42};
        assertThrows(IllegalStateException.class,
            () -> HOTLeafPayloadCodec.decodeHeap(new ByteArrayBytesIn(frame), heap, 0, 0));
        assertEquals(42, output.get(ValueLayout.JAVA_BYTE, 0));
        assertArrayEquals(new byte[] {42}, heap);
      }
    }
  }

  @Test
  void emptyEncodedZeroRunPayloadRemainsReadable() throws Exception {
    try (Arena arena = Arena.ofConfined(); MemorySegmentBytesOut wire = new MemorySegmentBytesOut()) {
      final MemorySegment output = arena.allocate(8);
      output.fill((byte) 42);
      final byte[] frame = {2, 0, 0, 0, 0, (byte) 0xff, 0};
      final ByteArrayBytesIn source = new ByteArrayBytesIn(frame);
      HOTLeafPayloadCodec.decode(source, output, 0);
      assertEquals(frame.length, source.position());
      assertEquals(42, output.get(ValueLayout.JAVA_BYTE, 0));
      final byte[] heap = {42};
      source.position(0);
      HOTLeafPayloadCodec.decodeHeap(source, heap, 0, 0);
      assertArrayEquals(new byte[] {42}, heap);
      writeHeader(wire, (byte) 2, 0, 0);
      wire.write(frame);
      try (HOTLeafPage decoded = (HOTLeafPage) new PagePersister().deserializePage(CONFIG,
          new ByteArrayBytesIn(wire.toByteArray()), SerializationType.DATA)) {
        assertEquals(0, decoded.getEntryCount());
        assertEquals(0, decoded.slotCapacity());
      }
      final ByteArrayBytesIn imageInput = new ByteArrayBytesIn(wire.toByteArray());
      imageInput.position(1);
      assertNull(HOTProjectionImage.copyIfFits(imageInput).select(new byte[] {7}, 0));
    }
  }

  @Test
  void nativeZeroRunOutputHonorsLogicalCapacityBeforeWriting() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment output = arena.allocate(32);
      output.fill((byte) 42);
      final byte[] frame = {(byte) 0xff, 8, (byte) 0x86};
      assertThrows(IndexOutOfBoundsException.class, () -> ZeroRunByteCodec.decode(frame, 0, 3, output, 7, 7));
      assertThrows(IndexOutOfBoundsException.class, () -> ZeroRunByteCodec.decode(frame, 0, 3, output, 7, 26));
      for (int i = 0; i < 32; i++) {
        assertEquals(42, output.get(ValueLayout.JAVA_BYTE, i));
      }
      assertEquals(8, ZeroRunByteCodec.decode(frame, 0, 3, output, 7, 8));
      assertEquals(42, output.get(ValueLayout.JAVA_BYTE, 6));
      assertEquals(0, output.get(ValueLayout.JAVA_BYTE, 7));
      assertEquals(0, output.get(ValueLayout.JAVA_BYTE, 14));
      assertEquals(42, output.get(ValueLayout.JAVA_BYTE, 15));
    }
  }

  @Test
  void projectionScratchRejectsEveryShortFrameAndDoesNotAliasRetainedImages() {
    try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION);
        MemorySegmentBytesOut output = new MemorySegmentBytesOut()) {
      final byte[] key = {7};
      final byte[] value = new byte[128];
      value[0] = 42; // Remains compressible with zero-run when native LZ4 is unavailable.
      assertTrue(leaf.put(key, value));
      PageKind.HOT_LEAF_PAGE.serializePage(CONFIG, output, leaf, SerializationType.DATA);
      final byte[] wire = output.toByteArray();
      assertTrue((wire[2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) != 0);
      final PageReference reference = new PageReference().setKey(7);
      reference.setHash(PageHasher.compute(wire));
      final HOTProjectionImage retained =
          HOTProjectionImage.copyVerifiedFrame(ByteBuffer.wrap(wire), reference, CONFIG);
      final ByteArrayBytesIn complete = new ByteArrayBytesIn(wire);
      complete.position(1);
      final HOTProjectionImage retainedSecond = HOTProjectionImage.copyIfFits(complete);
      for (int length = 3; length < wire.length; length++) {
        final byte[] shortFrame = Arrays.copyOf(wire, length);
        // Fill reusable scratch with valid bytes immediately before every truncated copy.
        HOTProjectionImage.copyVerifiedFrame(ByteBuffer.wrap(wire), reference, CONFIG);
        assertThrows(RuntimeException.class,
            () -> HOTProjectionImage.copyVerifiedFrame(ByteBuffer.wrap(shortFrame), new PageReference(), CONFIG));
        complete.position(1);
        HOTProjectionImage.copyIfFits(complete);
        final ByteArrayBytesIn shortInput = new ByteArrayBytesIn(shortFrame);
        shortInput.position(1);
        assertThrows(RuntimeException.class, () -> HOTProjectionImage.copyIfFits(shortInput));
      }
      final byte[] scratch = Arrays.copyOf(wire, wire.length + 100);
      assertArrayEquals(HOTLeafPayloadCodec.expandImage(wire, 1),
          HOTLeafPayloadCodec.expandImage(scratch, 1, wire.length));
      assertThrows(IndexOutOfBoundsException.class, () -> HOTLeafPayloadCodec.expandImage(scratch, 1, wire.length - 1));
      assertThrows(IndexOutOfBoundsException.class,
          () -> HOTLeafPayloadCodec.expandImage(scratch, 1, scratch.length + 1));
      assertThrows(IndexOutOfBoundsException.class, () -> HOTLeafPayloadCodec.expandImage(scratch, 1, 2));
      final byte[] raw = HOTLeafPayloadCodec.expandImage(wire, 1);
      assertThrows(IllegalArgumentException.class, () -> HOTLeafPayloadCodec.expandImage(raw, 1));
      assertThrows(IllegalArgumentException.class,
          () -> HOTLeafPayloadCodec.expandImage(Arrays.copyOf(raw, raw.length + 100), 1, raw.length));
      final byte[] replacement = new byte[64];
      replacement[0] = 99;
      assertTrue(leaf.updateValue(leaf.findEntry(key), replacement));
      output.clear();
      PageKind.HOT_LEAF_PAGE.serializePage(CONFIG, output, leaf, SerializationType.DATA);
      final byte[] replacementWire = output.toByteArray();
      assertTrue((replacementWire[2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) != 0);
      final ByteArrayBytesIn replacementInput = new ByteArrayBytesIn(replacementWire);
      replacementInput.position(1);
      assertArrayEquals(replacement, HOTProjectionImage.copyIfFits(replacementInput).select(key, 0).value());
      assertArrayEquals(value, retained.select(key, 0).value());
      assertArrayEquals(value, retainedSecond.select(key, 0).value());
      // Hash the logical short frame, never the longer scratch capacity.
      assertArrayEquals(value,
          HOTProjectionImage.copyVerifiedFrame(ByteBuffer.wrap(wire), reference, CONFIG).select(key, 0).value());
    }
  }

  @Test
  void writerAndSelectiveDecoderInitializeScratchOnANewWorkerThread() throws Exception {
    try (var worker = Executors.newSingleThreadExecutor()) {
      final byte[] value = worker.submit(() -> {
        try (HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PROJECTION);
            MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
          assertTrue(page.put(new byte[] {7}, new byte[256]));
          PageKind.HOT_LEAF_PAGE.serializePage(CONFIG, sink, page, SerializationType.DATA);
          final ByteArrayBytesIn source = new ByteArrayBytesIn(sink.toByteArray());
          source.readByte();
          final HOTProjectionImage image = HOTProjectionImage.copyIfFits(source);
          assertArrayEquals(new byte[256], image.select(new byte[] {7}, 0).value());
          source.position(1);
          return PageKind.deserializeHOTProjectionEntry(source, new byte[] {7}, 0).value();
        }
      }).get();
      assertArrayEquals(new byte[256], value);
    }
  }

  @Test
  void failedDecodeReturnsItsUnpublishedNativeFrame() throws Exception {
    final FrameSlotAllocator allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    try (MemorySegmentBytesOut wire = new MemorySegmentBytesOut()) {
      writeHeader(wire, (byte) 2, 1, 6);
      wire.writeInt(0);
      wire.writeInt(9);
      wire.writeByte((byte) 0);
      wire.write(new byte[] {(byte) 0xff, 6, 5, 1, 0, 7, 1, 0, 42});
      final byte[] bytes = wire.toByteArray();
      final long active = allocator.getActiveMemoryBytes();
      try (HOTLeafPage decoded = (HOTLeafPage) new PagePersister().deserializePage(CONFIG, new ByteArrayBytesIn(bytes),
          SerializationType.DATA)) {
        assertArrayEquals(new byte[] {42}, decoded.getValue(decoded.findEntry(new byte[] {7})));
      }
      assertEquals(active, allocator.getActiveMemoryBytes(), "successful page close returns the decoded frame");
      bytes[28] = 0; // corrupt the zero-run marker after the offset directory and codec frame header
      assertThrows(IllegalStateException.class,
          () -> new PagePersister().deserializePage(CONFIG, new ByteArrayBytesIn(bytes), SerializationType.DATA));
      assertEquals(active, allocator.getActiveMemoryBytes(), "failed decode returns the unpublished frame");
    }
  }

  @Test
  void maximumDimensionsAndStoredFrameBoundaryRemainReadable() {
    // Non-minimal, legal five-byte varint plus 512 literal runs reaches the stored-size cap exactly.
    final int used = 65536;
    final int stored = 66054;
    try (MemorySegmentBytesOut frame = new MemorySegmentBytesOut();
        MemorySegmentBytesOut wire = new MemorySegmentBytesOut();
        Arena arena = Arena.ofConfined()) {
      frame.writeInt(stored);
      frame.writeByte((byte) 0);
      frame.write(new byte[] {(byte) 0xff, (byte) 0x80, (byte) 0x80, (byte) 0x84, (byte) 0x80, 0});
      final byte[] literal = new byte[128];
      Arrays.fill(literal, (byte) 7);
      for (int i = 0; i < 512; i++) {
        frame.writeByte((byte) 127);
        frame.write(literal);
      }
      final byte[] expected = new byte[used];
      Arrays.fill(expected, (byte) 7);
      final byte[] payload = frame.toByteArray();
      assertEquals(stored + 5, payload.length);
      final MemorySegment output = arena.allocate(used);
      HOTLeafPayloadCodec.decode(new ByteArrayBytesIn(payload), output, used);
      assertArrayEquals(expected, output.toArray(ValueLayout.JAVA_BYTE));
      final byte[] heap = new byte[used];
      HOTLeafPayloadCodec.decodeHeap(new ByteArrayBytesIn(payload), heap, 0, used);
      assertArrayEquals(expected, heap);
      writeHeader(wire, (byte) 2, 512, used);
      for (int i = 0; i < 512; i++) {
        wire.writeInt(i * 128);
      }
      wire.write(payload);
      final byte[] expanded = HOTLeafPayloadCodec.expandImage(wire.toByteArray(), 1);
      assertEquals(0, expanded[2]);
      assertArrayEquals(expected, Arrays.copyOfRange(expanded, 19 + 512 * 4, expanded.length));
    }
  }

  @Test
  void failedSinkReturnsBothCodecScratchBuffers() {
    try (MemorySegmentBytesOut raw = new MemorySegmentBytesOut();
        MemorySegmentBytesOut recovered = new MemorySegmentBytesOut();
        MemorySegmentBytesOut failing = new MemorySegmentBytesOut() {
          @Override
          public MemorySegmentBytesOut write(final byte[] bytes, final int offset, final int length) {
            throw new IllegalArgumentException("injected sink failure");
          }
        }) {
      writeHeader(raw, (byte) 0, 0, 128);
      raw.write(new byte[128]);
      final byte[] wire = raw.toByteArray();
      final IllegalArgumentException failure =
          assertThrows(IllegalArgumentException.class, () -> HOTLeafPayloadCodec.writePage(failing, wire, wire.length));
      assertEquals("injected sink failure", failure.getMessage());
      // A leaked input or output lease makes this second encode reject reacquisition.
      HOTLeafPayloadCodec.writePage(recovered, wire, wire.length);
      assertTrue((recovered.toByteArray()[2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) != 0);
    }
  }

  @Test
  void encodedLookingInputStillRunsANonemptyOuterPipeline() throws Exception {
    final FrameSlotAllocator allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    try (HOTLeafPage original = new HOTLeafPage(1, 1, IndexType.PROJECTION);
        MemorySegmentBytesOut sink = new MemorySegmentBytesOut();
        Arena arena = Arena.ofConfined()) {
      final byte[] value = new byte[256];
      assertTrue(original.put(new byte[] {7}, value));
      PageKind.HOT_LEAF_PAGE.serializePage(CONFIG, sink, original, SerializationType.DATA);
      final byte[] bytes = sink.toByteArray();
      assertEquals(2, bytes[2]);
      final MemorySegment wire = arena.allocate(bytes.length);
      wire.copyFrom(MemorySegment.ofArray(bytes));
      final MemorySegment outerInput = arena.allocate(3);
      outerInput.set(ValueLayout.JAVA_BYTE, 0, (byte) 12);
      outerInput.set(ValueLayout.JAVA_BYTE, 2, (byte) 2);
      final ByteHandler handler = mock(ByteHandler.class);
      when(handler.supportsMemorySegments()).thenReturn(true);
      final ByteHandlerPipeline identity = new ByteHandlerPipeline();
      when(handler.decompressScoped(outerInput)).thenAnswer(invocation -> identity.decompressScoped(wire));
      final long before = allocator.getActiveMemoryBytes();
      try (
          MMFileReader reader =
              new MMFileReader(MemorySegment.NULL, MemorySegment.NULL, new ByteHandlerPipeline(handler),
                  SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build(), null);
          HOTLeafPage decoded = (HOTLeafPage) reader.deserializeFromSegment(CONFIG, outerInput)) {
        assertEquals(1, decoded.getEntryCount());
        assertArrayEquals(value, decoded.getValue(decoded.findEntry(new byte[] {7})));
        assertEquals(4096, allocator.getActiveMemoryBytes() - before);
      }
      assertEquals(before, allocator.getActiveMemoryBytes());
    }
  }

  @Test
  void strongerCodecMustBeatZeroRunByMoreThanFiveBytes() {
    assumeTrue(FFILz4Compressor.isNativeAvailable());
    final FFILz4Compressor compressor = new FFILz4Compressor();
    try (Arena arena = Arena.ofConfined();
        MemorySegmentBytesOut raw = new MemorySegmentBytesOut();
        MemorySegmentBytesOut encoded = new MemorySegmentBytesOut()) {
      final MemorySegment input = arena.allocate(100);
      final MemorySegment output = arena.allocate(compressor.compressBound(100));
      final byte[] zeroRun = new byte[ZeroRunByteCodec.maxEncodedSize(100)];
      for (final int saving : new int[] {4, 5, 6}) {
        boolean witnessed = false;
        for (int prefix = 1; prefix < 90; prefix++) {
          final byte[] payload = new byte[100];
          Arrays.fill(payload, 0, prefix, (byte) 4);
          Arrays.fill(payload, 98, 100, (byte) 4);
          input.copyFrom(MemorySegment.ofArray(payload));
          final int lz4Length = compressor.compressSegmentBounded(input, 100, output, (int) output.byteSize());
          final int zeroLength = ZeroRunByteCodec.encode(payload, 0, payload.length, zeroRun, 0);
          if (zeroLength - lz4Length != saving) {
            continue;
          }
          raw.clear();
          encoded.clear();
          writeHeader(raw, (byte) 0, 0, payload.length);
          raw.write(payload);
          HOTLeafPayloadCodec.writePage(encoded, raw.toByteArray(), (int) raw.position());
          final byte[] wire = encoded.toByteArray();
          assertEquals(2, wire[2]);
          assertEquals(saving == 6
              ? 1
              : 0, wire[23], "five-byte decoder preference, saving=" + saving);
          assertArrayEquals(raw.toByteArray(), HOTLeafPayloadCodec.expandImage(wire, 1));
          witnessed = true;
          break;
        }
        assertTrue(witnessed, "native codec must supply the boundary witness for saving=" + saving);
      }
    }
  }

  @Test
  void ordinaryOwnedReadsRetainSmallFramesAndPromoteOnlyOnMutation() throws Exception {
    final FrameSlotAllocator allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    final ByteHandlerPipeline pipeline = spy(new ByteHandlerPipeline());
    final PagePersister persister = new PagePersister();
    for (final boolean repeated : new boolean[] {false, true}) {
      try (HOTLeafPage original = new HOTLeafPage(1, 1, IndexType.PROJECTION);
          MemorySegmentBytesOut sink = new MemorySegmentBytesOut();
          Arena arena = Arena.ofConfined()) {
        final byte[] value = new byte[128];
        if (repeated) {
          for (int i = 0; i < value.length / 2; i++) {
            value[i] = (byte) (i % 17 + 1);
          }
        }
        for (int i = 0; i < 32; i++) {
          assertTrue(original.put(new byte[] {(byte) i}, value));
        }
        assertEquals(4256, original.getUsedSlotsSize());
        original.setPageReference(7, new PageReference().setKey(123));
        for (final VersioningType versioning : VersioningType.values()) {
          final ResourceConfiguration config =
              new ResourceConfiguration.Builder("owned-codec").versioningApproach(versioning).build();
          for (final boolean encoded : new boolean[] {false, true}) {
            sink.clear();
            if (encoded) {
              PageKind.HOT_LEAF_PAGE.serializePage(config, sink, original, SerializationType.DATA);
            } else {
              PageKind.serializeRawHOTLeaf(config, sink, original);
            }
            final byte[] bytes = sink.toByteArray();
            if (encoded) {
              assertTrue((bytes[2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) != 0);
            }
            final MemorySegment wire = arena.allocate(bytes.length);
            wire.copyFrom(MemorySegment.ofArray(bytes));
            final long before = allocator.getActiveMemoryBytes();
            if (encoded) {
              try (DecompressionResult truncated = pipeline.decompressScoped(wire.asSlice(0, bytes.length - 1))) {
                assertThrows(RuntimeException.class, () -> persister.deserializePage(config,
                    new MemorySegmentBytesIn(truncated.segment()), SerializationType.DATA, truncated));
              }
              assertEquals(before, allocator.getActiveMemoryBytes());
            }
            final HOTLeafPage decoded;
            try (DecompressionResult outer = pipeline.decompressScoped(wire)) {
              decoded = (HOTLeafPage) persister.deserializePage(config, new MemorySegmentBytesIn(outer.segment()),
                  SerializationType.DATA, outer);
              assertEquals(!encoded, outer.ownershipTransferred().get());
            }
            try (decoded) {
              assertEquals(8192, allocator.getActiveMemoryBytes() - before);
              assertEquals(4256, decoded.getActualMemorySize());
              final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1L << 20);
              final PageReference reference = new PageReference().setKey(1);
              cache.put(reference, decoded);
              assertEquals(decoded.getActualMemorySize() + decoded.estimatedCanonicalCacheRetainedHeapBytes(),
                  cache.getCurrentWeightBytes());
              assertTrue(cache.getCurrentWeightBytes() < 16384);
              wire.fill((byte) 0xa5);
              assertArrayEquals(value, decoded.getValue(decoded.findEntry(new byte[] {17})));
              assertEquals(123, decoded.getPageReference(7).getKey());
              final long binding = decoded.readStampBinding();
              final long stamp = decoded.readStamp();
              assertTrue(decoded.validateStamp(binding, stamp));
              assertTrue(decoded.put(new byte[] {32}, value));
              assertEquals(HOTLeafPage.DEFAULT_SIZE, decoded.slotCapacity());
              assertTrue(!decoded.validateStamp(binding, stamp));
              assertEquals(HOTLeafPage.DEFAULT_SIZE, allocator.getActiveMemoryBytes() - before);
              assertArrayEquals(value, decoded.getValue(decoded.findEntry(new byte[] {17})));
              cache.clear();
              assertEquals(0, cache.getCurrentWeightBytes());
            }
            assertEquals(before, allocator.getActiveMemoryBytes());
            wire.copyFrom(MemorySegment.ofArray(bytes));
            clearInvocations(pipeline);
            try (MMFileReader reader = new MMFileReader(MemorySegment.NULL, MemorySegment.NULL, pipeline,
                SerializationType.DATA, persister, Caffeine.newBuilder().build(), null)) {
              assertThrows(RuntimeException.class,
                  () -> reader.deserializeFromSegment(config, wire.asSlice(0, bytes.length - 1)));
              assertEquals(before, allocator.getActiveMemoryBytes());
              try (HOTLeafPage read = (HOTLeafPage) reader.deserializeFromSegment(config, wire)) {
                assertEquals(8192, allocator.getActiveMemoryBytes() - before);
                assertEquals(4256, read.getActualMemorySize());
                assertEquals(config.getDatabaseId(), read.getPageReference(7).getDatabaseId());
                assertEquals(config.getID(), read.getPageReference(7).getResourceId());
                wire.fill((byte) 0xa5);
                assertArrayEquals(value, read.getValue(read.findEntry(new byte[] {17})));
              }
            }
            if (encoded) {
              verify(pipeline, never()).decompressScoped(wire);
            }
            assertEquals(before, allocator.getActiveMemoryBytes());
          }
        }
      }
    }
  }

  @Test
  void ownedReadsKeepTheirMemoryContractWithoutNativeLz4() throws Exception {
    try (MockedStatic<FFILz4Compressor> codec = mockStatic(FFILz4Compressor.class, CALLS_REAL_METHODS)) {
      codec.when(FFILz4Compressor::isNativeAvailable).thenReturn(false);
      ordinaryOwnedReadsRetainSmallFramesAndPromoteOnlyOnMutation();
    }
  }

  @Test
  void codecSizeTiesPreferZeroRun() {
    final byte[] payload = new byte[100];
    Arrays.fill(payload, 0, 8, (byte) 4);
    Arrays.fill(payload, 98, 100, (byte) 4);
    try (MemorySegmentBytesOut raw = new MemorySegmentBytesOut();
        MemorySegmentBytesOut encoded = new MemorySegmentBytesOut()) {
      writeHeader(raw, (byte) 0, 0, payload.length);
      raw.write(payload);
      HOTLeafPayloadCodec.writePage(encoded, raw.toByteArray(), (int) raw.position());
      final byte[] wire = encoded.toByteArray();
      assertEquals(2, wire[2]);
      assertEquals(0, wire[23], "both native LZ4 and zero-run encode this fixture to 15 bytes");
      assertEquals(19 + 5 + 15, wire.length);
    }
  }

  @Test
  void encodedImagesRejectTruncationBeforePublication() {
    try (MemorySegmentBytesOut wire = new MemorySegmentBytesOut()) {
      writeHeader(wire, (byte) 2, 0, 8);
      wire.write(HexFormat.of().parseHex("0300000000ff0886"));
      final byte[] valid = wire.toByteArray();
      assertEquals(27, HOTLeafPayloadCodec.expandImage(valid, 1).length);
      for (int length = 0; length < valid.length; length++) {
        final byte[] truncated = Arrays.copyOf(valid, length);
        assertThrows(RuntimeException.class, () -> HOTLeafPayloadCodec.expandImage(truncated, 1));
      }
    }
  }

  @Test
  void selectiveReadCannotExposeScratchBeyondDecodedPayload() {
    // Valid codec frame, corrupt slot value length: the reusable heap scratch is larger than "used".
    final byte[] slot = {1, 0, 7, 10, 0, 42};
    try (MemorySegmentBytesOut wire = new MemorySegmentBytesOut()) {
      writeHeader(wire, (byte) 2, 1, slot.length);
      wire.writeInt(0);
      wire.writeInt(9);
      wire.writeByte((byte) 0);
      wire.write(new byte[] {(byte) 0xff, 6, 5});
      wire.write(slot);
      final ByteArrayBytesIn source = new ByteArrayBytesIn(wire.toByteArray());
      source.readByte();
      assertThrows(IllegalStateException.class,
          () -> PageKind.deserializeHOTProjectionEntry(source, new byte[] {7}, 0));
    }
  }

  private static void writeHeader(final MemorySegmentBytesOut sink, final byte flags, final int count, final int used) {
    sink.writeByte((byte) 12);
    PageKind.writeVersionAndFlags(sink, flags);
    Utils.putVarLong(sink, 1);
    sink.writeInt(1);
    sink.writeByte(IndexType.PROJECTION.getID());
    sink.writeShort((short) 0);
    sink.writeInt(count);
    sink.writeInt(used);
  }

  @Test
  void encodedLeafDoesNotAdoptTheOuterDecompressionFrame() throws Exception {
    try (HOTLeafPage original = new HOTLeafPage(1, 1, IndexType.PROJECTION);
        MemorySegmentBytesOut sink = new MemorySegmentBytesOut();
        Arena arena = Arena.ofConfined()) {
      final byte[] value = new byte[1024];
      assertTrue(original.put(new byte[] {7}, value));
      PageKind.HOT_LEAF_PAGE.serializePage(CONFIG, sink, original, SerializationType.DATA);
      final byte[] bytes = sink.toByteArray();
      assertEquals(2, bytes[2]);
      final MemorySegment wire = arena.allocate(bytes.length);
      MemorySegment.copy(bytes, 0, wire, ValueLayout.JAVA_BYTE, 0, bytes.length);
      final AtomicInteger releases = new AtomicInteger();
      final HOTLeafPage decoded;
      try (DecompressionResult outer = new DecompressionResult(wire, releases::incrementAndGet)) {
        decoded = (HOTLeafPage) new PagePersister().deserializePage(CONFIG, new MemorySegmentBytesIn(wire),
            SerializationType.DATA, outer);
        assertEquals(0, releases.get());
      }
      try (decoded) {
        assertEquals(1, releases.get(), "the outer owner remains responsible for compressed input");
        wire.fill((byte) 0xa5);
        assertArrayEquals(value, decoded.getValue(decoded.findEntry(new byte[] {7})));
      }
      assertEquals(1, releases.get(), "closing the leaf must release only its separate decoded frame");
    }
  }

  @Test
  void rawFallbackAccountsForFramingAndZeroRunGoldenPinsTheBoundary() {
    for (final int used : new int[] {0, 1, 4, 5, 6, 7, 8, 9, 32}) {
      try (MemorySegmentBytesOut raw = new MemorySegmentBytesOut();
          MemorySegmentBytesOut encoded = new MemorySegmentBytesOut()) {
        raw.writeByte((byte) 12);
        PageKind.writeVersionAndFlags(raw);
        raw.writeByte((byte) 1); // page key varint
        raw.writeInt(1);
        raw.writeByte(IndexType.PROJECTION.getID());
        raw.writeShort((short) 0);
        raw.writeInt(0);
        raw.writeInt(used);
        raw.write(new byte[used]);
        HOTLeafPayloadCodec.writePage(encoded, raw.toByteArray(), (int) raw.position());
        final byte[] bytes = encoded.toByteArray();
        if (used <= 8) {
          assertArrayEquals(raw.toByteArray(), bytes, "framing ties and tiny payloads remain raw");
        } else {
          assertEquals(2, bytes[2]);
          assertArrayEquals(new byte[] {3, 0, 0, 0, 0, (byte) 0xff, (byte) used, (byte) (0x80 + used - 2)},
              Arrays.copyOfRange(bytes, 19, bytes.length));
        }
      }
    }
  }

  @Test
  void sparseFragmentsAndMixedStreamsReadExactlyOneImage() throws Exception {
    final ResourceConfiguration sliding =
        new ResourceConfiguration.Builder("codec-sparse").versioningApproach(VersioningType.SLIDING_SNAPSHOT).build();
    try (HOTLeafPage base = new HOTLeafPage(7, 1, IndexType.PROJECTION);
        MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      assertTrue(base.put(new byte[] {1}, new byte[256]));
      try (HOTLeafPage changed = base.copy()) {
        assertTrue(changed.put(new byte[] {2}, new byte[512]));
        changed.setCompleteDump(true);
        changed.setPageReference(7, new PageReference().setKey(123));
        PageKind.serializeRawHOTLeaf(sliding, sink, changed);
        final byte[] rawFragment = sink.toByteArray();
        sink.clear();
        PageKind.HOT_LEAF_PAGE.serializePage(sliding, sink, changed, SerializationType.DATA);
        final int boundary = (int) sink.position();
        assertEquals(3, sink.toByteArray()[2]);
        assertArrayEquals(rawFragment, HOTLeafPayloadCodec.expandImage(sink.toByteArray(), 1));
        PageKind.serializeRawHOTLeaf(CONFIG, sink, base);
        final ByteArrayBytesIn source = new ByteArrayBytesIn(sink.toByteArray());
        try (HOTLeafPage first =
            (HOTLeafPage) new PagePersister().deserializePage(sliding, source, SerializationType.DATA)) {
          assertEquals(1, first.getEntryCount());
          assertArrayEquals(new byte[] {2}, first.getKey(0));
          assertArrayEquals(new byte[512], first.getValue(0));
          assertTrue(first.isCompleteDump());
          assertEquals(123, first.getPageReference(7).getKey());
          assertEquals(boundary, source.position());
        }
        try (HOTLeafPage second =
            (HOTLeafPage) new PagePersister().deserializePage(CONFIG, source, SerializationType.DATA)) {
          assertArrayEquals(new byte[] {1}, second.getKey(0));
          assertEquals(sink.position(), source.position());
        }
      }
    }
  }

  @Test
  void goldenPayloadsDecodeWithBothInputKindsAndLeaveNextBytesUnread() {
    for (final String hex : new String[] {"0300000000ff0886", "05000000014001020304"}) {
      final byte[] wire = HexFormat.of().parseHex(hex);
      final byte[] expected = hex.endsWith("86")
          ? new byte[8]
          : new byte[] {1, 2, 3, 4};
      final byte[] heap = new byte[expected.length + 4];
      Arrays.fill(heap, (byte) 99);
      HOTLeafPayloadCodec.decodeHeap(new ByteArrayBytesIn(wire), heap, 2, expected.length);
      assertArrayEquals(expected, Arrays.copyOfRange(heap, 2, 2 + expected.length));
      assertEquals(99, heap[0]);
      assertEquals(99, heap[heap.length - 1]);
      final byte[] stream = Arrays.copyOf(wire, wire.length + 1);
      stream[wire.length] = 99;
      try (Arena arena = Arena.ofConfined()) {
        final MemorySegment nativeWire = arena.allocate(stream.length);
        nativeWire.copyFrom(MemorySegment.ofArray(stream));
        for (final BytesIn<?> source : new BytesIn<?>[] {new ByteArrayBytesIn(stream),
            new MemorySegmentBytesIn(nativeWire), new MemorySegmentBytesIn(MemorySegment.ofArray(stream))}) {
          final MemorySegment output = arena.allocate(expected.length);
          HOTLeafPayloadCodec.decode(source, output, expected.length);
          assertArrayEquals(expected, output.toArray(ValueLayout.JAVA_BYTE));
          assertEquals(99, source.readByte());
        }
      }
    }
  }

  @Test
  void corruptPayloadsRejectEveryTruncationAndLengthMismatch() {
    final byte[][] frames =
        {HexFormat.of().parseHex("0300000000ff0886"), HexFormat.of().parseHex("05000000014001020304")};
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment output = arena.allocate(8);
      for (final byte[] frame : frames) {
        for (int size = 0; size < frame.length; size++) {
          final byte[] truncated = Arrays.copyOf(frame, size);
          assertThrows(RuntimeException.class,
              () -> HOTLeafPayloadCodec.decode(new ByteArrayBytesIn(truncated), output, 8));
        }
        final byte[] unknown = frame.clone();
        unknown[4] = 9;
        assertThrows(IllegalStateException.class,
            () -> HOTLeafPayloadCodec.decode(new ByteArrayBytesIn(unknown), output, 8));
        assertThrows(RuntimeException.class, () -> HOTLeafPayloadCodec.decode(new ByteArrayBytesIn(frame), output, 3));
      }
      for (final String hex : new String[] {"ffffffff00", "0000000000", "0300000000ff0086", "0300000001000000"}) {
        assertThrows(RuntimeException.class,
            () -> HOTLeafPayloadCodec.decode(new ByteArrayBytesIn(HexFormat.of().parseHex(hex)), output, 8));
      }
    }
  }

  @Test
  void mixedRawAndEncodedPagesPreserveValuesSideReferencesAndCompactReads() throws Exception {
    final SplittableRandom random = new SplittableRandom(0x51c0dec);
    for (int pattern = 0; pattern < 3; pattern++) {
      final HOTLeafPage original = new HOTLeafPage(17, 23, IndexType.PROJECTION);
      try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
        for (int i = 0; i < 64; i++) {
          final byte[] value = new byte[192];
          for (int j = 0; j < value.length; j++) {
            value[j] = pattern == 0
                ? 0
                : pattern == 1
                    ? (byte) (j % 7 + 1)
                    : (byte) random.nextInt(256);
          }
          assertTrue(original.put(new byte[] {(byte) i}, value));
        }
        original.setCompleteDump(true);
        original.setPageReference(7, new PageReference().setKey(123));
        PageKind.serializeRawHOTLeaf(CONFIG, sink, original);
        final byte[] raw = sink.toByteArray();
        sink.clear();
        PageKind.HOT_LEAF_PAGE.serializePage(CONFIG, sink, original, SerializationType.DATA);
        final byte[] encoded = sink.toByteArray();
        assertTrue(encoded.length <= raw.length);
        assertArrayEquals(raw, (encoded[2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) == 0
            ? encoded
            : HOTLeafPayloadCodec.expandImage(encoded, 1),
            "cached images must have exactly the legacy raw bytes after one cold expansion");
        if (pattern == 0 || (pattern == 1 && FFILz4Compressor.isNativeAvailable())) {
          assertEquals(3, encoded[2]);
          assertTrue(encoded.length < raw.length / 2);
        }
        for (final byte[] wire : new byte[][] {raw, encoded}) {
          assertDecodedLeaf(original, wire);
          assertSelectedEntries(original, wire);
        }
      } finally {
        original.close();
      }
    }
  }

  private static void assertDecodedLeaf(final HOTLeafPage original, final byte[] wire) throws IOException {
    for (final boolean compact : new boolean[] {false, true}) {
      final HOTLeafPage decoded = (HOTLeafPage) (compact
          ? new PagePersister().deserializeHOTLeafFragment(CONFIG, new ByteArrayBytesIn(wire), SerializationType.DATA)
          : new PagePersister().deserializePage(CONFIG, new ByteArrayBytesIn(wire), SerializationType.DATA));
      try {
        assertEquals(64, decoded.getEntryCount());
        assertTrue(decoded.isCompleteDump());
        assertEquals(123, decoded.getPageReference(7).getKey());
        for (int i = 0; i < 64; i++) {
          assertArrayEquals(original.getValue(i), decoded.getValue(decoded.findEntry(new byte[] {(byte) i})));
        }
      } finally {
        decoded.close();
      }
    }
  }

  private static void assertSelectedEntries(final HOTLeafPage original, final byte[] wire) {
    for (int i = 0; i <= 64; i++) {
      final ByteArrayBytesIn pointSource = new ByteArrayBytesIn(wire);
      pointSource.readByte();
      final var point = PageKind.deserializeHOTProjectionEntry(pointSource, new byte[] {(byte) i}, 7);
      if (i == 64) {
        assertNull(point);
      } else {
        assertArrayEquals(original.getValue(i), point.value());
        assertEquals(123, point.sideReference().getKey());
      }
      assertEquals(wire.length, pointSource.position());
    }
    final ByteArrayBytesIn source = new ByteArrayBytesIn(wire);
    source.readByte();
    final HOTProjectionImage image = HOTProjectionImage.copyIfFits(source);
    for (int i = 0; i < 64; i++) {
      assertArrayEquals(original.getValue(i), image.select(new byte[] {(byte) i}, 7).value());
    }
  }
}
