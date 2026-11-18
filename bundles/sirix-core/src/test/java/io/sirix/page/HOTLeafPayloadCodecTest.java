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

  /**
   * Every codec a reader accepts. Writers emit LZ4 only; zero-run frames come from earlier writers.
   */
  private static final int[] CODECS = {HOTLeafPayloadCodec.CODEC_ZERO_RUN, HOTLeafPayloadCodec.CODEC_LZ4};

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
    for (final int codec : CODECS) {
      try (HOTLeafPage leaf = new HOTLeafPage(7, 3, IndexType.PROJECTION);
          MemorySegmentBytesOut output = new MemorySegmentBytesOut()) {
        final byte[] key = {7};
        final byte[] value = new byte[128];
        value[0] = 42;
        assertTrue(leaf.put(key, value));
        PageKind.serializeRawHOTLeaf(CONFIG, output, leaf);
        final byte[] wire = encodedImage(output.toByteArray(), codec);
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
        assertThrows(IndexOutOfBoundsException.class,
            () -> HOTLeafPayloadCodec.expandImage(scratch, 1, wire.length - 1));
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
        PageKind.serializeRawHOTLeaf(CONFIG, output, leaf);
        final byte[] replacementWire = encodedImage(output.toByteArray(), codec);
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
  }

  @Test
  void writerAndSelectiveDecoderInitializeScratchOnANewWorkerThread() throws Exception {
    try (var worker = Executors.newSingleThreadExecutor()) {
      final byte[] value = worker.submit(() -> {
        try (HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PROJECTION);
            MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
          assertTrue(page.put(new byte[] {7}, new byte[256]));
          // The writer with compression switched on; it is off by default.
          PageKind.serializeHOTLeaf(CONFIG, sink, page, true);
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
    assumeTrue(FFILz4Compressor.isNativeAvailable(), "the writer takes its scratch only when it can compress");
    final byte[] incompressible = new byte[128];
    new SplittableRandom(9).nextBytes(incompressible);
    // The encoded exit (128 zero bytes) and the raw exit (128 random bytes) both write to the sink
    // while the two leases are held.
    for (final byte[] payload : new byte[][] {new byte[128], incompressible}) {
      try (MemorySegmentBytesOut raw = new MemorySegmentBytesOut();
          MemorySegmentBytesOut recovered = new MemorySegmentBytesOut();
          MemorySegmentBytesOut failing = new MemorySegmentBytesOut() {
            @Override
            public MemorySegmentBytesOut write(final byte[] bytes, final int offset, final int length) {
              throw new IllegalArgumentException("injected sink failure");
            }
          }) {
        writeHeader(raw, (byte) 0, 0, payload.length);
        raw.write(payload);
        final byte[] wire = raw.toByteArray();
        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> HOTLeafPayloadCodec.writePage(failing, wire, wire.length));
        assertEquals("injected sink failure", failure.getMessage());
        // A leaked input or output lease makes this second encode reject reacquisition.
        HOTLeafPayloadCodec.writePage(recovered, wire, wire.length);
        assertEquals(payload != incompressible,
            (recovered.toByteArray()[2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) != 0);
      }
    }
  }

  @Test
  void encodedLookingInputStillRunsANonemptyOuterPipeline() throws Exception {
    final FrameSlotAllocator allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    for (final int codec : CODECS) {
      try (HOTLeafPage original = new HOTLeafPage(1, 1, IndexType.PROJECTION);
          MemorySegmentBytesOut sink = new MemorySegmentBytesOut();
          Arena arena = Arena.ofConfined()) {
        final byte[] value = new byte[256];
        assertTrue(original.put(new byte[] {7}, value));
        PageKind.serializeRawHOTLeaf(CONFIG, sink, original);
        final byte[] bytes = encodedImage(sink.toByteArray(), codec);
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
  }

  @Test
  void lz4IsKeptExactlyWhenItSavesAThirdOfThePayloadFramingIncluded() {
    assertEquals(5, HOTLeafPayloadCodec.FRAME_OVERHEAD);
    assertEquals(3, HOTLeafPayloadCodec.LZ4_MIN_SAVING_DIVISOR);
    // used, and the largest stored length that still saves a third: used - 5 - ceil(used / 3).
    for (final int[] boundary : new int[][] {{9, 1}, {10, 1}, {11, 2}, {12, 3}, {300, 195}, {301, 195}, {302, 196},
        {4256, 2832}, {65536, 43685}}) {
      final int used = boundary[0];
      final int largestKept = boundary[1];
      assertEquals(used - 5 - (used + 2) / 3, largestKept, "the table must state the rule, used=" + used);
      assertTrue(HOTLeafPayloadCodec.keepsLz4(used, largestKept - 1), "one byte better than needed, used=" + used);
      assertTrue(HOTLeafPayloadCodec.keepsLz4(used, largestKept), "exactly a third saved, used=" + used);
      assertTrue(!HOTLeafPayloadCodec.keepsLz4(used, largestKept + 1), "one byte short of a third, used=" + used);
    }
    // Nothing shorter than nine bytes can repay five bytes of framing with a third to spare.
    for (int used = 0; used < 9; used++) {
      assertTrue(!HOTLeafPayloadCodec.keepsLz4(used, 1), "used=" + used);
    }
    assertTrue(!HOTLeafPayloadCodec.keepsLz4(Integer.MAX_VALUE, Integer.MAX_VALUE), "no overflow at the extremes");
    assertTrue(HOTLeafPayloadCodec.keepsLz4(Integer.MAX_VALUE, 1));
  }

  @Test
  void theWriterAppliesTheRuleToTheBlockNativeLz4Produces() {
    assumeTrue(FFILz4Compressor.isNativeAvailable());
    final FFILz4Compressor compressor = new FFILz4Compressor();
    final int used = 300;
    final int largestKept = used - HOTLeafPayloadCodec.FRAME_OVERHEAD - (used + 2) / 3;
    final SplittableRandom random = new SplittableRandom(0x7e1ea5e);
    final boolean[] witnessed = new boolean[3]; // one short of the boundary, on it, one past it
    try (Arena arena = Arena.ofConfined();
        MemorySegmentBytesOut raw = new MemorySegmentBytesOut();
        MemorySegmentBytesOut written = new MemorySegmentBytesOut()) {
      final MemorySegment input = arena.allocate(used);
      final MemorySegment output = arena.allocate(compressor.compressBound(used));
      // An incompressible prefix followed by zeros: each prefix byte adds about one stored byte, so
      // the sweep crosses the boundary in single steps.
      for (int prefix = 0; prefix <= used; prefix++) {
        final byte[] payload = new byte[used];
        for (int i = 0; i < prefix; i++) {
          payload[i] = (byte) (1 + random.nextInt(255));
        }
        input.copyFrom(MemorySegment.ofArray(payload));
        final int stored = compressor.compressSegmentBounded(input, used, output, (int) output.byteSize());
        raw.clear();
        written.clear();
        writeHeader(raw, (byte) 0, 0, used);
        raw.write(payload);
        HOTLeafPayloadCodec.writePage(written, raw.toByteArray(), (int) raw.position());
        final byte[] wire = written.toByteArray();
        if (stored <= largestKept) {
          assertEquals(2, wire[2], "stored=" + stored + " saves a third and must be written as LZ4");
          assertEquals(HOTLeafPayloadCodec.CODEC_LZ4, wire[23]);
          assertEquals(19 + HOTLeafPayloadCodec.FRAME_OVERHEAD + stored, wire.length);
          assertArrayEquals(raw.toByteArray(), HOTLeafPayloadCodec.expandImage(wire, 1));
        } else {
          assertArrayEquals(raw.toByteArray(), wire, "stored=" + stored + " saves less than a third: raw");
        }
        if (Math.abs(stored - largestKept) <= 1) {
          witnessed[stored - largestKept + 1] = true;
        }
      }
    }
    assertTrue(witnessed[0] && witnessed[1] && witnessed[2],
        "native LZ4 must supply a block one byte under, on and one byte over the boundary: "
            + Arrays.toString(witnessed));
  }

  @Test
  void theWriterNeverEmitsZeroRun() {
    // Payloads zero-run encodes well, including the 100-byte fixture on which both codecs used to
    // tie at 15 bytes: every page is LZ4 or raw.
    for (int prefix = 0; prefix <= 100; prefix++) {
      final byte[] payload = new byte[100];
      Arrays.fill(payload, 0, prefix, (byte) 4);
      Arrays.fill(payload, 98, 100, (byte) 4);
      try (MemorySegmentBytesOut raw = new MemorySegmentBytesOut();
          MemorySegmentBytesOut written = new MemorySegmentBytesOut()) {
        writeHeader(raw, (byte) 0, 0, payload.length);
        raw.write(payload);
        HOTLeafPayloadCodec.writePage(written, raw.toByteArray(), (int) raw.position());
        final byte[] wire = written.toByteArray();
        if ((wire[2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) == 0) {
          assertArrayEquals(raw.toByteArray(), wire, "prefix=" + prefix);
        } else {
          assertEquals(HOTLeafPayloadCodec.CODEC_LZ4, wire[23], "prefix=" + prefix);
          assertArrayEquals(raw.toByteArray(), HOTLeafPayloadCodec.expandImage(wire, 1), "prefix=" + prefix);
        }
      }
    }
  }

  @Test
  void aHostWithoutNativeLz4WritesRawPages() {
    try (MockedStatic<FFILz4Compressor> codec = mockStatic(FFILz4Compressor.class, CALLS_REAL_METHODS);
        MemorySegmentBytesOut raw = new MemorySegmentBytesOut();
        MemorySegmentBytesOut written = new MemorySegmentBytesOut()) {
      codec.when(FFILz4Compressor::isNativeAvailable).thenReturn(false);
      writeHeader(raw, (byte) 0, 0, 4096);
      raw.write(new byte[4096]);
      HOTLeafPayloadCodec.writePage(written, raw.toByteArray(), (int) raw.position());
      assertArrayEquals(raw.toByteArray(), written.toByteArray(), "4,096 zero bytes stay raw without native LZ4");
    }
  }

  @Test
  void tinyAndEmptyPayloadsStayRaw() {
    for (final int used : new int[] {0, 1, 4, 5, 6, 7, 8, 9, 12}) {
      try (MemorySegmentBytesOut raw = new MemorySegmentBytesOut();
          MemorySegmentBytesOut written = new MemorySegmentBytesOut()) {
        raw.writeByte((byte) 12);
        PageKind.writeVersionAndFlags(raw);
        raw.writeByte((byte) 1); // page key varint
        raw.writeInt(1);
        raw.writeByte(IndexType.PROJECTION.getID());
        raw.writeShort((short) 0);
        raw.writeInt(0);
        raw.writeInt(used);
        raw.write(new byte[used]);
        HOTLeafPayloadCodec.writePage(written, raw.toByteArray(), (int) raw.position());
        // LZ4 cannot shorten fewer than thirteen bytes, so none of these can save a third.
        assertArrayEquals(raw.toByteArray(), written.toByteArray(), "used=" + used);
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
          // Raw, then the same page under each codec a reader accepts.
          for (final int form : new int[] {-1, HOTLeafPayloadCodec.CODEC_ZERO_RUN, HOTLeafPayloadCodec.CODEC_LZ4}) {
            final boolean encoded = form >= 0;
            sink.clear();
            PageKind.serializeRawHOTLeaf(config, sink, original);
            final byte[] bytes = encoded
                ? encodedImage(sink.toByteArray(), form)
                : sink.toByteArray();
            assertEquals(encoded, (bytes[2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) != 0);
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
    for (final int codec : CODECS) {
      try (HOTLeafPage original = new HOTLeafPage(1, 1, IndexType.PROJECTION);
          MemorySegmentBytesOut sink = new MemorySegmentBytesOut();
          Arena arena = Arena.ofConfined()) {
        final byte[] value = new byte[1024];
        assertTrue(original.put(new byte[] {7}, value));
        PageKind.serializeRawHOTLeaf(CONFIG, sink, original);
        final byte[] bytes = encodedImage(sink.toByteArray(), codec);
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
  }

  @Test
  void sparseFragmentsAndMixedStreamsReadExactlyOneImage() throws Exception {
    final ResourceConfiguration sliding =
        new ResourceConfiguration.Builder("codec-sparse").versioningApproach(VersioningType.SLIDING_SNAPSHOT).build();
    // The page as a writer with compression switched on stores it, and under each codec a reader
    // accepts.
    for (final int form : new int[] {-1, HOTLeafPayloadCodec.CODEC_ZERO_RUN, HOTLeafPayloadCodec.CODEC_LZ4}) {
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
          if (form < 0) {
            PageKind.serializeHOTLeaf(sliding, sink, changed, true);
            // 512 zero bytes save far more than a third wherever LZ4 can be written.
            assertEquals(FFILz4Compressor.isNativeAvailable()
                ? 3
                : 1, sink.toByteArray()[2]);
          } else {
            sink.write(encodedImage(rawFragment, form));
            assertEquals(3, sink.toByteArray()[2]);
          }
          final int boundary = (int) sink.position();
          final byte[] first = sink.toByteArray();
          assertArrayEquals(rawFragment, (first[2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) == 0
              ? first
              : HOTLeafPayloadCodec.expandImage(first, 1));
          PageKind.serializeRawHOTLeaf(CONFIG, sink, base);
          final ByteArrayBytesIn source = new ByteArrayBytesIn(sink.toByteArray());
          try (HOTLeafPage firstPage =
              (HOTLeafPage) new PagePersister().deserializePage(sliding, source, SerializationType.DATA)) {
            assertEquals(1, firstPage.getEntryCount());
            assertArrayEquals(new byte[] {2}, firstPage.getKey(0));
            assertArrayEquals(new byte[512], firstPage.getValue(0));
            assertTrue(firstPage.isCompleteDump());
            assertEquals(123, firstPage.getPageReference(7).getKey());
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
        PageKind.serializeHOTLeaf(CONFIG, sink, original, true);
        final byte[] written = sink.toByteArray();
        assertTrue(written.length <= raw.length);
        assertArrayEquals(raw, (written[2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) == 0
            ? written
            : HOTLeafPayloadCodec.expandImage(written, 1),
            "cached images must have exactly the legacy raw bytes after one cold expansion");
        if (pattern != 2 && FFILz4Compressor.isNativeAvailable()) {
          assertEquals(3, written[2], "zeros and a short repeat save far more than a third");
          assertTrue(written.length < raw.length / 2);
        } else {
          assertArrayEquals(raw, written, "random bytes, or no native LZ4: the page is written raw");
        }
        for (final byte[] wire : new byte[][] {raw, written, encodedImage(raw, HOTLeafPayloadCodec.CODEC_ZERO_RUN),
            encodedImage(raw, HOTLeafPayloadCodec.CODEC_LZ4)}) {
          assertDecodedLeaf(original, wire);
          assertSelectedEntries(original, wire);
        }
      } finally {
        original.close();
      }
    }
  }

  /**
   * The raw image with its slot payload stored under {@code codec}, whatever this host's writer would
   * choose. Readers must accept zero-run frames, which no writer emits any more, and LZ4 frames on
   * hosts that cannot write them: there the block is a literal-only one, which is valid LZ4 and needs
   * no compressor.
   */
  private static byte[] encodedImage(final byte[] raw, final int codec) {
    final ByteArrayBytesIn header = new ByteArrayBytesIn(raw);
    header.position(3);
    Utils.getVarLong(header);
    header.readInt();
    header.readByte();
    header.skip(Short.toUnsignedInt(header.readShort()));
    final int count = header.readInt() & Integer.MAX_VALUE;
    final int used = header.readInt();
    header.skip((long) count * Integer.BYTES);
    final int slots = (int) header.position();
    final byte[] payload;
    final int stored;
    if (codec == HOTLeafPayloadCodec.CODEC_ZERO_RUN) {
      payload = new byte[ZeroRunByteCodec.maxEncodedSize(used)];
      stored = ZeroRunByteCodec.encode(raw, slots, used, payload, 0);
    } else if (FFILz4Compressor.isNativeAvailable()) {
      final FFILz4Compressor compressor = new FFILz4Compressor();
      try (Arena arena = Arena.ofConfined()) {
        final MemorySegment input = arena.allocate(Math.max(1, used));
        MemorySegment.copy(raw, slots, input, ValueLayout.JAVA_BYTE, 0, used);
        final MemorySegment output = arena.allocate(compressor.compressBound(used));
        stored = compressor.compressSegmentBounded(input, used, output, (int) output.byteSize());
        assertTrue(stored > 0, "native LZ4 must produce a block");
        payload = output.asSlice(0, stored).toArray(ValueLayout.JAVA_BYTE);
      }
    } else {
      try (MemorySegmentBytesOut block = new MemorySegmentBytesOut()) {
        block.writeByte((byte) (Math.min(used, 15) << 4));
        for (int rest = used - 15; rest >= 0; rest -= 255) {
          block.writeByte((byte) Math.min(rest, 255));
        }
        block.write(raw, slots, used);
        payload = block.toByteArray();
        stored = payload.length;
      }
    }
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      sink.write(raw, 0, 2);
      sink.writeByte((byte) (raw[2] | HOTLeafPayloadCodec.FLAG_COMPRESSED));
      sink.write(raw, 3, slots - 3);
      sink.writeInt(stored);
      sink.writeByte((byte) codec);
      sink.write(payload, 0, stored);
      sink.write(raw, slots + used, raw.length - slots - used);
      return sink.toByteArray();
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
