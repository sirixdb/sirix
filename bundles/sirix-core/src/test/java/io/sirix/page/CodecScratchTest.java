/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.BinaryEncodingVersion;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ScratchStats;
import io.sirix.cache.ScratchUse;
import io.sirix.exception.SirixIOException;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.node.Bytes;
import io.sirix.node.MemorySegmentBytesOut;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The codecs' native staging is borrowed from the allocator per call and returned with it: the LZ77
 * decoder's landing area for an output without tail slack, the overflow-payload decoder's, and the
 * native copies LZ4's foreign calls need of heap inputs. None of them keeps a buffer per thread or
 * allocates outside the allocator's budget any more.
 *
 * <p>
 * The assertions are deltas of the process allocator's counters for {@link ScratchUse#CODEC} and of
 * its lent-buffer gauge, so the class runs isolated.
 */
@Isolated
final class CodecScratchTest {

  private static final ResourceConfiguration CONFIG = ResourceConfiguration.newBuilder("codec-scratch").build();

  @BeforeAll
  static void initializeAllocator() {
    assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance()).init(8L * 1024 * 1024 * 1024);
  }

  private static ScratchStats stats() {
    return Allocators.getInstance().scratchStats();
  }

  private static long borrows(final ScratchStats stats) {
    return stats.acquisitions(ScratchUse.CODEC);
  }

  @Test
  void anLz77DecodeWithoutTailSlackLandsInBorrowedScratchAndOneWithSlackBorrowsNothing() {
    final byte[] raw = compressible(10_000);
    final MemorySegment input = nativeCopy(raw);
    final byte[] encoded = new byte[SirixLZ77Codec.maxEncodedSize(raw.length)];
    final int encodedLength = SirixLZ77Codec.encode(input, 0L, raw.length, encoded, 0);
    assertTrue(encodedLength < raw.length, "the fixture must compress, or the decoder copies literals only");

    final ScratchStats start = stats();
    final long lent = start.totalLentBuffers();

    final MemorySegment exact = Arena.ofAuto().allocate(raw.length);
    assertEquals(raw.length, SirixLZ77Codec.decode(encoded, 0, encodedLength, exact, 0L));
    assertArrayEquals(raw, exact.toArray(ValueLayout.JAVA_BYTE));
    final ScratchStats slack = stats();
    assertEquals(1, borrows(slack) - borrows(start), "an exact-length output is decoded through a landing area");
    assertEquals(lent, slack.totalLentBuffers(), "which is returned with the call");

    final MemorySegment roomy = Arena.ofAuto().allocate(raw.length + 4096L);
    assertEquals(raw.length, SirixLZ77Codec.decode(encoded, 0, encodedLength, roomy, 0L));
    assertArrayEquals(raw, roomy.asSlice(0, raw.length).toArray(ValueLayout.JAVA_BYTE));
    assertEquals(0, borrows(stats()) - borrows(slack), "an output with tail slack is decoded in place");
  }

  @Test
  void lz4CopiesHeapInputsIntoBorrowedScratchForItsForeignCalls() {
    assumeTrue(FFILz4Compressor.isNativeAvailable(), "native LZ4 is not installed");
    final FFILz4Compressor lz4 = new FFILz4Compressor();
    final byte[] raw = compressible(50_000);

    final ScratchStats start = stats();
    final long lent = start.totalLentBuffers();

    final MemorySegment fromHeap = lz4.compress(MemorySegment.ofArray(raw));
    final ScratchStats heapCompressed = stats();
    assertEquals(2, borrows(heapCompressed) - borrows(start),
        "a native copy of the heap source, and the output staging");
    assertEquals(lent, heapCompressed.totalLentBuffers());

    final MemorySegment fromNative = lz4.compress(nativeCopy(raw));
    final ScratchStats nativeCompressed = stats();
    assertEquals(1, borrows(nativeCompressed) - borrows(heapCompressed),
        "a native source needs the output staging only");
    assertEquals(lent, nativeCompressed.totalLentBuffers());
    assertArrayEquals(fromHeap.toArray(ValueLayout.JAVA_BYTE), fromNative.toArray(ValueLayout.JAVA_BYTE));
    assertTrue(fromHeap.byteSize() < raw.length, "the fixture must compress");

    final byte[] compressed = fromHeap.toArray(ValueLayout.JAVA_BYTE);
    try (ByteHandler.DecompressionResult result = lz4.decompressScoped(MemorySegment.ofArray(compressed))) {
      assertArrayEquals(raw, result.segment().toArray(ValueLayout.JAVA_BYTE));
      final ScratchStats decompressed = stats();
      assertEquals(1, borrows(decompressed) - borrows(nativeCompressed), "a native copy of the heap input");
      assertEquals(lent, decompressed.totalLentBuffers(), "returned although the decompressed page is still open");
    }

    assertEquals(0, stats().staleReleases() - start.staleReleases());
  }

  @Test
  void failedLz4DecodeReturnsBothBorrows() {
    final FFILz4Compressor lz4 = new FFILz4Compressor();
    final byte[] raw = compressible(50_000);
    final byte[] encoded = lz4.compress(MemorySegment.ofArray(raw)).toArray(ValueLayout.JAVA_BYTE);
    assumeTrue(ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN).getInt() > 0, "fixture uses compressed LZ4");
    try (ByteHandler.DecompressionResult warm = lz4.decompressScoped(MemorySegment.ofArray(encoded))) {
      assertArrayEquals(raw, warm.segment().toArray(ValueLayout.JAVA_BYTE));
    }
    final FrameSlotAllocator allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    final long committed = allocator.getPhysicalMemoryBytes();
    final long active = allocator.getActiveMemoryBytes();
    final ScratchStats before = stats();
    final OutOfMemoryError failure = new OutOfMemoryError("injected decoder failure");
    final FFILz4Compressor failing = spy(new FFILz4Compressor());
    doThrow(failure).when(failing).decompressSegment(any(MemorySegment.class), any(MemorySegment.class), anyInt());
    doThrow(failure).when(failing).decompressSegmentFast(any(MemorySegment.class), any(MemorySegment.class), anyInt());
    assertSame(failure,
        assertThrows(OutOfMemoryError.class, () -> failing.decompressScoped(MemorySegment.ofArray(encoded))));
    assertEquals(before.lentBytes(), stats().lentBytes());
    assertEquals(active, allocator.getActiveMemoryBytes());
    assertEquals(committed, allocator.getPhysicalMemoryBytes());
    try (ByteHandler.DecompressionResult following = lz4.decompressScoped(MemorySegment.ofArray(encoded))) {
      assertArrayEquals(raw, following.segment().toArray(ValueLayout.JAVA_BYTE));
    }
    assertEquals(before.lentBytes(), stats().lentBytes());
  }

  @Test
  void aRefusedLz4PayloadReturnsItsInputCopy() {
    assumeTrue(FFILz4Compressor.isNativeAvailable(), "native LZ4 is not installed");
    // The fast decompressor trusts the declared size and reads accordingly; only the safe one can be
    // handed a payload that lies about it.
    assumeFalse(Boolean.getBoolean("sirix.lz4.fast.decompress"), "the fast decompressor cannot refuse a payload");
    final FFILz4Compressor lz4 = new FFILz4Compressor();
    final byte[] raw = compressible(50_000);
    final byte[] lying = lz4.compress(MemorySegment.ofArray(raw)).toArray(ValueLayout.JAVA_BYTE);
    // The header declares one byte more than the payload decompresses to.
    ByteBuffer.wrap(lying).order(ByteOrder.LITTLE_ENDIAN).putInt(0, raw.length + 1);

    final ScratchStats start = stats();
    final long activeBefore =
        assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance()).getActiveMemoryBytes();
    assertThrows(SirixIOException.class, () -> lz4.decompressScoped(MemorySegment.ofArray(lying)));
    final ScratchStats refused = stats();
    assertEquals(1, borrows(refused) - borrows(start), "the refused payload had its native input copy");
    assertEquals(start.totalLentBuffers(), refused.totalLentBuffers(), "which went back on the way out");
    assertEquals(activeBefore,
        assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance()).getActiveMemoryBytes(),
        "together with the frame the page would have been decompressed into");
    assertEquals(0, refused.staleReleases() - start.staleReleases());
  }

  @Test
  void aCompressedOverflowPayloadIsDecodedThroughBorrowedScratch() throws IOException {
    final byte[] payload = runs(100_000);
    final PagePersister persister = new PagePersister();
    // The frame is built by hand with the LZ77 payload codec, so the test does not depend on which
    // codec the writer elects for this payload.
    final byte[] encoded = new byte[SirixLZ77Codec.maxEncodedSize(payload.length)];
    final int encodedLength = SirixLZ77Codec.encode(nativeCopy(payload), 0L, payload.length, encoded, 0);
    final byte[] wire;
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(1024)) {
      sink.writeByte((byte) 9); // OverflowPage page-kind id.
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());
      sink.writeByte((byte) 1); // Compressed overflow payload flag.
      sink.writeInt(payload.length);
      sink.writeInt(encodedLength);
      sink.writeByte((byte) 3); // LZ77 payload codec.
      sink.write(encoded, 0, encodedLength);
      wire = sink.toByteArray();
    }

    final ScratchStats start = stats();
    final long lent = start.totalLentBuffers();
    try (OverflowPage read = assertInstanceOf(OverflowPage.class,
        persister.deserializePage(CONFIG, Bytes.wrapForRead(nativeCopy(wire)), SerializationType.DATA))) {
      assertArrayEquals(payload, read.getDataBytes());
      final ScratchStats decoded = stats();
      assertEquals(1, borrows(decoded) - borrows(start), "one landing area for the payload");
      assertEquals(lent, decoded.totalLentBuffers(), "returned although the page that owns the bytes is open");
    }

    // A payload cut short inside its compressed bytes is refused; the landing area goes back.
    final byte[] truncated = Arrays.copyOf(wire, wire.length - 16);
    assertThrows(RuntimeException.class,
        () -> persister.deserializePage(CONFIG, Bytes.wrapForRead(nativeCopy(truncated)), SerializationType.DATA));
    assertEquals(lent, stats().totalLentBuffers(), "a refused payload returned its landing area");
  }

  private static MemorySegment nativeCopy(final byte[] bytes) {
    final MemorySegment segment = Arena.ofAuto().allocate(bytes.length);
    MemorySegment.copy(bytes, 0, segment, ValueLayout.JAVA_BYTE, 0L, bytes.length);
    return segment;
  }

  /** Runs of one value, 1,024 bytes each: a payload any codec shrinks to a fraction. */
  private static byte[] runs(final int length) {
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (i >>> 10);
    }
    return bytes;
  }

  /** Text-like bytes: long repeats with a varying tail, so every codec finds matches. */
  private static byte[] compressible(final int length) {
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) ("the quick brown fox ".charAt(i % 20) + (i / 4096));
    }
    return bytes;
  }
}
