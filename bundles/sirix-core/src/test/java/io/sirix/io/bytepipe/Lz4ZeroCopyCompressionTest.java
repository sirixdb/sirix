package io.sirix.io.bytepipe;

import io.sirix.node.MemorySegmentBytesOut;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class Lz4ZeroCopyCompressionTest {

  @Test
  void nativeFFIZeroCopyRoundTrip() {
    assumeTrue(FFILz4Compressor.isNativeAvailable());

    final byte[] payload = new byte[1024];
    new Random(1).nextBytes(payload);

    final var pipeline = new ByteHandlerPipeline(new FFILz4Compressor());
    final MemorySegment compressed = pipeline.compress(MemorySegment.ofArray(payload));

    try (var result = pipeline.decompressScoped(compressed)) {
      final byte[] decompressed = new byte[payload.length];
      MemorySegment.copy(result.segment(), 0, MemorySegment.ofArray(decompressed), 0, decompressed.length);
      assertArrayEquals(payload, decompressed);
    }
  }

  @Test
  void streamFallbackRoundTrip() throws Exception {
    final byte[] payload = new byte[2048];
    new Random(2).nextBytes(payload);

    final var pipeline = new ByteHandlerPipeline(new LZ4Compressor());

    final byte[] compressed;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         net.jpountz.lz4.LZ4BlockOutputStream lzOut = new net.jpountz.lz4.LZ4BlockOutputStream(out)) {
      lzOut.write(payload);
      lzOut.finish();
      compressed = out.toByteArray();
    }

    final byte[] decompressed;
    try (InputStream in = pipeline.deserialize(new ByteArrayInputStream(compressed))) {
      decompressed = in.readAllBytes();
    }

    assertArrayEquals(payload, decompressed);
  }

  @Test
  void memorySegmentBytesOutCopiesSegment() {
    final byte[] payload = new byte[]{1, 2, 3, 4};
    final MemorySegment segment = MemorySegment.ofArray(payload);

    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut()) {
      out.write(segment);
      assertArrayEquals(payload, out.bytesForRead().toByteArray());
    }
  }
}

