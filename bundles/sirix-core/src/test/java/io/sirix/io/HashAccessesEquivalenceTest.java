/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the FORMAT-STABILITY contract of {@link HashAccesses}: the Unsafe-free accesses must produce
 * hashes bit-identical to the library's default access, because every checksum already persisted in
 * a sirix data file was computed through that default. A single differing bit here means existing
 * resources fail verification on open. The oracle is the library's own {@code hashBytes} (the
 * Unsafe path), exercised across every length stripe the XXH3 implementation switches on (empty,
 * &le;16, 17–128, 129–240, and long inputs spanning multiple blocks) plus unaligned offsets, and
 * across byte[], heap-segment and native-segment shapes.
 */
final class HashAccessesEquivalenceTest {

  private static final LongHashFunction XX3 = LongHashFunction.xx3();

  /** Every XXH3 stripe boundary, its neighbours, and block-spanning sizes. */
  private static final int[] LENGTHS = {0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 175,
      239, 240, 241, 511, 512, 1024, 1025, 4096, 65_536, 65_537, 1 << 20};

  @Test
  @DisplayName("byte[] hashes through the VarHandle access match the library default bit-for-bit")
  void byteArrayAccessMatchesDefault() {
    final Random random = new Random(0x5151C5_1EEDL);
    for (final int length : LENGTHS) {
      for (final int offset : new int[] {0, 1, 7, 13}) {
        final byte[] buffer = new byte[offset + length + 3];
        random.nextBytes(buffer);
        final long expected = XX3.hashBytes(buffer, offset, length);
        assertEquals(expected, XX3.hash(buffer, HashAccesses.BYTES, offset, length),
            "length=" + length + " offset=" + offset);
      }
    }
  }

  @Test
  @DisplayName("heap, native, and mmap-offset segment hashes match byte[] bit-for-bit")
  void segmentAccessMatchesDefault() {
    final Random random = new Random(0xD1C7_F007L);
    try (Arena arena = Arena.ofConfined()) {
      for (final int length : LENGTHS) {
        for (final int offset : new int[] {0, 1, 4, 7}) {
          final byte[] bytes = new byte[offset + length + 3];
          random.nextBytes(bytes);
          final long expected = XX3.hashBytes(bytes, offset, length);

          final MemorySegment heap = MemorySegment.ofArray(bytes).asSlice(offset, length);
          assertEquals(expected, XX3.hash(heap, HashAccesses.SEGMENT, 0, length),
              "heap length=" + length + " offset=" + offset);

          final MemorySegment nativeBuffer = arena.allocate(Math.max(bytes.length, 1));
          MemorySegment.copy(bytes, 0, nativeBuffer, ValueLayout.JAVA_BYTE, 0, bytes.length);
          final MemorySegment nativeSlice = nativeBuffer.asSlice(offset, length);
          assertEquals(expected, XX3.hash(nativeSlice, HashAccesses.SEGMENT, 0, length),
              "native length=" + length + " offset=" + offset);

          final ByteBuffer directBuffer = ByteBuffer.allocateDirect(Math.max(bytes.length, 1));
          directBuffer.put(bytes).position(offset).limit(offset + length);
          assertEquals(expected, PageHasher.computeLong(directBuffer, HashAlgorithm.XXH3),
              "direct-buffer kernel length=" + length + " offset=" + offset);
          assertEquals(offset, directBuffer.position(), "hashing must not change the buffer position");
          assertEquals(offset + length, directBuffer.limit(), "hashing must not change the buffer limit");
        }
      }
    }
  }

  @Test
  @DisplayName("HashAlgorithm.XXH3 answers identically across all three input shapes")
  void hashAlgorithmShapesAgree() {
    final Random random = new Random(0xA1607_1774L);
    try (Arena arena = Arena.ofConfined()) {
      for (final int length : LENGTHS) {
        final byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        final long fromArray = HashAlgorithm.XXH3.computeHashLong(bytes);
        assertEquals(XX3.hashBytes(bytes), fromArray, "array vs library, length=" + length);
        assertEquals(fromArray, HashAlgorithm.XXH3.computeHashLong(MemorySegment.ofArray(bytes)),
            "heap segment, length=" + length);
        final MemorySegment nativeSegment = arena.allocate(Math.max(length, 1));
        MemorySegment.copy(bytes, 0, nativeSegment, ValueLayout.JAVA_BYTE, 0, length);
        assertEquals(fromArray, HashAlgorithm.XXH3.computeHashLong(nativeSegment.asSlice(0, length)),
            "native segment, length=" + length);
      }
    }
  }

  @Test
  @DisplayName("real mmap payload slices after a four-byte length header retain checksum format")
  void realMmapPayloadOffsetsMatchPersistedFormat(@TempDir final Path tempDir) throws IOException {
    final int maxLength = LENGTHS[LENGTHS.length - 1];
    final byte[] fileBytes = new byte[Integer.BYTES + maxLength + 3];
    new Random(0x4d4d_4150L).nextBytes(fileBytes);
    final Path file = tempDir.resolve("hash-offset-pages");
    Files.write(file, fileBytes);

    try (Arena arena = Arena.ofConfined(); FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      final MemorySegment mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0L, fileBytes.length, arena);
      for (final int length : LENGTHS) {
        final long expected = XX3.hashBytes(fileBytes, Integer.BYTES, length);
        assertEquals(expected, HashAlgorithm.XXH3.computeHashLong(mapped.asSlice(Integer.BYTES, length)),
            "mmap payload length=" + length + " offset=" + Integer.BYTES);
      }
    }
  }

  @Test
  @DisplayName("native hashing rejects a closed segment before passing its address to openhft")
  void nativeHasherRejectsClosedSegment() {
    final MemorySegment closedSegment;
    try (Arena arena = Arena.ofConfined()) {
      closedSegment = arena.allocate(32);
    }

    assertThrows(IllegalStateException.class, () -> HashAlgorithm.XXH3.computeHashLong(closedSegment));
  }

  @Test
  @DisplayName("native hashing rejects a segment confined to another thread")
  void nativeHasherRejectsWrongThreadSegment() throws Exception {
    final CompletableFuture<MemorySegment> publishedSegment = new CompletableFuture<>();
    final CountDownLatch releaseOwner = new CountDownLatch(1);
    final Thread owner = new Thread(() -> {
      try (Arena arena = Arena.ofConfined()) {
        publishedSegment.complete(arena.allocate(32));
        releaseOwner.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        publishedSegment.completeExceptionally(e);
      } catch (final Throwable throwable) {
        publishedSegment.completeExceptionally(throwable);
      }
    }, "confined-hash-segment-owner");
    owner.start();

    try {
      final MemorySegment segment = publishedSegment.get(5, TimeUnit.SECONDS);
      assertThrows(WrongThreadException.class, () -> HashAlgorithm.XXH3.computeHashLong(segment));
    } finally {
      releaseOwner.countDown();
      owner.join();
    }
  }

  @Test
  @DisplayName("concurrent direct-buffer hashes are deterministic")
  void concurrentDirectBufferHashesAreDeterministic() throws Exception {
    final int workers = 8;
    final int iterations = 10_000;
    final List<Callable<Void>> tasks = new ArrayList<>(workers);
    for (int worker = 0; worker < workers; worker++) {
      final int[] pageLikeLengths = {10_151, 12_018, 12_129, 64 * 1024, 128 * 1024};
      final byte[] bytes = new byte[pageLikeLengths[worker % pageLikeLengths.length]];
      new Random(0xC0FFEE00L + worker).nextBytes(bytes);
      final long expected = XX3.hashBytes(bytes);
      final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
      buffer.put(bytes).flip();
      tasks.add(() -> {
        for (int iteration = 0; iteration < iterations; iteration++) {
          assertEquals(expected, PageHasher.computeLong(buffer, HashAlgorithm.XXH3),
              "iteration=" + iteration + " thread=" + Thread.currentThread().getName());
        }
        return null;
      });
    }

    try (var executor = Executors.newFixedThreadPool(workers)) {
      for (final var future : executor.invokeAll(tasks)) {
        future.get();
      }
    }
  }
}
