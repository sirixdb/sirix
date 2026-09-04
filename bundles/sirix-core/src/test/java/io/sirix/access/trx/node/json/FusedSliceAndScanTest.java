/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FusedSliceAndScanTest {

  @Test
  void chunkBufferStreamsAcrossSlabsWithoutHumongousBackingArrays() throws IOException {
    final int slabBytes = FusedSliceAndScan.ChunkBuffer.SLAB_BYTES;
    final byte[] first = patternedBytes(slabBytes + 37, 11);
    final byte[] second = patternedBytes(slabBytes * 2 + 19, 73);
    final byte[] expected = new byte[first.length + second.length];
    System.arraycopy(first, 0, expected, 0, first.length);
    System.arraycopy(second, 0, expected, first.length, second.length);

    final FusedSliceAndScan.ChunkBuffer buffer = new FusedSliceAndScan.ChunkBuffer();
    buffer.append(first, 0, first.length);
    buffer.append(second, 0, second.length);

    assertEquals(expected.length, buffer.length());
    assertEquals(expected[slabBytes - 1], buffer.byteAt(slabBytes - 1));
    assertEquals(expected[slabBytes], buffer.byteAt(slabBytes));
    assertEquals(expected[slabBytes * 3], buffer.byteAt(slabBytes * 3));
    assertTrue(buffer.largestArrayPayloadBytes() <= 256 << 10,
        "every importer chunk backing array must remain at or below 256 KiB");
    assertArrayEquals(expected, buffer.prepareRead(expected.length).readAllBytes());
  }

  @Test
  void chunkBufferMatchesAndCopiesRangesThatCrossSlabs() {
    final int slabBytes = FusedSliceAndScan.ChunkBuffer.SLAB_BYTES;
    final byte[] source = patternedBytes(slabBytes * 2 + 101, 29);
    final FusedSliceAndScan.ChunkBuffer buffer = new FusedSliceAndScan.ChunkBuffer();
    buffer.append(source, 0, source.length);

    final int rangeStart = slabBytes - 23;
    final int rangeLength = slabBytes + 71;
    final byte[] range = Arrays.copyOfRange(source, rangeStart, rangeStart + rangeLength);
    assertTrue(buffer.matches(range, rangeStart, rangeLength));
    range[range.length - 1] ^= 1;
    assertFalse(buffer.matches(range, rangeStart, rangeLength));

    final byte[] copy = new byte[rangeLength];
    buffer.copyTo(rangeStart, copy, 0, rangeLength);
    assertArrayEquals(Arrays.copyOfRange(source, rangeStart, rangeStart + rangeLength), copy);
  }

  @Test
  void clearedChunkBufferReusesItsSlabsAndResetsReadBounds() throws IOException {
    final FusedSliceAndScan.ChunkBuffer buffer = new FusedSliceAndScan.ChunkBuffer();
    final byte[] prior = patternedBytes(FusedSliceAndScan.ChunkBuffer.SLAB_BYTES + 1, 3);
    buffer.append(prior, 0, prior.length);
    buffer.prepareRead(prior.length);

    buffer.clear();
    assertEquals(0, buffer.length());
    assertEquals(-1, buffer.read());
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.byteAt(0));

    final byte[] replacement = patternedBytes(4097, 97);
    buffer.append(replacement, 0, replacement.length);
    assertArrayEquals(replacement, buffer.prepareRead(replacement.length).readAllBytes());
    assertThrows(IllegalArgumentException.class, () -> buffer.prepareRead(replacement.length + 1));
  }

  @Test
  void oversizedRecordDoesNotPermanentlyInflateAPooledChunkBuffer() {
    final int slabBytes = FusedSliceAndScan.ChunkBuffer.SLAB_BYTES;
    final FusedSliceAndScan.ChunkBuffer buffer = new FusedSliceAndScan.ChunkBuffer();
    final byte[] source = patternedBytes(slabBytes * 7, 41);
    buffer.append(source, 0, source.length);
    assertEquals(7, buffer.allocatedSlabCount());

    buffer.clear();
    buffer.trimToBytes(slabBytes * 2);
    assertEquals(2, buffer.allocatedSlabCount());
  }

  @Test
  void memberLimitBoundsProjectionMetadataForTinyRecords() throws IOException {
    final byte[] json = "[0,1,2,3,4,5,6,7,8,9]".getBytes(StandardCharsets.UTF_8);
    final FusedSliceAndScan fused = new FusedSliceAndScan(new ByteArrayInputStream(json), 1 << 20, true, 3);
    fused.consumeArrayOpen();

    long members = 0;
    int chunks = 0;
    FusedSliceAndScan.Chunk chunk;
    while ((chunk = fused.nextChunk()) != null) {
      assertTrue(chunk.members() <= 3);
      assertEquals(chunk.members(), chunk.memberNodes().length);
      members += chunk.members();
      chunks++;
      fused.releaseChunkBuffer(chunk.bytes());
    }
    assertEquals(10, members);
    assertEquals(4, chunks);
  }

  @Test
  void pendingRecordQueueReleasesConsumed256KiBBlocksInOrder() {
    final ParallelBulkJsonImporter.PendingRecordQueue queue = new ParallelBulkJsonImporter.PendingRecordQueue();
    final int blockEntries = ParallelBulkJsonImporter.PendingRecordQueue.blockEntries();
    for (int i = 0; i < blockEntries + 3; i++) {
      queue.addLast(i, i + 100L);
    }
    assertEquals(2, queue.blockCount());
    assertEquals(blockEntries + 3L, queue.size());

    for (int i = 0; i < blockEntries; i++) {
      assertEquals(i, queue.firstRoot());
      assertEquals(i + 100L, queue.firstEnd());
      queue.removeFirst();
    }
    assertEquals(1, queue.blockCount());
    assertEquals(blockEntries, queue.firstRoot());
    assertEquals(blockEntries + 2L, queue.lastRoot());

    queue.removeFirst();
    queue.removeFirst();
    queue.removeFirst();
    assertTrue(queue.isEmpty());
    assertEquals(0, queue.blockCount());
  }

  private static byte[] patternedBytes(final int length, final int seed) {
    final byte[] bytes = new byte[length];
    int value = seed;
    for (int i = 0; i < length; i++) {
      value = value * 1103515245 + 12345;
      bytes[i] = (byte) (value >>> 16);
    }
    return bytes;
  }
}
