/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ScratchStats;
import io.sirix.cache.ScratchUse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The import scanner and the chunks it hands out.
 *
 * <p>
 * A chunk's bytes are 256 KiB slabs of allocator scratch, borrowed as the chunk fills and returned
 * by whoever consumed it. The slab assertions are deltas of the process allocator's counters for
 * {@link ScratchUse#IMPORT} and of its lent-buffer gauge, so the class runs isolated.
 */
@Isolated
final class FusedSliceAndScanTest {

  @BeforeAll
  static void initializeAllocator() {
    assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance()).init(8L * 1024 * 1024 * 1024);
  }

  private static ScratchStats stats() {
    return Allocators.getInstance().scratchStats();
  }

  private static long borrows(final ScratchStats stats) {
    return stats.acquisitions(ScratchUse.IMPORT);
  }

  @Test
  void chunkBufferStreamsAcrossSlabsItBorrowsAsItFillsAndReturnsOnRelease() throws IOException {
    final int slabBytes = FusedSliceAndScan.ChunkBuffer.SLAB_BYTES;
    final byte[] first = patternedBytes(slabBytes + 37, 11);
    final byte[] second = patternedBytes(slabBytes * 2 + 19, 73);
    final byte[] expected = new byte[first.length + second.length];
    System.arraycopy(first, 0, expected, 0, first.length);
    System.arraycopy(second, 0, expected, first.length, second.length);

    final ScratchStats before = stats();
    final FusedSliceAndScan.ChunkBuffer buffer = new FusedSliceAndScan.ChunkBuffer();
    assertEquals(0, borrows(stats()) - borrows(before), "an empty chunk holds no memory");
    buffer.append(first, 0, first.length);
    assertEquals(2, buffer.slabCount());
    buffer.append(second, 0, second.length);

    assertEquals(expected.length, buffer.length());
    assertEquals(4, buffer.slabCount(), "three slabs and 56 bytes of a fourth");
    final ScratchStats filled = stats();
    assertEquals(4, borrows(filled) - borrows(before), "one borrow per slab");
    assertEquals(4L * slabBytes, filled.requestedBytes(ScratchUse.IMPORT) - before.requestedBytes(ScratchUse.IMPORT),
        "every slab is the allocator's largest slot class");
    assertEquals(before.totalLentBuffers() + 4, filled.totalLentBuffers());
    assertEquals(expected[slabBytes - 1], buffer.byteAt(slabBytes - 1));
    assertEquals(expected[slabBytes], buffer.byteAt(slabBytes));
    assertEquals(expected[slabBytes * 3], buffer.byteAt(slabBytes * 3));
    assertArrayEquals(expected, buffer.prepareRead(expected.length).readAllBytes());

    buffer.release();
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers(), "the release returned every slab");
    assertEquals(0, buffer.slabCount());
    buffer.release();
    assertEquals(0, stats().staleReleases() - before.staleReleases(), "a repeated release returns nothing twice");
  }

  @Test
  void chunkBufferCopiesRangesThatCrossSlabs() {
    final int slabBytes = FusedSliceAndScan.ChunkBuffer.SLAB_BYTES;
    final byte[] source = patternedBytes(slabBytes * 2 + 101, 29);
    final FusedSliceAndScan.ChunkBuffer buffer = new FusedSliceAndScan.ChunkBuffer();
    try {
      buffer.append(source, 0, source.length);

      final int rangeStart = slabBytes - 23;
      final int rangeLength = slabBytes + 71;
      final byte[] copy = new byte[rangeLength + 2];
      buffer.copyTo(rangeStart, copy, 1, rangeLength);
      assertArrayEquals(Arrays.copyOfRange(source, rangeStart, rangeStart + rangeLength),
          Arrays.copyOfRange(copy, 1, 1 + rangeLength));
      assertEquals(0, copy[0]);
      assertEquals(0, copy[copy.length - 1]);
      assertThrows(IndexOutOfBoundsException.class, () -> buffer.copyTo(source.length - 1, copy, 0, 2));
    } finally {
      buffer.release();
    }
  }

  @Test
  void aReleasedChunkBufferReadsAsExhaustedAndRefusesAppends() throws IOException {
    final FusedSliceAndScan.ChunkBuffer buffer = new FusedSliceAndScan.ChunkBuffer();
    final byte[] prior = patternedBytes(FusedSliceAndScan.ChunkBuffer.SLAB_BYTES + 1, 3);
    buffer.append(prior, 0, prior.length);
    buffer.prepareRead(prior.length);
    assertEquals(prior[0] & 0xff, buffer.read());

    buffer.release();
    assertEquals(0, buffer.length());
    assertEquals(0, buffer.available());
    assertEquals(-1, buffer.read(), "the slabs are another borrower's now: nothing of them is read");
    assertEquals(-1, buffer.read(new byte[8], 0, 8));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.byteAt(0));
    assertThrows(IllegalArgumentException.class, () -> buffer.prepareRead(1));
    assertThrows(IllegalStateException.class, () -> buffer.append(prior, 0, 1));
  }

  @Test
  void aRecordLargerThanTheSchedulingTargetLeavesNothingBehind() {
    final int slabBytes = FusedSliceAndScan.ChunkBuffer.SLAB_BYTES;
    final ScratchStats before = stats();
    final FusedSliceAndScan.ChunkBuffer buffer = new FusedSliceAndScan.ChunkBuffer();
    final byte[] source = patternedBytes(slabBytes * 7, 41);
    buffer.append(source, 0, source.length);
    assertEquals(7, buffer.slabCount());
    assertEquals(before.totalLentBuffers() + 7, stats().totalLentBuffers());

    buffer.release();
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers(),
        "a chunk keeps no slab for its next use, however large it was");
  }

  @Test
  void memberLimitBoundsProjectionMetadataForTinyRecords() throws IOException {
    final byte[] json = "[0,1,2,3,4,5,6,7,8,9]".getBytes(StandardCharsets.UTF_8);
    final ScratchStats before = stats();
    long members = 0;
    int chunks = 0;
    try (FusedSliceAndScan fused = new FusedSliceAndScan(new ByteArrayInputStream(json), 1 << 20, true, 3)) {
      fused.consumeArrayOpen();
      FusedSliceAndScan.Chunk chunk;
      while ((chunk = fused.nextChunk()) != null) {
        assertTrue(chunk.members() <= 3);
        assertEquals(chunk.members(), chunk.memberNodes().length);
        members += chunk.members();
        chunks++;
        chunk.bytes().release();
      }
    }
    assertEquals(10, members);
    assertEquals(4, chunks);
    final ScratchStats after = stats();
    assertEquals(4, borrows(after) - borrows(before), "one slab per chunk of a few bytes");
    assertEquals(before.totalLentBuffers(), after.totalLentBuffers());
  }

  @Test
  void aScannerStoppedInsideAChunkReturnsTheSlabsOfTheChunkItWasFilling() throws IOException {
    // 600 KiB of members and then the input ends, inside a member and far from the chunk budget.
    final StringBuilder json = new StringBuilder("[");
    while (json.length() < 600 << 10) {
      json.append("{\"k\":\"").append("v".repeat(1000)).append("\"},");
    }
    json.append("{\"k\":\"cut");
    final ScratchStats before = stats();
    final FusedSliceAndScan fused =
        new FusedSliceAndScan(new ByteArrayInputStream(json.toString().getBytes(StandardCharsets.UTF_8)), 4 << 20);
    try {
      fused.consumeArrayOpen();
      assertThrows(IllegalStateException.class, fused::nextChunk);
      assertTrue(stats().totalLentBuffers() >= before.totalLentBuffers() + 2,
          "the fixture must leave slabs with the scanner, or closing it returns nothing");
    } finally {
      fused.close();
    }
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers(), "closing returned the unfinished chunk");
    fused.close();
    assertEquals(0, stats().staleReleases() - before.staleReleases());
  }

  /**
   * A field name is resolved in the read buffer it was scanned in; only a name a refill cuts in two
   * is read back from the chunk. Streams that deliver a few bytes per read cut every name at every
   * offset, including right after its opening quote and right before its closing one, and must
   * produce the chunks, names, paths and tallies the whole document produces.
   */
  @Test
  void namesCutByARefillResolveLikeNamesReadWhole() throws IOException {
    final StringBuilder json = new StringBuilder("[");
    final String longName = "n".repeat(300);
    for (int record = 0; record < 60; record++) {
      if (record > 0) {
        json.append(',');
      }
      if (record % 7 == 3) {
        // Another key order and another key set: the per-position prediction misses.
        json.append("{\"tags\":[\"a\",{\"id\":").append(record).append("}],\"id\":").append(record).append('}');
      } else {
        json.append("{\"id\":")
            .append(record)
            .append(",\"na\\\"me\":\"x")
            .append(record)
            .append("\",\"")
            .append(longName)
            .append("\":null,\"gr\u00fc\u00dfe\":{\"id\":true,\"tags\":[1,2]}}");
      }
    }
    json.append(']');
    final byte[] bytes = json.toString().getBytes(StandardCharsets.UTF_8);

    final List<String> whole = describeChunks(new ByteArrayInputStream(bytes), 2_000);
    assertTrue(whole.size() > 5, "the fixture must cut several chunks");
    assertTrue(
        whole.get(0).contains("na\"me") && whole.get(0).contains(longName) && whole.get(0).contains("gr\u00fc\u00dfe"),
        "the first chunk introduces the escaped, the long and the non-ASCII name: " + whole.get(0));
    for (final int bytesPerRead : new int[] {1, 2, 3, 5, 7, 13, 301}) {
      assertEquals(whole, describeChunks(new TricklingInputStream(bytes, bytesPerRead), 2_000),
          bytesPerRead + " bytes per read");
    }
  }

  /** Everything a chunk tells the coordinator, and its bytes, as one comparable line per chunk. */
  private static List<String> describeChunks(final InputStream in, final int chunkByteBudget) throws IOException {
    final ScratchStats before = stats();
    final List<String> chunks = new ArrayList<>();
    try (FusedSliceAndScan fused = new FusedSliceAndScan(in, chunkByteBudget, true)) {
      fused.consumeArrayOpen();
      FusedSliceAndScan.Chunk chunk;
      while ((chunk = fused.nextChunk()) != null) {
        final StringBuilder line = new StringBuilder();
        line.append("nodes=")
            .append(chunk.nodes())
            .append(" members=")
            .append(chunk.members())
            .append(" last=")
            .append(chunk.lastMemberNodes())
            .append(" final=")
            .append(chunk.isFinal())
            .append(" memberNodes=")
            .append(Arrays.toString(chunk.memberNodes()))
            .append(" newNames=")
            .append(chunk.newNames())
            .append(" tallies=");
        for (final int[] tally : chunk.nameTallies()) {
          line.append(Arrays.toString(tally));
        }
        line.append(" newSteps=");
        for (final FusedSliceAndScan.PathStep step : chunk.newSteps()) {
          line.append(path(step)).append(';');
        }
        line.append(" touched=");
        for (final FusedSliceAndScan.PathStep step : chunk.touchedSteps()) {
          line.append(path(step)).append(';');
        }
        line.append(" occurrences=")
            .append(Arrays.toString(chunk.stepOccurrences()))
            .append(" bytes=")
            .append(new String(chunk.bytes().prepareRead(chunk.length()).readAllBytes(), StandardCharsets.UTF_8));
        chunks.add(line.toString());
        chunk.bytes().release();
      }
    }
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers(), "the scan returned every slab");
    return chunks;
  }

  private static String path(final FusedSliceAndScan.PathStep step) {
    final StringBuilder path = new StringBuilder();
    for (FusedSliceAndScan.PathStep at = step; at != null; at = at.parent) {
      path.insert(0, at.name == null
          ? "/[]"
          : "/" + at.name);
    }
    return path.toString();
  }

  /** Delivers a fixed number of bytes per read, however many are asked for. */
  private static final class TricklingInputStream extends InputStream {
    private final byte[] bytes;
    private final int bytesPerRead;
    private int position;

    TricklingInputStream(final byte[] bytes, final int bytesPerRead) {
      this.bytes = bytes;
      this.bytesPerRead = bytesPerRead;
    }

    @Override
    public int read() {
      return position == bytes.length
          ? -1
          : bytes[position++] & 0xff;
    }

    @Override
    public int read(final byte[] destination, final int offset, final int length) {
      if (position == bytes.length) {
        return -1;
      }
      final int count = Math.min(Math.min(length, bytesPerRead), bytes.length - position);
      System.arraycopy(bytes, position, destination, offset, count);
      position += count;
      return count;
    }
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
