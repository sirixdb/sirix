package io.sirix.page;

import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.settings.Constants;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/**
 * Cold, page-owned storage for scan-visible slot images that could not be placed in the slotted
 * page's bump heap.
 *
 * <p>
 * The common page pays one nullable reference in {@link KeyValueLeafPage}; all metadata and native
 * payload storage are allocated only after the first capacity spill. Images are packed into
 * allocator-owned native chunks. A slot image never crosses a chunk boundary, which keeps every
 * accessor to one segment plus one offset and avoids a heap {@code byte[][]}.
 * </p>
 *
 * <p>
 * Mutation is a two-step prepare/publish operation. {@link #prepare(int, MemorySegment, int)}
 * copies into unpublished space and can therefore allocate or compact without changing the logical
 * slot. {@link #publish(int, long)} only changes fixed metadata and cannot allocate. No flyweight
 * is ever bound to this storage, so compaction cannot invalidate a bound node.
 * </p>
 *
 * <p>
 * This class follows {@link KeyValueLeafPage}'s single-writer mutation contract. Published pages
 * may be read concurrently, but a page must not be mutated concurrently with a scan.
 * </p>
 */
final class OverflowSlotSidecar implements AutoCloseable {

  /**
   * Largest side-slot image. An alias of the inline-record cap — a side slot holds the same record
   * shape the heap would have held — so the two can never disagree.
   */
  static final int MAX_IMAGE_BYTES = PageConstants.MAX_RECORD_SIZE;

  private static final int SLOT_COUNT = Constants.NDP_NODE_COUNT;
  private static final int BITMAP_WORDS = SLOT_COUNT >>> 6;
  private static final int INITIAL_CHUNK_BYTES = MemorySegmentAllocator.FOUR_KB;
  private static final int MAX_CHUNK_BYTES = MemorySegmentAllocator.TWO_FIFTYSIX_KB;
  private static final int MAX_LIVE_BYTES = SLOT_COUNT * MAX_IMAGE_BYTES;
  private static final int LOCATION_OFFSET_BITS = 18;
  private static final int LOCATION_OFFSET_MASK = (1 << LOCATION_OFFSET_BITS) - 1;
  private static final int NO_LOCATION = -1;

  private final MemorySegmentAllocator allocator;
  private final long[] bitmap = new long[BITMAP_WORDS];
  private int[] locations = new int[SLOT_COUNT];
  private int[] spareLocations;
  private final short[] lengths = new short[SLOT_COUNT];
  private final byte[] kinds = new byte[SLOT_COUNT];

  private MemorySegment[] chunks = new MemorySegment[4];
  private int[] chunkUsed = new int[4];
  private int chunkCount;
  private int count;
  private int liveBytes;
  private int deadBytes;
  private long retainedBytes;
  private long nextToken;
  private long pendingToken;
  private int pendingChunkIndex;
  private int pendingOffset;
  private int pendingLength;
  private int pendingKind;
  private boolean closed;

  OverflowSlotSidecar(final MemorySegmentAllocator allocator) {
    if (allocator == null) {
      throw new NullPointerException("allocator");
    }
    this.allocator = allocator;
    Arrays.fill(locations, NO_LOCATION);
  }

  boolean has(final int slot) {
    checkSlot(slot);
    return (bitmap[slot >>> 6] & (1L << slot)) != 0L;
  }

  int count() {
    return count;
  }

  long bitmapWord(final int wordIndex) {
    if (wordIndex < 0 || wordIndex >= BITMAP_WORDS) {
      throw new IndexOutOfBoundsException("wordIndex=" + wordIndex);
    }
    return bitmap[wordIndex];
  }

  int kind(final int slot) {
    return has(slot)
        ? kinds[slot] & 0xFF
        : 0;
  }

  int imageLength(final int slot) {
    return has(slot)
        ? lengths[slot] & 0xFFFF
        : 0;
  }

  MemorySegment image(final int slot) {
    if (!has(slot)) {
      return null;
    }
    final int location = locations[slot];
    return chunks[location >>> LOCATION_OFFSET_BITS].asSlice(location & LOCATION_OFFSET_MASK, lengths[slot] & 0xFFFF);
  }

  MemorySegment segment(final int slot) {
    if (!has(slot)) {
      return null;
    }
    return chunks[locations[slot] >>> LOCATION_OFFSET_BITS];
  }

  long offset(final int slot) {
    if (!has(slot)) {
      return -1L;
    }
    return locations[slot] & LOCATION_OFFSET_MASK;
  }

  void copyBitmapTo(final long[] destination) {
    if (destination == null) {
      throw new NullPointerException("destination");
    }
    if (destination.length < BITMAP_WORDS) {
      throw new IllegalArgumentException("Side-slot bitmap needs " + BITMAP_WORDS + " words");
    }
    System.arraycopy(bitmap, 0, destination, 0, BITMAP_WORDS);
  }

  /**
   * Copy an image to unpublished native storage and return an opaque token for {@link #publish}.
   */
  long prepare(final int kind, final MemorySegment image, final int length) {
    requireOpen();
    if (kind < 0 || kind > 0xFF) {
      throw new IllegalArgumentException("kind out of unsigned-byte range: " + kind);
    }
    if (image == null) {
      throw new NullPointerException("image");
    }
    if (length <= 0 || length > MAX_IMAGE_BYTES || length > image.byteSize()) {
      throw new IllegalArgumentException("side-slot image length must be 1.." + MAX_IMAGE_BYTES + ": " + length);
    }

    ensureAppendCapacity(length);
    final int chunkIndex = chunkCount - 1;
    final int offset = chunkUsed[chunkIndex];
    MemorySegment.copy(image, 0L, chunks[chunkIndex], offset, length);
    chunkUsed[chunkIndex] = offset + length;
    // Until publish claims the bytes they are ordinary append garbage. This also makes an abandoned
    // prepare token harmless and eligible for the next compaction.
    deadBytes += length;

    long token = ++nextToken;
    if (token == 0L) {
      token = ++nextToken;
    }
    pendingToken = token;
    pendingChunkIndex = chunkIndex;
    pendingOffset = offset;
    pendingLength = length;
    pendingKind = kind;
    return token;
  }

  /** Publish a previously prepared image without allocating or copying. */
  void publish(final int slot, final long token) {
    requireOpen();
    checkSlot(slot);
    if (token == 0L || token != pendingToken) {
      throw new IllegalArgumentException("not a side-slot prepare token");
    }
    final int length = pendingLength;
    final int kind = pendingKind;
    final int offset = pendingOffset;
    final int chunkIndex = pendingChunkIndex;
    if (length <= 0 || length > MAX_IMAGE_BYTES || chunkIndex >= chunkCount
        || offset + length > chunkUsed[chunkIndex]) {
      throw new IllegalStateException("corrupt side-slot prepare token");
    }
    // Consume before publishing metadata. A replay is rejected even if a later validation is added
    // below; prepare/publish is deliberately a one-shot transaction.
    pendingToken = 0L;

    final boolean replacing = (bitmap[slot >>> 6] & (1L << slot)) != 0L;
    if (replacing) {
      final int oldLength = lengths[slot] & 0xFFFF;
      liveBytes -= oldLength;
      deadBytes += oldLength;
    } else {
      bitmap[slot >>> 6] |= 1L << slot;
      count++;
    }
    locations[slot] = chunkIndex << LOCATION_OFFSET_BITS | offset;
    lengths[slot] = (short) length;
    kinds[slot] = (byte) kind;
    liveBytes += length;
    deadBytes -= length;
  }

  boolean remove(final int slot) {
    requireOpen();
    checkSlot(slot);
    final long bit = 1L << slot;
    final int word = slot >>> 6;
    if ((bitmap[word] & bit) == 0L) {
      return false;
    }
    final int oldLength = lengths[slot] & 0xFFFF;
    bitmap[word] &= ~bit;
    locations[slot] = NO_LOCATION;
    lengths[slot] = 0;
    kinds[slot] = 0;
    count--;
    liveBytes -= oldLength;
    deadBytes += oldLength;
    return true;
  }

  long retainedBytes() {
    return retainedBytes;
  }

  int liveBytes() {
    return liveBytes;
  }

  boolean isEmpty() {
    return count == 0;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    try {
      releaseChunks(chunks, chunkCount);
    } finally {
      // Even an allocator-release failure must not leave a closed sidecar exposing payload that may
      // already have been returned to the pool by an earlier successful release in the same pass.
      Arrays.fill(chunks, null);
      Arrays.fill(chunkUsed, 0);
      Arrays.fill(bitmap, 0L);
      Arrays.fill(locations, NO_LOCATION);
      if (spareLocations != null) {
        Arrays.fill(spareLocations, NO_LOCATION);
      }
      Arrays.fill(lengths, (short) 0);
      Arrays.fill(kinds, (byte) 0);
      chunkCount = 0;
      count = 0;
      liveBytes = 0;
      deadBytes = 0;
      retainedBytes = 0L;
      pendingToken = 0L;
    }
  }

  private void ensureAppendCapacity(final int length) {
    if (chunkCount != 0 && chunkUsed[chunkCount - 1] + length <= chunks[chunkCount - 1].byteSize()) {
      return;
    }

    // Reclaim append garbage before growing when it can materially change the decision. Compaction
    // builds a complete replacement first and only swaps after all allocator calls/copies succeed.
    if (deadBytes >= length && (deadBytes >= liveBytes || retainedBytes - liveBytes >= length)) {
      compact();
      if (chunkCount != 0 && chunkUsed[chunkCount - 1] + length <= chunks[chunkCount - 1].byteSize()) {
        return;
      }
    }

    final int previousSize = chunkCount == 0
        ? 0
        : (int) chunks[chunkCount - 1].byteSize();
    int requested = previousSize == 0
        ? INITIAL_CHUNK_BYTES
        : Math.min(previousSize << 1, MAX_CHUNK_BYTES);
    while (requested < length) {
      requested <<= 1;
    }
    // Near the absolute live-set ceiling, do not reserve a geometrically grown 256-KiB tail for the
    // final few KiB. The allocator rounds to its next size class, so passing the remaining logical
    // headroom still preserves aligned ownership while bounding cache weight.
    final int liveHeadroom = Math.max(length, MAX_LIVE_BYTES - liveBytes);
    requested = Math.max(INITIAL_CHUNK_BYTES, Math.min(requested, liveHeadroom));
    appendChunk(requested);
  }

  /** Compact live images into fresh chunks; the old representation stays valid on any failure. */
  private void compact() {
    if (deadBytes == 0) {
      return;
    }
    if (count == 0) {
      try {
        releaseChunks(chunks, chunkCount);
      } finally {
        Arrays.fill(chunks, null);
        Arrays.fill(chunkUsed, 0);
        chunkCount = 0;
        retainedBytes = 0L;
        deadBytes = 0;
        pendingToken = 0L;
      }
      return;
    }

    MemorySegment[] replacementChunks = new MemorySegment[4];
    int[] replacementUsed = new int[4];
    final int[] replacementLocations = spareLocations == null
        ? new int[SLOT_COUNT]
        : spareLocations;
    Arrays.fill(replacementLocations, NO_LOCATION);
    int replacementCount = 0;
    long replacementRetained = 0L;
    int packedBytes = 0;
    try {
      int nextChunkSize = INITIAL_CHUNK_BYTES;
      for (int slot = 0; slot < SLOT_COUNT; slot++) {
        if ((bitmap[slot >>> 6] & (1L << slot)) == 0L) {
          continue;
        }
        final int length = lengths[slot] & 0xFFFF;
        if (replacementCount == 0
            || replacementUsed[replacementCount - 1] + length > replacementChunks[replacementCount - 1].byteSize()) {
          while (nextChunkSize < length) {
            nextChunkSize <<= 1;
          }
          if (replacementCount == replacementChunks.length) {
            replacementChunks = Arrays.copyOf(replacementChunks, replacementCount << 1);
            replacementUsed = Arrays.copyOf(replacementUsed, replacementCount << 1);
          }
          final int remainingLiveBytes = liveBytes - packedBytes;
          final int requestedBytes = Math.max(INITIAL_CHUNK_BYTES, Math.min(nextChunkSize, remainingLiveBytes));
          final MemorySegment chunk = allocator.allocate(requestedBytes);
          replacementChunks[replacementCount++] = chunk;
          replacementRetained += chunk.byteSize();
          nextChunkSize = Math.min(nextChunkSize << 1, MAX_CHUNK_BYTES);
        }
        final int oldLocation = locations[slot];
        final MemorySegment oldChunk = chunks[oldLocation >>> LOCATION_OFFSET_BITS];
        final int oldOffset = oldLocation & LOCATION_OFFSET_MASK;
        final int newChunkIndex = replacementCount - 1;
        final int newOffset = replacementUsed[newChunkIndex];
        MemorySegment.copy(oldChunk, oldOffset, replacementChunks[newChunkIndex], newOffset, length);
        replacementUsed[newChunkIndex] = newOffset + length;
        replacementLocations[slot] = newChunkIndex << LOCATION_OFFSET_BITS | newOffset;
        packedBytes += length;
      }
    } catch (final RuntimeException | Error failure) {
      try {
        releaseChunks(replacementChunks, replacementCount);
      } catch (final Throwable cleanupFailure) {
        failure.addSuppressed(cleanupFailure);
      }
      throw failure;
    }

    final MemorySegment[] oldChunks = chunks;
    final int oldChunkCount = chunkCount;
    chunks = replacementChunks;
    chunkUsed = replacementUsed;
    chunkCount = replacementCount;
    retainedBytes = replacementRetained;
    final int[] oldLocations = locations;
    locations = replacementLocations;
    spareLocations = oldLocations;
    deadBytes = 0;
    pendingToken = 0L;
    releaseChunks(oldChunks, oldChunkCount);
  }

  private void appendChunk(final int requestedBytes) {
    if (chunkCount == 1024) {
      throw new IllegalStateException("too many side-slot payload chunks");
    }
    if (chunkCount == chunks.length) {
      chunks = Arrays.copyOf(chunks, chunkCount << 1);
      chunkUsed = Arrays.copyOf(chunkUsed, chunkCount << 1);
    }
    final MemorySegment chunk = allocator.allocate(requestedBytes);
    chunks[chunkCount] = chunk;
    chunkUsed[chunkCount] = 0;
    chunkCount++;
    retainedBytes += chunk.byteSize();
  }

  private void releaseChunks(final MemorySegment[] ownedChunks, final int ownedCount) {
    Throwable firstFailure = null;
    for (int i = 0; i < ownedCount; i++) {
      final MemorySegment chunk = ownedChunks[i];
      if (chunk != null) {
        try {
          allocator.release(chunk);
        } catch (final RuntimeException | Error failure) {
          if (firstFailure == null) {
            firstFailure = failure;
          } else {
            firstFailure.addSuppressed(failure);
          }
        }
      }
    }
    if (firstFailure instanceof RuntimeException runtimeFailure) {
      throw runtimeFailure;
    }
    if (firstFailure instanceof Error error) {
      throw error;
    }
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("side-slot storage is closed");
    }
  }

  private static void checkSlot(final int slot) {
    if (slot < 0 || slot >= SLOT_COUNT) {
      throw new IndexOutOfBoundsException("slot=" + slot);
    }
  }
}
