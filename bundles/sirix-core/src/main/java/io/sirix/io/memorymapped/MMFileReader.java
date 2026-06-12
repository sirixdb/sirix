/*
 * Copyright (c) 2020, SirixDB All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.io.memorymapped;

import com.github.benmanes.caffeine.cache.Cache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.io.AbstractReader;
import io.sirix.io.IOStorage;
import io.sirix.io.RevisionFileData;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Reader, to read from a memory-mapped file.
 *
 * @author Johannes Lichtenberger
 */
public final class MMFileReader extends AbstractReader {

  static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
  static final ValueLayout.OfInt LAYOUT_INT = ValueLayout.JAVA_INT;
  /** The revisions records and beacon trailers are pinned little-endian. */
  static final ValueLayout.OfLong LAYOUT_LONG_LE =
      ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

  private final MemorySegment dataFileSegment;

  private final MemorySegment revisionsOffsetFileSegment;

  private final Cache<Integer, RevisionFileData> cache;

  /**
   * Arena for memory-mapped segments. May be null if the arena is managed externally (e.g., by
   * MMStorage for shared segments).
   */
  @Nullable
  private final Arena arena;

  /**
   * Arena generation for reference counting. Used when managed by MMStorage.
   */
  private final MMStorage.@Nullable ArenaGeneration generation;

  /**
   * Reference to the storage for releasing the generation. Used when managed by MMStorage.
   */
  @Nullable
  private final MMStorage storage;

  /**
   * Constructor for standalone reader with its own arena.
   *
   * @param dataFileSegment memory-mapped segment for the data file
   * @param revisionFileSegment memory-mapped segment for the revisions file
   * @param byteHandler {@link ByteHandler} instance
   * @param type serialization type
   * @param pagePersister page persister
   * @param cache revision file data cache
   * @param arena arena managing the segments, or null if managed externally
   */
  public MMFileReader(final MemorySegment dataFileSegment, final MemorySegment revisionFileSegment,
      final ByteHandler byteHandler, final SerializationType type, final PagePersister pagePersister,
      final Cache<Integer, RevisionFileData> cache, @Nullable final Arena arena) {
    this(dataFileSegment, revisionFileSegment, byteHandler, type, pagePersister, cache, null, null);
  }

  /**
   * Constructor for reader managed by MMStorage with reference counting.
   *
   * @param dataFileSegment memory-mapped segment for the data file
   * @param revisionFileSegment memory-mapped segment for the revisions file
   * @param byteHandler {@link ByteHandler} instance
   * @param type serialization type
   * @param pagePersister page persister
   * @param cache revision file data cache
   * @param generation arena generation for reference counting
   * @param storage storage for releasing the generation
   */
  public MMFileReader(final MemorySegment dataFileSegment, final MemorySegment revisionFileSegment,
      final ByteHandler byteHandler, final SerializationType type, final PagePersister pagePersister,
      final Cache<Integer, RevisionFileData> cache, final MMStorage.@Nullable ArenaGeneration generation,
      @Nullable final MMStorage storage) {
    super(byteHandler, pagePersister, type);
    this.dataFileSegment = requireNonNull(dataFileSegment);
    this.revisionsOffsetFileSegment = requireNonNull(revisionFileSegment);
    this.cache = requireNonNull(cache);
    this.arena = null; // Not used when managed by MMStorage
    this.generation = generation;
    this.storage = storage;
    // Hint the kernel: analytical scans read pages sequentially within each
    // thread's range. MADV_SEQUENTIAL enables aggressive readahead (~2 MB
    // instead of the default 128 KB), overlapping I/O with compute.
    adviseMadvSequential(dataFileSegment);
  }

  private static final int MADV_SEQUENTIAL = 2;

  private static void adviseMadvSequential(final MemorySegment seg) {
    try {
      final var linker = java.lang.foreign.Linker.nativeLinker();
      final var madvise = linker.downcallHandle(
          linker.defaultLookup().find("madvise").orElseThrow(),
          java.lang.foreign.FunctionDescriptor.of(
              java.lang.foreign.ValueLayout.JAVA_INT,
              java.lang.foreign.ValueLayout.ADDRESS,
              java.lang.foreign.ValueLayout.JAVA_LONG,
              java.lang.foreign.ValueLayout.JAVA_INT));
      final int rc = (int) madvise.invokeExact(seg, seg.byteSize(), MADV_SEQUENTIAL);
      if (rc != 0) {
        // Non-fatal: kernel may not support MADV_SEQUENTIAL on all mappings.
      }
    } catch (final Throwable ignored) {
      // madvise unavailable (non-Linux) — no-op.
    }
  }

  /**
   * Fail-fast parity with {@code FileChannelReader#checkDataLength}: the declared length comes
   * straight from the file, so corrupt input can present any 32-bit value — validate it BEFORE
   * sizing a slice or a byte[] (a huge bogus length surfaces as an opaque
   * {@link IndexOutOfBoundsException} from {@code asSlice} or an OOM-prone allocation).
   */
  private void checkDataLength(final int dataLength) {
    final long fileSize = dataFileSegment.byteSize();
    if (dataLength < 0 || dataLength > fileSize) {
      throw new SirixIOException("Corrupt page reference: declared data length " + dataLength
          + " is out of bounds for a data file of " + fileSize + " bytes.");
    }
  }

  @Override
  public Page read(final PageReference reference,
      final @Nullable ResourceConfiguration resourceConfiguration) {
    try {
      final long offset = reference.getKey() + LAYOUT_INT.byteSize();
      final int dataLength = dataFileSegment.get(LAYOUT_INT, reference.getKey());
      checkDataLength(dataLength);

      // Check if we can use zero-copy MemorySegment path (Umbra-style)
      if (byteHandler.supportsMemorySegments()) {
        // Slice mmap segment directly instead of copying to byte[]
        // For empty pipeline: identity (no decompression needed)
        // For non-empty pipeline: decompressScoped() allocates buffer from pool
        MemorySegment pageSlice = dataFileSegment.asSlice(offset, dataLength);
        // Verify checksum for non-KVLP pages (KVLP verified after decompression)
        verifyChecksumIfNeeded(pageSlice, reference, resourceConfiguration);
        // Pass reference for KVLP verification after decompression
        return deserializeFromSegment(resourceConfiguration, pageSlice, reference);
      } else {
        // Fallback: copy to byte[] for stream-based decompression
        final byte[] page = new byte[dataLength];
        MemorySegment.copy(dataFileSegment, LAYOUT_BYTE, offset, page, 0, dataLength);
        // Verify checksum for non-KVLP pages (KVLP verified after decompression)
        verifyChecksumIfNeeded(page, reference, resourceConfiguration);
        // Pass reference for KVLP verification after decompression
        return deserialize(resourceConfiguration, page, reference);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public RevisionRootPage readRevisionRootPage(final int revision, final ResourceConfiguration resourceConfiguration) {
    try {
      // noinspection DataFlowIssue
      final var dataFileOffset = cache.get(revision, (unused) -> getRevisionFileData(revision)).offset();

      final int dataLength = dataFileSegment.get(LAYOUT_INT, dataFileOffset);
      checkDataLength(dataLength);
      final long offset = dataFileOffset + LAYOUT_INT.byteSize();

      // Check if we can use zero-copy MemorySegment path (Umbra-style)
      if (byteHandler.supportsMemorySegments()) {
        // Slice mmap segment directly instead of copying to byte[]
        MemorySegment pageSlice = dataFileSegment.asSlice(offset, dataLength);
        return (RevisionRootPage) deserializeFromSegment(resourceConfiguration, pageSlice);
      } else {
        // Fallback: copy to byte[] for stream-based decompression
        final byte[] page = new byte[dataLength];
        MemorySegment.copy(dataFileSegment, LAYOUT_BYTE, offset, page, 0, dataLength);
        return (RevisionRootPage) deserialize(resourceConfiguration, page);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Instant readRevisionRootPageCommitTimestamp(int revision) {
    // noinspection DataFlowIssue
    return cache.get(revision, (unused) -> getRevisionFileData(revision)).timestamp();
  }

  @Override
  public RevisionFileData getRevisionFileData(int revision) {
    final var fileOffset = IOStorage.revisionsFileOffset(revision);
    // The three 8-byte fields read below end at fileOffset + 24 — a shorter mapping means a
    // truncated record, which would otherwise surface as a raw IndexOutOfBoundsException.
    if (fileOffset + 3L * Long.BYTES > revisionsOffsetFileSegment.byteSize()) {
      throw new SirixIOException("Truncated revisions record for revision " + revision);
    }
    final long revisionOffset = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, fileOffset);
    final long timestampMillis = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, fileOffset + 8);
    final long storedChecksum = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, fileOffset + 16);
    // These 16 bytes are the ONLY path to the revision's root page — verify them.
    if (storedChecksum != IOStorage.revisionRecordChecksum(revisionOffset, timestampMillis)) {
      throw new io.sirix.exception.SirixIOException("Corrupt revisions record for revision " + revision
          + " (checksum mismatch) — torn write or storage corruption");
    }
    return new RevisionFileData(revisionOffset, Instant.ofEpochMilli(timestampMillis));
  }

  @Override
  protected java.nio.ByteBuffer readBeaconSlot(final long offset) {
    final long available = dataFileSegment.byteSize() - offset;
    if (available < Integer.BYTES) {
      throw new io.sirix.exception.SirixIOException("Truncated beacon slot at offset " + offset);
    }
    final long slotBytes = Math.min(IOStorage.BEACON_SLOT_BYTES, available);
    final byte[] slot = new byte[(int) slotBytes];
    MemorySegment.copy(dataFileSegment, LAYOUT_BYTE, offset, slot, 0, (int) slotBytes);
    return java.nio.ByteBuffer.wrap(slot);
  }

  @Override
  public void close() {
    // If managed by MMStorage with reference counting, release the generation
    if (generation != null && storage != null) {
      storage.releaseGeneration(generation);
    }
    // Only close the arena if we own it (not null).
    // When arena is null, the storage (MMStorage) owns and manages the shared arena.
    else if (arena != null) {
      arena.close();
    }
  }
}
