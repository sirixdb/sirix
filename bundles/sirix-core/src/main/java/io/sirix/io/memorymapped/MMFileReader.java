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
import io.sirix.page.RegionsOnlyPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import io.sirix.settings.Constants;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Reader, to read from a memory-mapped file.
 *
 * @author Johannes Lichtenberger
 */
public final class MMFileReader extends AbstractReader {

  static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
  /** Record length prefixes are pinned little-endian like every other on-disk scalar. */
  static final ValueLayout.OfInt LAYOUT_INT =
      ValueLayout.JAVA_INT.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
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
  private static final int MADV_WILLNEED = 3;
  private static final long PAGE_MASK = ~4095L;

  /** Cached madvise downcall — {@code null} where unavailable (non-Linux); every use is a hint. */
  private static final MethodHandle MADVISE_HANDLE;

  static {
    MethodHandle h = null;
    try {
      final Linker linker = Linker.nativeLinker();
      h = linker.downcallHandle(
          linker.defaultLookup().find("madvise").orElseThrow(),
          FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
              ValueLayout.JAVA_INT));
    } catch (final Throwable ignored) {
      // madvise unavailable — hints become no-ops.
    }
    MADVISE_HANDLE = h;
  }

  private static void adviseMadvSequential(final MemorySegment seg) {
    if (MADVISE_HANDLE == null) {
      return;
    }
    try {
      final int rc = (int) MADVISE_HANDLE.invokeExact(seg, seg.byteSize(), MADV_SEQUENTIAL);
      if (rc != 0) {
        // Non-fatal: kernel may not support MADV_SEQUENTIAL on all mappings.
      }
    } catch (final Throwable ignored) {
      // madvise unavailable (non-Linux) — no-op.
    }
  }

  /**
   * Async-readahead hint for one page-aligned region of the data mapping. Purely advisory —
   * failures are ignored; correctness never depends on it.
   */
  private void adviseWillNeed(final long offset, final long length) {
    if (MADVISE_HANDLE == null) {
      return;
    }
    final long alignedStart = offset & PAGE_MASK;
    final long end = Math.min(offset + length, dataFileSegment.byteSize());
    if (end <= alignedStart) {
      return;
    }
    try {
      final MemorySegment region = dataFileSegment.asSlice(alignedStart, end - alignedStart);
      final int rc = (int) MADVISE_HANDLE.invokeExact(region, region.byteSize(), MADV_WILLNEED);
      if (rc != 0) {
        // Advisory only.
      }
    } catch (final Throwable ignored) {
      // Advisory only.
    }
  }

  /**
   * Batched read with COLD-MAPPING readahead: the inherited default demand-faults each segment's
   * pages one at a time (~100 µs of device latency per 4 KiB page, serially — a cold projection
   * column fetch measured ~1,100 major faults for one query). Two WILLNEED passes turn that into
   * kernel-parallel readahead: first every reference's length-header page, then — once the
   * headers are resident enough to read cheaply — every full span. The copy/deserialize loop
   * then runs over mostly-resident pages. Semantics identical to the default loop.
   */
  @Override
  public Page[] read(final PageReference[] references, final @Nullable ResourceConfiguration resourceConfiguration) {
    if (MADVISE_HANDLE != null && references.length >= 8) {
      willNeedSpans(references, references.length);
    }
    final Page[] pages = new Page[references.length];
    for (int i = 0; i < references.length; i++) {
      if (references[i] != null && references[i].getKey() != Constants.NULL_ID_LONG) {
        pages[i] = read(references[i], resourceConfiguration);
      }
    }
    return pages;
  }

  /** The two WILLNEED passes: every length-header page first, then every full span. */
  private void willNeedSpans(final PageReference[] references, final int count) {
    for (int i = 0; i < count; i++) {
      final PageReference ref = references[i];
      if (ref != null && ref.getKey() >= 0) {
        adviseWillNeed(ref.getKey(), LAYOUT_INT.byteSize());
      }
    }
    for (int i = 0; i < count; i++) {
      final PageReference ref = references[i];
      if (ref == null || ref.getKey() < 0) {
        continue;
      }
      final long key = ref.getKey();
      if (key + LAYOUT_INT.byteSize() > dataFileSegment.byteSize()) {
        continue; // the read below reports the corruption attributably
      }
      final int dataLength = dataFileSegment.get(LAYOUT_INT, key);
      if (dataLength > 0 && dataLength <= dataFileSegment.byteSize()) {
        adviseWillNeed(key + LAYOUT_INT.byteSize(), dataLength);
      }
    }
  }

  /** Per-batch cap for {@link #prefetch}: the one-build A/B hatch — {@code 0} disables every
   * downstream prefetch consumer including its reference-resolution work. */
  private static final int PREFETCH_BATCH = Integer.getInteger("sirix.mm.prefetchBatch", 128);

  /**
   * Advisory WILLNEED over the referenced spans — arms the DORMANT record-page prefetch seam
   * ({@code StorageEngineReader.prefetchRecordPages}): a cold point/scan query's winner
   * materialization demand-faults each slotted leaf's span serially without it.
   */
  @Override
  public void prefetch(final PageReference[] references, final int count) {
    if (MADVISE_HANDLE == null || count <= 0) {
      return;
    }
    willNeedSpans(references, Math.min(count, references.length));
  }

  @Override
  public int preferredPrefetchBatch() {
    return MADVISE_HANDLE == null
        ? 0
        : PREFETCH_BATCH;
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
        // The parent-reference hash covers the COMPRESSED payload — verify here, before decode.
        verifyChecksumIfNeeded(pageSlice, reference, resourceConfiguration);
        return deserializeFromSegment(resourceConfiguration, pageSlice, reference);
      } else {
        // Fallback: copy to byte[] for stream-based decompression
        final byte[] page = new byte[dataLength];
        MemorySegment.copy(dataFileSegment, LAYOUT_BYTE, offset, page, 0, dataLength);
        // The parent-reference hash covers the COMPRESSED payload — verify here, before decode.
        verifyChecksumIfNeeded(page, reference, resourceConfiguration);
        return deserialize(resourceConfiguration, page, reference);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public RegionsOnlyPage readRegionsOnly(final PageReference reference,
      final ResourceConfiguration resourceConfiguration, final int regionKindMask,
      final int regionDeferMask) {
    try {
      if (!byteHandler.supportsMemorySegments()) {
        return null;  // caller falls back to the full read path
      }
      final long offset = reference.getKey() + LAYOUT_INT.byteSize();
      final int dataLength = dataFileSegment.get(LAYOUT_INT, reference.getKey());
      checkDataLength(dataLength);
      final MemorySegment pageSlice = dataFileSegment.asSlice(offset, dataLength);
      verifyChecksumIfNeeded(pageSlice, reference, resourceConfiguration);
      return deserializeRegionsOnlyFromSegment(resourceConfiguration, pageSlice, regionKindMask, regionDeferMask);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public RevisionRootPage readRevisionRootPage(final int revision, final ResourceConfiguration resourceConfiguration) {
    try {
      // The cached record carries (offset, timestamp, pageHash) — reuse it so we neither re-read
      // the revisions record nor lose the page hash needed to integrity-check the body below.
      // noinspection DataFlowIssue
      final RevisionFileData revisionFileData = cache.get(revision, (unused) -> getRevisionFileData(revision));
      final long dataFileOffset = revisionFileData.offset();

      final int dataLength = dataFileSegment.get(LAYOUT_INT, dataFileOffset);
      checkDataLength(dataLength);
      final long offset = dataFileOffset + LAYOUT_INT.byteSize();

      // The RevisionRootPage read path carries no parent PageReference, so unlike every other page
      // its body was historically NOT integrity-checked here. The hash recorded alongside the
      // record (the same XXH3 the writer set on the page's reference) lets us verify the compressed
      // payload BEFORE deserialization — mirroring read(reference, config) exactly. Gated on
      // verifyChecksumsOnRead and skipped for legacy/RAM records that carry no hash (pageHash == 0).
      final PageReference reference = revisionRootReference(dataFileOffset, revisionFileData.pageHash());

      // Check if we can use zero-copy MemorySegment path (Umbra-style)
      if (byteHandler.supportsMemorySegments()) {
        // Slice mmap segment directly instead of copying to byte[]
        MemorySegment pageSlice = dataFileSegment.asSlice(offset, dataLength);
        verifyChecksumIfNeeded(pageSlice, reference, resourceConfiguration);
        return (RevisionRootPage) deserializeFromSegment(resourceConfiguration, pageSlice, reference);
      } else {
        // Fallback: copy to byte[] for stream-based decompression
        final byte[] page = new byte[dataLength];
        MemorySegment.copy(dataFileSegment, LAYOUT_BYTE, offset, page, 0, dataLength);
        verifyChecksumIfNeeded(page, reference, resourceConfiguration);
        return (RevisionRootPage) deserialize(resourceConfiguration, page, reference);
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
    // The four 8-byte fields read below end at fileOffset + 32 — a shorter mapping means a
    // truncated record, which would otherwise surface as a raw IndexOutOfBoundsException.
    if (fileOffset + 4L * Long.BYTES > revisionsOffsetFileSegment.byteSize()) {
      throw new SirixIOException("Truncated revisions record for revision " + revision);
    }
    final long revisionOffset = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, fileOffset);
    final long timestampMillis = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, fileOffset + 8);
    final long storedChecksum = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, fileOffset + 16);
    // 4th field: the RevisionRootPage's own page hash (0 = legacy record / no hash).
    final long pageHash = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, fileOffset + 24);
    // These bytes are the ONLY path to the revision's root page — verify them. The checksum covers
    // 24 bytes when a page hash is present, 16 when legacy (hash == 0), so beta1 resources open.
    if (storedChecksum != IOStorage.expectedRevisionRecordChecksum(revisionOffset, timestampMillis, pageHash)) {
      throw new io.sirix.exception.SirixIOException("Corrupt revisions record for revision " + revision
          + " (checksum mismatch) — torn write or storage corruption");
    }
    return new RevisionFileData(revisionOffset, Instant.ofEpochMilli(timestampMillis), pageHash);
  }

  @Override
  public RevisionFileData[] getRevisionFileData(final int fromRevision, final int count) {
    if (count <= 0) {
      return new RevisionFileData[0];
    }
    // Mapped reads need no syscalls or staging buffers, so unlike FileChannelReader the bulk
    // path is not about I/O batching — it only hoists the truncation check out of the
    // per-record loop (the revision-index load calls this with the full history).
    final long lastRecordOffset = IOStorage.revisionsFileOffset(fromRevision + count - 1);
    if (lastRecordOffset + 4L * Long.BYTES > revisionsOffsetFileSegment.byteSize()) {
      final long tail = revisionsOffsetFileSegment.byteSize() - IOStorage.REVISIONS_RECORDS_START - 4L * Long.BYTES;
      final long firstTruncated = tail < 0 ? 0 : tail / IOStorage.REVISIONS_FILE_RECORD_SIZE + 1;
      throw new SirixIOException("Truncated revisions record for revision " + Math.max(fromRevision, firstTruncated));
    }
    final RevisionFileData[] result = new RevisionFileData[count];
    for (int i = 0; i < count; i++) {
      final long base = IOStorage.revisionsFileOffset(fromRevision + i);
      final long revisionOffset = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, base);
      final long timestampMillis = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, base + 8);
      final long storedChecksum = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, base + 16);
      // 4th field: the RevisionRootPage's own page hash (0 = legacy record / no hash).
      final long pageHash = revisionsOffsetFileSegment.get(LAYOUT_LONG_LE, base + 24);
      if (storedChecksum != IOStorage.expectedRevisionRecordChecksum(revisionOffset, timestampMillis, pageHash)) {
        throw new io.sirix.exception.SirixIOException("Corrupt revisions record for revision " + (fromRevision + i)
            + " (checksum mismatch) — torn write or storage corruption");
      }
      result[i] = new RevisionFileData(revisionOffset, Instant.ofEpochMilli(timestampMillis), pageHash);
    }
    return result;
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
