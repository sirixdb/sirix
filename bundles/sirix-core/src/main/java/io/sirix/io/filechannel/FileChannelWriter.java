/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.io.AbstractForwardingReader;
import io.sirix.io.IOStorage;
import io.sirix.io.Superblock;
import io.sirix.io.PageHasher;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.Writer;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * File Writer for providing read/write access for file as a Sirix backend.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class FileChannelWriter extends AbstractForwardingReader implements Writer {

  /**
   * Random access to work on.
   */
  private final FileChannel dataFileChannel;

  /**
   * Write-through (DSYNC) channel to the SAME data file, used exclusively for the two uber-page
   * beacon slot writes — see the constructor javadoc.
   */
  private final FileChannel beaconDurableChannel;

  /**
   * {@link FileChannelReader} reference for this writer.
   */
  private final FileChannelReader reader;

  private final SerializationType serializationType;

  private final FileChannel revisionsFileChannel;

  private final PagePersister pagePersister;

  private final AsyncCache<Integer, RevisionFileData> cache;

  private final RevisionIndexHolder revisionIndexHolder;

  /**
   * Low-latency durable-commit profile (HFT). When enabled, the data and revisions files are
   * preallocated in large chunks so per-commit writes never extend {@code i_size}; the single
   * write-ahead barrier in {@link #writeUberPageReference} then becomes {@code fdatasync}
   * ({@code force(false)}) — on a non-growing file this skips the ext4/xfs metadata-journal commit
   * that the growing-file {@code force(true)} forces, and the O_SYNC revision record write becomes
   * in-place (also journal-free). The dual beacons are already in-place FUA writes.
   *
   * <p>On by default; disabling it restores the legacy grow+{@code force(true)} path. (For a file
   * that has ALREADY run preallocated, the legacy path derives its append offset from the physical
   * file size, so subsequent commits land after the existing zero tail — readable and consistent,
   * but the padding becomes a permanent unreachable gap; the modes are only byte-identical for
   * files that never ran preallocated.) The logical write frontier is derived from the durable
   * revision graph
   * (the last revision root, located via the uber beacon), NOT from the preallocation-inflated
   * physical file size. The zero-filled tail is PHYSICALLY allocated — that is the point: in-place
   * writes must never allocate fresh blocks — and it persists across sessions by design, because
   * trimming it on close would force a fresh chunk-allocation fsync on every session cycle. Growth
   * is adaptive (see {@link #ensureDataCapacity}), so a small resource's at-rest padding stays
   * proportional to its size instead of paying the full {@link #preallocChunkBytes} cap up front.
   *
   * <p>FILE_CHANNEL-only: the MEMORY_MAPPED backend constructs this writer too, but its readers
   * only remap the file when the PHYSICAL size grew — in-place preallocated commits leave the size
   * unchanged, so fresh readers would keep serving a mapping created before those commits (mmap
   * visibility of later {@code write()}s is unspecified). MM therefore passes
   * {@code preallocationSupported=false} and stays on the legacy grow path, whose per-commit size
   * growth is exactly the remap trigger.
   */
  private final boolean preallocatedCommit;

  /**
   * Upper bound in bytes for a single adaptive preallocation grow of the data file. Growth roughly
   * doubles the file per grow, from {@link #MIN_PREALLOC_CHUNK_BYTES} up to this cap — sustained
   * writers amortize one allocation fsync over many MiBs, while the physically-allocated at-rest
   * padding of a small resource stays proportional to its size.
   */
  private final long preallocChunkBytes =
      Long.getLong("sirix.commit.preallocChunkBytes", 8L * 1024 * 1024);

  /** Smallest adaptive preallocation grow of the data file (also the small-resource padding floor). */
  private static final long MIN_PREALLOC_CHUNK_BYTES = 256L * 1024;

  /**
   * Fixed preallocation chunk for the revisions file, whose records are 32 bytes each
   * ({@code IOStorage.REVISIONS_FILE_RECORD_SIZE}) — 64 KiB covers 2,048 commits. The revisions
   * channel is opened O_DSYNC, so its zero-fill is synchronous write-through; keeping this chunk
   * small keeps that stall negligible (reusing the data file's multi-MiB chunk here would
   * preallocate hundreds of thousands of commits' worth through the sync channel).
   */
  private static final long REVISIONS_PREALLOC_CHUNK_BYTES = 64L * 1024;

  /**
   * Low-latency beacon durability (requires {@link #preallocatedCommit}). Makes the two uber-page
   * beacons durable with ONE buffered {@code fdatasync} on the data channel instead of two O_DSYNC
   * (FUA) writes through the beacon channel — one fewer device round-trip per commit. Both the
   * write-ahead ordering (page tail durable BEFORE the beacons) and the two-copy beacon redundancy
   * are preserved, so the durability contract is unchanged; only the I/O shape is cheaper. The two
   * beacons live in separate {@code BEACON_SLOT_BYTES} blocks, so a single torn block still leaves
   * the other copy, and the write-ahead guarantees whichever copy survives names a durable tail.
   * On by default (with {@code preallocatedCommit}); together the pair turns five device
   * round-trips per durable commit into three (measurements: docs/COMPARISON_POSTGRES.md §0.1).
   */
  private final boolean bufferedBeacons =
      Boolean.parseBoolean(System.getProperty("sirix.commit.bufferedBeacons", "true"));

  /** Read-only zero block for {@link #growFile}; duplicated per use, never allocated per call. */
  private static final ByteBuffer ZERO_BLOCK = ByteBuffer.allocateDirect(1 << 20).asReadOnlyBuffer();

  /** Logical write frontier of the data file (replaces {@code dataFileChannel.size()}); -1 = uninit. */
  private long dataLogicalEnd = -1L;
  /** Physical preallocated end of the data file ({@code >= dataLogicalEnd}). */
  private long dataPreallocEnd = -1L;
  /** Physical preallocated end of the revisions file (records land at deterministic slots). */
  private long revisionsPreallocEnd = -1L;
  /** Whether the data frontier has been derived from the durable revision graph this session. */
  private boolean frontiersInitialised;

  /**
   * Lazy revision records (requires {@link #preallocatedCommit}). The per-commit 32-byte revision
   * record is written through a BUFFERED revisions channel (no O_SYNC device round-trip) and a
   * checksummed copy rides a {@link IOStorage#REVISION_RECORD_TAIL_LOG_CAPACITY}-entry ring — the
   * "tail-log" — in the last {@link IOStorage#REVISION_RECORD_TAIL_LOG_BYTES} bytes of BOTH
   * uber-beacon slots' zero pad. The ring is written to the data file BEFORE the existing
   * write-ahead {@code fdatasync} barrier, so the invariant "the revision's locator is durable
   * before any beacon advertises it" is preserved exactly; only the separate synchronous revisions
   * write disappears. Together with {@link #bufferedBeacons} a durable commit costs TWO device
   * round-trips (write-ahead barrier + beacon flush) instead of three.
   *
   * <p>Recovery reads the record from the revisions file as before; a record the crash lost is
   * salvaged from the tail-log and healed back into the file
   * ({@link FileChannelReader#getRevisionFileData(int, int)}). A ring entry may only be EVICTED
   * (its 48-byte slot reused, {@code capacity} commits later) once its record is known durable —
   * tracked per resource in {@link RevisionRecordDurability} and enforced with a synchronous
   * {@code force(false)} that fires roughly once per {@code capacity} commits in the worst case.
   */
  private final boolean lazyRevisionRecords;

  /** Path of the revisions file — identity key for {@link RevisionRecordDurability}. */
  private final Path revisionsFilePath;

  /** Resource UUID halves — the durability entry's identity key together with the path. */
  private final long resourceUuidMsb;
  private final long resourceUuidLsb;

  /** Per-resource durability state; resolved eagerly, re-resolved after truncation. */
  private RevisionRecordDurability durability;

  /**
   * In-memory image of the beacon tail-log ring; {@code null} until first initialised (adopted
   * from the predecessor writer or merged from the on-disk beacon slots). Nullness IS the
   * initialised state — no separate flag can drift out of sync with it.
   */
  private byte[] tailLog;

  /** Little-endian view over {@link #tailLog}; created and cleared together with it. */
  private ByteBuffer tailLogView;

  /** Reusable direct buffer for persisting the ring to the two beacon slots. */
  private ByteBuffer tailLogWriteBuffer;

  /** Highest revision whose record this writer wrote; {@code -1} = none. */
  private long highestWrittenRevision = -1L;

  /**
   * Whether the ring provably contains every live entry at init time (predecessor handoff, or the
   * predecessor's entry visible on disk). When {@code false}, the beacon phase re-merges the
   * on-disk slots before overwriting them — the depth-1 pipelined-async gap.
   */
  private boolean ringCompleteAtInit;

  /**
   * Temporary page serialization buffer.
   *
   * <p>Pre-size to FLUSH_SIZE to avoid repeated grow/copy churn when serializing medium/large pages.
   */
  private final BytesOut<?> byteBufferBytes = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);

  /**
   * Constructor.
   *
   * @param dataFileChannel the data file channel
   * @param revisionsOffsetFileChannel the channel to the file, which holds pointers to the revision
   *        root pages — MUST be opened with {@link java.nio.file.StandardOpenOption#DSYNC}: the
   *        32-byte revision record (and the one-time superblock) are written through it, and the
   *        commit protocol relies on those writes being durable at write-return instead of paying
   *        a separate fsync per commit
   * @param beaconDurableChannel a SECOND channel to the data file, opened with
   *        {@link java.nio.file.StandardOpenOption#DSYNC}, used ONLY for the two uber-page beacon
   *        slot writes. Write-through gives the dual-beacon ordering (secondary durable before the
   *        primary is even issued) and the commit acknowledge (primary durable at write-return)
   *        without any explicit fsync — on NVMe these map to FUA writes, far cheaper than full
   *        cache flushes. The bulk data channel stays buffered.
   * @param serializationType the serialization type (for the transaction log or the data file)
   * @param pagePersister transforms in-memory pages into byte-arrays and back
   * @param cache the revision file data cache
   * @param revisionIndexHolder the holder for the optimized revision index
   * @param reader the reader delegate
   * @param preallocatedCommit whether this writer runs the preallocated-commit profile (see
   *        {@link #preallocatedCommit}) — COMPUTED BY THE OWNING STORAGE from backend support and
   *        {@link IOStorage#preallocatedCommitsEnabled()} at the same instant it chose the channel
   *        open modes; the writer deliberately does NOT re-read the property, since a flip between
   *        two reads would pair channels with a mismatched durability protocol
   * @param lazyRevisionRecords whether the owning backend opened the revisions channel BUFFERED
   *        for the lazy-revision-record profile (see {@link #lazyRevisionRecords}); must be
   *        {@code false} when that channel is write-through. Only honored together with the
   *        preallocated profile
   * @param revisionsFilePath path of the revisions file, identity key for the durability state
   * @param resourceUuidMsb most significant resource-UUID half ({@code 0} = legacy, no UUID)
   * @param resourceUuidLsb least significant resource-UUID half ({@code 0} = legacy)
   */
  public FileChannelWriter(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final FileChannel beaconDurableChannel, final SerializationType serializationType,
      final PagePersister pagePersister, final AsyncCache<Integer, RevisionFileData> cache,
      final RevisionIndexHolder revisionIndexHolder, final FileChannelReader reader,
      final boolean preallocatedCommit, final boolean lazyRevisionRecords, final Path revisionsFilePath,
      final long resourceUuidMsb, final long resourceUuidLsb) {
    this.preallocatedCommit = preallocatedCommit;
    this.lazyRevisionRecords = lazyRevisionRecords && preallocatedCommit;
    this.revisionsFilePath = requireNonNull(revisionsFilePath);
    this.resourceUuidMsb = resourceUuidMsb;
    this.resourceUuidLsb = resourceUuidLsb;
    this.durability = RevisionRecordDurability.forFile(revisionsFilePath, resourceUuidMsb, resourceUuidLsb);
    this.dataFileChannel = dataFileChannel;
    this.beaconDurableChannel = requireNonNull(beaconDurableChannel);
    this.serializationType = requireNonNull(serializationType);
    this.revisionsFileChannel = revisionsOffsetFileChannel;
    this.pagePersister = requireNonNull(pagePersister);
    this.cache = requireNonNull(cache);
    this.revisionIndexHolder = requireNonNull(revisionIndexHolder);
    this.reader = requireNonNull(reader);
  }

  @Override
  public Writer truncateTo(final int revision) {
    try {
      final var dataFileRevisionRootPageOffset =
          cache.get(revision, _ -> getRevisionFileData(revision)).get(5, TimeUnit.SECONDS).offset();

      // Read the length header from the file — VALIDATING every step. This code runs during
      // crash recovery, exactly when the on-disk state may be garbage: an unchecked short read
      // left the zero-filled buffer (dataLength=0) and a corrupt/negative length silently
      // truncated INTO older committed revisions (destroying good data) or threw from a
      // negative truncation size.
      final var buffer = ByteBuffer.allocateDirect(IOStorage.OTHER_BEACON).order(ByteOrder.LITTLE_ENDIAN);
      int totalRead = 0;
      while (buffer.hasRemaining()) {
        final int n = dataFileChannel.read(buffer, dataFileRevisionRootPageOffset + totalRead);
        if (n < 0) {
          break;
        }
        totalRead += n;
      }
      if (totalRead < IOStorage.OTHER_BEACON) {
        throw new SirixIOException("truncateTo(" + revision + "): short read of the revision-root "
            + "length header at offset " + dataFileRevisionRootPageOffset + " (got " + totalRead + " bytes)");
      }

      buffer.position(0);
      final int dataLength = buffer.getInt();
      final long fileSize = dataFileChannel.size();
      final long newSize = dataFileRevisionRootPageOffset + IOStorage.OTHER_BEACON + (long) dataLength;
      if (dataLength < 0 || newSize > fileSize) {
        throw new SirixIOException("truncateTo(" + revision + "): implausible revision-root length "
            + dataLength + " at offset " + dataFileRevisionRootPageOffset + " (file size " + fileSize
            + ") — refusing to truncate");
      }

      dataFileChannel.truncate(newSize);

      // Also truncate the REVISIONS file to drop records of revisions beyond the target —
      // leftover records from failed/rolled-back commits otherwise shift or shadow later
      // lookups — and drop now-stale cache entries (they were populated at WRITE time, before
      // durability).
      final long revisionsKeep = IOStorage.revisionsFileOffset(revision + 1);
      if (revisionsFileChannel.size() > revisionsKeep) {
        revisionsFileChannel.truncate(revisionsKeep);
      }
      // Drop cached RevisionFileData for THIS resource — entries are populated at write time
      // (before durability), so any past the truncated revision are stale. The per-resource
      // invalidateAll only clears this resource's entries; truncateTo is a cold recovery/
      // rollback path, so re-fetching the survivors is fine.
      cache.synchronous().invalidateAll();

      // Claims above the truncated-to revision are stale (their record slots will be rewritten
      // with different content), and the writer's UUID is known here — drop the whole entry and
      // re-resolve, then re-derive the ring from the repaired slots at the next commit.
      RevisionRecordDurability.invalidateFor(revisionsFilePath);
      durability = RevisionRecordDurability.forFile(revisionsFilePath, resourceUuidMsb, resourceUuidLsb);
      tailLog = null;
      tailLogView = null;
      if (highestWrittenRevision > revision) {
        highestWrittenRevision = revision;
      }

      repairBeaconSlotsAfterTruncate(revision);

      if (preallocatedCommit) {
        // Recovery seeds the frontier from the revision we trimmed back to, so subsequent
        // preallocated writes resume at the recovered data end (not past the crashed garbage).
        dataLogicalEnd = newSize;
        dataPreallocEnd = Math.max(newSize, IOStorage.DATA_REGION_START);
        revisionsPreallocEnd = revisionsFileChannel.size();
        frontiersInitialised = true;
      }
    } catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
      throw new IllegalStateException(e);
    }

    return this;
  }

  /**
   * After truncating to {@code revision}, both beacon slots must advertise exactly that
   * revision. The crash this recovery handles (died between the secondary and primary beacon
   * writes) leaves the SECONDARY advertising the truncated-away revision — harmless for the
   * happy path (the primary wins), but until the next commit rewrote the slots, a primary
   * corruption made fallback dereference the stale-forward secondary and the resource
   * unopenable although every surviving revision was intact. Repair by copying the slot that
   * matches the truncated-to revision over the one that doesn't (which also heals a torn
   * primary right at recovery instead of at the next commit).
   */
  private void repairBeaconSlotsAfterTruncate(final int revision) throws IOException {
    final int primaryRevision = reader.beaconRevisionOrMinusOne(IOStorage.PRIMARY_BEACON_OFFSET);
    final int secondaryRevision = reader.beaconRevisionOrMinusOne(IOStorage.SECONDARY_BEACON_OFFSET);
    if (primaryRevision == revision && secondaryRevision == revision) {
      return;
    }
    final long goodOffset;
    final long staleOffset;
    if (primaryRevision == revision) {
      goodOffset = IOStorage.PRIMARY_BEACON_OFFSET;
      staleOffset = IOStorage.SECONDARY_BEACON_OFFSET;
    } else if (secondaryRevision == revision) {
      goodOffset = IOStorage.SECONDARY_BEACON_OFFSET;
      staleOffset = IOStorage.PRIMARY_BEACON_OFFSET;
    } else {
      // Crash recovery always truncates to the revision one of the slots was opened from, so
      // one slot matches and the other gets repaired above. An EXPLICIT rollback
      // (StorageEngineWriter.truncateTo to an older revision) instead truncates AWAY the
      // revision both slots advertise — no slot carries the target revision's uber page, and
      // the caller's subsequent commit rewrites both slots. Leave them alone in that flow.
      return;
    }

    final ByteBuffer slot = ByteBuffer.allocateDirect(IOStorage.BEACON_SLOT_BYTES);
    while (slot.hasRemaining()) {
      if (dataFileChannel.read(slot, goodOffset + slot.position()) < 0) {
        throw new SirixIOException("truncateTo(" + revision + "): short read of the good beacon slot at offset "
            + goodOffset);
      }
    }
    slot.flip();
    while (slot.hasRemaining()) {
      dataFileChannel.write(slot, staleOffset + slot.position());
    }
    dataFileChannel.force(false);
  }

  @Override
  public FileChannelWriter write(final ResourceConfiguration resourceConfiguration, final PageReference pageReference,
      final Page page, final BytesOut<?> bufferedBytes) {
    try {
      final long offset = getOffset(bufferedBytes);
      return writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, offset);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private long getOffset(BytesOut<?> bufferedBytes) throws IOException {
    if (preallocatedCommit) {
      return dataFrontier() + bufferedBytes.writePosition();
    }
    // The header region (superblock + beacon slots) is a SPARSE hole until the first commit
    // writes it — data pages always start at DATA_REGION_START.
    return Math.max(dataFileChannel.size(), IOStorage.DATA_REGION_START) + bufferedBytes.writePosition();
  }

  /**
   * Returns the data file's logical write frontier (the next append offset), lazily derived from the
   * durable revision graph — NOT from {@code channel.size()}, which preallocation inflates with zeros.
   */
  private long dataFrontier() throws IOException {
    initFrontiersIfNeeded();
    return dataLogicalEnd;
  }

  /**
   * Derive the data write frontier from the last committed revision — the same durable information
   * {@link #truncateTo} uses for recovery — so a per-transaction writer never trusts the
   * preallocation-inflated physical file size for the <em>logical</em> end. {@code dataPreallocEnd}
   * legitimately becomes the physical size (that is genuinely how much is allocated), which is why
   * preallocation stops compounding across transactions once the frontier is sourced correctly.
   */
  private void initFrontiersIfNeeded() throws IOException {
    if (frontiersInitialised) {
      return;
    }
    dataPreallocEnd = Math.max(dataFileChannel.size(), IOStorage.DATA_REGION_START);
    revisionsPreallocEnd = revisionsFileChannel.size();

    int lastRevision = reader.beaconRevisionOrMinusOne(IOStorage.PRIMARY_BEACON_OFFSET);
    if (lastRevision < 0) {
      lastRevision = reader.beaconRevisionOrMinusOne(IOStorage.SECONDARY_BEACON_OFFSET);
    }
    if (lastRevision < 0) {
      // No committed revision yet: data pages start at the data-region boundary.
      dataLogicalEnd = IOStorage.DATA_REGION_START;
    } else {
      // Data frontier = end of the last revision root page = offset + OTHER_BEACON (4-byte length
      // prefix) + payload length, exactly as FileChannelReader/truncateTo frame it.
      final long revRootOffset = getRevisionFileData(lastRevision).offset();
      final ByteBuffer lenBuf = ByteBuffer.allocateDirect(IOStorage.OTHER_BEACON).order(ByteOrder.LITTLE_ENDIAN);
      int read = 0;
      while (lenBuf.hasRemaining()) {
        final int n = dataFileChannel.read(lenBuf, revRootOffset + read);
        if (n < 0) {
          break;
        }
        read += n;
      }
      if (read < IOStorage.OTHER_BEACON) {
        throw new SirixIOException("preallocated frontier: short read of the revision-root length header at "
            + revRootOffset + " for revision " + lastRevision);
      }
      lenBuf.flip();
      final int dataLength = lenBuf.getInt();
      dataLogicalEnd = revRootOffset + IOStorage.OTHER_BEACON + dataLength;
    }
    frontiersInitialised = true;
  }

  /** Ensure the data file is physically (and durably) block-allocated to at least {@code needed} bytes. */
  private void ensureDataCapacity(final long needed) throws IOException {
    if (needed > dataPreallocEnd) {
      // Adaptive chunk: roughly double the file per grow, clamped to
      // [MIN_PREALLOC_CHUNK_BYTES, preallocChunkBytes]. A tiny resource then carries at most
      // ~its own size (floor 256 KiB) of physically-allocated padding instead of a full
      // cap-sized chunk, while sustained writers still amortize one allocation fsync per
      // up-to-the-cap grow.
      final long grow = Math.min(preallocChunkBytes, Math.max(MIN_PREALLOC_CHUNK_BYTES, dataPreallocEnd));
      final long target = Math.max(needed, dataPreallocEnd + grow);
      growFile(dataFileChannel, dataPreallocEnd, target);
      dataPreallocEnd = target;
    }
  }

  /** Ensure the revisions file is physically (and durably) block-allocated to at least {@code needed} bytes. */
  private void ensureRevisionsCapacity(final long needed) throws IOException {
    if (needed > revisionsPreallocEnd) {
      final long target = Math.max(needed, revisionsPreallocEnd + REVISIONS_PREALLOC_CHUNK_BYTES);
      growFile(revisionsFileChannel, revisionsPreallocEnd, target);
      revisionsPreallocEnd = target;
    }
  }

  /**
   * Physically allocate blocks in {@code [from, to)} by writing zeros and making the allocation
   * durable with a single {@code fsync}, so that subsequent in-place writes neither extend
   * {@code i_size} nor allocate fresh blocks — letting each commit's {@code fdatasync}/O_SYNC write
   * skip the ext4/xfs metadata-journal commit. This one-time fsync per chunk is amortised over the
   * many commits the chunk absorbs.
   */
  private static void growFile(final FileChannel channel, final long from, final long to) throws IOException {
    long off = from;
    while (off < to) {
      // duplicate() shares the one static zero block without allocating a fresh 1 MiB direct
      // buffer per grow (direct buffers are only reclaimed by GC-run Cleaners).
      final ByteBuffer zeros = ZERO_BLOCK.duplicate();
      if (zeros.remaining() > to - off) {
        zeros.limit((int) (to - off));
      }
      final int written = channel.write(zeros, off);
      if (written <= 0) {
        throw new IOException("Preallocation stalled at offset " + off + " (target " + to + ")");
      }
      off += written;
    }
    channel.force(true); // durably allocate the blocks once, so per-commit fdatasync stays journal-free
  }

  private static void writeToBufferedBytes(BytesOut<?> bufferedBytes, byte[] serializedPageBytes,
      MemorySegment serializedPageSegment, int serializedPageLength) {
    if (serializedPageSegment != null) {
      bufferedBytes.writeSegment(serializedPageSegment, 0, serializedPageLength);
    } else if (serializedPageBytes != null) {
      bufferedBytes.write(serializedPageBytes);
    }
  }


  private FileChannelWriter writePageReference(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, final BytesOut<?> bufferedBytes, long offset) {
    // Perform byte operations.
    try {
      // Serialize page.
      pagePersister.serializePage(resourceConfiguration, byteBufferBytes, page, serializationType);
      final BytesIn<?> uncompressedBytes = byteBufferBytes.bytesForRead();
      final var pipeline = resourceConfiguration.byteHandlePipeline;
      byte[] serializedPageBytes = null;
      MemorySegment serializedPageSegment = null;

      if (page instanceof KeyValueLeafPage keyValueLeafPage) {
        // Check compressed MemorySegment cache first (slotted page format path).
        serializedPageSegment = keyValueLeafPage.getCompressedSegment();
        if (serializedPageSegment == null) {
          // Check legacy byte[] cache.
          final var cached = keyValueLeafPage.getBytes();
          if (cached != null) {
            if (cached instanceof MemorySegmentBytesOut msOut) {
              serializedPageSegment = msOut.getDestination();
            } else {
              serializedPageBytes = cached.toByteArray();
            }
          }
        }
      }

      if (serializedPageSegment == null && serializedPageBytes == null) {
        if (pipeline.supportsMemorySegments() && uncompressedBytes instanceof MemorySegmentBytesIn segmentIn) {
          serializedPageSegment = pipeline.compress(segmentIn.getSource());
        } else {
          final byte[] byteArray = uncompressedBytes.toByteArray();
          try (final ByteArrayOutputStream output = new ByteArrayOutputStream(byteArray.length);
              final DataOutputStream dataOutput = new DataOutputStream(reader.getByteHandler().serialize(output))) {
            dataOutput.write(byteArray);
            dataOutput.flush();
            serializedPageBytes = output.toByteArray();
          }
        }
      }

      final int serializedPageLength;
      if (serializedPageSegment != null) {
        serializedPageLength = (int) serializedPageSegment.byteSize();
      } else if (serializedPageBytes != null) {
        serializedPageLength = serializedPageBytes.length;
      } else {
        throw new IllegalStateException("Failed to build serialized page payload");
      }

      if (io.sirix.io.file.StorageProfile.isEnabled()) {
        // Raw (pre-compression) size — we read it from the reader view before
        // clearing. This is the byte count the pagePersister produced.
        final int rawSize;
        if (uncompressedBytes instanceof io.sirix.node.MemorySegmentBytesIn msIn) {
          rawSize = (int) msIn.getSource().byteSize();
        } else {
          rawSize = uncompressedBytes.toByteArray().length;
        }
        io.sirix.io.file.StorageProfile.record(page.getClass().getSimpleName(), rawSize, serializedPageLength);
      }

      byteBufferBytes.clear();

      int offsetToAdd = 0;

      // Getting actual offset and appending to the end of the current file.
      if (serializationType == SerializationType.DATA) {
        if (page instanceof UberPage) {
          // Beacon slot layout: [u32 len][payload][u64 xxh3][zero pad] in a BEACON_SLOT_BYTES
          // slot. An oversized uber page would silently overlap the next slot / the data region
          // (unrecoverable database). Fail loudly instead.
          if (serializedPageLength + IOStorage.OTHER_BEACON + Long.BYTES >= IOStorage.BEACON_SLOT_BYTES) {
            throw new SirixIOException("Serialized UberPage (" + serializedPageLength
                + " bytes + header + checksum) exceeds its " + IOStorage.BEACON_SLOT_BYTES
                + "-byte slot — the on-disk uber-page layout must be revised before this can be written");
          }
          offsetToAdd = IOStorage.BEACON_SLOT_BYTES
              - ((serializedPageLength + IOStorage.OTHER_BEACON + Long.BYTES) % IOStorage.BEACON_SLOT_BYTES);
        } else if (offset % PAGE_FRAGMENT_BYTE_ALIGN != 0) {
          offsetToAdd = (int) (PAGE_FRAGMENT_BYTE_ALIGN - (offset & (PAGE_FRAGMENT_BYTE_ALIGN - 1)));// (offset %
                                                                                                     // PAGE_FRAGMENT_BYTE_ALIGN));
          offset += offsetToAdd;
        }
      }

      if (!(page instanceof UberPage) && offsetToAdd > 0) {
        bufferedBytes.writePosition(bufferedBytes.writePosition() + offsetToAdd);
      }

      // Compute hash on compressed bytes for ALL page types (consistent approach). Computed
      // BEFORE buffering: the uber beacon slot embeds it as an integrity trailer.
      final byte[] pageHash;
      if (serializedPageSegment != null) {
        pageHash = PageHasher.compute(serializedPageSegment, PageHasher.DEFAULT_ALGORITHM);
      } else if (serializedPageBytes != null) {
        pageHash = PageHasher.compute(serializedPageBytes);
      } else {
        throw new IllegalStateException("Failed to compute page hash due to missing payload");
      }

      bufferedBytes.writeInt(serializedPageLength);
      writeToBufferedBytes(bufferedBytes, serializedPageBytes, serializedPageSegment, serializedPageLength);

      if (page instanceof UberPage) {
        // Beacon integrity trailer: recovery validates [len][payload][xxh3] instead of relying
        // on "deserialization didn't throw" (the beacons have no parent reference to carry a
        // checksum, unlike every other page).
        bufferedBytes.write(pageHash);
        if (offsetToAdd > 0) {
          bufferedBytes.write(new byte[(int) offsetToAdd]);
        }
      }

      if (bufferedBytes.writePosition() > FLUSH_SIZE) {
        flushBuffer(bufferedBytes);
      }

      // Remember page coordinates.
      pageReference.setKey(offset);
      pageReference.setHash(pageHash);

      if (serializationType == SerializationType.DATA && page instanceof RevisionRootPage revisionRootPage) {
        // DETERMINISTIC slot (the shared layout formula) — append-at-file-size shifted every
        // later slot after a failed commit or a torn record. The record carries an XXH3 of its
        // first 16/24 bytes: these records are the only path to any RevisionRootPage and used to
        // be completely unprotected. The former "reserved" 4th field now stores the
        // RevisionRootPage's own page hash (XXH3 of its compressed payload, the same value set on
        // pageReference above) so the page body is integrity-checked on read; the checksum covers
        // it. Normalize an (astronomically unlikely) all-zero hash to a sentinel so the stored
        // field is never 0 — 0 is reserved to mean "legacy record, no hash".
        final long storedPageHash =
            IOStorage.normalizeRevisionRootPageHash(io.sirix.io.HashAlgorithm.bytesToLong(pageHash));
        final ByteBuffer buffer =
            ByteBuffer.allocateDirect(IOStorage.REVISIONS_FILE_RECORD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(offset);
        buffer.putLong(revisionRootPage.getRevisionTimestamp());
        buffer.putLong(
            IOStorage.revisionRecordChecksum(offset, revisionRootPage.getRevisionTimestamp(), storedPageHash));
        buffer.putLong(storedPageHash);
        buffer.flip();
        final long revisionsFileOffset = IOStorage.revisionsFileOffset(revisionRootPage.getRevision());
        if (preallocatedCommit) {
          ensureRevisionsCapacity(revisionsFileOffset + IOStorage.REVISIONS_FILE_RECORD_SIZE);
        }
        while (buffer.hasRemaining()) {
          if (revisionsFileChannel.write(buffer, revisionsFileOffset + buffer.position()) <= 0) {
            throw new IOException("Revision-record write stalled: no progress");
          }
        }
        if (lazyRevisionRecords) {
          // The record above went through a BUFFERED channel — stage its checksummed copy in the
          // in-memory ring; writeUberPageReference persists the ring ahead of the write-ahead
          // barrier, so the locator is durable before any beacon names it.
          stageTailLogEntry(resourceConfiguration, revisionRootPage.getRevision(), offset,
              revisionRootPage.getRevisionTimestamp(),
              IOStorage.revisionRecordChecksum(offset, revisionRootPage.getRevisionTimestamp(), storedPageHash),
              storedPageHash);
        }
        final long currOffset = offset;
        final long currTimestamp = revisionRootPage.getRevisionTimestamp();
        cache.put(revisionRootPage.getRevision(), CompletableFuture.supplyAsync(
            () -> new RevisionFileData(currOffset, Instant.ofEpochMilli(currTimestamp), storedPageHash)));
        // Update the optimized revision index
        revisionIndexHolder.addRevision(currOffset, currTimestamp);
      }

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      // Preallocated profile: i_size is stable, so fdatasync suffices. The preallocated tail is
      // intentionally NOT trimmed here — writers are per-transaction, so trimming on every close
      // would force a fresh preallocation each commit; the file stays at high-water-mark and the
      // frontier is re-derived from the durable revision graph on reopen.
      final boolean metaData = !preallocatedCommit;
      if (dataFileChannel != null) {
        dataFileChannel.force(metaData);
      }
      if (revisionsFileChannel != null && !lazyRevisionRecords) {
        // Legacy profile: make any buffered revisions bytes durable on close. The LAZY profile
        // deliberately SKIPS this force — it is a full device round-trip per writer (per COMMIT
        // under the per-transaction writer lifecycle) and redundant by design: every ring-window
        // record has a durable tail-log copy in the beacon slots, and anything older was made
        // durable by the eviction guard before its copy was reused. A crash after a clean close
        // recovers exactly like a crash before it: salvage + heal.
        revisionsFileChannel.force(metaData);
      }
      if (beaconDurableChannel != null && beaconDurableChannel.isOpen()) {
        beaconDurableChannel.close();
      }
      if (reader != null) {
        reader.close();
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Writer writeUberPageReference(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, final BytesOut<?> bufferedBytes) {
    try {
      if (bufferedBytes.writePosition() > 0) {
        flushBuffer(bufferedBytes);
      }

      // First commit on fresh files: write the superblocks (file identity: magic, layout
      // version, endianness, geometry). The header region is a sparse hole until now (so that
      // IOStorage.exists(), which checks size > 0, still distinguishes fresh resources) — and
      // because both files may already have grown PAST the header via sparse positioned writes
      // (the revision record lands at REVISIONS_RECORDS_START before this runs), presence is
      // probed via the magic bytes, not the file size.
      final long uuidMsb = resourceConfiguration.resourceUuid != null
          ? resourceConfiguration.resourceUuid.getMostSignificantBits() : 0L;
      final long uuidLsb = resourceConfiguration.resourceUuid != null
          ? resourceConfiguration.resourceUuid.getLeastSignificantBits() : 0L;
      if (writeRevisionsSuperblockIfMissing(resourceConfiguration) && lazyRevisionRecords) {
        // One-time: the revisions channel is BUFFERED in this profile, and the superblock (file
        // identity) must be durable before the first beacon acknowledges anything — the tail-log
        // protects records, not the superblock. Also covers the bootstrap revision's record.
        revisionsFileChannel.force(false);
        if (highestWrittenRevision >= 0) {
          durability.advance(highestWrittenRevision - 1);
        }
      }
      if (superblockMissing(dataFileChannel)) {
        final ByteBuffer sb = Superblock.build(Superblock.ROLE_DATA, uuidMsb, uuidLsb);
        while (sb.hasRemaining()) {
          dataFileChannel.write(sb, sb.position());
        }
      }

      if (lazyRevisionRecords) {
        // Persist the ring to BOTH beacon slots' pad region ahead of the write-ahead barrier
        // below — the barrier then makes the new revision's locator durable BEFORE either beacon
        // is touched, exactly the ordering the synchronous record write used to give. The
        // full-slot beacon writes later rewrite the identical bytes (the live ring is poked into
        // the slot images), so they cannot wipe it.
        final int uberRevision = page instanceof UberPage uberPage ? uberPage.getRevisionNumber() : -1;
        if (uberRevision >= 0 && tailLog != null
            && tailLogEntryRevision(IOStorage.tailLogRingIndex(uberRevision)) == uberRevision) {
          if (!ringCompleteAtInit) {
            // Rare gap (no predecessor handoff AND its entry was not yet on disk at init — the
            // depth-1 pipelined async case): re-merge the slots so the full-slot writes below
            // cannot destroy a record's only salvage source.
            mergeSlotTailLog(IOStorage.PRIMARY_BEACON_OFFSET, uberRevision);
            mergeSlotTailLog(IOStorage.SECONDARY_BEACON_OFFSET, uberRevision);
          }
          persistTailLogToSlots();
        } else {
          // This writer never staged the revision's record (unexpected flow) — fall back to the
          // legacy ordering: make the revisions file itself durable before the beacons go out.
          revisionsFileChannel.force(false);
          if (uberRevision >= 0) {
            durability.advance(uberRevision - 1);
          }
        }
      } else if (durability.beginLegacyTailCheck()) {
        // Legacy profile after a lazy-profile session: the full-slot beacon writes below WIPE the
        // on-disk ring, whose entries may be the only durable copy of trailing records. Make the
        // revisions file durable once before destroying that salvage source. Once per RESOURCE per
        // JVM — writers are per-commit, so a writer-local flag re-ran the scan on every commit.
        if (onDiskTailLogHasAnyValidEntry()) {
          revisionsFileChannel.force(false);
        }
      }

      // WRITE-AHEAD BARRIER: make the just-flushed page tail (which essentially always contains
      // the new RevisionRootPage — children are serialized first) durable BEFORE the uber page is
      // written. Without it, power loss could persist the new uber page pointing at revision
      // data that never reached disk, and recovery would then truncate to a bogus length
      // (bricked resource). force(true): the tail append grows the file, and "durable before the
      // beacons" must include the size extension even on stacks where fdatasync's
      // metadata-required-to-retrieve clause is weaker than POSIX promises (the power-loss
      // simulation's metadata-split model loses the tail ahead of the beacons otherwise).
      // Preallocated profile: the data file does not grow per commit (i_size is stable), so
      // fdatasync (force(false)) makes the just-flushed page tail durable WITHOUT the metadata-journal
      // commit that the growing-file force(true) forces — that journal tax is the remaining per-commit
      // cost this profile removes. The blocks were durably allocated up-front by growFile.
      dataFileChannel.force(!preallocatedCommit);
      // The revisions file needs NO explicit barrier: its only writes — the 32-byte record for
      // the new revision (during page serialization) and the one-time superblock — go through a
      // DSYNC-opened channel and are durable at write-return, well before any beacon advertises
      // the new revisionCount.

      writePageReference(resourceConfiguration, pageReference, page, bufferedBytes,
                         IOStorage.PRIMARY_BEACON_OFFSET);
      writePageReference(resourceConfiguration, pageReference, page, bufferedBytes,
                         IOStorage.SECONDARY_BEACON_OFFSET);

      if (lazyRevisionRecords && tailLog != null) {
        // The slot images just built end in ZERO pad — poke the live ring into both so the
        // full-slot beacon writes rewrite byte-identical content instead of wiping the copy
        // persisted ahead of the barrier.
        pokeTailLogIntoBeaconBuffers(bufferedBytes);
      }

      final var segment = (MemorySegment) bufferedBytes.underlyingObject();
      final var buffer = segment.asByteBuffer();
      final int slot = IOStorage.BEACON_SLOT_BYTES;
      if (bufferedBeacons && preallocatedCommit) {
        // B (low-latency beacons): write BOTH beacon copies buffered to the data channel, then make
        // them durable with ONE fdatasync — one fewer device round-trip than two O_DSYNC FUA writes.
        // The write-ahead force above already made the page tail durable, and the two copies live in
        // separate BEACON_SLOT_BYTES blocks, so this preserves both the write-ahead ordering (a valid
        // beacon never names a non-durable tail) and the dual-copy redundancy (a torn beacon flush
        // loses at most one copy; the survivor — the new revision or the prior one — stays valid).
        final ByteBuffer secondary = buffer.duplicate();
        secondary.position(slot).limit(2 * slot);
        while (secondary.hasRemaining()) {
          dataFileChannel.write(secondary, IOStorage.SECONDARY_BEACON_OFFSET + (secondary.position() - slot));
        }
        final ByteBuffer primary = buffer.duplicate();
        primary.position(0).limit(slot);
        while (primary.hasRemaining()) {
          dataFileChannel.write(primary, IOStorage.PRIMARY_BEACON_OFFSET + primary.position());
        }
        dataFileChannel.force(false);
      } else {
        // ORDERED dual-copy update through the WRITE-THROUGH beacon channel: each write is durable
        // when it returns (O_DSYNC — an FUA write on NVMe, far cheaper than a cache flush), so the
        // SECONDARY is durable before the primary is even issued, and the PRIMARY's write-return
        // IS the commit acknowledge — no explicit fsync needed for either. At every instant at
        // least one intact copy exists: the primary tearing leaves the new secondary; crashing
        // before the primary write leaves the old primary (whose data is intact, since the new
        // revision was never acknowledged). Residual risk: both copies share one 4 KiB filesystem
        // block, so block-granularity tearing remains a (format-level) exposure.
        final ByteBuffer secondary = buffer.duplicate();
        secondary.position(slot).limit(2 * slot);
        while (secondary.hasRemaining()) {
          beaconDurableChannel.write(secondary, IOStorage.SECONDARY_BEACON_OFFSET + (secondary.position() - slot));
        }
        final ByteBuffer primary = buffer.duplicate();
        primary.position(0).limit(slot);
        while (primary.hasRemaining()) {
          beaconDurableChannel.write(primary, IOStorage.PRIMARY_BEACON_OFFSET + primary.position());
        }
      }
      bufferedBytes.clear();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }

  @Override
  public void flushBufferedWrites(final BytesOut<?> bufferedBytes) {
    if (bufferedBytes.writePosition() > 0) {
      try {
        flushBuffer(bufferedBytes);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }
  }

  @Override
  public void forceAll() {
    try {
      // Preallocated profile: i_size is stable, so fdatasync (force(false)) suffices and avoids the
      // metadata-journal commit a growing-file fsync forces.
      final boolean metaData = !preallocatedCommit;
      if (dataFileChannel != null) {
        dataFileChannel.force(metaData);
      }
      if (revisionsFileChannel != null) {
        revisionsFileChannel.force(metaData);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }


  // ===== Revision-record tail log (lazy revision records) =====

  /**
   * Stages the just-written record's checksummed copy in the in-memory ring. Runs the EVICTION
   * GUARD first: the slot about to be reused may hold the only durable copy of a record written
   * {@code capacity} commits ago — if that record is not yet known durable, one
   * {@code force(false)} makes every buffered record durable before its salvage source dies.
   */
  private void stageTailLogEntry(final ResourceConfiguration resourceConfiguration, final int revision,
      final long offset, final long timestampMillis, final long recordChecksum, final long pageHash)
      throws IOException {
    ensureTailLogInitialised(resourceConfiguration, revision);
    final int ringIndex = IOStorage.tailLogRingIndex(revision);
    final long evictedRevision = tailLogEntryRevision(ringIndex);
    if (evictedRevision >= 0 && evictedRevision != revision && durability.highestDurable() < evictedRevision) {
      revisionsFileChannel.force(false);
      // Claim only COMPLETED commits: this one may still fail and be retried, and the retry
      // rewrites its record with content the force above never covered.
      durability.advance(revision - 1);
    }
    writeTailLogEntry(ringIndex, revision, offset, timestampMillis, recordChecksum, pageHash);
    durability.storeTailLogSnapshot(tailLog, revision);
    if (revision > highestWrittenRevision) {
      highestWrittenRevision = revision;
    }
  }

  /** Writes one entry into the in-memory ring at the given index. */
  private void writeTailLogEntry(final int ringIndex, final int revision, final long offset,
      final long timestampMillis, final long recordChecksum, final long pageHash) {
    final int base = ringIndex * IOStorage.REVISION_RECORD_TAIL_LOG_ENTRY_BYTES;
    tailLogView.putInt(base, revision);
    tailLogView.putInt(base + Integer.BYTES, 0);
    tailLogView.putLong(base + 8, offset);
    tailLogView.putLong(base + 16, timestampMillis);
    tailLogView.putLong(base + 24, recordChecksum);
    tailLogView.putLong(base + 32, pageHash);
    tailLogView.putLong(base + 40,
        IOStorage.tailLogEntryChecksum(revision, offset, timestampMillis, recordChecksum, pageHash));
  }

  /**
   * First use in this writer: adopt the predecessor's ring (per-JVM handoff) or merge the
   * surviving entries from BOTH on-disk beacon slots, purge entries of revisions at or above the
   * one being committed (a rolled-back stale timeline — those records are dead), and self-heal any
   * ring-window record the revisions file lost.
   */
  private void ensureTailLogInitialised(final ResourceConfiguration resourceConfiguration,
      final int currentRevision) throws IOException {
    if (tailLog != null) {
      return;
    }
    // Fast path: adopt the predecessor writer's ring. Writers are per-transaction, so without
    // this every commit re-derived the ring from the beacon slots — two 4 KiB preads plus up to
    // capacity record verifications per commit. It also covers the pipelined-async gap by
    // construction: the predecessor STAGED its entry (phase 1) before any successor can commit,
    // so the snapshot already contains it even while its beacon write is still in flight.
    final byte[] adopted = durability.adoptTailLogSnapshot(currentRevision);
    if (adopted != null) {
      tailLog = adopted;
      tailLogView = ByteBuffer.wrap(tailLog).order(ByteOrder.LITTLE_ENDIAN);
      ringCompleteAtInit = true;
      return;
    }
    tailLog = new byte[IOStorage.REVISION_RECORD_TAIL_LOG_BYTES];
    tailLogView = ByteBuffer.wrap(tailLog).order(ByteOrder.LITTLE_ENDIAN);
    mergeSlotTailLog(IOStorage.PRIMARY_BEACON_OFFSET, currentRevision - 1);
    mergeSlotTailLog(IOStorage.SECONDARY_BEACON_OFFSET, currentRevision - 1);
    purgeTailLogEntriesAtOrAbove(currentRevision);
    healRingWindowRecords(resourceConfiguration);
    ringCompleteAtInit = currentRevision == 0
        || tailLogEntryRevision(IOStorage.tailLogRingIndex(currentRevision - 1)) == currentRevision - 1;
  }

  /**
   * Merges an on-disk slot's ring into the in-memory one: a checksum-valid disk entry replaces the
   * in-memory entry at its index when it names a NEWER revision. Entries above
   * {@code maxRevisionInclusive} are ignored — they belong to a rolled-back timeline (or a
   * concurrent successor, which persists its own superset image).
   */
  private void mergeSlotTailLog(final long slotOffset, final long maxRevisionInclusive) {
    final ByteBuffer slot = reader.readBeaconSlot(slotOffset);
    if (slot.remaining() < IOStorage.BEACON_SLOT_BYTES) {
      return; // fresh or short file — no tail-log on disk
    }
    slot.order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < IOStorage.REVISION_RECORD_TAIL_LOG_CAPACITY; i++) {
      final int base = IOStorage.tailLogEntryOffsetAtIndex(i);
      if (!IOStorage.tailLogEntryValidAt(slot, base)) {
        continue;
      }
      final long diskRevision = slot.getInt(base);
      if (diskRevision > maxRevisionInclusive || diskRevision <= tailLogEntryRevision(i)) {
        continue;
      }
      slot.get(base, tailLog, i * IOStorage.REVISION_RECORD_TAIL_LOG_ENTRY_BYTES,
          IOStorage.REVISION_RECORD_TAIL_LOG_ENTRY_BYTES);
    }
  }

  /** The revision of the valid entry at the given ring index, or {@code -1}. */
  private long tailLogEntryRevision(final int ringIndex) {
    if (tailLog == null) {
      return -1L;
    }
    final int base = ringIndex * IOStorage.REVISION_RECORD_TAIL_LOG_ENTRY_BYTES;
    return IOStorage.tailLogEntryValidAt(tailLogView, base) ? tailLogView.getInt(base) : -1L;
  }

  /** Zeroes ring entries whose revision is at or above the given one (stale after a rollback). */
  private void purgeTailLogEntriesAtOrAbove(final int revision) {
    for (int i = 0; i < IOStorage.REVISION_RECORD_TAIL_LOG_CAPACITY; i++) {
      if (tailLogEntryRevision(i) >= revision) {
        final int base = i * IOStorage.REVISION_RECORD_TAIL_LOG_ENTRY_BYTES;
        Arrays.fill(tailLog, base, base + IOStorage.REVISION_RECORD_TAIL_LOG_ENTRY_BYTES, (byte) 0);
      }
    }
  }

  /**
   * Rewrites (from the ring) every ring-window record the revisions file lost or tore. The ring's
   * revisions are consecutive in the healthy case, so their records occupy ONE contiguous range —
   * a single bulk pread verifies all of them. When the revisions file lost its SUPERBLOCK too, it
   * is restored FIRST: records in a superblock-less file read as "non-empty with an all-zero
   * header", which the validator rejects as corruption.
   */
  private void healRingWindowRecords(final ResourceConfiguration resourceConfiguration) throws IOException {
    long minRevision = Long.MAX_VALUE;
    long maxRevision = -1L;
    for (int i = 0; i < IOStorage.REVISION_RECORD_TAIL_LOG_CAPACITY; i++) {
      final long entryRevision = tailLogEntryRevision(i);
      if (entryRevision >= 0) {
        minRevision = Math.min(minRevision, entryRevision);
        maxRevision = Math.max(maxRevision, entryRevision);
      }
    }
    if (maxRevision < 0) {
      return; // empty ring — fresh resource
    }
    // Clamp: a single entry surviving from an older ring image (torn slot write, restored data
    // file) would otherwise size the buffer from an arbitrary span — or overflow int.
    final long rangeSpan = maxRevision - minRevision + 1;
    if (rangeSpan > IOStorage.REVISION_RECORD_TAIL_LOG_CAPACITY) {
      return;
    }
    final int rangeRecords = (int) rangeSpan;
    final ByteBuffer rangeBuffer =
        ByteBuffer.allocate(rangeRecords * IOStorage.REVISIONS_FILE_RECORD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    final long rangeOffset = IOStorage.revisionsFileOffset((int) minRevision);
    int rangeRead = 0;
    while (rangeBuffer.hasRemaining()) {
      final int read = revisionsFileChannel.read(rangeBuffer, rangeOffset + rangeRead);
      if (read <= 0) {
        break; // EOF, or a zero-byte read (legal per FileChannel) — never spin on it
      }
      rangeRead += read;
    }

    boolean healedAny = false;
    for (int i = 0; i < IOStorage.REVISION_RECORD_TAIL_LOG_CAPACITY; i++) {
      final long entryRevision = tailLogEntryRevision(i);
      if (entryRevision < 0) {
        continue;
      }
      final int base = (int) (entryRevision - minRevision) * IOStorage.REVISIONS_FILE_RECORD_SIZE;
      boolean intact = false;
      if (base + IOStorage.REVISIONS_FILE_RECORD_SIZE <= rangeRead) {
        final long offset = rangeBuffer.getLong(base);
        final long timestampMillis = rangeBuffer.getLong(base + 8);
        final long storedChecksum = rangeBuffer.getLong(base + 16);
        final long pageHash = rangeBuffer.getLong(base + 24);
        intact = storedChecksum == IOStorage.expectedRevisionRecordChecksum(offset, timestampMillis, pageHash);
      }
      if (!intact) {
        if (!healedAny) {
          healedAny = true;
          writeRevisionsSuperblockIfMissing(resourceConfiguration);
        }
        final ByteBuffer heal = ByteBuffer.wrap(tailLog,
            i * IOStorage.REVISION_RECORD_TAIL_LOG_ENTRY_BYTES + Long.BYTES,
            IOStorage.REVISIONS_FILE_RECORD_SIZE).slice();
        final long recordOffset = IOStorage.revisionsFileOffset((int) entryRevision);
        while (heal.hasRemaining()) {
          if (revisionsFileChannel.write(heal, recordOffset + heal.position()) <= 0) {
            throw new IOException("Revision-record heal stalled: no progress");
          }
        }
      }
    }
    if (healedAny) {
      revisionsFileChannel.force(false);
    }
  }

  /**
   * Writes the revisions-file superblock when absent — shared by the first-commit path in
   * {@link #writeUberPageReference} and the init-heal above, so the two cannot drift.
   *
   * @return whether a superblock was written
   */
  private boolean writeRevisionsSuperblockIfMissing(final ResourceConfiguration resourceConfiguration)
      throws IOException {
    if (!superblockMissing(revisionsFileChannel)) {
      return false;
    }
    final long uuidMsb = resourceConfiguration.resourceUuid != null
        ? resourceConfiguration.resourceUuid.getMostSignificantBits() : 0L;
    final long uuidLsb = resourceConfiguration.resourceUuid != null
        ? resourceConfiguration.resourceUuid.getLeastSignificantBits() : 0L;
    final ByteBuffer sb = Superblock.build(Superblock.ROLE_REVISIONS, uuidMsb, uuidLsb);
    while (sb.hasRemaining()) {
      if (revisionsFileChannel.write(sb, sb.position()) <= 0) {
        throw new IOException("Revisions superblock write stalled: no progress");
      }
    }
    return true;
  }

  /** Persists the in-memory ring into BOTH beacon slots' reserved pad region. */
  private void persistTailLogToSlots() throws IOException {
    if (tailLogWriteBuffer == null) {
      tailLogWriteBuffer = ByteBuffer.allocateDirect(IOStorage.REVISION_RECORD_TAIL_LOG_BYTES)
                                     .order(ByteOrder.LITTLE_ENDIAN);
    }
    tailLogWriteBuffer.clear();
    tailLogWriteBuffer.put(tailLog);
    tailLogWriteBuffer.flip();
    writeTailLogToSlot(IOStorage.PRIMARY_BEACON_OFFSET);
    tailLogWriteBuffer.rewind();
    writeTailLogToSlot(IOStorage.SECONDARY_BEACON_OFFSET);
  }

  private void writeTailLogToSlot(final long slotOffset) throws IOException {
    final long target = slotOffset + IOStorage.REVISION_RECORD_TAIL_LOG_SLOT_OFFSET;
    while (tailLogWriteBuffer.hasRemaining()) {
      if (dataFileChannel.write(tailLogWriteBuffer, target + tailLogWriteBuffer.position()) <= 0) {
        throw new IOException("Tail-log slot write stalled: no progress");
      }
    }
  }

  /**
   * Pokes the live ring into the two beacon slot IMAGES so the full-slot beacon writes rewrite the
   * identical ring bytes instead of wiping the copy persisted ahead of the barrier.
   */
  private void pokeTailLogIntoBeaconBuffers(final BytesOut<?> bufferedBytes) {
    final MemorySegment segment = (MemorySegment) bufferedBytes.underlyingObject();
    final MemorySegment ring = MemorySegment.ofArray(tailLog);
    MemorySegment.copy(ring, 0, segment, IOStorage.REVISION_RECORD_TAIL_LOG_SLOT_OFFSET,
        IOStorage.REVISION_RECORD_TAIL_LOG_BYTES);
    MemorySegment.copy(ring, 0, segment,
        (long) IOStorage.BEACON_SLOT_BYTES + IOStorage.REVISION_RECORD_TAIL_LOG_SLOT_OFFSET,
        IOStorage.REVISION_RECORD_TAIL_LOG_BYTES);
  }

  /** Whether either on-disk slot still carries any valid ring entry (legacy-profile guard). */
  private boolean onDiskTailLogHasAnyValidEntry() {
    for (final long slotOffset : new long[] {IOStorage.PRIMARY_BEACON_OFFSET, IOStorage.SECONDARY_BEACON_OFFSET}) {
      final ByteBuffer slot = reader.readBeaconSlot(slotOffset);
      if (slot.remaining() < IOStorage.BEACON_SLOT_BYTES) {
        continue;
      }
      slot.order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < IOStorage.REVISION_RECORD_TAIL_LOG_CAPACITY; i++) {
        if (IOStorage.tailLogEntryValidAt(slot, IOStorage.tailLogEntryOffsetAtIndex(i))) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean superblockMissing(final FileChannel channel) throws IOException {
    final ByteBuffer probe = ByteBuffer.allocate(Superblock.MAGIC.length);
    final int read = channel.read(probe, 0);
    if (read < Superblock.MAGIC.length) {
      return true;
    }
    probe.flip();
    final byte[] bytes = new byte[Superblock.MAGIC.length];
    probe.get(bytes);
    return !java.util.Arrays.equals(bytes, Superblock.MAGIC);
  }

  private void flushBuffer(BytesOut<?> bufferedBytes) throws IOException {
    final var segment = (MemorySegment) bufferedBytes.underlyingObject();
    final var buffer = segment.asByteBuffer();
    if (preallocatedCommit) {
      final int len = buffer.remaining();
      final long offset = dataFrontier();
      ensureDataCapacity(offset + len);
      dataFileChannel.write(buffer, offset);
      dataLogicalEnd = offset + len;
      bufferedBytes.clear();
      return;
    }
    final long offset = Math.max(dataFileChannel.size(), IOStorage.DATA_REGION_START);
    dataFileChannel.write(buffer, offset);
    bufferedBytes.clear();
  }

  @Override
  protected Reader delegate() {
    return reader;
  }

  @Override
  public Writer truncate() {
    try {
      dataFileChannel.truncate(0);
      dataLogicalEnd = -1L;
      dataPreallocEnd = -1L;
      revisionsPreallocEnd = -1L;
      frontiersInitialised = false;

      if (revisionsFileChannel != null) {
        revisionsFileChannel.truncate(0);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }
}
