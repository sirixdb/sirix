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
import io.sirix.HftBoundaryTelemetry;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.io.AbstractForwardingReader;
import io.sirix.io.HashAlgorithm;
import io.sirix.io.IOStorage;
import io.sirix.io.PageHasher;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.RevisionRecordDurability;
import io.sirix.io.Superblock;
import io.sirix.io.Writer;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.PageSectionDiag;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesOut;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import net.openhft.hashing.Access;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

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
   * <p>
   * On by default; disabling it restores the legacy grow+{@code force(true)} path. (For a file that
   * has ALREADY run preallocated, the legacy path derives its append offset from the physical file
   * size, so subsequent commits land after the existing zero tail — readable and consistent, but the
   * padding becomes a permanent unreachable gap; the modes are only byte-identical for files that
   * never ran preallocated.) The logical write frontier is derived from the durable revision graph
   * (the last revision root, located via the uber beacon), NOT from the preallocation-inflated
   * physical file size. The zero-filled tail is PHYSICALLY allocated — that is the point: in-place
   * writes must never allocate fresh blocks — and it persists across sessions by design, because
   * trimming it on close would require fresh allocation on every session cycle. Data-file growth does
   * not force from the async snapshot worker: its metadata force is coalesced with the existing
   * write-ahead barrier before the uber beacons. Growth is adaptive (see
   * {@link #ensureDataCapacity}), so a small resource's at-rest padding stays proportional to its
   * size instead of paying the full {@link #preallocChunkBytes} cap up front.
   *
   * <p>
   * FILE_CHANNEL-only: the MEMORY_MAPPED backend constructs this writer too, but its readers only
   * remap the file when the PHYSICAL size grew — in-place preallocated commits leave the size
   * unchanged, so fresh readers would keep serving a mapping created before those commits (mmap
   * visibility of later {@code write()}s is unspecified). MM therefore passes
   * {@code preallocationSupported=false} and stays on the legacy grow path, whose per-commit size
   * growth is exactly the remap trigger.
   */
  private final boolean preallocatedCommit;

  /**
   * Upper bound in bytes for a single adaptive preallocation grow of the data file. Growth roughly
   * doubles the file per grow, from {@link #MIN_PREALLOC_CHUNK_BYTES} up to this cap — sustained
   * writers amortize allocation over many MiBs and coalesce its metadata force with the commit
   * barrier, while the physically-allocated at-rest padding of a small resource stays proportional to
   * its size.
   */
  private final long preallocChunkBytes = Long.getLong("sirix.commit.preallocChunkBytes", 8L * 1024 * 1024);

  /**
   * Smallest adaptive preallocation grow of the data file (also the small-resource padding floor).
   */
  private static final long MIN_PREALLOC_CHUNK_BYTES = 256L * 1024;

  /**
   * Fixed preallocation chunk for the revisions file, whose records are 32 bytes each
   * ({@code IOStorage.REVISIONS_FILE_RECORD_SIZE}) — 64 KiB covers 2,048 commits. The revisions
   * channel is opened O_DSYNC, so its zero-fill is synchronous write-through; keeping this chunk
   * small keeps that stall negligible (reusing the data file's multi-MiB chunk here would preallocate
   * hundreds of thousands of commits' worth through the sync channel).
   */
  private static final long REVISIONS_PREALLOC_CHUNK_BYTES = 64L * 1024;

  /**
   * Low-latency beacon durability (requires {@link #preallocatedCommit}). Makes the two uber-page
   * beacons durable with ONE buffered {@code fdatasync} on the data channel instead of two O_DSYNC
   * (FUA) writes through the beacon channel — one fewer device round-trip per commit. Both the
   * write-ahead ordering (page tail durable BEFORE the beacons) and the two-copy beacon redundancy
   * are preserved, so the durability contract is unchanged; only the I/O shape is cheaper. The two
   * beacons live in separate {@code BEACON_SLOT_BYTES} blocks, so a single torn block still leaves
   * the other copy, and the write-ahead guarantees whichever copy survives names a durable tail. On
   * by default (with {@code preallocatedCommit}); together the pair turns five device round-trips per
   * durable commit into three (measurements: docs/COMPARISON_POSTGRES.md §0.1).
   */
  private final boolean bufferedBeacons =
      Boolean.parseBoolean(System.getProperty("sirix.commit.bufferedBeacons", "true"));

  /**
   * Read-only zero block for {@link #allocateFileRange}; duplicated per use, never allocated per
   * call.
   */
  private static final ByteBuffer ZERO_BLOCK = ByteBuffer.allocateDirect(1 << 20).asReadOnlyBuffer();

  /** Same XXH3 implementation as {@link PageHasher#DEFAULT_ALGORITHM}, for exact scratch ranges. */
  private static final LongHashFunction XXH3_PAGE_HASH = LongHashFunction.xx3();

  private static final int FRAME_HASH_WINDOW_BYTES = 64 * 1024;

  /**
   * Logical write frontier of the data file (replaces {@code dataFileChannel.size()}); -1 = uninit.
   */
  private long dataLogicalEnd = -1L;
  /** Physical preallocated end of the data file ({@code >= dataLogicalEnd}). */
  private long dataPreallocEnd = -1L;
  /** Physical preallocated end of the revisions file (records land at deterministic slots). */
  private long revisionsPreallocEnd = -1L;
  /** Whether the data frontier has been derived from the durable revision graph this session. */
  private boolean frontiersInitialised;

  /**
   * Allocation-free, opt-in counters used to attribute an async-flush epoch's file-growth tail. The
   * flag is shared with {@code NodeStorageEngineWriter}'s HFT telemetry and is static-final so the
   * grow path contains no counter or clock work when telemetry is disabled.
   */
  private static final boolean HFT_TELEMETRY_ENABLED = Boolean.getBoolean("sirix.hft.telemetry");

  private long hftDataAllocationGrowCount;
  private long hftDataAllocationGrowBytes;
  private long hftDataAllocationGrowNanos;

  /**
   * Shared durability state for every writer borrowing the same data channel. A pooled writer must
   * not own this marker locally: its close-time metadata force can fail while its finally block
   * correctly returns the channel, and the successor must inherit the still-dirty requirement.
   * Owned/MM writers receive a private instance.
   */
  static final class DataAllocationDurability {
    private boolean metadataDirty;

    synchronized void markMetadataDirty() {
      metadataDirty = true;
    }

    /** Force now; clear a pending metadata requirement only after a successful metadata force. */
    synchronized void force(final FileChannel channel, final boolean forceMetadata) throws IOException {
      final boolean metadata = forceMetadata || metadataDirty;
      channel.force(metadata);
      if (metadata) {
        metadataDirty = false;
      }
    }
  }

  private final DataAllocationDurability dataAllocationDurability;

  /**
   * Lazy revision records (requires {@link #preallocatedCommit}). The per-commit 32-byte revision
   * record is written through a BUFFERED revisions channel (no O_SYNC device round-trip) and a
   * checksummed copy rides a {@link IOStorage#REVISION_RECORD_TAIL_LOG_CAPACITY}-entry ring — the
   * "tail-log" — in the last {@link IOStorage#REVISION_RECORD_TAIL_LOG_BYTES} bytes of BOTH
   * uber-beacon slots' zero pad. The ring is written to the data file BEFORE the existing write-ahead
   * {@code fdatasync} barrier, so the invariant "the revision's locator is durable before any beacon
   * advertises it" is preserved exactly; only the separate synchronous revisions write disappears.
   * Together with {@link #bufferedBeacons} a durable commit costs TWO device round-trips (write-ahead
   * barrier + beacon flush) instead of three.
   *
   * <p>
   * Recovery reads the record from the revisions file as before; a record the crash lost is salvaged
   * from the tail-log and healed back into the file
   * ({@link FileChannelReader#getRevisionFileData(int, int)}). A ring entry may only be EVICTED (its
   * 48-byte slot reused, {@code capacity} commits later) once its record is known durable — tracked
   * per resource in {@link RevisionRecordDurability} and enforced with a synchronous
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
   * In-memory image of the beacon tail-log ring; {@code null} until first initialised (adopted from
   * the predecessor writer or merged from the on-disk beacon slots). Nullness IS the initialised
   * state — no separate flag can drift out of sync with it.
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
   * predecessor's entry visible on disk). When {@code false}, the beacon phase re-merges the on-disk
   * slots before overwriting them — the depth-1 pipelined-async gap.
   */
  private boolean ringCompleteAtInit;

  /**
   * Release action for a writer borrowing the storage's SHARED channel triple, or {@code null} when
   * this writer owns its channels. See the constructor.
   */
  private final @Nullable Runnable releaseAction;

  /** Guards against a double close: the borrow must be handed back exactly once. */
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Temporary page serialization buffer.
   *
   * <p>
   * Pre-size to FLUSH_SIZE to avoid repeated grow/copy churn when serializing medium/large pages.
   */
  private final MemorySegmentBytesOut byteBufferBytes = MemorySegmentBytesOut.synchronousScratch(Writer.FLUSH_SIZE);

  /**
   * Constructor.
   *
   * @param dataFileChannel the data file channel
   * @param revisionsOffsetFileChannel the channel to the file, which holds pointers to the revision
   *        root pages — MUST be opened with {@link java.nio.file.StandardOpenOption#DSYNC}: the
   *        32-byte revision record (and the one-time superblock) are written through it, and the
   *        commit protocol relies on those writes being durable at write-return instead of paying a
   *        separate fsync per commit
   * @param beaconDurableChannel a SECOND channel to the data file, opened with
   *        {@link java.nio.file.StandardOpenOption#DSYNC}, used ONLY for the two uber-page beacon
   *        slot writes. Write-through gives the dual-beacon ordering (secondary durable before the
   *        primary is even issued) and the commit acknowledge (primary durable at write-return)
   *        without any explicit fsync — on NVMe these map to FUA writes, far cheaper than full cache
   *        flushes. The bulk data channel stays buffered.
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
   * @param lazyRevisionRecords whether the owning backend opened the revisions channel BUFFERED for
   *        the lazy-revision-record profile (see {@link #lazyRevisionRecords}); must be {@code false}
   *        when that channel is write-through. Only honored together with the preallocated profile
   * @param revisionsFilePath path of the revisions file, identity key for the durability state
   * @param resourceUuidMsb most significant resource-UUID half ({@code 0} = legacy, no UUID)
   * @param resourceUuidLsb least significant resource-UUID half ({@code 0} = legacy)
   * @param releaseAction callback for a writer borrowing the storage's SHARED channel triple:
   *        {@link #close()} runs it INSTEAD of closing the channels, so the storage can close the
   *        pool once the last borrower is gone. {@code null} means this writer owns its channels and
   *        closes them directly (recovery, test harnesses, {@code MMStorage})
   */
  public FileChannelWriter(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final FileChannel beaconDurableChannel, final SerializationType serializationType,
      final PagePersister pagePersister, final AsyncCache<Integer, RevisionFileData> cache,
      final RevisionIndexHolder revisionIndexHolder, final FileChannelReader reader, final boolean preallocatedCommit,
      final boolean lazyRevisionRecords, final Path revisionsFilePath, final long resourceUuidMsb,
      final long resourceUuidLsb, final @Nullable Runnable releaseAction) {
    this(dataFileChannel, revisionsOffsetFileChannel, beaconDurableChannel, serializationType, pagePersister, cache,
        revisionIndexHolder, reader, preallocatedCommit, lazyRevisionRecords, revisionsFilePath, resourceUuidMsb,
        resourceUuidLsb, new DataAllocationDurability(), releaseAction);
  }

  /** Constructor used by {@link FileChannelStorage}'s shared writer-channel pool. */
  FileChannelWriter(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final FileChannel beaconDurableChannel, final SerializationType serializationType,
      final PagePersister pagePersister, final AsyncCache<Integer, RevisionFileData> cache,
      final RevisionIndexHolder revisionIndexHolder, final FileChannelReader reader, final boolean preallocatedCommit,
      final boolean lazyRevisionRecords, final Path revisionsFilePath, final long resourceUuidMsb,
      final long resourceUuidLsb, final DataAllocationDurability dataAllocationDurability,
      final @Nullable Runnable releaseAction) {
    this.releaseAction = releaseAction;
    this.preallocatedCommit = preallocatedCommit;
    this.lazyRevisionRecords = lazyRevisionRecords && preallocatedCommit;
    this.dataAllocationDurability = requireNonNull(dataAllocationDurability);
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

  /**
   * Convenience constructor for a writer that OWNS its channels (no pooling).
   *
   * @see #FileChannelWriter(FileChannel, FileChannel, FileChannel, SerializationType, PagePersister,
   *      AsyncCache, RevisionIndexHolder, FileChannelReader, boolean, boolean, Path, long, long,
   *      Runnable)
   */
  public FileChannelWriter(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final FileChannel beaconDurableChannel, final SerializationType serializationType,
      final PagePersister pagePersister, final AsyncCache<Integer, RevisionFileData> cache,
      final RevisionIndexHolder revisionIndexHolder, final FileChannelReader reader, final boolean preallocatedCommit,
      final boolean lazyRevisionRecords, final Path revisionsFilePath, final long resourceUuidMsb,
      final long resourceUuidLsb) {
    this(dataFileChannel, revisionsOffsetFileChannel, beaconDurableChannel, serializationType, pagePersister, cache,
        revisionIndexHolder, reader, preallocatedCommit, lazyRevisionRecords, revisionsFilePath, resourceUuidMsb,
        resourceUuidLsb, null);
  }

  @Override
  public Writer truncateTo(final int revision) {
    try {
      final long fileSize = dataFileChannel.size();
      final int durableRevision = durableBeaconRevision();
      if (durableRevision < revision) {
        throw new SirixIOException("truncateTo(" + revision + "): durable beacon revision " + durableRevision
            + " is older than the requested frontier");
      }
      final RevisionFileData revisionFileData = reader.getRevisionFileData(revision);
      final ValidatedFrame frame = validateRevisionRootFrame(dataFileChannel, revisionFileData, revision, fileSize, -1L,
          "truncateTo(" + revision + ")");
      final long newSize = frame.frameEnd();

      RevisionRecordDurability.invalidateFor(revisionsFilePath);

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
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    return this;
  }

  /**
   * After truncating to {@code revision}, both beacon slots must advertise exactly that revision. The
   * crash this recovery handles (died between the secondary and primary beacon writes) leaves the
   * SECONDARY advertising the truncated-away revision — harmless for the happy path (the primary
   * wins), but until the next commit rewrote the slots, a primary corruption made fallback
   * dereference the stale-forward secondary and the resource unopenable although every surviving
   * revision was intact. Repair by copying the slot that matches the truncated-to revision over the
   * one that doesn't (which also heals a torn primary right at recovery instead of at the next
   * commit).
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
    readFully(dataFileChannel, slot, goodOffset);
    slot.flip();
    writeFully(dataFileChannel, slot, staleOffset);
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
   * durable revision graph — NOT from {@code channel.size()}, which preallocation inflates with
   * zeros.
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

    // Fast path: adopt the predecessor writer's frontier. Writers are per-transaction, so deriving
    // it from the durable revision graph costs a 4 KiB beacon pread, a revisions-record read and a
    // length-header pread on the FIRST write of EVERY commit. The handoff is dropped wholesale on
    // truncation and rollback (RevisionRecordDurability.invalidateFor), so it can never survive a
    // timeline change; the two physical sizes above are still read from the files themselves.
    if (adoptCachedDataFrontier()) {
      frontiersInitialised = true;
      return;
    }

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
      final RevisionFileData revisionFileData = reader.getRevisionFileData(lastRevision);
      dataLogicalEnd = validateRevisionRootFrame(dataFileChannel, revisionFileData, lastRevision, dataPreallocEnd, -1L,
          "preallocated frontier").frameEnd();
    }
    frontiersInitialised = true;
  }

  /**
   * Adopts the cached logical data frontier if it is CONSISTENT with the files as they are right now.
   * The snapshot is taken as one immutable triple, so the checks below run against the same values
   * that are adopted.
   *
   * <p>
   * Rejection is always safe — the caller falls back to deriving the frontier from the durable
   * revision graph. Accepting a WRONG frontier would not be: a too-small one overwrites live pages.
   * Hence three guards: the logical end must be a real data-region offset, it must not exceed the
   * preallocated end the storing writer recorded, and that preallocated end must still be backed by
   * the file (a file shrunk or replaced out of band fails here and falls back to disk).
   *
   * @return whether {@link #dataLogicalEnd} was adopted from the cache
   */
  private boolean adoptCachedDataFrontier() throws IOException {
    final long[] cached = durability.cachedFrontiers();
    final long cachedLogicalEnd = cached[0];
    final long cachedDataPreallocEnd = cached[1];
    final int cachedRevision = Math.toIntExact(cached[3]);
    final long cachedRevisionRootOffset = cached[4];
    final long cachedRevisionRootHash = cached[5];
    if (cachedLogicalEnd < IOStorage.DATA_REGION_START || cachedLogicalEnd > cachedDataPreallocEnd
        || cachedDataPreallocEnd > dataPreallocEnd || cachedRevision < 0 || durableBeaconRevision() != cachedRevision) {
      return false;
    }
    final RevisionFileData revisionFileData = reader.getRevisionFileData(cachedRevision);
    if (revisionFileData.offset() != cachedRevisionRootOffset
        || revisionFileData.pageHash() != cachedRevisionRootHash) {
      return false;
    }
    validateRevisionRootFrame(dataFileChannel, revisionFileData, cachedRevision, dataPreallocEnd, cachedLogicalEnd,
        "cached preallocated frontier");
    dataLogicalEnd = cachedLogicalEnd;
    return true;
  }

  private int durableBeaconRevision() {
    final int primaryRevision = reader.beaconRevisionOrMinusOne(IOStorage.PRIMARY_BEACON_OFFSET);
    return primaryRevision >= 0
        ? primaryRevision
        : reader.beaconRevisionOrMinusOne(IOStorage.SECONDARY_BEACON_OFFSET);
  }

  static ValidatedFrame validateRevisionRootFrame(final FileChannel channel, final RevisionFileData revisionFileData,
      final int revision, final long fileSize, final long expectedFrameEnd, final String context) throws IOException {
    requireNonNull(channel, "channel");
    requireNonNull(revisionFileData, "revisionFileData");
    requireNonNull(context, "context");
    final long frameOffset = revisionFileData.offset();
    if (revision < 0 || fileSize < IOStorage.DATA_REGION_START || frameOffset < IOStorage.DATA_REGION_START
        || frameOffset > fileSize - IOStorage.OTHER_BEACON || revisionFileData.pageHash() == 0L) {
      throw new SirixIOException(context + ": invalid revision-root identity for revision " + revision);
    }
    final ByteBuffer lengthBuffer = ByteBuffer.allocateDirect(IOStorage.OTHER_BEACON).order(ByteOrder.LITTLE_ENDIAN);
    readFully(channel, lengthBuffer, frameOffset);
    lengthBuffer.flip();
    final int dataLength = lengthBuffer.getInt();
    if (dataLength <= 0) {
      throw new SirixIOException(
          context + ": non-positive revision-root length " + dataLength + " for revision " + revision);
    }
    final long frameEnd = checkedFrameEnd(frameOffset, dataLength, context, revision);
    final long payloadOffset = frameOffset + IOStorage.OTHER_BEACON;
    if (frameEnd > fileSize || (expectedFrameEnd >= 0L && frameEnd != expectedFrameEnd)) {
      throw new SirixIOException(context + ": revision-root frame end " + frameEnd
          + " is inconsistent with durable boundary " + (expectedFrameEnd >= 0L
              ? expectedFrameEnd
              : fileSize)
          + " for revision " + revision);
    }
    final long actualHash;
    try {
      actualHash = IOStorage.normalizeRevisionRootPageHash(XXH3_PAGE_HASH.hash(
          new ChannelHashInput(channel, payloadOffset, dataLength), CHANNEL_HASH_ACCESS, 0L, dataLength));
    } catch (final UncheckedIOException readFailure) {
      throw readFailure.getCause();
    }
    if (actualHash != revisionFileData.pageHash()) {
      throw new SirixIOException(context + ": revision-root payload hash mismatch for revision " + revision);
    }
    return new ValidatedFrame(frameOffset, payloadOffset, frameEnd, dataLength);
  }

  static long checkedFrameEnd(final long frameOffset, final int dataLength, final String context, final int revision) {
    if (frameOffset < IOStorage.DATA_REGION_START || dataLength <= 0) {
      throw new SirixIOException(context + ": invalid revision-root frame metadata for revision " + revision);
    }
    try {
      return Math.addExact(Math.addExact(frameOffset, IOStorage.OTHER_BEACON), dataLength);
    } catch (final ArithmeticException overflow) {
      throw new SirixIOException(context + ": revision-root frame overflows for revision " + revision, overflow);
    }
  }

  record ValidatedFrame(long frameOffset, long payloadOffset, long frameEnd, int dataLength) {
  }

  private static final Access<ChannelHashInput> CHANNEL_HASH_ACCESS = new ChannelHashAccess(ByteOrder.LITTLE_ENDIAN);

  private static final class ChannelHashInput {
    private final FileChannel channel;
    private final long payloadOffset;
    private final int length;
    private final ByteBuffer window = ByteBuffer.allocateDirect(FRAME_HASH_WINDOW_BYTES + Long.BYTES);
    private long windowStart = -1L;

    private ChannelHashInput(final FileChannel channel, final long payloadOffset, final int length) {
      this.channel = channel;
      this.payloadOffset = payloadOffset;
      this.length = length;
    }

    private byte get(final long offset) {
      if (offset < 0 || offset >= length) {
        throw new IndexOutOfBoundsException("revision-root hash offset " + offset + " outside " + length);
      }
      if (windowStart < 0 || offset < windowStart || offset >= windowStart + window.limit()) {
        refill(offset);
      }
      return window.get(Math.toIntExact(offset - windowStart));
    }

    private void refill(final long offset) {
      windowStart = offset;
      final int bytes = Math.min(window.capacity(), Math.toIntExact((long) length - offset));
      window.clear().limit(bytes);
      try {
        readFully(channel, window, Math.addExact(payloadOffset, offset));
      } catch (final IOException failure) {
        throw new UncheckedIOException(failure);
      }
      window.flip();
    }
  }

  private static final class ChannelHashAccess extends Access<ChannelHashInput> {
    private final ByteOrder order;
    private final Access<ChannelHashInput> reverse;

    private ChannelHashAccess(final ByteOrder order) {
      this.order = order;
      this.reverse = order == ByteOrder.LITTLE_ENDIAN
          ? new ReverseChannelHashAccess(this)
          : null;
    }

    @Override
    public int getByte(final ChannelHashInput input, final long offset) {
      return input.get(offset);
    }

    @Override
    public ByteOrder byteOrder(final ChannelHashInput input) {
      return order;
    }

    @Override
    protected Access<ChannelHashInput> reverseAccess() {
      return reverse;
    }
  }

  private static final class ReverseChannelHashAccess extends Access<ChannelHashInput> {
    private final Access<ChannelHashInput> reverse;

    private ReverseChannelHashAccess(final Access<ChannelHashInput> reverse) {
      this.reverse = reverse;
    }

    @Override
    public int getByte(final ChannelHashInput input, final long offset) {
      return input.get(offset);
    }

    @Override
    public ByteOrder byteOrder(final ChannelHashInput input) {
      return ByteOrder.BIG_ENDIAN;
    }

    @Override
    protected Access<ChannelHashInput> reverseAccess() {
      return reverse;
    }
  }

  /**
   * Ensure the data file is physically block-allocated to at least {@code needed} bytes.
   *
   * <p>
   * The allocation's metadata durability is deliberately coalesced into the next write-ahead barrier
   * (or {@link #forceAll()}/{@link #close()}) instead of forcing inside an async snapshot rotation.
   * Until that barrier succeeds, no durable uber-page beacon can reference these bytes.
   * </p>
   */
  private void ensureDataCapacity(final long needed) throws IOException {
    if (needed > dataPreallocEnd) {
      // Adaptive chunk: roughly double the file per grow, clamped to
      // [MIN_PREALLOC_CHUNK_BYTES, preallocChunkBytes]. A tiny resource then carries at most
      // ~its own size (floor 256 KiB) of physically-allocated padding instead of a full
      // cap-sized chunk, while sustained writers still amortize allocation over each
      // up-to-the-cap grow and pay no separate metadata force in this worker.
      final long grow = Math.min(preallocChunkBytes, Math.max(MIN_PREALLOC_CHUNK_BYTES, dataPreallocEnd));
      final long target = Math.max(needed, dataPreallocEnd + grow);
      final long allocationStart = HFT_TELEMETRY_ENABLED
          ? System.nanoTime()
          : 0L;
      try {
        // Mark first: a partial zero-fill that extends i_size must still make close()/forceAll()
        // request a metadata force, and a failed final barrier must leave the requirement armed.
        dataAllocationDurability.markMetadataDirty();
        allocateFileRange(dataFileChannel, dataPreallocEnd, target);
      } finally {
        if (HFT_TELEMETRY_ENABLED) {
          // Count attempts as well as successes. A failed/partial grow is precisely the tail event
          // that a poisoned async epoch needs to retain for diagnosis, and the requested byte range
          // remains unambiguous because dataPreallocEnd is published only after success below.
          hftDataAllocationGrowCount++;
          hftDataAllocationGrowBytes += target - dataPreallocEnd;
          hftDataAllocationGrowNanos += System.nanoTime() - allocationStart;
        }
      }
      dataPreallocEnd = target;
    }
  }

  /**
   * Ensure the revisions file is physically (and durably) block-allocated to at least {@code needed}
   * bytes.
   */
  private void ensureRevisionsCapacity(final long needed) throws IOException {
    if (needed > revisionsPreallocEnd) {
      final long target = Math.max(needed, revisionsPreallocEnd + REVISIONS_PREALLOC_CHUNK_BYTES);
      allocateFileRange(revisionsFileChannel, revisionsPreallocEnd, target);
      // Keep revisions-file semantics unchanged. Its deterministic record may be needed for
      // recovery independently of a later data-file write-ahead barrier.
      revisionsFileChannel.force(true);
      revisionsPreallocEnd = target;
    }
  }

  /**
   * Physically allocate blocks in {@code [from, to)} by writing zeros. The caller owns the durability
   * barrier: revisions growth forces immediately, while data growth is coalesced with the commit's
   * existing write-ahead force after all reachable page-tail bytes have been written.
   */
  private static void allocateFileRange(final FileChannel channel, final long from, final long to) throws IOException {
    long off = from;
    while (off < to) {
      // duplicate() shares the one static zero block without allocating a fresh 1 MiB direct
      // buffer per grow (direct buffers are only reclaimed by GC-run Cleaners).
      final ByteBuffer zeros = ZERO_BLOCK.duplicate();
      if (zeros.remaining() > to - off) {
        zeros.limit((int) (to - off));
      }
      final int bytes = zeros.remaining();
      writeFully(channel, zeros, off);
      off += bytes;
    }
  }

  /**
   * Forces the data file, upgrading an otherwise content-only barrier when preallocation metadata is
   * still unforced. The dirty bit is cleared only after the force returns successfully.
   */
  private void forceDataFile(final boolean forceMetadata) throws IOException {
    dataAllocationDurability.force(dataFileChannel, forceMetadata);
  }

  /**
   * Writer-local data-allocation attempts for exact async-epoch attribution. Returns zero when HFT
   * telemetry is disabled. Read only after the append-owner handoff/fence.
   */
  public long hftDataAllocationGrowCount() {
    return hftDataAllocationGrowCount;
  }

  /** Requested bytes covered by {@link #hftDataAllocationGrowCount()} on this writer. */
  public long hftDataAllocationGrowBytes() {
    return hftDataAllocationGrowBytes;
  }

  /** Nanoseconds spent marking and zero-filling the writer-local data-allocation attempts. */
  public long hftDataAllocationGrowNanos() {
    return hftDataAllocationGrowNanos;
  }

  private static void writeToBufferedBytes(BytesOut<?> bufferedBytes, byte[] serializedPageBytes,
      MemorySegment serializedPageSegment, int serializedPageLength) {
    if (serializedPageSegment != null) {
      bufferedBytes.writeSegment(serializedPageSegment, 0, serializedPageLength);
    } else if (serializedPageBytes != null) {
      bufferedBytes.write(serializedPageBytes);
    }
  }

  /**
   * Fail before append-buffer alignment or native-address hashing can observe an invalid payload.
   *
   * <p>
   * The FFM access performed by {@code PagePersister} used to enforce both conditions as a side
   * effect. A pre-serialized KVL bypasses that copy, so the writer must retain the same temporal and
   * thread-confinement boundary explicitly. Both checks are allocation-free on the valid hot path.
   */
  private static void requireAccessiblePayload(final MemorySegment payload) {
    if (!payload.scope().isAlive()) {
      throw new IllegalStateException("Cannot append a page from a closed memory segment");
    }
    if (!payload.isAccessibleBy(Thread.currentThread())) {
      throw new WrongThreadException("Cannot append a page from a memory segment confined to another thread");
    }
  }


  private FileChannelWriter writePageReference(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, final BytesOut<?> bufferedBytes, long offset) {
    try {
      final int pageAlignmentPadding = serializationType == SerializationType.DATA && !(page instanceof UberPage)
          && offset % PAGE_FRAGMENT_BYTE_ALIGN != 0
              ? (int) (PAGE_FRAGMENT_BYTE_ALIGN - (offset & (PAGE_FRAGMENT_BYTE_ALIGN - 1)))
              : 0;
      final long referenceOffset = offset + pageAlignmentPadding;
      final long pageHash;
      if (page instanceof KeyValueLeafPage keyValueLeafPage) {
        // close() owns the same monitor. Pin the KVL serialization lifetime beginning before the
        // first cache read: a delayed async append must neither resurrect a fully closed/cacheless
        // page through PagePersister nor read a frame returned to the allocator during serialization.
        // bufferSerializedPage eagerly copies the payload, so the pin can end before file I/O.
        synchronized (keyValueLeafPage) {
          if (keyValueLeafPage.isClosed()) {
            throw new IllegalStateException("cannot append a closed key-value leaf page");
          }
          pageHash = bufferSerializedPage(resourceConfiguration, page, bufferedBytes, pageAlignmentPadding);
        }
      } else {
        pageHash = bufferSerializedPage(resourceConfiguration, page, bufferedBytes, pageAlignmentPadding);
      }

      if (bufferedBytes.writePosition() > FLUSH_SIZE) {
        flushBuffer(bufferedBytes);
      }

      // Remember page coordinates only after its append buffer has been flushed successfully when
      // required. A failed flush must not publish a durable-looking key/hash pair.
      pageReference.setKey(referenceOffset);
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
        final long storedPageHash = IOStorage.normalizeRevisionRootPageHash(pageHash);
        final ByteBuffer buffer =
            ByteBuffer.allocateDirect(IOStorage.REVISIONS_FILE_RECORD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(referenceOffset);
        buffer.putLong(revisionRootPage.getRevisionTimestamp());
        buffer.putLong(
            IOStorage.revisionRecordChecksum(referenceOffset, revisionRootPage.getRevisionTimestamp(), storedPageHash));
        buffer.putLong(storedPageHash);
        buffer.flip();
        final long revisionsFileOffset = IOStorage.revisionsFileOffset(revisionRootPage.getRevision());
        if (preallocatedCommit) {
          ensureRevisionsCapacity(revisionsFileOffset + IOStorage.REVISIONS_FILE_RECORD_SIZE);
        }
        writeFully(revisionsFileChannel, buffer, revisionsFileOffset);
        if (lazyRevisionRecords) {
          // The record above went through a BUFFERED channel — stage its checksummed copy in the
          // in-memory ring; writeUberPageReference persists the ring ahead of the write-ahead
          // barrier, so the locator is durable before any beacon names it.
          stageTailLogEntry(resourceConfiguration, revisionRootPage.getRevision(), referenceOffset,
              revisionRootPage.getRevisionTimestamp(), IOStorage.revisionRecordChecksum(referenceOffset,
                  revisionRootPage.getRevisionTimestamp(), storedPageHash),
              storedPageHash);
        }
        final long currOffset = referenceOffset;
        final long currTimestamp = revisionRootPage.getRevisionTimestamp();
        cache.put(revisionRootPage.getRevision(), CompletableFuture.supplyAsync(
            () -> new RevisionFileData(currOffset, Instant.ofEpochMilli(currTimestamp), storedPageHash)));
        // Update the optimized revision index
        revisionIndexHolder.addRevision(currOffset, currTimestamp);
      }

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    } finally {
      // In the empty-pipeline fast path serializedPageSegment aliases this reusable buffer. The
      // append above is an eager MemorySegment.copy, so no alias escapes. Reset on every exit,
      // including serialization/compression/write failures, so a retry can never append a stale
      // prefix left by the failed page.
      byteBufferBytes.clear();
    }
  }

  private long bufferSerializedPage(final ResourceConfiguration resourceConfiguration, final Page page,
      final BytesOut<?> bufferedBytes, final int pageAlignmentPadding) throws IOException {
    final var pipeline = resourceConfiguration.byteHandlePipeline;
    final boolean storageProfileEnabled = io.sirix.io.file.StorageProfile.isEnabled();
    byte[] serializedPageBytes = null;
    // A pre-serialized segment cache is already the exact payload that this method hashes and
    // appends. Running it through PagePersister first only copies the same bytes into
    // byteBufferBytes; the writer then ignores that scratch copy and selects this cache again
    // below. Async bulk flushes hit this branch for every primary leaf page, so avoid streaming the
    // complete database through a throwaway buffer a second time. The legacy BytesOut cache keeps
    // its historical PagePersister traversal and is covered separately below.
    final KeyValueLeafPage keyValueLeafPage = page instanceof KeyValueLeafPage leafPage
        ? leafPage
        : null;
    MemorySegment serializedPageSegment = keyValueLeafPage == null
        ? null
        : keyValueLeafPage.getCompressedSegment();
    final BytesOut<?> preSerializedLegacyBytes =
        !storageProfileEnabled || keyValueLeafPage == null || serializedPageSegment != null
            ? null
            : keyValueLeafPage.getBytes();
    final boolean preSerializedKeyValueLeafCache = serializedPageSegment != null || preSerializedLegacyBytes != null;
    final int cachedByteHandlerInputLength = storageProfileEnabled && preSerializedKeyValueLeafCache
        ? keyValueLeafPage.getByteHandlerInputLength()
        : KeyValueLeafPage.UNKNOWN_BYTE_HANDLER_INPUT_LENGTH;
    final boolean preSerializedSegment = serializedPageSegment != null;
    boolean segmentIsKeyValueLeafCache = preSerializedSegment;
    if (!preSerializedSegment) {
      pagePersister.serializePage(resourceConfiguration, byteBufferBytes, page, serializationType);
    }
    boolean serializedPageBorrowsScratch = false;

    if (!preSerializedSegment && keyValueLeafPage != null) {
      // Check compressed MemorySegment cache first (slotted page format path).
      serializedPageSegment = keyValueLeafPage.getCompressedSegment();
      if (serializedPageSegment != null) {
        segmentIsKeyValueLeafCache = true;
      } else {
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
      if (pipeline.isEmpty()) {
        // The empty pipeline is identity. ByteHandlerPipeline.compress() must normally return an
        // owned copy because general callers may cache its result; this writer does not. It hashes
        // and copies the exact [0, writePosition) scratch range into bufferedBytes synchronously
        // below and publishes only the hash/offset. baseSegment() deliberately avoids allocating an
        // exact-view wrapper for every varying structural-page length; every consumer below receives
        // the logical length separately. Keeping the live view local avoids one page-sized heap
        // byte[] per pinned-trie prewrite without weakening the pipeline's ownership contract for KVL
        // caches or any other caller.
        serializedPageSegment = byteBufferBytes.baseSegment();
        serializedPageBorrowsScratch = true;
      } else if (pipeline.supportsMemorySegments()) {
        serializedPageSegment = pipeline.compress(byteBufferBytes.getDestination());
      } else {
        final byte[] byteArray = byteBufferBytes.toByteArray();
        try (final ByteArrayOutputStream output = new ByteArrayOutputStream(byteArray.length);
            final DataOutputStream dataOutput = new DataOutputStream(reader.getByteHandler().serialize(output))) {
          dataOutput.write(byteArray);
          dataOutput.flush();
          serializedPageBytes = output.toByteArray();
        }
      }
    }

    if (serializedPageSegment != null) {
      // Validate before changing bufferedBytes.writePosition() for alignment below. A rejected
      // cached segment must leave the append buffer reusable by the next page.
      requireAccessiblePayload(serializedPageSegment);
    }

    final int serializedPageLength;
    if (serializedPageBorrowsScratch) {
      serializedPageLength = Math.toIntExact(byteBufferBytes.writePosition());
    } else if (serializedPageSegment != null) {
      serializedPageLength = (int) serializedPageSegment.byteSize();
    } else if (serializedPageBytes != null) {
      serializedPageLength = serializedPageBytes.length;
    } else {
      throw new IllegalStateException("Failed to build serialized page payload");
    }

    if (PageSectionDiag.ENABLED && keyValueLeafPage != null) {
      // [DIAG] The single choke point for bytes that reach the file. The serialization ledger's
      // encode counts are meaningless without it: only a per-index-type write count can say which
      // pages are encoded more often than they are written.
      //
      // A leaf reaches this writer without an index type in the unit tests that drive it directly,
      // and the core test task turns the diagnostic ON by default, so an unguarded dereference here
      // fails eight of them. A page whose type is unknown is simply not attributed: the ledger's
      // question is per-type, and a diagnostic may never decide whether a page is written.
      final var writtenIndexType = keyValueLeafPage.getIndexType();
      if (writtenIndexType != null) {
        PageSectionDiag.recordPageWrite(writtenIndexType.getID());
      }
    }

    if (storageProfileEnabled) {
      final String pageKind = page.getClass().getSimpleName();
      if (preSerializedKeyValueLeafCache) {
        // Metadata is captured before PagePersister: the legacy cache path still copies its
        // already-processed bytes into scratch, so scratch length is not the raw length either.
        int rawSize = cachedByteHandlerInputLength;
        if (rawSize == KeyValueLeafPage.UNKNOWN_BYTE_HANDLER_INPUT_LENGTH && pipeline.isEmpty()) {
          rawSize = serializedPageLength; // identity pipeline: encoded and raw are provably equal
        }
        if (rawSize == KeyValueLeafPage.UNKNOWN_BYTE_HANDLER_INPUT_LENGTH) {
          io.sirix.io.file.StorageProfile.recordUnknownRaw(pageKind, serializedPageLength);
        } else {
          io.sirix.io.file.StorageProfile.record(pageKind, rawSize, serializedPageLength);
        }
      } else {
        io.sirix.io.file.StorageProfile.record(pageKind, Math.toIntExact(byteBufferBytes.writePosition()),
            serializedPageLength);
      }
    }

    int offsetToAdd = pageAlignmentPadding;

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
      }
    }

    // Compute hash on compressed bytes for ALL page types (consistent approach). Computed
    // BEFORE buffering: the uber beacon slot embeds it as an integrity trailer.
    final long pageHash;
    boolean payloadBuffered = false;
    if (serializedPageBorrowsScratch) {
      if (PageHasher.DEFAULT_ALGORITHM != HashAlgorithm.XXH3) {
        throw new IllegalStateException("The scratch-range hasher must track the default page-hash algorithm");
      }
      // hashDirect consumes exactly MemorySegmentBytesOut.writePosition(), not the spare capacity
      // exposed by baseSegment(), and the primitive result flows to storage/reference metadata
      // without materializing the canonical byte[8] representation.
      pageHash = byteBufferBytes.hashDirect(XXH3_PAGE_HASH);
    } else if (serializedPageSegment != null) {
      if (segmentIsKeyValueLeafCache) {
        // The KVL wrapper holds the page monitor from before its first cache read through this
        // eager copy. Revalidate nevertheless: serialization may legitimately replace a cache,
        // while CLOSED_BIT and the current frame remain the authoritative teardown/ownership state.
        assert Thread.holdsLock(keyValueLeafPage);
        if (keyValueLeafPage.isClosed()) {
          throw new IllegalStateException("cannot append the serialized cache of a closed page");
        }
        if (keyValueLeafPage.getCompressedSegment() != serializedPageSegment) {
          throw new IllegalStateException("serialized page cache changed while an append was in progress");
        }
        requireAccessiblePayload(serializedPageSegment);
        final MemorySegment frame = keyValueLeafPage.getSlottedPage();
        final boolean pageOwnedFrame = serializedPageSegment.isNative() && frame != null && frame.isNative()
            && serializedPageSegment.address() == frame.address()
            && serializedPageSegment.byteSize() <= frame.byteSize();
        pageHash = pageOwnedFrame
            ? XXH3_PAGE_HASH.hashMemory(serializedPageSegment.address(), serializedPageSegment.byteSize())
            : PageHasher.computeLong(serializedPageSegment, PageHasher.DEFAULT_ALGORITHM);
        if (!(page instanceof UberPage) && offsetToAdd > 0) {
          bufferedBytes.writePosition(bufferedBytes.writePosition() + offsetToAdd);
        }
        bufferedBytes.writeInt(serializedPageLength);
        writeToBufferedBytes(bufferedBytes, serializedPageBytes, serializedPageSegment, serializedPageLength);
        payloadBuffered = true;
      } else {
        // Arbitrary pipeline/arena segments stay on FFM-checked accesses: an isAlive() pre-check
        // cannot pin a closeable shared arena against a concurrent close.
        pageHash = PageHasher.computeLong(serializedPageSegment, PageHasher.DEFAULT_ALGORITHM);
      }
    } else if (serializedPageBytes != null) {
      pageHash = PageHasher.computeLong(serializedPageBytes);
    } else {
      throw new IllegalStateException("Failed to compute page hash due to missing payload");
    }

    if (!payloadBuffered) {
      if (!(page instanceof UberPage) && offsetToAdd > 0) {
        bufferedBytes.writePosition(bufferedBytes.writePosition() + offsetToAdd);
      }
      bufferedBytes.writeInt(serializedPageLength);
      writeToBufferedBytes(bufferedBytes, serializedPageBytes, serializedPageSegment, serializedPageLength);
    }

    if (page instanceof UberPage) {
      // Beacon integrity trailer: recovery validates [len][payload][xxh3] instead of relying
      // on "deserialization didn't throw" (the beacons have no parent reference to carry a
      // checksum, unlike every other page). BytesOut is little-endian; reverse to retain the
      // canonical big-endian 8-byte checksum wire used before hashes became primitive.
      bufferedBytes.writeLong(Long.reverseBytes(pageHash));
      if (offsetToAdd > 0) {
        bufferedBytes.write(new byte[(int) offsetToAdd]);
      }
    }
    return pageHash;
  }

  @Override
  public void close() {
    // Idempotent: a second close must not force again, and — when the channels are POOLED — must
    // not hand the same borrow back twice, which would let the pool close channels still in use.
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    try {
      // Preallocated profile: fdatasync suffices when i_size was already durable. An aborted writer
      // may instead close with freshly zero-filled allocation metadata that never reached a commit
      // barrier; forceDataFile upgrades that close to fsync before the shared channel can be handed
      // to a successor. The preallocated tail is intentionally NOT trimmed here — writers are
      // per-transaction, so trimming on every close would force a fresh preallocation each commit;
      // the file stays at high-water-mark and the frontier is re-derived from the durable revision
      // graph on reopen.
      final boolean metaData = !preallocatedCommit;
      if (dataFileChannel != null) {
        forceDataFile(metaData);
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
    } catch (final IOException e) {
      throw new SirixIOException(e);
    } finally {
      // In a finally block: a failed force must still surrender the channels, or a single I/O
      // error leaks three descriptors per commit AND pins the pool open forever.
      if (releaseAction != null) {
        // Borrowed: the reader delegate shares the pooled channels and was given a no-op release,
        // so closing it frees its own state without touching them.
        if (reader != null) {
          reader.close();
        }
        releaseAction.run();
      } else {
        try {
          if (beaconDurableChannel != null && beaconDurableChannel.isOpen()) {
            beaconDurableChannel.close();
          }
        } catch (final IOException e) {
          throw new SirixIOException(e);
        } finally {
          if (reader != null) {
            reader.close();
          }
        }
      }
    }
  }

  @Override
  public Writer writeUberPageReference(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, final BytesOut<?> bufferedBytes) {
    try {
      if (bufferedBytes.writePosition() > 0) {
        flushBuffer(bufferedBytes);
      }

      // Resolve the durable revision-root identity BEFORE either uber beacon can be published.
      // The preallocated writer hands this exact identity to its successor after the beacon
      // acknowledgement below; discovering a missing/torn revisions record only afterwards would
      // throw while leaving both durable beacons advertising a revision that cannot be opened.
      // A bare low-level UberPage write has no data frontier to hand off and retains its historical
      // test/storage-bootstrap behaviour.
      final RevisionFileData durableRevisionFileData;
      if (frontiersInitialised) {
        if (!(page instanceof UberPage uberPage) || uberPage.getRevisionNumber() < 0) {
          throw new SirixIOException("cannot publish a data frontier without an uber-page revision identity");
        }
        durableRevisionFileData = reader.getRevisionFileData(uberPage.getRevisionNumber());
      } else {
        durableRevisionFileData = null;
      }

      // First commit on fresh files: write the superblocks (file identity: magic, layout
      // version, endianness, geometry). The header region is a sparse hole until now (so that
      // IOStorage.exists(), which checks size > 0, still distinguishes fresh resources) — and
      // because both files may already have grown PAST the header via sparse positioned writes
      // (the revision record lands at REVISIONS_RECORDS_START before this runs), presence is
      // probed via the magic bytes, not the file size.
      final long uuidMsb = resourceConfiguration.resourceUuid != null
          ? resourceConfiguration.resourceUuid.getMostSignificantBits()
          : 0L;
      final long uuidLsb = resourceConfiguration.resourceUuid != null
          ? resourceConfiguration.resourceUuid.getLeastSignificantBits()
          : 0L;
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
        writeFully(dataFileChannel, sb, 0L);
      }

      if (lazyRevisionRecords) {
        // Persist the ring to BOTH beacon slots' pad region ahead of the write-ahead barrier
        // below — the barrier then makes the new revision's locator durable BEFORE either beacon
        // is touched, exactly the ordering the synchronous record write used to give. The
        // full-slot beacon writes later rewrite the identical bytes (the live ring is poked into
        // the slot images), so they cannot wipe it.
        final int uberRevision = page instanceof UberPage uberPage
            ? uberPage.getRevisionNumber()
            : -1;
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
      // cost this profile removes. When this transaction DID extend the preallocated region, the
      // allocation is still metadata-dirty and forceDataFile upgrades this one barrier to
      // force(true), coalescing allocation + page-tail durability before either beacon is written.
      forceDataFile(!preallocatedCommit);
      // The revisions file needs NO explicit barrier: its only writes — the 32-byte record for
      // the new revision (during page serialization) and the one-time superblock — go through a
      // DSYNC-opened channel and are durable at write-return, well before any beacon advertises
      // the new revisionCount.

      writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, IOStorage.PRIMARY_BEACON_OFFSET);
      writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, IOStorage.SECONDARY_BEACON_OFFSET);

      if (lazyRevisionRecords && tailLog != null) {
        // The slot images just built end in ZERO pad — poke the live ring into both so the
        // full-slot beacon writes rewrite byte-identical content instead of wiping the copy
        // persisted ahead of the barrier.
        pokeTailLogIntoBeaconBuffers(bufferedBytes);
      }

      final ByteBuffer buffer = readableByteBuffer(bufferedBytes);
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
        writeFully(dataFileChannel, secondary, IOStorage.SECONDARY_BEACON_OFFSET);
        final ByteBuffer primary = buffer.duplicate();
        primary.position(0).limit(slot);
        writeFully(dataFileChannel, primary, IOStorage.PRIMARY_BEACON_OFFSET);
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
        writeFully(beaconDurableChannel, secondary, IOStorage.SECONDARY_BEACON_OFFSET);
        final ByteBuffer primary = buffer.duplicate();
        primary.position(0).limit(slot);
        writeFully(beaconDurableChannel, primary, IOStorage.PRIMARY_BEACON_OFFSET);
      }
      bufferedBytes.clear();

      // Hand the frontier to the NEXT writer — only now, with the commit acknowledged. Publishing
      // earlier would let a failed-and-retried commit leave a successor starting past a revision
      // that never became durable: harmless for correctness (the store is append-only) but it
      // would strand the skipped range forever.
      if (frontiersInitialised) {
        final UberPage uberPage = (UberPage) page;
        final int durableRevision = uberPage.getRevisionNumber();
        durability.storeFrontiers(dataLogicalEnd, dataPreallocEnd, revisionsPreallocEnd, durableRevision,
            durableRevisionFileData.offset(), durableRevisionFileData.pageHash());
      }
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
  public boolean supportsReclaimableUncommittedWrites() {
    // A new preallocated writer resumes from the last durable logical frontier, not i_size. An
    // aborted batch is therefore overwritten by the next writer. The legacy append profile uses
    // physical size as its frontier and would strand every prewritten page permanently.
    return preallocatedCommit;
  }

  @Override
  public void forceAll() {
    try {
      // Preallocated profile: fdatasync (force(false)) suffices when i_size is already durable and
      // avoids the metadata-journal commit a growing-file fsync forces. Fresh allocation instead
      // upgrades this explicit barrier to force(true); a failed force keeps that requirement armed.
      final boolean metaData = !preallocatedCommit;
      if (dataFileChannel != null) {
        forceDataFile(metaData);
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
   * Stages the just-written record's checksummed copy in the in-memory ring. Runs the EVICTION GUARD
   * first: the slot about to be reused may hold the only durable copy of a record written
   * {@code capacity} commits ago — if that record is not yet known durable, one {@code force(false)}
   * makes every buffered record durable before its salvage source dies.
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
  private void writeTailLogEntry(final int ringIndex, final int revision, final long offset, final long timestampMillis,
      final long recordChecksum, final long pageHash) {
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
   * First use in this writer: adopt the predecessor's ring (per-JVM handoff) or merge the surviving
   * entries from BOTH on-disk beacon slots, purge entries of revisions at or above the one being
   * committed (a rolled-back stale timeline — those records are dead), and self-heal any ring-window
   * record the revisions file lost.
   */
  private void ensureTailLogInitialised(final ResourceConfiguration resourceConfiguration, final int currentRevision)
      throws IOException {
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
   * {@code maxRevisionInclusive} are ignored — they belong to a rolled-back timeline (or a concurrent
   * successor, which persists its own superset image).
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
    return IOStorage.tailLogEntryValidAt(tailLogView, base)
        ? tailLogView.getInt(base)
        : -1L;
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
   * revisions are consecutive in the healthy case, so their records occupy ONE contiguous range — a
   * single bulk pread verifies all of them. When the revisions file lost its SUPERBLOCK too, it is
   * restored FIRST: records in a superblock-less file read as "non-empty with an all-zero header",
   * which the validator rejects as corruption.
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
      final int read = readAt(revisionsFileChannel, rangeBuffer, rangeOffset + rangeRead);
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
        final ByteBuffer heal =
            ByteBuffer.wrap(tailLog, i * IOStorage.REVISION_RECORD_TAIL_LOG_ENTRY_BYTES + Long.BYTES,
                IOStorage.REVISIONS_FILE_RECORD_SIZE).slice();
        final long recordOffset = IOStorage.revisionsFileOffset((int) entryRevision);
        writeFully(revisionsFileChannel, heal, recordOffset);
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
        ? resourceConfiguration.resourceUuid.getMostSignificantBits()
        : 0L;
    final long uuidLsb = resourceConfiguration.resourceUuid != null
        ? resourceConfiguration.resourceUuid.getLeastSignificantBits()
        : 0L;
    final ByteBuffer sb = Superblock.build(Superblock.ROLE_REVISIONS, uuidMsb, uuidLsb);
    writeFully(revisionsFileChannel, sb, 0L);
    return true;
  }

  /** Persists the in-memory ring into BOTH beacon slots' reserved pad region. */
  private void persistTailLogToSlots() throws IOException {
    if (tailLogWriteBuffer == null) {
      tailLogWriteBuffer =
          ByteBuffer.allocateDirect(IOStorage.REVISION_RECORD_TAIL_LOG_BYTES).order(ByteOrder.LITTLE_ENDIAN);
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
    writeFully(dataFileChannel, tailLogWriteBuffer, target);
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
    final int read = readAt(channel, probe, 0L);
    if (read < Superblock.MAGIC.length) {
      return true;
    }
    probe.flip();
    final byte[] bytes = new byte[Superblock.MAGIC.length];
    probe.get(bytes);
    return !java.util.Arrays.equals(bytes, Superblock.MAGIC);
  }

  private void flushBuffer(BytesOut<?> bufferedBytes) throws IOException {
    final ByteBuffer buffer = readableByteBuffer(bufferedBytes);
    if (preallocatedCommit) {
      final int len = buffer.remaining();
      final long offset = dataFrontier();
      ensureDataCapacity(offset + len);
      writeFully(dataFileChannel, buffer, offset);
      dataLogicalEnd = offset + len;
      bufferedBytes.clear();
      return;
    }
    final long offset = Math.max(dataFileChannel.size(), IOStorage.DATA_REGION_START);
    writeFully(dataFileChannel, buffer, offset);
    bufferedBytes.clear();
  }

  /**
   * Prepare an exact readable view for synchronous channel writes.
   *
   * <p>
   * The production append buffers use {@link MemorySegmentBytesOut}, whose per-instance view is
   * reused across varying flush lengths and rebuilt only after its backing segment grows. Keeping the
   * cache on the buffer—rather than this writer—also keeps foreground and background append ownership
   * independent. Other {@link BytesOut} implementations retain the generic segment fallback and are
   * explicitly limited to their logical write position.
   * </p>
   */
  static ByteBuffer readableByteBuffer(final BytesOut<?> bufferedBytes) {
    requireNonNull(bufferedBytes);
    if (bufferedBytes instanceof MemorySegmentBytesOut memorySegmentBytesOut) {
      return memorySegmentBytesOut.readableByteBuffer();
    }

    final MemorySegment segment = (MemorySegment) bufferedBytes.underlyingObject();
    final long writtenLength = bufferedBytes.writePosition();
    if (writtenLength < 0L || writtenLength > segment.byteSize()) {
      throw new IndexOutOfBoundsException(
          "Write position " + writtenLength + " is outside segment capacity " + segment.byteSize());
    }
    final ByteBuffer buffer = segment.asByteBuffer();
    buffer.limit(Math.toIntExact(writtenLength));
    return buffer;
  }

  /**
   * Drain {@code buffer} with positional writes, preserving the exact append offset across legal
   * short writes. A blocking regular-file channel should always make progress; returning zero while
   * bytes remain is treated as an I/O failure rather than spinning forever or publishing a torn
   * record tail.
   */
  static void writeFully(final FileChannel channel, final ByteBuffer buffer, final long offset) throws IOException {
    long writeOffset = offset;
    while (buffer.hasRemaining()) {
      final int written = channel.write(buffer, writeOffset);
      if (written <= 0) {
        throw new IOException("Positional file write made no progress at offset " + writeOffset + " with "
            + buffer.remaining() + " bytes remaining");
      }
      HftBoundaryTelemetry.storageWrite(written);
      writeOffset += written;
    }
  }

  static void readFully(final FileChannel channel, final ByteBuffer buffer, final long offset) throws IOException {
    long readOffset = offset;
    while (buffer.hasRemaining()) {
      final int read = readAt(channel, buffer, readOffset);
      if (read <= 0) {
        throw new IOException("Positional file read made no progress at offset " + readOffset + " with "
            + buffer.remaining() + " bytes remaining");
      }
      readOffset += read;
    }
  }

  private static int readAt(final FileChannel channel, final ByteBuffer buffer, final long offset) throws IOException {
    final int read = channel.read(buffer, offset);
    if (read > 0) {
      HftBoundaryTelemetry.storageRead(read);
    }
    return read;
  }

  @Override
  protected Reader delegate() {
    return reader;
  }

  @Override
  public Writer truncate() {
    try {
      RevisionRecordDurability.invalidateFor(revisionsFilePath);
      durability = RevisionRecordDurability.forFile(revisionsFilePath, resourceUuidMsb, resourceUuidLsb);
      tailLog = null;
      tailLogView = null;
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
