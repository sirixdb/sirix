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
import io.sirix.api.StorageEngineReader;
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
import java.time.Instant;
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
   */
  public FileChannelWriter(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final FileChannel beaconDurableChannel, final SerializationType serializationType,
      final PagePersister pagePersister, final AsyncCache<Integer, RevisionFileData> cache,
      final RevisionIndexHolder revisionIndexHolder, final FileChannelReader reader) {
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
  public Writer truncateTo(final StorageEngineReader storageEngineReader, final int revision) {
    try {
      final var dataFileRevisionRootPageOffset =
          cache.get(revision, _ -> getRevisionFileData(revision)).get(5, TimeUnit.SECONDS).offset();

      // Read the length header from the file — VALIDATING every step. This code runs during
      // crash recovery, exactly when the on-disk state may be garbage: an unchecked short read
      // left the zero-filled buffer (dataLength=0) and a corrupt/negative length silently
      // truncated INTO older committed revisions (destroying good data) or threw from a
      // negative truncation size.
      final var buffer = ByteBuffer.allocateDirect(IOStorage.OTHER_BEACON).order(ByteOrder.nativeOrder());
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

      repairBeaconSlotsAfterTruncate(revision);
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
    // The header region (superblock + beacon slots) is a SPARSE hole until the first commit
    // writes it — data pages always start at DATA_REGION_START.
    return Math.max(dataFileChannel.size(), IOStorage.DATA_REGION_START) + bufferedBytes.writePosition();
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
        // first 16 bytes: these records are the only path to any RevisionRootPage and used to
        // be completely unprotected.
        final ByteBuffer buffer =
            ByteBuffer.allocateDirect(IOStorage.REVISIONS_FILE_RECORD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(offset);
        buffer.putLong(revisionRootPage.getRevisionTimestamp());
        buffer.putLong(IOStorage.revisionRecordChecksum(offset, revisionRootPage.getRevisionTimestamp()));
        buffer.putLong(0L); // reserved
        buffer.flip();
        final long revisionsFileOffset = IOStorage.revisionsFileOffset(revisionRootPage.getRevision());
        while (buffer.hasRemaining()) {
          revisionsFileChannel.write(buffer, revisionsFileOffset + buffer.position());
        }
        final long currOffset = offset;
        final long currTimestamp = revisionRootPage.getRevisionTimestamp();
        cache.put(revisionRootPage.getRevision(), CompletableFuture.supplyAsync(
            () -> new RevisionFileData(currOffset, Instant.ofEpochMilli(currTimestamp))));
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
      if (dataFileChannel != null) {
        dataFileChannel.force(true);
      }
      if (revisionsFileChannel != null) {
        revisionsFileChannel.force(true);
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
      if (superblockMissing(revisionsFileChannel)) {
        final ByteBuffer sb = Superblock.build(Superblock.ROLE_REVISIONS);
        while (sb.hasRemaining()) {
          revisionsFileChannel.write(sb, sb.position());
        }
      }
      if (superblockMissing(dataFileChannel)) {
        final ByteBuffer sb = Superblock.build(Superblock.ROLE_DATA);
        while (sb.hasRemaining()) {
          dataFileChannel.write(sb, sb.position());
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
      dataFileChannel.force(true);
      // The revisions file needs NO explicit barrier: its only writes — the 32-byte record for
      // the new revision (during page serialization) and the one-time superblock — go through a
      // DSYNC-opened channel and are durable at write-return, well before any beacon advertises
      // the new revisionCount.

      writePageReference(resourceConfiguration, pageReference, page, bufferedBytes,
                         IOStorage.PRIMARY_BEACON_OFFSET);
      writePageReference(resourceConfiguration, pageReference, page, bufferedBytes,
                         IOStorage.SECONDARY_BEACON_OFFSET);

      final var segment = (MemorySegment) bufferedBytes.underlyingObject();
      final var buffer = segment.asByteBuffer();
      // ORDERED dual-copy update through the WRITE-THROUGH beacon channel: each write is durable
      // when it returns (O_DSYNC — an FUA write on NVMe, far cheaper than a cache flush), so the
      // SECONDARY is durable before the primary is even issued, and the PRIMARY's write-return
      // IS the commit acknowledge — no explicit fsync needed for either. At every instant at
      // least one intact copy exists: the primary tearing leaves the new secondary; crashing
      // before the primary write leaves the old primary (whose data is intact, since the new
      // revision was never acknowledged). Residual risk: both copies share one 4 KiB filesystem
      // block, so block-granularity tearing remains a (format-level) exposure.
      final int slot = IOStorage.BEACON_SLOT_BYTES;
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
      if (dataFileChannel != null) {
        dataFileChannel.force(true);
      }
      if (revisionsFileChannel != null) {
        revisionsFileChannel.force(true);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
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
    final long offset = Math.max(dataFileChannel.size(), IOStorage.DATA_REGION_START);
    final var segment = (MemorySegment) bufferedBytes.underlyingObject();
    final var buffer = segment.asByteBuffer();
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

      if (revisionsFileChannel != null) {
        revisionsFileChannel.truncate(0);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }
}
