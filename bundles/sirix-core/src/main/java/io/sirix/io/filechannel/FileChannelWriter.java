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
   * {@link FileChannelReader} reference for this writer.
   */
  private final FileChannelReader reader;

  private final SerializationType serializationType;

  private final FileChannel revisionsFileChannel;

  private final PagePersister pagePersister;

  private final AsyncCache<Integer, RevisionFileData> cache;

  private final RevisionIndexHolder revisionIndexHolder;

  private boolean isFirstUberPage;

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
   *        root pages
   * @param serializationType the serialization type (for the transaction log or the data file)
   * @param pagePersister transforms in-memory pages into byte-arrays and back
   * @param cache the revision file data cache
   * @param revisionIndexHolder the holder for the optimized revision index
   * @param reader the reader delegate
   */
  public FileChannelWriter(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final SerializationType serializationType, final PagePersister pagePersister,
      final AsyncCache<Integer, RevisionFileData> cache, final RevisionIndexHolder revisionIndexHolder,
      final FileChannelReader reader) {
    this.dataFileChannel = dataFileChannel;
    this.serializationType = requireNonNull(serializationType);
    this.revisionsFileChannel = revisionsOffsetFileChannel;
    this.pagePersister = requireNonNull(pagePersister);
    this.cache = requireNonNull(cache);
    this.revisionIndexHolder = requireNonNull(revisionIndexHolder);
    this.reader = requireNonNull(reader);
  }

  /**
   * Constructor (backward compatibility).
   */
  public FileChannelWriter(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final SerializationType serializationType, final PagePersister pagePersister,
      final AsyncCache<Integer, RevisionFileData> cache, final FileChannelReader reader) {
    this(dataFileChannel, revisionsOffsetFileChannel, serializationType, pagePersister, cache,
        new RevisionIndexHolder(), reader);
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
    } catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
      throw new IllegalStateException(e);
    }

    return this;
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
    final long fileSize = dataFileChannel.size();
    long offset;

    if (fileSize == 0) {
      offset = IOStorage.FIRST_BEACON;
      offset += (PAGE_FRAGMENT_BYTE_ALIGN - (offset & (PAGE_FRAGMENT_BYTE_ALIGN - 1)));
      offset += bufferedBytes.writePosition();
    } else {
      offset = fileSize + bufferedBytes.writePosition();
    }

    return offset;
  }

  private static void writeToBufferedBytes(BytesOut<?> bufferedBytes, byte[] serializedPageBytes,
      MemorySegment serializedPageSegment, int serializedPageLength) {
    if (serializedPageSegment != null) {
      bufferedBytes.writeSegment(serializedPageSegment, 0, serializedPageLength);
    } else if (serializedPageBytes != null) {
      bufferedBytes.write(serializedPageBytes);
    }
  }

  private static void writeToBuffer(ByteBuffer buffer, byte[] serializedPageBytes, MemorySegment serializedPageSegment,
      int serializedPageLength) {
    if (serializedPageSegment != null) {
      buffer.put(serializedPageSegment.asSlice(0, serializedPageLength).asByteBuffer());
    } else if (serializedPageBytes != null) {
      buffer.put(serializedPageBytes);
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
          // The dual-copy layout reserves exactly UBER_PAGE_BYTE_ALIGN bytes per copy (offsets 0
          // and FIRST_BEACON>>1): an oversized uber page would silently overlap the second copy
          // and then the data region (unrecoverable database). Fail loudly instead.
          if (serializedPageLength + IOStorage.OTHER_BEACON >= UBER_PAGE_BYTE_ALIGN) {
            throw new SirixIOException("Serialized UberPage (" + serializedPageLength
                + " bytes + header) exceeds its " + UBER_PAGE_BYTE_ALIGN
                + "-byte slot — the on-disk uber-page layout must be revised before this can be written");
          }
          offsetToAdd =
              UBER_PAGE_BYTE_ALIGN - ((serializedPageLength + IOStorage.OTHER_BEACON) % UBER_PAGE_BYTE_ALIGN);
        } else if (page instanceof RevisionRootPage && offset % REVISION_ROOT_PAGE_BYTE_ALIGN != 0) {
          offsetToAdd = (int) (REVISION_ROOT_PAGE_BYTE_ALIGN - (offset & (REVISION_ROOT_PAGE_BYTE_ALIGN - 1)));
          offset += offsetToAdd;
        } else if (offset % PAGE_FRAGMENT_BYTE_ALIGN != 0) {
          offsetToAdd = (int) (PAGE_FRAGMENT_BYTE_ALIGN - (offset & (PAGE_FRAGMENT_BYTE_ALIGN - 1)));// (offset %
                                                                                                     // PAGE_FRAGMENT_BYTE_ALIGN));
          offset += offsetToAdd;
        }
      }

      if (!(page instanceof UberPage) && offsetToAdd > 0) {
        bufferedBytes.writePosition(bufferedBytes.writePosition() + offsetToAdd);
      }

      bufferedBytes.writeInt(serializedPageLength);
      writeToBufferedBytes(bufferedBytes, serializedPageBytes, serializedPageSegment, serializedPageLength);

      if (page instanceof UberPage && offsetToAdd > 0) {
        final byte[] bytesToAdd = new byte[(int) offsetToAdd];
        bufferedBytes.write(bytesToAdd);
      }

      if (bufferedBytes.writePosition() > FLUSH_SIZE) {
        flushBuffer(bufferedBytes);
      }

      // Remember page coordinates.
      pageReference.setKey(offset);

      // Compute hash on compressed bytes for ALL page types (consistent approach)
      if (serializedPageSegment != null) {
        pageReference.setHash(PageHasher.compute(serializedPageSegment, PageHasher.DEFAULT_ALGORITHM));
      } else if (serializedPageBytes != null) {
        pageReference.setHash(PageHasher.compute(serializedPageBytes));
      } else {
        throw new IllegalStateException("Failed to compute page hash due to missing payload");
      }

      if (serializationType == SerializationType.DATA) {
        if (page instanceof RevisionRootPage revisionRootPage) {
          ByteBuffer buffer = ByteBuffer.allocateDirect(16).order(ByteOrder.nativeOrder());
          buffer.putLong(offset);
          buffer.position(8);
          buffer.putLong(revisionRootPage.getRevisionTimestamp());
          buffer.position(0);
          // DETERMINISTIC slot (the shared layout formula). The old append-at-file-size
          // positioning agreed with it only while the file held exactly one record per
          // committed revision: a commit that failed AFTER appending its record
          // (rollback/truncateTo never touched this file), or a partial record from power
          // loss, shifted EVERY later slot — all subsequent revision lookups returned garbage
          // offsets, permanently. Writing at the formula's offset is identical in the happy
          // path and self-healing after a failed attempt (the retry overwrites).
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
        } else if (page instanceof UberPage && isFirstUberPage) {
          ByteBuffer buffer = ByteBuffer.allocateDirect(Writer.UBER_PAGE_BYTE_ALIGN).order(ByteOrder.nativeOrder());
          writeToBuffer(buffer, serializedPageBytes, serializedPageSegment, serializedPageLength);
          buffer.position(0);
          revisionsFileChannel.write(buffer, 0);
          buffer.position(0);
          revisionsFileChannel.write(buffer, Writer.UBER_PAGE_BYTE_ALIGN);
        }
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

      // WRITE-AHEAD BARRIER: make the just-flushed page tail (which essentially always contains
      // the new RevisionRootPage — children are serialized first) durable BEFORE the uber page is
      // written. The commit's t3 forceAll() barrier ran while this tail was still buffered in
      // memory, so a single later fsync covered both the tail AND the uber page with no
      // intra-fsync ordering: power loss could persist the new uber page (offset 0) pointing at
      // revision data that never reached disk, and recovery would then truncate to a bogus length
      // (bricked resource). force(false) = data only; the metadata fsync happens at commit end.
      dataFileChannel.force(false);
      // Same write-ahead rule for the revisions file: its 16-byte slot record for the NEW
      // revision (written during page serialization) must be durable BEFORE either uber beacon
      // advertises the new revisionCount — otherwise a crash leaves a beacon pointing at a
      // zero/garbage revisions slot, which nothing checksums. force(true): the append grows the
      // file, so the size metadata must be durable too.
      revisionsFileChannel.force(true);

      isFirstUberPage = true;
      writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, 0);
      isFirstUberPage = false;
      writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, IOStorage.FIRST_BEACON >> 1);

      final var segment = (MemorySegment) bufferedBytes.underlyingObject();
      final var buffer = segment.asByteBuffer();
      // ORDERED dual-copy update: both copies used to go out in ONE write covered by ONE fsync,
      // so a torn write during power loss could corrupt both beacons at once and the fallback
      // read protected against nothing. Make the SECONDARY durable first, then write the
      // primary (forced by the commit's final barrier): at every instant at least one intact
      // copy exists — the primary tearing leaves the new secondary; crashing before the primary
      // write leaves the old primary (whose data is intact, since the new revision was never
      // acknowledged). Residual risk: both copies share one 4 KiB filesystem block, so
      // block-granularity tearing remains a (format-level) exposure.
      final int half = IOStorage.FIRST_BEACON >> 1;
      final ByteBuffer secondary = buffer.duplicate();
      secondary.position(half);
      while (secondary.hasRemaining()) {
        dataFileChannel.write(secondary, half + (secondary.position() - half));
      }
      dataFileChannel.force(false);
      final ByteBuffer primary = buffer.duplicate();
      primary.position(0).limit(half);
      while (primary.hasRemaining()) {
        dataFileChannel.write(primary, primary.position());
      }
      // The primary copy is forced by forceAll() at the end of the commit (single final barrier).
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

  private void flushBuffer(BytesOut<?> bufferedBytes) throws IOException {
    final long fileSize = dataFileChannel.size();
    long offset;

    if (fileSize == 0) {
      offset = IOStorage.FIRST_BEACON;
      offset += (PAGE_FRAGMENT_BYTE_ALIGN - (offset % PAGE_FRAGMENT_BYTE_ALIGN));
    } else {
      offset = fileSize;
    }

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
