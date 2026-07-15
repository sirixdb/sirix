/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
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

package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Cache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixIOException;
import io.sirix.io.AbstractReader;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * File Reader. Used for {@link StorageEngineReader} to provide read only access on a
 * RandomAccessFile.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class FileChannelReader extends AbstractReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileChannelReader.class);

  /**
   * Data file channel.
   */
  private final FileChannel dataFileChannel;

  /**
   * Revisions offset file channel.
   */
  private final FileChannel revisionsOffsetFileChannel;

  private final Cache<Integer, RevisionFileData> cache;

  /**
   * Release action for a reader borrowing one of the storage's SHARED channel stripes (a small
   * reference-counted pool per {@link FileChannelStorage}, positional reads only): instead of
   * closing the channels, {@link #close()} runs this callback so the storage can close the pool
   * once the LAST borrowing reader is gone. {@code null} means this reader owns its channels and
   * closes them directly (writer delegate, {@code MMStorage}, recovery, test harnesses).
   */
  private final @Nullable Runnable releaseAction;

  /**
   * Guards against double-release: a reader must decrement the storage's borrow count exactly once.
   */
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Direct-ByteBuffer pool for page reads. Owning a buffer via
   * {@link java.util.concurrent.ArrayBlockingQueue#poll} gives a thread exclusive use
   * without holding any shared monitor during the expensive decompress + deserialize
   * phase. Replaces the prior {@code synchronized(STRIPE_LOCK)} design which serialized
   * {@code read → decompress → deserialize} per-stripe and was the dominant cold-cache
   * wall-time contributor (profiled: 97 % of lock samples, ~770 s off-CPU at cold 100M).
   *
   * <p>HFT constraints honored: bounded off-heap (POOL_SIZE × per-buffer capacity),
   * zero alloc in steady state (buffers are reused), virtual-thread-safe (queue applies
   * back-pressure instead of letting the buffer population scale with thread count).
   */
  private static final int POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
  private static final java.util.concurrent.ArrayBlockingQueue<ByteBuffer> BUF_POOL =
      new java.util.concurrent.ArrayBlockingQueue<>(POOL_SIZE);

  /**
   * Per-pool-buffer capacity. Kept at 128 KiB because measurement at cold 100 M
   * showed that enlarging to 1 MiB didn't help wall-time (the pool-growth path
   * is hit rarely enough to not matter) but increased FS-input by 20% —
   * apparently {@code FileChannel.read(buffer, pos)} with a 1 MiB-limit buffer
   * on a 200 KiB page triggers extra readahead we don't benefit from.
   *
   * <p>Override with {@code -Dsirix.filechannel.bufferBytes=<N>} (N = bytes,
   * clamped to [64 KiB, 16 MiB]). Kept configurable for future tuning on
   * different workloads (e.g. LZ4-on DBs where pages are ~32 KiB).
   */
  private static final int BUFFER_BYTES =
      Math.max(64 * 1024, Math.min(16 * 1024 * 1024,
          Integer.getInteger("sirix.filechannel.bufferBytes", 128 * 1024)));

  static {
    for (int i = 0; i < POOL_SIZE; i++) {
      BUF_POOL.offer(ByteBuffer.allocateDirect(BUFFER_BYTES).order(ByteOrder.nativeOrder()));
    }
  }

  /**
   * Acquire a direct ByteBuffer from the pool, or — rare — allocate a fresh one if the
   * pool is temporarily drained. Always pair with {@link #releaseBuffer}.
   *
   * <p>We do not block on {@code take()} here: a drained pool means every worker is
   * already servicing a read in parallel; adding queue-wait on top would serialize
   * them again. One extra allocation is cheaper than that serialization.
   */
  private static ByteBuffer acquireBuffer(final int minCapacity) {
    ByteBuffer b = BUF_POOL.poll();
    if (b == null || b.capacity() < minCapacity) {
      if (b != null) {
        // Pooled buffer too small for this page — drop it (direct memory GC'd) and
        // allocate replacement sized to current page. Pool capacity is maintained.
        BUF_POOL.offer(b);
      }
      final int cap = Math.max(minCapacity, BUFFER_BYTES);
      b = ByteBuffer.allocateDirect(cap).order(ByteOrder.nativeOrder());
    }
    return b;
  }

  /** Return a buffer to the pool. If pool is full (transient extras), drop the reference. */
  private static void releaseBuffer(final ByteBuffer b) {
    if (b == null) return;
    BUF_POOL.offer(b);
  }

  /**
   * Constructor.
   *
   * @param dataFileChannel the data file channel
   * @param revisionsOffsetFileChannel the file, which holds pointers to the revision root pages
   * @param handler {@link ByteHandler} instance
   */
  public FileChannelReader(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final ByteHandler handler, final SerializationType type, final PagePersister pagePersistenter,
      final Cache<Integer, RevisionFileData> cache) {
    this(dataFileChannel, revisionsOffsetFileChannel, handler, type, pagePersistenter, cache, null);
  }

  /**
   * Constructor.
   *
   * @param dataFileChannel the data file channel
   * @param revisionsOffsetFileChannel the file, which holds pointers to the revision root pages
   * @param handler {@link ByteHandler} instance
   * @param releaseAction run on {@link #close()} INSTEAD of closing the channels, for readers
   *        borrowing the storage's shared channel stripes; {@code null} makes this reader own and
   *        close its channels
   */
  public FileChannelReader(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final ByteHandler handler, final SerializationType type, final PagePersister pagePersistenter,
      final Cache<Integer, RevisionFileData> cache, final @Nullable Runnable releaseAction) {
    super(handler, pagePersistenter, type);
    this.dataFileChannel = dataFileChannel;
    this.revisionsOffsetFileChannel = revisionsOffsetFileChannel;
    this.cache = cache;
    this.releaseAction = releaseAction;
  }

  /**
   * Validate a page's declared data-length header before it is used to size an allocation
   * ({@link #acquireBuffer} / {@code new byte[dataLength]}). The length is read straight from the
   * file, so a corrupt or garbled beacon/page can present any 32-bit value; a large positive one
   * (e.g. random bytes read as ~2 GiB) would trigger a multi-gigabyte allocation that OOMs or
   * stalls the JVM in GC instead of failing fast — the cause of {@code UberPageCorruptionTest}
   * flakiness. A page can never be longer than the file that contains it, so bound it accordingly.
   */
  private void checkDataLength(final int dataLength) throws IOException {
    final long fileSize = dataFileChannel.size();
    if (dataLength < 0 || dataLength > fileSize) {
      throw new SirixIOException("Corrupt page reference: declared data length " + dataLength
          + " is out of bounds for a data file of " + fileSize + " bytes.");
    }
  }

  /**
   * Reads the buffer's full remaining range from the given file offset, failing with a clean
   * {@link SirixIOException} on EOF/short data. A single unchecked {@code read} returned -1 at
   * EOF and left the buffer empty, so the subsequent {@code getInt()}/bulk get surfaced raw
   * {@code BufferUnderflowException}s when a crash-truncated file's beacon advertised a page
   * past the durable tail (found by the power-loss simulation harness).
   */
  private void readFully(final ByteBuffer buffer, final long offset, final String what) throws IOException {
    while (buffer.hasRemaining()) {
      final int n = dataFileChannel.read(buffer, offset + buffer.position());
      if (n < 0) {
        throw new SirixIOException("Truncated " + what + " at offset " + offset
            + " — the data file ends before the expected " + buffer.limit() + " bytes.");
      }
    }
  }

  public Page read(final PageReference reference,
      final @Nullable ResourceConfiguration resourceConfiguration) {
    // First pread: 4-byte length header. Uses a pooled buffer so we can size the
    // data buffer exactly for the second pread.
    ByteBuffer buffer = acquireBuffer(4);
    try {
      final long position = reference.getKey();

      buffer.clear().limit(4);
      readFully(buffer, position, "page length header");
      buffer.flip();
      final int dataLength = buffer.getInt();
      checkDataLength(dataLength);

      // If the header-probe buffer is too small for the page body, swap it for a
      // right-sized one. The rare-extra-alloc branch returns the old buffer to the
      // pool and gives us one large enough. Keeps the pool invariant.
      if (buffer.capacity() < dataLength) {
        final ByteBuffer grown = acquireBuffer(dataLength);
        releaseBuffer(buffer);
        buffer = grown;
      }

      buffer.clear().limit(dataLength);
      readFully(buffer, position + 4, "page body");
      buffer.flip();

      // Deserialize while this thread exclusively owns `buffer`. No shared monitor.
      // The buffer is released in finally — this is safe because deserialize either
      // returns a Page with no reference back to the buffer, or copies the bytes
      // into a page-owned allocation before returning.
      if (byteHandler.supportsMemorySegments()) {
        final MemorySegment segment = MemorySegment.ofBuffer(buffer);
        verifyChecksumIfNeeded(segment, reference, resourceConfiguration);
        return deserializeFromSegment(resourceConfiguration, segment, reference);
      } else {
        final byte[] page = new byte[dataLength];
        buffer.get(page);
        verifyChecksumIfNeeded(page, reference, resourceConfiguration);
        return deserialize(resourceConfiguration, page, reference);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    } finally {
      releaseBuffer(buffer);
    }
  }


  @Override
  public RevisionRootPage readRevisionRootPage(final int revision, final ResourceConfiguration resourceConfiguration) {
    ByteBuffer buffer = acquireBuffer(4);
    try {
      // The cached record carries (offset, timestamp, pageHash) — reuse it so we neither re-read
      // the revisions record nor lose the page hash needed to integrity-check the body below.
      final RevisionFileData revisionFileData =
          cache.get(revision, (unused) -> getRevisionFileData(revision));
      final long dataFileOffset = revisionFileData.offset();

      buffer.clear().limit(4);
      readFully(buffer, dataFileOffset, "revision-root length header");
      buffer.flip();
      final int dataLength = buffer.getInt();
      checkDataLength(dataLength);

      if (buffer.capacity() < dataLength) {
        final ByteBuffer grown = acquireBuffer(dataLength);
        releaseBuffer(buffer);
        buffer = grown;
      }

      buffer.clear().limit(dataLength);
      readFully(buffer, dataFileOffset + 4, "revision-root body");
      buffer.flip();

      // The RevisionRootPage read path carries no parent PageReference, so unlike every other page
      // its body was historically NOT integrity-checked here. The hash recorded alongside the
      // record (the same XXH3 the writer set on the page's reference) lets us verify the compressed
      // payload BEFORE deserialization — mirroring read(reference, config) exactly. Gated on
      // verifyChecksumsOnRead and skipped for legacy/RAM records that carry no hash (pageHash == 0).
      final PageReference reference = revisionRootReference(dataFileOffset, revisionFileData.pageHash());

      if (byteHandler.supportsMemorySegments()) {
        final MemorySegment segment = MemorySegment.ofBuffer(buffer);
        verifyChecksumIfNeeded(segment, reference, resourceConfiguration);
        return (RevisionRootPage) deserializeFromSegment(resourceConfiguration, segment, reference);
      } else {
        final byte[] page = new byte[dataLength];
        buffer.get(page);
        verifyChecksumIfNeeded(page, reference, resourceConfiguration);
        return (RevisionRootPage) deserialize(resourceConfiguration, page, reference);
      }
    } catch (IOException e) {
      throw new SirixIOException(e);
    } finally {
      releaseBuffer(buffer);
    }
  }

  @Override
  protected ByteBuffer readBeaconSlot(final long offset) {
    try {
      final ByteBuffer slot = ByteBuffer.allocate(IOStorage.BEACON_SLOT_BYTES);
      int position = 0;
      while (slot.hasRemaining()) {
        final int read = dataFileChannel.read(slot, offset + position);
        if (read < 0) {
          break; // EOF — short slot is handled by the verifier
        }
        position += read;
      }
      slot.flip();
      return slot;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Instant readRevisionRootPageCommitTimestamp(int revision) {
    return cache.get(revision, (_) -> getRevisionFileData(revision)).timestamp();
  }

  @Override
  public RevisionFileData getRevisionFileData(int revision) {
    return getRevisionFileData(revision, 1)[0];
  }

  @Override
  public RevisionFileData[] getRevisionFileData(final int fromRevision, final int count) {
    if (count <= 0) {
      return new RevisionFileData[0];
    }
    final int byteCount = count * IOStorage.REVISIONS_FILE_RECORD_SIZE;
    // One bulk pread + one pooled buffer for the whole range. The previous one-record-per-call
    // loop issued one syscall AND one ByteBuffer.allocateDirect per revision — allocateDirect
    // registers with a JVM-global synchronized Cleaner list, so concurrent readers rebuilding
    // the revision index serialized on it and storage opens became linear in revision count.
    final ByteBuffer buffer = acquireBuffer(byteCount);
    try {
      final long fileOffset = IOStorage.revisionsFileOffset(fromRevision);
      buffer.clear().limit(byteCount);
      buffer.order(ByteOrder.LITTLE_ENDIAN);
      int position = 0;
      while (buffer.hasRemaining()) {
        final int read = revisionsOffsetFileChannel.read(buffer, fileOffset + position);
        if (read < 0) {
          throw new SirixIOException("Truncated revisions record for revision "
              + (fromRevision + position / IOStorage.REVISIONS_FILE_RECORD_SIZE));
        }
        position += read;
      }
      final RevisionFileData[] result = new RevisionFileData[count];
      for (int i = 0; i < count; i++) {
        final int base = i * IOStorage.REVISIONS_FILE_RECORD_SIZE;
        final long offset = buffer.getLong(base);
        final long timestamp = buffer.getLong(base + 8);
        final long storedChecksum = buffer.getLong(base + 16);
        // 4th field: the RevisionRootPage's own page hash (0 = legacy record / no hash).
        final long pageHash = buffer.getLong(base + 24);
        // These bytes are the ONLY path to the revision's root page — verify them. The checksum
        // covers 24 bytes when a page hash is present, 16 when legacy (hash == 0), so beta1
        // resources still open cleanly.
        if (storedChecksum != IOStorage.expectedRevisionRecordChecksum(offset, timestamp, pageHash)) {
          throw new SirixIOException("Corrupt revisions record for revision " + (fromRevision + i)
              + " (checksum mismatch) — torn write or storage corruption");
        }
        result[i] = new RevisionFileData(offset, Instant.ofEpochMilli(timestamp), pageHash);
      }
      return result;
    } catch (IOException e) {
      throw new SirixIOException(e);
    } finally {
      buffer.order(ByteOrder.nativeOrder());
      releaseBuffer(buffer);
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    if (releaseAction != null) {
      // Borrowed shared channels — hand the borrow back; the storage closes the pool when the
      // last borrower is gone.
      releaseAction.run();
      return;
    }
    try {
      dataFileChannel.close();
      revisionsOffsetFileChannel.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

}
