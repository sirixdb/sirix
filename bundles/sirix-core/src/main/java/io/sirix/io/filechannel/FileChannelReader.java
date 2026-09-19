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
import it.unimi.dsi.fastutil.ints.IntArrays;
import io.sirix.HftBoundaryTelemetry;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixIOException;
import io.sirix.io.AbstractReader;
import io.sirix.io.IOStorage;
import io.sirix.io.RevisionFileData;
import io.sirix.io.Superblock;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.RegionsOnlyPage;
import io.sirix.settings.StringCompressionType;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.LongAdder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
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

  /** Switch for decoding coalesced batch members straight from the borrowed read buffer. */
  static final String BORROW_BATCH_INPUT = "sirix.filechannel.borrowBatchInput";

  /** Overflow-input switch of {@link AbstractReader}; the default of {@link #BORROW_BATCH_INPUT}. */
  static final String BORROW_OVERFLOW_INPUT = "sirix.io.borrowOverflowInput";

  /** Decode a coalesced page while its read buffer is still exclusively owned by this call. */
  private final boolean borrowBatchInput = batchInputBorrowingEnabled();

  /**
   * Requested offsets only: bounded OS read-ahead without queued tasks or staged page objects.
   *
   * <p>
   * {@code BATCH_READ_AHEAD} is how many pages of a batch may be hinted ahead of the batch's read
   * cursor. Every caller today hands at most 1,024 references, so the default hints the WHOLE batch
   * up front — the kernel then has every page of the batch in flight while the first run is still
   * being read, instead of the sixteen-page trickle that kept a cold coalesced fill at queue depth
   * one for most of its life. The bound exists for a pathological batch, not for the ordinary one.
   *
   * <p>
   * {@code BATCH_READ_AHEAD_BYTES} is the tail hinted past an offset whose page length is unknown —
   * the last page of a run, or every page of an advisory {@link #prefetch} whose successor is farther
   * away than this. A page is bounded by the next page's offset in an append-only data file, so every
   * other page's extent is exact and hinting it costs no bandwidth. 32 KiB covers the length header
   * and the whole body of the typical page (the 100M JSONBench Q4/Q5 cold reads average 9–12 KiB) so
   * the body read that follows the header is a cache hit rather than a second device round trip.
   */
  private static final int BATCH_READ_AHEAD =
      Math.max(0, Math.min(4096, Integer.getInteger("sirix.filechannel.batchReadAhead", 1024)));
  private static final long BATCH_READ_AHEAD_BYTES =
      Math.max(4096L, Math.min(1024 * 1024L, Long.getLong("sirix.filechannel.batchReadAheadBytes", 32L * 1024)));

  /**
   * Pages per advisory {@link #prefetch} batch advertised through {@link #preferredPrefetchBatch()};
   * {@code 0} disables the advisory route and restores the pre-hint behaviour of every caller (the
   * HOT trie sibling window, the projection directory walk and the column-store sweep all gate on
   * it). 32 matches the NVMe queue-depth sweet spot the callers were sized for.
   */
  private static final int PREFETCH_BATCH =
      Math.max(0, Math.min(1024, Integer.getInteger("sirix.filechannel.prefetchBatch", 32)));

  private static final int UNRESOLVED_READ_AHEAD_FD = -2;
  private volatile int readAheadFd = UNRESOLVED_READ_AHEAD_FD;

  /**
   * Revisions offset file channel.
   */
  private final FileChannel revisionsOffsetFileChannel;

  private final Cache<Integer, RevisionFileData> cache;

  /**
   * Release action for a reader borrowing one of the storage's SHARED channel stripes (a small
   * reference-counted pool per {@link FileChannelStorage}, positional reads only): instead of closing
   * the channels, {@link #close()} runs this callback so the storage can close the pool once the LAST
   * borrowing reader is gone. {@code null} means this reader owns its channels and closes them
   * directly (writer delegate, {@code MMStorage}, recovery, test harnesses).
   */
  private final @Nullable Runnable releaseAction;

  /**
   * Guards against double-release: a reader must decrement the storage's borrow count exactly once.
   */
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Direct-ByteBuffer pool for page reads. Atomic slot ownership avoids a common queue lock both
   * during acquisition and return. No buffer is shared while a reader decompresses or deserializes.
   *
   * <p>
   * Retained buffers are bounded by POOL_SIZE, independent of platform or virtual thread count. An
   * exhausted pool permits a transient allocation; returns to a full pool are discarded. A successful
   * borrow/return allocates nothing and retains no per-thread state.
   */
  private static final int POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
  private static final BufferPool BUF_POOL =
      new BufferPool(POOL_SIZE, Boolean.parseBoolean(System.getProperty("sirix.filechannel.lockFreeBuffers", "true")));

  /** Bounded unordered pool: FIFO ordering is unnecessary for exclusive scratch-buffer ownership. */
  static final class BufferPool {
    // Four-byte references leave at least two cache lines between active slots. The sparse
    // array is small (two slots per processor); separate workers normally reuse separate slots.
    private static final int SLOT_STRIDE = 32;
    private static final VarHandle SLOTS = MethodHandles.arrayElementVarHandle(ByteBuffer[].class);
    private final int capacity;
    private final ByteBuffer @Nullable [] slots;
    private final @Nullable ArrayBlockingQueue<ByteBuffer> queue;

    BufferPool(final int capacity, final boolean lockFree) {
      if (capacity < 1 || capacity > Integer.MAX_VALUE / SLOT_STRIDE) {
        throw new IllegalArgumentException("invalid buffer pool capacity: " + capacity);
      }
      this.capacity = capacity;
      this.slots = lockFree
          ? new ByteBuffer[capacity * SLOT_STRIDE]
          : null;
      this.queue = lockFree
          ? null
          : new ArrayBlockingQueue<>(capacity);
    }

    @Nullable
    ByteBuffer poll() {
      if (queue != null) {
        return queue.poll();
      }
      int slot = (int) (Thread.currentThread().threadId() % capacity) * SLOT_STRIDE;
      for (int remaining = capacity; remaining > 0; remaining--) {
        final ByteBuffer buffer = (ByteBuffer) SLOTS.getAcquire(slots, slot);
        if (buffer != null && SLOTS.compareAndSet(slots, slot, buffer, null)) {
          return buffer;
        }
        slot += SLOT_STRIDE;
        if (slot == slots.length) {
          slot = 0;
        }
      }
      return null;
    }

    boolean offer(final ByteBuffer buffer) {
      Objects.requireNonNull(buffer, "buffer");
      if (queue != null) {
        return queue.offer(buffer);
      }
      int slot = (int) (Thread.currentThread().threadId() % capacity) * SLOT_STRIDE;
      for (int remaining = capacity; remaining > 0; remaining--) {
        if (SLOTS.getOpaque(slots, slot) == null && SLOTS.compareAndSet(slots, slot, null, buffer)) {
          return true;
        }
        slot += SLOT_STRIDE;
        if (slot == slots.length) {
          slot = 0;
        }
      }
      return false;
    }
  }

  /**
   * Per-pool-buffer capacity. Kept at 128 KiB because measurement at cold 100 M showed that enlarging
   * to 1 MiB didn't help wall-time (the pool-growth path is hit rarely enough to not matter) but
   * increased FS-input by 20% — apparently {@code FileChannel.read(buffer, pos)} with a 1 MiB-limit
   * buffer on a 200 KiB page triggers extra readahead we don't benefit from.
   *
   * <p>
   * Override with {@code -Dsirix.filechannel.bufferBytes=<N>} (N = bytes, clamped to [64 KiB, 16
   * MiB]). Kept configurable for future tuning on different workloads (e.g. LZ4-on DBs where pages
   * are ~32 KiB).
   */
  private static final int BUFFER_BYTES =
      Math.max(64 * 1024, Math.min(16 * 1024 * 1024, Integer.getInteger("sirix.filechannel.bufferBytes", 128 * 1024)));

  /** Bounded prefix for small pages; four bytes restores separate header and body reads. */
  private static final int PAGE_PREFIX_BYTES =
      Math.max(Integer.BYTES, Math.min(64 * 1024, Integer.getInteger("sirix.filechannel.pagePrefixBytes", 1024)));

  static {
    for (int i = 0; i < POOL_SIZE; i++) {
      BUF_POOL.offer(ByteBuffer.allocateDirect(BUFFER_BYTES).order(ByteOrder.LITTLE_ENDIAN));
    }
  }

  /**
   * Acquire a direct ByteBuffer from the pool, or — rare — allocate a fresh one if the pool is
   * temporarily drained. Always pair with {@link #releaseBuffer}.
   *
   * <p>
   * We do not block on {@code take()} here: a drained pool means every worker is already servicing a
   * read in parallel; adding queue-wait on top would serialize them again. One extra allocation is
   * cheaper than that serialization.
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
      b = ByteBuffer.allocateDirect(cap).order(ByteOrder.LITTLE_ENDIAN);
    }
    return b;
  }

  /** Return a buffer to the pool. If pool is full (transient extras), drop the reference. */
  private static void releaseBuffer(final ByteBuffer b) {
    if (b == null)
      return;
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
   * (e.g. random bytes read as ~2 GiB) would trigger a multi-gigabyte allocation that OOMs or stalls
   * the JVM in GC instead of failing fast — the cause of {@code UberPageCorruptionTest} flakiness. A
   * page can never be longer than the file that contains it, so bound it accordingly.
   */
  private void checkDataLength(final int dataLength) throws IOException {
    checkDataLength(dataLength, -1L);
  }

  private void checkDataLength(final int dataLength, final long batchFileSize) throws IOException {
    // Zero is as invalid as negative: no serialized page is empty. It matters under preallocated
    // commits (the default), where a stale/corrupt reference into the zero-filled preallocation
    // tail reads a 0 length header — fail with this clean diagnostic instead of feeding a
    // zero-length payload into the decompression/deserialization pipeline.
    final long fileSize = batchFileSize < 0L
        ? dataFileChannel.size()
        : batchFileSize;
    if (dataLength <= 0 || dataLength > fileSize) {
      throw new SirixIOException("Corrupt page reference: declared data length " + dataLength
          + " is out of bounds for a data file of " + fileSize + " bytes.");
    }
  }

  /**
   * Reads the buffer's full remaining range from the given file offset, failing with a clean
   * {@link SirixIOException} on EOF/short data. A single unchecked {@code read} returned -1 at EOF
   * and left the buffer empty, so the subsequent {@code getInt()}/bulk get surfaced raw
   * {@code BufferUnderflowException}s when a crash-truncated file's beacon advertised a page past the
   * durable tail (found by the power-loss simulation harness).
   */
  private void readFully(final ByteBuffer buffer, final long offset, final String what) throws IOException {
    while (buffer.hasRemaining()) {
      final int n = readAt(dataFileChannel, buffer, offset + buffer.position());
      if (n <= 0) {
        throw new SirixIOException("Truncated " + what + " at offset " + offset
            + " — the data file ends before the expected " + buffer.limit() + " bytes.");
      }
    }
  }

  public Page read(final PageReference reference, final @Nullable ResourceConfiguration resourceConfiguration) {
    return read(reference, resourceConfiguration, false);
  }

  @Override
  public Page readRecordPageLazily(final PageReference reference,
      final @Nullable ResourceConfiguration resourceConfiguration) {
    return read(reference, resourceConfiguration, true);
  }

  private Page read(final PageReference reference, final @Nullable ResourceConfiguration resourceConfiguration,
      final boolean lazyRecordPage) {
    return read(reference, resourceConfiguration, lazyRecordPage, -1L);
  }

  private Page read(final PageReference reference, final @Nullable ResourceConfiguration resourceConfiguration,
      final boolean lazyRecordPage, final long batchFileSize) {
    ByteBuffer buffer = acquireBuffer(PAGE_PREFIX_BYTES);
    try {
      final long position = reference.getKey();
      final long fileSize = batchFileSize < 0L
          ? dataFileChannel.size()
          : batchFileSize;
      final int prefixBytes = (int) Math.min(PAGE_PREFIX_BYTES, Math.max(Integer.BYTES, fileSize - position));
      buffer.clear().limit(prefixBytes);
      // A concurrent truncation beyond this page may shorten only the speculative suffix.
      // Require the header and declared body below, never bytes belonging to a following page.
      readAtMost(buffer, position);
      if (buffer.position() < Integer.BYTES) {
        buffer.limit(Integer.BYTES);
        readFully(buffer, position, "page length header");
      }
      buffer.flip();
      final int dataLength = buffer.getInt();
      checkDataLength(dataLength, fileSize);
      if (dataLength <= buffer.remaining()) {
        // Checksum and deserialization see exactly the declared body, excluding both the
        // length header and any following bytes fetched by the bounded prefix.
        buffer.limit(buffer.position() + dataLength);
      } else {
        if (buffer.capacity() < dataLength) {
          final ByteBuffer grown = acquireBuffer(dataLength);
          grown.clear().put(buffer);
          releaseBuffer(buffer);
          buffer = grown;
        } else {
          buffer.compact();
        }
        buffer.limit(dataLength);
        readFully(buffer, position + Integer.BYTES, "page body");
        buffer.flip();
      }

      // Deserialize while this thread exclusively owns `buffer`. No shared monitor.
      // The buffer is released in finally — this is safe because deserialize either
      // returns a Page with no reference back to the buffer, or copies the bytes
      // into a page-owned allocation before returning.
      if (byteHandler.supportsMemorySegments()) {
        final MemorySegment segment = MemorySegment.ofBuffer(buffer);
        verifyChecksumIfNeeded(buffer, reference, resourceConfiguration);
        return deserializeFromSegment(resourceConfiguration, segment, reference, lazyRecordPage);
      } else {
        final byte[] page = new byte[dataLength];
        buffer.get(page);
        verifyChecksumIfNeeded(page, reference, resourceConfiguration);
        return deserialize(resourceConfiguration, page, reference, lazyRecordPage);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    } finally {
      releaseBuffer(buffer);
    }
  }

  @Override
  public RegionsOnlyPage readRegionsOnly(final PageReference reference,
      final ResourceConfiguration resourceConfiguration, final int regionKindMask, final int regionDeferMask) {
    // Two bounded preads instead of the whole page: the header, which says where the region table
    // begins, and then a chunk of that table. A ~26 KB page yields its columns from ~4 KB, and the
    // record body — the majority of the page — is never pulled off disk at all.
    //
    // FSST resources are excluded: their string predicates need the symbol-table id, which lives in
    // the page tail behind everything else, so those pages are read whole (below).
    if (byteHandler.supportsMemorySegments() && regionChunkEligible(resourceConfiguration)) {
      final RegionsOnlyPage chunked =
          recordChunkOutcome(readRegionsFromChunk(reference, resourceConfiguration, regionKindMask, regionDeferMask));
      if (chunked != null) {
        return chunked;
      }
    }
    ByteBuffer buffer = acquireBuffer(4);
    try {
      final long position = reference.getKey();

      buffer.clear().limit(4);
      readFully(buffer, position, "page length header");
      buffer.flip();
      final int dataLength = buffer.getInt();
      checkDataLength(dataLength);

      if (buffer.capacity() < dataLength) {
        final ByteBuffer grown = acquireBuffer(dataLength);
        releaseBuffer(buffer);
        buffer = grown;
      }

      buffer.clear().limit(dataLength);
      readFully(buffer, position + 4, "page body");
      buffer.flip();

      if (!byteHandler.supportsMemorySegments()) {
        return null; // caller falls back to the full read path
      }
      final MemorySegment segment = MemorySegment.ofBuffer(buffer);
      verifyChecksumIfNeeded(buffer, reference, resourceConfiguration);
      return deserializeRegionsOnlyFromSegment(resourceConfiguration, segment, regionKindMask, regionDeferMask);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    } finally {
      releaseBuffer(buffer);
    }
  }


  /**
   * Whether a page of this resource can be served from a partial read. Two things forbid it: a
   * page-level checksum, which can only be verified over bytes we do not intend to read, and FSST
   * string compression, whose symbol-table id sits in the page tail.
   */
  private static boolean regionChunkEligible(final @Nullable ResourceConfiguration config) {
    return config != null && !config.verifyChecksumsOnRead
        && config.stringCompressionType != StringCompressionType.FSST;
  }

  /**
   * Fetch the header, then a bounded window of the region table. Returns {@code null} when the window
   * did not cover the requested regions, leaving the caller to read the page whole.
   */
  private RegionsOnlyPage readRegionsFromChunk(final PageReference reference,
      final ResourceConfiguration resourceConfiguration, final int regionKindMask, final int regionDeferMask) {
    ByteBuffer header = acquireBuffer(4 + REGION_PROBE_BYTES);
    final long position = reference.getKey();
    final int dataLength;
    final long regionOffset;
    final long[] probe = PROBE_OUT.get();
    final long[] bitmap = PROBE_BITMAP.get();
    try {
      header.clear().limit(4 + REGION_PROBE_BYTES);
      readAtMost(header, position);
      header.flip();
      if (header.remaining() < 8) {
        return null;
      }
      dataLength = header.getInt();
      checkDataLength(dataLength);
      final MemorySegment headerSeg = MemorySegment.ofBuffer(header.slice());
      regionOffset = pagePersister.probeRegionTableOffset(new MemorySegmentBytesIn(headerSeg), probe, bitmap);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    } catch (final IndexOutOfBoundsException | IllegalStateException e) {
      return null; // header did not fit the probe window, or is not a record page
    } finally {
      releaseBuffer(header);
    }
    if (regionOffset < 0 || regionOffset >= dataLength) {
      return null;
    }
    if (probe[4] == 0L) {
      // The header cannot positively certify that PAX covers every value. Do not issue a second
      // bounded read whose result the storage-engine reader must reject anyway; the whole-image
      // fallback reads the existing tail and distinguishes an older safe image from real overflow.
      return null;
    }

    final int want = (int) Math.min(REGION_CHUNK_BYTES, dataLength - regionOffset);
    ByteBuffer chunk = acquireBuffer(want);
    try {
      chunk.clear().limit(want);
      readAtMost(chunk, position + 4 + regionOffset);
      chunk.flip();
      // The bitmap is copied out of the thread-local scratch: the page outlives this call and the
      // scratch is reused by the next page this thread reads.
      // probe[3] is the page's FSST dictionary id, which only a chunked body states this early; a
      // monolith page reports "none" here and is kept off this path by regionChunkEligible.
      return deserializeRegionTableFromChunk(resourceConfiguration, MemorySegment.ofBuffer(chunk), probe[0],
          (int) probe[1], (int) probe[2], probe[3], regionKindMask, regionDeferMask, bitmap.clone(), probe[4] != 0L);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    } finally {
      releaseBuffer(chunk);
    }
  }

  /** Read up to the buffer's limit; a short read at EOF is expected here, not an error. */
  private void readAtMost(final ByteBuffer buffer, final long offset) throws IOException {
    while (buffer.hasRemaining()) {
      final int n = readAt(dataFileChannel, buffer, offset + buffer.position());
      if (n <= 0) {
        break;
      }
    }
  }

  /**
   * Maximum gap between two consecutive page offsets that still coalesces them into one ranged pread
   * — a column's BODY segments are strided by the leaf's other segments, so a generous stride keeps a
   * whole column fill in a handful of sequential reads. Beyond the gap the pages read individually
   * (no wasted bandwidth on sparse layouts).
   */
  private static final long COALESCE_MAX_GAP = Long.getLong("sirix.filechannel.coalesceGapBytes", 64L * 1024);

  /** Cap on one coalesced span; bounds the transient read buffer. */
  private static final long COALESCE_MAX_SPAN = Long.getLong("sirix.filechannel.coalesceSpanBytes", 8L * 1024 * 1024);

  private static final boolean BATCH_FILE_SIZE =
      Boolean.parseBoolean(System.getProperty("sirix.filechannel.batchFileSize", "true"));


  /**
   * {@inheritDoc}
   *
   * <p>
   * Coalesced override: ascending runs of near-adjacent offsets are read with TWO preads per run (the
   * span up to the last page's length header, then the last page's body) instead of two per page.
   * Page bodies of non-final run members always end before the next member's offset in an append-only
   * data file; a page that violates that bound (foreign layout, corruption) falls back to its own
   * exact per-page read.
   */
  @Override
  public Page[] read(final PageReference[] references, final @Nullable ResourceConfiguration resourceConfiguration) {
    final int n = references.length;
    final Page[] pages = new Page[n];
    // Coalesce over the offsets in FILE order, not caller order. Callers hand references in
    // LOGICAL order (ascending rowGroupId), but the pages were written in the trie writer's
    // commit-walk order, which diverges — and a run builder over zig-zagging offsets restarts
    // at every descent and RE-READS the same region: measured on a two-column aggregate fill,
    // ~900 runs spanning 180 MB per chain over a ~36 MB region (5× re-coverage, 355 MB of
    // syscall reads for 9 MB of segments). One permutation sort makes the runs disjoint and
    // near-sequential; results scatter back input-aligned, so callers see no difference.
    final int[] order = new int[n];
    for (int k = 0; k < n; k++) {
      order[k] = k;
    }
    IntArrays.quickSort(order, (a, b) -> Long.compare(keyOf(references[a]), keyOf(references[b])));
    if (n == 0 || keyOf(references[order[n - 1]]) < 0) {
      return pages;
    }
    // All durable references in this batch already exist. Capture their allocation bound once;
    // never retain it across calls, since the same reader may observe a later committed append.
    final long batchFileSize;
    try {
      batchFileSize = BATCH_FILE_SIZE
          ? dataFileChannel.size()
          : -1L;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
    // Ordinary single-page readers pay no native-advice setup. Concurrent first batches may
    // resolve the same descriptor twice; publication needs no lock and retains no extra resource.
    final int batchReadAheadFd = BATCH_READ_AHEAD > 0 && n > 1
        ? readAheadFd()
        : -1;
    // Unresolved references sort first; the durable part of the batch starts after them.
    int i = 0;
    while (i < n && keyOf(references[order[i]]) < 0) {
      i++;
    }
    // Hint cursor: the first position whose RUN has not been hinted yet. Hints run ahead of the
    // reads by at most BATCH_READ_AHEAD pages and are issued per run, so a whole batch costs one
    // advice call per coalesced span rather than one per page — and every span is in flight
    // before its predecessor's synchronous pread returns.
    int hintFrom = i;
    try {
      while (i < n) {
        final int j = runEnd(references, order, i, n);
        if (batchReadAheadFd >= 0) {
          final int hintLimit = Math.min(n, i + BATCH_READ_AHEAD);
          while (hintFrom < hintLimit) {
            final int hintTo = runEnd(references, order, hintFrom, n);
            adviseRun(batchReadAheadFd, keyOf(references[order[hintFrom]]), keyOf(references[order[hintTo]]),
                hintTo + 1 < n
                    ? keyOf(references[order[hintTo + 1]])
                    : Long.MAX_VALUE,
                batchFileSize);
            hintFrom = hintTo + 1;
          }
        }
        if (j == i) {
          pages[order[i]] = read(references[order[i]], resourceConfiguration, false, batchFileSize);
          i++;
          continue;
        }
        readRun(references, pages, order, i, j, resourceConfiguration, batchFileSize);
        i = j + 1;
      }
    } catch (final RuntimeException | Error failure) {
      retireDecodedPages(pages, failure);
      throw failure;
    }
    return pages;
  }

  /**
   * Last index (in sorted {@code order}) of the coalesced run that starts at {@code from}: offsets
   * stay strictly ascending, near-adjacent, and inside the span cap. {@code from} must name a durable
   * reference.
   */
  private static int runEnd(final PageReference[] references, final int[] order, final int from, final int n) {
    final long start = keyOf(references[order[from]]);
    long last = start;
    int j = from;
    while (j + 1 < n) {
      final long next = keyOf(references[order[j + 1]]);
      if (next <= last || next - last > COALESCE_MAX_GAP || next - start > COALESCE_MAX_SPAN) {
        break;
      }
      last = next;
      j++;
    }
    return j;
  }

  /**
   * One {@code WILLNEED} for a whole coalesced run: its span up to the last member's length header,
   * plus a bounded tail for the last body whose length is unknown until that header is read. The tail
   * never reaches into the next run (that run gets its own hint, and in an append-only file the body
   * ends before the next page anyway) nor past the file. Advisory only: a declined hint changes
   * nothing about the reads that follow.
   */
  private static void adviseRun(final int fd, final long start, final long lastOffset, final long nextRunStart,
      final long fileSize) {
    long end = lastOffset + Integer.BYTES + BATCH_READ_AHEAD_BYTES;
    if (nextRunStart > lastOffset && nextRunStart < end) {
      end = nextRunStart;
    }
    if (fileSize >= 0L && end > fileSize) {
      end = fileSize;
    }
    if (end > start) {
      PosixFadvise.adviseWillNeed(fd, start, end - start);
    }
  }

  /**
   * The data channel's raw descriptor for native advice, resolved once per reader; {@code -1} when
   * the platform cannot provide it (then every advisory route below is a no-op).
   */
  private int readAheadFd() {
    int fd = readAheadFd;
    if (fd == UNRESOLVED_READ_AHEAD_FD) {
      fd = PosixFadvise.extractFd(dataFileChannel);
      readAheadFd = fd;
    }
    return fd;
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * The standard backend advertises its batch only where the advice can actually reach the kernel
   * (Linux, extractable descriptor). Resolved once per reader — this is called once per transaction —
   * so the scan loops that gate on it pay nothing per page.
   */
  @Override
  public int preferredPrefetchBatch() {
    if (PREFETCH_BATCH == 0) {
      return 0;
    }
    return readAheadFd() >= 0
        ? PREFETCH_BATCH
        : 0;
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * {@code posix_fadvise(WILLNEED)} over the referenced offsets: the kernel submits every page of the
   * batch to the device at once and the {@link #read(PageReference, ResourceConfiguration)} that
   * follows for each of them finds the bytes in the page cache instead of waiting one device round
   * trip per page. Extents are derived from the SORTED keys — a page ends where the next one begins
   * in an append-only file — so touching pages merge into one advice call and only a page without a
   * near successor is hinted with the bounded tail. Stages nothing, reads no page objects, and never
   * throws: {@link PosixFadvise#adviseWillNeed} swallows every failure, so a declined hint leaves the
   * subsequent reads exactly as they are without it.
   */
  @Override
  public void prefetch(final PageReference[] references, final int count) {
    if (references == null || count <= 0) {
      return;
    }
    final int fd = readAheadFd();
    if (fd < 0) {
      return;
    }
    final int n = Math.min(count, references.length);
    if (n == 1) {
      // The single-reference hint of a trie descent: no sort, no scratch.
      final long key = keyOf(references[0]);
      if (key >= 0) {
        PosixFadvise.adviseWillNeed(fd, key, BATCH_READ_AHEAD_BYTES);
      }
      return;
    }
    final long[] keys = new long[n];
    int durable = 0;
    for (int i = 0; i < n; i++) {
      final long key = keyOf(references[i]);
      if (key >= 0) {
        keys[durable++] = key;
      }
    }
    if (durable == 0) {
      return;
    }
    Arrays.sort(keys, 0, durable);
    long start = keys[0];
    long end = start;
    for (int i = 0; i < durable; i++) {
      final long key = keys[i];
      if (key > end) {
        // A gap the tail did not bridge: this extent is complete, the next one starts here.
        PosixFadvise.adviseWillNeed(fd, start, end - start);
        start = key;
      }
      long pageEnd = key + BATCH_READ_AHEAD_BYTES;
      if (i + 1 < durable) {
        final long span = keys[i + 1] - key;
        if (span > 0 && span < BATCH_READ_AHEAD_BYTES) {
          pageEnd = key + span; // exact: the page cannot extend past its successor's offset
        }
      }
      if (pageEnd > end) {
        end = pageEnd;
      }
    }
    PosixFadvise.adviseWillNeed(fd, start, end - start);
  }

  /**
   * Resolves whether coalesced batch reads decode from the borrowed read buffer. An explicitly set
   * {@code sirix.filechannel.borrowBatchInput} decides on its own; while it is unset the batch path
   * follows {@code sirix.io.borrowOverflowInput}, so setting only that switch to {@code false}
   * restores owned input on both paths. With neither set, input is borrowed.
   *
   * @return {@code true} if batch members may be decoded from the borrowed read buffer
   */
  static boolean batchInputBorrowingEnabled() {
    return borrowedInputEnabled(System.getProperty(BORROW_BATCH_INPUT) != null
        ? BORROW_BATCH_INPUT
        : BORROW_OVERFLOW_INPUT);
  }

  private static long keyOf(final @Nullable PageReference reference) {
    return reference == null
        ? -1L
        : reference.getKey();
  }

  /**
   * One coalesced run [{@code from}, {@code to}]: span pread + last-body pread + per-page
   * deserialize.
   */
  /** DIAGNOSTIC (-Dsirix.projDiag): span bytes read and per-page fallbacks across all runs. */
  private static final boolean DIAG = Boolean.getBoolean("sirix.projDiag");
  private static final LongAdder RUN_SPAN_BYTES = new LongAdder();
  private static final LongAdder RUN_FALLBACKS = new LongAdder();
  private static final LongAdder RUN_COUNT = new LongAdder();

  public static String runDiagSummary() {
    return "[runs] count=" + RUN_COUNT.sum() + " spanBytes=" + RUN_SPAN_BYTES.sum() + " fallbacks="
        + RUN_FALLBACKS.sum();
  }

  private void readRun(final PageReference[] references, final Page[] pages, final int[] order, final int from,
      final int to, final @Nullable ResourceConfiguration resourceConfiguration, final long batchFileSize) {
    final long start = references[order[from]].getKey();
    final long lastOffset = references[order[to]].getKey();
    final int spanLen = (int) (lastOffset + 4 - start);
    if (DIAG) {
      RUN_COUNT.increment();
      RUN_SPAN_BYTES.add(spanLen);
    }
    ByteBuffer buffer = acquireBuffer(spanLen);
    try {
      buffer.clear().limit(spanLen);
      readFully(buffer, start, "coalesced page span");
      buffer.flip();
      final boolean useSegments = borrowBatchInput && byteHandler.supportsMemorySegments();
      final MemorySegment span = useSegments
          ? MemorySegment.ofBuffer(buffer)
          : null;
      final ByteBuffer checksumView = useSegments
          ? buffer.duplicate()
          : null;
      // Non-final members: length header + full body sit inside the span.
      for (int k = from; k < to; k++) {
        final PageReference member = references[order[k]];
        final long offset = member.getKey();
        final int rel = (int) (offset - start);
        final int dataLength = buffer.getInt(rel);
        final long bound = references[order[k + 1]].getKey() - offset - 4;
        if (dataLength <= 0 || dataLength > bound) {
          // Body would cross the next page's offset — not the append-only layout this
          // fast path assumes. Exact per-page read decides whether it is corruption.
          if (DIAG) {
            RUN_FALLBACKS.increment();
          }
          pages[order[k]] = read(member, resourceConfiguration, false, batchFileSize);
          continue;
        }
        if (useSegments) {
          // The independent view leaves header lookup bounds intact for the next member.
          checksumView.clear().position(rel + Integer.BYTES).limit(rel + Integer.BYTES + dataLength);
          verifyChecksumIfNeeded(checksumView, member, resourceConfiguration);
          pages[order[k]] =
              deserializeFromSegment(resourceConfiguration, span.asSlice(rel + Integer.BYTES, dataLength), member);
        } else {
          final byte[] page = new byte[dataLength];
          buffer.get(rel + 4, page);
          verifyChecksumIfNeeded(page, member, resourceConfiguration);
          pages[order[k]] = deserialize(resourceConfiguration, page, member);
        }
      }
      // Final member: its length header ends the span; the body needs one more pread.
      final int lastLength = buffer.getInt(spanLen - 4);
      checkDataLength(lastLength, batchFileSize);
      if (buffer.capacity() < lastLength) {
        final ByteBuffer grown = acquireBuffer(lastLength);
        releaseBuffer(buffer);
        buffer = grown;
      }
      buffer.clear().limit(lastLength);
      readFully(buffer, lastOffset + 4, "coalesced last page body");
      buffer.flip();
      final PageReference lastMember = references[order[to]];
      if (useSegments) {
        verifyChecksumIfNeeded(buffer, lastMember, resourceConfiguration);
        pages[order[to]] = deserializeFromSegment(resourceConfiguration, MemorySegment.ofBuffer(buffer), lastMember);
      } else {
        final byte[] page = new byte[lastLength];
        buffer.get(page);
        verifyChecksumIfNeeded(page, lastMember, resourceConfiguration);
        pages[order[to]] = deserialize(resourceConfiguration, page, lastMember);
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
      final RevisionFileData revisionFileData = cache.get(revision, (unused) -> getRevisionFileData(revision));
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
        verifyChecksumIfNeeded(buffer, reference, resourceConfiguration);
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
        final int read = readAt(dataFileChannel, slot, offset + position);
        if (read <= 0) {
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
      int bytesRead = 0;
      while (buffer.hasRemaining()) {
        final int read = readAt(revisionsOffsetFileChannel, buffer, fileOffset + bytesRead);
        if (read <= 0) {
          // Crash-shortened revisions file (or a zero-byte read, legal per the FileChannel
          // contract): under lazy revision records the trailing records may never have reached
          // the file — each missing one is salvaged from the uber-beacon tail-log below (and the
          // file healed) instead of failing the whole range.
          break;
        }
        bytesRead += read;
      }
      final RevisionFileData[] result = new RevisionFileData[count];
      // Both beacon slots, read at most ONCE for the whole range and only when something actually
      // needs salvaging — the ring is a salvage source, not a second opinion on a valid record.
      ByteBuffer[] slots = null;
      for (int i = 0; i < count; i++) {
        final int revision = fromRevision + i;
        final int base = i * IOStorage.REVISIONS_FILE_RECORD_SIZE;
        if (base + IOStorage.REVISIONS_FILE_RECORD_SIZE <= bytesRead) {
          final long offset = buffer.getLong(base);
          final long timestamp = buffer.getLong(base + 8);
          final long storedChecksum = buffer.getLong(base + 16);
          // 4th field: the RevisionRootPage's own page hash (0 = legacy record / no hash).
          final long pageHash = buffer.getLong(base + 24);
          // These bytes are the ONLY file-resident path to the revision's root page — verify
          // them. The checksum covers 24 bytes when a page hash is present, 16 when legacy
          // (hash == 0), so beta1 resources still open cleanly.
          if (storedChecksum == IOStorage.expectedRevisionRecordChecksum(offset, timestamp, pageHash)) {
            result[i] = new RevisionFileData(offset, Instant.ofEpochMilli(timestamp), pageHash);
            continue;
          }
        }
        if (slots == null) {
          slots = readUberBeaconSlots();
        }
        final RevisionFileData ringRecord = tailLogRecord(slots, revision);
        if (ringRecord != null) {
          LOGGER.warn("Revision record {} was missing or torn in the revisions file — salvaged from "
              + "the uber-beacon tail-log, healing the file", revision);
          healRevisionRecord(revision, ringRecord);
          result[i] = ringRecord;
          continue;
        }
        throw new SirixIOException("Corrupt or missing revisions record for revision " + revision
            + " (checksum mismatch or truncated file), and no salvageable tail-log copy exists in the "
            + "uber-beacon slots — torn write or storage corruption");
      }
      return result;
    } catch (IOException e) {
      throw new SirixIOException(e);
    } finally {
      buffer.order(ByteOrder.LITTLE_ENDIAN);
      releaseBuffer(buffer);
    }
  }

  /** Reads both uber-beacon slots once (little-endian) for tail-log probing. */
  private ByteBuffer[] readUberBeaconSlots() {
    final ByteBuffer primary = readBeaconSlot(IOStorage.PRIMARY_BEACON_OFFSET);
    final ByteBuffer secondary = readBeaconSlot(IOStorage.SECONDARY_BEACON_OFFSET);
    primary.order(ByteOrder.LITTLE_ENDIAN);
    secondary.order(ByteOrder.LITTLE_ENDIAN);
    return new ByteBuffer[] {primary, secondary};
  }

  /**
   * The tail-log copy of the given revision's record from either beacon slot, or {@code null}. The
   * entry must sit at the revision's deterministic ring index, name EXACTLY that revision and pass
   * {@link IOStorage#tailLogEntryValidAt} — a torn or stale entry can never masquerade as the record.
   */
  private static RevisionFileData tailLogRecord(final ByteBuffer[] slots, final int revision) {
    final int base = IOStorage.tailLogEntryOffsetInSlot(revision);
    for (final ByteBuffer slot : slots) {
      if (slot.remaining() < IOStorage.BEACON_SLOT_BYTES) {
        continue; // fresh or short file — no tail-log in this slot
      }
      if (slot.getInt(base) != revision || !IOStorage.tailLogEntryValidAt(slot, base)) {
        continue;
      }
      return new RevisionFileData(slot.getLong(base + 8), Instant.ofEpochMilli(slot.getLong(base + 16)),
          slot.getLong(base + 32));
    }
    return null;
  }

  /**
   * Best-effort self-heal: rewrites the salvaged 32-byte record at its deterministic slot. A
   * read-only or otherwise unwritable channel only logs — the salvaged value is served either way,
   * and a writer's next force makes a successful heal durable.
   *
   * <p>
   * Skipped when the revisions file's SUPERBLOCK is absent (file lost/recreated): writing a record
   * into a superblock-less file would make it non-empty with an all-zero header, which the validator
   * correctly rejects as corruption. The next write transaction heals both — it knows the resource
   * UUID, which this reader does not.
   *
   * <p>
   * The slot is re-read immediately before writing and the heal skipped if a checksum-valid record
   * appeared there meanwhile: a concurrent writer (recovery truncation + recommit) may legitimately
   * have rewritten it, and this reader's salvage source could belong to the replaced timeline.
   */
  private void healRevisionRecord(final int revision, final RevisionFileData ringRecord) {
    try {
      final ByteBuffer magicProbe = ByteBuffer.allocate(Superblock.MAGIC.length);
      final int probeRead = readAt(revisionsOffsetFileChannel, magicProbe, 0L);
      if (probeRead < Superblock.MAGIC.length) {
        return;
      }
      magicProbe.flip();
      final byte[] magic = new byte[Superblock.MAGIC.length];
      magicProbe.get(magic);
      if (!Arrays.equals(magic, Superblock.MAGIC)) {
        return;
      }
      final long recordOffset = IOStorage.revisionsFileOffset(revision);
      final long offset = ringRecord.offset();
      final long timestampMillis = ringRecord.timestamp().toEpochMilli();
      final long pageHash = ringRecord.pageHash();
      final ByteBuffer current =
          ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      int currentRead = 0;
      while (current.hasRemaining()) {
        final int n = readAt(revisionsOffsetFileChannel, current, recordOffset + currentRead);
        if (n <= 0) {
          break;
        }
        currentRead += n;
      }
      if (currentRead == IOStorage.REVISIONS_FILE_RECORD_SIZE) {
        final boolean nowValid = current.getLong(16) == IOStorage.expectedRevisionRecordChecksum(current.getLong(0),
            current.getLong(8), current.getLong(24));
        if (nowValid && (current.getLong(0) != offset || current.getLong(8) != timestampMillis
            || current.getLong(24) != pageHash)) {
          LOGGER.warn("Skipping heal of revision record {} — a concurrent writer rewrote the slot "
              + "with a different valid record", revision);
          return;
        }
      }
      final ByteBuffer record =
          ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      record.putLong(offset);
      record.putLong(timestampMillis);
      record.putLong(IOStorage.expectedRevisionRecordChecksum(offset, timestampMillis, pageHash));
      record.putLong(pageHash);
      record.flip();
      while (record.hasRemaining()) {
        final int written = revisionsOffsetFileChannel.write(record, recordOffset + record.position());
        if (written <= 0) {
          throw new IOException("Revision-record heal stalled: no progress");
        }
        HftBoundaryTelemetry.storageWrite(written);
      }
    } catch (final IOException | RuntimeException healFailure) {
      LOGGER.warn("Salvaged revision record {} could not be healed back into the revisions file", revision,
          healFailure);
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
