/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.exception.SirixIOException;
import io.sirix.io.HashAlgorithm;
import io.sirix.page.PageKind.SlottedPageDecodeState;
import io.sirix.page.pax.RegionTable;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

/**
 * The still-compressed chunks of a page whose body was parsed but whose records were not expanded,
 * and the machinery that expands one chunk when a reader first asks for a slot inside it.
 *
 * <p>
 * A chunk-framed body states its section lengths on the wire, which is what makes this possible:
 * the reader can decode the META frame alone, derive every entry's in-memory length from it, and
 * lay out the complete directory of a heap whose bytes it has not looked at. Heap addresses are
 * therefore final from the moment the page exists — expansion only ever fills bytes in, never moves
 * them — so every zero-copy reader keeps its contract behind a single acquire-load gate.
 *
 * <p>
 * <b>What is retained.</b> Each chunk's encoded bytes, in a page-owned array that is dropped the
 * moment the chunk is expanded, plus the page's own copy of the META-derived state the expansion
 * needs. That copy is not an optimisation but a correctness requirement: the eager path derives
 * into per-thread scratches, and a chunk expanded later runs on whichever thread happens to read
 * the slot, long after the deserializing thread reused its scratches for other pages.
 *
 * <p>
 * <b>Why the pending bytes are a byte array and not a segment.</b> Every codec's decode entry point
 * takes a {@code byte[]} input, so a native pending buffer would have to be copied back to the heap
 * before it could be decoded. The uniform-native-provenance rule exists for the segments the column
 * kernels load from (oracle/graal#14255); a decoder input reaches no vector load site. This is the
 * same shape {@code RegionTable} defers a compressed region in, tail slack included — sized
 * exactly, the buffer fails the native decoder's precondition and every expansion silently takes
 * the Java decoder.
 */
final class LazyChunkedBody {

  /**
   * Re-injects the values the writer moved into PAX regions, for one chunk's worth of records.
   *
   * <p>
   * Supplied by the deserializer rather than reimplemented here: the two injection passes are
   * parameterised by half a dozen per-page arrays each, and threading those through this class would
   * duplicate bookkeeping that only the section parse understands. One virtual call per materialized
   * chunk, never per record.
   */
  @FunctionalInterface
  interface RangeInjector {

    /**
     * @param fromEntry first entry rank of the chunk, inclusive
     * @param toEntry last entry rank of the chunk, exclusive
     * @param fromSlot first slot of the chunk, inclusive
     * @param toSlot last slot of the chunk, inclusive
     */
    void inject(MemorySegment slottedPage, RegionTable regionTable, int fromEntry, int toEntry, int fromSlot,
        int toSlot);
  }

  /** No-op injector for pages on which neither elision fired. */
  private static final RangeInjector NO_INJECTION = (_, _, _, _, _, _) -> {
  };

  /**
   * Per-chunk expansion scratch: native, because the decoders write into a {@link MemorySegment}, and
   * over-allocated by the decoder's output tail slack for the reason spelled out in
   * {@code RegionTable#allocate}. Per-thread rather than per-page — a chunk is decoded and consumed
   * entirely inside one synchronized block, so no other thread can observe this buffer.
   *
   * <p>
   * Deliberately not the deserializer's staging scratch: a page can be expanded from a reader thread
   * that is in the middle of deserializing another page further down its own stack, and sharing one
   * buffer between the two would let the inner expansion overwrite the outer page's body.
   */
  private static final ThreadLocal<MemorySegment> EXPAND_SCRATCH =
      ThreadLocal.withInitial(() -> Arena.ofAuto().allocate(64 * 1024 + SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK));

  /**
   * Per-chunk materialized bits, as a {@code long[2]} covering the 128 chunks a table can hold.
   * Acquire/release rather than plain: the expansion writes a chunk's heap bytes and then publishes
   * the bit, and a reader that observes the bit must observe those bytes. Plain accesses here are the
   * textbook unsafe publication — a reader sees the flag without the record behind it.
   */
  private static final VarHandle MATERIALIZED = MethodHandles.arrayElementVarHandle(long[].class);

  /** Slot value in {@link #chunkOfSlot} for a slot no chunk covers. */
  private static final byte NO_CHUNK = -1;

  private final long recordPageKey;
  private final long pageKeyBase;
  private final int chunkCount;

  /** Heap bytes each chunk decodes to. */
  private final int[] rawLen;
  /** Wire codec of each chunk. */
  private final int[] codec;
  /** XXH3-64 over each chunk's stored bytes. */
  private final long[] hash;
  /** Encoded bytes of each chunk, nulled as the chunk is expanded. */
  private final byte[][] pending;
  /** Encoded length within {@link #pending}; the buffer itself carries decoder tail slack past it. */
  private final int[] pendingLen;

  /** First entry rank of each chunk. */
  private final int[] firstEntry;
  /** Entries each chunk covers. */
  private final int[] entryCount;
  /** First and last slot of each chunk, for the slot-ranged value injection. */
  private final int[] firstSlot;
  private final int[] lastSlot;
  /** In-memory heap range each chunk owns, for the poison fill. */
  private final int[] heapFrom;
  private final int[] heapTo;

  /** Slot to chunk index, or {@link #NO_CHUNK}. One array load, no branch, on the accessor gate. */
  private final byte[] chunkOfSlot;
  /** Entry rank to slot, so an expansion walks its own entries without re-walking the bitmap. */
  private final short[] slotOfEntry;

  /**
   * The page's own copy of the META-derived expansion state, or {@code null} on a degenerate body
   * whose records are stored verbatim and need no expansion at all.
   */
  private final SlottedPageDecodeState decodeState;

  private final RangeInjector injector;

  private final long[] materialized = new long[2];

  /**
   * Set once every chunk has been expanded. Volatile: it is the gate every heap accessor reads, and
   * the edge that orders the last expansion's writes before a reader's loads.
   */
  private volatile boolean allMaterialized;

  /** Chunks expanded so far; written under the page monitor only. */
  private int materializedCount;

  /** Encoded bytes still held, which is what a cache is asked to weigh this page by. */
  private volatile int pendingBytes;

  /**
   * Take ownership of a page's chunk table.
   *
   * <p>
   * The table itself is a per-thread scratch the next page decoded on this thread will overwrite, so
   * every row is copied out at exactly the width this page needs. The slot ranges and heap ranges
   * come from the caller because only the directory walk knows them.
   *
   * @param table the parsed chunk table, whose pending wire buffers are taken over and cleared
   * @param firstSlot first slot of each chunk, and {@code lastSlot} its last, both inclusive
   * @param heapFrom the in-memory heap range each chunk owns, as offsets from the heap's start
   * @param slotOfEntry the page's entry rank to slot mapping
   * @param decodeState the page's own expansion state, or {@code null} for a degenerate body
   * @param injector re-injects region-held values per chunk, or {@code null} when nothing was elided
   */
  LazyChunkedBody(final long recordPageKey, final long pageKeyBase, final PageKind.ChunkTable table,
      final int[] firstSlot, final int[] lastSlot, final int[] heapFrom, final int[] heapTo, final short[] slotOfEntry,
      final SlottedPageDecodeState decodeState, final RangeInjector injector) {
    final int count = table.count;
    this.recordPageKey = recordPageKey;
    this.pageKeyBase = pageKeyBase;
    this.chunkCount = count;
    this.rawLen = Arrays.copyOf(table.rawLen, count);
    this.codec = Arrays.copyOf(table.codec, count);
    this.hash = Arrays.copyOf(table.hash, count);
    this.pending = Arrays.copyOf(table.pendingWire, count);
    this.pendingLen = Arrays.copyOf(table.encLen, count);
    this.firstEntry = Arrays.copyOf(table.firstEntry, count);
    this.entryCount = Arrays.copyOf(table.entryCount, count);
    this.firstSlot = Arrays.copyOf(firstSlot, count);
    this.lastSlot = Arrays.copyOf(lastSlot, count);
    this.heapFrom = Arrays.copyOf(heapFrom, count);
    this.heapTo = Arrays.copyOf(heapTo, count);
    this.slotOfEntry = slotOfEntry;
    this.decodeState = decodeState;
    this.injector = injector == null
        ? NO_INJECTION
        : injector;

    final byte[] map = new byte[PageLayout.SLOT_COUNT];
    Arrays.fill(map, NO_CHUNK);
    int bytes = 0;
    for (int c = 0; c < count; c++) {
      // Filled per chunk rather than per slot: a chunk's entries are consecutive in rank order and
      // entries ascend with slots, so each chunk owns one contiguous slot run and the whole map
      // costs a handful of memsets. The holes between runs keep NO_CHUNK, which is what an
      // unpopulated slot must read.
      Arrays.fill(map, firstSlot[c], lastSlot[c] + 1, (byte) c);
      bytes += table.encLen[c];
    }
    this.chunkOfSlot = map;
    this.pendingBytes = bytes;
    this.allMaterialized = count == 0;
    // The scratch table must not keep the page's wire buffers alive past this point.
    Arrays.fill(table.pendingWire, 0, count, null);
  }

  /** Whether every chunk has been expanded. One volatile load; the gate every heap accessor takes. */
  boolean isAllMaterialized() {
    return allMaterialized;
  }

  /** Encoded bytes this page still holds on behalf of unexpanded chunks. */
  int pendingBytes() {
    return pendingBytes;
  }

  /** Chunks the page's body was framed into. */
  int chunkCount() {
    return chunkCount;
  }

  /** Whether {@code chunk} has been expanded into the page heap. */
  boolean isMaterialized(final int chunk) {
    return ((long) MATERIALIZED.getAcquire(materialized, chunk >>> 6) & (1L << (chunk & 63))) != 0;
  }

  /** The chunk covering {@code slotNumber}, or a negative value when no chunk does. */
  int chunkOf(final int slotNumber) {
    return chunkOfSlot[slotNumber];
  }

  /**
   * Fill the heap ranges of every unexpanded chunk with the poison byte, so a reader that bypasses
   * the gate fails on bytes that cannot be mistaken for a record.
   */
  void poison(final MemorySegment slottedPage) {
    for (int c = 0; c < chunkCount; c++) {
      if (!isMaterialized(c)) {
        slottedPage.asSlice(PageLayout.HEAP_START + heapFrom[c], heapTo[c] - heapFrom[c])
                   .fill(ChunkedBodyConfig.POISON_BYTE);
      }
    }
  }

  /**
   * Expand the chunk holding {@code slotNumber}, unless it is already expanded.
   *
   * <p>
   * The fast path is one volatile load, one array load and one acquire load, all of which a reader
   * that keeps hitting the same chunk predicts perfectly.
   */
  void ensureChunkFor(final KeyValueLeafPage page, final int slotNumber) {
    if (allMaterialized) {
      return;
    }
    final int chunk = chunkOfSlot[slotNumber];
    if (chunk < 0 || isMaterialized(chunk)) {
      return;
    }
    materialize(page, chunk);
  }

  /** Expand every chunk this page still holds compressed. */
  void ensureAllChunks(final KeyValueLeafPage page) {
    if (allMaterialized) {
      return;
    }
    for (int c = 0; c < chunkCount; c++) {
      if (!isMaterialized(c)) {
        materialize(page, c);
      }
    }
  }

  /**
   * Decode one chunk and expand its records into the slots the directory already reserved for them.
   *
   * <p>
   * Synchronized on the page, which is the same monitor {@link KeyValueLeafPage#close} holds: the
   * segment being written here must not be released underneath the write, and the guard-count
   * protocol that keeps a cached page alive is left exactly as it was.
   */
  private void materialize(final KeyValueLeafPage page, final int chunk) {
    synchronized (page) {
      final byte[] wire = pending[chunk];
      if (wire == null) {
        // Another thread expanded it while this one waited for the monitor.
        return;
      }
      final MemorySegment slottedPage = page.getSlottedPage();
      if (slottedPage == null) {
        throw new SirixIOException(
            "page " + recordPageKey + " was closed before chunk " + chunk + " could be expanded");
      }
      final int chunkRawLen = rawLen[chunk];
      final MemorySegment scratch = expandScratch(chunkRawLen);
      decodeFrame(wire, pendingLen[chunk], codec[chunk], chunkRawLen, hash[chunk], scratch, 0L, recordPageKey, chunk);

      if (decodeState == null) {
        // Degenerate body: the chunk's bytes are the records, verbatim, and the directory already
        // says where they go.
        MemorySegment.copy(scratch, 0L, slottedPage, PageLayout.HEAP_START + heapFrom[chunk], chunkRawLen);
      } else {
        final int from = firstEntry[chunk];
        final int to = from + entryCount[chunk];
        long srcOff = 0;
        for (int entry = from; entry < to; entry++) {
          srcOff += decodeState.expandEntryInto(slottedPage, entry, slotOfEntry[entry] & 0xFFFF, scratch, srcOff,
              pageKeyBase);
        }
        if (srcOff != chunkRawLen) {
          throw new SirixIOException("page " + recordPageKey + " chunk " + chunk + " expanded " + srcOff + " of its "
              + chunkRawLen + " heap bytes");
        }
        injector.inject(slottedPage, page.getRegionTable(), from, to, firstSlot[chunk], lastSlot[chunk]);
      }

      pending[chunk] = null;
      pendingBytes -= pendingLen[chunk];
      // Release AFTER every byte of the chunk is in place: this store is the edge a reader's
      // acquire-load of the same bit synchronizes with.
      MATERIALIZED.setRelease(materialized, chunk >>> 6,
          (long) MATERIALIZED.get(materialized, chunk >>> 6) | (1L << (chunk & 63)));
      if (++materializedCount == chunkCount) {
        allMaterialized = true;
      }
      ChunkedBodyConfig.recordChunkMaterialization();
    }
  }

  /** Drop everything the page no longer needs once it is closed. */
  void release() {
    Arrays.fill(pending, null);
    pendingBytes = 0;
  }

  private static MemorySegment expandScratch(final int rawLen) {
    MemorySegment scratch = EXPAND_SCRATCH.get();
    final long needed = (long) rawLen + SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK;
    if (scratch.byteSize() < needed) {
      scratch = Arena.ofAuto().allocate(Math.max(needed, scratch.byteSize() * 2L));
      EXPAND_SCRATCH.set(scratch);
    }
    return scratch;
  }

  /**
   * Verify one frame's stored bytes against the checksum its table row carries, and decode them into
   * {@code dst}.
   *
   * <p>
   * The checksum is verified before the decode, not after: a corrupted frame must be named as corrupt
   * rather than produce whatever a codec makes of damaged input. Shared with the eager reader so a
   * chunk decoded at load time and one decoded on demand cannot disagree about what a frame means.
   *
   * @param chunkIndex the chunk's index, or {@code -1} for the META frame
   */
  static void decodeFrame(final byte[] src, final int encLen, final int codec, final int rawLen,
      final long expectedHash, final MemorySegment dst, final long dstOff, final long recordPageKey,
      final int chunkIndex) {
    final long actualHash = HashAlgorithm.XXH3.computeHashLong(src, 0, encLen);
    if (actualHash != expectedHash) {
      throw new SirixIOException("page " + recordPageKey + " " + frameName(chunkIndex) + " fails its checksum:"
          + " expected 0x" + Long.toHexString(expectedHash) + ", computed 0x" + Long.toHexString(actualHash) + " over "
          + encLen + " stored bytes");
    }
    final int decoded;
    if (codec == 0) {
      decoded = ZeroRunByteCodec.decode(src, 0, encLen, dst, dstOff);
    } else if (codec == 2) {
      decoded = ByteRunCodec.decode(src, 0, encLen, dst, dstOff);
    } else if (codec == 3) {
      decoded = SirixLZ77Codec.decode(src, 0, encLen, dst, dstOff);
    } else if (codec == ChunkedBodyConfig.CODEC_STORED) {
      MemorySegment.copy(src, 0, dst, ValueLayout.JAVA_BYTE, dstOff, encLen);
      decoded = encLen;
    } else {
      throw new SirixIOException(
          "page " + recordPageKey + " " + frameName(chunkIndex) + " uses unsupported codec " + codec);
    }
    if (decoded != rawLen) {
      throw new SirixIOException("page " + recordPageKey + " " + frameName(chunkIndex) + " decoded " + decoded
          + " bytes, the chunk table says " + rawLen);
    }
  }

  private static String frameName(final int chunkIndex) {
    return chunkIndex < 0
        ? "META frame"
        : "chunk " + chunkIndex;
  }
}
