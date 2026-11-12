/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import static io.sirix.cache.ShardedPageCache.retainCleanupFailure;
import static io.sirix.cache.ShardedPageCache.rethrowCleanupFailure;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.sirix.cache.ScratchLease;
import io.sirix.cache.ScratchUse;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jspecify.annotations.Nullable;

/**
 * The parallel importer's fused Stage-A: ONE pass over the raw UTF-8 stream that simultaneously
 * SLICES member-aligned chunks (bulk segment copies, never per-byte appends) and produces every
 * piece of coordinator metadata — exact node count, member count, last-member node count, name
 * occurrences and path resolution — without decoding a single value byte and, in steady state,
 * without allocating per name occurrence:
 *
 * <ul>
 * <li>Names resolve through an open-addressed BYTE-hash table (FNV-1a over the raw name bytes,
 * verified by byte comparison): decode-to-String happens only on a table miss, i.e. once per
 * distinct name for the whole load. A name is resolved in the read buffer it was scanned in; only a
 * name a refill cuts in two is read back from the chunk.</li>
 * <li>Each name entry carries its own tiny {@code parentPCR → fieldPCR} map, so the per-occurrence
 * path cost is one long-hash probe instead of a String-keyed memo lookup; misses resolve through
 * the REAL summary writer on the coordinator (global first-occurrence order preserved: path-resolve
 * before name-intern, the sequential path's exact dictionary call order).</li>
 * <li>Reference-count deltas accumulate per path node and flush per chunk (before any rotation can
 * flush summary pages cold).</li>
 * </ul>
 *
 * <p>
 * The build that follows verifies the count exactly (final minted key == range end AND populated
 * slots == n), so a divergence between this pass and the full tokenizer refuses loudly per chunk.
 */
final class FusedSliceAndScan implements AutoCloseable {
  static volatile @Nullable Runnable chunkHandoffHookForTesting;

  /**
   * One node of the feeder-owned PATH TRIE: scan context is tracked by trie-node IDENTITY, never by
   * PCR values, so the feeder needs no access to the path summary at all. The coordinator resolves
   * {@link #pcr} for NEW nodes at chunk handoff, in first-occurrence order — the exact order a
   * sequential load inserts paths. Field ownership is disjoint across the two threads: the feeder
   * mutates the children maps and tallies, the coordinator writes {@code pcr}; the handoff queue's
   * put/take pair is the fence that publishes each chunk's new nodes.
   */
  static final class PathStep {
    final PathStep parent;
    /** The STABLE dense name id of this step, or {@code -1} for an {@code __array__} step. */
    final int nameSlot;
    /** The decoded field name (null for {@code __array__} steps); immutable pre-publication. */
    final String name;
    long pcr = -1;
    private Int2ObjectOpenHashMap<PathStep> childrenByNameSlot;
    private PathStep arrayChild;
    /** Occurrences THIS chunk (drained at handoff into reference-count deltas). */
    int chunkOccurrences;
    boolean touchedThisChunk;

    PathStep(final PathStep parent, final int nameSlot, final String name) {
      this.parent = parent;
      this.nameSlot = nameSlot;
      this.name = name;
    }

    Int2ObjectOpenHashMap<PathStep> childrenByNameSlot() {
      if (childrenByNameSlot == null) {
        childrenByNameSlot = new Int2ObjectOpenHashMap<>(4);
      }
      return childrenByNameSlot;
    }

    Int2ObjectOpenHashMap<PathStep> childrenRaw() {
      return childrenByNameSlot;
    }

    PathStep arrayChild() {
      return arrayChild;
    }

    void setArrayChild(final PathStep child) {
      this.arrayChild = child;
    }

  }

  /**
   * One member-aligned chunk plus everything the coordinator must resolve before its build.
   *
   * @param memberNodes the node count of each member in document order, or {@code null} when the
   *        coordinator did not ask for them. Only a load that must name every record — a one-pass
   *        projection build — needs them; an ordinary import pays neither the array nor the stores.
   */
  record Chunk(ChunkBuffer bytes, int length, boolean isFinal, long nodes, long members, long lastMemberNodes,
      List<PathStep> newSteps, List<PathStep> touchedSteps, int[] stepOccurrences, List<String> newNames,
      List<int[]> nameTallies, long @Nullable [] memberNodes) {
  }

  /**
   * An append-only byte buffer over 256 KiB slabs of allocator scratch.
   *
   * <p>
   * The importer deliberately schedules multi-megabyte member-aligned chunks: that is large enough to
   * amortize path resolution, worker dispatch and tail-page stitching. One contiguous buffer per
   * chunk would have to be sized for the largest record a load ever meets; this slab chain preserves
   * the scheduling unit with one fixed capacity, the allocator's largest slot class, and admits a
   * single record larger than the scheduling target by linking more slabs instead of growing and
   * copying.
   * </p>
   *
   * <p>
   * The slabs are borrowed ({@link ScratchUse#IMPORT}) as the chunk fills and all returned by
   * {@link #release()}, which whoever consumed the chunk calls: the worker that built its pages, or
   * the coordinator for a chunk no worker will read. A buffer holds memory only between those two
   * points. The slabs used to be heap arrays recycled through a queue of sixteen chains the scanner
   * kept for itself, outside every budget.
   * </p>
   *
   * <p>
   * The two-level chain avoids a growing reference directory. A block holds 256 slab handles, each
   * slab 256 KiB of payload: the ordinary 4 MiB chunk occupies one block and 16 slabs.
   * </p>
   *
   * <p>
   * One thread at a time: the scanner appends, then hands the buffer over (through the handoff queue
   * and the build pool, which publish it) to the one thread that reads and releases it.
   * {@link #release()} alone may also be called by a second thread once the first is known to have
   * finished, so it is synchronized and idempotent.
   * </p>
   */
  static final class ChunkBuffer extends InputStream {

    static final int SLAB_BYTES = 256 << 10;
    private static final int SLABS_PER_BLOCK = 256;
    private static final int BLOCK_BYTES = SLAB_BYTES * SLABS_PER_BLOCK;

    private static final class SlabBlock {
      /** The borrowed slabs in append order; {@code null} from the first slab not yet needed. */
      private final ScratchLease[] slabs = new ScratchLease[SLABS_PER_BLOCK];
      private SlabBlock next;
    }

    private final SlabBlock firstBlock = new SlabBlock();
    private SlabBlock appendBlock = firstBlock;
    private int appendBlockIndex;
    private int length;
    private int readPosition;
    private int readLimit;
    private boolean released;

    int length() {
      return length;
    }

    void append(final byte[] source, final int sourceOffset, final int byteCount) {
      Objects.checkFromIndexSize(sourceOffset, byteCount, source.length);
      if (byteCount == 0) {
        return;
      }
      if (released) {
        throw new IllegalStateException("append to an import chunk whose slabs were returned");
      }
      if (length > Integer.MAX_VALUE - byteCount) {
        throw new IllegalStateException("one JSON import chunk exceeds the 2 GiB addressable limit");
      }

      int remaining = byteCount;
      int sourcePosition = sourceOffset;
      while (remaining > 0) {
        final int blockIndex = length / BLOCK_BYTES;
        moveAppendBlockTo(blockIndex);
        final int offsetInBlock = length - blockIndex * BLOCK_BYTES;
        final int slabIndex = offsetInBlock / SLAB_BYTES;
        final int slabOffset = offsetInBlock - slabIndex * SLAB_BYTES;
        ScratchLease slab = appendBlock.slabs[slabIndex];
        if (slab == null) {
          slab = ScratchLease.of(SLAB_BYTES, ScratchUse.IMPORT);
          appendBlock.slabs[slabIndex] = slab;
        }
        final int copied = Math.min(remaining, SLAB_BYTES - slabOffset);
        MemorySegment.copy(source, sourcePosition, slab.segment(), ValueLayout.JAVA_BYTE, slabOffset, copied);
        sourcePosition += copied;
        remaining -= copied;
        length += copied;
      }
    }

    byte byteAt(final int index) {
      Objects.checkIndex(index, length);
      final int blockIndex = index / BLOCK_BYTES;
      final int offsetInBlock = index - blockIndex * BLOCK_BYTES;
      final ScratchLease slab = blockAt(blockIndex).slabs[offsetInBlock / SLAB_BYTES];
      return slab.segment().get(ValueLayout.JAVA_BYTE, offsetInBlock & (SLAB_BYTES - 1));
    }

    void copyTo(final int sourceOffset, final byte[] destination, final int destinationOffset, final int byteCount) {
      Objects.checkFromIndexSize(sourceOffset, byteCount, length);
      Objects.checkFromIndexSize(destinationOffset, byteCount, destination.length);
      int sourcePosition = sourceOffset;
      int destinationPosition = destinationOffset;
      int remaining = byteCount;
      while (remaining > 0) {
        final int blockIndex = sourcePosition / BLOCK_BYTES;
        final int offsetInBlock = sourcePosition - blockIndex * BLOCK_BYTES;
        final int slabIndex = offsetInBlock / SLAB_BYTES;
        final int slabOffset = offsetInBlock - slabIndex * SLAB_BYTES;
        final int copied = Math.min(remaining, SLAB_BYTES - slabOffset);
        MemorySegment.copy(blockAt(blockIndex).slabs[slabIndex].segment(), ValueLayout.JAVA_BYTE, slabOffset,
            destination, destinationPosition, copied);
        sourcePosition += copied;
        destinationPosition += copied;
        remaining -= copied;
      }
    }

    InputStream prepareRead(final int byteCount) {
      if (byteCount < 0 || byteCount > length) {
        throw new IllegalArgumentException("read length must be in [0, " + length + "], got " + byteCount);
      }
      readPosition = 0;
      readLimit = byteCount;
      return this;
    }

    @Override
    public int read() {
      return readPosition >= readLimit
          ? -1
          : byteAt(readPosition++) & 0xff;
    }

    @Override
    public int read(final byte[] destination, final int destinationOffset, final int byteCount) {
      Objects.checkFromIndexSize(destinationOffset, byteCount, destination.length);
      if (byteCount == 0) {
        return 0;
      }
      if (readPosition >= readLimit) {
        return -1;
      }
      final int copied = Math.min(byteCount, readLimit - readPosition);
      copyTo(readPosition, destination, destinationOffset, copied);
      readPosition += copied;
      return copied;
    }

    @Override
    public int available() {
      return readLimit - readPosition;
    }

    /**
     * Return every slab to the allocator. The buffer is empty afterwards and stays so: it reads as
     * exhausted and refuses appends. Idempotent, and safe to call from a second thread once the thread
     * that read the chunk has finished with it.
     */
    synchronized void release() {
      if (released) {
        return;
      }
      released = true;
      length = 0;
      readPosition = 0;
      readLimit = 0;
      Throwable failure = null;
      for (SlabBlock block = firstBlock; block != null; block = block.next) {
        final ScratchLease[] slabs = block.slabs;
        for (int i = 0; i < SLABS_PER_BLOCK; i++) {
          final ScratchLease slab = slabs[i];
          if (slab != null) {
            slabs[i] = null;
            try {
              slab.close();
            } catch (final Throwable cleanup) {
              failure = retainCleanupFailure(failure, cleanup);
            }
          }
        }
      }
      firstBlock.next = null;
      appendBlock = firstBlock;
      appendBlockIndex = 0;
      rethrowCleanupFailure(failure);
    }

    /** Slabs this buffer holds borrowed. */
    int slabCount() {
      int count = 0;
      for (SlabBlock block = firstBlock; block != null; block = block.next) {
        for (final ScratchLease slab : block.slabs) {
          if (slab != null) {
            count++;
          }
        }
      }
      return count;
    }

    private void moveAppendBlockTo(final int blockIndex) {
      if (blockIndex < appendBlockIndex) {
        appendBlock = firstBlock;
        appendBlockIndex = 0;
      }
      while (appendBlockIndex < blockIndex) {
        if (appendBlock.next == null) {
          appendBlock.next = new SlabBlock();
        }
        appendBlock = appendBlock.next;
        appendBlockIndex++;
      }
    }

    private SlabBlock blockAt(final int blockIndex) {
      SlabBlock block = firstBlock;
      for (int i = 0; i < blockIndex; i++) {
        block = block.next;
        if (block == null) {
          throw new IllegalStateException("missing slab block " + blockIndex + " for " + length + " bytes");
        }
      }
      return block;
    }
  }

  private static final int READ_BUFFER_BYTES = 256 << 10;
  private static final int INITIAL_DEPTH_CAPACITY = 64;
  private static final int NAME_TABLE_CAPACITY = 1 << 10;

  private final InputStream in;
  private final int chunkByteBudget;
  private final int maxMembersPerChunk;

  private final byte[] readBuffer = new byte[READ_BUFFER_BYTES];
  private int position;
  private int limit;
  /** Start of the not-yet-copied segment of the read buffer (bulk chunk copies). */
  private int segmentStart;

  private ChunkBuffer chunkBuffer = new ChunkBuffer();
  private int chunkLength;

  private boolean inString;
  private boolean escaped;
  private boolean stringIsName;

  /**
   * Where the field name being scanned starts in {@link #readBuffer}, or {@code -1}: no name is open,
   * or a refill has moved its beginning out of the read buffer (see {@link #nameStartInChunk}).
   */
  private int nameStartInBuffer = -1;

  /**
   * Where the field name being scanned starts in the chunk, set by the refill that cut the name in
   * two; {@code -1} otherwise.
   */
  private int nameStartInChunk = -1;
  private boolean arrayClosed;

  // Container context: the current PATH STEP + object flag, mirroring the assembler's rules
  // (plain OBJECT is transparent; arrays add one __array__ step; named containers anchor theirs).
  private PathStep[] contextStep = new PathStep[INITIAL_DEPTH_CAPACITY];
  private boolean[] contextIsObject = new boolean[INITIAL_DEPTH_CAPACITY];
  private int depth;

  /** A name string just closed; the NEXT value token binds to it (its path step precomputed). */
  private PathStep pendingFieldStep;
  private boolean namePending;

  /** The document-ordered handoff lists for the CURRENT chunk. */
  private final ArrayList<PathStep> newSteps = new ArrayList<>();
  private final ArrayList<PathStep> touchedSteps = new ArrayList<>();
  private final ArrayList<String> newNames = new ArrayList<>();

  private long nodes;
  private long members;
  private long lastMemberNodes;
  private long memberStartNodes;

  /**
   * Per-member node counts for the CURRENT chunk, or {@code null} when the coordinator does not need
   * them. Reserved once at the requested capacity and reused for every chunk, so a load that does
   * need them still allocates no per-chunk array beyond the handed-off copy.
   */
  private final @Nullable LongArrayList memberNodes;

  // ==== open-addressed byte-hash name table (slots map to STABLE dense ids) ===================
  /**
   * Field position within the record being scanned; reset whenever a record starts. Only an index
   * into {@link #predictedSlotByOrdinal} — nothing depends on it being right, because every guess is
   * verified against the name's own bytes.
   */
  private int fieldOrdinal;

  /**
   * The name-table slot the field at each position resolved to LAST time. A wrong prediction is a
   * failed byte compare and the hash path runs as before, so a document whose records do not share a
   * key order loses one compare per field and nothing else.
   */
  private final int[] predictedSlotByOrdinal = newPredictionTable();

  private static int[] newPredictionTable() {
    final int[] table = new int[PREDICTED_ORDINALS];
    Arrays.fill(table, -1);
    return table;
  }

  /** Field positions a record may have before predictions stop; beyond it the hash path serves. */
  private static final int PREDICTED_ORDINALS = 512;

  private byte[][] nameBytes = new byte[NAME_TABLE_CAPACITY][];
  private int[] nameIdBySlot = new int[NAME_TABLE_CAPACITY];
  private String[] nameStringById = new String[64];
  private int[] nameChunkOccurrencesById = new int[64];
  private int nameCount;
  private int nameMask = NAME_TABLE_CAPACITY - 1;

  /** Distinct-name slots touched THIS chunk, for occurrence-delta emission + reset. */
  private int[] touchedNameSlots = new int[256];
  private int touchedNameCount;

  /** The trie root = the top-level array's own step; the coordinator sets its PCR up front. */
  private final PathStep rootStep = new PathStep(null, -1, null);

  FusedSliceAndScan(final InputStream in, final int chunkByteBudget) {
    this(in, chunkByteBudget, false, Integer.MAX_VALUE);
  }

  /**
   * @param trackMemberNodes whether each chunk should carry its members' node counts — the only way a
   *        coordinator can name every record's root key by range arithmetic
   */
  FusedSliceAndScan(final InputStream in, final int chunkByteBudget, final boolean trackMemberNodes) {
    this(in, chunkByteBudget, trackMemberNodes, Integer.MAX_VALUE);
  }

  FusedSliceAndScan(final InputStream in, final int chunkByteBudget, final boolean trackMemberNodes,
      final int maxMembersPerChunk) {
    if (maxMembersPerChunk <= 0) {
      throw new IllegalArgumentException("maxMembersPerChunk must be positive: " + maxMembersPerChunk);
    }
    this.in = in;
    this.chunkByteBudget = Math.max(1024, chunkByteBudget);
    this.maxMembersPerChunk = maxMembersPerChunk;
    this.memberNodes = trackMemberNodes
        ? new LongArrayList(1024)
        : null;
  }

  PathStep rootStep() {
    return rootStep;
  }

  /**
   * Returns the slabs of the chunk being filled. The thread that scans calls this when it stops,
   * however it stops: a chunk handed out by {@link #nextChunk()} is its receiver's to release, the
   * one still being filled is nobody else's. No chunk can be scanned afterwards.
   */
  @Override
  public void close() {
    chunkBuffer.release();
  }

  /** Consumes leading whitespace and the top-level {@code '['}. */
  void consumeArrayOpen() throws IOException {
    while (true) {
      if (position == limit && !fill()) {
        throw new IllegalStateException("input ended before the top-level array opened");
      }
      final byte c = readBuffer[position];
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
        position++;
        segmentStart = position;
        continue;
      }
      if (c != '[') {
        throw new IllegalStateException("parallel import requires a top-level array, got byte " + c);
      }
      position++;
      segmentStart = position;
      return;
    }
  }

  /** The next member-aligned chunk with its metadata, or {@code null} at the array's end. */
  Chunk nextChunk() throws IOException {
    if (arrayClosed) {
      return null;
    }
    chunkLength = 0;
    nodes = 0;
    members = 0;
    lastMemberNodes = 0;
    memberStartNodes = 0;
    if (memberNodes != null) {
      memberNodes.clear();
    }
    depth = 0;
    contextStep[0] = rootStep;
    contextIsObject[0] = false;
    namePending = false;
    newSteps.clear();
    touchedSteps.clear();
    newNames.clear();

    while (true) {
      if (position == limit && !refillPreservingChunk()) {
        throw new IllegalStateException("input ended inside the top-level array (depth " + depth + ")");
      }
      final byte c = readBuffer[position];

      if (inString) {
        if (escaped) {
          escaped = false;
          position++;
          continue;
        }
        // Tight skip: race through the string body to the next quote or backslash — the bulk of
        // real-world bytes live inside string values, and this loop is the whole cost of passing
        // them (no decode, no copy beyond the segment flush).
        final byte[] buffer = readBuffer;
        int scanPosition = position;
        final int scanLimit = limit;
        while (scanPosition < scanLimit) {
          final byte b = buffer[scanPosition];
          if (b == '"' || b == '\\') {
            break;
          }
          scanPosition++;
        }
        position = scanPosition;
        if (position == limit) {
          continue; // refill via the loop head
        }
        if (buffer[position] == '\\') {
          escaped = true;
          position++;
          continue;
        }
        // Closing quote.
        inString = false;
        position++;
        if (stringIsName) {
          stringIsName = false;
          final int nameStart = nameStartInBuffer;
          if (nameStart >= 0) {
            // The whole name is still in the read buffer: resolve it there, on the heap array the
            // scan loop already has in cache, without touching the chunk.
            nameStartInBuffer = -1;
            resolveName(buffer, nameStart, position - 1 - nameStart);
          } else {
            resolveStraddlingName();
          }
        } else {
          nodes++;
          valueCompleted();
        }
        continue;
      }

      switch (c) {
        case ' ', '\t', '\n', '\r', ',', ':' -> position++;
        case '"' -> {
          inString = true;
          // Name iff directly inside an object and no name is pending yet.
          stringIsName = contextIsObject[depth] && !namePending;
          if (stringIsName) {
            nameStartInBuffer = position + 1;
          }
          position++;
        }
        case '{' -> {
          nodes++;
          if (namePending) {
            namePending = false;
            push(pendingFieldStep, true);
          } else {
            // Plain OBJECT is transparent for path anchoring.
            push(contextStep[depth], true);
          }
          position++;
        }
        case '[' -> {
          nodes++;
          if (namePending) {
            namePending = false;
            push(arrayStepUnder(pendingFieldStep), false);
          } else {
            push(arrayStepUnder(contextStep[depth]), false);
          }
          position++;
        }
        case '}' -> {
          if (depth == 0) {
            throw new IllegalStateException("unbalanced '}' at the top level");
          }
          depth--;
          position++;
          if (depth == 0) {
            memberCompleted();
            final Chunk chunk = maybeCloseChunk();
            if (chunk != null) {
              return chunk;
            }
          }
        }
        case ']' -> {
          if (depth == 0) {
            arrayClosed = true;
            flushSegmentUpTo(position);
            position++;
            segmentStart = position;
            return nodes > 0
                ? finishChunk(true)
                : null;
          }
          depth--;
          position++;
          if (depth == 0) {
            memberCompleted();
            final Chunk chunk = maybeCloseChunk();
            if (chunk != null) {
              return chunk;
            }
          }
        }
        default -> {
          // Number or literal: skip to its structural end.
          nodes++;
          if (namePending) {
            namePending = false;
          }
          position++;
          while (true) {
            if (position == limit && !refillPreservingChunk()) {
              throw new IllegalStateException("input ended inside a literal");
            }
            final byte v = readBuffer[position];
            if (v == ',' || v == '}' || v == ']' || v == ' ' || v == '\t' || v == '\n' || v == '\r') {
              break;
            }
            position++;
          }
          if (depth == 0) {
            memberCompleted();
            final Chunk chunk = maybeCloseChunk();
            if (chunk != null) {
              return chunk;
            }
          }
        }
      }
    }
  }

  private void valueCompleted() {
    if (namePending) {
      namePending = false;
    }
    if (depth == 0) {
      memberCompleted();
    }
  }

  private void memberCompleted() {
    members++;
    lastMemberNodes = nodes - memberStartNodes;
    memberStartNodes = nodes;
    if (memberNodes != null) {
      memberNodes.add(lastMemberNodes);
    }
  }

  /** Closes the chunk at this member boundary when the budget is met. */
  private Chunk maybeCloseChunk() throws IOException {
    flushSegmentUpTo(position);
    if (chunkLength < chunkByteBudget && members < maxMembersPerChunk) {
      return null;
    }
    final boolean isFinal = peekArrayCloses();
    return finishChunk(isFinal);
  }

  private boolean peekArrayCloses() throws IOException {
    while (true) {
      if (position == limit && !fill()) {
        throw new IllegalStateException("input ended inside the top-level array");
      }
      final byte c = readBuffer[position];
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',') {
        position++;
        segmentStart = position;
        continue;
      }
      if (c == ']') {
        arrayClosed = true;
        position++;
        segmentStart = position;
        return true;
      }
      return false;
    }
  }

  // ==== chunk buffer (bulk segment copies) =====================================================

  private void flushSegment() {
    flushSegmentUpTo(position);
  }

  private void flushSegmentUpTo(final int end) {
    final int len = end - segmentStart;
    if (len > 0) {
      chunkBuffer.append(readBuffer, segmentStart, len);
      chunkLength += len;
    }
    segmentStart = end;
  }

  private boolean fill() throws IOException {
    if (nameStartInBuffer >= 0) {
      // A field name is open and this refill replaces the bytes read of it so far. They are flushed
      // into the chunk below; remember where they start there (the bytes before them in the pending
      // segment come first).
      nameStartInChunk = chunkLength + (nameStartInBuffer - segmentStart);
      nameStartInBuffer = -1;
    }
    flushSegmentUpTo(limit);
    limit = in.read(readBuffer, 0, readBuffer.length);
    position = 0;
    segmentStart = 0;
    return limit > 0;
  }

  private boolean refillPreservingChunk() throws IOException {
    return fill();
  }

  // ==== name resolution ========================================================================

  /**
   * Resolves a name a refill cut in two: its first part went into the chunk with that refill, so the
   * rest is flushed after it and the name is read back from the chunk. At most one name per refill
   * takes this path, which is why it may allocate.
   */
  private void resolveStraddlingName() {
    flushSegment(); // through the closing quote
    final int start = nameStartInChunk;
    nameStartInChunk = -1;
    final int len = chunkLength - 1 - start;
    final byte[] name = new byte[len];
    chunkBuffer.copyTo(start, name, 0, len);
    resolveName(name, 0, len);
  }

  /** Name bytes live at {@code source[start .. start + len)} (quotes excluded). */
  private void resolveName(final byte[] source, final int start, final int len) {
    // PREDICT BEFORE HASHING. Records of a collection repeat their key set in the same order — that
    // is what makes them a collection — so the name resolved at this position in the previous record
    // is almost always this one. Verifying that guess is a length check and a byte compare over a
    // short name; missing it costs one extra compare before the hash path below. Hashing every
    // occurrence instead costs an fnv1a64 over the name's bytes for EVERY field of EVERY record:
    // profiled on a 100M-row import, 7.1 % of the single feeder thread the whole load waits on.
    final int ordinal = fieldOrdinal++;
    if (ordinal < predictedSlotByOrdinal.length) {
      final int predicted = predictedSlotByOrdinal[ordinal];
      if (predicted >= 0) {
        final byte[] existing = nameBytes[predicted];
        if (existing != null && matches(existing, source, start, len)) {
          useName(predicted);
          return;
        }
      }
    }
    long hash = 0xcbf29ce484222325L;
    for (int i = start, end = start + len; i < end; i++) {
      hash = (hash ^ source[i]) * 0x100000001b3L;
    }
    int slot = (int) hash & nameMask;
    while (true) {
      final byte[] existing = nameBytes[slot];
      if (existing == null) {
        insertName(slot, source, start, len);
        break;
      }
      if (matches(existing, source, start, len)) {
        break;
      }
      slot = (slot + 1) & nameMask;
    }
    if (ordinal < predictedSlotByOrdinal.length) {
      predictedSlotByOrdinal[ordinal] = slot; // what to guess at this position next time
    }
    useName(slot);
  }

  /** Everything resolveName does once it knows the slot, whether it guessed or hashed. */
  private void useName(final int slot) {
    final int nameId = nameIdBySlot[slot];
    if (nameChunkOccurrencesById[nameId]++ == 0) {
      rememberTouched(nameId);
    }
    pendingFieldStep = fieldStepUnder(contextStep[depth], nameId);
    namePending = true;
  }

  /** Get-or-create the named child step; occurrences tally per step for reference deltas. */
  private PathStep fieldStepUnder(final PathStep parent, final int nameId) {
    final Int2ObjectOpenHashMap<PathStep> children = parent.childrenByNameSlot();
    PathStep step = children.get(nameId);
    if (step == null) {
      step = new PathStep(parent, nameId, nameStringById[nameId]);
      children.put(nameId, step);
      newSteps.add(step);
    }
    tallyStep(step);
    return step;
  }

  private PathStep arrayStepUnder(final PathStep parent) {
    PathStep step = parent.arrayChild();
    if (step == null) {
      step = new PathStep(parent, -1, null);
      parent.setArrayChild(step);
      newSteps.add(step);
    }
    tallyStep(step);
    return step;
  }

  private void tallyStep(final PathStep step) {
    if (!step.touchedThisChunk) {
      step.touchedThisChunk = true;
      touchedSteps.add(step);
    }
    step.chunkOccurrences++;
  }

  private Chunk finishChunk(final boolean isFinal) {
    final List<int[]> tallies = new ArrayList<>(touchedNameCount);
    for (int i = 0; i < touchedNameCount; i++) {
      final int nameId = touchedNameSlots[i];
      tallies.add(new int[] {nameId, nameChunkOccurrencesById[nameId]});
    }
    final int[] stepOccurrences = new int[touchedSteps.size()];
    for (int i = 0; i < touchedSteps.size(); i++) {
      final PathStep step = touchedSteps.get(i);
      stepOccurrences[i] = step.chunkOccurrences;
    }
    // The chunk's slabs leave with it; the next chunk borrows its own as it fills.
    final ChunkBuffer out = chunkBuffer;
    final ChunkBuffer next = new ChunkBuffer();
    final Runnable hook = chunkHandoffHookForTesting;
    if (hook != null) {
      hook.run();
    }
    final long[] memberNodeCounts = memberNodes == null
        ? null
        : memberNodes.toLongArray();
    final Chunk result = new Chunk(out, chunkLength, isFinal, nodes, members, lastMemberNodes, List.copyOf(newSteps),
        List.copyOf(touchedSteps), stepOccurrences, List.copyOf(newNames), tallies, memberNodeCounts);
    for (int i = 0; i < touchedNameCount; i++) {
      nameChunkOccurrencesById[touchedNameSlots[i]] = 0;
    }
    touchedNameCount = 0;
    for (int i = 0; i < touchedSteps.size(); i++) {
      final PathStep step = touchedSteps.get(i);
      step.chunkOccurrences = 0;
      step.touchedThisChunk = false;
    }
    chunkBuffer = next;
    return result;
  }

  private static boolean matches(final byte[] existing, final byte[] source, final int start, final int len) {
    return existing.length == len && Arrays.equals(existing, 0, len, source, start, start + len);
  }

  private void insertName(final int slot, final byte[] source, final int start, final int len) {
    final byte[] copy = Arrays.copyOfRange(source, start, start + len);
    final String decoded = decodeName(copy);
    final int nameId = nameCount++;
    nameBytes[slot] = copy;
    nameIdBySlot[slot] = nameId;
    if (nameId == nameStringById.length) {
      nameStringById = Arrays.copyOf(nameStringById, nameStringById.length << 1);
      nameChunkOccurrencesById = Arrays.copyOf(nameChunkOccurrencesById, nameChunkOccurrencesById.length << 1);
    }
    nameStringById[nameId] = decoded;
    newNames.add(decoded);
    if (nameCount * 2 > nameMask) {
      growNameTable();
    }
  }

  private void rememberTouched(final int slot) {
    if (touchedNameCount == touchedNameSlots.length) {
      touchedNameSlots = Arrays.copyOf(touchedNameSlots, touchedNameSlots.length << 1);
    }
    touchedNameSlots[touchedNameCount++] = slot;
  }

  private void growNameTable() {
    final int newCapacity = (nameMask + 1) << 1;
    final byte[][] oldBytes = nameBytes;
    final int[] oldIds = nameIdBySlot;
    nameBytes = new byte[newCapacity][];
    nameIdBySlot = new int[newCapacity];
    nameMask = newCapacity - 1;
    for (int i = 0; i < oldBytes.length; i++) {
      final byte[] name = oldBytes[i];
      if (name == null) {
        continue;
      }
      long hash = 0xcbf29ce484222325L;
      for (final byte b : name) {
        hash = (hash ^ b) * 0x100000001b3L;
      }
      int slot = (int) hash & nameMask;
      while (nameBytes[slot] != null) {
        slot = (slot + 1) & nameMask;
      }
      nameBytes[slot] = name;
      nameIdBySlot[slot] = oldIds[i];
    }
  }

  private void push(final PathStep step, final boolean isObject) {
    if (isObject) {
      // A fresh object restarts the field positions the prediction table is keyed by. Nested objects
      // reset it mid-record, which costs accuracy and never correctness: a prediction is only ever a
      // guess that the name's own bytes then confirm.
      fieldOrdinal = 0;
    }
    depth++;
    if (depth == contextStep.length) {
      contextStep = Arrays.copyOf(contextStep, contextStep.length << 1);
      contextIsObject = Arrays.copyOf(contextIsObject, contextIsObject.length << 1);
    }
    contextStep[depth] = step;
    contextIsObject[depth] = isObject;
  }

  private static String decodeName(final byte[] raw) {
    final String utf8 = new String(raw, StandardCharsets.UTF_8);
    if (utf8.indexOf('\\') < 0) {
      return utf8;
    }
    final StringBuilder out = new StringBuilder(utf8.length());
    for (int i = 0; i < utf8.length(); i++) {
      char c = utf8.charAt(i);
      if (c == '\\') {
        i++;
        c = utf8.charAt(i);
        switch (c) {
          case '"', '\\', '/' -> out.append(c);
          case 'b' -> out.append('\b');
          case 'f' -> out.append('\f');
          case 'n' -> out.append('\n');
          case 'r' -> out.append('\r');
          case 't' -> out.append('\t');
          case 'u' -> {
            out.append((char) Integer.parseInt(utf8.substring(i + 1, i + 5), 16));
            i += 4;
          }
          default -> throw new IllegalStateException("invalid escape \\" + c + " in field name");
        }
      } else {
        out.append(c);
      }
    }
    return out.toString();
  }
}
