/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.PageContainer;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * HOT-backed persistent storage for projection-index leaf payloads, with
 * <b>sub-leaf chunking</b> so updates merge rather than rewrite the whole
 * serialised leaf.
 *
 * <h2>Known scale limit</h2>
 *
 * The underlying {@link HOTLeafPage} / {@link io.sirix.access.trx.page.HOTTrieReader}
 * traversal misroutes for dense-common-prefix keys after ~2 levels of
 * split. Our composite keys share 6-7 bytes of prefix, so the storage is
 * reliable up to roughly the first split (~100 entries including chunks)
 * and starts missing entries beyond that. Tracked as task #57.
 *
 * <h2>Architectural path forward</h2>
 *
 * The correct design — also what the user proposed when reviewing this
 * code — is to lift chunk bytes OUT of HOT slot values and into their
 * OWN pages, keeping HOT slots tiny:
 *
 * <pre>
 *   HOTLeafPage  ↓ one slot per logical leaf
 *     slot value = ChunkDirectory { chunk_count, (chunkIdx → PageReference)[] }
 *                                                               ↓
 *                                              Separate ProjectionChunkPage
 *                                              (or OverflowPage) per chunk.
 *                                              Each is CoW-versioned at the
 *                                              standard Sirix page granularity
 *                                              — exactly matches SLIDING_SNAPSHOT.
 * </pre>
 *
 * This moves the HOT trie to carrying ~16-byte values instead of 4 KB
 * values, so one HOTLeafPage fits thousands of entries before splitting,
 * pushing the scale limit well past 100 M records. Requires: new page
 * kind + {@code combineRecordPages} variant + ChunkDirectory value
 * format + NodeKind routing — substantial cross-cutting work tracked as
 * task #57's preferred resolution.
 *
 * <h2>Chunked-values contract (current, inline-value version)</h2>
 *
 * Each logical projection leaf ({@link ProjectionIndexLeafPage#serialize()})
 * is split into fixed-size {@link #CHUNK_SIZE}-byte chunks and stored as
 * multiple HOT entries under a <em>composite key</em>:
 *
 * <pre>
 *   rawKey = (leafIndex &lt;&lt; 8) | (chunkIdx &amp; 0xFF)
 *   hotKey = PathKeySerializer.serialize(rawKey)   // sign-flipped 8-byte BE
 * </pre>
 *
 * With the sign-flip encoding, unsigned byte comparison preserves
 * {@code (leafIndex, chunkIdx)} tuple ordering — chunks of the same leaf
 * stay contiguous, and leaves appear in ascending {@code leafIndex} order
 * inside a range scan.
 *
 * <p>Because each chunk is a separate HOT slot, Sirix's HOTLeafPage CoW
 * merge shares unchanged chunk slots across revisions: modifying a single
 * row in column {@code c} of leaf {@code L} rewrites only the chunk(s)
 * whose bytes actually changed, not the full ~20 KB leaf. This aligns the
 * projection with the SLIDING_SNAPSHOT contract the framework promises for
 * CAS / PATH / NAME indexes. The {@code ProjectionIndexLeafPage} javadoc
 * notes this as required before GA.
 *
 * <h2>Caller-facing API</h2>
 *
 * {@link #put(long, byte[])} takes a full serialised leaf and handles the
 * chunk split internally. {@link #get(long)} reassembles the chunks.
 * {@link #readAll(StorageEngineReader, int)} walks all leaves in ascending
 * {@code leafIndex} order, concatenating each leaf's chunks in-order.
 * None of the call sites in {@code ScaleBenchProjectionSetup} or the
 * executor need to know the value is internally chunked.
 */
public final class ProjectionIndexHOTStorage extends AbstractHOTIndexWriter<Long> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionIndexHOTStorage.class);

  /** Sentinel for explicit chunk deletion. HOTLeafPage has no per-entry
   *  delete, so we tombstone with an empty payload. */
  private static final byte[] EMPTY_CHUNK = new byte[0];

  /**
   * Enables the depth-2 parallel hydrate path in {@link #readAll(StorageEngineReader, int)}.
   * Default {@code true} — set {@code -Dsirix.projection.hydrate.parallel=false}
   * to force the serial path (iter#03c rollback switch). At runtime the flag is
   * read once per JVM; no per-call toggling.
   *
   * <p>When the measured depth-2 fan-out is below {@link #DEPTH2_MIN_FANOUT},
   * the parallel path falls back to the serial cursor internally so the flag
   * is still a safe "on".
   */
  private static final boolean PARALLEL_HYDRATE =
      Boolean.parseBoolean(System.getProperty("sirix.projection.hydrate.parallel", "true"));

  /**
   * Minimum number of depth-2 sub-roots required before the parallel hydrate
   * path fires. Fewer sub-roots = fork-join submit overhead swamps the work.
   * Below this threshold, fall back to the serial cursor. 8 is the gate
   * defined in the iter#03c analysis (matches the 20-core target: 8 sub-roots
   * keeps at least 40% of workers busy).
   */
  private static final int DEPTH2_MIN_FANOUT =
      Math.max(2, Integer.getInteger("sirix.projection.hydrate.depth2MinFanout", 8));

  /**
   * One-shot log gate: we emit the depth-2 fan-out measurement exactly once
   * per JVM so repeated bench runs don't flood logs. Subsequent hydrate calls
   * silently reuse the parallel path based on the flag.
   */
  private static final AtomicBoolean FANOUT_LOGGED = new AtomicBoolean(false);

  /**
   * Fixed chunk size in bytes. Override with
   * {@code -Dsirix.projection.chunkSize=N} — useful in regression tests
   * that exercise multi-chunk behaviour explicitly. 4 KB is the tuned
   * default: one 20 KB leaf → 5 chunks, which is the granularity at
   * which HOTLeafPage CoW shares unchanged chunk slots across revisions.
   */
  public static final int CHUNK_SIZE =
      Integer.parseInt(System.getProperty("sirix.projection.chunkSize",
          String.valueOf(4096)));

  /**
   * Max chunks per leaf. Enforced by the composite-key encoding: {@code
   * chunkIdx} occupies 8 bits. At {@code CHUNK_SIZE=4096} this caps a
   * single leaf at {@code 256 * 4096 = 1 MB} serialised — two orders of
   * magnitude above the typical 20 KB leaf. Exceeding the cap means the
   * caller produced an unreasonably large leaf and should reconsider its
   * row capacity, not an encoding limitation we can raise on the fly.
   */
  public static final int MAX_CHUNKS_PER_LEAF = 256;

  /** 8-byte scratch for encoding composite (leafIndex, chunkIdx) keys. */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[8]);

  /**
   * Per-thread off-heap scratch for
   * {@link #putFromSegment(long, MemorySegment, long, int)} — holds one
   * serialised projection leaf between the builder emitting it and the
   * HOT chunks consuming it. Allocated via an {@code Arena.ofShared}
   * associated with the thread; grown on demand but never shrunk. At
   * 256 KB initial (= {@link #MAX_CHUNKS_PER_LEAF} × {@link #CHUNK_SIZE})
   * one allocation covers the largest possible leaf.
   */
  private static final ThreadLocal<ScratchSegment> SCRATCH_SEGMENT =
      ThreadLocal.withInitial(() -> new ScratchSegment(256 * 1024));

  /** Holder for a thread-local growable off-heap scratch segment. */
  private static final class ScratchSegment {
    private Arena arena;
    private MemorySegment segment;
    private long capacity;

    ScratchSegment(final long initialCapacity) {
      this.arena = Arena.ofShared();
      this.segment = arena.allocate(initialCapacity);
      this.capacity = initialCapacity;
    }

    /** Return a segment with at least {@code needed} bytes of capacity. */
    MemorySegment ensureCapacity(final long needed) {
      if (needed <= capacity) return segment;
      // Grow 2× or the requested size, whichever is larger. Release the
      // old arena so off-heap memory is returned to the OS — we don't
      // hold onto unbounded scratch forever.
      final Arena old = arena;
      long newCap = capacity * 2L;
      while (newCap < needed) newCap *= 2L;
      arena = Arena.ofShared();
      segment = arena.allocate(newCap);
      capacity = newCap;
      old.close();
      return segment;
    }
  }

  private final PathKeySerializer keySerializer = PathKeySerializer.INSTANCE;

  public ProjectionIndexHOTStorage(final StorageEngineWriter storageEngineWriter, final int indexNumber) {
    super(storageEngineWriter, IndexType.PROJECTION, indexNumber);
    initializeProjectionIndex();
  }

  private void initializeProjectionIndex() {
    final RevisionRootPage revisionRootPage = storageEngineWriter.getActualRevisionRootPage();
    final PageReference projPageRef = revisionRootPage.getProjectionIndexPageReference();

    final PageContainer projContainer = storageEngineWriter.getLog().get(projPageRef);
    final ProjectionIndexPage projPage;
    if (projContainer != null && projContainer.getModified() instanceof ProjectionIndexPage modifiedProj) {
      projPage = modifiedProj;
    } else {
      projPage = storageEngineWriter.getProjectionIndexPage(revisionRootPage);
      storageEngineWriter.appendLogRecord(projPageRef, PageContainer.getInstance(projPage, projPage));
    }

    final PageReference existingRef = projPage.getOrCreateReference(indexNumber);
    final boolean exists = existingRef != null
        && (existingRef.getKey() != Constants.NULL_ID_LONG
            || existingRef.getLogKey() != Constants.NULL_ID_INT
            || existingRef.getPage() != null);
    if (!exists) {
      projPage.createProjectionIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
    }
    rootReference = projPage.getOrCreateReference(indexNumber);
  }

  /**
   * Insert or update the leaf at {@code leafIndex}. The payload is split
   * into {@link #CHUNK_SIZE}-byte chunks; each chunk becomes its own HOT
   * entry. A re-put overwrites every chunk — for true merge semantics on
   * partial updates, the caller should use {@link #putChunk} directly to
   * rewrite only the chunks that actually changed.
   *
   * <p>If the new payload is <em>shorter</em> than the previous one, stale
   * high-index chunks are explicitly cleared (written with an empty byte[])
   * so a later {@code get} does not concatenate leftover bytes from the
   * prior version. The HOT trie has no per-entry delete today — zero-length
   * chunks are used as tombstones.
   */
  public void put(final long leafIndex, final byte[] payload) {
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    if (payload == null) {
      throw new IllegalArgumentException("payload must not be null");
    }
    final int chunkCount = chunkCount(payload.length);
    if (chunkCount > MAX_CHUNKS_PER_LEAF) {
      throw new IllegalArgumentException("payload " + payload.length
          + " bytes exceeds MAX_CHUNKS_PER_LEAF=" + MAX_CHUNKS_PER_LEAF + " at CHUNK_SIZE=" + CHUNK_SIZE);
    }

    // Probe incrementally instead of pre-scanning all 256 slots. On a fresh
    // build every leaf is new → one probe at chunkCount tells us no tail
    // needs tombstoning. Cost: +1 HOT lookup per leaf instead of 256.
    final boolean needsTailCheck = getChunk(leafIndex, chunkCount) != null;

    // Correct path: re-navigate per chunk. The HOT trie's split topology
    // can disperse chunks with the same leaf-index prefix across
    // different HOTLeafPages (empirically observed at ~200 leaves × 3
    // chunks), so a single-leaf write cache is unsafe. Navigation is
    // cheap (same cached path in HOTTrieWriter) and the per-chunk value
    // consumption is still zero-copy via {@code putRange(byte[], byte[],
    // int, int)}.
    final byte[] keyBuf = KEY_BUFFER.get();
    final int keyLen = keyBuf.length;
    for (int i = 0; i < chunkCount; i++) {
      final int off = i * CHUNK_SIZE;
      final int len = Math.min(CHUNK_SIZE, payload.length - off);
      encodeCompositeKey(leafIndex, i, keyBuf);
      final LeafNavigationResult navResult = getLeafWithPath(rootReference, keyBuf, keyLen);
      writeChunkRangeToLeafOrSplit(navResult.leaf(), navResult, keyBuf, keyLen, payload, off, len);
    }

    if (needsTailCheck) {
      for (int i = chunkCount; i < MAX_CHUNKS_PER_LEAF; i++) {
        if (getChunk(leafIndex, i) == null) break;
        putChunk(leafIndex, i, EMPTY_CHUNK);
      }
    }
  }

  /**
   * HFT zero-allocation write path for the build pipeline: serialised
   * leaf payload lives in an off-heap {@code MemorySegment} owned by
   * the caller (typically the scratch returned by {@link #scratchSegment()}),
   * sliced into HOT chunks via {@link HOTLeafPage#putRange(byte[], MemorySegment, long, int)}.
   * No heap allocation per chunk, no intermediate {@code byte[]}.
   *
   * <p>Same contract as {@link #put(long, byte[])} — splits the payload
   * into {@link #CHUNK_SIZE}-byte chunks, tombstones stale tail chunks
   * from a prior revision if the new payload is shorter. Difference is
   * pure hot-path: the value bytes never hit the Java heap.
   */
  public void putFromSegment(final long leafIndex, final MemorySegment src,
      final long srcOff, final int srcLen) {
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    if (src == null) {
      throw new IllegalArgumentException("src must not be null");
    }
    if (srcOff < 0 || srcLen < 0 || srcOff + srcLen > src.byteSize()) {
      throw new IndexOutOfBoundsException(
          "srcOff=" + srcOff + " srcLen=" + srcLen + " segBytes=" + src.byteSize());
    }
    final int chunkCount = chunkCount(srcLen);
    if (chunkCount > MAX_CHUNKS_PER_LEAF) {
      throw new IllegalArgumentException("payload " + srcLen
          + " bytes exceeds MAX_CHUNKS_PER_LEAF=" + MAX_CHUNKS_PER_LEAF + " at CHUNK_SIZE=" + CHUNK_SIZE);
    }

    final boolean needsTailCheck = getChunk(leafIndex, chunkCount) != null;

    final byte[] keyBuf = KEY_BUFFER.get();
    final int keyLen = keyBuf.length;
    for (int i = 0; i < chunkCount; i++) {
      final long off = srcOff + (long) i * CHUNK_SIZE;
      final int len = Math.min(CHUNK_SIZE, srcLen - i * CHUNK_SIZE);
      encodeCompositeKey(leafIndex, i, keyBuf);
      final LeafNavigationResult navResult = getLeafWithPath(rootReference, keyBuf, keyLen);
      writeChunkSegmentToLeafOrSplit(navResult.leaf(), navResult, keyBuf, keyLen, src, off, len);
    }

    if (needsTailCheck) {
      for (int i = chunkCount; i < MAX_CHUNKS_PER_LEAF; i++) {
        if (getChunk(leafIndex, i) == null) break;
        putChunk(leafIndex, i, EMPTY_CHUNK);
      }
    }
  }

  /** @return the thread-local off-heap scratch segment (grown to at least {@code needed} bytes). */
  public static MemorySegment scratchSegment(final long needed) {
    return SCRATCH_SEGMENT.get().ensureCapacity(needed);
  }

  /** MemorySegment-sourced variant of {@link #writeChunkRangeToLeafOrSplit}. */
  private boolean writeChunkSegmentToLeafOrSplit(final HOTLeafPage currentLeaf,
      final LeafNavigationResult navResult, final byte[] keyBuf, final int keyLen,
      final MemorySegment src, final long srcOff, final int srcLen) {
    if (currentLeaf.putRange(keyBuf, src, srcOff, srcLen)) {
      return true;
    }
    final int idx = currentLeaf.findEntry(keyBuf);
    if (idx >= 0) {
      if (currentLeaf.updateValueRange(idx, src, srcOff, srcLen)) {
        return true;
      }
      final byte[] sized = new byte[srcLen];
      MemorySegment.copy(src, ValueLayout.JAVA_BYTE, srcOff, sized, 0, srcLen);
      currentLeaf.updateValue(idx, sized);
      return true;
    }
    // Split path — single right-sized alloc at a low-frequency event.
    final byte[] sized = new byte[srcLen];
    MemorySegment.copy(src, ValueLayout.JAVA_BYTE, srcOff, sized, 0, srcLen);
    final boolean inserted = trieWriter.handleLeafSplitAndInsert(
        storageEngineWriter, storageEngineWriter.getLog(), currentLeaf, navResult.leafRef(),
        rootReference, navResult.pathNodes(), navResult.pathRefs(), navResult.pathChildIndices(),
        navResult.pathDepth(), keyBuf, keyLen, sized, srcLen);
    markIndexPageDirty();
    if (!inserted) {
      throw new SirixIOException("Projection HOT chunk insert failed after split");
    }
    return false;
  }

  /**
   * Hot-path zero-alloc chunk writer: consumes the chunk value directly
   * from {@code payload[valueOff..valueOff+valueLen)} via
   * {@link HOTLeafPage#putRange} / {@link HOTLeafPage#updateValueRange}
   * — no intermediate {@code byte[]}.
   *
   * <p>Returns {@code true} iff the chunk landed on {@code currentLeaf}
   * (the caller can stay on the same leaf for the next chunk). Returns
   * {@code false} if a split was required — the trie writer has already
   * inserted the chunk into the post-split leaf, and the caller must
   * re-navigate for the following key.
   */
  private boolean writeChunkRangeToLeafOrSplit(final HOTLeafPage currentLeaf,
      final LeafNavigationResult navResult, final byte[] keyBuf, final int keyLen,
      final byte[] payload, final int valueOff, final int valueLen) {
    if (currentLeaf.putRange(keyBuf, payload, valueOff, valueLen)) {
      return true;
    }
    final int idx = currentLeaf.findEntry(keyBuf);
    if (idx >= 0) {
      // Existing entry. Prefer the in-place updateValueRange fast path;
      // if size changed, fall back to the copying update.
      if (currentLeaf.updateValueRange(idx, payload, valueOff, valueLen)) {
        return true;
      }
      final byte[] sized = new byte[valueLen];
      System.arraycopy(payload, valueOff, sized, 0, valueLen);
      currentLeaf.updateValue(idx, sized);
      return true;
    }
    // Page full — split via the trie writer. handleLeafSplitAndInsert
    // requires a byte[] value; we pay a single right-sized alloc here,
    // NOT on the steady-state path. Split is O(log N) in build frequency.
    final byte[] sized = new byte[valueLen];
    System.arraycopy(payload, valueOff, sized, 0, valueLen);
    final boolean inserted = trieWriter.handleLeafSplitAndInsert(
        storageEngineWriter, storageEngineWriter.getLog(), currentLeaf, navResult.leafRef(),
        rootReference, navResult.pathNodes(), navResult.pathRefs(), navResult.pathChildIndices(),
        navResult.pathDepth(), keyBuf, keyLen, sized, valueLen);
    markIndexPageDirty();
    if (!inserted) {
      throw new SirixIOException("Projection HOT chunk insert failed after split");
    }
    return false;
  }

  /**
   * Write a single chunk. Intended for incremental-update callers that know
   * which chunk changed — the listener or an in-place row append.
   *
   * @param leafIndex the logical leaf this chunk belongs to
   * @param chunkIdx  0-based chunk index (0 is the head chunk)
   * @param chunk     chunk bytes (may be up to {@link #CHUNK_SIZE});
   *                  zero-length acts as a tombstone for the slot
   */
  public void putChunk(final long leafIndex, final int chunkIdx, final byte[] chunk) {
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    if (chunkIdx < 0 || chunkIdx >= MAX_CHUNKS_PER_LEAF) {
      throw new IllegalArgumentException("chunkIdx must be in [0, " + MAX_CHUNKS_PER_LEAF + "): " + chunkIdx);
    }
    if (chunk == null) {
      throw new IllegalArgumentException("chunk must not be null (use empty array to tombstone)");
    }
    if (chunk.length > CHUNK_SIZE) {
      throw new IllegalArgumentException("chunk " + chunk.length + " > CHUNK_SIZE=" + CHUNK_SIZE);
    }

    final byte[] keyBuf = KEY_BUFFER.get();
    final int keyLen = encodeCompositeKey(leafIndex, chunkIdx, keyBuf);

    final LeafNavigationResult navResult = getLeafWithPath(rootReference, keyBuf, keyLen);
    final HOTLeafPage leaf = navResult.leaf();

    if (leaf.put(keyBuf, chunk)) {
      return;
    }

    final int idx = leaf.findEntry(keyBuf);
    if (idx >= 0) {
      leaf.updateValue(idx, chunk);
      return;
    }

    // Page full → split + insert, same path CAS/PATH use.
    final boolean inserted = trieWriter.handleLeafSplitAndInsert(
        storageEngineWriter, storageEngineWriter.getLog(), leaf, navResult.leafRef(),
        rootReference, navResult.pathNodes(), navResult.pathRefs(), navResult.pathChildIndices(),
        navResult.pathDepth(), keyBuf, keyLen, chunk, chunk.length);
    markIndexPageDirty();

    if (!inserted) {
      throw new SirixIOException("Projection HOT chunk insert failed after split for leafIndex=" + leafIndex
          + " chunkIdx=" + chunkIdx + " (" + chunk.length + " bytes, indexNumber=" + indexNumber + ")");
    }
  }

  /**
   * Read a single chunk. Returns {@code null} if the slot is absent;
   * returns an empty array if the slot was explicitly tombstoned by a
   * shrinking {@link #put}.
   */
  public @Nullable byte[] getChunk(final long leafIndex, final int chunkIdx) {
    final byte[] keyBuf = KEY_BUFFER.get();
    encodeCompositeKey(leafIndex, chunkIdx, keyBuf);
    final HOTLeafPage leaf = getLeafForRead(keyBuf);
    if (leaf == null) return null;
    final int idx = leaf.findEntry(keyBuf);
    if (idx < 0) return null;
    return leaf.getValue(idx);
  }

  /**
   * Zero-copy single-chunk read: returns a {@link MemorySegment} slice
   * backed by the HOT leaf's off-heap slot memory. No byte-array copy,
   * no heap allocation. Callers must not read past the slice's byteSize.
   *
   * <p>Lifetime: the returned segment shares the HOT leaf page's scope —
   * valid as long as the leaf stays resident in the read-only trx's
   * cache. Typical scan kernels hold the segment only for the duration
   * of one kernel invocation, well within that window.
   */
  public @Nullable MemorySegment getChunkSlice(final long leafIndex, final int chunkIdx) {
    final byte[] keyBuf = KEY_BUFFER.get();
    encodeCompositeKey(leafIndex, chunkIdx, keyBuf);
    final HOTLeafPage leaf = getLeafForRead(keyBuf);
    if (leaf == null) return null;
    final int idx = leaf.findEntry(keyBuf);
    if (idx < 0) return null;
    return leaf.getValueSlice(idx);
  }

  /**
   * Reassemble all chunks for {@code leafIndex} into a single logical
   * payload. Returns {@code null} if no chunks exist (the leaf has never
   * been written or has been fully tombstoned).
   *
   * <p>HFT path: single-pass accumulation into a geometrically-grown
   * heap buffer, re-navigating the HOT trie per chunk. The re-navigation
   * is cheap — consecutive chunk keys share 7 bytes of prefix, so the
   * navigator stays on the same indirect-page path via cursor state —
   * and keeps us correct when the HOT split topology disperses chunks
   * of one leaf across multiple HOT leaf pages.
   */
  public @Nullable byte[] get(final long leafIndex) {
    // HFT-grade read path:
    //  (1) encode chunk-0 key once; mutate only byte 7 per chunk (cheap).
    //  (2) call the underlying leaf/findEntry chain directly (avoids a
    //      redundant KEY_BUFFER.get() per chunk in getChunkSlice).
    //  (3) break early if a partial chunk (n < CHUNK_SIZE) is seen — by
    //      construction the last chunk is the only partial one, so no
    //      further lookup (or tombstone probe) is needed. For values
    //      ≤ CHUNK_SIZE this halves the number of trie descents.
    //  (4) size the output buffer exactly (not rounded up) when the first
    //      chunk is partial — avoids the trailing Arrays.copyOf.
    final byte[] keyBuf = KEY_BUFFER.get();
    encodeCompositeKey(leafIndex, 0, keyBuf);

    byte[] buf = null;
    int len = 0;
    for (int i = 0; i < MAX_CHUNKS_PER_LEAF; i++) {
      keyBuf[7] = (byte) i; // low byte of composite; chunk 0..255 fits
      final HOTLeafPage leaf = getLeafForRead(keyBuf);
      if (leaf == null) break;
      final int idx = leaf.findEntry(keyBuf);
      if (idx < 0) break;
      final MemorySegment slice = leaf.getValueSlice(idx);
      if (slice == null) break;
      final int n = (int) slice.byteSize();
      if (n == 0) break; // tombstone
      if (buf == null) {
        // First chunk: if it's partial, it's also the only chunk — size
        // exactly. Otherwise pre-allocate CHUNK_SIZE.
        buf = new byte[n < CHUNK_SIZE ? n : CHUNK_SIZE];
      } else if (len + n > buf.length) {
        int newCap = buf.length * 2;
        while (newCap < len + n) newCap *= 2;
        buf = Arrays.copyOf(buf, newCap);
      }
      MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, buf, len, n);
      len += n;
      if (n < CHUNK_SIZE) break; // partial chunk = final chunk
    }
    if (buf == null) return null;
    return len == buf.length ? buf : Arrays.copyOf(buf, len);
  }

  /**
   * Parallel multi-leaf read — amortizes page-load I/O by dispatching
   * chunk fetches across the common ForkJoinPool. Intended for cold
   * starts where many leaves need to be materialised before a scan.
   *
   * <p>Concurrency model: each task owns one {@code leafIndex} and
   * single-walks the HOT trie for all chunks of that leaf. Leaves are
   * independent — no shared mutable state — so the parallel speedup is
   * pagination-bound: ~N-thread speedup as long as pages aren't already
   * in the buffer manager.
   *
   * <p>Returns the per-leaf payloads in the same order as {@code leafIndexes}.
   * {@code null} entries indicate an absent/fully-tombstoned leaf.
   */
  public byte[][] getMany(final long[] leafIndexes) {
    if (leafIndexes == null) {
      throw new IllegalArgumentException("leafIndexes must not be null");
    }
    final int n = leafIndexes.length;
    final byte[][] out = new byte[n][];
    if (n == 0) return out;
    if (n == 1) {
      out[0] = this.get(leafIndexes[0]);
      return out;
    }
    // Tiny batches: the FJP submission overhead isn't worth splitting.
    if (n < 8) {
      for (int i = 0; i < n; i++) out[i] = this.get(leafIndexes[i]);
      return out;
    }
    final ProjectionIndexHOTStorage self = this;
    ForkJoinPool.commonPool().invoke(
        new RecursiveAction() {
          @Override
          protected void compute() {
            final int workers = Math.min(n, Runtime.getRuntime().availableProcessors());
            final int chunk = (n + workers - 1) / workers;
            final RecursiveAction[] subs =
                new RecursiveAction[workers];
            for (int w = 0; w < workers; w++) {
              final int lo = w * chunk;
              final int hi = Math.min(n, lo + chunk);
              subs[w] = new RecursiveAction() {
                @Override
                protected void compute() {
                  for (int i = lo; i < hi; i++) out[i] = self.get(leafIndexes[i]);
                }
              };
            }
            invokeAll(subs);
          }
        });
    return out;
  }

  /**
   * Parallel variant of {@link #readAll(StorageEngineReader, int)} — walks
   * the cursor on the calling thread to collect leaf indexes + per-leaf
   * payload lengths (same first pass), then dispatches the byte[] assembly
   * across the common pool via {@link #getMany}. The assembly stage
   * dominates cost at large scales where most leaves are cold, so
   * parallelizing it gives near-linear speedup.
   */
  public static List<byte[]> readAllParallel(final ProjectionIndexHOTStorage storage,
      final StorageEngineReader reader, final int indexNumber) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) return Collections.emptyList();

    // Probe-based leaf discovery (same as {@link #readAll}): correct at
    // arbitrary scale, ~1 µs per probe warm. Then dispatch payload
    // assembly across the common pool via {@link #getMany}.
    final int GAP_EXIT = 16;
    final it.unimi.dsi.fastutil.longs.LongArrayList present = new it.unimi.dsi.fastutil.longs.LongArrayList();
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = KEY_BUFFER.get();
      int gap = 0;
      for (long leafIndex = 0; gap < GAP_EXIT; leafIndex++) {
        encodeCompositeKey(leafIndex, 0, keyBuf);
        final MemorySegment slice = trieReader.get(rootRef, keyBuf);
        if (slice == null || slice.byteSize() == 0) {
          gap++;
          continue;
        }
        gap = 0;
        present.add(leafIndex);
      }
    }
    final long[] idx = present.toLongArray();
    final byte[][] payloads = storage.getMany(idx);
    final ArrayList<byte[]> out = new ArrayList<>(payloads.length);
    for (final byte[] p : payloads) {
      if (p != null) out.add(p);
    }
    return out;
  }

  /**
   * Zero-copy cursor-style read: invokes {@code consumer} once per chunk
   * of {@code leafIndex} with an off-heap slice view. No heap allocation
   * on the read path at all. {@code consumer} must not retain the slice
   * past the call.
   *
   * <p>Intended for the scan kernel: it can {@code MemorySegment.copy}
   * chunk bytes straight into its own scratch / SIMD input buffer, or
   * iterate column bytes in place via layouts. Returns the number of
   * chunks delivered to the consumer.
   */
  public int forEachChunk(final long leafIndex, final Consumer<MemorySegment> consumer) {
    int delivered = 0;
    for (int i = 0; i < MAX_CHUNKS_PER_LEAF; i++) {
      final MemorySegment slice = getChunkSlice(leafIndex, i);
      if (slice == null) break;
      if (slice.byteSize() == 0) break; // tombstone → end
      consumer.accept(slice);
      delivered++;
    }
    return delivered;
  }

  @Override
  protected byte[] getKeyBuffer() {
    return KEY_BUFFER.get();
  }

  @Override
  protected void setKeyBuffer(final byte[] newBuffer) {
    KEY_BUFFER.set(newBuffer);
  }

  @Override
  protected int serializeKey(final Long key, final byte[] buffer, final int offset) {
    // Key passed here is the already-encoded composite key — unused
    // AbstractHOTIndexWriter hook. Required by the abstract parent for
    // the generic {@code index(K, ...)} path we don't expose.
    return keySerializer.serialize(key, buffer, offset);
  }

  /** Returns the number of CHUNK_SIZE chunks needed to store {@code payloadLen} bytes. */
  private static int chunkCount(final int payloadLen) {
    if (payloadLen < 0) {
      throw new IllegalArgumentException("payloadLen must be >= 0: " + payloadLen);
    }
    return payloadLen == 0 ? 1 : (payloadLen + CHUNK_SIZE - 1) / CHUNK_SIZE;
  }

  /**
   * Count contiguous chunks for {@code leafIndex}. Used by the tail-
   * tombstone path when {@link #put} shrinks a payload, and by the
   * {@link #chunkCountOf} diagnostic accessor. Not on the steady-state
   * put hot path — that uses a single probe at {@code chunkCount}.
   */
  private int countExistingChunks(final long leafIndex) {
    for (int i = 0; i < MAX_CHUNKS_PER_LEAF; i++) {
      if (getChunk(leafIndex, i) == null) return i;
    }
    return MAX_CHUNKS_PER_LEAF;
  }

  /**
   * Encode {@code (leafIndex, chunkIdx)} into the 8-byte composite key
   * used by the HOT trie. Packs {@code leafIndex} into the top 56 bits
   * (with sign-flipping for signed-long order preservation) and
   * {@code chunkIdx} into the low 8 bits.
   */
  public static int encodeCompositeKey(final long leafIndex, final int chunkIdx, final byte[] dest) {
    final long composite = (leafIndex << 8) | (chunkIdx & 0xFFL);
    return PathKeySerializer.INSTANCE.serialize(composite, dest, 0);
  }

  /** Public standalone form for the reader side. */
  public static byte[] encodeCompositeKey(final long leafIndex, final int chunkIdx) {
    final byte[] out = new byte[8];
    encodeCompositeKey(leafIndex, chunkIdx, out);
    return out;
  }

  /**
   * Decode a composite key back to {@code (leafIndex, chunkIdx)}. Returned
   * as a two-element long[] to avoid allocating a record — this method
   * sits on the scan warm-up path.
   */
  public static long[] decodeCompositeKey(final byte[] keyBytes) {
    final long composite = PathKeySerializer.INSTANCE.deserialize(keyBytes, 0, keyBytes.length);
    return new long[] { composite >> 8, composite & 0xFF };
  }

  /**
   * Root reference of the projection sub-tree for {@code indexNumber} under
   * the given reader's current revision, or {@code null} if no index is
   * installed.
   */
  public static @Nullable PageReference rootReference(final StorageEngineReader reader, final int indexNumber) {
    final RevisionRootPage rrp = reader.getActualRevisionRootPage();
    final ProjectionIndexPage projPage = reader.getProjectionIndexPage(rrp);
    if (projPage == null) return null;
    final PageReference ref = projPage.getOrCreateReference(indexNumber);
    if (ref == null) return null;
    if (ref.getKey() == Constants.NULL_ID_LONG && ref.getLogKey() == Constants.NULL_ID_INT && ref.getPage() == null) {
      return null;
    }
    return ref;
  }

  /**
   * Walk every persisted leaf in ascending {@code leafIndex} order,
   * concatenating each leaf's chunks in-order. Empty-chunk tombstones
   * terminate a leaf's payload (anything after is treated as not-present).
   *
   * <p>HFT pipeline: each emitted leaf byte[] is allocated <em>right-sized</em>
   * in a single pass — the cursor's MemorySegment keys are decoded
   * scalar-style (no keyBytes() byte[] copy), and per-chunk MemorySegment
   * values are bulk-copied into the destination buffer in a second cursor
   * walk. No intermediate {@code ArrayList<byte[]>} / {@code concat} churn.
   */
  public static List<byte[]> readAll(final StorageEngineReader reader, final int indexNumber) {
    // iter#03c: parallelize at HOT depth-2, not depth-1. The prior attempt
    // (readAllViaCursorParallel) partitioned at root fan-out 3-5 and stranded
    // 15-17 commonPool workers parked. Depth-2 drops into the 10-30+ sub-root
    // range (composite keys share a 7-byte common prefix so depth-1 is near-
    // trivial; the real fan-out lands at depth-2).
    //
    // Safety gates (see profiling-output/iter03c-parallel-depth2-analysis.md):
    //   - per-worker HOTTrieReader (no shared mutable state between readers)
    //   - shared StorageEngineReader is safe for concurrent loadHOTPage
    //     (TIL null for RO trx, cache is ConcurrentHashMap, FileChannelReader
    //     uses pooled buffers)
    //   - HOT disc-bit routing guarantees one leafIndex lives in exactly one
    //     depth-2 sub-tree → no cross-thread tombstone-merge hazard
    //   - AtomicInteger guardCount on HOTLeafPage → concurrent acquire/release safe
    //
    // Flag: -Dsirix.projection.hydrate.parallel=false forces serial fallback.
    // Fan-out < DEPTH2_MIN_FANOUT also falls back to serial internally.
    if (PARALLEL_HYDRATE) {
      return readAllViaCursorParallelDepth2(reader, indexNumber);
    }
    return readAllViaCursor(reader, indexNumber);
  }

  /**
   * Parallel cursor-based scan. Splits the HOT sub-tree rooted at
   * {@code rootRef} into per-child tasks, each running its own DFS. Results
   * are merged by composite key (leafIndex + chunkIdx) and reassembled in
   * ascending leafIndex order.
   */
  static List<byte[]> readAllViaCursorParallel(final StorageEngineReader reader, final int indexNumber) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) return Collections.emptyList();

    // Load root once on the calling thread to see its shape.
    final var rootPage = reader.loadHOTPage(rootRef);
    if (!(rootPage instanceof HOTIndirectPage rootInd)) {
      // Single-leaf tree → fall back to serial (fork-join overhead not worth it).
      return readAllViaCursor(reader, indexNumber);
    }

    final int numChildren = rootInd.getNumChildren();
    if (numChildren < 2) {
      return readAllViaCursor(reader, indexNumber);
    }

    // One task per direct child of the root. Each task runs a full DFS of
    // that sub-tree using its own HOTTrieReader + HOTRangeCursor — no shared
    // cursor state. StorageEngineReader reads are thread-safe (per-trx page
    // cache, ThreadLocal lookup buffers).
    final CompletableFuture<?>[] futures =
        new CompletableFuture<?>[numChildren];

    final PageReference[] childRefs = new PageReference[numChildren];
    for (int i = 0; i < numChildren; i++) childRefs[i] = rootInd.getChildReference(i);

    @SuppressWarnings("unchecked")
    final Long2ObjectOpenHashMap<byte[]>[] perTaskBuffers = new Long2ObjectOpenHashMap[numChildren];
    @SuppressWarnings("unchecked")
    final Long2IntOpenHashMap[] perTaskLengths =
        new Long2IntOpenHashMap[numChildren];

    for (int i = 0; i < numChildren; i++) {
      final int taskIdx = i;
      final PageReference childRef = childRefs[taskIdx];
      if (childRef == null) {
        futures[taskIdx] = CompletableFuture.completedFuture(null);
        continue;
      }
      futures[taskIdx] = CompletableFuture.runAsync(() -> {
        final Long2ObjectOpenHashMap<byte[]> bufs = new Long2ObjectOpenHashMap<>();
        final Long2IntOpenHashMap lens =
            new Long2IntOpenHashMap();
        perTaskBuffers[taskIdx] = bufs;
        perTaskLengths[taskIdx] = lens;
        collectSubtreeChunks(reader, childRef, bufs, lens);
      }, ForkJoinPool.commonPool());
    }

    // Wait for all sub-tree scans to finish.
    CompletableFuture.allOf(futures).join();

    // Merge per-task chunk buffers by composite key. Entries are
    // partitioned by leafIndex across sub-trees (HOT hashes leafIndex
    // into a disc-bit path, so each leafIndex lands in exactly one
    // sub-tree), so no key collisions between tasks in the merge.
    final Long2ObjectRBTreeMap<byte[]> outBufs =
        new Long2ObjectRBTreeMap<>();
    final Long2IntOpenHashMap outLens =
        new Long2IntOpenHashMap();
    for (int i = 0; i < numChildren; i++) {
      if (perTaskBuffers[i] == null) continue;
      outBufs.putAll(perTaskBuffers[i]);
      outLens.putAll(perTaskLengths[i]);
    }

    // Emit in ascending leafIndex order, truncating each buffer to its
    // actual written length.
    final ArrayList<byte[]> out = new ArrayList<>(outBufs.size());
    for (final var e : outBufs.long2ObjectEntrySet()) {
      final long leafIndex = e.getLongKey();
      final byte[] buf = e.getValue();
      final int len = outLens.get(leafIndex);
      out.add(len == buf.length ? buf : Arrays.copyOf(buf, len));
    }
    return out;
  }

  /**
   * Collect all chunk entries from a sub-tree rooted at {@code subRootRef},
   * aggregating them into {@code bufs}/{@code lens} keyed by leafIndex.
   * Uses its own cursor + reader — safe to call from multiple threads as long
   * as each caller passes its own bufs/lens maps.
   */
  private static void collectSubtreeChunks(final StorageEngineReader reader,
      final PageReference subRootRef,
      final Long2ObjectOpenHashMap<byte[]> bufs,
      final Long2IntOpenHashMap lens) {
    final byte[] minKey = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
    final byte[] maxKey = new byte[] {
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
    };
    final LongOpenHashSet tombstoned =
        new LongOpenHashSet();

    try (HOTTrieReader trieReader = new HOTTrieReader(reader);
         HOTRangeCursor cursor = trieReader.range(subRootRef, minKey, maxKey)) {
      while (cursor.hasNext()) {
        final var entry = cursor.next();
        final long composite = decodeCompositeKeySegment(entry.key());
        final long leafIndex = composite >> 8;
        if (tombstoned.contains(leafIndex)) continue;
        final MemorySegment value = entry.value();
        final int valueSize = (int) value.byteSize();
        if (valueSize == 0) {
          tombstoned.add(leafIndex);
          bufs.remove(leafIndex);
          lens.remove(leafIndex);
          continue;
        }
        byte[] buf = bufs.get(leafIndex);
        int len = lens.get(leafIndex);
        if (buf == null) {
          buf = new byte[Math.max(valueSize, CHUNK_SIZE)];
          bufs.put(leafIndex, buf);
          len = 0;
        } else if (len + valueSize > buf.length) {
          int newCap = buf.length * 2;
          while (newCap < len + valueSize) newCap *= 2;
          buf = Arrays.copyOf(buf, newCap);
          bufs.put(leafIndex, buf);
        }
        MemorySegment.copy(value, ValueLayout.JAVA_BYTE, 0,
            buf, len, valueSize);
        lens.put(leafIndex, len + valueSize);
      }
    }
  }

  /**
   * iter#03c parallel hydrate: enumerates HOT sub-tree roots at depth 2 of
   * the trie and dispatches one task per sub-root to the common ForkJoinPool.
   *
   * <p><b>Why depth 2 specifically.</b> The prior {@link #readAllViaCursorParallel}
   * partitioned at depth 1 (HOT root's direct children). On the projection
   * workload, composite keys share a 7-byte common prefix (the leafIndex
   * high bits) so the root's discriminative bits pick out only 3-5 paths.
   * That gave 3-5 sub-trees → 15-17 commonPool workers idle while 3-5 did
   * serial DFS internally. Depth 2 expands into 10-30+ sub-roots — enough
   * to keep a 20-core box busy, with the commonPool's work-stealing
   * absorbing any straggler variance.
   *
   * <p><b>Correctness invariants.</b> See
   * {@code profiling-output/iter03c-parallel-depth2-analysis.md}:
   * <ul>
   *   <li>Per-worker {@link HOTTrieReader} + per-worker scratch maps; no
   *       shared mutable state.</li>
   *   <li>HOT disc-bit routing guarantees a given leafIndex lives in
   *       exactly one depth-2 sub-tree → tombstones and non-tombstone
   *       chunks never split across workers.</li>
   *   <li>{@code CompletableFuture.allOf(...).join()} provides
   *       acquire-release happens-before for all per-worker map mutations
   *       relative to the merge thread.</li>
   *   <li>{@link HOTLeafPage#guardCount} is an {@code AtomicInteger}; safe
   *       under concurrent pin/release.</li>
   * </ul>
   *
   * <p><b>Fallback rules.</b>
   * <ul>
   *   <li>No projection sub-tree installed → empty list.</li>
   *   <li>Root is a leaf (single-leaf projection) → serial.</li>
   *   <li>Root fan-out &lt; 2 → serial.</li>
   *   <li>Depth-2 fan-out &lt; {@link #DEPTH2_MIN_FANOUT} → serial (not
   *       enough parallelism to amortize fork-join overhead).</li>
   * </ul>
   *
   * <p>First-time invocation per JVM logs the measured depth-2 fan-out at
   * INFO level for telemetry. Subsequent invocations silently reuse the
   * parallel path (fan-out is an invariant of the persisted index shape
   * for a given revision; we don't need to re-log).
   */
  static List<byte[]> readAllViaCursorParallelDepth2(
      final StorageEngineReader reader, final int indexNumber) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) return Collections.emptyList();

    final Page rootPage = reader.loadHOTPage(rootRef);
    if (!(rootPage instanceof HOTIndirectPage rootInd)) {
      // Root is a leaf (single-leaf tree) or null — fall back to serial.
      return readAllViaCursor(reader, indexNumber);
    }
    if (rootInd.getNumChildren() < 2) {
      return readAllViaCursor(reader, indexNumber);
    }

    // Walk depth-1 children and collect their own children as depth-2
    // sub-roots. Any depth-1 child that is itself a leaf contributes one
    // "sub-root" directly (its own ref) so its entries are still scanned
    // in parallel with the rest.
    final PageReference[] depth2Roots = enumerateDepth2SubRoots(reader, rootInd);
    final int fanout = depth2Roots.length;

    if (FANOUT_LOGGED.compareAndSet(false, true)) {
      LOGGER.info(
          "[projection-hydrate] indexNumber={} depth2_fanout={} root_children={} (serial fallback threshold={})",
          indexNumber, fanout, rootInd.getNumChildren(), DEPTH2_MIN_FANOUT);
    }

    if (fanout < DEPTH2_MIN_FANOUT) {
      return readAllViaCursor(reader, indexNumber);
    }

    // Pre-size the per-worker scratch maps proportional to the expected
    // per-worker leaf count. At 97k leaves / 20 workers ≈ 5k leaves per
    // worker, pre-sizing to 8k (next power of two with headroom) eliminates
    // fastutil rehash churn on the steady-state path.
    final int perTaskHint = Math.max(64, Integer.highestOneBit((1 << 20) / Math.max(1, fanout)) << 1);

    @SuppressWarnings("unchecked")
    final Long2ObjectOpenHashMap<byte[]>[] perTaskBufs = new Long2ObjectOpenHashMap[fanout];
    final Long2IntOpenHashMap[] perTaskLens = new Long2IntOpenHashMap[fanout];

    final CompletableFuture<?>[] futures = new CompletableFuture<?>[fanout];
    for (int i = 0; i < fanout; i++) {
      final int taskIdx = i;
      final PageReference subRootRef = depth2Roots[taskIdx];
      if (subRootRef == null) {
        futures[taskIdx] = CompletableFuture.completedFuture(null);
        continue;
      }
      futures[taskIdx] = CompletableFuture.runAsync(() -> {
        final Long2ObjectOpenHashMap<byte[]> bufs = new Long2ObjectOpenHashMap<>(perTaskHint);
        final Long2IntOpenHashMap lens = new Long2IntOpenHashMap(perTaskHint);
        lens.defaultReturnValue(0);
        perTaskBufs[taskIdx] = bufs;
        perTaskLens[taskIdx] = lens;
        collectSubtreeChunks(reader, subRootRef, bufs, lens);
      }, ForkJoinPool.commonPool());
    }

    // Barrier with acquire-release happens-before for the caller's merge.
    CompletableFuture.allOf(futures).join();

    // K-way-style merge via a single RBTree. Per-task leafIndex ranges are
    // disjoint (partitioning invariant) so putAll collides on zero keys —
    // but RBTree also gives ascending iteration for free on the emit side.
    // Pre-size the accumulator int-length map: sum worker sizes.
    int totalLeaves = 0;
    for (int i = 0; i < fanout; i++) {
      if (perTaskBufs[i] != null) totalLeaves += perTaskBufs[i].size();
    }
    final Long2ObjectRBTreeMap<byte[]> outBufs = new Long2ObjectRBTreeMap<>();
    final Long2IntOpenHashMap outLens = new Long2IntOpenHashMap(Math.max(16, totalLeaves));
    outLens.defaultReturnValue(0);
    for (int i = 0; i < fanout; i++) {
      if (perTaskBufs[i] == null) continue;
      outBufs.putAll(perTaskBufs[i]);
      outLens.putAll(perTaskLens[i]);
    }

    final ArrayList<byte[]> out = new ArrayList<>(outBufs.size());
    for (final var e : outBufs.long2ObjectEntrySet()) {
      final long leafIndex = e.getLongKey();
      final byte[] buf = e.getValue();
      final int len = outLens.get(leafIndex);
      out.add(len == buf.length ? buf : Arrays.copyOf(buf, len));
    }
    return out;
  }

  /**
   * Enumerate depth-2 sub-tree roots under {@code rootInd}. Walks each
   * depth-1 child and collects its own children; if a depth-1 child is
   * itself a leaf (no further indirects), the child's own reference is
   * used as the "sub-root" to preserve coverage. Callers get back a flat
   * {@link PageReference}[] array sized to the total depth-2 fan-out.
   *
   * <p>This is called exactly once per hydrate, on the calling thread,
   * before dispatching workers. The small number of page loads here (at
   * most root_fanout pages, typically 3-5) runs serially.
   */
  private static PageReference[] enumerateDepth2SubRoots(
      final StorageEngineReader reader, final HOTIndirectPage rootInd) {
    final int rootChildren = rootInd.getNumChildren();
    // Worst-case capacity: each depth-1 child expands to up to 32 children
    // (HOT MAX_NODE_ENTRIES). Typical fan-out is closer to 16 per sub-node.
    final ArrayList<PageReference> collected = new ArrayList<>(rootChildren * 16);
    for (int i = 0; i < rootChildren; i++) {
      final PageReference childRef = rootInd.getChildReference(i);
      if (childRef == null) continue;
      final Page childPage = reader.loadHOTPage(childRef);
      if (childPage instanceof HOTIndirectPage childInd) {
        final int grandchildren = childInd.getNumChildren();
        for (int j = 0; j < grandchildren; j++) {
          final PageReference grandRef = childInd.getChildReference(j);
          if (grandRef != null) collected.add(grandRef);
        }
      } else {
        // Depth-1 child is already a leaf (or null): treat its ref as a
        // depth-2 sub-root so its entries still get scanned. This
        // preserves completeness on unbalanced trees where one depth-1
        // child is a leaf while others are indirects.
        collected.add(childRef);
      }
    }
    return collected.toArray(new PageReference[0]);
  }

  /**
   * Legacy probe-based {@code readAll}. Kept as a correctness fallback: if the
   * cursor regresses in the future, flip the default. Not used on the hot path.
   *
   * <p>Cost: per-leaf top-down trie walk. O(leaves × depth) cold-cache page reads.
   */
  @SuppressWarnings("unused")
  static List<byte[]> readAllViaProbe(final StorageEngineReader reader, final int indexNumber) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) return Collections.emptyList();

    final int GAP_EXIT = 16;
    // Temporary storage instance — needed only to call the instance
    // {@code get} method. Opening a writer trx just for the read is
    // acceptable overhead for the one-shot fast-path hydrate call.
    // If the reader supports direct HOT lookup we use that; else
    // fall back to a wrapper that uses loadHOTPage under the hood.
    final Long2ObjectOpenHashMap<byte[]> gathered = new Long2ObjectOpenHashMap<>();
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = KEY_BUFFER.get();
      int gap = 0;
      for (long leafIndex = 0; gap < GAP_EXIT; leafIndex++) {
        byte[] buf = null;
        int len = 0;
        for (int ci = 0; ci < MAX_CHUNKS_PER_LEAF; ci++) {
          encodeCompositeKey(leafIndex, ci, keyBuf);
          final MemorySegment slice = trieReader.get(rootRef, keyBuf);
          if (slice == null) break;
          final int n = (int) slice.byteSize();
          if (n == 0) break;
          if (buf == null) {
            buf = new byte[Math.max(n, CHUNK_SIZE)];
          } else if (len + n > buf.length) {
            int newCap = buf.length * 2;
            while (newCap < len + n) newCap *= 2;
            buf = Arrays.copyOf(buf, newCap);
          }
          MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, buf, len, n);
          len += n;
        }
        if (buf == null) {
          gap++;
          continue;
        }
        gap = 0;
        gathered.put(leafIndex, len == buf.length ? buf : Arrays.copyOf(buf, len));
      }
    }

    // Emit in ascending leafIndex order.
    final long[] keys = gathered.keySet().toLongArray();
    Arrays.sort(keys);
    final ArrayList<byte[]> out = new ArrayList<>(keys.length);
    for (final long k : keys) out.add(gathered.get(k));
    return out;
  }

  /**
   * Legacy cursor-based readAll. Kept for comparison / microbenchmarks
   * — do NOT use on the hydrate hot path because {@link HOTRangeCursor}
   * can drop entries on large trees (pre-existing HOT traversal issue).
   *
   * <p>Retained because the byte-range bounds and scalar decode
   * are useful as a template for future cursor-based reads when the
   * underlying HOT traversal is fixed.
   */
  /** Package-private for cross-check correctness tests vs {@link #readAll}. */
  static List<byte[]> readAllViaCursor(final StorageEngineReader reader, final int indexNumber) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) return Collections.emptyList();

    final byte[] minKey = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
    final byte[] maxKey = new byte[] {
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
    };

    // Single-pass assembly keyed by leafIndex. HOTRangeCursor visits
    // entries in HOT tree topology order, which for the projection's
    // long-common-prefix composite keys can diverge from strict
    // key-sorted order after splits (observed: leaves 0-59, then 128+,
    // then 64-127). We therefore keep a per-leafIndex growable buffer
    // and emit in ascending order at the end — correct regardless of
    // cursor traversal quirks.
    //
    // Keys are decoded scalar-style from the MemorySegment (no
    // keyBytes() byte[] churn); values are bulk-copied from the HOT slot
    // segment via MemorySegment.copy — no valueBytes() alloc either.
    final Long2ObjectRBTreeMap<byte[]> buffers =
        new Long2ObjectRBTreeMap<>();
    final Long2IntOpenHashMap lengths =
        new Long2IntOpenHashMap();
    final LongOpenHashSet tombstoned =
        new LongOpenHashSet();

    try (HOTTrieReader trieReader = new HOTTrieReader(reader);
         HOTRangeCursor cursor = trieReader.range(rootRef, minKey, maxKey)) {
      while (cursor.hasNext()) {
        final HOTRangeCursor.Entry entry = cursor.next();
        final long composite = decodeCompositeKeySegment(entry.key());
        final long leafIndex = composite >> 8;
        if (tombstoned.contains(leafIndex)) continue;
        final MemorySegment value = entry.value();
        final int valueSize = (int) value.byteSize();
        if (valueSize == 0) {
          tombstoned.add(leafIndex);
          // A tombstoned leaf emits no payload. Drop any partial buffer
          // we had already accumulated out of sort-quirk ordering.
          buffers.remove(leafIndex);
          lengths.remove(leafIndex);
          continue;
        }

        byte[] buf = buffers.get(leafIndex);
        int len = lengths.get(leafIndex);
        if (buf == null) {
          buf = new byte[Math.max(valueSize, CHUNK_SIZE)];
          buffers.put(leafIndex, buf);
          len = 0;
        } else if (len + valueSize > buf.length) {
          int newCap = buf.length * 2;
          while (newCap < len + valueSize) newCap *= 2;
          buf = Arrays.copyOf(buf, newCap);
          buffers.put(leafIndex, buf);
        }
        MemorySegment.copy(value, ValueLayout.JAVA_BYTE, 0,
            buf, len, valueSize);
        lengths.put(leafIndex, len + valueSize);
      }
    }

    final ArrayList<byte[]> out = new ArrayList<>(buffers.size());
    for (final var e : buffers.long2ObjectEntrySet()) {
      final long leafIndex = e.getLongKey();
      final byte[] buf = e.getValue();
      final int len = lengths.get(leafIndex);
      out.add(len == buf.length ? buf : Arrays.copyOf(buf, len));
    }
    return out;
  }

  /**
   * Decode the composite key directly from its {@link MemorySegment} form
   * — skips the {@code keyBytes()} {@code byte[]} allocation. Called in
   * the cursor-hot loop inside {@link #readAll}.
   */
  private static long decodeCompositeKeySegment(final MemorySegment key) {
    final long signFlipped = key.get(
        ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN), 0);
    return signFlipped ^ 0x8000_0000_0000_0000L;
  }

  /**
   * Legacy single-key encode used by tests and by the cursor range bounds
   * in {@link #readAll} — anchors at {@code chunkIdx=0} for the given
   * {@code leafIndex}. New callers should use {@link #encodeCompositeKey}
   * directly so the chunk boundary is explicit.
   */
  public static byte[] encodeKey(final long leafIndex) {
    return encodeCompositeKey(leafIndex, 0);
  }

  /** Inverse of {@link #encodeKey} — returns the leafIndex portion only. */
  public static long decodeKey(final byte[] keyBytes) {
    return decodeCompositeKey(keyBytes)[0];
  }

  /** Number of chunks currently stored for {@code leafIndex}. Diagnostic. */
  public int chunkCountOf(final long leafIndex) {
    return countExistingChunks(leafIndex);
  }
}
