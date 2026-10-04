/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.index.hot;

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * Generic HOT index writer for object keys (CASValue, QNm).
 *
 * <p>
 * Stores secondary-index postings in a height-optimized trie. Uses thread-local buffers for
 * zero-allocation key serialization.
 * </p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 * <li>Thread-local byte buffers for key/value serialization</li>
 * <li>No Optional - uses @Nullable returns</li>
 * <li>Pre-allocated traversal state</li>
 * </ul>
 *
 * @param <K> the key type (must implement Comparable)
 * @author Johannes Lichtenberger
 */
public final class HOTIndexWriter<K extends Comparable<? super K>> extends AbstractHOTIndexWriter<K> {

  /**
   * Thread-local buffer for key serialization. The largest escaped CAS prefix is 504 bytes; its chunk
   * trailer and optional delta suffix fit in 512 bytes. NAME keys grow the buffer when their
   * serialized names need more room.
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[512]);

  /**
   * Thread-local single-bit chunk-payload {@link NodeReferences} reused across writes to avoid
   * per-call {@code Roaring64Bitmap} allocation. {@link #addNodeKeyToChunk(Comparable, long)} clears
   * the bitmap, sets one bit, and serialises into {@link #lastSerializedValueBuf}.
   */
  private static final ThreadLocal<NodeReferences> SINGLE_BIT_REFS = ThreadLocal.withInitial(NodeReferences::new);

  private final HOTKeySerializer<K> keySerializer;

  /** Whether hot chunks of this index take the append-only delta path ({@link PostingDeltas}). */
  private final boolean postingDeltas;
  private final int hotChunkBytes;
  private final int foldBound;

  /** Transaction-confined scratch; no per-change delta arrays. */
  private final @Nullable ChunkView chunkView;

  private static final AtomicLong DELTA_WRITES = new AtomicLong();
  private static final AtomicLong DELTA_FOLDS = new AtomicLong();

  /** Delta slots written, counted when the existing HOT merge diagnostics are enabled. */
  public static long postingDeltaWrites() {
    return DELTA_WRITES.get();
  }

  /** In-memory folds, counted when the existing HOT merge diagnostics are enabled. */
  public static long postingDeltaFolds() {
    return DELTA_FOLDS.get();
  }

  private static final int DELTA_NOT_APPLICABLE = 0;
  private static final int DELTA_SKIPPED = 1;
  private static final int DELTA_WRITTEN = 2;
  private static final int DELTA_FOLDED = 3;

  /** Lazy reader for chunked-bitmap reassembly during {@link #get} / range scans. */
  private @Nullable HOTTrieReader chunkReader;

  /**
   * Private constructor.
   *
   * @param storageEngineWriter the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType the index type (PATH, CAS, NAME)
   * @param indexNumber the index number
   */
  private HOTIndexWriter(StorageEngineWriter storageEngineWriter, HOTKeySerializer<K> keySerializer,
      IndexType indexType, int indexNumber, int hotChunkBytes, int foldBound) {
    super(storageEngineWriter, indexType, indexNumber);
    this.keySerializer = requireNonNull(keySerializer);
    this.postingDeltas = indexType == IndexType.CAS || indexType == IndexType.VALIDTIME;
    this.hotChunkBytes = hotChunkBytes;
    this.foldBound = foldBound;
    this.chunkView = postingDeltas
        ? new ChunkView(foldBound)
        : null;

    // Initialize HOT index tree based on type
    initializeHOTIndex();
  }

  /**
   * Initialize the HOT index tree structure.
   */
  private void initializeHOTIndex() {
    switch (indexType) {
      case PATH -> initializePathIndex();
      case CAS -> initializeCASIndex();
      case NAME -> initializeNameIndex();
      case VALIDTIME -> initializeValidTimeIndex();
      default -> throw new IllegalArgumentException("Unsupported index type for HOT: " + indexType);
    }
  }

  /**
   * Creates a new HOTIndexWriter.
   *
   * @param storageEngineWriter the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType the index type
   * @param indexNumber the index number
   * @param <K> the key type
   * @return a new HOTIndexWriter instance
   */
  public static <K extends Comparable<? super K>> HOTIndexWriter<K> create(StorageEngineWriter storageEngineWriter,
      HOTKeySerializer<K> keySerializer, IndexType indexType, int indexNumber) {
    return create(storageEngineWriter, keySerializer, indexType, indexNumber, PostingDeltas.HOT_CHUNK_BYTES,
        PostingDeltas.FOLD_BOUND);
  }

  /** Transaction-local test seam for exercising geometry across different delta bounds. */
  static <K extends Comparable<? super K>> HOTIndexWriter<K> create(StorageEngineWriter storageEngineWriter,
      HOTKeySerializer<K> keySerializer, IndexType indexType, int indexNumber, int hotChunkBytes, int foldBound) {
    requireNonNull(storageEngineWriter);
    requireNonNull(indexType);
    if (hotChunkBytes <= 0 || foldBound < 2 || foldBound > PostingDeltas.MAX_SEQ + 1) {
      throw new IllegalArgumentException("invalid posting delta thresholds");
    }
    HOTIndexNumberValidator.validate(storageEngineWriter, indexType, indexNumber);
    return new HOTIndexWriter<>(storageEngineWriter, keySerializer, indexType, indexNumber, hotChunkBytes, foldBound);
  }

  /**
   * Index a key-value pair using chunked-bitmap storage.
   *
   * <p>
   * The logical {@link NodeReferences} is split across multiple HOT slots, one per <em>chunk</em>. A
   * chunk holds the low-16 bits of all nodeKeys whose {@code (int)(nodeKey >>> 16)} equals its
   * chunkIdx. The HOT key for a chunk is the composite {@code prefix(key) ‖ chunkIdx_be4}; see
   * {@link HOTKeySerializer#serializeWithChunkIdx}.
   * </p>
   *
   * <h3>Why chunk?</h3>
   * <p>
   * Per-revision write cost grows with the size of the slot value rewritten on update. Without
   * chunking, every commit that touches a single nodeKey on a popular logical key rewrites the whole
   * bitmap (potentially MBs). With chunking, only the one Roaring chunk of the modified nodeKey is
   * rewritten — typical chunk size is a few hundred bytes.
   * </p>
   *
   * <p>
   * If the chunk slot already exists, {@link HOTLeafPage#mergeWithNodeRefs} handles the OR-merge of
   * the new bit into the existing chunk's bitmap; failure paths (page split / compact) are inherited
   * unchanged from the per-slot write.
   * </p>
   *
   * @param key the logical index key (e.g. a {@code QNm} for NAME, a {@code CASValue} for CAS)
   * @param value the node references
   * @return the supplied node references after all contained node keys have been indexed
   */
  public NodeReferences index(K key, NodeReferences value) {
    requireNonNull(key);
    requireNonNull(value);

    final Roaring64Bitmap bitmap = value.getNodeKeys();
    if (bitmap.isEmpty()) {
      return value;
    }
    final LongIterator it = bitmap.getLongIterator();
    while (it.hasNext()) {
      addNodeKeyToChunk(key, it.next());
    }
    return value;
  }

  /**
   * Add a single nodeKey to {@code key}'s chunked bitmap.
   *
   * <p>
   * Equivalent to {@link #index(Comparable, NodeReferences)} with a one-element
   * {@link NodeReferences}, minus the {@code Roaring64Bitmap} allocation: the slot write is an
   * OR-merge ({@link HOTLeafPage#mergeWithNodeRefs}), so a caller that only wants to ADD one
   * reference avoids allocating a one-element bitmap. Hot CAS and VALIDTIME chunks instead append a
   * delta after checking membership against the base and its bounded live deltas.
   * </p>
   *
   * @param key the logical index key
   * @param nodeKey the node key to add; must be in {@code [0, 2^48)}
   */
  public void indexNodeKey(K key, long nodeKey) {
    requireNonNull(key);
    addNodeKeyToChunk(key, nodeKey);
  }

  /**
   * A loader that collects {@code (key, nodeKey)} pairs and materialises the whole index in one
   * {@link HOTBulkBuilder} pass — the right shape for building an index over an already-shredded
   * revision.
   *
   * <p>
   * Only valid while the index tree is still empty ({@link #isEmptyTree()}): the loader
   * <em>replaces</em> the root rather than merging into it. Callers that may run against a populated
   * tree must check first and fall back to {@link #indexNodeKey(Comparable, long)}.
   * </p>
   *
   * @return a fresh bulk loader bound to this writer
   */
  public HOTBulkIndexLoader<K> createBulkLoader() {
    return new HOTBulkIndexLoader<>(this, keySerializer);
  }

  /**
   * Add one nodeKey to its chunk slot. Chunked-bitmap write hot path.
   *
   * <p>
   * Hot CAS and VALIDTIME chunks check membership before appending a delta or folding. Other chunks
   * OR-merge a reusable single-bit payload through {@link AbstractHOTIndexWriter#doIndex}.
   * </p>
   */
  private void addNodeKeyToChunk(K key, long nodeKey) {
    AbstractHOTIndexWriter.checkNodeKeyRange(nodeKey);

    final int chunkIdx = (int) (nodeKey >>> 16);
    final long bit16 = nodeKey & 0xFFFFL;

    final byte[] keyBuf = chunkedKeyBuffer(key);
    final int compLen = keySerializer.serializeWithChunkIdx(key, chunkIdx, keyBuf, 0);

    if (postingDeltas && chunkIdx >= 0 && applyPostingDelta(keyBuf, compLen, bit16, false) != DELTA_NOT_APPLICABLE) {
      return;
    }

    // Reusable single-bit payload — clear, set, serialize. Avoids per-call bitmap allocation.
    serializeSingleBit(bit16);

    doIndex(keyBuf, compLen, lastSerializedValueBuf, lastSerializedValueLen);
  }

  private void serializeSingleBit(final long bit16) {
    final NodeReferences singleBit = SINGLE_BIT_REFS.get();
    final Roaring64Bitmap singleBitmap = singleBit.getNodeKeys();
    singleBitmap.clear();
    singleBitmap.add(bit16);
    serializeValueInto(singleBit);
  }

  // ---------------------------------------------------------------------------------------------
  // Append-only posting deltas (see PostingDeltas): a hot chunk's change is one tiny delta slot;
  // At the fold bound, the live deltas and current operation replace the base once.
  // ---------------------------------------------------------------------------------------------

  /** The writer's view of one chunk: its base payload and its live deltas in application order. */
  private static final class ChunkView {
    private boolean baseExists;
    private int baseLength;
    private @Nullable Roaring64Bitmap baseBits;
    private byte[] cachedBase = new byte[0];
    private byte[] baseReadBuffer = new byte[0];
    private int cachedBaseLength = -1;
    private int liveCount;
    private final boolean[] liveRemove;
    private final long[] liveBit;

    private ChunkView(final int foldBound) {
      liveRemove = new boolean[foldBound - 1];
      liveBit = new long[foldBound - 1];
    }

    /** Reuse a decoded base only after comparing every current payload byte under the leaf guard. */
    private void readBase(final HOTLeafPage leaf, final long ref, final int length) {
      if (length > leaf.slotCapacity()) {
        throw new IllegalStateException("posting base exceeds its leaf slot capacity");
      }
      if (baseReadBuffer.length < length) {
        baseReadBuffer = new byte[Math.max(length, baseReadBuffer.length * 2)];
      }
      leaf.copyRefInto(ref, 0, baseReadBuffer, 0, length);
      finishReadBase(length);
    }

    private void readBase(final byte[] payload) {
      if (cachedBaseLength != payload.length
          || !Arrays.equals(cachedBase, 0, payload.length, payload, 0, payload.length)) {
        if (baseReadBuffer.length < payload.length) {
          baseReadBuffer = new byte[Math.max(payload.length, baseReadBuffer.length * 2)];
        }
        System.arraycopy(payload, 0, baseReadBuffer, 0, payload.length);
        cacheReadBase(payload.length);
      }
      baseLength = payload.length;
      baseExists = true;
    }

    private void finishReadBase(final int length) {
      if (cachedBaseLength != length || !Arrays.equals(cachedBase, 0, length, baseReadBuffer, 0, length)) {
        cacheReadBase(length);
      }
      baseLength = length;
      baseExists = true;
    }

    private void cacheReadBase(final int length) {
      final byte[] previous = cachedBase;
      cachedBase = baseReadBuffer;
      baseReadBuffer = previous;
      baseBits = NodeReferencesSerializer.deserializeChunk(cachedBase, 0, length).getNodeKeys();
      cachedBaseLength = length;
    }

    private void addLive(final int seq, final boolean remove, final long bit16) {
      if (seq != liveCount || liveCount >= liveBit.length) {
        throw new IllegalStateException(
            "non-contiguous or overfull posting delta sequence: " + seq + " at " + liveCount);
      }
      liveRemove[liveCount] = remove;
      liveBit[liveCount] = bit16;
      liveCount++;
    }
  }

  private ChunkView readChunkView(final byte[] keyBuf, final int compLen) {
    final ChunkView view = requireNonNull(chunkView);
    view.baseExists = false;
    view.baseLength = 0;
    view.liveCount = 0;
    final PageReference rootRef = rootReference;
    if (rootRef == null) {
      return view;
    }
    final HOTLeafPage baseLeaf = acquireLeafForRead(keyBuf, compLen);
    if (baseLeaf == null) {
      return view;
    }
    Throwable failure = null;
    try {
      final int index = baseLeaf.findEntry(keyBuf, compLen);
      if (index < 0) {
        return view;
      }
      final long ref = baseLeaf.valueRef(index);
      final int length = HOTLeafPage.refLength(ref);
      if (NodeReferencesSerializer.isReferenced(baseLeaf, ref)) {
        view.readBase(NodeReferencesSerializer.resolveReferencedPayload(baseLeaf,
            NodeReferencesSerializer.referencedKey(baseLeaf, ref),
            NodeReferencesSerializer.referencedPayloadLength(baseLeaf, ref),
            NodeReferencesSerializer.referencedPayloadHash(baseLeaf, ref),
            storageEngineWriter.getResourceSession().getResourceConfig().verifyChecksumsOnRead,
            storageEngineWriter::readSideOverflowPage));
      } else {
        if (length < hotChunkBytes) {
          return view; // A cold base cannot have live deltas: only a fold changes the base.
        }
        view.readBase(baseLeaf, ref, length);
      }
      // The guard already protects the base leaf. If the next logical chunk is on it too, every
      // delta fits in this leaf and no second tree descent or range-cursor allocation is needed.
      final int end = baseLeaf.getEntryCount();
      if (baseLeaf.compareKeyPrefix(end - 1, keyBuf, compLen) > 0) {
        readLeafDeltas(view, baseLeaf, index + 1, keyBuf, compLen);
        return view;
      }
      // A leaf boundary may split the chunk from its deltas. Re-walk that bounded suffix range.
      view.liveCount = 0;
    } catch (final RuntimeException | Error e) {
      failure = e;
      throw e;
    } finally {
      releaseLeafReadGuard(baseLeaf, failure);
    }
    if (chunkReader == null) {
      chunkReader = new HOTTrieReader(storageEngineWriter);
    }
    final int deltaLen = compLen + PostingDeltas.SUFFIX_BYTES;
    HOTKeySerializer.writeChunkIdxBE(keyBuf, compLen, PostingDeltas.suffix(0, false));
    // One stamp covers a whole leaf's delta batch. Scratch changes are retained only after that
    // stamp validates; a torn batch rolls back its count and retries the same leaf and position.
    // Closing the reusable reader also releases a guard acquired by its bounded recovery path.
    try (HOTTrieReader reader = chunkReader) {
      final HOTTrieReader.LowerBoundResult start = reader.lowerBound(rootRef, keyBuf, deltaLen);
      HOTLeafPage leaf = start.leaf;
      int index = start.indexInLeaf;
      int tornRounds = 0;
      while (leaf != null) {
        final int liveBeforeLeaf = view.liveCount;
        final boolean complete;
        try {
          complete = readLeafDeltas(view, leaf, index, keyBuf, compLen);
        } catch (final RuntimeException e) {
          if (reader.validateCurrentLeaf()) {
            throw e;
          }
          view.liveCount = liveBeforeLeaf;
          reader.recoverTorn(++tornRounds, "posting delta leaf batch");
          leaf = reader.currentLeafPage();
          continue;
        }
        if (!reader.validateCurrentLeaf()) {
          view.liveCount = liveBeforeLeaf;
          reader.recoverTorn(++tornRounds, "posting delta leaf batch");
          leaf = reader.currentLeafPage();
          continue;
        }
        tornRounds = 0;
        if (complete) {
          break;
        }
        leaf = reader.advanceToNextLeaf();
        index = 0;
      }
    }
    return view;
  }

  /** Read one leaf batch; the caller must guard it or validate before keeping the added deltas. */
  private static boolean readLeafDeltas(final ChunkView view, final HOTLeafPage leaf, final int start,
      final byte[] keyBuf, final int compLen) {
    final int end = leaf.getEntryCount();
    final boolean sharedPrefix = leaf.getCommonPrefixLen() >= compLen;
    if (sharedPrefix && start < end) {
      final int order = leaf.compareKeyPrefix(start, keyBuf, compLen);
      if (order != 0) {
        return order > 0;
      }
    }
    for (int index = start; index < end; index++) {
      if (!sharedPrefix) {
        final int order = leaf.compareKeyPrefix(index, keyBuf, compLen);
        if (order > 0) {
          return true;
        }
        if (order < 0) {
          continue;
        }
      }
      if (leaf.getKeyLength(index) != compLen + PostingDeltas.SUFFIX_BYTES) {
        throw new IllegalStateException("invalid posting delta key length");
      }
      final long suffix = leaf.readKeyIntBE(index, compLen) & 0xFFFFFFFFL;
      if (!PostingDeltas.isDelta(suffix)
          || suffix > (PostingDeltas.suffix(PostingDeltas.MAX_SEQ, true) & 0xFFFFFFFFL)) {
        throw new IllegalStateException("invalid posting delta suffix");
      }
      final long bit = NodeReferencesSerializer.readDeltaBit(leaf, leaf.valueRef(index));
      if (bit >= 0) {
        view.addLive(PostingDeltas.seq(suffix), PostingDeltas.isRemove(suffix), bit);
      }
    }
    return false;
  }

  /**
   * Try to record one add/remove of {@code bit16} in the composite chunk as a delta.
   *
   * @return {@link #DELTA_NOT_APPLICABLE} when the chunk is not hot (the caller takes the direct
   *         chunk path), {@link #DELTA_SKIPPED} when the operation would not change the postings (the
   *         byte-equal skip), {@link #DELTA_WRITTEN} or {@link #DELTA_FOLDED}
   */
  private int applyPostingDelta(final byte[] keyBuf, final int compLen, final long bit16, final boolean remove) {
    storageEngineWriter.assertTransactionWritable();
    try {
      return applyPostingDeltaChecked(keyBuf, compLen, bit16, remove);
    } catch (final RuntimeException | Error failure) {
      markTransactionRollbackOnly(failure);
      throw failure;
    }
  }

  private int applyPostingDeltaChecked(final byte[] keyBuf, final int compLen, final long bit16, final boolean remove) {
    final ChunkView view = readChunkView(keyBuf, compLen);
    if (!view.baseExists || (view.liveCount == 0 && view.baseLength < hotChunkBytes)) {
      return DELTA_NOT_APPLICABLE;
    }
    boolean present = false;
    boolean changedByDelta = false;
    for (int i = view.liveCount - 1; i >= 0; i--) {
      if (view.liveBit[i] == bit16) {
        present = !view.liveRemove[i];
        changedByDelta = true;
        break;
      }
    }
    if (!changedByDelta) {
      present = view.baseBits != null && view.baseBits.contains(bit16);
    }
    if (remove != present) {
      return DELTA_SKIPPED; // adding a present key or removing an absent one changes nothing
    }
    if (view.liveCount + 1 >= foldBound) {
      foldPostingDeltas(keyBuf, compLen, view, bit16, remove);
      if (VersioningType.hotMergeDiagEnabled()) {
        DELTA_FOLDS.incrementAndGet();
      }
      return DELTA_FOLDED;
    }
    final int deltaLen = compLen + PostingDeltas.SUFFIX_BYTES;
    HOTKeySerializer.writeChunkIdxBE(keyBuf, compLen, PostingDeltas.suffix(view.liveCount, remove));
    serializeSingleBit(bit16);
    doIndex(keyBuf, deltaLen, lastSerializedValueBuf, lastSerializedValueLen);
    if (VersioningType.hotMergeDiagEnabled()) {
      DELTA_WRITES.incrementAndGet();
    }
    return DELTA_WRITTEN;
  }

  /**
   * Fold in memory and replace the base once before tombstoning the known delta slots. Readers at
   * older revisions keep the older base plus its live deltas.
   */
  private void foldPostingDeltas(final byte[] keyBuf, final int compLen, final ChunkView view, final long bit16,
      final boolean remove) {
    final byte[] baseKey = Arrays.copyOf(keyBuf, compLen);
    // Fold in memory — the base bits, the live deltas in application order, then the operation — and
    // store the result once. Hot payloads live in side pages referenced by their leaf markers.
    final Roaring64Bitmap folded = requireNonNull(view.baseBits);
    view.cachedBaseLength = -1; // The cached bitmap is about to diverge from its payload bytes.
    for (int i = 0; i < view.liveCount; i++) {
      if (view.liveRemove[i]) {
        folded.removeLong(view.liveBit[i]);
      } else {
        folded.addLong(view.liveBit[i]);
      }
    }
    if (remove) {
      folded.removeLong(bit16);
    } else {
      folded.addLong(bit16);
    }
    serializeValueInto(NodeReferences.owning(folded));
    doReplacePostingChunk(baseKey, compLen, lastSerializedValueBuf, lastSerializedValueLen,
        lastSerializedValueLen >= hotChunkBytes);
    final byte[] deltaKey = Arrays.copyOf(baseKey, compLen + PostingDeltas.SUFFIX_BYTES);
    HOTLeafPage writableLeaf = null;
    for (int i = 0; i < view.liveCount; i++) {
      HOTKeySerializer.writeChunkIdxBE(deltaKey, compLen, PostingDeltas.suffix(i, view.liveRemove[i]));
      int index = writableLeaf == null
          ? -1
          : writableLeaf.findEntry(deltaKey, deltaKey.length);
      if (writableLeaf == null || index < 0) {
        // Suffixes are visited in key order. Tombstones retain their keys and do not split or
        // consolidate leaves, so one writable descent suffices for every delta on this leaf.
        writableLeaf = prepareLeafOfTree(rootReference, deltaKey, deltaKey.length).leaf();
        index = writableLeaf.findEntry(deltaKey, deltaKey.length);
      }
      if (index < 0
          || NodeReferencesSerializer.readDeltaBit(writableLeaf, writableLeaf.valueRef(index)) != view.liveBit[i]
          || !writableLeaf.deleteAt(index)) {
        // applyPostingDelta owns the rollback-only catch, including failures after partial cleanup.
        throw new IllegalStateException("missing or changed delta during posting fold");
      }
    }
  }


  /**
   * Reassemble all chunks of a logical key into a single {@link NodeReferences}.
   *
   * <p>
   * Range-scans composite keys in {@code [(prefix, 0), (prefix, 0xFFFFFFFF)]} via
   * {@link HOTTrieReader#lowerBound} (Phase 0b — Binna §4.2) so the seek is O(tree-height) even when
   * the smallest existing chunkIdx for {@code key} is {@code > 0}. For every matching chunk slot the
   * value bitmap is decoded and each bit16 is expanded to a full 64-bit nodeKey via
   * {@code (chunkIdx << 16) | bit16}.
   * </p>
   *
   * @param key the logical index key
   * @param mode must be {@link SearchMode#EQUAL}; range modes go through the reader's
   *        {@code range}/{@code iteratorFrom} APIs
   * @return reassembled NodeReferences, or {@code null} if no chunks exist for {@code key}
   * @throws IllegalArgumentException if {@code mode} is not {@link SearchMode#EQUAL}. This parameter
   *         used to be documented as advisory and silently ignored, i.e. every mode got the
   *         {@code EQUAL} answer; the guard is shared with {@link HOTIndexReader#get} so a caller
   *         cannot learn a different contract from whichever of the two twins it happened to pick.
   */
  public @Nullable NodeReferences get(final K key, final SearchMode mode) {
    requireNonNull(key);
    // Same contract as the reader's get, enforced through the reader's helper so there is ONE copy of
    // the rule. A writer-backed lookup is never memoized, so the cache-key argument does not apply
    // here — but this method ignores `mode` exactly as the reader's did, and leaving the twin
    // unguarded is how a caller learns the restriction from whichever of the two it happened to pick.
    AbstractHOTIndexReader.requireEqualMode(mode);

    final byte[] keyBuf = prefixKeyBuffer(key);
    final int prefixLen = keySerializer.serialize(key, keyBuf, 0);
    return reassembleChunksForPrefix(keyBuf, prefixLen);
  }

  /**
   * Visible to {@link AbstractHOTIndexWriter} sub-paths and internal helpers — assemble the
   * NodeReferences for a prefix already in {@code prefixBuf[0..prefixLen)}.
   */
  private @Nullable NodeReferences reassembleChunksForPrefix(byte[] prefixBuf, int prefixLen) {
    final PageReference rootRef = rootReference;
    if (rootRef == null) {
      return null;
    }

    final byte[] fromBytes = new byte[prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(prefixBuf, 0, fromBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(fromBytes, prefixLen, 0);

    final byte[] toBytes = new byte[prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(prefixBuf, 0, toBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(toBytes, prefixLen, 0xFFFFFFFF);

    if (chunkReader == null) {
      chunkReader = new HOTTrieReader(storageEngineWriter);
    }
    // The sweep reads UNPINNED leaves under optimistic stamps — the shared helper validates each
    // slot's copies against the cursor's leaf stamp before anything reaches the deserializer.
    final Roaring64Bitmap merged;
    try (HOTRangeCursor cursor = chunkReader.range(rootRef, fromBytes, toBytes)) {
      merged = NodeReferencesSerializer.mergeChunksInPrefixRange(cursor, prefixBuf, prefixLen, postingDeltas);
    }
    if (merged == null || merged.isEmpty()) {
      return null;
    }
    return NodeReferences.owning(merged);
  }

  /**
   * Remove a single nodeKey from the chunked bitmap of {@code key}.
   *
   * <p>
   * Hot CAS and VALIDTIME chunks use the same membership check and delta/fold path as additions.
   * Other chunks use {@link AbstractHOTIndexWriter#doRemovePostingBit}; a referenced base is resolved
   * before mutation and its replacement keeps the side page and marker together.
   * </p>
   *
   * @return true if a bit was actually cleared, false if absent
   */
  public boolean remove(K key, long nodeKey) {
    requireNonNull(key);
    AbstractHOTIndexWriter.checkNodeKeyRange(nodeKey);

    final int chunkIdx = (int) (nodeKey >>> 16);
    final long bit16 = nodeKey & 0xFFFFL;

    final byte[] keyBuf = chunkedKeyBuffer(key);
    final int compLen = keySerializer.serializeWithChunkIdx(key, chunkIdx, keyBuf, 0);

    if (postingDeltas && chunkIdx >= 0) {
      final int outcome = applyPostingDelta(keyBuf, compLen, bit16, true);
      if (outcome == DELTA_SKIPPED) {
        return false;
      }
      if (outcome != DELTA_NOT_APPLICABLE) {
        return true;
      }
    }
    return doRemovePostingBit(keyBuf, compLen, bit16);
  }

  /**
   * The thread-local key buffer, grown first if {@code key} could need more than it currently holds.
   *
   * <p>
   * Sizing has to happen BEFORE the write. The previous shape serialized into the buffer and only
   * then compared the returned length against {@code buffer.length} — by which point a key larger
   * than the buffer had already been written past its end. Only NAME keys can reach that: a CAS key
   * is bounded by a constant well under the buffer, but a local name is raw UTF-8 of whatever the
   * document called the field.
   * </p>
   *
   * @param key the key about to be serialized
   * @return a buffer with room for {@code key}'s prefix
   */
  private byte[] prefixKeyBuffer(final K key) {
    return keyBufferOfAtLeast(keySerializer.maxSerializedLength(key));
  }

  /** As {@link #prefixKeyBuffer}, with room for the chunk trailer and any posting delta suffix. */
  private byte[] chunkedKeyBuffer(final K key) {
    return keyBufferOfAtLeast(keySerializer.maxSerializedLength(key) + HOTKeySerializer.CHUNK_IDX_BYTES + (postingDeltas
        ? PostingDeltas.SUFFIX_BYTES
        : 0));
  }

  private static byte[] keyBufferOfAtLeast(final int required) {
    byte[] keyBuf = KEY_BUFFER.get();
    if (required > keyBuf.length) {
      keyBuf = new byte[required];
      KEY_BUFFER.set(keyBuf);
    }
    return keyBuf;
  }

  @Override
  protected byte[] getKeyBuffer() {
    return KEY_BUFFER.get();
  }

  @Override
  protected void setKeyBuffer(byte[] newBuffer) {
    KEY_BUFFER.set(newBuffer);
  }

  @Override
  protected int serializeKey(K key, byte[] buffer, int offset) {
    return keySerializer.serialize(key, buffer, offset);
  }
}
