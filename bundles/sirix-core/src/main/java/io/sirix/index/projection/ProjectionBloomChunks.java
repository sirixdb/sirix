/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.page.HOTLeafPage;
import io.sirix.settings.Constants;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

/**
 * Bounded streaming store for the projection's contiguous string-fingerprint acceleration.
 *
 * <p>The original acceleration was one {@code PBLM} blob per string column. A streaming bulk load
 * therefore retained every row group's fingerprint until the final commit, even though the
 * per-row-group fingerprint segment itself had already been persisted. This store cuts that block
 * into fixed {@link #CHUNK_LEAVES}-row-group blobs and writes each full chunk immediately. The
 * writer retains only one reusable reference array per string column.</p>
 *
 * <p>A column's small, versioned manifest occupies the legacy block slot ({@code 16 + column}) and
 * is published only after every chunk is durable in the transaction. An old reader sees an unknown
 * magic where it expected {@code PBLM} and safely falls back to the per-leaf chain. A new reader
 * accepts both layouts. Missing or malformed chunks are represented as absent evidence: probes keep
 * every leaf in that span, preserving the Bloom filter's no-false-negative contract.</p>
 */
public final class ProjectionBloomChunks {

  /** Row groups per persisted fingerprint chunk. */
  static final int CHUNK_LEAVES = 256;

  /**
   * First reserved chunk slot. Row-group composite slots end at {@code 2^40 + 65535}, fence chunks
   * start at {@code 2^42}, and this namespace occupies less than {@code 2^44}. It therefore
   * cannot collide with either family and stays well inside the side-map owner-key limit
   * ({@code |ownerSlotKey| < 2^47}).
   */
  static final long CHUNK_SLOT_BASE = 1L << 43;

  /** One 16-bit chunk id covers the shared 2^24 row-group limit at 256 leaves per chunk. */
  private static final int MAX_CHUNKS = ProjectionIndexHOTStorage.MAX_ROW_GROUPS / CHUNK_LEAVES;

  /** Manifest magic, {@code "PBMF"} in little-endian byte order. */
  private static final int MANIFEST_MAGIC = 0x464D4250;
  private static final byte MANIFEST_VERSION = 1;
  private static final int MANIFEST_BYTES = Integer.BYTES + 1 + 3 * Integer.BYTES;

  /** Referenced chunk payloads held at once by one pruning call. */
  static final int FETCH_WINDOW_CHUNKS = 4;

  /** Hard payload-byte ceiling for both a chunk window and a deferred legacy compatibility read. */
  static final int MAX_FETCH_WINDOW_BYTES = FETCH_WINDOW_CHUNKS
      * ProjectionIndexColumnSegmentCodec.maxBloomBlockBytes(CHUNK_LEAVES);

  /** A deferred legacy candidate proved not to be an exact PBLM block. */
  static final int EVIDENCE_UNUSABLE = -1;

  /** Fixed owner-thread scratch; payload references are cleared before every window is released. */
  private static final ThreadLocal<FetchScratch> FETCH_SCRATCH = ThreadLocal.withInitial(FetchScratch::new);

  private ProjectionBloomChunks() {
  }

  /** Number of fixed chunks needed for {@code rowGroupCount}. */
  static int chunkCount(final int rowGroupCount) {
    checkRowGroupCount(rowGroupCount);
    return (rowGroupCount + CHUNK_LEAVES - 1) / CHUNK_LEAVES;
  }

  /**
   * Collision-free HOT blob key for one column/chunk pair.
   *
   * <p>The column occupies the high 14 useful bits of the low namespace and the chunk id the low
   * 16. Addition is intentional and safe because {@link #CHUNK_SLOT_BASE}'s low 43 bits are zero.</p>
   */
  static long chunkSlotKey(final int column, final int chunkId) {
    if (column < 0 || column >= RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalArgumentException(
          "column out of range [0, " + RowGroupDescriptor.MAX_COLUMNS + "): " + column);
    }
    if (chunkId < 0 || chunkId >= MAX_CHUNKS) {
      throw new IllegalArgumentException("chunkId out of range [0, " + MAX_CHUNKS + "): " + chunkId);
    }
    final long key = CHUNK_SLOT_BASE + ((long) column << 16) + chunkId;
    // Keep the owner-key proof executable rather than relying on the constants' documentary math.
    HOTLeafPage.overflowPageRefKey(key, 0);
    return key;
  }

  private static void checkRowGroupCount(final int rowGroupCount) {
    if (rowGroupCount < 0 || rowGroupCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException("rowGroupCount out of range [0, "
          + ProjectionIndexHOTStorage.MAX_ROW_GROUPS + "]: " + rowGroupCount);
    }
  }

  /** Fixed manifest payload; the enclosing PIXB blob supplies length and XXH3 verification. */
  private static byte[] manifest(final int rowGroupCount) {
    final byte[] bytes = new byte[MANIFEST_BYTES];
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 0, MANIFEST_MAGIC);
    bytes[Integer.BYTES] = MANIFEST_VERSION;
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, Integer.BYTES + 1, rowGroupCount);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, Integer.BYTES + 1 + Integer.BYTES, CHUNK_LEAVES);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, Integer.BYTES + 1 + 2 * Integer.BYTES,
        chunkCount(rowGroupCount));
    return bytes;
  }

  /** Parsed chunk count, or {@code -1}; a negative expected count accepts any valid manifest. */
  private static int manifestChunkCount(final byte @Nullable [] bytes, final int expectedRowGroupCount) {
    if (bytes == null || bytes.length != MANIFEST_BYTES
        || ProjectionIndexRowGroupCodec.getIntLE(bytes, 0) != MANIFEST_MAGIC
        || bytes[Integer.BYTES] != MANIFEST_VERSION) {
      return -1;
    }
    final int rowGroupCount = ProjectionIndexRowGroupCodec.getIntLE(bytes, Integer.BYTES + 1);
    final int chunkLeaves = ProjectionIndexRowGroupCodec.getIntLE(bytes, Integer.BYTES + 1 + Integer.BYTES);
    final int chunks = ProjectionIndexRowGroupCodec.getIntLE(bytes, Integer.BYTES + 1 + 2 * Integer.BYTES);
    if (rowGroupCount < 0 || rowGroupCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS
        || (expectedRowGroupCount >= 0 && rowGroupCount != expectedRowGroupCount)
        || chunkLeaves != CHUNK_LEAVES || chunks != chunkCount(rowGroupCount)) {
      return -1;
    }
    return chunks;
  }

  /** Whether {@code bytes} is the exact manifest for {@code expectedRowGroupCount}. */
  static boolean isManifest(final byte @Nullable [] bytes, final int expectedRowGroupCount) {
    return manifestChunkCount(bytes, expectedRowGroupCount) >= 0;
  }

  private static boolean isStringKind(final byte kind) {
    return kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
        || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
  }

  /**
   * Immutable per-column pruning evidence. A manifest-backed instance retains only primitive
   * durable locators (plus at most one small inline tail); payloads are fetched and released in a
   * fixed {@link #FETCH_WINDOW_CHUNKS}-chunk window by {@link #prune}.
   */
  public static final class ColumnEvidence {
    private final byte @Nullable [] inlineLegacyBlock;
    private final long legacyOffset;
    private final int legacyLength;
    private final long legacyHash;
    private final ProjectionIndexHOTStorage.@Nullable BlobLocators chunks;
    private final int rowGroupCount;

    private ColumnEvidence(final byte @Nullable [] inlineLegacyBlock, final long legacyOffset,
        final int legacyLength, final long legacyHash,
        final ProjectionIndexHOTStorage.@Nullable BlobLocators chunks, final int rowGroupCount) {
      this.inlineLegacyBlock = inlineLegacyBlock;
      this.legacyOffset = legacyOffset;
      this.legacyLength = legacyLength;
      this.legacyHash = legacyHash;
      this.chunks = chunks;
      this.rowGroupCount = rowGroupCount;
    }

    private static ColumnEvidence inlineLegacy(final byte[] block, final int rowGroupCount) {
      return new ColumnEvidence(block, Constants.NULL_ID_LONG, block.length,
          ProjectionIndexColumnSegmentCodec.contentHash(block), null, rowGroupCount);
    }

    private static ColumnEvidence deferredLegacy(final ProjectionIndexHOTStorage.BlobLocators roots,
        final int column, final int rowGroupCount) {
      return new ColumnEvidence(null, roots.offset(column), roots.length(column), roots.hash(column), null,
          rowGroupCount);
    }

    private static ColumnEvidence chunked(final ProjectionIndexHOTStorage.BlobLocators chunks,
        final int rowGroupCount) {
      return new ColumnEvidence(null, Constants.NULL_ID_LONG, -1, 0L, chunks, rowGroupCount);
    }

    /** Resident bytes charged to the decoded-handle cache. */
    private long retainedBytes() {
      long bytes = 64L;
      if (inlineLegacyBlock != null) {
        bytes += inlineLegacyBlock.length;
      }
      if (chunks != null) {
        bytes += chunks.retainedBytes();
      }
      return bytes;
    }

    /**
     * Clear bits proved absent by this evidence.
     *
     * @return newly cleared bits, or {@link #EVIDENCE_UNUSABLE} when a deferred legacy candidate is
     *         not an exact block (the caller then falls back to the authoritative per-leaf chain)
     */
    int prune(final long hash, final long[] keep, final int leafCount,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      if (leafCount != rowGroupCount) {
        throw new IllegalArgumentException("leafCount " + leafCount + " != evidence rowGroupCount "
            + rowGroupCount);
      }
      if (keep == null || keep.length < ((leafCount + 63) >>> 6) || fetcher == null) {
        throw new IllegalArgumentException("keep/fetcher must cover the evidence leaf count");
      }
      if (inlineLegacyBlock != null) {
        return pruneBlock(inlineLegacyBlock, 0, leafCount, hash, keep);
      }
      if (legacyLength >= 0) {
        return pruneDeferredLegacy(hash, keep, fetcher);
      }
      return pruneChunks(hash, keep, fetcher);
    }

    private int pruneDeferredLegacy(final long hash, final long[] keep,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      if (legacyOffset == Constants.NULL_ID_LONG
          || legacyLength > MAX_FETCH_WINDOW_BYTES
          || !ProjectionIndexColumnSegmentCodec.bloomBlockLengthCouldBeWellFormed(legacyLength, rowGroupCount)) {
        return EVIDENCE_UNUSABLE;
      }
      final FetchScratch scratch = acquireScratch();
      try {
        scratch.offsets[0] = legacyOffset;
        try {
          fetcher.fetchRange(scratch.offsets, 0, FETCH_WINDOW_CHUNKS, scratch.payloads);
        } catch (final RuntimeException unreadable) {
          return EVIDENCE_UNUSABLE;
        }
        final byte[] block = scratch.payloads[0];
        if (!referencedBlockIsValid(block, legacyLength, legacyHash, rowGroupCount)) {
          return EVIDENCE_UNUSABLE;
        }
        return pruneBlock(block, 0, rowGroupCount, hash, keep);
      } finally {
        releaseScratch(scratch);
      }
    }

    private int pruneChunks(final long hash, final long[] keep,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      final ProjectionIndexHOTStorage.BlobLocators localChunks = chunks;
      if (localChunks == null) {
        return EVIDENCE_UNUSABLE;
      }
      final FetchScratch scratch = acquireScratch();
      int dropped = 0;
      try {
        for (int windowBase = 0; windowBase < localChunks.size(); windowBase += FETCH_WINDOW_CHUNKS) {
          scratch.clearPayloadsAndOffsets();
          final int inWindow = Math.min(FETCH_WINDOW_CHUNKS, localChunks.size() - windowBase);
          boolean needsFetch = false;
          for (int j = 0; j < inWindow; j++) {
            final int chunkId = windowBase + j;
            final int expectedLeaves = expectedChunkLeaves(chunkId, rowGroupCount);
            if (localChunks.inlinePayload(chunkId) == null
                && localChunks.offset(chunkId) != Constants.NULL_ID_LONG
                && ProjectionIndexColumnSegmentCodec.bloomBlockLengthCouldBeWellFormed(
                    localChunks.length(chunkId), expectedLeaves)) {
              scratch.offsets[j] = localChunks.offset(chunkId);
              needsFetch = true;
            }
          }
          boolean fetchSucceeded = !needsFetch;
          if (needsFetch) {
            try {
              fetcher.fetchRange(scratch.offsets, 0, FETCH_WINDOW_CHUNKS, scratch.payloads);
              fetchSucceeded = true;
            } catch (final RuntimeException unreadable) {
              // Optional evidence. Referenced chunks in this window stay kept; an inline tail in
              // the same window is still independently useful below.
            }
          }
          for (int j = 0; j < inWindow; j++) {
            final int chunkId = windowBase + j;
            final int expectedLeaves = expectedChunkLeaves(chunkId, rowGroupCount);
            final byte[] inline = localChunks.inlinePayload(chunkId);
            final byte[] block;
            if (inline != null) {
              block = ProjectionIndexColumnSegmentCodec.bloomBlockIsWellFormed(inline, expectedLeaves)
                  ? inline
                  : null;
            } else {
              final byte[] fetched = fetchSucceeded
                  ? scratch.payloads[j]
                  : null;
              block = referencedBlockIsValid(fetched, localChunks.length(chunkId), localChunks.hash(chunkId),
                  expectedLeaves)
                      ? fetched
                      : null;
            }
            if (block != null) {
              dropped += pruneBlock(block, chunkId * CHUNK_LEAVES, expectedLeaves, hash, keep);
            }
          }
          // The payload window is not live across the next fetch. This explicit clear matters for
          // owner-thread scratch, which otherwise promotes the last pages into a long-lived thread.
          Arrays.fill(scratch.payloads, null);
        }
        return dropped;
      } finally {
        releaseScratch(scratch);
      }
    }
  }

  private static int expectedChunkLeaves(final int chunkId, final int rowGroupCount) {
    return Math.min(CHUNK_LEAVES, rowGroupCount - chunkId * CHUNK_LEAVES);
  }

  private static boolean referencedBlockIsValid(final byte @Nullable [] block, final int expectedLength,
      final long expectedHash, final int expectedLeaves) {
    return block != null && block.length == expectedLength
        && ProjectionIndexColumnSegmentCodec.bloomBlockLengthCouldBeWellFormed(expectedLength, expectedLeaves)
        && ProjectionIndexColumnSegmentCodec.contentHash(block) == expectedHash
        && ProjectionIndexColumnSegmentCodec.bloomBlockIsWellFormed(block, expectedLeaves);
  }

  private static int pruneBlock(final byte[] block, final int firstLeaf, final int leafCount, final long hash,
      final long[] keep) {
    int dropped = 0;
    for (int localLeaf = 0; localLeaf < leafCount; localLeaf++) {
      final int leaf = firstLeaf + localLeaf;
      final long mask = 1L << (leaf & 63);
      if ((keep[leaf >>> 6] & mask) != 0
          && !ProjectionIndexColumnSegmentCodec.bloomBlockMayContainHashValidated(block, localLeaf, leafCount,
              hash)) {
        keep[leaf >>> 6] &= ~mask;
        dropped++;
      }
    }
    return dropped;
  }

  private static FetchScratch acquireScratch() {
    final FetchScratch scratch = FETCH_SCRATCH.get();
    if (scratch.inUse) {
      // Re-entrant pruning is not a production shape, but correctness must not depend on it. The
      // bounded fallback retains the same four-payload ceiling.
      final FetchScratch nested = new FetchScratch();
      nested.inUse = true;
      nested.clearPayloadsAndOffsets();
      return nested;
    }
    scratch.inUse = true;
    scratch.clearPayloadsAndOffsets();
    return scratch;
  }

  private static void releaseScratch(final FetchScratch scratch) {
    scratch.clearPayloadsAndOffsets();
    scratch.inUse = false;
  }

  /** Package-private regression probe: owner-thread scratch must never retain fetched pages. */
  static boolean fetchScratchIsClearForTesting() {
    final FetchScratch scratch = FETCH_SCRATCH.get();
    if (scratch.inUse) {
      return false;
    }
    for (final byte[] payload : scratch.payloads) {
      if (payload != null) {
        return false;
      }
    }
    return true;
  }

  private static final class FetchScratch {
    private final long[] offsets = new long[FETCH_WINDOW_CHUNKS];
    private final byte[][] payloads = new byte[FETCH_WINDOW_CHUNKS][];
    private boolean inUse;

    private void clearPayloadsAndOffsets() {
      Arrays.fill(offsets, Constants.NULL_ID_LONG);
      Arrays.fill(payloads, null);
    }
  }

  /** Wrap legacy in-memory blocks (used by callers/tests that already assembled them). */
  static ColumnEvidence @Nullable [] fromLegacyBlocks(final byte @Nullable [] @Nullable [] blocks,
      final int rowGroupCount) {
    checkRowGroupCount(rowGroupCount);
    if (blocks == null) {
      return null;
    }
    final ColumnEvidence[] evidence = new ColumnEvidence[blocks.length];
    boolean any = false;
    for (int c = 0; c < blocks.length; c++) {
      final byte[] block = blocks[c];
      final int declared = ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(block);
      if (declared == rowGroupCount
          && ProjectionIndexColumnSegmentCodec.bloomBlockIsWellFormed(block, declared)) {
        evidence[c] = ColumnEvidence.inlineLegacy(block, rowGroupCount);
        any = true;
      }
    }
    return any ? evidence : null;
  }

  /**
   * Read every string column's legacy block or chunk manifest from a committed projection.
   * Corruption is deliberately local: an unreadable manifest disables the column acceleration;
   * an unreadable chunk leaves only its 256-row-group span unpruned.
   */
  static ColumnEvidence @Nullable [] read(final StorageEngineReader reader, final int indexNumber,
      final byte[] columnKinds, final int rowGroupCount) {
    checkRowGroupCount(rowGroupCount);
    if (reader == null || columnKinds == null || columnKinds.length > RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalArgumentException("reader and a bounded columnKinds array are required");
    }
    final ColumnEvidence[] evidence = new ColumnEvidence[columnKinds.length];
    final ProjectionIndexHOTStorage.BlobLocators roots;
    try {
      roots = ProjectionIndexHOTStorage.collectBlobLocators(reader, indexNumber,
          ProjectionIndexHOTStorage.bloomBlockSlotKey(0), columnKinds.length);
    } catch (final IllegalStateException unreadable) {
      return null;
    }
    boolean any = false;
    for (int c = 0; c < columnKinds.length; c++) {
      if (!isStringKind(columnKinds[c])) {
        continue;
      }
      final ColumnEvidence column = readColumn(reader, indexNumber, roots, c, rowGroupCount);
      if (column != null) {
        evidence[c] = column;
        any = true;
      }
    }
    return any ? evidence : null;
  }

  private static @Nullable ColumnEvidence readColumn(final StorageEngineReader reader, final int indexNumber,
      final ProjectionIndexHOTStorage.BlobLocators roots, final int column, final int rowGroupCount) {
    final byte[] root = roots.inlinePayload(column);
    final int legacyLeafCount = ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(root);
    if (legacyLeafCount == rowGroupCount
        && ProjectionIndexColumnSegmentCodec.bloomBlockIsWellFormed(root, legacyLeafCount)) {
      return ColumnEvidence.inlineLegacy(root, rowGroupCount);
    }
    if (isManifest(root, rowGroupCount)) {
      final int chunks = chunkCount(rowGroupCount);
      try {
        return ColumnEvidence.chunked(ProjectionIndexHOTStorage.collectBlobLocators(reader, indexNumber,
            chunkSlotKey(column, 0), chunks), rowGroupCount);
      } catch (final IllegalStateException unreadable) {
        return null;
      }
    }
    // A referenced root is a legacy corpus block candidate. Its payload remains deferred and must
    // declare EXACTLY rowGroupCount at probe time; anything else returns EVIDENCE_UNUSABLE so the
    // store falls through to the authoritative per-leaf chain.
    if (roots.offset(column) != Constants.NULL_ID_LONG
        && roots.length(column) <= MAX_FETCH_WINDOW_BYTES
        && ProjectionIndexColumnSegmentCodec.bloomBlockLengthCouldBeWellFormed(roots.length(column),
            rowGroupCount)) {
      return ColumnEvidence.deferredLegacy(roots, column, rowGroupCount);
    }
    return null;
  }

  /** Resident primitive/inline evidence bytes charged to the catalog's decoded-handle weight. */
  static long retainedBytes(final ColumnEvidence @Nullable [] evidence) {
    if (evidence == null) {
      return 0L;
    }
    long bytes = 32L + (long) evidence.length * Long.BYTES;
    for (final ColumnEvidence column : evidence) {
      if (column != null) {
        bytes += column.retainedBytes();
      }
    }
    return bytes;
  }

  /**
   * Owner-confined streaming writer. Accepting a row group allocates nothing: the encoded Bloom
   * segment references are placed into reusable 256-entry arrays. Only a persisted chunk payload
   * and the final fixed manifests allocate.
   */
  public static final class Writer {
    private ColumnBuffer @Nullable [] columns;
    private int @Nullable [] stringColumns;
    private int acceptedRowGroups;
    private int pendingLeaves;
    private int nextChunkId;
    private byte @Nullable [] publicationKinds;
    private int @Nullable [] priorChunkCounts;
    private boolean chunksFinished;

    /** Add one encoded row group, in exact ascending id order. */
    public void append(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded, final int rowGroupId,
        final ProjectionIndexHOTStorage storage) {
      if (chunksFinished) {
        throw new IllegalStateException("Bloom chunk writer is already finished");
      }
      if (rowGroupId != acceptedRowGroups + 1 || rowGroupId > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
        throw new IllegalArgumentException("rowGroupId must be the next id " + (acceptedRowGroups + 1)
            + " within MAX_ROW_GROUPS=" + ProjectionIndexHOTStorage.MAX_ROW_GROUPS + ", got " + rowGroupId);
      }
      if (encoded == null || storage == null) {
        throw new NullPointerException("encoded and storage must not be null");
      }
      ensureColumns(encoded.descriptor());
      final ColumnBuffer[] buffers = columns;
      final int slot = pendingLeaves;
      final int[] ids = encoded.columnSegmentIds();
      final byte[][] segments = encoded.segments();
      if (ids.length != segments.length) {
        throw new IllegalArgumentException("encoded segment ids/bytes must be index-aligned");
      }
      for (int i = 0; i < ids.length; i++) {
        final int id = ids[i];
        if (id <= 0 || id >= ProjectionIndexColumnSegmentCodec.DICT_HASH_SEGMENT_BASE
            || id % ProjectionIndexColumnSegmentCodec.SEGMENTS_PER_COLUMN != 0) {
          continue;
        }
        final int column = id / ProjectionIndexColumnSegmentCodec.SEGMENTS_PER_COLUMN - 1;
        if (column < 0 || column >= buffers.length) {
          throw new IllegalStateException("Bloom segment id " + id + " names out-of-shape column " + column
              + " of " + buffers.length);
        }
        final ColumnBuffer buffer = buffers[column];
        if (buffer == null) {
          throw new IllegalStateException("Bloom segment id " + id + " names non-string column " + column);
        }
        if (buffer.segments[slot] != null) {
          throw new IllegalStateException("Encoded row group " + rowGroupId
              + " carries two Bloom segments for column " + column);
        }
        buffer.segments[slot] = segments[i];
      }
      acceptedRowGroups++;
      pendingLeaves++;
      if (pendingLeaves == CHUNK_LEAVES) {
        flush(storage, CHUNK_LEAVES);
      }
    }

    private void ensureColumns(final byte[] descriptor) {
      final int columnCount = RowGroupDescriptor.columnCount(descriptor);
      final ColumnBuffer[] existing = columns;
      if (existing != null) {
        if (existing.length != columnCount) {
          throw new IllegalStateException("Projection column count changed during streaming build: "
              + existing.length + " -> " + columnCount);
        }
        return;
      }
      final ColumnBuffer[] created = new ColumnBuffer[columnCount];
      int stringColumnCount = 0;
      for (int c = 0; c < columnCount; c++) {
        final byte kind = RowGroupDescriptor.kind(descriptor, c);
        if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
            || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
          created[c] = new ColumnBuffer();
          stringColumnCount++;
        }
      }
      final int[] compactColumns = new int[stringColumnCount];
      int compactIndex = 0;
      for (int c = 0; c < created.length; c++) {
        if (created[c] != null) {
          compactColumns[compactIndex++] = c;
        }
      }
      columns = created;
      stringColumns = compactColumns;
    }

    private void flush(final ProjectionIndexHOTStorage storage, final int leafCount) {
      final ColumnBuffer[] buffers = columns;
      final int[] compactColumns = stringColumns;
      if (buffers != null && compactColumns != null) {
        for (final int c : compactColumns) {
          final ColumnBuffer buffer = buffers[c];
          final long slotKey = chunkSlotKey(c, nextChunkId);
          final byte[] block = ProjectionIndexColumnSegmentCodec.encodeBloomBlock(buffer.segments, leafCount);
          if (block == null) {
            // A current all-empty span must not inherit a chunk from an abandoned older build.
            storage.tombstoneRowGroup(slotKey);
          } else {
            storage.putBlob(slotKey, block);
          }
          Arrays.fill(buffer.segments, 0, leafCount, null);
        }
      }
      nextChunkId++;
      pendingLeaves = 0;
    }

    /**
     * Persist a partial tail and remember prior PBMF chunk counts before its manifest is invalidated.
     * The remembered exact counts let publication reclaim only a rebuild's trailing old range.
     */
    public void finishChunks(final ProjectionIndexHOTStorage storage, final int rowGroupCount,
        final byte[] columnKinds) {
      if (chunksFinished) {
        throw new IllegalStateException("Bloom chunks already finished");
      }
      if (storage == null || columnKinds == null || columnKinds.length > RowGroupDescriptor.MAX_COLUMNS) {
        throw new IllegalArgumentException("storage and a bounded columnKinds array are required");
      }
      if (rowGroupCount != acceptedRowGroups) {
        throw new IllegalArgumentException("rowGroupCount " + rowGroupCount + " != accepted "
            + acceptedRowGroups);
      }
      final ColumnBuffer[] buffers = columns;
      if (buffers != null && buffers.length != columnKinds.length) {
        throw new IllegalArgumentException("columnKinds length " + columnKinds.length
            + " != streamed descriptor width " + buffers.length);
      }
      publicationKinds = columnKinds.clone();
      final int[] priorCounts = new int[columnKinds.length];
      Arrays.fill(priorCounts, -1);
      final ProjectionIndexHOTStorage.BlobLocators priorRoots = storage.collectBlobLocatorsForWrite(
          ProjectionIndexHOTStorage.bloomBlockSlotKey(0), columnKinds.length);
      for (int c = 0; c < columnKinds.length; c++) {
        priorCounts[c] = manifestChunkCount(priorRoots.inlinePayload(c), -1);
      }
      priorChunkCounts = priorCounts;
      if (pendingLeaves != 0) {
        flush(storage, pendingLeaves);
      }
      if (nextChunkId != chunkCount(rowGroupCount)) {
        throw new IllegalStateException("Persisted " + nextChunkId + " Bloom chunks for " + rowGroupCount
            + " row groups; expected " + chunkCount(rowGroupCount));
      }
      chunksFinished = true;
    }

    /**
     * Publish one manifest per string column. Call only after old block/manifest slots were
     * invalidated and after {@link #finishChunks}; this is the visibility point for all chunks.
     */
    public void publishManifests(final ProjectionIndexHOTStorage storage, final int rowGroupCount) {
      if (!chunksFinished || rowGroupCount != acceptedRowGroups) {
        throw new IllegalStateException("finishChunks must complete for the same rowGroupCount before publication");
      }
      final byte[] kinds = publicationKinds;
      final int[] priorCounts = priorChunkCounts;
      if (kinds == null || priorCounts == null) {
        throw new IllegalStateException("finishChunks did not capture publication shape");
      }
      final int currentChunkCount = chunkCount(rowGroupCount);
      final ColumnBuffer[] buffers = columns;
      // Reclaim before publication. For a still-string column, only the exact old trailing range is
      // stale; for a column whose final representation is no longer local-string, both old chunks
      // and any chunks emitted before its representation election are unreachable and reclaimed.
      for (int c = 0; c < kinds.length; c++) {
        final boolean publish = isStringKind(kinds[c]);
        final int generatedChunks = buffers != null && buffers[c] != null
            ? currentChunkCount
            : 0;
        final int oldChunks = Math.max(priorCounts[c], 0);
        final int firstStale = publish
            ? currentChunkCount
            : 0;
        final int staleEnd = Math.max(oldChunks, publish
            ? 0
            : generatedChunks);
        for (int chunkId = firstStale; chunkId < staleEnd; chunkId++) {
          storage.tombstoneRowGroup(chunkSlotKey(c, chunkId));
        }
      }
      // PBMF is the visibility point and therefore strictly last.
      for (int c = 0; c < kinds.length; c++) {
        if (isStringKind(kinds[c])) {
          storage.putBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(c), manifest(rowGroupCount));
        }
      }
    }

    /** Drop references to the bounded pending segment set after finish or abort. */
    public void release() {
      final ColumnBuffer[] buffers = columns;
      if (buffers != null) {
        for (final ColumnBuffer buffer : buffers) {
          if (buffer != null) {
            Arrays.fill(buffer.segments, null);
          }
        }
      }
      columns = null;
      stringColumns = null;
      publicationKinds = null;
      priorChunkCounts = null;
      pendingLeaves = 0;
    }
  }

  private static final class ColumnBuffer {
    private final byte[][] segments = new byte[CHUNK_LEAVES][];
  }
}
